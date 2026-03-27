# -*- coding: utf-8 -*-
# Copyright (c) 2024 youngcan. All Rights Reserved.
# 本代码仅供个人学习研究使用，未经授权不得用于商业目的。
# 商业授权请联系作者支付授权费用。

"""
统一数据源：个股日线 tushare 优先（qfq）→ akshare→baostock→efinance；大盘 tushare 直连

输出格式与 akshare 兼容：日期, 开盘, 最高, 最低, 收盘, 成交量, 成交额, 涨跌幅, 换手率, 振幅
"""

from __future__ import annotations

import atexit
import json
import os
import re
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from datetime import date, timedelta
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Literal
from http.client import RemoteDisconnected

import pandas as pd


_L1_CACHE: dict[str, pd.DataFrame] = {}
_L1_CACHE_LOCK = threading.RLock()
_BAOSTOCK_LOGGED = False
_BAOSTOCK_EXIT_HOOKED = False
_BAOSTOCK_MODULE = None
_BAOSTOCK_LOCK = threading.RLock()
_SPOT_SNAPSHOT_TTL_SECONDS = int(os.getenv("SPOT_SNAPSHOT_TTL_SECONDS", "20"))
_SPOT_SNAPSHOT_TIMEOUT_SECONDS = float(
    os.getenv("SPOT_SNAPSHOT_TIMEOUT_SECONDS", "8.0")
)
_SPOT_SNAPSHOT_TS = 0.0
_SPOT_SNAPSHOT_MAP: dict[str, dict[str, float | None]] = {}
_SPOT_SNAPSHOT_LOCK = threading.RLock()
_SPOT_TURNOVER_MAX_REL_ERR = float(os.getenv("SPOT_TURNOVER_MAX_REL_ERR", "0.35"))
_DATA_SOURCE_DEBUG = os.getenv("DATA_SOURCE_DEBUG", "").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
_BAOSTOCK_MAX_SECONDS = float(os.getenv("BAOSTOCK_MAX_SECONDS", "6.0"))
_BAOSTOCK_SOCKET_TIMEOUT = float(os.getenv("BAOSTOCK_SOCKET_TIMEOUT", "3.0"))
_BAOSTOCK_CIRCUIT_THRESHOLD = int(os.getenv("BAOSTOCK_CIRCUIT_THRESHOLD", "10"))
_AKSHARE_RETRY_TIMES = max(int(os.getenv("AKSHARE_RETRY_TIMES", "2")), 1)
_AKSHARE_RETRY_SLEEP_SECONDS = float(os.getenv("AKSHARE_RETRY_SLEEP_SECONDS", "0.8"))
_BAOSTOCK_CONSEC_FAILS = 0
_BAOSTOCK_CIRCUIT_OPEN = False
_BAOSTOCK_CIRCUIT_NOTE = ""


def _l1_cache_key(symbol: str, adjust: str, start: date, end: date) -> str:
    return f"{symbol}|{adjust}|{start.isoformat()}|{end.isoformat()}"


def _l1_get(symbol: str, adjust: str, start: date, end: date) -> pd.DataFrame | None:
    key = _l1_cache_key(symbol, adjust, start, end)
    with _L1_CACHE_LOCK:
        df = _L1_CACHE.get(key)
        if df is not None:
            return df.copy()
    return None


def _l1_set(symbol: str, adjust: str, start: date, end: date, df: pd.DataFrame) -> None:
    if df is None or df.empty:
        return
    key = _l1_cache_key(symbol, adjust, start, end)
    with _L1_CACHE_LOCK:
        _L1_CACHE[key] = df.copy()


def _debug_source_fail(source: str, err: Exception) -> None:
    if _DATA_SOURCE_DEBUG:
        print(f"[data_source] {source} failed: {type(err).__name__}: {err}")


def _baostock_circuit_state() -> tuple[bool, str]:
    with _BAOSTOCK_LOCK:
        return (_BAOSTOCK_CIRCUIT_OPEN, _BAOSTOCK_CIRCUIT_NOTE)


def _baostock_mark_success() -> None:
    global _BAOSTOCK_CONSEC_FAILS
    with _BAOSTOCK_LOCK:
        _BAOSTOCK_CONSEC_FAILS = 0


def _baostock_mark_failure(reason: str) -> None:
    global _BAOSTOCK_CONSEC_FAILS, _BAOSTOCK_CIRCUIT_OPEN, _BAOSTOCK_CIRCUIT_NOTE
    with _BAOSTOCK_LOCK:
        _BAOSTOCK_CONSEC_FAILS += 1
        if (
            not _BAOSTOCK_CIRCUIT_OPEN
            and _BAOSTOCK_CIRCUIT_THRESHOLD > 0
            and _BAOSTOCK_CONSEC_FAILS >= _BAOSTOCK_CIRCUIT_THRESHOLD
        ):
            _BAOSTOCK_CIRCUIT_OPEN = True
            _BAOSTOCK_CIRCUIT_NOTE = (
                f"consecutive_failures={_BAOSTOCK_CONSEC_FAILS}, reason={reason}"
            )
            if _DATA_SOURCE_DEBUG:
                print(
                    "[data_source] baostock circuit opened: "
                    f"{_BAOSTOCK_CIRCUIT_NOTE}"
                )


def _compact_error(err: Exception, max_len: int = 120) -> str:
    msg = str(err or "").strip().replace("\n", " ")
    msg = re.sub(r"\s+", " ", msg)
    if len(msg) > max_len:
        msg = msg[: max_len - 3] + "..."
    if msg:
        return f"{type(err).__name__}: {msg}"
    return type(err).__name__


def _network_hint_from_details(details: list[str]) -> str:
    blob = " ".join(details).lower()
    dns_markers = [
        "nameresolutionerror",
        "nodename nor servname provided",
        "temporary failure in name resolution",
        "getaddrinfo failed",
        "failed to resolve",
    ]
    ssl_markers = [
        "ssl",
        "certificate",
        "cert verify failed",
    ]
    if any(k in blob for k in dns_markers):
        return "疑似 DNS/网络异常，请检查代理、DNS、系统防火墙或公司网络策略。"
    if any(k in blob for k in ssl_markers):
        return "疑似 SSL/证书链异常，请检查系统证书与 Python requests/certifi 环境。"
    if "remotedisconnected" in blob or "remote end closed connection" in blob:
        return "疑似上游行情源瞬时断连，可稍后重试；服务端已支持自动重试。"
    if "permission denied" in blob and "efinance" in blob:
        return "部署环境对 site-packages 为只读，efinance 本地缓存写入失败；建议依赖 tushare/akshare/baostock 或启用兼容修复。"
    return ""


def _is_retryable_akshare_error(err: Exception) -> bool:
    text = _compact_error(err).lower()
    markers = [
        "remotedisconnected",
        "remote end closed connection",
        "connection aborted",
        "connection reset",
        "read timed out",
        "connecttimeout",
        "proxyerror",
    ]
    return any(m in text for m in markers) or isinstance(err, RemoteDisconnected)


def _to_ts_code(symbol: str) -> str:
    """6 位代码转 tushare 格式：000001 -> 000001.SZ，600519 -> 600519.SH"""
    s = str(symbol).strip()
    if "." in s:
        return s
    if s.startswith(("600", "601", "603", "605", "688")):
        return f"{s}.SH"
    return f"{s}.SZ"


def _index_to_ts_code(code: str) -> str:
    """指数代码转 tushare 格式：000001->000001.SH, 399001->399001.SZ, 399006->399006.SZ"""
    s = str(code).strip()
    if "." in s:
        return s
    if s.startswith(("000", "880", "899")):
        return f"{s}.SH"
    return f"{s}.SZ"


def _tag_source(df: pd.DataFrame, source: str) -> pd.DataFrame:
    """在 DataFrame 上附加真实数据源标识，供上层缓存/展示使用。"""
    df.attrs["source"] = source
    return df


def _to_float_or_none(v: Any) -> float | None:
    if v is None or pd.isna(v):
        return None
    try:
        return float(v)
    except Exception:
        try:
            s = str(v).strip().replace(",", "")
            if s.endswith("%"):
                s = s[:-1]
            return float(s)
        except Exception:
            return None


def _pick_first(row: pd.Series, candidates: tuple[str, ...]) -> Any:
    for key in candidates:
        if key in row.index:
            v = row.get(key)
            if v is not None and not pd.isna(v):
                return v
    return None


def _normalize_spot_symbol(v: Any) -> str:
    s = str(v or "").strip()
    if "." in s:
        s = s.split(".", 1)[0]
    m = re.search(r"(\d{6})", s)
    if m:
        return m.group(1)
    if s.isdigit():
        return s.zfill(6)
    return ""


def _normalize_spot_turnover(
    close_v: float | None,
    volume_v: float | None,
    amount_v: float | None,
) -> tuple[float | None, float | None, bool]:
    """
    统一实时快照的量能单位到“股/元”。
    不同数据源可能返回“股/手”与“元/千元/万元”混合口径。
    用“隐含成交均价≈最新价”做最优匹配；若误差过大，返回不可用。
    """
    if close_v is None or volume_v is None or amount_v is None:
        return (None, None, False)
    close = float(close_v)
    vol_raw = float(volume_v)
    amt_raw = float(amount_v)
    if close <= 0 or vol_raw <= 0 or amt_raw <= 0:
        return (None, None, False)

    # volume: 原始可能是 股 或 手；amount: 原始可能是 元 / 千元 / 万元
    vol_factors = (1.0, 100.0)
    amt_factors = (1.0, 1000.0, 10000.0)
    best: tuple[float, float, float] | None = None  # (rel_err, vol_shares, amt_yuan)
    for vf in vol_factors:
        vol_shares = vol_raw * vf
        if vol_shares <= 0:
            continue
        for af in amt_factors:
            amt_yuan = amt_raw * af
            if amt_yuan <= 0:
                continue
            implied_price = amt_yuan / vol_shares
            rel_err = abs(implied_price - close) / max(close, 1e-9)
            if best is None or rel_err < best[0]:
                best = (rel_err, vol_shares, amt_yuan)
    if best is None:
        return (None, None, False)

    rel_err, vol_shares, amt_yuan = best
    if rel_err <= max(_SPOT_TURNOVER_MAX_REL_ERR, 0.0):
        return (float(vol_shares), float(amt_yuan), True)
    return (None, None, False)


def _load_spot_snapshot_map(force_refresh: bool = False) -> dict[str, dict[str, float | None]]:
    global _SPOT_SNAPSHOT_TS, _SPOT_SNAPSHOT_MAP
    now_ts = time.time()
    with _SPOT_SNAPSHOT_LOCK:
        if (
            not force_refresh
            and _SPOT_SNAPSHOT_MAP
            and (now_ts - _SPOT_SNAPSHOT_TS) < max(_SPOT_SNAPSHOT_TTL_SECONDS, 1)
        ):
            return _SPOT_SNAPSHOT_MAP

        try:
            import akshare as ak

            with ThreadPoolExecutor(max_workers=1) as ex:
                fut = ex.submit(ak.stock_zh_a_spot_em)
                df = fut.result(timeout=max(_SPOT_SNAPSHOT_TIMEOUT_SECONDS, 1.0))
            if df is None or df.empty:
                raise RuntimeError("spot snapshot empty")

            code_col = "代码"
            if code_col not in df.columns:
                fallback_cols = [c for c in df.columns if "代码" in str(c)]
                if fallback_cols:
                    code_col = str(fallback_cols[0])
                else:
                    raise RuntimeError("spot snapshot code column missing")

            spot_map: dict[str, dict[str, float | None]] = {}
            for _, row in df.iterrows():
                symbol = _normalize_spot_symbol(row.get(code_col))
                if not symbol:
                    continue
                close_v = _to_float_or_none(
                    _pick_first(row, ("最新价", "最新", "现价", "收盘"))
                )
                if close_v is None or close_v <= 0:
                    continue
                open_v = _to_float_or_none(_pick_first(row, ("今开", "开盘")))
                high_v = _to_float_or_none(_pick_first(row, ("最高",)))
                low_v = _to_float_or_none(_pick_first(row, ("最低",)))
                volume_raw = _to_float_or_none(_pick_first(row, ("成交量", "总手", "总量")))
                amount_raw = _to_float_or_none(_pick_first(row, ("成交额", "金额")))
                volume_v, amount_v, turnover_unit_ok = _normalize_spot_turnover(
                    close_v=close_v,
                    volume_v=volume_raw,
                    amount_v=amount_raw,
                )
                pct_v = _to_float_or_none(_pick_first(row, ("涨跌幅", "涨跌幅%")))

                spot_map[symbol] = {
                    "open": open_v,
                    "high": high_v,
                    "low": low_v,
                    "close": close_v,
                    "volume": volume_v,
                    "amount": amount_v,
                    "pct_chg": pct_v,
                    "turnover_unit_ok": 1.0 if turnover_unit_ok else 0.0,
                }
            if not spot_map:
                raise RuntimeError("spot snapshot parsed empty")

            _SPOT_SNAPSHOT_MAP = spot_map
            _SPOT_SNAPSHOT_TS = now_ts
            return _SPOT_SNAPSHOT_MAP
        except FuturesTimeoutError:
            _debug_source_fail(
                "spot_snapshot",
                TimeoutError(
                    f"timeout>{_SPOT_SNAPSHOT_TIMEOUT_SECONDS:.1f}s"
                ),
            )
            return _SPOT_SNAPSHOT_MAP
        except Exception as e:
            _debug_source_fail("spot_snapshot", e)
            return _SPOT_SNAPSHOT_MAP


def fetch_stock_spot_snapshot(
    symbol: str,
    *,
    force_refresh: bool = False,
) -> dict[str, float | None] | None:
    """
    获取单只股票最新快照（open/high/low/close/volume/amount/pct_chg）。
    用于日线延迟时的“单点补偿”。
    """
    s = _normalize_spot_symbol(symbol)
    if not s:
        return None
    spot_map = _load_spot_snapshot_map(force_refresh=force_refresh)
    return spot_map.get(s)


# --- 个股 ---


def _fetch_stock_akshare(
    symbol: str, start: str, end: str, adjust: str
) -> pd.DataFrame:
    import akshare as ak

    df = ak.stock_zh_a_hist(
        symbol=symbol,
        period="daily",
        start_date=start,
        end_date=end,
        adjust=adjust if adjust else "",
    )
    if df is None or df.empty:
        raise RuntimeError("akshare empty")
    if "日期" in df.columns:
        df = df.copy()
        df["日期"] = pd.to_datetime(df["日期"], errors="coerce").dt.strftime("%Y-%m-%d")
    return df


def _fetch_stock_baostock(symbol: str, start: str, end: str) -> pd.DataFrame:
    if symbol.startswith(("600", "601", "603", "605", "688")):
        bs_code = f"sh.{symbol}"
    else:
        bs_code = f"sz.{symbol}"
    start_dash = f"{start[:4]}-{start[4:6]}-{start[6:]}"
    end_dash = f"{end[:4]}-{end[4:6]}-{end[6:]}"
    with _BAOSTOCK_LOCK:
        old_sock_timeout = socket.getdefaulttimeout()
        if _BAOSTOCK_SOCKET_TIMEOUT > 0:
            socket.setdefaulttimeout(_BAOSTOCK_SOCKET_TIMEOUT)
        bs = _ensure_baostock_login()
        try:
            started = time.monotonic()
            rs = bs.query_history_k_data_plus(
                bs_code,
                "date,open,high,low,close,volume,amount,pctChg",
                start_date=start_dash,
                end_date=end_dash,
                frequency="d",
                adjustflag="2",  # 前复权
            )
            if rs.error_code != "0":
                raise RuntimeError(f"baostock: {rs.error_msg}")
            rows: list[list[str]] = []
            while rs.next():
                if (
                    _BAOSTOCK_MAX_SECONDS > 0
                    and (time.monotonic() - started) > _BAOSTOCK_MAX_SECONDS
                ):
                    raise TimeoutError(
                        f"baostock hard timeout > {_BAOSTOCK_MAX_SECONDS:.2f}s"
                    )
                rows.append(rs.get_row_data())
        finally:
            socket.setdefaulttimeout(old_sock_timeout)
    if not rows:
        raise RuntimeError("baostock empty")
    df = pd.DataFrame(rows, columns=rs.fields)
    df = df.rename(
        columns={
            "date": "日期",
            "open": "开盘",
            "high": "最高",
            "low": "最低",
            "close": "收盘",
            "volume": "成交量",
            "amount": "成交额",
            "pctChg": "涨跌幅",
        }
    )
    df["日期"] = pd.to_datetime(df["日期"], errors="coerce").dt.strftime("%Y-%m-%d")
    for c in ["开盘", "最高", "最低", "收盘", "成交量", "成交额", "涨跌幅"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df["换手率"] = pd.NA
    df["振幅"] = pd.NA
    return df


def _baostock_logout_on_exit() -> None:
    global _BAOSTOCK_LOGGED
    with _BAOSTOCK_LOCK:
        bs = _BAOSTOCK_MODULE
        if not _BAOSTOCK_LOGGED or bs is None:
            return
        try:
            bs.logout()
        except Exception:
            pass
        _BAOSTOCK_LOGGED = False


def _ensure_baostock_login():
    """
    进程内复用 baostock 会话，避免每只股票 login/logout 导致大量开销与阻塞日志。
    运行特性说明：该会话在当前 Python 进程生命周期内复用，并由 atexit 在进程退出时回收。
    若未来改为长生命周期守护进程/热重载模式，需要关注其“跨任务复用”行为是否符合预期。
    """
    global _BAOSTOCK_LOGGED, _BAOSTOCK_EXIT_HOOKED, _BAOSTOCK_MODULE
    with _BAOSTOCK_LOCK:
        import baostock as bs

        _BAOSTOCK_MODULE = bs
        if _BAOSTOCK_LOGGED:
            return bs

        lg = bs.login()
        if lg.error_code != "0":
            raise RuntimeError(f"baostock login: {lg.error_msg}")
        _BAOSTOCK_LOGGED = True

        if not _BAOSTOCK_EXIT_HOOKED:
            atexit.register(_baostock_logout_on_exit)
            _BAOSTOCK_EXIT_HOOKED = True
        return bs


def _fetch_stock_efinance(symbol: str, start: str, end: str) -> pd.DataFrame:
    # Streamlit Cloud / 只读部署环境下，efinance 在 import 阶段会尝试写 site-packages/efinance/data。
    # 这里做一次兼容导入：临时忽略该 mkdir 的 PermissionError，随后把缓存目录重定向到 /tmp。
    import pathlib
    import tempfile

    orig_mkdir = pathlib.Path.mkdir

    def _patched_mkdir(self, *args, **kwargs):
        try:
            return orig_mkdir(self, *args, **kwargs)
        except PermissionError:
            path_text = str(self)
            if "site-packages" in path_text and "efinance" in path_text and "data" in path_text:
                return None
            raise

    pathlib.Path.mkdir = _patched_mkdir
    try:
        import efinance as ef
        import efinance.config as ef_cfg
        # 预触发内部检查，某些版本在此处会尝试读取 data 目录
        from efinance.common.sh_stock_check import is_sh_stock
    except (PermissionError, FileNotFoundError) as e:
        _debug_source_fail("efinance_patch", e)
    finally:
        pathlib.Path.mkdir = orig_mkdir

    cache_dir = Path(tempfile.gettempdir()) / "efinance-cache"
    try:
        cache_dir.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    ef_cfg.DATA_DIR = cache_dir
    ef_cfg.SEARCH_RESULT_CACHE_PATH = str(cache_dir / "search-cache.json")
    
    # 额外抑制 efinance 内部对 site-packages 下 data 目录的硬编码访问尝试导致的 FileNotFoundError
    # 这种错误通常发生在 Python 3.13 + Streamlit Cloud 环境下

    # fqt: 0 不复权, 1 前复权, 2 后复权
    fqt = 1  # 默认前复权
    result = ef.stock.get_quote_history(symbol, beg=start, end=end, klt=101, fqt=fqt)
    if isinstance(result, dict):
        df = result.get(str(symbol))
    else:
        df = result
    if df is None or (hasattr(df, "empty") and df.empty):
        raise RuntimeError("efinance empty")

    # efinance 不同版本列名可能带单位后缀，如：涨跌幅(%)、成交额(元)
    df = df.copy()

    def _rename_prefix(std: str) -> None:
        if std in df.columns:
            return
        for c in df.columns:
            if str(c).startswith(std):
                df.rename(columns={c: std}, inplace=True)
                return

    # 日期列兼容
    if "日期" not in df.columns:
        for c in df.columns:
            if str(c).endswith("日期") or "日期" in str(c):
                df.rename(columns={c: "日期"}, inplace=True)
                break

    for std in [
        "开盘",
        "最高",
        "最低",
        "收盘",
        "成交量",
        "成交额",
        "涨跌幅",
        "换手率",
        "振幅",
    ]:
        _rename_prefix(std)
    # efinance: 日期, 开盘, 收盘, 最高, 最低, 成交量, 成交额, 振幅, 涨跌幅, 换手率
    out_cols = [
        "日期",
        "开盘",
        "最高",
        "最低",
        "收盘",
        "成交量",
        "成交额",
        "涨跌幅",
        "换手率",
        "振幅",
    ]
    for c in ["日期", "开盘", "最高", "最低", "收盘", "成交量", "成交额", "涨跌幅"]:
        if c not in df.columns:
            raise RuntimeError(f"efinance missing column {c}")
    for c in ["换手率", "振幅"]:
        if c not in df.columns:
            df = df.assign(**{c: pd.NA})
    df["日期"] = pd.to_datetime(df["日期"]).dt.strftime("%Y-%m-%d")
    return df[out_cols].copy()


def _fetch_stock_tushare(
    symbol: str, start: str, end: str, adjust: str
) -> pd.DataFrame:
    import tushare as ts
    from utils.tushare_client import get_pro

    pro = get_pro()
    if pro is None:
        raise RuntimeError("TUSHARE_TOKEN 未配置")
    ts_code = _to_ts_code(symbol)
    # 口径固定：优先使用前复权（qfq）。
    adj_val = "qfq"
    # ts.pro_bar 实际上不支持名为 `proapi` 的底层透传参数，会抛出 TypeError
    # 它会自动使用我们前面 pro ＝ get_pro() 间接配置好的全局 token。
    df = ts.pro_bar(ts_code=ts_code, adj=adj_val, start_date=start, end_date=end)
    
    if df is None or df.empty:
        # 诊断：尝试拉取不复权数据，看是否是权限问题（qfq 需要更高积分）
        try:
            df_no_adj = pro.daily(ts_code=ts_code, start_date=start, end_date=end)
            if df_no_adj is not None and not df_no_adj.empty:
                raise RuntimeError("tushare empty (qfq auth limit?)")
        except Exception:
            pass
        raise RuntimeError("tushare empty")
    
    df = df.rename(
        columns={
            "trade_date": "日期",
            "open": "开盘",
            "high": "最高",
            "low": "最低",
            "close": "收盘",
            "vol": "成交量",
            "amount": "成交额",
            "pct_chg": "涨跌幅",
        }
    )
    df["成交量"] = pd.to_numeric(df["成交量"], errors="coerce") * 100  # 手 -> 股
    df["成交额"] = pd.to_numeric(df["成交额"], errors="coerce") * 1000  # 千元 -> 元
    df["换手率"] = pd.NA
    df["振幅"] = pd.NA
    df["日期"] = (
        df["日期"].astype(str).str[:4]
        + "-"
        + df["日期"].astype(str).str[4:6]
        + "-"
        + df["日期"].astype(str).str[6:8]
    )
    return df[
        [
            "日期",
            "开盘",
            "最高",
            "最低",
            "收盘",
            "成交量",
            "成交额",
            "涨跌幅",
            "换手率",
            "振幅",
        ]
    ].copy()


def fetch_stock_hist(
    symbol: str,
    start: str | date,
    end: str | date,
    adjust: Literal["", "qfq", "hfq"] = "qfq",
    use_cache: bool = True,
) -> pd.DataFrame:
    """
    个股日线：支持三级缓存 + 增量更新

    缓存策略:
    - L1 内存缓存: 同一次运行中重复请求直接返回
    - L2 数据库缓存: Supabase 持久化存储
    - L3 API 拉取: 缓存未命中时从数据源拉取

    增量更新:
    - 如果缓存部分覆盖，只拉取缺失日期的数据
    - 合并缓存数据和新拉取的数据

    参数:
        symbol: 股票代码
        start: 开始日期
        end: 结束日期
        adjust: 复权类型 (qfq/hfq/"")
        use_cache: 是否使用缓存 (默认 True)

    返回列：日期, 开盘, 最高, 最低, 收盘, 成交量, 成交额, 涨跌幅, 换手率, 振幅
    """
    start_date = start if isinstance(start, date) else date.fromisoformat(str(start).replace("-", "")[:4] + "-" + str(start).replace("-", "")[4:6] + "-" + str(start).replace("-", "")[6:8]) if len(str(start).replace("-", "")) == 8 else date.today()
    end_date = end if isinstance(end, date) else date.fromisoformat(str(end).replace("-", "")[:4] + "-" + str(end).replace("-", "")[4:6] + "-" + str(end).replace("-", "")[6:8]) if len(str(end).replace("-", "")) == 8 else date.today()

    adjust_key = adjust or "none"

    if use_cache:
        l1_cached = _l1_get(symbol, adjust_key, start_date, end_date)
        if l1_cached is not None:
            return l1_cached

    start_s = (
        start.strftime("%Y%m%d")
        if isinstance(start, date)
        else str(start).replace("-", "")
    )
    end_s = (
        end.strftime("%Y%m%d") if isinstance(end, date) else str(end).replace("-", "")
    )

    cache_meta = None
    cached_df = None
    fetch_start_date = start_date
    fetch_end_date = end_date
    actual_source = ""

    if use_cache:
        from core.stock_cache import get_cache_meta, load_cached_history
        cache_meta = get_cache_meta(symbol, adjust_key)

        if cache_meta is not None:
            if cache_meta.end_date >= end_date and cache_meta.start_date <= start_date:
                cached_df = load_cached_history(
                    symbol, adjust_key, cache_meta.source, start_date, end_date
                )
                if cached_df is not None and not cached_df.empty:
                    from core.stock_cache import denormalize_hist_df
                    result = denormalize_hist_df(cached_df)
                    result = _ensure_output_columns(result)
                    _l1_set(symbol, adjust_key, start_date, end_date, result)
                    return _tag_source(result, f"{cache_meta.source}(cached)")
            elif cache_meta.end_date >= start_date and cache_meta.end_date < end_date:
                fetch_start_date = cache_meta.end_date + timedelta(days=1)
                cached_df = load_cached_history(
                    symbol, adjust_key, cache_meta.source, start_date, cache_meta.end_date
                )
                actual_source = cache_meta.source
                fetch_start_s = fetch_start_date.strftime("%Y%m%d")
                fetch_end_s = fetch_end_date.strftime("%Y%m%d")
            else:
                cache_meta = None

    failed_sources: list[str] = []
    failed_details: list[str] = []
    from utils.tushare_client import get_pro

    pro = get_pro()
    df = None
    source = ""

    fetch_start_s = fetch_start_date.strftime("%Y%m%d")
    fetch_end_s = fetch_end_date.strftime("%Y%m%d")

    if pro is not None:
        try:
            df = _fetch_stock_tushare(symbol, fetch_start_s, fetch_end_s, "qfq")
            source = "tushare"
        except Exception as e:
            _debug_source_fail("tushare", e)
            failed_sources.append("tushare")
            failed_details.append(f"tushare={_compact_error(e)}")
    else:
        failed_sources.append("tushare(unconfigured)")
        failed_details.append("tushare=token_missing")

    disable_akshare = os.getenv("DATA_SOURCE_DISABLE_AKSHARE", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    disable_baostock = os.getenv("DATA_SOURCE_DISABLE_BAOSTOCK", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    disable_efinance = os.getenv("DATA_SOURCE_DISABLE_EFINANCE", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }

    if df is None and not disable_akshare:
        last_akshare_err: Exception | None = None
        for attempt in range(1, _AKSHARE_RETRY_TIMES + 1):
            try:
                df = _fetch_stock_akshare(symbol, fetch_start_s, fetch_end_s, adjust)
                source = "akshare"
                break
            except ModuleNotFoundError as e:
                _debug_source_fail("akshare", e)
                failed_sources.append(f"akshare(缺少依赖 {e.name})")
                failed_details.append(f"akshare={_compact_error(e)}")
                last_akshare_err = e
                break
            except Exception as e:
                last_akshare_err = e
                _debug_source_fail("akshare", e)
                if attempt < _AKSHARE_RETRY_TIMES and _is_retryable_akshare_error(e):
                    time.sleep(max(_AKSHARE_RETRY_SLEEP_SECONDS, 0.0))
                    continue
                failed_sources.append("akshare")
                failed_details.append(f"akshare={_compact_error(e)}")
                break

    baostock_circuit_open, baostock_circuit_note = _baostock_circuit_state()
    if df is None and not disable_baostock and not baostock_circuit_open:
        started = time.monotonic()
        try:
            df = _fetch_stock_baostock(symbol, fetch_start_s, fetch_end_s)
            elapsed = time.monotonic() - started
            if _BAOSTOCK_MAX_SECONDS > 0 and elapsed > _BAOSTOCK_MAX_SECONDS:
                raise TimeoutError(
                    f"baostock slow={elapsed:.2f}s > {_BAOSTOCK_MAX_SECONDS:.2f}s"
                )
            _baostock_mark_success()
            source = "baostock"
        except ModuleNotFoundError as e:
            _debug_source_fail("baostock", e)
            _baostock_mark_failure(_compact_error(e))
            failed_sources.append(f"baostock(未安装: {e.name})")
            failed_details.append(f"baostock={_compact_error(e)}")
        except Exception as e:
            _debug_source_fail("baostock", e)
            _baostock_mark_failure(_compact_error(e))
            failed_sources.append("baostock")
            failed_details.append(f"baostock={_compact_error(e)}")

    if df is None and not disable_efinance:
        try:
            df = _fetch_stock_efinance(symbol, fetch_start_s, fetch_end_s)
            source = "efinance"
        except ModuleNotFoundError as e:
            _debug_source_fail("efinance", e)
            failed_sources.append(f"efinance(未安装: {e.name})")
            failed_details.append(f"efinance={_compact_error(e)}")
        except Exception as e:
            _debug_source_fail("efinance", e)
            failed_sources.append("efinance")
            failed_details.append(f"efinance={_compact_error(e)}")

    if df is None:
        detail_suffix = (
            f" 失败详情：{'；'.join(failed_details[:4])}。"
            if failed_details
            else ""
        )
        hint = _network_hint_from_details(failed_details)
        hint_suffix = f" 诊断提示：{hint}" if hint else ""
        raise RuntimeError(
            f"所有数据源均拉取失败：{symbol} {fetch_start_s}-{fetch_end_s}。"
            f"已尝试：{', '.join(failed_sources)}。{detail_suffix}{hint_suffix}"
        )

    if cached_df is not None and not cached_df.empty:
        from core.stock_cache import normalize_hist_df, denormalize_hist_df
        new_normalized = normalize_hist_df(df)
        combined = pd.concat([cached_df, new_normalized], ignore_index=True)
        combined = combined.drop_duplicates(subset=["date"], keep="last")
        combined = combined.sort_values("date").reset_index(drop=True)
        df = denormalize_hist_df(combined)
        source = actual_source or source

    df = _ensure_output_columns(df)

    if use_cache and df is not None and not df.empty:
        from core.stock_cache import upsert_cache_data, upsert_cache_meta
        upsert_cache_data(symbol, adjust_key, source, df)
        upsert_cache_meta(symbol, adjust_key, source, start_date, end_date)

    _l1_set(symbol, adjust_key, start_date, end_date, df)

    return _tag_source(df, source)


def _ensure_output_columns(df: pd.DataFrame) -> pd.DataFrame:
    """确保输出包含所有必需列"""
    required = ["日期", "开盘", "最高", "最低", "收盘", "成交量", "成交额", "涨跌幅", "换手率", "振幅"]
    for col in required:
        if col not in df.columns:
            df[col] = pd.NA
    return df[required].copy()


# --- 大盘指数 ---


def _fetch_index_tushare(code: str, start: str, end: str) -> pd.DataFrame:
    from utils.tushare_client import get_pro

    pro = get_pro()
    if pro is None:
        raise RuntimeError(
            "拉取失败（非程序错误）：大盘指数需 Tushare Token，免费数据源（akshare 等）不支持大盘指数。请配置 TUSHARE_TOKEN。"
        )
    ts_code = _index_to_ts_code(code)
    df = pro.index_daily(ts_code=ts_code, start_date=start, end_date=end)
    if df is None or df.empty:
        raise RuntimeError("拉取失败（非程序错误）：tushare 大盘指数返回空数据")
    df = df.copy()
    df["date"] = (
        df["trade_date"].astype(str).str[:4]
        + "-"
        + df["trade_date"].astype(str).str[4:6]
        + "-"
        + df["trade_date"].astype(str).str[6:8]
    )
    df["volume"] = pd.to_numeric(df["vol"], errors="coerce")
    return df[["date", "open", "high", "low", "close", "volume", "pct_chg"]].copy()


def fetch_index_hist(code: str, start: str | date, end: str | date) -> pd.DataFrame:
    """
    大盘指数日线：直接使用 tushare（免费源大盘 100% 失败，故不试）。
    返回列：date, open, high, low, close, volume, pct_chg（小写，供 step2 使用）
    """
    start_s = (
        start.strftime("%Y%m%d")
        if isinstance(start, date)
        else str(start).replace("-", "")
    )
    end_s = (
        end.strftime("%Y%m%d") if isinstance(end, date) else str(end).replace("-", "")
    )
    return _fetch_index_tushare(code, start_s, end_s)


# --- 行业 & 市值批量获取（tushare） ---

_DATA_CACHE_DIR = Path(__file__).resolve().parent.parent / "data"
_SECTOR_CACHE = _DATA_CACHE_DIR / "sector_map_cache.json"
_MARKET_CAP_CACHE = _DATA_CACHE_DIR / "market_cap_cache.json"
_CACHE_TTL = 24 * 60 * 60


def _atomic_write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_name: str | None = None
    try:
        with NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=str(path.parent),
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as tmp:
            json.dump(payload, tmp, ensure_ascii=False)
            tmp.flush()
            os.fsync(tmp.fileno())
            tmp_name = tmp.name
        os.replace(tmp_name, path)
        tmp_name = None
    finally:
        if tmp_name and os.path.exists(tmp_name):
            try:
                os.remove(tmp_name)
            except Exception:
                pass


def _ts_code_to_symbol(ts_code: str) -> str:
    """000001.SZ -> 000001"""
    return ts_code.split(".")[0] if "." in ts_code else ts_code


def fetch_sector_map() -> dict[str, str]:
    """
    全市场 code->行业映射。优先用缓存，过期后通过 tushare stock_basic 刷新。
    """
    try:
        if (
            _SECTOR_CACHE.exists()
            and (time.time() - _SECTOR_CACHE.stat().st_mtime) < _CACHE_TTL
        ):
            with open(_SECTOR_CACHE, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        _debug_source_fail("sector_cache_read", e)

    from utils.tushare_client import get_pro

    pro = get_pro()
    if pro is None:
        try:
            if _SECTOR_CACHE.exists():
                with open(_SECTOR_CACHE, "r", encoding="utf-8") as f:
                    return json.load(f)
        except Exception as e:
            _debug_source_fail("sector_cache_fallback_read", e)
        return {}

    df = pro.stock_basic(fields="ts_code,industry")
    if df is None or df.empty:
        return {}

    mapping = {}
    for _, row in df.iterrows():
        sym = _ts_code_to_symbol(str(row["ts_code"]))
        industry = str(row.get("industry", "")).strip()
        if sym and industry:
            mapping[sym] = industry

    try:
        _atomic_write_json(_SECTOR_CACHE, mapping)
    except Exception as e:
        _debug_source_fail("sector_cache_write", e)

    return mapping


def fetch_market_cap_map() -> dict[str, float]:
    """
    全市场 code->总市值(亿元)。通过 tushare daily_basic 获取最新交易日数据。
    """
    try:
        if (
            _MARKET_CAP_CACHE.exists()
            and (time.time() - _MARKET_CAP_CACHE.stat().st_mtime) < _CACHE_TTL
        ):
            with open(_MARKET_CAP_CACHE, "r", encoding="utf-8") as f:
                return {k: float(v) for k, v in json.load(f).items()}
    except Exception as e:
        _debug_source_fail("market_cap_cache_read", e)

    from utils.tushare_client import get_pro

    pro = get_pro()
    if pro is None:
        try:
            if _MARKET_CAP_CACHE.exists():
                with open(_MARKET_CAP_CACHE, "r", encoding="utf-8") as f:
                    return {k: float(v) for k, v in json.load(f).items()}
        except Exception as e:
            _debug_source_fail("market_cap_cache_fallback_read", e)
        return {}

    from datetime import date as _date, timedelta as _td

    # 尝试最近几个交易日
    mapping: dict[str, float] = {}
    for offset in range(5):
        d = _date.today() - _td(days=1 + offset)
        trade_date = d.strftime("%Y%m%d")
        try:
            df = pro.daily_basic(trade_date=trade_date, fields="ts_code,total_mv")
            if df is not None and not df.empty:
                for _, row in df.iterrows():
                    sym = _ts_code_to_symbol(str(row["ts_code"]))
                    total_mv = row.get("total_mv")
                    if sym and pd.notna(total_mv):
                        mapping[sym] = float(total_mv) / 10000.0  # 万元 -> 亿元
                break
        except Exception as e:
            _debug_source_fail(f"tushare_daily_basic[{trade_date}]", e)
            continue

    if mapping:
        try:
            _atomic_write_json(_MARKET_CAP_CACHE, mapping)
        except Exception as e:
            _debug_source_fail("market_cap_cache_write", e)

    return mapping


def fetch_all_stocks_by_trade_date(
    start_date: date | str,
    end_date: date | str,
    adjust: Literal["", "qfq", "hfq"] = "qfq",
) -> dict[str, pd.DataFrame]:
    """
    按交易日批量获取全市场股票数据（高效模式）
    
    使用 pro.daily(trade_date=...) 按交易日获取，每个交易日只需一次请求。
    相比逐只股票请求，大幅减少 API 调用次数。
    
    Args:
        start_date: 开始日期
        end_date: 结束日期
        adjust: 复权方式 (qfq=前复权, hfq=后复权, ""=不复权)
    
    Returns:
        dict[str, pd.DataFrame]: {symbol: DataFrame}
    """
    from utils.tushare_client import get_pro
    from utils.trade_calendar import get_trade_dates
    
    pro = get_pro()
    if pro is None:
        print("[fetch_all_stocks_by_trade_date] TUSHARE_TOKEN 未配置，无法使用批量模式")
        return {}
    
    start_s = start_date.strftime("%Y%m%d") if isinstance(start_date, date) else str(start_date).replace("-", "")
    end_s = end_date.strftime("%Y%m%d") if isinstance(end_date, date) else str(end_date).replace("-", "")
    
    trade_dates = get_trade_dates(start_s, end_s)
    if not trade_dates:
        print(f"[fetch_all_stocks_by_trade_date] 未找到交易日: {start_s} ~ {end_s}")
        return {}
    
    print(f"[fetch_all_stocks_by_trade_date] 获取 {len(trade_dates)} 个交易日数据...")
    
    all_data: dict[str, list[dict]] = {}
    
    for i, trade_date in enumerate(trade_dates):
        if _DATA_SOURCE_DEBUG:
            print(f"[fetch_all_stocks_by_trade_date] ({i+1}/{len(trade_dates)}) {trade_date}")
        
        try:
            df = pro.daily(trade_date=trade_date)
            if df is None or df.empty:
                continue
            
            for _, row in df.iterrows():
                ts_code = str(row["ts_code"])
                sym = _ts_code_to_symbol(ts_code)
                if not sym:
                    continue
                
                if sym not in all_data:
                    all_data[sym] = []
                
                all_data[sym].append({
                    "日期": str(row["trade_date"])[:4] + "-" + str(row["trade_date"])[4:6] + "-" + str(row["trade_date"])[6:8],
                    "开盘": row.get("open"),
                    "最高": row.get("high"),
                    "最低": row.get("low"),
                    "收盘": row.get("close"),
                    "成交量": pd.to_numeric(row.get("vol", 0), errors="coerce") * 100,
                    "成交额": pd.to_numeric(row.get("amount", 0), errors="coerce") * 1000,
                    "涨跌幅": row.get("pct_chg"),
                    "换手率": pd.NA,
                    "振幅": pd.NA,
                })
            
            time.sleep(0.12)
            
        except Exception as e:
            _debug_source_fail(f"pro.daily({trade_date})", e)
            continue
    
    if adjust == "qfq":
        print(f"[fetch_all_stocks_by_trade_date] 获取前复权因子...")
        adj_factor_map = _fetch_adj_factors_batch(list(all_data.keys()), start_s, end_s)
    else:
        adj_factor_map = {}
    
    result: dict[str, pd.DataFrame] = {}
    for sym, records in all_data.items():
        df = pd.DataFrame(records)
        df = df.sort_values("日期").reset_index(drop=True)
        
        if adjust == "qfq" and sym in adj_factor_map:
            adj_factors = adj_factor_map[sym]
            df = _apply_adj_factor(df, adj_factors)
        
        df = _ensure_output_columns(df)
        df.attrs["source"] = "tushare"
        result[sym] = df
    
    print(f"[fetch_all_stocks_by_trade_date] 完成，获取 {len(result)} 只股票")
    return result


def _fetch_adj_factors_batch(
    symbols: list[str],
    start_date: str,
    end_date: str,
) -> dict[str, pd.DataFrame]:
    """
    批量获取前复权因子
    """
    from utils.tushare_client import get_pro
    
    pro = get_pro()
    if pro is None:
        return {}
    
    ts_codes = [_to_ts_code(sym) for sym in symbols]
    
    all_factors: dict[str, list[dict]] = {}
    
    batch_size = 500
    for i in range(0, len(ts_codes), batch_size):
        batch = ts_codes[i:i + batch_size]
        try:
            df = pro.adj_factor(
                ts_code=",".join(batch),
                start_date=start_date,
                end_date=end_date,
            )
            if df is None or df.empty:
                continue
            
            for _, row in df.iterrows():
                ts_code = str(row["ts_code"])
                sym = _ts_code_to_symbol(ts_code)
                if not sym:
                    continue
                
                if sym not in all_factors:
                    all_factors[sym] = []
                
                all_factors[sym].append({
                    "trade_date": str(row["trade_date"]),
                    "adj_factor": row.get("adj_factor"),
                })
            
            time.sleep(0.12)
            
        except Exception as e:
            _debug_source_fail(f"pro.adj_factor batch", e)
            continue
    
    result: dict[str, pd.DataFrame] = {}
    for sym, records in all_factors.items():
        df = pd.DataFrame(records)
        df = df.sort_values("trade_date").reset_index(drop=True)
        result[sym] = df
    
    return result


def _apply_adj_factor(df: pd.DataFrame, adj_factors: pd.DataFrame) -> pd.DataFrame:
    """
    应用前复权因子
    """
    if adj_factors.empty:
        return df
    
    df = df.copy()
    df["日期_str"] = df["日期"].str.replace("-", "")
    
    factor_map = dict(zip(
        adj_factors["trade_date"].astype(str),
        adj_factors["adj_factor"].astype(float)
    ))
    
    df["adj_factor"] = df["日期_str"].map(factor_map)
    df["adj_factor"] = df["adj_factor"].fillna(method="ffill").fillna(1.0)
    
    for col in ["开盘", "最高", "最低", "收盘"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce") * df["adj_factor"]
    
    df = df.drop(columns=["日期_str", "adj_factor"], errors="ignore")
    return df
