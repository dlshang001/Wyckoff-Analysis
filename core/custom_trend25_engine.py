# -*- coding: utf-8 -*-
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import date
from typing import Any

import pandas as pd

from core.wyckoff_engine import normalize_hist_from_fetch
from integrations.data_source import fetch_market_cap_map, fetch_sector_map, fetch_stock_hist
from integrations.fetch_a_share_csv import get_all_stocks, _resolve_trading_window
from integrations.supabase_market_signal import load_latest_market_signal_daily
from utils.trading_clock import resolve_end_calendar_day
from utils.tushare_client import get_pro


@dataclass
class CustomTrend25Config:
    strategy_id: str = "custom_trend25"
    trading_days: int = 260
    only_main_board: bool = True
    exclude_chinext: bool = True
    exclude_star: bool = True
    exclude_bse: bool = True
    limit_count: int = 800
    max_workers: int = 8

    ma_short: int = 10
    ma_mid: int = 25
    no_new_high_window: int = 20
    min_return_window: int = 60
    min_return_pct: float = 15.0
    max_return_5d_pct: float = 20.0
    no_limitup_window: int = 3
    limitup_threshold_pct: float = 9.9

    burst_window: int = 10
    burst_threshold_pct: float = 6.0
    vol_peak_window: int = 10
    vol_avg_window: int = 60
    vol_peak_ratio: float = 1.5

    min_avg_amount_5d_yuan: float = 5e8
    min_market_cap_yi: float = 10.0

    enable_water_adapt: bool = True
    enable_sector_resonance: bool = True
    top_n_sectors: int = 5


def _to_bool(v: Any, default: bool) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "on"}


def _parse_config(payload: dict[str, Any] | None) -> CustomTrend25Config:
    p = dict(payload or {})
    cfg = CustomTrend25Config(
        trading_days=max(int(p.get("trading_days", 260) or 260), 120),
        only_main_board=_to_bool(p.get("only_main_board"), True),
        exclude_chinext=_to_bool(p.get("exclude_chinext"), True),
        exclude_star=_to_bool(p.get("exclude_star"), True),
        exclude_bse=_to_bool(p.get("exclude_bse"), True),
        limit_count=max(int(p.get("limit_count", 800) or 800), 0),
        max_workers=min(max(int(p.get("max_workers", 8) or 8), 1), 24),
        ma_short=max(int(p.get("ma_short", 10) or 10), 2),
        ma_mid=max(int(p.get("ma_mid", 25) or 25), 3),
        no_new_high_window=max(int(p.get("no_new_high_window", 20) or 20), 5),
        min_return_window=max(int(p.get("min_return_window", 60) or 60), 10),
        min_return_pct=float(p.get("min_return_pct", 15.0) or 15.0),
        max_return_5d_pct=float(p.get("max_return_5d_pct", 20.0) or 20.0),
        no_limitup_window=max(int(p.get("no_limitup_window", 3) or 3), 1),
        limitup_threshold_pct=float(p.get("limitup_threshold_pct", 9.9) or 9.9),
        burst_window=max(int(p.get("burst_window", 10) or 10), 2),
        burst_threshold_pct=float(p.get("burst_threshold_pct", 6.0) or 6.0),
        vol_peak_window=max(int(p.get("vol_peak_window", 10) or 10), 2),
        vol_avg_window=max(int(p.get("vol_avg_window", 60) or 60), 10),
        vol_peak_ratio=float(p.get("vol_peak_ratio", 1.5) or 1.5),
        min_avg_amount_5d_yuan=float(p.get("min_avg_amount_5d_yuan", 5e8) or 5e8),
        min_market_cap_yi=float(p.get("min_market_cap_yi", 10.0) or 10.0),
        enable_water_adapt=_to_bool(p.get("enable_water_adapt"), True),
        enable_sector_resonance=_to_bool(p.get("enable_sector_resonance"), True),
        top_n_sectors=max(int(p.get("top_n_sectors", 5) or 5), 1),
    )
    if cfg.ma_short >= cfg.ma_mid:
        cfg.ma_short = max(2, cfg.ma_mid - 1)
    return cfg


def _is_main(code: str) -> bool:
    return str(code).startswith(("600", "601", "603", "605", "000", "001", "002", "003"))


def _is_chinext(code: str) -> bool:
    return str(code).startswith(("300", "301"))


def _is_star(code: str) -> bool:
    return str(code).startswith("688")


def _is_bse(code: str) -> bool:
    return str(code).startswith(("43", "83", "87", "88", "92"))


def _build_universe(cfg: CustomTrend25Config) -> tuple[list[str], dict[str, str]]:
    items = get_all_stocks()
    symbols: list[str] = []
    name_map: dict[str, str] = {}
    for item in items:
        code = str(item.get("code", "")).strip()
        name = str(item.get("name", "")).strip()
        if len(code) != 6 or not code.isdigit():
            continue
        if "ST" in name.upper():
            continue
        if cfg.only_main_board and (not _is_main(code)):
            continue
        if cfg.exclude_chinext and _is_chinext(code):
            continue
        if cfg.exclude_star and _is_star(code):
            continue
        if cfg.exclude_bse and _is_bse(code):
            continue
        symbols.append(code)
        name_map[code] = name
    if cfg.limit_count > 0:
        symbols = symbols[: cfg.limit_count]
    return symbols, name_map


def _fetch_circ_mv_map() -> dict[str, float]:
    pro = get_pro()
    if pro is None:
        return fetch_market_cap_map()
    for back in range(1, 6):
        td = (date.today() - pd.Timedelta(days=back)).strftime("%Y%m%d")
        try:
            df = pro.daily_basic(trade_date=td, fields="ts_code,circ_mv,total_mv")
            if df is None or df.empty:
                continue
            out: dict[str, float] = {}
            for _, row in df.iterrows():
                ts_code = str(row.get("ts_code", ""))
                code = ts_code.split(".")[0] if "." in ts_code else ts_code
                circ_mv = row.get("circ_mv")
                total_mv = row.get("total_mv")
                mv = circ_mv if pd.notna(circ_mv) else total_mv
                if code and pd.notna(mv):
                    out[code] = float(mv) / 10000.0
            if out:
                return out
        except Exception:
            continue
    return fetch_market_cap_map()


def _fetch_one(symbol: str, window) -> tuple[str, pd.DataFrame | None]:
    try:
        raw = fetch_stock_hist(symbol=symbol, start=window.start_trade_date, end=window.end_trade_date, adjust="qfq")
        if raw is None or raw.empty:
            return symbol, None
        return symbol, normalize_hist_from_fetch(raw)
    except Exception:
        return symbol, None


def _return_pct(close: pd.Series, lookback: int) -> float | None:
    s = pd.to_numeric(close, errors="coerce").dropna()
    if len(s) <= lookback:
        return None
    start = float(s.iloc[-lookback - 1])
    end = float(s.iloc[-1])
    if start <= 0:
        return None
    return (end - start) / start * 100.0


def _adapt_by_regime(cfg: CustomTrend25Config, regime: dict[str, Any] | None) -> tuple[dict[str, Any], dict[str, Any]]:
    tuned = {
        "min_avg_amount_5d_yuan": cfg.min_avg_amount_5d_yuan,
        "vol_peak_ratio": cfg.vol_peak_ratio,
        "max_return_5d_pct": cfg.max_return_5d_pct,
        "capacity_mult": 1.0,
    }
    context = {
        "benchmark_regime": "UNKNOWN",
        "premarket_regime": "UNKNOWN",
    }
    row = dict(regime or {})
    bench = str(row.get("benchmark_regime", "") or "UNKNOWN").upper()
    pre = str(row.get("premarket_regime", "") or "UNKNOWN").upper()
    context.update({"benchmark_regime": bench, "premarket_regime": pre})
    if not cfg.enable_water_adapt:
        return tuned, context

    cold = bench in {"RISK_OFF", "CRASH", "BLACK_SWAN"} or pre in {"RISK_OFF", "BLACK_SWAN"}
    hot = bench == "RISK_ON" and pre in {"NORMAL", "CAUTION"}

    if cold:
        tuned["min_avg_amount_5d_yuan"] = cfg.min_avg_amount_5d_yuan * 1.3
        tuned["vol_peak_ratio"] = cfg.vol_peak_ratio + 0.2
        tuned["max_return_5d_pct"] = min(cfg.max_return_5d_pct, 15.0)
        tuned["capacity_mult"] = 0.7
    elif hot:
        tuned["min_avg_amount_5d_yuan"] = cfg.min_avg_amount_5d_yuan * 0.9
        tuned["vol_peak_ratio"] = max(cfg.vol_peak_ratio - 0.1, 1.2)
        tuned["capacity_mult"] = 1.2
    return tuned, context


def _eval_symbol(
    symbol: str,
    df: pd.DataFrame,
    cfg: CustomTrend25Config,
    tuned: dict[str, Any],
    market_cap_map: dict[str, float],
    name_map: dict[str, str],
    sector_map: dict[str, str],
) -> dict[str, Any] | None:
    need = max(cfg.vol_avg_window + 2, cfg.ma_mid + 2, cfg.min_return_window + 2, cfg.no_new_high_window + 2)
    if df is None or df.empty or len(df) < need:
        return None
    s = df.sort_values("date").reset_index(drop=True)
    close = pd.to_numeric(s["close"], errors="coerce")
    high = pd.to_numeric(s["high"], errors="coerce")
    volume = pd.to_numeric(s["volume"], errors="coerce")
    amount = pd.to_numeric(s.get("amount"), errors="coerce")
    pct = pd.to_numeric(s.get("pct_chg"), errors="coerce")

    ma_short = close.rolling(cfg.ma_short).mean()
    ma_mid = close.rolling(cfg.ma_mid).mean()
    ma_mid_up = bool(pd.notna(ma_mid.iloc[-1]) and pd.notna(ma_mid.iloc[-2]) and ma_mid.iloc[-1] > ma_mid.iloc[-2])
    cond_ma = (
        ma_mid_up
        and bool(ma_short.iloc[-1] > ma_mid.iloc[-1])
        and bool(close.iloc[-1] > ma_mid.iloc[-1])
    )

    ret60 = _return_pct(close, cfg.min_return_window)
    ret5 = _return_pct(close, 5)
    cond_ret = ret60 is not None and ret60 >= cfg.min_return_pct and ret5 is not None and ret5 < float(tuned["max_return_5d_pct"])

    no_limitup = bool(pct.tail(cfg.no_limitup_window).max() < cfg.limitup_threshold_pct)
    burst10 = bool(pct.tail(cfg.burst_window).max() >= cfg.burst_threshold_pct)

    avg_amt_5d = float(amount.tail(5).mean()) if not amount.tail(5).isna().all() else 0.0
    cond_amount = avg_amt_5d >= float(tuned["min_avg_amount_5d_yuan"])

    vol_peak = float(volume.tail(cfg.vol_peak_window).max()) if not volume.tail(cfg.vol_peak_window).isna().all() else 0.0
    vol_avg = float(volume.tail(cfg.vol_avg_window).mean()) if not volume.tail(cfg.vol_avg_window).isna().all() else 0.0
    vol_ratio = vol_peak / vol_avg if vol_avg > 0 else 0.0
    cond_vol = vol_ratio >= float(tuned["vol_peak_ratio"])

    prior_high = float(high.tail(cfg.no_new_high_window + 1).iloc[:-1].max())
    cond_no_new_high = bool(close.iloc[-1] < prior_high)

    mcap = float(market_cap_map.get(symbol, 0.0) or 0.0)
    cond_mcap = mcap >= cfg.min_market_cap_yi

    passed = all([cond_ma, cond_ret, no_limitup, burst10, cond_amount, cond_vol, cond_no_new_high, cond_mcap])
    if not passed:
        return None

    score = (
        min(max((ret60 or 0.0) / 40.0, 0.0), 2.0) * 35.0
        + min(max(vol_ratio / 2.0, 0.0), 2.0) * 35.0
        + min(max(float(pct.tail(cfg.burst_window).max()) / 10.0, 0.0), 2.0) * 30.0
    )
    return {
        "code": symbol,
        "name": name_map.get(symbol, symbol),
        "industry": str(sector_map.get(symbol, "") or "未知行业"),
        "score": round(float(score), 4),
        "close": float(close.iloc[-1]),
        "ma_short": float(ma_short.iloc[-1]),
        "ma_mid": float(ma_mid.iloc[-1]),
        "ret_window_pct": round(float(ret60 or 0.0), 4),
        "ret_5d_pct": round(float(ret5 or 0.0), 4),
        "burst_max_pct": round(float(pct.tail(cfg.burst_window).max()), 4),
        "vol_peak_ratio": round(float(vol_ratio), 4),
        "avg_amount_5d": round(float(avg_amt_5d), 2),
        "market_cap_yi": round(float(mcap), 4),
        "tags": ["趋势在", "爆发过", "没走完", "资金能进"],
    }


def run_custom_trend25(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    cfg = _parse_config(payload)
    symbols, name_map = _build_universe(cfg)
    window = _resolve_trading_window(end_calendar_day=resolve_end_calendar_day(), trading_days=cfg.trading_days)

    market_cap_map = _fetch_circ_mv_map()
    sector_map = fetch_sector_map()
    regime_row = load_latest_market_signal_daily()
    tuned, regime_context = _adapt_by_regime(cfg, regime_row)

    df_map: dict[str, pd.DataFrame] = {}
    with ThreadPoolExecutor(max_workers=cfg.max_workers) as ex:
        futures = [ex.submit(_fetch_one, sym, window) for sym in symbols]
        for fut in as_completed(futures):
            code, df = fut.result()
            if df is not None and not df.empty:
                df_map[code] = df

    rows: list[dict[str, Any]] = []
    for sym, df in df_map.items():
        item = _eval_symbol(sym, df, cfg, tuned, market_cap_map, name_map, sector_map)
        if item is not None:
            rows.append(item)

    rows.sort(key=lambda x: (-float(x.get("score", 0.0)), str(x.get("code", ""))))

    top_sectors: list[str] = []
    if cfg.enable_sector_resonance and rows:
        sec_counts: dict[str, int] = {}
        for r in rows:
            sec = str(r.get("industry", "") or "未知行业")
            sec_counts[sec] = sec_counts.get(sec, 0) + 1
        top_sectors = [k for k, _ in sorted(sec_counts.items(), key=lambda kv: (-kv[1], kv[0]))[: cfg.top_n_sectors]]
        top_set = set(top_sectors)
        rows = [r for r in rows if str(r.get("industry", "") or "未知行业") in top_set]

    cap_mult = float(tuned.get("capacity_mult", 1.0) or 1.0)
    final_cap = int(max(10, (cfg.limit_count if cfg.limit_count > 0 else len(rows)) * 0.25 * cap_mult))
    rows = rows[:final_cap]

    return {
        "strategy_id": cfg.strategy_id,
        "ok": True,
        "params": asdict(cfg),
        "tuned_params": tuned,
        "regime_context": regime_context,
        "trade_window": {
            "start_trade_date": window.start_trade_date.isoformat(),
            "end_trade_date": window.end_trade_date.isoformat(),
        },
        "summary": {
            "pool_symbols": len(symbols),
            "fetched_symbols": len(df_map),
            "selected_symbols": len(rows),
            "top_sectors": top_sectors,
        },
        "symbols_for_report": rows,
    }
