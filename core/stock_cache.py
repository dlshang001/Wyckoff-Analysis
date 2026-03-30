# -*- coding: utf-8 -*-
"""
股票日线数据缓存模块

三级缓存策略:
- L1 内存缓存: 同一次运行中重复请求直接返回
- L2 数据库缓存: Supabase 持久化存储
- L3 API 拉取: 缓存未命中时从数据源拉取
"""
from __future__ import annotations

import os
import threading
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Optional

import pandas as pd
from postgrest.exceptions import APIError

from core.constants import TABLE_STOCK_CACHE_DATA, TABLE_STOCK_CACHE_META
from integrations.supabase_client import get_supabase_client
from core.app_logger import log_event

STOCK_CACHE_ENABLED = os.getenv("STOCK_CACHE_ENABLED", "true").strip().lower() not in {
    "0", "false", "no", "off", "disabled"
}
STOCK_CACHE_TTL_DAYS = int(os.getenv("STOCK_CACHE_TTL_DAYS", "30"))
STOCK_CACHE_MAX_TRADING_DAYS = int(os.getenv("STOCK_CACHE_MAX_TRADING_DAYS", "120"))

_L1_CACHE: dict[str, pd.DataFrame] = {}
_L1_CACHE_LOCK = threading.RLock()


import re


def _parse_iso_datetime(value: str) -> datetime:
    s = str(value).replace("Z", "+00:00")
    s = re.sub(r"\.(\d{1,6})", lambda m: f".{m.group(1):0<6}", s)
    s = re.sub(r"\.(\d{7,})", lambda m: f".{m.group(1)[:6]}", s)
    return datetime.fromisoformat(s)


def _l1_cache_key(symbol: str, adjust: str, start: date, end: date) -> str:
    return f"{symbol}|{adjust}|{start.isoformat()}|{end.isoformat()}"


def _l1_get(symbol: str, adjust: str, start: date, end: date) -> Optional[pd.DataFrame]:
    if not STOCK_CACHE_ENABLED:
        return None
    key = _l1_cache_key(symbol, adjust, start, end)
    with _L1_CACHE_LOCK:
        df = _L1_CACHE.get(key)
        if df is not None:
            return df.copy()
    return None


def _l1_set(symbol: str, adjust: str, start: date, end: date, df: pd.DataFrame) -> None:
    if not STOCK_CACHE_ENABLED or df is None or df.empty:
        return
    key = _l1_cache_key(symbol, adjust, start, end)
    with _L1_CACHE_LOCK:
        _L1_CACHE[key] = df.copy()


def clear_l1_cache() -> None:
    with _L1_CACHE_LOCK:
        _L1_CACHE.clear()


@dataclass
class CacheMeta:
    symbol: str
    adjust: str
    source: str
    start_date: date
    end_date: date
    updated_at: datetime


_COL_MAP = {
    "日期": "date",
    "开盘": "open",
    "最高": "high",
    "最低": "low",
    "收盘": "close",
    "成交量": "volume",
    "成交额": "amount",
    "涨跌幅": "pct_chg",
}

_COL_MAP_REVERSE = {v: k for k, v in _COL_MAP.items()}


def normalize_hist_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.rename(columns=_COL_MAP).copy()
    keep = ["date", "open", "high", "low", "close", "volume", "amount", "pct_chg"]
    out = out[[c for c in keep if c in out.columns]].copy()
    for col in ["open", "high", "low", "close", "volume", "amount", "pct_chg"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    if "date" in out.columns:
        out["date"] = out["date"].astype(str)
    return out


def denormalize_hist_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.rename(columns=_COL_MAP_REVERSE).copy()
    return out


def get_cache_meta(symbol: str, adjust: str) -> Optional[CacheMeta]:
    if not STOCK_CACHE_ENABLED:
        return None
    supabase = get_supabase_client()
    if supabase is None:
        return None
    try:
        resp = (
            supabase.table(TABLE_STOCK_CACHE_META)
            .select("symbol,adjust,source,start_date,end_date,updated_at")
            .eq("symbol", symbol)
            .eq("adjust", adjust)
            .order("updated_at", desc=True)
            .limit(1)
            .execute()
        )
        if not resp.data:
            return None
        row = resp.data[0]
        return CacheMeta(
            symbol=row["symbol"],
            adjust=row["adjust"],
            source=row["source"],
            start_date=_parse_iso_datetime(row["start_date"]).date(),
            end_date=_parse_iso_datetime(row["end_date"]).date(),
            updated_at=_parse_iso_datetime(row["updated_at"]),
        )
    except APIError:
        return None
    except Exception:
        return None


def batch_get_cache_meta(symbols: list[str], adjust: str) -> dict[str, CacheMeta]:
    """
    批量查询多只股票的缓存元数据
    返回: {symbol: CacheMeta}
    """
    if not STOCK_CACHE_ENABLED or not symbols:
        log_event("info", "batch_get_cache_meta skipped", {
            "reason": "cache_disabled" if not STOCK_CACHE_ENABLED else "no_symbols"
        })
        return {}
    supabase = get_supabase_client()
    if supabase is None:
        log_event("warning", "batch_get_cache_meta supabase unavailable", {})
        return {}
    result: dict[str, CacheMeta] = {}
    try:
        batch_size = 500
        total_batches = (len(symbols) + batch_size - 1) // batch_size
        log_event("info", "batch_get_cache_meta start", {
            "total_symbols": len(symbols),
            "batch_size": batch_size,
            "total_batches": total_batches
        })
        
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i + batch_size]
            resp = (
                supabase.table(TABLE_STOCK_CACHE_META)
                .select("symbol,adjust,source,start_date,end_date,updated_at")
                .in_("symbol", batch)
                .eq("adjust", adjust)
                .execute()
            )
            if not resp.data:
                continue
            for row in resp.data:
                sym = row["symbol"]
                if sym in result:
                    continue
                result[sym] = CacheMeta(
                    symbol=sym,
                    adjust=row["adjust"],
                    source=row["source"],
                    start_date=_parse_iso_datetime(row["start_date"]).date(),
                    end_date=_parse_iso_datetime(row["end_date"]).date(),
                    updated_at=_parse_iso_datetime(row["updated_at"]),
                )
        
        log_event("info", "batch_get_cache_meta done", {
            "total_symbols": len(symbols),
            "cached_symbols": len(result),
            "hit_rate": len(result) / len(symbols) if symbols else 0.0
        })
    except Exception as e:
        log_event("error", "batch_get_cache_meta error", {
            "error": str(e)
        })
    return result


def load_cached_history(
    symbol: str,
    adjust: str,
    source: str,
    start_date: date,
    end_date: date,
) -> Optional[pd.DataFrame]:
    if not STOCK_CACHE_ENABLED:
        return None
    supabase = get_supabase_client()
    if supabase is None:
        return None
    try:
        resp = (
            supabase.table(TABLE_STOCK_CACHE_DATA)
            .select("date,open,high,low,close,volume,amount,pct_chg")
            .eq("symbol", symbol)
            .eq("adjust", adjust)
            .eq("source", source)
            .gte("date", start_date.isoformat())
            .lte("date", end_date.isoformat())
            .order("date")
            .execute()
        )
        if not resp.data:
            return None
        return pd.DataFrame(resp.data)
    except APIError:
        return None
    except Exception:
        return None


def upsert_cache_data(
    symbol: str,
    adjust: str,
    source: str,
    df: pd.DataFrame,
) -> None:
    if not STOCK_CACHE_ENABLED or df is None or df.empty:
        return
    supabase = get_supabase_client()
    if supabase is None:
        return
    normalized = normalize_hist_df(df)
    if normalized.empty:
        return
    payload = normalized.copy()
    payload["symbol"] = symbol
    payload["adjust"] = adjust
    payload["source"] = source
    payload["updated_at"] = datetime.utcnow().isoformat()
    records = payload.to_dict(orient="records")
    try:
        supabase.table(TABLE_STOCK_CACHE_DATA).upsert(records).execute()
    except Exception:
        return


def upsert_cache_meta(
    symbol: str,
    adjust: str,
    source: str,
    start_date: date,
    end_date: date,
) -> None:
    if not STOCK_CACHE_ENABLED:
        return
    supabase = get_supabase_client()
    if supabase is None:
        return
    payload = {
        "symbol": symbol,
        "adjust": adjust,
        "source": source,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "updated_at": datetime.utcnow().isoformat(),
    }
    try:
        supabase.table(TABLE_STOCK_CACHE_META).upsert(payload).execute()
    except Exception:
        return


def delete_old_cache_data(symbol: str, adjust: str, before_date: date) -> None:
    """删除指定日期之前的缓存数据"""
    if not STOCK_CACHE_ENABLED:
        return
    supabase = get_supabase_client()
    if supabase is None:
        return
    try:
        supabase.table(TABLE_STOCK_CACHE_DATA).delete().eq("symbol", symbol).eq(
            "adjust", adjust
        ).lt("date", before_date.isoformat()).execute()
    except Exception:
        pass


def cleanup_cache(ttl_days: int | None = None) -> None:
    """清理过期缓存数据"""
    if not STOCK_CACHE_ENABLED:
        return
    ttl = ttl_days or STOCK_CACHE_TTL_DAYS
    supabase = get_supabase_client()
    if supabase is None:
        return
    cutoff = datetime.utcnow() - timedelta(days=ttl)
    cutoff_iso = cutoff.isoformat()
    try:
        supabase.table(TABLE_STOCK_CACHE_DATA).delete().lt(
            "updated_at", cutoff_iso
        ).execute()
    except Exception:
        pass
    try:
        supabase.table(TABLE_STOCK_CACHE_META).delete().lt(
            "updated_at", cutoff_iso
        ).execute()
    except Exception:
        pass


def _refresh_cache_meta_range(symbol: str, adjust: str) -> None:
    """同步缓存表的最早/最晚日期到 meta，避免裁剪后 meta 仍指向过早日期导致反复重拉。"""
    supabase = get_supabase_client()
    if supabase is None:
        return
    try:
        first_resp = (
            supabase.table(TABLE_STOCK_CACHE_DATA)
            .select("date")
            .eq("symbol", symbol)
            .eq("adjust", adjust)
            .order("date", asc=True)
            .limit(1)
            .execute()
        )
        last_resp = (
            supabase.table(TABLE_STOCK_CACHE_DATA)
            .select("date")
            .eq("symbol", symbol)
            .eq("adjust", adjust)
            .order("date", desc=True)
            .limit(1)
            .execute()
        )
        if not first_resp.data or not last_resp.data:
            print(f"[_refresh_cache_meta_range] no data for {symbol}/{adjust}, skip")
            return
        start_date = _parse_iso_datetime(first_resp.data[0]["date"]).date()
        end_date = _parse_iso_datetime(last_resp.data[0]["date"]).date()
        meta = get_cache_meta(symbol, adjust)
        source = meta.source if meta is not None else "tushare"
        upsert_cache_meta(symbol, adjust, source, start_date, end_date)
        print(
            f"[_refresh_cache_meta_range] {symbol}/{adjust}: meta synced to start={start_date}, end={end_date}, source={source}"
        )
    except Exception as e:
        print(f"[_refresh_cache_meta_range] failed: {e}")


def trim_cache_to_max_days(symbol: str, adjust: str, max_days: int | None = None) -> None:
    """将缓存数据裁剪到最大天数"""
    max_days = max_days or STOCK_CACHE_MAX_TRADING_DAYS
    if max_days <= 0:
        return
    supabase = get_supabase_client()
    if supabase is None:
        return
    try:
        resp = (
            supabase.table(TABLE_STOCK_CACHE_DATA)
            .select("date")
            .eq("symbol", symbol)
            .eq("adjust", adjust)
            .order("date", desc=True)
            .limit(1)
            .execute()
        )
        if not resp.data:
            return
        latest_date = _parse_iso_datetime(resp.data[0]["date"]).date()
        cutoff_date = latest_date - timedelta(days=max_days * 2)
        delete_old_cache_data(symbol, adjust, cutoff_date)
        _refresh_cache_meta_range(symbol, adjust)
    except Exception:
        pass


def get_cache_stats() -> dict:
    """获取缓存统计信息"""
    supabase = get_supabase_client()
    if supabase is None:
        return {"enabled": STOCK_CACHE_ENABLED, "error": "no_supabase_client"}
    try:
        meta_resp = (
            supabase.table(TABLE_STOCK_CACHE_META)
            .select("symbol,start_date,end_date,updated_at", count="exact")
            .limit(1000)
            .execute()
        )
        data_resp = (
            supabase.table(TABLE_STOCK_CACHE_DATA)
            .select("symbol", count="exact")
            .limit(1)
            .execute()
        )

        meta_count = getattr(meta_resp, "count", 0) or 0
        data_count = getattr(data_resp, "count", 0) or 0

        oldest_date = None
        newest_date = None
        total_trading_days = 0

        if meta_resp.data:
            for row in meta_resp.data:
                start = row.get("start_date")
                end = row.get("end_date")
                if start:
                    if oldest_date is None or start < oldest_date:
                        oldest_date = start
                if end:
                    if newest_date is None or end > newest_date:
                        newest_date = end
                if start and end:
                    try:
                        start_dt = _parse_iso_datetime(start)
                        end_dt = _parse_iso_datetime(end)
                        days = (end_dt - start_dt).days
                        total_trading_days += max(days, 0)
                    except Exception:
                        pass

        estimated_size_mb = (data_count * 9 * 8) / (1024 * 1024)

        return {
            "enabled": STOCK_CACHE_ENABLED,
            "meta_count": meta_count,
            "data_count": data_count,
            "l1_cache_size": len(_L1_CACHE),
            "oldest_date": oldest_date,
            "newest_date": newest_date,
            "estimated_size_mb": round(estimated_size_mb, 2),
            "avg_trading_days": round(total_trading_days / max(meta_count, 1), 1) if meta_count > 0 else 0,
        }
    except Exception as e:
        return {"enabled": STOCK_CACHE_ENABLED, "error": str(e)}
