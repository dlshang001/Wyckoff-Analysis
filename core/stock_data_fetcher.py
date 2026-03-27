# -*- coding: utf-8 -*-
"""
股票数据拉取器

统一管理股票日线数据的拉取、缓存和分发。
支持批量预加载、增量更新、并发拉取。
"""
from __future__ import annotations

import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Callable, Literal

import pandas as pd

from core.stock_cache import (
    STOCK_CACHE_ENABLED,
    STOCK_CACHE_MAX_TRADING_DAYS,
    CacheMeta,
    batch_get_cache_meta,
    clear_l1_cache,
    delete_old_cache_data,
    denormalize_hist_df,
    get_cache_stats,
    load_cached_history,
    normalize_hist_df,
    trim_cache_to_max_days,
    upsert_cache_data,
    upsert_cache_meta,
)
from integrations.data_source import fetch_stock_hist


@dataclass
class FetchResult:
    symbol: str
    df: pd.DataFrame | None
    source: str
    error: str | None
    from_cache: bool
    incremental: bool


@dataclass
class BatchFetchSummary:
    total_symbols: int
    cached_symbols: int
    fetched_symbols: int
    failed_symbols: int
    elapsed_seconds: float
    cache_hit_rate: float


class StockDataFetcher:
    """
    股票数据拉取器

    功能:
    - 批量预加载股票数据
    - 三级缓存 (内存 → 数据库 → API)
    - 增量更新
    - 并发拉取
    - 数据裁剪 (限制最大天数)

    用法:
        fetcher = StockDataFetcher(max_workers=8)
        df_map = fetcher.fetch_all(symbols, start_date, end_date)
    """

    def __init__(
        self,
        max_workers: int = 8,
        use_cache: bool = True,
        max_trading_days: int | None = None,
        progress_callback: Callable[[int, int, str], None] | None = None,
    ):
        self.max_workers = max_workers
        self.use_cache = use_cache
        self.max_trading_days = max_trading_days or STOCK_CACHE_MAX_TRADING_DAYS
        self.progress_callback = progress_callback
        self._l1_cache: dict[str, pd.DataFrame] = {}

    def _l1_key(self, symbol: str, adjust: str, start: date, end: date) -> str:
        return f"{symbol}|{adjust}|{start.isoformat()}|{end.isoformat()}"

    def _l1_get(self, symbol: str, adjust: str, start: date, end: date) -> pd.DataFrame | None:
        key = self._l1_key(symbol, adjust, start, end)
        df = self._l1_cache.get(key)
        if df is not None:
            return df.copy()
        return None

    def _l1_set(self, symbol: str, adjust: str, start: date, end: date, df: pd.DataFrame) -> None:
        if df is None or df.empty:
            return
        key = self._l1_key(symbol, adjust, start, end)
        self._l1_cache[key] = df.copy()

    def clear_cache(self) -> None:
        self._l1_cache.clear()
        clear_l1_cache()

    def fetch_one(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        adjust: Literal["", "qfq", "hfq"] = "qfq",
    ) -> FetchResult:
        """
        拉取单只股票数据

        优先级: L1 内存 → L2 数据库 → L3 API
        """
        adjust_key = adjust or "none"

        l1_df = self._l1_get(symbol, adjust_key, start_date, end_date)
        if l1_df is not None:
            return FetchResult(
                symbol=symbol,
                df=l1_df,
                source="memory",
                error=None,
                from_cache=True,
                incremental=False,
            )

        if not self.use_cache:
            try:
                df = fetch_stock_hist(symbol, start_date, end_date, adjust, use_cache=False)
                self._l1_set(symbol, adjust_key, start_date, end_date, df)
                return FetchResult(
                    symbol=symbol,
                    df=df,
                    source="api",
                    error=None,
                    from_cache=False,
                    incremental=False,
                )
            except Exception as e:
                return FetchResult(
                    symbol=symbol,
                    df=None,
                    source="",
                    error=str(e),
                    from_cache=False,
                    incremental=False,
                )

        from core.stock_cache import get_cache_meta

        cache_meta = get_cache_meta(symbol, adjust_key)
        cached_df = None
        fetch_start = start_date
        fetch_end = end_date
        actual_source = ""
        incremental = False

        if cache_meta is not None:
            if cache_meta.end_date >= end_date and cache_meta.start_date <= start_date:
                cached_df = load_cached_history(
                    symbol, adjust_key, cache_meta.source, start_date, end_date
                )
                if cached_df is not None and not cached_df.empty:
                    result_df = denormalize_hist_df(cached_df)
                    self._l1_set(symbol, adjust_key, start_date, end_date, result_df)
                    return FetchResult(
                        symbol=symbol,
                        df=result_df,
                        source=f"{cache_meta.source}(cached)",
                        error=None,
                        from_cache=True,
                        incremental=False,
                    )
            elif cache_meta.end_date >= start_date and cache_meta.end_date < end_date:
                fetch_start = cache_meta.end_date + timedelta(days=1)
                cached_df = load_cached_history(
                    symbol, adjust_key, cache_meta.source, start_date, cache_meta.end_date
                )
                actual_source = cache_meta.source
                incremental = True

        df = None
        source = ""
        error = None

        try:
            df = fetch_stock_hist(symbol, fetch_start, fetch_end, adjust, use_cache=False)
            source = "api"
        except Exception as e:
            error = str(e)

        if df is not None and cached_df is not None and not cached_df.empty:
            new_normalized = normalize_hist_df(df)
            combined = pd.concat([cached_df, new_normalized], ignore_index=True)
            combined = combined.drop_duplicates(subset=["date"], keep="last")
            combined = combined.sort_values("date").reset_index(drop=True)
            df = denormalize_hist_df(combined)
            source = actual_source or source

        if df is not None and not df.empty:
            self._l1_set(symbol, adjust_key, start_date, end_date, df)

            if self.use_cache:
                upsert_cache_data(symbol, adjust_key, source, df)
                upsert_cache_meta(symbol, adjust_key, source, start_date, end_date)

                if self.max_trading_days > 0:
                    trim_cache_to_max_days(symbol, adjust_key, self.max_trading_days)

        return FetchResult(
            symbol=symbol,
            df=df,
            source=source,
            error=error,
            from_cache=False,
            incremental=incremental,
        )

    def fetch_all(
        self,
        symbols: list[str],
        start_date: date,
        end_date: date,
        adjust: Literal["", "qfq", "hfq"] = "qfq",
    ) -> tuple[dict[str, pd.DataFrame], BatchFetchSummary]:
        """
        批量拉取股票数据

        返回: (df_map, summary)
        - df_map: {symbol: DataFrame}
        - summary: 拉取统计信息
        """
        if not symbols:
            return {}, BatchFetchSummary(
                total_symbols=0,
                cached_symbols=0,
                fetched_symbols=0,
                failed_symbols=0,
                elapsed_seconds=0.0,
                cache_hit_rate=0.0,
            )

        start_time = time.monotonic()
        df_map: dict[str, pd.DataFrame] = {}
        cached_count = 0
        fetched_count = 0
        failed_count = 0
        adjust_key = adjust or "none"

        cached_symbols: list[str] = []
        uncached_symbols: list[str] = []

        if self.use_cache and STOCK_CACHE_ENABLED:
            meta_map = batch_get_cache_meta(symbols, adjust_key)
            for sym in symbols:
                meta = meta_map.get(sym)
                if meta is not None and meta.end_date >= end_date and meta.start_date <= start_date:
                    cached_symbols.append(sym)
                else:
                    uncached_symbols.append(sym)
        else:
            uncached_symbols = list(symbols)

        for sym in cached_symbols:
            meta = meta_map.get(sym)
            if meta is None:
                uncached_symbols.append(sym)
                continue
            cached_df = load_cached_history(sym, adjust_key, meta.source, start_date, end_date)
            if cached_df is not None and not cached_df.empty:
                result_df = denormalize_hist_df(cached_df)
                df_map[sym] = result_df
                self._l1_set(sym, adjust_key, start_date, end_date, result_df)
                cached_count += 1
            else:
                uncached_symbols.append(sym)

        if uncached_symbols:
            total = len(uncached_symbols)
            completed = 0

            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {
                    executor.submit(self.fetch_one, sym, start_date, end_date, adjust): sym
                    for sym in uncached_symbols
                }

                for future in as_completed(futures):
                    sym = futures[future]
                    completed += 1

                    try:
                        result = future.result()
                        if result.df is not None and not result.df.empty:
                            df_map[sym] = result.df
                            if result.from_cache:
                                cached_count += 1
                            else:
                                fetched_count += 1
                        else:
                            failed_count += 1
                    except Exception:
                        failed_count += 1

                    if self.progress_callback:
                        self.progress_callback(completed, total, sym)

        elapsed = time.monotonic() - start_time
        total = len(symbols)
        cache_hit_rate = cached_count / total if total > 0 else 0.0

        summary = BatchFetchSummary(
            total_symbols=total,
            cached_symbols=cached_count,
            fetched_symbols=fetched_count,
            failed_symbols=failed_count,
            elapsed_seconds=elapsed,
            cache_hit_rate=cache_hit_rate,
        )

        return df_map, summary

    def get_stats(self) -> dict:
        """获取缓存统计信息"""
        stats = get_cache_stats()
        stats["l1_local_size"] = len(self._l1_cache)
        return stats


_default_fetcher: StockDataFetcher | None = None


def get_default_fetcher() -> StockDataFetcher:
    """获取默认的数据拉取器实例"""
    global _default_fetcher
    if _default_fetcher is None:
        max_workers = int(os.getenv("STOCK_FETCHER_MAX_WORKERS", "8"))
        _default_fetcher = StockDataFetcher(max_workers=max_workers)
    return _default_fetcher


def fetch_stock_data(
    symbols: list[str],
    start_date: date,
    end_date: date,
    adjust: Literal["", "qfq", "hfq"] = "qfq",
    max_workers: int = 8,
    progress_callback: Callable[[int, int, str], None] | None = None,
) -> tuple[dict[str, pd.DataFrame], BatchFetchSummary]:
    """
    便捷函数：批量拉取股票数据

    用法:
        df_map, summary = fetch_stock_data(
            symbols=["000001", "000002"],
            start_date=date(2024, 1, 1),
            end_date=date(2024, 12, 31),
        )
    """
    fetcher = StockDataFetcher(
        max_workers=max_workers,
        progress_callback=progress_callback,
    )
    return fetcher.fetch_all(symbols, start_date, end_date, adjust)
