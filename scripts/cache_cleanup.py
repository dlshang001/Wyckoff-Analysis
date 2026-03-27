# -*- coding: utf-8 -*-
"""
缓存清理定时任务

清理过期的股票日线缓存数据，避免存储空间无限增长。
可通过 GitHub Actions 每周执行一次。
"""
from __future__ import annotations

import os
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.stock_cache import (
    STOCK_CACHE_ENABLED,
    STOCK_CACHE_TTL_DAYS,
    cleanup_cache,
    get_cache_stats,
)

TZ = ZoneInfo("Asia/Shanghai")


def _now() -> str:
    return datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")


def _log(msg: str) -> None:
    print(f"[{_now()}] {msg}", flush=True)


def main() -> int:
    _log("========== 缓存清理任务开始 ==========")

    if not STOCK_CACHE_ENABLED:
        _log("缓存功能已禁用，跳过清理")
        return 0

    stats_before = get_cache_stats()
    _log(f"清理前统计:")
    _log(f"  - 缓存股票数: {stats_before.get('meta_count', 0)}")
    _log(f"  - 数据条数: {stats_before.get('data_count', 0)}")
    _log(f"  - 估算存储: {stats_before.get('estimated_size_mb', 0):.2f} MB")

    ttl_days = STOCK_CACHE_TTL_DAYS
    _log(f"开始清理超过 {ttl_days} 天的缓存数据...")

    try:
        cleanup_cache(ttl_days=ttl_days)
        _log("清理完成")
    except Exception as e:
        _log(f"清理失败: {e}")
        return 1

    stats_after = get_cache_stats()
    _log(f"清理后统计:")
    _log(f"  - 缓存股票数: {stats_after.get('meta_count', 0)}")
    _log(f"  - 数据条数: {stats_after.get('data_count', 0)}")
    _log(f"  - 估算存储: {stats_after.get('estimated_size_mb', 0):.2f} MB")

    meta_cleaned = stats_before.get("meta_count", 0) - stats_after.get("meta_count", 0)
    data_cleaned = stats_before.get("data_count", 0) - stats_after.get("data_count", 0)
    _log(f"清理结果:")
    _log(f"  - 清理股票数: {meta_cleaned}")
    _log(f"  - 清理数据条数: {data_cleaned}")

    _log("========== 缓存清理任务完成 ==========")
    return 0


if __name__ == "__main__":
    sys.exit(main())
