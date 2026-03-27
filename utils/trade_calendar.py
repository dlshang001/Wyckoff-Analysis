# -*- coding: utf-8 -*-
"""
交易日历工具

使用 tushare 获取交易日历
"""
from __future__ import annotations

import os
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

_TRADE_CALENDAR_CACHE_FILE = Path(__file__).resolve().parent.parent / "data" / "trade_calendar_cache.json"
_CACHE_TTL_SECONDS = 24 * 60 * 60


def get_trade_dates(start_date: str, end_date: str) -> list[str]:
    """
    获取指定范围内的交易日列表
    
    Args:
        start_date: 开始日期 (YYYYMMDD 或 YYYY-MM-DD)
        end_date: 结束日期 (YYYYMMDD 或 YYYY-MM-DD)
    
    Returns:
        list[str]: 交易日列表 (YYYYMMDD 格式)
    """
    start_s = str(start_date).replace("-", "")
    end_s = str(end_date).replace("-", "")
    
    from utils.tushare_client import get_pro
    pro = get_pro()
    
    if pro is not None:
        try:
            df = pro.trade_cal(
                exchange="SSE",
                start_date=start_s,
                end_date=end_s,
                is_open="1",
            )
            if df is not None and not df.empty:
                dates = df["cal_date"].astype(str).tolist()
                return sorted(dates)
        except Exception as e:
            print(f"[trade_calendar] tushare 获取交易日历失败: {e}")
    
    return _generate_trade_dates_fallback(start_s, end_s)


def _generate_trade_dates_fallback(start_date: str, end_date: str) -> list[str]:
    """
    回退方案：生成交易日列表（排除周末）
    
    注意：这是简化版本，不考虑法定节假日
    """
    start = date(int(start_date[:4]), int(start_date[4:6]), int(start_date[6:8]))
    end = date(int(end_date[:4]), int(end_date[4:6]), int(end_date[6:8]))
    
    dates = []
    current = start
    while current <= end:
        if current.weekday() < 5:
            dates.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)
    
    return dates


def is_trade_date(date_str: str) -> bool:
    """
    判断是否为交易日
    
    Args:
        date_str: 日期 (YYYYMMDD 或 YYYY-MM-DD)
    
    Returns:
        bool: 是否为交易日
    """
    date_s = str(date_str).replace("-", "")
    
    from utils.tushare_client import get_pro
    pro = get_pro()
    
    if pro is not None:
        try:
            df = pro.trade_cal(
                exchange="SSE",
                start_date=date_s,
                end_date=date_s,
                is_open="1",
            )
            return df is not None and not df.empty
        except Exception:
            pass
    
    d = date(int(date_s[:4]), int(date_s[4:6]), int(date_s[6:8]))
    return d.weekday() < 5


def get_last_n_trade_dates(n: int, end_date: str | None = None) -> list[str]:
    """
    获取最近 N 个交易日
    
    Args:
        n: 交易日数量
        end_date: 结束日期 (默认今天)
    
    Returns:
        list[str]: 交易日列表 (YYYYMMDD 格式)
    """
    if end_date:
        end = date(int(end_date[:4]), int(end_date[4:6]), int(end_date[6:8]))
    else:
        end = date.today()
    
    start = end - timedelta(days=n * 3)
    
    dates = get_trade_dates(start.strftime("%Y%m%d"), end.strftime("%Y%m%d"))
    
    if len(dates) > n:
        dates = dates[-n:]
    
    return dates
