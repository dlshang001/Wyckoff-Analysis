# -*- coding: utf-8 -*-
"""
缓存功能测试脚本

测试步骤:
1. 检查缓存是否启用
2. 检查 Supabase 连接
3. 检查缓存元数据
4. 测试批量查询
5. 测试缓存命中逻辑
"""
from __future__ import annotations

import os
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 加载 .env 文件
env_path = Path(__file__).resolve().parent.parent / ".env"
if env_path.exists():
    from dotenv import load_dotenv
    load_dotenv(env_path)
    print(f"[0] 已加载 .env 文件: {env_path}")
else:
    print(f"[0] ⚠️ 未找到 .env 文件: {env_path}")

print("=" * 60)
print("缓存功能测试")
print("=" * 60)

# 1. 检查缓存是否启用
print("\n[1] 检查缓存配置...")
from core.stock_cache import (
    STOCK_CACHE_ENABLED,
    STOCK_CACHE_TTL_DAYS,
    STOCK_CACHE_MAX_TRADING_DAYS,
)

print(f"    STOCK_CACHE_ENABLED = {STOCK_CACHE_ENABLED}")
print(f"    STOCK_CACHE_TTL_DAYS = {STOCK_CACHE_TTL_DAYS}")
print(f"    STOCK_CACHE_MAX_TRADING_DAYS = {STOCK_CACHE_MAX_TRADING_DAYS}")

if not STOCK_CACHE_ENABLED:
    print("    ❌ 缓存已禁用！请设置 STOCK_CACHE_ENABLED=true")
    sys.exit(1)
print("    ✅ 缓存已启用")

# 2. 检查 Supabase 连接
print("\n[2] 检查 Supabase 连接...")

# 直接创建 Supabase 客户端（不依赖 Streamlit）
url = os.getenv("SUPABASE_URL", "").strip()
key = os.getenv("SUPABASE_KEY", "").strip()

if not url or not key:
    print("    ❌ 未找到 SUPABASE_URL 或 SUPABASE_KEY 环境变量")
    print(f"    SUPABASE_URL: {'已设置' if url else '未设置'}")
    print(f"    SUPABASE_KEY: {'已设置' if key else '未设置'}")
    sys.exit(1)

from supabase import create_client

supabase = create_client(url, key)
print("    ✅ Supabase 客户端初始化成功")

# 3. 检查缓存表
print("\n[3] 检查缓存表...")
from core.constants import TABLE_STOCK_CACHE_META, TABLE_STOCK_CACHE_DATA

try:
    meta_resp = supabase.table(TABLE_STOCK_CACHE_META).select("symbol", count="exact").limit(1).execute()
    meta_count = getattr(meta_resp, "count", 0) or 0
    print(f"    stock_cache_meta 表记录数: {meta_count}")
    
    data_resp = supabase.table(TABLE_STOCK_CACHE_DATA).select("symbol", count="exact").limit(1).execute()
    data_count = getattr(data_resp, "count", 0) or 0
    print(f"    stock_cache_data 表记录数: {data_count}")
    
    if meta_count == 0:
        print("    ⚠️ 缓存表为空，请先运行一次策略以填充缓存")
    else:
        print("    ✅ 缓存表有数据")
except Exception as e:
    print(f"    ❌ 查询缓存表失败: {e}")
    sys.exit(1)

# 4. 查看缓存样本
print("\n[4] 查看缓存样本...")
try:
    sample_resp = (
        supabase.table(TABLE_STOCK_CACHE_META)
        .select("symbol,adjust,source,start_date,end_date")
        .limit(5)
        .execute()
    )
    if sample_resp.data:
        for row in sample_resp.data:
            print(f"    {row['symbol']}: adjust={row['adjust']}, source={row['source']}, "
                  f"start={row['start_date']}, end={row['end_date']}")
    else:
        print("    无数据")
except Exception as e:
    print(f"    ❌ 查询失败: {e}")

# 5. 测试批量查询
print("\n[5] 测试批量查询...")
from core.stock_cache import batch_get_cache_meta

test_symbols = ["000001", "000002", "600000", "600519", "300750"]
meta_map = batch_get_cache_meta(test_symbols, "qfq")
print(f"    查询 {len(test_symbols)} 只股票，返回 {len(meta_map)} 条元数据")
if meta_map:
    for sym, meta in list(meta_map.items())[:3]:
        print(f"    {sym}: start={meta.start_date}, end={meta.end_date}, adjust={meta.adjust}")
    print("    ✅ 批量查询正常")
else:
    print("    ⚠️ 批量查询返回空，可能是 adjust 字段不匹配")

# 6. 测试 adjust 字段
print("\n[6] 检查 adjust 字段值...")
try:
    adjust_resp = (
        supabase.table(TABLE_STOCK_CACHE_META)
        .select("adjust")
        .limit(100)
        .execute()
    )
    if adjust_resp.data:
        adjust_values = set(row["adjust"] for row in adjust_resp.data)
        print(f"    adjust 字段值: {adjust_values}")
        if "qfq" not in adjust_values:
            print("    ⚠️ 缓存中没有 adjust=qfq 的数据！")
            print("    这可能是缓存未命中的原因")
except Exception as e:
    print(f"    ❌ 查询失败: {e}")

# 7. 测试缓存命中逻辑
print("\n[7] 测试缓存命中逻辑...")
if meta_map:
    sample_symbol = list(meta_map.keys())[0]
    sample_meta = meta_map[sample_symbol]
    
    # 测试完全命中
    test_start = sample_meta.start_date
    test_end = sample_meta.end_date
    
    print(f"    测试股票: {sample_symbol}")
    print(f"    缓存范围: {sample_meta.start_date} ~ {sample_meta.end_date}")
    print(f"    请求范围: {test_start} ~ {test_end}")
    
    # 模拟命中判断
    if sample_meta.end_date >= test_end and sample_meta.start_date <= test_start:
        print("    ✅ 应该命中缓存")
    else:
        print("    ❌ 不会命中缓存")
        
    # 测试日期不匹配
    from datetime import timedelta
    test_start_earlier = sample_meta.start_date - timedelta(days=10)
    print(f"\n    测试更早的开始日期: {test_start_earlier}")
    if sample_meta.end_date >= test_end and sample_meta.start_date <= test_start_earlier:
        print("    ✅ 应该命中缓存")
    else:
        print("    ⚠️ 不会命中缓存（需要增量更新）")

# 8. 测试完整拉取流程
print("\n[8] 测试完整拉取流程...")
from core.stock_data_fetcher import StockDataFetcher

fetcher = StockDataFetcher(max_workers=2)
test_symbols_full = ["000001", "000002"]

# 使用缓存中实际的日期范围
if meta_map:
    sample_meta = list(meta_map.values())[0]
    test_start = sample_meta.start_date
    test_end = sample_meta.end_date
else:
    test_start = date(2026, 3, 1)
    test_end = date(2026, 3, 26)

print(f"    拉取 {test_symbols_full} 数据...")
print(f"    请求范围: {test_start} ~ {test_end}")
try:
    df_map, summary = fetcher.fetch_all(
        symbols=test_symbols_full,
        start_date=test_start,
        end_date=test_end,
        adjust="qfq",
        use_batch_mode=False,
    )
    print(f"    结果: {len(df_map)} 只股票")
    print(f"    缓存命中率: {summary.cache_hit_rate:.1%}")
    print(f"    耗时: {summary.elapsed_seconds:.2f}s")
    
    if summary.cache_hit_rate > 0:
        print("    ✅ 缓存命中正常")
    else:
        print("    ⚠️ 缓存未命中")
except Exception as e:
    print(f"    ❌ 拉取失败: {e}")

# 9. 测试批量模式
print("\n[9] 测试批量模式（按交易日获取）...")
test_symbols_batch = ["000001", "000002", "600000", "600519", "300750"]
print(f"    拉取 {len(test_symbols_batch)} 只股票数据（批量模式）...")
try:
    df_map_batch, summary_batch = fetcher.fetch_all(
        symbols=test_symbols_batch,
        start_date=test_start,
        end_date=test_end,
        adjust="qfq",
        use_batch_mode=True,
    )
    print(f"    结果: {len(df_map_batch)} 只股票")
    print(f"    耗时: {summary_batch.elapsed_seconds:.2f}s")
    print("    ✅ 批量模式正常")
except Exception as e:
    print(f"    ❌ 批量模式失败: {e}")

print("\n" + "=" * 60)
print("测试完成")
print("=" * 60)
