# -*- coding: utf-8 -*-
"""测试日志功能（不依赖 Streamlit）"""
import os
import sys

# 添加项目根目录到 Python 路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# 手动加载环境变量
env_path = os.path.join(project_root, ".env")
if os.path.exists(env_path):
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                os.environ[key.strip()] = value.strip()

from core.app_logger import log_event

print("=" * 60)
print("测试日志功能")
print("=" * 60)

# 检查环境变量
print("\n检查环境变量:")
print(f"SUPABASE_URL: {os.getenv('SUPABASE_URL')}")
print(f"SUPABASE_KEY: {'***' if os.getenv('SUPABASE_KEY') else 'None'}")
print(f"SUPABASE_LOG_ENABLED: {os.getenv('SUPABASE_LOG_ENABLED', '1')}")

# 测试 1: 简单日志
print("\n" + "=" * 60)
print("测试 1: 写入信息级别日志")
print("=" * 60)
try:
    log_event("info", "测试日志 - 信息级别", {"test": "value1", "number": 123})
    print("✓ 信息级别日志写入完成")
except Exception as e:
    print(f"✗ 信息级别日志写入失败: {e}")
    import traceback
    traceback.print_exc()

# 测试 2: 警告日志
print("\n" + "=" * 60)
print("测试 2: 写入警告级别日志")
print("=" * 60)
try:
    log_event("warning", "测试日志 - 警告级别", {"test": "value2", "warning": "something might be wrong"})
    print("✓ 警告级别日志写入完成")
except Exception as e:
    print(f"✗ 警告级别日志写入失败: {e}")
    import traceback
    traceback.print_exc()

# 测试 3: 错误日志
print("\n" + "=" * 60)
print("测试 3: 写入错误级别日志")
print("=" * 60)
try:
    log_event("error", "测试日志 - 错误级别", {"test": "value3", "error": "something went wrong"})
    print("✓ 错误级别日志写入完成")
except Exception as e:
    print(f"✗ 错误级别日志写入失败: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 60)
print("日志测试完成！")
print("=" * 60)
print("\n请检查 Supabase 的 app_logs 表是否有数据。")
print("如果表中没有数据，请检查：")
print("1. app_logs 表是否存在")
print("2. SUPABASE_URL 和 SUPABASE_KEY 是否正确")
print("3. Supabase 表的 RLS（行级安全策略）设置")
