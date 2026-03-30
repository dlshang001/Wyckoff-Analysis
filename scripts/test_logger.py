# -*- coding: utf-8 -*-
"""测试日志功能"""
import os
import sys

# 添加项目根目录到 Python 路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.app_logger import log_event

print("开始测试日志功能...")

# 测试 1: 简单日志
print("\n测试 1: 简单日志")
log_event("info", "测试日志 - 信息级别", {"test": "value1"})

# 测试 2: 警告日志
print("\n测试 2: 警告日志")
log_event("warning", "测试日志 - 警告级别", {"test": "value2"})

# 测试 3: 错误日志
print("\n测试 3: 错误日志")
log_event("error", "测试日志 - 错误级别", {"test": "value3"})

print("\n日志测试完成！请检查 Supabase 的 app_logs 表是否有数据。")
