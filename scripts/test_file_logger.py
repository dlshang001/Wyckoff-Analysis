# -*- coding: utf-8 -*-
"""测试文件日志功能"""
import os
import sys

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

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
print("测试文件日志功能")
print("=" * 60)

log_dir = os.path.join(project_root, "logs")
log_file = os.path.join(log_dir, "app.log")
print(f"\n日志目录: {log_dir}")
print(f"日志文件: {log_file}")

print("\n" + "=" * 60)
print("写入测试日志")
print("=" * 60)

log_event("debug", "这是一条调试日志", {"debug": "data", "value": 123})
print("✓ 调试日志已写入")

log_event("info", "这是一条信息日志", {"info": "data", "value": 456})
print("✓ 信息日志已写入")

log_event("warning", "这是一条警告日志", {"warning": "data", "value": 789})
print("✓ 警告日志已写入")

log_event("error", "这是一条错误日志", {"error": "data", "value": 101112})
print("✓ 错误日志已写入")

log_event("critical", "这是一条严重日志", {"critical": "data", "value": 131415})
print("✓ 严重日志已写入")

print("\n" + "=" * 60)
print("检查日志文件内容")
print("=" * 60)

if os.path.exists(log_file):
    print(f"\n日志文件大小: {os.path.getsize(log_file)} 字节")
    print("\n最近 10 条日志:")
    with open(log_file, "r", encoding="utf-8") as f:
        lines = f.readlines()
        for line in lines[-10:]:
            print(f"  {line.rstrip()}")
else:
    print("\n✗ 日志文件不存在")

print("\n" + "=" * 60)
print("测试完成！")
print("=" * 60)
