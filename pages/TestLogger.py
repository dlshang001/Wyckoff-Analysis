# -*- coding: utf-8 -*-
import streamlit as st
from core.app_logger import log_event

st.title("日志功能测试")

st.write("点击下方按钮测试日志写入功能")

if st.button("测试日志写入"):
    st.write("开始测试...")
    
    # 测试 1: 简单日志
    st.write("1. 写入信息级别日志...")
    log_event("info", "测试日志 - 信息级别", {"test": "value1"})
    
    # 测试 2: 警告日志
    st.write("2. 写入警告级别日志...")
    log_event("warning", "测试日志 - 警告级别", {"test": "value2"})
    
    # 测试 3: 错误日志
    st.write("3. 写入错误级别日志...")
    log_event("error", "测试日志 - 错误级别", {"test": "value3"})
    
    st.success("日志测试完成！请检查 Supabase 的 app_logs 表是否有数据。")
    st.info("请查看控制台输出，应该能看到 [app_logger] 开头的日志信息")
