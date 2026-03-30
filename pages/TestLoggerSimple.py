# -*- coding: utf-8 -*-
"""日志功能测试（直接在 Streamlit 中运行）"""
import streamlit as st
import os
import sys

# 添加项目根目录到 Python 路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

st.title("日志功能测试")

st.write("这个页面将测试日志写入功能")

# 检查环境变量
st.subheader("1. 检查环境变量")
st.write(f"SUPABASE_URL: {os.getenv('SUPABASE_URL')}")
st.write(f"SUPABASE_KEY: {'***' if os.getenv('SUPABASE_KEY') else 'None'}")
st.write(f"SUPABASE_LOG_ENABLED: {os.getenv('SUPABASE_LOG_ENABLED', '1')}")

# 测试 Supabase 连接
st.subheader("2. 测试 Supabase 连接")
try:
    from supabase import create_client
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    
    if url and key:
        supabase = create_client(url, key)
        st.success("✓ Supabase 客户端创建成功")
    else:
        st.error("✗ 缺少 SUPABASE_URL 或 SUPABASE_KEY")
except Exception as e:
    st.error(f"✗ Supabase 客户端创建失败: {e}")
    import traceback
    st.code(traceback.format_exc())

# 测试 app_logs 表
st.subheader("3. 测试 app_logs 表")
try:
    from supabase import create_client
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    
    if url and key:
        supabase = create_client(url, key)
        result = supabase.table("app_logs").select("count").execute()
        st.success(f"✓ app_logs 表存在，当前记录数: {len(result.data) if result.data else 0}")
    else:
        st.error("✗ 缺少 SUPABASE_URL 或 SUPABASE_KEY")
except Exception as e:
    st.error(f"✗ app_logs 表查询失败: {e}")
    import traceback
    st.code(traceback.format_exc())

# 测试日志写入
st.subheader("4. 测试日志写入")
if st.button("写入测试日志"):
    try:
        from core.app_logger import log_event
        
        log_event("info", "测试日志 - 信息级别", {"test": "value1", "number": 123})
        log_event("warning", "测试日志 - 警告级别", {"test": "value2", "warning": "something might be wrong"})
        log_event("error", "测试日志 - 错误级别", {"test": "value3", "error": "something went wrong"})
        
        st.success("✓ 测试日志写入完成！")
        st.info("请检查 Supabase 的 app_logs 表是否有数据")
        
        # 尝试读取刚写入的日志
        try:
            from supabase import create_client
            url = os.getenv("SUPABASE_URL")
            key = os.getenv("SUPABASE_KEY")
            
            if url and key:
                supabase = create_client(url, key)
                result = supabase.table("app_logs").select("*").order("created_at", desc=True).limit(10).execute()
                st.write("最近 10 条日志:")
                for record in result.data:
                    st.json(record)
        except Exception as e:
            st.error(f"读取日志失败: {e}")
            
    except Exception as e:
        st.error(f"✗ 日志写入失败: {e}")
        import traceback
        st.code(traceback.format_exc())
