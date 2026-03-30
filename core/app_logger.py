# -*- coding: utf-8 -*-
"""轻量 Supabase 日志落盘工具。

设计要点：
- 默认启用，可通过环境变量 SUPABASE_LOG_ENABLED=0 关闭。
- 尽量失败兜底：Supabase 不可用时静默返回，避免影响主流程。
- 上报字段：level, message, context(json), user_id。
"""
from __future__ import annotations

import json
import os
import sys
from typing import Any, Mapping

from core.constants import TABLE_APP_LOGS


def _to_bool(v: Any, default: bool = True) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "on"}


def _get_supabase_client_direct():
    """直接创建 Supabase 客户端（不依赖 Streamlit session_state）"""
    from supabase import create_client
    
    # 尝试从环境变量加载
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    
    # 如果环境变量没有，尝试从 .env 文件加载
    if not url or not key:
        try:
            from dotenv import load_dotenv
            env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
            if os.path.exists(env_path):
                load_dotenv(env_path)
                url = os.getenv("SUPABASE_URL")
                key = os.getenv("SUPABASE_KEY")
        except ImportError:
            pass
        except Exception as e:
            pass
    
    if not url or not key:
        try:
            import streamlit as st
            url = st.secrets.get("SUPABASE_URL")
            key = st.secrets.get("SUPABASE_KEY")
        except (FileNotFoundError, KeyError, ImportError):
            pass
    
    if not url or not key:
        return None
    
    try:
        return create_client(url, key)
    except Exception as e:
        return None


def _current_user_id() -> str | None:
    try:
        import streamlit as st

        user = st.session_state.get("user")
        if isinstance(user, dict):
            return user.get("id")
    except Exception:
        pass
    return None


def log_event(level: str, message: str, context: Mapping[str, Any] | None = None) -> None:
    # 检查是否启用日志
    if not _to_bool(os.getenv("SUPABASE_LOG_ENABLED", "1"), True):
        return
    
    try:
        supabase = _get_supabase_client_direct()
        if supabase is None:
            return
        
        payload = {
            "level": str(level).lower(),
            "message": message,
            "context": json.dumps(context or {}, ensure_ascii=False) if context else None,
            "user_id": _current_user_id(),
        }
        
        # 执行插入
        result = supabase.table(TABLE_APP_LOGS).insert(payload).execute()
        
    except Exception as e:
        # 静默失败，不影响主流程
        return
