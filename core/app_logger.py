# -*- coding: utf-8 -*-
"""轻量 Supabase 日志落盘工具。

设计要点：
- 默认启用，可通过环境变量 SUPABASE_LOG_ENABLED=0 关闭。
- 同时支持文件日志（按日期轮转，最多保留7天）、控制台日志和 Supabase 日志。
- 尽量失败兜底：Supabase 不可用时静默返回，避免影响主流程。
- 上报字段：level, message, context(json), user_id。
"""
from __future__ import annotations

import json
import logging
import os
from logging.handlers import TimedRotatingFileHandler
from typing import Any, Mapping

from core.constants import TABLE_APP_LOGS


def _to_bool(v: Any, default: bool = True) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "on"}


def _setup_file_logger():
    """设置文件日志记录器（按日期轮转，最多保留7天）"""
    log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs")
    os.makedirs(log_dir, exist_ok=True)
    
    log_file = os.path.join(log_dir, "app.log")
    
    logger = logging.getLogger("app_logger")
    logger.setLevel(logging.DEBUG)
    
    if logger.handlers:
        return logger
    
    file_handler = TimedRotatingFileHandler(
        log_file,
        when="midnight",
        interval=1,
        backupCount=7,
        encoding="utf-8"
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.suffix = "%Y-%m-%d"
    
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger


_file_logger = _setup_file_logger()


def _get_supabase_client_direct():
    """直接创建 Supabase 客户端（不依赖 Streamlit session_state）"""
    from supabase import create_client
    
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    
    if not url or not key:
        try:
            from dotenv import load_dotenv
            env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
            if os.path.exists(env_path):
                load_dotenv(dotenv_path=env_path, encoding="utf-8")
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
    context_dict = dict(context) if context is not None else {}
    context_str = json.dumps(context_dict, ensure_ascii=False) if context_dict else ""
    log_message = f"{message} | context: {context_str}"
    
    level_lower = str(level).lower()
    if level_lower == "debug":
        _file_logger.debug(log_message)
    elif level_lower == "info":
        _file_logger.info(log_message)
    elif level_lower == "warning":
        _file_logger.warning(log_message)
    elif level_lower == "error":
        _file_logger.error(log_message)
    elif level_lower == "critical":
        _file_logger.critical(log_message)
    else:
        _file_logger.info(log_message)
    
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
        
        supabase.table(TABLE_APP_LOGS).insert(payload).execute()
        
    except Exception as e:
        return
