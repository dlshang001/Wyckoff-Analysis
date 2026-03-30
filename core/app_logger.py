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
from typing import Any, Mapping

from core.constants import TABLE_APP_LOGS
from integrations.supabase_client import get_supabase_client


def _to_bool(v: Any, default: bool = True) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "on"}


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
    if not _to_bool(os.getenv("SUPABASE_LOG_ENABLED", "1"), True):
        return
    try:
        supabase = get_supabase_client()
        payload = {
            "level": str(level).lower(),
            "message": message,
            "context": json.dumps(context or {}, ensure_ascii=False) if context else None,
            "user_id": _current_user_id(),
        }
        supabase.table(TABLE_APP_LOGS).insert(payload).execute()
    except Exception:
        # 避免日志失败影响业务
        return
