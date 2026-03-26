# -*- coding: utf-8 -*-
from __future__ import annotations

import os
from datetime import datetime
from typing import Any

import streamlit as st
from supabase import Client, create_client

from core.constants import TABLE_STRATEGY_CONFIGS
from core.custom_trend25_engine import CustomTrend25Config


def _get_supabase_base_client() -> Client:
    url = (os.getenv("SUPABASE_URL") or "").strip()
    key = (os.getenv("SUPABASE_KEY") or "").strip()
    if not url or not key:
        try:
            url = st.secrets["SUPABASE_URL"]
            key = st.secrets["SUPABASE_KEY"]
        except (FileNotFoundError, KeyError):
            pass
    if not url or not key:
        raise ValueError("SUPABASE_URL/SUPABASE_KEY 未配置")
    return create_client(url, key)


def _apply_user_session(supabase: Client) -> None:
    access_token = st.session_state.get("access_token")
    refresh_token = st.session_state.get("refresh_token")
    if access_token and refresh_token:
        try:
            supabase.auth.set_session(access_token, refresh_token)
        except Exception:
            pass
    if access_token:
        supabase.postgrest.auth(access_token)


def get_supabase_client() -> Client:
    if "supabase_client_base" not in st.session_state:
        st.session_state.supabase_client_base = _get_supabase_base_client()
    supabase = st.session_state.supabase_client_base
    _apply_user_session(supabase)
    return supabase


def is_supabase_configured() -> bool:
    url = (os.getenv("SUPABASE_URL") or "").strip()
    key = (os.getenv("SUPABASE_KEY") or "").strip()
    if not url or not key:
        try:
            url = st.secrets["SUPABASE_URL"]
            key = st.secrets["SUPABASE_KEY"]
        except (FileNotFoundError, KeyError):
            pass
    return bool(url and key)


def load_strategy_config(user_id: str, strategy_id: str = "custom_trend25") -> dict[str, Any] | None:
    user_id = str(user_id or "").strip()
    strategy_id = str(strategy_id or "custom_trend25").strip()
    if not user_id or not is_supabase_configured():
        return None
    try:
        client = get_supabase_client()
        resp = (
            client.table(TABLE_STRATEGY_CONFIGS)
            .select("*")
            .eq("user_id", user_id)
            .eq("strategy_id", strategy_id)
            .limit(1)
            .execute()
        )
        if not resp.data:
            return None
        row = resp.data[0] or {}
        if not isinstance(row, dict):
            return None
        return row
    except Exception as e:
        print(f"[supabase_strategy_config] load_strategy_config failed: {e}")
        return None


def save_strategy_config(user_id: str, strategy_id: str, config: dict[str, Any]) -> bool:
    user_id = str(user_id or "").strip()
    strategy_id = str(strategy_id or "custom_trend25").strip()
    if not user_id or not strategy_id or not is_supabase_configured():
        return False
    try:
        client = get_supabase_client()
        payload = {
            "user_id": user_id,
            "strategy_id": strategy_id,
            **config,
            "updated_at": datetime.utcnow().isoformat(),
        }
        client.table(TABLE_STRATEGY_CONFIGS).upsert(
            payload,
            on_conflict="user_id,strategy_id",
        ).execute()
        return True
    except Exception as e:
        print(f"[supabase_strategy_config] save_strategy_config failed: {e}")
        return False


def get_strategy_config_with_defaults(user_id: str, strategy_id: str = "custom_trend25") -> dict[str, Any]:
    defaults = CustomTrend25Config()
    default_dict = {
        "trading_days": defaults.trading_days,
        "only_main_board": defaults.only_main_board,
        "exclude_chinext": defaults.exclude_chinext,
        "exclude_star": defaults.exclude_star,
        "exclude_bse": defaults.exclude_bse,
        "limit_count": defaults.limit_count,
        "max_workers": defaults.max_workers,
        "ma_short": defaults.ma_short,
        "ma_mid": defaults.ma_mid,
        "no_new_high_window": defaults.no_new_high_window,
        "min_return_window": defaults.min_return_window,
        "min_return_pct": defaults.min_return_pct,
        "max_return_5d_window": defaults.max_return_5d_window,
        "max_return_5d_pct": defaults.max_return_5d_pct,
        "no_limitup_window": defaults.no_limitup_window,
        "limitup_threshold_pct": defaults.limitup_threshold_pct,
        "burst_window": defaults.burst_window,
        "burst_threshold_pct": defaults.burst_threshold_pct,
        "vol_peak_window": defaults.vol_peak_window,
        "vol_avg_window": defaults.vol_avg_window,
        "vol_peak_ratio": defaults.vol_peak_ratio,
        "min_avg_amount_5d_yuan": defaults.min_avg_amount_5d_yuan,
        "min_market_cap_yi": defaults.min_market_cap_yi,
        "enable_water_adapt": defaults.enable_water_adapt,
        "enable_sector_resonance": defaults.enable_sector_resonance,
        "top_n_sectors": defaults.top_n_sectors,
    }
    saved = load_strategy_config(user_id, strategy_id)
    if not saved:
        return default_dict
    for key in default_dict:
        if key in saved and saved[key] is not None:
            default_dict[key] = saved[key]
    return default_dict
