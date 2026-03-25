# -*- coding: utf-8 -*-
"""Wyckoff Funnel 后台筛选页。"""
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import streamlit as st

from app.background_jobs import (
    background_jobs_ready_for_current_user,
    load_latest_job_result,
    refresh_background_job_data,
    submit_background_job,
    sync_background_job_state,
)
from app.layout import setup_page
from app.navigation import show_right_nav
from utils import extract_symbols_from_text

setup_page(page_title="Wyckoff Funnel", page_icon="🔬")

TRIGGER_LABELS = {
    "sos": "SOS（量价点火）",
    "spring": "Spring（终极震仓）",
    "lps": "LPS（缩量回踩）",
    "evr": "Effort vs Result（放量不跌）",
}
STATE_KEY = "funnel_background_job"
CUSTOM_STATE_KEY = "custom_trend25_background_job"
CUSTOM_WORKFLOW = os.getenv(
    "GITHUB_ACTIONS_CUSTOM_TREND25_WORKFLOW_FILE",
    "custom_trend25_jobs.yml",
).strip() or "custom_trend25_jobs.yml"




def _parse_symbols(text: str) -> str:
    codes = extract_symbols_from_text(str(text or ""), valid_codes=None)
    deduped: list[str] = []
    seen: set[str] = set()
    for code in codes:
        code_s = str(code or "").strip()
        if not code_s or code_s in seen:
            continue
        seen.add(code_s)
        deduped.append(code_s)
    return ",".join(deduped)


def _render_job_status(state: dict | None) -> dict | None:
    if not isinstance(state, dict):
        return None
    run = state.get("run")
    result = state.get("result")
    request_id = str(state.get("request_id", "") or "").strip()
    if request_id:
        st.caption(f"请求 ID: `{request_id}`")
    if run is None:
        st.info("后台任务已提交，GitHub Actions 运行实例还在排队创建。")
        return result if isinstance(result, dict) else None

    status = f"{getattr(run, 'status', '') or '--'}"
    conclusion = f"{getattr(run, 'conclusion', '') or '--'}"
    html_url = str(getattr(run, "html_url", "") or "").strip()
    if status == "completed":
        if conclusion == "success":
            st.success("后台筛选完成。")
        else:
            st.error(f"后台任务已结束，但结论为 `{conclusion}`。")
    else:
        st.info(f"后台任务进行中：`{status}`")
    if html_url:
        st.markdown(f"[打开 GitHub Actions 运行详情]({html_url})")
    if isinstance(result, dict) and str(result.get("status", "") or "") == "error":
        st.error(str(result.get("error", "后台任务失败")))
    return result if isinstance(result, dict) else None


def _render_funnel_result(result: dict) -> None:
    summary = result.get("summary", {}) or {}
    metrics = result.get("metrics", {}) or {}
    trigger_groups = result.get("trigger_groups", {}) or {}
    symbols_for_report = result.get("symbols_for_report", []) or []

    st.subheader("漏斗结果")
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("股票池", int(summary.get("total_symbols", 0) or 0))
    col2.metric("L1", int(summary.get("layer1", 0) or 0))
    col3.metric("L2", int(summary.get("layer2", 0) or 0))
    col4.metric("L3", int(summary.get("layer3", 0) or 0))
    col5.metric("L4 命中", int(summary.get("l4_unique_hits", 0) or 0))

    top_sectors = result.get("top_sectors", []) or []
    if top_sectors:
        st.info(f"Top 行业: {', '.join(str(x) for x in top_sectors)}")

    st.caption(
        "后台版结果只回传轻量摘要与候选，不再把全量 OHLCV 明细塞进页面会话。"
    )

    st.markdown("### AI 候选池")
    if symbols_for_report:
        st.session_state["ai_find_gold_background_symbols"] = symbols_for_report
        rows = []
        for item in symbols_for_report:
            rows.append(
                {
                    "代码": str(item.get("code", "")),
                    "名称": str(item.get("name", "")),
                    "行业": str(item.get("industry", "")),
                    "轨道": str(item.get("track", "")),
                    "阶段": str(item.get("stage", "")),
                    "标签": str(item.get("tag", "")),
                    "评分": round(float(item.get("score", 0.0) or 0.0), 3),
                    "风控": str(item.get("exit_signal", "") or "-"),
                }
            )
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        st.page_link("pages/AIAnalysis.py", label="前往 AI 分析页使用这批候选", icon="🤖")
    else:
        st.caption("无 AI 候选。")

    st.markdown("### L4 触发分组")
    for key, label in TRIGGER_LABELS.items():
        rows = trigger_groups.get(key, []) or []
        st.markdown(f"**{label}**")
        if not rows:
            st.caption("无")
            continue
        table_rows = [
            {
                "代码": str(row.get("code", "")),
                "名称": str(row.get("name", "")),
                "行业": str(row.get("industry", "")),
                "评分": round(float(row.get("score", 0.0) or 0.0), 3),
            }
            for row in rows
        ]
        st.dataframe(pd.DataFrame(table_rows), use_container_width=True, hide_index=True)

    benchmark_context = result.get("benchmark_context", {}) or {}
    if benchmark_context:
        with st.expander("市场上下文"):
            st.json(benchmark_context)
    with st.expander("后台摘要 JSON"):
        st.json(
            {
                "request_id": result.get("request_id"),
                "job_kind": result.get("job_kind"),
                "metrics": metrics,
            }
        )


def _render_custom_result(result: dict) -> None:
    summary = result.get("summary", {}) or {}
    symbols = result.get("symbols_for_report", []) or []
    tuned = result.get("tuned_params", {}) or {}
    regime = result.get("regime_context", {}) or {}

    st.subheader("中期趋势策略结果")

    c1, c2, c3 = st.columns(3)
    c1.metric("股票池", int(summary.get("pool_symbols", 0) or 0))
    c2.metric("已拉取", int(summary.get("fetched_symbols", 0) or 0))
    c3.metric("入选", int(summary.get("selected_symbols", 0) or 0))

    top_sectors = summary.get("top_sectors", []) or []
    if top_sectors:
        st.info(f"行业共振 Top: {', '.join(str(x) for x in top_sectors)}")

    st.caption(
        "水温档位："
        f"benchmark={regime.get('benchmark_regime', 'UNKNOWN')} / "
        f"premarket={regime.get('premarket_regime', 'UNKNOWN')}"
    )
    st.caption(
        "动态阈值："
        f"成交额≥{float(tuned.get('min_avg_amount_5d_yuan', 0.0) or 0.0):,.0f}，"
        f"量能比≥{float(tuned.get('vol_peak_ratio', 0.0) or 0.0):.2f}"
    )

    if symbols:
        table = []
        for item in symbols:
            table.append(
                {
                    "代码": str(item.get("code", "")),
                    "名称": str(item.get("name", "")),
                    "行业": str(item.get("industry", "")),
                    "评分": round(float(item.get("score", 0.0) or 0.0), 3),
                    "60日涨幅%": round(float(item.get("ret_window_pct", 0.0) or 0.0), 2),
                    "短期涨幅%": round(float(item.get("ret_5d_pct", 0.0) or 0.0), 2),

                    "10日爆发%": round(float(item.get("burst_max_pct", 0.0) or 0.0), 2),
                    "量峰比": round(float(item.get("vol_peak_ratio", 0.0) or 0.0), 2),
                }
            )
        st.dataframe(pd.DataFrame(table), use_container_width=True, hide_index=True)
        st.session_state["ai_find_gold_background_symbols"] = symbols
        st.page_link("pages/AIAnalysis.py", label="前往 AI 分析页使用这批候选", icon="🤖")
    else:
        st.caption("无候选。")

    with st.expander("后台摘要 JSON"):
        st.json(
            {
                "request_id": result.get("request_id"),
                "job_kind": result.get("job_kind"),
                "summary": summary,
            }
        )


content_col = show_right_nav()

with content_col:
    st.title("🔬 Wyckoff Funnel")
    st.markdown("后台版筛选：保留原漏斗，并行新增中期趋势策略独立任务。")

    st.warning(
        "网页端不再本地执行全量筛选。重计算迁移到 GitHub Actions，"
        "原漏斗与新策略通过独立 workflow 隔离执行。"
    )

    tab_funnel, tab_custom = st.tabs(["4层漏斗", "中期趋势策略"])


    with tab_funnel:
        st.subheader("漏斗参数")
        f1, f2, f3 = st.columns(3)
        with f1:
            min_cap = st.number_input("最小市值(亿)", min_value=5.0, max_value=100.0, value=35.0, step=5.0, format="%.0f")
            ma_short = st.number_input("短期均线", min_value=1, max_value=100, value=50, step=1)
            ma_hold = st.number_input("守线均线", min_value=1, max_value=60, value=20, step=1)
            lps_vol_dry = st.number_input("LPS 缩量比", min_value=0.1, max_value=0.8, value=0.35, step=0.05, format="%.2f")
        with f2:
            min_amt = st.number_input("近20日均成交额阈值(万)", min_value=1000.0, max_value=20000.0, value=5000.0, step=1000.0, format="%.0f")
            ma_long = st.number_input("长期均线", min_value=1, max_value=500, value=200, step=1)
            top_n = st.number_input("Top-N 行业", min_value=1, max_value=10, value=3, step=1)
            evr_vol_ratio = st.number_input("EvR 量比阈值", min_value=1.0, max_value=5.0, value=2.0, step=0.5, format="%.1f")
        with f3:
            spring_support_w = st.number_input("Spring 支撑窗口", min_value=1, max_value=120, value=60, step=1)
            trading_days = st.number_input("交易日数量", min_value=1, max_value=1200, value=500, step=1)
            max_workers = int(st.number_input("后台并发拉取数", min_value=1, max_value=16, value=8, step=1))
            limit_count = int(st.number_input("股票数量上限", min_value=0, max_value=5000, value=500, step=100))


        st.subheader("股票池")
        pool_mode = st.radio("来源", options=["板块", "手动输入"], horizontal=True)
        board = "all"
        manual_symbols = ""
        if pool_mode == "手动输入":
            manual_symbols = st.text_area("股票代码", placeholder="例如: 600519, 000001", height=120)
        else:
            board = st.selectbox(
                "选择板块",
                options=["all", "main", "chinext"],
                format_func=lambda v: {"all": "全部主板+创业板", "main": "主板", "chinext": "创业板"}.get(v, v),
            )

        run_btn = st.button("提交后台漏斗筛选", type="primary")
        refresh_btn = st.button("刷新后台状态")

        if run_btn:
            ready, msg = background_jobs_ready_for_current_user()
            if not ready:
                st.error(msg)
                st.stop()
            payload = {
                "pool_mode": "manual" if pool_mode == "手动输入" else "board",
                "board": board,
                "manual_symbols": _parse_symbols(manual_symbols),
                "limit_count": limit_count,
                "trading_days": max(1, int(trading_days)),
                "max_workers": int(max_workers),
                "min_market_cap_yi": float(min_cap),
                "min_avg_amount_wan": float(min_amt),
                "ma_short": max(1, int(ma_short)),
                "ma_long": max(1, int(ma_long)),
                "ma_hold": max(1, int(ma_hold)),
                "top_n_sectors": int(top_n),
                "spring_support_window": max(1, int(spring_support_w)),
                "lps_vol_dry_ratio": float(lps_vol_dry),
                "evr_vol_ratio": float(evr_vol_ratio),
            }

            request_id = submit_background_job("funnel_screen", payload, state_key=STATE_KEY)
            st.success(f"后台任务已提交：`{request_id}`")

        state = sync_background_job_state(state_key=STATE_KEY)
        active_result = _render_job_status(state)

        if refresh_btn:
            refresh_background_job_data()
            st.rerun()

        if not active_result:
            latest_run, latest_result = load_latest_job_result("funnel_screen")
            if latest_result:
                st.divider()
                st.caption(
                    "以下展示当前账号最近一次成功的后台漏斗结果。"
                    + (f" Run #{latest_run.run_number}" if latest_run else "")
                )
                active_result = latest_result

        if active_result:
            _render_funnel_result(active_result)

    with tab_custom:
        st.subheader("股票池")
        p1, p2 = st.columns(2)
        with p1:
            c_only_main = st.checkbox("仅主板", value=True, key="ct_only_main")
            c_ex_chinext = st.checkbox("排除创业板", value=True, key="ct_ex_chinext")
            c_ex_star = st.checkbox("排除科创板", value=True, key="ct_ex_star")
            c_ex_bse = st.checkbox("排除北交所", value=True, key="ct_ex_bse")
        with p2:
            c_limit = st.number_input("股票池上限", min_value=1, max_value=5000, value=800, step=100, key="ct_limit")
            c_trading_days = st.number_input("交易日窗口", min_value=1, max_value=600, value=260, step=1, key="ct_days")
            c_max_workers = st.number_input("后台并发拉取数", min_value=1, max_value=24, value=8, step=1, key="ct_workers")

        st.divider()
        st.subheader("策略参数")

        st.markdown("**趋势条件**")
        g1, g2, g3 = st.columns(3)
        with g1:
            c_ma_short = st.number_input("短均线", min_value=1, max_value=30, value=10, step=1, key="ct_ma_short")
        with g2:
            c_ma_mid = st.number_input("中均线", min_value=1, max_value=60, value=25, step=1, key="ct_ma_mid")
        with g3:
            c_no_new_high = st.number_input("未创新高窗口(天)", min_value=1, max_value=120, value=20, step=1, key="ct_no_new_high")

        st.markdown("**涨幅与爆发条件**")
        r1, r2, r3 = st.columns(3)
        with r1:
            c_min_ret_window = st.number_input("中期涨幅窗口(天)", min_value=1, max_value=180, value=60, step=1, key="ct_min_ret_window")
            c_min_ret = st.number_input("中期最小涨幅%", min_value=0.0, max_value=80.0, value=15.0, step=1.0, key="ct_min_ret")
        with r2:
            c_max_ret5_window = st.number_input("短期涨幅窗口(天)", min_value=1, max_value=30, value=5, step=1, key="ct_max_ret5_window")
            c_max_ret5 = st.number_input("短期最大涨幅%", min_value=0.0, max_value=40.0, value=20.0, step=1.0, key="ct_max_ret5")
        with r3:
            c_burst_window = st.number_input("爆发观察窗口(天)", min_value=1, max_value=30, value=10, step=1, key="ct_burst_window")
            c_burst_th = st.number_input("爆发阈值%", min_value=0.0, max_value=15.0, value=6.0, step=0.5, key="ct_burst_th")

        st.markdown("**风险约束条件**")
        k1, k2 = st.columns(2)
        with k1:
            c_no_limitup_window = st.number_input("禁涨停窗口(天)", min_value=1, max_value=20, value=3, step=1, key="ct_no_limitup_window")
        with k2:
            c_limitup_pct = st.number_input("涨停判定阈值%", min_value=0.0, max_value=20.0, value=9.9, step=0.1, key="ct_limitup_pct")

        st.markdown("**量能与资金条件**")
        a1, a2, a3 = st.columns(3)
        with a1:
            c_vol_peak_window = st.number_input("量峰窗口(天)", min_value=1, max_value=30, value=10, step=1, key="ct_vol_peak_window")
            c_vol_ratio = st.number_input("量峰比阈值", min_value=0.1, max_value=5.0, value=1.5, step=0.1, key="ct_vol_ratio")
        with a2:
            c_vol_avg_window = st.number_input("量均窗口(天)", min_value=1, max_value=180, value=60, step=1, key="ct_vol_avg_window")
            c_min_amt = st.number_input("5日均成交额下限(亿)", min_value=0.0, max_value=50.0, value=5.0, step=0.5, key="ct_min_amt")
        with a3:
            c_min_mv = st.number_input("流通市值下限(亿)", min_value=0.0, max_value=1000.0, value=10.0, step=1.0, key="ct_min_mv")

        st.markdown("**增强项**")
        e1, e2 = st.columns(2)
        with e1:
            c_water = st.checkbox("启用水温自适应", value=True, key="ct_water")
        with e2:
            c_sector = st.checkbox("启用行业共振", value=True, key="ct_sector")
            c_topn = st.number_input("行业TopN", min_value=1, max_value=10, value=5, step=1, key="ct_topn")

        c_run = st.button("提交中期趋势策略后台筛选", type="primary")
        c_refresh = st.button("刷新中期趋势策略状态")

        if c_run:
            ready, msg = background_jobs_ready_for_current_user()
            if not ready:
                st.error(msg)
                st.stop()
            payload = {
                "trading_days": max(1, int(c_trading_days)),
                "only_main_board": bool(c_only_main),
                "exclude_chinext": bool(c_ex_chinext),
                "exclude_star": bool(c_ex_star),
                "exclude_bse": bool(c_ex_bse),
                "limit_count": max(1, int(c_limit)),
                "max_workers": max(1, int(c_max_workers)),
                "ma_short": max(1, int(c_ma_short)),
                "ma_mid": max(1, int(c_ma_mid)),
                "no_new_high_window": max(1, int(c_no_new_high)),
                "min_return_window": max(1, int(c_min_ret_window)),
                "min_return_pct": float(c_min_ret),
                "max_return_5d_window": max(1, int(c_max_ret5_window)),
                "max_return_5d_pct": float(c_max_ret5),
                "no_limitup_window": max(1, int(c_no_limitup_window)),
                "limitup_threshold_pct": float(c_limitup_pct),
                "burst_window": max(1, int(c_burst_window)),
                "burst_threshold_pct": float(c_burst_th),
                "vol_peak_window": max(1, int(c_vol_peak_window)),
                "vol_avg_window": max(1, int(c_vol_avg_window)),
                "vol_peak_ratio": float(c_vol_ratio),
                "min_avg_amount_5d_yuan": float(c_min_amt) * 1e8,
                "min_market_cap_yi": float(c_min_mv),
                "enable_water_adapt": bool(c_water),
                "enable_sector_resonance": bool(c_sector),
                "top_n_sectors": int(c_topn),
            }
            request_id = submit_background_job(
                "custom_trend25_screen",
                payload,
                state_key=CUSTOM_STATE_KEY,
                workflow=CUSTOM_WORKFLOW,
            )
            st.success(f"中期趋势策略任务已提交：`{request_id}`")

        c_state = sync_background_job_state(state_key=CUSTOM_STATE_KEY)
        c_result = _render_job_status(c_state)

        if c_refresh:
            refresh_background_job_data()
            st.rerun()

        if not c_result:
            c_latest_run, c_latest_result = load_latest_job_result(
                "custom_trend25_screen",
                workflow=CUSTOM_WORKFLOW,
            )
            if c_latest_result:
                st.divider()
                st.caption(
                    "以下展示当前账号最近一次成功的中期趋势策略结果。"
                    + (f" Run #{c_latest_run.run_number}" if c_latest_run else "")
                )
                c_result = c_latest_result

        if c_result:
            _render_custom_result(c_result)


