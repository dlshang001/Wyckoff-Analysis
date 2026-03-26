import streamlit as st
import os
import sys

# Add parent directory to path to import modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.layout import setup_page
from app.navigation import show_right_nav
from integrations.supabase_client import save_user_settings, save_strategy_config, TREND25_DEFAULT_CONFIG
from integrations.llm_client import OPENAI_COMPATIBLE_BASE_URLS
from app.ui_helpers import show_page_loading

setup_page(page_title="设置", page_icon="⚙️")

# Show Navigation
content_col = show_right_nav()
with content_col:

    st.title("⚙️ 设置 (Settings)")
    st.markdown("配置您的 API Key 和通知服务，让 Akshare 更加智能。")

    # 获取当前用户 ID
    user = st.session_state.get("user") or {}
    user_id = user.get("id") if isinstance(user, dict) else None
    if not user_id:
        st.error("无法识别当前用户，设置页已拒绝展示。请重新登录。")
        st.stop()

    # 兼容旧会话：新增字段可能尚未初始化，先补默认值，避免 AttributeError。
    st.session_state.setdefault("openai_base_url", OPENAI_COMPATIBLE_BASE_URLS.get("openai", ""))
    st.session_state.setdefault("zhipu_base_url", OPENAI_COMPATIBLE_BASE_URLS.get("zhipu", ""))
    st.session_state.setdefault("minimax_base_url", OPENAI_COMPATIBLE_BASE_URLS.get("minimax", ""))
    st.session_state.setdefault("deepseek_base_url", OPENAI_COMPATIBLE_BASE_URLS.get("deepseek", ""))
    st.session_state.setdefault("qwen_base_url", OPENAI_COMPATIBLE_BASE_URLS.get("qwen", ""))

    # 顶部展示 user_id，方便复制
    with st.expander("🔑 账户信息", expanded=True):
        st.info(f"当前用户 ID (SUPABASE_USER_ID): `{user_id}`")
        st.caption("请复制此 ID 并配置到 GitHub Secrets 的 SUPABASE_USER_ID 中，以便定时任务能识别您的账户。")


    def on_save_settings():
        """保存配置到云端"""
        if not user_id:
            st.error("用户未登录，无法保存配置")
            return

        settings = {
            # 通知
            "feishu_webhook": st.session_state.feishu_webhook,
            "wecom_webhook": st.session_state.wecom_webhook,
            "dingtalk_webhook": st.session_state.dingtalk_webhook,
            # 大模型
            "gemini_api_key": st.session_state.gemini_api_key,
            "gemini_model": st.session_state.gemini_model,
            "openai_api_key": st.session_state.openai_api_key,
            "openai_model": st.session_state.openai_model,
            "openai_base_url": st.session_state.openai_base_url,
            "zhipu_api_key": st.session_state.zhipu_api_key,
            "zhipu_model": st.session_state.zhipu_model,
            "zhipu_base_url": st.session_state.zhipu_base_url,
            "minimax_api_key": st.session_state.minimax_api_key,
            "minimax_model": st.session_state.minimax_model,
            "minimax_base_url": st.session_state.minimax_base_url,
            "deepseek_api_key": st.session_state.deepseek_api_key,
            "deepseek_model": st.session_state.deepseek_model,
            "deepseek_base_url": st.session_state.deepseek_base_url,
            "qwen_api_key": st.session_state.qwen_api_key,
            "qwen_model": st.session_state.qwen_model,
            "qwen_base_url": st.session_state.qwen_base_url,
            # 其它
            "tushare_token": st.session_state.tushare_token,
            "tg_bot_token": st.session_state.tg_bot_token,
            "tg_chat_id": st.session_state.tg_chat_id,
        }

        loading = show_page_loading(title="加载中...", subtitle="正在保存到云端")
        try:
            if save_user_settings(user_id, settings):
                st.toast("✅ 配置已保存到云端", icon="☁️")
            else:
                st.toast("❌ 保存失败，请检查网络", icon="⚠️")
        finally:
            loading.empty()


    col1, col2 = st.columns([2, 1])

    with col1:
        # 1. 通知配置：飞书 / 企微 / 钉钉
        st.subheader("🔔 通知配置")
        with st.container(border=True):
            st.markdown(
                "配置群机器人的 **Webhook**，定时任务与批量操作完成后可自动推送到对应群。"
            )

            new_feishu_webhook = st.text_input(
                "飞书 Webhook URL",
                value=st.session_state.feishu_webhook,
                type="password",
                placeholder="https://open.feishu.cn/open-apis/bot/v2/hook/...",
                help="飞书自定义机器人 Webhook，详见飞书官方文档。",
            )

            new_wecom_webhook = st.text_input(
                "企业微信 Webhook URL",
                value=st.session_state.wecom_webhook,
                type="password",
                placeholder="https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=...",
                help="企业微信群机器人 Webhook，可选。",
            )

            new_dingtalk_webhook = st.text_input(
                "钉钉 Webhook URL",
                value=st.session_state.dingtalk_webhook,
                type="password",
                placeholder="https://oapi.dingtalk.com/robot/send?access_token=...",
                help="钉钉群机器人 Webhook，可选。",
            )

            if st.button("💾 保存通知配置", key="save_webhook"):
                st.session_state.feishu_webhook = new_feishu_webhook
                st.session_state.wecom_webhook = new_wecom_webhook
                st.session_state.dingtalk_webhook = new_dingtalk_webhook
                on_save_settings()

        st.divider()

        # 2. 大模型配置：Gemini / OpenAI / 智谱 / Minimax / DeepSeek / Qwen
        st.subheader("🧠 AI 配置")
        with st.container(border=True):
            st.markdown("配置各家大模型的 API Key 与默认模型，后续在任务/研报中按需切换使用。")

            st.markdown("**Gemini (Google)**")
            new_gemini_key = st.text_input(
                "Gemini API Key",
                value=st.session_state.gemini_api_key,
                type="password",
                placeholder="AIzaSy...",
                help="获取 Key: Google AI Studio。",
            )
            new_gemini_model = st.text_input(
                "Gemini 默认模型",
                value=st.session_state.gemini_model,
                placeholder="gemini-3.1-flash-lite-preview",
                help="例如：gemini-3.1-flash-lite-preview、gemini-2.5-flash 等。",
            )

            st.markdown("---")
            st.markdown("**OpenAI / 兼容 OpenAI 协议的厂商**")
            new_openai_key = st.text_input(
                "OpenAI API Key",
                value=st.session_state.openai_api_key,
                type="password",
                placeholder="sk-...",
            )
            new_openai_model = st.text_input(
                "OpenAI 默认模型",
                value=st.session_state.openai_model,
                placeholder="gpt-4.1-mini",
            )
            new_openai_base_url = st.text_input(
                "OpenAI Base URL",
                value=st.session_state.openai_base_url,
                placeholder="https://api.openai.com/v1",
                help="支持自定义网关地址；当前值会作为优先地址，未配置时回退到系统默认值。",
            )

            st.markdown("---")
            st.markdown("**智谱 AI (GLM)**")
            new_zhipu_key = st.text_input(
                "智谱 API Key",
                value=st.session_state.zhipu_api_key,
                type="password",
                placeholder="xxxxx",
            )
            new_zhipu_model = st.text_input(
                "智谱默认模型",
                value=st.session_state.zhipu_model,
                placeholder="glm-4-air",
            )
            new_zhipu_base_url = st.text_input(
                "智谱 Base URL",
                value=st.session_state.zhipu_base_url,
                placeholder="https://open.bigmodel.cn/api/paas/v4",
            )

            st.markdown("---")
            st.markdown("**Minimax**")
            new_minimax_key = st.text_input(
                "Minimax API Key",
                value=st.session_state.minimax_api_key,
                type="password",
                placeholder="xxxxx",
            )
            new_minimax_model = st.text_input(
                "Minimax 默认模型",
                value=st.session_state.minimax_model,
                placeholder="abab6.5-chat",
            )
            new_minimax_base_url = st.text_input(
                "Minimax Base URL",
                value=st.session_state.minimax_base_url,
                placeholder="https://api.minimax.chat/v1",
            )

            st.markdown("---")
            st.markdown("**DeepSeek**")
            new_deepseek_key = st.text_input(
                "DeepSeek API Key",
                value=st.session_state.deepseek_api_key,
                type="password",
                placeholder="sk-...",
            )
            new_deepseek_model = st.text_input(
                "DeepSeek 默认模型",
                value=st.session_state.deepseek_model,
                placeholder="deepseek-chat",
            )
            new_deepseek_base_url = st.text_input(
                "DeepSeek Base URL",
                value=st.session_state.deepseek_base_url,
                placeholder="https://api.deepseek.com/v1",
            )

            st.markdown("---")
            st.markdown("**Qwen (通义千问)**")
            new_qwen_key = st.text_input(
                "Qwen API Key",
                value=st.session_state.qwen_api_key,
                type="password",
                placeholder="sk-...",
            )
            new_qwen_model = st.text_input(
                "Qwen 默认模型",
                value=st.session_state.qwen_model,
                placeholder="qwen-max",
            )
            new_qwen_base_url = st.text_input(
                "Qwen Base URL",
                value=st.session_state.qwen_base_url,
                placeholder="https://dashscope.aliyuncs.com/compatible-mode/v1",
            )

            if st.button("💾 保存 AI 配置", key="save_ai"):
                st.session_state.gemini_api_key = new_gemini_key
                st.session_state.gemini_model = new_gemini_model
                st.session_state.openai_api_key = new_openai_key
                st.session_state.openai_model = new_openai_model
                st.session_state.openai_base_url = new_openai_base_url
                st.session_state.zhipu_api_key = new_zhipu_key
                st.session_state.zhipu_model = new_zhipu_model
                st.session_state.zhipu_base_url = new_zhipu_base_url
                st.session_state.minimax_api_key = new_minimax_key
                st.session_state.minimax_model = new_minimax_model
                st.session_state.minimax_base_url = new_minimax_base_url
                st.session_state.deepseek_api_key = new_deepseek_key
                st.session_state.deepseek_model = new_deepseek_model
                st.session_state.deepseek_base_url = new_deepseek_base_url
                st.session_state.qwen_api_key = new_qwen_key
                st.session_state.qwen_model = new_qwen_model
                st.session_state.qwen_base_url = new_qwen_base_url
                on_save_settings()

        st.divider()

        # 3. 数据源
        st.subheader("📊 数据源配置")
        with st.container(border=True):
            st.markdown("**Tushare Token**（可选）用于行情、市值等。不配置时优先用 akshare/baostock/efinance，三者均失败时才需 Tushare。")
            new_tushare = st.text_input(
                "Tushare Token",
                value=st.session_state.tushare_token,
                type="password",
                placeholder="Tushare Pro token",
                key="tushare_input",
            )
            if st.button("💾 保存数据源配置", key="save_tushare"):
                st.session_state.tushare_token = new_tushare
                on_save_settings()

        st.divider()

        # 4. 私人决断
        st.subheader("🕶️ 私人决断")
        with st.container(border=True):
            st.markdown("可选，用于 Telegram 私密推送买卖建议。")
            new_tg_bot = st.text_input("Telegram Bot Token", value=st.session_state.tg_bot_token, type="password", key="tg_bot")
            new_tg_chat = st.text_input("Telegram Chat ID", value=st.session_state.tg_chat_id, type="password", key="tg_chat")
            if st.button("💾 保存 Step4 配置", key="save_step4"):
                st.session_state.tg_bot_token = new_tg_bot
                st.session_state.tg_chat_id = new_tg_chat
                on_save_settings()

        st.divider()

        # 5. 中期趋势策略参数
        st.subheader("📈 中期趋势策略参数")
        with st.container(border=True):
            st.markdown("配置定时任务使用的中期趋势策略参数。修改后点击保存即可生效。")

            default_cfg = TREND25_DEFAULT_CONFIG
            st.session_state.setdefault("trend25_only_main_board", default_cfg["only_main_board"])
            st.session_state.setdefault("trend25_exclude_chinext", default_cfg["exclude_chinext"])
            st.session_state.setdefault("trend25_exclude_star", default_cfg["exclude_star"])
            st.session_state.setdefault("trend25_exclude_bse", default_cfg["exclude_bse"])
            st.session_state.setdefault("trend25_limit_count", default_cfg["limit_count"])
            st.session_state.setdefault("trend25_ma_short", default_cfg["ma_short"])
            st.session_state.setdefault("trend25_ma_mid", default_cfg["ma_mid"])
            st.session_state.setdefault("trend25_min_return_pct", default_cfg["min_return_pct"])
            st.session_state.setdefault("trend25_max_return_5d_pct", default_cfg["max_return_5d_pct"])
            st.session_state.setdefault("trend25_burst_window", default_cfg["burst_window"])
            st.session_state.setdefault("trend25_burst_threshold_pct", default_cfg["burst_threshold_pct"])
            st.session_state.setdefault("trend25_vol_peak_ratio", default_cfg["vol_peak_ratio"])
            st.session_state.setdefault("trend25_min_avg_amount_5d_yuan", default_cfg["min_avg_amount_5d_yuan"])
            st.session_state.setdefault("trend25_min_market_cap_yi", default_cfg["min_market_cap_yi"])
            st.session_state.setdefault("trend25_enable_water_adapt", default_cfg["enable_water_adapt"])
            st.session_state.setdefault("trend25_enable_sector_resonance", default_cfg["enable_sector_resonance"])

            st.markdown("**股票池设置**")
            c1, c2, c3, c4 = st.columns(4)
            with c1:
                new_only_main = st.checkbox("仅主板", value=st.session_state.trend25_only_main_board, key="trend25_only_main")
            with c2:
                new_exclude_chinext = st.checkbox("排除创业板", value=st.session_state.trend25_exclude_chinext, key="trend25_exclude_chinext_input")
            with c3:
                new_exclude_star = st.checkbox("排除科创板", value=st.session_state.trend25_exclude_star, key="trend25_exclude_star_input")
            with c4:
                new_exclude_bse = st.checkbox("排除北交所", value=st.session_state.trend25_exclude_bse, key="trend25_exclude_bse_input")

            new_limit_count = st.number_input(
                "股票池上限",
                min_value=100,
                max_value=5000,
                value=st.session_state.trend25_limit_count,
                step=100,
                help="初始股票池的最大数量",
                key="trend25_limit_count_input",
            )

            st.markdown("**趋势条件**")
            c1, c2 = st.columns(2)
            with c1:
                new_ma_short = st.number_input("短均线周期", min_value=5, max_value=30, value=st.session_state.trend25_ma_short, key="trend25_ma_short_input")
            with c2:
                new_ma_mid = st.number_input("中均线周期", min_value=10, max_value=60, value=st.session_state.trend25_ma_mid, key="trend25_ma_mid_input")

            st.markdown("**涨幅条件**")
            c1, c2 = st.columns(2)
            with c1:
                new_min_return_pct = st.number_input(
                    "中期最小涨幅 (%)",
                    min_value=0.0,
                    max_value=50.0,
                    value=st.session_state.trend25_min_return_pct,
                    step=1.0,
                    help="60日涨幅下限",
                    key="trend25_min_return_pct_input",
                )
            with c2:
                new_max_return_5d_pct = st.number_input(
                    "短期最大涨幅 (%)",
                    min_value=5.0,
                    max_value=50.0,
                    value=st.session_state.trend25_max_return_5d_pct,
                    step=1.0,
                    help="5日涨幅上限，避免追高",
                    key="trend25_max_return_5d_pct_input",
                )

            st.markdown("**爆发条件**")
            c1, c2 = st.columns(2)
            with c1:
                new_burst_window = st.number_input("爆发观察窗口 (天)", min_value=5, max_value=30, value=st.session_state.trend25_burst_window, key="trend25_burst_window_input")
            with c2:
                new_burst_threshold_pct = st.number_input(
                    "爆发阈值 (%)",
                    min_value=3.0,
                    max_value=15.0,
                    value=st.session_state.trend25_burst_threshold_pct,
                    step=0.5,
                    help="单日涨幅超过此值视为爆发",
                    key="trend25_burst_threshold_pct_input",
                )

            st.markdown("**量能条件**")
            c1, c2 = st.columns(2)
            with c1:
                new_vol_peak_ratio = st.number_input(
                    "量峰比阈值",
                    min_value=1.0,
                    max_value=5.0,
                    value=st.session_state.trend25_vol_peak_ratio,
                    step=0.1,
                    help="当日成交量 / 60日均量",
                    key="trend25_vol_peak_ratio_input",
                )
            with c2:
                new_min_avg_amount = st.number_input(
                    "5日均成交额下限 (亿)",
                    min_value=1.0,
                    max_value=50.0,
                    value=st.session_state.trend25_min_avg_amount_5d_yuan / 1e8,
                    step=1.0,
                    help="流动性筛选",
                    key="trend25_min_avg_amount_input",
                )

            new_min_market_cap = st.number_input(
                "流通市值下限 (亿)",
                min_value=5.0,
                max_value=100.0,
                value=st.session_state.trend25_min_market_cap_yi,
                step=5.0,
                help="市值筛选",
                key="trend25_min_market_cap_input",
            )

            st.markdown("**增强项**")
            c1, c2 = st.columns(2)
            with c1:
                new_water_adapt = st.checkbox("启用水温自适应", value=st.session_state.trend25_enable_water_adapt, key="trend25_water_adapt_input")
            with c2:
                new_sector_resonance = st.checkbox("启用行业共振", value=st.session_state.trend25_enable_sector_resonance, key="trend25_sector_resonance_input")

            if st.button("💾 保存策略参数", key="save_trend25"):
                st.session_state.trend25_only_main_board = new_only_main
                st.session_state.trend25_exclude_chinext = new_exclude_chinext
                st.session_state.trend25_exclude_star = new_exclude_star
                st.session_state.trend25_exclude_bse = new_exclude_bse
                st.session_state.trend25_limit_count = new_limit_count
                st.session_state.trend25_ma_short = new_ma_short
                st.session_state.trend25_ma_mid = new_ma_mid
                st.session_state.trend25_min_return_pct = new_min_return_pct
                st.session_state.trend25_max_return_5d_pct = new_max_return_5d_pct
                st.session_state.trend25_burst_window = new_burst_window
                st.session_state.trend25_burst_threshold_pct = new_burst_threshold_pct
                st.session_state.trend25_vol_peak_ratio = new_vol_peak_ratio
                st.session_state.trend25_min_avg_amount_5d_yuan = new_min_avg_amount * 1e8
                st.session_state.trend25_min_market_cap_yi = new_min_market_cap
                st.session_state.trend25_enable_water_adapt = new_water_adapt
                st.session_state.trend25_enable_sector_resonance = new_sector_resonance

                config = {
                    "only_main_board": new_only_main,
                    "exclude_chinext": new_exclude_chinext,
                    "exclude_star": new_exclude_star,
                    "exclude_bse": new_exclude_bse,
                    "limit_count": new_limit_count,
                    "ma_short": new_ma_short,
                    "ma_mid": new_ma_mid,
                    "min_return_pct": new_min_return_pct,
                    "max_return_5d_pct": new_max_return_5d_pct,
                    "burst_window": new_burst_window,
                    "burst_threshold_pct": new_burst_threshold_pct,
                    "vol_peak_ratio": new_vol_peak_ratio,
                    "min_avg_amount_5d_yuan": new_min_avg_amount * 1e8,
                    "min_market_cap_yi": new_min_market_cap,
                    "enable_water_adapt": new_water_adapt,
                    "enable_sector_resonance": new_sector_resonance,
                }
                loading = show_page_loading(title="保存中...", subtitle="正在保存策略参数")
                try:
                    if save_strategy_config(user_id, "custom_trend25", config):
                        st.toast("✅ 策略参数已保存到云端", icon="☁️")
                    else:
                        st.toast("❌ 保存失败，请检查网络", icon="⚠️")
                finally:
                    loading.empty()

        st.info("☁️ 您的配置已启用云端同步，将在所有登录设备间自动漫游。")
