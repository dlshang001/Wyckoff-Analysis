# -*- coding: utf-8 -*-
"""
中期趋势策略定时任务：Custom Trend25 → 批量研报 → 私人账户再平衡

与 daily_job.py 类似，但阶段1使用中期趋势策略替代 Wyckoff Funnel。
阶段2（批量研报）和阶段3（私人账户再平衡）完全复用。
"""
from __future__ import annotations

import argparse
import os
import sys
from contextlib import redirect_stderr, redirect_stdout
from datetime import date, datetime
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.custom_trend25_engine import run_custom_trend25
from integrations.fetch_a_share_csv import _resolve_trading_window
from integrations.llm_client import OPENAI_COMPATIBLE_BASE_URLS
from integrations.supabase_market_signal import upsert_market_signal_daily
from integrations.supabase_recommendation import (
    mark_ai_recommendations,
    sync_all_tracking_prices,
    upsert_recommendations,
)
from utils.notify import send_all_webhooks
from utils.trading_clock import resolve_end_calendar_day

TZ = ZoneInfo("Asia/Shanghai")
STEP3_REASON_MAP = {
    "data_all_failed": "OHLCV 全部拉取失败",
    "llm_failed": "大模型调用失败",
    "feishu_failed": "飞书推送失败",
    "skipped_no_symbols": "无输入股票，已跳过",
    "no_data_but_no_error": "无可用数据",
    "ok_preview": "预演模式：未调用模型，仅展示输入",
}
STEP4_REASON_MAP = {
    "missing_api_key": "GEMINI_API_KEY 缺失",
    "skipped_invalid_portfolio": "用户持仓缺失或格式错误，已跳过",
    "skipped_telegram_unconfigured": "Telegram 未配置，已跳过",
    "skipped_idempotency": "今日已运行，已跳过",
    "skipped_no_decisions": "模型未给出有效决策，已跳过",
    "llm_failed": "Step4 模型调用失败",
    "telegram_failed": "Telegram 推送失败",
    "ok": "ok",
}


def _now() -> str:
    return datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")


def _log(msg: str, logs_path: str | None = None) -> None:
    line = f"[{_now()}] {msg}"
    print(line, flush=True)
    if logs_path:
        os.makedirs(os.path.dirname(logs_path) or ".", exist_ok=True)
        with open(logs_path, "a", encoding="utf-8") as f:
            f.write(line + "\n")


class _TeeStream:
    def __init__(self, console_stream, file_stream):
        self.console_stream = console_stream
        self.file_stream = file_stream

    def write(self, data: str) -> int:
        self.console_stream.write(data)
        self.file_stream.write(data)
        return len(data)

    def flush(self) -> None:
        self.console_stream.flush()
        self.file_stream.flush()


def _run_with_stdout_tee(logs_path: str | None, fn, *args, **kwargs):
    if not logs_path:
        return fn(*args, **kwargs)
    os.makedirs(os.path.dirname(logs_path) or ".", exist_ok=True)
    with open(logs_path, "a", encoding="utf-8") as log_file:
        tee = _TeeStream(sys.stdout, log_file)
        with redirect_stdout(tee), redirect_stderr(tee):
            return fn(*args, **kwargs)


def _latest_trade_date_str() -> str:
    window = _resolve_trading_window(
        end_calendar_day=resolve_end_calendar_day(),
        trading_days=30,
    )
    return window.end_trade_date.isoformat()


def _persist_regime_context(regime_context: dict, logs_path: str | None = None) -> None:
    if not regime_context:
        return
    trade_date = _latest_trade_date_str()
    payload = {
        "benchmark_regime": str(regime_context.get("benchmark_regime", "") or "").strip().upper() or None,
        "source_jobs": {
            "daily_job_trend25": {
                "updated_at": datetime.now(TZ).isoformat(),
                "writer": "custom_trend25_regime",
            }
        },
    }
    ok = upsert_market_signal_daily(trade_date, payload)
    _log(
        f"市场信号写库(regime): ok={ok}, trade_date={trade_date}, regime={payload.get('benchmark_regime')}",
        logs_path,
    )


def _load_step4_target() -> tuple[dict | None, str]:
    target_user_id = os.getenv("SUPABASE_USER_ID", "").strip()
    if not target_user_id:
        return None, "SUPABASE_USER_ID 未配置"

    portfolio_id = f"USER_LIVE:{target_user_id}"
    try:
        from integrations.supabase_portfolio import load_portfolio_state
    except Exception as e:
        return None, f"supabase portfolio 读取器不可用: {e}"

    p = load_portfolio_state(portfolio_id)
    has_env_fallback = bool(os.getenv("MY_PORTFOLIO_STATE", "").strip())
    if not isinstance(p, dict) and not has_env_fallback:
        return None, f"未匹配到 user_id={target_user_id} 的持仓（{portfolio_id}）"

    return {
        "user_id": target_user_id,
        "portfolio_id": portfolio_id,
    }, ("ok_supabase" if isinstance(p, dict) else "ok_env_fallback")


def _build_notify_content(result: dict) -> str:
    summary = result.get("summary", {}) or {}
    symbols = result.get("symbols_for_report", []) or []
    tuned = result.get("tuned_params", {}) or {}
    regime = result.get("regime_context", {}) or {}

    lines = [
        "## 中期趋势策略结果",
        f"- **股票池**: {int(summary.get('pool_symbols', 0) or 0)} 只",
        f"- **已拉取**: {int(summary.get('fetched_symbols', 0) or 0)} 只",
        f"- **入选**: {int(summary.get('selected_symbols', 0) or 0)} 只",
        f"- **Top 行业**: {', '.join(str(x) for x in (summary.get('top_sectors', []) or [])) or '无'}",
        "",
        "## 水温与阈值",
        f"- **水温档位**: benchmark={regime.get('benchmark_regime', 'UNKNOWN')} / premarket={regime.get('premarket_regime', 'UNKNOWN')}",
        f"- **动态阈值**: 成交额≥{float(tuned.get('min_avg_amount_5d_yuan', 0.0) or 0.0):,.0f}，量能比≥{float(tuned.get('vol_peak_ratio', 0.0) or 0.0):.2f}",
        "",
        "## 入选股票",
    ]
    if symbols:
        for item in symbols[:30]:
            code = str(item.get("code", ""))
            name = str(item.get("name", code))
            industry = str(item.get("industry", ""))
            score = float(item.get("score", 0.0) or 0.0)
            ret60 = float(item.get("ret_window_pct", 0.0) or 0.0)
            ret5 = float(item.get("ret_5d_pct", 0.0) or 0.0)
            lines.append(f"- {code} {name} | {industry} | score={score:.2f} | 60日={ret60:+.1f}% | 5日={ret5:+.1f}%")
        if len(symbols) > 30:
            lines.append(f"- ... 共 {len(symbols)} 只")
    else:
        lines.append("- 无候选")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="每日定时任务：中期趋势策略 → 批量研报")
    parser.add_argument("--dry-run", action="store_true", help="仅校验配置，不执行任务")
    parser.add_argument("--logs", default=None, help="日志文件路径")
    args = parser.parse_args()

    webhook = os.getenv("FEISHU_WEBHOOK_URL", "").strip()
    wecom_webhook = os.getenv("WECOM_WEBHOOK_URL", "").strip()
    dingtalk_webhook = os.getenv("DINGTALK_WEBHOOK_URL", "").strip()
    provider = os.getenv("DEFAULT_LLM_PROVIDER", "gemini").strip().lower() or "gemini"
    api_key = (os.getenv(f"{provider.upper()}_API_KEY") or os.getenv("GEMINI_API_KEY") or "").strip()
    model_env_key = f"{provider.upper()}_MODEL"
    model = (os.getenv(model_env_key) or os.getenv("GEMINI_MODEL", "gemini-3.1-flash-lite-preview")).strip() or "gemini-3.1-flash-lite-preview"
    base_url_env_key = f"{provider.upper()}_BASE_URL"
    llm_base_url = (
        os.getenv(base_url_env_key)
        or OPENAI_COMPATIBLE_BASE_URLS.get(provider, "")
        or ""
    ).strip()
    step3_skip_llm = os.getenv("STEP3_SKIP_LLM", "").strip().lower() in {"1", "true", "yes", "on"}
    skip_step4 = os.getenv("DAILY_JOB_SKIP_STEP4", "").strip().lower() in {"1", "true", "yes", "on"}

    logs_path = args.logs or os.path.join(
        os.getenv("LOGS_DIR", "logs"),
        f"daily_job_trend25_{datetime.now(TZ).strftime('%Y%m%d_%H%M%S')}.log",
    )

    missing = []
    if not webhook:
        missing.append("FEISHU_WEBHOOK_URL")
    require_api_key = (not step3_skip_llm) or (not skip_step4)
    if require_api_key and not api_key:
        missing.append(f"{provider.upper()}_API_KEY 或 GEMINI_API_KEY")
    if missing:
        _log(f"配置缺失: {', '.join(missing)}", logs_path)
        return 1

    if args.dry_run:
        _log("--dry-run: 配置校验通过，退出", logs_path)
        return 0

    if provider in OPENAI_COMPATIBLE_BASE_URLS:
        _log(f"LLM base_url: {llm_base_url or '(empty)'} (env={base_url_env_key})", logs_path)

    from scripts.step3_batch_report import (
        extract_operation_pool_codes,
        run as run_step3,
    )
    from scripts.step4_rebalancer import run as run_step4
    from integrations.supabase_client import load_user_trend25_config

    summary: list[dict] = []
    has_blocking_failure = False
    symbols_info: list[dict] = []
    regime_context: dict = {}
    step3_report_text = ""
    recommend_trade_date_int: int | None = None

    _log("开始中期趋势策略定时任务", logs_path)

    target_user_id = os.getenv("SUPABASE_USER_ID", "").strip()
    trend25_config = load_user_trend25_config(target_user_id)
    _log(f"加载用户策略配置: user_id={target_user_id}, limit_count={trend25_config.get('limit_count')}, ma_short={trend25_config.get('ma_short')}, ma_mid={trend25_config.get('ma_mid')}", logs_path)

    t0 = datetime.now(TZ)
    step1_ok = False
    step1_err = None
    try:
        result = run_custom_trend25(payload=trend25_config)
        step1_ok = bool(result.get("ok", False))
        if step1_ok:
            symbols_info = result.get("symbols_for_report", []) or []
            regime_context = result.get("regime_context", {}) or {}
            tuned = result.get("tuned_params", {}) or {}
            summary_data = result.get("summary", {}) or {}

            notify_title = f"📈 中期趋势策略 {date.today().strftime('%Y-%m-%d')}"
            notify_content = _build_notify_content(result)
            send_all_webhooks(webhook, wecom_webhook, dingtalk_webhook, notify_title, notify_content)
            _log(f"阶段 1 中期趋势策略: 已推送通知", logs_path)
        else:
            step1_err = "策略执行失败"
    except Exception as e:
        step1_ok = False
        step1_err = str(e)
    elapsed1 = (datetime.now(TZ) - t0).total_seconds()
    summary.append({
        "step": "中期趋势策略",
        "ok": step1_ok and step1_err is None,
        "err": step1_err,
        "elapsed_s": round(elapsed1, 1),
        "output": f"{len(symbols_info)} symbols",
    })
    _log(f"阶段 1 中期趋势策略: ok={step1_ok}, symbols={len(symbols_info)}, elapsed={elapsed1:.1f}s, err={step1_err}", logs_path)
    if step1_err:
        has_blocking_failure = True
    elif regime_context:
        _persist_regime_context(regime_context, logs_path)

    if step1_ok and symbols_info:
        try:
            recommend_trade_date_int = int(_latest_trade_date_str().replace("-", ""))
            rec_ok = upsert_recommendations(recommend_trade_date_int, symbols_info)
            _log(
                f"推荐记录入库: ok={rec_ok}, count={len(symbols_info)}, date={recommend_trade_date_int}",
                logs_path,
            )
        except Exception as e:
            _log(f"推荐记录入库失败: {e}", logs_path)

    step3_ok = True
    step3_err = None
    step3_operation_codes: list[str] = []
    if symbols_info:
        t0 = datetime.now(TZ)
        try:
            benchmark_context = {"regime": regime_context.get("benchmark_regime", "NEUTRAL")}
            step3_ok, step3_reason, step3_report_text = _run_with_stdout_tee(
                logs_path,
                run_step3,
                symbols_info,
                webhook,
                api_key,
                model,
                benchmark_context=benchmark_context,
                provider=provider,
                llm_base_url=llm_base_url,
                wecom_webhook=wecom_webhook,
                dingtalk_webhook=dingtalk_webhook,
            )
            step3_err = None if step3_ok else STEP3_REASON_MAP.get(step3_reason, step3_reason)
        except Exception as e:
            step3_ok = False
            step3_err = str(e)
        if step3_ok and step3_report_text:
            allowed_codes = [
                str(item.get("code", "")).strip()
                for item in symbols_info
                if isinstance(item, dict)
            ]
            try:
                step3_operation_codes = extract_operation_pool_codes(
                    report=step3_report_text,
                    allowed_codes=allowed_codes,
                )
            except Exception as e:
                step3_operation_codes = []
                _log(f"阶段 2 批量研报: 可操作池解析失败，已降级为空。err={e}", logs_path)
        elapsed3 = (datetime.now(TZ) - t0).total_seconds()
        summary.append({
            "step": "批量研报",
            "ok": step3_ok and step3_err is None,
            "err": step3_err,
            "elapsed_s": round(elapsed3, 1),
            "output": f"{len(symbols_info)} symbols",
        })
        _log(f"阶段 2 批量研报: ok={step3_ok}, elapsed={elapsed3:.1f}s, err={step3_err}", logs_path)
        preview_codes = ", ".join(step3_operation_codes[:8]) if step3_operation_codes else "无"
        _log(
            f"阶段 2 批量研报: 可操作池代码={len(step3_operation_codes)} ({preview_codes})",
            logs_path,
        )
        if recommend_trade_date_int is not None:
            try:
                ai_mark_ok = mark_ai_recommendations(
                    recommend_date=recommend_trade_date_int,
                    ai_codes=step3_operation_codes,
                )
                _log(
                    "推荐记录AI标记: "
                    f"ok={ai_mark_ok}, date={recommend_trade_date_int}, ai_count={len(step3_operation_codes)}",
                    logs_path,
                )
            except Exception as e:
                _log(f"推荐记录AI标记失败: {e}", logs_path)
    else:
        summary.append({"step": "批量研报", "ok": True, "err": None, "elapsed_s": 0, "output": "skipped (no symbols)"})
        _log("阶段 2 批量研报: 跳过（无筛选结果）", logs_path)

    if skip_step4:
        summary.append({
            "step": "私人再平衡",
            "ok": True,
            "err": None,
            "elapsed_s": 0,
            "output": "skipped (DAILY_JOB_SKIP_STEP4=1)",
        })
        _log("阶段 3 私人再平衡: 跳过（DAILY_JOB_SKIP_STEP4=1）", logs_path)
        step4_target = None
    else:
        step4_target, step4_target_reason = _load_step4_target()
    if not skip_step4 and not step4_target:
        summary.append({
            "step": "私人再平衡",
            "ok": True,
            "err": None,
            "elapsed_s": 0,
            "output": f"skipped ({step4_target_reason})",
        })
        _log(f"阶段 3 私人再平衡: 跳过（{step4_target_reason}）", logs_path)
    elif not skip_step4:
        tg_bot_token = os.getenv("TG_BOT_TOKEN", "").strip()
        tg_chat_id = os.getenv("TG_CHAT_ID", "").strip()
        if not tg_bot_token or not tg_chat_id:
            summary.append({
                "step": "私人再平衡",
                "ok": True,
                "err": None,
                "elapsed_s": 0,
                "output": "skipped (TG_BOT_TOKEN/TG_CHAT_ID 未配置)",
            })
            _log("阶段 3 私人再平衡: 跳过（TG_BOT_TOKEN/TG_CHAT_ID 未配置）", logs_path)
            step4_target = None
        if step4_target is not None:
            t0 = datetime.now(TZ)
            user_id = str(step4_target.get("user_id", "") or "").strip()
            portfolio_id = str(step4_target.get("portfolio_id", "") or "").strip()
            step4_candidate_meta: list[dict] = []
            if step3_operation_codes:
                allowed_set = set(step3_operation_codes)
                for item in symbols_info:
                    if not isinstance(item, dict):
                        continue
                    code = str(item.get("code", "")).strip()
                    if code in allowed_set:
                        step4_candidate_meta.append(item)
            _log(
                f"阶段 3 私人再平衡: 候选收口为 Step3 可操作池 {len(step4_candidate_meta)} 只",
                logs_path,
            )
            step4_ok = True
            step4_reason = "ok"
            step4_err = None
            try:
                step4_ok, step4_reason = run_step4(
                    external_report=step3_report_text,
                    benchmark_context={"regime": regime_context.get("benchmark_regime", "NEUTRAL")},
                    api_key=api_key,
                    model=model,
                    candidate_meta=step4_candidate_meta,
                    portfolio_id=portfolio_id,
                    tg_bot_token=tg_bot_token,
                    tg_chat_id=tg_chat_id,
                )
                step4_err = None if step4_ok else STEP4_REASON_MAP.get(step4_reason, step4_reason)
            except Exception as e:
                step4_ok = False
                step4_reason = "unexpected_exception"
                step4_err = str(e)
            elapsed4 = (datetime.now(TZ) - t0).total_seconds()
            summary.append({
                "step": "私人再平衡",
                "ok": step4_ok and step4_err is None,
                "err": step4_err,
                "elapsed_s": round(elapsed4, 1),
                "output": (
                    f"user={user_id}, portfolio={portfolio_id}, reason={step4_reason}"
                ),
            })
            _log(
                f"阶段 3 私人再平衡: user={user_id}, portfolio={portfolio_id}, "
                f"ok={step4_ok}, reason={step4_reason}, elapsed={elapsed4:.1f}s, err={step4_err}",
                logs_path,
            )

    _log("开始同步所有推荐记录的实时价格...", logs_path)
    try:
        updated_n = sync_all_tracking_prices(price_map=None)
        _log(f"实时价格同步完成，共更新 {updated_n} 条记录", logs_path)
    except Exception as e:
        _log(f"实时价格同步失败: {e}", logs_path)

    total_elapsed = sum(s.get("elapsed_s", 0) for s in summary)
    _log("", logs_path)
    _log("=== 阶段汇总 ===", logs_path)
    for s in summary:
        status = "✅" if s["ok"] else "❌"
        _log(f"  {status} {s['step']}: {s.get('elapsed_s', 0)}s, {s.get('output', '')}" + (f" | {s['err']}" if s.get("err") else ""), logs_path)
    _log(f"总耗时: {total_elapsed:.1f}s", logs_path)
    _log("定时任务结束", logs_path)

    if has_blocking_failure:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
