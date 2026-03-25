from __future__ import annotations

from typing import Any

import streamlit as st

from integrations.github_actions import (
    background_jobs_allowed_for_user,
    clear_github_actions_caches,
    find_run_by_request_id,
    github_actions_ready,
    load_latest_result,
    load_result_json_for_run,
    trigger_web_job,
)


def current_user_id() -> str:
    user = st.session_state.get("user") or {}
    if isinstance(user, dict):
        return str(user.get("id", "") or "").strip()
    return ""


def background_jobs_ready_for_current_user() -> tuple[bool, str]:
    ready, msg = github_actions_ready()
    if not ready:
        return (False, msg)
    user_id = current_user_id()
    if not user_id:
        return (False, "当前未登录")
    if not background_jobs_allowed_for_user(user_id):
        return (False, "当前账号未被授权触发后台任务")
    return (True, "")


def submit_background_job(
    job_kind: str,
    payload: dict[str, Any],
    *,
    state_key: str,
    workflow: str = "",
) -> str:
    user_id = current_user_id()
    merged_payload = {"user_id": user_id, **payload}
    request_id = trigger_web_job(job_kind, merged_payload, workflow=workflow)
    st.session_state[state_key] = {
        "job_kind": job_kind,
        "request_id": request_id,
        "workflow": workflow,
        "run": None,
        "result": None,
    }
    return request_id


def sync_background_job_state(*, state_key: str) -> dict[str, Any] | None:

    state = st.session_state.get(state_key)
    if not isinstance(state, dict):
        return None
    request_id = str(state.get("request_id", "") or "").strip()
    if not request_id:
        return state
    workflow = str(state.get("workflow", "") or "").strip()
    run = find_run_by_request_id(request_id, workflow=workflow)

    state["run"] = run
    if run and run.status == "completed":
        state["result"] = load_result_json_for_run(run.run_id)
    st.session_state[state_key] = state
    return state


def load_latest_job_result(
    job_kind: str,
    *,
    per_page: int = 10,
    workflow: str = "",
) -> tuple[Any, dict[str, Any] | None]:
    user_id = current_user_id()
    return load_latest_result(
        job_kind,
        requested_by_user_id=user_id,
        per_page=per_page,
        workflow=workflow,
    )



def refresh_background_job_data() -> None:
    clear_github_actions_caches()
