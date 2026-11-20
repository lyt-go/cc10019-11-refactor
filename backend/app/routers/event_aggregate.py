"""事件聚合路由：执行事件聚合并获取结果。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request

from app.schemas import ActionPayload, ActionResult, EntryPayload
from app.services.event_aggregate import aggregate_events, get_aggregate_result
from app.services.replay_diag import (
    archive_diagnostics,
    build_diagnostics,
    list_diagnostics,
    plan_replay,
    replay_batch,
    retry_archive,
)
from app.store import store

router = APIRouter(prefix="/api/event-aggregate", tags=["event_aggregate"])


@router.get("")
def index(page: int = 1, size: int = 20) -> dict[str, Any]:
    rows = store.rows("event_aggregate")
    total = len(rows)
    start = (page - 1) * size
    return {"items": rows[start : start + size], "total": total, "page": page, "size": size}




@router.get("/export")
def export_entries() -> dict[str, Any]:
    rows = store.rows("event_aggregate")
    return {"items": rows, "total": len(rows)}
@router.get("/{entry_id}")
def detail(entry_id: int) -> dict[str, Any]:
    row = store.find("event_aggregate", entry_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Not found")
    return row


@router.post("/{config_id}/run")
def run(config_id: str) -> ActionResult:
    try:
        result = aggregate_events(config_id)
        return ActionResult(ok=True, message="Aggregated", entry=result.model_dump())
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/{config_id}/result")
def result(config_id: str) -> dict[str, Any]:
    res = get_aggregate_result(config_id)
    if res is None:
        raise HTTPException(status_code=404, detail="Result not found")
    return res


# ========== 聚合批次回放 ==========
@router.post("/{config_id}/replay/diagnostics")
def create_replay_diagnostics(config_id: str) -> ActionResult:
    """生成并归档聚合批次的回放诊断段。"""
    try:
        diags = build_diagnostics(config_id)
        archive_diagnostics(config_id, diags)
        return ActionResult(ok=True, message="Diagnostics archived", entry={"segments": diags})
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/{config_id}/replay/diagnostics")
def get_replay_diagnostics(config_id: str) -> dict[str, Any]:
    """查看批次已归档的回放诊断段。"""
    return {"items": list_diagnostics(config_id)}


@router.post("/{config_id}/replay/diagnostics/retry")
def retry_replay_diagnostics(config_id: str, payload: EntryPayload) -> ActionResult:
    """诊断失败后重试归档：规则与事件必须属于原批次。"""
    try:
        diags = retry_archive(
            config_id,
            rule_id=str(payload.values.get("rule_id", "")),
            event_id=str(payload.values.get("event_id", "")),
        )
        return ActionResult(ok=True, message="Diagnostics re-archived", entry={"segments": diags})
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/{config_id}/replay/plan")
def get_replay_plan(config_id: str) -> dict[str, Any]:
    """按可核实的回放诊断圈定要重放的事件段。"""
    try:
        return plan_replay(config_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/{config_id}/replay")
def run_replay(config_id: str) -> ActionResult:
    """从聚合批次回放：重放圈定的事件段并与已归档证据比对。"""
    try:
        result = replay_batch(config_id)
        return ActionResult(ok=True, message="Replayed", entry=result)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/{entry_id}/actions")
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    row = store.find("event_aggregate", entry_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Not found")
    action = payload.action
    row["status"] = action
    row["pending"] = False
    return ActionResult(ok=True, message=action, entry=row)
