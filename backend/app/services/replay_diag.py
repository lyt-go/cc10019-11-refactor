"""回放诊断：为聚合批次生成、归档回放诊断段，并按可核实证据圈定重放范围。

诊断数据的归档统一收拢在 archive_diagnostics 一处；
回放校验（段证据指纹）在归档生成与回放核实两侧共用，保证可核实性。
已确认的清洗回执与聚合统计不被回放流程改写，仍按原值查询。
"""
from __future__ import annotations

import hashlib
import json
from typing import Any

from app.store import store
from app.services.event_data import read_raw_events, split_event_ranges
from app.services.event_cleanse import find_cleanse_receipt

DIAG_ARTIFACT = "replay_diag"
# 每个聚合批次的回放诊断段数
SEGMENT_COUNT = 2


class ArchiveError(RuntimeError):
    """诊断段归档写入失败。"""


def _segment_fingerprint(events: list[dict[str, Any]]) -> str:
    """段证据指纹：归档与回放共用的回放校验，指纹一致才算可核实。"""
    payload = json.dumps(events, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _is_range_violation(evt: dict[str, Any], rule: dict[str, Any] | None) -> bool:
    """判断事件是否存在越界字段（超出规则合法范围的校验字段）。"""
    if not rule:
        return False
    valid_range = rule.get("valid_range", {})
    for field in rule.get("check_fields", []):
        if field not in valid_range:
            continue
        val = evt.get(field)
        if val is None or val == "":
            continue
        range_cfg = valid_range[field]
        try:
            num = float(val)
        except (ValueError, TypeError):
            continue
        if "min" in range_cfg:
            try:
                if num < float(range_cfg["min"]):
                    return True
            except (ValueError, TypeError):
                pass
        if "max" in range_cfg:
            try:
                if num > float(range_cfg["max"]):
                    return True
            except (ValueError, TypeError):
                pass
    return False


def _load_batch(config_id: str) -> tuple[dict[str, Any], dict[str, Any] | None, dict[str, Any] | None]:
    """读取聚合批次配置、原批次清洗回执与清洗规则。"""
    config = store.find_by_field("event_aggregate", "config_id", config_id)
    if config is None:
        raise ValueError(f"Aggregate config not found: {config_id}")
    receipt = find_cleanse_receipt(config["event_id"])
    rule = None
    if receipt is not None:
        rule = store.find_by_field("event_cleanse", "rule_id", receipt.get("rule_id"))
    return config, receipt, rule


def build_diagnostics(config_id: str) -> list[dict[str, Any]]:
    """按聚合批次生成回放诊断段：圈定段范围并记录覆盖的异常类型与证据指纹。"""
    config, receipt, rule = _load_batch(config_id)
    event_id = config["event_id"]
    events = read_raw_events(event_id, 500)
    ranges = split_event_ranges(len(events), SEGMENT_COUNT)

    diags: list[dict[str, Any]] = []
    for idx, (start, end) in enumerate(ranges):
        seg_events = events[start:end]
        diags.append({
            "diag_id": f"{config_id}-SEG-{idx}",
            "batch_id": config_id,
            "config_id": config_id,
            "event_id": event_id,
            "rule_id": receipt.get("rule_id") if receipt else None,
            "segment_index": idx,
            "start": start,
            "end": end,
            "record_count": end - start,
            "covers_dedup": any(e.get("is_dup") for e in seg_events),
            "covers_range_violation": any(_is_range_violation(e, rule) for e in seg_events),
            "covers_bot": any(e.get("is_bot") for e in seg_events),
            "fingerprint": _segment_fingerprint(seg_events),
            "status": "pending",
        })
    return diags


def archive_diagnostics(config_id: str, diags: list[dict[str, Any]], fail_at: int | None = None) -> list[dict[str, Any]]:
    """诊断归档唯一入口：逐段写入归档区。

    fail_at 用于模拟段间写入失败（如两段诊断只写成一段），已写入的段保留。
    """
    for idx, diag in enumerate(diags):
        if fail_at is not None and idx == fail_at:
            raise ArchiveError(f"诊断段写入失败: {diag['diag_id']}")
        store.set_artifact(DIAG_ARTIFACT, diag["diag_id"], diag)
    return diags


def list_diagnostics(config_id: str) -> list[dict[str, Any]]:
    """列出批次已归档的诊断段（按段序号排序）。"""
    out = []
    for key in store.get_artifact_keys(DIAG_ARTIFACT):
        diag = store.get_artifact(DIAG_ARTIFACT, key)
        if diag and diag.get("batch_id") == config_id:
            out.append(diag)
    return sorted(out, key=lambda d: d["segment_index"])


def retry_archive(config_id: str, rule_id: str, event_id: str) -> list[dict[str, Any]]:
    """诊断失败后重试归档：所用规则与事件必须属于原批次，否则拒绝。"""
    config, receipt, _ = _load_batch(config_id)
    if config["event_id"] != event_id:
        raise ValueError(f"重试事件必须属于原批次: 期望 {config['event_id']}, 实际 {event_id}")
    expected_rule = receipt.get("rule_id") if receipt else None
    if expected_rule is None or rule_id != expected_rule:
        raise ValueError(f"重试规则必须属于原批次: 期望 {expected_rule}, 实际 {rule_id}")
    diags = build_diagnostics(config_id)
    archive_diagnostics(config_id, diags)
    return diags


def plan_replay(config_id: str) -> dict[str, Any]:
    """按可核实的回放诊断圈定要重放的事件段；证据不可核实的段不圈定。"""
    config, _, _ = _load_batch(config_id)
    event_id = config["event_id"]
    events = read_raw_events(event_id, 500)

    segments: list[dict[str, Any]] = []
    archived_indexes: set[int] = set()
    for diag in list_diagnostics(config_id):
        archived_indexes.add(diag["segment_index"])
        seg_events = events[diag["start"]:diag["end"]]
        if _segment_fingerprint(seg_events) != diag["fingerprint"]:
            continue  # 证据不可核实，不圈定
        segments.append(dict(diag))

    missing = [idx for idx in range(SEGMENT_COUNT) if idx not in archived_indexes]
    return {
        "config_id": config_id,
        "event_id": event_id,
        "segments": segments,
        "replay_ranges": [(s["start"], s["end"]) for s in segments],
        "missing_segments": missing,
    }


def replay_batch(config_id: str) -> dict[str, Any]:
    """从聚合批次回放：重放圈定的事件段，与已归档证据比对后标记已检查。

    只处理可核实段；缺失段没有归档证据，不会被圈定也不会被标成已检查。
    回放不改写已确认的清洗回执与聚合统计。
    """
    plan = plan_replay(config_id)
    _, _, rule = _load_batch(config_id)
    events = read_raw_events(plan["event_id"], 500)

    replayed: list[dict[str, Any]] = []
    for seg in plan["segments"]:
        seg_events = events[seg["start"]:seg["end"]]
        # 比较实际重放的事件段与已归档证据
        if _segment_fingerprint(seg_events) != seg["fingerprint"]:
            raise ValueError(f"重放段与归档证据不一致: {seg['diag_id']}")
        replayed.append({
            "diag_id": seg["diag_id"],
            "segment_index": seg["segment_index"],
            "start": seg["start"],
            "end": seg["end"],
            "record_count": len(seg_events),
            "events": seg_events,
        })
        diag = store.get_artifact(DIAG_ARTIFACT, seg["diag_id"])
        if diag is not None:
            diag["status"] = "checked"
            store.set_artifact(DIAG_ARTIFACT, seg["diag_id"], diag)

    replayed_events = [e for seg in replayed for e in seg["events"]]
    coverage = {
        "covers_dedup": any(e.get("is_dup") for e in replayed_events),
        "covers_range_violation": any(_is_range_violation(e, rule) for e in replayed_events),
        "covers_bot": any(e.get("is_bot") for e in replayed_events),
    }
    return {
        "config_id": config_id,
        "event_id": plan["event_id"],
        "verified": True,
        "segments": [{k: v for k, v in seg.items() if k != "events"} for seg in replayed],
        "replayed_ranges": [(seg["start"], seg["end"]) for seg in replayed],
        "missing_segments": plan["missing_segments"],
        "coverage": coverage,
    }
