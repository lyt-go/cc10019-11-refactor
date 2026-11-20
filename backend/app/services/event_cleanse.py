"""事件清洗引擎：校验事件数据，执行去重和异常处理。

字段校验与刷量检测是两个独立阶段，各自按规则配置对异常事件
执行隔离（quarantine）、丢弃（discard）或标记（flag）。
"""
from __future__ import annotations

from typing import Any

from app.store import store
from app.schemas import CleanseResult
from app.services.event_data import read_raw_events


def _apply_anomaly_action(evt: dict[str, Any], action: str) -> bool:
    """对异常事件应用处理动作：隔离 / 丢弃 / 标记。返回是否丢弃该事件。"""
    if action == "discard":
        return True
    if action == "quarantine":
        evt["quarantined"] = True
    else:  # flag
        evt["flagged"] = True
    return False


def _check_field_valid(evt: dict[str, Any], check_fields: list[str], valid_range: dict[str, Any]) -> bool:
    """字段校验：必填缺失或超出合法范围视为不合法。"""
    for field in check_fields:
        val = evt.get(field)
        if val is None or val == "":
            return False
        # 范围校验
        if field in valid_range:
            range_cfg = valid_range[field]
            if "min" in range_cfg:
                try:
                    if float(val) < float(range_cfg["min"]):
                        return False
                except (ValueError, TypeError):
                    pass
            if "max" in range_cfg:
                try:
                    if float(val) > float(range_cfg["max"]):
                        return False
                except (ValueError, TypeError):
                    pass
    return True


def cleanse_events(rule_id: str) -> CleanseResult:
    """执行事件清洗。"""
    rule = store.find_by_field("event_cleanse", "rule_id", rule_id)
    if rule is None:
        raise ValueError(f"Cleanse rule not found: {rule_id}")

    event_id = rule["event_id"]
    check_fields = rule.get("check_fields", [])
    valid_range = rule.get("valid_range", {})
    # 字段校验与刷量检测分别处理异常：刷量动作可独立配置，缺省沿用字段校验动作
    field_action = rule["anomaly_action"]
    bot_action = rule.get("bot_action") or field_action
    bot_detect = rule.get("bot_detect", False)
    dedup_window = rule.get("dedup_window", 0)

    # 获取原始事件数据
    events = read_raw_events(event_id, 500)

    total_records = len(events)
    valid_records = 0
    invalid_records = 0
    deduped_records = 0

    # 去重窗口
    seen: set[tuple[str, ...]] = set()
    cleansed: list[dict[str, Any]] = []

    for evt in events:
        # 刷量检测：独立应用隔离 / 丢弃 / 标记
        if bot_detect and evt.get("is_bot"):
            invalid_records += 1
            if _apply_anomaly_action(evt, bot_action):
                continue

        # 字段校验：独立应用隔离 / 丢弃 / 标记
        if not _check_field_valid(evt, check_fields, valid_range):
            invalid_records += 1
            if _apply_anomaly_action(evt, field_action):
                continue

        # 去重
        if dedup_window > 0:
            dup_key = tuple(str(evt.get(f, "")) for f in ["user_id", "event_id", "timestamp"])
            if dup_key in seen:
                deduped_records += 1
                continue
            seen.add(dup_key)

        cleansed.append(evt)
        valid_records += 1

    result = CleanseResult(
        rule_id=rule_id,
        event_id=event_id,
        total_records=total_records,
        valid_records=valid_records,
        invalid_records=invalid_records,
        deduped_records=deduped_records,
        output_table=f"cleansed_{event_id}",
        snapshot_path=f"/data/cleansed/{rule_id}.parquet",
    )

    # 存储产物（清洗回执，供事件聚合读取）
    store.set_artifact("event_cleanse", rule_id, result.model_dump())

    # 更新状态
    store.update("event_cleanse", rule["id"], {
        "status": "已生效",
        "pending": False,
    })

    return result


def get_cleanse_result(rule_id: str) -> dict[str, Any] | None:
    """获取清洗结果（清洗回执）。"""
    artifact = store.get_artifact("event_cleanse", rule_id)
    if artifact:
        return artifact
    try:
        result = cleanse_events(rule_id)
        return result.model_dump()
    except ValueError:
        return None


def find_cleanse_receipt(event_id: str) -> dict[str, Any] | None:
    """按事件编号查找已确认的清洗回执。"""
    for artifact in store.list_artifacts("event_cleanse"):
        if artifact.get("event_id") == event_id:
            return artifact
    return None
