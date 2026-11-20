"""事件聚合引擎：消费清洗后的事件数据，执行聚合计算。

聚合时读取对应事件的清洗回执并随产物归档，形成可回放的聚合批次；
聚合口径（维度分组、时间粒度、聚合函数）保持不变。
"""
from __future__ import annotations

from typing import Any
from collections import defaultdict

from app.store import store
from app.schemas import AggResult
from app.services.event_data import read_cleansed_events
from app.services.event_cleanse import find_cleanse_receipt


def aggregate_events(config_id: str) -> AggResult:
    """执行事件聚合。"""
    config = store.find_by_field("event_aggregate", "config_id", config_id)
    if config is None:
        raise ValueError(f"Aggregate config not found: {config_id}")

    event_id = config["event_id"]
    agg_dimensions = config.get("agg_dimensions", [])
    agg_function = config["agg_function"]
    time_grain = config["time_grain"]
    filter_expr = config.get("filter_expr", "")

    # 读取清洗回执：聚合批次须能回溯到产生输入数据的清洗批次
    cleanse_receipt = find_cleanse_receipt(event_id)

    # 生成事件数据
    events = read_cleansed_events(event_id, 1000)

    # 应用过滤
    if filter_expr:
        filtered = []
        for e in events:
            # 简单过滤：ATTR-003 > 0 等价于 value > 0
            if "ATTR-003" in filter_expr and e.get("value", 0) > 0:
                filtered.append(e)
        events = filtered

    # 聚合计算
    grouped: dict[tuple[str, ...], list[dict[str, Any]]] = defaultdict(list)
    for e in events:
        key_parts = []
        for dim in agg_dimensions:
            key_parts.append(str(e.get(dim, "unknown")))
        # 时间粒度
        ts = e.get("timestamp", 0)
        if time_grain == "hour":
            time_key = f"{ts // 3600 % 24:02d}:00"
        elif time_grain == "day":
            time_key = f"day_{ts // 86400}"
        elif time_grain == "week":
            time_key = f"week_{ts // 604800}"
        else:
            time_key = "all"
        key_parts.append(time_key)
        grouped[tuple(key_parts)].append(e)

    result_rows: list[dict[str, Any]] = []
    for key, evts in sorted(grouped.items()):
        row: dict[str, Any] = {}
        for i, dim in enumerate(agg_dimensions):
            row[dim] = key[i]
        row["time_grain"] = key[-1]

        if agg_function == "count":
            row["agg_value"] = len(evts)
        elif agg_function == "sum":
            row["agg_value"] = sum(e.get("value", 0) for e in evts)
        elif agg_function == "avg":
            vals = [e.get("value", 0) for e in evts]
            row["agg_value"] = sum(vals) / len(vals) if vals else 0
        elif agg_function == "max":
            row["agg_value"] = max((e.get("value", 0) for e in evts), default=0)
        elif agg_function == "min":
            row["agg_value"] = min((e.get("value", 0) for e in evts), default=0)
        elif agg_function == "uv":
            row["agg_value"] = len(set(e.get("user_id") for e in evts))
        else:
            row["agg_value"] = len(evts)

        result_rows.append(row)

    result = AggResult(
        config_id=config_id,
        event_id=event_id,
        agg_dimensions=agg_dimensions,
        time_grain=time_grain,
        result_rows=result_rows,
        total_count=len(result_rows),
        output_table=config.get("output_table", ""),
        cleanse_receipt=cleanse_receipt or {},
    )

    # 存储产物
    store.set_artifact("event_aggregate", config_id, result.model_dump())

    # 更新状态
    store.update("event_aggregate", config["id"], {
        "status": "已生效",
        "pending": False,
    })

    return result


def get_aggregate_result(config_id: str) -> dict[str, Any] | None:
    """获取聚合结果。"""
    artifact = store.get_artifact("event_aggregate", config_id)
    if artifact:
        return artifact
    try:
        result = aggregate_events(config_id)
        return result.model_dump()
    except ValueError:
        return None
