"""共用事件数据读取：清洗与聚合统一从这里取数，保证口径一致。

真实项目里这里会替换成对事件存储（Kafka/数仓）的读取层；
当前实现用确定性的模拟数据，保证同一 event_id 任何时候读到的内容一致，
这也是回放诊断证据可核实的前提。
"""
from __future__ import annotations

from typing import Any

BASE_TIME = 1700000000


def read_raw_events(event_id: str, count: int = 500) -> list[dict[str, Any]]:
    """读取原始事件数据（清洗输入 / 回放诊断数据源）。"""
    events: list[dict[str, Any]] = []
    for i in range(1, count + 1):
        user_id = f"U{i:05d}"
        # 模拟部分异常数据
        is_bot = i % 20 == 0
        is_dup = i % 15 == 0
        is_invalid = i % 25 == 0

        events.append({
            "user_id": user_id,
            "event_id": event_id,
            "timestamp": BASE_TIME + i * 60,
            "page_url": "" if is_invalid else f"/page{i % 10}",
            "item_id": -1 if is_invalid else i,
            "amount": -10.0 if is_invalid else float(i * 5.5),
            "is_bot": is_bot,
            "is_dup": is_dup,
        })
        if is_dup:
            # 插入重复数据
            events.append({
                "user_id": user_id,
                "event_id": event_id,
                "timestamp": BASE_TIME + i * 60,
                "page_url": f"/page{i % 10}",
                "item_id": i,
                "amount": float(i * 5.5),
                "is_bot": False,
                "is_dup": True,
            })
    return events


def read_cleansed_events(event_id: str, count: int = 1000) -> list[dict[str, Any]]:
    """读取清洗后的事件数据（聚合输入）。"""
    events: list[dict[str, Any]] = []
    for i in range(1, count + 1):
        user_id = f"U{i:05d}"
        events.append({
            "user_id": user_id,
            "event_id": event_id,
            "timestamp": BASE_TIME + (i % 24) * 3600 + (i // 24) * 86400,
            "ATTR-001": ["app", "web", "miniapp", "h5"][i % 4],
            "ATTR-004": ["ios", "android", "pc", "other"][i % 4],
            "ATTR-005": ["普通", "银卡", "金卡", "钻石"][i % 4],
            "value": float(i * 10.5),
        })
    return events


def split_event_ranges(event_count: int, segment_count: int) -> list[tuple[int, int]]:
    """把事件序列均匀切成 segment_count 段，返回 [(start, end), ...] 左闭右开区间。"""
    if segment_count <= 0:
        raise ValueError("segment_count must be positive")
    base, extra = divmod(event_count, segment_count)
    ranges: list[tuple[int, int]] = []
    start = 0
    for idx in range(segment_count):
        size = base + (1 if idx < extra else 0)
        ranges.append((start, start + size))
        start += size
    return ranges
