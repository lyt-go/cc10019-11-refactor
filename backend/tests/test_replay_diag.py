"""回放诊断与聚合批次回放测试。

覆盖场景：同一批次两段回放诊断只写成一段（另一段写入失败）→
运营从聚合批次回放 → 确认覆盖重复记录与越界字段、核对重放范围 →
诊断在两段之间失败后重试 → 比较实际重放段与已归档证据 →
聚合数值原样保留、缺失段从未被标成已检查。
"""
from __future__ import annotations

import unittest

# 设置路径
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.store import store
from app.services.event_cleanse import cleanse_events, get_cleanse_result
from app.services.event_aggregate import aggregate_events, get_aggregate_result
from app.services.replay_diag import (
    ArchiveError,
    SEGMENT_COUNT,
    archive_diagnostics,
    build_diagnostics,
    list_diagnostics,
    plan_replay,
    replay_batch,
    retry_archive,
)

# 场景批次：EC-003/EA-003 的合法范围含 amount 下限，原始数据中存在越界字段与重复记录
RULE_ID = "EC-003"
CONFIG_ID = "EA-003"
EVENT_ID = "EV-003"


class TestCleanseRefactor(unittest.TestCase):
    """重构后既有清洗输入、输出和聚合口径不变。"""

    def setUp(self) -> None:
        store._artifacts.clear()
        store._compute_cache.clear()

    def test_cleanse_output_unchanged(self) -> None:
        """清洗统计口径与重构前一致。"""
        result = cleanse_events("EC-001")
        self.assertEqual(result.total_records, 533)
        self.assertEqual(result.valid_records, 473)
        self.assertEqual(result.invalid_records, 40)
        self.assertEqual(result.deduped_records, 20)

    def test_cleanse_actions_quarantine_discard_flag(self) -> None:
        """字段校验和刷量检测分别处理隔离、丢弃及标记。"""
        # discard：异常事件被丢弃
        r1 = cleanse_events("EC-001")
        self.assertEqual(r1.valid_records + r1.invalid_records, 513)  # 533 - 20 去重
        # quarantine：异常事件保留但打隔离标记
        r2 = cleanse_events("EC-002")
        self.assertEqual(r2.valid_records + r2.deduped_records, r2.total_records)
        # flag：异常事件保留并全部计入有效
        r3 = cleanse_events("EC-003")
        self.assertEqual(r3.valid_records, r3.total_records)

    def test_aggregate_reads_cleanse_receipt(self) -> None:
        """事件聚合读取清洗回执。"""
        cleanse_events(RULE_ID)
        result = aggregate_events(CONFIG_ID)
        self.assertEqual(result.cleanse_receipt.get("rule_id"), RULE_ID)
        self.assertEqual(result.cleanse_receipt.get("event_id"), EVENT_ID)
        artifact = get_aggregate_result(CONFIG_ID)
        self.assertEqual(artifact["cleanse_receipt"]["rule_id"], RULE_ID)

    def test_aggregate_output_unchanged(self) -> None:
        """聚合口径与重构前一致（确定性）。"""
        first = aggregate_events("EA-001")
        self.assertEqual(first.total_count, 24)
        again = aggregate_events("EA-001")
        self.assertEqual(first.result_rows, again.result_rows)


class TestReplayDiagnostics(unittest.TestCase):
    """回放诊断归档、圈定、重放与重试。"""

    def setUp(self) -> None:
        store._artifacts.clear()
        store._compute_cache.clear()
        # 准备原批次：清洗回执 + 聚合批次
        cleanse_events(RULE_ID)
        aggregate_events(CONFIG_ID)

    def test_partial_archive_then_replay_then_retry(self) -> None:
        """两段诊断只写成一段 → 回放 → 段间失败后重试 → 证据比对。"""
        cleanse_before = get_cleanse_result(RULE_ID)
        agg_before = get_aggregate_result(CONFIG_ID)

        # 1. 同一批次生成两段回放诊断
        diags = build_diagnostics(CONFIG_ID)
        self.assertEqual(len(diags), SEGMENT_COUNT)
        self.assertEqual(diags[0]["rule_id"], RULE_ID)
        self.assertEqual(diags[1]["segment_index"], 1)

        # 2. 归档在两段之间失败：只写成第一段，第二段写入失败
        with self.assertRaises(ArchiveError):
            archive_diagnostics(CONFIG_ID, diags, fail_at=1)
        archived = list_diagnostics(CONFIG_ID)
        self.assertEqual([d["segment_index"] for d in archived], [0])

        # 3. 运营从聚合批次回放：系统只圈定可核实的已归档段
        plan = plan_replay(CONFIG_ID)
        self.assertEqual([s["segment_index"] for s in plan["segments"]], [0])
        self.assertEqual(plan["missing_segments"], [1])
        # 核对具体要重放的范围
        seg0 = plan["segments"][0]
        self.assertEqual(plan["replay_ranges"], [(seg0["start"], seg0["end"])])
        self.assertEqual(seg0["end"] - seg0["start"], seg0["record_count"])

        # 4. 执行重放：确认本次处理覆盖了重复记录和越界字段
        replay = replay_batch(CONFIG_ID)
        self.assertTrue(replay["verified"])
        self.assertTrue(replay["coverage"]["covers_dedup"])
        self.assertTrue(replay["coverage"]["covers_range_violation"])
        self.assertEqual(replay["replayed_ranges"], plan["replay_ranges"])

        # 5. 缺失段从未被标成已检查
        remaining = list_diagnostics(CONFIG_ID)
        self.assertFalse(any(d["segment_index"] == 1 for d in remaining))
        self.assertFalse(any(d["segment_index"] == 1 and d["status"] == "checked" for d in remaining))

        # 6. 诊断失败后重试：规则与事件必须属于原批次
        with self.assertRaises(ValueError):
            retry_archive(CONFIG_ID, rule_id="EC-001", event_id=EVENT_ID)
        with self.assertRaises(ValueError):
            retry_archive(CONFIG_ID, rule_id=RULE_ID, event_id="EV-001")
        retry_archive(CONFIG_ID, rule_id=RULE_ID, event_id=EVENT_ID)
        self.assertEqual(len(list_diagnostics(CONFIG_ID)), SEGMENT_COUNT)

        # 7. 重试后回放覆盖两段，实际重放段与已归档证据比对通过
        replay2 = replay_batch(CONFIG_ID)
        self.assertTrue(replay2["verified"])
        self.assertEqual(len(replay2["segments"]), SEGMENT_COUNT)
        self.assertEqual(replay2["missing_segments"], [])
        archived_map = {d["diag_id"]: d for d in list_diagnostics(CONFIG_ID)}
        for seg in replay2["segments"]:
            evidence = archived_map[seg["diag_id"]]
            self.assertEqual(seg["start"], evidence["start"])
            self.assertEqual(seg["end"], evidence["end"])
            self.assertEqual(evidence["status"], "checked")

        # 8. 已确认的清洗和聚合统计仍可按原值查询
        cleanse_after = get_cleanse_result(RULE_ID)
        self.assertEqual(cleanse_after["valid_records"], cleanse_before["valid_records"])
        self.assertEqual(cleanse_after["invalid_records"], cleanse_before["invalid_records"])
        self.assertEqual(cleanse_after["deduped_records"], cleanse_before["deduped_records"])
        agg_after = get_aggregate_result(CONFIG_ID)
        self.assertEqual(agg_after["result_rows"], agg_before["result_rows"])
        self.assertEqual(agg_after["total_count"], agg_before["total_count"])

    def test_plan_requires_verifiable_evidence(self) -> None:
        """证据不可核实的诊断段不被圈定。"""
        diags = build_diagnostics(CONFIG_ID)
        archive_diagnostics(CONFIG_ID, diags)
        # 篡改已归档证据
        tampered = store.get_artifact("replay_diag", diags[0]["diag_id"])
        tampered["fingerprint"] = "0" * 64
        store.set_artifact("replay_diag", diags[0]["diag_id"], tampered)
        plan = plan_replay(CONFIG_ID)
        self.assertEqual([s["segment_index"] for s in plan["segments"]], [1])

    def test_missing_segment_never_checked(self) -> None:
        """只有第一段归档时，重放不会把缺失段标成已检查。"""
        diags = build_diagnostics(CONFIG_ID)
        archive_diagnostics(CONFIG_ID, diags[:1])
        replay_batch(CONFIG_ID)
        statuses = {d["segment_index"]: d["status"] for d in list_diagnostics(CONFIG_ID)}
        self.assertEqual(statuses, {0: "checked"})


if __name__ == "__main__":
    unittest.main(verbosity=2)
