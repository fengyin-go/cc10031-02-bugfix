"""结果校验覆盖修复测试 — 验证抽样覆盖口径与隔离回执一致性。

测试覆盖需求点：
1. 抽样比例较低时，未实际执行的规则不能被记为通过（待复核而不是通过）
2. 抽样记录与未抽样记录分开计数
3. 同一记录再次校验时不在隔离区重复出现，隔离回执唯一
4. 并发写入隔离回执只接受一个版本
5. 低覆盖批次两种处理方案（整批重抽替换 / 保留旧报告追加未覆盖清单）
   都只产生一个最终质量状态
6. 校验工作台可看到每条规则的抽样数、执行数和覆盖状态
7. 隔离原因与当前最终质量状态保持一致
8. 质量列表与隔离工作台的待复核记录对得上
9. 跨业务面同步更新待办，业务清单与结果台账一致
10. 保留一份校验收据明细可供核对
"""
from __future__ import annotations

import sys
import threading
import uuid
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.services.result_verify import ResultVerifyService  # noqa: E402
from app.store import store  # noqa: E402


# ── 测试工具 ──


def _unique_batch(prefix: str = "BATC-T") -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _steady_rng(value: float = 0.5):
    """恒定随机源：0.5 既不会触发 8% 格式异常，也便于按抽样比例分流。"""
    return lambda: value


@pytest.fixture
def verify_env():
    """准备隔离的校验规则环境：关闭既有规则，登记测试规则，用后关闭。"""
    svc = ResultVerifyService()
    rows = store.rows("result_verify")
    saved_status = {r.get("id"): (r.get("status"), r.get("pending")) for r in rows}
    for row in rows:
        row["status"] = "已关闭"

    added_ids: list[int] = []

    def add_rule(name: str, field: str, condition: str = "non_empty",
                 sample_rate: str = "1.0") -> int:
        new_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
        rows.append({
            "id": new_id,
            "规则名称": name,
            "校验字段": field,
            "校验条件": condition,
            "期望模式": "non_empty",
            "异常动作": "quarantine",
            "抽样比例": sample_rate,
            "关联流程": "测试流程",
            "status": "已启用",
            "pending": False,
            "abnormal": False,
        })
        added_ids.append(new_id)
        return new_id

    yield svc, add_rule

    # 收尾：测试规则关闭，既有规则恢复原状态
    for row in rows:
        if row.get("id") in added_ids:
            row["status"] = "已关闭"
            row["pending"] = False
        elif row.get("id") in saved_status:
            row["status"], row["pending"] = saved_status[row.get("id")]


def _run_verify(svc: ResultVerifyService, batch_ref: str,
                records: list[dict], rng=None) -> dict:
    """写入采集记录并发布采集事件，执行一轮校验。"""
    for fields in records:
        store.collect_records.add(batch_ref, "data_extract", fields)
    store.channel.publish(
        topic="rpa.data_collected", source="data_extract",
        payload={"batch_ref": batch_ref, "site": "test-site"})
    return svc.verify_collected_data(rng=rng)


# ── 1. 未执行规则不能被当作通过 ──


class TestNonExecutedRulesNotPassed:
    def test_zero_sample_rate_not_passed(self, verify_env):
        """抽样比例为 0：规则全部跳过，记录不能记为通过。"""
        svc, add_rule = verify_env
        add_rule("规则A", "名称", "non_empty", "0.0")
        batch = _unique_batch()
        result = _run_verify(svc, batch, [{"名称": "产品1"}, {"名称": "产品2"}],
                             rng=_steady_rng())

        assert result["passed"] == 0, "未执行的规则不能被当作通过"
        assert result["failed"] == 0
        assert result["uncovered"] == 2, "覆盖不足应进入待复核"
        summary = result["batches"][0]
        assert summary["final_status"] == "待复核"

    def test_empty_applied_no_longer_passes(self, verify_env):
        """原缺陷：applied==0 直接返回通过；修复后应为待复核。"""
        svc, add_rule = verify_env
        add_rule("低比例规则", "名称", "non_empty", "0.0")
        quality = svc.run_single_validate({"名称": "产品1"})
        assert quality["passed"] is False
        assert quality["final_status"] == "待复核"
        assert quality["coverage_status"] == "未覆盖"
        assert quality["rules_executed"] == 0

    def test_partial_coverage_pending_not_passed(self, verify_env):
        """部分规则被抽样执行、其余跳过：记录不得记为通过。"""
        svc, add_rule = verify_env
        add_rule("全量规则", "名称", "non_empty", "1.0")
        add_rule("低比例规则", "金额", "numeric", "0.0")
        batch = _unique_batch()
        result = _run_verify(
            svc, batch, [{"名称": "产品1", "金额": "100"}], rng=_steady_rng())

        assert result["passed"] == 0
        assert result["uncovered"] == 1
        entry = store.quality_lineage.list_by_batch(batch)[0]
        assert entry.rules_sampled == 1
        assert entry.rules_executed == 1
        assert entry.rules_skipped == 1
        assert entry.coverage_status == "部分覆盖"
        assert entry.final_status == "待复核"

    def test_full_coverage_passes(self, verify_env):
        """全量抽样且校验通过：记录正常记为通过（回归保护）。"""
        svc, add_rule = verify_env
        add_rule("全量规则", "名称", "non_empty", "1.0")
        batch = _unique_batch()
        result = _run_verify(svc, batch, [{"名称": "产品1"}], rng=_steady_rng())
        assert result["passed"] == 1
        assert result["uncovered"] == 0
        entry = store.quality_lineage.list_by_batch(batch)[0]
        assert entry.coverage_status == "已覆盖"
        assert entry.final_status == "通过"

    def test_failed_rule_marks_failed(self, verify_env):
        """规则执行且未通过：记录记为未通过并隔离。"""
        svc, add_rule = verify_env
        add_rule("非空规则", "名称", "non_empty", "1.0")
        batch = _unique_batch()
        result = _run_verify(svc, batch, [{"名称": "  "}], rng=_steady_rng())
        assert result["failed"] == 1
        entry = store.quality_lineage.list_by_batch(batch)[0]
        assert entry.final_status == "未通过"


# ── 2. 抽样记录与未抽样记录分开计数 ──


class TestSampledUnsampledCounts:
    def test_sampled_and_unsampled_counted_separately(self, verify_env):
        """同一批次内，被抽样的记录与未被抽样的记录分开计数。"""
        svc, add_rule = verify_env
        add_rule("半抽样规则", "名称", "non_empty", "0.5")
        batch = _unique_batch()
        records = [{"名称": f"产品{i}"} for i in range(4)]
        # rng 序列：第 1、3 条抽中（0.4 <= 0.5），第 2、4 条跳过（0.6 > 0.5）
        draws = iter([0.4, 0.5, 0.6, 0.4, 0.5, 0.6])
        result = _run_verify(svc, batch, records, rng=lambda: next(draws, 0.5))

        summary = result["batches"][0]
        assert summary["sampled_records"] == 2, "抽样记录数应单独统计"
        assert summary["unsampled_records"] == 2, "未抽样记录数应单独统计"
        assert summary["sampled_records"] + summary["unsampled_records"] == 4
        # 抽中的记录全量覆盖（唯一规则被执行）→ 通过；未抽中的 → 待复核
        assert summary["passed"] == 2
        assert summary["uncovered"] == 2


# ── 3. 隔离区去重与唯一回执 ──


class TestQuarantineDedup:
    def test_revalidation_no_duplicate_in_vault(self, verify_env):
        """同一记录再次校验时不能在隔离区重复出现。"""
        svc, add_rule = verify_env
        add_rule("低比例规则", "名称", "non_empty", "0.0")
        batch = _unique_batch()
        _run_verify(svc, batch, [{"名称": "产品1"}], rng=_steady_rng())
        before = len(store.dirty_vault.list_by_batch(batch))
        assert before == 1

        # 再次校验同一批次（整批重抽路径，仍然覆盖不足）
        svc.resolve_low_coverage_batch(batch, "resample", rng=_steady_rng())
        after = len(store.dirty_vault.list_by_batch(batch))
        assert after == before, "同一记录再次校验不应在隔离区重复出现"

    def test_unique_receipt_per_record(self, verify_env):
        """按处理项乙规则：每条记录保存唯一的隔离回执。"""
        svc, add_rule = verify_env
        add_rule("低比例规则", "名称", "non_empty", "0.0")
        batch = _unique_batch()
        _run_verify(svc, batch, [{"名称": "产品1"}, {"名称": "产品2"}],
                    rng=_steady_rng())
        svc.resolve_low_coverage_batch(batch, "resample", rng=_steady_rng())

        receipts = store.quarantine_ledger.list_by_batch(batch)
        keys = [r.record_key for r in receipts]
        assert len(keys) == len(set(keys)) == 2, "每条记录只能有一份隔离回执"

    def test_concurrent_receipt_write_single_version(self, verify_env):
        """并发再次写入只接受一个版本：同一记录始终只有一份回执。"""
        svc, _ = verify_env
        batch = _unique_batch()
        barrier = threading.Barrier(8)

        def write_receipt():
            barrier.wait()
            store.quarantine_ledger.save_receipt(
                source=svc.MODULE, batch_ref=batch, entry_id=1,
                reason="规则覆盖不足", final_status="待复核")

        threads = [threading.Thread(target=write_receipt) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        receipts = store.quarantine_ledger.list_by_batch(batch)
        assert len(receipts) == 1, "并发写入只能保留一个版本的回执"
        assert receipts[0].record_key.endswith(":1")


# ── 4. 低覆盖批次冲突处理：两种方案都只有一个最终质量状态 ──


class TestLowCoverageResolution:
    def _make_low_coverage_batch(self, svc, add_rule, sample_rate="0.0"):
        add_rule("低比例规则", "名称", "non_empty", sample_rate)
        batch = _unique_batch()
        records = [{"名称": f"产品{i}"} for i in range(3)]
        _run_verify(svc, batch, records, rng=_steady_rng())
        return batch

    def test_resample_replaces_report_single_final_status(self, verify_env):
        """整批重抽后替换报告：批次只保留一个最终质量状态。"""
        svc, add_rule = verify_env
        batch = self._make_low_coverage_batch(svc, add_rule)
        report_v1 = store.quality_reports.current(batch)
        assert report_v1.status == "待复核"
        assert report_v1.version == 1

        # 调全抽样比例后整批重抽
        for row in store.rows("result_verify"):
            if row.get("规则名称") == "低比例规则":
                row["抽样比例"] = "1.0"
        result = svc.resolve_low_coverage_batch(batch, "resample",
                                                rng=_steady_rng())

        assert result["ok"] is True
        assert result["final_status"] == "通过"
        report_v2 = store.quality_reports.current(batch)
        assert report_v2.version == 2, "替换报告应升版"
        assert report_v2.status == "通过", "最终质量状态唯一"
        assert report_v2.uncovered == 0
        # 重抽通过后，谱系与隔离工作台不再有该批次待复核
        assert len(store.quality_lineage.list_pending(batch)) == 0
        assert len(store.quarantine_ledger.list_pending_review(batch)) == 0
        assert len(store.dirty_vault.list_pending_review(batch)) == 0

    def test_append_keeps_report_and_appends_uncovered(self, verify_env):
        """保留旧报告并追加未覆盖清单：同样只产生一个最终质量状态。"""
        svc, add_rule = verify_env
        batch = self._make_low_coverage_batch(svc, add_rule)
        report_v1 = store.quality_reports.current(batch)
        assert report_v1.status == "待复核"

        result = svc.resolve_low_coverage_batch(batch, "append")
        assert result["ok"] is True
        assert result["final_status"] == "待复核", "追加方案不改变最终质量状态"
        report_v2 = store.quality_reports.current(batch)
        assert report_v2.version == 2
        assert report_v2.strategy == "append"
        assert len(report_v2.uncovered_list) == 3, "未覆盖清单应完整追加"
        # 仍然只有一个当前报告
        assert store.quality_reports.current(batch) is report_v2

    def test_both_strategies_yield_single_status(self, verify_env):
        """两个可选处理方案都只能产生一个最终质量状态。"""
        svc, add_rule = verify_env
        batch_a = self._make_low_coverage_batch(svc, add_rule)
        batch_b = self._make_low_coverage_batch(svc, add_rule)

        svc.resolve_low_coverage_batch(batch_a, "append")
        svc.resolve_low_coverage_batch(batch_b, "append")
        svc.resolve_low_coverage_batch(batch_b, "resample", rng=_steady_rng())

        for batch in (batch_a, batch_b):
            current = store.quality_reports.current(batch)
            assert current is not None
            assert current.status in ("通过", "未通过", "待复核")
        # 台账中每批次只有一条当前报告
        versions = store.quality_reports.snapshot()["versions"]
        assert batch_a in versions and batch_b in versions

    def test_resolve_unknown_batch_and_strategy(self, verify_env):
        svc, _ = verify_env
        assert svc.resolve_low_coverage_batch("BATC-不存在", "resample")["ok"] is False
        batch = _unique_batch()
        store.quality_reports.publish(
            batch, status="待复核", passed=0, failed=0, uncovered=1,
            sampled_records=0, unsampled_records=1)
        assert svc.resolve_low_coverage_batch(batch, "unknown")["ok"] is False


# ── 5. 校验工作台：抽样数、执行数、覆盖状态 ──


class TestVerifyWorkbench:
    def test_rule_coverage_stats(self, verify_env):
        """用户可在校验工作台看到每条规则的抽样数、执行数和覆盖状态。"""
        svc, add_rule = verify_env
        rule_a = add_rule("全量规则", "名称", "non_empty", "1.0")
        rule_b = add_rule("零抽样规则", "金额", "numeric", "0.0")
        batch = _unique_batch()
        records = [{"名称": f"产品{i}", "金额": str(100 + i)} for i in range(3)]
        _run_verify(svc, batch, records, rng=_steady_rng())

        stats = {s["rule_id"]: s for s in svc.get_rule_coverage_stats()}
        assert stats[rule_a]["抽样数"] == 3
        assert stats[rule_a]["执行数"] == 3
        assert stats[rule_a]["覆盖状态"] == "已覆盖"
        assert stats[rule_b]["抽样数"] == 0, "抽样跳过的规则不增加命中计数"
        assert stats[rule_b]["执行数"] == 0
        assert stats[rule_b]["覆盖状态"] == "未覆盖"

    def test_receipt_detail_kept(self, verify_env):
        """保留一份校验收据明细：逐规则抽样/执行/通过事实可核对。"""
        svc, add_rule = verify_env
        add_rule("全量规则", "名称", "non_empty", "1.0")
        add_rule("零抽样规则", "金额", "numeric", "0.0")
        batch = _unique_batch()
        _run_verify(svc, batch, [{"名称": "产品1", "金额": "100"}],
                    rng=_steady_rng())

        receipts = svc.get_validation_receipts(batch)
        assert len(receipts) == 1
        details = receipts[0]["rule_details"]
        assert len(details) == 2, "每条规则的校验事实都应保留"
        by_coverage = {d["coverage"] for d in details}
        assert "已执行" in by_coverage
        assert "未抽样" in by_coverage
        executed = [d for d in details if d["executed"]]
        assert executed[0]["passed"] is True

    def test_quarantine_reason_matches_final_status(self, verify_env):
        """隔离原因与当前最终质量状态保持一致。"""
        svc, add_rule = verify_env
        add_rule("零抽样规则", "名称", "non_empty", "0.0")
        batch = _unique_batch()
        _run_verify(svc, batch, [{"名称": "产品1"}], rng=_steady_rng())

        reviews = svc.get_quarantine_review_list(batch)
        assert len(reviews) == 1
        assert reviews[0]["final_status"] == "待复核"
        assert reviews[0]["review_status"] == "待复核"
        assert "覆盖不足" in reviews[0]["reason"], "隔离原因应反映覆盖缺口"

        lineage = store.quality_lineage.list_by_batch(batch)[0]
        assert reviews[0]["final_status"] == lineage.final_status

    def test_quality_list_matches_quarantine_workbench(self, verify_env):
        """质量列表与隔离工作台的待复核记录对得上（同一份覆盖事实）。"""
        svc, add_rule = verify_env
        add_rule("零抽样规则", "名称", "non_empty", "0.0")
        batch = _unique_batch()
        _run_verify(svc, batch, [{"名称": "产品1"}, {"名称": "产品2"}],
                    rng=_steady_rng())

        consistency = svc.check_consistency(batch)
        assert consistency["consistent"] is True
        assert consistency["lineage_pending"] == 2
        assert consistency["receipts_pending"] == 2
        assert consistency["vault_pending_review"] == 2


# ── 6. 跨业务面同步待办 ──


class TestCrossModuleTodoSync:
    def test_rule_rows_sync_with_ledger(self, verify_env):
        """业务清单与结果台账保持一致：规则待办随谱系待复核同步。"""
        svc, add_rule = verify_env
        rule_id = add_rule("零抽样规则", "名称", "non_empty", "0.0")
        batch = _unique_batch()
        _run_verify(svc, batch, [{"名称": "产品1"}], rng=_steady_rng())

        row = store.find("result_verify", rule_id)
        assert row["pending"] is True, "存在待复核谱系时规则应置为待处理"

        # 整批重抽通过后，待办解除
        for r in store.rows("result_verify"):
            if r.get("id") == rule_id:
                r["抽样比例"] = "1.0"
        svc.resolve_low_coverage_batch(batch, "resample", rng=_steady_rng())
        assert store.find("result_verify", rule_id)["pending"] is False

    def test_batch_tracker_synced(self, verify_env):
        """批次追踪台账随最终质量状态同步校验状态与待办。"""
        svc, add_rule = verify_env
        add_rule("零抽样规则", "名称", "non_empty", "0.0")
        batch = _unique_batch()
        tracker_rows = store.rows("batch_tracker")
        tracker_rows.append({
            "id": max((int(r.get("id", 0)) for r in tracker_rows), default=0) + 1,
            "批次编号": batch, "触发流程": "测试流程", "采集时间": "2026-11-18",
            "目标条数": "1", "成功条数": "1", "遗漏条数": "0",
            "校验状态": "校验中", "status": "校验中",
            "pending": False, "abnormal": False,
        })
        try:
            _run_verify(svc, batch, [{"名称": "产品1"}], rng=_steady_rng())
            tracker = [r for r in tracker_rows if r.get("批次编号") == batch][0]
            assert tracker["校验状态"] == "待复核"
            assert tracker["pending"] is True
        finally:
            tracker_rows.remove(tracker)


# ── 7. 谱系数据集与阈值口径 ──


class TestLineageDataset:
    def test_lineage_dataset_has_coverage_status(self, verify_env):
        """生成带覆盖状态的采集质量谱系数据集。"""
        svc, add_rule = verify_env
        add_rule("全量规则", "名称", "non_empty", "1.0")
        add_rule("零抽样规则", "金额", "numeric", "0.0")
        batch = _unique_batch()
        _run_verify(svc, batch, [{"名称": "产品1", "金额": "100"}],
                    rng=_steady_rng())

        entries = store.quality_lineage.list_by_batch(batch)
        assert len(entries) == 1
        entry = entries[0]
        assert entry.coverage_status == "部分覆盖"
        assert entry.coverage_ratio == 0.5
        assert entry.pending is True, "待复核谱系应标记为待处理"
        assert entry.version == 1

    def test_lineage_upsert_single_version(self, verify_env):
        """同一记录再次校验时谱系升版替换，只保留一个当前版本。"""
        svc, add_rule = verify_env
        add_rule("零抽样规则", "名称", "non_empty", "0.0")
        batch = _unique_batch()
        _run_verify(svc, batch, [{"名称": "产品1"}], rng=_steady_rng())
        svc.resolve_low_coverage_batch(batch, "resample", rng=_steady_rng())

        entries = store.quality_lineage.list_by_batch(batch)
        assert len(entries) == 1, "同一记录只保留一条谱系"
        assert entries[0].version == 2

    def test_coverage_threshold_scopes_rule_range(self, verify_env):
        """按阈值口径判定规则范围：覆盖率低于阈值不能判定通过。"""
        svc, add_rule = verify_env
        add_rule("规则A", "名称", "non_empty", "1.0")
        add_rule("规则B", "金额", "numeric", "0.5")
        # rng 序列：规则A抽中并执行，规则B跳过 → 覆盖率 0.5 < 阈值 1.0
        draws = iter([0.1, 0.5, 0.9])
        quality = svc._validate_record(
            {"名称": "产品1", "金额": "100"}, svc._get_active_rules(),
            rng=lambda: next(draws, 0.5))
        assert quality["coverage_ratio"] == 0.5
        assert quality["final_status"] == "待复核"
        assert quality["passed"] is False


# ── 运行入口 ──

if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
