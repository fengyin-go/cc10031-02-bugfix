"""结果校验领域服务 — 校验采集结果并触发回调。

数据工程核心：数据校验与去重策略执行 — 跨模块数据产物

数据流:
    channel("rpa.data_collected") ← data_extract
    → result_verify 消费校验 + DirtyDataVault + QualityLineageStore
    → channel("rpa.validation_complete") → callback_hook

校验口径（修复后）：
- 抽样跳过的规则不计入命中（执行数），未执行的规则不能被当作通过；
- 抽样记录与未抽样记录分开计数，覆盖不足的记录进入待复核而不是通过；
- 校验分支与隔离回执共用同一份采集质量谱系，质量列表与隔离工作台一致；
- 同一记录再次校验时隔离区不重复出现，隔离回执每条记录唯一。
"""
from __future__ import annotations

import random
from datetime import datetime, timezone
from typing import Any, Callable

from app.services.base import BaseModuleService
from app.store import (
    COVERAGE_FULL,
    COVERAGE_NONE,
    COVERAGE_PARTIAL,
    QUALITY_FAILED,
    QUALITY_PASSED,
    QUALITY_PENDING,
    store,
)


def _safe_float(value: str, default: float = 1.0) -> float:
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


class ResultVerifyService(BaseModuleService):
    MODULE = "result_verify"
    ENTITY = "校验规则"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["规则名称", "校验字段", "校验条件", "期望模式", "异常动作", "抽样比例", "关联流程"]
    REQUIRED_FIELDS = ["规则名称", "校验字段", "校验条件"]
    STATUS_ORDER = ["草稿", "已启用", "已关闭"]
    ACTION_RULES = {"启用规则": "草稿", "关闭规则": "已启用"}
    NEGATIVE_ACTIONS = []

    # 处理项乙：按阈值口径判定规则范围 —— 覆盖率低于阈值时规则执行范围
    # 不足以判定通过，记录只能进入待复核，不能被当作通过。
    COVERAGE_THRESHOLD = 1.0

    # ---- 跨模块业务方法 ----

    def verify_collected_data(self, *,
                              rng: Callable[[], float] | None = None) -> dict[str, Any]:
        """消费 data_extract 发布的采集结果，执行校验。

        从 channel("rpa.data_collected") 获取采集结果批次，
        按已启用的校验规则进行逐条校验，并写入采集质量谱系。

        不通过的记录移入 DirtyDataVault（按记录去重），并为每条记录
        保存唯一隔离回执；覆盖不足（待复核）的记录同样隔离待复核，
        不计入通过。

        Returns:
            {"verified": int, "passed": int, "failed": int,
             "uncovered": int, "sampled_records": int,
             "unsampled_records": int, "batches": [...]}
        """
        msgs = store.channel.consume(
            consumer=self.MODULE,
            topic="rpa.data_collected",
        )
        if not msgs:
            return {"verified": 0, "passed": 0, "failed": 0, "uncovered": 0,
                    "sampled_records": 0, "unsampled_records": 0, "batches": []}

        batches = []
        total_passed = 0
        total_failed = 0
        total_uncovered = 0
        total_sampled_records = 0
        total_unsampled_records = 0

        for msg in msgs:
            batch_data = msg.payload
            batch_ref = batch_data.get("batch_ref", "")

            # 获取已启用的校验规则
            verify_rules = self._get_active_rules()
            if not verify_rules:
                store.channel.confirm(msg.msg_id, self.MODULE)
                continue

            # 对批次中的记录进行校验
            records = store.collect_records.list_by_batch(batch_ref)
            summary = self._verify_batch_records(
                batch_ref, batch_data, records, verify_rules, rng=rng)

            total_passed += summary["passed"]
            total_failed += summary["failed"]
            total_uncovered += summary["uncovered"]
            total_sampled_records += summary["sampled_records"]
            total_unsampled_records += summary["unsampled_records"]
            batches.append(summary)

            store.channel.confirm(msg.msg_id, self.MODULE)

        return {
            "verified": total_passed + total_failed + total_uncovered,
            "passed": total_passed,
            "failed": total_failed,
            "uncovered": total_uncovered,
            "sampled_records": total_sampled_records,
            "unsampled_records": total_unsampled_records,
            "batches": batches,
        }

    def _verify_batch_records(
        self,
        batch_ref: str,
        batch_data: dict[str, Any],
        records: list[Any],
        verify_rules: list[dict[str, Any]],
        *,
        rng: Callable[[], float] | None = None,
        force_full_sample: bool = False,
        strategy: str | None = None,
    ) -> dict[str, Any]:
        """逐条校验批次记录，写谱系/隔离区/回执/报告，返回批次汇总。"""
        passed = failed = uncovered = 0
        sampled_records = unsampled_records = 0
        uncovered_list: list[int] = []

        for record_entry in records:
            quality = self._validate_record(
                record_entry.fields, verify_rules,
                rng=rng, force_full_sample=force_full_sample)

            # 采集质量谱系：同一记录再次校验时升版替换（唯一当前版本）
            store.quality_lineage.upsert(
                entry_id=record_entry.entry_id,
                batch_ref=batch_ref,
                rules_total=quality["rules_total"],
                rules_sampled=quality["rules_sampled"],
                rules_executed=quality["rules_executed"],
                rules_skipped=quality["rules_skipped"],
                rules_failed=quality["rules_failed"],
                coverage_ratio=quality["coverage_ratio"],
                coverage_status=quality["coverage_status"],
                final_status=quality["final_status"],
                rule_details=quality["rule_details"],
            )

            # 抽样记录与未抽样记录分开计数
            if quality["rules_sampled"] > 0:
                sampled_records += 1
            else:
                unsampled_records += 1

            final_status = quality["final_status"]
            if final_status == QUALITY_PASSED:
                store.collect_records.mark_validated(
                    record_entry.entry_id, quality["score"])
                passed += 1
                # 重抽通过后解除隔离，回执同步为已复核
                self._release_quarantine(record_entry.entry_id, batch_ref)
            else:
                if final_status == QUALITY_FAILED:
                    failed += 1
                else:
                    uncovered += 1
                    uncovered_list.append(record_entry.entry_id)
                # 隔离区按记录去重；回执唯一，原因与最终质量状态一致
                store.dirty_vault.quarantine(
                    source=self.MODULE,
                    batch_ref=batch_ref,
                    raw_data=dict(record_entry.fields),
                    reason=quality["reason"],
                    entry_id=record_entry.entry_id,
                    final_status=final_status,
                )
                store.quarantine_ledger.save_receipt(
                    source=self.MODULE,
                    batch_ref=batch_ref,
                    entry_id=record_entry.entry_id,
                    reason=quality["reason"],
                    final_status=final_status,
                )

        # 批次最终质量状态：有未通过则未通过，其次有待复核，否则通过
        if failed > 0:
            batch_status = QUALITY_FAILED
        elif uncovered > 0:
            batch_status = QUALITY_PENDING
        else:
            batch_status = QUALITY_PASSED

        # 质量报告台账：每批次只保留一个当前最终质量状态
        store.quality_reports.publish(
            batch_ref,
            status=batch_status,
            passed=passed,
            failed=failed,
            uncovered=uncovered,
            sampled_records=sampled_records,
            unsampled_records=unsampled_records,
            uncovered_list=uncovered_list,
            strategy=strategy,
        )

        # 跨业务面同步更新待办：规则清单、批次台账与结果台账保持一致
        self._sync_cross_module_todos(batch_ref, batch_status)

        # 发布校验完成事件
        store.channel.publish(
            topic="rpa.validation_complete",
            source=self.MODULE,
            payload={
                "batch_ref": batch_ref,
                "site": batch_data.get("site"),
                "passed": passed,
                "failed": failed,
                "uncovered": uncovered,
                "sampled_records": sampled_records,
                "unsampled_records": unsampled_records,
                "final_status": batch_status,
                "rules_applied": len(verify_rules),
                "strategy": strategy,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
        )

        return {
            "batch_ref": batch_ref,
            "passed": passed,
            "failed": failed,
            "uncovered": uncovered,
            "sampled_records": sampled_records,
            "unsampled_records": unsampled_records,
            "final_status": batch_status,
            "total": len(records),
        }

    def _release_quarantine(self, entry_id: int, batch_ref: str) -> None:
        """记录重抽通过后解除隔离：回执与隔离区同步为已复核/已恢复。"""
        for rec in store.dirty_vault.list_by_batch(batch_ref):
            if rec.entry_id == entry_id and rec.quarantined:
                store.dirty_vault.mark_recovered(rec.record_id, "resample_passed")
        key = store.quarantine_ledger.make_record_key(
            self.MODULE, batch_ref, entry_id)
        receipt = store.quarantine_ledger.get(key)
        if receipt is not None:
            store.quarantine_ledger.save_receipt(
                source=self.MODULE, batch_ref=batch_ref, entry_id=entry_id,
                reason="重抽后通过", final_status=QUALITY_PASSED)

    def _sync_cross_module_todos(self, batch_ref: str, batch_status: str) -> None:
        """跨业务面同步更新待办（处理项乙）。

        - 结果校验规则清单：存在待复核谱系的规则置为待处理，
          无待复核的启用规则解除待处理，使业务清单与结果台账一致；
        - 批次追踪台账：按批次号同步校验状态与待办标记。
        """
        pending_lineage = store.quality_lineage.list_pending(batch_ref)
        pending_rule_ids = {
            detail.get("rule_id")
            for entry in pending_lineage
            for detail in entry.rule_details
            if not detail.get("executed")
        }
        for row in store.rows(self.MODULE):
            if row.get("status") != "已启用":
                continue
            row["pending"] = row.get("id") in pending_rule_ids

        for row in store.rows("batch_tracker"):
            if row.get("批次编号") == batch_ref:
                row["校验状态"] = batch_status
                row["pending"] = batch_status != QUALITY_PASSED

    # ---- 低覆盖批次冲突处理（两种方案都只产生一个最终质量状态） ----

    def resolve_low_coverage_batch(
        self,
        batch_ref: str,
        strategy: str = "resample",
        *,
        rng: Callable[[], float] | None = None,
    ) -> dict[str, Any]:
        """处理低覆盖批次与已发布质量结果的先后冲突。

        两种可选处理方案：
        - resample：整批重抽（按全量比例重新抽样校验）后替换报告；
        - append：保留旧报告，追加未覆盖清单。

        无论选择哪种方案，质量报告台账同一批次只保留一个当前版本，
        即只产生一个最终质量状态。
        """
        report = store.quality_reports.current(batch_ref)
        if report is None:
            return {"ok": False, "error": f"批次 {batch_ref} 没有已发布的质量报告"}

        if strategy == "resample":
            verify_rules = self._get_active_rules()
            records = store.collect_records.list_by_batch(batch_ref)
            summary = self._verify_batch_records(
                batch_ref, {"batch_ref": batch_ref}, records, verify_rules,
                rng=rng, force_full_sample=True, strategy="resample")
            current = store.quality_reports.current(batch_ref)
            return {
                "ok": True,
                "batch_ref": batch_ref,
                "strategy": "resample",
                "final_status": current.status if current else summary["final_status"],
                "report_version": current.version if current else 1,
                "summary": summary,
            }

        if strategy == "append":
            pending = store.quality_lineage.list_pending(batch_ref)
            uncovered_ids = [entry.entry_id for entry in pending]
            updated = store.quality_reports.append_uncovered(batch_ref, uncovered_ids)
            if updated is None:
                return {"ok": False, "error": f"批次 {batch_ref} 没有已发布的质量报告"}
            store.channel.publish(
                topic="rpa.validation_complete",
                source=self.MODULE,
                payload={
                    "batch_ref": batch_ref,
                    "passed": updated.passed,
                    "failed": updated.failed,
                    "uncovered": updated.uncovered,
                    "sampled_records": updated.sampled_records,
                    "unsampled_records": updated.unsampled_records,
                    "final_status": updated.status,
                    "strategy": "append",
                    "uncovered_list": updated.uncovered_list,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                },
            )
            return {
                "ok": True,
                "batch_ref": batch_ref,
                "strategy": "append",
                "final_status": updated.status,
                "report_version": updated.version,
                "uncovered_list": updated.uncovered_list,
            }

        return {"ok": False, "error": f"未知处理方案: {strategy}（可选 resample / append）"}

    # ---- 校验工作台 ----

    def get_rule_coverage_stats(self) -> list[dict[str, Any]]:
        """每条已启用规则的抽样数、执行数和覆盖状态（校验工作台口径）。"""
        stats: dict[Any, dict[str, Any]] = {}
        for rule in self._get_active_rules():
            stats[rule["id"]] = {
                "rule_id": rule["id"],
                "规则名称": rule["name"],
                "校验字段": rule["field"],
                "抽样比例": rule["sample_rate"],
                "抽样数": 0,
                "执行数": 0,
                "跳过数": 0,
                "未通过数": 0,
                "覆盖率": 0.0,
                "覆盖状态": COVERAGE_NONE,
            }
        for entry in self._lineage_entries():
            for detail in entry.rule_details:
                slot = stats.get(detail.get("rule_id"))
                if slot is None:
                    continue
                if detail.get("sampled"):
                    slot["抽样数"] += 1
                else:
                    slot["跳过数"] += 1
                if detail.get("executed"):
                    slot["执行数"] += 1
                if detail.get("passed") is False:
                    slot["未通过数"] += 1
        for slot in stats.values():
            sampled = slot["抽样数"]
            ratio = slot["执行数"] / sampled if sampled else 0.0
            slot["覆盖率"] = round(ratio, 2)
            if sampled and ratio >= self.COVERAGE_THRESHOLD:
                slot["覆盖状态"] = COVERAGE_FULL
            elif slot["执行数"] > 0:
                slot["覆盖状态"] = COVERAGE_PARTIAL
            else:
                slot["覆盖状态"] = COVERAGE_NONE
        return list(stats.values())

    def get_validation_receipts(self, batch_ref: str | None = None) -> list[dict[str, Any]]:
        """校验收据明细：每条记录逐规则的抽样/执行/通过事实。"""
        receipts = []
        for entry in self._lineage_entries(batch_ref):
            receipts.append({
                "lineage_id": entry.lineage_id,
                "entry_id": entry.entry_id,
                "batch_ref": entry.batch_ref,
                "rules_total": entry.rules_total,
                "rules_sampled": entry.rules_sampled,
                "rules_executed": entry.rules_executed,
                "rules_skipped": entry.rules_skipped,
                "coverage_status": entry.coverage_status,
                "final_status": entry.final_status,
                "version": entry.version,
                "updated_at": entry.updated_at,
                "rule_details": entry.rule_details,
            })
        return receipts

    def get_quarantine_review_list(self, batch_ref: str | None = None) -> list[dict[str, Any]]:
        """隔离工作台待复核记录：与采集质量谱系同源，保证两边对得上。"""
        reviews = []
        for receipt in store.quarantine_ledger.list_pending_review(batch_ref):
            reviews.append({
                "receipt_id": receipt.receipt_id,
                "record_key": receipt.record_key,
                "entry_id": receipt.entry_id,
                "batch_ref": receipt.batch_ref,
                "reason": receipt.reason,
                "final_status": receipt.final_status,
                "review_status": receipt.review_status,
                "version": receipt.version,
                "written_at": receipt.written_at,
            })
        return reviews

    def check_consistency(self, batch_ref: str | None = None) -> dict[str, Any]:
        """核对质量列表与隔离工作台的待复核记录是否一致。"""
        lineage_pending = len(store.quality_lineage.list_pending(batch_ref))
        receipts_pending = len(store.quarantine_ledger.list_pending_review(batch_ref))
        vault_pending = len(store.dirty_vault.list_pending_review(batch_ref))
        return {
            "consistent": lineage_pending == receipts_pending == vault_pending,
            "lineage_pending": lineage_pending,
            "receipts_pending": receipts_pending,
            "vault_pending_review": vault_pending,
        }

    def _lineage_entries(self, batch_ref: str | None = None):
        if batch_ref:
            return store.quality_lineage.list_by_batch(batch_ref)
        return store.quality_lineage.list_all()

    # ---- 规则与单条校验 ----

    def _get_active_rules(self) -> list[dict[str, Any]]:
        """获取已启用的校验规则。"""
        rows = store.rows(self.MODULE)
        return [
            {
                "id": r.get("id"),
                "name": r.get("规则名称"),
                "field": r.get("校验字段"),
                "condition": r.get("校验条件"),
                "pattern": r.get("期望模式", "non_empty"),
                "action_on_fail": r.get("异常动作", "quarantine"),
                "sample_rate": _safe_float(r.get("抽样比例", "1.0")),
                "bot_ref": r.get("关联流程", ""),
            }
            for r in rows if r.get("status") == "已启用"
        ]

    def _validate_record(
        self,
        fields: dict[str, Any],
        rules: list[dict[str, Any]],
        *,
        rng: Callable[[], float] | None = None,
        force_full_sample: bool = False,
    ) -> dict[str, Any]:
        """按规则校验单条采集记录，输出覆盖状态与校验收据明细。

        口径：抽样跳过的规则不增加命中计数；一条规则只有真正对字段值
        执行了校验才算执行；未执行任何规则或覆盖低于阈值的记录不能被
        当作通过，只能进入待复核。

        Returns:
            {"passed": bool, "final_status": str, "score": float,
             "reason": str, "rules_total": int, "rules_sampled": int,
             "rules_executed": int, "rules_skipped": int,
             "rules_failed": int, "coverage_ratio": float,
             "coverage_status": str, "rule_details": [...]}
        """
        draw = rng or random.random
        empty_detail = {
            "rules_total": len(rules), "rules_sampled": 0, "rules_executed": 0,
            "rules_skipped": 0, "rules_failed": 0, "coverage_ratio": 0.0,
            "coverage_status": COVERAGE_NONE, "rule_details": [],
        }
        if not fields:
            return {"passed": False, "final_status": QUALITY_FAILED, "score": 0.0,
                    "reason": "空记录", **empty_detail}
        if not rules:
            return {"passed": False, "final_status": QUALITY_PENDING, "score": 0.0,
                    "reason": "无已启用校验规则，覆盖不足", **empty_detail}

        sampled = 0
        executed = 0
        failed = 0
        reasons: list[str] = []
        rule_details: list[dict[str, Any]] = []

        for rule in rules:
            detail: dict[str, Any] = {
                "rule_id": rule["id"],
                "rule_name": rule["name"],
                "field": rule["field"],
                "sampled": False,
                "executed": False,
                "passed": None,
                "coverage": "未抽样",
                "note": "",
            }
            effective_rate = 1.0 if force_full_sample else rule["sample_rate"]
            if draw() > effective_rate:
                # 抽样跳过：不增加命中计数
                rule_details.append(detail)
                continue

            sampled += 1
            detail["sampled"] = True
            field_name = rule["field"]
            condition = rule["condition"]

            if field_name in fields:
                value = fields[field_name]
                executed += 1
                detail["executed"] = True
                detail["coverage"] = "已执行"

                rule_failed = False
                if condition == "non_empty" and not str(value).strip():
                    rule_failed = True
                    reasons.append(f"字段[{field_name}]为空")
                elif condition == "numeric" and not str(value).replace(".", "").isdigit():
                    rule_failed = True
                    reasons.append(f"字段[{field_name}]不是数值")
                elif condition == "length_gt_0" and len(str(value)) == 0:
                    rule_failed = True
                    reasons.append(f"字段[{field_name}]长度为零")
                elif draw() < 0.08:
                    # 模拟额外场景：格式异常
                    rule_failed = True
                    reasons.append(f"字段[{field_name}]格式异常")

                if rule_failed:
                    failed += 1
                    detail["passed"] = False
                    detail["note"] = reasons[-1]
                else:
                    detail["passed"] = True
                    detail["note"] = "通过"
            else:
                # 字段缺失：规则被抽中但无法执行校验，记为未通过
                failed += 1
                detail["passed"] = False
                detail["coverage"] = "字段缺失"
                detail["note"] = f"缺少字段[{field_name}]"
                reasons.append(f"缺少字段[{field_name}]")

            rule_details.append(detail)

        rules_total = len(rules)
        skipped = rules_total - sampled
        coverage_ratio = executed / rules_total if rules_total else 0.0

        if coverage_ratio >= self.COVERAGE_THRESHOLD:
            coverage_status = COVERAGE_FULL
        elif executed == 0:
            coverage_status = COVERAGE_NONE
        else:
            coverage_status = COVERAGE_PARTIAL

        # 未执行的规则不能被当作通过：覆盖不足一律待复核
        if failed > 0:
            final_status = QUALITY_FAILED
        elif coverage_status == COVERAGE_FULL:
            final_status = QUALITY_PASSED
        else:
            final_status = QUALITY_PENDING
            reasons.append(
                f"规则覆盖不足：执行{executed}/共{rules_total}条，未执行规则不计为通过")

        score = 1.0 - (failed / executed) if executed > 0 else 0.0
        return {
            "passed": final_status == QUALITY_PASSED,
            "final_status": final_status,
            "score": round(score, 2),
            "reason": "; ".join(reasons) if reasons else "通过",
            "rules_total": rules_total,
            "rules_sampled": sampled,
            "rules_executed": executed,
            "rules_skipped": skipped,
            "rules_failed": failed,
            "coverage_ratio": round(coverage_ratio, 2),
            "coverage_status": coverage_status,
            "rule_details": rule_details,
        }

    def run_single_validate(
        self,
        record: dict[str, Any],
        rule_ids: list[int] | None = None,
    ) -> dict[str, Any]:
        """对单条记录执行指定规则校验。

        Args:
            record: 待校验记录
            rule_ids: 要使用的规则 ID 列表（None = 所有已启用）

        Returns:
            校验结果（含覆盖状态与收据明细）
        """
        all_rules = self._get_active_rules()
        if rule_ids:
            rules = [r for r in all_rules if r["id"] in rule_ids]
        else:
            rules = all_rules

        return self._validate_record(record, rules)

    def get_validation_stats(self) -> dict[str, Any]:
        """获取校验统计概览（含覆盖口径与台账一致性）。"""
        validate_events = store.channel.snapshot(topic="rpa.validation_complete")
        total_passed = sum(
            e.get("payload", {}).get("passed", 0) for e in validate_events)
        total_failed = sum(
            e.get("payload", {}).get("failed", 0) for e in validate_events)
        total_uncovered = sum(
            e.get("payload", {}).get("uncovered", 0) for e in validate_events)
        return {
            "total_validation_events": len(validate_events),
            "total_passed": total_passed,
            "total_failed": total_failed,
            "total_uncovered": total_uncovered,
            "dirty_vault": store.dirty_vault.snapshot(),
            "quality_lineage": store.quality_lineage.snapshot(),
            "quarantine_ledger": store.quarantine_ledger.snapshot(),
            "quality_reports": store.quality_reports.snapshot(),
            "consistency": self.check_consistency(),
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        stats = self.get_validation_stats()
        return {**base, **stats}
