"""结果校验领域服务 — 校验采集结果并触发回调。

数据工程核心：数据校验与去重策略执行 — 跨模块数据产物

数据流:
    channel("rpa.data_collected") ← data_extract
    → result_verify 消费校验 + DirtyDataVault
    → channel("rpa.validation_complete") → callback_hook
"""
from __future__ import annotations

import random
from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


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

    # ---- 跨模块业务方法 ----

    def verify_collected_data(self) -> dict[str, Any]:
        """消费 data_extract 发布的采集结果，执行校验。

        从 channel("rpa.data_collected") 获取采集结果批次，
        按已启用的校验规则进行逐条校验。

        不通过的记录移入 DirtyDataVault。

        Returns:
            {"verified": int, "passed": int, "failed": int, "batches": [...]}
        """
        msgs = store.channel.consume(
            consumer=self.MODULE,
            topic="rpa.data_collected",
        )
        if not msgs:
            return {"verified": 0, "passed": 0, "failed": 0, "batches": []}

        batches = []
        total_passed = 0
        total_failed = 0

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
            passed, failed = 0, 0

            for record_entry in records:
                quality = self._validate_record(
                    record_entry.fields, verify_rules)
                if quality["passed"]:
                    store.collect_records.mark_validated(
                        record_entry.entry_id, quality["score"])
                    passed += 1
                else:
                    failed += 1
                    # 移入脏数据区
                    store.dirty_vault.quarantine(
                        source=self.MODULE,
                        batch_ref=batch_ref,
                        raw_data=dict(record_entry.fields),
                        reason=quality["reason"],
                    )

            total_passed += passed
            total_failed += failed

            # 发布校验完成事件
            store.channel.publish(
                topic="rpa.validation_complete",
                source=self.MODULE,
                payload={
                    "batch_ref": batch_ref,
                    "site": batch_data.get("site"),
                    "passed": passed,
                    "failed": failed,
                    "rules_applied": len(verify_rules),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                },
            )
            store.channel.confirm(msg.msg_id, self.MODULE)

            batches.append({
                "batch_ref": batch_ref,
                "passed": passed,
                "failed": failed,
                "total": len(records),
            })

        return {
            "verified": total_passed + total_failed,
            "passed": total_passed,
            "failed": total_failed,
            "batches": batches,
        }

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
    ) -> dict[str, Any]:
        """按规则校验单条采集记录。

        Returns:
            {"passed": bool, "score": float, "reason": str}
        """
        if not fields:
            return {"passed": False, "score": 0.0, "reason": "空记录"}

        applied = 0
        failed = 0
        reasons = []

        for rule in rules:
            if random.random() > rule["sample_rate"]:
                continue  # 抽样跳过

            field_name = rule["field"]
            condition = rule["condition"]

            # 模拟校验逻辑
            if field_name in fields:
                value = fields[field_name]
                applied += 1

                if condition == "non_empty" and not str(value).strip():
                    failed += 1
                    reasons.append(f"字段[{field_name}]为空")
                elif condition == "numeric" and not str(value).replace(".", "").isdigit():
                    failed += 1
                    reasons.append(f"字段[{field_name}]不是数值")
                elif condition == "length_gt_0" and len(str(value)) == 0:
                    failed += 1
                    reasons.append(f"字段[{field_name}]长度为零")
                # 模拟额外场景
                elif random.random() < 0.08:
                    failed += 1
                    reasons.append(f"字段[{field_name}]格式异常")
            else:
                # 字段缺失
                failed += 1
                reasons.append(f"缺少字段[{field_name}]")

        if applied == 0:
            # 无匹配字段视为通过
            return {"passed": True, "score": 1.0, "reason": "无校验规则匹配"}

        passed = failed == 0
        score = 1.0 - (failed / max(applied, 1)) if applied > 0 else 0.0
        return {
            "passed": passed,
            "score": round(score, 2),
            "reason": "; ".join(reasons) if reasons else "通过",
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
            校验结果
        """
        all_rules = self._get_active_rules()
        if rule_ids:
            rules = [r for r in all_rules if r["id"] in rule_ids]
        else:
            rules = all_rules

        return self._validate_record(record, rules)

    def get_validation_stats(self) -> dict[str, Any]:
        """获取校验统计概览。"""
        validate_events = store.channel.snapshot(topic="rpa.validation_complete")
        total_passed = sum(
            e.get("payload", {}).get("passed", 0) for e in validate_events)
        total_failed = sum(
            e.get("payload", {}).get("failed", 0) for e in validate_events)
        return {
            "total_validation_events": len(validate_events),
            "total_passed": total_passed,
            "total_failed": total_failed,
            "dirty_vault": store.dirty_vault.snapshot(),
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        stats = self.get_validation_stats()
        return {**base, **stats}