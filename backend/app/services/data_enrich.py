"""数据补全领域服务 — 从外部数据源填充缺失字段，输出 domain_snapshot。

数据工程核心：数据聚合计算与指标产出 — 跨模块数据产物
"""
from __future__ import annotations

import random
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class DataEnrichService(BaseModuleService):
    MODULE = "data_enrich"
    ENTITY = "补全规则"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["规则名称", "目标字段", "补全来源", "匹配键", "补全方式", "缺失处理", "数据时效"]
    REQUIRED_FIELDS = ["规则名称", "目标字段", "补全来源"]
    STATUS_ORDER = ["草稿", "已生效", "已停用"]
    ACTION_RULES = {"启用规则": "草稿", "停用规则": "已生效"}
    NEGATIVE_ACTIONS = ["停用规则"]

    def enrich_records(self, batch_ref: str) -> dict[str, Any]:
        """对指定批次的采集记录执行数据补全。

        模拟从外部数据源（如 API、缓存库）补全缺失字段。

        Returns:
            {"batch_ref": str, "enriched": int, "skipped": int}
        """
        records = store.collect_records.list_by_batch(batch_ref)
        active_rules = self._get_active_rules()

        enriched = 0
        skipped = 0
        for record in records:
            fields = record.fields
            needs_enrich = any(
                rule["field"] not in fields or not str(fields.get(rule["field"], "")).strip()
                for rule in active_rules
            )
            if needs_enrich:
                for rule in active_rules:
                    if rule["field"] not in fields or not str(fields.get(rule["field"], "")).strip():
                        # 模拟从外部数据源补全
                        fields[rule["field"]] = self._generate_fill_value(rule)
                enriched += 1
            else:
                skipped += 1

        return {"batch_ref": batch_ref, "enriched": enriched, "skipped": skipped}

    def _get_active_rules(self) -> list[dict[str, Any]]:
        rows = store.rows(self.MODULE)
        return [
            {"field": r.get("目标字段"), "source": r.get("补全来源"),
             "match_key": r.get("匹配键"), "method": r.get("补全方式")}
            for r in rows if r.get("status") == "已生效"
        ]

    @staticmethod
    def _generate_fill_value(rule: dict[str, Any]) -> str:
        """根据规则生成模拟补全值。"""
        source = rule.get("source", "")
        templates = {
            "名称": f"{source}-补全",
            "类型": random.choice(["标准", "特殊", "通用"]),
            "状态": "已补全",
            "金额": str(round(random.uniform(100, 5000), 2)),
            "日期": "2026-01-01",
            "标签": "补全数据",
        }
        field = rule.get("field", "")
        return templates.get(field, f"{source}-补全值")

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        return {
            **base,
            "active_rules": sum(1 for r in rows if r.get("status") == "已生效"),
            "stopped_rules": sum(1 for r in rows if r.get("status") == "已停用"),
        }