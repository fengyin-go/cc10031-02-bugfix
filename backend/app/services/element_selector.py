"""元素定位领域服务 — 管理 UI 元素的定位策略和选择器规则。

数据工程核心：数据标准化与字段映射转换 — 跨模块数据产物

数据流:
    element_selector (管理定位策略) → channel("rpa.locator_strategies")
    → runtime_session 消费执行自动化操作
"""
from __future__ import annotations

from enum import Enum
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class LocatorType(Enum):
    CSS = "css"
    XPATH = "xpath"
    ID = "id"
    TEXT = "text"
    ATTRIBUTE = "attribute"


class ElementSelectorService(BaseModuleService):
    MODULE = "element_selector"
    ENTITY = "定位规则"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["规则名称", "选择器类型", "定位表达式", "匹配策略", "等待条件", "回退方案", "所属流程"]
    REQUIRED_FIELDS = ["规则名称", "选择器类型", "定位表达式"]
    STATUS_ORDER = ["草稿", "已测试", "已启用", "定位失效"]
    ACTION_RULES = {"测试定位": "草稿", "启用规则": "已测试"}
    NEGATIVE_ACTIONS = []

    # ---- 跨模块业务方法 (element_locator 角色) ----

    def publish_locator_strategy(self, rule_id: int) -> dict[str, Any] | None:
        """发布定位规则作为结构化策略，供 runtime_session 消费。

        将定位规则（选择器类型、表达式、匹配策略、回退方案）封装为
        结构化策略写入 channel。

        Returns:
            发布结果概要或 None
        """
        entry = store.find(self.MODULE, rule_id)
        if entry is None:
            return None

        current = entry.get("status")
        if current not in ("已测试", "草稿"):
            return None

        entry["status"] = "已启用"
        entry["pending"] = True

        strategy = {
            "rule_id": rule_id,
            "rule_name": entry.get("规则名称"),
            "selector_type": entry.get("选择器类型", "css"),
            "expression": entry.get("定位表达式", ""),
            "match_strategy": entry.get("匹配策略", "first_match"),
            "wait_condition": entry.get("等待条件", "visible"),
            "fallback": entry.get("回退方案", ""),
            "bot_ref": entry.get("所属流程", ""),
        }

        msg = store.channel.publish(
            topic="rpa.locator_strategies",
            source=self.MODULE,
            payload=strategy,
        )
        return {
            "rule_id": rule_id,
            "status": "已启用",
            "msg_id": msg.msg_id,
            "selector_type": strategy["selector_type"],
        }

    def resolve_element(self, element_spec: str,
                         page_context: dict[str, Any] | None = None) -> dict[str, Any]:
        """解析指定元素的定位策略。

        根据元素名称或表达式，从已启用的定位规则中查找推荐策略。

        Args:
            element_spec: 元素标识（如 "提交按钮", "结果表格" 等）
            page_context: 页面上下文信息

        Returns:
            {"found": bool, "strategies": [...], "recommended": dict | None}
        """
        rows = store.rows(self.MODULE)
        active = [
            r for r in rows if r.get("status") == "已启用"
        ]

        matches = []
        for rule in active:
            rule_name = rule.get("规则名称", "")
            expression = rule.get("定位表达式", "")
            if (element_spec and element_spec in rule_name) or \
               (expression and element_spec in expression):
                matches.append({
                    "rule_id": rule["id"],
                    "name": rule_name,
                    "selector_type": rule.get("选择器类型"),
                    "expression": expression,
                    "match_strategy": rule.get("匹配策略"),
                    "wait_condition": rule.get("等待条件"),
                    "fallback": rule.get("回退方案"),
                })

        return {
            "found": len(matches) > 0,
            "strategies": matches,
            "recommended": matches[0] if matches else None,
        }

    def batch_resolve_elements(self, elements: list[str],
                                bot_id: int | None = None) -> dict[str, Any]:
        """批量解析多个元素的定位策略。

        供 runtime_session 在启动前批量获取所需元素的定位信息。

        Args:
            elements: 元素名称列表
            bot_id: 关联的流程 ID（用于过滤相关策略）

        Returns:
            {"total": int, "resolved": int, "results": [...]}
        """
        resolved = []
        for elem in elements:
            result = self.resolve_element(elem)
            resolved.append({
                "element": elem,
                "found": result["found"],
                "strategy": result["recommended"],
            })

        # 发布批量解析结果到 channel
        if resolved:
            store.channel.publish(
                topic="rpa.elements_resolved",
                source=self.MODULE,
                payload={
                    "bot_id": bot_id,
                    "total": len(elements),
                    "resolved_count": sum(1 for r in resolved if r["found"]),
                    "results": resolved,
                },
            )

        return {
            "total": len(elements),
            "resolved": sum(1 for r in resolved if r["found"]),
            "results": resolved,
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        active_rules = sum(
            1 for r in store.rows(self.MODULE) if r.get("status") == "已启用")
        return {
            **base,
            "active_locator_rules": active_rules,
            "strategies_published": len(
                store.channel.snapshot(topic="rpa.locator_strategies")),
        }