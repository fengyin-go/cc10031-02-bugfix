"""频控策略领域服务 — 采集请求频控规则管理，输出 domain_snapshot。

数据工程核心：数据清洗与转换链路处理 — 跨模块数据产物
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class RateLimiterService(BaseModuleService):
    MODULE = "rate_limiter"
    ENTITY = "频控规则"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["规则名称", "目标站点", "每秒上限", "每日上限", "并发上限", "冷却时间", "优先级"]
    REQUIRED_FIELDS = ["规则名称", "目标站点", "每秒上限"]
    STATUS_ORDER = ["草稿", "已生效", "已暂停", "已超限"]
    ACTION_RULES = {"启用规则": "草稿", "暂停规则": "已生效"}
    NEGATIVE_ACTIONS = []

    def check_rate_limit(self, site: str) -> dict[str, Any]:
        """检查指定站点的频控状态。"""
        rows = store.rows(self.MODULE)
        rules = [
            r for r in rows
            if r.get("status") == "已生效" and site in r.get("目标站点", "")
        ]
        return {
            "site": site,
            "rules_applied": len(rules),
            "rate_limits": [
                {
                    "id": r.get("id"), "name": r.get("规则名称"),
                    "per_second": r.get("每秒上限"), "per_day": r.get("每日上限"),
                    "concurrency": r.get("并发上限"), "cooldown": r.get("冷却时间"),
                }
                for r in rules
            ],
            "over_limit": any(r.get("status") == "已超限" for r in rows if site in r.get("目标站点", "")),
        }

    def get_rate_stats(self) -> dict[str, Any]:
        """获取频控统计概览。"""
        rows = store.rows(self.MODULE)
        return {
            "total_rules": len(rows),
            "active": sum(1 for r in rows if r.get("status") == "已生效"),
            "paused": sum(1 for r in rows if r.get("status") == "已暂停"),
            "over_limit": sum(1 for r in rows if r.get("status") == "已超限"),
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        stats = self.get_rate_stats()
        return {**base, **stats}