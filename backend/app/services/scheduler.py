"""调度计划领域服务 — 管理定时采集计划，输出 domain_snapshot。

数据工程核心：数据清洗与转换链路处理 — 跨模块数据产物
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class SchedulerService(BaseModuleService):
    MODULE = "scheduler"
    ENTITY = "采集计划"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["计划名称", "绑定流程", "cron表达式", "执行窗口", "并发数", "优先级", "通知渠道"]
    REQUIRED_FIELDS = ["计划名称", "绑定流程", "cron表达式"]
    STATUS_ORDER = ["草稿", "已激活", "已暂停", "已归档"]
    ACTION_RULES = {"激活计划": "草稿", "暂停计划": "已激活", "归档计划": "已暂停"}
    NEGATIVE_ACTIONS = []

    def schedule_crawl_jobs(self) -> dict[str, Any]:
        """模拟调度触发：将已激活的计划对应的采集任务加入队列。

        遍历已激活的调度计划，模拟根据 cron 表达式判断是否在当前窗口，
        将到期任务发布到跨模块通道。

        Returns:
            {"triggered": int, "plans": [...]}
        """
        rows = store.rows(self.MODULE)
        active = [
            r for r in rows if r.get("status") == "已激活"
        ]

        triggered = []
        for plan in active:
            msg = store.channel.publish(
                topic="rpa.crawl_scheduled",
                source=self.MODULE,
                payload={
                    "plan_id": plan.get("id"),
                    "plan_name": plan.get("计划名称"),
                    "bot_ref": plan.get("绑定流程"),
                    "cron": plan.get("cron表达式"),
                    "priority": plan.get("优先级"),
                    "triggered_at": datetime.now(timezone.utc).isoformat(),
                },
            )
            triggered.append({
                "plan_id": plan.get("id"),
                "plan_name": plan.get("计划名称"),
                "msg_id": msg.msg_id,
            })

        return {"triggered": len(triggered), "plans": triggered}

    def list_active_plans(self) -> list[dict[str, Any]]:
        """列出已激活的调度计划。"""
        rows = store.rows(self.MODULE)
        return [
            {
                "id": r.get("id"), "name": r.get("计划名称"),
                "cron": r.get("cron表达式"), "bot": r.get("绑定流程"),
                "window": r.get("执行窗口"), "concurrency": r.get("并发数"),
                "priority": r.get("优先级"),
            }
            for r in rows if r.get("status") == "已激活"
        ]

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        return {
            **base,
            "active_plans": sum(1 for r in rows if r.get("status") == "已激活"),
            "paused_plans": sum(1 for r in rows if r.get("status") == "已暂停"),
            "archived_plans": sum(1 for r in rows if r.get("status") == "已归档"),
        }