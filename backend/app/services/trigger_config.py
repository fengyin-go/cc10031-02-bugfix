"""触发配置领域服务 — 管理 RPA 流程触发器，输出 domain_snapshot。

数据工程核心：数据聚合计算与指标产出 — 跨模块数据产物
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class TriggerConfigService(BaseModuleService):
    MODULE = "trigger_config"
    ENTITY = "触发器"
    OPERATION_TERM = "调度"
    LIST_FIELDS = ["触发器名称", "触发类型", "调度表达式", "事件源", "前置条件", "延迟窗口", "并发控制"]
    REQUIRED_FIELDS = ["触发器名称", "触发类型", "调度表达式"]
    STATUS_ORDER = ["草稿", "已激活", "已暂停", "已过期"]
    ACTION_RULES = {"激活触发器": "草稿", "暂停触发器": "已激活"}
    NEGATIVE_ACTIONS = []

    def activate_trigger(self, trigger_id: int) -> dict[str, Any] | None:
        """激活触发器并发布触发事件到通道。"""
        entry = store.find(self.MODULE, trigger_id)
        if entry is None:
            return None
        if entry.get("status") not in ("草稿", "已暂停"):
            return None

        entry["status"] = "已激活"
        entry["pending"] = True

        store.channel.publish(
            topic="rpa.trigger_activated",
            source=self.MODULE,
            payload={
                "trigger_id": trigger_id,
                "name": entry.get("触发器名称"),
                "type": entry.get("触发类型"),
                "cron": entry.get("调度表达式"),
                "event_source": entry.get("事件源"),
                "activated_at": datetime.now(timezone.utc).isoformat(),
            },
        )
        return {"trigger_id": trigger_id, "status": "已激活"}

    def list_active_triggers(self) -> list[dict[str, Any]]:
        """列出所有已激活的触发器。"""
        rows = store.rows(self.MODULE)
        return [
            {
                "id": r.get("id"), "name": r.get("触发器名称"),
                "type": r.get("触发类型"), "cron": r.get("调度表达式"),
                "event_source": r.get("事件源"), "condition": r.get("前置条件"),
            }
            for r in rows if r.get("status") == "已激活"
        ]

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        return {
            **base,
            "active_triggers": sum(1 for r in rows if r.get("status") == "已激活"),
            "paused_triggers": sum(1 for r in rows if r.get("status") == "已暂停"),
            "event_types": list({r.get("触发类型", "") for r in rows if r.get("触发类型")}),
        }