"""RPA 流程定义领域服务 — 跨模块数据流的起点。

数据工程核心：水位追踪与增量采集调度 — 跨模块数据产物

数据流:
    bot_definition (创建流程) → channel("rpa.bot_created")
    → step_editor 消费流程信息编排步骤
    → task_queue 消费编排结果生成执行队列
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class BotDefinitionService(BaseModuleService):
    MODULE = "bot_definition"
    ENTITY = "RPA流程"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["流程名称", "目标系统", "触发方式", "执行环境", "超时上限", "失败策略", "版本号"]
    REQUIRED_FIELDS = ["流程名称", "目标系统", "触发方式"]
    STATUS_ORDER = ["草稿", "测试中", "已发布", "已停用"]
    ACTION_RULES = {"发布流程": "测试中", "停用流程": "已发布"}
    NEGATIVE_ACTIONS = ["停用流程"]

    # ---- 跨模块业务方法 ----

    def publish_bot(self, bot_id: int) -> dict[str, Any] | None:
        """发布流程：变更状态并写入跨模块数据通道，供 step_editor 消费。

        数据流：bot_definition → channel("rpa.bot_created") → step_editor

        Returns:
            成功时返回 channel 消息概要，失败返回 None
        """
        entry = store.find(self.MODULE, bot_id)
        if entry is None:
            return None
        # 状态流转：仅草稿/测试中可发布
        current = entry.get("status")
        if current not in ("草稿", "测试中"):
            return None
        entry["status"] = "已发布"
        entry["pending"] = False
        entry["version"] = entry.get("版本号", "1.0.0")

        # 写入跨模块数据通道
        channel_payload = {
            "bot_id": bot_id,
            "bot_name": entry.get("流程名称"),
            "target_system": entry.get("目标系统"),
            "trigger_mode": entry.get("触发方式"),
            "exec_env": entry.get("执行环境"),
            "timeout": entry.get("超时上限", "300s"),
            "failure_strategy": entry.get("失败策略", "重试"),
            "version": entry.get("version"),
        }
        msg = store.channel.publish(
            topic="rpa.bot_created",
            source=self.MODULE,
            payload=channel_payload,
        )
        return {
            "bot_id": bot_id,
            "status": entry["status"],
            "msg_id": msg.msg_id,
            "topic": msg.topic,
        }

    def query_active_bots(self) -> list[dict[str, Any]]:
        """查询所有已发布（活跃）的流程，供调度和编排查询。"""
        rows = store.rows(self.MODULE)
        return [
            {
                "id": r.get("id"), "name": r.get("流程名称"),
                "target": r.get("目标系统"), "trigger": r.get("触发方式"),
                "env": r.get("执行环境"), "timeout": r.get("超时上限"),
                "version": r.get("版本号"),
            }
            for r in rows if r.get("status") == "已发布"
        ]

    def bot_summary_for_editor(self) -> list[dict[str, Any]]:
        """为 step_editor 提供流程摘要信息。

        消费方：step_editor.enrich_steps_from_channel()
        输出字段与 channel 消息格式对齐。
        """
        active = self.query_active_bots()
        return [
            {
                "bot_id": b["id"], "bot_name": b["name"],
                "target_system": b["target"], "trigger_mode": b["trigger"],
                "available_steps": self._estimate_step_count(b),
            }
            for b in active
        ]

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        active = self.query_active_bots()
        # 补充 channel 侧的流程消费统计
        bot_topics = store.channel.snapshot(topic="rpa.bot_created")
        return {
            **base,
            "active_bots": len(active),
            "channel_events": len(bot_topics),
            "latest_channel_event": bot_topics[-1] if bot_topics else None,
        }

    @staticmethod
    def _estimate_step_count(bot: dict[str, Any]) -> int:
        """估算流程所需步骤数（基于目标系统复杂度）。"""
        complexity_map = {"ERP": 12, "外部数据源": 8, "CMS": 6, "交易平台": 10, "OA": 5}
        target = bot.get("target", "")
        for keyword, count in complexity_map.items():
            if keyword in target:
                return count
        return 6  # 默认最小步骤