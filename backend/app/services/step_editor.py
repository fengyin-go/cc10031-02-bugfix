"""步骤编排领域服务 — 消费流程信息并生成步骤定义。

数据工程核心：数据聚合计算与指标产出 — 跨模块数据产物

数据流:
    channel("rpa.bot_created") ← bot_definition
    → step_editor 消费 → channel("rpa.steps_submitted")
    → task_queue / execution_queue 消费
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class StepEditorService(BaseModuleService):
    MODULE = "step_editor"
    ENTITY = "流程步骤"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["步骤序号", "步骤名称", "操作类型", "目标元素", "输入参数", "等待策略", "异常处理"]
    REQUIRED_FIELDS = ["步骤序号", "步骤名称", "操作类型"]
    STATUS_ORDER = ["草稿", "已编排", "已验证", "已废弃"]
    ACTION_RULES = {"提交编排": "草稿", "验证步骤": "已编排", "废弃步骤": "已验证"}
    NEGATIVE_ACTIONS = []

    # ---- 跨模块业务方法 ----

    def enrich_steps_from_channel(self) -> dict[str, Any]:
        """消费 bot_definition 发布的流程信息，为步骤补充上下文。

        从 channel("rpa.bot_created") 拉取最新未消费的 bot 信息，
        更新已有步骤的流程关联字段。这是跨模块数据流的核心消费点。

        Returns:
            {"enriched": int, "bots_found": int, "details": [...]}
        """
        msgs = store.channel.consume(
            consumer=self.MODULE,
            topic="rpa.bot_created",
        )
        if not msgs:
            return {"enriched": 0, "bots_found": 0, "details": []}

        details = []
        enriched_count = 0
        for msg in msgs:
            bot_data = msg.payload
            # 查找关联此 bot 的步骤并更新上下文
            steps = self._find_steps_by_bot(bot_data.get("bot_name", ""))
            for step in steps:
                step["bot_id"] = bot_data.get("bot_id")
                step["target_system"] = bot_data.get("target_system")
                step["exec_env"] = bot_data.get("exec_env")
                enriched_count += 1

            store.channel.confirm(msg.msg_id, self.MODULE)
            details.append({
                "bot_id": bot_data.get("bot_id"),
                "bot_name": bot_data.get("bot_name"),
                "linked_steps": len(steps),
            })

        return {
            "enriched": enriched_count,
            "bots_found": len(msgs),
            "details": details,
        }

    def _find_steps_by_bot(self, bot_name: str) -> list[dict[str, Any]]:
        """按流程名称模糊匹配步骤（模拟真实关联查询）。"""
        if not bot_name:
            return []
        rows = store.rows(self.MODULE)
        return [
            r for r in rows
            if bot_name[:4] in r.get("步骤名称", "")
            or r.get("status") == "草稿"
        ][:10]  # 上限避免过度关联

    def submit_steps_to_queue(self, bot_id: int,
                               steps_data: list[dict[str, Any]]) -> dict[str, Any]:
        """编排完成：将步骤提交到跨模块通道，供执行队列消费。

        模拟从步骤编排到任务队列的真实转换：
        - 解析每个步骤的元信息（序号、类型、元素、参数）
        - 按依赖拓扑排序
        - 生成执行计划写入 channel

        Args:
            bot_id: 关联的流程 ID
            steps_data: 步骤列表 [{"seq": 1, "name": "打开页面", "type": "navigate", ...}, ...]

        Returns:
            执行计划概要
        """
        if not steps_data:
            return {"submitted": 0, "plan_id": None, "error": "无步骤数据"}

        # 步骤排序（按 seq 字段）
        sorted_steps = sorted(steps_data, key=lambda s: int(s.get("seq", 0)))

        # 构造执行计划
        execution_plan = {
            "bot_id": bot_id,
            "plan_type": "sequential",
            "total_steps": len(sorted_steps),
            "steps": [],
        }

        submitted = 0
        for step in sorted_steps:
            # 为每个步骤登记到 step_editor 表
            values = {
                "步骤序号": str(step.get("seq", 0)),
                "步骤名称": step.get("name", f"Step-{step.get('seq')}"),
                "操作类型": step.get("type", "custom"),
                "目标元素": step.get("element", ""),
                "输入参数": step.get("params", ""),
                "等待策略": step.get("wait", "auto"),
                "异常处理": step.get("on_error", "retry"),
            }
            entry, missing = self.create_entry(values)
            if entry:
                # 立即标记为已编排
                entry["status"] = "已编排"
                entry["pending"] = True
                execution_plan["steps"].append({
                    "step_id": entry["id"],
                    "seq": step.get("seq"),
                    "name": step.get("name"),
                    "type": step.get("type"),
                })
                submitted += 1

        # 发布到跨模块通道
        msg = store.channel.publish(
            topic="rpa.steps_submitted",
            source=self.MODULE,
            payload=execution_plan,
        )
        return {
            "submitted": submitted,
            "plan_id": f"PLAN-{bot_id}-{msg.msg_id}",
            "total_steps": len(sorted_steps),
            "msg_id": msg.msg_id,
        }

    def extract_element_strategy(self, step_entries: list[int]) -> dict[str, Any]:
        """提取步骤中的元素定位策略，供 element_selector 消费。

        分析每个步骤使用的目标元素，汇总定位策略使用情况。

        Returns:
            {"total_elements": int, "by_type": dict, "recommendations": list}
        """
        element_types: dict[str, int] = {}
        recommendations = []

        for sid in step_entries:
            entry = store.find(self.MODULE, sid)
            if entry is None:
                continue
            op_type = entry.get("操作类型", "")
            element = entry.get("目标元素", "")

            if op_type:
                element_types[op_type] = element_types.get(op_type, 0) + 1
            if element:
                recommendations.append({
                    "step_id": sid,
                    "element": element,
                    "suggested_locator": f"css:{element}" if not element.startswith("xpath:") else element,
                })

        return {
            "total_elements": len(recommendations),
            "by_type": element_types,
            "recommendations": recommendations,
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        # 补充步骤编排侧的 channel 输出
        channel_snaps = store.channel.snapshot(topic="rpa.steps_submitted")
        return {
            **base,
            "steps_submitted_to_channel": len(channel_snaps),
            "latest_submission": channel_snaps[-1] if channel_snaps else None,
        }