"""异常日志领域服务 — 记录和追踪 RPA 执行异常，输出 domain_snapshot。

数据工程核心：水位追踪与增量采集调度 — 跨模块数据产物
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class AnomalyLogService(BaseModuleService):
    MODULE = "anomaly_log"
    ENTITY = "异常事件"
    OPERATION_TERM = "关联"
    LIST_FIELDS = ["事件编号", "关联流程", "异常类型", "异常详情", "发生步骤", "发生时间", "恢复状态"]
    REQUIRED_FIELDS = ["事件编号", "关联流程", "异常类型"]
    STATUS_ORDER = ["未处理", "处理中", "已恢复", "已忽略"]
    ACTION_RULES = {"标记恢复": "未处理", "忽略事件": "处理中"}
    NEGATIVE_ACTIONS = ["忽略事件"]

    def log_exception(self, bot_ref: str, step: str,
                       error_type: str, detail: str) -> dict[str, Any]:
        """记录一条执行异常事件（增强版创建）。"""
        rows = store.rows(self.MODULE)
        event_number = f"ANOM-{len(rows) + 1:04d}"
        values = {
            "事件编号": event_number,
            "关联流程": bot_ref,
            "异常类型": error_type,
            "异常详情": detail,
            "发生步骤": step,
            "发生时间": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "恢复状态": "未处理",
        }
        entry, missing = self.create_entry(values)
        if entry:
            # 同时写入恢复管理器
            store.recovery.log(
                module=self.MODULE,
                batch_ref=event_number,
                step=step,
                error_detail=detail,
            )
        return {"event_number": event_number, "created": entry is not None}

    def get_pending_anomalies(self) -> list[dict[str, Any]]:
        """获取未处理的异常事件列表。"""
        rows = store.rows(self.MODULE)
        return [
            {
                "id": r.get("id"), "event_no": r.get("事件编号"),
                "bot": r.get("关联流程"), "type": r.get("异常类型"),
                "step": r.get("发生步骤"), "detail": r.get("异常详情"),
                "time": r.get("发生时间"),
            }
            for r in rows if r.get("status") in ("未处理", "处理中")
        ]

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        return {
            **base,
            "unresolved": sum(1 for r in rows if r.get("status") in ("未处理", "处理中")),
            "resolved": sum(1 for r in rows if r.get("status") == "已恢复"),
            "ignored": sum(1 for r in rows if r.get("status") == "已忽略"),
            "recovery_pending": store.recovery.pending_count(),
        }