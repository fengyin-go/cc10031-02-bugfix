"""凭据管理领域服务 — 安全凭据生命周期，输出 domain_snapshot。

数据工程核心：数据入湖存储与分区管理 — 跨模块数据产物
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class CredentialStoreService(BaseModuleService):
    MODULE = "credential_store"
    ENTITY = "系统凭据"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["凭据名称", "目标系统", "认证方式", "账户标识", "有效期", "轮换周期", "最后使用"]
    REQUIRED_FIELDS = ["凭据名称", "目标系统", "认证方式"]
    STATUS_ORDER = ["正常", "即将过期", "已过期", "已吊销"]
    ACTION_RULES = {"轮换凭据": "即将过期", "吊销凭据": "正常"}
    NEGATIVE_ACTIONS = []

    def check_expiring_credentials(self) -> dict[str, Any]:
        """检查即将过期的凭据列表。"""
        rows = store.rows(self.MODULE)
        expiring = [
            {
                "id": r.get("id"), "name": r.get("凭据名称"),
                "system": r.get("目标系统"), "auth_type": r.get("认证方式"),
                "expiry": r.get("有效期"), "rotation": r.get("轮换周期"),
            }
            for r in rows if r.get("status") in ("即将过期", "已过期")
        ]
        return {
            "total_expiring": len(expiring),
            "expiring_credentials": expiring,
            "normal_count": sum(1 for r in rows if r.get("status") == "正常"),
        }

    def get_system_credentials(self, system: str) -> list[dict[str, Any]]:
        """查询指定目标系统的凭据状态。"""
        rows = store.rows(self.MODULE)
        return [
            {
                "id": r.get("id"), "name": r.get("凭据名称"),
                "auth_type": r.get("认证方式"), "status": r.get("status"),
                "last_used": r.get("最后使用"),
            }
            for r in rows if system in r.get("目标系统", "") and r.get("status") in ("正常", "即将过期")
        ]

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        expiring = self.check_expiring_credentials()
        return {**base, "expiring_count": expiring["total_expiring"]}