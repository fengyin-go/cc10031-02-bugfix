"""环境配置领域服务 — 管理 RPA 执行环境，输出 domain_snapshot。

数据工程核心：数据聚合计算与指标产出 — 跨模块数据产物
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class EnvProfileService(BaseModuleService):
    MODULE = "env_profile"
    ENTITY = "执行环境"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["环境名称", "浏览器版本", "操作系统", "网络代理", "分辨率", "插件列表", "沙箱模式"]
    REQUIRED_FIELDS = ["环境名称", "浏览器版本", "操作系统"]
    STATUS_ORDER = ["待验证", "可用", "维护中", "已退役"]
    ACTION_RULES = {"验证环境": "待验证", "标记退役": "可用"}
    NEGATIVE_ACTIONS = []

    def list_available_environments(self) -> list[dict[str, Any]]:
        """列出所有可用的执行环境。"""
        rows = store.rows(self.MODULE)
        return [
            {
                "id": r.get("id"), "name": r.get("环境名称"),
                "browser": r.get("浏览器版本"), "os": r.get("操作系统"),
                "proxy": r.get("网络代理"), "sandbox": r.get("沙箱模式"),
            }
            for r in rows if r.get("status") in ("可用", "待验证")
        ]

    def get_env_health(self) -> dict[str, Any]:
        """获取执行环境健康概览。"""
        rows = store.rows(self.MODULE)
        return {
            "total": len(rows),
            "available": sum(1 for r in rows if r.get("status") == "可用"),
            "maintenance": sum(1 for r in rows if r.get("status") == "维护中"),
            "pending_verify": sum(1 for r in rows if r.get("status") == "待验证"),
            "retired": sum(1 for r in rows if r.get("status") == "已退役"),
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        health = self.get_env_health()
        return {**base, **health}