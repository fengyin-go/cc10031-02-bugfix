"""代理路由领域服务 — 代理节点池管理，输出 domain_snapshot。

数据工程核心：数据聚合计算与指标产出 — 跨模块数据产物
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class ProxyRouterService(BaseModuleService):
    MODULE = "proxy_router"
    ENTITY = "代理节点"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["节点名称", "IP地址", "出口地域", "协议类型", "带宽上限", "延迟", "可用状态"]
    REQUIRED_FIELDS = ["节点名称", "IP地址", "出口地域"]
    STATUS_ORDER = ["在线", "离线", "限速中", "已禁用"]
    ACTION_RULES = {"启用节点": "离线", "禁用节点": "限速中"}
    NEGATIVE_ACTIONS = []

    def get_healthy_nodes(self) -> list[dict[str, Any]]:
        """获取在线且可用状态正常的代理节点。"""
        rows = store.rows(self.MODULE)
        return [
            {
                "id": r.get("id"), "name": r.get("节点名称"),
                "ip": r.get("IP地址"), "region": r.get("出口地域"),
                "protocol": r.get("协议类型"), "bandwidth": r.get("带宽上限"),
                "latency": r.get("延迟"),
            }
            for r in rows if r.get("status") == "在线" and r.get("可用状态") == "正常"
        ]

    def get_node_pool_stats(self) -> dict[str, Any]:
        """获取节点池统计。"""
        rows = store.rows(self.MODULE)
        return {
            "total_nodes": len(rows),
            "online": sum(1 for r in rows if r.get("status") == "在线"),
            "offline": sum(1 for r in rows if r.get("status") == "离线"),
            "rate_limited": sum(1 for r in rows if r.get("status") == "限速中"),
            "disabled": sum(1 for r in rows if r.get("status") == "已禁用"),
            "regions": list({r.get("出口地域", "") for r in rows if r.get("出口地域")}),
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        stats = self.get_node_pool_stats()
        return {**base, **stats}