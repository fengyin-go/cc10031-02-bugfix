"""输出格式领域服务 — 管理采集结果的输出格式模板，输出 domain_snapshot。

数据工程核心：数据校验与去重策略执行 — 跨模块数据产物
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class OutputFormatService(BaseModuleService):
    MODULE = "output_format"
    ENTITY = "格式模板"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["模板名称", "输出类型", "字段映射", "分隔符", "编码方式", "嵌套展开", "空值表示"]
    REQUIRED_FIELDS = ["模板名称", "输出类型", "字段映射"]
    STATUS_ORDER = ["草稿", "已应用", "已废弃"]
    ACTION_RULES = {"应用模板": "草稿", "废弃模板": "已应用"}
    NEGATIVE_ACTIONS = []

    def format_output(self, template_id: int,
                       records: list[dict[str, Any]]) -> dict[str, Any]:
        """按指定模板格式化输出数据。

        模拟将采集记录按模板配置的输出格式转换。

        Returns:
            {"template_id": int, "formatted_count": int, "format_type": str}
        """
        template = store.find(self.MODULE, template_id)
        if template is None:
            return {"template_id": template_id, "error": "模板不存在"}

        output_type = template.get("输出类型", "csv")
        for record in records:
            record["_format"] = output_type
            record["_template"] = template.get("模板名称")

        return {
            "template_id": template_id,
            "formatted_count": len(records),
            "format_type": output_type,
            "encoding": template.get("编码方式", "utf-8"),
            "null_placeholder": template.get("空值表示", "NA"),
        }

    def list_active_templates(self) -> list[dict[str, Any]]:
        """列出已应用的输出模板。"""
        rows = store.rows(self.MODULE)
        return [
            {
                "id": r.get("id"), "name": r.get("模板名称"),
                "type": r.get("输出类型"), "mapping": r.get("字段映射"),
                "delimiter": r.get("分隔符"), "encoding": r.get("编码方式"),
            }
            for r in rows if r.get("status") == "已应用"
        ]

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        return {
            **base,
            "active_templates": sum(1 for r in rows if r.get("status") == "已应用"),
            "deprecated_templates": sum(1 for r in rows if r.get("status") == "已废弃"),
            "format_types": list({r.get("输出类型", "") for r in rows if r.get("输出类型")}),
        }