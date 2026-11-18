"""RPA流程自动化中心 后端服务入口。

启动：uvicorn app.main:app --host 127.0.0.1 --port 8000
健康检查：GET /api/health
"""
from __future__ import annotations

from typing import Any

from fastapi import Body, FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.routers import ROUTERS
from app.store import store
from app.services import runtime_session, callback_hook

app = FastAPI(title="RPA流程自动化中心", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/api/health")
def health() -> dict[str, object]:
    """健康检查：确认服务已经监听、示例数据已经就绪。"""
    return {"ok": True, "app": settings.app_name, "modules": len(store.module_names())}


@app.get("/api/overview")
def overview() -> dict[str, object]:
    """运营概览：把各业务模块的待处理量汇总成看板卡片。"""
    return store.overview()


# ---- 跨模块数据流 API ----

@app.get("/api/nodestats")
def nodestats(module: str | None = Query(default=None)) -> dict[str, object]:
    """节点统计：某一模块或全平台的领域快照。

    用于跨模块数据消费方查看各领域数据质量状态。
    """
    from app.services import SERVICES, service_for

    if module:
        svc = service_for(module)
        if svc is None:
            return {"error": f"未知模块: {module}"}
        return svc.domain_snapshot()

    # 全平台聚合快照
    snapshots = {}
    for key, svc in SERVICES.items():
        snapshots[key] = svc.domain_snapshot()

    # 附加非模块服务
    snapshots["runtime_session"] = runtime_session.domain_snapshot()
    snapshots["callback_hook"] = callback_hook.domain_snapshot()

    # 存储基础设施概览
    infra = {
        "channel": {
            "total_messages": store.channel.total_messages,
            "unconfirmed": store.channel.uncomfirmed_count(),
            "topics": list(set(
                m["topic"] for m in store.channel.snapshot()
            )),
        },
        "dirty_vault": store.dirty_vault.snapshot(),
        "collect_records": store.collect_records.snapshot(),
        "recovery": store.recovery.snapshot(),
        "quality_lineage": store.quality_lineage.snapshot(),
        "quarantine_ledger": store.quarantine_ledger.snapshot(),
        "quality_reports": store.quality_reports.snapshot(),
    }

    return {"modules": snapshots, "infrastructure": infra}


@app.get("/api/channel/{topic}")
def channel_snapshot(topic: str) -> dict[str, object]:
    """获取指定 topic 的通道消息快照。"""
    msgs = store.channel.snapshot(topic=topic)
    latest = store.channel.latest(topic)
    return {"topic": topic, "count": len(msgs), "messages": msgs, "latest": latest}


@app.get("/api/dirty-vault")
def dirty_vault(source: str | None = Query(default=None)) -> dict[str, object]:
    """脏数据隔离区查询。"""
    if source:
        records = store.dirty_vault.list_by_source(source)
    else:
        return store.dirty_vault.snapshot()
    return {
        "source": source,
        "count": len(records),
        "records": [
            {"id": r.record_id, "batch": r.batch_ref, "reason": r.reason,
             "raw": r.raw_data, "detected_at": r.detected_at}
            for r in records
        ],
    }


@app.get("/api/collect-records")
def collect_records(batch_ref: str | None = Query(default=None),
                     source: str | None = Query(default=None)) -> dict[str, object]:
    """采集记录查询。"""
    if batch_ref:
        records = store.collect_records.list_by_batch(batch_ref)
    elif source:
        records = store.collect_records.list_by_source(source)
    else:
        return store.collect_records.snapshot()
    return {
        "count": len(records),
        "records": [
            {"id": e.entry_id, "batch": e.batch_ref, "fields": e.fields,
             "validated": e.validated, "score": e.quality_score}
            for e in records
        ],
    }


@app.get("/api/recovery")
def recovery_stats() -> dict[str, object]:
    """失败恢复状态查询。"""
    return store.recovery.snapshot()


@app.get("/api/runtime-sessions")
def runtime_sessions() -> dict[str, object]:
    """运行时会话列表。"""
    sessions = runtime_session.list_sessions()
    return {"count": len(sessions), "sessions": sessions}


# ---- 跨模块数据流编排入口 ----

@app.post("/api/pipeline/rpa-lifecycle")
def pipeline_rpa_lifecycle() -> dict[str, object]:
    """执行 RPA 流程全生命周期数据流。

    模拟完整的端到端数据流：bot_definition → step_editor → task_queue → page_model → data_extract → result_verify → callback_hook
    """
    from app.services import (
        BotDefinitionService, StepEditorService, TaskQueueService,
        PageModelService, DataExtractService, ResultVerifyService,
    )

    results: dict[str, Any] = {}
    errors: list[str] = []

    # 1. 发布流程
    bot_svc = BotDefinitionService()
    active_bots = bot_svc.query_active_bots()
    publish_results = []
    for bot in active_bots:
        r = bot_svc.publish_bot(bot["id"])
        if r:
            publish_results.append(r)
    results["bot_published"] = len(publish_results)

    # 2. 编排步骤
    step_svc = StepEditorService()
    enrich = step_svc.enrich_steps_from_channel()
    results["steps_enriched"] = enrich["enriched"]

    # 3. 提交到队列
    task_svc = TaskQueueService()
    queue_result = task_svc.consume_steps_to_queue()
    results["tasks_queued"] = queue_result["generated"]

    # 4. 发布源端配置
    page_svc = PageModelService()
    active_models = page_svc.list_active_models()
    config_published = 0
    for model in active_models:
        r = page_svc.publish_source_config(model["model_id"])
        if r:
            config_published += 1
    # 如果没有active模型，尝试发布已验证模型
    if config_published == 0:
        models = store.rows("page_model")
        for m in models:
            if m.get("status") in ("草稿", "已验证"):
                r = page_svc.publish_source_config(m["id"])
                if r:
                    config_published += 1
                    break
    results["source_configs_published"] = config_published

    # 5. 执行数据采集
    extract_svc = DataExtractService()
    extract_result = extract_svc.consume_source_and_extract()
    results["data_extracted"] = extract_result

    # 6. 校验采集结果
    verify_svc = ResultVerifyService()
    verify_result = verify_svc.verify_collected_data()
    results["validation"] = verify_result

    # 7. 触发回调
    cb_result = callback_hook.consume_validation_and_callback()
    results["callbacks"] = cb_result

    # 8. 标记任务完成
    scheduled = task_svc.schedule_next_batch(max_tasks=3)
    results["tasks_scheduled"] = scheduled["scheduled"]

    return {"ok": True, "results": results, "errors": errors if errors else None}


@app.post("/api/pipeline/crawl-schedule")
def pipeline_crawl_schedule() -> dict[str, object]:
    """触发调度采集流程。"""
    from app.services import SchedulerService
    sched_svc = SchedulerService()
    result = sched_svc.schedule_crawl_jobs()
    return {"ok": True, "scheduled": result}


@app.post("/api/pipeline/element-execution")
def pipeline_element_execution() -> dict[str, object]:
    """触发元素定位与自动化执行流程。"""
    result = runtime_session.consume_strategies_and_execute()
    return {"ok": True, "result": result}


# ---- 校验工作台 API（结果校验模块的覆盖修复配套） ----

@app.get("/api/verify-workbench/summary")
def verify_workbench_summary() -> dict[str, object]:
    """校验工作台总览：每条规则的抽样数、执行数、覆盖状态，以及
    质量列表与隔离工作台待复核记录的一致性核对。"""
    from app.services import ResultVerifyService

    svc = ResultVerifyService()
    reports = [
        {
            "batch_ref": report.batch_ref,
            "status": report.status,
            "passed": report.passed,
            "failed": report.failed,
            "uncovered": report.uncovered,
            "sampled_records": report.sampled_records,
            "unsampled_records": report.unsampled_records,
            "uncovered_list": report.uncovered_list,
            "strategy": report.strategy,
            "version": report.version,
        }
        for report in store.quality_reports.list_current()
    ]
    return {
        "rules": svc.get_rule_coverage_stats(),
        "stats": svc.get_validation_stats(),
        "consistency": svc.check_consistency(),
        "reports": reports,
    }


@app.get("/api/verify-workbench/lineage")
def verify_workbench_lineage(
    batch_ref: str | None = Query(default=None),
) -> dict[str, object]:
    """采集质量谱系数据集：每条记录的抽样覆盖与最终质量状态。"""
    from app.services import ResultVerifyService

    svc = ResultVerifyService()
    receipts = svc.get_validation_receipts(batch_ref)
    return {"count": len(receipts), "items": receipts}


@app.get("/api/verify-workbench/receipts")
def verify_workbench_receipts(
    batch_ref: str | None = Query(default=None),
) -> dict[str, object]:
    """校验收据明细：每条记录逐规则的抽样/执行/通过事实。"""
    from app.services import ResultVerifyService

    svc = ResultVerifyService()
    receipts = svc.get_validation_receipts(batch_ref)
    return {"count": len(receipts), "items": receipts}


@app.get("/api/verify-workbench/quarantine")
def verify_workbench_quarantine(
    batch_ref: str | None = Query(default=None),
) -> dict[str, object]:
    """隔离工作台待复核记录：隔离回执与当前最终质量状态保持一致。"""
    from app.services import ResultVerifyService

    svc = ResultVerifyService()
    reviews = svc.get_quarantine_review_list(batch_ref)
    return {
        "count": len(reviews),
        "items": reviews,
        "consistency": svc.check_consistency(batch_ref),
    }


@app.post("/api/verify-workbench/verify")
def verify_workbench_run() -> dict[str, object]:
    """执行采集结果校验：读取原始采集记录与已启用校验规则，
    生成带覆盖状态的采集质量谱系数据集。"""
    from app.services import ResultVerifyService

    svc = ResultVerifyService()
    result = svc.verify_collected_data()
    return {"ok": True, "result": result, "consistency": svc.check_consistency()}


@app.post("/api/verify-workbench/resolve-batch")
def verify_workbench_resolve_batch(
    payload: dict[str, Any] = Body(...),
) -> dict[str, object]:
    """处理低覆盖批次与已发布质量结果的冲突。

    strategy=resample：整批重抽后替换报告；
    strategy=append：保留旧报告并追加未覆盖清单。
    两种方案都只产生一个最终质量状态。
    """
    from app.services import ResultVerifyService

    batch_ref = str(payload.get("batch_ref", ""))
    strategy = str(payload.get("strategy", "resample"))
    svc = ResultVerifyService()
    result = svc.resolve_low_coverage_batch(batch_ref, strategy)
    return {**result, "consistency": svc.check_consistency(batch_ref)}


# 资源路由含 /api/{module}，必须放在 health / overview 之后注册，
# 否则固定路径会被动态路径抢先匹配。
for module in ROUTERS:
    app.include_router(module.router)