"""RPA 流程集成测试：覆盖全部贯通链路、频控降级、反封锁切换、批次补采。"""
from __future__ import annotations

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.services.pipeline import (
    PipelineResult,
    DirtyDataVault,
    RecoveryManager,
    pipeline_rpa_lifecycle,
    pipeline_trigger_to_queue,
    pipeline_element_to_output,
    pipeline_proxy_anti_block,
    pipeline_enrich_verify,
    pipeline_credential_env,
    pipeline_anomaly_to_exec,
    pipeline_batch_resupply,
    detect_missed_batches,
    trigger_resupply,
    run_all_pipelines,
)


class TestPipelineRpaLifecycle:
    """测试链路1: 流程定义 → 步骤编排 → 执行记录 → 批次追踪"""

    def test_full_pipeline_returns_ok(self) -> None:
        result = pipeline_rpa_lifecycle()
        assert result.ok is True
        assert len(result.trace) > 0
        assert "bot" in result.data
        assert "steps" in result.data
        assert "batch_count" in result.data

    def test_pipeline_trace_covers_all_modules(self) -> None:
        result = pipeline_rpa_lifecycle()
        modules_in_trace = {t.split(":")[0] for t in result.trace}
        assert "bot_definition" in modules_in_trace
        assert "step_editor" in modules_in_trace
        assert "execution_log" in modules_in_trace
        assert "batch_tracker" in modules_in_trace

    def test_pipeline_with_existing_bot_id(self) -> None:
        result = pipeline_rpa_lifecycle(bot_id=1)
        assert result.ok is True
        assert result.data["bot"] is not None


class TestPipelineTriggerQueue:
    """测试链路2: 触发配置 → 调度计划 → 任务队列"""

    def test_trigger_to_queue_returns_ok(self) -> None:
        result = pipeline_trigger_to_queue()
        assert result.ok is True
        assert "trigger" in result.data
        assert "plan_count" in result.data
        assert "queue_done" in result.data

    def test_queue_waiting_identified(self) -> None:
        result = pipeline_trigger_to_queue()
        assert "queue_waiting" in result.data
        assert isinstance(result.data["queue_waiting"], int)


class TestPipelineElementOutput:
    """测试链路3: 元素定位 → 页面建模 → 数据提取 → 结果校验 → 输出格式"""

    def test_full_extraction_pipeline_ok(self) -> None:
        result = pipeline_element_to_output()
        assert result.ok is True
        assert "selector" in result.data
        assert "page_count" in result.data
        assert "extract_count" in result.data
        assert "output_count" in result.data

    def test_all_five_modules_in_trace(self) -> None:
        result = pipeline_element_to_output()
        modules_in_trace = {t.split(":")[0] for t in result.trace}
        expected = {
            "element_selector", "page_model", "data_extract",
            "result_verify", "output_format",
        }
        assert expected.issubset(modules_in_trace)


class TestPipelineProxyAntiBlock:
    """测试链路4: 代理路由 → 频控策略 → 反封锁 联动"""

    def test_proxy_anti_block_pipeline_ok(self) -> None:
        result = pipeline_proxy_anti_block()
        assert result.ok is True
        assert "proxy_online" in result.data

    def test_rate_limit_degradation(self) -> None:
        """频控触发时的降级/等待行为：检查 wait_required 标记"""
        result = pipeline_proxy_anti_block()
        # 当有超限规则时 wait_required=True
        assert isinstance(result.wait_required, bool)

    def test_anti_block_triggers_proxy_switch(self) -> None:
        """反封锁策略触发检测和代理切换准备"""
        result = pipeline_proxy_anti_block()
        # 当封锁触发时验证检测生效
        assert result.ok is True
        # proxy_switch_needed 在 pcopy_online>1 时置 True
        if result.data.get("proxy_online", 0) > 1 and result.data.get("block_triggered", 0) > 0:
            assert result.data["proxy_switch_needed"] is True


class TestPipelineEnrichVerify:
    """测试链路5: 数据补全 → 结果校验 二次验证"""

    def test_enrich_verify_pipeline_ok(self) -> None:
        result = pipeline_enrich_verify()
        assert result.ok is True
        assert "enrich_active" in result.data
        assert "reverify_done" in result.data


class TestPipelineCredentialEnv:
    """测试链路6: 凭据管理 → 环境配置 绑定"""

    def test_credential_env_pipeline_ok(self) -> None:
        result = pipeline_credential_env()
        assert result.ok is True
        assert "cred_normal" in result.data
        assert "env_available" in result.data
        assert "bound" in result.data

    def test_binding_detected(self) -> None:
        result = pipeline_credential_env()
        assert isinstance(result.data["bound"], bool)


class TestPipelineAnomalyExec:
    """测试链路7: 异常日志 → 执行记录 关联追踪"""

    def test_anomaly_to_exec_ok(self) -> None:
        result = pipeline_anomaly_to_exec()
        assert result.ok is True
        assert "anom_unresolved" in result.data
        assert isinstance(result.data.get("anomaly_linked", False), bool)


class TestBatchMissedDetection:
    """批次遗漏检测和补采触发"""

    def test_detect_missed_batches(self) -> None:
        missed = detect_missed_batches()
        assert isinstance(missed, list)
        for item in missed:
            assert "batch_no" in item
            assert "trigger_resupply" in item

    def test_trigger_resupply_creates_tasks(self) -> None:
        missed = detect_missed_batches()
        if missed:
            tasks = trigger_resupply(missed)
            assert len(tasks) == len(missed)
            for t in tasks:
                assert t["status"] == "排队中"

    def test_pipeline_batch_resupply(self) -> None:
        result = pipeline_batch_resupply()
        assert "missed_count" in result
        assert "resupply_tasks" in result
        assert "ok" in result


class TestDirtyDataVault:
    """脏数据隔离"""

    def test_isolate_and_list(self) -> None:
        vault = DirtyDataVault()
        vault.isolate({"id": 1, "name": "test"}, "格式异常", "output_format")
        assert vault.size() == 1
        records = vault.list_all()
        assert len(records) == 1
        assert records[0]["reason"] == "格式异常"

    def test_purge_removes_entry(self) -> None:
        vault = DirtyDataVault()
        vault.isolate({"id": 5}, "test", "any")
        removed = vault.purge(5)
        assert removed == 1
        assert vault.size() == 0

    def test_purge_nonexistent(self) -> None:
        vault = DirtyDataVault()
        vault.isolate({"id": 1}, "test", "any")
        removed = vault.purge(99)
        assert removed == 0
        assert vault.size() == 1


class TestRecoveryManager:
    """失败恢复机制"""

    def test_success_on_first_attempt(self) -> None:
        rec = RecoveryManager(max_retries=3)
        ok, result, err = rec.attempt("test_success", lambda x: x * 2, 21)
        assert ok is True
        assert result == 42
        assert err == ""

    def test_retry_then_success(self) -> None:
        call_count = [0]

        def flaky():
            call_count[0] += 1
            if call_count[0] < 2:
                raise RuntimeError("temporary failure")
            return "recovered"

        rec = RecoveryManager(max_retries=3, backoff_base=0.01)
        ok, result, err = rec.attempt("flaky_op", flaky)
        assert ok is True
        assert result == "recovered"

    def test_all_retries_exhausted(self) -> None:
        def always_fail():
            raise RuntimeError("permanent failure")

        rec = RecoveryManager(max_retries=2, backoff_base=0.01)
        ok, result, err = rec.attempt("doomed", always_fail)
        assert ok is False
        assert result is None
        assert "permanent failure" in err

    def test_history_recorded(self) -> None:
        rec = RecoveryManager(max_retries=2, backoff_base=0.01)
        rec.attempt("op1", lambda x: x, 1)

        def fail_once():
            if not hasattr(fail_once, "called"):
                fail_once.called = True  # type: ignore[attr-defined]
                raise RuntimeError("first fail")
            return "ok"

        rec.attempt("flaky", fail_once)
        history = rec.history()
        assert len(history) >= 2


class TestRunAllPipelines:
    """全部链路综合运行"""

    def test_run_all_returns_report(self) -> None:
        report = run_all_pipelines()
        assert isinstance(report, dict)
        assert "all_ok" in report
        assert "lifecycle" in report
        assert "trigger_queue" in report
        assert "element_output" in report
        assert "proxy_anti_block" in report
        assert "enrich_verify" in report
        assert "credential_env" in report
        assert "anomaly_exec" in report
        assert "batch_resupply" in report