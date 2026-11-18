"""内存数据仓库：给每个业务模块准备一份可筛选、可流转的示例数据。

真实项目里这里会换成数据库访问层；当前实现只依赖标准库，保证克隆下来就能起。

跨模块数据流能力：
- DataChannel: 服务间数据发布/消费通道，支持 topic 订阅、快照读、消费确认
- DirtyDataVault: 脏数据隔离区，存储校验不通过/解析失败的脏数据记录
- FailureRecovery: 失败恢复管理器，记录失败步骤并支持重试
- CollectRecord: 采集记录存储，记录每次采集产出的结构化数据
- QualityLineageStore: 采集质量谱系，记录每条采集记录的规则覆盖与最终质量状态
- QuarantineLedger: 隔离回执台账，每条记录唯一回执，并发写入只保留一个版本
- QualityReportLedger: 质量报告台账，每批次只保留一个当前最终质量状态
"""
from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from app.seed import SEED_ROWS

# ---------------------------------------------------------------------------
# 跨模块数据通道 (DataChannel) — 服务间发布/消费
# ---------------------------------------------------------------------------

@dataclass
class ChannelMessage:
    """数据通道中的单条消息，带 topic、来源、负载和确认状态。"""
    msg_id: int
    topic: str
    source_module: str
    payload: dict[str, Any]
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    consumed_by: list[str] = field(default_factory=list)
    confirmed: bool = False

    def mark_consumed(self, consumer: str) -> None:
        if consumer not in self.consumed_by:
            self.consumed_by.append(consumer)


class DataChannel:
    """跨模块数据通道：支持 topic 发布/订阅，服务间数据流转的桥梁。

    用法::

        store.channel.publish("rpa.bot_created", "bot_definition", bot_data)
        msgs = store.channel.consume("step_editor", "rpa.bot_created")
        store.channel.confirm(msg_id, "step_editor")
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._seq = 0
        self._messages: list[ChannelMessage] = []

    @property
    def total_messages(self) -> int:
        with self._lock:
            return len(self._messages)

    def publish(self, topic: str, source: str, payload: dict[str, Any]) -> ChannelMessage:
        with self._lock:
            self._seq += 1
            msg = ChannelMessage(
                msg_id=self._seq, topic=topic,
                source_module=source, payload=payload,
            )
            self._messages.append(msg)
            return msg

    def consume(self, consumer: str, topic: str | None = None,
                since_msg_id: int | None = None) -> list[ChannelMessage]:
        """获取指定 consumer 尚未消费的消息。"""
        with self._lock:
            results: list[ChannelMessage] = []
            for msg in self._messages:
                if topic and msg.topic != topic:
                    continue
                if since_msg_id is not None and msg.msg_id <= since_msg_id:
                    continue
                if consumer not in msg.consumed_by:
                    results.append(msg)
                    msg.mark_consumed(consumer)
            return results

    def confirm(self, msg_id: int, consumer: str) -> bool:
        with self._lock:
            for msg in self._messages:
                if msg.msg_id == msg_id:
                    msg.confirmed = True
                    return True
            return False

    def snapshot(self, topic: str | None = None) -> list[dict[str, Any]]:
        """返回消息快照，供跨模块分析/看板消费。"""
        with self._lock:
            msgs = self._messages if topic is None else [
                m for m in self._messages if m.topic == topic
            ]
            return [{
                "msg_id": m.msg_id, "topic": m.topic,
                "source": m.source_module, "payload": m.payload,
                "created_at": m.created_at, "consumed_by": m.consumed_by,
                "confirmed": m.confirmed,
            } for m in msgs]

    def latest(self, topic: str) -> dict[str, Any] | None:
        with self._lock:
            for msg in reversed(self._messages):
                if msg.topic == topic:
                    return {
                        "msg_id": msg.msg_id, "topic": msg.topic,
                        "source": msg.source_module, "payload": msg.payload,
                        "created_at": msg.created_at, "confirmed": msg.confirmed,
                    }
            return None

    def uncomfirmed_count(self) -> int:
        with self._lock:
            return sum(1 for m in self._messages if not m.confirmed)


# ---------------------------------------------------------------------------
# 脏数据隔离区 (DirtyDataVault)
# ---------------------------------------------------------------------------

@dataclass
class DirtyRecord:
    """脏数据记录：校验失败或解析异常的数据行。"""
    record_id: int
    source_module: str
    batch_ref: str
    raw_data: dict[str, Any]
    reason: str  # 如 "字段缺失", "格式非法", "解析超时"
    detected_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    quarantined: bool = True
    recovered_to: str | None = None
    entry_id: int | None = None  # 关联的采集记录 ID（隔离去重键的一部分）
    final_status: str = "未通过"  # 与当前最终质量状态保持一致：未通过 / 待复核


class DirtyDataVault:
    """脏数据隔离区：被校验规则或解析器标记为问题数据后移入此处。

    用法::

        vault = store.dirty_vault
        vault.quarantine("data_extract", "BATC-001", row, "缺少必填字段")
        dirty = vault.list_by_source("data_extract")

    隔离写入约束（处理项乙）：
    - 同一记录（source + batch_ref + entry_id）再次写入时只保留一条，
      原地更新原因与最终质量状态，不在隔离区重复出现；
    - 未携带 entry_id 的写入保持既有行为（逐条追加），兼容既有字段口径。
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._seq = 0
        self._records: list[DirtyRecord] = []

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._records)

    def quarantine(self, source: str, batch_ref: str,
                   raw_data: dict[str, Any], reason: str,
                   entry_id: int | None = None,
                   final_status: str = "未通过") -> DirtyRecord:
        with self._lock:
            if entry_id is not None:
                # 同一记录再次校验：去重，原地更新，保证隔离区不重复出现
                for rec in self._records:
                    if (rec.source_module == source and rec.batch_ref == batch_ref
                            and rec.entry_id == entry_id and rec.quarantined):
                        rec.raw_data = dict(raw_data)
                        rec.reason = reason
                        rec.final_status = final_status
                        rec.detected_at = datetime.now(timezone.utc).isoformat()
                        return rec
            self._seq += 1
            rec = DirtyRecord(
                record_id=self._seq, source_module=source,
                batch_ref=batch_ref, raw_data=raw_data, reason=reason,
                entry_id=entry_id, final_status=final_status,
            )
            self._records.append(rec)
            return rec

    def list_by_source(self, source: str) -> list[DirtyRecord]:
        with self._lock:
            return [r for r in self._records if r.source_module == source]

    def list_by_batch(self, batch_ref: str) -> list[DirtyRecord]:
        with self._lock:
            return [r for r in self._records if r.batch_ref == batch_ref]

    def list_pending_review(self, batch_ref: str | None = None) -> list[DirtyRecord]:
        """待复核记录：最终质量状态为待复核且仍在隔离中的记录。"""
        with self._lock:
            return [
                r for r in self._records
                if r.quarantined and r.final_status == "待复核"
                and (batch_ref is None or r.batch_ref == batch_ref)
            ]

    def mark_recovered(self, record_id: int, recovered_to: str) -> bool:
        with self._lock:
            for r in self._records:
                if r.record_id == record_id:
                    r.quarantined = False
                    r.recovered_to = recovered_to
                    return True
            return False

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            quarantined = sum(1 for r in self._records if r.quarantined)
            by_reason: dict[str, int] = {}
            for r in self._records:
                by_reason[r.reason] = by_reason.get(r.reason, 0) + 1
            return {
                "total": len(self._records),
                "quarantined": quarantined,
                "recovered": len(self._records) - quarantined,
                "pending_review": sum(
                    1 for r in self._records
                    if r.quarantined and r.final_status == "待复核"),
                "by_reason": by_reason,
            }


# ---------------------------------------------------------------------------
# 采集记录 (CollectRecord) — 每次采集产出的结构化数据
# ---------------------------------------------------------------------------

@dataclass
class CollectEntry:
    entry_id: int
    batch_ref: str
    source_module: str
    fields: dict[str, Any]
    collected_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    validated: bool = False
    quality_score: float = 1.0


class CollectRecordStore:
    """采集记录存储：每次采集流程产出的结构化数据行。

    用法::

        rec = store.collect_records.add("BATC-001", "data_extract", {"字段1": "值1", ...})
        store.collect_records.mark_validated(rec.entry_id)
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._seq = 0
        self._entries: list[CollectEntry] = []

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._entries)

    def add(self, batch_ref: str, source: str,
            fields: dict[str, Any]) -> CollectEntry:
        with self._lock:
            self._seq += 1
            entry = CollectEntry(
                entry_id=self._seq, batch_ref=batch_ref,
                source_module=source, fields=dict(fields),
            )
            self._entries.append(entry)
            return entry

    def list_by_batch(self, batch_ref: str) -> list[CollectEntry]:
        with self._lock:
            return [e for e in self._entries if e.batch_ref == batch_ref]

    def list_by_source(self, source: str) -> list[CollectEntry]:
        with self._lock:
            return [e for e in self._entries if e.source_module == source]

    def mark_validated(self, entry_id: int, score: float = 1.0) -> bool:
        with self._lock:
            for e in self._entries:
                if e.entry_id == entry_id:
                    e.validated = True
                    e.quality_score = score
                    return True
            return False

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            validated = sum(1 for e in self._entries if e.validated)
            by_source: dict[str, int] = {}
            for e in self._entries:
                by_source[e.source_module] = by_source.get(e.source_module, 0) + 1
            return {
                "total": len(self._entries),
                "validated": validated,
                "unvalidated": len(self._entries) - validated,
                "by_source": by_source,
            }


# ---------------------------------------------------------------------------
# 采集质量谱系 (QualityLineage) — 每条采集记录的规则覆盖与最终质量状态
# ---------------------------------------------------------------------------

# 覆盖状态口径
COVERAGE_FULL = "已覆盖"
COVERAGE_PARTIAL = "部分覆盖"
COVERAGE_NONE = "未覆盖"

# 最终质量状态口径
QUALITY_PASSED = "通过"
QUALITY_FAILED = "未通过"
QUALITY_PENDING = "待复核"


@dataclass
class QualityLineageEntry:
    """采集质量谱系条目：一条采集记录在一轮校验中的规则覆盖事实。

    校验收据明细（rule_details）随条目保留，供校验工作台逐条核对。
    """
    lineage_id: int
    entry_id: int            # 采集记录 ID
    batch_ref: str
    rules_total: int         # 已启用规则总数
    rules_sampled: int       # 抽样命中的规则数（抽样数）
    rules_executed: int      # 实际执行的规则数（执行数）
    rules_skipped: int       # 抽样跳过的规则数
    rules_failed: int        # 执行后未通过的规则数
    coverage_ratio: float    # 覆盖率 = 执行数 / 规则总数
    coverage_status: str     # 已覆盖 / 部分覆盖 / 未覆盖
    final_status: str        # 通过 / 未通过 / 待复核
    pending: bool            # 待处理（待复核时为 True）
    rule_details: list[dict[str, Any]] = field(default_factory=list)  # 校验收据明细
    version: int = 1
    updated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class QualityLineageStore:
    """采集质量谱系数据集：按 (batch_ref, entry_id) 唯一，重复校验原地升版。

    质量列表与隔离工作台共用这一份覆盖事实，保证两边待复核记录对得上。
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._seq = 0
        self._entries: list[QualityLineageEntry] = []

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._entries)

    def upsert(self, entry_id: int, batch_ref: str, *,
               rules_total: int, rules_sampled: int, rules_executed: int,
               rules_skipped: int, rules_failed: int, coverage_ratio: float,
               coverage_status: str, final_status: str,
               rule_details: list[dict[str, Any]]) -> QualityLineageEntry:
        """写入谱系：同一记录再次校验时升版替换，只保留一个当前版本。"""
        with self._lock:
            for entry in self._entries:
                if entry.entry_id == entry_id and entry.batch_ref == batch_ref:
                    entry.rules_total = rules_total
                    entry.rules_sampled = rules_sampled
                    entry.rules_executed = rules_executed
                    entry.rules_skipped = rules_skipped
                    entry.rules_failed = rules_failed
                    entry.coverage_ratio = coverage_ratio
                    entry.coverage_status = coverage_status
                    entry.final_status = final_status
                    entry.pending = final_status == QUALITY_PENDING
                    entry.rule_details = list(rule_details)
                    entry.version += 1
                    entry.updated_at = datetime.now(timezone.utc).isoformat()
                    return entry
            self._seq += 1
            entry = QualityLineageEntry(
                lineage_id=self._seq, entry_id=entry_id, batch_ref=batch_ref,
                rules_total=rules_total, rules_sampled=rules_sampled,
                rules_executed=rules_executed, rules_skipped=rules_skipped,
                rules_failed=rules_failed, coverage_ratio=coverage_ratio,
                coverage_status=coverage_status, final_status=final_status,
                pending=final_status == QUALITY_PENDING,
                rule_details=list(rule_details),
            )
            self._entries.append(entry)
            return entry

    def get(self, entry_id: int, batch_ref: str) -> QualityLineageEntry | None:
        with self._lock:
            for entry in self._entries:
                if entry.entry_id == entry_id and entry.batch_ref == batch_ref:
                    return entry
            return None

    def list_by_batch(self, batch_ref: str) -> list[QualityLineageEntry]:
        with self._lock:
            return [e for e in self._entries if e.batch_ref == batch_ref]

    def list_all(self) -> list[QualityLineageEntry]:
        with self._lock:
            return list(self._entries)

    def list_pending(self, batch_ref: str | None = None) -> list[QualityLineageEntry]:
        """待处理（待复核）谱系条目。"""
        with self._lock:
            return [
                e for e in self._entries
                if e.pending and (batch_ref is None or e.batch_ref == batch_ref)
            ]

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            by_coverage: dict[str, int] = {}
            by_status: dict[str, int] = {}
            for e in self._entries:
                by_coverage[e.coverage_status] = by_coverage.get(e.coverage_status, 0) + 1
                by_status[e.final_status] = by_status.get(e.final_status, 0) + 1
            return {
                "total": len(self._entries),
                "pending": sum(1 for e in self._entries if e.pending),
                "by_coverage": by_coverage,
                "by_status": by_status,
            }


# ---------------------------------------------------------------------------
# 隔离回执台账 (QuarantineLedger) — 每条记录唯一回执，并发写入只保留一个版本
# ---------------------------------------------------------------------------

@dataclass
class QuarantineReceipt:
    """隔离回执：一条采集记录进入隔离区的唯一凭证。"""
    receipt_id: int
    record_key: str        # 唯一键：{source}:{batch_ref}:{entry_id}
    source_module: str
    batch_ref: str
    entry_id: int | None
    reason: str            # 隔离原因，与当前最终质量状态保持一致
    final_status: str      # 当前最终质量状态：未通过 / 待复核
    review_status: str     # 待复核 / 已复核
    version: int = 1
    written_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class QuarantineLedger:
    """隔离回执台账（处理项乙写入模块）。

    写入约束：
    - 按处理项乙规则为每条记录保存唯一回执（record_key 唯一）；
    - 并发再次写入只接受一个版本：锁内 check-and-set，原地升版，
      任何时刻同一记录只有一份有效回执；
    - 回执原因与当前最终质量状态同步更新，保持清单与台账一致。
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._seq = 0
        self._receipts: dict[str, QuarantineReceipt] = {}

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._receipts)

    @staticmethod
    def make_record_key(source: str, batch_ref: str, entry_id: int | None) -> str:
        return f"{source}:{batch_ref}:{entry_id if entry_id is not None else 'raw'}"

    def save_receipt(self, source: str, batch_ref: str, entry_id: int | None,
                     reason: str, final_status: str) -> tuple[QuarantineReceipt, bool]:
        """保存唯一隔离回执。

        Returns:
            (回执, 是否新建)：同一记录再次写入时原地升版，不新增回执。
        """
        key = self.make_record_key(source, batch_ref, entry_id)
        with self._lock:
            existing = self._receipts.get(key)
            if existing is not None:
                existing.reason = reason
                existing.final_status = final_status
                existing.review_status = (
                    "待复核" if final_status == QUALITY_PENDING else "已复核")
                existing.version += 1
                existing.written_at = datetime.now(timezone.utc).isoformat()
                return existing, False
            self._seq += 1
            receipt = QuarantineReceipt(
                receipt_id=self._seq, record_key=key, source_module=source,
                batch_ref=batch_ref, entry_id=entry_id, reason=reason,
                final_status=final_status,
                review_status="待复核" if final_status == QUALITY_PENDING else "已复核",
            )
            self._receipts[key] = receipt
            return receipt, True

    def get(self, record_key: str) -> QuarantineReceipt | None:
        with self._lock:
            return self._receipts.get(record_key)

    def list_by_batch(self, batch_ref: str) -> list[QuarantineReceipt]:
        with self._lock:
            return [r for r in self._receipts.values() if r.batch_ref == batch_ref]

    def list_pending_review(self, batch_ref: str | None = None) -> list[QuarantineReceipt]:
        with self._lock:
            return [
                r for r in self._receipts.values()
                if r.review_status == "待复核"
                and (batch_ref is None or r.batch_ref == batch_ref)
            ]

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            receipts = list(self._receipts.values())
            return {
                "total": len(receipts),
                "pending_review": sum(1 for r in receipts if r.review_status == "待复核"),
                "reviewed": sum(1 for r in receipts if r.review_status == "已复核"),
                "max_version": max((r.version for r in receipts), default=0),
            }


# ---------------------------------------------------------------------------
# 质量报告台账 (QualityReportLedger) — 每批次只保留一个当前最终质量状态
# ---------------------------------------------------------------------------

@dataclass
class QualityReport:
    """批次质量报告：一个批次的最终质量状态只保留一个当前版本。"""
    report_id: int
    batch_ref: str
    status: str                     # 最终质量状态：通过 / 未通过 / 待复核
    passed: int
    failed: int
    uncovered: int                  # 未覆盖（待复核）记录数
    sampled_records: int            # 抽样记录数
    unsampled_records: int          # 未抽样记录数
    uncovered_list: list[int] = field(default_factory=list)  # 未覆盖清单（entry_id）
    strategy: str | None = None     # 冲突处理方案：resample / append / None
    version: int = 1
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class QualityReportLedger:
    """质量报告台账：低覆盖批次与已发布结果冲突时，无论整批重抽替换
    还是保留旧报告追加未覆盖清单，同一批次只保留一个当前最终质量状态。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._seq = 0
        self._reports: dict[str, QualityReport] = {}  # batch_ref -> 当前版本

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._reports)

    def publish(self, batch_ref: str, *, status: str, passed: int, failed: int,
                uncovered: int, sampled_records: int, unsampled_records: int,
                uncovered_list: list[int] | None = None,
                strategy: str | None = None) -> QualityReport:
        """发布/替换批次报告：同一批次只保留一个当前版本。"""
        with self._lock:
            existing = self._reports.get(batch_ref)
            version = existing.version + 1 if existing else 1
            self._seq += 1
            report = QualityReport(
                report_id=self._seq, batch_ref=batch_ref, status=status,
                passed=passed, failed=failed, uncovered=uncovered,
                sampled_records=sampled_records,
                unsampled_records=unsampled_records,
                uncovered_list=list(uncovered_list or []),
                strategy=strategy, version=version,
            )
            self._reports[batch_ref] = report
            return report

    def append_uncovered(self, batch_ref: str,
                         uncovered_list: list[int]) -> QualityReport | None:
        """保留旧报告，追加未覆盖清单（仍只有一个当前版本）。"""
        with self._lock:
            report = self._reports.get(batch_ref)
            if report is None:
                return None
            merged = sorted(set(report.uncovered_list) | set(uncovered_list))
            report.uncovered_list = merged
            report.uncovered = len(merged)
            report.strategy = "append"
            report.version += 1
            report.created_at = datetime.now(timezone.utc).isoformat()
            return report

    def current(self, batch_ref: str) -> QualityReport | None:
        with self._lock:
            return self._reports.get(batch_ref)

    def list_current(self) -> list[QualityReport]:
        """全部批次的当前报告（每批次只有一个最终质量状态）。"""
        with self._lock:
            return list(self._reports.values())

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            reports = list(self._reports.values())
            by_status: dict[str, int] = {}
            for r in reports:
                by_status[r.status] = by_status.get(r.status, 0) + 1
            return {
                "total": len(reports),
                "by_status": by_status,
                "versions": {r.batch_ref: r.version for r in reports},
            }


# ---------------------------------------------------------------------------
# 失败恢复管理 (FailureRecovery)
# ---------------------------------------------------------------------------

@dataclass
class RecoveryEntry:
    recovery_id: int
    module: str
    batch_ref: str
    step: str
    error_detail: str
    attempt: int = 0
    max_attempts: int = 3
    resolved: bool = False
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class FailureRecovery:
    """失败恢复管理器：记录失败步骤并支持自动重试。

    用法::

        rec = store.recovery.log("step_editor", "BATC-001", "元素定位",
                                  "选择器超时未匹配")
        store.recovery.retry(rec.recovery_id)
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._seq = 0
        self._entries: list[RecoveryEntry] = []

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._entries)

    def log(self, module: str, batch_ref: str, step: str,
            error_detail: str, max_attempts: int = 3) -> RecoveryEntry:
        with self._lock:
            self._seq += 1
            entry = RecoveryEntry(
                recovery_id=self._seq, module=module,
                batch_ref=batch_ref, step=step,
                error_detail=error_detail, max_attempts=max_attempts,
            )
            self._entries.append(entry)
            return entry

    def retry(self, recovery_id: int) -> tuple[bool, str]:
        with self._lock:
            for e in self._entries:
                if e.recovery_id == recovery_id:
                    if e.resolved:
                        return False, "已恢复，无需重试"
                    e.attempt += 1
                    if e.attempt > e.max_attempts:
                        return False, f"已达最大重试次数 {e.max_attempts}"
                    return True, f"重试 {e.attempt}/{e.max_attempts}"
            return False, "恢复记录不存在"

    def resolve(self, recovery_id: int) -> bool:
        with self._lock:
            for e in self._entries:
                if e.recovery_id == recovery_id:
                    e.resolved = True
                    return True
            return False

    def pending_count(self) -> int:
        with self._lock:
            return sum(1 for e in self._entries if not e.resolved)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            pending = sum(1 for e in self._entries if not e.resolved)
            return {
                "total": len(self._entries),
                "resolved": len(self._entries) - pending,
                "pending": pending,
            }


# ---------------------------------------------------------------------------
# 增强版 Store — 整合通道、脏数据、采集记录、恢复
# ---------------------------------------------------------------------------

class Store:
    def __init__(self) -> None:
        self._tables: dict[str, list[dict[str, Any]]] = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }
        # 跨模块基础设施
        self.channel = DataChannel()
        self.dirty_vault = DirtyDataVault()
        self.collect_records = CollectRecordStore()
        self.recovery = FailureRecovery()
        self.quality_lineage = QualityLineageStore()
        self.quarantine_ledger = QuarantineLedger()
        self.quality_reports = QualityReportLedger()

    # ---- 基础数据操作 ----

    def module_names(self) -> list[str]:
        return sorted(self._tables)

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._tables.setdefault(module, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
        ]
        return {"cards": cards, "modules": modules}


store = Store()