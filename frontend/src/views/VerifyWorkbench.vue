<template>
  <section class="verify-workbench" data-testid="verify-workbench">
    <header class="page-head">
      <div>
        <h3>校验工作台</h3>
        <p class="page-desc">
          抽样覆盖口径：未执行的规则不计为通过；抽样记录与未抽样记录分开计数；
          隔离回执与当前最终质量状态保持一致。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="run-verify" type="button" :disabled="running" @click="runVerify">
          {{ running ? '校验中…' : '执行采集校验' }}
        </button>
        <button class="btn" data-testid="refresh-verify" type="button" @click="reload">刷新</button>
      </div>
    </header>

    <p v-if="consistency" class="consistency-line" :class="{ bad: !consistency.consistent }" data-testid="consistency">
      待复核一致性：质量列表 {{ consistency.lineage_pending }} ／ 隔离回执 {{ consistency.receipts_pending }} ／ 隔离区
      {{ consistency.vault_pending_review }}
      <strong>{{ consistency.consistent ? '一致' : '不一致' }}</strong>
    </p>

    <h4 class="block-title">规则覆盖（抽样数／执行数／覆盖状态）</h4>
    <table class="data-table" data-testid="rule-coverage-table">
      <thead>
        <tr>
          <th>规则名称</th>
          <th>校验字段</th>
          <th>抽样比例</th>
          <th>抽样数</th>
          <th>执行数</th>
          <th>跳过数</th>
          <th>未通过数</th>
          <th>覆盖率</th>
          <th>覆盖状态</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="rule in rules" :key="rule.rule_id" :data-testid="`rule-coverage-${rule.rule_id}`">
          <td>{{ rule.规则名称 }}</td>
          <td>{{ rule.校验字段 }}</td>
          <td>{{ rule.抽样比例 }}</td>
          <td>{{ rule.抽样数 }}</td>
          <td>{{ rule.执行数 }}</td>
          <td>{{ rule.跳过数 }}</td>
          <td>{{ rule.未通过数 }}</td>
          <td>{{ rule.覆盖率 }}</td>
          <td><span class="tag" :class="coverageClass(rule.覆盖状态)">{{ rule.覆盖状态 }}</span></td>
        </tr>
        <tr v-if="!rules.length">
          <td colspan="9" class="empty-state">暂无已启用校验规则</td>
        </tr>
      </tbody>
    </table>

    <h4 class="block-title">批次最终质量状态</h4>
    <table class="data-table" data-testid="batch-report-table">
      <thead>
        <tr>
          <th>批次</th>
          <th>最终质量状态</th>
          <th>通过</th>
          <th>未通过</th>
          <th>待复核</th>
          <th>抽样记录</th>
          <th>未抽样记录</th>
          <th>报告版本</th>
          <th>低覆盖处理</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="report in reports" :key="report.batch_ref" :data-testid="`batch-report-${report.batch_ref}`">
          <td>{{ report.batch_ref }}</td>
          <td><span class="tag" :class="statusClass(report.status)">{{ report.status }}</span></td>
          <td>{{ report.passed }}</td>
          <td>{{ report.failed }}</td>
          <td>{{ report.uncovered }}</td>
          <td>{{ report.sampled_records }}</td>
          <td>{{ report.unsampled_records }}</td>
          <td>v{{ report.version }}</td>
          <td class="row-actions">
            <template v-if="report.uncovered > 0">
              <button class="link" type="button" :data-testid="`resample-${report.batch_ref}`"
                @click="resolveBatch(report.batch_ref, 'resample')">整批重抽替换</button>
              <button class="link" type="button" :data-testid="`append-${report.batch_ref}`"
                @click="resolveBatch(report.batch_ref, 'append')">保留并追加未覆盖清单</button>
            </template>
            <span v-else class="muted">—</span>
          </td>
        </tr>
        <tr v-if="!reports.length">
          <td colspan="9" class="empty-state">暂无质量报告，可先执行采集校验</td>
        </tr>
      </tbody>
    </table>

    <h4 class="block-title">隔离区待复核（隔离回执）</h4>
    <table class="data-table" data-testid="quarantine-table">
      <thead>
        <tr>
          <th>回执号</th>
          <th>批次</th>
          <th>记录</th>
          <th>隔离原因</th>
          <th>最终质量状态</th>
          <th>复核状态</th>
          <th>版本</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="item in quarantine" :key="item.receipt_id" :data-testid="`quarantine-${item.receipt_id}`">
          <td>#{{ item.receipt_id }}</td>
          <td>{{ item.batch_ref }}</td>
          <td>{{ item.entry_id }}</td>
          <td>{{ item.reason }}</td>
          <td><span class="tag" :class="statusClass(item.final_status)">{{ item.final_status }}</span></td>
          <td>{{ item.review_status }}</td>
          <td>v{{ item.version }}</td>
        </tr>
        <tr v-if="!quarantine.length">
          <td colspan="7" class="empty-state">隔离区暂无待复核记录</td>
        </tr>
      </tbody>
    </table>

    <h4 class="block-title">校验收据明细</h4>
    <table class="data-table" data-testid="receipt-table">
      <thead>
        <tr>
          <th>批次</th>
          <th>记录</th>
          <th>规则数</th>
          <th>抽样数</th>
          <th>执行数</th>
          <th>覆盖状态</th>
          <th>最终质量状态</th>
          <th>逐规则明细</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="receipt in receipts" :key="receipt.lineage_id" :data-testid="`receipt-${receipt.lineage_id}`">
          <td>{{ receipt.batch_ref }}</td>
          <td>{{ receipt.entry_id }}</td>
          <td>{{ receipt.rules_total }}</td>
          <td>{{ receipt.rules_sampled }}</td>
          <td>{{ receipt.rules_executed }}</td>
          <td><span class="tag" :class="coverageClass(receipt.coverage_status)">{{ receipt.coverage_status }}</span></td>
          <td><span class="tag" :class="statusClass(receipt.final_status)">{{ receipt.final_status }}</span></td>
          <td>
            <ul class="rule-detail-list">
              <li v-for="detail in receipt.rule_details" :key="detail.rule_id">
                {{ detail.rule_name }}：{{ detail.coverage }}<template v-if="detail.note">（{{ detail.note }}）</template>
              </li>
            </ul>
          </td>
        </tr>
        <tr v-if="!receipts.length">
          <td colspan="8" class="empty-state">暂无校验收据</td>
        </tr>
      </tbody>
    </table>

    <p v-if="errorMessage" class="error-text">{{ errorMessage }}</p>
    <p v-if="notice" class="notice-text" data-testid="verify-notice">{{ notice }}</p>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import {
  loadVerifyLineage,
  loadVerifyQuarantine,
  loadVerifyWorkbenchSummary,
  resolveVerifyBatch,
  runVerifyCollected,
} from '@/api/backend-service'

type RuleCoverage = {
  rule_id: number
  规则名称: string
  校验字段: string
  抽样比例: number
  抽样数: number
  执行数: number
  跳过数: number
  未通过数: number
  覆盖率: number
  覆盖状态: string
}

type BatchReport = {
  batch_ref: string
  status: string
  passed: number
  failed: number
  uncovered: number
  sampled_records: number
  unsampled_records: number
  uncovered_list: number[]
  strategy: string | null
  version: number
}

type QuarantineItem = {
  receipt_id: number
  batch_ref: string
  entry_id: number
  reason: string
  final_status: string
  review_status: string
  version: number
}

type ReceiptItem = {
  lineage_id: number
  batch_ref: string
  entry_id: number
  rules_total: number
  rules_sampled: number
  rules_executed: number
  coverage_status: string
  final_status: string
  rule_details: { rule_id: number; rule_name: string; coverage: string; note: string }[]
}

type Consistency = {
  consistent: boolean
  lineage_pending: number
  receipts_pending: number
  vault_pending_review: number
}

const rules = ref<RuleCoverage[]>([])
const reports = ref<BatchReport[]>([])
const quarantine = ref<QuarantineItem[]>([])
const receipts = ref<ReceiptItem[]>([])
const consistency = ref<Consistency | null>(null)
const running = ref(false)
const errorMessage = ref('')
const notice = ref('')

function coverageClass(status: string) {
  return {
    'tag-full': status === '已覆盖',
    'tag-partial': status === '部分覆盖',
    'tag-none': status === '未覆盖',
  }
}

function statusClass(status: string) {
  return {
    'tag-pass': status === '通过',
    'tag-fail': status === '未通过',
    'tag-pending': status === '待复核',
  }
}

async function reload() {
  errorMessage.value = ''
  try {
    const summary = (await loadVerifyWorkbenchSummary()) as {
      rules: RuleCoverage[]
      reports: BatchReport[]
      consistency: Consistency
    }
    rules.value = summary.rules || []
    reports.value = summary.reports || []
    consistency.value = summary.consistency || null

    const quarantineData = (await loadVerifyQuarantine()) as { items: QuarantineItem[] }
    quarantine.value = quarantineData.items || []

    const lineageData = (await loadVerifyLineage()) as { items: ReceiptItem[] }
    receipts.value = lineageData.items || []
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '校验工作台读取失败'
  }
}

async function runVerify() {
  running.value = true
  errorMessage.value = ''
  notice.value = ''
  try {
    const payload = (await runVerifyCollected()) as {
      result: { verified: number; passed: number; failed: number; uncovered: number }
    }
    const result = payload.result
    notice.value = `校验完成：共 ${result.verified} 条，通过 ${result.passed}，未通过 ${result.failed}，待复核 ${result.uncovered}`
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '校验执行失败'
  } finally {
    running.value = false
  }
}

async function resolveBatch(batchRef: string, strategy: 'resample' | 'append') {
  errorMessage.value = ''
  notice.value = ''
  try {
    const result = (await resolveVerifyBatch(batchRef, strategy)) as {
      ok: boolean
      final_status?: string
      error?: string
    }
    if (!result.ok) throw new Error(result.error || '处理失败')
    notice.value = `批次 ${batchRef} 已按「${strategy === 'resample' ? '整批重抽替换' : '保留并追加未覆盖清单'}」处理，最终质量状态：${result.final_status}`
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '低覆盖批次处理失败'
  }
}

onMounted(reload)
</script>

<style scoped>
.verify-workbench {
  margin-top: 20px;
  border-top: 2px solid var(--border);
  padding-top: 12px;
}

.block-title {
  margin: 16px 0 8px;
  font-size: 14px;
}

.consistency-line {
  background: #ecfdf3;
  border: 1px solid #a6f4c5;
  border-radius: 6px;
  padding: 8px 12px;
  font-size: 13px;
}

.consistency-line.bad {
  background: #fef3f2;
  border-color: #fecdca;
}

.tag {
  display: inline-block;
  padding: 2px 8px;
  border-radius: 10px;
  font-size: 12px;
  background: #eef2f6;
}

.tag-full,
.tag-pass {
  background: #dcfae6;
  color: #067647;
}

.tag-partial,
.tag-pending {
  background: #fef0c7;
  color: #b54708;
}

.tag-none,
.tag-fail {
  background: #fee4e2;
  color: #b42318;
}

.muted {
  color: var(--muted);
}

.rule-detail-list {
  margin: 0;
  padding-left: 16px;
  font-size: 12px;
  color: var(--muted);
}

.notice-text {
  color: #067647;
  font-size: 13px;
}
</style>
