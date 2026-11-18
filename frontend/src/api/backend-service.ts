import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

/** 跨模块数据流：获取全平台领域快照 */
export async function loadNodeStats(module?: string): Promise<Record<string, unknown>> {
  const query = module ? `?module=${module}` : ''
  const response = await request(`/api/nodestats${query}`)
  if (!response.ok) throw new Error('节点统计读取失败')
  return await response.json()
}

/** 跨模块数据流：获取指定 topic 的通道消息 */
export async function loadChannelSnapshot(topic: string): Promise<Record<string, unknown>> {
  const response = await request(`/api/channel/${topic}`)
  if (!response.ok) throw new Error('通道快照读取失败')
  return await response.json()
}

/** 跨模块数据流：查询脏数据隔离区 */
export async function loadDirtyVault(source?: string): Promise<Record<string, unknown>> {
  const query = source ? `?source=${source}` : ''
  const response = await request(`/api/dirty-vault${query}`)
  if (!response.ok) throw new Error('脏数据查询失败')
  return await response.json()
}

/** 跨模块数据流：查询采集记录 */
export async function loadCollectRecords(batchRef?: string): Promise<Record<string, unknown>> {
  const query = batchRef ? `?batch_ref=${batchRef}` : ''
  const response = await request(`/api/collect-records${query}`)
  if (!response.ok) throw new Error('采集记录查询失败')
  return await response.json()
}

/** 跨模块数据流：执行RPA全生命周期管道 */
export async function triggerPipeline(lifecycle: string): Promise<Record<string, unknown>> {
  const response = await request(`/api/pipeline/${lifecycle}`, {
    method: 'POST',
  })
  if (!response.ok) throw new Error('管道执行失败')
  return await response.json()
}

/** 跨模块数据流：获取失败恢复状态 */
export async function loadRecoveryStats(): Promise<Record<string, unknown>> {
  const response = await request('/api/recovery')
  if (!response.ok) throw new Error('恢复状态查询失败')
  return await response.json()
}

/** 校验工作台：规则覆盖统计（抽样数/执行数/覆盖状态）与一致性核对 */
export async function loadVerifyWorkbenchSummary(): Promise<Record<string, unknown>> {
  const response = await request('/api/verify-workbench/summary')
  if (!response.ok) throw new Error('校验工作台读取失败')
  return await response.json()
}

/** 校验工作台：采集质量谱系数据集（带覆盖状态） */
export async function loadVerifyLineage(batchRef?: string): Promise<Record<string, unknown>> {
  const query = batchRef ? `?batch_ref=${encodeURIComponent(batchRef)}` : ''
  const response = await request(`/api/verify-workbench/lineage${query}`)
  if (!response.ok) throw new Error('采集质量谱系读取失败')
  return await response.json()
}

/** 校验工作台：隔离区待复核记录（隔离回执） */
export async function loadVerifyQuarantine(batchRef?: string): Promise<Record<string, unknown>> {
  const query = batchRef ? `?batch_ref=${encodeURIComponent(batchRef)}` : ''
  const response = await request(`/api/verify-workbench/quarantine${query}`)
  if (!response.ok) throw new Error('隔离待复核记录读取失败')
  return await response.json()
}

/** 校验工作台：执行采集结果校验 */
export async function runVerifyCollected(): Promise<Record<string, unknown>> {
  const response = await request('/api/verify-workbench/verify', { method: 'POST' })
  if (!response.ok) throw new Error('校验执行失败')
  return await response.json()
}

/** 校验工作台：处理低覆盖批次冲突（resample 整批重抽替换 / append 保留并追加未覆盖清单） */
export async function resolveVerifyBatch(
  batchRef: string,
  strategy: 'resample' | 'append',
): Promise<Record<string, unknown>> {
  const response = await request('/api/verify-workbench/resolve-batch', {
    method: 'POST',
    body: JSON.stringify({ batch_ref: batchRef, strategy }),
  })
  if (!response.ok) throw new Error('低覆盖批次处理失败')
  return await response.json()
}
