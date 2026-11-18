/**
 * 作业编排与故障转移链路接口封装。
 *
 * 对应后端 /api/orchestration 路由：
 * 作业配置 → 依赖校验 → 编排数据 → 拓扑部署 → 故障转移清单
 * → 序号25异常/恢复 → 双故障取舍（保留拓扑切备用源 / 停止重建）
 */
import { request } from './client'

export type DependencyRef = {
  ref: string
  value: string
  exists: boolean
  status: string
  valid: boolean
  reason: string
}

export type CallChainNode = {
  step: string
  ref: string | null
  status: string
  valid: boolean
}

export type ExceptionRecord = {
  exception_id: string
  boundary_no: number
  error_type: string
  error_detail: string
  recorded_at: string
  recovered: boolean
}

export type FailoverEntry = {
  order: number
  action: string
  target: string
  mode: string
  status: string
}

export type BackupPath = {
  path_id: string
  type: string
  switch_to: string
  status: string
}

export type FailoverManifest = {
  manifest_id: string
  job_id: number
  status: string
  published_at: string
  entries: FailoverEntry[]
  backup_paths: BackupPath[]
  refs: DependencyRef[]
}

export type JobConfig = {
  job_id: number
  job_name: string
  parallelism: string
  source: {
    name: string
    queue: string
    topic: string
    format: string
    consumer_group: string
    status: string
  }
  window: {
    name: string
    type: string
    size: string
    slide: string
    watermark_delay: string
    status: string
  }
}

export type Orchestration = {
  job_id: number
  job_name: string
  version: number
  is_current: boolean
  status: string
  source: string
  window: string
  parallelism: string
  orchestration_ready: boolean
  current_topology_id: number | null
  dependency_refs: DependencyRef[]
  call_chain: CallChainNode[]
  failover_manifest: FailoverManifest | null
  exceptions: ExceptionRecord[]
}

export type TopologyDetail = {
  topology_id: number
  topology_name: string
  status: string
  run_state: string
  job_id: number | null
  job_version: number | null
  is_current: boolean
  chain: string[]
  current_path: string[]
  backup_paths: BackupPath[]
  failover_results: Record<string, unknown>[]
  manifest_status: string
  exception_reasons: ExceptionRecord[]
}

export type ExecutableVersion = {
  job_id: number
  job_name: string
  version: number
  status: string
  current_topology_id: number | null
  manifest_published: boolean
  boundary_25_exception: boolean
}

export type TopologyListItem = {
  topology_id: number
  topology_name: string
  job_id: number | null
  status: string
  is_current: boolean
  backup_path_count: number
}

async function postJson<T>(path: string, body?: unknown): Promise<T> {
  const response = await request(path, {
    method: 'POST',
    body: JSON.stringify(body ?? {}),
  })
  return (await response.json()) as T
}

async function getJson<T>(path: string): Promise<T> {
  const response = await request(path)
  return (await response.json()) as T
}

const BASE = '/api/orchestration'

export const orchestrationApi = {
  build(sourceName: string, windowName: string, jobName?: string, parallelism = 4) {
    return postJson<{ ok: boolean; job_id?: number; idempotent?: boolean; error?: string; rolled_back?: boolean }>(
      `${BASE}/build`,
      { source_name: sourceName, window_name: windowName, job_name: jobName, parallelism },
    )
  },
  readConfig(jobId: number) {
    return getJson<{ ok: boolean; error?: string; config: JobConfig | null }>(
      `${BASE}/jobs/${jobId}/config`,
    )
  },
  getOrchestration(jobId: number) {
    return getJson<{ ok: boolean; empty: boolean; reason?: string; orchestration: Orchestration | null }>(
      `${BASE}/jobs/${jobId}`,
    )
  },
  validate(jobId: number) {
    return postJson<{ ok: boolean; all_valid: boolean; refs: DependencyRef[] }>(
      `${BASE}/validate`,
      { job_id: jobId },
    )
  },
  start(jobId: number) {
    return postJson<{ ok: boolean; idempotent?: boolean; topology_id?: number; topology_name?: string; backup_paths?: BackupPath[]; error?: string; rolled_back?: boolean }>(
      `${BASE}/jobs/${jobId}/start`,
    )
  },
  publishManifest(jobId: number) {
    return postJson<{ ok: boolean; idempotent?: boolean; manifest?: FailoverManifest; error?: string; rolled_back?: boolean }>(
      `${BASE}/jobs/${jobId}/failover-manifest/publish`,
    )
  },
  getManifest(jobId: number) {
    return getJson<{ ok: boolean; empty: boolean; reason?: string; manifest: FailoverManifest | null }>(
      `${BASE}/jobs/${jobId}/failover-manifest`,
    )
  },
  recordException(jobId: number, errorType: string, errorDetail: string, boundaryNo = 25) {
    return postJson<{ ok: boolean; idempotent?: boolean; exception?: ExceptionRecord; message?: string }>(
      `${BASE}/jobs/${jobId}/exceptions`,
      { error_type: errorType, error_detail: errorDetail, boundary_no: boundaryNo },
    )
  },
  recover25(jobId: number) {
    return postJson<{ ok: boolean; idempotent?: boolean; recovery?: { boundary_no: number; status: string; recovered_count: number }; message?: string }>(
      `${BASE}/jobs/${jobId}/exceptions/recover-25`,
    )
  },
  dualFailure(jobId: number, strategy: 'keep_topology' | 'rebuild', backupSource?: string, backupWindow?: string) {
    return postJson<{
      ok: boolean
      strategy?: string
      terminal_state?: string
      job_id?: number
      old_job_id?: number
      version?: number
      current_topology_id?: number | null
      executable_jobs?: ExecutableVersion[]
      error?: string
      rolled_back?: boolean
    }>(`${BASE}/jobs/${jobId}/dual-failure`, {
      strategy,
      backup_source: backupSource,
      backup_window: backupWindow,
    })
  },
  executableVersions() {
    return getJson<{ ok: boolean; empty: boolean; total: number; items: ExecutableVersion[] }>(
      `${BASE}/jobs/executable/versions`,
    )
  },
  listTopologies() {
    return getJson<{ ok: boolean; empty: boolean; total: number; items: TopologyListItem[] }>(
      `${BASE}/topologies`,
    )
  },
  topologyDetail(topologyId: number) {
    return getJson<{ ok: boolean; empty: boolean; reason?: string; detail: TopologyDetail | null }>(
      `${BASE}/topologies/${topologyId}`,
    )
  },
  boundary(jobId: number, boundaryNo = 25) {
    return getJson<{
      ok: boolean
      empty: boolean
      boundary_no: number
      job_id?: number
      version?: number
      orchestration_ready?: boolean
      status?: string
      terminal_state?: string
      exceptions: ExceptionRecord[]
      topology_id?: number | null
      manifest_published?: boolean
      reason?: string
    }>(`${BASE}/jobs/${jobId}/boundary/${boundaryNo}`)
  },
}
