<template>
  <section class="page" data-testid="stream-job-page">
    <header class="page-head">
      <div>
        <h2>流式作业编排</h2>
        <p class="page-desc">
          读取源端与窗口配置、校验拓扑依赖后形成完整作业编排数据，再部署任务拓扑并发布故障转移清单。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" data-testid="refresh-jobs" type="button" @click="reloadJobs">刷新可执行版本</button>
      </div>
    </header>

    <div class="stat-row">
      <article class="stat-card">
        <span class="stat-label">可执行作业版本</span>
        <strong class="stat-value">{{ versions.length }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">运行中版本</span>
        <strong class="stat-value">{{ runningCount }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">序号25异常作业</span>
        <strong class="stat-value">{{ boundaryJobCount }}</strong>
      </article>
    </div>

    <form class="analysis-form" data-testid="build-form" @submit.prevent="submitBuild">
      <label>
        源端引用
        <input v-model="buildForm.sourceName" data-testid="build-source" placeholder="如：流式源端样例2" required />
      </label>
      <label>
        窗口引用
        <input v-model="buildForm.windowName" data-testid="build-window" placeholder="如：窗口配置样例2" required />
      </label>
      <label>
        作业名称
        <input v-model="buildForm.jobName" data-testid="build-job-name" placeholder="可选，默认自动生成" />
      </label>
      <label>
        并行度
        <input v-model.number="buildForm.parallelism" data-testid="build-parallelism" type="number" min="1" max="64" />
      </label>
      <button class="btn primary" data-testid="build-submit" type="submit" :disabled="busy">
        {{ busy ? '提交中…' : '形成作业编排数据' }}
      </button>
    </form>

    <p v-if="message" class="page-foot" data-testid="build-message">
      <span :class="messageOk ? '' : 'error-text'">{{ message }}</span>
    </p>

    <table class="data-table" data-testid="job-version-table">
      <thead>
        <tr>
          <th>作业ID</th><th>作业名称</th><th>版本</th><th>状态</th><th>当前拓扑ID</th><th>故障转移清单</th><th>序号25异常</th><th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="item in versions" :key="item.job_id" :data-testid="`job-row-${item.job_id}`">
          <td>{{ item.job_id }}</td>
          <td>{{ item.job_name }}</td>
          <td>v{{ item.version }}</td>
          <td>{{ item.status }}</td>
          <td>{{ item.current_topology_id ?? '—' }}</td>
          <td>{{ item.manifest_published ? '已发布' : '暂无数据' }}</td>
          <td>{{ item.boundary_25_exception ? '有异常' : '无' }}</td>
          <td class="row-actions">
            <button class="link" type="button" @click="selectJob(item.job_id)">查看编排</button>
          </td>
        </tr>
        <tr v-if="!versions.length">
          <td colspan="8" class="empty-state" data-testid="job-empty-state">
            暂无可执行作业版本，请先形成完整作业编排数据
          </td>
        </tr>
      </tbody>
    </table>

    <div v-if="selectedJobId !== null" class="timeline-wrap" data-testid="job-detail" style="margin-top: 16px">
      <div class="page-head">
        <div>
          <h3>作业 #{{ selectedJobId }} 编排数据（按版本核对）</h3>
          <p class="page-desc">跟着流式作业调用链定位异常，源端与窗口引用全部有效后清单才可发布。</p>
        </div>
        <div class="page-actions">
          <button class="btn primary" type="button" :disabled="busy" @click="startSelected">部署拓扑/启动</button>
          <button class="btn" type="button" :disabled="busy" @click="publishSelected">发布故障转移清单</button>
        </div>
      </div>

      <template v-if="orchestration">
        <dl class="dag-detail">
          <dt>作业版本</dt><dd>v{{ orchestration.version }}（{{ orchestration.is_current ? '当前版本' : '历史版本' }}）</dd>
          <dt>运行状态</dt><dd>{{ orchestration.status }}</dd>
          <dt>源端引用</dt><dd>{{ orchestration.source }}</dd>
          <dt>窗口引用</dt><dd>{{ orchestration.window }}</dd>
          <dt>并行度</dt><dd>{{ orchestration.parallelism }}</dd>
          <dt>当前拓扑</dt><dd>{{ orchestration.current_topology_id ?? '未部署' }}</dd>
        </dl>

        <h4>流式作业调用链</h4>
        <table class="data-table" data-testid="call-chain-table">
          <thead><tr><th>环节</th><th>引用</th><th>状态</th><th>是否有效</th></tr></thead>
          <tbody>
            <tr v-for="node in orchestration.call_chain" :key="node.step" :data-testid="`chain-${node.step}`">
              <td>{{ node.step }}</td>
              <td>{{ node.ref ?? '—' }}</td>
              <td>{{ node.status }}</td>
              <td>{{ node.valid ? '有效' : '未就绪' }}</td>
            </tr>
          </tbody>
        </table>

        <h4>拓扑依赖校验</h4>
        <table class="data-table" data-testid="dependency-table">
          <thead><tr><th>依赖</th><th>引用值</th><th>状态</th><th>校验结果</th><th>原因</th></tr></thead>
          <tbody>
            <tr v-for="ref in orchestration.dependency_refs" :key="ref.ref">
              <td>{{ ref.ref }}</td>
              <td>{{ ref.value }}</td>
              <td>{{ ref.status || '—' }}</td>
              <td>{{ ref.valid ? '有效' : '失效' }}</td>
              <td>{{ ref.reason || '—' }}</td>
            </tr>
          </tbody>
        </table>

        <h4>故障转移清单</h4>
        <table v-if="orchestration.failover_manifest" class="data-table" data-testid="manifest-table">
          <thead><tr><th>顺序</th><th>动作</th><th>目标</th><th>模式</th><th>状态</th></tr></thead>
          <tbody>
            <tr v-for="entry in orchestration.failover_manifest.entries" :key="entry.order">
              <td>{{ entry.order }}</td><td>{{ entry.action }}</td><td>{{ entry.target }}</td>
              <td>{{ entry.mode }}</td><td>{{ entry.status }}</td>
            </tr>
          </tbody>
        </table>
        <p v-else class="empty-state" data-testid="manifest-empty">
          故障转移清单暂无数据：源端与窗口引用未全部有效或尚未发布
        </p>

        <h4>序号25异常记录与恢复（失败/重试/中断，重复异常只记一次）</h4>
        <div class="analysis-form">
          <button class="btn" type="button" :disabled="busy" @click="reportException('failure')">记录失败</button>
          <button class="btn" type="button" :disabled="busy" @click="reportException('retry')">记录重试</button>
          <button class="btn" type="button" :disabled="busy" @click="reportException('interrupt')">记录中断</button>
          <button class="btn primary" type="button" :disabled="busy" @click="recover25">序号25恢复（只执行一次）</button>
        </div>
        <table class="data-table" data-testid="exception-table">
          <thead><tr><th>异常ID</th><th>边界</th><th>类型</th><th>原因</th><th>记录时间</th><th>已恢复</th></tr></thead>
          <tbody>
            <tr v-for="exc in orchestration.exceptions" :key="exc.exception_id" :data-testid="`exc-${exc.exception_id}`">
              <td>{{ exc.exception_id }}</td>
              <td>{{ exc.boundary_no }}</td>
              <td>{{ exc.error_type }}</td>
              <td>{{ exc.error_detail }}</td>
              <td>{{ exc.recorded_at }}</td>
              <td>{{ exc.recovered ? '是' : '否' }}</td>
            </tr>
          </tbody>
        </table>
        <p v-if="!orchestration.exceptions.length" class="empty-state" data-testid="exception-empty">
          序号25暂无异常数据
        </p>

        <h4>双故障取舍（源端失效 + 窗口停用同时发生）</h4>
        <div class="analysis-form">
          <label>
            备用源端
            <input v-model="backupForm.source" placeholder="如：流式源端样例1" />
          </label>
          <label>
            备用窗口
            <input v-model="backupForm.window" placeholder="如：窗口配置样例1" />
          </label>
          <button class="btn" type="button" :disabled="busy" @click="dualFailure('keep_topology')">
            保留拓扑切备用源
          </button>
          <button class="btn primary" type="button" :disabled="busy" @click="dualFailure('rebuild')">
            停止作业后重建
          </button>
        </div>

        <p class="page-foot">
          <RouterLink class="link" :to="`/task_topology/current/${orchestration.current_topology_id ?? ''}`">
            前往任务拓扑页核对当前路径与故障转移清单 →
          </RouterLink>
        </p>
      </template>

      <p v-else class="empty-state" data-testid="detail-empty">
        {{ detailEmptyReason || '作业编排数据加载中…' }}
      </p>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref, watch } from 'vue'
import { useRoute } from 'vue-router'

import { orchestrationApi } from '@/api/orchestration'
import type { ExecutableVersion, Orchestration } from '@/api/orchestration'

const route = useRoute()
const versions = ref<ExecutableVersion[]>([])
const selectedJobId = ref<number | null>(null)
const orchestration = ref<Orchestration | null>(null)
const detailEmptyReason = ref('')
const message = ref('')
const messageOk = ref(true)
const busy = ref(false)

const buildForm = reactive({
  sourceName: '流式源端样例2',
  windowName: '窗口配置样例2',
  jobName: '',
  parallelism: 4,
})
const backupForm = reactive({ source: '流式源端样例1', window: '窗口配置样例1' })

const runningCount = computed(() => versions.value.filter((item) => item.status === '运行中').length)
const boundaryJobCount = computed(() =>
  versions.value.filter((item) => item.boundary_25_exception).length,
)

function notify(text: string, ok = true) {
  message.value = text
  messageOk.value = ok
}

async function reloadJobs() {
  try {
    const payload = await orchestrationApi.executableVersions()
    versions.value = payload.items
  } catch (error) {
    notify(error instanceof Error ? error.message : '可执行版本读取失败', false)
  }
}

async function submitBuild() {
  busy.value = true
  try {
    const result = await orchestrationApi.build(
      buildForm.sourceName.trim(),
      buildForm.windowName.trim(),
      buildForm.jobName.trim() || undefined,
      buildForm.parallelism,
    )
    if (!result.ok) throw new Error(result.error || '编排数据构建失败（事务已回滚，无半成品）')
    notify(result.idempotent ? '相同引用的编排已存在，幂等返回原作业版本' : '作业编排数据已形成')
    await reloadJobs()
    if (result.job_id) await selectJob(result.job_id)
  } catch (error) {
    notify(error instanceof Error ? error.message : '编排数据构建失败', false)
  } finally {
    busy.value = false
  }
}

async function selectJob(jobId: number) {
  selectedJobId.value = jobId
  await loadDetail()
}

async function loadDetail() {
  if (selectedJobId.value === null) return
  try {
    const payload = await orchestrationApi.getOrchestration(selectedJobId.value)
    if (payload.empty || !payload.orchestration) {
      orchestration.value = null
      detailEmptyReason.value = payload.reason || '该作业暂无编排数据'
      return
    }
    orchestration.value = payload.orchestration
    detailEmptyReason.value = ''
  } catch (error) {
    orchestration.value = null
    detailEmptyReason.value = error instanceof Error ? error.message : '编排数据读取失败'
  }
}

async function startSelected() {
  if (selectedJobId.value === null) return
  busy.value = true
  try {
    const result = await orchestrationApi.start(selectedJobId.value)
    if (!result.ok) throw new Error(result.error || '拓扑部署失败')
    notify(result.idempotent ? '当前拓扑已存在，未生成第二个拓扑' : '拓扑已部署，作业进入运行中')
    await Promise.all([reloadJobs(), loadDetail()])
  } catch (error) {
    notify(error instanceof Error ? error.message : '作业启动失败', false)
  } finally {
    busy.value = false
  }
}

async function publishSelected() {
  if (selectedJobId.value === null) return
  busy.value = true
  try {
    const result = await orchestrationApi.publishManifest(selectedJobId.value)
    if (!result.ok) throw new Error(result.error || '清单发布失败')
    notify(result.idempotent ? '清单已发布过，本次幂等刷新' : '故障转移清单已发布')
    await loadDetail()
  } catch (error) {
    notify(error instanceof Error ? error.message : '清单发布失败', false)
  } finally {
    busy.value = false
  }
}

async function reportException(kind: 'failure' | 'retry' | 'interrupt') {
  if (selectedJobId.value === null) return
  busy.value = true
  try {
    const detailMap: Record<string, string> = {
      failure: '序号25源端连接失败',
      retry: '序号25重试调度失败',
      interrupt: '序号25作业中断',
    }
    const result = await orchestrationApi.recordException(
      selectedJobId.value,
      kind,
      detailMap[kind],
    )
    notify(result.idempotent ? '重复异常只记一次，已返回原记录' : '异常已写入作业编排数据')
    await loadDetail()
  } catch (error) {
    notify(error instanceof Error ? error.message : '异常记录失败', false)
  } finally {
    busy.value = false
  }
}

async function recover25() {
  if (selectedJobId.value === null) return
  busy.value = true
  try {
    const result = await orchestrationApi.recover25(selectedJobId.value)
    if (!result.ok) throw new Error('序号25恢复失败')
    notify(result.idempotent ? '序号25已恢复过，重复调用保持幂等' : '序号25并发异常已恢复（仅一次）')
    await loadDetail()
  } catch (error) {
    notify(error instanceof Error ? error.message : '恢复失败', false)
  } finally {
    busy.value = false
  }
}

async function dualFailure(strategy: 'keep_topology' | 'rebuild') {
  if (selectedJobId.value === null) return
  busy.value = true
  try {
    const result = await orchestrationApi.dualFailure(
      selectedJobId.value,
      strategy,
      backupForm.source.trim() || undefined,
      backupForm.window.trim() || undefined,
    )
    if (!result.ok) throw new Error(result.error || '双故障处理失败（事务已回滚）')
    notify(
      strategy === 'keep_topology'
        ? '已保留拓扑并切换到备用源，终态：topology_kept_source_switched'
        : `已停止旧作业并重建为 v${result.version}，终态：job_stopped_and_rebuilt`,
    )
    await reloadJobs()
    if (result.job_id) await selectJob(result.job_id)
  } catch (error) {
    notify(error instanceof Error ? error.message : '双故障处理失败', false)
  } finally {
    busy.value = false
  }
}

watch(
  () => route.params.jobId,
  (raw) => {
    if (typeof raw === 'string' && /^\d+$/.test(raw)) {
      void selectJob(Number(raw))
    }
  },
)

onMounted(reloadJobs)
</script>
