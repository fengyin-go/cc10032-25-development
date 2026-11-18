<template>
  <section class="page" data-testid="task-topology-page">
    <header class="page-head">
      <div>
        <h2>任务拓扑与故障转移</h2>
        <p class="page-desc">
          读取作业编排数据生成可执行拓扑与备用路径，在序号25数据流核对边界，并把故障转移结果写回运行状态。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" data-testid="refresh-topologies" type="button" @click="reloadTopologies">
          刷新当前拓扑
        </button>
      </div>
    </header>

    <div class="stat-row">
      <article class="stat-card">
        <span class="stat-label">当前拓扑数</span>
        <strong class="stat-value">{{ topologies.length }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">含备用路径拓扑</span>
        <strong class="stat-value">{{ withBackupCount }}</strong>
      </article>
    </div>

    <table class="data-table" data-testid="topology-table">
      <thead>
        <tr>
          <th>拓扑ID</th><th>拓扑名称</th><th>作业ID</th><th>状态</th><th>备用路径数</th><th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="item in topologies" :key="item.topology_id" :data-testid="`topo-row-${item.topology_id}`">
          <td>{{ item.topology_id }}</td>
          <td>{{ item.topology_name }}</td>
          <td>{{ item.job_id ?? '—' }}</td>
          <td>{{ item.status }}</td>
          <td>{{ item.backup_path_count }}</td>
          <td class="row-actions">
            <button class="link" type="button" @click="selectTopology(item.topology_id)">核对路径与清单</button>
            <button
              v-if="item.job_id"
              class="link"
              type="button"
              @click="checkBoundary(item.job_id)"
            >
              序号25边界
            </button>
          </td>
        </tr>
        <tr v-if="!topologies.length">
          <td colspan="6" class="empty-state" data-testid="topology-empty-state">
            暂无当前可执行拓扑数据，作业编排数据完成并部署后在此呈现
          </td>
        </tr>
      </tbody>
    </table>

    <div v-if="selectedTopologyId !== null" class="timeline-wrap" style="margin-top: 16px" data-testid="topology-detail">
      <div class="page-head">
        <div>
          <h3>拓扑 #{{ selectedTopologyId }} 详情</h3>
          <p class="page-desc">核对当前路径、备用路径、故障转移清单与序号25异常原因。</p>
        </div>
      </div>

      <template v-if="detail">
        <dl class="dag-detail">
          <dt>拓扑名称</dt><dd>{{ detail.topology_name }}</dd>
          <dt>运行状态</dt><dd>{{ detail.run_state || detail.status }}</dd>
          <dt>关联作业</dt><dd>#{{ detail.job_id ?? '—' }}（v{{ detail.job_version ?? '—' }}）</dd>
          <dt>清单状态</dt><dd>{{ detail.manifest_status }}</dd>
        </dl>

        <h4>当前路径（可执行拓扑）</h4>
        <div class="timeline-wrap" data-testid="current-path">
          <div v-for="(node, index) in detail.current_path" :key="`${node}-${index}`" class="timeline-row">
            <span class="timeline-label">{{ index + 1 }}. {{ node }}</span>
            <span class="timeline-track">
              <span class="timeline-bar status-运行中" :style="{ left: `${index * 12}%`, width: '18%' }">
                {{ node }}
              </span>
            </span>
          </div>
          <p v-if="!detail.current_path.length" class="empty-state">当前路径暂无数据</p>
        </div>

        <h4>备用路径（故障转移候选）</h4>
        <table class="data-table" data-testid="backup-path-table">
          <thead><tr><th>路径ID</th><th>类型</th><th>切换目标</th><th>状态</th></tr></thead>
          <tbody>
            <tr v-for="path in detail.backup_paths" :key="path.path_id">
              <td>{{ path.path_id }}</td>
              <td>{{ path.type === 'backup_source' ? '备用源端' : '备用窗口' }}</td>
              <td>{{ path.switch_to }}</td>
              <td>{{ path.status }}</td>
            </tr>
          </tbody>
        </table>
        <p v-if="!detail.backup_paths.length" class="empty-state" data-testid="backup-empty">
          备用路径暂无数据
        </p>

        <h4>故障转移结果（写回运行状态）</h4>
        <table v-if="detail.failover_results.length" class="data-table" data-testid="failover-result-table">
          <thead><tr><th>边界</th><th>策略</th><th>记录时间</th></tr></thead>
          <tbody>
            <tr v-for="(result, index) in detail.failover_results" :key="index">
              <td>{{ result.boundary_no }}</td>
              <td>{{ result.strategy }}</td>
              <td>{{ result.recorded_at }}</td>
            </tr>
          </tbody>
        </table>
        <p v-else class="empty-state" data-testid="failover-result-empty">
          故障转移结果暂无数据，尚未触发切换
        </p>

        <h4>序号25异常原因（详情页说明）</h4>
        <table class="data-table" data-testid="exception-reason-table">
          <thead><tr><th>边界</th><th>类型</th><th>异常原因</th><th>已恢复</th></tr></thead>
          <tbody>
            <tr v-for="reason in detail.exception_reasons" :key="reason.exception_id">
              <td>{{ reason.boundary_no }}</td>
              <td>{{ reason.error_type }}</td>
              <td>{{ reason.error_detail }}</td>
              <td>{{ reason.recovered ? '是' : '否' }}</td>
            </tr>
          </tbody>
        </table>
        <p v-if="!detail.exception_reasons.length" class="empty-state" data-testid="exception-reason-empty">
          序号25暂无异常数据
        </p>

        <p class="page-foot">
          <RouterLink v-if="detail.job_id" class="link" :to="`/stream_job/current/${detail.job_id}`">
            ← 返回流式作业页查看源端和窗口引用
          </RouterLink>
        </p>
      </template>

      <p v-else class="empty-state" data-testid="detail-empty">
        {{ detailEmptyReason || '拓扑详情加载中…' }}
      </p>
    </div>

    <div v-if="boundary" class="timeline-wrap" style="margin-top: 16px" data-testid="boundary-panel">
      <h3>序号{{ boundary.boundary_no }} 数据流边界核对</h3>
      <dl class="dag-detail">
        <dt>作业版本</dt><dd>v{{ boundary.version }}</dd>
        <dt>编排完整</dt><dd>{{ boundary.orchestration_ready ? '是' : '否' }}</dd>
        <dt>作业状态</dt><dd>{{ boundary.status }}</dd>
        <dt>拓扑终态</dt><dd>{{ boundary.terminal_state || '—' }}</dd>
        <dt>清单已发布</dt><dd>{{ boundary.manifest_published ? '是' : '否' }}</dd>
        <dt>异常条数</dt><dd>{{ boundary.exceptions.length }}</dd>
      </dl>
      <p v-if="boundary.empty" class="empty-state">{{ boundary.reason }}</p>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'
import { useRoute } from 'vue-router'

import { orchestrationApi } from '@/api/orchestration'
import type { TopologyDetail, TopologyListItem } from '@/api/orchestration'

type Boundary = Awaited<ReturnType<typeof orchestrationApi.boundary>>

const route = useRoute()
const topologies = ref<TopologyListItem[]>([])
const selectedTopologyId = ref<number | null>(null)
const detail = ref<TopologyDetail | null>(null)
const detailEmptyReason = ref('')
const boundary = ref<Boundary | null>(null)

const withBackupCount = computed(() =>
  topologies.value.filter((item) => item.backup_path_count > 0).length,
)

async function reloadTopologies() {
  try {
    const payload = await orchestrationApi.listTopologies()
    topologies.value = payload.items
    if (selectedTopologyId.value === null && payload.items.length) {
      await selectTopology(payload.items[0].topology_id)
    }
  } catch (error) {
    detail.value = null
    detailEmptyReason.value = error instanceof Error ? error.message : '拓扑清单读取失败'
  }
}

async function selectTopology(topologyId: number) {
  selectedTopologyId.value = topologyId
  boundary.value = null
  try {
    const payload = await orchestrationApi.topologyDetail(topologyId)
    if (payload.empty || !payload.detail) {
      detail.value = null
      detailEmptyReason.value = payload.reason || '拓扑暂无数据'
      return
    }
    detail.value = payload.detail
    detailEmptyReason.value = ''
  } catch (error) {
    detail.value = null
    detailEmptyReason.value = error instanceof Error ? error.message : '拓扑详情读取失败'
  }
}

async function checkBoundary(jobId: number) {
  try {
    boundary.value = await orchestrationApi.boundary(jobId, 25)
  } catch (error) {
    boundary.value = null
    detailEmptyReason.value = error instanceof Error ? error.message : '边界核对失败'
  }
}

watch(
  () => route.params.topologyId,
  (raw) => {
    if (typeof raw === 'string' && /^\d+$/.test(raw)) {
      void selectTopology(Number(raw))
    }
  },
  { immediate: true },
)

onMounted(reloadTopologies)
</script>
