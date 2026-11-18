import { createRouter, createWebHistory } from 'vue-router'

import Overview from '@/views/Overview.vue'
import StreamJobOrchestrationView from '@/views/StreamJobOrchestrationView.vue'
import TaskTopologyFailoverView from '@/views/TaskTopologyFailoverView.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    // 专用编排链路页面：固定路径优先于模块兜底路由
    { path: '/stream_job', name: 'stream-job', component: StreamJobOrchestrationView },
    { path: '/stream_job/current/:jobId', name: 'stream-job-version', component: StreamJobOrchestrationView },
    { path: '/task_topology', name: 'task-topology', component: TaskTopologyFailoverView },
    { path: '/task_topology/current/:topologyId', name: 'task-topology-detail', component: TaskTopologyFailoverView },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
