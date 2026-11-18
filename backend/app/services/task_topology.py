"""任务拓扑领域服务：管理流式计算任务的DAG拓扑结构。

增强能力: 算子链校验、节点分发策略、故障转移评估、资源配额检查、关联stream_job

编排链路（任务拓扑 ↔ 作业编排数据 ↔ 故障转移状态）:
  - build_executable_topology：读取 stream_job 的完整作业编排数据，
    生成可执行拓扑（当前路径）和备用路径，并把拓扑结果写回作业运行状态
  - build_failover_plan：基于备用源端/检查点/恢复方案生成故障转移清单条目
  - write_failover_result：把故障转移结果写回拓扑运行状态（序号25边界）
  - get_topology_detail：详情页说明异常原因，拒绝返回半成品
  - 重复部署同一作业不会生成第二个当前拓扑（由 stream_job 保证幂等）
"""
from __future__ import annotations

import copy
import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

# 序号 25：作业编排数据异常边界，拓扑详情页据此说明异常原因
BOUNDARY_NO = 25


class TaskTopologyService(BaseModuleService):
    MODULE = "task_topology"
    ENTITY = "拓扑配置"
    LIST_FIELDS = ["拓扑名称", "算子链", "并行度", "数据分发", "故障转移", "资源配额", "运行状态"]
    REQUIRED_FIELDS = ["拓扑名称", "算子链", "并行度"]
    STATUS_ORDER = ["草稿", "已部署", "运行中", "已停止"]
    ACTION_RULES = {"部署拓扑": "草稿", "停止拓扑": "运行中"}
    NEGATIVE_ACTIONS = []

    def validate_topology_dag(self, topology_name: str) -> dict[str, Any]:
        """校验拓扑DAG的合法性：算子链节点数、并行度范围、资源配额。"""
        topo = None
        for r in store.rows(self.MODULE):
            if r.get("拓扑名称") == topology_name:
                topo = r
                break
        if topo is None:
            return {"ok": False, "error": f"拓扑「{topology_name}」不存在"}

        checks = []
        chain = [
            c.strip()
            for c in topo.get("算子链", "").split("→")
            if c.strip()
        ]
        checks.append({
            "check": "chain_nodes",
            "ok": 2 <= len(chain) <= 10,
            "detail": f"算子链节点数: {len(chain)}",
        })

        try:
            parallelism = int(topo.get("并行度", "4"))
            checks.append({
                "check": "parallelism",
                "ok": 1 <= parallelism <= 64,
                "detail": f"并行度: {parallelism}",
            })
        except (ValueError, TypeError):
            checks.append({"check": "parallelism", "ok": False, "detail": "并行度格式错误"})

        # 资源配额校验
        quota = topo.get("资源配额", "")
        if quota:
            checks.append({
                "check": "resource_quota",
                "ok": any(unit in quota for unit in ("cpu", "mem", "gpu", "core")),
                "detail": f"资源配额: {quota}",
            })

        return {
            "ok": all(c["ok"] for c in checks),
            "topology": topology_name,
            "chain": chain,
            "checks": checks,
        }

    def deploy_with_topology_check(
        self, topology_name: str,
    ) -> dict[str, Any]:
        """部署拓扑前检查算子链中每个节点对应的模块是否可用。"""
        topo = None
        for r in store.rows(self.MODULE):
            if r.get("拓扑名称") == topology_name:
                topo = r
                break
        if topo is None:
            return {"ok": False, "error": f"拓扑「{topology_name}」不存在"}

        chain = [
            c.strip()
            for c in topo.get("算子链", "").split("→")
            if c.strip()
        ]
        node_status = []
        for node in chain:
            found = False
            for mod in ["stream_source", "calc_operator", "sliding_index", "window_config"]:
                for r in store.rows(mod):
                    if node in str(r.get(list(r.keys())[:3])):
                        found = True
                        node_status.append({
                            "node": node,
                            "module": mod,
                            "found": True,
                            "status": r.get("status", ""),
                        })
                        break
                if found:
                    break
            if not found:
                node_status.append({"node": node, "found": False})

        all_found = all(ns["found"] for ns in node_status)
        if all_found:
            topo["status"] = "运行中"
            topo["pending"] = True

        return {
            "ok": all_found,
            "topology": topology_name,
            "nodes": node_status,
        }

    def evaluate_failover(self, topology_name: str) -> dict[str, Any]:
        """评估故障转移能力：检查拓扑中是否有备用调度路径。"""
        topo = None
        for r in store.rows(self.MODULE):
            if r.get("拓扑名称") == topology_name:
                topo = r
                break
        if topo is None:
            return {"ok": False, "error": f"拓扑「{topology_name}」不存在"}

        failover_strategy = topo.get("故障转移", "")
        restore_plans = [
            rp for rp in store.rows("restore_plan")
            if topology_name in str(rp.get("任务范围", ""))
        ]
        checkpoints = [
            cp for cp in store.rows("checkpoint_mgr")
            if cp.get("checkpoint_id") and cp.get("status") == "已启用"
        ]

        can_failover = bool(
            failover_strategy or len(restore_plans) > 0 or len(checkpoints) > 0
        )
        return {
            "ok": True,
            "topology": topology_name,
            "can_failover": can_failover,
            "strategy": failover_strategy,
            "restore_plans": len(restore_plans),
            "available_checkpoints": len(checkpoints),
        }

    # ------------------------------------------------------------------
    # 作业编排数据 → 可执行拓扑 + 备用路径
    # ------------------------------------------------------------------

    SOURCE_OK_STATUSES = ("草稿", "已接入")
    WINDOW_OK_STATUSES = ("草稿", "已生效")

    def build_executable_topology(self, job_id: int) -> dict[str, Any]:
        """读取作业编排数据，生成可执行拓扑（当前路径）和备用路径。

        前置条件：作业存在且编排数据完整（orchestration_ready），
        否则拒绝生成，避免产出半成品拓扑。
        拓扑生成后把拓扑 id 写回作业运行状态的动作由 stream_job 完成，
        本方法只负责生成拓扑行与备用路径，保证职责单一。
        """
        job = store.find("stream_job", job_id)
        if job is None:
            return {"ok": False, "error": f"作业 {job_id} 不存在，无法生成拓扑"}
        if not job.get("orchestration_ready"):
            return {
                "ok": False,
                "error": "作业编排数据不完整，禁止生成可执行拓扑",
                "half_finished": True,
            }

        source_name = job.get("源端配置", "")
        window_name = job.get("窗口策略", "")
        source_row = next(
            (r for r in store.rows("stream_source") if r.get("源端名称") == source_name),
            None,
        )
        window_row = next(
            (r for r in store.rows("window_config") if r.get("策略名称") == window_name),
            None,
        )
        if source_row is None or window_row is None:
            return {"ok": False, "error": "作业引用的源端或窗口策略缺失，拓扑边界校验失败"}

        # 当前路径（主链路）：源端 → 窗口 → 计算算子 → 拓扑
        chain = [source_name, window_name]
        for op in store.rows("calc_operator"):
            if op.get("status") in ("已部署", "运行中"):
                chain.append(op.get("算子名称", ""))
                break
        chain.append(f"topo_job_{job_id}")

        # 备用路径：可用的备用源端 + 备用窗口策略
        backup_sources = [
            r.get("源端名称", "")
            for r in store.rows("stream_source")
            if r.get("源端名称") != source_name
            and r.get("status") in self.SOURCE_OK_STATUSES
        ]
        backup_windows = [
            r.get("策略名称", "")
            for r in store.rows("window_config")
            if r.get("策略名称") != window_name
            and r.get("status") in self.WINDOW_OK_STATUSES
        ]
        backup_paths = [
            {
                "path_id": f"bp_src_{name}",
                "type": "backup_source",
                "switch_to": name,
                "status": "standby",
            }
            for name in backup_sources
        ] + [
            {
                "path_id": f"bp_win_{name}",
                "type": "backup_window",
                "switch_to": name,
                "status": "standby",
            }
            for name in backup_windows
        ]

        try:
            parallelism = int(job.get("并行度", "4"))
        except (ValueError, TypeError):
            parallelism = 4

        topo_id = max(
            (int(r.get("id", 0)) for r in store.rows(self.MODULE)), default=0
        ) + 1
        topo_name = f"topo_job_{job_id}_v{job.get('version', 1)}"
        topology = {
            "id": topo_id,
            "拓扑名称": topo_name,
            "算子链": "→".join(chain),
            "并行度": str(parallelism),
            "数据分发": "rebalance",
            "故障转移": "backup_source_first_then_checkpoint",
            "资源配额": f"{parallelism} cpu/core,{parallelism * 2} mem/gb",
            "运行状态": "运行中",
            "status": "运行中",
            "pending": False,
            "abnormal": False,
            "is_current": True,
            "job_id": job_id,
            "job_version": job.get("version", 1),
            "current_path": copy.deepcopy(chain),
            "backup_paths": copy.deepcopy(backup_paths),
            "_failover_results": [],
            "_created_at": time.time(),
        }
        store.rows(self.MODULE).append(topology)
        return {
            "ok": True,
            "topology_id": topo_id,
            "topology_name": topo_name,
            "job_id": job_id,
            "current_path": chain,
            "backup_paths": backup_paths,
        }

    def build_failover_plan(
        self, job_id: int, topology_id: int | None = None,
    ) -> dict[str, Any]:
        """基于备用路径、检查点和恢复方案生成故障转移清单条目。"""
        job = store.find("stream_job", job_id)
        if job is None:
            return {"ok": False, "entries": [], "backup_paths": [], "error": f"作业 {job_id} 不存在"}

        topology = None
        if topology_id is not None:
            topology = store.find(self.MODULE, topology_id)
        backup_paths = copy.deepcopy((topology or {}).get("backup_paths", []))

        entries: list[dict[str, Any]] = []
        order = 1
        # 优先级1：备用源端切换
        for path in backup_paths:
            if path.get("type") == "backup_source":
                entries.append({
                    "order": order,
                    "action": "switch_source",
                    "target": path.get("switch_to", ""),
                    "mode": "hot_standby",
                    "status": "ready",
                })
                order += 1
        # 优先级2：启用检查点恢复
        for cp in store.rows("checkpoint_mgr"):
            if cp.get("status") == "已启用":
                entries.append({
                    "order": order,
                    "action": "restore_checkpoint",
                    "target": str(cp.get("id", cp.get("配置名称", ""))),
                    "mode": "from_checkpoint",
                    "status": "ready",
                })
                order += 1
        # 优先级3：关联的故障恢复方案
        topo_name = (topology or {}).get("拓扑名称", "")
        for rp in store.rows("restore_plan"):
            if rp.get("status") in ("已演练", "已生效") and (
                topo_name in str(rp.get("任务范围", ""))
                or str(job_id) in str(rp.get("任务范围", ""))
            ):
                entries.append({
                    "order": order,
                    "action": "apply_restore_plan",
                    "target": rp.get("方案名称", ""),
                    "mode": rp.get("恢复模式", "restart"),
                    "status": "ready",
                })
                order += 1
        # 兜底：重启作业
        entries.append({
            "order": order,
            "action": "restart_job",
            "target": job.get("作业名称", ""),
            "mode": "restart",
            "status": "ready",
        })
        return {"ok": True, "entries": entries, "backup_paths": backup_paths}

    def write_failover_result(
        self, topology_id: int, result: dict[str, Any],
    ) -> dict[str, Any]:
        """把故障转移结果写回拓扑运行状态（序号25边界结果落库）。"""
        topo = store.find(self.MODULE, topology_id)
        if topo is None:
            return {"ok": False, "error": f"拓扑 {topology_id} 不存在"}
        record = {
            "boundary_no": result.get("boundary_no", BOUNDARY_NO),
            "strategy": result.get("strategy", ""),
            "detail": copy.deepcopy(result),
            "recorded_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        topo.setdefault("_failover_results", []).append(record)
        topo["运行状态"] = f"故障转移:{result.get('strategy', '')}"
        topo["_updated_at"] = time.time()
        return {"ok": True, "topology_id": topology_id, "record": record}

    def get_topology_detail(self, topology_id: int) -> dict[str, Any]:
        """拓扑详情：当前路径、备用路径、故障转移清单与序号25异常原因。

        拓扑不存在时返回明确空态；数据不完整时不返回半成品。
        """
        topo = store.find(self.MODULE, topology_id)
        if topo is None:
            return {
                "ok": False,
                "empty": True,
                "reason": f"拓扑 {topology_id} 暂无数据",
                "detail": None,
            }

        job_id = topo.get("job_id")
        exception_reasons: list[dict[str, Any]] = []
        manifest_status = "unpublished"
        if job_id is not None:
            job = store.find("stream_job", int(job_id))
            if job is not None:
                exception_reasons = [
                    {
                        "boundary_no": e.get("boundary_no"),
                        "error_type": e.get("error_type"),
                        "error_detail": e.get("error_detail"),
                        "recovered": e.get("recovered", False),
                    }
                    for e in job.get("_exceptions", [])
                ]
                manifest = job.get("_failover_manifest")
                if manifest:
                    manifest_status = manifest.get("status", "published")

        return {
            "ok": True,
            "empty": False,
            "detail": {
                "topology_id": topology_id,
                "topology_name": topo.get("拓扑名称", ""),
                "status": topo.get("status", ""),
                "run_state": topo.get("运行状态", ""),
                "job_id": job_id,
                "job_version": topo.get("job_version"),
                "is_current": topo.get("is_current", False),
                "chain": [
                    c.strip()
                    for c in topo.get("算子链", "").split("→")
                    if c.strip()
                ],
                "current_path": copy.deepcopy(topo.get("current_path", [])),
                "backup_paths": copy.deepcopy(topo.get("backup_paths", [])),
                "failover_results": copy.deepcopy(topo.get("_failover_results", [])),
                "manifest_status": manifest_status,
                "exception_reasons": exception_reasons,
            },
        }

    def list_executable_topologies(self) -> dict[str, Any]:
        """列出当前可执行拓扑；没有任何当前拓扑时返回空态清单。"""
        items = [
            {
                "topology_id": t["id"],
                "topology_name": t.get("拓扑名称", ""),
                "job_id": t.get("job_id"),
                "status": t.get("status", ""),
                "is_current": t.get("is_current", False),
                "backup_path_count": len(t.get("backup_paths", [])),
            }
            for t in store.rows(self.MODULE)
            if t.get("is_current")
        ]
        return {
            "ok": True,
            "empty": len(items) == 0,
            "total": len(items),
            "items": items,
        }

    def verify_orchestration_boundary(
        self, job_id: int, boundary_no: int = BOUNDARY_NO,
    ) -> dict[str, Any]:
        """在任务拓扑数据流核对序号25边界：编排数据、异常与终态版本。"""
        job = store.find("stream_job", job_id)
        if job is None:
            return {
                "ok": False,
                "boundary_no": boundary_no,
                "empty": True,
                "reason": f"作业 {job_id} 暂无数据",
            }
        boundary_exceptions = [
            e for e in job.get("_exceptions", []) if e.get("boundary_no") == boundary_no
        ]
        topology = None
        topo_id = job.get("current_topology_id")
        if topo_id:
            topology = store.find(self.MODULE, int(topo_id))
        return {
            "ok": True,
            "boundary_no": boundary_no,
            "empty": False,
            "job_id": job_id,
            "version": job.get("version", 1),
            "orchestration_ready": bool(job.get("orchestration_ready")),
            "status": job.get("status", ""),
            "terminal_state": (topology or {}).get("运行状态", ""),
            "exceptions": copy.deepcopy(boundary_exceptions),
            "topology_id": topo_id,
            "manifest_published": bool(job.get("_failover_manifest")),
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        base["deployed"] = sum(1 for r in rows if r.get("status") == "已部署")
        base["running"] = sum(1 for r in rows if r.get("status") == "运行中")
        base["current_topologies"] = sum(1 for r in rows if r.get("is_current"))
        base["with_backup_paths"] = sum(1 for r in rows if r.get("backup_paths"))
        return base