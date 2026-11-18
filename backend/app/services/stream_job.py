"""流式作业领域服务：消费 stream_source + window_config 创建计算作业。

数据流: stream_source（源端配置）→ window_config（窗口策略）→ stream_job（流式作业）
       → task_topology（任务拓扑）→ failover_manifest（故障转移清单）

核心链路（作业编排数据 ↔ 故障转移状态）:
  1. 读取作业配置（源端引用 + 窗口引用 + 并行度等运行参数）
  2. 校验拓扑依赖（源端与窗口引用必须存在且处于可用状态）
  3. 形成完整作业编排数据（事务化构建，失败整体回滚，不产生半成品）
  4. 编排数据完整后才允许部署拓扑（重复启动同一作业不生成第二个当前拓扑）
  5. 源端与窗口引用全部有效后才发布故障转移清单
  6. 源端失效 + 窗口停用同时发生时支持两种终态：
       - keep_topology：保留拓扑切换备用源
       - rebuild：停止当前作业后重建，列表只保留一个可执行作业版本
  7. 序号 25 并发异常边界：失败/重试/中断只记录一次，恢复只执行一次且保持幂等，
     事务失败回滚到原结果，不同取舍形成序号 25 下的不同终态版本。
"""
from __future__ import annotations

import copy
import time
from typing import Any, Callable

from app.services.base import BaseModuleService
from app.store import store

# 序号 25：并发异常边界编号，贯穿作业编排数据的失败/重试/中断/恢复全过程
BOUNDARY_NO = 25
# 编排相关的内存表（事务快照与回滚需要覆盖）
ORCH_TABLES = (
    "stream_job",
    "task_topology",
    "restore_plan",
    "stream_source",
    "window_config",
)


class StreamJobService(BaseModuleService):
    MODULE = "stream_job"
    ENTITY = "流式作业"
    LIST_FIELDS = ["作业名称", "源端配置", "窗口策略", "计算拓扑", "并行度", "启动时间", "运行水位"]
    REQUIRED_FIELDS = ["作业名称", "源端配置", "窗口策略"]
    STATUS_ORDER = ["草稿", "待启动", "运行中", "已暂停", "已停止"]
    ACTION_RULES = {"启动作业": "待启动", "暂停作业": "运行中", "停止作业": "已暂停"}
    NEGATIVE_ACTIONS = ["停止作业"]

    # 源端可用状态（异常断连/已下线均视为失效）
    SOURCE_OK_STATUSES = ("草稿", "已接入")
    # 窗口策略可用状态（已停用视为失效）
    WINDOW_OK_STATUSES = ("草稿", "已生效")

    # ------------------------------------------------------------------
    # 基础查找
    # ------------------------------------------------------------------

    def _find_source(self, source_name: str) -> dict[str, Any] | None:
        return next(
            (r for r in store.rows("stream_source") if r.get("源端名称") == source_name),
            None,
        )

    def _find_window(self, window_name: str) -> dict[str, Any] | None:
        return next(
            (r for r in store.rows("window_config") if r.get("策略名称") == window_name),
            None,
        )

    def _find_job(self, job_id: int) -> dict[str, Any] | None:
        return store.find(self.MODULE, job_id)

    @staticmethod
    def _snapshot_tables() -> dict[str, list[dict[str, Any]]]:
        """对编排链路涉及的表做深拷贝快照，用于事务回滚。"""
        return {name: copy.deepcopy(store.rows(name)) for name in ORCH_TABLES}

    @staticmethod
    def _restore_tables(snapshot: dict[str, list[dict[str, Any]]]) -> None:
        for name, rows in snapshot.items():
            store.rows(name)[:] = copy.deepcopy(rows)

    def _transactional(self, operation: Callable[[], dict[str, Any]]) -> dict[str, Any]:
        """在内存表上执行事务化操作：任何异常都回滚到调用前的原结果。

        回滚后重新读取目标作业，保证调用方拿到的仍是事务前的稳定状态，
        不会出现半成品编排数据。
        """
        snapshot = self._snapshot_tables()
        try:
            return operation()
        except Exception as exc:  # noqa: BLE001 - 事务边界需要兜底所有异常
            self._restore_tables(snapshot)
            return {
                "ok": False,
                "rolled_back": True,
                "error": f"事务失败已回滚: {exc}",
            }

    # ------------------------------------------------------------------
    # 1. 读取作业配置
    # ------------------------------------------------------------------

    def read_job_config(self, job_id: int) -> dict[str, Any]:
        """读取作业配置：作业行 + 源端配置快照 + 窗口策略快照 + 运行参数。

        配置缺失时返回 ok=False 的完整说明，不返回半成品。
        """
        job = self._find_job(job_id)
        if job is None:
            return {"ok": False, "error": f"作业 {job_id} 不存在", "job_id": job_id}

        source_name = job.get("源端配置", "")
        window_name = job.get("窗口策略", "")
        source_row = self._find_source(source_name)
        window_row = self._find_window(window_name)

        missing: list[str] = []
        if source_row is None:
            missing.append(f"源端配置「{source_name}」不存在")
        if window_row is None:
            missing.append(f"窗口策略「{window_name}」不存在")
        if missing:
            return {
                "ok": False,
                "job_id": job_id,
                "error": "; ".join(missing),
                "config": None,
            }

        config = {
            "job_id": job_id,
            "job_name": job.get("作业名称", ""),
            "parallelism": job.get("并行度", "4"),
            "source": {
                "name": source_name,
                "queue": source_row.get("消息队列", ""),
                "topic": source_row.get("Topic名称", ""),
                "format": source_row.get("数据格式", ""),
                "consumer_group": source_row.get("消费者组", ""),
                "status": source_row.get("status", ""),
            },
            "window": {
                "name": window_name,
                "type": window_row.get("窗口类型", ""),
                "size": window_row.get("窗口大小", ""),
                "slide": window_row.get("滑动步长", ""),
                "watermark_delay": window_row.get("水位延迟", ""),
                "status": window_row.get("status", ""),
            },
        }
        return {"ok": True, "job_id": job_id, "config": config}

    # ------------------------------------------------------------------
    # 2. 校验拓扑依赖
    # ------------------------------------------------------------------

    def validate_topology_dependencies(
        self,
        source_name: str | None = None,
        window_name: str | None = None,
        job_id: int | None = None,
    ) -> dict[str, Any]:
        """校验拓扑依赖：源端引用与窗口引用必须存在且处于可用状态。

        可直接传引用名称，也可传 job_id 从作业编排数据中读取引用。
        """
        if job_id is not None:
            job = self._find_job(job_id)
            if job is None:
                return {"ok": False, "error": f"作业 {job_id} 不存在", "refs": []}
            source_name = job.get("源端配置", "")
            window_name = job.get("窗口策略", "")

        refs: list[dict[str, Any]] = []

        source_row = self._find_source(source_name or "")
        source_valid = bool(
            source_row and source_row.get("status") in self.SOURCE_OK_STATUSES
        )
        refs.append({
            "ref": "stream_source",
            "value": source_name,
            "exists": source_row is not None,
            "status": (source_row or {}).get("status", ""),
            "valid": source_valid,
            "reason": "" if source_valid else self._invalid_reason(
                source_row, "源端", self.SOURCE_OK_STATUSES,
            ),
        })

        window_row = self._find_window(window_name or "")
        window_valid = bool(
            window_row and window_row.get("status") in self.WINDOW_OK_STATUSES
        )
        refs.append({
            "ref": "window_config",
            "value": window_name,
            "exists": window_row is not None,
            "status": (window_row or {}).get("status", ""),
            "valid": window_valid,
            "reason": "" if window_valid else self._invalid_reason(
                window_row, "窗口策略", self.WINDOW_OK_STATUSES,
            ),
        })

        all_valid = all(r["valid"] for r in refs)
        return {
            "ok": all_valid,
            "all_valid": all_valid,
            "job_id": job_id,
            "refs": refs,
        }

    @staticmethod
    def _invalid_reason(
        row: dict[str, Any] | None, label: str, ok_statuses: tuple[str, ...],
    ) -> str:
        if row is None:
            return f"{label}引用不存在"
        return f"{label}状态「{row.get('status', '')}」不可用（可用状态: {'/'.join(ok_statuses)}）"

    # ------------------------------------------------------------------
    # 3. 形成完整作业编排数据（事务化）
    # ------------------------------------------------------------------

    def build_orchestration(
        self,
        source_name: str,
        window_name: str,
        job_name: str | None = None,
        parallelism: int = 4,
    ) -> dict[str, Any]:
        """构建完整作业编排数据。

        流程：读取作业配置 → 校验拓扑依赖 → 写入编排数据（含调用链与依赖清单）。
        整个过程事务化：任一步骤失败整体回滚，已存在的同引用编排直接幂等返回，
        绝不返回半成品结果。
        """
        def _do() -> dict[str, Any]:
            # 幂等：相同 源端+窗口 的完整编排已存在则直接返回，不重复构建
            existing = next(
                (
                    j for j in store.rows(self.MODULE)
                    if j.get("源端配置") == source_name
                    and j.get("窗口策略") == window_name
                    and j.get("orchestration_ready")
                ),
                None,
            )
            if existing is not None:
                return {
                    "ok": True,
                    "idempotent": True,
                    "job_id": existing["id"],
                    "orchestration": self.get_orchestration(existing["id"]),
                }

            # 3.1 读取引用配置（不存在直接失败，不写任何数据）
            source_row = self._find_source(source_name)
            if source_row is None:
                raise ValueError(f"源端配置「{source_name}」不存在，无法形成编排数据")
            window_row = self._find_window(window_name)
            if window_row is None:
                raise ValueError(f"窗口策略「{window_name}」不存在，无法形成编排数据")

            # 3.2 校验拓扑依赖
            dependency = self.validate_topology_dependencies(source_name, window_name)
            if not dependency["all_valid"]:
                reasons = "; ".join(
                    r["reason"] for r in dependency["refs"] if not r["valid"]
                )
                raise ValueError(f"拓扑依赖校验失败: {reasons}")

            # 3.3 写入完整编排数据
            name = job_name or f"{source_name}_{window_name}_{int(time.time())}"
            job_id = max(
                (int(r.get("id", 0)) for r in store.rows(self.MODULE)), default=0
            ) + 1
            now = time.time()
            job = {
                "id": job_id,
                "作业名称": name,
                "源端配置": source_name,
                "窗口策略": window_name,
                "计算拓扑": "",
                "并行度": str(parallelism),
                "启动时间": "",
                "运行水位": "0",
                "status": "待启动",
                "pending": True,
                "abnormal": False,
                # —— 作业编排数据 ——
                "orchestration_ready": True,
                "version": 1,
                "is_current": True,
                "current_topology_id": None,
                "_source_topic": source_row.get("Topic名称", ""),
                "_window_type": window_row.get("窗口类型", ""),
                "_window_size": window_row.get("窗口大小", ""),
                "_dependency_refs": copy.deepcopy(dependency["refs"]),
                "_call_chain": [
                    {
                        "step": "stream_source",
                        "ref": source_name,
                        "status": source_row.get("status", ""),
                        "valid": True,
                    },
                    {
                        "step": "window_config",
                        "ref": window_name,
                        "status": window_row.get("status", ""),
                        "valid": True,
                    },
                    {"step": "stream_job", "ref": name, "status": "待启动", "valid": True},
                    {"step": "task_topology", "ref": None, "status": "未部署", "valid": False},
                ],
                "_failover_manifest": None,
                "_exceptions": [],
                "_created_at": now,
                "_updated_at": now,
            }
            store.rows(self.MODULE).append(job)
            return {
                "ok": True,
                "idempotent": False,
                "job_id": job_id,
                "orchestration": self.get_orchestration(job_id),
            }

        return self._transactional(_do)

    def get_orchestration(self, job_id: int) -> dict[str, Any] | None:
        """按作业版本核对作业编排数据（含调用链、依赖引用、异常记录、清单状态）。"""
        job = self._find_job(job_id)
        if job is None or not job.get("orchestration_ready"):
            return None
        return {
            "job_id": job_id,
            "job_name": job.get("作业名称", ""),
            "version": job.get("version", 1),
            "is_current": job.get("is_current", False),
            "status": job.get("status", ""),
            "source": job.get("源端配置", ""),
            "window": job.get("窗口策略", ""),
            "parallelism": job.get("并行度", ""),
            "orchestration_ready": job.get("orchestration_ready", False),
            "current_topology_id": job.get("current_topology_id"),
            "dependency_refs": copy.deepcopy(job.get("_dependency_refs", [])),
            "call_chain": copy.deepcopy(job.get("_call_chain", [])),
            "failover_manifest": copy.deepcopy(job.get("_failover_manifest")),
            "exceptions": copy.deepcopy(job.get("_exceptions", [])),
        }

    # ------------------------------------------------------------------
    # 4. 部署拓扑：编排完整后才部署，重复启动不生成第二个当前拓扑
    # ------------------------------------------------------------------

    def start_job(self, job_id: int) -> dict[str, Any]:
        """启动作业并部署当前拓扑。

        前置条件：作业编排数据完整；重复启动同一作业幂等返回原当前拓扑。
        部署动作委托任务拓扑模块生成可执行拓扑和备用路径，并把结果写回运行状态。
        """
        def _do() -> dict[str, Any]:
            job = self._find_job(job_id)
            if job is None:
                raise ValueError(f"作业 {job_id} 不存在")
            if not job.get("orchestration_ready"):
                raise ValueError("作业编排数据不完整，禁止部署拓扑")

            # 幂等：已有当前拓扑则不生成第二个
            if job.get("current_topology_id"):
                topo = store.find("task_topology", int(job["current_topology_id"]))
                if topo is not None:
                    return {
                        "ok": True,
                        "idempotent": True,
                        "job_id": job_id,
                        "topology_id": topo["id"],
                        "topology_name": topo.get("拓扑名称", ""),
                        "message": "当前拓扑已存在，重复启动未生成第二个拓扑",
                    }

            # 部署门禁：编排数据完整之外，源端与窗口引用必须仍然有效
            dependency = self.validate_topology_dependencies(job_id=job_id)
            if not dependency["all_valid"]:
                reasons = "; ".join(
                    r["reason"] for r in dependency["refs"] if not r["valid"]
                )
                raise ValueError(f"拓扑依赖失效，禁止部署拓扑: {reasons}")

            # 延迟导入避免模块间循环依赖
            from app.services.task_topology import TaskTopologyService
            topo_service = TaskTopologyService()
            built = topo_service.build_executable_topology(job_id)
            if not built.get("ok"):
                raise ValueError(built.get("error", "拓扑生成失败"))

            topo_id = built["topology_id"]
            job["current_topology_id"] = topo_id
            job["计算拓扑"] = built["topology_name"]
            job["status"] = "运行中"
            job["启动时间"] = time.strftime("%Y-%m-%d %H:%M:%S")
            job["_updated_at"] = time.time()
            self._mark_chain_step(job, "task_topology", built["topology_name"], "运行中", True)
            return {
                "ok": True,
                "idempotent": False,
                "job_id": job_id,
                "topology_id": topo_id,
                "topology_name": built["topology_name"],
                "backup_paths": built.get("backup_paths", []),
            }

        return self._transactional(_do)

    # ------------------------------------------------------------------
    # 5. 故障转移清单：引用全部有效后才可发布
    # ------------------------------------------------------------------

    def publish_failover_manifest(self, job_id: int) -> dict[str, Any]:
        """发布故障转移清单。

        前置条件：作业编排数据完整，且源端与窗口引用全部有效。
        清单内容由任务拓扑模块的备用路径 + 故障转移策略汇总生成；
        重复发布会刷新同一清单（幂等），不会产生第二份清单。
        """
        def _do() -> dict[str, Any]:
            job = self._find_job(job_id)
            if job is None:
                raise ValueError(f"作业 {job_id} 不存在")
            if not job.get("orchestration_ready"):
                raise ValueError("作业编排数据不完整，故障转移清单不可发布")

            dependency = self.validate_topology_dependencies(job_id=job_id)
            if not dependency["all_valid"]:
                reasons = "; ".join(
                    r["reason"] for r in dependency["refs"] if not r["valid"]
                )
                raise ValueError(f"源端与窗口引用未全部有效，清单不可发布: {reasons}")

            from app.services.task_topology import TaskTopologyService
            topo_service = TaskTopologyService()
            topo_id = job.get("current_topology_id")
            plan = topo_service.build_failover_plan(
                job_id,
                topology_id=int(topo_id) if topo_id else None,
            )

            existing = job.get("_failover_manifest")
            manifest_id = existing["manifest_id"] if existing else (
                f"fm_{job_id}_{int(time.time())}"
            )
            manifest = {
                "manifest_id": manifest_id,
                "job_id": job_id,
                "status": "published",
                "published_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                "entries": plan["entries"],
                "backup_paths": plan["backup_paths"],
                "refs": copy.deepcopy(dependency["refs"]),
            }
            job["_failover_manifest"] = manifest
            job["_updated_at"] = time.time()
            return {
                "ok": True,
                "idempotent": bool(existing),
                "manifest": manifest,
            }

        return self._transactional(_do)

    def get_failover_manifest(self, job_id: int) -> dict[str, Any]:
        """查询故障转移清单；未发布时返回明确空态（不返回半成品）。"""
        job = self._find_job(job_id)
        if job is None:
            return {"ok": False, "empty": True, "reason": f"作业 {job_id} 不存在", "manifest": None}
        manifest = job.get("_failover_manifest")
        if not manifest:
            return {
                "ok": True,
                "empty": True,
                "reason": "故障转移清单暂无数据：引用未全部有效或尚未发布",
                "manifest": None,
            }
        return {"ok": True, "empty": False, "manifest": copy.deepcopy(manifest)}

    # ------------------------------------------------------------------
    # 6. 异常记录：失败/重试/中断写入作业编排数据，重复异常只记一次
    # ------------------------------------------------------------------

    def record_job_exception(
        self,
        job_id: int,
        error_type: str,
        error_detail: str,
        boundary_no: int = BOUNDARY_NO,
    ) -> dict[str, Any]:
        """把失败、重试、中断异常写入作业编排数据。

        同一作业 + 同一边界序号 + 同一异常类型只记录一次（幂等去重），
        重复上报返回既有记录，不产生第二条异常。
        """
        job = self._find_job(job_id)
        if job is None:
            return {"ok": False, "error": f"作业 {job_id} 不存在"}

        exceptions = job.setdefault("_exceptions", [])
        duplicate = next(
            (
                e for e in exceptions
                if e.get("boundary_no") == boundary_no and e.get("error_type") == error_type
            ),
            None,
        )
        if duplicate is not None:
            return {
                "ok": True,
                "idempotent": True,
                "exception": copy.deepcopy(duplicate),
                "message": "重复异常只记一次",
            }

        record = {
            "exception_id": f"ex_{job_id}_{boundary_no}_{len(exceptions) + 1}",
            "boundary_no": boundary_no,
            "error_type": error_type,  # failure / retry / interrupt
            "error_detail": error_detail,
            "recorded_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "recovered": False,
        }
        exceptions.append(record)
        job["abnormal"] = True
        job["_updated_at"] = time.time()
        return {"ok": True, "idempotent": False, "exception": copy.deepcopy(record)}

    def recover_concurrent_exception_25(self, job_id: int) -> dict[str, Any]:
        """序号 25 并发异常恢复：只恢复一次，并保持幂等。

        第一次调用把序号 25 下的异常标记为已恢复并写入恢复结果；
        后续重复调用直接返回原恢复结果，不再执行第二次恢复。
        """
        job = self._find_job(job_id)
        if job is None:
            return {"ok": False, "error": f"作业 {job_id} 不存在"}

        existing = job.get("_recovery_25")
        if existing is not None:
            return {
                "ok": True,
                "idempotent": True,
                "recovery": copy.deepcopy(existing),
                "message": "序号25并发异常已恢复，重复调用不重复恢复",
            }

        boundary_exceptions = [
            e for e in job.get("_exceptions", []) if e.get("boundary_no") == BOUNDARY_NO
        ]
        recovery = {
            "boundary_no": BOUNDARY_NO,
            "recovered_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "recovered_count": len(boundary_exceptions),
            "strategy": "single_recovery",
            "status": "recovered",
        }
        for exc in boundary_exceptions:
            exc["recovered"] = True
        job["_recovery_25"] = recovery
        job["abnormal"] = False
        job["_updated_at"] = time.time()
        return {"ok": True, "idempotent": False, "recovery": copy.deepcopy(recovery)}

    # ------------------------------------------------------------------
    # 7. 源端失效 + 窗口停用同时发生：两种终态取舍
    # ------------------------------------------------------------------

    def handle_dual_failure(
        self,
        job_id: int,
        strategy: str = "keep_topology",
        backup_source: str | None = None,
        backup_window: str | None = None,
    ) -> dict[str, Any]:
        """处理源端失效与窗口策略停用同时发生的双故障。

        strategy:
          - keep_topology：优先保留拓扑，继续切换备用源（同版本内切换）
          - rebuild：停止当前作业后重建（生成新版本，旧版本退出可执行列表）
        两种取舍最终都只呈现一个可执行作业版本、一个当前拓扑，
        构成作业编排数据在序号 25 下的不同终态版本。
        """
        if strategy not in ("keep_topology", "rebuild"):
            return {"ok": False, "error": f"未知故障取舍策略: {strategy}"}

        def _do() -> dict[str, Any]:
            job = self._find_job(job_id)
            if job is None:
                raise ValueError(f"作业 {job_id} 不存在")
            if not job.get("orchestration_ready"):
                raise ValueError("作业编排数据不完整，无法执行故障转移")

            # 记录双故障（去重，只记一次）
            self.record_job_exception(
                job_id, "dual_failure",
                "源端失效与窗口策略停用同时发生",
            )

            if strategy == "keep_topology":
                return self._dual_failure_keep_topology(job, backup_source, backup_window)
            return self._dual_failure_rebuild(job, backup_source, backup_window)

        return self._transactional(_do)

    def _dual_failure_keep_topology(
        self,
        job: dict[str, Any],
        backup_source: str | None,
        backup_window: str | None,
    ) -> dict[str, Any]:
        """保留拓扑切换备用源：拓扑不重建，作业引用切到备用源/备用窗口。"""
        target_source = backup_source or job.get("源端配置", "")
        target_window = backup_window or job.get("窗口策略", "")
        dependency = self.validate_topology_dependencies(target_source, target_window)
        if not dependency["all_valid"]:
            reasons = "; ".join(r["reason"] for r in dependency["refs"] if not r["valid"])
            raise ValueError(f"备用路径不可用，无法保留拓扑切换: {reasons}")

        old_source = job.get("源端配置", "")
        old_window = job.get("窗口策略", "")
        job["源端配置"] = target_source
        job["窗口策略"] = target_window
        job["_dependency_refs"] = copy.deepcopy(dependency["refs"])
        job["status"] = "运行中"
        job["abnormal"] = False
        job["_updated_at"] = time.time()

        # 拓扑保留：当前拓扑不变，仅在拓扑上记录备用路径切换结果
        topo_id = job.get("current_topology_id")
        if topo_id:
            from app.services.task_topology import TaskTopologyService
            TaskTopologyService().write_failover_result(
                int(topo_id),
                {
                    "boundary_no": BOUNDARY_NO,
                    "strategy": "keep_topology",
                    "from_source": old_source,
                    "to_source": target_source,
                    "from_window": old_window,
                    "to_window": target_window,
                    "topology_kept": True,
                },
            )

        self._mark_chain_step(job, "stream_source", target_source, "已接入", True)
        self._mark_chain_step(job, "window_config", target_window, "已生效", True)
        return {
            "ok": True,
            "strategy": "keep_topology",
            "terminal_state": "topology_kept_source_switched",
            "job_id": job["id"],
            "version": job.get("version", 1),
            "current_topology_id": job.get("current_topology_id"),
            "executable_jobs": self._executable_job_versions(),
        }

    def _dual_failure_rebuild(
        self,
        job: dict[str, Any],
        backup_source: str | None,
        backup_window: str | None,
    ) -> dict[str, Any]:
        """停止当前作业后重建：旧版本停止，新版本成为唯一可执行版本。"""
        target_source = backup_source or job.get("源端配置", "")
        target_window = backup_window or job.get("窗口策略", "")
        dependency = self.validate_topology_dependencies(target_source, target_window)
        if not dependency["all_valid"]:
            reasons = "; ".join(r["reason"] for r in dependency["refs"] if not r["valid"])
            raise ValueError(f"备用路径不可用，无法重建作业: {reasons}")

        old_job_id = job["id"]
        old_topo_id = job.get("current_topology_id")

        # 1. 停止当前作业与当前拓扑
        job["status"] = "已停止"
        job["is_current"] = False
        job["pending"] = False
        if old_topo_id:
            old_topo = store.find("task_topology", int(old_topo_id))
            if old_topo is not None:
                old_topo["status"] = "已停止"
                old_topo["is_current"] = False

        # 2. 用备用引用重建新版本作业（新版本号，同一作业族）
        new_version = int(job.get("version", 1)) + 1
        new_job_id = max(
            (int(r.get("id", 0)) for r in store.rows(self.MODULE)), default=0
        ) + 1
        now = time.time()
        rebuilt = {
            "id": new_job_id,
            "作业名称": job.get("作业名称", ""),
            "源端配置": target_source,
            "窗口策略": target_window,
            "计算拓扑": "",
            "并行度": job.get("并行度", "4"),
            "启动时间": "",
            "运行水位": "0",
            "status": "待启动",
            "pending": True,
            "abnormal": False,
            "orchestration_ready": True,
            "version": new_version,
            "is_current": True,
            "rebuilt_from_job_id": old_job_id,
            "current_topology_id": None,
            "_dependency_refs": copy.deepcopy(dependency["refs"]),
            "_call_chain": [
                {"step": "stream_source", "ref": target_source, "status": "已接入", "valid": True},
                {"step": "window_config", "ref": target_window, "status": "已生效", "valid": True},
                {"step": "stream_job", "ref": job.get("作业名称", ""), "status": "待启动", "valid": True},
                {"step": "task_topology", "ref": None, "status": "未部署", "valid": False},
            ],
            "_failover_manifest": None,
            "_exceptions": [{
                "exception_id": f"ex_{new_job_id}_{BOUNDARY_NO}_1",
                "boundary_no": BOUNDARY_NO,
                "error_type": "dual_failure_rebuild",
                "error_detail": f"由作业 {old_job_id} 双故障重建",
                "recorded_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                "recovered": True,
            }],
            "_created_at": now,
            "_updated_at": now,
        }
        store.rows(self.MODULE).append(rebuilt)

        # 3. 新版本部署新拓扑（旧拓扑已停止，不会并存两个当前拓扑）
        start_result = self.start_job(new_job_id)
        if not start_result.get("ok"):
            raise ValueError(start_result.get("error", "重建后拓扑部署失败"))

        return {
            "ok": True,
            "strategy": "rebuild",
            "terminal_state": "job_stopped_and_rebuilt",
            "old_job_id": old_job_id,
            "job_id": new_job_id,
            "version": new_version,
            "current_topology_id": rebuilt.get("current_topology_id"),
            "executable_jobs": self._executable_job_versions(),
        }

    def _executable_job_versions(self) -> list[dict[str, Any]]:
        """当前可执行作业版本清单：每个作业族只呈现一个当前版本。"""
        return [
            {
                "job_id": j["id"],
                "job_name": j.get("作业名称", ""),
                "version": j.get("version", 1),
                "status": j.get("status", ""),
                "current_topology_id": j.get("current_topology_id"),
                "manifest_published": bool(j.get("_failover_manifest")),
                "boundary_25_exception": any(
                    e.get("boundary_no") == BOUNDARY_NO
                    for e in j.get("_exceptions", [])
                ),
            }
            for j in store.rows(self.MODULE)
            if j.get("orchestration_ready") and j.get("is_current")
        ]

    @staticmethod
    def _mark_chain_step(
        job: dict[str, Any], step: str, ref: Any, status: str, valid: bool,
    ) -> None:
        for node in job.get("_call_chain", []):
            if node.get("step") == step:
                node["ref"] = ref
                node["status"] = status
                node["valid"] = valid
                return
        job.setdefault("_call_chain", []).append(
            {"step": step, "ref": ref, "status": status, "valid": valid}
        )

    # ------------------------------------------------------------------
    # 兼容既有能力
    # ------------------------------------------------------------------

    def assemble_job(
        self,
        source_name: str,
        window_name: str,
        job_name: str | None = None,
        parallelism: int = 4,
    ) -> dict[str, Any]:
        """从 stream_source 和 window_config 组装流式作业（兼容旧调用）。

        内部直接走完整编排链路，保证组装出的作业自带编排数据。
        """
        result = self.build_orchestration(source_name, window_name, job_name, parallelism)
        if not result.get("ok"):
            return {"ok": False, "error": result.get("error", "作业组装失败")}
        job_id = result["job_id"]
        job = self._find_job(job_id)
        return {
            "ok": True,
            "job_id": job_id,
            "job_name": job.get("作业名称", ""),
            "source": source_name,
            "window": window_name,
            "parallelism": parallelism,
            "idempotent": result.get("idempotent", False),
        }

    def validate_job_integrity(self, job_id: int) -> dict[str, Any]:
        """校验作业中源端和窗口策略的引用完整性（兼容旧调用）。"""
        job = store.find(self.MODULE, job_id)
        if job is None:
            return {"ok": False, "error": "作业不存在"}
        result = self.validate_topology_dependencies(job_id=job_id)
        if not result["all_valid"]:
            job["abnormal"] = True
            job["_integrity_check"] = result["refs"]
        return {
            "ok": result["all_valid"],
            "job_id": job_id,
            "refs": result["refs"],
            "all_valid": result["all_valid"],
        }

    def list_running_jobs(self) -> list[dict[str, Any]]:
        """列出所有运行中的作业及其吞吐快照。"""
        running = [
            j for j in store.rows(self.MODULE)
            if j.get("status") == "运行中"
        ]
        snapshots = []
        for j in running:
            snap = {
                "job_id": j["id"],
                "job_name": j.get("作业名称", ""),
                "source": j.get("源端配置", ""),
                "window": j.get("窗口策略", ""),
                "watermark": j.get("运行水位", "0"),
            }
            # 获取对应吞吐配置
            for tp in store.rows("throughput_mgr"):
                if tp.get("数据源") == j.get("源端配置"):
                    snap["max_qps"] = tp.get("最大QPS", "N/A")
                    snap["is_throttled"] = tp.get("status") == "已触发"
                    break
            snapshots.append(snap)
        return snapshots

    def domain_snapshot(self) -> dict[str, Any]:
        """输出模块级数据质量快照，附加跨模块引用健康度与编排链路统计。"""
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        running = sum(1 for j in rows if j.get("status") == "运行中")
        base["running_jobs"] = running
        base["abnormal_jobs"] = sum(1 for j in rows if j.get("abnormal"))
        base["orchestrated_jobs"] = sum(1 for j in rows if j.get("orchestration_ready"))
        base["current_versions"] = sum(
            1 for j in rows if j.get("orchestration_ready") and j.get("is_current")
        )
        base["published_manifests"] = sum(
            1 for j in rows if j.get("_failover_manifest")
        )
        base["boundary_25_exceptions"] = sum(
            1
            for j in rows
            for e in j.get("_exceptions", [])
            if e.get("boundary_no") == BOUNDARY_NO
        )
        return base
