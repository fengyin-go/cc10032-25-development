"""作业编排与故障转移专用接口。

串联流式作业（stream_job）与任务拓扑（task_topology）两个页面的完整链路：
  作业配置读取 → 拓扑依赖校验 → 作业编排数据 → 拓扑部署 → 故障转移清单
  → 序号25异常记录/恢复 → 双故障取舍（保留拓扑切备用源 / 停止重建）

约定：
  - 所有接口要么返回完整结果，要么返回 ok=False + error，绝不返回半成品
  - 空态统一用 empty=true + reason 表达，便于前端空态页呈现
  - 本路由在资源路由之前注册，固定路径优先于 /api/{module} 动态匹配
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel, Field

from app.services.stream_job import BOUNDARY_NO, StreamJobService
from app.services.task_topology import TaskTopologyService

router = APIRouter(prefix="/api/orchestration", tags=["作业编排与故障转移"])

job_service = StreamJobService()
topology_service = TaskTopologyService()


class BuildPayload(BaseModel):
    source_name: str
    window_name: str
    job_name: str | None = None
    parallelism: int = 4


class ExceptionPayload(BaseModel):
    error_type: str
    error_detail: str
    boundary_no: int = BOUNDARY_NO


class DualFailurePayload(BaseModel):
    strategy: str = "keep_topology"  # keep_topology | rebuild
    backup_source: str | None = None
    backup_window: str | None = None


class ValidatePayload(BaseModel):
    source_name: str | None = None
    window_name: str | None = None
    job_id: int | None = None


def _fail(message: str, **extra: Any) -> dict[str, Any]:
    return {"ok": False, "error": message, **extra}


@router.post("/build")
def build_orchestration(payload: BuildPayload) -> dict[str, Any]:
    """形成完整作业编排数据（事务化，失败整体回滚）。"""
    result = job_service.build_orchestration(
        payload.source_name,
        payload.window_name,
        payload.job_name,
        payload.parallelism,
    )
    if not result.get("ok"):
        return _fail(result.get("error", "编排数据构建失败"), rolled_back=result.get("rolled_back", False))
    return result


@router.get("/jobs/{job_id}/config")
def read_job_config(job_id: int) -> dict[str, Any]:
    """读取作业配置（源端引用、窗口引用与运行参数）。"""
    return job_service.read_job_config(job_id)


@router.post("/validate")
def validate_dependencies(payload: ValidatePayload) -> dict[str, Any]:
    """校验拓扑依赖：源端与窗口引用是否存在且可用。"""
    return job_service.validate_topology_dependencies(
        source_name=payload.source_name,
        window_name=payload.window_name,
        job_id=payload.job_id,
    )


@router.get("/jobs/{job_id}")
def get_orchestration(job_id: int) -> dict[str, Any]:
    """按作业版本核对作业编排数据；不存在时返回空态。"""
    data = job_service.get_orchestration(job_id)
    if data is None:
        return {
            "ok": False,
            "empty": True,
            "reason": f"作业 {job_id} 暂无编排数据",
            "orchestration": None,
        }
    return {"ok": True, "empty": False, "orchestration": data}


@router.post("/jobs/{job_id}/start")
def start_job(job_id: int) -> dict[str, Any]:
    """部署拓扑并启动作业；重复启动幂等，不生成第二个当前拓扑。"""
    result = job_service.start_job(job_id)
    if not result.get("ok"):
        return _fail(result.get("error", "作业启动失败"), rolled_back=result.get("rolled_back", False))
    return result


@router.post("/jobs/{job_id}/failover-manifest/publish")
def publish_manifest(job_id: int) -> dict[str, Any]:
    """发布故障转移清单（引用全部有效后才可发布，重复发布幂等）。"""
    result = job_service.publish_failover_manifest(job_id)
    if not result.get("ok"):
        return _fail(result.get("error", "故障转移清单发布失败"), rolled_back=result.get("rolled_back", False))
    return result


@router.get("/jobs/{job_id}/failover-manifest")
def get_manifest(job_id: int) -> dict[str, Any]:
    """查询故障转移清单；未发布返回空态。"""
    return job_service.get_failover_manifest(job_id)


@router.post("/jobs/{job_id}/exceptions")
def record_exception(job_id: int, payload: ExceptionPayload) -> dict[str, Any]:
    """序号25失败/重试/中断写入作业编排数据；重复异常只记一次。"""
    return job_service.record_job_exception(
        job_id,
        payload.error_type,
        payload.error_detail,
        payload.boundary_no,
    )


@router.post("/jobs/{job_id}/exceptions/recover-25")
def recover_exception_25(job_id: int) -> dict[str, Any]:
    """序号25并发异常恢复：只恢复一次，重复调用保持幂等。"""
    return job_service.recover_concurrent_exception_25(job_id)


@router.post("/jobs/{job_id}/dual-failure")
def handle_dual_failure(job_id: int, payload: DualFailurePayload) -> dict[str, Any]:
    """源端失效与窗口停用同时发生：保留拓扑切备用源 或 停止后重建。"""
    result = job_service.handle_dual_failure(
        job_id,
        strategy=payload.strategy,
        backup_source=payload.backup_source,
        backup_window=payload.backup_window,
    )
    if not result.get("ok"):
        return _fail(result.get("error", "双故障处理失败"), rolled_back=result.get("rolled_back", False))
    return result


@router.get("/jobs/executable/versions")
def executable_versions() -> dict[str, Any]:
    """可执行作业版本列表：每个作业族只呈现一个当前版本。"""
    items = job_service._executable_job_versions()
    return {
        "ok": True,
        "empty": len(items) == 0,
        "total": len(items),
        "items": items,
    }


@router.get("/topologies")
def list_topologies() -> dict[str, Any]:
    """当前可执行拓扑清单；无当前拓扑时返回空态。"""
    return topology_service.list_executable_topologies()


@router.get("/topologies/{topology_id}")
def topology_detail(topology_id: int) -> dict[str, Any]:
    """拓扑详情：当前路径、备用路径、故障转移清单与异常原因。"""
    return topology_service.get_topology_detail(topology_id)


@router.get("/jobs/{job_id}/boundary/{boundary_no}")
def orchestration_boundary(job_id: int, boundary_no: int) -> dict[str, Any]:
    """在任务拓扑数据流核对作业编排数据的异常边界（序号25）。"""
    return topology_service.verify_orchestration_boundary(job_id, boundary_no)
