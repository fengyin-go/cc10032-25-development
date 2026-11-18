"""作业编排数据 ↔ 故障转移状态完整链路测试（序号25边界）。

覆盖需求：
  - 读取作业配置、校验拓扑依赖后才能形成完整编排数据
  - 编排完整后才允许部署拓扑，重复启动不生成第二个当前拓扑
  - 源端与窗口引用全部有效后故障转移清单才可发布，重复发布幂等
  - 源端失效+窗口停用双故障的两种终态取舍，列表只呈现一个可执行作业版本
  - 序号25失败/重试/中断写入编排数据，重复异常只记一次
  - 序号25并发异常只恢复一次并保持幂等，事务失败回滚原结果
  - 接口不返回半成品，空态/暂无数据可观察
  - 流式作业调用链定位异常，任务拓扑数据流核对序号25边界
"""
from __future__ import annotations

import copy

import pytest

from app.routers import orchestration as orch_router
from app.services.stream_job import BOUNDARY_NO, ORCH_TABLES, StreamJobService
from app.services.task_topology import TaskTopologyService
from app.store import store

VALID_SOURCE = "流式源端样例2"   # 已接入
VALID_WINDOW = "窗口配置样例2"   # 已生效
BACKUP_SOURCE = "流式源端样例1"  # 草稿（可用）
BACKUP_WINDOW = "窗口配置样例1"  # 草稿（可用）


@pytest.fixture
def isolated_store():
    """每个用例前后还原编排链路相关内存表，避免用例间污染。"""
    snapshot = {name: copy.deepcopy(store.rows(name)) for name in ORCH_TABLES}
    yield
    for name, rows in snapshot.items():
        store.rows(name)[:] = copy.deepcopy(rows)


@pytest.fixture
def started_job(isolated_store):
    """构建编排并启动拓扑的作业，供后续故障转移类用例使用。"""
    svc = StreamJobService()
    built = svc.build_orchestration(VALID_SOURCE, VALID_WINDOW, job_name="链路测试作业")
    assert built["ok"]
    started = svc.start_job(built["job_id"])
    assert started["ok"]
    return svc, built["job_id"], started["topology_id"]


def _break_source_and_window() -> None:
    """模拟源端失效与窗口策略停用同时发生。"""
    for row in store.rows("stream_source"):
        if row.get("源端名称") == VALID_SOURCE:
            row["status"] = "异常断连"
    for row in store.rows("window_config"):
        if row.get("策略名称") == VALID_WINDOW:
            row["status"] = "已停用"


# ---------------------------------------------------------------------------
# 1. 读取作业配置 + 校验拓扑依赖 + 形成完整编排数据
# ---------------------------------------------------------------------------

class TestBuildOrchestration:
    def test_read_job_config_contains_source_and_window_refs(self, isolated_store):
        svc = StreamJobService()
        built = svc.build_orchestration(VALID_SOURCE, VALID_WINDOW, job_name="配置作业")
        config = svc.read_job_config(built["job_id"])
        assert config["ok"] is True
        assert config["config"]["source"]["name"] == VALID_SOURCE
        assert config["config"]["window"]["name"] == VALID_WINDOW
        assert config["config"]["source"]["topic"]

    def test_read_job_config_missing_ref_returns_complete_error(self, isolated_store):
        svc = StreamJobService()
        # 手工构造一个引用悬空的作业行
        store.rows("stream_job").append({
            "id": 99901, "作业名称": "悬空作业", "源端配置": "缺失源", "窗口策略": "缺失窗口",
            "并行度": "2", "status": "草稿", "orchestration_ready": False,
        })
        result = svc.read_job_config(99901)
        assert result["ok"] is False
        assert result["config"] is None
        assert "不存在" in result["error"]

    def test_dependency_validation_detects_invalid_refs(self, isolated_store):
        svc = StreamJobService()
        result = svc.validate_topology_dependencies("缺失源", "缺失窗口")
        assert result["ok"] is False
        assert {r["ref"] for r in result["refs"]} == {"stream_source", "window_config"}
        assert all(r["valid"] is False for r in result["refs"])

    def test_build_requires_complete_orchestration_no_half_result(self, isolated_store):
        before = len(store.rows("stream_job"))
        result = StreamJobService().build_orchestration("缺失源", VALID_WINDOW)
        after = len(store.rows("stream_job"))
        assert result["ok"] is False
        assert result.get("rolled_back") is True
        assert after == before, "失败后不能留下半成品作业行"

    def test_build_blocked_when_source_disconnected(self, isolated_store):
        for row in store.rows("stream_source"):
            if row.get("源端名称") == VALID_SOURCE:
                row["status"] = "异常断连"
        result = StreamJobService().build_orchestration(VALID_SOURCE, VALID_WINDOW)
        assert result["ok"] is False
        assert "异常断连" in result["error"]

    def test_build_blocked_when_window_disabled(self, isolated_store):
        for row in store.rows("window_config"):
            if row.get("策略名称") == VALID_WINDOW:
                row["status"] = "已停用"
        result = StreamJobService().build_orchestration(VALID_SOURCE, VALID_WINDOW)
        assert result["ok"] is False
        assert "已停用" in result["error"]

    def test_build_orchestration_records_call_chain(self, isolated_store):
        svc = StreamJobService()
        built = svc.build_orchestration(VALID_SOURCE, VALID_WINDOW)
        orch = svc.get_orchestration(built["job_id"])
        steps = [node["step"] for node in orch["call_chain"]]
        assert steps == ["stream_source", "window_config", "stream_job", "task_topology"]
        assert orch["orchestration_ready"] is True

    def test_build_same_refs_is_idempotent(self, isolated_store):
        svc = StreamJobService()
        first = svc.build_orchestration(VALID_SOURCE, VALID_WINDOW, job_name="幂等作业")
        second = svc.build_orchestration(VALID_SOURCE, VALID_WINDOW, job_name="幂等作业")
        assert first["ok"] is second["ok"] is True
        assert second["idempotent"] is True
        assert first["job_id"] == second["job_id"]


# ---------------------------------------------------------------------------
# 2. 部署拓扑：编排完整才部署，重复启动不生成第二个当前拓扑
# ---------------------------------------------------------------------------

class TestDeployTopology:
    def test_start_requires_orchestration_data(self, isolated_store):
        store.rows("stream_job").append({
            "id": 99902, "作业名称": "未编排作业", "源端配置": VALID_SOURCE,
            "窗口策略": VALID_WINDOW, "并行度": "2", "status": "草稿",
            "orchestration_ready": False,
        })
        result = StreamJobService().start_job(99902)
        assert result["ok"] is False
        assert "编排数据不完整" in result["error"]

    def test_start_generates_executable_topology_and_backup_paths(self, started_job):
        svc, job_id, topology_id = started_job
        topo = store.find("task_topology", topology_id)
        assert topo["is_current"] is True
        assert len(topo["current_path"]) >= 3
        assert topo["backup_paths"], "应生成备用路径"
        job = store.find("stream_job", job_id)
        assert job["current_topology_id"] == topology_id
        assert job["status"] == "运行中"

    def test_restart_does_not_create_second_current_topology(self, started_job):
        svc, job_id, topology_id = started_job
        before = len([t for t in store.rows("task_topology") if t.get("is_current")])
        second = svc.start_job(job_id)
        after = len([t for t in store.rows("task_topology") if t.get("is_current")])
        assert second["idempotent"] is True
        assert second["topology_id"] == topology_id
        assert before == after == 1


# ---------------------------------------------------------------------------
# 3. 故障转移清单：引用全部有效才可发布
# ---------------------------------------------------------------------------

class TestFailoverManifest:
    def test_manifest_published_after_refs_valid(self, started_job):
        svc, job_id, _ = started_job
        result = svc.publish_failover_manifest(job_id)
        assert result["ok"] is True
        assert result["manifest"]["status"] == "published"
        actions = [e["action"] for e in result["manifest"]["entries"]]
        assert "restart_job" in actions  # 兜底条目始终存在

    def test_manifest_blocked_when_refs_invalid(self, started_job):
        svc, job_id, _ = started_job
        _break_source_and_window()
        result = svc.publish_failover_manifest(job_id)
        assert result["ok"] is False
        assert "清单不可发布" in result["error"]
        assert svc._find_job(job_id)["_failover_manifest"] is None

    def test_manifest_republish_is_idempotent(self, started_job):
        svc, job_id, _ = started_job
        first = svc.publish_failover_manifest(job_id)
        second = svc.publish_failover_manifest(job_id)
        assert second["idempotent"] is True
        assert first["manifest"]["manifest_id"] == second["manifest"]["manifest_id"]

    def test_manifest_empty_state_before_publish(self, isolated_store):
        svc = StreamJobService()
        built = svc.build_orchestration(VALID_SOURCE, VALID_WINDOW)
        result = svc.get_failover_manifest(built["job_id"])
        assert result["empty"] is True
        assert result["manifest"] is None
        assert "暂无数据" in result["reason"]


# ---------------------------------------------------------------------------
# 4. 序号25：失败/重试/中断、重复异常只记一次、恢复只一次
# ---------------------------------------------------------------------------

class TestBoundary25Exceptions:
    def test_failure_retry_interrupt_written_to_orchestration(self, started_job):
        svc, job_id, _ = started_job
        for kind, detail in [
            ("failure", "源端连接失败"),
            ("retry", "重试调度失败"),
            ("interrupt", "作业被中断"),
        ]:
            assert svc.record_job_exception(job_id, kind, detail)["ok"] is True
        orch = svc.get_orchestration(job_id)
        assert len(orch["exceptions"]) == 3
        assert all(e["boundary_no"] == BOUNDARY_NO for e in orch["exceptions"])

    def test_duplicate_exception_recorded_once(self, started_job):
        svc, job_id, _ = started_job
        first = svc.record_job_exception(job_id, "failure", "第一次失败")
        second = svc.record_job_exception(job_id, "failure", "重复失败上报")
        assert first["idempotent"] is False
        assert second["idempotent"] is True
        assert first["exception"]["exception_id"] == second["exception"]["exception_id"]
        assert len(svc.get_orchestration(job_id)["exceptions"]) == 1

    def test_concurrent_exception_recovers_once_and_idempotent(self, started_job):
        svc, job_id, _ = started_job
        svc.record_job_exception(job_id, "failure", "并发异常")
        first = svc.recover_concurrent_exception_25(job_id)
        second = svc.recover_concurrent_exception_25(job_id)
        assert first["idempotent"] is False
        assert second["idempotent"] is True
        assert first["recovery"] == second["recovery"]
        assert first["recovery"]["status"] == "recovered"
        assert all(e["recovered"] for e in svc.get_orchestration(job_id)["exceptions"])

    def test_transaction_failure_restores_original_result(self, isolated_store):
        svc = StreamJobService()
        built = svc.build_orchestration(VALID_SOURCE, VALID_WINDOW, job_name="事务作业")
        job_id = built["job_id"]
        before = svc.get_orchestration(job_id)
        # 依赖失效后启动拓扑必然失败，事务回滚，作业保持原状态
        _break_source_and_window()
        result = svc.start_job(job_id)
        assert result["ok"] is False
        assert result.get("rolled_back") is True
        after = svc.get_orchestration(job_id)
        assert after["status"] == before["status"] == "待启动"
        assert after["current_topology_id"] is None

    def test_topology_detail_explains_exception_reason(self, started_job):
        svc, job_id, topology_id = started_job
        svc.record_job_exception(job_id, "failure", "序号25源端断连")
        detail = TaskTopologyService().get_topology_detail(topology_id)
        assert detail["ok"] is True
        reasons = detail["detail"]["exception_reasons"]
        assert any("序号25源端断连" == r["error_detail"] for r in reasons)

    def test_boundary_25_data_flow_check(self, started_job):
        svc, job_id, topology_id = started_job
        svc.record_job_exception(job_id, "interrupt", "中断")
        boundary = TaskTopologyService().verify_orchestration_boundary(job_id, BOUNDARY_NO)
        assert boundary["ok"] is True
        assert boundary["boundary_no"] == 25
        assert boundary["orchestration_ready"] is True
        assert len(boundary["exceptions"]) == 1
        assert boundary["topology_id"] == topology_id


# ---------------------------------------------------------------------------
# 5. 双故障两种终态取舍：只呈现一个可执行作业版本
# ---------------------------------------------------------------------------

class TestDualFailureTerminalStates:
    def test_keep_topology_switches_backup_source_and_keeps_topology(self, started_job):
        svc, job_id, topology_id = started_job
        _break_source_and_window()
        result = svc.handle_dual_failure(
            job_id, "keep_topology",
            backup_source=BACKUP_SOURCE, backup_window=BACKUP_WINDOW,
        )
        assert result["ok"] is True
        assert result["terminal_state"] == "topology_kept_source_switched"
        assert result["current_topology_id"] == topology_id
        job = svc._find_job(job_id)
        assert job["源端配置"] == BACKUP_SOURCE
        assert job["窗口策略"] == BACKUP_WINDOW
        assert job["status"] == "运行中"
        assert len(result["executable_jobs"]) == 1

    def test_rebuild_stops_old_job_and_creates_single_new_version(self, started_job):
        svc, job_id, old_topology_id = started_job
        _break_source_and_window()
        result = svc.handle_dual_failure(
            job_id, "rebuild",
            backup_source=BACKUP_SOURCE, backup_window=BACKUP_WINDOW,
        )
        assert result["ok"] is True
        assert result["terminal_state"] == "job_stopped_and_rebuilt"
        new_job_id = result["job_id"]
        assert new_job_id != job_id

        old_job = svc._find_job(job_id)
        new_job = svc._find_job(new_job_id)
        assert old_job["status"] == "已停止" and old_job["is_current"] is False
        assert new_job["is_current"] is True and new_job["version"] == 2

        current_topos = [t for t in store.rows("task_topology") if t.get("is_current")]
        assert len(current_topos) == 1
        assert current_topos[0]["id"] == new_job["current_topology_id"]
        assert store.find("task_topology", old_topology_id)["status"] == "已停止"
        assert len(result["executable_jobs"]) == 1

    def test_two_strategies_produce_different_terminal_states(self, isolated_store):
        """序号25异常边界在两种取舍下形成不同终态版本。"""
        outcomes = {}
        for strategy in ("keep_topology", "rebuild"):
            svc = StreamJobService()
            built = svc.build_orchestration(VALID_SOURCE, VALID_WINDOW, job_name=f"取舍-{strategy}")
            svc.start_job(built["job_id"])
            _break_source_and_window()
            outcomes[strategy] = svc.handle_dual_failure(
                built["job_id"], strategy,
                backup_source=BACKUP_SOURCE, backup_window=BACKUP_WINDOW,
            )
            # 还原引用供下一轮构建
            for row in store.rows("stream_source"):
                if row.get("源端名称") == VALID_SOURCE:
                    row["status"] = "已接入"
            for row in store.rows("window_config"):
                if row.get("策略名称") == VALID_WINDOW:
                    row["status"] = "已生效"
        assert outcomes["keep_topology"]["terminal_state"] != outcomes["rebuild"]["terminal_state"]
        # 每个作业族各自只呈现一个可执行版本（两个作业族互不干扰）
        family_jobs = {
            strategy: [
                item for item in result["executable_jobs"]
                if item["job_name"] == f"取舍-{strategy}"
            ]
            for strategy, result in outcomes.items()
        }
        assert all(len(items) == 1 for items in family_jobs.values())
        current_topos = [t for t in store.rows("task_topology") if t.get("is_current")]
        assert len(current_topos) == 2, "两个作业族各有一个当前拓扑"

    def test_dual_failure_without_available_backup_rolls_back(self, started_job):
        svc, job_id, _ = started_job
        _break_source_and_window()
        # 备用源也失效 → 无可用备用路径，事务回滚
        for row in store.rows("stream_source"):
            if row.get("源端名称") == BACKUP_SOURCE:
                row["status"] = "异常断连"
        result = svc.handle_dual_failure(
            job_id, "keep_topology",
            backup_source=BACKUP_SOURCE, backup_window=BACKUP_WINDOW,
        )
        assert result["ok"] is False
        assert result.get("rolled_back") is True
        # 原作业引用未被改动
        job = svc._find_job(job_id)
        assert job["源端配置"] == VALID_SOURCE


# ---------------------------------------------------------------------------
# 6. 空态与暂无数据
# ---------------------------------------------------------------------------

class TestEmptyStates:
    def test_topology_detail_empty_state(self, isolated_store):
        result = TaskTopologyService().get_topology_detail(98765)
        assert result["empty"] is True
        assert result["detail"] is None

    def test_boundary_empty_state_for_missing_job(self, isolated_store):
        result = TaskTopologyService().verify_orchestration_boundary(98765, BOUNDARY_NO)
        assert result["empty"] is True

    def test_orchestration_empty_state_for_missing_job(self, isolated_store):
        result = StreamJobService().get_orchestration(98765)
        assert result is None

    def test_executable_topology_list_shape(self, started_job):
        result = TaskTopologyService().list_executable_topologies()
        assert result["empty"] is False
        assert result["total"] >= 1
        assert all(item["is_current"] for item in result["items"])


# ---------------------------------------------------------------------------
# 7. HTTP 接口：不返回半成品，串联两个异常页面
# ---------------------------------------------------------------------------

class TestOrchestrationAPI:
    """通过路由处理函数验证接口契约（环境不含 httpx，无法起 TestClient）。"""

    def test_full_chain_via_api(self, isolated_store):
        # 构建编排
        built = orch_router.build_orchestration(orch_router.BuildPayload(
            source_name=VALID_SOURCE, window_name=VALID_WINDOW,
            job_name="接口链路作业", parallelism=4,
        ))
        assert built["ok"]
        job_id = built["job_id"]

        # 读取配置
        config = orch_router.read_job_config(job_id)
        assert config["ok"] and config["config"]["source"]["name"] == VALID_SOURCE

        # 依赖校验
        validation = orch_router.validate_dependencies(
            orch_router.ValidatePayload(job_id=job_id),
        )
        assert validation["all_valid"] is True

        # 启动
        started = orch_router.start_job(job_id)
        assert started["ok"] and started["topology_id"]
        topology_id = started["topology_id"]

        # 清单发布
        manifest = orch_router.publish_manifest(job_id)
        assert manifest["ok"] and manifest["manifest"]["entries"]

        # 序号25异常 → 恢复
        exc = orch_router.record_exception(job_id, orch_router.ExceptionPayload(
            error_type="failure", error_detail="接口侧序号25失败",
        ))
        assert exc["ok"]
        recovered = orch_router.recover_exception_25(job_id)
        assert recovered["ok"] and recovered["recovery"]["boundary_no"] == 25

        # 拓扑详情页能看到异常原因（两个异常页面串联）
        detail = orch_router.topology_detail(topology_id)
        assert detail["ok"]
        assert any(r["boundary_no"] == 25 for r in detail["detail"]["exception_reasons"])

        # 序号25边界核对
        boundary = orch_router.orchestration_boundary(job_id, 25)
        assert boundary["ok"] and boundary["exceptions"]

    def test_api_build_failure_returns_no_half_result(self, isolated_store):
        body = orch_router.build_orchestration(orch_router.BuildPayload(
            source_name="不存在的源", window_name=VALID_WINDOW,
        ))
        assert body["ok"] is False
        assert body["error"]
        # 通用列表接口确认没有新增半成品
        listed, total = StreamJobService().list_entries()
        assert all(item.get("源端配置") != "不存在的源" for item in listed)

    def test_api_manifest_empty_state(self, isolated_store):
        body = orch_router.get_manifest(98765)
        assert body["empty"] is True and body["manifest"] is None

    def test_api_dual_failure_rebuild_keeps_one_executable_version(self, isolated_store):
        built = orch_router.build_orchestration(orch_router.BuildPayload(
            source_name=VALID_SOURCE, window_name=VALID_WINDOW, job_name="接口重建作业",
        ))
        job_id = built["job_id"]
        orch_router.start_job(job_id)
        _break_source_and_window()
        result = orch_router.handle_dual_failure(job_id, orch_router.DualFailurePayload(
            strategy="rebuild",
            backup_source=BACKUP_SOURCE,
            backup_window=BACKUP_WINDOW,
        ))
        assert result["ok"] and result["terminal_state"] == "job_stopped_and_rebuilt"
        versions = orch_router.executable_versions()
        assert versions["total"] == 1
        topos = orch_router.list_topologies()
        assert topos["total"] == 1

    def test_api_validate_endpoint_reports_invalid_refs(self, isolated_store):
        body = orch_router.validate_dependencies(orch_router.ValidatePayload(
            source_name="幽灵源", window_name="幽灵窗口",
        ))
        assert body["ok"] is False
        assert all(not ref["valid"] for ref in body["refs"])
