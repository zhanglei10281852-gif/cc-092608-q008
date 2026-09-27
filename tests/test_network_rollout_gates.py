from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from app.core.clock import FrozenClock, to_storage
from app.core.errors import ConflictError
from app.database import get_connection
from app.network.operations import NetworkOperationsService, cohort_for_subscriber
from app.network.rules import DEFAULT_RULES

T0 = datetime(2026, 9, 26, 8, 0, tzinfo=UTC)


def ts(moment: datetime) -> str:
    return to_storage(moment)


def ops_at(moment: datetime) -> NetworkOperationsService:
    return NetworkOperationsService(get_connection(), FrozenClock(moment))


def prepare(client):
    client.post(
        "/api/network/scenarios",
        json={"code": "venue-01", "name": "大型场馆", "scene_type": "venue", "timezone": "Asia/Shanghai", "max_concurrent_sessions": 100, "capacity_mbps": 1000},
    )
    for sequence, code in enumerate(("east", "west", "north"), start=1):
        client.post(
            "/api/network/scenarios/venue-01/segments",
            json={"code": code, "name": f"{code}-zone", "sequence_no": sequence, "expected_dwell_seconds": 600, "capacity_mbps": 400},
        )
    client.post(
        "/api/network/applications",
        json={"app_code": "live-stream", "name": "移动直播", "category": "live", "latency_target_ms": 120, "packet_loss_target": 0.02, "min_downlink_mbps": 10, "min_uplink_mbps": 8, "default_priority": 75},
    )
    policy = client.post("/api/network/scenarios/venue-01/policies", json={"rules": DEFAULT_RULES, "actor": "tests"}).json()
    client.post(f"/api/network/policies/{policy['id']}/publish", json={"actor": "tests", "effective_from": "2020-01-01T00:00:00Z"})
    return policy


def phase(name, segments=None, cohorts=None, *, observation=300, min_samples=1, max_ratio=0.5, max_critical=0):
    return {
        "name": name,
        "segment_codes": segments or [],
        "cohort_keys": cohorts or [],
        "min_observation_seconds": observation,
        "min_samples": min_samples,
        "max_degraded_ratio": max_ratio,
        "max_critical_incidents": max_critical,
    }


def create_campaign(client, code, phases, policy_id):
    response = client.post(
        "/api/network/operations/campaigns",
        json={
            "scenario_code": "venue-01",
            "policy_id": policy_id,
            "code": code,
            "name": f"{code}-活动",
            "strategy": "percentage",
            "target_percentage": 50,
            "phases": phases,
            "actor": "operator",
        },
    )
    assert response.status_code == 201, response.text
    return response.json()


def inject(client, key, segment, observed_at, subscriber="subscriber-gate-0001", *, latency=80, loss=0.005, down=20, up=8):
    response = client.post(
        "/api/network/samples",
        json={
            "sample_key": key,
            "scenario_code": "venue-01",
            "segment_code": segment,
            "app_code": "live-stream",
            "subscriber_hash": subscriber,
            "device_class": "phone",
            "train_speed_kmh": 0,
            "latency_ms": latency,
            "packet_loss": loss,
            "downlink_mbps": down,
            "uplink_mbps": up,
            "observed_at": ts(observed_at),
        },
    )
    assert response.status_code == 202, response.text
    return response.json()


def test_phased_rollout_advances_through_healthy_gates(client):
    policy = prepare(client)
    campaign = create_campaign(
        client,
        "gate-healthy",
        [phase("东区灰度", ["east"], observation=600, min_samples=3), phase("西区推广", ["west"], observation=600, min_samples=2)],
        policy["id"],
    )
    campaign_id = campaign["id"]
    assert [item["sequence_no"] for item in campaign["phases"]] == [1, 2]
    assert campaign["targets"] == []

    started = ops_at(T0).start_campaign(campaign_id, "operator", "进入发布窗口")
    assert started["phases"][0]["state"] == "observing"
    assert started["phases"][0]["activated_at"] == ts(T0)
    assert {target["state"] for target in started["phases"][0]["targets"]} == {"active"}
    assert started["phases"][1]["state"] == "pending"

    paused = ops_at(T0 + timedelta(seconds=100)).pause_campaign(campaign_id, "operator", "临时观察")
    assert {target["state"] for target in paused["phases"][0]["targets"]} == {"paused"}
    skipped = ops_at(T0 + timedelta(seconds=150)).advance_due_phases()
    assert skipped == {"evaluations": []}

    resumed = ops_at(T0 + timedelta(seconds=200)).start_campaign(campaign_id, "operator", "恢复观察")
    assert {target["state"] for target in resumed["phases"][0]["targets"]} == {"active"}

    for index in range(3):
        inject(client, f"healthy-east-{index}", "east", T0 + timedelta(seconds=100 * (index + 1)))
    early = ops_at(T0 + timedelta(seconds=599)).advance_due_phases()
    assert early == {"evaluations": []}

    first = ops_at(T0 + timedelta(seconds=601)).advance_due_phases()
    assert [item["conclusion"] for item in first["evaluations"]] == ["passed"]
    assert first["evaluations"][0]["samples"] == 3
    assert first["evaluations"][0]["window_start"] == ts(T0)

    for index in range(2):
        inject(client, f"healthy-west-{index}", "west", T0 + timedelta(seconds=700 + 100 * index))
    second = ops_at(T0 + timedelta(seconds=1202)).advance_due_phases()
    assert [item["conclusion"] for item in second["evaluations"]] == ["passed"]

    detail = client.get(f"/api/network/operations/campaigns/{campaign_id}").json()
    assert detail["state"] == "completed"
    assert detail["current_phase_id"] is None
    assert detail["rollback_scope"] == []
    assert detail["policy"]["id"] == policy["id"]

    first_phase, second_phase = detail["phases"]
    assert first_phase["state"] == "passed"
    assert first_phase["decision"] == "passed"
    assert first_phase["decided_by"] == "rollout-scheduler"
    assert first_phase["decided_at"] == ts(T0 + timedelta(seconds=601))
    assert first_phase["targets"][0]["state"] == "completed"
    evidence = first_phase["evaluations"][0]
    assert evidence["window_start"] == ts(T0)
    assert evidence["window_end"] == ts(T0 + timedelta(seconds=601))
    assert (evidence["samples"], evidence["degraded"], evidence["critical_incidents"]) == (3, 0, 0)
    assert evidence["degraded_ratio"] == 0.0
    assert evidence["conclusion"] == "passed"
    assert evidence["origin"] == "auto"
    assert evidence["policy_version_id"] == policy["id"]
    assert evidence["metrics"]["thresholds"] == {"min_samples": 3, "max_degraded_ratio": 0.5, "max_critical_incidents": 0}

    assert second_phase["activated_at"] == ts(T0 + timedelta(seconds=601))
    assert second_phase["decided_at"] == ts(T0 + timedelta(seconds=1202))
    assert second_phase["evaluations"][0]["window_start"] == ts(T0 + timedelta(seconds=601))
    assert second_phase["evaluations"][0]["samples"] == 2

    assert [event["event_type"] for event in detail["events"]] == [
        "created", "started", "phase_activated", "paused", "started",
        "phase_passed", "phase_activated", "phase_passed", "completed",
    ]


def test_insufficient_data_waits_and_manual_override_is_audited(client):
    policy = prepare(client)
    campaign = create_campaign(
        client,
        "gate-insufficient",
        [phase("东区灰度", ["east"], observation=300, min_samples=5), phase("西区推广", ["west"], observation=300, min_samples=1)],
        policy["id"],
    )
    campaign_id = campaign["id"]
    ops_at(T0).start_campaign(campaign_id, "operator", "进入发布窗口")
    for index in range(2):
        inject(client, f"sparse-east-{index}", "east", T0 + timedelta(seconds=60 * (index + 1)))

    first = ops_at(T0 + timedelta(seconds=301)).advance_due_phases()
    assert [item["conclusion"] for item in first["evaluations"]] == ["insufficient_data"]
    second = ops_at(T0 + timedelta(seconds=302)).advance_due_phases()
    assert [item["conclusion"] for item in second["evaluations"]] == ["insufficient_data"]

    detail = client.get(f"/api/network/operations/campaigns/{campaign_id}").json()
    assert detail["state"] == "running"
    phase_one = detail["phases"][0]
    assert phase_one["state"] == "observing"
    assert detail["current_phase_id"] == phase_one["id"]
    assert [item["conclusion"] for item in phase_one["evaluations"]] == ["insufficient_data", "insufficient_data"]
    assert phase_one["evaluations"][0]["samples"] == 2
    evaluated = [event for event in detail["events"] if event["event_type"] == "phase_evaluated"]
    assert len(evaluated) == 2
    assert "样本量 2 低于下限 5" in evaluated[0]["detail"]["reason"]

    override = client.post(
        f"/api/network/operations/campaigns/{campaign_id}/phases/{phase_one['id']}/override",
        json={"actor": "duty-manager", "reason": "现场确认无异常，人工放行", "decision": "pass"},
    )
    assert override.status_code == 200, override.text
    detail = override.json()
    phase_one = detail["phases"][0]
    assert phase_one["state"] == "passed"
    assert phase_one["decision"] == "override_pass"
    assert phase_one["decided_by"] == "duty-manager"
    assert phase_one["decision_reason"] == "现场确认无异常，人工放行"
    assert phase_one["evaluations"][-1]["origin"] == "manual"
    assert phase_one["evaluations"][-1]["conclusion"] == "passed"
    assert detail["phases"][1]["state"] == "observing"
    assert detail["state"] == "running"

    phase_two_id = detail["phases"][1]["id"]
    failed = client.post(
        f"/api/network/operations/campaigns/{campaign_id}/phases/{phase_two_id}/override",
        json={"actor": "duty-manager", "reason": "西区投诉激增，人工止损", "decision": "fail"},
    )
    assert failed.status_code == 200, failed.text
    detail = failed.json()
    assert detail["state"] == "paused"
    assert detail["phases"][1]["decision"] == "override_fail"
    assert detail["phases"][1]["decided_by"] == "duty-manager"
    assert [item["segment_code"] for item in detail["rollback_scope"]] == ["west"]
    assert detail["phases"][1]["targets"][0]["state"] == "rolled_back"

    resume = client.post(f"/api/network/operations/campaigns/{campaign_id}/start", json={"actor": "operator", "reason": "尝试恢复"})
    assert resume.status_code == 409

    finished = client.post(
        f"/api/network/operations/campaigns/{campaign_id}/phases/{phase_two_id}/override",
        json={"actor": "duty-manager", "reason": "复核后确认误报，完成发布", "decision": "pass"},
    )
    assert finished.status_code == 200, finished.text
    detail = finished.json()
    assert detail["state"] == "completed"
    assert detail["phases"][1]["decision"] == "override_pass"
    assert [item["segment_code"] for item in detail["rollback_scope"]] == ["west"]
    assert [event["event_type"] for event in detail["events"]].count("phase_overridden") == 3


def test_degrading_gate_pauses_campaign_and_rolls_back_targets(client):
    policy = prepare(client)
    campaign = create_campaign(
        client,
        "gate-degrading",
        [phase("东西区灰度", ["east", "west"], observation=300, min_samples=2, max_ratio=0.5, max_critical=0), phase("北区推广", ["north"], observation=300, min_samples=1)],
        policy["id"],
    )
    campaign_id = campaign["id"]
    ops_at(T0).start_campaign(campaign_id, "operator", "进入发布窗口")
    inject(client, "degrade-ok", "east", T0 + timedelta(seconds=60))
    for index, segment in enumerate(("east", "west", "west")):
        inject(client, f"degrade-bad-{index}", segment, T0 + timedelta(seconds=120 + 60 * index), latency=600, loss=0.2, down=2, up=1)

    result = ops_at(T0 + timedelta(seconds=301)).advance_due_phases()
    evaluation = result["evaluations"][0]
    assert evaluation["conclusion"] == "failed"
    assert (evaluation["samples"], evaluation["degraded"], evaluation["critical_incidents"]) == (4, 3, 3)

    detail = client.get(f"/api/network/operations/campaigns/{campaign_id}").json()
    assert detail["state"] == "paused"
    phase_one = detail["phases"][0]
    assert phase_one["state"] == "failed"
    assert phase_one["decision"] == "failed"
    assert phase_one["decided_by"] == "rollout-scheduler"
    assert "质差率 0.75 超过上限 0.5" in phase_one["decision_reason"]
    assert "严重事件 3 超过上限 0" in phase_one["decision_reason"]
    evidence = phase_one["evaluations"][0]
    assert evidence["window_start"] == ts(T0)
    assert evidence["window_end"] == ts(T0 + timedelta(seconds=301))
    assert evidence["degraded_ratio"] == 0.75

    assert {item["segment_code"] for item in detail["rollback_scope"]} == {"east", "west"}
    assert all(item["rolled_back_at"] == ts(T0 + timedelta(seconds=301)) for item in detail["rollback_scope"])
    assert {target["segment_code"]: target["state"] for target in phase_one["targets"]} == {"east": "rolled_back", "west": "rolled_back"}
    assert detail["phases"][1]["state"] == "pending"
    failed_event = [event for event in detail["events"] if event["event_type"] == "phase_failed"][0]
    assert {item["segment_code"] for item in failed_event["detail"]["rolled_back"]} == {"east", "west"}

    with pytest.raises(ConflictError):
        ops_at(T0 + timedelta(seconds=320)).start_campaign(campaign_id, "operator", "尝试恢复")

    overridden = ops_at(T0 + timedelta(seconds=400)).override_phase(campaign_id, phase_one["id"], "duty-manager", "已扩容并复核指标，人工放行", "pass")
    assert overridden["state"] == "running"
    assert overridden["phases"][0]["decision"] == "override_pass"
    assert overridden["phases"][0]["decided_by"] == "duty-manager"
    assert overridden["phases"][0]["decision_reason"] == "已扩容并复核指标，人工放行"
    assert overridden["phases"][1]["state"] == "observing"
    assert len(overridden["phases"][0]["evaluations"]) == 2
    assert {item["segment_code"] for item in overridden["rollback_scope"]} == {"east", "west"}

    inject(client, "north-healthy", "north", T0 + timedelta(seconds=450))
    final = ops_at(T0 + timedelta(seconds=701)).advance_due_phases()
    assert [item["conclusion"] for item in final["evaluations"]] == ["passed"]
    detail = client.get(f"/api/network/operations/campaigns/{campaign_id}").json()
    assert detail["state"] == "completed"


def test_gate_metrics_use_frozen_policy_and_survive_later_changes(client):
    policy = prepare(client)
    campaign = create_campaign(
        client,
        "gate-frozen",
        [phase("东区灰度", ["east"], observation=300, min_samples=2, max_ratio=1.0, max_critical=0)],
        policy["id"],
    )
    campaign_id = campaign["id"]
    ops_at(T0).start_campaign(campaign_id, "operator", "进入发布窗口")

    strict_rules = {
        "score": {**DEFAULT_RULES["score"], "major_threshold": 0.5, "critical_threshold": 1.0},
        "allocation": DEFAULT_RULES["allocation"],
    }
    strict = client.post("/api/network/scenarios/venue-01/policies", json={"rules": strict_rules, "actor": "tests"}).json()
    client.post(f"/api/network/policies/{strict['id']}/publish", json={"actor": "tests", "effective_from": "2020-01-02T00:00:00Z"})

    for index in range(2):
        inject(client, f"borderline-{index}", "east", T0 + timedelta(seconds=60 * (index + 1)), latency=240, loss=0.06, down=5, up=4)
    severities = [row["severity"] for row in get_connection().execute("SELECT severity FROM quality_incidents ORDER BY id").fetchall()]
    assert severities == ["critical", "critical"]

    result = ops_at(T0 + timedelta(seconds=301)).advance_due_phases()
    assert [item["conclusion"] for item in result["evaluations"]] == ["passed"]

    detail = client.get(f"/api/network/operations/campaigns/{campaign_id}").json()
    assert detail["state"] == "completed"
    assert detail["policy"]["id"] == policy["id"]
    assert detail["policy"]["state"] == "retired"
    evidence = detail["phases"][0]["evaluations"][0]
    assert evidence["policy_version_id"] == policy["id"]
    assert evidence["critical_incidents"] == 0
    assert evidence["samples"] == 2
    assert evidence["metrics"]["policy"]["version_no"] == policy["version_no"]
    snapshot = detail["phases"][0]["evaluations"]

    changed = {**DEFAULT_RULES, "allocation": {**DEFAULT_RULES["allocation"], "duration_seconds": 240}}
    another = client.post("/api/network/scenarios/venue-01/policies", json={"rules": changed, "actor": "tests"}).json()
    client.post(f"/api/network/policies/{another['id']}/publish", json={"actor": "tests", "effective_from": "2020-01-03T00:00:00Z"})
    again = client.get(f"/api/network/operations/campaigns/{campaign_id}").json()
    assert again["phases"][0]["evaluations"] == snapshot
    assert again["phases"][0]["decision"] == "passed"


def test_cohort_targets_only_count_matching_subscribers(client):
    policy = prepare(client)
    campaign = create_campaign(
        client,
        "gate-cohort",
        [phase("贵宾分群", cohorts=["vip"], observation=300, min_samples=2, max_ratio=1.0, max_critical=0), phase("常规分群", cohorts=["standard"], observation=300, min_samples=1, max_ratio=1.0, max_critical=5)],
        policy["id"],
    )
    campaign_id = campaign["id"]
    cohorts = ["standard", "vip"]
    vip_subscribers: list[str] = []
    standard_subscribers: list[str] = []
    for index in range(60):
        subscriber = f"subscriber-cohort-{index:03d}"
        assigned = cohort_for_subscriber(subscriber, cohorts)
        if assigned == "vip" and len(vip_subscribers) < 2:
            vip_subscribers.append(subscriber)
        if assigned == "standard" and len(standard_subscribers) < 3:
            standard_subscribers.append(subscriber)
    assert len(vip_subscribers) == 2 and len(standard_subscribers) == 3

    ops_at(T0).start_campaign(campaign_id, "operator", "进入发布窗口")
    for index, subscriber in enumerate(vip_subscribers):
        inject(client, f"cohort-vip-{index}", "east", T0 + timedelta(seconds=60 * (index + 1)), subscriber=subscriber)
    for index, subscriber in enumerate(standard_subscribers[:2]):
        inject(client, f"cohort-standard-{index}", "east", T0 + timedelta(seconds=120 + 60 * index), subscriber=subscriber, latency=600, loss=0.2, down=2, up=1)

    result = ops_at(T0 + timedelta(seconds=301)).advance_due_phases()
    evaluation = result["evaluations"][0]
    assert evaluation["conclusion"] == "passed"
    assert (evaluation["samples"], evaluation["degraded"], evaluation["critical_incidents"]) == (2, 0, 0)

    inject(client, "cohort-standard-late", "west", T0 + timedelta(seconds=400), subscriber=standard_subscribers[2])
    final = ops_at(T0 + timedelta(seconds=602)).advance_due_phases()
    assert [item["conclusion"] for item in final["evaluations"]] == ["passed"]
    detail = client.get(f"/api/network/operations/campaigns/{campaign_id}").json()
    assert detail["state"] == "completed"
    assert detail["phases"][0]["targets"][0]["cohort_key"] == "vip"
    assert detail["phases"][0]["targets"][0]["segment_code"] is None
    assert detail["phases"][1]["evaluations"][0]["samples"] == 1


def test_advance_endpoint_evaluates_due_phases(client):
    policy = prepare(client)
    campaign = create_campaign(client, "gate-endpoint", [phase("东区灰度", ["east"], observation=1, min_samples=1)], policy["id"])
    campaign_id = campaign["id"]
    ops_at(T0).start_campaign(campaign_id, "operator", "进入发布窗口")
    inject(client, "endpoint-healthy", "east", T0 + timedelta(seconds=1))

    response = client.post("/api/network/operations/campaigns/advance", params={"actor": "cron-job"})
    assert response.status_code == 200, response.text
    evaluations = response.json()["evaluations"]
    assert [item["conclusion"] for item in evaluations] == ["passed"]
    assert evaluations[0]["campaign_id"] == campaign_id
    detail = client.get(f"/api/network/operations/campaigns/{campaign_id}").json()
    assert detail["state"] == "completed"
    assert detail["phases"][0]["decided_by"] == "cron-job"


def test_phased_campaign_validation_and_override_guards(client):
    policy = prepare(client)
    base = {"scenario_code": "venue-01", "policy_id": policy["id"], "name": "校验活动", "strategy": "percentage", "actor": "operator"}

    mixed = client.post("/api/network/operations/campaigns", json={**base, "code": "gate-mixed", "segment_codes": ["east"], "phases": [phase("东区", ["east"])]})
    assert mixed.status_code == 422

    empty_targets = client.post("/api/network/operations/campaigns", json={**base, "code": "gate-empty", "phases": [phase("空阶段")]})
    assert empty_targets.status_code == 422

    duplicated = client.post("/api/network/operations/campaigns", json={**base, "code": "gate-dup", "phases": [phase("重复区段", ["east", "east"])]})
    assert duplicated.status_code == 422

    missing_segment = client.post("/api/network/operations/campaigns", json={**base, "code": "gate-missing", "phases": [phase("不存在", ["nowhere"])]})
    assert missing_segment.status_code == 404

    campaign = create_campaign(client, "gate-guards", [phase("东区灰度", ["east"]), phase("西区推广", ["west"])], policy["id"])
    campaign_id = campaign["id"]
    phase_one_id = campaign["phases"][0]["id"]
    phase_two_id = campaign["phases"][1]["id"]

    not_found = client.post(f"/api/network/operations/campaigns/{campaign_id}/phases/999999/override", json={"actor": "duty-manager", "reason": "阶段不存在", "decision": "pass"})
    assert not_found.status_code == 404

    not_started = client.post(f"/api/network/operations/campaigns/{campaign_id}/phases/{phase_one_id}/override", json={"actor": "duty-manager", "reason": "活动未启动", "decision": "pass"})
    assert not_started.status_code == 409

    bad_decision = client.post(f"/api/network/operations/campaigns/{campaign_id}/phases/{phase_one_id}/override", json={"actor": "duty-manager", "reason": "非法结论", "decision": "maybe"})
    assert bad_decision.status_code == 422

    ops_at(T0).start_campaign(campaign_id, "operator", "进入发布窗口")
    not_current = client.post(f"/api/network/operations/campaigns/{campaign_id}/phases/{phase_two_id}/override", json={"actor": "duty-manager", "reason": "不是当前阶段", "decision": "pass"})
    assert not_current.status_code == 409

    legacy = client.post(
        "/api/network/operations/campaigns",
        json={"scenario_code": "venue-01", "policy_id": policy["id"], "code": "gate-legacy", "name": "传统活动", "strategy": "segments", "segment_codes": ["east"], "actor": "operator"},
    )
    assert legacy.status_code == 201
    assert legacy.json()["phases"] == []
    assert legacy.json()["rollback_scope"] == []
