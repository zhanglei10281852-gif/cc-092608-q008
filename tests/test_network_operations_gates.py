from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from app.core.clock import FrozenClock, to_storage
from app.core.errors import ConflictError
from app.database import get_connection
from app.network.operations import NetworkOperationsService, cohort_for
from app.network.rules import DEFAULT_RULES, canonical_rules

T0 = datetime(2026, 9, 26, 8, 0, tzinfo=UTC)


def prepare(client):
    client.post(
        "/api/network/scenarios",
        json={"code": "venue-01", "name": "大型场馆", "scene_type": "venue", "timezone": "Asia/Shanghai", "max_concurrent_sessions": 100, "capacity_mbps": 1000},
    )
    for sequence, code in enumerate(("east", "west"), start=1):
        client.post(
            f"/api/network/scenarios/venue-01/segments",
            json={"code": code, "name": f"{code}-zone", "sequence_no": sequence, "expected_dwell_seconds": 600, "capacity_mbps": 400},
        )
    client.post(
        "/api/network/applications",
        json={"app_code": "live-stream", "name": "移动直播", "category": "live", "latency_target_ms": 120, "packet_loss_target": 0.02, "min_downlink_mbps": 10, "min_uplink_mbps": 8, "default_priority": 75},
    )
    policy = client.post("/api/network/scenarios/venue-01/policies", json={"rules": DEFAULT_RULES, "actor": "tests"}).json()
    client.post(f"/api/network/policies/{policy['id']}/publish", json={"actor": "tests", "effective_from": "2026-09-26T00:00:00Z"})
    return policy


def operations(moment: datetime) -> NetworkOperationsService:
    return NetworkOperationsService(get_connection(), FrozenClock(moment))


def stage(name: str, **overrides):
    payload = {
        "name": name,
        "segment_codes": ["east"],
        "cohort_keys": [],
        "min_observation_seconds": 600,
        "min_samples": 2,
        "max_degraded_ratio": 0.5,
        "max_critical_events": 1,
    }
    payload.update(overrides)
    return payload


def campaign_payload(policy_id: int, **overrides):
    payload = {
        "scenario_code": "venue-01",
        "policy_id": policy_id,
        "code": "venue-gated-rollout",
        "name": "场馆分阶段门禁发布",
        "strategy": "segments",
        "target_percentage": 100,
        "failure_action": "pause",
        "stages": [stage("第一阶段"), stage("第二阶段", segment_codes=["west"])],
        "actor": "operator",
    }
    payload.update(overrides)
    return payload


def sample(client, key: str, segment: str, observed_at: datetime, subscriber: str = "subscriber-gate-00001", metrics: str = "healthy"):
    values = {
        "healthy": {"latency_ms": 80, "packet_loss": 0.005, "downlink_mbps": 20, "uplink_mbps": 10},
        "mild": {"latency_ms": 200, "packet_loss": 0.03, "downlink_mbps": 8, "uplink_mbps": 8},
        "degrading": {"latency_ms": 900, "packet_loss": 0.3, "downlink_mbps": 0.5, "uplink_mbps": 0.1},
    }[metrics]
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
            "observed_at": to_storage(observed_at),
            **values,
        },
    )
    assert response.status_code == 202, response.text
    return response.json()


def test_gate_passes_stages_and_completes_with_fixed_clock(client):
    policy = prepare(client)
    service = operations(T0)
    campaign = service.create_campaign(campaign_payload(policy["id"]))
    assert campaign["gated"] is True
    assert [item["state"] for item in campaign["stages"]] == ["pending", "pending"]
    campaign_id = campaign["id"]

    started = service.start_campaign(campaign_id, "operator", "进入发布窗口")
    first, second = started["stages"]
    assert first["state"] == "observing"
    assert first["observation_started_at"] == to_storage(T0)
    assert {target["state"] for target in first["targets"]} == {"active"}
    assert second["state"] == "pending"
    assert {target["state"] for target in second["targets"]} == {"pending"}

    for index in range(4):
        sample(client, f"healthy-east-{index}", "east", T0 + timedelta(seconds=60))

    early = operations(T0 + timedelta(seconds=300)).advance_campaigns("scheduler")
    assert early["evaluations"] == []
    assert early["waiting"] == [campaign_id]

    advanced = operations(T0 + timedelta(seconds=600)).advance_campaigns("scheduler")
    assert [item["conclusion"] for item in advanced["evaluations"]] == ["passed"]
    evaluation = advanced["evaluations"][0]
    assert evaluation["samples"] == 4
    assert evaluation["degraded"] == 0
    assert evaluation["window_start"] == to_storage(T0)
    assert evaluation["window_end"] == to_storage(T0 + timedelta(seconds=600))

    detail = service.campaign_detail(campaign_id)
    first, second = detail["stages"]
    assert first["state"] == "passed"
    assert first["conclusion"] == "passed"
    assert first["evaluated_at"] == to_storage(T0 + timedelta(seconds=600))
    assert second["state"] == "observing"
    assert second["observation_started_at"] == to_storage(T0 + timedelta(seconds=600))
    assert {target["state"] for target in second["targets"]} == {"active"}

    for index in range(4):
        sample(client, f"healthy-west-{index}", "west", T0 + timedelta(seconds=700))

    finished = operations(T0 + timedelta(seconds=1200)).advance_campaigns("scheduler")
    assert [item["conclusion"] for item in finished["evaluations"]] == ["passed"]
    detail = service.campaign_detail(campaign_id)
    assert detail["state"] == "completed"
    assert [item["state"] for item in detail["stages"]] == ["passed", "passed"]
    assert {target["state"] for item in detail["stages"] for target in item["targets"]} == {"completed"}
    assert [event["event_type"] for event in detail["events"]] == ["created", "started", "stage_started", "stage_passed", "stage_started", "stage_passed", "completed"]


def test_gate_waits_when_samples_insufficient(client):
    policy = prepare(client)
    service = operations(T0)
    campaign = service.create_campaign(campaign_payload(policy["id"], code="venue-insufficient", stages=[stage("唯一阶段", min_samples=3)]))
    campaign_id = campaign["id"]
    service.start_campaign(campaign_id, "operator", "进入发布窗口")
    sample(client, "insufficient-1", "east", T0 + timedelta(seconds=60))

    first_pass = operations(T0 + timedelta(seconds=600)).advance_campaigns("scheduler")
    assert [item["conclusion"] for item in first_pass["evaluations"]] == ["insufficient_data"]
    evaluation = first_pass["evaluations"][0]
    assert evaluation["samples"] == 1
    assert evaluation["reasons"] == ["样本量 1 低于下限 3"]

    detail = service.campaign_detail(campaign_id)
    assert detail["state"] == "running"
    assert detail["stages"][0]["state"] == "observing"

    sample(client, "insufficient-2", "east", T0 + timedelta(seconds=660))
    sample(client, "insufficient-3", "east", T0 + timedelta(seconds=700))
    second_pass = operations(T0 + timedelta(seconds=1200)).advance_campaigns("scheduler")
    assert [item["conclusion"] for item in second_pass["evaluations"]] == ["passed"]

    detail = service.campaign_detail(campaign_id)
    assert detail["state"] == "completed"
    conclusions = [item["conclusion"] for item in detail["stages"][0]["evaluations"]]
    assert conclusions == ["insufficient_data", "passed"]


def test_gate_failure_rolls_back_activated_targets(client):
    policy = prepare(client)
    service = operations(T0)
    stages = [stage("第一阶段"), stage("第二阶段", segment_codes=["west"], max_degraded_ratio=0.3)]
    campaign = service.create_campaign(campaign_payload(policy["id"], code="venue-rollback", failure_action="rollback", stages=stages))
    campaign_id = campaign["id"]
    service.start_campaign(campaign_id, "operator", "进入发布窗口")
    for index in range(2):
        sample(client, f"rollback-healthy-{index}", "east", T0 + timedelta(seconds=60))
    operations(T0 + timedelta(seconds=600)).advance_campaigns("scheduler")

    for index in range(3):
        sample(client, f"rollback-degrading-{index}", "west", T0 + timedelta(seconds=700), metrics="degrading")
    failed = operations(T0 + timedelta(seconds=1200)).advance_campaigns("scheduler")
    assert [item["conclusion"] for item in failed["evaluations"]] == ["failed"]
    evaluation = failed["evaluations"][0]
    assert evaluation["samples"] == 3
    assert evaluation["degraded"] == 3
    assert evaluation["critical_events"] == 3
    assert evaluation["reasons"] == ["质差率 1.0 超过上限 0.3", "严重事件 3 起超过上限 1"]

    detail = service.campaign_detail(campaign_id)
    assert detail["state"] == "paused"
    first, second = detail["stages"]
    assert first["state"] == "passed"
    assert second["state"] == "failed"
    assert second["conclusion"] == "failed"
    rolled_back = [target for item in detail["stages"] for target in item["targets"] if target["state"] == "rolled_back"]
    assert {target["segment_code"] for target in rolled_back} == {"east", "west"}
    assert {target["rolled_back_at"] for target in rolled_back} == {to_storage(T0 + timedelta(seconds=1200))}

    failure_events = [event for event in detail["events"] if event["event_type"] == "stage_failed"]
    assert len(failure_events) == 1
    scope = failure_events[0]["detail"]["rollback_scope"]
    assert {item["segment_code"] for item in scope} == {"east", "west"}
    assert failure_events[0]["detail"]["failure_action"] == "rollback"

    with pytest.raises(ConflictError):
        service.start_campaign(campaign_id, "operator", "未经覆盖直接恢复")


def test_manual_override_records_actor_and_reason(client):
    policy = prepare(client)
    service = operations(T0)
    campaign = service.create_campaign(campaign_payload(policy["id"], code="venue-override"))
    campaign_id = campaign["id"]
    service.start_campaign(campaign_id, "operator", "进入发布窗口")
    for index in range(3):
        sample(client, f"override-degrading-{index}", "east", T0 + timedelta(seconds=60), metrics="degrading")
    operations(T0 + timedelta(seconds=600)).advance_campaigns("scheduler")

    detail = service.campaign_detail(campaign_id)
    assert detail["state"] == "paused"
    assert detail["stages"][0]["state"] == "failed"
    assert {target["state"] for target in detail["stages"][0]["targets"]} == {"paused"}
    stage_id = detail["stages"][0]["id"]

    overridden = operations(T0 + timedelta(seconds=900)).override_stage(campaign_id, stage_id, "duty-manager", "现场确认是临时干扰，继续发布", "passed")
    assert overridden["state"] == "running"
    first, second = overridden["stages"]
    assert first["state"] == "passed"
    assert {target["state"] for target in first["targets"]} == {"active"}
    assert second["state"] == "observing"
    assert second["observation_started_at"] == to_storage(T0 + timedelta(seconds=900))
    override_evaluation = first["evaluations"][-1]
    assert override_evaluation["conclusion"] == "passed"
    assert override_evaluation["overridden"] is True
    assert override_evaluation["override_actor"] == "duty-manager"
    assert override_evaluation["override_reason"] == "现场确认是临时干扰，继续发布"
    assert [item["conclusion"] for item in first["evaluations"]] == ["failed", "passed"]
    override_events = [event for event in overridden["events"] if event["event_type"] == "stage_overridden"]
    assert override_events[0]["actor"] == "duty-manager"
    assert override_events[0]["detail"]["reason"] == "现场确认是临时干扰，继续发布"

    with pytest.raises(ConflictError):
        service.override_stage(campaign_id, stage_id, "duty-manager", "已结论的阶段不能再次覆盖", "passed")

    for index in range(2):
        sample(client, f"override-healthy-{index}", "west", T0 + timedelta(seconds=1000))
    operations(T0 + timedelta(seconds=1500)).advance_campaigns("scheduler")
    assert service.campaign_detail(campaign_id)["state"] == "completed"


def test_frozen_policy_history_immune_to_later_policy_changes(client):
    policy = prepare(client)
    service = operations(T0)
    limits = {"min_samples": 2, "max_degraded_ratio": 1.0, "max_critical_events": 0}
    stages = [stage("第一阶段", **limits), stage("第二阶段", segment_codes=["west"], **limits)]
    campaign = service.create_campaign(campaign_payload(policy["id"], code="venue-frozen", stages=stages))
    campaign_id = campaign["id"]
    service.start_campaign(campaign_id, "operator", "进入发布窗口")
    for index in range(2):
        sample(client, f"frozen-healthy-{index}", "east", T0 + timedelta(seconds=60))
    operations(T0 + timedelta(seconds=600)).advance_campaigns("scheduler")

    changed = {"score": {**DEFAULT_RULES["score"], "major_threshold": 0.1, "critical_threshold": 0.2}, "allocation": DEFAULT_RULES["allocation"]}
    draft = client.post("/api/network/scenarios/venue-01/policies", json={"rules": changed, "actor": "tests"}).json()
    client.post(f"/api/network/policies/{draft['id']}/publish", json={"actor": "tests", "effective_from": "2026-09-26T07:00:00Z"})
    frozen = get_connection().execute("SELECT state FROM policy_versions WHERE id=?", (policy["id"],)).fetchone()
    assert frozen["state"] == "retired"

    for index in range(2):
        sample(client, f"frozen-mild-{index}", "west", T0 + timedelta(seconds=700), metrics="mild")
    advanced = operations(T0 + timedelta(seconds=1200)).advance_campaigns("scheduler")
    assert [item["conclusion"] for item in advanced["evaluations"]] == ["passed"]
    evaluation = advanced["evaluations"][0]
    _, frozen_digest = canonical_rules(DEFAULT_RULES)
    assert evaluation["policy_version_id"] == policy["id"]
    assert evaluation["rules_digest"] == frozen_digest
    assert evaluation["degraded"] == 2
    assert evaluation["critical_events"] == 0

    detail = service.campaign_detail(campaign_id)
    assert detail["state"] == "completed"
    first_evaluation = detail["stages"][0]["evaluations"][0]
    assert first_evaluation["conclusion"] == "passed"
    assert first_evaluation["samples"] == 2
    assert first_evaluation["window_start"] == to_storage(T0)
    assert first_evaluation["window_end"] == to_storage(T0 + timedelta(seconds=600))
    assert first_evaluation["policy_version_id"] == policy["id"]
    assert first_evaluation["rules_digest"] == frozen_digest


def test_cohort_targets_filter_samples_by_deterministic_bucket(client):
    policy = prepare(client)
    service = operations(T0)
    stages = [
        stage("高价值分群", segment_codes=[], cohort_keys=["premium"], min_samples=2, max_critical_events=0),
        stage("标准分群", segment_codes=[], cohort_keys=["standard"], min_samples=2, max_critical_events=0),
    ]
    campaign = service.create_campaign(campaign_payload(policy["id"], code="venue-cohort", strategy="percentage", stages=stages))
    campaign_id = campaign["id"]
    universe = ["premium", "standard"]
    premium = [f"subscriber-cohort-{index:04d}" for index in range(40) if cohort_for(f"subscriber-cohort-{index:04d}", universe) == "premium"]
    standard = [f"subscriber-cohort-{index:04d}" for index in range(40) if cohort_for(f"subscriber-cohort-{index:04d}", universe) == "standard"]
    assert premium and standard

    service.start_campaign(campaign_id, "operator", "进入发布窗口")
    for index, subscriber in enumerate(standard[:2]):
        sample(client, f"cohort-noise-{index}", "east", T0 + timedelta(seconds=60), subscriber=subscriber, metrics="degrading")
    for index, subscriber in enumerate(premium[:2]):
        sample(client, f"cohort-healthy-{index}", "east", T0 + timedelta(seconds=60), subscriber=subscriber)

    advanced = operations(T0 + timedelta(seconds=600)).advance_campaigns("scheduler")
    assert [item["conclusion"] for item in advanced["evaluations"]] == ["passed"]
    evaluation = advanced["evaluations"][0]
    assert evaluation["samples"] == 2
    assert evaluation["degraded"] == 0


def test_gated_campaign_api_surface(client):
    policy = prepare(client)
    missing_scope = campaign_payload(policy["id"], code="venue-invalid", stages=[stage("空阶段", segment_codes=[], cohort_keys=[])])
    assert client.post("/api/network/operations/campaigns", json=missing_scope).status_code == 422

    created = client.post("/api/network/operations/campaigns", json=campaign_payload(policy["id"], stages=[stage("第一阶段", min_observation_seconds=3600), stage("第二阶段", segment_codes=["west"], min_observation_seconds=3600)]))
    assert created.status_code == 201, created.text
    campaign = created.json()
    assert campaign["gated"] is True
    assert campaign["failure_action"] == "pause"
    assert campaign["stages"][0]["min_samples"] == 2
    campaign_id = campaign["id"]

    started = client.post(f"/api/network/operations/campaigns/{campaign_id}/start", json={"actor": "operator", "reason": "进入发布窗口"})
    assert started.status_code == 200
    stage_id = started.json()["stages"][0]["id"]

    advance = client.post("/api/network/operations/campaigns/advance")
    assert advance.status_code == 200
    assert advance.json()["evaluations"] == []
    assert campaign_id in advance.json()["waiting"]

    rejected = client.post(f"/api/network/operations/campaigns/{campaign_id}/stages/{stage_id}/override", json={"actor": "duty-manager", "reason": "结论取值非法", "decision": "unknown"})
    assert rejected.status_code == 422

    failed = client.post(f"/api/network/operations/campaigns/{campaign_id}/stages/{stage_id}/override", json={"actor": "duty-manager", "reason": "现场巡检发现质差，人工判定失败", "decision": "failed"})
    assert failed.status_code == 200, failed.text
    assert failed.json()["state"] == "paused"
    first = failed.json()["stages"][0]
    assert first["state"] == "failed"
    assert first["evaluations"][-1]["overridden"] is True
    assert first["evaluations"][-1]["override_actor"] == "duty-manager"

    resumed = client.post(f"/api/network/operations/campaigns/{campaign_id}/stages/{stage_id}/override", json={"actor": "duty-manager", "reason": "干扰已消除，人工放行", "decision": "passed"})
    assert resumed.status_code == 200
    assert resumed.json()["state"] == "running"
    assert resumed.json()["stages"][1]["state"] == "observing"
    event_types = [event["event_type"] for event in resumed.json()["events"]]
    assert event_types.count("stage_overridden") == 2
