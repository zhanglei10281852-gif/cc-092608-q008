from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import timedelta
from typing import Any

from app.core.clock import Clock, SystemClock, from_storage, to_storage
from app.core.errors import ConflictError, NotFoundError, ValidationError
from app.database import get_connection, transaction
from app.network.repository import NetworkRepository
from app.network.rules import judge_quality
from app.network.schema import ensure_network_schema


def cohort_for_subscriber(subscriber_hash: str, cohort_keys: list[str]) -> str:
    if not cohort_keys:
        return ""
    digest = hashlib.sha256(subscriber_hash.encode("utf-8")).hexdigest()
    return sorted(cohort_keys)[int(digest, 16) % len(cohort_keys)]


class NetworkOperationsService:
    def __init__(self, connection: sqlite3.Connection | None = None, clock: Clock | None = None) -> None:
        self.connection = connection or get_connection()
        ensure_network_schema(self.connection)
        self.clock = clock or SystemClock()
        self.repository = NetworkRepository(self.connection)

    def create_campaign(self, payload: dict[str, Any]) -> dict[str, Any]:
        scenario = self._scenario(payload["scenario_code"])
        policy = self.repository.policy_by_id(payload["policy_id"])
        if policy is None or policy["scenario_id"] != scenario["id"]:
            raise ValidationError("发布策略不属于目标场景")
        if policy["state"] not in {"draft", "published"}:
            raise ConflictError("退役策略不能用于新的发布活动")
        starts_at = self._optional_time(payload.get("starts_at"), "开始时间")
        ends_at = self._optional_time(payload.get("ends_at"), "结束时间")
        if starts_at and ends_at and ends_at <= starts_at:
            raise ValidationError("发布结束时间必须晚于开始时间")
        phases = payload.get("phases") or []
        segment_ids = self._segment_ids(scenario["id"], payload.get("segment_codes", []))
        phase_segments = [self._segment_ids(scenario["id"], phase.get("segment_codes", [])) for phase in phases]
        cohorts = payload.get("cohort_keys") or [""]
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            try:
                cursor = connection.execute(
                    "INSERT INTO rollout_campaigns(scenario_id,code,name,strategy,target_percentage,policy_version_id,starts_at,ends_at,created_by,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    (scenario["id"], payload["code"], payload["name"], payload["strategy"], payload["target_percentage"], policy["id"], starts_at, ends_at, payload["actor"], now, now),
                )
            except sqlite3.IntegrityError as exc:
                raise ConflictError("发布活动编码已存在") from exc
            campaign_id = cursor.lastrowid
            target_count = 0
            if phases:
                for sequence, (phase, phase_segment_ids) in enumerate(zip(phases, phase_segments), start=1):
                    phase_cursor = connection.execute(
                        "INSERT INTO rollout_phases(campaign_id,sequence_no,name,min_observation_seconds,min_samples,max_degraded_ratio,max_critical_incidents,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)",
                        (campaign_id, sequence, phase["name"], phase["min_observation_seconds"], phase["min_samples"], phase["max_degraded_ratio"], phase["max_critical_incidents"], now, now),
                    )
                    pairs = sorted({(segment_id, cohort) for segment_id in (phase_segment_ids or [None]) for cohort in (phase.get("cohort_keys") or [""])}, key=lambda pair: (pair[0] or 0, pair[1]))
                    for segment_id, cohort in pairs:
                        connection.execute(
                            "INSERT INTO rollout_phase_targets(phase_id,segment_id,cohort_key) VALUES(?,?,?)",
                            (phase_cursor.lastrowid, segment_id, cohort),
                        )
                        target_count += 1
            else:
                targets = segment_ids or [None]
                for segment_id in targets:
                    for cohort in cohorts:
                        connection.execute(
                            "INSERT INTO rollout_targets(campaign_id,segment_id,cohort_key) VALUES(?,?,?)",
                            (campaign_id, segment_id, cohort),
                        )
                        target_count += 1
            self._event(connection, "campaign", campaign_id, "created", payload["actor"], {"targets": target_count, "phases": len(phases)}, now)
            return self.campaign_detail(campaign_id, connection)

    def campaign_detail(self, campaign_id: int, connection: sqlite3.Connection | None = None) -> dict[str, Any]:
        connection = connection or self.connection
        campaign = connection.execute("SELECT * FROM rollout_campaigns WHERE id=?", (campaign_id,)).fetchone()
        if campaign is None:
            raise NotFoundError("发布活动不存在")
        result = dict(campaign)
        result["targets"] = [dict(row) for row in connection.execute(
            "SELECT t.*,s.code AS segment_code,s.name AS segment_name FROM rollout_targets t LEFT JOIN network_segments s ON s.id=t.segment_id WHERE t.campaign_id=? ORDER BY t.id",
            (campaign_id,),
        ).fetchall()]
        phases: list[dict[str, Any]] = []
        rollback_scope: list[dict[str, Any]] = []
        for phase in connection.execute("SELECT * FROM rollout_phases WHERE campaign_id=? ORDER BY sequence_no", (campaign_id,)).fetchall():
            item = dict(phase)
            item["targets"] = [dict(row) for row in connection.execute(
                "SELECT t.*,s.code AS segment_code,s.name AS segment_name FROM rollout_phase_targets t LEFT JOIN network_segments s ON s.id=t.segment_id WHERE t.phase_id=? ORDER BY t.id",
                (phase["id"],),
            ).fetchall()]
            item["evaluations"] = [self._evaluation(row) for row in connection.execute(
                "SELECT * FROM rollout_phase_evaluations WHERE phase_id=? ORDER BY id",
                (phase["id"],),
            ).fetchall()]
            for target in item["targets"]:
                if target["state"] == "rolled_back":
                    rollback_scope.append({
                        "phase_id": phase["id"],
                        "sequence_no": phase["sequence_no"],
                        "phase_name": phase["name"],
                        "segment_code": target["segment_code"],
                        "cohort_key": target["cohort_key"],
                        "rolled_back_at": target["rolled_back_at"],
                    })
            phases.append(item)
        result["phases"] = phases
        current = next((phase for phase in phases if phase["state"] != "passed"), None)
        result["current_phase_id"] = current["id"] if current else None
        result["rollback_scope"] = rollback_scope
        policy = connection.execute("SELECT id,version_no,state,rules_digest FROM policy_versions WHERE id=?", (campaign["policy_version_id"],)).fetchone()
        result["policy"] = dict(policy) if policy is not None else None
        result["events"] = self._events(connection, "campaign", campaign_id)
        return result

    def list_campaigns(self, scenario_code: str | None = None, state: str | None = None) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if scenario_code:
            clauses.append("n.code=?")
            params.append(scenario_code)
        if state:
            clauses.append("c.state=?")
            params.append(state)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        rows = self.connection.execute(
            "SELECT c.*,n.code AS scenario_code,n.name AS scenario_name FROM rollout_campaigns c JOIN network_scenarios n ON n.id=c.scenario_id" + where + " ORDER BY c.created_at DESC,c.id DESC",
            params,
        ).fetchall()
        return [dict(row) for row in rows]

    def start_campaign(self, campaign_id: int, actor: str, reason: str) -> dict[str, Any]:
        campaign = self._campaign(campaign_id)
        if campaign["state"] not in {"draft", "scheduled", "paused"}:
            raise ConflictError("当前发布活动状态不能启动")
        now = to_storage(self.clock.now())
        if campaign["starts_at"] and campaign["starts_at"] > now:
            raise ConflictError("发布活动尚未到开始时间")
        current = self._current_phase(self.connection, campaign_id)
        if current is not None and current["state"] == "failed":
            raise ConflictError("门禁失败的阶段需要通过人工覆盖处理")
        with transaction(immediate=True) as connection:
            connection.execute("UPDATE rollout_campaigns SET state='running',updated_at=? WHERE id=?", (now, campaign_id))
            connection.execute("UPDATE rollout_targets SET state='active',activated_at=COALESCE(activated_at,?),version=version+1 WHERE campaign_id=? AND state IN ('pending','paused')", (now, campaign_id))
            self._event(connection, "campaign", campaign_id, "started", actor, {"reason": reason}, now)
            if current is not None and current["state"] == "pending":
                connection.execute("UPDATE rollout_phases SET state='observing',activated_at=?,updated_at=? WHERE id=?", (now, now, current["id"]))
                connection.execute("UPDATE rollout_phase_targets SET state='active',activated_at=? WHERE phase_id=? AND state='pending'", (now, current["id"]))
                self._event(connection, "campaign", campaign_id, "phase_activated", actor, {"phase_id": current["id"], "sequence_no": current["sequence_no"], "name": current["name"]}, now)
            elif current is not None and current["state"] == "observing":
                connection.execute("UPDATE rollout_phase_targets SET state='active' WHERE phase_id=? AND state='paused'", (current["id"],))
            return self.campaign_detail(campaign_id, connection)

    def pause_campaign(self, campaign_id: int, actor: str, reason: str) -> dict[str, Any]:
        campaign = self._campaign(campaign_id)
        if campaign["state"] != "running":
            raise ConflictError("只有运行中的发布活动可以暂停")
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            connection.execute("UPDATE rollout_campaigns SET state='paused',updated_at=? WHERE id=?", (now, campaign_id))
            connection.execute("UPDATE rollout_targets SET state='paused',version=version+1 WHERE campaign_id=? AND state='active'", (campaign_id,))
            connection.execute(
                "UPDATE rollout_phase_targets SET state='paused' WHERE state='active' AND phase_id IN (SELECT id FROM rollout_phases WHERE campaign_id=?)",
                (campaign_id,),
            )
            self._event(connection, "campaign", campaign_id, "paused", actor, {"reason": reason}, now)
            return self.campaign_detail(campaign_id, connection)

    def complete_campaign(self, campaign_id: int, actor: str, reason: str) -> dict[str, Any]:
        campaign = self._campaign(campaign_id)
        if campaign["state"] not in {"running", "paused"}:
            raise ConflictError("当前发布活动状态不能完成")
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            connection.execute("UPDATE rollout_campaigns SET state='completed',ends_at=COALESCE(ends_at,?),updated_at=? WHERE id=?", (now, now, campaign_id))
            connection.execute("UPDATE rollout_targets SET state='completed',completed_at=?,version=version+1 WHERE campaign_id=? AND state IN ('active','paused')", (now, campaign_id))
            connection.execute(
                "UPDATE rollout_phase_targets SET state='completed',completed_at=? WHERE state IN ('active','paused') AND phase_id IN (SELECT id FROM rollout_phases WHERE campaign_id=?)",
                (now, campaign_id),
            )
            self._event(connection, "campaign", campaign_id, "completed", actor, {"reason": reason}, now)
            return self.campaign_detail(campaign_id, connection)

    def advance_due_phases(self, actor: str = "rollout-scheduler") -> dict[str, Any]:
        now_value = self.clock.now()
        due = self.connection.execute(
            "SELECT p.id,p.campaign_id,p.activated_at,p.min_observation_seconds FROM rollout_phases p "
            "JOIN rollout_campaigns c ON c.id=p.campaign_id WHERE p.state='observing' AND c.state='running' ORDER BY p.id",
        ).fetchall()
        evaluations = []
        for row in due:
            activated = from_storage(row["activated_at"])
            if activated is None or now_value < activated + timedelta(seconds=int(row["min_observation_seconds"])):
                continue
            evaluations.append(self._evaluate_phase(row["campaign_id"], row["id"], actor, origin="auto"))
        return {"evaluations": evaluations}

    def override_phase(self, campaign_id: int, phase_id: int, actor: str, reason: str, decision: str) -> dict[str, Any]:
        campaign = self._campaign(campaign_id)
        if decision not in {"pass", "fail"}:
            raise ValidationError("人工覆盖结论只能是 pass 或 fail")
        phase = self.connection.execute("SELECT * FROM rollout_phases WHERE id=?", (phase_id,)).fetchone()
        if phase is None or phase["campaign_id"] != campaign_id:
            raise NotFoundError("发布阶段不存在")
        if campaign["state"] not in {"running", "paused"}:
            raise ConflictError("只有运行或暂停中的发布活动可以人工覆盖门禁")
        current = self._current_phase(self.connection, campaign_id)
        if current is None or current["id"] != phase_id:
            raise ConflictError("只能覆盖当前门禁阶段")
        if phase["state"] not in {"observing", "failed"}:
            raise ConflictError("当前阶段状态不能人工覆盖")
        if phase["state"] == "failed" and decision == "fail":
            raise ConflictError("阶段已处于失败状态")
        self._evaluate_phase(campaign_id, phase_id, actor, origin="manual", override=decision, override_reason=reason)
        return self.campaign_detail(campaign_id)

    def create_maintenance(self, payload: dict[str, Any]) -> dict[str, Any]:
        scenario = self._scenario(payload["scenario_code"])
        segment_id = None
        if payload.get("segment_code"):
            segment = self.repository.segment_by_code(scenario["id"], payload["segment_code"])
            if segment is None:
                raise NotFoundError("维护区段不存在")
            segment_id = segment["id"]
        starts_at = self._required_time(payload["starts_at"], "开始时间")
        ends_at = self._required_time(payload["ends_at"], "结束时间")
        if ends_at <= starts_at:
            raise ValidationError("维护结束时间必须晚于开始时间")
        overlap = self.connection.execute(
            "SELECT id FROM maintenance_windows WHERE scenario_id=? AND segment_id IS ? AND state IN ('scheduled','active') AND starts_at<? AND ends_at>?",
            (scenario["id"], segment_id, ends_at, starts_at),
        ).fetchone()
        if overlap:
            raise ConflictError("相同范围已有重叠维护窗口")
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            try:
                cursor = connection.execute(
                    "INSERT INTO maintenance_windows(scenario_id,segment_id,code,reason,starts_at,ends_at,drain_mode,created_by,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (scenario["id"], segment_id, payload["code"], payload["reason"], starts_at, ends_at, payload["drain_mode"], payload["actor"], now, now),
                )
            except sqlite3.IntegrityError as exc:
                raise ConflictError("维护窗口编码已存在") from exc
            self._event(connection, "maintenance", cursor.lastrowid, "scheduled", payload["actor"], {"drain_mode": payload["drain_mode"]}, now)
            return self.maintenance_detail(cursor.lastrowid, connection)

    def maintenance_detail(self, window_id: int, connection: sqlite3.Connection | None = None) -> dict[str, Any]:
        connection = connection or self.connection
        row = connection.execute(
            "SELECT w.*,n.code AS scenario_code,n.name AS scenario_name,s.code AS segment_code,s.name AS segment_name FROM maintenance_windows w JOIN network_scenarios n ON n.id=w.scenario_id LEFT JOIN network_segments s ON s.id=w.segment_id WHERE w.id=?",
            (window_id,),
        ).fetchone()
        if row is None:
            raise NotFoundError("维护窗口不存在")
        result = dict(row)
        result["events"] = self._events(connection, "maintenance", window_id)
        return result

    def activate_due_maintenance(self, actor: str = "maintenance-scheduler") -> dict[str, Any]:
        now = to_storage(self.clock.now())
        activated: list[int] = []
        completed: list[int] = []
        with transaction(immediate=True) as connection:
            due = connection.execute("SELECT * FROM maintenance_windows WHERE state='scheduled' AND starts_at<=? ORDER BY id", (now,)).fetchall()
            for window in due:
                connection.execute("UPDATE maintenance_windows SET state='active',updated_at=? WHERE id=?", (now, window["id"]))
                self._event(connection, "maintenance", window["id"], "activated", actor, {}, now)
                activated.append(window["id"])
            ended = connection.execute("SELECT * FROM maintenance_windows WHERE state='active' AND ends_at<=? ORDER BY id", (now,)).fetchall()
            for window in ended:
                connection.execute("UPDATE maintenance_windows SET state='completed',updated_at=? WHERE id=?", (now, window["id"]))
                self._event(connection, "maintenance", window["id"], "completed", actor, {}, now)
                completed.append(window["id"])
        return {"activated": activated, "completed": completed}

    def blocks_new_session(self, scenario_id: int, segment_id: int | None, now: str) -> dict[str, Any] | None:
        row = self.connection.execute(
            "SELECT * FROM maintenance_windows WHERE scenario_id=? AND (segment_id IS NULL OR segment_id IS ?) AND state IN ('scheduled','active') AND starts_at<=? AND ends_at>? ORDER BY segment_id DESC,id LIMIT 1",
            (scenario_id, segment_id, now, now),
        ).fetchone()
        if row is None:
            return None
        return dict(row)

    def _evaluate_phase(self, campaign_id: int, phase_id: int, actor: str, *, origin: str, override: str | None = None, override_reason: str | None = None) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            campaign = connection.execute("SELECT * FROM rollout_campaigns WHERE id=?", (campaign_id,)).fetchone()
            phase = connection.execute("SELECT * FROM rollout_phases WHERE id=?", (phase_id,)).fetchone()
            if campaign is None or phase is None or phase["campaign_id"] != campaign_id:
                raise NotFoundError("发布阶段不存在")
            if phase["state"] not in {"observing", "failed"}:
                raise ConflictError("当前阶段状态不能评估门禁")
            policy = connection.execute("SELECT * FROM policy_versions WHERE id=?", (campaign["policy_version_id"],)).fetchone()
            rules = json.loads(policy["rules_json"])
            metrics = self._phase_metrics(connection, campaign, phase, policy, rules, phase["activated_at"], now)
            if override == "pass":
                conclusion = "passed"
            elif override == "fail":
                conclusion = "failed"
            elif metrics["samples"] < phase["min_samples"]:
                conclusion = "insufficient_data"
            elif metrics["degraded_ratio"] > phase["max_degraded_ratio"] or metrics["critical_incidents"] > phase["max_critical_incidents"]:
                conclusion = "failed"
            else:
                conclusion = "passed"
            reason = override_reason or self._gate_reason(phase, metrics, conclusion)
            cursor = connection.execute(
                "INSERT INTO rollout_phase_evaluations(phase_id,campaign_id,policy_version_id,window_start,window_end,samples,degraded,degraded_ratio,critical_incidents,conclusion,origin,metrics_json,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (phase_id, campaign_id, policy["id"], phase["activated_at"], now, metrics["samples"], metrics["degraded"], metrics["degraded_ratio"], metrics["critical_incidents"], conclusion, origin, json.dumps(metrics, ensure_ascii=False, sort_keys=True), now),
            )
            evaluation_id = cursor.lastrowid
            event_detail = {
                "phase_id": phase_id,
                "sequence_no": phase["sequence_no"],
                "evaluation_id": evaluation_id,
                "conclusion": conclusion,
                "samples": metrics["samples"],
                "degraded_ratio": metrics["degraded_ratio"],
                "critical_incidents": metrics["critical_incidents"],
                "window_start": phase["activated_at"],
                "window_end": now,
            }
            if conclusion == "insufficient_data":
                connection.execute("UPDATE rollout_phases SET updated_at=? WHERE id=?", (now, phase_id))
                self._event(connection, "campaign", campaign_id, "phase_evaluated", actor, {**event_detail, "reason": reason}, now)
            elif conclusion == "passed":
                label = "passed" if origin == "auto" else "override_pass"
                connection.execute(
                    "UPDATE rollout_phases SET state='passed',decision=?,decided_at=?,decided_by=?,decision_reason=?,updated_at=? WHERE id=?",
                    (label, now, actor, reason, now, phase_id),
                )
                connection.execute("UPDATE rollout_phase_targets SET state='completed',completed_at=? WHERE phase_id=? AND state IN ('active','paused')", (now, phase_id))
                self._event(connection, "campaign", campaign_id, "phase_passed" if origin == "auto" else "phase_overridden", actor, {**event_detail, "decision": label, "reason": reason}, now)
                next_phase = connection.execute(
                    "SELECT * FROM rollout_phases WHERE campaign_id=? AND sequence_no>? ORDER BY sequence_no LIMIT 1",
                    (campaign_id, phase["sequence_no"]),
                ).fetchone()
                if next_phase is not None:
                    connection.execute("UPDATE rollout_campaigns SET state='running',updated_at=? WHERE id=?", (now, campaign_id))
                    connection.execute("UPDATE rollout_phases SET state='observing',activated_at=?,updated_at=? WHERE id=?", (now, now, next_phase["id"]))
                    connection.execute("UPDATE rollout_phase_targets SET state='active',activated_at=? WHERE phase_id=? AND state='pending'", (now, next_phase["id"]))
                    self._event(connection, "campaign", campaign_id, "phase_activated", actor, {"phase_id": next_phase["id"], "sequence_no": next_phase["sequence_no"], "name": next_phase["name"]}, now)
                else:
                    connection.execute("UPDATE rollout_campaigns SET state='completed',ends_at=COALESCE(ends_at,?),updated_at=? WHERE id=?", (now, now, campaign_id))
                    connection.execute(
                        "UPDATE rollout_phase_targets SET state='completed',completed_at=? WHERE state IN ('active','paused') AND phase_id IN (SELECT id FROM rollout_phases WHERE campaign_id=?)",
                        (now, campaign_id),
                    )
                    connection.execute("UPDATE rollout_targets SET state='completed',completed_at=?,version=version+1 WHERE campaign_id=? AND state IN ('active','paused')", (now, campaign_id))
                    self._event(connection, "campaign", campaign_id, "completed", actor, {"reason": "全部阶段门禁通过"}, now)
            else:
                label = "failed" if origin == "auto" else "override_fail"
                connection.execute(
                    "UPDATE rollout_phases SET state='failed',decision=?,decided_at=?,decided_by=?,decision_reason=?,updated_at=? WHERE id=?",
                    (label, now, actor, reason, now, phase_id),
                )
                rolled_back = connection.execute(
                    "SELECT t.id,t.segment_id,t.cohort_key,p.sequence_no,s.code AS segment_code FROM rollout_phase_targets t "
                    "JOIN rollout_phases p ON p.id=t.phase_id LEFT JOIN network_segments s ON s.id=t.segment_id "
                    "WHERE p.campaign_id=? AND t.state IN ('active','paused') ORDER BY t.id",
                    (campaign_id,),
                ).fetchall()
                connection.execute(
                    "UPDATE rollout_phase_targets SET state='rolled_back',rolled_back_at=? WHERE state IN ('active','paused') AND phase_id IN (SELECT id FROM rollout_phases WHERE campaign_id=?)",
                    (now, campaign_id),
                )
                connection.execute("UPDATE rollout_targets SET state='paused',version=version+1 WHERE campaign_id=? AND state='active'", (campaign_id,))
                connection.execute("UPDATE rollout_campaigns SET state='paused',updated_at=? WHERE id=?", (now, campaign_id))
                self._event(
                    connection, "campaign", campaign_id, "phase_failed" if origin == "auto" else "phase_overridden", actor,
                    {**event_detail, "decision": label, "reason": reason, "rolled_back": [{"sequence_no": row["sequence_no"], "segment_code": row["segment_code"], "cohort_key": row["cohort_key"]} for row in rolled_back]},
                    now,
                )
            return {
                "evaluation_id": evaluation_id,
                "campaign_id": campaign_id,
                "phase_id": phase_id,
                "sequence_no": phase["sequence_no"],
                "conclusion": conclusion,
                "origin": origin,
                "samples": metrics["samples"],
                "degraded": metrics["degraded"],
                "degraded_ratio": metrics["degraded_ratio"],
                "critical_incidents": metrics["critical_incidents"],
                "window_start": phase["activated_at"],
                "window_end": now,
            }

    def _phase_metrics(self, connection: sqlite3.Connection, campaign: sqlite3.Row, phase: sqlite3.Row, policy: sqlite3.Row, rules: dict[str, Any], window_start: str, window_end: str) -> dict[str, Any]:
        targets = connection.execute("SELECT * FROM rollout_phase_targets WHERE phase_id=? ORDER BY id", (phase["id"],)).fetchall()
        cohort_keys = [row[0] for row in connection.execute(
            "SELECT DISTINCT t.cohort_key FROM rollout_phase_targets t JOIN rollout_phases p ON p.id=t.phase_id WHERE p.campaign_id=? AND t.cohort_key<>'' ORDER BY t.cohort_key",
            (campaign["id"],),
        ).fetchall()]
        rows = connection.execute(
            "SELECT s.segment_id,s.subscriber_hash,s.latency_ms,s.packet_loss,s.downlink_mbps,s.uplink_mbps,"
            "a.latency_target_ms,a.packet_loss_target,a.min_downlink_mbps,a.min_uplink_mbps,a.default_priority "
            "FROM experience_samples s JOIN application_profiles a ON a.id=s.app_id "
            "WHERE s.scenario_id=? AND s.observed_at>=? AND s.observed_at<? ORDER BY s.id",
            (campaign["scenario_id"], window_start, window_end),
        ).fetchall()
        samples = 0
        degraded = 0
        critical = 0
        for row in rows:
            item = dict(row)
            cohort = cohort_for_subscriber(item["subscriber_hash"], cohort_keys)
            if not any(
                (target["segment_id"] is None or target["segment_id"] == item["segment_id"])
                and (not target["cohort_key"] or target["cohort_key"] == cohort)
                for target in targets
            ):
                continue
            samples += 1
            decision = judge_quality(item, item, rules)
            if decision.degraded:
                degraded += 1
                if decision.severity == "critical":
                    critical += 1
        return {
            "samples": samples,
            "degraded": degraded,
            "degraded_ratio": round(degraded / samples, 6) if samples else 0.0,
            "critical_incidents": critical,
            "window_start": window_start,
            "window_end": window_end,
            "thresholds": {
                "min_samples": phase["min_samples"],
                "max_degraded_ratio": phase["max_degraded_ratio"],
                "max_critical_incidents": phase["max_critical_incidents"],
            },
            "policy": {
                "id": policy["id"],
                "version_no": policy["version_no"],
                "rules_digest": policy["rules_digest"],
            },
        }

    @staticmethod
    def _gate_reason(phase: sqlite3.Row, metrics: dict[str, Any], conclusion: str) -> str:
        if conclusion == "insufficient_data":
            return f"样本量 {metrics['samples']} 低于下限 {phase['min_samples']}，继续观察"
        if conclusion == "failed":
            breaches = []
            if metrics["degraded_ratio"] > phase["max_degraded_ratio"]:
                breaches.append(f"质差率 {metrics['degraded_ratio']} 超过上限 {phase['max_degraded_ratio']}")
            if metrics["critical_incidents"] > phase["max_critical_incidents"]:
                breaches.append(f"严重事件 {metrics['critical_incidents']} 超过上限 {phase['max_critical_incidents']}")
            return "；".join(breaches)
        return f"样本量 {metrics['samples']} 达到下限 {phase['min_samples']}，质差率 {metrics['degraded_ratio']} 未超上限 {phase['max_degraded_ratio']}，严重事件 {metrics['critical_incidents']} 未超上限 {phase['max_critical_incidents']}"

    def _campaign(self, campaign_id: int) -> sqlite3.Row:
        row = self.connection.execute("SELECT * FROM rollout_campaigns WHERE id=?", (campaign_id,)).fetchone()
        if row is None:
            raise NotFoundError("发布活动不存在")
        return row

    @staticmethod
    def _current_phase(connection: sqlite3.Connection, campaign_id: int) -> sqlite3.Row | None:
        return connection.execute(
            "SELECT * FROM rollout_phases WHERE campaign_id=? AND state<>'passed' ORDER BY sequence_no LIMIT 1",
            (campaign_id,),
        ).fetchone()

    def _scenario(self, code: str) -> sqlite3.Row:
        row = self.repository.scenario_by_code(code)
        if row is None:
            raise NotFoundError("网络场景不存在")
        return row

    def _segment_ids(self, scenario_id: int, codes: list[str]) -> list[int]:
        result = []
        for code in codes:
            segment = self.repository.segment_by_code(scenario_id, code)
            if segment is None:
                raise NotFoundError(f"发布区段不存在：{code}")
            result.append(int(segment["id"]))
        return result

    @staticmethod
    def _required_time(value: str, label: str) -> str:
        try:
            return to_storage(from_storage(value))
        except (TypeError, ValueError) as exc:
            raise ValidationError(f"{label}格式不正确") from exc

    @classmethod
    def _optional_time(cls, value: str | None, label: str) -> str | None:
        return cls._required_time(value, label) if value else None

    @staticmethod
    def _evaluation(row: sqlite3.Row) -> dict[str, Any]:
        item = dict(row)
        item["metrics"] = json.loads(item.pop("metrics_json"))
        return item

    @staticmethod
    def _event(connection: sqlite3.Connection, resource_type: str, resource_id: int, event_type: str, actor: str, detail: dict[str, Any], now: str) -> None:
        connection.execute(
            "INSERT INTO operation_events(resource_type,resource_id,event_type,actor,detail_json,created_at) VALUES(?,?,?,?,?,?)",
            (resource_type, resource_id, event_type, actor, json.dumps(detail, ensure_ascii=False, sort_keys=True), now),
        )

    @staticmethod
    def _events(connection: sqlite3.Connection, resource_type: str, resource_id: int) -> list[dict[str, Any]]:
        rows = connection.execute(
            "SELECT * FROM operation_events WHERE resource_type=? AND resource_id=? ORDER BY id",
            (resource_type, resource_id),
        ).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["detail"] = json.loads(item.pop("detail_json"))
            result.append(item)
        return result
