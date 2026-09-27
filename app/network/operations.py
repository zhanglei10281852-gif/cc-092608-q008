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


def cohort_for(subscriber_hash: str, cohort_keys: list[str]) -> str:
    """按用户标识哈希确定性归入某个分群，分群清单即灰度分桶。"""
    if not cohort_keys:
        return ""
    digest = int.from_bytes(hashlib.sha256(subscriber_hash.encode("utf-8")).digest()[:8], "big")
    return cohort_keys[digest % len(cohort_keys)]


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
        failure_action = payload.get("failure_action") or "pause"
        if failure_action not in {"pause", "rollback"}:
            raise ValidationError("门禁失败处理方式必须是 pause 或 rollback")
        stages = [self._stage_definition(scenario["id"], item) for item in payload.get("stages") or []]
        segment_ids = self._segment_ids(scenario["id"], payload.get("segment_codes", []))
        cohorts = payload.get("cohort_keys") or [""]
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            try:
                cursor = connection.execute(
                    "INSERT INTO rollout_campaigns(scenario_id,code,name,strategy,target_percentage,policy_version_id,failure_action,starts_at,ends_at,created_by,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                    (scenario["id"], payload["code"], payload["name"], payload["strategy"], payload["target_percentage"], policy["id"], failure_action, starts_at, ends_at, payload["actor"], now, now),
                )
            except sqlite3.IntegrityError as exc:
                raise ConflictError("发布活动编码已存在") from exc
            campaign_id = cursor.lastrowid
            if stages:
                for stage_no, stage in enumerate(stages, start=1):
                    stage_cursor = connection.execute(
                        "INSERT INTO rollout_stages(campaign_id,stage_no,name,min_observation_seconds,min_samples,max_degraded_ratio,max_critical_events,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)",
                        (campaign_id, stage_no, stage["name"], stage["min_observation_seconds"], stage["min_samples"], stage["max_degraded_ratio"], stage["max_critical_events"], now, now),
                    )
                    for segment_id in stage["segment_ids"] or [None]:
                        for cohort in stage["cohort_keys"] or [""]:
                            connection.execute(
                                "INSERT INTO rollout_stage_targets(stage_id,campaign_id,segment_id,cohort_key) VALUES(?,?,?,?)",
                                (stage_cursor.lastrowid, campaign_id, segment_id, cohort),
                            )
                self._event(connection, "campaign", campaign_id, "created", payload["actor"], {"stages": len(stages), "failure_action": failure_action}, now)
            else:
                targets = segment_ids or [None]
                for segment_id in targets:
                    for cohort in cohorts:
                        connection.execute(
                            "INSERT INTO rollout_targets(campaign_id,segment_id,cohort_key) VALUES(?,?,?)",
                            (campaign_id, segment_id, cohort),
                        )
                self._event(connection, "campaign", campaign_id, "created", payload["actor"], {"targets": len(targets) * len(cohorts)}, now)
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
        stages = connection.execute("SELECT * FROM rollout_stages WHERE campaign_id=? ORDER BY stage_no", (campaign_id,)).fetchall()
        result["gated"] = bool(stages)
        result["stages"] = []
        for stage in stages:
            item = dict(stage)
            item["targets"] = [dict(row) for row in connection.execute(
                "SELECT t.*,s.code AS segment_code,s.name AS segment_name FROM rollout_stage_targets t LEFT JOIN network_segments s ON s.id=t.segment_id WHERE t.stage_id=? ORDER BY t.id",
                (stage["id"],),
            ).fetchall()]
            item["evaluations"] = [self._evaluation(row) for row in connection.execute(
                "SELECT * FROM rollout_stage_evaluations WHERE stage_id=? ORDER BY id",
                (stage["id"],),
            ).fetchall()]
            result["stages"].append(item)
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
        stages = self._stages(self.connection, campaign_id)
        if stages and any(stage["state"] == "failed" for stage in stages):
            raise ConflictError("存在门禁失败的阶段，请先人工覆盖后再启动")
        with transaction(immediate=True) as connection:
            connection.execute("UPDATE rollout_campaigns SET state='running',updated_at=? WHERE id=?", (now, campaign_id))
            self._event(connection, "campaign", campaign_id, "started", actor, {"reason": reason}, now)
            if stages:
                observing = next((stage for stage in stages if stage["state"] == "observing"), None)
                if observing is None:
                    first = next((stage for stage in stages if stage["state"] == "pending"), None)
                    if first is None:
                        raise ConflictError("发布活动没有可以启动的阶段")
                    self._begin_stage(connection, campaign_id, first, now, actor)
                connection.execute("UPDATE rollout_stage_targets SET state='active',version=version+1 WHERE campaign_id=? AND state='paused'", (campaign_id,))
            else:
                connection.execute("UPDATE rollout_targets SET state='active',activated_at=COALESCE(activated_at,?),version=version+1 WHERE campaign_id=? AND state IN ('pending','paused')", (now, campaign_id))
            return self.campaign_detail(campaign_id, connection)

    def pause_campaign(self, campaign_id: int, actor: str, reason: str) -> dict[str, Any]:
        campaign = self._campaign(campaign_id)
        if campaign["state"] != "running":
            raise ConflictError("只有运行中的发布活动可以暂停")
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            connection.execute("UPDATE rollout_campaigns SET state='paused',updated_at=? WHERE id=?", (now, campaign_id))
            connection.execute("UPDATE rollout_targets SET state='paused',version=version+1 WHERE campaign_id=? AND state='active'", (campaign_id,))
            connection.execute("UPDATE rollout_stage_targets SET state='paused',version=version+1 WHERE campaign_id=? AND state='active'", (campaign_id,))
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
            connection.execute("UPDATE rollout_stage_targets SET state='completed',completed_at=?,version=version+1 WHERE campaign_id=? AND state IN ('active','paused')", (now, campaign_id))
            connection.execute("UPDATE rollout_stages SET state='skipped',updated_at=? WHERE campaign_id=? AND state IN ('pending','observing')", (now, campaign_id))
            self._event(connection, "campaign", campaign_id, "completed", actor, {"reason": reason}, now)
            return self.campaign_detail(campaign_id, connection)

    def advance_campaigns(self, actor: str = "gate-scheduler") -> dict[str, Any]:
        """推进所有运行中的门禁活动：观察期满即按冻结策略评估并应用结论。"""
        now_value = self.clock.now()
        now = to_storage(now_value)
        evaluations: list[dict[str, Any]] = []
        waiting: list[int] = []
        rows = self.connection.execute(
            "SELECT s.campaign_id AS id FROM rollout_stages s JOIN rollout_campaigns c ON c.id=s.campaign_id WHERE c.state='running' AND s.state='observing' GROUP BY s.campaign_id ORDER BY s.campaign_id",
        ).fetchall()
        for row in rows:
            campaign_id = int(row["id"])
            stage = self.connection.execute(
                "SELECT * FROM rollout_stages WHERE campaign_id=? AND state='observing' ORDER BY stage_no LIMIT 1",
                (campaign_id,),
            ).fetchone()
            mature_at = from_storage(stage["observation_started_at"]) + timedelta(seconds=int(stage["min_observation_seconds"]))
            if now_value < mature_at:
                waiting.append(campaign_id)
                continue
            with transaction(immediate=True) as connection:
                campaign = connection.execute("SELECT * FROM rollout_campaigns WHERE id=?", (campaign_id,)).fetchone()
                stage = connection.execute("SELECT * FROM rollout_stages WHERE id=? AND state='observing'", (stage["id"],)).fetchone()
                if campaign is None or campaign["state"] != "running" or stage is None:
                    continue
                evaluation = self._evaluate_stage(connection, campaign, stage, now, actor)
                self._apply_outcome(connection, campaign, stage, evaluation, now, actor)
                evaluations.append(evaluation)
        return {"evaluations": evaluations, "waiting": waiting}

    def override_stage(self, campaign_id: int, stage_id: int, actor: str, reason: str, decision: str) -> dict[str, Any]:
        """人工覆盖阶段门禁结论，必须记录操作者与理由。"""
        campaign = self._campaign(campaign_id)
        if campaign["state"] not in {"running", "paused"}:
            raise ConflictError("当前发布活动状态不能人工覆盖")
        if decision not in {"passed", "failed"}:
            raise ValidationError("人工覆盖结论只能是 passed 或 failed")
        stage = self._stage(campaign_id, stage_id)
        if decision == "passed" and stage["state"] not in {"observing", "failed"}:
            raise ConflictError("只有观察中或门禁失败的阶段可以人工放行")
        if decision == "failed" and stage["state"] != "observing":
            raise ConflictError("只有观察中的阶段可以人工判定失败")
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            evaluation = self._evaluate_stage(connection, campaign, stage, now, actor, overridden=True, override_actor=actor, override_reason=reason, forced=decision)
            self._event(
                connection,
                "campaign",
                campaign_id,
                "stage_overridden",
                actor,
                {"stage_id": stage_id, "stage_no": stage["stage_no"], "decision": decision, "reason": reason, "evaluation_id": evaluation["id"]},
                now,
            )
            if decision == "passed":
                connection.execute("UPDATE rollout_campaigns SET state='running',updated_at=? WHERE id=?", (now, campaign_id))
            self._apply_outcome(connection, campaign, stage, evaluation, now, actor)
            return self.campaign_detail(campaign_id, connection)

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

    def _evaluate_stage(
        self,
        connection: sqlite3.Connection,
        campaign: sqlite3.Row,
        stage: sqlite3.Row,
        now: str,
        actor: str,
        *,
        overridden: bool = False,
        override_actor: str | None = None,
        override_reason: str | None = None,
        forced: str | None = None,
    ) -> dict[str, Any]:
        """按活动冻结的策略版本重算观察窗口内的样本指标并落库为不可变评估记录。"""
        policy = connection.execute("SELECT * FROM policy_versions WHERE id=?", (campaign["policy_version_id"],)).fetchone()
        rules = json.loads(policy["rules_json"])
        targets = connection.execute("SELECT * FROM rollout_stage_targets WHERE stage_id=? ORDER BY id", (stage["id"],)).fetchall()
        segment_ids = sorted({int(target["segment_id"]) for target in targets if target["segment_id"] is not None})
        all_segments = any(target["segment_id"] is None for target in targets)
        cohort_keys: list[str] = []
        for target in targets:
            if target["cohort_key"] and target["cohort_key"] not in cohort_keys:
                cohort_keys.append(target["cohort_key"])
        universe = self._campaign_cohorts(connection, campaign["id"]) if cohort_keys else []
        sql = "SELECT * FROM experience_samples WHERE scenario_id=? AND observed_at>=? AND observed_at<?"
        params: list[Any] = [campaign["scenario_id"], stage["observation_started_at"], now]
        if not all_segments and segment_ids:
            sql += " AND segment_id IN (" + ",".join("?" * len(segment_ids)) + ")"
            params.extend(segment_ids)
        sql += " ORDER BY id"
        rows = connection.execute(sql, params).fetchall()
        profiles: dict[int, dict[str, Any]] = {}
        samples = degraded = minor = major = critical = 0
        for row in rows:
            if cohort_keys and cohort_for(row["subscriber_hash"], universe) not in cohort_keys:
                continue
            profile = profiles.get(row["app_id"])
            if profile is None:
                profile = dict(connection.execute("SELECT * FROM application_profiles WHERE id=?", (row["app_id"],)).fetchone())
                profiles[row["app_id"]] = profile
            decision = judge_quality(dict(row), profile, rules)
            samples += 1
            if decision.degraded:
                degraded += 1
                if decision.severity == "critical":
                    critical += 1
                elif decision.severity == "major":
                    major += 1
                else:
                    minor += 1
        ratio = round(degraded / samples, 6) if samples else 0.0
        reasons: list[str] = []
        if samples < int(stage["min_samples"]):
            conclusion = "insufficient_data"
            reasons.append(f"样本量 {samples} 低于下限 {stage['min_samples']}")
        else:
            if ratio > float(stage["max_degraded_ratio"]):
                reasons.append(f"质差率 {ratio} 超过上限 {stage['max_degraded_ratio']}")
            if critical > int(stage["max_critical_events"]):
                reasons.append(f"严重事件 {critical} 起超过上限 {stage['max_critical_events']}")
            conclusion = "failed" if reasons else "passed"
        if forced is not None:
            conclusion = forced
        thresholds = {
            "min_observation_seconds": int(stage["min_observation_seconds"]),
            "min_samples": int(stage["min_samples"]),
            "max_degraded_ratio": float(stage["max_degraded_ratio"]),
            "max_critical_events": int(stage["max_critical_events"]),
        }
        cursor = connection.execute(
            "INSERT INTO rollout_stage_evaluations(stage_id,campaign_id,policy_version_id,rules_digest,window_start,window_end,samples,degraded,degraded_ratio,minor_events,major_events,critical_events,thresholds_json,reasons_json,conclusion,actor,overridden,override_actor,override_reason,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                stage["id"],
                campaign["id"],
                policy["id"],
                policy["rules_digest"],
                stage["observation_started_at"],
                now,
                samples,
                degraded,
                ratio,
                minor,
                major,
                critical,
                json.dumps(thresholds, ensure_ascii=False, sort_keys=True),
                json.dumps(reasons, ensure_ascii=False),
                conclusion,
                actor,
                1 if overridden else 0,
                override_actor,
                override_reason,
                now,
            ),
        )
        row = connection.execute("SELECT * FROM rollout_stage_evaluations WHERE id=?", (cursor.lastrowid,)).fetchone()
        return self._evaluation(row)

    def _apply_outcome(self, connection: sqlite3.Connection, campaign: sqlite3.Row, stage: sqlite3.Row, evaluation: dict[str, Any], now: str, actor: str) -> None:
        conclusion = evaluation["conclusion"]
        if conclusion == "insufficient_data":
            return
        if conclusion == "passed":
            connection.execute("UPDATE rollout_stages SET state='passed',conclusion='passed',evaluated_at=?,updated_at=? WHERE id=?", (now, now, stage["id"]))
            connection.execute("UPDATE rollout_stage_targets SET state='active',version=version+1 WHERE campaign_id=? AND state='paused'", (campaign["id"],))
            self._event(
                connection,
                "campaign",
                campaign["id"],
                "stage_passed",
                actor,
                {"stage_id": stage["id"], "stage_no": stage["stage_no"], "evaluation_id": evaluation["id"], "samples": evaluation["samples"], "degraded_ratio": evaluation["degraded_ratio"], "critical_events": evaluation["critical_events"]},
                now,
            )
            next_stage = connection.execute("SELECT * FROM rollout_stages WHERE campaign_id=? AND state='pending' ORDER BY stage_no LIMIT 1", (campaign["id"],)).fetchone()
            if next_stage is not None:
                self._begin_stage(connection, campaign["id"], next_stage, now, actor)
            else:
                connection.execute("UPDATE rollout_campaigns SET state='completed',ends_at=COALESCE(ends_at,?),updated_at=? WHERE id=?", (now, now, campaign["id"]))
                connection.execute("UPDATE rollout_stage_targets SET state='completed',completed_at=?,version=version+1 WHERE campaign_id=? AND state IN ('active','paused')", (now, campaign["id"]))
                self._event(connection, "campaign", campaign["id"], "completed", actor, {"reason": "全部阶段门禁通过"}, now)
            return
        connection.execute("UPDATE rollout_stages SET state='failed',conclusion='failed',evaluated_at=?,updated_at=? WHERE id=?", (now, now, stage["id"]))
        rollback_scope: list[dict[str, Any]] = []
        if campaign["failure_action"] == "rollback":
            rows = connection.execute(
                "SELECT t.id,t.cohort_key,s.stage_no,g.code AS segment_code FROM rollout_stage_targets t JOIN rollout_stages s ON s.id=t.stage_id LEFT JOIN network_segments g ON g.id=t.segment_id WHERE t.campaign_id=? AND t.state='active' ORDER BY t.id",
                (campaign["id"],),
            ).fetchall()
            connection.execute("UPDATE rollout_stage_targets SET state='rolled_back',rolled_back_at=?,version=version+1 WHERE campaign_id=? AND state='active'", (now, campaign["id"]))
            rollback_scope = [{"target_id": row["id"], "stage_no": row["stage_no"], "segment_code": row["segment_code"], "cohort_key": row["cohort_key"]} for row in rows]
        else:
            connection.execute("UPDATE rollout_stage_targets SET state='paused',version=version+1 WHERE campaign_id=? AND state='active'", (campaign["id"],))
        connection.execute("UPDATE rollout_campaigns SET state='paused',updated_at=? WHERE id=?", (now, campaign["id"]))
        self._event(
            connection,
            "campaign",
            campaign["id"],
            "stage_failed",
            actor,
            {"stage_id": stage["id"], "stage_no": stage["stage_no"], "evaluation_id": evaluation["id"], "reasons": evaluation["reasons"], "failure_action": campaign["failure_action"], "rollback_scope": rollback_scope},
            now,
        )
        self._event(connection, "campaign", campaign["id"], "paused", actor, {"reason": "门禁失败自动暂停"}, now)

    def _begin_stage(self, connection: sqlite3.Connection, campaign_id: int, stage: sqlite3.Row, now: str, actor: str) -> None:
        connection.execute("UPDATE rollout_stages SET state='observing',observation_started_at=?,updated_at=? WHERE id=?", (now, now, stage["id"]))
        connection.execute("UPDATE rollout_stage_targets SET state='active',activated_at=COALESCE(activated_at,?),version=version+1 WHERE stage_id=? AND state IN ('pending','paused')", (now, stage["id"]))
        self._event(connection, "campaign", campaign_id, "stage_started", actor, {"stage_id": stage["id"], "stage_no": stage["stage_no"], "name": stage["name"]}, now)

    def _campaign_cohorts(self, connection: sqlite3.Connection, campaign_id: int) -> list[str]:
        rows = connection.execute(
            "SELECT t.cohort_key FROM rollout_stage_targets t JOIN rollout_stages s ON s.id=t.stage_id WHERE t.campaign_id=? AND t.cohort_key<>'' ORDER BY s.stage_no,t.id",
            (campaign_id,),
        ).fetchall()
        universe: list[str] = []
        for row in rows:
            if row["cohort_key"] not in universe:
                universe.append(row["cohort_key"])
        return universe

    def _stage_definition(self, scenario_id: int, payload: dict[str, Any]) -> dict[str, Any]:
        name = str(payload.get("name") or "").strip()
        if len(name) < 2:
            raise ValidationError("阶段名称不能为空")
        segment_ids = self._segment_ids(scenario_id, payload.get("segment_codes") or [])
        cohort_keys = payload.get("cohort_keys") or []
        if not segment_ids and not cohort_keys:
            raise ValidationError("每个阶段必须定义目标区段或用户分群")
        if len(cohort_keys) != len(set(cohort_keys)):
            raise ValidationError("阶段内用户分群不能重复")
        try:
            min_observation = int(payload.get("min_observation_seconds"))
            min_samples = int(payload.get("min_samples"))
            max_ratio = float(payload.get("max_degraded_ratio"))
            max_critical = int(payload.get("max_critical_events"))
        except (TypeError, ValueError) as exc:
            raise ValidationError("阶段门禁阈值格式不正确") from exc
        if min_observation <= 0:
            raise ValidationError("阶段最短观察期必须大于零")
        if min_samples <= 0:
            raise ValidationError("阶段样本量下限必须大于零")
        if not 0 <= max_ratio <= 1:
            raise ValidationError("阶段质差率上限必须在 0 到 1 之间")
        if max_critical < 0:
            raise ValidationError("阶段严重事件上限必须是非负整数")
        return {
            "name": name,
            "segment_ids": segment_ids,
            "cohort_keys": list(cohort_keys),
            "min_observation_seconds": min_observation,
            "min_samples": min_samples,
            "max_degraded_ratio": max_ratio,
            "max_critical_events": max_critical,
        }

    def _stages(self, connection: sqlite3.Connection, campaign_id: int) -> list[sqlite3.Row]:
        return connection.execute("SELECT * FROM rollout_stages WHERE campaign_id=? ORDER BY stage_no", (campaign_id,)).fetchall()

    def _stage(self, campaign_id: int, stage_id: int) -> sqlite3.Row:
        row = self.connection.execute("SELECT * FROM rollout_stages WHERE id=? AND campaign_id=?", (stage_id, campaign_id)).fetchone()
        if row is None:
            raise NotFoundError("发布阶段不存在")
        return row

    @staticmethod
    def _evaluation(row: sqlite3.Row) -> dict[str, Any]:
        result = dict(row)
        result["thresholds"] = json.loads(result.pop("thresholds_json"))
        result["reasons"] = json.loads(result.pop("reasons_json"))
        result["overridden"] = bool(result["overridden"])
        return result

    def _campaign(self, campaign_id: int) -> sqlite3.Row:
        row = self.connection.execute("SELECT * FROM rollout_campaigns WHERE id=?", (campaign_id,)).fetchone()
        if row is None:
            raise NotFoundError("发布活动不存在")
        return row

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
