from __future__ import annotations

"""
DQ runner.

Supports two rule shapes:
  - sql_check : returns violating rows; runner compares COUNT to threshold.
  - sql_metric: returns a single scalar number; runner compares to threshold.

Threshold:
  { op: eq|ne|lt|lte|gt|gte, value: <number> }

Severity:
  MUST_PASS : any failure flips the run's aggregate `must_pass` flag to False.
  WARN      : counted and dashboarded; does not affect `must_pass`.

The runner is deliberately small. It writes one dq.run + one dq.result per
rule + writes/updates dq.rule metadata from YAML so the DB reflects what's
currently shipped (source of truth = YAML).
"""

import os
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional, Tuple

import yaml

import psycopg

from mal_pipeline.common.db import execute, fetchall


VALID_OPS = {"eq", "ne", "lt", "lte", "gt", "gte"}
VALID_DIMENSIONS = {
    "completeness",
    "uniqueness",
    "validity",
    "consistency",
    "timeliness",
    "referential",
}
VALID_SEVERITIES = {"MUST_PASS", "WARN"}


@dataclass(frozen=True)
class Threshold:
    op: str
    value: float


@dataclass(frozen=True)
class Rule:
    rule_id: str
    description: str
    severity: str
    layer: str
    dimension: Optional[str]
    owner: Optional[str]
    scope: Dict[str, Any] = field(default_factory=dict)
    sql_check: Optional[str] = None
    sql_metric: Optional[str] = None
    threshold: Threshold = field(default_factory=lambda: Threshold(op="lte", value=0))

    @property
    def metric_type(self) -> str:
        return "violation_count" if self.sql_check is not None else "ratio"


def _validate(rule: Rule) -> None:
    if rule.severity not in VALID_SEVERITIES:
        raise ValueError(f"{rule.rule_id}: invalid severity {rule.severity!r}")
    if rule.dimension is not None and rule.dimension not in VALID_DIMENSIONS:
        raise ValueError(f"{rule.rule_id}: invalid dimension {rule.dimension!r}")
    if rule.threshold.op not in VALID_OPS:
        raise ValueError(f"{rule.rule_id}: invalid threshold op {rule.threshold.op!r}")
    if bool(rule.sql_check) == bool(rule.sql_metric):
        raise ValueError(f"{rule.rule_id}: must set exactly one of sql_check or sql_metric")


def load_rules(path: str) -> List[Rule]:
    with open(path, "r", encoding="utf-8") as f:
        doc = yaml.safe_load(f) or {}
    rules: List[Rule] = []
    for r in doc.get("rules", []):
        th = r.get("threshold") or {}
        rule = Rule(
            rule_id=r["rule_id"],
            description=r["description"],
            severity=r["severity"],
            layer=r["layer"],
            dimension=r.get("dimension"),
            owner=r.get("owner"),
            scope=r.get("scope") or {},
            sql_check=r.get("sql_check"),
            sql_metric=r.get("sql_metric"),
            threshold=Threshold(op=th.get("op", "lte"), value=float(th.get("value", 0))),
        )
        _validate(rule)
        rules.append(rule)
    return rules


def upsert_rules(conn: psycopg.Connection, rules: List[Rule]) -> None:
    """
    Keep dq.rule aligned with the YAML. The table has the richer columns
    from sql/08_dq_framework_upgrade.sql; we populate them when present.
    """
    for r in rules:
        execute(
            conn,
            """
            insert into dq.rule (
              rule_id, description, severity, layer, sql_check,
              dimension, metric_type, threshold_op, threshold_value, scope, owner, enabled
            ) values (
              %(rule_id)s, %(description)s, %(severity)s::dq.severity, %(layer)s, %(sql_check)s,
              %(dimension)s::dq.dimension, %(metric_type)s::dq.metric_type,
              %(threshold_op)s::dq.threshold_op, %(threshold_value)s, %(scope)s::jsonb, %(owner)s, true
            )
            on conflict (rule_id) do update set
              description = excluded.description,
              severity = excluded.severity,
              layer = excluded.layer,
              sql_check = excluded.sql_check,
              dimension = excluded.dimension,
              metric_type = excluded.metric_type,
              threshold_op = excluded.threshold_op,
              threshold_value = excluded.threshold_value,
              scope = excluded.scope,
              owner = excluded.owner,
              enabled = true
            """,
            {
                "rule_id": r.rule_id,
                "description": r.description,
                "severity": r.severity,
                "layer": r.layer,
                "sql_check": r.sql_check or r.sql_metric or "",
                "dimension": r.dimension,
                "metric_type": r.metric_type,
                "threshold_op": r.threshold.op,
                "threshold_value": r.threshold.value,
                "scope": r.scope,
                "owner": r.owner,
            },
        )


def compare(value: float, op: str, threshold: float) -> bool:
    if op == "eq":  return value == threshold
    if op == "ne":  return value != threshold
    if op == "lt":  return value <  threshold
    if op == "lte": return value <= threshold
    if op == "gt":  return value >  threshold
    if op == "gte": return value >= threshold
    raise ValueError(f"unknown op {op!r}")


def evaluate_rule(conn: psycopg.Connection, rule: Rule) -> Tuple[bool, float, List[Any]]:
    """
    Run a single rule. Returns (passed, metric_value, sample_violations).
    sample_violations is empty for sql_metric rules.
    """
    if rule.sql_check:
        rows = fetchall(conn, rule.sql_check)
        metric = float(len(rows))
        sample = list(rows[:10])
    else:
        rows = fetchall(conn, rule.sql_metric or "")
        if not rows:
            metric = 0.0
        else:
            first = rows[0][0]
            metric = float(first) if first is not None else 0.0
        sample = []

    passed = compare(metric, rule.threshold.op, rule.threshold.value)
    return passed, metric, sample


def run_dq(
    conn: psycopg.Connection,
    rules: List[Rule],
    scope: Optional[Dict[str, Any]] = None,
    *,
    publish_layer: Optional[str] = None,
    sns_topic_arn: Optional[str] = None,
    sns_notify: Literal["on_fail", "always"] = "on_fail",
    sns_client: Optional[Any] = None,
) -> Tuple[str, bool]:
    """
    Execute rules and persist results. If `publish_layer` is provided and the
    run finishes with every MUST_PASS rule passing, the publication marker
    for that layer is advanced in the same transaction path.

    Returns (dq_run_id, aggregate_must_pass).

    Optional alerting: set ``sns_topic_arn`` or env ``MAL_PIPELINE_ALERT_SNS_TOPIC_ARN``.
    When ``sns_notify`` is ``on_fail`` (default), SNS fires only if a MUST_PASS rule fails.
    """
    dq_run_id = str(uuid.uuid4())
    execute(
        conn,
        "insert into dq.run (dq_run_id, scope, status) values (%(id)s::uuid, %(scope)s::jsonb, 'succeeded')",
        {"id": dq_run_id, "scope": scope or {}},
    )

    aggregate_must_pass = True
    failed_must_pass: List[str] = []
    for rule in rules:
        passed, metric, sample = evaluate_rule(conn, rule)
        if rule.severity == "MUST_PASS" and not passed:
            aggregate_must_pass = False
            failed_must_pass.append(rule.rule_id)

        execute(
            conn,
            """
            insert into dq.result (
              dq_run_id, rule_id, passed, violation_count, sample_violations,
              metric_value, threshold_op, threshold_value
            ) values (
              %(dq_run_id)s::uuid, %(rule_id)s, %(passed)s, %(violation_count)s, %(sample)s::jsonb,
              %(metric)s, %(op)s::dq.threshold_op, %(tv)s
            )
            """,
            {
                "dq_run_id": dq_run_id,
                "rule_id": rule.rule_id,
                "passed": passed,
                "violation_count": int(metric) if rule.sql_check else 0,
                "sample": sample,
                "metric": metric,
                "op": rule.threshold.op,
                "tv": rule.threshold.value,
            },
        )

    if publish_layer is not None:
        # Import locally to avoid a hard dependency cycle if callers don't
        # use publication.
        from mal_pipeline.dq.publication import publish_if_green

        publish_if_green(
            conn,
            layer=publish_layer,
            dq_run_id=dq_run_id,
            must_pass=aggregate_must_pass,
            updated_by="dq-runner",
            notes=f"scope={scope or {}}",
        )

    topic = sns_topic_arn or os.environ.get("MAL_PIPELINE_ALERT_SNS_TOPIC_ARN")
    if topic:
        should_send = sns_notify == "always" or (
            sns_notify == "on_fail" and not aggregate_must_pass
        )
        if should_send:
            from mal_pipeline.alerting.sns import format_dq_run_alert, publish_sns_message

            subject, body = format_dq_run_alert(
                dq_run_id=dq_run_id,
                must_pass=aggregate_must_pass,
                failed_must_pass_rule_ids=failed_must_pass,
                scope=scope,
            )
            publish_sns_message(
                topic_arn=topic,
                subject=subject,
                message=body,
                sns_client=sns_client,
            )

    return dq_run_id, aggregate_must_pass
