from __future__ import annotations

"""
Cross-database reconciliation helpers.

Writes results into dq.reconciliation so the risk team sees parity alerts
alongside rule results. Everything is deliberately connection-agnostic so
the same helper works for RDS<->Redshift and RDS<->S3.

See docs/cross_database_dq.md for the full strategy.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

import psycopg

from mal_pipeline.common.db import execute, fetchall


@dataclass(frozen=True)
class ReconciliationResult:
    name: str
    left_source: str
    right_source: str
    metric_type: str              # "row_count" | "fingerprint" | "hash"
    left_metric: Optional[float]
    right_metric: Optional[float]
    left_fingerprint: Optional[str]
    right_fingerprint: Optional[str]
    discrepancy: Optional[float]
    tolerance: Optional[float]
    passed: bool
    window_start: Optional[datetime]
    window_end: Optional[datetime]
    details: Dict[str, Any]


# ---------------------------------------------------------------------------
# Writer
# ---------------------------------------------------------------------------


def write_reconciliation(
    conn: psycopg.Connection,
    *,
    dq_run_id: Optional[str],
    result: ReconciliationResult,
) -> int:
    rows = fetchall(
        conn,
        """
        insert into dq.reconciliation (
          dq_run_id, name, left_source, right_source, metric_type,
          left_metric, right_metric, left_fingerprint, right_fingerprint,
          discrepancy, tolerance, passed, window_start, window_end, details
        ) values (
          %(dq_run_id)s::uuid, %(name)s, %(left)s, %(right)s, %(mt)s,
          %(lm)s, %(rm)s, %(lf)s, %(rf)s,
          %(disc)s, %(tol)s, %(passed)s, %(ws)s, %(we)s, %(details)s::jsonb
        )
        returning reconciliation_id
        """,
        {
            "dq_run_id": dq_run_id,
            "name": result.name,
            "left": result.left_source,
            "right": result.right_source,
            "mt": result.metric_type,
            "lm": result.left_metric,
            "rm": result.right_metric,
            "lf": result.left_fingerprint,
            "rf": result.right_fingerprint,
            "disc": result.discrepancy,
            "tol": result.tolerance,
            "passed": result.passed,
            "ws": result.window_start,
            "we": result.window_end,
            "details": result.details,
        },
    )
    return int(rows[0][0])


# ---------------------------------------------------------------------------
# Row-count parity helpers
# ---------------------------------------------------------------------------


def _count_bronze_objects(conn: psycopg.Connection, source: str, window_start: datetime, window_end: datetime) -> int:
    rows = fetchall(
        conn,
        """
        select count(distinct (o.s3_bucket, o.s3_key))
        from audit.ingestion_object o
        join audit.ingestion_run r on r.run_id = o.run_id
        where r.source = %(src)s
          and o.created_at >= %(ws)s
          and o.created_at <  %(we)s
        """,
        {"src": source, "ws": window_start, "we": window_end},
    )
    return int(rows[0][0]) if rows and rows[0][0] is not None else 0


# The silver row-count query is source-specific because tables differ; we
# route on `source`. Keeps the call site simple.
_SILVER_COUNT_BY_SOURCE: Dict[str, str] = {
    "aecb": "select count(*) from silver.aecb_report where created_at >= %(ws)s and created_at < %(we)s",
    "aml":  "select count(*) from silver.aml_screen where created_at >= %(ws)s and created_at < %(we)s",
    "fraud":"select count(*) from silver.fraud_score where created_at >= %(ws)s and created_at < %(we)s",
    "internal_profile": "select count(*) from silver.customer_profile where updated_at >= %(ws)s and updated_at < %(we)s",
}


def bronze_to_silver_row_parity(
    conn: psycopg.Connection,
    *,
    source: str,
    window_start: datetime,
    window_end: datetime,
    tolerance_ratio: float = 0.01,
    dq_run_id: Optional[str] = None,
) -> ReconciliationResult:
    """
    Compare distinct Bronze objects vs Silver rows for `source` in a window.
    Tolerance is relative to the left side (in-flight rows OK).
    """
    if source not in _SILVER_COUNT_BY_SOURCE:
        raise ValueError(f"unsupported source for parity: {source}")

    left = _count_bronze_objects(conn, source, window_start, window_end)
    rows = fetchall(conn, _SILVER_COUNT_BY_SOURCE[source], {"ws": window_start, "we": window_end})
    right = int(rows[0][0]) if rows and rows[0][0] is not None else 0

    discrepancy = abs(left - right)
    allowed = max(0.0, left * tolerance_ratio)
    passed = discrepancy <= allowed

    result = ReconciliationResult(
        name=f"bronze_silver_{source}",
        left_source=f"bronze/{source}",
        right_source=f"silver.{source}",
        metric_type="row_count",
        left_metric=float(left),
        right_metric=float(right),
        left_fingerprint=None,
        right_fingerprint=None,
        discrepancy=float(discrepancy),
        tolerance=float(allowed),
        passed=passed,
        window_start=window_start,
        window_end=window_end,
        details={"tolerance_ratio": tolerance_ratio},
    )
    if dq_run_id is not None:
        write_reconciliation(conn, dq_run_id=dq_run_id, result=result)
    return result


# ---------------------------------------------------------------------------
# Fingerprint parity (Postgres <-> Redshift style)
# ---------------------------------------------------------------------------


FingerprintFetcher = Callable[[datetime, datetime], List[Tuple[str, str, int]]]
"""
A fetcher returns rows shaped like (bucket_key, fingerprint, count), where
`bucket_key` is a stable string identifying the bucket (e.g., an ISO date).
"""


def fingerprint_parity(
    conn: psycopg.Connection,
    *,
    name: str,
    left_source: str,
    right_source: str,
    left_fetch: FingerprintFetcher,
    right_fetch: FingerprintFetcher,
    window_start: datetime,
    window_end: datetime,
    dq_run_id: Optional[str] = None,
) -> List[ReconciliationResult]:
    """
    Compare fingerprints between two systems bucket-by-bucket. Returns one
    ReconciliationResult per bucket so the dashboard can show granular drift.

    Design notes:
    - Callers pass `left_fetch`/`right_fetch` that each issue the same SQL
      against their respective system and return the aggregated rows. This
      keeps this module independent of Redshift client choice.
    """
    left_rows = {k: (fp, n) for k, fp, n in left_fetch(window_start, window_end)}
    right_rows = {k: (fp, n) for k, fp, n in right_fetch(window_start, window_end)}
    all_keys = sorted(set(left_rows) | set(right_rows))

    results: List[ReconciliationResult] = []
    for key in all_keys:
        lfp, ln = left_rows.get(key, (None, 0))
        rfp, rn = right_rows.get(key, (None, 0))
        passed = (lfp is not None and lfp == rfp) and (ln == rn)
        result = ReconciliationResult(
            name=f"{name}[{key}]",
            left_source=left_source,
            right_source=right_source,
            metric_type="fingerprint",
            left_metric=float(ln),
            right_metric=float(rn),
            left_fingerprint=lfp,
            right_fingerprint=rfp,
            discrepancy=float(abs(ln - rn)),
            tolerance=0.0,
            passed=passed,
            window_start=window_start,
            window_end=window_end,
            details={"bucket": key},
        )
        results.append(result)
        if dq_run_id is not None:
            write_reconciliation(conn, dq_run_id=dq_run_id, result=result)
    return results


# ---------------------------------------------------------------------------
# Snapshot replay sample (delegates to decision.replay)
# ---------------------------------------------------------------------------


def snapshot_replay_sample(
    conn: psycopg.Connection,
    *,
    sample_pct: float = 0.01,
    window_hours: int = 24,
    dq_run_id: Optional[str] = None,
    replayer: Optional[Callable[[psycopg.Connection, str], Any]] = None,
) -> ReconciliationResult:
    """
    Replay a random sample of recent snapshots and verify hash integrity.

    `replayer` defaults to `mal_pipeline.decision.replay.replay_snapshot`;
    tests can pass a stub.
    """
    if replayer is None:
        from mal_pipeline.decision.replay import replay_snapshot
        replayer = lambda c, sid: replay_snapshot(c, snapshot_id=sid)

    window_end = datetime.now(timezone.utc)
    window_start = window_end - timedelta(hours=window_hours)

    rows = fetchall(
        conn,
        """
        select snapshot_id::text
        from gold.decision_input_snapshot
        where snapshot_ts between %(ws)s and %(we)s
        order by random()
        limit greatest(1, floor(%(pct)s * (
          select count(*) from gold.decision_input_snapshot
          where snapshot_ts between %(ws)s and %(we)s
        ))::int)
        """,
        {"ws": window_start, "we": window_end, "pct": sample_pct},
    )

    total = len(rows)
    mismatches: List[str] = []
    for (sid,) in rows:
        verdict = replayer(conn, sid)
        if not (getattr(verdict, "hash_matches", True) and getattr(verdict, "all_artifacts_ok", True)):
            mismatches.append(sid)

    passed = len(mismatches) == 0
    result = ReconciliationResult(
        name="snapshot_replay_sample",
        left_source="gold.decision_input_snapshot",
        right_source="s3://bronze/* (artifacts)",
        metric_type="hash",
        left_metric=float(total),
        right_metric=float(total - len(mismatches)),
        left_fingerprint=None,
        right_fingerprint=None,
        discrepancy=float(len(mismatches)),
        tolerance=0.0,
        passed=passed,
        window_start=window_start,
        window_end=window_end,
        details={"sampled": total, "mismatches": mismatches[:20]},
    )
    if dq_run_id is not None:
        write_reconciliation(conn, dq_run_id=dq_run_id, result=result)
    return result
