from __future__ import annotations

"""
Anomaly detection (statistical baseline tier).

Two detectors are provided for launch:

- z_score: classic (x - mean) / stddev over a trailing window.
- mad    : robust (x - median) / (1.4826 * MAD) over a trailing window.

Both are SQL-driven: the caller supplies a metric SQL that returns rows of
(timestamp, numeric). The detector computes baseline statistics in Python
from those rows (small N, typically 28-60 points) to keep everything
debuggable and easy to unit-test.

Results are written to dq.anomaly; a single run can emit many anomalies.
See docs/anomaly_detection.md.
"""

from dataclasses import dataclass, field
from datetime import datetime
from statistics import mean, median, pstdev
from typing import Any, Dict, List, Optional, Sequence, Tuple

import yaml

import psycopg

from mal_pipeline.common.db import execute, fetchall


@dataclass(frozen=True)
class AnomalyResult:
    detector_id: str
    method: str
    severity: str                     # info | warn | page
    metric_name: str
    observed_value: Optional[float]
    baseline_value: Optional[float]
    deviation: Optional[float]
    z_score: Optional[float]
    window_start: Optional[datetime]
    window_end: Optional[datetime]
    details: Dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Statistical helpers (pure)
# ---------------------------------------------------------------------------


def _mean_std(xs: Sequence[float]) -> Tuple[float, float]:
    if len(xs) < 2:
        return (xs[0] if xs else 0.0, 0.0)
    return mean(xs), pstdev(xs)


def _mad(xs: Sequence[float]) -> Tuple[float, float]:
    if not xs:
        return (0.0, 0.0)
    m = median(xs)
    deviations = [abs(x - m) for x in xs]
    return m, median(deviations) if deviations else 0.0


def z_score(x: float, baseline: Sequence[float]) -> Tuple[float, float, float]:
    """
    Returns (mu, sigma, z). If sigma == 0 or len(baseline) < 2, returns z=0.
    """
    mu, sigma = _mean_std(list(baseline))
    z = 0.0 if sigma == 0 or len(baseline) < 2 else (x - mu) / sigma
    return mu, sigma, z


def mad_score(x: float, baseline: Sequence[float]) -> Tuple[float, float, float]:
    """
    Returns (median, mad, z-like robust score). Scaling constant 1.4826
    makes MAD a consistent estimator of stddev for Gaussian data.
    """
    med, mad = _mad(list(baseline))
    denom = 1.4826 * mad
    robust_z = 0.0 if denom == 0 or len(baseline) < 2 else (x - med) / denom
    return med, mad, robust_z


# ---------------------------------------------------------------------------
# Detector configuration
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Detector:
    detector_id: str
    method: str                       # z_score | mad
    metric_sql: str                   # returns (ts, numeric) rows, ordered by ts
    baseline_window: int              # number of trailing samples used as baseline
    threshold: float                  # sigma or robust-z threshold
    severity_at_threshold: str = "warn"
    severity_at_two_times_threshold: str = "page"
    metric_name: str = "metric"


def _severity_for_score(abs_z: float, threshold: float, detector: Detector) -> str:
    if abs_z >= 2 * threshold:
        return detector.severity_at_two_times_threshold
    if abs_z >= threshold:
        return detector.severity_at_threshold
    return "info"


def evaluate_detector(
    conn: psycopg.Connection, detector: Detector
) -> Optional[AnomalyResult]:
    """
    Pull the time-series, split into (baseline | current), score, and return
    an AnomalyResult (which the caller may or may not persist). Returns None
    if there isn't enough data to evaluate.
    """
    rows = fetchall(conn, detector.metric_sql)
    if not rows:
        return None

    # rows look like [(ts, value), ...] sorted ascending
    series: List[Tuple[Optional[datetime], float]] = [
        (r[0], float(r[1]) if r[1] is not None else 0.0) for r in rows
    ]

    if len(series) < detector.baseline_window + 1:
        # Cold start: not enough samples yet; emit info-only anomaly.
        return AnomalyResult(
            detector_id=detector.detector_id,
            method=detector.method,
            severity="info",
            metric_name=detector.metric_name,
            observed_value=None,
            baseline_value=None,
            deviation=None,
            z_score=None,
            window_start=series[0][0],
            window_end=series[-1][0],
            details={"reason": "cold_start", "samples": len(series)},
        )

    *baseline_rows, current_row = series[-(detector.baseline_window + 1):]
    baseline_values = [v for _, v in baseline_rows]
    current_value = current_row[1]

    if detector.method == "z_score":
        mu, sigma, z = z_score(current_value, baseline_values)
        baseline_value, score = mu, z
    elif detector.method == "mad":
        med, _, z = mad_score(current_value, baseline_values)
        baseline_value, score = med, z
    else:
        raise ValueError(f"unsupported method {detector.method!r}")

    severity = _severity_for_score(abs(score), detector.threshold, detector)
    deviation = current_value - baseline_value

    return AnomalyResult(
        detector_id=detector.detector_id,
        method=detector.method,
        severity=severity,
        metric_name=detector.metric_name,
        observed_value=current_value,
        baseline_value=baseline_value,
        deviation=deviation,
        z_score=score,
        window_start=series[0][0],
        window_end=current_row[0],
        details={
            "baseline_window": detector.baseline_window,
            "threshold": detector.threshold,
            "n_baseline": len(baseline_values),
        },
    )


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------


def write_anomaly(
    conn: psycopg.Connection,
    *,
    dq_run_id: Optional[str],
    result: AnomalyResult,
) -> int:
    rows = fetchall(
        conn,
        """
        insert into dq.anomaly (
          dq_run_id, detector_id, method, severity, metric_name,
          observed_value, baseline_value, deviation, z_score,
          window_start, window_end, details
        ) values (
          %(dq_run_id)s::uuid, %(detector_id)s, %(method)s, %(severity)s::dq.anomaly_severity, %(metric_name)s,
          %(observed)s, %(baseline)s, %(deviation)s, %(z)s,
          %(ws)s, %(we)s, %(details)s::jsonb
        )
        returning anomaly_id
        """,
        {
            "dq_run_id": dq_run_id,
            "detector_id": result.detector_id,
            "method": result.method,
            "severity": result.severity,
            "metric_name": result.metric_name,
            "observed": result.observed_value,
            "baseline": result.baseline_value,
            "deviation": result.deviation,
            "z": result.z_score,
            "ws": result.window_start,
            "we": result.window_end,
            "details": result.details,
        },
    )
    return int(rows[0][0])


# ---------------------------------------------------------------------------
# YAML-driven suite runner (parallels run_dq.load_rules)
# ---------------------------------------------------------------------------


def load_detectors(path: str) -> List[Detector]:
    with open(path, "r", encoding="utf-8") as f:
        doc = yaml.safe_load(f) or {}

    out: List[Detector] = []
    for d in doc.get("anomalies", []):
        out.append(
            Detector(
                detector_id=d["detector_id"],
                method=d["method"],
                metric_sql=d["metric_sql"],
                baseline_window=int(d.get("baseline_window", 28)),
                threshold=float(d.get("threshold_sigma") or d.get("threshold_k") or 3),
                severity_at_threshold=d.get("severity_at_threshold", "warn"),
                severity_at_two_times_threshold=d.get(
                    "severity_at_two_times_threshold", "page"
                ),
                metric_name=d.get("metric_name", d["detector_id"]),
            )
        )
    return out


def run_anomaly_suite(
    conn: psycopg.Connection,
    detectors: List[Detector],
    *,
    dq_run_id: Optional[str] = None,
) -> List[AnomalyResult]:
    results: List[AnomalyResult] = []
    for d in detectors:
        r = evaluate_detector(conn, d)
        if r is None:
            continue
        results.append(r)
        if dq_run_id is not None:
            write_anomaly(conn, dq_run_id=dq_run_id, result=r)
    return results
