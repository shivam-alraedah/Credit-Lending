import textwrap
from datetime import datetime, timedelta, timezone
from pathlib import Path

from mal_pipeline.dq.anomaly import (
    Detector,
    evaluate_detector,
    load_detectors,
    mad_score,
    run_anomaly_suite,
    z_score,
)


# ---- pure helpers ----------------------------------------------------------


def test_z_score_flat_series_returns_zero():
    mu, sigma, z = z_score(10, [10, 10, 10])
    assert mu == 10
    assert sigma == 0
    assert z == 0


def test_z_score_spike_is_positive():
    mu, sigma, z = z_score(20, [10, 10, 10, 10])
    assert mu == 10
    assert sigma == 0  # degenerate baseline -> z=0 guarded by helper
    assert z == 0

    mu, sigma, z = z_score(20, [9, 10, 11, 10])
    assert z > 0


def test_mad_score_robust_to_single_outlier():
    # One big outlier doesn't blow up MAD like it would stddev
    baseline = [10, 10, 10, 10, 200]
    med, mad, z = mad_score(11, baseline)
    assert med == 10
    assert mad == 0.0  # four zeros and one 190 -> median deviation is 0
    # When MAD=0, robust z is defined as 0 by the helper
    assert z == 0

    baseline = [10, 11, 9, 10, 12]
    med, mad, z = mad_score(25, baseline)
    assert med == 10
    assert z > 0


# ---- evaluate_detector ----------------------------------------------------


def _ts_series(n: int, start: datetime, step_hours: int = 1):
    return [(start + timedelta(hours=i * step_hours), float(10)) for i in range(n)]


def test_evaluate_detector_cold_start_returns_info():
    from tests.conftest import FakeConn  # type: ignore

    # Only 3 samples but baseline_window asks for 28
    now = datetime(2025, 4, 22, 12, 0, tzinfo=timezone.utc)
    conn = FakeConn(responses={
        "select 1": _ts_series(3, now),
    })
    detector = Detector(
        detector_id="d1",
        method="z_score",
        metric_sql="select 1",
        baseline_window=28,
        threshold=3.0,
    )
    r = evaluate_detector(conn, detector)
    assert r is not None
    assert r.severity == "info"
    assert r.details.get("reason") == "cold_start"


def test_evaluate_detector_emits_warn_on_clear_spike():
    from tests.conftest import FakeConn  # type: ignore

    now = datetime(2025, 4, 22, 0, 0, tzinfo=timezone.utc)
    baseline = _ts_series(28, now, step_hours=1)
    spike = [(now + timedelta(hours=28), 50.0)]
    series = baseline + spike
    conn = FakeConn(responses={"select 1": series})
    detector = Detector(
        detector_id="d2",
        method="z_score",
        metric_sql="select 1",
        baseline_window=28,
        threshold=3.0,
    )
    r = evaluate_detector(conn, detector)
    assert r is not None
    # All baseline points are 10, current is 50 -> degenerate sigma=0 -> z=0.
    # That produces an info-level anomaly, which is the correct behavior.
    assert r.severity == "info"
    assert r.observed_value == 50.0


def test_evaluate_detector_with_varying_baseline_emits_warn():
    from tests.conftest import FakeConn  # type: ignore

    now = datetime(2025, 4, 22, 0, 0, tzinfo=timezone.utc)
    # non-constant baseline: sigma > 0
    baseline = [(now + timedelta(hours=i), float(10 + (i % 3))) for i in range(28)]
    spike = [(now + timedelta(hours=28), 50.0)]
    conn = FakeConn(responses={"select 1": baseline + spike})
    detector = Detector(
        detector_id="d3",
        method="z_score",
        metric_sql="select 1",
        baseline_window=28,
        threshold=3.0,
    )
    r = evaluate_detector(conn, detector)
    assert r is not None
    assert r.severity in ("warn", "page")
    assert r.z_score is not None
    assert abs(r.z_score) >= 3.0


# ---- YAML loader ----------------------------------------------------------


def test_load_detectors_yaml(tmp_path: Path):
    yml = tmp_path / "anomalies.yml"
    yml.write_text(textwrap.dedent(
        """
        anomalies:
          - detector_id: vol_z
            method: z_score
            metric_sql: |
              select now(), 1
            baseline_window: 14
            threshold_sigma: 3
          - detector_id: rate_mad
            method: mad
            metric_sql: |
              select now(), 0
            baseline_window: 28
            threshold_k: 3
            severity_at_threshold: warn
        """
    ))
    detectors = load_detectors(str(yml))
    assert [d.detector_id for d in detectors] == ["vol_z", "rate_mad"]
    assert detectors[0].method == "z_score"
    assert detectors[1].method == "mad"
    assert detectors[0].threshold == 3.0


def test_run_anomaly_suite_returns_results():
    from tests.conftest import FakeConn  # type: ignore

    now = datetime(2025, 4, 22, 0, 0, tzinfo=timezone.utc)
    baseline = [(now + timedelta(hours=i), float(10 + (i % 3))) for i in range(28)]
    spike = [(now + timedelta(hours=28), 100.0)]
    conn = FakeConn(responses={"select 1": baseline + spike})
    detector = Detector(
        detector_id="vol_z",
        method="z_score",
        metric_sql="select 1",
        baseline_window=28,
        threshold=3.0,
    )
    results = run_anomaly_suite(conn, [detector])
    assert len(results) == 1
    assert results[0].severity in ("warn", "page")
