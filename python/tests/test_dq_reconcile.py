from datetime import datetime, timedelta, timezone

from mal_pipeline.dq.reconcile import (
    ReconciliationResult,
    bronze_to_silver_row_parity,
    fingerprint_parity,
    snapshot_replay_sample,
)


def _window():
    we = datetime(2025, 4, 22, 12, 0, tzinfo=timezone.utc)
    return we - timedelta(hours=1), we


def test_bronze_to_silver_row_parity_passes_within_tolerance():
    from tests.conftest import FakeConn  # type: ignore

    ws, we = _window()
    conn = FakeConn(responses={
        "select count(distinct (o.s3_bucket, o.s3_key))": [(100,)],
        "from silver.aml_screen": [(99,)],
    })
    result = bronze_to_silver_row_parity(
        conn, source="aml", window_start=ws, window_end=we, tolerance_ratio=0.02
    )
    assert result.passed is True
    assert result.left_metric == 100
    assert result.right_metric == 99
    assert result.discrepancy == 1


def test_bronze_to_silver_row_parity_fails_when_discrepancy_large():
    from tests.conftest import FakeConn  # type: ignore

    ws, we = _window()
    conn = FakeConn(responses={
        "select count(distinct (o.s3_bucket, o.s3_key))": [(100,)],
        "from silver.aml_screen": [(50,)],
    })
    result = bronze_to_silver_row_parity(
        conn, source="aml", window_start=ws, window_end=we, tolerance_ratio=0.01
    )
    assert result.passed is False
    assert result.discrepancy == 50


def test_fingerprint_parity_matches_and_mismatches():
    from tests.conftest import FakeConn  # type: ignore

    ws, we = _window()
    conn = FakeConn()

    def left_fetch(_ws, _we):
        return [("2025-04-20", "hash_a", 10), ("2025-04-21", "hash_b", 12)]

    def right_fetch(_ws, _we):
        return [("2025-04-20", "hash_a", 10), ("2025-04-21", "DIFF", 11)]

    results = fingerprint_parity(
        conn,
        name="pg_rs_decision",
        left_source="rds.gold.decision",
        right_source="rs.gold.decision",
        left_fetch=left_fetch,
        right_fetch=right_fetch,
        window_start=ws,
        window_end=we,
    )
    assert len(results) == 2
    passed_map = {r.name.split("[", 1)[1].rstrip("]"): r.passed for r in results}
    assert passed_map["2025-04-20"] is True
    assert passed_map["2025-04-21"] is False


def test_snapshot_replay_sample_passes_when_replayer_says_ok():
    from tests.conftest import FakeConn  # type: ignore

    class OkVerdict:
        hash_matches = True
        all_artifacts_ok = True

    conn = FakeConn(responses={
        "from gold.decision_input_snapshot": [("sid-1",), ("sid-2",)],
    })
    result = snapshot_replay_sample(
        conn, sample_pct=1.0, window_hours=24, replayer=lambda c, sid: OkVerdict()
    )
    assert result.passed is True
    assert result.left_metric == 2
    assert result.right_metric == 2


def test_snapshot_replay_sample_fails_when_any_mismatch():
    from tests.conftest import FakeConn  # type: ignore

    class VerdictFactory:
        def __init__(self):
            self.calls = 0

        def __call__(self, c, sid):
            self.calls += 1

            class V:
                hash_matches = (sid != "sid-2")
                all_artifacts_ok = True

            return V()

    conn = FakeConn(responses={
        "from gold.decision_input_snapshot": [("sid-1",), ("sid-2",)],
    })
    factory = VerdictFactory()
    result = snapshot_replay_sample(
        conn, sample_pct=1.0, window_hours=24, replayer=factory
    )
    assert factory.calls == 2
    assert result.passed is False
    assert result.details["mismatches"] == ["sid-2"]


def test_reconciliation_result_dataclass_roundtrip():
    r = ReconciliationResult(
        name="x", left_source="l", right_source="r",
        metric_type="row_count", left_metric=1, right_metric=1,
        left_fingerprint=None, right_fingerprint=None,
        discrepancy=0, tolerance=0, passed=True,
        window_start=None, window_end=None, details={},
    )
    assert r.passed is True
