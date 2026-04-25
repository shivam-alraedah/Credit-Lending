import textwrap
from pathlib import Path

import pytest

from mal_pipeline.dq.run_dq import (
    Rule,
    Threshold,
    compare,
    evaluate_rule,
    load_rules,
    run_dq,
)


# ---- compare --------------------------------------------------------------


def test_compare_ops():
    assert compare(0, "lte", 0) is True
    assert compare(1, "lte", 0) is False
    assert compare(0.04, "lt", 0.05) is True
    assert compare(0.05, "lt", 0.05) is False
    assert compare(2, "gte", 2) is True
    assert compare(1, "ne", 0) is True
    assert compare(1, "eq", 1) is True


def test_compare_invalid_op():
    with pytest.raises(ValueError):
        compare(0, "nope", 0)


# ---- load_rules -----------------------------------------------------------


def test_load_rules_parses_yaml(tmp_path: Path):
    yml = tmp_path / "rules.yml"
    yml.write_text(textwrap.dedent(
        """
        rules:
          - rule_id: r1
            dimension: completeness
            severity: MUST_PASS
            layer: silver
            description: X
            sql_check: |
              select 1
          - rule_id: r2
            dimension: consistency
            severity: WARN
            layer: mart
            description: Y
            sql_metric: |
              select 0.02
            threshold: {op: lt, value: 0.05}
        """
    ))
    rules = load_rules(str(yml))
    assert [r.rule_id for r in rules] == ["r1", "r2"]
    assert rules[0].metric_type == "violation_count"
    assert rules[0].threshold.op == "lte"
    assert rules[1].metric_type == "ratio"
    assert rules[1].threshold.op == "lt"
    assert rules[1].threshold.value == 0.05


def test_load_rules_rejects_both_shapes(tmp_path: Path):
    yml = tmp_path / "rules.yml"
    yml.write_text(textwrap.dedent(
        """
        rules:
          - rule_id: bad
            severity: WARN
            layer: silver
            description: has both
            sql_check: select 1
            sql_metric: select 1
        """
    ))
    with pytest.raises(ValueError):
        load_rules(str(yml))


# ---- evaluate_rule --------------------------------------------------------


def _make_rule_check() -> Rule:
    return Rule(
        rule_id="uk_dup",
        description="dupes",
        severity="MUST_PASS",
        layer="silver",
        dimension="uniqueness",
        owner="data-engineering",
        sql_check="select source, source_record_id from silver.identity_observation",
        threshold=Threshold(op="lte", value=0),
    )


def _make_rule_metric(op="lt", value=0.05) -> Rule:
    return Rule(
        rule_id="null_ratio",
        description="null ratio",
        severity="WARN",
        layer="silver",
        dimension="completeness",
        owner="data-engineering",
        sql_metric="select 0 :: numeric",
        threshold=Threshold(op=op, value=value),
    )


def test_evaluate_rule_sql_check_fails_when_violations(conftest_fakeconn_factory=None):
    from tests.conftest import FakeConn  # type: ignore

    conn = FakeConn(responses={
        "from silver.identity_observation": [("aml", "e1"), ("aml", "e2")],
    })
    passed, metric, sample = evaluate_rule(conn, _make_rule_check())
    assert passed is False
    assert metric == 2.0
    assert len(sample) == 2


def test_evaluate_rule_sql_check_passes_when_empty():
    from tests.conftest import FakeConn  # type: ignore

    conn = FakeConn(responses={})
    passed, metric, sample = evaluate_rule(conn, _make_rule_check())
    assert passed is True
    assert metric == 0.0
    assert sample == []


def test_evaluate_rule_sql_metric_respects_threshold():
    from tests.conftest import FakeConn  # type: ignore

    rule = _make_rule_metric(op="lt", value=0.05)
    # Returns 0.03 -> passes `< 0.05`.
    conn = FakeConn(responses={"select 0": [(0.03,)]})
    passed, metric, sample = evaluate_rule(conn, rule)
    assert passed is True
    assert metric == 0.03
    assert sample == []


def test_evaluate_rule_sql_metric_fails_on_null_returns_zero():
    from tests.conftest import FakeConn  # type: ignore

    rule = _make_rule_metric(op="gte", value=1)
    conn = FakeConn(responses={"select 0": [(None,)]})
    passed, metric, _ = evaluate_rule(conn, rule)
    assert metric == 0.0
    assert passed is False


# ---- run_dq aggregate ----------------------------------------------------


def test_run_dq_must_pass_aggregation():
    from tests.conftest import FakeConn  # type: ignore

    # One MUST_PASS violated, one WARN violated.
    rules = [
        _make_rule_check(),                   # MUST_PASS, will fail
        _make_rule_metric(op="eq", value=0),  # WARN, will pass (0 == 0)
    ]
    conn = FakeConn(responses={
        "from silver.identity_observation": [("aml", "e1")],
        "select 0": [(0,)],
    })
    run_id, must_pass = run_dq(conn, rules, scope={"layer": "silver"})
    assert must_pass is False  # MUST_PASS failed
    # Ensure dq.run + dq.result inserts were attempted
    sqls = " \n ".join(s for s, _ in conn.executed)
    assert "insert into dq.run" in sqls
    assert "insert into dq.result" in sqls
