from __future__ import annotations

from mal_pipeline.alerting.sns import format_dq_run_alert, publish_sns_message
from mal_pipeline.dq.run_dq import Rule, Threshold, run_dq


def test_format_dq_run_alert_no_samples():
    subj, body = format_dq_run_alert(
        dq_run_id="aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        must_pass=False,
        failed_must_pass_rule_ids=["r1", "r2"],
        scope={"layer": "gold"},
    )
    assert "FAIL" in subj
    assert "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee" in body
    assert "r1" in body and "r2" in body
    assert "row-level payloads" in body.lower()


def test_publish_sns_message_uses_injected_client():
    class DummySNS:
        def __init__(self):
            self.calls: list[dict] = []

        def publish(self, **kwargs):
            self.calls.append(kwargs)
            return {"MessageId": "msg-123"}

    dummy = DummySNS()
    mid = publish_sns_message(
        topic_arn="arn:aws:sns:us-east-1:123456789012:ops",
        subject="hello",
        message="world",
        sns_client=dummy,
    )
    assert mid == "msg-123"
    assert dummy.calls[0]["TopicArn"].endswith(":ops")
    assert dummy.calls[0]["Subject"] == "hello"
    assert dummy.calls[0]["Message"] == "world"


def _make_failing_must_pass_rule() -> Rule:
    return Rule(
        rule_id="uk_dup",
        description="dupes",
        severity="MUST_PASS",
        layer="silver",
        dimension="uniqueness",
        owner="data-engineering",
        sql_check="select 1 from silver.identity_observation",
        threshold=Threshold(op="lte", value=0),
    )


def test_run_dq_sns_on_fail_not_sent_when_green():
    from tests.conftest import FakeConn  # type: ignore

    class DummySNS:
        def publish(self, **_kwargs):
            raise AssertionError("unexpected publish")

    conn = FakeConn(responses={})
    rules = [
        Rule(
            rule_id="ok",
            description="ok",
            severity="MUST_PASS",
            layer="silver",
            dimension="completeness",
            owner="data-engineering",
            sql_check="select 1 where false",
            threshold=Threshold(op="lte", value=0),
        )
    ]
    _, must_pass = run_dq(
        conn,
        rules,
        sns_topic_arn="arn:aws:sns:us-east-1:123456789012:ops",
        sns_notify="on_fail",
        sns_client=DummySNS(),
    )
    assert must_pass is True


def test_run_dq_sns_on_fail_sent_when_red():
    from tests.conftest import FakeConn  # type: ignore

    class DummySNS:
        def __init__(self):
            self.calls: list[dict] = []

        def publish(self, **kwargs):
            self.calls.append(kwargs)
            return {"MessageId": "mid"}

    dummy = DummySNS()
    conn = FakeConn(
        responses={
            "from silver.identity_observation": [("aml", "e1")],
        }
    )
    _, must_pass = run_dq(
        conn,
        [_make_failing_must_pass_rule()],
        sns_topic_arn="arn:aws:sns:us-east-1:123456789012:ops",
        sns_notify="on_fail",
        sns_client=dummy,
    )
    assert must_pass is False
    assert len(dummy.calls) == 1
    assert "uk_dup" in dummy.calls[0]["Message"]


def test_run_dq_sns_always_sent_when_green():
    from tests.conftest import FakeConn  # type: ignore

    class DummySNS:
        def __init__(self):
            self.calls: list[dict] = []

        def publish(self, **kwargs):
            self.calls.append(kwargs)
            return {"MessageId": "mid"}

    dummy = DummySNS()
    conn = FakeConn(responses={})
    rules = [
        Rule(
            rule_id="ok",
            description="ok",
            severity="MUST_PASS",
            layer="silver",
            dimension="completeness",
            owner="data-engineering",
            sql_check="select 1 where false",
            threshold=Threshold(op="lte", value=0),
        )
    ]
    _, must_pass = run_dq(
        conn,
        rules,
        sns_topic_arn="arn:aws:sns:us-east-1:123456789012:ops",
        sns_notify="always",
        sns_client=dummy,
    )
    assert must_pass is True
    assert len(dummy.calls) == 1
