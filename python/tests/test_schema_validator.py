import pytest

from mal_pipeline.dq.schema_validator import (
    SchemaValidationError,
    validate_or_raise,
    validate_payload,
)


# ---- Fraud response -------------------------------------------------------


def _valid_fraud_response():
    return {
        "schema_version": "1.0",
        "decision_id": "dec-1",
        "score": 123.4,
        "risk_band": "medium",
        "reason_codes": ["VEL_01"],
        "provider_response_ts": "2025-04-22T12:34:56Z",
    }


def test_fraud_response_valid():
    assert validate_payload("fraud_response", "1.0", _valid_fraud_response()) == []


def test_fraud_response_missing_required_field():
    bad = _valid_fraud_response()
    del bad["score"]
    errs = validate_payload("fraud_response", "1.0", bad)
    assert len(errs) == 1
    assert "score" in errs[0].message


def test_fraud_response_enum_rejected():
    bad = _valid_fraud_response()
    bad["risk_band"] = "EXTREME"
    errs = validate_payload("fraud_response", "1.0", bad)
    assert any("enum" in e.message.lower() or "'EXTREME'" in e.message for e in errs)


def test_fraud_response_out_of_range_score():
    bad = _valid_fraud_response()
    bad["score"] = 9999
    errs = validate_payload("fraud_response", "1.0", bad)
    assert errs, "score above max should fail"


def test_validate_or_raise_raises():
    bad = _valid_fraud_response()
    del bad["decision_id"]
    with pytest.raises(SchemaValidationError) as exc:
        validate_or_raise("fraud_response", "1.0", bad)
    assert exc.value.source == "fraud_response"
    assert exc.value.violations


# ---- AML webhook ----------------------------------------------------------


def _valid_aml_webhook():
    return {
        "schema_version": "1.0",
        "event_id": "evt-1",
        "screening_ts": "2025-04-22T12:00:00Z",
        "subject": {"full_name": "Ahmed Ali", "dob": "1990-05-01"},
        "status": "clear",
        "match_details": {},
    }


def test_aml_webhook_valid():
    assert validate_payload("aml_webhook", "1.0", _valid_aml_webhook()) == []


def test_aml_webhook_bad_status():
    bad = _valid_aml_webhook()
    bad["status"] = "super_clear"
    errs = validate_payload("aml_webhook", "1.0", bad)
    assert errs


def test_aml_webhook_missing_subject():
    bad = _valid_aml_webhook()
    del bad["subject"]
    errs = validate_payload("aml_webhook", "1.0", bad)
    assert errs


# ---- Decision input snapshot ---------------------------------------------


def _valid_snapshot():
    return {
        "schema_version": "1.0",
        "snapshot_id": "11111111-1111-1111-1111-111111111111",
        "decision_id": "dec-1",
        "customer_key": "22222222-2222-2222-2222-222222222222",
        "identity_summary": {"rule": "emirates_id_exact"},
        "inputs": {
            "fraud": {"score": 100, "risk_band": "low"},
            "aecb": {"bureau_score": 712},
            "aml": {"status": "clear"},
            "internal_profile": {"kyc_status": "verified"},
        },
        "dq_summary": {"must_pass": True, "failures": []},
        "model_rule_version": "rules@2025-04-22",
    }


def test_snapshot_valid():
    assert validate_payload("decision_input_snapshot", "1.0", _valid_snapshot()) == []


def test_snapshot_missing_inputs_block():
    bad = _valid_snapshot()
    del bad["inputs"]["aecb"]
    errs = validate_payload("decision_input_snapshot", "1.0", bad)
    assert errs


def test_snapshot_bad_schema_version_format():
    bad = _valid_snapshot()
    bad["schema_version"] = "2.0"
    errs = validate_payload("decision_input_snapshot", "1.0", bad)
    assert errs
    assert any(e.path == "/schema_version" for e in errs)


# ---- Internal profile ----------------------------------------------------


def test_internal_profile_valid_minimal():
    payload = {
        "internal_uuid": "33333333-3333-3333-3333-333333333333",
        "effective_ts": "2025-04-22T01:00:00Z",
    }
    assert validate_payload("internal_profile", "1.0", payload) == []


def test_internal_profile_bad_phone():
    payload = {
        "internal_uuid": "33333333-3333-3333-3333-333333333333",
        "effective_ts": "2025-04-22T01:00:00Z",
        "phone_e164": "971501234567",  # missing +
    }
    errs = validate_payload("internal_profile", "1.0", payload)
    assert errs


# ---- AECB canonical row --------------------------------------------------


def test_aecb_valid():
    payload = {
        "emirates_id": "784-1990-1234567-1",
        "bureau_batch_id": "B-01",
        "report_ts": "2025-04-22T02:00:00Z",
        "bureau_score": 712,
        "delinquency_flags": {"Dpd30Plus": "0"},
    }
    assert validate_payload("aecb", "1.0", payload) == []


def test_aecb_bad_emirates_id():
    payload = {
        "emirates_id": "BAD-ID",
        "bureau_batch_id": "B-01",
        "report_ts": "2025-04-22T02:00:00Z",
    }
    errs = validate_payload("aecb", "1.0", payload)
    assert errs
