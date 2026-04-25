from __future__ import annotations

import uuid
from typing import Any, Dict, List, Optional

import psycopg

from mal_pipeline.common.db import execute, fetchall
from mal_pipeline.common.hashing import sha256_json_canonical


def _get_silver_inputs_for_decision(
    conn: psycopg.Connection, decision_id: str, customer_key: Optional[str]
) -> Dict[str, Any]:
    """
    Assemble the conformed input payload for a decision.

    - Fraud is keyed by decision_id (write-through at decision time).
    - Bureau / AML / internal profile are keyed by customer_key and fetched
      as the most recent in-scope row.
    """
    inputs: Dict[str, Any] = {"fraud": None, "aecb": None, "aml": None, "internal_profile": None}

    fraud = fetchall(
        conn,
        "select provider_name, score, risk_band, reason_codes, provider_response_ts "
        "from silver.fraud_score where decision_id = %(d)s",
        {"d": decision_id},
    )
    if fraud:
        r = fraud[0]
        inputs["fraud"] = {
            "provider_name": r[0],
            "score": float(r[1]) if r[1] is not None else None,
            "risk_band": r[2],
            "reason_codes": r[3],
            "provider_response_ts": r[4].isoformat() if r[4] else None,
        }

    if customer_key:
        aecb = fetchall(
            conn,
            """
            select bureau_batch_id, bureau_score, delinquency_flags, report_ts
            from silver.aecb_report
            where emirates_id = (select emirates_id from silver.customer where customer_key = %(c)s::uuid)
            order by report_ts desc nulls last
            limit 1
            """,
            {"c": customer_key},
        )
        if aecb:
            r = aecb[0]
            inputs["aecb"] = {
                "bureau_batch_id": r[0],
                "bureau_score": r[1],
                "delinquency_flags": r[2],
                "report_ts": r[3].isoformat() if r[3] else None,
            }

        aml = fetchall(
            conn,
            """
            select aml_event_id, status, match_details, screening_ts
            from silver.aml_screen a
            join silver.customer c on c.full_name_norm = a.full_name and c.dob = a.dob
            where c.customer_key = %(c)s::uuid
            order by screening_ts desc
            limit 1
            """,
            {"c": customer_key},
        )
        if aml:
            r = aml[0]
            inputs["aml"] = {
                "aml_event_id": r[0],
                "status": r[1],
                "match_details": r[2],
                "screening_ts": r[3].isoformat() if r[3] else None,
            }

        internal = fetchall(
            conn,
            """
            select internal_uuid, kyc_status, updated_at
            from silver.customer_profile
            where internal_uuid = (select internal_uuid from silver.customer where customer_key = %(c)s::uuid)
            """,
            {"c": customer_key},
        )
        if internal:
            r = internal[0]
            inputs["internal_profile"] = {
                "internal_uuid": str(r[0]) if r[0] else None,
                "kyc_status": r[1],
                "updated_at": r[2].isoformat() if r[2] else None,
            }

    return inputs


def create_decision_snapshot(
    conn: psycopg.Connection,
    *,
    decision_id: str,
    customer_key: Optional[str],
    identity_summary: Dict[str, Any],
    dq_summary: Dict[str, Any],
    model_rule_version: Optional[str],
    artifacts: List[Dict[str, str]],
    supersedes_snapshot_id: Optional[str] = None,
) -> str:
    """
    Writes an immutable Gold snapshot row + artifact pointers.

    artifacts: list of {source, s3_bucket, s3_key, sha256_hex}
    """
    snapshot_id = str(uuid.uuid4())
    inputs = _get_silver_inputs_for_decision(conn, decision_id, customer_key)

    payload = {
        "snapshot_id": snapshot_id,
        "decision_id": decision_id,
        "customer_key": customer_key,
        "identity_summary": identity_summary,
        "inputs": inputs,
        "dq_summary": dq_summary,
        "model_rule_version": model_rule_version,
        "supersedes_snapshot_id": supersedes_snapshot_id,
    }
    payload_hash = sha256_json_canonical(payload)

    execute(
        conn,
        """
        insert into gold.decision_input_snapshot (
          snapshot_id, decision_id, customer_key, identity_summary, inputs, dq_summary,
          model_rule_version, supersedes_snapshot_id, sha256_hex
        ) values (
          %(snapshot_id)s::uuid, %(decision_id)s, %(customer_key)s::uuid, %(identity_summary)s::jsonb,
          %(inputs)s::jsonb, %(dq_summary)s::jsonb, %(model_rule_version)s, %(supersedes_snapshot_id)s::uuid,
          %(sha256_hex)s
        )
        """,
        {
            "snapshot_id": snapshot_id,
            "decision_id": decision_id,
            "customer_key": customer_key,
            "identity_summary": identity_summary,
            "inputs": inputs,
            "dq_summary": dq_summary,
            "model_rule_version": model_rule_version,
            "supersedes_snapshot_id": supersedes_snapshot_id,
            "sha256_hex": payload_hash,
        },
    )

    for a in artifacts:
        execute(
            conn,
            """
            insert into gold.decision_input_artifact (snapshot_id, source, s3_bucket, s3_key, sha256_hex)
            values (%(snapshot_id)s::uuid, %(source)s, %(bucket)s, %(key)s, %(hash)s)
            """,
            {
                "snapshot_id": snapshot_id,
                "source": a["source"],
                "bucket": a["s3_bucket"],
                "key": a["s3_key"],
                "hash": a["sha256_hex"],
            },
        )

    return snapshot_id

