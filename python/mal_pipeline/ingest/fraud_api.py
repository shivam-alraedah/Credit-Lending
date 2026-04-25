from __future__ import annotations

"""
Fraud provider integration (Option 1: write-through at decision time).

The decisioning service is expected to call `score_and_persist(...)` synchronously
during a credit-decision request. This module:

1. POSTs the customer signals to the fraud provider.
2. Writes the raw request and response to S3 Bronze (append-only, partitioned by decision_date).
3. Upserts a standardized row into silver.fraud_score keyed by decision_id.
4. Records audit entries so every decision has traceable provenance.

This keeps the medallion model intact even for real-time inputs: the Bronze/Silver
artifacts are produced at the moment the fraud score is obtained, so Gold decision
snapshots built seconds later can reference them by hash.
"""

import json
import os
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Dict, Optional

import requests

import psycopg

from mal_pipeline.common.audit import Run, end_run, record_object, start_run
from mal_pipeline.common.db import execute
from mal_pipeline.common.hashing import sha256_bytes
from mal_pipeline.common.s3 import put_bytes

FRAUD_PARSER_VERSION = os.environ.get("FRAUD_PARSER_VERSION", "fraud-client@0.1.0")


@dataclass(frozen=True)
class FraudScoreResult:
    decision_id: str
    score: Optional[float]
    risk_band: Optional[str]
    reason_codes: Any
    provider_request_ts: datetime
    provider_response_ts: datetime
    raw_req_object_id: int
    raw_res_object_id: int


def _bronze_key(prefix: str, source: str, decision_id: str, filename: str) -> str:
    dt = date.today().isoformat()
    return f"{prefix.rstrip('/')}/fraud/{source}/decision_date={dt}/decision_id={decision_id}/{filename}"


def _put_json(
    *,
    bucket: str,
    key: str,
    payload: Dict[str, Any],
    kms_key_id: Optional[str],
    region: Optional[str],
    metadata: Optional[Dict[str, str]] = None,
):
    body = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    sha = sha256_bytes(body)
    put = put_bytes(
        bucket=bucket,
        key=key,
        data=body,
        content_type="application/json",
        metadata={**(metadata or {}), "sha256": sha},
        kms_key_id=kms_key_id,
        region=region,
    )
    return body, sha, put


def score_and_persist(
    conn: psycopg.Connection,
    *,
    decision_id: str,
    phone_e164: str,
    email_norm: str,
    context: Dict[str, Any],
    provider_url: str,
    provider_name: str,
    provider_api_key: str,
    bronze_bucket: str,
    bronze_prefix: str,
    kms_key_id: Optional[str] = None,
    region: Optional[str] = None,
    request_timeout_sec: float = 3.0,
) -> FraudScoreResult:
    """
    Synchronously call the fraud provider and persist Bronze + Silver + audit.

    Raises on any network/parse failure; caller (decisioning service) decides
    whether to hard-block or degrade based on product policy.
    """
    run: Run = start_run(
        conn,
        source="fraud",
        trigger_type="decisioning_service",
        parser_version=FRAUD_PARSER_VERSION,
        notes=f"decision_id={decision_id};provider={provider_name}",
    )
    try:
        request_payload: Dict[str, Any] = {
            "decision_id": decision_id,
            "phone": phone_e164,
            "email": email_norm,
            "context": context,
        }

        req_ts = datetime.now(timezone.utc)

        # 1) Persist raw request to Bronze BEFORE calling provider.
        #    This guarantees we retain the exact inputs even if the provider call fails.
        req_key = _bronze_key(bronze_prefix, "req", decision_id, "request.json")
        req_body, req_sha, req_put = _put_json(
            bucket=bronze_bucket,
            key=req_key,
            payload=request_payload,
            kms_key_id=kms_key_id,
            region=region,
            metadata={"source": "fraud_req", "decision_id": decision_id, "run_id": run.run_id},
        )
        req_object_id = record_object(
            conn,
            run_id=run.run_id,
            s3_bucket=bronze_bucket,
            s3_key=req_key,
            content_type="application/json",
            bytes_len=len(req_body),
            record_count=1,
            sha256_hex=req_sha,
            etag=req_put.etag,
            s3_version_id=req_put.version_id,
        )

        # 2) Call provider synchronously.
        headers = {"Authorization": f"Bearer {provider_api_key}", "Content-Type": "application/json"}
        resp = requests.post(provider_url, json=request_payload, headers=headers, timeout=request_timeout_sec)
        resp.raise_for_status()
        response_json = resp.json()
        res_ts = datetime.now(timezone.utc)

        # 3) Persist raw response to Bronze (append-only).
        res_key = _bronze_key(bronze_prefix, "res", decision_id, "response.json")
        res_body, res_sha, res_put = _put_json(
            bucket=bronze_bucket,
            key=res_key,
            payload=response_json,
            kms_key_id=kms_key_id,
            region=region,
            metadata={"source": "fraud_res", "decision_id": decision_id, "run_id": run.run_id},
        )
        res_object_id = record_object(
            conn,
            run_id=run.run_id,
            s3_bucket=bronze_bucket,
            s3_key=res_key,
            content_type="application/json",
            bytes_len=len(res_body),
            record_count=1,
            sha256_hex=res_sha,
            etag=res_put.etag,
            s3_version_id=res_put.version_id,
        )

        score = response_json.get("score")
        risk_band = response_json.get("risk_band")
        reason_codes = response_json.get("reason_codes")

        # 4) Upsert standardized silver row keyed by decision_id.
        execute(
            conn,
            """
            insert into silver.fraud_score (
              decision_id, phone_e164, email_norm, provider_name,
              provider_request_ts, provider_response_ts, score, risk_band, reason_codes,
              raw_req_object_id, raw_res_object_id
            ) values (
              %(decision_id)s, %(phone)s, %(email)s, %(provider)s,
              %(req_ts)s, %(res_ts)s, %(score)s, %(band)s, %(codes)s::jsonb,
              %(req_obj)s, %(res_obj)s
            )
            on conflict (decision_id) do update set
              score = excluded.score,
              risk_band = excluded.risk_band,
              reason_codes = excluded.reason_codes,
              provider_response_ts = excluded.provider_response_ts,
              raw_res_object_id = excluded.raw_res_object_id
            """,
            {
                "decision_id": decision_id,
                "phone": phone_e164,
                "email": email_norm,
                "provider": provider_name,
                "req_ts": req_ts,
                "res_ts": res_ts,
                "score": score,
                "band": risk_band,
                "codes": reason_codes,
                "req_obj": req_object_id,
                "res_obj": res_object_id,
            },
        )

        end_run(conn, run, status="SUCCEEDED", notes=f"score={score}")
        return FraudScoreResult(
            decision_id=decision_id,
            score=float(score) if score is not None else None,
            risk_band=risk_band,
            reason_codes=reason_codes,
            provider_request_ts=req_ts,
            provider_response_ts=res_ts,
            raw_req_object_id=req_object_id,
            raw_res_object_id=res_object_id,
        )
    except Exception as e:
        end_run(conn, run, status="FAILED", notes=f"error={e}")
        raise
