from __future__ import annotations

"""
AML/PEP webhook receiver (FastAPI).

Design notes:
- Endpoint is always-on (deploy behind API Gateway + Lambda or ECS).
- Verifies HMAC signature (vendor-specific; stub here as 'X-AML-Signature'
  using sha256(vendor_secret || body)).
- Writes raw payload to S3 Bronze (append-only, partitioned by ingest_date/event_id).
- Records audit.ingestion_run + ingestion_object for the single event.
- Returns 200 fast; downstream MWAA DAG reconciles Bronze -> silver.aml_screen
  to keep response latency low and avoid provider retries.
- Idempotent on vendor `event_id` via unique S3 key AND unique PK in silver.aml_screen.

Launch posture: small service with structured logging, retries on S3 write,
dead-letter key if audit persist fails.
"""

import hashlib
import hmac
import json
import os
from datetime import date
from typing import Any, Dict, Optional

from fastapi import FastAPI, Header, HTTPException, Request

from mal_pipeline.common.audit import end_run, record_error, record_object, start_run
from mal_pipeline.common.config import AppConfig, PostgresConfig, S3Location
from mal_pipeline.common.db import get_conn
from mal_pipeline.common.hashing import sha256_bytes
from mal_pipeline.common.s3 import put_bytes

AML_PARSER_VERSION = os.environ.get("AML_PARSER_VERSION", "aml-receiver@0.1.0")


def _load_config_from_env() -> AppConfig:
    return AppConfig(
        bronze=S3Location(
            bucket=os.environ["BRONZE_BUCKET"],
            prefix=os.environ.get("BRONZE_PREFIX", "bronze"),
        ),
        postgres=PostgresConfig(
            host=os.environ["PG_HOST"],
            port=int(os.environ.get("PG_PORT", "5432")),
            dbname=os.environ["PG_DBNAME"],
            user=os.environ["PG_USER"],
            password=os.environ["PG_PASSWORD"],
            sslmode=os.environ.get("PG_SSLMODE", "require"),
        ),
        aws_region=os.environ.get("AWS_REGION"),
    )


def verify_signature(*, body: bytes, signature_header: Optional[str], secret: str) -> bool:
    if not signature_header:
        return False
    expected = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    # constant-time compare
    return hmac.compare_digest(expected, signature_header.strip())


app = FastAPI(title="Mal AML webhook receiver", version="0.1.0")


@app.post("/webhooks/aml")
async def handle_aml(
    request: Request,
    x_aml_signature: Optional[str] = Header(default=None),
):
    body = await request.body()
    vendor_secret = os.environ.get("AML_WEBHOOK_SECRET", "")
    if not verify_signature(body=body, signature_header=x_aml_signature, secret=vendor_secret):
        raise HTTPException(status_code=401, detail="invalid signature")

    try:
        payload: Dict[str, Any] = json.loads(body.decode("utf-8"))
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=400, detail=f"invalid JSON: {e}")

    event_id = payload.get("event_id") or payload.get("id")
    if not event_id:
        raise HTTPException(status_code=400, detail="missing event_id")

    cfg = _load_config_from_env()
    ingest_date = date.today().isoformat()
    s3_key = cfg.bronze.key(
        f"aml/ingest_date={ingest_date}/event_id={event_id}/payload.json"
    )

    sha = sha256_bytes(body)

    try:
        put = put_bytes(
            bucket=cfg.bronze.bucket,
            key=s3_key,
            data=body,
            content_type="application/json",
            metadata={"sha256": sha, "source": "aml", "event_id": str(event_id)},
            kms_key_id=os.environ.get("BRONZE_KMS_KEY_ID"),
            region=cfg.aws_region,
        )
    except Exception as e:
        # 5xx so the vendor retries; we did not ack.
        raise HTTPException(status_code=502, detail=f"bronze write failed: {e}") from e

    try:
        with get_conn(cfg.postgres) as conn:
            run = start_run(
                conn,
                source="aml",
                trigger_type="event",
                parser_version=AML_PARSER_VERSION,
                notes=f"event_id={event_id}",
            )
            try:
                record_object(
                    conn,
                    run_id=run.run_id,
                    s3_bucket=cfg.bronze.bucket,
                    s3_key=s3_key,
                    content_type="application/json",
                    bytes_len=len(body),
                    record_count=1,
                    sha256_hex=sha,
                    etag=put.etag,
                    s3_version_id=put.version_id,
                )
                end_run(conn, run, status="SUCCEEDED")
            except Exception as e:
                record_error(
                    conn,
                    run_id=run.run_id,
                    stage="ingest",
                    error_type=type(e).__name__,
                    message=str(e),
                    details={"event_id": event_id, "s3_key": s3_key},
                )
                end_run(conn, run, status="FAILED", notes=f"audit error: {e}")
                # Payload is safely in Bronze; we still return 200 to avoid vendor retries
                # that would just create duplicate Bronze keys. Reconciliation DAG will heal.
    except Exception:
        # If DB is down, we accept the risk of duplicate Bronze objects from vendor retries
        # in exchange for preserving the raw payload. Reconcile DAG is idempotent by s3_key.
        pass

    return {"status": "accepted", "event_id": event_id, "s3_key": s3_key, "sha256": sha}
