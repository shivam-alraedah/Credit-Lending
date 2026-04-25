from __future__ import annotations

"""
Internal customer profile extract: source Postgres -> S3 Bronze snapshot (CSV).

For launch we do a daily full-table snapshot (small volume). Post-launch we
switch to CDC (DMS/Debezium) and incremental snapshots stitched via run_id.

Outputs:
  s3://<bronze>/internal_profile/snapshot_date=YYYY-MM-DD/export.csv
and audit.ingestion_run + ingestion_object.

An upsert loader (not shown here) reads the CSV into silver.customer_profile
under MWAA; keeping the Bronze snapshot ensures we can replay Silver at will.
"""

import csv
import io
import os
from datetime import date
from typing import Iterable, Optional, Sequence

import psycopg

from mal_pipeline.common.audit import Run, end_run, record_object, start_run
from mal_pipeline.common.db import fetchall
from mal_pipeline.common.hashing import sha256_bytes
from mal_pipeline.common.s3 import put_bytes

INTERNAL_PARSER_VERSION = os.environ.get("INTERNAL_PARSER_VERSION", "internal-extract@0.1.0")


_EXTRACT_SQL = """
select internal_uuid::text, full_name, dob::text, phone_e164, email_norm, emirates_id, kyc_status,
       coalesce(updated_at, created_at)::text as effective_ts
from internal.customer_profile
"""


def _rows_to_csv(header: Sequence[str], rows: Iterable[Sequence]) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(header)
    for r in rows:
        w.writerow(r)
    return buf.getvalue().encode("utf-8")


def extract_internal_profile(
    source_conn: psycopg.Connection,
    audit_conn: psycopg.Connection,
    *,
    bronze_bucket: str,
    bronze_prefix: str,
    kms_key_id: Optional[str] = None,
    region: Optional[str] = None,
    as_of: Optional[date] = None,
):
    dt = (as_of or date.today()).isoformat()
    run: Run = start_run(
        audit_conn,
        source="internal_profile",
        trigger_type="schedule",
        parser_version=INTERNAL_PARSER_VERSION,
        notes=f"snapshot_date={dt}",
    )
    try:
        rows = fetchall(source_conn, _EXTRACT_SQL)
        header = ["internal_uuid", "full_name", "dob", "phone_e164", "email_norm", "emirates_id", "kyc_status", "effective_ts"]
        body = _rows_to_csv(header, rows)
        sha = sha256_bytes(body)

        key = f"{bronze_prefix.rstrip('/')}/internal_profile/snapshot_date={dt}/export.csv"
        put = put_bytes(
            bucket=bronze_bucket,
            key=key,
            data=body,
            content_type="text/csv",
            metadata={"sha256": sha, "source": "internal_profile", "run_id": run.run_id},
            kms_key_id=kms_key_id,
            region=region,
        )

        record_object(
            audit_conn,
            run_id=run.run_id,
            s3_bucket=bronze_bucket,
            s3_key=key,
            content_type="text/csv",
            bytes_len=len(body),
            record_count=len(rows),
            sha256_hex=sha,
            etag=put.etag,
            s3_version_id=put.version_id,
        )
        end_run(audit_conn, run, status="SUCCEEDED", notes=f"rows={len(rows)}")
        return {"run_id": run.run_id, "s3_key": key, "rows": len(rows), "bytes": len(body)}
    except Exception as e:
        end_run(audit_conn, run, status="FAILED", notes=f"error={e}")
        raise
