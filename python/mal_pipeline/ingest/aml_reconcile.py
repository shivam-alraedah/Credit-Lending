from __future__ import annotations

"""
AML reconciliation job: Bronze -> Silver.

Runs under MWAA on a schedule (e.g., every 5 minutes). For each unprocessed
audit.ingestion_object with source=aml, fetch the raw payload from S3, parse,
and upsert into silver.aml_screen. Idempotent on aml_event_id PK.

This keeps the webhook hot path minimal and pushes normalization + DQ into
an orchestrated, observable pipeline.
"""

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

import boto3
import psycopg

from mal_pipeline.common.db import execute, fetchall


@dataclass(frozen=True)
class AmlEvent:
    event_id: str
    full_name: Optional[str]
    dob: Optional[str]
    screening_ts: datetime
    status: str
    match_details: Dict[str, Any]


def _fetch_unprocessed_aml_objects(conn: psycopg.Connection, batch_size: int = 500):
    return fetchall(
        conn,
        """
        select o.object_id, o.s3_bucket, o.s3_key
        from audit.ingestion_object o
        join audit.ingestion_run r on r.run_id = o.run_id
        left join silver.aml_screen a on a.raw_object_id = o.object_id
        where r.source = 'aml'
          and a.aml_event_id is null
        order by o.created_at asc
        limit %(batch_size)s
        """,
        {"batch_size": batch_size},
    )


def _parse_aml_payload(raw: bytes) -> AmlEvent:
    doc = json.loads(raw.decode("utf-8"))
    event_id = doc.get("event_id") or doc["id"]
    status = (doc.get("status") or "unknown").lower()
    ts_str = doc.get("screening_ts") or doc.get("ts") or datetime.utcnow().isoformat()
    ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    subject = doc.get("subject") or {}
    return AmlEvent(
        event_id=str(event_id),
        full_name=subject.get("full_name"),
        dob=subject.get("dob"),
        screening_ts=ts,
        status=status,
        match_details=doc.get("match_details") or {},
    )


def reconcile_aml(conn: psycopg.Connection, *, s3_client=None, batch_size: int = 500) -> int:
    s3 = s3_client or boto3.client("s3")
    rows = _fetch_unprocessed_aml_objects(conn, batch_size=batch_size)
    written = 0
    for object_id, bucket, key in rows:
        obj = s3.get_object(Bucket=bucket, Key=key)
        raw = obj["Body"].read()
        ev = _parse_aml_payload(raw)
        execute(
            conn,
            """
            insert into silver.aml_screen (
              aml_event_id, full_name, dob, screening_ts, status, match_details, raw_object_id
            ) values (
              %(event_id)s, %(name)s, %(dob)s::date, %(ts)s, %(status)s, %(details)s::jsonb, %(raw_object_id)s
            )
            on conflict (aml_event_id) do update set
              status = excluded.status,
              match_details = excluded.match_details,
              screening_ts = excluded.screening_ts,
              raw_object_id = excluded.raw_object_id
            """,
            {
                "event_id": ev.event_id,
                "name": ev.full_name,
                "dob": ev.dob,
                "ts": ev.screening_ts,
                "status": ev.status,
                "details": ev.match_details,
                "raw_object_id": object_id,
            },
        )
        written += 1
    return written
