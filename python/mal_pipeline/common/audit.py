from __future__ import annotations

"""
Small audit helpers for audit.ingestion_run / ingestion_object / ingestion_error.

Every ingestion path uses these so we get consistent run-level auditability
for UAE Central Bank compliance (append-only trail).
"""

import uuid
from dataclasses import dataclass
from typing import Any, Dict, Optional

import psycopg

from .db import execute, fetchall


@dataclass(frozen=True)
class Run:
    run_id: str
    source: str


def start_run(
    conn: psycopg.Connection,
    *,
    source: str,
    trigger_type: str,
    parser_version: str,
    notes: Optional[str] = None,
) -> Run:
    run_id = str(uuid.uuid4())
    execute(
        conn,
        """
        insert into audit.ingestion_run (run_id, source, status, trigger_type, parser_version, notes)
        values (%(run_id)s::uuid, %(source)s, 'STARTED', %(trigger_type)s, %(parser_version)s, %(notes)s)
        """,
        {
            "run_id": run_id,
            "source": source,
            "trigger_type": trigger_type,
            "parser_version": parser_version,
            "notes": notes,
        },
    )
    return Run(run_id=run_id, source=source)


def end_run(conn: psycopg.Connection, run: Run, *, status: str, notes: Optional[str] = None) -> None:
    execute(
        conn,
        """
        update audit.ingestion_run
           set ended_at = now(),
               status = %(status)s::audit.run_status,
               notes = coalesce(%(notes)s, notes)
         where run_id = %(run_id)s::uuid
        """,
        {"run_id": run.run_id, "status": status, "notes": notes},
    )


def record_object(
    conn: psycopg.Connection,
    *,
    run_id: str,
    s3_bucket: str,
    s3_key: str,
    content_type: str,
    bytes_len: int,
    record_count: Optional[int],
    sha256_hex: str,
    etag: Optional[str] = None,
    s3_version_id: Optional[str] = None,
) -> int:
    rows = fetchall(
        conn,
        """
        insert into audit.ingestion_object (
          run_id, s3_bucket, s3_key, content_type, bytes, record_count, sha256_hex, etag, s3_version_id
        ) values (
          %(run_id)s::uuid, %(bucket)s, %(key)s, %(content_type)s, %(bytes)s, %(record_count)s, %(sha)s, %(etag)s, %(version_id)s
        )
        returning object_id
        """,
        {
            "run_id": run_id,
            "bucket": s3_bucket,
            "key": s3_key,
            "content_type": content_type,
            "bytes": bytes_len,
            "record_count": record_count,
            "sha": sha256_hex,
            "etag": etag,
            "version_id": s3_version_id,
        },
    )
    return int(rows[0][0])


def record_error(
    conn: psycopg.Connection,
    *,
    run_id: Optional[str],
    stage: str,
    error_type: str,
    message: str,
    details: Optional[Dict[str, Any]] = None,
) -> None:
    execute(
        conn,
        """
        insert into audit.ingestion_error (run_id, stage, error_type, message, details)
        values (%(run_id)s::uuid, %(stage)s, %(error_type)s, %(message)s, %(details)s::jsonb)
        """,
        {
            "run_id": run_id,
            "stage": stage,
            "error_type": error_type,
            "message": message,
            "details": details or {},
        },
    )
