from __future__ import annotations

"""
AECB (UAE Credit Bureau) XML ingestion.

Flow (MWAA DAG-friendly):
1. Pull XML files from SFTP (connection configured via Airflow Secrets / env).
2. Compute SHA-256 over raw bytes (integrity + idempotency key).
3. Upload raw bytes to S3 Bronze under a partitioned prefix:
     s3://<bronze>/aecb/ingest_date=YYYY-MM-DD/batch_id=<id>/<filename>.xml
4. Record audit.ingestion_run + audit.ingestion_object.
5. Parse XML -> canonical rows, upsert into silver.aecb_report with link back
   to the audit object_id for lineage.

This module is intentionally pure Python + stdlib for SFTP via a pluggable
fetcher. The real Airflow task passes an SFTPHook implementation.
"""

import io
import os
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional, Protocol

from lxml import etree

import psycopg

from mal_pipeline.common.audit import Run, end_run, record_object, start_run
from mal_pipeline.common.db import execute
from mal_pipeline.common.hashing import sha256_bytes
from mal_pipeline.common.s3 import put_bytes

AECB_PARSER_VERSION = os.environ.get("AECB_PARSER_VERSION", "aecb-parser@0.1.0")


class SftpFetcher(Protocol):
    def list_files(self, remote_dir: str) -> List[str]: ...
    def read_bytes(self, remote_path: str) -> bytes: ...


@dataclass(frozen=True)
class AecbReportRow:
    emirates_id: str
    bureau_batch_id: str
    report_ts: Optional[datetime]
    bureau_score: Optional[int]
    delinquency_flags: Dict[str, Any]


def _text(el: Optional[etree._Element]) -> Optional[str]:
    if el is None:
        return None
    t = el.text
    return t.strip() if t else None


def parse_aecb_xml(raw: bytes) -> List[AecbReportRow]:
    """
    Minimal AECB XML parser for demo. Real AECB schema is more complex;
    map to the subset you use for decisioning.

    Expected (illustrative) structure:
      <AECBReport batchId="B123" reportTs="2025-04-22T12:00:00Z">
        <Subject>
          <EmiratesId>784-1990-1234567-1</EmiratesId>
          <BureauScore>712</BureauScore>
          <Delinquencies>
            <Dpd30Plus>0</Dpd30Plus>
            <Dpd60Plus>0</Dpd60Plus>
          </Delinquencies>
        </Subject>
        ...
      </AECBReport>
    """
    tree = etree.parse(io.BytesIO(raw))
    root = tree.getroot()
    batch_id = root.get("batchId") or "UNKNOWN"
    report_ts_str = root.get("reportTs")
    report_ts = datetime.fromisoformat(report_ts_str.replace("Z", "+00:00")) if report_ts_str else None

    rows: List[AecbReportRow] = []
    for subject in root.iter("Subject"):
        emirates_id = _text(subject.find("EmiratesId"))
        if not emirates_id:
            # skip malformed subjects; DQ will surface these
            continue
        score_str = _text(subject.find("BureauScore"))
        try:
            bureau_score = int(score_str) if score_str is not None else None
        except ValueError:
            bureau_score = None

        delinquencies: Dict[str, Any] = {}
        delq = subject.find("Delinquencies")
        if delq is not None:
            for child in delq:
                delinquencies[child.tag] = _text(child)

        rows.append(
            AecbReportRow(
                emirates_id=emirates_id,
                bureau_batch_id=batch_id,
                report_ts=report_ts,
                bureau_score=bureau_score,
                delinquency_flags=delinquencies,
            )
        )
    return rows


def _upsert_aecb_rows(
    conn: psycopg.Connection, rows: Iterable[AecbReportRow], raw_object_id: int
) -> int:
    count = 0
    for r in rows:
        execute(
            conn,
            """
            insert into silver.aecb_report (
              emirates_id, bureau_batch_id, report_ts, bureau_score, delinquency_flags, raw_object_id
            ) values (
              %(emirates_id)s, %(batch_id)s, %(report_ts)s, %(score)s, %(flags)s::jsonb, %(raw_object_id)s
            )
            on conflict (emirates_id, bureau_batch_id) do update set
              report_ts = excluded.report_ts,
              bureau_score = excluded.bureau_score,
              delinquency_flags = excluded.delinquency_flags,
              raw_object_id = excluded.raw_object_id
            """,
            {
                "emirates_id": r.emirates_id,
                "batch_id": r.bureau_batch_id,
                "report_ts": r.report_ts,
                "score": r.bureau_score,
                "flags": r.delinquency_flags,
                "raw_object_id": raw_object_id,
            },
        )
        count += 1
    return count


def ingest_one_file(
    conn: psycopg.Connection,
    fetcher: SftpFetcher,
    *,
    remote_path: str,
    bronze_bucket: str,
    bronze_prefix: str,
    kms_key_id: Optional[str],
    region: Optional[str],
    ingest_date: Optional[date] = None,
) -> Dict[str, Any]:
    """
    Ingest a single AECB XML file end-to-end: SFTP -> S3 Bronze -> Silver.
    Returns an audit summary dict.
    """
    run: Run = start_run(
        conn,
        source="aecb",
        trigger_type="schedule",
        parser_version=AECB_PARSER_VERSION,
        notes=f"remote_path={remote_path}",
    )
    try:
        raw = fetcher.read_bytes(remote_path)
        sha = sha256_bytes(raw)

        dt = (ingest_date or date.today()).isoformat()
        # Peek batch id cheaply (parse once to get root attribute)
        root = etree.fromstring(raw)
        batch_id = root.get("batchId") or "UNKNOWN"

        filename = os.path.basename(remote_path)
        s3_key = f"{bronze_prefix.rstrip('/')}/aecb/ingest_date={dt}/batch_id={batch_id}/{filename}"
        put = put_bytes(
            bucket=bronze_bucket,
            key=s3_key,
            data=raw,
            content_type="application/xml",
            metadata={"sha256": sha, "source": "aecb", "run_id": run.run_id},
            kms_key_id=kms_key_id,
            region=region,
        )

        rows = parse_aecb_xml(raw)

        object_id = record_object(
            conn,
            run_id=run.run_id,
            s3_bucket=bronze_bucket,
            s3_key=s3_key,
            content_type="application/xml",
            bytes_len=len(raw),
            record_count=len(rows),
            sha256_hex=sha,
            etag=put.etag,
            s3_version_id=put.version_id,
        )

        written = _upsert_aecb_rows(conn, rows, raw_object_id=object_id)
        end_run(conn, run, status="SUCCEEDED", notes=f"silver_rows={written}")
        return {
            "run_id": run.run_id,
            "batch_id": batch_id,
            "s3_key": s3_key,
            "bytes": len(raw),
            "parsed_rows": len(rows),
            "written_rows": written,
        }
    except Exception as e:
        end_run(conn, run, status="FAILED", notes=f"error={e}")
        raise
