from __future__ import annotations

"""
Snapshot replay & verification.

Given a `snapshot_id`, this module:
1. Reads the stored snapshot row from gold.decision_input_snapshot.
2. Re-fetches the referenced Bronze objects from S3 and verifies their SHA-256.
3. Re-builds the canonical snapshot payload and recomputes the hash.
4. Returns a structured verdict consumable by audit scripts.

Use cases:
- Central Bank audit: prove that the inputs used for a decision are identical
  to what's still on disk.
- Incident response: detect tampering or accidental mutation.
"""

from dataclasses import dataclass
from typing import Any, Dict, List

import boto3
import psycopg

from mal_pipeline.common.db import fetchall
from mal_pipeline.common.hashing import sha256_bytes, sha256_json_canonical


@dataclass(frozen=True)
class ArtifactVerification:
    source: str
    s3_bucket: str
    s3_key: str
    expected_sha: str
    actual_sha: str
    ok: bool


@dataclass(frozen=True)
class ReplayVerdict:
    snapshot_id: str
    decision_id: str
    stored_hash: str
    recomputed_hash: str
    hash_matches: bool
    artifacts: List[ArtifactVerification]
    all_artifacts_ok: bool


def _load_snapshot(conn: psycopg.Connection, snapshot_id: str) -> Dict[str, Any]:
    rows = fetchall(
        conn,
        """
        select snapshot_id::text, decision_id, customer_key::text, identity_summary,
               inputs, dq_summary, model_rule_version, supersedes_snapshot_id::text, sha256_hex
        from gold.decision_input_snapshot
        where snapshot_id = %(id)s::uuid
        """,
        {"id": snapshot_id},
    )
    if not rows:
        raise ValueError(f"snapshot_id {snapshot_id} not found")
    r = rows[0]
    return {
        "snapshot_id": r[0],
        "decision_id": r[1],
        "customer_key": r[2],
        "identity_summary": r[3],
        "inputs": r[4],
        "dq_summary": r[5],
        "model_rule_version": r[6],
        "supersedes_snapshot_id": r[7],
        "stored_hash": r[8],
    }


def _load_artifacts(conn: psycopg.Connection, snapshot_id: str) -> List[Dict[str, str]]:
    rows = fetchall(
        conn,
        """
        select source, s3_bucket, s3_key, sha256_hex
        from gold.decision_input_artifact
        where snapshot_id = %(id)s::uuid
        order by artifact_id
        """,
        {"id": snapshot_id},
    )
    return [
        {"source": r[0], "s3_bucket": r[1], "s3_key": r[2], "sha256_hex": r[3]}
        for r in rows
    ]


def replay_snapshot(conn: psycopg.Connection, *, snapshot_id: str, s3_client=None) -> ReplayVerdict:
    s3 = s3_client or boto3.client("s3")
    snap = _load_snapshot(conn, snapshot_id)
    artifacts = _load_artifacts(conn, snapshot_id)

    # 1) Recompute snapshot payload hash (must match the stored hash bit-for-bit).
    payload = {
        "snapshot_id": snap["snapshot_id"],
        "decision_id": snap["decision_id"],
        "customer_key": snap["customer_key"],
        "identity_summary": snap["identity_summary"],
        "inputs": snap["inputs"],
        "dq_summary": snap["dq_summary"],
        "model_rule_version": snap["model_rule_version"],
        "supersedes_snapshot_id": snap["supersedes_snapshot_id"],
    }
    recomputed = sha256_json_canonical(payload)

    # 2) Verify each Bronze artifact is unchanged.
    verifications: List[ArtifactVerification] = []
    for a in artifacts:
        obj = s3.get_object(Bucket=a["s3_bucket"], Key=a["s3_key"])
        raw = obj["Body"].read()
        actual = sha256_bytes(raw)
        verifications.append(
            ArtifactVerification(
                source=a["source"],
                s3_bucket=a["s3_bucket"],
                s3_key=a["s3_key"],
                expected_sha=a["sha256_hex"],
                actual_sha=actual,
                ok=(actual == a["sha256_hex"]),
            )
        )

    return ReplayVerdict(
        snapshot_id=snap["snapshot_id"],
        decision_id=snap["decision_id"],
        stored_hash=snap["stored_hash"],
        recomputed_hash=recomputed,
        hash_matches=(recomputed == snap["stored_hash"]),
        artifacts=verifications,
        all_artifacts_ok=all(v.ok for v in verifications),
    )
