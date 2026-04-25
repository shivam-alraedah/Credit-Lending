# Contract: Gold decision input snapshot (Mal-owned)

## 1. Identity

| Field | Value |
|---|---|
| **Name** | `decision_input_snapshot` |
| **Current version** | `v1.0` |
| **Owner (producer)** | Decisioning service (via `mal_pipeline.decision.snapshot_inputs.create_decision_snapshot`) |
| **Consumers** | Regulators (UAE Central Bank audits), model monitoring, reassessment, IFRS 9 model linkage |
| **Classification** | Regulated (primary audit artifact) + derived PII |

## 2. Delivery

- **Transport**: Postgres insert into `gold.decision_input_snapshot` (+ artifact pointer rows in `gold.decision_input_artifact`). Snapshot payload is additionally hashable via canonical JSON and **immutable** (append-only).
- **Cadence**: Exactly one snapshot per decision (may have superseding snapshots after reassessments).
- **Failure semantics**: Snapshot write failure **blocks the decision from being surfaced to the applicant** — a must-pass invariant.

## 3. Schema

Machine-readable: [`python/mal_pipeline/schemas/decision_input_snapshot.v1.json`](../../python/mal_pipeline/schemas/decision_input_snapshot.v1.json).

Canonical payload (JSON):

```json
{
  "schema_version": "1.0",
  "snapshot_id": "UUID",
  "decision_id": "STRING",
  "customer_key": "UUID",
  "identity_summary": { /* rule summary across sources */ },
  "inputs": {
    "fraud": { "provider_name": "...", "score": 0.0, "risk_band": "low", "reason_codes": [], "provider_response_ts": "..." },
    "aecb":  { "bureau_batch_id": "...", "bureau_score": 0, "delinquency_flags": {}, "report_ts": "..." },
    "aml":   { "aml_event_id": "...", "status": "clear", "match_details": {}, "screening_ts": "..." },
    "internal_profile": { "internal_uuid": "...", "kyc_status": "verified", "updated_at": "..." }
  },
  "dq_summary": { "must_pass": true, "failures": [] },
  "model_rule_version": "rules@YYYY-MM-DD",
  "supersedes_snapshot_id": null
}
```

The snapshot's `sha256_hex` is computed over the canonical JSON **including** `schema_version`.

## 4. Identifiers

- **Primary key**: `snapshot_id` (UUID).
- **Business key**: `(decision_id, sha256_hex)` — uniquely identifies a payload version for a decision.
- **Supersede pointer**: `supersedes_snapshot_id` for reassessments (e.g., after AECB arrives post-degraded-approval).

## 5. SLAs / SLOs

| Metric | Target |
|---|---|
| Snapshot write success rate | 100% must-pass invariant |
| Replay hash match for random 1% sampling | 100% |
| Bronze artifact hash match on replay | 100% |

## 6. Quality rules

- **Must-pass**:
  - `gold_snapshot_references_decision` (already defined).
  - `gold_snapshot_hash_matches_canonical_json` — replay every snapshot written in the last 24h.
  - `gold_snapshot_artifacts_exist` — every artifact pointer resolves to an S3 object whose SHA matches.
- **Warn**:
  - Snapshot payload size P99 > 64 KB (schema growth signal).
  - `model_rule_version` value set shrinking day-over-day (possible accidental rollback).

## 7. Evolution policy

- Snapshots are **append-only**. Changes never rewrite existing rows.
- New fields in `inputs`, `identity_summary`, `dq_summary` require a **schema_version bump** (minor for additive, major for breaking).
- The validator loads the schema corresponding to the payload's declared `schema_version`; old snapshots remain verifiable with old schemas retained indefinitely.

## 8. Operational contacts

- Mal owner: Decisioning engineering lead.
- Regulatory interface: Compliance.
- Escalation: Decisioning → Data Engineering → Compliance for any integrity issue (rotate through the DR runbook if hashes do not match on replay).
