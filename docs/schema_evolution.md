# Schema Evolution Strategy

This document defines how schemas change in the Mal decision-input pipeline **without losing auditability, without breaking consumers, and without requiring heroic backfills**. It spans four surfaces:

1. **Vendor payload schemas** (AECB XML, Fraud JSON, AML webhook JSON, Internal Postgres rows).
2. **Bronze storage layout** (S3 keys + object metadata).
3. **Silver / Gold relational schemas** (RDS Postgres DDL).
4. **Gold snapshot payload** (`gold.decision_input_snapshot.inputs`, `identity_summary`, `dq_summary`).

Each surface evolves on its own cadence. The strategy is to **decouple them via versioned contracts** so a change in one does not cascade into forced migrations in others.

---

## 1. Principles

- **Every payload is versioned.** Bronze objects carry `schema_version` in S3 object metadata; vendor-produced payloads carry `schema_version` as a top-level field where the vendor supports it, and we track the observed version in `audit.source_schema_version`.
- **Additive changes are free. Removals/renames are opt-in migrations.** The default answer to “can I add this field?” is yes, provided consumers ignore unknown fields.
- **Breaking changes require a new major version.** We run old and new versions in parallel until all consumers migrate. Bronze keeps both; Silver adds columns, not replaces them.
- **Gold snapshots never change shape in place.** The snapshot payload JSON is immutable. If we add fields going forward, old snapshots remain valid under their original schema version.
- **DQ enforces the contracts.** JSON Schemas under `python/mal_pipeline/schemas/` are validated as **must-pass** rules so drift is detected on the first offending row, not in production dashboards.

---

## 2. What counts as breaking vs non-breaking

| Change | Category | Example |
|---|---|---|
| Add optional field | **Non-breaking** | AECB adds `<SubjectSegment>`; parser ignores if unknown |
| Add required field at source | **Breaking** for producers; non-breaking for consumers if we default it | Fraud provider now requires `channel` in requests |
| Rename field | **Breaking** | AML `status` renamed to `screening_status` |
| Change field type (e.g., string → number) | **Breaking** | Bureau score from `"712"` to `712` |
| Tighten validation (stricter regex) | **Breaking** | Emirates ID format tightened |
| Remove a field | **Breaking** | Fraud drops `reason_codes` |
| Semantic change with same type | **Breaking** (hard to spot — require version bump) | `risk_band` scale changed from 1–5 to 1–10 |
| Widen enum | **Non-breaking** | AML adds `in_progress` status |
| Narrow enum | **Breaking** | AML stops ever returning `error` |

Rule of thumb: **if it can make an existing consumer wrong or crash, it's breaking**.

---

## 3. Vendor payload schemas

### 3.1 Registry

We track every observed vendor schema version in `audit.source_schema_version`:

```sql
-- see sql/07_schema_registry.sql
select * from audit.source_schema_version
 where source = 'fraud';
```

Each ingestion path:

1. Reads `schema_version` from the payload (or derives it from payload shape).
2. Looks up the corresponding JSON Schema under `python/mal_pipeline/schemas/<source>.v<major>.json`.
3. **Validates** the payload before writing Silver. Validation failure blocks Silver load for that record and raises a must-pass DQ alert.
4. Records the `schema_version` used as part of the audit chain (`audit.ingestion_object` metadata + `audit.source_schema_version`).

### 3.2 Version bump workflow (per source)

```mermaid
flowchart TD
  A[Vendor announces change] --> B{Breaking?}
  B -- No --> C[Extend JSON Schema as optional]
  C --> D[Ship; old + new payloads both pass]
  B -- Yes --> E[Add schemas/<source>.v(N+1).json]
  E --> F[Register vN+1 in audit.source_schema_version]
  F --> G[Ingestion detects version per payload]
  G --> H[Silver loader dispatches on version]
  H --> I[Run in parallel until cutover date]
  I --> J[Backfill: re-parse Bronze with new loader if required]
  J --> K[Retire vN from ingestion only after 0 traffic for 14 days]
```

Each step has a test gate in CI:

- `tests/schemas/test_<source>_v<N>.py` runs fixtures through the validator.
- Must-pass DQ rule `payload_conforms_to_registered_schema` runs in production.

### 3.3 Source-specific notes

- **AECB (XML)**. The vendor's XSD is our source of truth. Our `aecb_xml.parse_aecb_xml` maps into a *stable internal shape* (`AecbReportRow`) which is the thing we version. Raw XML stays in Bronze untouched; parser versions are recorded in `audit.ingestion_run.parser_version`.
- **Fraud (JSON)**. Vendor supports `schema_version` in the request + response; we echo it into S3 object metadata. Changes have happened quarterly historically; our cap on frequency is “no more than one major per 90 days”.
- **AML (webhook JSON)**. Vendor includes `version` in webhook payloads. Unknown minor versions are accepted; unknown **major** versions are rejected with 501 and alerted so we onboard explicitly.
- **Internal profile (Postgres)**. Internally owned; schema changes are reviewed in PR. We keep the extract CSV header stable; additions go at the end; removals require a migration doc.

---

## 4. Bronze storage layout evolution

S3 keys are a contract. They change only with a **major bump**.

- Non-breaking additions: new prefix (e.g., `bronze/fraud/req/v2/decision_date=.../...`) alongside existing `req/`.
- Never rename existing keys. If a mistake was shipped, write *new* keys and leave old ones.
- Object metadata (`x-amz-meta-sha256`, `x-amz-meta-schema_version`, `x-amz-meta-source`) is append-only.

Glue Data Catalog / Athena partition projection is defined **per major version** so analytics queries aren't broken by a new layout.

---

## 5. Silver / Gold DDL evolution (Postgres)

Tooling: **Flyway** or **Liquibase** (CI-managed), never ad-hoc `ALTER` in prod.

### 5.1 Allowed and disallowed operations

| Operation | Allowed? | Notes |
|---|---|---|
| `ADD COLUMN NULL` | Yes | Preferred for additive changes |
| `ADD COLUMN DEFAULT <const>` | Yes (PG 11+) | Fast-path; no table rewrite |
| `DROP COLUMN` | **No** | Use a migration that stops writing, then drops in a much later release |
| `RENAME COLUMN` | **No** | Add new column, dual-write, migrate readers, drop old later |
| `ALTER TYPE` widening | With care | e.g., `int -> bigint` — must be done in a maintenance window |
| `ALTER TYPE` narrowing | **No** | Requires new column + backfill |
| Add check constraint | Use `NOT VALID` first | Validate in a separate step, off peak |
| Add FK to a partitioned child | Via trigger (PG limitation) | See partitioning doc |

### 5.2 Migration pattern for “risky” changes (expand → migrate → contract)

1. **Expand**: add the new column/table; keep old reads/writes working.
2. **Dual-write**: producers write to both old and new.
3. **Backfill**: async MWAA job fills history.
4. **Switch reads**: consumers move to new.
5. **Contract**: stop writing old; remove old after a safety window (typically ≥ 30 days).

We never compress steps 1–5 into a single release. Every step has a DQ rule verifying invariants (e.g., `count_where_new_is_null_should_be_zero`).

### 5.3 Gold snapshot immutability rule

Schemas *around* `gold.decision_input_snapshot` can evolve, but the **row itself** is append-only and hashed. If we need a richer snapshot shape:

- Add new optional keys in `inputs`/`identity_summary`/`dq_summary` → bump `snapshot.schema_version` (carried inside the payload).
- Old snapshots keep their original `schema_version` and `sha256_hex`. Replay continues to recompute the *same* hash from the stored JSON.
- New snapshots record the new version. Downstream code loads the correct JSON Schema based on the payload's `schema_version`.

---

## 6. Snapshot payload versioning (Gold)

The snapshot JSON (`inputs`, `identity_summary`, `dq_summary`) is owned by the decisioning service. We treat it as a **first-class contract** because regulators will replay it verbatim years from now.

```json
{
  "schema_version": "1.0",
  "snapshot_id": "...",
  "decision_id": "...",
  "customer_key": "...",
  "inputs": { "fraud": {...}, "aecb": {...}, "aml": {...}, "internal_profile": {...} },
  "identity_summary": {...},
  "dq_summary": {...},
  "model_rule_version": "rules@2025-04-20"
}
```

Rules:

- `schema_version` is **part of the canonical JSON that is hashed**. It's intentional: changing the schema cannot accidentally preserve a previous hash.
- The validator loads `python/mal_pipeline/schemas/decision_input_snapshot.v{major}.json` based on the payload's `schema_version`.
- If we introduce `schema_version = 2.0`, we keep v1 schema around so old snapshots stay verifiable. Our replay tool (`decision.replay.replay_snapshot`) already supports this pattern.

---

## 7. Backfill policy

Backfills are orchestrated in MWAA, never manually:

- **Idempotent by design**: Bronze keys contain natural IDs; Silver upserts on PK; Gold snapshots are append-only with hash-deduped `unique (decision_id, sha256_hex)`.
- **Window-scoped**: each backfill DAG takes a `start_date`/`end_date` arg and processes one Bronze partition at a time.
- **Observable**: backfills write to `audit.ingestion_run` with `trigger_type = 'backfill'` so analysts can exclude them from operational metrics.
- **Rate-limited**: we bound backfill throughput to avoid affecting live decisioning (hard cap on concurrent partitions + dedicated Airflow pool).

Common triggers that require backfill:

- Vendor-side correction (e.g., wrong AECB bureau score in a batch) → re-ingest the batch with `bureau_batch_id` suffixed.
- Parser bug fix → run a re-parse DAG that reads Bronze and rewrites Silver with the new `parser_version`.
- New derived feature for `mart.customer_risk_features_latest` → rebuild the mart window-by-window.

---

## 8. CI gates

- **Static**: every schema PR runs JSON Schema validation of repo-included fixtures.
- **Contract linter**: rejects removals/renames without a migration note (see §5.1).
- **DB migrations**: Flyway runs against a throwaway Postgres container with a seed dataset; failure halts merge.
- **Compatibility test**: existing Gold snapshot fixtures are re-validated against their declared `schema_version` to detect accidental schema tightening.

---

## 9. Ops playbook (TL;DR)

1. Announce change in #data-contracts channel + open a PR against `docs/data_contracts/<source>.md`.
2. Update / add JSON Schema in `python/mal_pipeline/schemas/`.
3. Add or extend fixtures in `python/tests/` and run locally.
4. Merge; CI deploys schemas to MWAA image; DQ starts validating.
5. Register the new `schema_version` in `audit.source_schema_version` (Flyway migration).
6. For breaking changes: run parallel versions until cutover; after 14 days of zero old-version traffic, retire in a cleanup PR.

---

## 10. Related files

- `docs/data_contracts/` — human-facing contracts per source.
- `python/mal_pipeline/schemas/` — machine-readable JSON Schemas.
- `python/mal_pipeline/dq/schema_validator.py` — reusable validator used by ingestion + DQ.
- `sql/07_schema_registry.sql` — `audit.source_schema_version` table.
- `docs/lineage.md` — table-level lineage so we can reason about blast radius when a contract changes.
