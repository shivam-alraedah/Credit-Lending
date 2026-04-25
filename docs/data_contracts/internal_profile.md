# Contract: Internal customer profile (Postgres source-of-truth)

## 1. Identity

| Field | Value |
|---|---|
| **Name** | `internal_profile` |
| **Current version** | `v1.0` |
| **Owner (producer)** | Customer Systems team (owns the core `internal.customer_profile` table) |
| **Owner (Mal-side)** | Data Engineering (extract + load) |
| **Consumers** | `silver.customer_profile`, identity resolution, `gold.decision_input_snapshot.inputs.internal_profile`, marts |
| **Classification** | Internal + PII (full_name, dob, phone, email, emirates_id) |

## 2. Delivery

- **Transport (launch)**: daily logical extract to S3 CSV by `mal_pipeline.ingest.internal_profile_extract`.
- **Transport (post-launch)**: AWS DMS CDC into Bronze (parquet), same downstream loader.
- **Cadence**: Daily at 01:00 GST (snapshot date = prior calendar day).
- **Expected volume**: ~500k rows at launch, steady growth.
- **Failure semantics**: A missed snapshot for a day is a Warn (not Must-pass); we have strict ownership of source and can re-extract on demand.

## 3. Schema

Machine-readable: [`python/mal_pipeline/schemas/internal_profile.v1.json`](../../python/mal_pipeline/schemas/internal_profile.v1.json).

Human-readable CSV columns (header order is part of the contract):

| Column | Type | Required | Notes |
|---|---|---|---|
| `internal_uuid` | UUID | yes | Primary key; matches `silver.customer_profile.internal_uuid`. |
| `full_name` | string | no | **PII**. |
| `dob` | date | no | **PII**. |
| `phone_e164` | string | no | **PII**. Must be E.164 if present. |
| `email_norm` | string | no | **PII**. Lowercased + trimmed. |
| `emirates_id` | string | no | **PII**. Same regex as AECB. |
| `kyc_status` | string | no | Mal-internal enum: `pending` | `verified` | `rejected`. |
| `effective_ts` | timestamp | yes | `coalesce(updated_at, created_at)` from source. Drives recency in identity graph. |

## 4. Identifiers

- **Primary**: `internal_uuid`.
- **Secondary joins**: `emirates_id`, `phone_e164 + email_norm`, `full_name_norm + dob` — all PII.

## 5. SLAs / SLOs

| Metric | Target |
|---|---|
| Snapshot arrival | daily by 02:00 GST |
| Row-count drift vs 7-day median | within ±20% |
| Silver upsert latency from Bronze write | ≤ 15 min |

## 6. Quality rules

- **Must-pass**:
  - `internal_uuid` non-null and unique per snapshot.
  - `phone_e164` respects E.164 pattern if present.
  - `emirates_id` respects canonical regex if present.
- **Warn**:
  - Snapshot row count drift > 20% vs trend.
  - `kyc_status` enum drift.

## 7. Evolution policy

- Because this is Mal-internal, PRs against the core schema are required to include:
  - An updated contract entry here.
  - A migration plan in `docs/schema_evolution.md` if breaking.
  - A linked Flyway migration for the consumers in this repo.
- Removing or renaming a column requires an **expand → migrate → contract** sequence.

## 8. Operational contacts

- Producer team: Customer Systems (`#customer-systems` Slack).
- Mal consumer: Data Engineering.
- Escalation: Data Engineering → VP Engineering.
