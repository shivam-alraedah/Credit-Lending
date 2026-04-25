# Keys, Constraints & Composite-Key Policy

This doc is the **single place** that answers: *"For table X, what's the PK? What's unique? What foreign keys exist, and where do we enforce integrity in the DB vs the DQ framework?"*. Applicable to the Postgres schemas in `sql/01..04_*.sql` and the partitioned variants in `sql/05_partitioning.sql`.

---

## 1. Policy overview

1. **Every table has a primary key.** No exceptions. If we don't have a natural key, we use a surrogate `bigserial` / `uuid`.
2. **Business keys get unique constraints even if we also use a surrogate key.** This prevents duplicate reality slipping in through the service layer.
3. **Foreign keys are used whenever the parent lives in the same Postgres database.** Cross-database parents (S3 Bronze, Redshift marts) are enforced by DQ rules instead — see `docs/cross_database_dq.md`.
4. **Composite keys are preferred over "bag of indexes"** where three or more columns identify a row uniquely. Readers can tell what "uniqueness" means from the key alone.
5. **All FKs are declared `ON DELETE RESTRICT`.** Audit data is never cascaded away; retention is its own, explicit process.
6. **Partial unique indexes are used for "only sometimes unique" columns** (e.g., `emirates_id` on `silver.customer` is unique **only when non-null**).

---

## 2. Per-table key map

Below is the current state. "DB-enforced" means Postgres enforces it; "DQ-enforced" means there's a matching rule in `dq.rule` for monitoring drift (often both are true).

### 2.1 `audit.*`

| Table | Primary key | Unique | Foreign keys | Notes |
|---|---|---|---|---|
| `audit.ingestion_run` | `run_id (uuid)` | — | — | Event log; append-only. |
| `audit.ingestion_object` | `object_id (bigserial)` | `(s3_bucket, s3_key, coalesce(s3_version_id,''))` | `run_id -> ingestion_run` | Unique constraint makes idempotent retries safe. |
| `audit.ingestion_error` | `error_id (bigserial)` | — | `run_id -> ingestion_run (nullable)` | Error may predate run creation. |
| `audit.payload_hash_log` | `payload_id (bigserial)` | — | — | Extra lineage for decision-time writes. |
| `audit.source_schema_version` | `(source, version, effective_from)` | — | — | Composite PK by design; history preserved. |

### 2.2 `silver.*`

| Table | Primary key | Unique | Foreign keys | Notes |
|---|---|---|---|---|
| `silver.customer_profile` | `internal_uuid (uuid)` | — | — | Natural key from source of truth. |
| `silver.aecb_report` | `aecb_report_id (bigserial)` | `(emirates_id, bureau_batch_id)` | `raw_object_id -> audit.ingestion_object` | **Composite UK** = "one AECB row per subject per batch". |
| `silver.fraud_score` | `decision_id (text)` | — | `raw_req_object_id`, `raw_res_object_id -> audit.ingestion_object` | PK is the business key; write-through at decision time. |
| `silver.aml_screen` | `aml_event_id (text)` | — | `raw_object_id -> audit.ingestion_object` | Vendor event id, idempotent. |
| `silver.identity_observation` | `observation_id (bigserial)` | `(source, source_record_id)` | — | **Composite UK** = "one observation per source record". |
| `silver.customer` | `customer_key (uuid)` | *(partial uniques, added in `sql/08_*`)* | — | See §3 for partial-unique design. |
| `silver.customer_link` | `link_id (bigserial)` | `(observation_id, customer_key)` | `observation_id -> identity_observation`, `customer_key -> customer` | Many-to-one with dedup. |
| `silver.customer_merge_event` | `merge_event_id (bigserial)` | `(from_customer_key, to_customer_key)` | both -> `customer` | Append-only audit. |

### 2.3 `gold.*`

| Table | Primary key | Unique | Foreign keys | Notes |
|---|---|---|---|---|
| `gold.product` | `product_id (text)` | — | — | Catalog. |
| `gold.decision` | `decision_id (text)` | — | `product_id -> gold.product` (added in `04_*`) | On partitioned variant: PK is `(decision_id, requested_at)` (partition key in PK is a PG requirement). |
| `gold.decision_input_snapshot` | `snapshot_id (uuid)` | `(decision_id, sha256_hex)` | `decision_id -> gold.decision`, `supersedes_snapshot_id -> gold.decision_input_snapshot` | Append-only with hash dedup. |
| `gold.decision_input_artifact` | `artifact_id (bigserial)` | — | `snapshot_id -> gold.decision_input_snapshot` | S3 pointer existence is **DQ-enforced** (cross-db). |
| `gold.decision_reassessment` | `reassessment_id (uuid)` | — | `decision_id -> gold.decision`, `new_snapshot_id -> gold.decision_input_snapshot` | Post-bureau fix-up. |
| `gold.degraded_policy_cap` | `(product_id, policy_code, effective_from)` | — | `product_id -> gold.product` | **Composite PK** captures effective-dated caps. |
| `gold.degraded_policy_daily_usage` | `(as_of_date, product_id, policy_code)` | — | — | **Composite PK** is the natural identity of the running tally. |

### 2.4 `dq.*`, `mart.*`, `ifrs9.*`

See the corresponding SQL files; the same rules apply. Of note:

- `dq.rule` PK is `rule_id` (text); `dq.run` PK is `dq_run_id`; `dq.result` has `(dq_run_id, rule_id)` as a natural identifier but uses a surrogate `bigserial` for paginated scans.
- `mart.risk_portfolio_daily` PK `(as_of_date, product_type)` — **composite, naturally identifying**.
- `ifrs9.provision_calculation` UNIQUE `(account_id, as_of_date, run_id)` — critical so re-runs don't double count.
- `ifrs9.account_segment_assignment` PK `(account_id, effective_from)` — **effective-dated composite**, which is a pattern we reuse for Sharia product versions and DQ rule history.

---

## 3. Partial unique indexes on `silver.customer` (survivorship guard)

`silver.customer` holds the best-known attributes per resolved identity. We don't want **two different `customer_key`s sharing the same strong identifier** — that's always a data bug:

```sql
-- lives in sql/08_dq_framework_upgrade.sql
create unique index if not exists ux_customer_emirates_id
  on silver.customer (emirates_id)
  where emirates_id is not null;

create unique index if not exists ux_customer_internal_uuid
  on silver.customer (internal_uuid)
  where internal_uuid is not null;

create unique index if not exists ux_customer_phone_email
  on silver.customer (phone_e164, email_norm)
  where phone_e164 is not null and email_norm is not null;
```

These are **partial** — they leave newly-created, partially-populated customers (where we only know a name and DOB) alone, but they fire immediately if a second row ever claims the same strong key. Paired with a MUST_PASS DQ rule, this gives us belt-and-braces protection without needing manual joins in the resolver.

---

## 4. Composite keys, explained in one place

We use composite keys in three common patterns:

1. **Natural identity** — the business identity really is multi-column:
   - `silver.aecb_report (emirates_id, bureau_batch_id)` → one report per subject per batch.
   - `silver.identity_observation (source, source_record_id)` → one observation per source record.
   - `mart.risk_portfolio_daily (as_of_date, product_type)` → one row per day per product.

2. **Effective-dated** — include the "valid-from" in the key to let history accumulate safely:
   - `audit.source_schema_version (source, version, effective_from)`.
   - `gold.degraded_policy_cap (product_id, policy_code, effective_from)`.
   - `ifrs9.account_segment_assignment (account_id, effective_from)`.

3. **Partition-forced** — Postgres requires any UNIQUE/PK on a partitioned table to include the partition key:
   - `gold.decision_partitioned (decision_id, requested_at)`.
   - `gold.decision_input_snapshot_partitioned (snapshot_id, snapshot_ts)`.
   - We accept this trade-off because the "real" unique constraint (`decision_id` alone) is also enforced by the application layer + a MUST_PASS DQ rule that scans for duplicate `decision_id` within the trailing window.

---

## 5. Where Postgres can't enforce, DQ does

The list of invariants we **cannot** express as Postgres constraints, with the corresponding rule categories:

| Invariant | Why no DB constraint | DQ rule (dimension) |
|---|---|---|
| Every `gold.decision_input_artifact.s3_key` resolves in S3 with matching SHA-256 | Foreign data store | `referential` (MUST_PASS) |
| Every `gold.decision` has ≥1 `gold.decision_input_snapshot` | Cardinality check across partitioned tables | `referential` (MUST_PASS) |
| `silver.fraud_score.decision_id` appears exactly once (uniqueness across partitioned children, if we partition this post-launch) | Partition UK limitation | `uniqueness` (MUST_PASS) |
| Redshift `gold.decision` row count matches RDS `gold.decision` for yesterday | Cross-DB | `consistency` (WARN → MUST_PASS after stable) |
| `silver.customer_link.confidence ∈ [0,1]` | PG CHECK would work; we still run DQ for dashboards | `validity` (MUST_PASS + CHECK) |
| `silver.identity_observation.dob <= current_date` | CHECK possible but rigid; DQ allows softer alerting | `validity` (WARN) |

---

## 6. Naming conventions we use in DDL

- PK: `pk_<table>` (Postgres autogenerates; we rely on that).
- Composite UK: `uq_<table>_<cols>` (e.g., `uq_aecb_report_emirates_batch`).
- Partial UK: `ux_<table>_<col>` (e.g., `ux_customer_emirates_id`).
- FK: `fk_<child>_<parent>` (e.g., `fk_customer_link_observation`).
- Index (non-unique): `idx_<table>_<cols>`.

Keep names ≤ 63 chars (Postgres limit). If a column list is too long, use a semantic suffix (e.g., `uq_dec_policy_effective`) and put a comment on the constraint.

---

## 7. Migration discipline for keys

Changing a key is a **breaking change**. Follow the expand → migrate → contract pattern from `docs/schema_evolution.md`:

1. **Add** the new PK/UK/FK as `NOT VALID` where possible.
2. **Validate** in a separate, off-peak window (`ALTER TABLE ... VALIDATE CONSTRAINT`).
3. **Switch producers** to the new identifier (dual-write if necessary).
4. **Retire** the old constraint in a later release after consumers are migrated.

For partitioned tables: any PK/UK change on the parent forces touching every partition; we schedule these migrations at month boundaries where possible.
