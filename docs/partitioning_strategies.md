# Partitioning Strategies — S3, Postgres, Redshift

This doc explains **how and why we partition** data in each storage layer of the pipeline. The goal is to hit the three constraints that matter at senior-DE scale: **query pruning**, **write hot-spotting**, and **lifecycle management** (retention, tiering, compliance hold).

---

## 1. Guiding principles

1. **Partition on a column that appears in most WHERE clauses** (usually an event date or decision date). This is the only way to get partition pruning.
2. **Pick a partition granularity that keeps per-partition size sane**: too granular = metadata bloat; too coarse = hot partitions.
3. **Keep natural keys in the partition path** so replays and idempotency are obvious.
4. **Do not partition on user IDs** in Postgres for this workload — traffic is decision-time-driven; date pruning matters more than tenant isolation.
5. **Every partitioning choice ties back to a retention decision**. If you can't drop/archive a whole partition cleanly, the partitioning is wrong.

---

## 2. S3 Bronze partitioning (Hive-style)

We use **Hive-style partition prefixes** so Athena / Glue Crawler / Spark can discover partitions automatically, and so lifecycle rules can target single partitions.

### 2.1 Layout

```
bronze/
  aecb/ingest_date=YYYY-MM-DD/batch_id=<bureau_batch_id>/*.xml
  fraud/req/decision_date=YYYY-MM-DD/decision_id=<id>/request.json
  fraud/res/decision_date=YYYY-MM-DD/decision_id=<id>/response.json
  aml/ingest_date=YYYY-MM-DD/event_id=<vendor_event_id>/payload.json
  internal_profile/snapshot_date=YYYY-MM-DD/export.csv
  _manifests/source=<source>/run_id=<run_id>/manifest.json
```

### 2.2 Why these specific keys

| Source | Partition | Reason |
|---|---|---|
| AECB | `ingest_date` + `batch_id` | Batches arrive daily; `batch_id` makes replays safe (same bytes → same key) and isolates a bad delivery. |
| Fraud | `decision_date` + `decision_id` | `decision_id` gives **wide uniform distribution** (no S3 prefix throttling) and maps 1:1 to `silver.fraud_score`. |
| AML | `ingest_date` + `event_id` | Webhooks are bursty; `event_id` prevents prefix hotspots and enforces idempotency. |
| Internal profile | `snapshot_date` | Daily full snapshot; one object per day keeps replay trivial. |

### 2.3 Throughput & hot-spot considerations

- S3 partition-level request limits are **~3,500 PUT/s and 5,500 GET/s per prefix**. At 100K decisions/day with ~5 PUTs each (req, res, optional AML), we average ~6 PUTs/s — **well below limits** — but **bursts during business hours** can spike 10x. The `decision_id`/`event_id` suffix spreads load across many prefixes naturally.
- We **avoid** putting `customer_id` in the prefix because it creates skew for power users (corporate accounts, fraud rings) and creates joinability problems with PII policies.

### 2.4 Lifecycle & tiering

| Age | Storage class | Rationale |
|---|---|---|
| 0–30 days | S3 Standard (or Intelligent-Tiering) | Hot for DQ/reconcile + fraud replay |
| 30–365 days | Glacier Instant Retrieval | Rarely read but fast when needed |
| 365+ days | Glacier Deep Archive | Retention (≥7y for Central Bank) |

Partitioning by `date=` makes these rules **one-liner S3 lifecycle filters**: `filter: prefix=bronze/fraud/res/decision_date=2024-` → transition to Glacier IR.

### 2.5 Object Lock & immutability

- Bronze buckets use **S3 Object Lock (governance mode, 7y)**.
- Gold artifacts in S3 use **compliance mode** (even admins cannot shorten retention) until the legal hold is released via a regulator-approved process.

### 2.6 Catalog / query layer

- When we add Athena/Spectrum post-launch, register the Hive partitions via Glue Data Catalog + **partition projection** (`projection.ingest_date.type=date`) so we don't need a crawler.
- Partition projection removes the partition-listing cost (O(1) pruning) which is a huge win as we grow past 1M objects per source.

### 2.7 Common S3 partitioning pitfalls we avoided

- **Timestamp with time-of-day** in the path (e.g., `/hour=13/`) — tempting but increases partition count 24x without giving us better pruning for our workloads.
- **Year/month/day as separate prefix levels** vs a single `YYYY-MM-DD` — both work, but the single form plays better with partition projection and lifecycle filters.
- **Putting PII in the key** (e.g., `/emirates_id=...`) — never; keys show up in audit logs and SIEM tooling.

---

## 3. Postgres partitioning (RDS)

We use **native declarative partitioning** (`PARTITION BY RANGE`) for tables that grow **unbounded over time** and whose access patterns are time-scoped. For everything else we use **indexes**, not partitioning.

### 3.1 What to partition and what not to

| Table | Decision | Strategy |
|---|---|---|
| `gold.decision` | **Partition** | `PARTITION BY RANGE (requested_at)` — monthly |
| `gold.decision_input_snapshot` | **Partition** | `PARTITION BY RANGE (snapshot_ts)` — monthly |
| `gold.decision_input_artifact` | **Partition** | `PARTITION BY RANGE (created_at)` — monthly (large row count, join by snapshot_id stays partition-local if you ensure same partition key) |
| `audit.ingestion_object` | **Partition** | `PARTITION BY RANGE (created_at)` — monthly (biggest row-count table) |
| `audit.ingestion_run` / `audit.ingestion_error` | Partition if growth warrants (monthly) | Else index only |
| `silver.fraud_score` | Index-only | PK `decision_id` is uniform; <100K/day is fine unpartitioned for >2y |
| `silver.aecb_report` | Index-only (at launch) | Revisit if per-customer history grows big |
| `silver.aml_screen` | Index-only | Event-driven, moderate volume |
| `silver.customer*` / identity graph | **No partitioning** | Access is by `customer_key`; random access patterns; partitioning would hurt |
| `dq.result` | Partition by `created_at` monthly | Can grow fast; we keep samples as JSON |
| `mart.*_daily` | **No partitioning** | Small aggregates; PK already narrow |

### 3.2 Why monthly RANGE partitions

- At **100K decisions/day**, a year is ~36M `gold.decision` rows and ~36M snapshot rows. Monthly partitions keep each partition ~3M rows — a comfortable size for Postgres where per-partition VACUUM/ANALYZE is fast.
- Monthly partitions align with **accounting/IFRS9 runs** (month-end), which makes queries like "give me all snapshots from March" a one-partition scan.
- Monthly partitions are the **smallest practical unit for retention**: we can detach + archive + drop a whole month as a single DDL.

### 3.3 How we maintain partitions

- **pg_partman** manages monthly partition creation with 3-month pre-creation and 84-month retention (7 years) for audit tables.
- A monthly MWAA DAG calls `partman.run_maintenance()` and verifies that next month's partitions exist.
- Retention for decision snapshots is **detach-then-S3-archive** (not drop) because of compliance; we dump the detached partition with `pg_dump --data-only` to a Gold-lifecycled S3 prefix before dropping.

### 3.4 Hash vs List vs Range considerations

- **RANGE (time)**: chosen — aligns with pruning + retention.
- **LIST (product_type)**: considered for marts; rejected because our mart row counts are small (daily aggregates) and joins across products are common.
- **HASH (customer_key)**: rejected; our workload is decision-time-driven, not tenant-driven. HASH would hurt date-pruning without giving us meaningful partition isolation.

### 3.5 Postgres partitioning pitfalls we explicitly avoid

- **PK must include partition key**: Postgres requires the partition column in any UNIQUE index. We design PKs accordingly (e.g., `(decision_id, requested_at)` on the top-level view) or accept a `UNIQUE (decision_id)` only within each partition + app-layer guarantees.
- **Cross-partition foreign keys**: FKs from partitioned tables to non-partitioned ones are fine; FKs *to* a partitioned table are not allowed (we use triggers or design the child as partitioned with the same key).
- **Autovacuum tuning**: bump `autovacuum_vacuum_scale_factor` down (e.g., 0.02) on partitioned tables because large partitions otherwise accumulate bloat.
- **Query planner & `enable_partition_pruning`**: ensure planner-time and execution-time pruning both happen; always check `EXPLAIN (ANALYZE)` plans show `Pruned`.

### 3.6 Indexing strategy (after partitioning)

- `gold.decision(customer_key)` — supports portfolio queries for a specific customer.
- `gold.decision(product_type, requested_at)` — supports per-product monitoring queries (planner will combine partition pruning + product filter).
- `gold.decision_input_snapshot(decision_id)` — lookup on audit path.
- `audit.ingestion_object(run_id)` and `(sha256_hex)` — lineage and integrity verification.
- `silver.identity_observation` composite indexes we already defined (phone+email, emirates_id, name+dob).

---

## 4. Redshift partitioning (marts, post-launch)

Redshift doesn't have PG-style partitions; it has **distribution** and **sort** keys that serve the same goals (prune + avoid shuffles).

### 4.1 Distribution (DISTSTYLE / DISTKEY)

| Table | DISTSTYLE | Why |
|---|---|---|
| `mart.risk_portfolio_daily` | `ALL` | Tiny (≤1000 rows/day × 3 products × Nyears); broadcast to every node for join locality. |
| `mart.dq_scorecard_daily` | `ALL` | Same; small dimension-like table. |
| `mart.customer_risk_features_latest` | `KEY (customer_key)` | Large; joined heavily with decisions/snapshots on `customer_key`. |
| `gold.decision` (copy) | `KEY (customer_key)` | Joins to snapshots and customer features co-locate. |
| `gold.decision_input_snapshot` (copy) | `KEY (customer_key)` | Same co-location benefit. |
| Generic fact tables we don't know yet | `AUTO` first, then move to `KEY` once hot-join patterns emerge | Let Redshift learn. |

Rules we follow:

- Never `EVEN` for tables we join with frequently; you'll pay shuffle costs forever.
- `ALL` only for **small** dimensions (< ~3M rows or < ~2GB compressed), otherwise memory pressure on each node.

### 4.2 Sort keys (SORTKEY / COMPOUND vs INTERLEAVED)

| Table | SORTKEY | Kind |
|---|---|---|
| `gold.decision` | `requested_at, product_type` | `COMPOUND` — time is primary filter |
| `gold.decision_input_snapshot` | `snapshot_ts` | `COMPOUND` |
| `mart.risk_portfolio_daily` | `as_of_date, product_type` | `COMPOUND` |
| `mart.customer_risk_features_latest` | `updated_at` | `COMPOUND` |

We prefer **compound sortkeys** because almost every dashboard query filters on a **time** column first; interleaved is rarely worth the rewrite cost.

Sort key discipline:

- First column should be the **most selective filter** (usually time).
- Second column, if added, should be the **next most common filter** (e.g., `product_type`).
- Don't add more than 2–3 columns; sort-key maintenance cost rises.

### 4.3 Compression

- Let Redshift choose via `COPY … COMPUPDATE ON` for the first load; then freeze via `ALTER TABLE` encodings. `ZSTD` is the modern default for most text columns.

### 4.4 Loading pattern (S3 → Redshift)

- Export Postgres → S3 **as partitioned parquet** using an MWAA DAG (one file per month) so we get **per-partition COPY** and easy backfills.
- Use a **manifest file** per load so retries/idempotency are deterministic.
- For Gold snapshots, we load with `COPY … REGION … FORMAT PARQUET ACCEPTINVCHARS` into a staging table, then merge into the target.

### 4.5 Post-load maintenance

- **VACUUM SORT ONLY** after large loads.
- **ANALYZE** on loaded tables.
- **Workload Management (WLM)**: a queue for ETL, a queue for BI, a short-query accelerator for dashboard leaf queries.

### 4.6 Post-launch choice: Serverless vs provisioned

- Start with **Redshift Serverless** for variable load, minimal ops.
- Switch to **RA3** when we have predictable heavy query volume; RA3 separates compute from managed storage, making scale-up cheap.

---

## 5. Cross-layer coherence

The three layers share these invariants on purpose:

- **Time-based partitioning everywhere** (S3 `*_date=`, Postgres `RANGE (ts)`, Redshift `SORTKEY(ts)`): one mental model for pruning and retention.
- **Natural keys in the path/row** (decision_id, event_id, batch_id): idempotent replays; join sanity.
- **Retention by partition drop**: cheap, atomic, reviewable.

When an auditor asks "show me everything about this decision", we hit **one S3 prefix**, **one Postgres partition per relevant table**, and **one Redshift sort range** — and we can prove each layer's hash chain matches.

---

## 6. Concrete examples in this repo

- `sql/05_partitioning.sql` — Postgres partitioning DDL for `audit.ingestion_object`, `gold.decision`, and `gold.decision_input_snapshot` (monthly, pg_partman-friendly).
- `sql/06_redshift_marts.sql` — Redshift-flavor DDL with DISTSTYLE/DISTKEY/SORTKEY for the marts + `COPY` template from S3 parquet.
