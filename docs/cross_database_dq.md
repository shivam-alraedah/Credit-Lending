# Cross-Database Data Quality (Reconciliation)

When data lives in more than one system — S3 Bronze, RDS Postgres, and (post-launch) Redshift — referential integrity cannot be enforced by a `FOREIGN KEY`. This doc defines how we **reconcile** across stores so we detect divergence the moment it happens, not when a dashboard looks wrong.

See also:

- `docs/data_quality.md` — the DQ framework (dimensions, severities, gating).
- `docs/keys_and_constraints.md` — where DB enforces vs where DQ enforces.

---

## 1. Pairs we reconcile

| Left | Right | Cadence | Purpose |
|---|---|---|---|
| S3 Bronze objects (per source) | `audit.ingestion_object` rows | Hourly | Every S3 object has an audit row (no silent drops). |
| `audit.ingestion_object` | Silver canonical table for source | Hourly | Every audited object turned into the expected Silver rows within SLA. |
| `silver.*` (per source) | `gold.decision_input_snapshot.inputs.<source>` | Hourly | Gold snapshots actually reference the Silver values they claim. |
| `gold.decision_input_artifact.s3_key` | S3 Bronze SHA-256 | Daily sample (1%) | Bronze bytes have not been tampered with. |
| `gold.decision_input_snapshot.sha256_hex` | Canonical-JSON recompute | Daily sample (1%) | Gold payload itself has not been tampered with. |
| RDS `gold.decision` (yesterday) | Redshift `gold.decision` (yesterday) | Daily post-ETL | RDS → Redshift ELT lossless. |
| RDS `mart.risk_portfolio_daily` | Redshift `mart.risk_portfolio_daily` | Daily | Dashboards reading Redshift agree with source of truth. |

Each row produces a record in `dq.reconciliation` (see `sql/08_dq_framework_upgrade.sql`) with left/right metrics, discrepancy, tolerance, and a pass flag.

---

## 2. Reconciliation strategies

We combine three techniques depending on cost and sensitivity.

### 2.1 Row count parity

Cheapest. Compare counts over the same window on both sides:

```sql
-- Left: audit.ingestion_object (bronze metadata)
select count(*) from audit.ingestion_object o
join audit.ingestion_run r on r.run_id = o.run_id
where r.source = 'aml' and o.created_at >= now() - interval '1 hour';

-- Right: silver.aml_screen
select count(*) from silver.aml_screen
where created_at >= now() - interval '1 hour';
```

- **Tolerance**: `|left - right|  <= max(0, left * 0.01)` (we allow up to 1% in-flight during the reconcile window).
- **Implementation**: `reconcile.bronze_to_silver_row_parity(...)` in `python/mal_pipeline/dq/reconcile.py`.

Pitfalls:

- Time skew between services: add a small lookback so rows arriving near the boundary aren't counted on only one side.
- Idempotent re-ingests count twice on left (unique S3 key deduplicates on right): count `distinct (s3_bucket, s3_key)` on the left.

### 2.2 Hash / content parity

Far stronger than row counts; catches tampering and silent value drift.

- **Gold snapshot replay**: for a sample of recent snapshots, recompute `sha256_json_canonical` on the reconstructed payload and compare to `sha256_hex` stored at write time.
- **Bronze artifact parity**: for each artifact referenced by a sampled snapshot, re-fetch S3 and compare SHA-256 to `gold.decision_input_artifact.sha256_hex`.

Implemented by `reconcile.snapshot_replay_sample(...)`, which in turn reuses `mal_pipeline.decision.replay.replay_snapshot`. Sample size is tuned so we hit every month's partition at least a few times per day; 1% default is usually enough at 100K decisions/day (~1K replays/day).

### 2.3 Aggregated hash ("fingerprint")

Between RDS and Redshift we don't want to ship every row to compare; we use an aggregated hash per time bucket:

```sql
-- Both systems compute the same fingerprint
select date_trunc('day', requested_at) as d,
       md5(string_agg(decision_id || '|' || decision_status, ',' order by decision_id)) as fp,
       count(*) as n
from gold.decision
where requested_at >= current_date - interval '7 days'
group by 1;
```

`reconcile.pg_to_redshift_fingerprint_parity(...)` runs that query on both connections and compares `(d, fp, n)`. A mismatch is a hard fail and alerts immediately.

**Caveats**:

- Keep the column list small and stable; it's what defines "sameness" for this check.
- Use a deterministic ordering inside `string_agg` — unordered aggregates are nondeterministic and will flap.

---

## 3. What goes into `dq.reconciliation`

See `sql/08_dq_framework_upgrade.sql`. The shape is deliberately small:

```sql
create table dq.reconciliation (
  reconciliation_id  bigserial primary key,
  dq_run_id          uuid references dq.run(dq_run_id),
  name               text not null,           -- e.g. bronze_silver_aml
  left_source        text not null,           -- e.g. s3://bronze/aml
  right_source       text not null,           -- e.g. rds.silver.aml_screen
  metric_type        text not null,           -- row_count | fingerprint | hash
  left_metric        numeric,
  right_metric       numeric,
  left_fingerprint   text,
  right_fingerprint  text,
  discrepancy        numeric,                 -- |left - right| or derived value
  tolerance          numeric,                 -- absolute or ratio
  passed             boolean not null,
  window_start       timestamptz,
  window_end         timestamptz,
  details            jsonb,                   -- freeform: sampled ids, S3 keys, etc.
  created_at         timestamptz not null default now()
);
```

Reconciliation results feed the mart `mart.dq_scorecard_daily` alongside rule results, so the risk team sees both.

---

## 4. DAG wiring (MWAA)

New DAG `reconcile_cross_db` runs:

1. `bronze_to_silver_parity` for each source.
2. `silver_to_gold_parity` (did every fraud `silver.fraud_score` row in the window show up in a snapshot?).
3. `pg_to_redshift_fingerprint_parity` per mart (post-launch only).
4. `snapshot_replay_sample` (1% of recent snapshots).

On any MUST_PASS reconciliation failure, the DAG branches to `alert_and_hold_gold_publish`. We prefer holding Gold to publishing a dashboard that disagrees with RDS — the business is better served by a stale dashboard than a wrong one.

---

## 5. Tolerance policy

The tolerance column stores the **policy**, not the actual discrepancy. That way a reviewer can tell from one row whether tolerance was tight or loose:

| Pair | Default tolerance | Rationale |
|---|---|---|
| Bronze ↔ audit.ingestion_object | 0 | Every S3 object must have an audit row. |
| audit.ingestion_object ↔ Silver | 1% (window) | Allows in-flight rows mid-reconcile. |
| Silver ↔ Gold inputs | 0 | No reason for a snapshot to claim a silver value that isn't there. |
| Gold artifact ↔ S3 SHA | 0 | Integrity must be bit-exact. |
| RDS ↔ Redshift (same day, T-1) | 0 row count, 0 fingerprint | ELT must be exact. |

Loosening any of these requires explicit sign-off + a timed exception in `dq.reconciliation.details`.

---

## 6. Reading the dashboard

`mart.dq_scorecard_daily` now has three "kinds" rolled up per layer per day:

- `must_pass_failed` — count of MUST_PASS rule failures.
- `warn_failed` — count of WARN rule failures above threshold.
- `reconciliation_failed` — count of reconciliation rows with `passed = false`.

If the risk team sees any of those non-zero, the runbook points back to this file (for reconciliation failures), `docs/data_quality.md` (for rule failures), and the relevant contract in `docs/data_contracts/` (if the failure class is a schema violation).

---

## 7. What we *don't* do

- **No live two-phase commit across Postgres and Redshift.** Eventual consistency + reconciliation is the pattern; 2PC across these systems is operationally nightmarish and not worth the marginal gain.
- **No row-level diff against Redshift for large tables.** We stop at fingerprints. If a fingerprint mismatches, the runbook is "re-run the ELT for that partition, then re-check", not "find the 3 rows that differ".
- **No reconciliation for PII across stores.** We compare IDs, counts, and hashes — never PII values side by side — because logs are a DLP risk.
