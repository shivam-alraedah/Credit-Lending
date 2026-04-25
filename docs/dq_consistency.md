# Keeping DQ Status Consistent Across Databases

Once the pipeline spans **S3 Bronze + RDS Postgres + Redshift** (and the occasional SaaS dashboard on top), the hard question stops being "did DQ pass?" and becomes "**does every consumer see the same answer**?". This doc lays out the model we use to make that answer unambiguous.

See also:

- [`docs/data_quality.md`](data_quality.md) — the framework and rule shapes.
- [`docs/dq_integration.md`](dq_integration.md) — where rules physically attach to data.
- [`docs/cross_database_dq.md`](cross_database_dq.md) — reconciliation rather than status *per se*.

---

## 1. One source of truth: `dq.*` in RDS Postgres

- `dq.rule`, `dq.run`, `dq.result`, `dq.reconciliation`, `dq.anomaly`, `dq.publication_marker` all live in **one** Postgres instance (Multi-AZ, PITR on).
- Nothing else "re-derives" DQ status. Redshift, Athena, and dashboards all read from these tables (directly or via replicated copies).
- S3 is never the DQ system of record; we reconcile S3 against audit tables via `dq.reconciliation` instead.

**Why one place**: DQ is an operational truth that must not diverge. If two stores both computed pass/fail, the first question after any incident would be "which one is right?" — and nobody wants that question.

---

## 2. The "last green run" abstraction

Downstream consumers don't want to reason about individual rule results. They want one question answered: **"Is it safe to read this layer right now?"**. We expose that as a tiny table and one view:

```sql
-- sql/09_dq_integration.sql
create table dq.publication_marker (
  layer            text primary key,           -- bronze | silver | gold | mart | cross_db
  last_green_run_id uuid not null references dq.run(dq_run_id),
  last_green_at    timestamptz not null,
  updated_by       text,
  notes            text
);

create view dq.v_latest_green as
  select layer, last_green_run_id, last_green_at
  from dq.publication_marker;
```

Contract:

- The runner updates this table **only** when a DQ run finishes with `must_pass = True` for the layer.
- Updates are `UPSERT` on `(layer)`, transactional with the DQ run write.
- Consumers read `dq.v_latest_green` once at the start of their job and use the returned `last_green_run_id` consistently throughout.

This is equivalent to the "current snapshot" pattern used in versioned data lakes (Iceberg/Delta), but for DQ state rather than data content.

---

## 3. Stamping produced rows with `dq_run_id`

Some tables benefit from a per-row link back to the DQ run that vouched for them:

| Table | `dq_run_id` column? | Why |
|---|---|---|
| `gold.decision` | Yes | Decision-time verdict is critical for compliance. |
| `gold.decision_input_snapshot` | Yes (also embedded in JSON + hash) | Cryptographically tied to the decision artifact. |
| `mart.risk_portfolio_daily` | Yes | Dashboard queries filter by DQ-green days. |
| `mart.customer_risk_features_latest` | Yes | Same. |
| `silver.*` | No column by default | Use `dq.publication_marker` instead; adding a column to every Silver row balloons row size. |
| `audit.*` | No | They're the source of truth; recursion not allowed. |

Stamping rules:

- **Same-transaction** with the row write (add to the `INSERT` / `UPSERT` statement). No separate "stamping" pass.
- **Null is not allowed** on `gold.*` tables once the stamp column exists; `mart.*` allows null for backfills but the view in §4 filters them out.

---

## 4. Consumer-safe views

We publish views that *only* expose rows that belong to a green DQ run. Dashboards and downstream services should read the view, not the raw table:

```sql
create or replace view mart.v_publishable_risk_portfolio_daily as
with g as (select last_green_run_id from dq.publication_marker where layer = 'mart')
select m.*
from mart.risk_portfolio_daily m, g
where m.dq_run_id = g.last_green_run_id;

create or replace view gold.v_publishable_decisions as
with g as (select last_green_run_id from dq.publication_marker where layer = 'gold')
select d.*
from gold.decision d, g
where d.dq_run_id = g.last_green_run_id;
```

**Result**: every dashboard query is implicitly scoped to the "current agreed reality". If `must_pass` fails for a layer, no new rows become visible to consumers until the next green run — without having to change a single dashboard query.

---

## 5. Propagation to Redshift

Three ways DQ state reaches Redshift:

### 5.1 Copy the `dq.*` schema

ELT copies the four core tables (`dq.rule`, `dq.run`, `dq.result`, `dq.publication_marker`) to Redshift nightly + on-demand. Dashboards built in Redshift read the same logical schema.

### 5.2 Include `dq_run_id` on mart loads

Each mart table in Redshift (see `sql/06_redshift_marts.sql`) carries `dq_run_id`. The `COPY`-merge pattern for Redshift becomes:

```sql
-- After ELT from RDS to Redshift staging...
begin;
  delete from mart.risk_portfolio_daily d using stage.risk_portfolio_daily s
   where d.as_of_date = s.as_of_date and d.product_type = s.product_type;
  insert into mart.risk_portfolio_daily select * from stage.risk_portfolio_daily
    where dq_run_id = (select last_green_run_id from dq.publication_marker where layer = 'mart');
commit;
```

Rows lacking a green stamp are held in staging (not pushed to the mart). The reconciliation DAG compares staging ↔ mart counts to detect "we never got a green run for day D" and pages.

### 5.3 Fingerprint parity alarms

`reconcile.fingerprint_parity` already compares `(bucket, fp, n)` between RDS and Redshift. If RDS publishes a green run but Redshift's mart fingerprint disagrees, that's a consistency break between two databases — not a DQ break per se, but functionally the same to a consumer.

---

## 6. Propagation to S3 artifacts

S3 Bronze objects are immutable; we don't stamp them with DQ state. Instead we carry DQ facts in two places that can be joined back:

- `audit.ingestion_object` has `sha256_hex` — reconciliation proves the object is unchanged.
- `gold.decision_input_artifact` points from a snapshot (which already embeds `dq_summary`) to the exact S3 key.

When regulators ask "was DQ clean when this decision was made?", we answer with the snapshot's embedded `dq_summary` + `dq_run_id` → joined to `dq.result` rows → with `audit.ingestion_object` hashes verified via replay. No mutation of S3 required.

---

## 7. Failure modes and recovery

The pattern only works if we thought through what happens when things break:

| Scenario | Observed behavior | Recovery |
|---|---|---|
| DQ runs fail (no green for layer L for >N hours) | `dq.v_latest_green` returns a stale `last_green_run_id`; consumers keep reading **yesterday's** data (stale, not wrong). | Alert on `now() - last_green_at > sla_hours`. Fix, rerun DQ. |
| A new Silver row lands but the DAG crashes before stamping with `dq_run_id` | Row exists but isn't visible through consumer views. | Run a replay DAG that stamps rows in the window matching the green run. |
| A MUST_PASS rule fails after marts were already published earlier that day | The marker doesn't roll back automatically (we don't rewrite history). | On operator confirmation, set `dq.publication_marker.last_green_run_id` to the previous green run (a **manual, audited** action). |
| Redshift falls behind on ELT | Redshift dashboards show yesterday; RDS dashboards show today. | Reconciliation alert fires; the expected fix is ELT restart, not rolling back Postgres. |
| Cross-region DR failover | RDS replica promoted; `dq.*` replicated, so markers and results survive. | No special DQ action required. |

Key principle: **we never delete DQ history to "fix" consistency**. We roll the marker forward or back, which is always one row, auditable, and reversible.

---

## 8. Consistency guarantees summary

- **Within RDS**: ACID — `dq.run`, `dq.result`, and `dq.publication_marker` updates are one transaction. All or nothing.
- **Between RDS and Redshift**: eventual — bounded by ELT cadence; the reconciliation DAG catches divergence; the `dq_run_id` on every mart row lets us quantify drift exactly.
- **Between RDS and S3**: eventual hashing parity — the replay sampler catches drift within 24h.
- **Consumer-visible**: "atomic switch" via the `v_publishable_*` views — consumers jump cleanly from last green to current green when the marker moves.

That gives us a multi-store system where a single table (`dq.publication_marker`) is the canonical answer to "is this clean?" and every surface honors it.
