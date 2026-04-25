# How DQ Rules Integrate with Tables

This doc answers: *"Where, physically, do DQ rules touch the data — and how tightly?"*. It sits between [`docs/data_quality.md`](data_quality.md) (the framework) and [`docs/dq_consistency.md`](dq_consistency.md) (cross-database state).

---

## 1. Four integration points

We run rules at four distinct **moments**, with different coupling and consequences:

| Integration point | Runs on | Coupling to data | Can block | Example |
|---|---|---|---|---|
| **Ingest-time** | Per-record, synchronously | Tightest — validates payload **before** Silver insert | Yes (rejects row) | `schema_validator.validate_or_raise` inside `fraud_api.score_and_persist` |
| **Pre-publish** | Batch gate between Silver → Gold / mart build | Loose — runs SQL over staged rows | Yes (skips the publish task) | MWAA DAG `run_dq_and_publish_gold` |
| **Decision-time** | Per decision, synchronously, inside the decisioning service | Tight — verdict goes into the Gold snapshot payload | Yes (block the decision) | `dq_summary` in `gold.decision_input_snapshot` |
| **Scheduled sweep** | Hourly / daily across whole layers | Loose — drift / anomaly scans | Can raise alerts; doesn't block writes directly | `dq.run` with scope `{ layer: silver, window: 1h }` |

Rule authors pick the **earliest** of these that covers the invariant, because later = more expensive to fix. Schema validation is always ingest-time; referential integrity is usually pre-publish; freshness is scheduled.

---

## 2. Coupling modes between a rule and the data it checks

### 2.1 DB-level enforcement (hardest coupling)

When Postgres can enforce it, we let Postgres enforce it. These are encoded in DDL under `sql/01..08_*.sql`:

- `NOT NULL`, `CHECK`, `UNIQUE`, `FOREIGN KEY`.
- Partial `UNIQUE` indexes for "sometimes unique" columns (e.g., `silver.customer` in `sql/08_dq_framework_upgrade.sql`).
- `GENERATED ALWAYS AS ... STORED` columns for derived fields (e.g., could be used for `full_name_norm` to remove drift between producers).

Pros: impossible to bypass, zero orchestration cost.
Cons: inflexible; errors happen at INSERT time, not in a gated DAG. So we use DB-level enforcement for invariants that are truly structural — never for business thresholds.

### 2.2 Trigger / DML-event enforcement

For row-level invariants that are too complex for `CHECK` but must run on every write, we use triggers. Example (not yet shipped but supported by the schema):

```sql
create or replace function silver.enforce_observation_valid()
returns trigger language plpgsql as $$
begin
  -- must have at least one identifier populated
  if new.emirates_id is null
     and new.internal_uuid is null
     and new.phone_e164 is null
     and new.full_name_norm is null
  then
    raise exception 'identity_observation has no identifiers';
  end if;
  return new;
end $$;

create trigger trg_enforce_observation_valid
before insert or update on silver.identity_observation
for each row execute function silver.enforce_observation_valid();
```

We use triggers sparingly — they are harder to audit than declarative CHECKs — but they're available when an invariant spans multiple columns.

### 2.3 Runner-level SQL rules (the default)

The runner in `python/mal_pipeline/dq/run_dq.py` executes one of two shapes against the target schema:

- **`sql_check`**: returns violating rows — the runner counts them and compares to `threshold`.
- **`sql_metric`**: returns a single scalar — compared to `threshold` with an operator.

The rule is *completely decoupled* from the table: it's text in YAML, compiled into a SQL statement and persisted in `dq.rule`. The upside is flexibility (add, edit, retire rules without migrations); the downside is that a rule author can reference a column that no longer exists — so we add a lightweight **rule → target registry** (§3) and run a nightly "do all rules still compile?" smoke job.

### 2.4 Ingest-time structural validators

Schema validation (`dq/schema_validator.py`) sits between transport and Silver:

```python
from mal_pipeline.dq.schema_validator import validate_or_raise
validate_or_raise("fraud_response", "1.0", response_json)  # raises SchemaValidationError
```

It's a DQ rule too — just implemented as Python + JSON Schema instead of SQL, because the source of truth is the vendor payload shape, not a DB row. Violations are logged into `audit.ingestion_error` with `stage = 'validate'` so downstream analytics stays coherent with SQL-rule results.

### 2.5 Decision-time verdict stamping

The strongest coupling is at **Gold snapshot write time**. The decisioning service runs a small set of MUST_PASS rules inline and **embeds the outcome in the snapshot JSON**:

```json
"dq_summary": {
  "must_pass": true,
  "failures": [],
  "rule_ids_checked": ["silver_fraud_score_has_response_ts", "gold_decision_requested_amount_positive"],
  "dq_run_id": "..."
}
```

Because that JSON is part of the canonical SHA-256 over the snapshot payload, the DQ verdict is **cryptographically tied to the decision artifact**. You can't tamper with one without invalidating the other.

---

## 3. Rule → target registry

Rules in YAML are free-form SQL. To answer "which rules guard `silver.fraud_score`?" we maintain `dq.rule_target` (see `sql/09_dq_integration.sql`):

```sql
create table if not exists dq.rule_target (
  rule_id       text not null references dq.rule(rule_id),
  schema_name   text not null,
  table_name    text not null,
  column_name   text,            -- nullable for table-level rules
  kind          text not null,   -- 'column' | 'composite_key' | 'table' | 'cross_table'
  primary key (rule_id, schema_name, table_name, coalesce(column_name, ''))
);
```

Benefits:

- The partitioning/schema-evolution doc can answer **blast radius**: "if I rename `silver.fraud_score.score`, these rules depend on it."
- CI can ensure every newly added rule registers its target(s).
- The risk team's UI can show, per table, all currently-active rules and their pass/fail status.

The YAML author keeps this easy — a small optional `targets:` list in each rule, which `upsert_rules()` mirrors into `dq.rule_target`:

```yaml
- rule_id: silver_fraud_score_has_response_ts
  targets:
    - { schema: silver, table: fraud_score, column: provider_response_ts, kind: column }
  severity: MUST_PASS
  ...
```

If `targets` is missing, the runner does a best-effort extraction from the SQL (regex on `from <schema>.<table>`). Authors are encouraged to be explicit.

---

## 4. Per-row verdict strategies

Some teams want **every row to know its DQ verdict**. That's usually more weight than value at our scale. We pick one of three patterns per table based on its role.

| Table role | Pattern | Rationale |
|---|---|---|
| Audit/log tables (`audit.*`) | No verdict — they **are** the DQ record | Avoids recursive coupling. |
| Silver canonical tables | **Run-level** verdict: `dq_run_id` column + lookup in `dq.run` / `dq.result` | Cheap, coarse-grained; enough for DAG gating. |
| Gold decision artifacts | **Embedded** verdict in payload JSON + `dq_run_id` column | Needed for per-decision audit; already implemented. |
| Marts | **Table-level** verdict through `mart.dq_scorecard_daily` join | Dashboards filter on clean days. |

We explicitly do **not** write a per-row `dq_status` enum to Silver. That would require a synchronous runner at insert time for every row, bloat the table, and create ambiguity when rules change ("is this row's `dq_status='ok'` based on today's rules or the rules that existed when it was inserted?"). Run-level linkage sidesteps all of those.

### 4.1 How the `dq_run_id` column gets populated

Two paths:

1. **Producers know** the `dq_run_id`: decisioning service wrote it into the snapshot payload (Gold). Here the column is populated in the same transaction as the Gold row — no drift possible.
2. **Producers don't know**: for Silver tables filled by MWAA DAG tasks, the DAG wraps the ingest + DQ into a single Airflow task group. When the DQ job ends green, it calls `set_publication_marker(layer='silver', dq_run_id=...)`. The Silver rows produced in the batch have the `dq_run_id` column set to the marker's value.

The marker pattern is covered in [`docs/dq_consistency.md`](dq_consistency.md).

---

## 5. Example end-to-end wiring for one rule

Pick `silver_fraud_score_has_response_ts` (MUST_PASS, completeness).

1. **Contract**: `docs/data_contracts/fraud.md` — states `provider_response_ts` is required.
2. **Schema**: `python/mal_pipeline/schemas/fraud_response.v1.json` — `provider_response_ts` required.
3. **Ingest-time** (decisioning service calls `fraud_api.score_and_persist`):
   - `schema_validator.validate_or_raise("fraud_response", version, resp_json)` — rejects malformed responses before we try to build the Silver row.
4. **DB-level**: `silver.fraud_score.provider_response_ts` is `timestamptz` (no `NOT NULL` at DB level — we kept it nullable for recovery scenarios, but the rule below enforces it for published data).
5. **Pre-publish rule** (`rules.yml`):
   ```yaml
   - rule_id: silver_fraud_score_has_response_ts
     dimension: completeness
     severity: MUST_PASS
     layer: silver
     sql_check: "select decision_id from silver.fraud_score where provider_response_ts is null"
     threshold: { op: lte, value: 0 }
     targets:
       - { schema: silver, table: fraud_score, column: provider_response_ts, kind: column }
   ```
6. **Runner** evaluates it in the DQ DAG; if it fails, the MWAA branch routes to `alert_and_stop` and Gold is not built.
7. **Consumer view** (`mart.v_publishable_silver_fraud_score`, defined in `sql/09_dq_integration.sql`) hides Silver rows that belong to a non-green batch, so risk analysts can't accidentally read unpublished data.

Every rule has this same seven-step lifecycle. When you add a rule, you walk each step.

---

## 6. Ownership & change management

- Rules live next to the code they protect; each rule has an explicit `owner` in YAML.
- Schema changes (in `sql/`) and rule changes (in `rules.yml`) are **always in the same PR** — reviewers can see the invariant and its enforcement together.
- `docs/lineage.md` lists rule-to-downstream impact; a rule deletion is treated the same as a contract change (14-day deprecation window in WARN before removal).
