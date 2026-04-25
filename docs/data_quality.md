# Data Quality Framework

This document defines **what we check, where, with what severity, and who decides**. DQ at Mal is first-class: every launch-critical invariant is expressed as a rule, evaluated on a schedule, capable of **blocking decisioning or Gold publishing**, and logged into the same auditable store as the decisions themselves.

The implementation lives in `python/mal_pipeline/dq/` and in the `dq.*` schema in Postgres. This doc covers the model; see the sibling docs for the pieces that sit next to DQ:

- `docs/keys_and_constraints.md` — PK/UK/FK/composite-key policy (what we enforce at the DB level).
- `docs/cross_database_dq.md` — reconciliation across S3 / Postgres / Redshift.

---

## 1. DQ dimensions we enforce

Every rule is classified by a single **dimension**. This forces rule authors to pick the right pattern and makes dashboards trivially grouped.

| Dimension | Question it answers | Typical rule shape |
|---|---|---|
| **Completeness** | Are required values present? | null rates, required-field counts |
| **Uniqueness** | Are keys actually unique? | PK/UK duplicate scans |
| **Validity** | Do values respect their type/format/enum/range? | regex, enum, range, JSON-schema conformance |
| **Consistency** | Do related values agree across rows / tables / sources? | cross-table joins, derived-field equality |
| **Timeliness** | Does data arrive within SLA, and is the pipeline fresh? | freshness windows, lag computations |
| **Referential integrity** | Do references resolve to a real parent? (esp. across DBs) | `LEFT JOIN IS NULL`, Bronze object existence checks |

We reject a 7th, "accuracy" as an in-pipeline DQ dimension — that is a model/business truth question we answer via **sampling + replay** (see §7) rather than a SQL rule.

---

## 2. Rule severity policy

Two levels; we deliberately do not add more.

| Severity | Effect on pipeline | Paging? |
|---|---|---|
| **MUST_PASS** | A single failing row **blocks Gold publishing and, for decision-time rules, blocks the decision**. | Pages on-call immediately. |
| **WARN** | No block. Counted, dashboarded, alerted on threshold breach. | Only pages if trend threshold is breached. |

Principles:

- **MUST_PASS is earned, not assumed.** A rule becomes MUST_PASS only after proving it for 14 days in WARN.
- **Every MUST_PASS rule must have a well-understood remediation runbook**. If on-call has to think, the rule isn't ready for MUST_PASS.
- **Warn-threshold rules** make the WARN level useful: a non-zero WARN result only alerts if the rule's threshold is breached (e.g., null rate > 5%).

---

## 3. Rule shapes

There are exactly two kinds of rules — so every reviewer reads them the same way.

### 3.1 `sql_check` — count-violations rule

Returns a (possibly empty) set of violating rows.

- Default comparator: `violation_count <= threshold.value` (threshold 0 → zero tolerance).
- We store up to 10 sample violations in `dq.result.sample_violations` for debugging (never PII; see §9).

```yaml
- rule_id: silver_fraud_score_has_response_ts
  dimension: completeness
  severity: MUST_PASS
  layer: silver
  description: Every silver.fraud_score row has provider_response_ts.
  sql_check: |
    select decision_id
    from silver.fraud_score
    where provider_response_ts is null
  threshold:
    op: lte
    value: 0
```

### 3.2 `sql_metric` — scalar metric rule

Returns a single numeric value. The runner compares the value to the threshold.

```yaml
- rule_id: silver_aecb_null_bureau_score_ratio
  dimension: completeness
  severity: WARN
  layer: silver
  description: Null bureau_score rate stays below 5%.
  sql_metric: |
    select (count(*) filter (where bureau_score is null))::numeric
           / nullif(count(*),0) as ratio
    from silver.aecb_report
    where report_ts > now() - interval '1 day'
  threshold:
    op: lt
    value: 0.05
```

Comparators supported: `eq | ne | lt | lte | gt | gte`.

---

## 4. Scope & scheduling

Rules are executed in batches keyed by **scope**. A scope is a JSON object that says "what slice to check this run". Examples:

- `{ "layer": "silver" }` — run after every bronze→silver DAG.
- `{ "layer": "gold", "decision_id": "..." }` — run as a **decision-time** rule inside the decisioning service.
- `{ "source": "aecb", "batch_id": "B-20250422-01" }` — run as part of a batch ingest gate.
- `{ "window": "24h" }` — periodic sweep.

Each execution creates a `dq.run` row (uuid) that groups all rule results. The MWAA DAG that owns a given scope reads the run's outcome and **branches** (publish vs alert_and_stop) on any MUST_PASS failure.

| Scope | Cadence | Typical gate |
|---|---|---|
| `layer: bronze` | Per ingest run | Promotes Bronze → Silver |
| `layer: silver` | Post bronze→silver DAG | Promotes Silver → Gold & marts |
| `layer: gold` | Hourly sweep + decision-time | Blocks decision (decision-time); blocks marts (sweep) |
| `layer: mart` | Post mart build | Dashboards eligibility |
| `layer: cross_db` | Hourly | Reconciliation alerts |

---

## 5. Gating (the "must-pass" branch)

The same pattern every DAG uses:

```python
run_id, must_pass = run_dq(conn, rules, scope={...})
if not must_pass:
    alert_and_stop()
else:
    publish_gold_or_marts()
```

`run_dq.run_dq` returns `(run_id, must_pass)` where `must_pass` is `True` only if **every MUST_PASS rule passed**. See `dags/run_dq_and_publish_gold.py` for the Airflow shape.

Decision-time rules run synchronously in the decisioning service. A failing MUST_PASS rule there:

1. Creates a `gold.decision` with `decision_status = 'blocked'`.
2. Writes a snapshot so the block is itself auditable (yes — we snapshot blocks too).
3. Alerts on-call.

---

## 6. Thresholds & alert baselines

Three useful threshold shapes live in the YAML:

1. **Absolute** (`op: lte, value: 0`) — zero tolerance; the default.
2. **Ratio** (`op: lt, value: 0.05`) — "less than 5% null", etc.
3. **Baseline delta** — implemented as a `sql_metric` that computes the delta vs a 7-day trailing average. The repo keeps the SQL readable instead of introducing a new DSL.

For baseline-delta style, we compute the window inside the SQL so the rule stays declarative:

```yaml
- rule_id: mart_decision_volume_drift
  dimension: consistency
  severity: WARN
  layer: mart
  description: Daily decisions_total within +/-50% of trailing 7d mean.
  sql_metric: |
    with today as (
      select coalesce(sum(decisions_total),0)::numeric as v
      from mart.risk_portfolio_daily
      where as_of_date = current_date - 1
    ),
    trailing as (
      select avg(sum_d)::numeric as m
      from (
        select as_of_date, sum(decisions_total) as sum_d
        from mart.risk_portfolio_daily
        where as_of_date between current_date - 8 and current_date - 2
        group by as_of_date
      ) s
    )
    select abs((today.v - trailing.m) / nullif(trailing.m,0)) from today, trailing
  threshold:
    op: lt
    value: 0.5
```

---

## 7. Replay-based checks (a.k.a. integrity sampling)

Some invariants are too expensive to check on every row but critical for compliance:

- **Snapshot hash parity**: take a random 1% of yesterday's `gold.decision_input_snapshot` rows, rebuild the canonical JSON, recompute `sha256_hex`, verify equality.
- **Bronze artifact hash parity**: for each sampled snapshot, re-fetch each referenced `s3://.../...` and verify `sha256`.

This is the only DQ check that actually touches **bytes**; everything else is SQL. It's expressed as a Python job rather than YAML because it needs S3 + JSON canonicalization, and it lives in `mal_pipeline/decision/replay.py` today; the DQ wrapper calls it on schedule and writes a `dq.reconciliation` row. See `docs/cross_database_dq.md`.

---

## 8. Referential integrity — DB vs DQ

Some FKs can be enforced in Postgres (`FOREIGN KEY`). Others can't, either because:

- the parent lives in **S3** (e.g., Bronze objects referenced by `gold.decision_input_artifact`), or
- the parent is on a **different database** (post-launch: gold.decision copy in Redshift), or
- the child is in a **partitioned table** and the parent isn't (Postgres partitioning restricts FK direction).

For those, we enforce the invariant via **DQ rules** instead. They're tagged with `dimension: referential` and always MUST_PASS. See `docs/keys_and_constraints.md` and `docs/cross_database_dq.md` for the full matrix.

---

## 9. Safety rails

- **No PII in violations**: `sample_violations` is capped at 10 and never stores raw PII payloads. Rule authors return **IDs** (decision_id, aml_event_id, object_id), not fields like `emirates_id` directly.
- **Bounded SQL**: every rule's query has a time-window predicate (or a `LIMIT`). The runner enforces a statement timeout.
- **Isolation**: DQ runs in its own connection pool and its own read role. A malformed rule cannot write.
- **Circuit breaker**: if the runner itself crashes on ≥N rules, we alert and let the last-successful DQ run carry forward (Gold does **not** publish until the next clean run).

---

## 10. Storage model (`dq.*`)

- `dq.rule` — current metadata (rule_id, dimension, severity, layer, sql_check|sql_metric, threshold, scope, enabled).
- `dq.run` — one row per DQ execution (uuid, started_at, scope, status).
- `dq.result` — one row per (rule, run) with `passed`, measured `metric` value, `violation_count`, `sample_violations`.
- `dq.reconciliation` — cross-database parity rows (see `docs/cross_database_dq.md`).
- `mart.dq_scorecard_daily` — rollup for dashboards.

The YAML file is the **source of truth**; the runner upserts into `dq.rule` on each run so the DB reflects what's currently shipped.

---

## 11. Anti-patterns we avoid

- **Rules that "run forever"**: always bound by time window + index coverage.
- **Rules without thresholds**: every rule has one, even if it's `lte: 0`.
- **Business rules hiding as DQ rules**: "approval rate is low" is a business metric, not DQ.
- **Duplicated rules across layers**: rules live where the value is created; consumers assume upstream DQ passed.
- **Rules without owners**: every rule in YAML has a commented-out `# owner:` line in the contract doc it supports.

---

## 12. How to add a rule (checklist)

1. Identify the **dimension** and **layer**.
2. Draft the rule against the **data contract** (`docs/data_contracts/`) — the rule should enforce what the contract promises.
3. Start severity as **WARN** with a reasonable threshold.
4. Provide a runbook link in the description (what to do when it fails).
5. PR includes the rule, a fixture test, and a mention in the lineage YAML if it introduces a new cross-system dependency.
6. After 14 days of clean runs with no flakes, open a follow-up PR to promote to **MUST_PASS** (if appropriate).
