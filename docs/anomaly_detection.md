# Anomaly Detection

Threshold rules catch the violations you can describe. **Anomalies are the violations you didn't think to describe** — volume drops at 3 AM, a new reason-code appearing on 5% of fraud responses, null rates trending up without crossing any fixed threshold. This doc defines the tiered approach we take.

See also:

- [`docs/data_quality.md`](data_quality.md) — rule framework.
- [`docs/dq_integration.md`](dq_integration.md) — where rules physically attach.
- [`docs/dq_consistency.md`](dq_consistency.md) — publication state.

---

## 1. Three tiers, with increasing cost and sophistication

| Tier | Technique | Where it lives | When to use |
|---|---|---|---|
| **T1. Static threshold** | YAML `sql_check` / `sql_metric` + threshold | `python/mal_pipeline/dq/rules.yml` | Known-good bounds (score range, SLA, regex). |
| **T2. Statistical baseline** | z-score / MAD over a trailing window | `python/mal_pipeline/dq/anomaly.py` + `dq.anomaly` | Metrics with natural variability where "normal" shifts over time. |
| **T3. Model-driven** | Isolation forest / LSTM / Prophet (post-launch) | External service | Multi-dimensional drift, seasonality, complex feature correlations. |

We start at T1 for everything. We add T2 only when T1 would need to be retuned more than once a quarter. We reach for T3 only when T2 is noisy and the business value justifies the MLOps load.

---

## 2. Statistical baselines we actually use at launch

### 2.1 Z-score over a trailing window

For a metric $x_t$ at time $t$, baseline window $W$:

$$
z_t = \frac{x_t - \mu(W)}{\sigma(W)}
$$

Flag when $|z_t| \ge k$ (typically $k = 3$). Good for metrics with roughly-normal distribution (decision volume, null rate).

### 2.2 MAD (Median Absolute Deviation)

More robust to outliers pulling the mean/stddev:

$$
\text{MAD}(W) = \text{median}(|x_i - \tilde{x}|), \quad
z^{\text{MAD}}_t = \frac{x_t - \tilde{x}}{1.4826 \cdot \text{MAD}(W)}
$$

We prefer MAD for rates/ratios that get skewed by occasional spikes (AECB delivery delays, AML match bursts).

### 2.3 Both in SQL

Both are expressible in Postgres with window functions — no ML library required:

```sql
with s as (
  select as_of_date,
         sum(decisions_total) as x,
         avg(sum(decisions_total)) over w as mu,
         stddev_pop(sum(decisions_total)) over w as sigma
  from mart.risk_portfolio_daily
  where as_of_date between current_date - interval '30 days' and current_date
  group by as_of_date
  window w as (order by as_of_date rows between 28 preceding and 1 preceding)
)
select as_of_date,
       x, mu, sigma,
       case when sigma = 0 then 0 else (x - mu) / sigma end as z_score
from s
order by as_of_date desc
limit 1;
```

This goes straight through our existing runner as a `sql_metric` rule — the runner compares `|z_score|` to a threshold (e.g., 3). For MAD we'd wrap the MAD math the same way.

---

## 3. `dq.anomaly` table

Anomalies are persisted separately from rule results because they're often **informational** (we want to trend them) but sometimes **actionable** (we want to page). Schema in `sql/09_dq_integration.sql`:

```sql
create table dq.anomaly (
  anomaly_id       bigserial primary key,
  dq_run_id        uuid references dq.run(dq_run_id),
  detector_id      text not null,          -- e.g. decision_volume_z_score
  method           text not null,          -- z_score | mad | schema_drift | enum_drift
  severity         text not null,          -- info | warn | page
  metric_name      text not null,
  observed_value   numeric,
  baseline_value   numeric,
  deviation        numeric,                -- abs(observed - baseline)
  z_score          numeric,                -- when applicable
  window_start     timestamptz,
  window_end       timestamptz,
  details          jsonb,
  created_at       timestamptz not null default now()
);
```

Anomalies are also rolled up into `mart.dq_scorecard_daily` with a new `anomaly_count` column (see `sql/09_dq_integration.sql`).

---

## 4. Categories of anomalies we monitor

Each category has a dedicated detector under `mal_pipeline/dq/anomaly.py`:

- **Volume anomalies**: decisions/day, AECB file size, AML event rate.
- **Rate anomalies**: approval rate, fraud p99 score, AML `potential_match` ratio.
- **Freshness anomalies**: time-since-last-successful-run per source.
- **Null-rate anomalies**: per-column null rate vs 28-day baseline.
- **Schema drift anomalies**: new fields/enums observed in a payload (plugs into `schema_validator` + `audit.source_schema_observed`).
- **Enum drift**: new distinct values in fields with a small closed domain.
- **Correlated drift** (post-launch): changes in joint distributions (e.g., approval rate up while fraud score distribution shifts right).

---

## 5. Turning a detection into an alert

Every anomaly row carries one of three `severity` levels:

- `info`: stored and trended; never pages.
- `warn`: fires a non-paging alert (Slack); shows up in scorecards.
- `page`: wakes on-call.

Detector outputs are written to `dq.anomaly`; a downstream MWAA task fans them into SNS based on `severity`. Anomalies **never** block Gold publishing on their own — the block is reserved for MUST_PASS rule failures. This prevents a noisy detector from halting the business.

Edge case: if an anomaly is severe and reproducible, the team converts it into a **static MUST_PASS rule** (promoted via the same "14 days WARN" policy). That's the intended handoff between tiers.

---

## 6. Runtime shape (mirror of the rule runner)

Anomalies use a small YAML extension in `rules.yml`:

```yaml
anomalies:
  - detector_id: decision_volume_z_score
    method: z_score
    metric_sql: |
      select as_of_date::timestamptz, sum(decisions_total)::numeric
      from mart.risk_portfolio_daily
      where as_of_date > current_date - interval '30 days'
      group by as_of_date
      order by as_of_date
    baseline_window: 28
    threshold_sigma: 3
    severity_below_threshold: info
    severity_at_threshold: warn
    severity_at_two_times_threshold: page

  - detector_id: silver_aml_null_match_details_rate_mad
    method: mad
    metric_sql: |
      select date_trunc('day', screening_ts), avg((match_details is null)::int)::numeric
      from silver.aml_screen
      where screening_ts > now() - interval '30 days'
      group by 1
      order by 1
    baseline_window: 28
    threshold_k: 3
    severity_at_threshold: warn
```

`mal_pipeline.dq.anomaly.run_anomaly_suite(conn, rules_path)` iterates detectors, evaluates the metric SQL against the trailing window, computes the score, picks a severity, and writes rows.

---

## 7. Schema & enum drift (tier 1.5)

These are **anomalies in contract space** rather than in numeric space:

- **Schema drift**: a new top-level field appears in a sampled Bronze payload. Detected via `jsonschema`'s `additionalProperties: true` on purpose; we sample and log any extras in `audit.source_schema_observed.details.unknown_keys`.
- **Enum drift**: an observed enum value isn't in our registered set (e.g., a new AML `status` appearing). Detected via the validity rules plus a periodic scan:

  ```sql
  select status, count(*)
  from silver.aml_screen
  where screening_ts > now() - interval '24 hours'
    and status not in ('clear','potential_match','match','error','in_progress')
  group by 1;
  ```

Drift events produce `dq.anomaly` rows with `method in ('schema_drift', 'enum_drift')`. These are almost always `warn`: they aren't wrong data, they're new data we haven't agreed to accept yet.

---

## 8. When statistical baselines are wrong, they're spectacularly wrong

Pitfalls we watch for:

- **Seasonality**: Friday dips, month-end spikes, Ramadan effects. A flat 28-day baseline will cry wolf. Mitigation: use same-day-of-week baselines (trailing 4 Fridays) for anything business-calendar-driven.
- **Cold-start**: brand-new detectors with < N samples are always flagging. The runner suppresses anomalies when `n_samples < 2 * baseline_window` and labels them `info` only.
- **Drift that is the business**: a successful marketing campaign producing more approvals is **not** an anomaly. Detectors must be paired with a business-context runbook; on-call should be able to answer "is this expected?" in under two minutes.

---

## 9. Post-launch: when and why to go to T3

We promote to T3 (model-driven) only when we have:

1. A clearly bounded multi-dimensional question (e.g., "is this applicant's feature vector unlike anything we've approved before?").
2. Ground truth labels to evaluate a model.
3. Operational tolerance for false positives on par with a business feature.

Likely first T3 candidates:

- **Isolation forest** on decision feature vectors (fraud indicator signal).
- **Prophet** on decision volume + Ramadan/UAE holiday regressors for better seasonality baselines.
- **Distribution-shift detectors** (KS, PSI) on model input feature drift over time — this crosses into the **model monitoring** problem we anchor in the IFRS 9 section.

Until then, T1 + T2 plus schema/enum drift detection covers the practical range of issues we expect at launch.

---

## 10. How anomalies plug into the rest of the framework

- **Contracts** (`docs/data_contracts/`) — vendors see our anomaly outputs; breakages indicate contract drift.
- **Lineage** (`docs/lineage.md`) — an anomaly on `silver.aml_screen` explains which marts to distrust.
- **Publication** (`docs/dq_consistency.md`) — `dq.publication_marker` is **unaffected** by anomaly severity; only rule failures can change it. Anomalies influence people, not automation, by design.
- **Dashboards** — `mart.dq_scorecard_daily.anomaly_count` trends show the overall health envelope next to rule failures.
