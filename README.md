# Unified Decision-Input Data Pipeline (Mal.ai)

[![CI](https://github.com/YOUR_GITHUB_USER/mal.ai/actions/workflows/ci.yml/badge.svg)](https://github.com/YOUR_GITHUB_USER/mal.ai/actions/workflows/ci.yml)

Reference implementation (SQL + Python) for a **unified decision-input pipeline** supporting Mal.ai's three credit products (Personal Finance, BNPL, Card Alternative) with UAE Central Bank-grade auditability and Sharia-aligned product modeling.

After you create the GitHub remote, replace `YOUR_GITHUB_USER` in the badge URL above (or remove the badge line).

## Documentation navigation

This repo maps **design dimensions** (domain conflicts, DQ governance, scale, production trade-offs, Sharia alignment) to **docs + SQL + Python**:

| Dimension | Where to look first |
| --- | --- |
| Credit / lending domain judgment & data conflicts | [`docs/design_criteria_map.md`](docs/design_criteria_map.md) → *Real-world conflict scenarios*; then [`docs/architecture.md`](docs/architecture.md) |
| DQ rigor & governance | [`docs/design_criteria_map.md`](docs/design_criteria_map.md) → *DQ* row; [`docs/data_quality.md`](docs/data_quality.md), [`docs/dq_consistency.md`](docs/dq_consistency.md) |
| Scalability & maintainability | [`docs/partitioning_strategies.md`](docs/partitioning_strategies.md), [`docs/schema_evolution.md`](docs/schema_evolution.md), [`docs/aws_infrastructure.md`](docs/aws_infrastructure.md) |
| Trade-offs & production readiness | [`docs/tradeoffs_production_readiness.md`](docs/tradeoffs_production_readiness.md), [`docs/aws_infrastructure.md`](docs/aws_infrastructure.md) §7–9, [`docs/execution_plan_30_60_90.md`](docs/execution_plan_30_60_90.md) |
| Sharia-aligned banking | README *Compliance & Sharia highlights*; [`docs/architecture.md`](docs/architecture.md); `sql/04_sharia_and_policy.sql` |

**Orientation map:** [`docs/design_criteria_map.md`](docs/design_criteria_map.md) (criteria → evidence + suggested reading order).  
**Full doc index:** [`docs/README.md`](docs/README.md).

## Sources and their quirks

- **AECB** (UAE Credit Bureau): SFTP batch, XML, match on **Emirates ID**.
- **Fraud provider**: REST API, JSON, **real-time scoring**, match on **phone + email**.
- **AML/PEP screening**: webhook callbacks, JSON, match on **name + DOB**.
- **Internal customer profile**: PostgreSQL, match on **internal UUID**.

## Architecture at a glance

- **Bronze**: raw, immutable S3 landing (partitioned; hashed; KMS-encrypted).
- **Silver (RDS Postgres)**: conformed canonical tables + identity resolution to a stable `customer_key`.
- **Gold (RDS Postgres)**: append-only decision input snapshots (with canonical-JSON hash + artifact pointers) + DQ scorecard + portfolio monitoring marts.
- **Orchestrator**: AWS MWAA (Airflow).
- **Migration path**: keep schemas Redshift-compatible; move marts post-launch.

See (full list in [`docs/README.md`](docs/README.md)):
- [`docs/design_criteria_map.md`](docs/design_criteria_map.md) — **design dimensions → evidence** in this repo.
- [`docs/architecture.md`](docs/architecture.md) — full design, diagrams, scaling plan.
- [`docs/aws_infrastructure.md`](docs/aws_infrastructure.md) — AWS account/VPC/IAM/KMS topology and service map.
- [`docs/partitioning_strategies.md`](docs/partitioning_strategies.md) — S3 / Postgres / Redshift partitioning rationale and pitfalls.
- [`docs/schema_evolution.md`](docs/schema_evolution.md) — how vendor + internal schemas evolve safely (breaking vs non-breaking, backfills, registry).
- [`docs/data_contracts/`](docs/data_contracts/) — per-source data contracts (AECB, Fraud, AML, Internal Profile, Gold snapshot).
- [`docs/lineage.md`](docs/lineage.md) — table-level lineage diagram + machine-readable manifest used in CI.
- [`docs/data_quality.md`](docs/data_quality.md) — DQ framework: dimensions, severities, thresholds, gating.
- [`docs/keys_and_constraints.md`](docs/keys_and_constraints.md) — PK/UK/FK/composite-key policy per table; where DB enforces vs where DQ enforces.
- [`docs/cross_database_dq.md`](docs/cross_database_dq.md) — reconciliation across S3 / Postgres / Redshift.
- [`docs/dq_integration.md`](docs/dq_integration.md) — how rules physically integrate with tables (ingest / pre-publish / decision-time / sweep).
- [`docs/dq_consistency.md`](docs/dq_consistency.md) — `dq.publication_marker` + `dq_run_id` stamping + consumer-safe views.
- [`docs/anomaly_detection.md`](docs/anomaly_detection.md) — threshold → statistical baseline (z-score/MAD) → ML tiers.
- [`docs/alerting.md`](docs/alerting.md) — SNS notifications for DQ runs + suggested CloudWatch complements.

## CI, tests, PDF exports

- **CI**: GitHub Actions runs `pytest` on every push/PR (`.github/workflows/ci.yml`). A second job installs Pandoc + a minimal TeX stack and uploads **PDF artifacts** for the main design documents.
- **Tests**: `pytest` from the repo root (see `pytest.ini`).
- **PDFs locally**: `./scripts/export_pdfs.sh` writes to `exports/pdf/` (includes `docs/design_criteria_map.md` plus architecture, trade-offs, 30/60/90, IFRS9 design). On macOS without LaTeX, the script emits HTML fallbacks and prints install hints; with TeX available it writes PDFs directly.
- **Alerting**: set `MAL_PIPELINE_ALERT_SNS_TOPIC_ARN` or pass `sns_topic_arn=` / `sns_notify=` into `run_dq` (see `docs/alerting.md`).

## Publish to GitHub

```bash
git init   # skip if already initialized
git add -A
git commit -m "Initial commit: Mal decision-input pipeline"
git branch -M main
git remote add origin https://github.com/YOUR_GITHUB_USER/mal.ai.git
git push -u origin main
```

Then open the **Actions** tab → latest **CI** run → **docs-pdf** artifact to download the generated PDFs if you did not build them locally.

## Repo layout

```
sql/
  01_bronze_metadata.sql        # audit.ingestion_run/object/error + payload hash log
  02_silver_schema.sql          # canonical silver.* + identity resolution
  03_gold_dq_marts.sql          # gold.decision + snapshot + dq.* + mart.*
  04_sharia_and_policy.sql      # Sharia product catalog + degraded-policy + reassessment
  05_partitioning.sql           # Postgres RANGE partitioning (pg_partman) for hottest tables
  06_redshift_marts.sql         # Redshift marts DDL with DISTKEY/SORTKEY + COPY-from-S3 pattern
  07_schema_registry.sql        # audit.source_schema_version registry for contract versions
  08_dq_framework_upgrade.sql   # dq.rule dimension/threshold cols + dq.reconciliation + survivorship indexes
  09_dq_integration.sql         # dq.rule_target + dq.publication_marker + dq.anomaly + dq_run_id columns + v_publishable_* views

python/mal_pipeline/
  common/        config.py | db.py | hashing.py | s3.py | audit.py
  ingest/        aecb_xml.py | fraud_api.py | aml_webhook_receiver.py |
                 aml_reconcile.py | internal_profile_extract.py
  identity/      normalize.py | resolve_customer_keys.py
  decision/      snapshot_inputs.py | policy.py | replay.py
  dq/            rules.yml | run_dq.py | schema_validator.py | reconcile.py | publication.py | anomaly.py
  alerting/      sns.py  # optional SNS hooks used by run_dq
  etl/           build_marts.py
  schemas/       aecb.v1.json | fraud_request.v1.json | fraud_response.v1.json |
                 aml_webhook.v1.json | internal_profile.v1.json |
                 decision_input_snapshot.v1.json

python/tests/    pytest suite (hashing, normalization, policy, parser, webhook)

dags/            MWAA DAG skeletons (ingest_aecb_sftp_to_s3.py + README)

docs/
  design_criteria_map.md  # design dimensions → evidence
  README.md             # topical index of all docs
  architecture.md
  tradeoffs_production_readiness.md
  execution_plan_30_60_90.md

ifrs9/           # IFRS 9 ECL extension: schema, sample queries, design note
  01_schema.sql
  02_sample_queries.sql
  DESIGN.md
```

## Quickstart (local sanity)

1. Python env + deps:
   ```bash
   python3 -m venv .venv && source .venv/bin/activate
   pip install -r requirements.txt pytest
   ```
2. Run tests:
   ```bash
   pytest
   ```
3. Bootstrap Postgres schema in order:
   ```bash
   psql "$DATABASE_URL" -f sql/01_bronze_metadata.sql
   psql "$DATABASE_URL" -f sql/02_silver_schema.sql
   psql "$DATABASE_URL" -f sql/03_gold_dq_marts.sql
   psql "$DATABASE_URL" -f sql/04_sharia_and_policy.sql
   ```
4. Load DQ rules:
   ```python
   from mal_pipeline.common.config import AppConfig, PostgresConfig, S3Location
   from mal_pipeline.common.db import get_conn
   from mal_pipeline.dq.run_dq import load_rules, upsert_rules
   cfg = PostgresConfig(host="...", port=5432, dbname="...", user="...", password="...")
   with get_conn(cfg) as conn:
       upsert_rules(conn, load_rules("python/mal_pipeline/dq/rules.yml"))
   ```

## Component entry points

- **AECB**: `mal_pipeline.ingest.aecb_xml.ingest_one_file(...)` — pulls a single XML over SFTP, writes Bronze, loads Silver, and records audit metadata.
- **Fraud (write-through)**: `mal_pipeline.ingest.fraud_api.score_and_persist(...)` — called by the decisioning service; lands raw req/res to Bronze and upserts `silver.fraud_score`.
- **AML webhook**: FastAPI app in `mal_pipeline.ingest.aml_webhook_receiver:app` — verifies HMAC signature, writes Bronze, returns 200 quickly; the reconcile job in `aml_reconcile.py` promotes Bronze → `silver.aml_screen`.
- **Internal extract**: `mal_pipeline.ingest.internal_profile_extract.extract_internal_profile(...)`.
- **Identity resolution**: `mal_pipeline.identity.resolve_customer_keys.resolve_one_observation(...)`.
- **Policy engine**: `mal_pipeline.decision.policy.derive_policy(...)` → returns a `PolicyDecision` including degraded caps.
- **Snapshot**: `mal_pipeline.decision.snapshot_inputs.create_decision_snapshot(...)`.
- **Replay / audit**: `mal_pipeline.decision.replay.replay_snapshot(...)` → verifies canonical hash and Bronze artifact integrity.
- **DQ**: `mal_pipeline.dq.run_dq.run_dq(...)` returns a must-pass pass/fail verdict you can use as a gate.
- **Marts**: `mal_pipeline.etl.build_marts.build_risk_portfolio_daily / build_dq_scorecard_daily`.

## Compliance & Sharia highlights

- Full audit chain: Bronze hashes → `audit.ingestion_object` → immutable Gold snapshots with canonical-JSON hashes → replay tool to prove integrity.
- Sharia product catalog (`gold.product`) encodes contract type (Murabaha, Ijara, …), profit rate, and late-fee treatment (defaulting to charity routing).
- Degraded-decisioning table (`gold.degraded_policy_cap`) + per-day usage + reconciliation events (`gold.decision_reassessment`) make bureau-outage handling transparent and bounded.

## What's intentionally scoped out (see 30/60/90)

- Full CDC; probabilistic identity matching at scale; Redshift marts; fine-grained PII tokenization; end-to-end encryption for every column. These are sequenced in `docs/execution_plan_30_60_90.md`.
