# Design criteria → evidence in this repository

This page maps the pipeline’s **design dimensions** (domain judgment, DQ governance, scale, production trade-offs, Sharia alignment) to **concrete documentation, SQL, and Python** so engineers and reviewers can navigate the repo without guesswork.

---

## Design dimensions → evidence


| Dimension                                                                                                         | What this repo demonstrates                                                                                                                                     | Start here (docs)                                                                                                                                                                                                         | Ground truth (code / SQL)                                                                                                                                                                   |
| ----------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Credit / lending domain judgment** — **real-world data conflicts** (identifiers, timing, provider disagreement) | Four sources with different keys and cadences; **identity resolution**; **policy + caps** when data is missing or late; **reassessment** when bureau catches up | `[architecture.md](architecture.md)` §2–5, `[data_contracts/README.md](data_contracts/README.md)`                                                                                                                         | `python/mal_pipeline/identity/`, `decision/policy.py`, `sql/02_silver_schema.sql`, `sql/04_sharia_and_policy.sql`, `dags/`                                                                  |
| **DQ rigor & governance** for **regulated decision inputs**                                                       | MUST_PASS vs WARN; **publication gating**; **consumer-safe views**; cross-store **reconciliation**; **lineage**; schema **contracts**                           | `[data_quality.md](data_quality.md)`, `[dq_integration.md](dq_integration.md)`, `[dq_consistency.md](dq_consistency.md)`, `[cross_database_dq.md](cross_database_dq.md)`, `[lineage.md](lineage.md)`                      | `python/mal_pipeline/dq/` (`rules.yml`, `run_dq.py`, `publication.py`, `anomaly.py`, `reconcile.py`, `schema_validator.py`), `sql/08_dq_framework_upgrade.sql`, `sql/09_dq_integration.sql` |
| **Scalability & maintainability**                                                                                 | Medallion + partitioning; **schema evolution** playbook; Postgres-first with **Redshift-portable** marts; orchestration boundaries                              | `[partitioning_strategies.md](partitioning_strategies.md)`, `[schema_evolution.md](schema_evolution.md)`, `[aws_infrastructure.md](aws_infrastructure.md)` §3–3.2                                                         | `sql/05_partitioning.sql`, `sql/06_redshift_marts.sql`, `sql/07_schema_registry.sql`, `dags/`                                                                                               |
| **Trade-offs & production readiness**                                                                             | Explicit launch vs post-launch; security / DR / SLO-style signals; **cost levers**; alerting                                                                    | `[tradeoffs_production_readiness.md](tradeoffs_production_readiness.md)`, `[aws_infrastructure.md](aws_infrastructure.md)` §7–9, `[execution_plan_30_60_90.md](execution_plan_30_60_90.md)`, `[alerting.md](alerting.md)` | `.github/workflows/ci.yml`, `python/mal_pipeline/alerting/`, `docs/aws_infrastructure.md`                                                                                                   |
| **Sharia-compliant banking alignment**                                                                            | **Product catalog** in data model; **non-riba** fee treatment defaults; **transparent degraded policy** and **reassessment** auditable in Gold                  | `[architecture.md](architecture.md)` (Sharia / compliance sections), README “Compliance & Sharia highlights”                                                                                                              | `sql/04_sharia_and_policy.sql`, `gold.product`, `gold.degraded_policy_cap`, `gold.decision_reassessment`                                                                                    |


---

## Suggested reading order

**~15 minutes (orientation)**  

1. This file (you are here).
2. `[architecture.md](architecture.md)` — skim §2 diagram + § on identity / snapshots.
3. `[tradeoffs_production_readiness.md](tradeoffs_production_readiness.md)` — skim headings.
4. `[execution_plan_30_60_90.md](execution_plan_30_60_90.md)` — skim phases.

**~45 minutes (deeper review)**  
5. `[data_quality.md](data_quality.md)` + `[dq_consistency.md](dq_consistency.md)`.  
6. `[cross_database_dq.md](cross_database_dq.md)` + `[aws_infrastructure.md](aws_infrastructure.md)` §3 (where data lives) + §8 (DR).  
7. One data contract, e.g. `[data_contracts/aecb.md](data_contracts/aecb.md)` and `[data_contracts/decision_input_snapshot.md](data_contracts/decision_input_snapshot.md)`.

**Implementation depth**  
8. `python/tests/` — `pytest` covers normalization, hashing, policy, AECB parsing, webhook signature, DQ runner, schema validation, publication + anomaly helpers, SNS formatting.  
9. `sql/01..09_*.sql` — apply in order (see README Quickstart).

---

## Real-world conflict scenarios (domain + data)

Each row is a **traceable scenario** from conflict to tables and jobs.


| Scenario                      | Conflict                                                | How the design responds                                                                                                              | Where to look                                                                                                            |
| ----------------------------- | ------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------ |
| **Bureau late, fraud now**    | AECB batch ID lags; fraud score is real-time            | Decision uses available inputs; **policy** encodes caps / manual review; later **reassessment** when AECB lands                      | `decision/policy.py`, `sql/04_sharia_and_policy.sql` (`decision_reassessment`), `dags/aecb_post_arrival_reassessment.py` |
| **Same person, four keys**    | Emirates ID vs phone/email vs name+DOB vs internal UUID | **Silver observations** + resolution to stable `customer_key`; survivorship rules in DQ / keys doc                                   | `identity/resolve_customer_keys.py`, `sql/02_silver_schema.sql`, `[keys_and_constraints.md](keys_and_constraints.md)`    |
| **AML duplicate webhook**     | Vendor retries; at-least-once delivery                  | **Idempotent** Bronze + **reconcile** promotion to Silver                                                                            | `ingest/aml_webhook_receiver.py`, `ingest/aml_reconcile.py`, `dags/aml_webhook_reconcile.py`                             |
| **“Is this dashboard safe?”** | RDS vs Redshift vs BI tool may disagree                 | **Single DQ truth** in Postgres; **publication marker** + `**v_publishable_`*** views; Redshift is eventual copy with reconciliation | `[dq_consistency.md](dq_consistency.md)`, `sql/09_dq_integration.sql`                                                    |


---

## Tests, CI, and exports


| Artifact                   | Purpose                                                                                                                                                            |
| -------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `pytest` / `python/tests/` | Executable checks for normalization, hashing, policy, AECB parsing, webhook signature, DQ runner, schema validation, publication + anomaly helpers, SNS formatting |
| `.github/workflows/ci.yml` | CI runs `pytest` on push/PR                                                                                                                                        |
| `scripts/export_pdfs.sh`   | Regenerate static PDFs under `exports/pdf/` (local LaTeX or Docker `pandoc/latex:3.6`)                                                                             |


---

## Full documentation index

For a **curated topical index**, see `[docs/README.md](README.md)`.