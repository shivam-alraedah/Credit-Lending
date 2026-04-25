# Documentation index (`docs/`)

Use this page to **navigate by topic**. For a **criteria → evidence** map of the whole repo, start with [`design_criteria_map.md`](design_criteria_map.md).

---

## Start here

| Goal | Document |
| --- | --- |
| Map design dimensions to docs and code | [`design_criteria_map.md`](design_criteria_map.md) |
| End-to-end architecture (sources → Bronze → Silver → Gold → marts) | [`architecture.md`](architecture.md) |
| AWS topology, RDS vs Redshift, ECR, DR, SLO-style monitoring, cost levers | [`aws_infrastructure.md`](aws_infrastructure.md) |
| Launch vs post-launch, explicit trade-offs | [`tradeoffs_production_readiness.md`](tradeoffs_production_readiness.md) |
| Phased delivery (30 / 60 / 90) | [`execution_plan_30_60_90.md`](execution_plan_30_60_90.md) |

---

## Credit, lending, and conflicts

| Topic | Document |
| --- | --- |
| Per-source contracts (transport, keys, PII, SLAs) | [`data_contracts/README.md`](data_contracts/README.md) |
| Keys, uniqueness, FK policy (DB vs DQ enforcement) | [`keys_and_constraints.md`](keys_and_constraints.md) |
| Anomaly detection tiers (threshold → stats → ML) | [`anomaly_detection.md`](anomaly_detection.md) |

---

## Data quality and governance

| Topic | Document |
| --- | --- |
| DQ dimensions, severities, thresholds, gating | [`data_quality.md`](data_quality.md) |
| Where rules run (ingest / pre-publish / scheduled / decision-time) | [`dq_integration.md`](dq_integration.md) |
| Publication marker, `dq_run_id`, consumer views, Redshift propagation | [`dq_consistency.md`](dq_consistency.md) |
| Cross-database reconciliation (S3, RDS, Redshift) | [`cross_database_dq.md`](cross_database_dq.md) |
| Table-level lineage + manifest | [`lineage.md`](lineage.md) |
| Schema evolution and registry | [`schema_evolution.md`](schema_evolution.md) |
| Partitioning (S3, Postgres, Redshift) | [`partitioning_strategies.md`](partitioning_strategies.md) |

---

## Production operations

| Topic | Document |
| --- | --- |
| SNS alerting for DQ runs + CloudWatch complements | [`alerting.md`](alerting.md) |

---

## Related material outside `docs/`

| Area | Location |
| --- | --- |
| SQL DDL (apply in numeric order) | `../sql/01..09_*.sql` |
| Python pipeline package | `../python/mal_pipeline/` |
| Unit tests | `../python/tests/` |
| MWAA DAG skeletons | `../dags/` |
| IFRS9 ECL extension | `../ifrs9/DESIGN.md` + `../ifrs9/*.sql` |
| PDF export script | `../scripts/export_pdfs.sh` |
