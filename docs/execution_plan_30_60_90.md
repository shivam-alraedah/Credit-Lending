## 30/60/90 execution plan (launch-critical vs post-launch)

### First 30 days (launch-critical)

- **Bronze**: finalize S3 partitioning + manifests + hashing conventions for 4 sources.
- **Ingestion**:
  - MWAA DAG for AECB SFTP → S3 Bronze + audit tables
  - internal profile extract → S3 Bronze
  - AML webhook receiver → S3 Bronze (and MWAA reconcile job)
  - fraud write-through in decisioning service → S3 Bronze + upsert `silver.fraud_score`
- **Silver**: canonical schemas + loaders into RDS Postgres.
- **Identity**: deterministic customer_key resolution + evidence logging.
- **Gold**: immutable decision input snapshots + artifact pointers.
- **DQ**: must-pass rules + alerting + gating of publishing/decisioning.
- **Risk mart MVP**: daily portfolio aggregates + drill-through to decision snapshots.

### Days 31–60 (hardening + scale prep)

- Identity improvements: confidence tuning, merge events, exception queue, periodic re-resolution.
- DQ expansion: timeliness checks, anomaly thresholds, schema drift detection.
- Reliability: backfills, idempotent replays, dead-letter handling for webhook duplicates.
- Performance: partitioning/indexing strategy, incremental builds, connection pooling.
- Security/compliance: KMS encryption, IAM least privilege, retention configs, access audits.

### Days 61–90 (post-launch enhancements)

- **Redshift marts** for BI performance; ELT from Postgres/S3, keep snapshot immutability.
- CDC for internal profile (and optionally Silver/Gold).
- Portfolio monitoring enhancements: vintage/cohort curves, early warning indicators, limit management.
- Start IFRS 9 dataset: exposures, stage transitions, parameter versioning, audit trail.