# MWAA (Airflow) DAG skeletons

These DAGs are intentionally minimal and designed to show **task boundaries**, **idempotency**, and where **must-pass DQ gates** block publishing Gold/marts.

Suggested DAGs:
- `ingest_aecb_sftp_to_s3.py` (batch)
- `extract_internal_profile_to_s3.py` (daily)
- `bronze_to_silver_rds.py` (parse/load)
- `identity_resolution.py`
- `run_dq_and_publish_gold.py`
- `build_marts.py`

