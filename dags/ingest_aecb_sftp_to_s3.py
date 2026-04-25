"""
Skeleton DAG: AECB SFTP batch -> S3 Bronze + audit.ingestion_* metadata.

In MWAA, you'd configure:
- SFTP connection (Airflow connection or Secrets Manager)
- S3 bucket/prefix and KMS key
- schedule (e.g., hourly/daily based on bureau delivery)

This file is intentionally a minimal skeleton; wire SFTP, secrets, and `mal_pipeline` calls in MWAA for production.
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def pull_sftp_to_s3(**_context):
    # Pseudocode entrypoint:
    # - list remote files
    # - for each file:
    #   - download stream
    #   - compute sha256
    #   - write to s3://bronze/aecb/ingest_date=.../batch_id=.../
    #   - insert audit.ingestion_run + audit.ingestion_object
    pass


with DAG(
    dag_id="ingest_aecb_sftp_to_s3",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 3},
    tags=["bronze", "aecb"],
) as dag:
    PythonOperator(task_id="pull_sftp_to_s3", python_callable=pull_sftp_to_s3)

