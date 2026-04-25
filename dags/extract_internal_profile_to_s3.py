"""
MWAA DAG: nightly snapshot of internal.customer_profile to S3 Bronze.

Uses `mal_pipeline.ingest.internal_profile_extract.extract_internal_profile`.
Swap to CDC (DMS) post-launch; the Silver loader is the same.
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def run_extract(**_context):
    # Build connections/configs from Airflow Variables / Secrets Manager, then call
    # mal_pipeline.ingest.internal_profile_extract.extract_internal_profile(...)
    pass


with DAG(
    dag_id="extract_internal_profile_to_s3",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 3},
    tags=["bronze", "internal"],
) as dag:
    PythonOperator(task_id="extract", python_callable=run_extract)
