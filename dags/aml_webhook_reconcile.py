"""
MWAA DAG: micro-batch reconcile of AML Bronze payloads into silver.aml_screen.

Runs frequently (every few minutes) to keep AML status fresh while keeping
the webhook endpoint's hot path minimal.
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def _reconcile(**_context):
    # Resolve DB conn, call mal_pipeline.ingest.aml_reconcile.reconcile_aml(conn)
    pass


with DAG(
    dag_id="aml_webhook_reconcile",
    start_date=datetime(2025, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    default_args={"retries": 2},
    tags=["bronze_to_silver", "aml"],
) as dag:
    PythonOperator(task_id="reconcile", python_callable=_reconcile)
