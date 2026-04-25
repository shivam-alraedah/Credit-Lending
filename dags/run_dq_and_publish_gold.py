"""
MWAA DAG: runs DQ rule engine and gates Gold publishing.

Task graph:
  run_dq  --(must_pass == true)--> publish_gold
  run_dq  --(must_pass == false)--> alert_and_stop

Gating pattern: use BranchPythonOperator on the `must_pass` return value.
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator


def _run_dq(**_context):
    # Load rules.yml, upsert, run; return "publish_gold" or "alert_and_stop"
    return "publish_gold"


def _publish_gold(**_context):
    # Build marts, refresh feature views, etc.
    pass


def _alert_and_stop(**_context):
    # Publish to CloudWatch/SNS; no-op here for the skeleton.
    pass


with DAG(
    dag_id="run_dq_and_publish_gold",
    start_date=datetime(2025, 1, 1),
    schedule="@hourly",
    catchup=False,
    default_args={"retries": 2},
    tags=["dq", "gold"],
) as dag:
    dq = BranchPythonOperator(task_id="run_dq", python_callable=_run_dq)
    publish = PythonOperator(task_id="publish_gold", python_callable=_publish_gold)
    alert = PythonOperator(task_id="alert_and_stop", python_callable=_alert_and_stop)
    dq >> [publish, alert]
