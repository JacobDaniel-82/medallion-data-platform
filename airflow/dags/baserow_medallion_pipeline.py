import sys
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

sys.path.append("/opt/airflow/scripts")
import alert  # noqa: E402

DBT = "/home/airflow/dbt_venv/bin/dbt"
DBT_ARGS = (
    "--profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt "
    "--target-path /tmp/dbt_target_baserow --log-path /tmp/dbt_logs_baserow"
)


def notify_failure(context):
    ti = context["task_instance"]
    alert.send(
        f"[PIPELINE FAILED] {ti.dag_id}/{ti.task_id}\n"
        f"{str(context.get('exception'))[:300]}\n"
        f"Details: Airflow UI, http://localhost:8082"
    )


def notify_retry(context):
    ti = context["task_instance"]
    alert.send(f"[PIPELINE RETRY] {ti.dag_id}/{ti.task_id} failed and will be retried.")


with DAG(
    dag_id="baserow_medallion_pipeline",
    description="Daily: load Baserow tables, check bronze against the contract, then build and test Silver and Gold with dbt",
    start_date=pendulum.datetime(2026, 10, 19, tz="UTC"),
    schedule="0 3 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": notify_failure,
        "on_retry_callback": notify_retry,
    },
    tags=["baserow", "batch", "dbt", "medallion"],
) as dag:

    load_baserow_bronze = BashOperator(
        task_id="load_baserow_bronze",
        bash_command="python /opt/airflow/scripts/load_baserow_tables.py",
    )

    quality_gate = BashOperator(
        task_id="quality_gate",
        bash_command="python /opt/airflow/scripts/run_checks.py --gate baserow",
        retries=0,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"{DBT} run {DBT_ARGS} --select path:models/baserow",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{DBT} test {DBT_ARGS} --select path:models/baserow path:tests/baserow",
    )

    load_baserow_bronze >> quality_gate >> dbt_run >> dbt_test