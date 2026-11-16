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
    "--target-path /tmp/dbt_target --log-path /tmp/dbt_logs"
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
    dag_id="crypto_medallion_pipeline",
    description="Daily: reload batch candles, check bronze against the contract, then build and test Silver and Gold with dbt",
    start_date=pendulum.datetime(2026, 10, 19, tz="UTC"),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": notify_failure,
        "on_retry_callback": notify_retry,
    },
    tags=["crypto", "batch", "dbt", "medallion"],
) as dag:

    load_bronze_batch = BashOperator(
        task_id="load_bronze_batch",
        bash_command="python /opt/airflow/scripts/load_historical_candles.py",
    )

    quality_gate = BashOperator(
        task_id="quality_gate",
        bash_command="python /opt/airflow/scripts/run_checks.py --gate crypto",
        retries=0,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"{DBT} run {DBT_ARGS} --select path:models/crypto",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{DBT} test {DBT_ARGS} --select path:models/crypto path:tests/crypto",
    )

    load_bronze_batch >> quality_gate >> dbt_run >> dbt_test