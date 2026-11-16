import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

# No failure callback here on purpose: run_checks.py already sends a detailed alert,
# and a red task is the visual signal in the Airflow UI.
with DAG(
    dag_id="pipeline_monitor",
    description="Every 5 minutes: run all freshness, volume, quarantine, reconciliation and anomaly checks",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-engineering", "retries": 0},
    tags=["monitoring"],
) as dag:

    run_checks = BashOperator(
        task_id="run_checks",
        bash_command="python /opt/airflow/scripts/run_checks.py",
    )