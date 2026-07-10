from datetime import datetime
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="dbt_ecommerce_pipeline",
    description="Run dbt models and tests for the ecommerce lakehouse project",
    schedule="@daily",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=["dbt", "ecommerce"],
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt_project && dbt run --target docker",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt_project && dbt test --target docker",
    )

    dbt_run >> dbt_test