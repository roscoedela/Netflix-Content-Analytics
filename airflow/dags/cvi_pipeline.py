from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
import os
os.environ["SNOWFLAKE_USE_PYOPENSSL"] = "false"

PROJECT_ROOT = "/Users/rooscolrozambert/netflix-content-analytics"
DBT_PROJECT = f"{PROJECT_ROOT}/netflix_content_analytics"
GX_DIR = f"{PROJECT_ROOT}/gx"
VENV = f"{PROJECT_ROOT}/venv/bin"

default_args = {
    "owner": "rooscol",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["rooscolrozambert@gmail.com"],  # replace with your real email
}

with DAG(
    dag_id="cvi_pipeline",
    default_args=default_args,
    description="CVI data pipeline: ingest → dbt → GX → notify",
    schedule_interval="0 6 * * 1",  # Every Monday at 6am
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["cvi", "netflix"],
) as dag:

    load_movielens = BashOperator(
        task_id="load_movielens",
        bash_command=f"{VENV}/python {PROJECT_ROOT}/ingestion/load_movielens.py",
    )

    load_tmdb = BashOperator(
        task_id="load_tmdb",
        bash_command=f"{VENV}/python {PROJECT_ROOT}/ingestion/load_tmdb.py",
    )

    load_trends = BashOperator(
        task_id="load_trends",
        bash_command=f"{VENV}/python {PROJECT_ROOT}/ingestion/load_trends.py",
    )

    dbt_run = BashOperator(
    task_id="dbt_run",
    bash_command=f"cd {DBT_PROJECT} && {VENV}/dbt run --profiles-dir ~/.dbt",
    env={"SNOWFLAKE_USE_PYOPENSSL": "false", "PATH": os.environ.get("PATH", "")},
)

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT} && {VENV}/dbt test --profiles-dir ~/.dbt",
    )

    gx_checkpoint = BashOperator(
        task_id="gx_checkpoint",
        bash_command=f"cd {GX_DIR} && {VENV}/great_expectations checkpoint run cvi_checkpoint",
    )

    notify_success = EmailOperator(
        task_id="notify_success",
        to="rooscolrozambert@gmail.com",  # replace with your real email
        subject="✓ CVI Pipeline succeeded — {{ ds }}",
        html_content="""
            <h3>CVI Pipeline completed successfully</h3>
            <p>Run date: {{ ds }}</p>
            <p>All dbt tests and GX expectations passed.</p>
        """,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    notify_failure = EmailOperator(
        task_id="notify_failure",
        to="rooscolrozambert@gmail.com",  # replace with your real email
        subject="✗ CVI Pipeline FAILED — {{ ds }}",
        html_content="""
            <h3>CVI Pipeline failed</h3>
            <p>Run date: {{ ds }}</p>
            <p>Check Airflow logs for details.</p>
        """,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    # Dependencies
    [load_movielens, load_tmdb, load_trends] >> dbt_run
    dbt_run >> dbt_test >> gx_checkpoint
    gx_checkpoint >> [notify_success, notify_failure]