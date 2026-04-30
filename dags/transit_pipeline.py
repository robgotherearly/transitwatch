# dags/transit_pipeline.py
# Main TransitWatch DAG.
# Schedule: every 15 minutes.
# Flow: ingest_raw → run_dbt_models → run_dbt_tests → log_summary

import logging
import os
import sys
import subprocess
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

sys.path.insert(0, "/opt/airflow/ingestion")

logger = logging.getLogger(__name__)

default_args = {
    "owner":            "transitwatch",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=2),
    "email_on_failure": False,
    "email_on_retry":   False,
}

def run_dbt_command(command: str) -> str:
    """Run a dbt command and return output."""
    env = os.environ.copy()
    env.update({
        "POSTGRES_HOST":     os.getenv("POSTGRES_HOST", "postgres"),
        "POSTGRES_PORT":     os.getenv("POSTGRES_PORT", "5432"),
        "POSTGRES_USER":     os.getenv("POSTGRES_USER", "transitwatch"),
        "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD", ""),
        "POSTGRES_DB":       os.getenv("POSTGRES_DB", "transitwatch"),
    })

    result = subprocess.run(
        command.split(),
        cwd="/opt/airflow/dbt",
        capture_output=True,
        text=True,
        env=env,
    )

    logger.info("dbt stdout:\n%s", result.stdout)
    if result.stderr:
        logger.warning("dbt stderr:\n%s", result.stderr)

    if result.returncode != 0:
        raise Exception(
            f"dbt command failed with code {result.returncode}\n"
            f"stdout: {result.stdout}\n"
            f"stderr: {result.stderr}"
        )

    return result.stdout


with DAG(
    dag_id="transit_pipeline",
    description="Ingest GTFS Realtime → dbt transform → dbt test",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["transit", "gtfs", "mta"],
) as dag:

    def ingest_raw(**context):
        from fetch_gtfs import run_all_feeds
        results = run_all_feeds()
        context["ti"].xcom_push(key="ingest_results", value=results)
        failed = [r for r in results if r["status"] == "error"]
        if len(failed) == len(results):
            raise RuntimeError(f"All {len(failed)} feeds failed.")
        total = sum(r.get("trip_updates", 0) + r.get("vehicle_positions", 0) for r in results)
        logger.info(f"Ingested {total:,} records across {len(results)} feeds")
        return total

    task_ingest = PythonOperator(
        task_id="ingest_raw",
        python_callable=ingest_raw,
        provide_context=True,
    )


    def dbt_run(**context):
        output = run_dbt_command("dbt run --profiles-dir . --target dev")
        logger.info("dbt run completed:\n%s", output)

    task_dbt_run = PythonOperator(
        task_id="run_dbt_models",
        python_callable=dbt_run,
        provide_context=True,
    )

    def dbt_test(**context):
        try:
            output = run_dbt_command("dbt test --profiles-dir . --target dev")
            logger.info("dbt test completed:\n%s", output)
        except Exception as e:
            logger.warning("Some dbt tests failed: %s", str(e))

    task_dbt_test = PythonOperator(
        task_id="run_dbt_tests",
        python_callable=dbt_test,
        provide_context=True,
    )

    def log_summary(**context):
        ti = context["ti"]
        results = ti.xcom_pull(task_ids="ingest_raw", key="ingest_results") or []
        total = sum(r.get("trip_updates", 0) + r.get("vehicle_positions", 0) for r in results)
        success = sum(1 for r in results if r["status"] == "success")
        logger.info("=" * 50)
        logger.info("TransitWatch pipeline run complete")
        logger.info(f"  Feeds:   {success}/{len(results)} succeeded")
        logger.info(f"  Records: {total:,}")
        logger.info("=" * 50)
        return {"total_records": total, "success_feeds": success}

    task_summary = PythonOperator(
        task_id="log_summary",
        python_callable=log_summary,
        provide_context=True,
        trigger_rule="all_done",
    )

    task_ingest >> task_dbt_run >> task_dbt_test >> task_summary