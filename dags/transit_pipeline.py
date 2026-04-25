# dags/transit_pipeline.py
# Main TransitWatch DAG.
# Schedule: every 15 minutes.
# Flow: ingest_raw → run_dbt_models → run_dbt_tests → log_summary
#
# CI/CD note: Airflow hot-reloads DAGs from the /dags volume —
# no container restart needed when you change this file.

import logging
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Add ingestion module to path so we can import it directly
sys.path.insert(0, "/opt/airflow/ingestion")

logger = logging.getLogger(__name__)

# ── Default args — applied to every task ─────────────────────────────────────
default_args = {
    "owner":            "transitwatch",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "email_on_failure": False,
    "email_on_retry":   False,
}

# ── DAG definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id="transit_pipeline",
    description="Ingest GTFS Realtime → dbt transform → dbt test",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="*/15 * * * *",   # every 15 minutes
    catchup=False,                       # don't backfill missed runs
    max_active_runs=1,                   # prevent overlapping runs
    tags=["transit", "gtfs", "mta"],
) as dag:

    # ── Task 1: Ingest raw GTFS data ─────────────────────────────────────────
    def ingest_raw(**context):
        """
        Fetch all MTA subway feeds and write to raw_* tables.
        Pushes summary to XCom so downstream tasks can log it.
        """
        from fetch_gtfs import run_all_feeds

        results = run_all_feeds()

        # Push to XCom for the summary task
        context["ti"].xcom_push(key="ingest_results", value=results)

        # Fail the task if ALL feeds failed
        failed = [r for r in results if r["status"] == "error"]
        if len(failed) == len(results):
            raise RuntimeError(
                f"All {len(failed)} feeds failed. "
                f"First error: {failed[0].get('error', 'unknown')}"
            )

        if failed:
            logger.warning(f"{len(failed)}/{len(results)} feeds failed — continuing")

        total_records = sum(
            r.get("trip_updates", 0) + r.get("vehicle_positions", 0)
            for r in results
        )
        logger.info(f"Ingested {total_records:,} records across {len(results)} feeds")
        return total_records

    task_ingest = PythonOperator(
        task_id="ingest_raw",
        python_callable=ingest_raw,
        provide_context=True,
    )

    # ── Task 2: Run dbt models ────────────────────────────────────────────────
    # BashOperator is the standard way to run dbt from Airflow.
    # We pass --select to run only models that depend on raw data
    # (faster than running everything on every tick).
    task_dbt_run = BashOperator(
        task_id="run_dbt_models",
        bash_command="""
            cd /opt/airflow/dbt && \
            dbt run \
                --profiles-dir . \
                --target dev \
                --select staging.stg_trip_updates staging.stg_vehicle_positions \
                         marts.mart_route_delays marts.mart_hourly_performance \
                         marts.mart_stop_hotspots \
                --vars '{"lookback_hours": 2}'
        """,
        env={
            "POSTGRES_HOST":     os.getenv("POSTGRES_HOST", "postgres"),
            "POSTGRES_PORT":     os.getenv("POSTGRES_PORT", "5432"),
            "POSTGRES_USER":     os.getenv("POSTGRES_USER", "transitwatch"),
            "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD", ""),
            "POSTGRES_DB":       os.getenv("POSTGRES_DB", "transitwatch"),
            "DBT_PROFILES_DIR":  "/opt/airflow/dbt",
            **os.environ,
        },
    )

    # ── Task 3: Run dbt tests ─────────────────────────────────────────────────
    # Runs after every model refresh.
    # Failed tests are WARNING severity — they don't kill the pipeline
    # but they do write to dq_test_results for monitoring.
    task_dbt_test = BashOperator(
        task_id="run_dbt_tests",
        bash_command="""
            cd /opt/airflow/dbt && \
            dbt test \
                --profiles-dir . \
                --target dev \
                --select staging marts \
                --store-failures \
            && echo "All dbt tests passed" \
            || echo "Some dbt tests failed — check dq_test_results table"
        """,
        env={
            "POSTGRES_HOST":     os.getenv("POSTGRES_HOST", "postgres"),
            "POSTGRES_PORT":     os.getenv("POSTGRES_PORT", "5432"),
            "POSTGRES_USER":     os.getenv("POSTGRES_USER", "transitwatch"),
            "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD", ""),
            "POSTGRES_DB":       os.getenv("POSTGRES_DB", "transitwatch"),
            "DBT_PROFILES_DIR":  "/opt/airflow/dbt",
            **os.environ,
        },
    )

    # ── Task 4: Log pipeline summary ──────────────────────────────────────────
    def log_summary(**context):
        """
        Pull ingest results from XCom and write a clean summary to the logs.
        In a real setup this would also post to Slack / PagerDuty.
        """
        ti = context["ti"]
        results = ti.xcom_pull(task_ids="ingest_raw", key="ingest_results") or []

        total_records = sum(
            r.get("trip_updates", 0) + r.get("vehicle_positions", 0)
            for r in results
        )
        success_feeds = sum(1 for r in results if r["status"] == "success")
        avg_duration = (
            sum(r.get("duration_ms", 0) for r in results) / len(results)
            if results else 0
        )

        logger.info("=" * 50)
        logger.info("TransitWatch pipeline run complete")
        logger.info(f"  Feeds:       {success_feeds}/{len(results)} succeeded")
        logger.info(f"  Records:     {total_records:,}")
        logger.info(f"  Avg latency: {avg_duration:.0f}ms per feed")
        logger.info(f"  Run time:    {context['ts']}")
        logger.info("=" * 50)

        return {
            "total_records": total_records,
            "success_feeds": success_feeds,
            "avg_duration_ms": avg_duration,
        }

    task_summary = PythonOperator(
        task_id="log_summary",
        python_callable=log_summary,
        provide_context=True,
        trigger_rule="all_done",    # runs even if dbt tests warn/fail
    )

    # ── Task dependencies (the DAG shape) ─────────────────────────────────────
    #
    #   ingest_raw → run_dbt_models → run_dbt_tests → log_summary
    #
    task_ingest >> task_dbt_run >> task_dbt_test >> task_summary
