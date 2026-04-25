# dags/health_check.py
# Runs once daily at 6am.
# Checks data freshness, row counts, and anomalies.
# A real-world monitoring pattern every DE team uses.

import logging
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

logger = logging.getLogger(__name__)

default_args = {
    "owner":           "transitwatch",
    "retries":         1,
    "retry_delay":     timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="health_check",
    description="Daily data quality and freshness checks",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="0 6 * * *",     # 6am daily
    catchup=False,
    tags=["monitoring", "data-quality"],
) as dag:

    def check_data_freshness(**context):
        """
        Fails if no data has been ingested in the last 30 minutes.
        Catches cases where the main pipeline silently stopped.
        """
        import sys
        sys.path.insert(0, "/opt/airflow/ingestion")
        from db import get_cursor

        with get_cursor() as cur:
            cur.execute("""
                SELECT
                    MAX(fetched_at)                         AS last_fetch,
                    NOW() - MAX(fetched_at)                 AS time_since_last,
                    COUNT(*) FILTER (WHERE status = 'success'
                        AND fetched_at > NOW() - INTERVAL '1 hour') AS recent_successes,
                    COUNT(*) FILTER (WHERE status = 'error'
                        AND fetched_at > NOW() - INTERVAL '1 hour')   AS recent_errors
                FROM raw_feed_fetches
            """)
            row = cur.fetchone()

        last_fetch, staleness, recent_ok, recent_err = row

        if last_fetch is None:
            raise ValueError("No fetches found at all — pipeline has never run!")

        staleness_minutes = staleness.total_seconds() / 60
        logger.info(f"Last fetch: {last_fetch} ({staleness_minutes:.1f} minutes ago)")
        logger.info(f"Last hour: {recent_ok} successes, {recent_err} errors")

        if staleness_minutes > 30:
            raise ValueError(
                f"Data is stale! Last fetch was {staleness_minutes:.0f} minutes ago. "
                "Check that the transit_pipeline DAG is running."
            )

        logger.info("Freshness check passed.")

    def check_row_counts(**context):
        """
        Checks that today's data volume is within expected bounds.
        Flags if counts drop more than 50% vs yesterday (possible feed outage).
        """
        import sys
        sys.path.insert(0, "/opt/airflow/ingestion")
        from db import get_cursor

        with get_cursor() as cur:
            cur.execute("""
                SELECT
                    DATE(fetched_at)    AS day,
                    COUNT(*)            AS row_count
                FROM raw_trip_updates
                WHERE fetched_at >= NOW() - INTERVAL '2 days'
                GROUP BY DATE(fetched_at)
                ORDER BY day DESC
                LIMIT 2
            """)
            rows = cur.fetchall()

        if not rows:
            logger.warning("No trip update rows found — has ingestion run yet?")
            return

        today_count = rows[0][1] if rows else 0
        yesterday_count = rows[1][1] if len(rows) > 1 else None

        logger.info(f"Today's trip updates:     {today_count:,}")
        if yesterday_count:
            logger.info(f"Yesterday's trip updates: {yesterday_count:,}")
            drop_pct = (yesterday_count - today_count) / yesterday_count * 100
            if drop_pct > 50:
                raise ValueError(
                    f"Row count dropped {drop_pct:.0f}% vs yesterday "
                    f"({today_count:,} vs {yesterday_count:,}). "
                    "Possible feed outage or ingestion failure."
                )

        logger.info("Row count check passed.")

    def check_null_rates(**context):
        """
        Checks that key columns don't have unexpected null rates.
        High nulls = upstream feed issue or parser bug.
        """
        import sys
        sys.path.insert(0, "/opt/airflow/ingestion")
        from db import get_cursor

        with get_cursor() as cur:
            cur.execute("""
                SELECT
                    COUNT(*)                                        AS total,
                    COUNT(*) FILTER (WHERE trip_id IS NULL)         AS null_trip_id,
                    COUNT(*) FILTER (WHERE route_id IS NULL)        AS null_route_id,
                    COUNT(*) FILTER (WHERE arrival_delay IS NULL)   AS null_delay
                FROM raw_trip_updates
                WHERE fetched_at >= NOW() - INTERVAL '1 hour'
            """)
            total, null_trip, null_route, null_delay = cur.fetchone()

        if total == 0:
            logger.warning("No records in last hour — skipping null check")
            return

        null_trip_pct  = null_trip  / total * 100
        null_route_pct = null_route / total * 100
        null_delay_pct = null_delay / total * 100

        logger.info(f"Null rates (last hour, n={total:,}):")
        logger.info(f"  trip_id:       {null_trip_pct:.1f}%")
        logger.info(f"  route_id:      {null_route_pct:.1f}%")
        logger.info(f"  arrival_delay: {null_delay_pct:.1f}%")

        if null_trip_pct > 10:
            raise ValueError(f"trip_id null rate too high: {null_trip_pct:.1f}%")
        if null_route_pct > 10:
            raise ValueError(f"route_id null rate too high: {null_route_pct:.1f}%")

        logger.info("Null rate check passed.")

    # ── Tasks ─────────────────────────────────────────────────────────────────
    task_freshness = PythonOperator(
        task_id="check_data_freshness",
        python_callable=check_data_freshness,
        provide_context=True,
    )

    task_row_counts = PythonOperator(
        task_id="check_row_counts",
        python_callable=check_row_counts,
        provide_context=True,
    )

    task_null_rates = PythonOperator(
        task_id="check_null_rates",
        python_callable=check_null_rates,
        provide_context=True,
    )

    # Run all checks in parallel — they're independent
    task_freshness
    task_row_counts
    task_null_rates
