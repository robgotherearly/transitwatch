# ingestion/db.py
# Database connection + helper utilities.
# All ingestion scripts import from here — one place to change config.

import os
import logging
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


def get_connection():
    """Return a raw psycopg2 connection from environment variables."""
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        dbname=os.getenv("POSTGRES_DB", "transitwatch"),
        user=os.getenv("POSTGRES_USER", "transitwatch"),
        password=os.getenv("POSTGRES_PASSWORD", "transitwatch_dev"),
        connect_timeout=10,
    )


@contextmanager
def get_cursor():
    """
    Context manager that yields a cursor and auto-commits or rolls back.

    Usage:
        with get_cursor() as cur:
            cur.execute("SELECT 1")
    """
    conn = get_connection()
    try:
        with conn:                  # auto-commit on exit, rollback on exception
            with conn.cursor() as cur:
                yield cur
    finally:
        conn.close()


def bulk_insert(cursor, table: str, columns: list[str], rows: list[tuple]) -> int:
    """
    Fast bulk insert using psycopg2 execute_values.
    Returns number of rows inserted.
    """
    if not rows:
        return 0

    col_str = ", ".join(columns)
    sql = f"INSERT INTO {table} ({col_str}) VALUES %s ON CONFLICT DO NOTHING"
    execute_values(cursor, sql, rows, page_size=500)
    return len(rows)
