# ingestion/fetch_gtfs.py
# Fetches GTFS Realtime feeds from Malaysia's open API (no API key needed).
# Called by Airflow every 15 minutes via BashOperator.

import logging
import os
import time
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential, before_sleep_log

from db import get_cursor, bulk_insert
from gtfs_parser import parse_feed, extract_trip_updates, extract_vehicle_positions

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

# Free feeds — no API key required
FREE_FEEDS = {
    "ktmb_trains":         "https://api.data.gov.my/gtfs-realtime/vehicle-position/ktmb",
    "rapid_bus_kl":        "https://api.data.gov.my/gtfs-realtime/vehicle-position/prasarana?category=rapid-bus-kl",
    "rapid_bus_mrtfeeder": "https://api.data.gov.my/gtfs-realtime/vehicle-position/prasarana?category=rapid-bus-mrtfeeder",
}

TIMEOUT_SECONDS = 30


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
def fetch_feed_bytes(url: str) -> bytes:
    response = requests.get(url, timeout=TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.content


def log_fetch_start(feed_name: str, fetched_at: datetime) -> int:
    with get_cursor() as cur:
        cur.execute(
            "INSERT INTO raw_feed_fetches (feed_name, fetched_at, status) VALUES (%s, %s, 'running') RETURNING fetch_id",
            (feed_name, fetched_at),
        )
        return cur.fetchone()[0]


def log_fetch_complete(fetch_id: int, record_count: int, duration_ms: int):
    with get_cursor() as cur:
        cur.execute(
            "UPDATE raw_feed_fetches SET status='success', record_count=%s, duration_ms=%s WHERE fetch_id=%s",
            (record_count, duration_ms, fetch_id),
        )


def log_fetch_error(fetch_id: int, error: str, duration_ms: int):
    with get_cursor() as cur:
        cur.execute(
            "UPDATE raw_feed_fetches SET status='error', error_message=%s, duration_ms=%s WHERE fetch_id=%s",
            (str(error)[:500], duration_ms, fetch_id),
        )


def write_trip_updates(records, cursor) -> int:
    columns = ["fetch_id","fetched_at","trip_id","route_id","direction_id","start_time","start_date","schedule_relationship","stop_sequence","stop_id","arrival_delay","departure_delay","arrival_time","departure_time"]
    rows = [tuple(r[c] for c in columns) for r in records]
    return bulk_insert(cursor, "raw_trip_updates", columns, rows)


def write_vehicle_positions(records, cursor) -> int:
    columns = ["fetch_id","fetched_at","vehicle_id","trip_id","route_id","latitude","longitude","bearing","speed","current_stop_sequence","current_status","timestamp"]
    rows = [tuple(r[c] for c in columns) for r in records]
    return bulk_insert(cursor, "raw_vehicle_positions", columns, rows)


def ingest_feed(feed_name: str, url: str) -> dict:
    fetched_at = datetime.now(timezone.utc)
    start_ms = time.monotonic()
    fetch_id = log_fetch_start(feed_name, fetched_at)
    try:
        logger.info(f"Fetching feed: {feed_name}")
        raw_bytes = fetch_feed_bytes(url)
        logger.info(f"  Got {len(raw_bytes):,} bytes")
        feed = parse_feed(raw_bytes)
        trip_updates = extract_trip_updates(feed, fetch_id, fetched_at)
        vehicle_positions = extract_vehicle_positions(feed, fetch_id, fetched_at)
        with get_cursor() as cur:
            tu_count = write_trip_updates(trip_updates, cur)
            vp_count = write_vehicle_positions(vehicle_positions, cur)
        total = tu_count + vp_count
        duration_ms = int((time.monotonic() - start_ms) * 1000)
        log_fetch_complete(fetch_id, total, duration_ms)
        logger.info(f"  Done: {tu_count} trip updates, {vp_count} vehicles in {duration_ms}ms")
        return {"feed_name": feed_name, "status": "success", "trip_updates": tu_count, "vehicle_positions": vp_count, "duration_ms": duration_ms}
    except Exception as e:
        duration_ms = int((time.monotonic() - start_ms) * 1000)
        log_fetch_error(fetch_id, str(e), duration_ms)
        logger.error(f"  Failed: {e}")
        return {"feed_name": feed_name, "status": "error", "error": str(e), "duration_ms": duration_ms}


def run_all_feeds() -> list[dict]:
    results = [ingest_feed(name, url) for name, url in FREE_FEEDS.items()]
    success = sum(1 for r in results if r["status"] == "success")
    total = sum(r.get("trip_updates", 0) + r.get("vehicle_positions", 0) for r in results)
    logger.info(f"Ingestion complete: {success}/{len(results)} feeds OK, {total:,} records total")
    return results


if __name__ == "__main__":
    results = run_all_feeds()
    print("\n── Summary ──────────────────────────────")
    for r in results:
        status = "OK  " if r["status"] == "success" else "FAIL"
        print(f"  [{status}] {r['feed_name']}: {r.get('trip_updates',0)} trip updates, {r.get('vehicle_positions',0)} vehicles ({r.get('duration_ms',0)}ms)")
