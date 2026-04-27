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
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
logger = logging.getLogger(__name__)
MTA_API_KEY = os.getenv("MTA_API_KEY", "")
TIMEOUT_SECONDS = 30
MTA_FEEDS = {
    "nyct_gtfs":      "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs",
    "nyct_gtfs_l":    "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-l",
    "nyct_gtfs_nqrw": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-nqrw",
    "nyct_gtfs_bdfm": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-bdfm",
    "nyct_gtfs_ace":  "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-ace",
    "nyct_gtfs_g":    "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-g",
    "nyct_gtfs_jz":   "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-jz",
    "nyct_gtfs_7":    "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-7",
    "nyct_gtfs_si":   "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-si",
}
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), before_sleep=before_sleep_log(logger, logging.WARNING), reraise=True)
def fetch_feed_bytes(url, api_key):
    response = requests.get(url, headers={"x-api-key": api_key}, timeout=TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.content
def log_fetch_start(feed_name, fetched_at):
    with get_cursor() as cur:
        cur.execute("INSERT INTO raw_feed_fetches (feed_name, fetched_at, status) VALUES (%s, %s, 'running') RETURNING fetch_id", (feed_name, fetched_at))
        return cur.fetchone()[0]
def log_fetch_complete(fetch_id, record_count, duration_ms):
    with get_cursor() as cur:
        cur.execute("UPDATE raw_feed_fetches SET status='success', record_count=%s, duration_ms=%s WHERE fetch_id=%s", (record_count, duration_ms, fetch_id))
def log_fetch_error(fetch_id, error, duration_ms):
    with get_cursor() as cur:
        cur.execute("UPDATE raw_feed_fetches SET status='error', error_message=%s, duration_ms=%s WHERE fetch_id=%s", (str(error)[:500], duration_ms, fetch_id))
def write_trip_updates(records, cursor):
    columns = ["fetch_id","fetched_at","trip_id","route_id","direction_id","start_time","start_date","schedule_relationship","stop_sequence","stop_id","arrival_delay","departure_delay","arrival_time","departure_time"]
    return bulk_insert(cursor, "raw_trip_updates", columns, [tuple(r[c] for c in columns) for r in records])
def write_vehicle_positions(records, cursor):
    columns = ["fetch_id","fetched_at","vehicle_id","trip_id","route_id","latitude","longitude","bearing","speed","current_stop_sequence","current_status","timestamp"]
    return bulk_insert(cursor, "raw_vehicle_positions", columns, [tuple(r[c] for c in columns) for r in records])
def ingest_feed(feed_name, url, api_key):
    fetched_at = datetime.now(timezone.utc)
    start_ms = time.monotonic()
    fetch_id = log_fetch_start(feed_name, fetched_at)
    try:
        logger.info(f"Fetching feed: {feed_name}")
        raw_bytes = fetch_feed_bytes(url, api_key)
        logger.info(f"  Got {len(raw_bytes):,} bytes")
        feed = parse_feed(raw_bytes)
        trip_updates = extract_trip_updates(feed, fetch_id, fetched_at)
        vehicle_positions = extract_vehicle_positions(feed, fetch_id, fetched_at)
        with get_cursor() as cur:
            tu_count = write_trip_updates(trip_updates, cur)
            vp_count = write_vehicle_positions(vehicle_positions, cur)
        duration_ms = int((time.monotonic() - start_ms) * 1000)
        log_fetch_complete(fetch_id, tu_count + vp_count, duration_ms)
        logger.info(f"  Done: {tu_count} trip updates, {vp_count} vehicles in {duration_ms}ms")
        return {"feed_name": feed_name, "status": "success", "trip_updates": tu_count, "vehicle_positions": vp_count, "duration_ms": duration_ms}
    except Exception as e:
        duration_ms = int((time.monotonic() - start_ms) * 1000)
        log_fetch_error(fetch_id, str(e), duration_ms)
        logger.error(f"  Failed: {e}")
        return {"feed_name": feed_name, "status": "error", "error": str(e), "duration_ms": duration_ms}
def run_all_feeds():
    if not MTA_API_KEY:
        raise ValueError("MTA_API_KEY not set. Get a free key at https://api.mta.info/")
    results = [ingest_feed(name, url, MTA_API_KEY) for name, url in MTA_FEEDS.items()]
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