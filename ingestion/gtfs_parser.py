# ingestion/gtfs_parser.py
# Parses raw GTFS Realtime protobuf bytes into clean Python dicts.
# Separated from fetching so it can be unit tested without network calls.

import logging
from datetime import datetime, timezone
from typing import Optional

from google.transit import gtfs_realtime_pb2

logger = logging.getLogger(__name__)


def parse_feed(raw_bytes: bytes) -> gtfs_realtime_pb2.FeedMessage:
    """Parse raw protobuf bytes into a FeedMessage object."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(raw_bytes)
    return feed


def extract_trip_updates(
    feed: gtfs_realtime_pb2.FeedMessage,
    fetch_id: int,
    fetched_at: datetime,
) -> list[dict]:
    """
    Extract all StopTimeUpdates from TripUpdate entities.
    Returns one dict per stop per trip (matches raw_trip_updates schema).
    """
    records = []

    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue

        tu = entity.trip_update
        trip = tu.trip

        # Trip-level fields
        trip_id = trip.trip_id or None
        route_id = trip.route_id or None
        direction_id = trip.direction_id if trip.HasField("direction_id") else None
        start_time = trip.start_time or None
        start_date = trip.start_date or None
        schedule_rel = _schedule_relationship(trip.schedule_relationship)

        # One row per stop time update
        for stu in tu.stop_time_update:
            arrival_delay = None
            departure_delay = None
            arrival_time = None
            departure_time = None

            if stu.HasField("arrival"):
                arrival_delay = stu.arrival.delay if stu.arrival.HasField("delay") else None
                arrival_time = stu.arrival.time if stu.arrival.HasField("time") else None

            if stu.HasField("departure"):
                departure_delay = stu.departure.delay if stu.departure.HasField("delay") else None
                departure_time = stu.departure.time if stu.departure.HasField("time") else None

            records.append({
                "fetch_id":              fetch_id,
                "fetched_at":            fetched_at,
                "trip_id":               trip_id,
                "route_id":              route_id,
                "direction_id":          direction_id,
                "start_time":            start_time,
                "start_date":            start_date,
                "schedule_relationship": schedule_rel,
                "stop_sequence":         stu.stop_sequence or None,
                "stop_id":               stu.stop_id or None,
                "arrival_delay":         arrival_delay,
                "departure_delay":       departure_delay,
                "arrival_time":          arrival_time,
                "departure_time":        departure_time,
            })

    return records


def extract_vehicle_positions(
    feed: gtfs_realtime_pb2.FeedMessage,
    fetch_id: int,
    fetched_at: datetime,
) -> list[dict]:
    """
    Extract VehiclePosition entities from the feed.
    Returns one dict per vehicle.
    """
    records = []

    for entity in feed.entity:
        if not entity.HasField("vehicle"):
            continue

        vp = entity.vehicle
        trip = vp.trip
        pos = vp.position

        records.append({
            "fetch_id":             fetch_id,
            "fetched_at":           fetched_at,
            "vehicle_id":           vp.vehicle.id or None,
            "trip_id":              trip.trip_id or None,
            "route_id":             trip.route_id or None,
            "latitude":             round(pos.latitude, 6) if pos.HasField("latitude") else None,
            "longitude":            round(pos.longitude, 6) if pos.HasField("longitude") else None,
            "bearing":              round(pos.bearing, 1) if pos.HasField("bearing") else None,
            "speed":                round(pos.speed, 2) if pos.HasField("speed") else None,
            "current_stop_sequence": vp.current_stop_sequence or None,
            "current_status":       _vehicle_status(vp.current_status),
            "timestamp":            vp.timestamp or None,
        })

    return records


def _schedule_relationship(value: int) -> Optional[str]:
    mapping = {0: "SCHEDULED", 1: "ADDED", 2: "UNSCHEDULED", 3: "CANCELED"}
    return mapping.get(value)


def _vehicle_status(value: int) -> Optional[str]:
    mapping = {0: "INCOMING_AT", 1: "STOPPED_AT", 2: "IN_TRANSIT_TO"}
    return mapping.get(value)
