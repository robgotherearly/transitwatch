# ingestion/tests/test_gtfs_parser.py
# Tests for the parser module.
# These run in CI without any network or database connection.
# We build minimal protobuf objects by hand to test parsing logic.

import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock

from google.transit import gtfs_realtime_pb2
from gtfs_parser import (
    parse_feed,
    extract_trip_updates,
    extract_vehicle_positions,
    _schedule_relationship,
    _vehicle_status,
)


# ── Fixtures ─────────────────────────────────────────────────────────────────

FETCH_ID = 42
FETCHED_AT = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


def make_feed_with_trip_update(
    trip_id="TEST_TRIP_1",
    route_id="A",
    direction_id=0,
    stop_id="101",
    stop_sequence=1,
    arrival_delay=120,
    departure_delay=90,
) -> gtfs_realtime_pb2.FeedMessage:
    """Build a minimal FeedMessage containing one TripUpdate."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = 1717243200

    entity = feed.entity.add()
    entity.id = "entity_1"

    tu = entity.trip_update
    tu.trip.trip_id = trip_id
    tu.trip.route_id = route_id
    tu.trip.direction_id = direction_id
    tu.trip.start_time = "08:00:00"
    tu.trip.start_date = "20240601"

    stu = tu.stop_time_update.add()
    stu.stop_sequence = stop_sequence
    stu.stop_id = stop_id
    stu.arrival.delay = arrival_delay
    stu.departure.delay = departure_delay

    return feed


def make_feed_with_vehicle(
    vehicle_id="V001",
    trip_id="TEST_TRIP_1",
    route_id="A",
    lat=40.7128,
    lon=-74.0060,
    status=1,   # STOPPED_AT
) -> gtfs_realtime_pb2.FeedMessage:
    """Build a minimal FeedMessage containing one VehiclePosition."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = 1717243200

    entity = feed.entity.add()
    entity.id = "entity_v1"

    vp = entity.vehicle
    vp.vehicle.id = vehicle_id
    vp.trip.trip_id = trip_id
    vp.trip.route_id = route_id
    vp.position.latitude = lat
    vp.position.longitude = lon
    vp.current_status = status
    vp.timestamp = 1717243200

    return feed


# ── Trip update tests ─────────────────────────────────────────────────────────

class TestExtractTripUpdates:

    def test_returns_one_record_per_stop(self):
        feed = make_feed_with_trip_update()
        records = extract_trip_updates(feed, FETCH_ID, FETCHED_AT)
        assert len(records) == 1

    def test_trip_fields_parsed_correctly(self):
        feed = make_feed_with_trip_update(
            trip_id="TRIP_99", route_id="N", direction_id=1
        )
        record = extract_trip_updates(feed, FETCH_ID, FETCHED_AT)[0]
        assert record["trip_id"] == "TRIP_99"
        assert record["route_id"] == "N"
        assert record["direction_id"] == 1
        assert record["fetch_id"] == FETCH_ID
        assert record["fetched_at"] == FETCHED_AT

    def test_delay_values_extracted(self):
        feed = make_feed_with_trip_update(arrival_delay=180, departure_delay=60)
        record = extract_trip_updates(feed, FETCH_ID, FETCHED_AT)[0]
        assert record["arrival_delay"] == 180
        assert record["departure_delay"] == 60

    def test_negative_delay_means_early(self):
        feed = make_feed_with_trip_update(arrival_delay=-45)
        record = extract_trip_updates(feed, FETCH_ID, FETCHED_AT)[0]
        assert record["arrival_delay"] == -45

    def test_stop_fields_parsed(self):
        feed = make_feed_with_trip_update(stop_id="STOP_42", stop_sequence=5)
        record = extract_trip_updates(feed, FETCH_ID, FETCHED_AT)[0]
        assert record["stop_id"] == "STOP_42"
        assert record["stop_sequence"] == 5

    def test_multiple_stops_produces_multiple_records(self):
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.header.gtfs_realtime_version = "2.0"
        feed.header.timestamp = 1717243200

        entity = feed.entity.add()
        entity.id = "e1"
        tu = entity.trip_update
        tu.trip.trip_id = "MULTI_STOP"
        tu.trip.route_id = "B"

        for i in range(5):
            stu = tu.stop_time_update.add()
            stu.stop_sequence = i + 1
            stu.stop_id = f"S{i}"
            stu.arrival.delay = i * 30

        records = extract_trip_updates(feed, FETCH_ID, FETCHED_AT)
        assert len(records) == 5

    def test_empty_feed_returns_empty_list(self):
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.header.gtfs_realtime_version = "2.0"
        records = extract_trip_updates(feed, FETCH_ID, FETCHED_AT)
        assert records == []

    def test_entity_without_trip_update_is_skipped(self):
        feed = make_feed_with_vehicle()   # vehicle entity, not trip update
        records = extract_trip_updates(feed, FETCH_ID, FETCHED_AT)
        assert records == []


# ── Vehicle position tests ────────────────────────────────────────────────────

class TestExtractVehiclePositions:

    def test_returns_one_record_per_vehicle(self):
        feed = make_feed_with_vehicle()
        records = extract_vehicle_positions(feed, FETCH_ID, FETCHED_AT)
        assert len(records) == 1

    def test_vehicle_fields_parsed(self):
        feed = make_feed_with_vehicle(
            vehicle_id="V42", trip_id="T1", route_id="C",
            lat=40.7580, lon=-73.9855
        )
        record = extract_vehicle_positions(feed, FETCH_ID, FETCHED_AT)[0]
        assert record["vehicle_id"] == "V42"
        assert record["trip_id"] == "T1"
        assert record["route_id"] == "C"
        assert record["latitude"] == pytest.approx(40.7580, abs=1e-4)
        assert record["longitude"] == pytest.approx(-73.9855, abs=1e-4)

    def test_status_mapped_to_string(self):
        feed = make_feed_with_vehicle(status=1)  # STOPPED_AT
        record = extract_vehicle_positions(feed, FETCH_ID, FETCHED_AT)[0]
        assert record["current_status"] == "STOPPED_AT"

    def test_entity_without_vehicle_is_skipped(self):
        feed = make_feed_with_trip_update()
        records = extract_vehicle_positions(feed, FETCH_ID, FETCHED_AT)
        assert records == []


# ── Enum helper tests ─────────────────────────────────────────────────────────

class TestHelpers:

    def test_schedule_relationship_scheduled(self):
        assert _schedule_relationship(0) == "SCHEDULED"

    def test_schedule_relationship_added(self):
        assert _schedule_relationship(1) == "ADDED"

    def test_schedule_relationship_unknown(self):
        assert _schedule_relationship(99) is None

    def test_vehicle_status_stopped(self):
        assert _vehicle_status(1) == "STOPPED_AT"

    def test_vehicle_status_in_transit(self):
        assert _vehicle_status(2) == "IN_TRANSIT_TO"

    def test_vehicle_status_unknown(self):
        assert _vehicle_status(99) is None


# ── parse_feed tests ──────────────────────────────────────────────────────────

class TestParseFeed:

    def test_parses_valid_protobuf(self):
        original = make_feed_with_trip_update()
        raw_bytes = original.SerializeToString()
        parsed = parse_feed(raw_bytes)
        assert len(parsed.entity) == 1

    def test_invalid_bytes_raises(self):
        with pytest.raises(Exception):
            parse_feed(b"this is not a valid protobuf")
