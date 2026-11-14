"""
Shared GTFS-Realtime fetch/parse logic for every consumer in this repo:
realtime/collector.py (Supabase), realtime/inspect_feed.py, and the
always-on raw collector in collector/. Later phases (Spark, ML, serving)
import from here too, so there is exactly one copy of this logic.

Two ways to turn a vehicle entity into a dict:

- extract_row(): the fixed 19-column shape of the Supabase vehicle_positions
  table. Unchanged from the original realtime/collector.py.
- entity_to_raw(): every field the feed actually sent, including ones we have
  never seen yet, via protobuf's own JSON mapping. Used for the raw layer,
  where nothing is dropped or renamed.
"""

from datetime import datetime, timezone

import requests
from google.protobuf.json_format import MessageToDict
from google.transit import gtfs_realtime_pb2

FEED_BASE_URL = "https://api.data.gov.my/gtfs-realtime/vehicle-position/"
KOTA_BHARU_VEHICLE_POSITIONS_URL = FEED_BASE_URL + "mybas-kota-bharu"

DEFAULT_TIMEOUT_SECONDS = 10
USER_AGENT = "basmy-collector/1.0"

# Loose bounding box around Kelantan/Kota Bharu. Positions outside this are
# logged as a warning (data quality signal worth keeping) but still stored --
# we don't have grounds to assume they're invalid, only unusual.
PLAUSIBLE_LAT_RANGE = (4.5, 7.0)
PLAUSIBLE_LON_RANGE = (100.5, 103.0)


# --- fetch + parse ---------------------------------------------------------

def fetch_feed_bytes(
    url: str = KOTA_BHARU_VEHICLE_POSITIONS_URL,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
    session: requests.Session | None = None,
) -> bytes:
    """GET the feed and return the raw protobuf bytes. Raises
    requests.RequestException on network/HTTP errors."""
    getter = session.get if session is not None else requests.get
    response = getter(url, timeout=timeout, headers={"User-Agent": USER_AGENT})
    response.raise_for_status()
    return response.content


def parse_feed(payload: bytes) -> gtfs_realtime_pb2.FeedMessage:
    """Parse protobuf bytes. Raises google.protobuf.message.DecodeError on
    anything that isn't a valid FeedMessage (e.g. an HTML error page)."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(payload)
    return feed


def fetch_feed(url: str = KOTA_BHARU_VEHICLE_POSITIONS_URL) -> gtfs_realtime_pb2.FeedMessage:
    return parse_feed(fetch_feed_bytes(url))


def to_utc(epoch_seconds) -> datetime | None:
    if not epoch_seconds:
        return None
    return datetime.fromtimestamp(epoch_seconds, tz=timezone.utc)


# --- raw layer -------------------------------------------------------------

def _to_dict(message) -> dict:
    # preserving_proto_field_name keeps snake_case names identical to the
    # GTFS-Realtime spec. Only fields actually set on the wire are emitted,
    # so absence in the dict means absence in the feed. Note: uint64 fields
    # (timestamps) become strings, per the protobuf JSON mapping.
    return MessageToDict(message, preserving_proto_field_name=True)


def header_to_raw(feed: gtfs_realtime_pb2.FeedMessage) -> dict:
    return _to_dict(feed.header)


def entity_to_raw(entity) -> dict:
    return _to_dict(entity)


# --- Supabase vehicle_positions shape ---------------------------------------

def extract_row(entity, feed_header_timestamp) -> dict:
    vehicle = entity.vehicle
    row = {"entity_id": entity.id, "feed_header_timestamp": feed_header_timestamp}

    if vehicle.HasField("vehicle"):
        vd = vehicle.vehicle
        row["vehicle_id"] = vd.id if vd.HasField("id") else None
        row["vehicle_label"] = vd.label if vd.HasField("label") else None
        row["license_plate"] = vd.license_plate if vd.HasField("license_plate") else None
    else:
        row["vehicle_id"] = row["vehicle_label"] = row["license_plate"] = None

    pos = vehicle.position
    row["latitude"] = pos.latitude
    row["longitude"] = pos.longitude
    row["bearing"] = pos.bearing if pos.HasField("bearing") else None
    row["speed"] = pos.speed if pos.HasField("speed") else None

    if vehicle.HasField("trip"):
        trip = vehicle.trip
        row["trip_id"] = trip.trip_id if trip.HasField("trip_id") else None
        row["route_id"] = trip.route_id if trip.HasField("route_id") else None
        row["direction_id"] = trip.direction_id if trip.HasField("direction_id") else None
    else:
        row["trip_id"] = row["route_id"] = row["direction_id"] = None

    row["current_stop_sequence"] = (
        vehicle.current_stop_sequence if vehicle.HasField("current_stop_sequence") else None
    )
    row["stop_id"] = vehicle.stop_id if vehicle.HasField("stop_id") else None
    row["current_status"] = (
        gtfs_realtime_pb2.VehiclePosition.VehicleStopStatus.Name(vehicle.current_status)
        if vehicle.HasField("current_status") else None
    )
    row["congestion_level"] = (
        gtfs_realtime_pb2.VehiclePosition.CongestionLevel.Name(vehicle.congestion_level)
        if vehicle.HasField("congestion_level") else None
    )
    row["occupancy_status"] = (
        gtfs_realtime_pb2.VehiclePosition.OccupancyStatus.Name(vehicle.occupancy_status)
        if vehicle.HasField("occupancy_status") else None
    )
    row["occupancy_percentage"] = (
        vehicle.occupancy_percentage if vehicle.HasField("occupancy_percentage") else None
    )
    row["vehicle_timestamp"] = to_utc(vehicle.timestamp if vehicle.HasField("timestamp") else None)

    return row


def validate(row: dict) -> list[str]:
    """Return a list of warnings. Never raises -- we store what the feed
    gives us and flag anomalies rather than silently discarding data."""
    warnings = []
    lat, lon = row["latitude"], row["longitude"]
    if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
        warnings.append(f"lat/lon out of Earth bounds: {lat}, {lon}")
    elif not (PLAUSIBLE_LAT_RANGE[0] <= lat <= PLAUSIBLE_LAT_RANGE[1]) or not (
        PLAUSIBLE_LON_RANGE[0] <= lon <= PLAUSIBLE_LON_RANGE[1]
    ):
        warnings.append(f"lat/lon outside plausible Kelantan bounding box: {lat}, {lon}")
    return warnings
