"""
Inspect the live BAS.MY Kota Bharu GTFS-Realtime vehicle-position feed.

Fetches the feed once, parses it with the official GTFS-Realtime protobuf
schema, and reports exactly which fields are populated for each vehicle --
nothing here is assumed, every optional field is checked with HasField()
before being read.

Source: https://developer.data.gov.my/realtime-api/gtfs-realtime
        (mybas-kota-bharu vehicle-position feed, updated every 30s)
"""

import sys
from collections import Counter
from datetime import datetime, timezone

import requests
from google.transit import gtfs_realtime_pb2

FEED_URL = "https://api.data.gov.my/gtfs-realtime/vehicle-position/mybas-kota-bharu"

FIELDS = [
    "vehicle_id", "vehicle_label", "license_plate", "latitude", "longitude",
    "speed", "bearing", "trip_id", "route_id", "direction_id",
    "current_stop_sequence", "stop_id", "current_status", "congestion_level",
    "occupancy_status", "occupancy_percentage", "timestamp",
]


def fetch_feed() -> gtfs_realtime_pb2.FeedMessage:
    response = requests.get(FEED_URL, timeout=10)
    response.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)
    return feed


def describe_vehicle(entity) -> dict:
    vehicle = entity.vehicle
    info = {"entity_id": entity.id}

    if vehicle.HasField("vehicle"):
        vd = vehicle.vehicle
        info["vehicle_id"] = vd.id if vd.HasField("id") else None
        info["vehicle_label"] = vd.label if vd.HasField("label") else None
        info["license_plate"] = vd.license_plate if vd.HasField("license_plate") else None
    else:
        info["vehicle_id"] = info["vehicle_label"] = info["license_plate"] = None

    if vehicle.HasField("position"):
        pos = vehicle.position
        info["latitude"] = pos.latitude
        info["longitude"] = pos.longitude
        info["speed"] = pos.speed if pos.HasField("speed") else None
        info["bearing"] = pos.bearing if pos.HasField("bearing") else None
    else:
        info["latitude"] = info["longitude"] = info["speed"] = info["bearing"] = None

    if vehicle.HasField("trip"):
        trip = vehicle.trip
        info["trip_id"] = trip.trip_id if trip.HasField("trip_id") else None
        info["route_id"] = trip.route_id if trip.HasField("route_id") else None
        info["direction_id"] = trip.direction_id if trip.HasField("direction_id") else None
    else:
        info["trip_id"] = info["route_id"] = info["direction_id"] = None

    info["current_stop_sequence"] = (
        vehicle.current_stop_sequence if vehicle.HasField("current_stop_sequence") else None
    )
    info["stop_id"] = vehicle.stop_id if vehicle.HasField("stop_id") else None

    # current_status and congestion_level have proto2 defaults (IN_TRANSIT_TO,
    # UNKNOWN_CONGESTION_LEVEL), so we report whether they were EXPLICITLY set,
    # not just their effective (possibly default) value.
    info["current_status"] = (
        gtfs_realtime_pb2.VehiclePosition.VehicleStopStatus.Name(vehicle.current_status)
        if vehicle.HasField("current_status") else None
    )
    info["congestion_level"] = (
        gtfs_realtime_pb2.VehiclePosition.CongestionLevel.Name(vehicle.congestion_level)
        if vehicle.HasField("congestion_level") else None
    )
    info["occupancy_status"] = (
        gtfs_realtime_pb2.VehiclePosition.OccupancyStatus.Name(vehicle.occupancy_status)
        if vehicle.HasField("occupancy_status") else None
    )
    info["occupancy_percentage"] = (
        vehicle.occupancy_percentage if vehicle.HasField("occupancy_percentage") else None
    )

    if vehicle.HasField("timestamp"):
        info["timestamp"] = vehicle.timestamp
        info["timestamp_readable"] = datetime.fromtimestamp(
            vehicle.timestamp, tz=timezone.utc
        ).isoformat()
    else:
        info["timestamp"] = info["timestamp_readable"] = None

    return info


def main():
    print(f"Fetching feed: {FEED_URL}")
    feed = fetch_feed()

    print(f"GTFS-Realtime version: {feed.header.gtfs_realtime_version}")
    print(f"Feed header timestamp: {feed.header.timestamp}", end="")
    if feed.header.timestamp:
        readable = datetime.fromtimestamp(feed.header.timestamp, tz=timezone.utc).isoformat()
        print(f"  ({readable})")
    else:
        print()
    print(f"Total entities in feed: {len(feed.entity)}")

    vehicles = [e for e in feed.entity if e.HasField("vehicle")]
    print(f"Entities with a 'vehicle' payload: {len(vehicles)}")
    print("-" * 70)

    field_presence = Counter()
    rows = [describe_vehicle(entity) for entity in vehicles]

    for info in rows:
        for field in FIELDS:
            if info.get(field) is not None:
                field_presence[field] += 1

    for info in rows:
        print(f"Entity ID:      {info['entity_id']}")
        print(f"  Vehicle ID:      {info['vehicle_id']}")
        print(f"  Vehicle Label:   {info['vehicle_label']}")
        print(f"  License Plate:   {info['license_plate']}")
        print(f"  Latitude:        {info['latitude']}")
        print(f"  Longitude:       {info['longitude']}")
        print(f"  Speed:           {info['speed']}")
        print(f"  Bearing:         {info['bearing']}")
        print(f"  Trip ID:         {info['trip_id']}")
        print(f"  Route ID:        {info['route_id']}")
        print(f"  Direction ID:    {info['direction_id']}")
        print(f"  Stop Sequence:   {info['current_stop_sequence']}")
        print(f"  Stop ID:         {info['stop_id']}")
        print(f"  Current Status:  {info['current_status']}")
        print(f"  Congestion Lvl:  {info['congestion_level']}")
        print(f"  Occupancy Stat:  {info['occupancy_status']}")
        print(f"  Occupancy Pct:   {info['occupancy_percentage']}")
        print(f"  Timestamp:       {info['timestamp']} ({info['timestamp_readable']})")
        print("-" * 70)

    print("\nField availability across all vehicles in this poll:")
    total = len(vehicles) or 1
    for field in FIELDS:
        count = field_presence.get(field, 0)
        print(f"  {field:<24} {count}/{total}")

    if vehicles:
        print("\nRaw protobuf of first entity (ground truth, for reference):")
        print(vehicles[0])


if __name__ == "__main__":
    try:
        main()
    except requests.RequestException as exc:
        print(f"Failed to fetch feed: {exc}", file=sys.stderr)
        sys.exit(1)
