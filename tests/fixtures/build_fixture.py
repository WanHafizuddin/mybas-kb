"""
Build GTFS-Realtime FeedMessage fixtures in code, so tests never touch the
real network. Shapes mirror what the live feeds were observed to send:

- Kota Bharu (mybas-kota-bharu): vehicle descriptor + position + timestamp,
  usually no trip block outside service hours; trip_id only during service.
- Rapid KL (prasarana?category=rapid-bus-kl): also trip_id, route_id,
  start_time, start_date and speed.
- Empty feed: header only, zero entities (normal at night).
"""

from google.transit import gtfs_realtime_pb2

HEADER_TS = 1794603722  # 2026-11-13T21:02:02Z


def _header(feed: gtfs_realtime_pb2.FeedMessage, ts: int = HEADER_TS) -> None:
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.incrementality = gtfs_realtime_pb2.FeedHeader.FULL_DATASET
    feed.header.timestamp = ts


def kota_bharu_feed() -> gtfs_realtime_pb2.FeedMessage:
    feed = gtfs_realtime_pb2.FeedMessage()
    _header(feed)

    # Idle bus: no trip block, bearing only.
    e = feed.entity.add()
    e.id = "VKB1234"
    v = e.vehicle
    v.vehicle.id = "VKB1234"
    v.vehicle.label = "VKB1234"
    v.vehicle.license_plate = "VKB1234"
    v.position.latitude = 6.1254
    v.position.longitude = 102.2386
    v.position.bearing = 90.0
    v.timestamp = HEADER_TS - 5

    # In-service bus: trip_id only (the feed never sends route_id here).
    e = feed.entity.add()
    e.id = "VKB5678"
    v = e.vehicle
    v.vehicle.id = "VKB5678"
    v.vehicle.license_plate = "VKB5678"
    v.trip.trip_id = "weekday_C10_0_trip3"
    v.position.latitude = 6.1301
    v.position.longitude = 102.2402
    v.position.bearing = 180.0
    v.timestamp = HEADER_TS - 12

    # Outside the Kelantan bounding box -- validate() should warn, not drop.
    e = feed.entity.add()
    e.id = "VFAR001"
    v = e.vehicle
    v.vehicle.id = "VFAR001"
    v.position.latitude = 3.1390
    v.position.longitude = 101.6869
    v.timestamp = HEADER_TS - 30
    return feed


def rapid_kl_feed() -> gtfs_realtime_pb2.FeedMessage:
    feed = gtfs_realtime_pb2.FeedMessage()
    _header(feed, HEADER_TS + 34)
    e = feed.entity.add()
    e.id = "WB1234A"
    v = e.vehicle
    v.trip.trip_id = "260716010110S1"
    v.trip.start_time = "05:30:00"
    v.trip.start_date = "20261114"
    v.trip.route_id = "T789"
    v.vehicle.id = "WB1234A"
    v.vehicle.license_plate = "WB1234A"
    v.position.latitude = 3.0738
    v.position.longitude = 101.5183
    v.position.bearing = 270.0
    v.position.speed = 8.3
    v.current_status = gtfs_realtime_pb2.VehiclePosition.IN_TRANSIT_TO
    v.occupancy_status = gtfs_realtime_pb2.VehiclePosition.FEW_SEATS_AVAILABLE
    v.timestamp = HEADER_TS + 30
    return feed


def empty_feed() -> gtfs_realtime_pb2.FeedMessage:
    feed = gtfs_realtime_pb2.FeedMessage()
    _header(feed)
    return feed


def as_bytes(feed: gtfs_realtime_pb2.FeedMessage) -> bytes:
    return feed.SerializeToString()
