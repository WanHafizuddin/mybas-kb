import json
from pathlib import Path

import pytest
from google.protobuf.message import DecodeError

from basmy import gtfs_rt
from basmy.feeds import load_feeds
from tests.fixtures.build_fixture import (
    HEADER_TS,
    as_bytes,
    empty_feed,
    kota_bharu_feed,
    rapid_kl_feed,
)

GOLDEN = Path(__file__).parent / "fixtures" / "extract_row_golden.json"


def _jsonable(row):
    return {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in row.items()}


def test_parse_feed_roundtrip():
    feed = gtfs_rt.parse_feed(as_bytes(kota_bharu_feed()))
    assert feed.header.timestamp == HEADER_TS
    assert [e.id for e in feed.entity] == ["VKB1234", "VKB5678", "VFAR001"]


def test_parse_feed_empty_is_valid():
    feed = gtfs_rt.parse_feed(as_bytes(empty_feed()))
    assert len(feed.entity) == 0


def test_parse_feed_rejects_html_error_page():
    with pytest.raises(DecodeError):
        gtfs_rt.parse_feed(b"<html><body>502 Bad Gateway</body></html>")


def test_entity_to_raw_keeps_every_field_sent():
    entity = gtfs_rt.parse_feed(as_bytes(rapid_kl_feed())).entity[0]
    raw = gtfs_rt.entity_to_raw(entity)
    v = raw["vehicle"]
    assert raw["id"] == "WB1234A"
    assert v["trip"] == {
        "trip_id": "260716010110S1",
        "start_time": "05:30:00",
        "start_date": "20261114",
        "route_id": "T789",
    }
    assert v["vehicle"] == {"id": "WB1234A", "license_plate": "WB1234A"}
    assert v["position"]["speed"] == pytest.approx(8.3, rel=1e-6)
    assert v["current_status"] == "IN_TRANSIT_TO"
    assert v["occupancy_status"] == "FEW_SEATS_AVAILABLE"
    assert v["timestamp"] == str(HEADER_TS + 30)  # uint64 -> string in proto JSON


def test_entity_to_raw_omits_absent_fields():
    # Kota Bharu idle bus: no trip block on the wire, so none in the record.
    entity = gtfs_rt.parse_feed(as_bytes(kota_bharu_feed())).entity[0]
    raw = gtfs_rt.entity_to_raw(entity)
    assert "trip" not in raw["vehicle"]
    assert "speed" not in raw["vehicle"]["position"]


def test_header_to_raw():
    header = gtfs_rt.header_to_raw(gtfs_rt.parse_feed(as_bytes(empty_feed())))
    assert header == {
        "gtfs_realtime_version": "2.0",
        "incrementality": "FULL_DATASET",
        "timestamp": str(HEADER_TS),
    }


@pytest.mark.parametrize("name,builder", [("kota_bharu", kota_bharu_feed), ("rapid_kl", rapid_kl_feed)])
def test_extract_row_matches_pre_refactor_golden(name, builder):
    """Golden output was captured from realtime/collector.py BEFORE its
    fetch/parse/extract logic moved into basmy.gtfs_rt. Supabase rows must
    stay byte-for-byte identical."""
    expected = json.loads(GOLDEN.read_text())[name]
    feed = builder()
    hdr = gtfs_rt.to_utc(feed.header.timestamp)
    actual = []
    for entity in feed.entity:
        row = gtfs_rt.extract_row(entity, hdr)
        actual.append({"row": _jsonable(row), "warnings": gtfs_rt.validate(row)})
    assert actual == expected


def test_realtime_collector_uses_shared_module():
    # Load by file path under a distinct name: "collector" is also the name of
    # the new top-level collector/ package.
    import importlib.util

    path = Path(__file__).resolve().parent.parent / "realtime" / "collector.py"
    spec = importlib.util.spec_from_file_location("realtime_collector", path)
    rc = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(rc)
    assert rc.extract_row is gtfs_rt.extract_row
    assert rc.fetch_feed is gtfs_rt.fetch_feed
    assert rc.FEED_URL == "https://api.data.gov.my/gtfs-realtime/vehicle-position/mybas-kota-bharu"


def test_fetch_feed_bytes_sends_timeout_and_raises_on_http_error():
    class FakeResponse:
        content = b"payload"

        def __init__(self, status):
            self.status = status

        def raise_for_status(self):
            if self.status >= 400:
                import requests

                raise requests.HTTPError(f"{self.status}")

    class FakeSession:
        def __init__(self, status):
            self.status = status
            self.calls = []

        def get(self, url, timeout, headers):
            self.calls.append((url, timeout))
            return FakeResponse(self.status)

    ok = FakeSession(200)
    assert gtfs_rt.fetch_feed_bytes("http://x", timeout=3, session=ok) == b"payload"
    assert ok.calls == [("http://x", 3)]

    import requests

    with pytest.raises(requests.HTTPError):
        gtfs_rt.fetch_feed_bytes("http://x", session=FakeSession(503))


def test_load_feeds_default_config():
    feeds = load_feeds()
    assert [f.agency for f in feeds] == ["mybas-kota-bharu"]
    assert feeds[0].url == "https://api.data.gov.my/gtfs-realtime/vehicle-position/mybas-kota-bharu"


def test_load_feeds_query_string_path(tmp_path):
    cfg = tmp_path / "feeds.toml"
    cfg.write_text('[[feeds]]\nagency = "rapid-bus-kl"\npath = "prasarana?category=rapid-bus-kl"\n')
    (feed,) = load_feeds(cfg)
    assert feed.url == "https://api.data.gov.my/gtfs-realtime/vehicle-position/prasarana?category=rapid-bus-kl"


def test_load_feeds_rejects_unsafe_agency(tmp_path):
    cfg = tmp_path / "feeds.toml"
    cfg.write_text('[[feeds]]\nagency = "Bad/Name"\npath = "x"\n')
    with pytest.raises(ValueError, match="invalid agency"):
        load_feeds(cfg)
