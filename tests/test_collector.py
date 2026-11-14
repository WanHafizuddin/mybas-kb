import gzip
import json
from datetime import datetime, timedelta, timezone

import pytest
import requests

from basmy.feeds import Feed
from collector.main import MAX_BACKOFF_SECONDS, Collector
from collector.sinks import LocalSink
from collector.spool import Spool
from tests.fixtures.build_fixture import as_bytes, empty_feed, kota_bharu_feed, rapid_kl_feed

FEEDS = [
    Feed("mybas-kota-bharu", "https://example.test/kb"),
    Feed("rapid-bus-kl", "https://example.test/kl"),
    Feed("rapid-bus-penang", "https://example.test/penang"),
]


class FakeClock:
    def __init__(self, start):
        self.now = start
        self.mono = 1000.0

    def __call__(self):
        return self.now

    def monotonic(self):
        return self.mono

    def advance(self, seconds):
        self.now += timedelta(seconds=seconds)
        self.mono += seconds


class FakeFetch:
    """Per-agency scripted responses: bytes, or an exception to raise."""

    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    def __call__(self, feed, session):
        self.calls.append(feed.agency)
        r = self.responses[feed.agency]
        if isinstance(r, Exception):
            raise r
        return r


def make_collector(tmp_path, fetch, clock, sink=None):
    return Collector(
        feeds=FEEDS,
        spool=Spool(tmp_path / "spool", host="vm1", run_id="r1"),
        sink=sink or LocalSink(tmp_path / "out"),
        interval=30,
        health_path=tmp_path / "health.json",
        fetch=fetch,
        clock=clock,
        monotonic=clock.monotonic,
    )


def records_for(spool, agency):
    out = []
    for f in spool.files():
        if f.relative.startswith(f"agency={agency}/"):
            with gzip.open(f.path, "rt") as fh:
                out += [json.loads(line) for line in fh]
    return out


@pytest.fixture
def clock():
    return FakeClock(datetime(2026, 11, 13, 9, 0, 5, tzinfo=timezone.utc))


def test_one_failing_feed_does_not_stop_the_others(tmp_path, clock):
    fetch = FakeFetch({
        "mybas-kota-bharu": as_bytes(kota_bharu_feed()),
        "rapid-bus-kl": requests.ConnectionError("connection reset"),
        "rapid-bus-penang": as_bytes(empty_feed()),
    })
    c = make_collector(tmp_path, fetch, clock)
    c.tick()

    assert len(records_for(c.spool, "mybas-kota-bharu")) == 3
    assert records_for(c.spool, "rapid-bus-kl") == []
    assert c.states["rapid-bus-kl"].consecutive_failures == 1
    assert c.states["mybas-kota-bharu"].consecutive_failures == 0
    assert c.states["rapid-bus-penang"].last_entity_count == 0


def test_garbage_payload_and_unexpected_exception_are_isolated(tmp_path, clock):
    fetch = FakeFetch({
        "mybas-kota-bharu": b"<html>502</html>",
        "rapid-bus-kl": RuntimeError("boom"),
        "rapid-bus-penang": as_bytes(rapid_kl_feed()),
    })
    c = make_collector(tmp_path, fetch, clock)
    c.tick()
    assert c.states["mybas-kota-bharu"].last_error.startswith("DecodeError")
    assert c.states["rapid-bus-kl"].last_error == "RuntimeError: boom"
    assert len(records_for(c.spool, "rapid-bus-penang")) == 1


def test_record_shape_keeps_raw_fields_agency_and_fetch_time(tmp_path, clock):
    fetch = FakeFetch({
        "mybas-kota-bharu": as_bytes(empty_feed()),
        "rapid-bus-kl": as_bytes(rapid_kl_feed()),
        "rapid-bus-penang": as_bytes(empty_feed()),
    })
    c = make_collector(tmp_path, fetch, clock)
    c.tick()
    (rec,) = records_for(c.spool, "rapid-bus-kl")
    assert rec["agency"] == "rapid-bus-kl"
    assert rec["feed_url"] == "https://example.test/kl"
    assert rec["fetched_at"] == "2026-11-13T09:00:05.000Z"
    assert rec["header"]["gtfs_realtime_version"] == "2.0"
    assert rec["entity"]["vehicle"]["trip"]["route_id"] == "T789"


def test_backoff_grows_skips_feed_and_resets_on_success(tmp_path, clock):
    fetch = FakeFetch({
        "mybas-kota-bharu": as_bytes(empty_feed()),
        "rapid-bus-kl": requests.Timeout("slow"),
        "rapid-bus-penang": as_bytes(empty_feed()),
    })
    c = make_collector(tmp_path, fetch, clock)

    c.tick()  # KL failure 1 -> back off 30s
    clock.advance(30)
    c.tick()  # due again; failure 2 -> back off 60s
    assert c.states["rapid-bus-kl"].consecutive_failures == 2

    fetch.calls.clear()
    clock.advance(30)
    c.tick()  # only 30s of the 60s backoff elapsed: KL skipped, others polled
    assert fetch.calls.count("rapid-bus-kl") == 0
    assert fetch.calls.count("mybas-kota-bharu") == 1

    clock.advance(30)
    fetch.responses["rapid-bus-kl"] = as_bytes(rapid_kl_feed())
    c.tick()
    assert c.states["rapid-bus-kl"].consecutive_failures == 0
    assert len(records_for(c.spool, "rapid-bus-kl")) == 1


def test_backoff_is_capped(tmp_path, clock):
    fetch = FakeFetch({f.agency: requests.ConnectionError("down") for f in FEEDS})
    c = make_collector(tmp_path, fetch, clock)
    for _ in range(12):
        c.tick()
        clock.advance(MAX_BACKOFF_SECONDS)
    state = c.states["mybas-kota-bharu"]
    assert state.next_attempt_at - clock.monotonic() <= 0  # last wait was the cap
    assert state.consecutive_failures == 12


def test_hour_change_uploads_only_closed_hour(tmp_path, clock):
    fetch = FakeFetch({f.agency: as_bytes(kota_bharu_feed()) for f in FEEDS})
    clock.now = datetime(2026, 11, 13, 9, 59, 40, tzinfo=timezone.utc)
    c = make_collector(tmp_path, fetch, clock)
    c.tick()
    clock.advance(30)  # 10:00:10
    c.tick()

    out = tmp_path / "out" / "raw" / "vehicle_positions"
    uploaded = sorted(p.relative_to(out).as_posix() for p in out.rglob("*.jsonl.gz"))
    assert uploaded == [
        f"agency={f.agency}/date=2026-11-13/hour=09/part-vm1-r1.jsonl.gz" for f in FEEDS
    ]
    # hour=10 is still open and stays in the spool.
    assert {f.hour for f in c.spool.files()} == {"10"}


def test_startup_uploads_leftovers_and_shutdown_flushes_everything(tmp_path, clock):
    # A previous run left an unuploaded file behind.
    old = Spool(tmp_path / "spool", host="vm1", run_id="old")
    old.append("mybas-kota-bharu", clock() - timedelta(hours=3), [{"leftover": True}])

    fetch = FakeFetch({f.agency: as_bytes(kota_bharu_feed()) for f in FEEDS})
    c = make_collector(tmp_path, fetch, clock)
    c.stop_event.set()  # like SIGTERM arriving during the first poll
    c.run()

    out = tmp_path / "out" / "raw" / "vehicle_positions"
    names = sorted(p.name for p in out.rglob("*.jsonl.gz"))
    assert names.count("part-vm1-old.jsonl.gz") == 1
    assert c.spool.files() == []  # nothing left locally
    health = json.loads((tmp_path / "health.json").read_text())
    assert health["pending_spool_files"] == 0


def test_sigterm_mid_hour_uploads_open_hour(tmp_path, clock):
    fetch = FakeFetch({f.agency: as_bytes(kota_bharu_feed()) for f in FEEDS})
    c = make_collector(tmp_path, fetch, clock)
    c.run(duration=0)  # one tick, then normal shutdown path
    out = tmp_path / "out" / "raw" / "vehicle_positions"
    assert len(list(out.rglob("hour=09/*.jsonl.gz"))) == 3
    assert c.spool.files() == []


def test_health_file(tmp_path, clock):
    fetch = FakeFetch({
        "mybas-kota-bharu": as_bytes(kota_bharu_feed()),
        "rapid-bus-kl": requests.ConnectionError("down"),
        "rapid-bus-penang": as_bytes(empty_feed()),
    })
    c = make_collector(tmp_path, fetch, clock)
    c.tick()
    health = json.loads((tmp_path / "health.json").read_text())
    kb = health["feeds"]["mybas-kota-bharu"]
    assert kb["last_entity_count"] == 3 and kb["consecutive_failures"] == 0
    assert health["feeds"]["rapid-bus-kl"]["last_error"] == "ConnectionError: down"
