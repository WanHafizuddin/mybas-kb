"""
Always-on raw collector for GTFS-Realtime vehicle positions.

Every `--interval` seconds (default 30) it polls every feed in
config/feeds.toml in parallel, writes one JSON line per vehicle entity to a
local gzipped spool file, and once per UTC hour uploads the finished hour's
files to Cloud Storage (or a local folder), deleting each local file only after
its upload is confirmed.

This is the raw layer: every field the feed sends is kept as-is (via
basmy.gtfs_rt.entity_to_raw), plus agency, feed URL, the feed header and our
own fetch timestamp. No deduplication, no cleaning.

Usage:
    # production (systemd): upload to GCS using the VM's service account
    python -m collector.main --sink gcs --bucket MY_BUCKET --spool-dir /var/lib/basmy-collector/spool

    # local test run: write the same layout to ./out, stop after 2 minutes
    python -m collector.main --sink local --out ./out --spool-dir ./spool --duration 120
"""

import argparse
import json
import logging
import os
import signal
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import requests
from google.protobuf.message import DecodeError

from basmy import gtfs_rt
from basmy.feeds import DEFAULT_CONFIG_PATH, Feed, load_feeds
from collector.sinks import GcsSink, LocalSink, upload_files
from collector.spool import Spool, hour_key

log = logging.getLogger("collector")

MAX_BACKOFF_SECONDS = 600


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


@dataclass
class FeedState:
    feed: Feed
    session: requests.Session = field(default_factory=requests.Session)
    consecutive_failures: int = 0
    next_attempt_at: float = 0.0  # monotonic seconds
    last_success_at: str | None = None
    last_error_at: str | None = None
    last_error: str | None = None
    last_entity_count: int | None = None
    total_polls: int = 0
    total_records: int = 0


@dataclass
class PollResult:
    agency: str
    fetched_at: datetime
    records: list[dict] | None = None
    error: Exception | None = None


def build_records(feed: Feed, fetched_at: datetime, payload: bytes) -> list[dict]:
    message = gtfs_rt.parse_feed(payload)
    header = gtfs_rt.header_to_raw(message)
    fetched = iso(fetched_at)
    return [
        {
            "agency": feed.agency,
            "feed_url": feed.url,
            "fetched_at": fetched,
            "header": header,
            "entity": gtfs_rt.entity_to_raw(entity),
        }
        for entity in message.entity
    ]


class Collector:
    def __init__(
        self,
        feeds: list[Feed],
        spool: Spool,
        sink,
        interval: float = 30.0,
        health_path: Path | None = None,
        fetch: Callable[[Feed, requests.Session], bytes] | None = None,
        clock: Callable[[], datetime] = utcnow,
        monotonic: Callable[[], float] = time.monotonic,
    ):
        self.states = {f.agency: FeedState(feed=f) for f in feeds}
        self.spool = spool
        self.sink = sink
        self.interval = interval
        self.health_path = health_path
        self.fetch = fetch or (lambda feed, session: gtfs_rt.fetch_feed_bytes(feed.url, session=session))
        self.clock = clock
        self.monotonic = monotonic
        self.stop_event = threading.Event()
        self.executor = ThreadPoolExecutor(max_workers=max(1, len(feeds)), thread_name_prefix="feed")
        self.current_hour = hour_key(clock())
        self.started_at = iso(clock())

    # --- one feed -----------------------------------------------------------

    def _poll_feed(self, state: FeedState) -> PollResult:
        fetched_at = self.clock()
        try:
            payload = self.fetch(state.feed, state.session)
            records = build_records(state.feed, fetched_at, payload)
            return PollResult(state.feed.agency, fetched_at, records=records)
        except Exception as exc:  # noqa: BLE001 -- isolate every failure to its own feed
            return PollResult(state.feed.agency, fetched_at, error=exc)

    def _record_result(self, result: PollResult) -> None:
        state = self.states[result.agency]
        state.total_polls += 1
        if result.error is None:
            try:
                self.spool.append(result.agency, result.fetched_at, result.records)
            except OSError as exc:
                # Disk problem: count it against this feed but keep going.
                result.error = exc
        if result.error is None:
            state.consecutive_failures = 0
            state.next_attempt_at = 0.0
            state.last_success_at = iso(result.fetched_at)
            state.last_entity_count = len(result.records)
            state.total_records += len(result.records)
            if result.records:
                log.info("%s: %d vehicles", result.agency, len(result.records))
            else:
                log.info("%s: empty feed (normal outside service hours)", result.agency)
            return

        state.consecutive_failures += 1
        backoff = min(self.interval * 2 ** (state.consecutive_failures - 1), MAX_BACKOFF_SECONDS)
        state.next_attempt_at = self.monotonic() + backoff
        state.last_error_at = iso(result.fetched_at)
        state.last_error = f"{type(result.error).__name__}: {result.error}"
        kind = (
            "network error"
            if isinstance(result.error, requests.RequestException)
            else "bad payload"
            if isinstance(result.error, DecodeError)
            else "error"
        )
        log.warning(
            "%s: %s (%s); failure %d in a row, next try in %.0fs",
            result.agency, kind, state.last_error, state.consecutive_failures, backoff,
        )

    # --- one tick -----------------------------------------------------------

    def tick(self) -> None:
        now_mono = self.monotonic()
        due = [s for s in self.states.values() if s.next_attempt_at <= now_mono]
        for result in self.executor.map(self._poll_feed, due):
            self._record_result(result)

        hour = hour_key(self.clock())
        if hour != self.current_hour:
            self.current_hour = hour
            self.upload_closed()
        self.write_health()

    def upload_closed(self) -> None:
        files = self.spool.closed_files(self.clock())
        if files:
            upload_files(self.spool, self.sink, files)

    def upload_all(self) -> None:
        files = self.spool.files()
        if files:
            upload_files(self.spool, self.sink, files)

    def write_health(self) -> None:
        if self.health_path is None:
            return
        health = {
            "written_at": iso(self.clock()),
            "started_at": self.started_at,
            "sink": self.sink.describe(),
            "pending_spool_files": len(self.spool.files()),
            "feeds": {
                agency: {
                    "url": s.feed.url,
                    "last_success_at": s.last_success_at,
                    "last_entity_count": s.last_entity_count,
                    "consecutive_failures": s.consecutive_failures,
                    "last_error_at": s.last_error_at,
                    "last_error": s.last_error,
                    "total_polls": s.total_polls,
                    "total_records": s.total_records,
                }
                for agency, s in self.states.items()
            },
        }
        tmp = self.health_path.with_name(self.health_path.name + ".tmp")
        tmp.write_text(json.dumps(health, indent=2))
        os.replace(tmp, self.health_path)

    # --- lifecycle ----------------------------------------------------------

    def run(self, duration: float | None = None) -> None:
        log.info("starting: %d feeds, interval %.0fs, sink %s", len(self.states), self.interval, self.sink.describe())
        leftovers = self.spool.files()
        if leftovers:
            log.info("uploading %d leftover spool files from earlier runs", len(leftovers))
            upload_files(self.spool, self.sink, leftovers)

        deadline = None if duration is None else self.monotonic() + duration
        try:
            while not self.stop_event.is_set():
                started = self.monotonic()
                self.tick()
                if deadline is not None and self.monotonic() >= deadline:
                    break
                # Fixed cadence: sleep whatever is left of the interval. wait()
                # returns early on SIGTERM.
                self.stop_event.wait(max(0.0, self.interval - (self.monotonic() - started)))
        finally:
            log.info("shutting down: flushing and uploading all spool files")
            self.executor.shutdown(wait=True)
            self.upload_all()
            self.write_health()
            remaining = len(self.spool.files())
            if remaining:
                log.error("%d spool files could not be uploaded; they will be retried on next start", remaining)
            log.info("stopped")

    def request_stop(self, signum=None, _frame=None) -> None:
        log.info("received signal %s, stopping after current poll", signum)
        self.stop_event.set()


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Always-on GTFS-Realtime raw collector")
    p.add_argument("--config", default=os.environ.get("BASMY_FEEDS_CONFIG", str(DEFAULT_CONFIG_PATH)))
    p.add_argument("--sink", choices=["gcs", "local"], default=os.environ.get("BASMY_SINK", "gcs"))
    p.add_argument("--bucket", default=os.environ.get("BASMY_BUCKET"), help="GCS bucket (sink=gcs)")
    p.add_argument("--out", default=os.environ.get("BASMY_LOCAL_OUT", "./out"), help="output folder (sink=local)")
    p.add_argument("--spool-dir", default=os.environ.get("BASMY_SPOOL_DIR", "./spool"))
    p.add_argument("--health-file", default=os.environ.get("BASMY_HEALTH_FILE"),
                   help="default: <spool-dir>/../health.json")
    p.add_argument("--interval", type=float, default=float(os.environ.get("BASMY_INTERVAL", "30")))
    p.add_argument("--duration", type=float, default=None, help="stop after N seconds (test runs)")
    return p.parse_args(argv)


def main(argv=None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logging.Formatter.converter = time.gmtime  # log timestamps in UTC, like the data
    args = parse_args(argv)

    if args.sink == "gcs":
        if not args.bucket:
            raise SystemExit("--bucket (or BASMY_BUCKET) is required for --sink gcs")
        sink = GcsSink(args.bucket)
    else:
        sink = LocalSink(args.out)

    spool_dir = Path(args.spool_dir)
    health = Path(args.health_file) if args.health_file else spool_dir.resolve().parent / "health.json"
    run_id = utcnow().strftime("%Y%m%dT%H%M%SZ")
    collector = Collector(
        feeds=load_feeds(args.config),
        spool=Spool(spool_dir, host=socket.gethostname(), run_id=run_id),
        sink=sink,
        interval=args.interval,
        health_path=health,
    )
    signal.signal(signal.SIGTERM, collector.request_stop)
    signal.signal(signal.SIGINT, collector.request_stop)
    collector.run(duration=args.duration)


if __name__ == "__main__":
    main()
