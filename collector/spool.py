"""
Local spool: gzipped newline-delimited JSON, one file per agency per UTC hour
per collector run.

Layout (relative to the spool root, and identical under the GCS prefix):

    agency=<agency>/date=<YYYY-MM-DD>/hour=<HH>/part-<host>-<run_id>.jsonl.gz

Every append writes one *complete* gzip member and fsyncs it. A gzip file made
of several concatenated members is still a valid gzip file (gzip, zcat,
Python's gzip module, Spark and BigQuery all read it as one stream), and a
crash mid-write can only damage the last member, never earlier polls.

`run_id` (process start time) is part of the filename, so a restarted
collector never appends to a file a previous run may already have uploaded.
"""

import gzip
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

SUFFIX = ".jsonl.gz"


def hour_key(ts: datetime) -> tuple[str, str]:
    """(YYYY-MM-DD, HH) of a timestamp, in UTC."""
    ts = ts.astimezone(timezone.utc)
    return ts.strftime("%Y-%m-%d"), ts.strftime("%H")


def partition_path(agency: str, ts: datetime) -> str:
    date, hour = hour_key(ts)
    return f"agency={agency}/date={date}/hour={hour}"


@dataclass(frozen=True)
class SpoolFile:
    path: Path
    relative: str  # forward-slash path relative to the spool root (== GCS object suffix)
    date: str
    hour: str

    def is_before_hour(self, ts: datetime) -> bool:
        return (self.date, self.hour) < hour_key(ts)


class Spool:
    def __init__(self, root: Path | str, host: str, run_id: str):
        self.root = Path(root)
        self.host = host
        self.run_id = run_id
        self.root.mkdir(parents=True, exist_ok=True)

    def file_for(self, agency: str, ts: datetime) -> Path:
        return self.root / partition_path(agency, ts) / f"part-{self.host}-{self.run_id}{SUFFIX}"

    def append(self, agency: str, ts: datetime, records: list[dict]) -> Path | None:
        """Append records for one poll of one feed. No-op for an empty list."""
        if not records:
            return None
        path = self.file_for(agency, ts)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = "".join(json.dumps(r, separators=(",", ":"), ensure_ascii=False) + "\n" for r in records)
        member = gzip.compress(payload.encode("utf-8"))
        with open(path, "ab") as fh:
            fh.write(member)
            fh.flush()
            os.fsync(fh.fileno())
        return path

    def files(self) -> list[SpoolFile]:
        """Every spool file on disk, including leftovers from earlier runs."""
        found = []
        for path in sorted(self.root.glob(f"agency=*/date=*/hour=*/*{SUFFIX}")):
            rel = path.relative_to(self.root).as_posix()
            date = path.parent.parent.name.removeprefix("date=")
            hour = path.parent.name.removeprefix("hour=")
            found.append(SpoolFile(path=path, relative=rel, date=date, hour=hour))
        return found

    def closed_files(self, now: datetime) -> list[SpoolFile]:
        """Files for hours that have ended -- nothing will append to them again."""
        return [f for f in self.files() if f.is_before_hour(now)]

    def remove(self, spool_file: SpoolFile) -> None:
        spool_file.path.unlink()
        # Tidy now-empty partition directories, but never the root itself.
        parent = spool_file.path.parent
        while parent != self.root:
            try:
                parent.rmdir()
            except OSError:
                break
            parent = parent.parent
