import gzip
from datetime import datetime, timezone

import pytest
from google.api_core.exceptions import PreconditionFailed, ServiceUnavailable

from collector.sinks import GcsSink, LocalSink, upload_files
from collector.spool import Spool

TS = datetime(2026, 11, 13, 9, 5, tzinfo=timezone.utc)


class FakeBlob:
    def __init__(self, bucket, name):
        self.bucket = bucket
        self.name = name

    def upload_from_filename(self, filename, **kwargs):
        self.bucket.calls.append((self.name, kwargs))
        if self.bucket.fail_with is not None:
            raise self.bucket.fail_with
        with open(filename, "rb") as fh:
            self.bucket.objects[self.name] = fh.read()


class FakeBucket:
    def __init__(self):
        self.objects = {}
        self.calls = []
        self.fail_with = None

    def blob(self, name):
        return FakeBlob(self, name)


class FakeClient:
    def __init__(self):
        self.buckets = {}

    def bucket(self, name):
        return self.buckets.setdefault(name, FakeBucket())


@pytest.fixture
def spool(tmp_path):
    s = Spool(tmp_path / "spool", host="vm1", run_id="r1")
    s.append("mybas-kota-bharu", TS, [{"n": 1}])
    return s


def test_gcs_object_path_and_create_only_precondition(spool):
    client = FakeClient()
    sink = GcsSink("my-bucket", client=client)
    uploaded, failed = upload_files(spool, sink, spool.files())

    assert (uploaded, failed) == (1, 0)
    bucket = client.buckets["my-bucket"]
    name = "raw/vehicle_positions/agency=mybas-kota-bharu/date=2026-11-13/hour=09/part-vm1-r1.jsonl.gz"
    assert list(bucket.objects) == [name]
    assert gzip.decompress(bucket.objects[name]) == b'{"n":1}\n'
    (_, kwargs), = bucket.calls
    assert kwargs["if_generation_match"] == 0
    assert kwargs["checksum"] == "md5"
    assert spool.files() == []  # deleted only after the confirmed upload


def test_failed_upload_keeps_local_file(spool):
    client = FakeClient()
    sink = GcsSink("my-bucket", client=client)
    client.buckets["my-bucket"].fail_with = ServiceUnavailable("gcs down")

    uploaded, failed = upload_files(spool, sink, spool.files())
    assert (uploaded, failed) == (0, 1)
    assert len(spool.files()) == 1

    # Next attempt succeeds and only then removes the file.
    client.buckets["my-bucket"].fail_with = None
    assert upload_files(spool, sink, spool.files()) == (1, 0)
    assert spool.files() == []


def test_already_uploaded_object_counts_as_done(spool):
    client = FakeClient()
    sink = GcsSink("my-bucket", client=client)
    client.buckets["my-bucket"].fail_with = PreconditionFailed("exists")
    assert upload_files(spool, sink, spool.files()) == (1, 0)
    assert spool.files() == []


def test_local_sink_mirrors_gcs_layout(spool, tmp_path):
    out = tmp_path / "out"
    assert upload_files(spool, LocalSink(out), spool.files()) == (1, 0)
    dest = out / "raw/vehicle_positions/agency=mybas-kota-bharu/date=2026-11-13/hour=09/part-vm1-r1.jsonl.gz"
    assert gzip.decompress(dest.read_bytes()) == b'{"n":1}\n'
    assert not list(out.rglob("*.tmp"))
