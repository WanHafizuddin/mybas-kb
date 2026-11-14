import gzip
import json
from datetime import datetime, timedelta, timezone

from collector.spool import Spool, hour_key, partition_path

UTC = timezone.utc
KL = timezone(timedelta(hours=8))


def read_lines(path):
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        return [json.loads(line) for line in fh]


def test_partition_path_uses_utc_even_for_local_time_input():
    # 07:15 in Kuala Lumpur on 14 Nov is 23:15 UTC on 13 Nov.
    ts = datetime(2026, 11, 14, 7, 15, tzinfo=KL)
    assert partition_path("mybas-kota-bharu", ts) == "agency=mybas-kota-bharu/date=2026-11-13/hour=23"
    assert hour_key(ts) == ("2026-11-13", "23")


def test_file_naming(tmp_path):
    spool = Spool(tmp_path, host="vm1", run_id="20261113T210000Z")
    path = spool.file_for("rapid-bus-kl", datetime(2026, 11, 13, 9, 5, tzinfo=UTC))
    assert path.relative_to(tmp_path).as_posix() == (
        "agency=rapid-bus-kl/date=2026-11-13/hour=09/part-vm1-20261113T210000Z.jsonl.gz"
    )


def test_appends_are_one_valid_gzip_stream(tmp_path):
    spool = Spool(tmp_path, host="vm1", run_id="r1")
    ts = datetime(2026, 11, 13, 9, 0, 10, tzinfo=UTC)
    spool.append("a", ts, [{"n": 1}, {"n": 2}])
    spool.append("a", ts + timedelta(seconds=30), [{"n": 3}])
    (f,) = spool.files()
    assert read_lines(f.path) == [{"n": 1}, {"n": 2}, {"n": 3}]


def test_empty_poll_writes_nothing(tmp_path):
    spool = Spool(tmp_path, host="vm1", run_id="r1")
    assert spool.append("a", datetime(2026, 11, 13, 9, tzinfo=UTC), []) is None
    assert spool.files() == []


def test_rotation_across_hour_boundary(tmp_path):
    spool = Spool(tmp_path, host="vm1", run_id="r1")
    before = datetime(2026, 11, 13, 9, 59, 50, tzinfo=UTC)
    after = datetime(2026, 11, 13, 10, 0, 20, tzinfo=UTC)
    spool.append("a", before, [{"n": 1}])
    spool.append("a", after, [{"n": 2}])
    spool.append("b", before, [{"n": 3}])

    files = {f.relative for f in spool.files()}
    assert files == {
        "agency=a/date=2026-11-13/hour=09/part-vm1-r1.jsonl.gz",
        "agency=a/date=2026-11-13/hour=10/part-vm1-r1.jsonl.gz",
        "agency=b/date=2026-11-13/hour=09/part-vm1-r1.jsonl.gz",
    }
    closed = {f.relative for f in spool.closed_files(after)}
    assert closed == {
        "agency=a/date=2026-11-13/hour=09/part-vm1-r1.jsonl.gz",
        "agency=b/date=2026-11-13/hour=09/part-vm1-r1.jsonl.gz",
    }


def test_rotation_across_midnight_utc(tmp_path):
    spool = Spool(tmp_path, host="vm1", run_id="r1")
    spool.append("a", datetime(2026, 12, 31, 23, 59, 59, tzinfo=UTC), [{"n": 1}])
    closed = spool.closed_files(datetime(2027, 1, 1, 0, 0, 1, tzinfo=UTC))
    assert [f.relative for f in closed] == ["agency=a/date=2026-12-31/hour=23/part-vm1-r1.jsonl.gz"]


def test_leftovers_from_previous_run_are_found(tmp_path):
    old = Spool(tmp_path, host="vm1", run_id="old")
    now = datetime(2026, 11, 13, 9, 10, tzinfo=UTC)
    old.append("a", now, [{"n": 1}])

    new = Spool(tmp_path, host="vm1", run_id="new")
    assert [f.relative for f in new.files()] == ["agency=a/date=2026-11-13/hour=09/part-vm1-old.jsonl.gz"]
    # A new run never appends to the old run's file.
    assert new.file_for("a", now) != old.file_for("a", now)


def test_remove_cleans_empty_dirs_but_keeps_root(tmp_path):
    root = tmp_path / "spool"
    spool = Spool(root, host="vm1", run_id="r1")
    spool.append("a", datetime(2026, 11, 13, 9, tzinfo=UTC), [{"n": 1}])
    (f,) = spool.files()
    spool.remove(f)
    assert root.exists()
    assert list(root.iterdir()) == []


def test_crash_mid_write_keeps_earlier_polls_readable(tmp_path):
    spool = Spool(tmp_path, host="vm1", run_id="r1")
    ts = datetime(2026, 11, 13, 9, tzinfo=UTC)
    path = spool.append("a", ts, [{"n": 1}])
    with open(path, "ab") as fh:  # simulate a torn write of a second member
        fh.write(gzip.compress(b'{"n": 2}\n')[:7])

    recovered = []
    with gzip.open(path, "rt") as fh:
        try:
            for line in fh:
                recovered.append(json.loads(line))
        except EOFError:
            pass
    assert recovered == [{"n": 1}]
