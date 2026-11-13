"""
Upload destinations for closed spool files.

A sink's upload() returns only once the object is durably stored, and raises
otherwise. The caller deletes the local spool file only after upload()
returns, so a failed or interrupted upload always leaves the data on disk to
retry.
"""

import logging
import os
import shutil
from pathlib import Path

from collector.spool import Spool, SpoolFile

log = logging.getLogger("collector.sinks")

RAW_PREFIX = "raw/vehicle_positions"


class LocalSink:
    """Writes the same partition layout into a local folder (for test runs)."""

    def __init__(self, out_dir: Path | str, prefix: str = RAW_PREFIX):
        self.out_dir = Path(out_dir)
        self.prefix = prefix

    def describe(self) -> str:
        return str(self.out_dir / self.prefix)

    def upload(self, spool_file: SpoolFile) -> None:
        dest = self.out_dir / self.prefix / spool_file.relative
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_name(dest.name + ".tmp")
        shutil.copyfile(spool_file.path, tmp)
        with open(tmp, "r+b") as fh:  # fsync needs a writable handle on Windows
            os.fsync(fh.fileno())
        os.replace(tmp, dest)  # atomic: dest is either absent or complete


class GcsSink:
    """Uploads to gs://<bucket>/<prefix>/<partition path>.

    Auth is Application Default Credentials -- on the VM that is the attached
    service account, so there is no key file anywhere.

    The service account only has roles/storage.objectCreator: it can create
    objects but not read, overwrite or delete them. So:
    - "confirmed" means the upload call returned success, with an MD5 checksum
      the server verified on receipt (checksum="md5");
    - if_generation_match=0 means "create only, never overwrite". A 412 means
      this exact file (unique host + run id + hour) was already uploaded and the
      process died before deleting it locally -- safe to treat as done.
    """

    def __init__(self, bucket: str, prefix: str = RAW_PREFIX, client=None):
        if client is None:
            from google.cloud import storage  # imported lazily: local runs don't need it

            client = storage.Client()
        self.bucket_name = bucket
        self.bucket = client.bucket(bucket)
        self.prefix = prefix

    def describe(self) -> str:
        return f"gs://{self.bucket_name}/{self.prefix}"

    def upload(self, spool_file: SpoolFile) -> None:
        from google.api_core.exceptions import PreconditionFailed

        blob = self.bucket.blob(f"{self.prefix}/{spool_file.relative}")
        try:
            blob.upload_from_filename(
                str(spool_file.path),
                content_type="application/gzip",
                if_generation_match=0,
                checksum="md5",
                timeout=120,
            )
        except PreconditionFailed:
            log.warning("%s already exists in GCS (earlier upload); treating as uploaded", blob.name)


def upload_files(spool: Spool, sink, files: list[SpoolFile]) -> tuple[int, int]:
    """Upload each file, deleting it locally only after a confirmed upload.
    Returns (uploaded, failed). Never raises: failures stay on disk for the
    next attempt."""
    uploaded = failed = 0
    for f in files:
        try:
            sink.upload(f)
        except Exception as exc:  # noqa: BLE001 -- any failure means "keep the file"
            failed += 1
            log.error("upload failed for %s: %s (kept on disk, will retry)", f.relative, exc)
            continue
        spool.remove(f)
        uploaded += 1
        log.info("uploaded %s to %s", f.relative, sink.describe())
    return uploaded, failed
