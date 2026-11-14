"""
Model export and promotion.

Layout under a models root (local folder, or gs://<bucket>/models):

    <root>/<dataset>/<version>/model.json      XGBoost native format
    <root>/<dataset>/<version>/metadata.json   dates, MAE, features, dataset label
    <root>/<dataset>/current.json              {"version": ...}  the model serving uses

Real and synthetic models live under different <dataset> folders, so a
synthetic model can never become the "current" real model.

Promotion rule: a new model replaces the current one only if its MAE is
lower than the CURRENT model's MAE re-measured on the SAME test window.
"""

import json
import tempfile
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Decision:
    promote: bool
    reason: str


def should_promote(new_mae: float, current_mae: float | None, min_improvement: float = 0.0,
                   comparable: bool = True) -> Decision:
    if current_mae is None:
        return Decision(True, "no current model")
    if not comparable:
        return Decision(False, "current model uses a different feature set; not comparable, kept current")
    if new_mae < current_mae - min_improvement:
        return Decision(True, f"MAE improved {current_mae:.4f} -> {new_mae:.4f} on the same test window")
    return Decision(False, f"MAE did not improve by more than {min_improvement} "
                           f"({new_mae:.4f} vs current {current_mae:.4f} on the same test window)")


class Store:
    """Tiny file store over a local folder or gs://bucket/prefix."""

    def __init__(self, root: str):
        self.root = root.rstrip("/")
        self.gcs = root.startswith("gs://")
        if self.gcs:
            from google.cloud import storage

            bucket, _, prefix = root[5:].partition("/")
            self._bucket = storage.Client().bucket(bucket)
            self._prefix = prefix.strip("/")

    def _key(self, rel: str) -> str:
        return f"{self._prefix}/{rel}" if self._prefix else rel

    def read_text(self, rel: str) -> str | None:
        if self.gcs:
            blob = self._bucket.blob(self._key(rel))
            return blob.download_as_text() if blob.exists() else None
        p = Path(self.root) / rel
        return p.read_text(encoding="utf-8") if p.exists() else None

    def write_text(self, rel: str, text: str) -> None:
        if self.gcs:
            self._bucket.blob(self._key(rel)).upload_from_string(text, content_type="application/json")
            return
        p = Path(self.root) / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text, encoding="utf-8")

    def write_file(self, rel: str, local_path: str) -> None:
        if self.gcs:
            self._bucket.blob(self._key(rel)).upload_from_filename(local_path)
            return
        p = Path(self.root) / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(Path(local_path).read_bytes())

    def fetch_file(self, rel: str) -> str | None:
        """Local path to the file (downloaded to a temp file for gs://)."""
        if not self.gcs:
            p = Path(self.root) / rel
            return str(p) if p.exists() else None
        blob = self._bucket.blob(self._key(rel))
        if not blob.exists():
            return None
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=Path(rel).suffix)
        blob.download_to_filename(tmp.name)
        return tmp.name


def current_version(store: Store, dataset: str) -> str | None:
    text = store.read_text(f"{dataset}/current.json")
    return json.loads(text)["version"] if text else None


def read_metadata(store: Store, dataset: str, version: str) -> dict | None:
    text = store.read_text(f"{dataset}/{version}/metadata.json")
    return json.loads(text) if text else None


def export_model(store: Store, dataset: str, version: str, model_path: str, metadata: dict) -> None:
    store.write_file(f"{dataset}/{version}/model.json", model_path)
    store.write_text(f"{dataset}/{version}/metadata.json", json.dumps(metadata, indent=2, default=str))


def set_current(store: Store, dataset: str, version: str, reason: str) -> None:
    store.write_text(f"{dataset}/current.json", json.dumps({"version": version, "reason": reason}, indent=2))
