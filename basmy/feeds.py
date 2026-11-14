"""Load the list of GTFS-Realtime feeds from config/feeds.toml."""

import re
import tomllib
from dataclasses import dataclass
from pathlib import Path

from basmy.gtfs_rt import FEED_BASE_URL

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent.parent / "config" / "feeds.toml"

# The agency name becomes a GCS path segment (agency=<name>), so keep it to a
# safe, stable charset.
_AGENCY_RE = re.compile(r"^[a-z0-9][a-z0-9-]*$")


@dataclass(frozen=True)
class Feed:
    agency: str
    url: str


def load_feeds(path: Path | str = DEFAULT_CONFIG_PATH) -> list[Feed]:
    with open(path, "rb") as fh:
        config = tomllib.load(fh)

    base_url = config.get("base_url", FEED_BASE_URL)
    feeds = []
    seen = set()
    for entry in config.get("feeds", []):
        agency = entry["agency"]
        if not _AGENCY_RE.match(agency):
            raise ValueError(f"invalid agency name {agency!r}: use lowercase letters, digits and '-'")
        if agency in seen:
            raise ValueError(f"duplicate agency {agency!r} in {path}")
        seen.add(agency)
        feeds.append(Feed(agency=agency, url=base_url + entry["path"]))

    if not feeds:
        raise ValueError(f"no feeds configured in {path}")
    return feeds
