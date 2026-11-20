"""Set logos for the Binder Art Maker (`/binder-art`, templates/binder_art.html).

The tool itself runs entirely in the visitor's browser (static/binder-art.js); the server only
lists the set logos it can put on a card. Moved here from market.poke6s.com on 2026-11-20
(docs/market_inventory.md, option B); the market's /binder-art now redirects to this page.

Where the logos come from, without touching the market's database:

- the PNGs: `images/logos/<groupId>.png` in the repo, mounted read-only into the store
  container (docker-compose.yml). `scripts/utilities/sync_set_logos.py` keeps them and copies
  one logo to several groupIds (a set and its subset), so identical files are merged here into
  one entry named after the earliest set that uses it;
- set names and release dates: `data/extracted/pokemon_groups.csv`, which the market's nightly
  job writes and the store can already read (its data folder is mounted read-only).

A logo whose groupId isn't in the CSV is left out. Missing folder or CSV = an empty list, and
the page says the logos couldn't load.
"""

from __future__ import annotations

import csv
import hashlib
import threading
from pathlib import Path

# Logos shared by a family of small sets get one entry under the family name (keyed by any
# member groupId), listed after the dated sets. Same table as the market had.
LOGO_FAMILIES = {1422: "POP Series", 1418: "Black Star Promos", 1401: "McDonald's Promos"}


class SetLogos:
    """The dropdown list, rebuilt only when the logo folder or the groups CSV changes."""

    def __init__(self, logo_dir: Path, groups_csv: Path) -> None:
        self.logo_dir = logo_dir
        self.groups_csv = groups_csv
        self._lock = threading.Lock()
        self._key: tuple | None = None
        self._logos: list[dict] = []

    def _stamp(self, path: Path) -> float | None:
        try:
            return path.stat().st_mtime
        except OSError:
            return None

    def path(self, group_id: str) -> Path | None:
        """The PNG for one groupId, or None. Digits only, so no path tricks get through."""
        if not group_id.isdigit():
            return None
        file = self.logo_dir / f"{group_id}.png"
        return file if file.is_file() else None

    def logos(self) -> list[dict]:
        """[{"name", "year", "url"}], newest set first, then the families by name."""
        key = (self._stamp(self.logo_dir), self._stamp(self.groups_csv))
        with self._lock:
            if key != self._key:
                self._logos = self._build() if None not in key else []
                self._key = key
            return self._logos

    def _groups(self) -> dict[int, tuple[str, str]]:
        """groupId -> (set name, publishedOn as text)."""
        info: dict[int, tuple[str, str]] = {}
        try:
            with self.groups_csv.open(newline="", encoding="utf-8") as handle:
                for row in csv.DictReader(handle):
                    gid = (row.get("groupId") or "").strip()
                    if gid.isdigit():
                        info[int(gid)] = ((row.get("name") or "").strip(), (row.get("publishedOn") or "").strip())
        except (OSError, csv.Error, UnicodeDecodeError):
            return {}
        return info

    def _build(self) -> list[dict]:
        by_hash: dict[str, list[int]] = {}
        for file in self.logo_dir.glob("*.png"):
            if file.stem.isdigit():
                by_hash.setdefault(hashlib.md5(file.read_bytes()).hexdigest(), []).append(int(file.stem))
        info = self._groups()
        dated, families = [], []
        for ids in by_hash.values():
            known = sorted((gid for gid in ids if gid in info), key=lambda gid: (info[gid][1], gid))
            if not known:
                continue
            family = next((LOGO_FAMILIES[gid] for gid in ids if gid in LOGO_FAMILIES), None)
            first = known[0]
            name, published = info[first]
            entry = {"name": family or name, "year": "" if family else published[:4],
                     "url": f"/binder-art/logos/{first}.png", "_sort": published}
            (families if family else dated).append(entry)
        dated.sort(key=lambda e: e["_sort"], reverse=True)
        families.sort(key=lambda e: e["name"])
        return [{k: v for k, v in e.items() if k != "_sort"} for e in dated + families]
