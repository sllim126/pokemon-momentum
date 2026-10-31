"""Storefront settings, read from environment variables.

docker-compose passes only the variables named in its `storefront` service, with safe local
defaults, so the shop never sees the market app's other secrets. Secrets (Stripe, SMTP) arrive
in Phase 2 and stay in `.env`; never log them.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
STOREFRONT_DIR = REPO_ROOT / "storefront"


def _bool(value: str | None, default: bool = False) -> bool:
    if value is None or value.strip() == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    # Where the database, images and market reference live. Never committed (AGENTS.md).
    data_dir: Path
    # Public address used for canonical links, the sitemap and social cards. Stays a local
    # address until launch; the owner switches it to https://poke6s.com then.
    base_url: str
    # Search engines are told to stay away until launch (robots.txt Disallow + noindex), so
    # a LAN or test copy can never be indexed by accident.
    allow_indexing: bool
    market_base_url: str
    tcgplayer_store_url: str
    contact_email: str

    @property
    def db_path(self) -> Path:
        return self.data_dir / "store.db"

    @property
    def images_dir(self) -> Path:
        return self.data_dir / "images"

    @property
    def market_reference_path(self) -> Path:
        return self.data_dir / "market_reference.json"


def load_settings() -> Settings:
    return Settings(
        data_dir=Path(os.getenv("STOREFRONT_DATA_DIR", str(REPO_ROOT / "data" / "storefront"))),
        base_url=os.getenv("STOREFRONT_BASE_URL", "http://localhost:8010").rstrip("/"),
        allow_indexing=_bool(os.getenv("STOREFRONT_ALLOW_INDEXING"), False),
        market_base_url=os.getenv("STOREFRONT_MARKET_URL", "https://market.poke6s.com").rstrip("/"),
        tcgplayer_store_url=os.getenv("STOREFRONT_TCGPLAYER_URL", "https://poke6s.tcgplayerpro.com/"),
        contact_email=os.getenv("STOREFRONT_CONTACT_EMAIL", "admin@poke6s.com"),
    )
