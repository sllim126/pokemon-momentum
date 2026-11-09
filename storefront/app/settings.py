"""Storefront settings, read from environment variables.

docker-compose passes only the variables named in its `storefront` service, with safe local
defaults, so the shop never sees the market app's other secrets. Secrets (Stripe, SMTP, the
Discord webhook) live in `.env` and are never logged, printed or shown on a page.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
STOREFRONT_DIR = REPO_ROOT / "storefront"


def _int(name: str, default: int) -> int:
    value = (os.getenv(name) or "").strip()
    return int(value) if value.isdigit() else default


def _bool(value: str | None, default: bool = False) -> bool:
    if value is None or value.strip() == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


# Placeholder flat shipping rates by class, in cents, until the owner sets them from Pirate
# Ship costs (plan decision D3). Override each with STOREFRONT_SHIP_<CLASS>_CENTS.
DEFAULT_SHIP_RATES = {"single": 500, "accessory": 500, "small-sealed": 500, "large-sealed": 900}


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

    # --- checkout (Phase 2) --------------------------------------------------------------
    # Stripe keys. Checkout is switched on only when a secret key is set, and refuses live
    # keys (sk_live_...) unless STOREFRONT_ALLOW_LIVE=1: going live is the owner's call.
    stripe_secret_key: str = ""
    stripe_webhook_secret: str = ""
    allow_live: bool = False
    ship_rates_cents: dict = field(default_factory=lambda: dict(DEFAULT_SHIP_RATES))
    free_shipping_over_cents: int = 15000      # owner, 2026-11-05: free over $150
    # Ask the card's bank for 3-D Secure at or above this order total (plan "Checkout").
    three_d_secure_over_cents: int = 15000
    # Sales tax (decision D2, owner 2026-11-05): added on top at checkout by Stripe Tax.
    #   'stripe' - Stripe Tax works out the tax from the shipping address, only where the
    #              Stripe account is registered (Utah). Set up once in the Stripe dashboard:
    #              business address, Tax -> Registrations -> Utah. Costs ~0.5% per taxed order.
    #   'none'   - no tax (tests and local runs).
    # (A free fixed Utah rate was tried: Stripe no longer accepts per-state "dynamic" tax
    # rates in Checkout, so it can't skip out-of-state orders.)
    tax_mode: str = "none"
    reservation_minutes: int = 30
    # Order alerts. Empty = no alerts (the URL is a secret: never log it).
    discord_webhook_url: str = ""
    # Order emails through Microsoft 365. Without a host, emails are written to
    # data/storefront/outbox/ instead of sent, which is what tests and local runs use.
    smtp_host: str = ""
    smtp_port: int = 587
    smtp_user: str = ""
    smtp_password: str = ""
    mail_from: str = "Poke6s <admin@poke6s.com>"

    @property
    def stripe_mode(self) -> str:
        return "live" if "_live_" in self.stripe_secret_key else "test"

    @property
    def tax_problem(self) -> str:
        """Why the tax setting can't be used, or '' when it's fine. Checkout stays off while
        there is a problem, so a mistyped mode can't quietly sell without tax."""
        if self.tax_mode not in ("none", "stripe"):
            return f"STOREFRONT_TAX_MODE={self.tax_mode!r} is not 'none' or 'stripe'."
        return ""

    @property
    def checkout_enabled(self) -> bool:
        if not self.stripe_secret_key or self.tax_problem:
            return False
        return self.stripe_mode == "test" or self.allow_live

    @property
    def outbox_dir(self) -> Path:
        return self.data_dir / "outbox"

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
        stripe_secret_key=os.getenv("STRIPE_SECRET_KEY", "").strip(),
        stripe_webhook_secret=os.getenv("STRIPE_WEBHOOK_SECRET", "").strip(),
        allow_live=_bool(os.getenv("STOREFRONT_ALLOW_LIVE"), False),
        ship_rates_cents={name: _int(f"STOREFRONT_SHIP_{name.upper().replace('-', '_')}_CENTS", cents)
                          for name, cents in DEFAULT_SHIP_RATES.items()},
        free_shipping_over_cents=_int("STOREFRONT_FREE_SHIPPING_OVER_CENTS", 15000),
        three_d_secure_over_cents=_int("STOREFRONT_3DS_OVER_CENTS", 15000),
        tax_mode=os.getenv("STOREFRONT_TAX_MODE", "none").strip().lower() or "none",
        discord_webhook_url=os.getenv("STOREFRONT_DISCORD_WEBHOOK_URL", "").strip(),
        smtp_host=os.getenv("STOREFRONT_SMTP_HOST", "").strip(),
        smtp_port=_int("STOREFRONT_SMTP_PORT", 587),
        smtp_user=os.getenv("STOREFRONT_SMTP_USER", "").strip(),
        smtp_password=os.getenv("STOREFRONT_SMTP_PASSWORD", ""),
        mail_from=os.getenv("STOREFRONT_MAIL_FROM", "Poke6s <admin@poke6s.com>"),
    )
