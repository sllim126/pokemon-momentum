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


# Flat shipping rates by class, in cents (plan decision D3; owner, 2026-11-11, from Pirate Ship
# costs: about $7.50 for a small box and $15-25 for a large one, plus tracking and signature
# confirmation on every order). Override each with STOREFRONT_SHIP_<CLASS>_CENTS.
DEFAULT_SHIP_RATES = {"single": 1000, "accessory": 1000, "small-sealed": 1000, "large-sealed": 2000}


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
    # TCGplayer affiliate link (the same one market.poke6s.com uses). A product's TCGplayer page
    # is wrapped in it: <base>?u=<product page>. Empty = plain product links.
    tcgplayer_affiliate_base: str = "https://partner.tcgplayer.com/jR1OJb"

    # --- checkout (Phase 2) --------------------------------------------------------------
    # Stripe keys. Checkout is switched on only when a secret key is set, and refuses live
    # keys (sk_live_...) unless STOREFRONT_ALLOW_LIVE=1: going live is the owner's call.
    stripe_secret_key: str = ""
    stripe_webhook_secret: str = ""
    allow_live: bool = False
    # Master switch (STOREFRONT_CHECKOUT). Off = catalog only: no cart, product pages send
    # buyers to the TCGplayer store, even with Stripe keys in .env. The public site launched
    # catalog-only (owner, 2026-11-11); turn it on when Stripe is activated for live payments.
    checkout_switch: bool = True
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
    # Order emails through Microsoft 365: Microsoft Graph, signed in as the app registration
    # "poke6s-store-mail" (security review item 12; Microsoft is retiring password SMTP), which
    # Exchange lets send only as orders@poke6s.com (a free shared mailbox). The secret is never logged. SMTP below is the
    # older fallback. With neither, emails are written to data/storefront/outbox/ instead of
    # sent, which is what tests and local runs use.
    graph_tenant_id: str = ""
    graph_client_id: str = ""
    graph_client_secret: str = ""
    smtp_host: str = ""
    smtp_port: int = 587
    smtp_user: str = ""
    smtp_password: str = ""
    mail_from: str = "Poke6s <orders@poke6s.com>"

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
    def email_transport(self) -> str:
        """How order emails leave: 'graph', 'smtp' or 'outbox' (saved, not sent)."""
        if self.graph_tenant_id and self.graph_client_id and self.graph_client_secret:
            return "graph"
        return "smtp" if self.smtp_host else "outbox"

    @property
    def setup_problem(self) -> str:
        """Why checkout can't open with these settings ('' when it can). The webhook secret is
        required (security review 2026-11-11, item 11): refunds and disputes reach the shop only
        through Stripe's events, and the webhook is how they arrive promptly."""
        if self.tax_problem:
            return self.tax_problem
        if not self.stripe_webhook_secret:
            return "STRIPE_WEBHOOK_SECRET is not set (Stripe dashboard > Developers > Webhooks)."
        if self.stripe_mode == "live" and self.email_transport == "outbox":
            # Real customers must get their confirmation and tracking emails (item 12).
            return "No email sending is set up (STOREFRONT_GRAPH_*): customers would get no order emails."
        return ""

    @property
    def checkout_enabled(self) -> bool:
        if not self.checkout_switch or not self.stripe_secret_key or self.setup_problem:
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
        tcgplayer_affiliate_base=os.getenv("STOREFRONT_TCGPLAYER_AFFILIATE",
                                           "https://partner.tcgplayer.com/jR1OJb").strip(),
        stripe_secret_key=os.getenv("STRIPE_SECRET_KEY", "").strip(),
        stripe_webhook_secret=os.getenv("STRIPE_WEBHOOK_SECRET", "").strip(),
        allow_live=_bool(os.getenv("STOREFRONT_ALLOW_LIVE"), False),
        checkout_switch=_bool(os.getenv("STOREFRONT_CHECKOUT"), True),
        ship_rates_cents={name: _int(f"STOREFRONT_SHIP_{name.upper().replace('-', '_')}_CENTS", cents)
                          for name, cents in DEFAULT_SHIP_RATES.items()},
        free_shipping_over_cents=_int("STOREFRONT_FREE_SHIPPING_OVER_CENTS", 15000),
        three_d_secure_over_cents=_int("STOREFRONT_3DS_OVER_CENTS", 15000),
        tax_mode=os.getenv("STOREFRONT_TAX_MODE", "none").strip().lower() or "none",
        discord_webhook_url=os.getenv("STOREFRONT_DISCORD_WEBHOOK_URL", "").strip(),
        graph_tenant_id=os.getenv("STOREFRONT_GRAPH_TENANT_ID", "").strip(),
        graph_client_id=os.getenv("STOREFRONT_GRAPH_CLIENT_ID", "").strip(),
        graph_client_secret=os.getenv("STOREFRONT_GRAPH_CLIENT_SECRET", "").strip(),
        smtp_host=os.getenv("STOREFRONT_SMTP_HOST", "").strip(),
        smtp_port=_int("STOREFRONT_SMTP_PORT", 587),
        smtp_user=os.getenv("STOREFRONT_SMTP_USER", "").strip(),
        smtp_password=os.getenv("STOREFRONT_SMTP_PASSWORD", ""),
        mail_from=os.getenv("STOREFRONT_MAIL_FROM", "Poke6s <orders@poke6s.com>"),
    )
