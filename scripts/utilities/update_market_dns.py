#!/usr/bin/env python3
"""Keep market.poke6s.com pointing at the home connection's public IP (dynamic DNS).

market.poke6s.com is a DNS-only A record in Cloudflare that points straight at the home IP
(the router forwards to the Caddy proxy). The ISP can hand out a new IP after a power cut
(it did on 2026-10-16), and the site then goes dark until the record is fixed. poke6s.com
itself goes through the Cloudflare Tunnel and doesn't care about the IP.

Every run (cron, every 5 minutes, through scripts/monitoring/run_job.py as `market-dns`):

1. asks Cloudflare (https://1.1.1.1/cdn-cgi/trace, falling back to api.ipify.org) for the
   public IPv4 address this server is seen from;
2. reads each record in CLOUDFLARE_DDNS_RECORDS from the Cloudflare API;
3. if a record points elsewhere, changes only its IP (proxied, TTL and comment stay as they
   are) and posts the change to Discord (POKE6S_DISCORD_WEBHOOK_URL).

Settings, from the environment or the repo's .env (never printed):
  CLOUDFLARE_DNS_API_TOKEN  required. A Cloudflare API token from the "Edit zone DNS"
                            template, limited to the poke6s.com zone.
  CLOUDFLARE_DDNS_ZONE      the zone name (default poke6s.com).
  CLOUDFLARE_DDNS_RECORDS   comma-separated A records to keep updated (default
                            market.poke6s.com). Only names listed here are ever touched.

Exit codes: 0 = record correct or updated; 1 = something failed (no IP, API error, missing
token), so the Grafana "Did the jobs run?" panel shows it. Standard library only; runs on
the host's Python. `--dry-run` shows what would change without changing it.
"""

from __future__ import annotations

import argparse
import ipaddress
import json
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts" / "pipeline"))
from notify_discord import post, setting  # noqa: E402  (stdlib helpers shared with the nightly report)

API = "https://api.cloudflare.com/client/v4"
IP_SOURCES = ("https://1.1.1.1/cdn-cgi/trace", "https://api.ipify.org")
USER_AGENT = "Poke6sMarketDNS/1.0"
BLUE = 0x3498DB


class DnsError(Exception):
    """A Cloudflare API call or IP lookup failed; the message is safe to print."""


def parse_ip(text: str) -> str | None:
    """The IPv4 address in a trace answer (`ip=1.2.3.4` line) or a bare-address answer."""
    for line in text.splitlines():
        line = line.strip()
        candidate = line[3:] if line.startswith("ip=") else line
        try:
            address = ipaddress.ip_address(candidate)
        except ValueError:
            continue
        # The A record needs IPv4; a private address would mean the answer is wrong.
        if address.version == 4 and address.is_global:
            return str(address)
    return None


def public_ip() -> str:
    """This connection's public IPv4 address, from the first source that answers sensibly."""
    for url in IP_SOURCES:
        try:
            request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(request, timeout=15) as resp:
                ip = parse_ip(resp.read().decode("utf-8", "replace"))
        except (urllib.error.URLError, OSError):
            continue
        if ip:
            return ip
    raise DnsError("Could not find the public IP (both lookups failed); is the internet down?")


def cloudflare(token: str, method: str, path: str, body: dict | None = None) -> dict:
    """One Cloudflare API call; returns its `result`. Errors never include the token."""
    request = urllib.request.Request(
        API + path,
        data=json.dumps(body).encode("utf-8") if body is not None else None,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "User-Agent": USER_AGENT,
        },
        method=method,
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as resp:
            answer = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        try:
            errors = json.loads(exc.read().decode("utf-8")).get("errors", [])
        except (ValueError, OSError):
            errors = []
        detail = "; ".join(str(e.get("message", e)) for e in errors) or exc.reason
        raise DnsError(f"Cloudflare API {method} {path.split('?')[0]}: HTTP {exc.code} ({detail})") from None
    except (urllib.error.URLError, OSError) as exc:
        raise DnsError(f"Cloudflare API unreachable: {exc}") from None
    if not answer.get("success"):
        raise DnsError(f"Cloudflare API {method} {path.split('?')[0]} refused: {answer.get('errors')}")
    return answer["result"]


def zone_id(token: str, zone: str) -> str:
    """The Cloudflare zone ID for a zone name."""
    result = cloudflare(token, "GET", "/zones?" + urllib.parse.urlencode({"name": zone}))
    if not result:
        raise DnsError(f"Zone {zone} not found (is the token limited to a different zone?)")
    return result[0]["id"]


def plan_changes(records: list[dict], ip: str) -> list[dict]:
    """The records whose IP differs from `ip`: each the record, with `old` added."""
    return [dict(r, old=r["content"]) for r in records if r["content"] != ip]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dry-run", action="store_true", help="Show what would change; change nothing.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    token = setting("CLOUDFLARE_DNS_API_TOKEN")
    if not token:
        print("CLOUDFLARE_DNS_API_TOKEN is not set in .env; nothing checked.")
        return 1
    zone = setting("CLOUDFLARE_DDNS_ZONE", "poke6s.com")
    names = [n.strip() for n in setting("CLOUDFLARE_DDNS_RECORDS", "market.poke6s.com").split(",") if n.strip()]

    try:
        ip = public_ip()
        zid = zone_id(token, zone)
        records = []
        for name in names:
            found = cloudflare(token, "GET", f"/zones/{zid}/dns_records?" + urllib.parse.urlencode({"type": "A", "name": name}))
            if not found:
                raise DnsError(f"No A record named {name} in {zone}; create it in the dashboard first.")
            records.extend(found)
        changes = plan_changes(records, ip)
        if not changes:
            print(f"OK: {', '.join(names)} already point at the current IP.")
            return 0
        for record in changes:
            if args.dry_run:
                print(f"DRY RUN: would change {record['name']} from {record['old']} to {ip}")
                continue
            # PATCH sends only the new IP, so proxied, TTL and comment stay as they were.
            cloudflare(token, "PATCH", f"/zones/{zid}/dns_records/{record['id']}", {"content": ip})
            print(f"UPDATED: {record['name']} from {record['old']} to {ip}")
    except DnsError as exc:
        print(f"ERROR: {exc}")
        return 1

    if not args.dry_run:
        notify(changes, ip)
    return 0


def notify(changes: list[dict], ip: str) -> None:
    """Tell Discord the home IP changed and the records followed. Never fails the job."""
    url = setting("POKE6S_DISCORD_WEBHOOK_URL")
    if not url:
        return
    lines = [f"{c['name']}: {c['old']} → {ip}" for c in changes]
    payload = {
        "embeds": [{
            "title": "Home IP changed: market DNS updated",
            "description": "\n".join(lines) + "\nNothing to do; this is a heads-up.",
            "color": BLUE,
        }]
    }
    try:
        post(url, payload)
    except (urllib.error.URLError, OSError) as exc:
        print(f"Discord notification failed: {exc}")


if __name__ == "__main__":
    sys.exit(main())
