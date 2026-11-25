#!/usr/bin/env python3
"""Build, check, seed and announce last month's TCGCSV archive torrent (monthly cron job).

Owner's plan (2026-11-02 and 11-17): from November 2026 on, a torrent of each month's TCGCSV
archive files, built on the 2nd of the next month and posted in the TCGCSV Discord's
#useful-shares. This script does everything except that post, which the owner makes by hand
(it's someone else's server). See docs/operator_runbook.txt "TCGCSV archive torrents".

Steps, each safe to repeat (a rerun skips what's already done):

1. Wait for the month to be complete. TCGCSV publishes a day's archive file the next evening,
   so the last day of a month lands here around 21:30 UTC on the 1st (the archive backfill).
   If a day is still missing, exit 3 ("waiting": grey in Discord, no ping) and try again the
   next day; on the 5th it becomes a failure, because by then the day isn't coming.
2. Build the torrent with build_archive_torrent.py in a throwaway python:3.12-slim container
   (libtorrent isn't in the site's container or on the host). It runs as root, because the
   site's container wrote some data/raw files as root and only root may hard-link them; the
   new files are handed back to uid 1000 afterwards.
3. Copy the torrent's folder to the PlexVault share (/mnt/media/TCGCSV, which is Z:\\TCGCSV on
   the qBittorrent PC) and check every file on the copy against SHA256SUMS.txt.
4. Add the .torrent to qBittorrent (192.168.1.207:8080; the web UI lets this LAN in without a
   password) with save path Z:\\TCGCSV, category TCGCSV, tag "public", like the earlier ones.
   qBittorrent then checks every piece against the files: 100% means the copy is exactly what
   the torrent describes. If the torrent never shows up, qBittorrent is stuck (seen
   2026-11-17): the error says to restart it on the PC.
5. Email the owner the .torrent, its magnet link and a ready-to-paste #useful-shares message,
   through the store's Microsoft 365 sender (Graph; STOREFRONT_GRAPH_* in .env). No new cost.
6. Write <name>.published, so later runs this month stop at step 0.

Exit codes: 0 done (or already done), 3 waiting for missing days, 1 failed. The cron line runs
it through scripts/monitoring/run_job.py (Grafana) and run_monthly_torrent_host.sh posts the
result to Discord. Standard library only: it runs on the host's Python.

    python3 scripts/torrents/publish_monthly_torrent.py                  # last month
    python3 scripts/torrents/publish_monthly_torrent.py --month 2026-11  # a given month
    python3 scripts/torrents/publish_monthly_torrent.py --no-email       # everything but the email
"""

from __future__ import annotations

import argparse
import base64
import calendar
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import UTC, date, datetime
from email.message import EmailMessage
from email.utils import make_msgid, parseaddr
from email import policy
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = ROOT / "data" / "raw"
TORRENTS_DIR = ROOT / "data" / "torrents"
SHARE_DIR = Path(os.environ.get("TORRENT_SHARE_DIR", "/mnt/media/TCGCSV"))
QBIT_URL = os.environ.get("QBITTORRENT_URL", "http://192.168.1.207:8080")
QBIT_SAVE_PATH = "Z:\\TCGCSV"   # the same share, as the qBittorrent PC sees it
QBIT_CATEGORY = "TCGCSV"
QBIT_TAGS = "public"
BUILD_IMAGE = "python:3.12-slim"
LIBTORRENT = "libtorrent==2.1.3"  # the version the 2026-11-17 torrent was built with
LAST_TRY_DAY = 5                  # missing days after the 5th are a failure, not a wait
APPEAR_SECONDS = 120              # qBittorrent lists a new torrent within seconds when healthy
CHECK_SECONDS = 30 * 60           # checking ~120 MB over the LAN takes a minute or two
DEFAULT_TO = "admin@poke6s.com"
DEFAULT_FROM = "Poke6s <orders@poke6s.com>"
DISCORD_THREAD = "TCGCSV Discord, #useful-shares, the TCGCSV archive torrents thread"
EXIT_WAITING = 3


class Failed(Exception):
    """A step failed; the message says what to do and holds no secrets."""


def log(message: str) -> None:
    print(f"[{datetime.now(UTC):%Y-%m-%dT%H:%M:%SZ}] {message}", flush=True)


def env_file() -> dict[str, str]:
    """The repo's .env as a dict (values are secrets: never print them)."""
    values: dict[str, str] = {}
    path = ROOT / ".env"
    if path.exists():
        for line in path.read_text().splitlines():
            if "=" in line and not line.lstrip().startswith("#"):
                key, value = line.split("=", 1)
                values[key.strip()] = value.strip().strip('"').strip("'")
    return {**values, **{k: v for k, v in os.environ.items() if k.startswith(("STOREFRONT_", "TORRENT_"))}}


def previous_month(today: date) -> str:
    year, month = (today.year, today.month - 1) if today.month > 1 else (today.year - 1, 12)
    return f"{year:04d}-{month:02d}"


def missing_days(month: str) -> list[date]:
    year, number = (int(part) for part in month.split("-"))
    days = [date(year, number, day) for day in range(1, calendar.monthrange(year, number)[1] + 1)]
    return [day for day in days if not (RAW_DIR / f"prices-{day.isoformat()}.ppmd.7z").exists()]


def build(month: str, name: str) -> None:
    """Step 2: run the builder in a throwaway container, then give the files back to uid 1000."""
    if (TORRENTS_DIR / f"{name}.torrent").exists():
        log(f"Torrent already built: {name}.torrent")
        return
    script = (f"pip install -q --root-user-action=ignore --disable-pip-version-check {LIBTORRENT} && "
              f"python /app/scripts/torrents/build_archive_torrent.py --month {month} && "
              # Only the torrent's own files: the archive files are the same files as data/raw.
              f"chown 1000:1000 /app/data/torrents/{name} /app/data/torrents/{name}.torrent "
              f"/app/data/torrents/{name}.magnet.txt /app/data/torrents/{name}/README.txt "
              f"/app/data/torrents/{name}/SHA256SUMS.txt /app/data/torrents/{name}/tcgcsv-products-*.tar.xz")
    log(f"Building {name} in {BUILD_IMAGE}")
    result = subprocess.run(["docker", "run", "--rm", "-v", f"{ROOT}:/app", BUILD_IMAGE, "sh", "-c", script],
                            capture_output=True, text=True)
    for line in (result.stdout + result.stderr).splitlines()[-15:]:
        print("  " + line, flush=True)
    if result.returncode != 0 or not (TORRENTS_DIR / f"{name}.torrent").exists():
        raise Failed(f"Build failed (exit {result.returncode}); see the lines above")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def copy_to_share(name: str, expected_days: int) -> int:
    """Step 3: copy the folder, .torrent and magnet file to the share; check the copy. Returns bytes."""
    if not SHARE_DIR.is_dir():
        raise Failed(f"The share {SHARE_DIR} isn't mounted (PlexVault, 192.168.1.155)")
    source = TORRENTS_DIR / name
    target = SHARE_DIR / name
    target.mkdir(exist_ok=True)
    for path in sorted(source.iterdir()):
        copy = target / path.name
        if not copy.exists() or copy.stat().st_size != path.stat().st_size:
            shutil.copyfile(path, copy)
    for extra in (f"{name}.torrent", f"{name}.magnet.txt"):
        shutil.copyfile(TORRENTS_DIR / extra, SHARE_DIR / extra)
    sums = [line.split("  ", 1) for line in (target / "SHA256SUMS.txt").read_text().splitlines() if line]
    days = sum(1 for _, file in sums if file.startswith("prices-"))
    if days != expected_days:
        raise Failed(f"SHA256SUMS lists {days} days, expected {expected_days}")
    bad = [file for digest, file in sums if not (target / file).exists() or sha256(target / file) != digest]
    if bad:
        raise Failed(f"{len(bad)} file(s) on the share don't match SHA256SUMS, first {bad[0]}")
    total = sum(path.stat().st_size for path in target.iterdir())
    log(f"Copied to {target} and checked {len(sums)} files against SHA256SUMS ({total / 1e6:,.0f} MB)")
    return total


def qbit(path: str, data: bytes | None = None, content_type: str = "") -> bytes:
    request = urllib.request.Request(QBIT_URL + path, data=data, method="POST" if data is not None else "GET")
    if content_type:
        request.add_header("Content-Type", content_type)
    request.add_header("Referer", QBIT_URL)   # qBittorrent's CSRF check wants its own origin
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.read()
    except (urllib.error.URLError, OSError) as exc:
        raise Failed(f"qBittorrent at {QBIT_URL} didn't answer ({type(exc).__name__}): is the PC on?") from None


def qbit_torrent(info_hash: str) -> dict | None:
    found = json.loads(qbit(f"/api/v2/torrents/info?hashes={info_hash}") or b"[]")
    return found[0] if found else None


def seed(name: str, info_hash: str) -> None:
    """Step 4: add the torrent to qBittorrent (unless it's there) and wait for its check to reach 100%."""
    if qbit_torrent(info_hash) is None:
        boundary = uuid.uuid4().hex
        fields = {"savepath": QBIT_SAVE_PATH, "category": QBIT_CATEGORY, "tags": QBIT_TAGS}
        parts = [f'--{boundary}\r\nContent-Disposition: form-data; name="{key}"\r\n\r\n{value}\r\n'.encode()
                 for key, value in fields.items()]
        parts.append(f'--{boundary}\r\nContent-Disposition: form-data; name="torrents"; filename="{name}.torrent"\r\n'
                     f"Content-Type: application/x-bittorrent\r\n\r\n".encode()
                     + (TORRENTS_DIR / f"{name}.torrent").read_bytes() + b"\r\n")
        parts.append(f"--{boundary}--\r\n".encode())
        answer = qbit("/api/v2/torrents/add", b"".join(parts), f"multipart/form-data; boundary={boundary}")
        log(f"qBittorrent: {answer.decode(errors='replace').strip() or 'added'}")
        deadline = time.monotonic() + APPEAR_SECONDS
        while qbit_torrent(info_hash) is None:
            if time.monotonic() > deadline:
                raise Failed("qBittorrent accepted the torrent but never listed it: it's stuck. "
                             "Restart qBittorrent on the PC; the next run adds it again")
            time.sleep(5)
    deadline = time.monotonic() + CHECK_SECONDS
    while True:
        torrent = qbit_torrent(info_hash) or {}
        state, progress = torrent.get("state", "?"), float(torrent.get("progress", 0))
        if state in ("error", "missingFiles"):
            raise Failed(f"qBittorrent reports '{state}' for {name}: it can't read Z:\\TCGCSV\\{name}")
        if progress >= 1.0 and not state.startswith("checking"):
            log(f"qBittorrent checked every piece: 100%, {state}")
            return
        if time.monotonic() > deadline:
            raise Failed(f"qBittorrent still at {progress:.0%} ({state}) after {CHECK_SECONDS // 60} minutes")
        time.sleep(15)


def discord_post(name: str, label: str, days: int, size: int, magnet: str) -> str:
    """The message the owner pastes into the #useful-shares thread (plus the .torrent file)."""
    return (f"New monthly torrent: **TCGCSV archive, {label}** ({days} days, {size / 1e6:,.0f} MB), "
            f"with the product list (sets and products, every game). "
            f"Same layout as the others: one prices-YYYY-MM-DD.ppmd.7z per day, a README and SHA256SUMS.\n"
            f"```\n{magnet}\n```\n"
            f"The .torrent is attached. Thanks again to CptSpaceToaster for the archive.")


def graph_send(settings: dict[str, str], message: EmailMessage) -> None:
    """Send one MIME message through Microsoft Graph as the store's sender (app-only sign-in)."""
    tenant, client, secret = (settings.get(f"STOREFRONT_GRAPH_{key}", "") for key in ("TENANT_ID", "CLIENT_ID", "CLIENT_SECRET"))
    if not (tenant and client and secret):
        raise Failed("No STOREFRONT_GRAPH_* settings in .env: can't email the torrent")
    form = urllib.parse.urlencode({"grant_type": "client_credentials", "client_id": client, "client_secret": secret,
                                   "scope": "https://graph.microsoft.com/.default"}).encode()
    try:
        with urllib.request.urlopen(f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token", form, timeout=20) as response:
            token = json.load(response)["access_token"]
        sender = parseaddr(str(message["From"]))[1]
        # Graph takes the whole MIME message base64-encoded, with the email standard's CRLF
        # line endings (storefront/app/notify.py explains what bare LF breaks).
        request = urllib.request.Request(
            f"https://graph.microsoft.com/v1.0/users/{urllib.parse.quote(sender)}/sendMail",
            data=base64.b64encode(message.as_bytes(policy=policy.SMTP)), method="POST",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "text/plain"})
        with urllib.request.urlopen(request, timeout=60) as response:
            if response.status != 202:
                raise Failed(f"Graph sendMail answered HTTP {response.status}")
    except urllib.error.HTTPError as exc:
        raise Failed(f"Microsoft refused the email (HTTP {exc.code})") from None
    except (urllib.error.URLError, OSError, KeyError, ValueError) as exc:
        raise Failed(f"Couldn't reach Microsoft to send the email ({type(exc).__name__})") from None


def email(name: str, label: str, post: str, magnet: str, settings: dict[str, str]) -> str:
    """Step 5: the owner's copy: .torrent attached, magnet link, the post to paste. Returns the address."""
    to = settings.get("TORRENT_NOTIFY_TO") or DEFAULT_TO
    message = EmailMessage()
    message["From"] = settings.get("STOREFRONT_MAIL_FROM") or DEFAULT_FROM
    message["To"] = to
    message["Subject"] = f"TCGCSV torrent ready: {label}"
    message["Message-ID"] = make_msgid(domain="poke6s.com")
    message.set_content(
        f"{name} is built, checked and seeding from qBittorrent (Z:\\TCGCSV).\n\n"
        f"To share it: reply in the {DISCORD_THREAD} with the message below, and attach the\n"
        f"{name}.torrent file from this email.\n\n"
        f"----- paste from here -----\n{post}\n----- to here -----\n\n"
        f"Magnet link on its own:\n{magnet}\n")
    message.add_attachment((TORRENTS_DIR / f"{name}.torrent").read_bytes(), maintype="application",
                           subtype="x-bittorrent", filename=f"{name}.torrent")
    graph_send(settings, message)
    return to


def main(argv: list[str] | None = None, today: date | None = None) -> int:
    """Run the job; argv and today exist for the tests."""
    parser = argparse.ArgumentParser(description="Build, check, seed and announce a month's TCGCSV archive torrent.")
    parser.add_argument("--month", help="YYYY-MM (default: last month)")
    parser.add_argument("--no-email", action="store_true", help="Skip the email (and don't mark the month done).")
    args = parser.parse_args(argv)
    today = today or datetime.now(UTC).date()
    month = args.month or previous_month(today)
    name = f"tcgcsv-archive-{month}"
    label = datetime.strptime(month, "%Y-%m").strftime("%B %Y")
    marker = TORRENTS_DIR / f"{name}.published"
    if marker.exists():
        log(f"Already published: {name} ({marker.name}); nothing to do")
        return 0

    missing = missing_days(month)
    if missing:
        days = ", ".join(day.isoformat() for day in missing[:5]) + (" ..." if len(missing) > 5 else "")
        if args.month or today.day >= LAST_TRY_DAY:
            log(f"FAILED: {len(missing)} day(s) of {month} still missing ({days}); "
                f"check the archive backfill, or build by hand with --allow-gaps")
            return 1
        log(f"WAITING: {len(missing)} day(s) of {month} not downloaded yet ({days}); trying again tomorrow")
        return EXIT_WAITING

    try:
        days = calendar.monthrange(*(int(part) for part in month.split("-")))[1]
        build(month, name)
        magnet = (TORRENTS_DIR / f"{name}.magnet.txt").read_text().strip()
        info_hash = magnet.split("xt=urn:btih:", 1)[1].split("&", 1)[0].lower()
        size = copy_to_share(name, days)
        seed(name, info_hash)
        post = discord_post(name, label, days, size, magnet)
        if args.no_email:
            log("Email skipped (--no-email); the month isn't marked done")
            print(post)
            return 0
        to = email(name, label, post, magnet, env_file())
        log(f"Emailed the .torrent and the post to {to}")
        marker.write_text(json.dumps({"published": datetime.now(UTC).isoformat(timespec="seconds"),
                                      "info_hash": info_hash, "emailed_to": to}) + "\n")
        log(f"PUBLISHED {name}: {days} days, {size / 1e6:,.0f} MB, info hash {info_hash}")
        print(f"Magnet: {magnet}", flush=True)
        return 0
    except Failed as exc:
        log(f"FAILED: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
