"""Set (or change) the store admin's password. Run it yourself on the server, from the repo:

    python3 -m storefront.admin.passwd

It asks for the password twice (nothing is shown as you type) and saves its hash in .env as
STOREFRONT_ADMIN_PASSWORD_HASH. The first time, it also adds STOREFRONT_ADMIN_SESSION_SECRET,
the random key that signs sign-in cookies. Neither value is printed. Then reload the admin:
`bash deploy/update_storefront.sh`. Changing the password signs every device out.

Standard library only, so it runs on the host's Python. `--env PATH` picks another file.
"""

from __future__ import annotations

import argparse
import getpass
import os
import secrets
import sys
import tempfile
from pathlib import Path

from .auth import MIN_PASSWORD_LENGTH, hash_password

REPO_ROOT = Path(__file__).resolve().parents[2]
HASH_KEY = "STOREFRONT_ADMIN_PASSWORD_HASH"
SECRET_KEY = "STOREFRONT_ADMIN_SESSION_SECRET"


def update_env(path: Path, values: dict[str, str], keep_existing: tuple[str, ...] = ()) -> None:
    """Set each key in the .env file, replacing its line or adding one at the end.

    Keys in `keep_existing` are only added when missing. The file is rewritten through a
    temporary file in the same folder and keeps its permissions, so a crash can't leave a
    half-written .env.
    """
    lines = path.read_text(encoding="utf-8").splitlines() if path.exists() else []
    remaining = dict(values)
    out = []
    for line in lines:
        key = line.split("=", 1)[0].strip()
        if key in remaining:
            if key in keep_existing and line.split("=", 1)[1].strip():
                remaining.pop(key)
                out.append(line)
                continue
            out.append(f"{key}={remaining.pop(key)}")
        else:
            out.append(line)
    if remaining:
        out.append("# Store admin sign-in (python3 -m storefront.admin.passwd)")
        out.extend(f"{key}={value}" for key, value in remaining.items())
    mode = path.stat().st_mode & 0o777 if path.exists() else 0o600
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".env.")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write("\n".join(out) + "\n")
        os.chmod(tmp, mode)
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Set the store admin password.")
    parser.add_argument("--env", type=Path, default=REPO_ROOT / ".env", help="the .env file to update")
    args = parser.parse_args(argv)
    password = getpass.getpass("New admin password: ")
    if len(password) < MIN_PASSWORD_LENGTH:
        print(f"Too short: use at least {MIN_PASSWORD_LENGTH} characters (a passphrase is fine).")
        return 1
    if getpass.getpass("Same again: ") != password:
        print("The two didn't match; nothing changed.")
        return 1
    update_env(args.env, {HASH_KEY: hash_password(password), SECRET_KEY: secrets.token_hex(32)},
               keep_existing=(SECRET_KEY,))
    print(f"Saved in {args.env}. Reload the admin: bash deploy/update_storefront.sh")
    return 0


if __name__ == "__main__":
    sys.exit(main())
