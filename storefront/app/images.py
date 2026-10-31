"""Product images: keep the original, and make resized WebP copies for lists and pages.

Layout under data/storefront/images/:
    original/<name>.<ext>   the file as received (never changed)
    480/<name>.webp         product cards in lists
    1200/<name>.webp        product page and zoom

<name> ends in a short hash of the original's bytes, so a replaced photo gets a new address
and the long browser cache (see main.py /media) never shows a stale picture.
"""

from __future__ import annotations

import hashlib
import io
from pathlib import Path

from PIL import Image, ImageOps

SIZES = (480, 1200)
WEBP_QUALITY = 82


def image_name(base: str, data: bytes) -> str:
    return f"{base}-{hashlib.sha256(data).hexdigest()[:10]}"


def store_original(images_dir: Path, base: str, data: bytes, ext: str) -> tuple[str, str]:
    """Save the original bytes and make the WebP copies. Returns (name, ext)."""
    ext = (ext or "jpg").lower().lstrip(".")
    if ext == "jpeg":
        ext = "jpg"
    name = image_name(base, data)
    original = images_dir / "original" / f"{name}.{ext}"
    original.parent.mkdir(parents=True, exist_ok=True)
    if not original.exists():
        original.write_bytes(data)
    make_webp(images_dir, name, data)
    return name, ext


def make_webp(images_dir: Path, name: str, data: bytes, *, force: bool = False) -> None:
    with Image.open(io.BytesIO(data)) as source:
        source = ImageOps.exif_transpose(source)
        if source.mode not in ("RGB", "RGBA"):
            source = source.convert("RGBA" if "A" in source.getbands() else "RGB")
        for width in SIZES:
            target = images_dir / str(width) / f"{name}.webp"
            if target.exists() and not force:
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            copy = source.copy()
            copy.thumbnail((width, width * 2), Image.Resampling.LANCZOS)  # never enlarges
            copy.save(target, "WEBP", quality=WEBP_QUALITY, method=6)


def rebuild_all(images_dir: Path, *, force: bool = False) -> int:
    """Remake WebP copies from every original (after changing SIZES or quality)."""
    count = 0
    for original in sorted((images_dir / "original").glob("*.*")):
        make_webp(images_dir, original.stem, original.read_bytes(), force=force)
        count += 1
    return count
