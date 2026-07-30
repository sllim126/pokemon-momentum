#!/usr/bin/env python3
"""Helpers for caching card images inside the repo-backed /images tree."""

from __future__ import annotations

import mimetypes
from pathlib import Path
from urllib.parse import urlparse

import requests
from PIL import Image


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CARD_IMAGE_DIR = REPO_ROOT / "images" / "cards"
DEFAULT_DOWNLOAD_USER_AGENT = "pokemon-momentum/card-image-cache"

CONTENT_TYPE_EXTENSIONS = {
    "image/jpeg": ".jpg",
    "image/jpg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
    "image/gif": ".gif",
}


def normalize_language(value: str | None) -> str:
    text = str(value or "").strip().lower()
    if text in {"jp", "ja", "jpn", "japanese"}:
        return "japanese"
    return "english"


def guess_image_extension(image_url: str | None, content_type: str | None = None) -> str:
    normalized_content_type = str(content_type or "").split(";", 1)[0].strip().lower()
    if normalized_content_type in CONTENT_TYPE_EXTENSIONS:
        return CONTENT_TYPE_EXTENSIONS[normalized_content_type]

    parsed = urlparse(str(image_url or "").strip())
    suffix = Path(parsed.path).suffix.lower()
    if suffix in {".jpg", ".jpeg", ".png", ".webp", ".gif"}:
        return ".jpg" if suffix == ".jpeg" else suffix

    guessed_type, _ = mimetypes.guess_type(parsed.path)
    if guessed_type in CONTENT_TYPE_EXTENSIONS:
        return CONTENT_TYPE_EXTENSIONS[guessed_type]
    return ".jpg"


def build_card_image_path(
    *,
    image_root: Path,
    sku: str,
    language: str,
    image_url: str | None,
    content_type: str | None = None,
) -> Path:
    extension = guess_image_extension(image_url, content_type=content_type)
    normalized_language = normalize_language(language)
    safe_sku = str(sku or "").strip()
    if not safe_sku:
        raise ValueError("Missing sku for card image path")
    return image_root / normalized_language / f"{safe_sku}{extension}"


def build_card_image_public_url(*, image_path: Path, image_root: Path = DEFAULT_CARD_IMAGE_DIR) -> str:
    relative_path = image_path.resolve().relative_to(image_root.resolve())
    return "/images/cards/" + "/".join(relative_path.parts)


def _sample_corner_background(image: Image.Image) -> tuple[int, int, int, int]:
    # Use the average corner color so square padding blends into the card edge
    # instead of introducing a harsh solid bar.
    rgba = image.convert("RGBA")
    width, height = rgba.size
    points = [
        (0, 0),
        (max(width - 1, 0), 0),
        (0, max(height - 1, 0)),
        (max(width - 1, 0), max(height - 1, 0)),
    ]
    samples = [rgba.getpixel(point) for point in points]
    count = max(len(samples), 1)
    return tuple(int(sum(pixel[idx] for pixel in samples) / count) for idx in range(4))


def square_pad_image(image_path: Path, background: str = "edge") -> bool:
    with Image.open(image_path) as image:
        rgba = image.convert("RGBA")
        width, height = rgba.size
        if width == height:
            return False

        square_size = max(width, height)
        if background == "transparent":
            fill = (0, 0, 0, 0)
        else:
            fill = _sample_corner_background(rgba)

        # Center the full card on a square canvas so storefront thumbnails can
        # show the whole card without cropping the top and bottom edges.
        canvas = Image.new("RGBA", (square_size, square_size), fill)
        offset = ((square_size - width) // 2, (square_size - height) // 2)
        canvas.paste(rgba, offset, rgba)

        target_format = image.format or "PNG"
        if image_path.suffix.lower() in {".jpg", ".jpeg"}:
            canvas = canvas.convert("RGB")
            target_format = "JPEG"
        canvas.save(image_path, format=target_format)
    return True


def cache_card_image(
    *,
    image_url: str,
    image_path: Path,
    timeout: int,
    user_agent: str = DEFAULT_DOWNLOAD_USER_AGENT,
    force: bool = False,
    square_pad: bool = False,
) -> tuple[Path, bool]:
    if image_path.exists() and image_path.stat().st_size > 0 and not force:
        if square_pad:
            square_pad_image(image_path)
        return image_path, False

    image_path.parent.mkdir(parents=True, exist_ok=True)
    response = requests.get(
        image_url,
        headers={"User-Agent": user_agent},
        timeout=timeout,
        stream=True,
    )
    response.raise_for_status()

    resolved_path = build_card_image_path(
        image_root=image_path.parent.parent,
        sku=image_path.stem,
        language=image_path.parent.name,
        image_url=image_url,
        content_type=response.headers.get("Content-Type"),
    )
    # Scrydex may resolve to png while the preview path guessed jpg up front.
    # Reuse an existing correctly typed file when possible instead of creating
    # duplicate cache entries for the same SKU.
    if resolved_path != image_path and not force and resolved_path.exists() and resolved_path.stat().st_size > 0:
        if square_pad:
            square_pad_image(resolved_path)
        return resolved_path, False

    resolved_path.parent.mkdir(parents=True, exist_ok=True)
    with resolved_path.open("wb") as handle:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                handle.write(chunk)

    if square_pad:
        square_pad_image(resolved_path)

    if resolved_path != image_path and image_path.exists():
        image_path.unlink()
    return resolved_path, True
