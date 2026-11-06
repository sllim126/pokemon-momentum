#!/usr/bin/env python3
"""Create the public KPDH promo images from the owner's archival scans.

The intake PDFs contain one JPEG scan each. This script extracts that JPEG without
recompressing it, crops the reviewed card positions, and writes web-sized JPEGs plus
WebP thumbnails. The PDFs stay under ``data/`` and are never copied into the public
Docker image or Git repository.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from PIL import Image, ImageOps


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INTAKE = ROOT.parent / "data" / "kpdh_scan_intake" / "promos" / "2026-11-06"

# Coordinates are pixels in the losslessly extracted 5100 x 6600 scans. Front and
# back sheets use the same physical placement, so each pair intentionally shares
# crop boxes. The small margin retains the sealed sleeve as physical-card evidence.
SHEETS = {
    "scan0325": (
        ("kpdh01-pr-01", (600, 530, 2080, 2610)),
        ("kpdh01-pr-02", (2500, 370, 4000, 2510)),
    ),
    "scan0326": (
        ("kpdh01-pr-01", (600, 530, 2080, 2610)),
        ("kpdh01-pr-02", (2500, 370, 4000, 2510)),
    ),
    "scan0327": (
        ("kpdh01-pr2-01", (1510, 260, 3630, 1810)),
        ("kpdh01-pr2-03", (1510, 2020, 3670, 3650)),
        ("kpdh01-pr2-02", (1580, 3880, 3760, 5540)),
    ),
    "scan0328": (
        ("kpdh01-pr2-01", (1510, 260, 3630, 1810)),
        ("kpdh01-pr2-03", (1510, 2020, 3670, 3650)),
        ("kpdh01-pr2-02", (1580, 3880, 3760, 5540)),
    ),
    "scan0329": (
        ("kpdh01-pr1-01", (2930, 340, 4500, 2500)),
        ("kpdh01-pr1-02", (850, 2200, 2510, 4500)),
        ("kpdh01-pr1-03", (2790, 3660, 4430, 5750)),
    ),
    "scan0330": (
        ("kpdh01-pr1-01", (2930, 340, 4500, 2500)),
        ("kpdh01-pr1-02", (850, 2200, 2510, 4500)),
        ("kpdh01-pr1-03", (2790, 3660, 4430, 5750)),
    ),
}


def extract_embedded_jpeg(pdf: Path) -> bytes:
    """Return the single full-page JPEG stream embedded in a scanner PDF."""
    content = pdf.read_bytes()
    start = content.find(b"\xff\xd8\xff")
    end = content.rfind(b"\xff\xd9")
    if start < 0 or end < start:
        raise ValueError(f"No embedded JPEG found in {pdf}")
    return content[start : end + 2]


def source_image(intake: Path, stem: str, temporary: Path) -> Path:
    """Use an extracted JPEG if present, otherwise extract it from its PDF."""
    sheet_folder = "backs" if int(stem[-3:]) % 2 == 0 else "fronts"
    jpeg = intake / sheet_folder / f"{stem}.jpg"
    if jpeg.is_file():
        return jpeg
    pdf = intake / sheet_folder / f"{stem}.pdf"
    if not pdf.is_file():
        raise FileNotFoundError(f"Missing {pdf}")
    temporary.mkdir(parents=True, exist_ok=True)
    destination = temporary / f"{stem}.jpg"
    destination.write_bytes(extract_embedded_jpeg(pdf))
    return destination


def save_card(source: Image.Image, box: tuple[int, int, int, int], full: Path, thumb: Path) -> None:
    """Crop one reviewed card and save its full and thumbnail publishing assets."""
    crop = ImageOps.exif_transpose(source).crop(box).convert("RGB")
    full.parent.mkdir(parents=True, exist_ok=True)
    thumb.parent.mkdir(parents=True, exist_ok=True)
    crop.save(full, "JPEG", quality=92, optimize=True, progressive=True, dpi=(600, 600))
    crop.thumbnail((420, 420), Image.Resampling.LANCZOS)
    crop.save(thumb, "WEBP", quality=80, method=6)


def build(intake: Path, temporary: Path) -> None:
    for stem, cards in SHEETS.items():
        back = int(stem[-3:]) % 2 == 0
        with Image.open(source_image(intake, stem, temporary)) as sheet:
            if sheet.size != (5100, 6600):
                raise ValueError(f"{stem} is {sheet.size}; expected the reviewed 5100 x 6600 scan")
            for code, box in cards:
                folder = "backs/promo" if back else "promo"
                save_card(
                    sheet,
                    box,
                    ROOT / "cards" / folder / f"{code}.jpg",
                    ROOT / "cards" / "thumbs" / folder / f"{code}.webp",
                )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--intake", type=Path, default=DEFAULT_INTAKE)
    parser.add_argument("--temporary", type=Path, default=Path("/tmp/kpdh-promo-extracted"))
    args = parser.parse_args()
    build(args.intake, args.temporary)


if __name__ == "__main__":
    main()
