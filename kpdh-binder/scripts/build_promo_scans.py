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
        ("kpdh01-pr2-01", (1250, 200, 3700, 1980)),
        ("kpdh01-pr2-03", (1250, 2020, 3700, 3800)),
        ("kpdh01-pr2-02", (1250, 3860, 3700, 5620)),
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

# Pillow rotates positive angles counter-clockwise. Each value was measured from the
# corresponding sleeve/card edges after the first public crop review; fronts and backs
# are intentionally independent because the cards were repositioned when flipped.
ROTATIONS = {
    ("scan0325", "kpdh01-pr-01"): -0.5,
    ("scan0325", "kpdh01-pr-02"): 0.0,
    ("scan0326", "kpdh01-pr-01"): 0.0,
    ("scan0326", "kpdh01-pr-02"): 0.3,
    ("scan0327", "kpdh01-pr2-01"): -1.7,
    ("scan0327", "kpdh01-pr2-03"): -2.3,
    ("scan0327", "kpdh01-pr2-02"): -1.5,
    ("scan0328", "kpdh01-pr2-01"): -3.9,
    ("scan0328", "kpdh01-pr2-03"): -1.8,
    ("scan0328", "kpdh01-pr2-02"): -1.0,
    ("scan0329", "kpdh01-pr1-01"): 0.0,
    ("scan0329", "kpdh01-pr1-02"): -3.3,
    ("scan0329", "kpdh01-pr1-03"): 0.5,
    ("scan0330", "kpdh01-pr1-01"): 0.0,
    ("scan0330", "kpdh01-pr1-02"): 2.0,
    ("scan0330", "kpdh01-pr1-03"): 0.0,
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


def squared_crop(crop: Image.Image, angle: float) -> Image.Image:
    """Rotate one scan and return a centered standard-ratio crop without fill corners."""
    width, height = crop.size
    landscape = width > height
    rotated = crop.rotate(angle, resample=Image.Resampling.BICUBIC, expand=True)

    # A small rotation leaves triangular fill areas around the expanded image. The
    # conservative safe rectangle below stays inside the original pixels, then the
    # second step gives every result the physical card's 7:5 or 5:7 aspect ratio.
    from math import cos, radians, sin

    theta = abs(radians(angle))
    safe_width = max(1, round(width * cos(theta) - height * sin(theta)))
    safe_height = max(1, round(height * cos(theta) - width * sin(theta)))
    target_ratio = 7 / 5 if landscape else 5 / 7
    if safe_width / safe_height > target_ratio:
        safe_width = round(safe_height * target_ratio)
    else:
        safe_height = round(safe_width / target_ratio)
    left = (rotated.width - safe_width) // 2
    top = (rotated.height - safe_height) // 2
    return rotated.crop((left, top, left + safe_width, top + safe_height))


def save_card(
    source: Image.Image,
    box: tuple[int, int, int, int],
    angle: float,
    full: Path,
    thumb: Path,
) -> None:
    """Crop one reviewed card and save its full and thumbnail publishing assets."""
    crop = ImageOps.exif_transpose(source).crop(box).convert("RGB")
    crop = squared_crop(crop, angle)
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
                    ROTATIONS[(stem, code)],
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
