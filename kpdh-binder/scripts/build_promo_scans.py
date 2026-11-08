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

# Four outer sleeve corners in full-sheet coordinates, ordered upper-left,
# lower-left, lower-right, upper-right for Pillow's QUAD transform. Unlike a
# rotation followed by a center crop, these measurements correct perspective and
# retain every sleeve edge even when a card was not centered in its original box.
QUADS = {
    ("scan0325", "kpdh01-pr-01"): (675, 555, 675, 2600, 2080, 2590, 2075, 550),
    ("scan0325", "kpdh01-pr-02"): (2490, 280, 2485, 2510, 4070, 2495, 4050, 270),
    ("scan0326", "kpdh01-pr-01"): (700, 255, 700, 2310, 2155, 2300, 2150, 250),
    ("scan0326", "kpdh01-pr-02"): (2650, 180, 2650, 2370, 4210, 2380, 4200, 185),
    ("scan0329", "kpdh01-pr1-01"): (2920, 270, 2910, 2470, 4500, 2510, 4510, 300),
    ("scan0329", "kpdh01-pr1-02"): (800, 2140, 900, 4500, 2580, 4380, 2490, 2080),
    ("scan0329", "kpdh01-pr1-03"): (2780, 3480, 2770, 5750, 4430, 5780, 4430, 3500),
    ("scan0330", "kpdh01-pr1-01"): (2750, 400, 2750, 2590, 4380, 2630, 4400, 430),
    ("scan0330", "kpdh01-pr1-02"): (760, 2010, 700, 4310, 2400, 4390, 2440, 2070),
    ("scan0330", "kpdh01-pr1-03"): (2600, 3650, 2600, 5850, 4310, 5850, 4300, 3650),
}

# The second scan pass put one physical card on each page. These replace the
# corresponding crops from the crowded first-pass sheets above. Coordinates
# follow Pillow's upper-left, lower-left, lower-right, upper-right QUAD order and
# retain a narrow strip of the clear sleeve around the physical card. Landscape
# PR2 cards were placed upright on the scanner and are rotated after rectifying.
INDIVIDUAL_SCANS = {
    "scan0331": ("fronts", (("kpdh01-pr1-01", "front", (40, 45, 40, 2130, 1525, 2130, 1525, 45), False),)),
    "scan0332": ("backs", (("kpdh01-pr1-01", "back", (50, 35, 50, 2160, 1550, 2160, 1550, 35), False),)),
    "scan0333": ("fronts", (("kpdh01-pr1-02", "front", (40, 40, 40, 2125, 1525, 2125, 1525, 40), False),)),
    "scan0334": ("backs", (("kpdh01-pr1-02", "back", (50, 30, 50, 2160, 1570, 2160, 1570, 30), False),)),
    "scan0335": ("fronts", (("kpdh01-pr1-03", "front", (40, 45, 40, 2135, 1530, 2135, 1530, 45), False),)),
    "scan0336": ("fronts", (("kpdh01-pr2-01", "front", (40, 80, 40, 2180, 1530, 2180, 1530, 80), True),)),
    "scan0337": (
        "backs",
        (
            ("kpdh01-pr2-01", "back", (40, 5, 40, 2095, 1530, 2095, 1530, 5), True),
            ("kpdh01-pr2-02", "back", (40, 5, 40, 2095, 1530, 2095, 1530, 5), True),
            ("kpdh01-pr2-03", "back", (40, 5, 40, 2095, 1530, 2095, 1530, 5), True),
        ),
    ),
    "scan0338": ("fronts", (("kpdh01-pr2-02", "front", (40, 110, 40, 2210, 1540, 2210, 1540, 110), True),)),
    "scan0339": ("fronts", (("kpdh01-pr2-03", "front", (50, 105, 50, 2210, 1555, 2210, 1555, 105), True),)),
    "scan0340": ("fronts", (("kpdh01-pr-02", "front", (45, 90, 45, 2200, 1540, 2200, 1540, 90), False),)),
    "scan0341": ("fronts", (("kpdh01-pr-01", "front", (10, 10, 10, 2100, 1500, 2100, 1500, 10), False),)),
    "scan0343": ("backs", (("kpdh01-pr1-03", "back", (50, 20, 50, 2170, 1550, 2170, 1550, 20), False),)),
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


def individual_source_image(intake: Path, stem: str, folder: str, temporary: Path) -> Path:
    """Resolve a one-card rescan whose front/back folder is explicitly reviewed."""
    jpeg = intake / folder / f"{stem}.jpg"
    if jpeg.is_file():
        return jpeg
    pdf = intake / folder / f"{stem}.pdf"
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


def perspective_crop(source: Image.Image, quad: tuple[int, ...]) -> Image.Image:
    """Square one sleeve from its four corners and retain a uniform outer margin."""
    points = [(quad[index], quad[index + 1]) for index in range(0, 8, 2)]
    center_x = sum(point[0] for point in points) / 4
    center_y = sum(point[1] for point in points) / 4

    # Expanding the measured sleeve quadrilateral by 3% keeps a narrow strip of
    # scanner bed around all four sides. That makes the sleeve boundary visible
    # in the detail modal instead of letting it disappear against the image edge.
    expanded = tuple(
        coordinate
        for x, y in points
        for coordinate in (
            round(center_x + (x - center_x) * 1.03),
            round(center_y + (y - center_y) * 1.03),
        )
    )
    return source.transform(
        (1500, 2100),
        Image.Transform.QUAD,
        expanded,
        resample=Image.Resampling.BICUBIC,
    )


def individual_crop(source: Image.Image, quad: tuple[int, ...], landscape: bool) -> Image.Image:
    """Rectify a one-card scan, rotating cards printed in landscape orientation."""
    crop = source.transform(
        (1500, 2100),
        Image.Transform.QUAD,
        quad,
        resample=Image.Resampling.BICUBIC,
    )
    # The landscape cards were placed with their printed top edge at the scanner's
    # right side, so a clockwise turn restores the intended reading direction.
    return crop.rotate(-90, expand=True) if landscape else crop


def save_assets(crop: Image.Image, full: Path, thumb: Path) -> None:
    """Write one prepared scan at full detail and as its optimized thumbnail."""
    full.parent.mkdir(parents=True, exist_ok=True)
    thumb.parent.mkdir(parents=True, exist_ok=True)
    crop.save(full, "JPEG", quality=92, optimize=True, progressive=True, dpi=(600, 600))
    crop.thumbnail((420, 420), Image.Resampling.LANCZOS)
    crop.save(thumb, "WEBP", quality=80, method=6)


def save_card(
    source: Image.Image,
    box: tuple[int, int, int, int],
    angle: float,
    quad: tuple[int, ...] | None,
    full: Path,
    thumb: Path,
) -> None:
    """Crop one reviewed card and save its full and thumbnail publishing assets."""
    source = ImageOps.exif_transpose(source).convert("RGB")
    crop = perspective_crop(source, quad) if quad else squared_crop(source.crop(box), angle)
    save_assets(crop, full, thumb)


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
                    QUADS.get((stem, code)),
                    ROOT / "cards" / folder / f"{code}.jpg",
                    ROOT / "cards" / "thumbs" / folder / f"{code}.webp",
                )

    # Build the individually rescanned cards last so they supersede only the old
    # assets they replace. PR-01/PR-02 backs intentionally remain from scan0326.
    for stem, (source_folder, cards) in INDIVIDUAL_SCANS.items():
        with Image.open(individual_source_image(intake, stem, source_folder, temporary)) as sheet:
            if sheet.size != (5100, 6600):
                raise ValueError(f"{stem} is {sheet.size}; expected the reviewed 5100 x 6600 scan")
            source = ImageOps.exif_transpose(sheet).convert("RGB")
            for code, face, quad, landscape in cards:
                folder = "backs/promo" if face == "back" else "promo"
                crop = individual_crop(source, quad, landscape)
                save_assets(
                    crop,
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
