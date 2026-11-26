"""Social cards: the 1200x630 picture Discord, Facebook, X and others show when an article's link is
shared (og:image / twitter:image). Plan Phase 4, "social cards per article" (built 2026-11-26).

Each card has the article's category, its title in large type (and its description when it has
no cover), the Poke6s logo and poke6s.com on
the store's dark background, with the article's cover photo on the right (faded into the
background). A post without a cover gets the brand background alone. Covers come in every shape,
so they're cropped to fill the right side rather than squeezed.

Cards are drawn on first request and kept in data/storefront/cards/ (not committed; delete the
folder to redraw them all). The file name carries a hash of everything drawn on the card, and the
page links the card with that hash (`?v=`), so a changed title or cover is a new URL: sites that
cache shared links (Discord, Facebook) then fetch the new card instead of showing the old one.

The font is Bitstream Vera Sans Bold, which ships inside reportlab (already installed for packing
slips), so the image needs no extra font package.
"""

from __future__ import annotations

import hashlib
import os
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

WIDTH, HEIGHT = 1200, 630
PHOTO_WIDTH = 520           # the cover photo's share of the right side
MARGIN = 64
BACKGROUND = (8, 17, 29)    # store.css --bg
ACCENT = (77, 163, 255)     # store.css --accent
TEXT = (238, 243, 250)
MUTED = (150, 165, 185)
CARD_VERSION = "1"          # bump to redraw every card after a design change


def _font_path() -> Path:
    import reportlab
    return Path(os.path.dirname(reportlab.__file__)) / "fonts" / "VeraBd.ttf"


def card_key(title: str, category: str, cover: str | None, description: str = "") -> str:
    """A short hash of everything on the card: a new key means a new picture (and a new URL)."""
    text = "\n".join((CARD_VERSION, title, category, cover or "", "" if cover else description))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:10]


def _wrap(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.FreeTypeFont, width: int) -> list[str]:
    lines: list[str] = []
    for word in text.split():
        if lines and draw.textlength(f"{lines[-1]} {word}", font=font) <= width:
            lines[-1] = f"{lines[-1]} {word}"
        else:
            lines.append(word)
    return lines


def _title_lines(draw, title: str, width: int, max_height: int) -> tuple[list[str], ImageFont.FreeTypeFont, int]:
    """The largest size (64 down to 34 px) at which the title fits in four lines; below that
    the fourth line ends in an ellipsis."""
    path = str(_font_path())
    for size in range(64, 33, -4):
        font = ImageFont.truetype(path, size)
        lines = _wrap(draw, title, font, width)
        step = int(size * 1.18)
        if len(lines) <= 4 and len(lines) * step <= max_height:
            return lines, font, step
    lines = lines[:4]
    while draw.textlength(lines[-1] + "…", font=font) > width and " " in lines[-1]:
        lines[-1] = lines[-1].rsplit(" ", 1)[0]
    lines[-1] += "…"
    return lines, font, step


def _cover_panel(cover_path: Path | None) -> Image.Image | None:
    if cover_path is None or not cover_path.is_file():
        return None
    with Image.open(cover_path) as source:
        photo = source.convert("RGB")
    # Crop to fill the panel (centre crop), whatever shape the cover is.
    scale = max(PHOTO_WIDTH / photo.width, HEIGHT / photo.height)
    photo = photo.resize((max(PHOTO_WIDTH, round(photo.width * scale)), max(HEIGHT, round(photo.height * scale))),
                         Image.LANCZOS)
    left, top = (photo.width - PHOTO_WIDTH) // 2, (photo.height - HEIGHT) // 2
    return photo.crop((left, top, left + PHOTO_WIDTH, top + HEIGHT))


def draw_card(title: str, category: str, cover_path: Path | None, logo_path: Path | None,
              description: str = "") -> Image.Image:
    card = Image.new("RGB", (WIDTH, HEIGHT), BACKGROUND)
    photo = _cover_panel(cover_path)
    text_width = WIDTH - 2 * MARGIN
    if photo is not None:
        # The photo on the right, fading into the background over its left 200 px.
        mask = Image.new("L", photo.size, 255)
        fade = ImageDraw.Draw(mask)
        for x in range(200):
            fade.line([(x, 0), (x, HEIGHT)], fill=round(255 * x / 200))
        card.paste(photo, (WIDTH - PHOTO_WIDTH, 0), mask)
        text_width = WIDTH - PHOTO_WIDTH - MARGIN - 24
    draw = ImageDraw.Draw(card)
    draw.rectangle([0, 0, 12, HEIGHT], fill=ACCENT)          # the accent bar on the left edge

    small = ImageFont.truetype(str(_font_path()), 26)
    y = MARGIN
    if category:
        draw.text((MARGIN, y), category.upper(), font=small, fill=ACCENT)
    y += 56
    lines, font, step = _title_lines(draw, title, text_width, HEIGHT - y - 150)
    for line in lines:
        draw.text((MARGIN, y), line, font=font, fill=TEXT)
        y += step
    if photo is None and description:
        # No cover: the description fills the space, up to three lines.
        body = ImageFont.truetype(str(_font_path()).replace("VeraBd", "Vera"), 28)
        y += 18
        for n, line in enumerate(_wrap(draw, description, body, text_width)):
            if y + 34 > HEIGHT - MARGIN - 80:
                break
            if n == 2:
                line = line.rstrip(".,;:") + "…" if len(_wrap(draw, description, body, text_width)) > 3 else line
            draw.text((MARGIN, y), line, font=body, fill=MUTED)
            y += 38
            if n == 2:
                break

    footer_y = HEIGHT - MARGIN - 52
    x = MARGIN
    if logo_path is not None and logo_path.is_file():
        with Image.open(logo_path) as source:
            logo = source.convert("RGBA")
        logo = logo.resize((round(logo.width * 52 / logo.height), 52), Image.LANCZOS)
        card.paste(logo, (x, footer_y), logo)
        x += logo.width + 20
    draw.text((x, footer_y + 12), "poke6s.com", font=small, fill=MUTED)
    return card


def card_file(cards_dir: Path, slug: str, title: str, category: str, cover_path: Path | None,
              cover: str | None, logo_path: Path | None, description: str = "") -> Path:
    """The card's PNG, drawn once and kept (cards_dir/<slug>-<key>.png); older cards for the same
    article are removed when a new one is drawn."""
    key = card_key(title, category, cover, description)
    path = cards_dir / f"{slug}-{key}.png"
    if not path.is_file():
        cards_dir.mkdir(parents=True, exist_ok=True)
        # Only this article's cards: "<slug>-" plus a 10-character key, so a longer slug that
        # starts the same ("master-sets-part-2-...") is never touched.
        for old in cards_dir.glob(f"{slug}-*.png"):
            if len(old.stem) == len(slug) + 11:
                old.unlink(missing_ok=True)
        temporary = path.with_suffix(".tmp")
        draw_card(title, category, cover_path, logo_path, description).save(temporary, "PNG", optimize=True)
        temporary.replace(path)
    return path
