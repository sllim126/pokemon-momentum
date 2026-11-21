"""Articles (the blog): Markdown files in storefront/content/articles/<slug>.md.

Loaded once at start-up, like the text pages, so restart the container after adding or editing
one. Each file starts with flat front matter (see markdown.split_front_matter):

    ---
    title: Why I Built a Pokemon Market Screener
    date: 2026-03-19
    description: One or two sentences for the list page, search results and social cards.
    author: Adam Mills
    categories: Market Tools, Market Talk
    tags: Price Momentum, Sealed
    cover: why-i-built-a-pokemon-market-screener-3f2a9c1e
    old_path: /resources/why-i-built-a-pokemon-market-screener
    draft: false
    ---

`cover` is an image name in data/storefront/images (served at /media/<width>/<name>.webp);
without one, the first picture in the body is used. `old_path` is the article's Squarespace
address, answered with a 301 (main.py). Drafts are left out of the list, the sitemap and the
home page but still open at their address, with a draft note, so the owner can preview them.
Market boxes: a paragraph holding only `[[market <id> <id> ...]]` (TCGplayer product IDs) becomes
a row of boxes with each product's TCGplayer picture, today's market price and 30-day change,
filled in on every page view from market_reference.json (market_boxes below), so the prices in
an old post stay current. `[[cards <id> ...]]` shows just the pictures, names and sets.
Products missing from that file are left out.
A finished article (draft: false) with a future date is scheduled: hidden the same way, with a
"scheduled" note, until its date arrives (Articles.published).

The posts carried over from Squarespace were converted by article_import.py.
"""

from __future__ import annotations

import html as html_lib
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from urllib.parse import quote, unquote
from zoneinfo import ZoneInfo

from markdown_it import MarkdownIt

from .classify import fold
from .markdown import split_front_matter

WORDS_PER_MINUTE = 230
SHOP_TZ = ZoneInfo("America/Denver")   # same zone as notify.MOUNTAIN


def _today() -> str:
    """Today's date in the shop's time zone, as YYYY-MM-DD (how article dates are written)."""
    return datetime.now(SHOP_TZ).date().isoformat()


MEDIA_IMAGE = re.compile(r"/media/\d+/([a-z0-9-]+)\.webp")

# Raw HTML stays escaped, as on the other pages. Images in an article are lazy-loaded.
_md = MarkdownIt("commonmark", {"html": False, "linkify": False, "typographer": False})
_default_image = _md.renderer.rules.get("image")


def _lazy_image(renderer, tokens, idx, options, env):
    tokens[idx].attrSet("loading", "lazy")
    return _default_image(tokens, idx, options, env)


_md.add_render_rule("image", _lazy_image)


@dataclass(frozen=True)
class Article:
    slug: str
    title: str
    date: str                      # YYYY-MM-DD
    description: str
    author: str
    categories: tuple[str, ...]
    tags: tuple[str, ...]
    cover: str | None              # image name, or None
    cover_in_body: bool            # the cover also appears in the text (so not shown twice)
    old_path: str | None
    draft: bool
    html: str
    minutes: int                   # reading time
    search_text: str = field(repr=False, default="")

    def has_category(self, name: str) -> bool:
        return fold(name) in {fold(c) for c in self.categories}

    def has_tag(self, name: str) -> bool:
        return fold(name) in {fold(t) for t in self.tags}


def _list(value: str) -> tuple[str, ...]:
    return tuple(part.strip() for part in (value or "").split(",") if part.strip())


def parse(slug: str, text: str) -> Article:
    meta, body = split_front_matter(text)
    cover = meta.get("cover", "").strip() or None
    if cover is None:
        first = MEDIA_IMAGE.search(body)
        cover = first.group(1) if first else None
    # A cover that is also a picture in the text isn't repeated above it.
    cover_in_body = cover is not None and cover in set(MEDIA_IMAGE.findall(body))
    words = len(re.findall(r"\w+", re.sub(r"!\[[^\]]*\]\([^)]*\)", "", body)))
    title = meta.get("title", slug.replace("-", " ").title())
    return Article(
        slug=slug,
        title=title,
        date=meta.get("date", ""),
        description=meta.get("description", ""),
        author=meta.get("author", ""),
        categories=_list(meta.get("categories", "")),
        tags=_list(meta.get("tags", "")),
        cover=cover,
        cover_in_body=cover_in_body,
        old_path=meta.get("old_path", "").strip() or None,
        draft=meta.get("draft", "").strip().lower() == "true",
        html=_md.render(body),
        minutes=max(1, round(words / WORDS_PER_MINUTE)),
        search_text=fold(" ".join([title, meta.get("description", ""), meta.get("tags", ""),
                                   meta.get("categories", "")])),
    )


class Articles:
    """All articles, newest first, with lookups by slug and by old Squarespace address."""

    def __init__(self, folder: Path) -> None:
        loaded = [parse(path.stem, path.read_text(encoding="utf-8"))
                  for path in sorted(folder.glob("*.md"))] if folder.is_dir() else []
        self.all = sorted(loaded, key=lambda a: (a.date, a.slug), reverse=True)
        self.by_slug = {a.slug: a for a in self.all}
        self.by_old_path = {a.old_path.lower().rstrip("/"): a for a in self.all if a.old_path}

    def published(self, today: str | None = None) -> list[Article]:
        """Articles that are live: not drafts, and dated today or earlier.

        A finished article with a future date is scheduled: it stays out of the list, the
        sitemap and the home page until its date, then appears with no restart, because the
        date is checked on every call. `today` is for tests; it defaults to today's date in
        Mountain time.
        """
        today = today or _today()
        return [a for a in self.all if not a.draft and a.date <= today]

    def is_scheduled(self, article: Article, today: str | None = None) -> bool:
        """True for a finished article whose date hasn't come yet."""
        return not article.draft and article.date > (today or _today())

    def get(self, slug: str) -> Article | None:
        return self.by_slug.get(slug)

    def filtered(self, category: str = "", tag: str = "") -> list[Article]:
        found = self.published()
        if category:
            found = [a for a in found if a.has_category(category)]
        if tag:
            found = [a for a in found if a.has_tag(tag)]
        return found

    def categories(self) -> list[tuple[str, int]]:
        """(category, number of published articles), most used first."""
        counts: dict[str, int] = {}
        names: dict[str, str] = {}
        for article in self.published():
            for name in article.categories:
                key = fold(name)
                names.setdefault(key, name)
                counts[key] = counts.get(key, 0) + 1
        return sorted(((names[k], n) for k, n in counts.items()), key=lambda item: (-item[1], item[0]))

    def more_like(self, article: Article, limit: int = 3) -> list[Article]:
        """Other published articles, those sharing a category or tag first, then newest."""
        others = [a for a in self.published() if a.slug != article.slug]
        shared = set(map(fold, article.categories + article.tags))
        others.sort(key=lambda a: -len(shared & set(map(fold, a.categories + a.tags))))
        return others[:limit]

    def redirect_for(self, path: str) -> str | None:
        """New address for an old Squarespace blog address, or None if it isn't one.

        /resources                    -> /articles
        /resources/<old slug>         -> /articles/<slug>
        /resources/category/<name>    -> /articles?category=<name>
        /resources/tag/<name>         -> /articles?tag=<name>
        any other /resources/...      -> /articles
        """
        lowered = path.lower().rstrip("/")
        if lowered != "/resources" and not lowered.startswith("/resources/"):
            return None
        found = self.by_old_path.get(lowered)
        if found:
            return f"/articles/{found.slug}"
        parts = path.rstrip("/").split("/")
        if len(parts) == 4 and parts[2].lower() in {"category", "tag"}:
            name = unquote(parts[3].replace("+", " "))
            return f"/articles?{parts[2].lower()}={quote(name)}"
        return "/articles"


# --- market boxes -----------------------------------------------------------------------------

# `[[market 712094 712102]]` (pictures with prices) or `[[cards 662184 655886]]` (pictures only),
# alone in a paragraph, as markdown-it renders it.
MARKET_TAG = re.compile(r"<p>\[\[(market|cards)((?:\s+\d+)+)\s*\]\]</p>")
# TCGplayer's own product pictures (allowed in the article page's CSP, main.ARTICLE_CSP).
TCGPLAYER_IMAGE = "https://tcgplayer-cdn.tcgplayer.com/product/{id}_{size}w.jpg"
TCGPLAYER_CARD_LARGE = "https://tcgplayer-cdn.tcgplayer.com/product/{id}_in_1000x1000.jpg"


def _market_box(product_id: str, entry: dict, with_price: bool = True) -> str:
    name = html_lib.escape(entry.get("name") or "")
    price = f'<span class="market-box-price">${float(entry["price"]):,.2f}</span>' if with_price else ""
    change = entry.get("roc_30d_pct")
    if change is None or not with_price:
        trend = ""
    else:
        # Arrow and words, not colour: a falling price isn't bad news for a buyer.
        arrow = "▲" if change > 0 else "▼" if change < 0 else "•"
        trend = f'<span class="market-box-trend">{arrow} {abs(change):.1f}% in 30 days</span>'
    # Single cards get their set under the name, since an English and a Japanese printing
    # share the name; anything Japanese says so. ("ME02: Phantasmal Flames" -> "Phantasmal Flames")
    details = []
    if entry.get("kind") == "card" and entry.get("group_name"):
        details.append(entry["group_name"].split(": ", 1)[-1])
    if entry.get("market") == "JP":
        details.append("Japanese")
    detail = (f'<span class="market-box-set">{html_lib.escape(" · ".join(details))}</span>'
              if details else "")
    link = entry.get("url")
    history = (f'<a class="market-box-link" href="{html_lib.escape(link)}">Price history</a>'
               if link and with_price else "")
    if entry.get("kind") == "card":
        # Cards are shown big, for their art (a row of cards gets wider boxes, store.css).
        small, large = TCGPLAYER_IMAGE.format(id=product_id, size=400), TCGPLAYER_CARD_LARGE.format(id=product_id)
        width, height = 400, 558
    else:
        small, large = TCGPLAYER_IMAGE.format(id=product_id, size=200), TCGPLAYER_IMAGE.format(id=product_id, size=400)
        width, height = 200, 200
    return (f'<div class="market-box"><img src="{small}" srcset="{small} 1x, {large} 2x" alt="{name}" '
            f'width="{width}" height="{height}" loading="lazy">'
            f'<div class="market-box-text"><span class="market-box-name">{name}</span>{detail}'
            f'{price}{trend}{history}</div></div>')


def market_boxes(html: str, market) -> str:
    """Replace each [[market ...]] / [[cards ...]] paragraph with boxes from `market` (a
    MarketReference). [[cards ...]] shows only the pictures, names and sets, for the art.

    Done per request, not at load time, because the prices change daily. Unknown IDs are
    skipped; a tag with none left disappears, so a post never shows a broken box.
    """
    if "[[market" not in html and "[[cards" not in html:
        return html
    latest = (market.data() or {}).get("latest_date")

    def replace(match: re.Match) -> str:
        with_price = match.group(1) == "market"
        boxes, kinds = [], set()
        for product_id in match.group(2).split():
            entry = market.entry(product_id)
            if entry and entry.get("price"):
                boxes.append(_market_box(product_id, entry, with_price))
                kinds.add(entry.get("kind"))
        if not boxes:
            return ""
        css = "market-boxes market-boxes-cards" if kinds == {"card"} else "market-boxes"
        day = datetime.fromisoformat(latest) if latest else None
        as_of = f" as of {day:%B} {day.day}" if day else ""
        caption = (f"TCGplayer market prices{as_of}, updated daily." if with_price
                   else "Card images from TCGplayer.")
        return f'<figure class="{css}">{"".join(boxes)}<figcaption>{caption}</figcaption></figure>'

    return MARKET_TAG.sub(replace, html)
