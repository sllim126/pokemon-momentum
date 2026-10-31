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

The posts carried over from Squarespace were converted by article_import.py.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import quote, unquote

from markdown_it import MarkdownIt

from .classify import fold
from .markdown import split_front_matter

WORDS_PER_MINUTE = 230
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
    cover_in_body: bool            # the cover is the body's first picture (so not shown twice)
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
    cover_in_body = False
    if cover is None:
        first = MEDIA_IMAGE.search(body)
        cover = first.group(1) if first else None
        cover_in_body = cover is not None
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

    def published(self) -> list[Article]:
        return [a for a in self.all if not a.draft]

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
