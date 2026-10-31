"""Turn the saved Squarespace blog posts into storefront articles.

Reads the copy made by storefront/tools/archive_squarespace_site.py (posts/*.md with
front matter, images/, images.csv) and writes storefront/content/articles/<slug>.md in the
format articles.py reads. Images are stored like product photos (images.store_original:
original plus 480 and 1200 px WebP in data/storefront/images), and the Markdown points at
/media/1200/<name>.webp.

What changes on the way:
- slugs come from the title (the old Squarespace slugs were things like `sv9`); the old
  address is kept as `old_path` and answered with a 301;
- links to poke6s.com become site-relative (old /shop/p/... addresses redirect), and links to
  other posts point at their new /articles/<slug> address;
- an image wrapped in a link to its own Squarespace CDN file is unwrapped;
- an image missing from the copy (one was already broken on Squarespace) is dropped.

Existing article files are left alone unless --force is given, so later edits aren't lost.

The archive lives outside the repo, so mount it when running in the container:

    docker-compose run --rm -v <archive folder>:/archive:ro storefront \
        python -m storefront.app.article_import --from /archive
"""

from __future__ import annotations

import argparse
import csv
import html
import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlsplit

from . import images
from .classify import slugify
from .markdown import split_front_matter
from .settings import STOREFRONT_DIR, load_settings

ARTICLES_DIR = STOREFRONT_DIR / "content" / "articles"
LOCAL_IMAGE = re.compile(r"!\[([^\]]*)\]\(\.\./images/([^)\s]+)\)")
CDN_WRAPPED = re.compile(r"\[(!\[[^\]]*\]\([^)]*\))\]\(https?://images\.squarespace-cdn\.com/[^)]*\)")
CDN_LINK = re.compile(r"\]\((https?://images\.squarespace-cdn\.com/[^)\s]+)\)")
SITE_LINK = re.compile(r"\]\(https?://(?:www\.)?poke6s\.com(/[^)\s]*)?\)")
SLUG_LENGTH = 70


@dataclass
class ImportReport:
    written: list[str] = field(default_factory=list)
    kept: list[str] = field(default_factory=list)
    images: int = 0
    missing_images: list[str] = field(default_factory=list)


def _meta_value(value: str):
    """The archive writes strings and lists as JSON; anything else is plain text."""
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return value


def _one_line(text: str) -> str:
    return " ".join(html.unescape(text or "").split())


def _image_base(file_name: str) -> str:
    # Drop the archive's own hash suffix; store_original adds a content hash.
    stem = Path(file_name).stem
    stem = re.sub(r"-[0-9a-f]{8}$", "", stem)
    return slugify(stem, 40)


def article_slug(title: str) -> str:
    """Slug from the title, with apostrophes dropped: "Let's talk" -> "lets-talk", not "let-s-talk"."""
    return slugify(re.sub(r"['\u2019]", "", title), SLUG_LENGTH)


def run(archive: Path, *, articles_dir: Path = ARTICLES_DIR, images_dir: Path,
        force: bool = False, dry_run: bool = False) -> ImportReport:
    report = ImportReport()
    cdn_files: dict[str, str] = {}
    images_csv = archive / "images.csv"
    if images_csv.exists():
        with images_csv.open(encoding="utf-8", newline="") as handle:
            cdn_files = {row["url"]: row["file"] for row in csv.DictReader(handle)}

    posts = []
    for path in sorted((archive / "posts").glob("*.md")):
        raw_meta, body = split_front_matter(path.read_text(encoding="utf-8"))
        meta = {key: _meta_value(value) for key, value in raw_meta.items()}
        title = _one_line(str(meta.get("title") or path.stem))
        old_path = urlsplit(str(meta.get("old_url") or "")).path.rstrip("/") or None
        posts.append({"meta": meta, "body": body, "title": title, "slug": article_slug(title),
                      "old_path": old_path})
    new_address = {p["old_path"].lower(): f"/articles/{p['slug']}" for p in posts if p["old_path"]}

    stored: dict[str, str | None] = {}

    def store(file_name: str) -> str | None:
        """Store one archive image; returns its media name, or None if it isn't in the copy."""
        if file_name not in stored:
            source = archive / "images" / file_name
            if not source.is_file():
                stored[file_name] = None
                report.missing_images.append(file_name)
            elif dry_run:
                stored[file_name] = images.image_name(_image_base(file_name), source.read_bytes())
            else:
                name, _ext = images.store_original(images_dir, _image_base(file_name), source.read_bytes(),
                                                   source.suffix)
                stored[file_name] = name
                report.images += 1
        return stored[file_name]

    def site_link(match: re.Match) -> str:
        path = (match.group(1) or "/").rstrip("/") or "/"
        return f"]({new_address.get(path.lower(), path)})"

    def cdn_link(match: re.Match) -> str:
        file_name = cdn_files.get(match.group(1).split("?")[0].rstrip("/"))
        name = store(file_name) if file_name else None
        return f"](/media/1200/{name}.webp)" if name else match.group(0)

    def local_image(match: re.Match) -> str:
        name = store(match.group(2))
        return f"![{match.group(1)}](/media/1200/{name}.webp)" if name else ""

    articles_dir.mkdir(parents=True, exist_ok=True)
    for post in posts:
        meta, slug = post["meta"], post["slug"]
        target = articles_dir / f"{slug}.md"
        if target.exists() and not force:
            report.kept.append(slug)
            continue
        body = CDN_WRAPPED.sub(r"\1", post["body"])
        body = LOCAL_IMAGE.sub(local_image, body)
        body = CDN_LINK.sub(cdn_link, body)
        body = SITE_LINK.sub(site_link, body)
        body = re.sub(r"\n{3,}", "\n\n", body).strip() + "\n"

        cover = ""
        cover_ref = str(meta.get("cover_image") or "")
        if cover_ref.startswith("../images/"):
            cover = store(cover_ref.removeprefix("../images/")) or ""
        description = _one_line(str(meta.get("excerpt") or meta.get("seo_description") or ""))
        front = [
            "---",
            f"title: {post['title']}",
            f"date: {meta.get('date') or ''}",
            f"description: {description}",
            f"author: {_one_line(str(meta.get('author') or ''))}",
            f"categories: {', '.join(_one_line(c) for c in meta.get('categories') or [])}",
            f"tags: {', '.join(_one_line(t) for t in meta.get('tags') or [])}",
            f"cover: {cover}",
            f"old_path: {post['old_path'] or ''}",
            "draft: false",
            "---",
            "",
        ]
        if not dry_run:
            target.write_text("\n".join(front) + body, encoding="utf-8")
        report.written.append(slug)
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Import saved Squarespace blog posts as articles.")
    parser.add_argument("--from", dest="archive", required=True, type=Path,
                        help="folder written by storefront/tools/archive_squarespace_site.py")
    parser.add_argument("--force", action="store_true", help="overwrite existing article files")
    parser.add_argument("--dry-run", action="store_true", help="report only; write nothing")
    args = parser.parse_args(argv)
    settings = load_settings()
    report = run(args.archive, images_dir=settings.images_dir, force=args.force, dry_run=args.dry_run)
    print(f"articles written {len(report.written)}, kept {len(report.kept)}, images stored {report.images}")
    for slug in report.written:
        print("  wrote", slug)
    for slug in report.kept:
        print("  kept (exists; --force to overwrite)", slug)
    for name in report.missing_images:
        print("  image not in the copy, dropped:", name)
    return 0


if __name__ == "__main__":
    sys.exit(main())
