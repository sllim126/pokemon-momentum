"""Save a full copy of the public Squarespace site (poke6s.com) before the plan ends 2026-11-14.

Squarespace's own export covers products, orders and customers, but not page text, blog posts
("Resources") or images. This script fetches them from the public site, read-only, and writes:

    <out>/
      README.md            what's here and when it was fetched
      index.csv            every saved address: kind, title, Markdown file, raw file
      sitemap.xml          the site's sitemap as fetched
      pages/<slug>.md      page text as Markdown (header, footer and navigation left out)
      pages/<slug>.html    the page as served
      posts/<date>-<slug>.md   blog posts as Markdown with front matter (title, date, tags...)
      posts/<slug>.json    Squarespace's JSON for the post (body HTML, SEO fields, cover image)
      shop/<path>.html     shop, category and product pages as served (product data itself is
                           in the products export)
      images/<file>        every image used by pages, posts and products, original size
      images.csv           image address, saved file, and where it was used

Markdown files point at images as ../images/<file>. Re-running is safe: images already saved
are skipped, pages and posts are fetched again.

Run on the host (needs requests and beautifulsoup4, which the storefront container doesn't
have):

    python3 storefront/tools/archive_squarespace_site.py --out <folder> \
        --products-csv <products export.csv>

Politeness: one request at a time, 0.3 s apart; stops on HTTP 403 or 429.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
import re
import sys
import time
from pathlib import Path
from urllib.parse import unquote, urlsplit

import requests
from bs4 import BeautifulSoup, NavigableString, Tag

SITE = "https://poke6s.com"
DELAY_S = 0.3
USER_AGENT = "Mozilla/5.0 (poke6s site archive; owner copy)"
CDN_HOST = "images.squarespace-cdn.com"
# Blog listing pages by tag or category only repeat the posts; their names are kept in each
# post's front matter instead.
SKIP_PREFIXES = ("/resources/tag/", "/resources/category/")
BLOG_PATH = "/resources"


class Stop(Exception):
    """The site asked us to back off (403/429)."""


class Fetcher:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers["User-Agent"] = USER_AGENT
        self.last = 0.0

    def get(self, url: str) -> requests.Response:
        wait = DELAY_S - (time.monotonic() - self.last)
        if wait > 0:
            time.sleep(wait)
        response = self.session.get(url, timeout=60)
        self.last = time.monotonic()
        if response.status_code in (403, 429):
            raise Stop(f"HTTP {response.status_code} for {url}")
        return response


# ---------------------------------------------------------------------------------------------
# Images


def original_image_url(url: str) -> str | None:
    """Squarespace CDN address without resize parameters (that serves the original upload)."""
    if not url:
        return None
    url = url.strip()
    if url.startswith("//"):
        url = "https:" + url
    parts = urlsplit(url)
    if parts.netloc != CDN_HOST:
        return None
    return f"https://{parts.netloc}{parts.path.rstrip('/')}"


def image_file_name(url: str) -> str:
    """Readable, unique file name: original name plus a short hash of the address."""
    path = urlsplit(url).path
    name = unquote(path.rsplit("/", 1)[-1]) or "image"
    stem, dot, ext = name.rpartition(".")
    if not dot:
        stem, ext = name, "bin"
    stem = re.sub(r"[^A-Za-z0-9._-]+", "-", stem).strip("-")[:60] or "image"
    ext = re.sub(r"[^A-Za-z0-9]", "", ext).lower()[:5] or "bin"
    return f"{stem}-{hashlib.sha256(path.encode()).hexdigest()[:8]}.{ext}"


class Images:
    """Collects image addresses and where they're used; downloads them at the end."""

    def __init__(self) -> None:
        self.used_by: dict[str, set[str]] = {}

    def add(self, url: str | None, used_by: str) -> str | None:
        """Register an image; returns its file name in images/ (or None if not a CDN image)."""
        original = original_image_url(url or "")
        if not original:
            return None
        self.used_by.setdefault(original, set()).add(used_by)
        return image_file_name(original)

    def download(self, fetcher: Fetcher, folder: Path) -> tuple[int, int, list[str]]:
        folder.mkdir(parents=True, exist_ok=True)
        new = kept = 0
        failed: list[str] = []
        for url in sorted(self.used_by):
            target = folder / image_file_name(url)
            if target.exists() and target.stat().st_size > 0:
                kept += 1
                continue
            response = fetcher.get(url)
            if response.status_code != 200 or not response.content:
                failed.append(f"{url} (HTTP {response.status_code})")
                continue
            target.write_bytes(response.content)
            new += 1
        return new, kept, failed

    def write_csv(self, path: Path) -> None:
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(["url", "file", "used_by"])
            for url in sorted(self.used_by):
                writer.writerow([url, image_file_name(url), " | ".join(sorted(self.used_by[url]))])


def tag_image_url(img: Tag) -> str | None:
    # Squarespace lazy-loads: the real address is in data-src or data-image, src may be empty.
    return img.get("data-src") or img.get("data-image") or img.get("src")


# ---------------------------------------------------------------------------------------------
# HTML to Markdown (enough for Squarespace text, image and button blocks)


class Markdown:
    def __init__(self, images: Images, used_by: str) -> None:
        self.images = images
        self.used_by = used_by

    def convert(self, node: Tag) -> str:
        text = self._children(node)
        # Squarespace wraps blocks in many nested divs, which leaves runs of spaces at line
        # starts; four of them would make Markdown show the line as code. Keep indentation
        # only for list-item continuation lines, and keep "  " hard breaks at line ends.
        lines: list[str] = []
        in_list = False
        for line in text.splitlines():
            stripped = re.sub(r" {2,}", " ", line.strip())
            hard_break = "  " if line.endswith("  ") and stripped else ""
            if not stripped:
                in_list = False
                lines.append("")
            elif re.match(r"(- |\d+\. )", line):
                in_list = True
                lines.append(stripped + hard_break)
            elif in_list and line.startswith("   "):
                lines.append("   " + stripped + hard_break)
            else:
                in_list = False
                lines.append(stripped + hard_break)
        text = re.sub(r"\n{3,}", "\n\n", "\n".join(lines))
        return text.strip() + "\n"

    def _children(self, node: Tag) -> str:
        return "".join(self._node(child) for child in node.children)

    def _inline(self, node: Tag) -> str:
        return re.sub(r"\s+", " ", self._children(node)).strip()

    def _node(self, node) -> str:
        if isinstance(node, NavigableString):
            if node.__class__.__name__ in {"Comment", "Doctype", "Declaration"}:
                return ""
            return re.sub(r"\s+", " ", str(node).replace("\xa0", " "))
        if not isinstance(node, Tag):
            return ""
        name = node.name
        if name in {"script", "style", "noscript", "svg", "form", "button", "nav", "template"}:
            return ""
        if name in {"h1", "h2", "h3", "h4", "h5", "h6"}:
            text = self._inline(node)
            return f"\n\n{'#' * int(name[1])} {text}\n\n" if text else ""
        if name == "p":
            text = self._children(node).strip()
            return f"\n\n{text}\n\n" if text else ""
        if name == "br":
            return "  \n"
        if name == "hr":
            return "\n\n---\n\n"
        if name in {"strong", "b"}:
            text = self._inline(node)
            return f"**{text}**" if text else ""
        if name in {"em", "i"}:
            text = self._inline(node)
            return f"*{text}*" if text else ""
        if name == "code":
            return f"`{node.get_text()}`"
        if name == "a":
            text = self._inline(node)
            href = (node.get("href") or "").strip()
            if href.startswith("/"):
                href = SITE + href
            if not text:
                return ""
            return f"[{text}]({href})" if href and not href.startswith("javascript:") else text
        if name in {"ul", "ol"}:
            items = []
            for index, li in enumerate(node.find_all("li", recursive=False), start=1):
                marker = f"{index}." if name == "ol" else "-"
                body = self._children(li).strip().replace("\n\n", "\n").replace("\n", "\n   ")
                items.append(f"{marker} {body}")
            return "\n\n" + "\n".join(items) + "\n\n"
        if name == "blockquote":
            text = self._children(node).strip()
            return "\n\n" + "\n".join(f"> {line}" for line in text.splitlines()) + "\n\n"
        if name == "img":
            file_name = self.images.add(tag_image_url(node), self.used_by)
            if not file_name:
                return ""
            alt = (node.get("alt") or "").replace("]", "").strip()
            return f"\n\n![{alt}](../images/{file_name})\n\n"
        if name == "iframe":
            src = node.get("src") or node.get("data-src") or ""
            return f"\n\n[Embedded content]({src})\n\n" if src else ""
        if name == "figcaption":
            text = self._inline(node)
            return f"\n\n*{text}*\n\n" if text else ""
        return self._children(node)


# ---------------------------------------------------------------------------------------------
# Site walk


def sitemap_paths(xml: str) -> list[str]:
    paths = []
    for loc in re.findall(r"<loc>([^<]+)</loc>", xml):
        path = urlsplit(loc.strip()).path or "/"
        if path not in paths:
            paths.append(path)
    return paths


def path_to_slug(path: str) -> str:
    slug = path.strip("/").replace("/", "__")
    return slug or "home"


def page_title(soup: BeautifulSoup) -> str:
    title = soup.find("title")
    return title.get_text(strip=True) if title else ""


def page_main(soup: BeautifulSoup) -> Tag | None:
    """The page body without the site header and footer."""
    main = soup.find("main") or soup.find(id="sections") or soup.body
    if main is None:
        return None
    for unwanted in main.select("header, footer, .header, #footer-sections"):
        unwanted.decompose()
    return main


def ms_to_date(value) -> str:
    try:
        return dt.datetime.fromtimestamp(int(value) / 1000, tz=dt.timezone.utc).date().isoformat()
    except (TypeError, ValueError):
        return ""


def yaml_list(values) -> str:
    return "[" + ", ".join(json.dumps(v, ensure_ascii=False) for v in values or []) + "]"


def save_posts(fetcher: Fetcher, out: Path, images: Images, index: list[dict]) -> int:
    """Fetch every blog post through the blog's JSON feed (follows pagination)."""
    folder = out / "posts"
    folder.mkdir(parents=True, exist_ok=True)
    url = f"{SITE}{BLOG_PATH}?format=json"
    count = 0
    while url:
        data = fetcher.get(url).json()
        for item in data.get("items") or []:
            slug = item.get("urlId") or item.get("id")
            date = ms_to_date(item.get("publishOn") or item.get("addedOn"))
            used_by = f"post:{slug}"
            cover = images.add(item.get("assetUrl"), used_by)
            body_html = item.get("body") or ""
            body = Markdown(images, used_by).convert(BeautifulSoup(body_html, "html.parser"))
            seo = item.get("seoData") or {}
            author = (item.get("author") or {}).get("displayName", "")
            front = [
                "---",
                f"title: {json.dumps(item.get('title') or '', ensure_ascii=False)}",
                f"date: {date}",
                f"old_url: {SITE}{item.get('fullUrl') or BLOG_PATH + '/' + slug}",
                f"author: {json.dumps(author, ensure_ascii=False)}",
                f"categories: {yaml_list(item.get('categories'))}",
                f"tags: {yaml_list(item.get('tags'))}",
                f"cover_image: {('../images/' + cover) if cover else ''}",
                "excerpt: " + json.dumps(
                    BeautifulSoup(item.get("excerpt") or "", "html.parser").get_text(" ", strip=True),
                    ensure_ascii=False),
                f"seo_title: {json.dumps(seo.get('seoTitle') or '', ensure_ascii=False)}",
                f"seo_description: {json.dumps(seo.get('seoDescription') or '', ensure_ascii=False)}",
                "---",
                "",
            ]
            md_name = f"{date}-{slug}.md"
            (folder / md_name).write_text("\n".join(front) + body, encoding="utf-8")
            (folder / f"{slug}.json").write_text(
                json.dumps(item, indent=2, ensure_ascii=False), encoding="utf-8")
            index.append({"path": item.get("fullUrl") or "", "kind": "post",
                          "title": item.get("title") or "", "markdown": f"posts/{md_name}",
                          "raw": f"posts/{slug}.json"})
            count += 1
        pagination = data.get("pagination") or {}
        next_url = pagination.get("nextPageUrl") if pagination.get("nextPage") else None
        url = f"{SITE}{next_url}&format=json" if next_url else None
    return count


def save_html_pages(fetcher: Fetcher, out: Path, paths: list[str], images: Images,
                    index: list[dict], skip_paths: set[str]) -> int:
    count = 0
    for path in paths:
        if path in skip_paths or path.startswith(SKIP_PREFIXES):
            continue
        response = fetcher.get(SITE + path)
        if response.status_code != 200:
            index.append({"path": path, "kind": f"error {response.status_code}", "title": "",
                          "markdown": "", "raw": ""})
            continue
        soup = BeautifulSoup(response.text, "html.parser")
        title = page_title(soup)
        slug = path_to_slug(path)
        is_shop = path == "/shop" or path.startswith("/shop/")
        folder = out / ("shop" if is_shop else "pages")
        folder.mkdir(parents=True, exist_ok=True)
        (folder / f"{slug}.html").write_text(response.text, encoding="utf-8")
        used_by = f"page:{path}"
        main = page_main(soup)
        markdown_file = ""
        if main is not None:
            if is_shop:
                # Record the images only; product text is in the products export.
                for img in main.find_all("img"):
                    images.add(tag_image_url(img), used_by)
            else:
                body = Markdown(images, used_by).convert(main)
                front = f"---\ntitle: {json.dumps(title, ensure_ascii=False)}\nold_url: {SITE}{path}\n---\n\n"
                (folder / f"{slug}.md").write_text(front + body, encoding="utf-8")
                markdown_file = f"pages/{slug}.md"
        # Social card image set in the page settings.
        og = soup.find("meta", attrs={"property": "og:image"})
        if og:
            images.add(og.get("content"), used_by + " (social card)")
        index.append({"path": path, "kind": "shop" if is_shop else "page", "title": title,
                      "markdown": markdown_file,
                      "raw": f"{'shop' if is_shop else 'pages'}/{slug}.html"})
        count += 1
    return count


def add_product_images(products_csv: Path, images: Images) -> int:
    with products_csv.open(encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    count = 0
    for row in rows:
        label = f"product:{row.get('SKU') or row.get('Product URL') or row.get('Title')}"
        for url in (row.get("Hosted Image URLs") or "").split():
            if images.add(url, label):
                count += 1
    return count


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--out", required=True, type=Path, help="folder to write the copy to")
    parser.add_argument("--products-csv", type=Path,
                        help="Squarespace products export, for product photo addresses")
    parser.add_argument("--no-images", action="store_true", help="skip downloading images")
    args = parser.parse_args(argv)

    out: Path = args.out
    out.mkdir(parents=True, exist_ok=True)
    fetcher = Fetcher()
    images = Images()
    index: list[dict] = []
    try:
        sitemap = fetcher.get(f"{SITE}/sitemap.xml").text
        (out / "sitemap.xml").write_text(sitemap, encoding="utf-8")
        paths = sitemap_paths(sitemap)
        posts = save_posts(fetcher, out, images, index)
        post_paths = {row["path"] for row in index if row["kind"] == "post"}
        pages = save_html_pages(fetcher, out, paths, images, index, post_paths)
        product_refs = add_product_images(args.products_csv, images) if args.products_csv else 0
        new = kept = 0
        failed: list[str] = []
        if not args.no_images:
            new, kept, failed = images.download(fetcher, out / "images")
    except Stop as stop:
        print(f"Stopped: {stop}. Nothing more fetched; re-run later.", file=sys.stderr)
        return 2

    images.write_csv(out / "images.csv")
    with (out / "index.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["path", "kind", "title", "markdown", "raw"])
        writer.writeheader()
        writer.writerows(index)
    fetched = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    (out / "README.md").write_text(
        f"# Copy of {SITE} (Squarespace)\n\n"
        f"Fetched {fetched} by `storefront/tools/archive_squarespace_site.py` in the "
        "pokemon-momentum repo.\n\n"
        f"- {posts} blog posts (Resources) in `posts/`: Markdown with front matter, plus "
        "Squarespace's JSON.\n"
        f"- {pages} pages in `pages/` (Markdown + HTML) and `shop/` (HTML only; product data "
        "is in the products export).\n"
        f"- {len(images.used_by)} images in `images/` at original size; `images.csv` says where "
        "each was used.\n"
        "- `index.csv` lists every saved address.\n\n"
        "Blog tag and category listing pages were not saved: they only list the posts, and each "
        "post's front matter keeps its tags and categories.\n",
        encoding="utf-8")
    print(f"posts {posts}, pages {pages}, product image references {product_refs}, "
          f"images {len(images.used_by)} ({new} new, {kept} already saved, {len(failed)} failed)")
    for line in failed:
        print("  failed:", line)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
