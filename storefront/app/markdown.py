"""Markdown rendering for pages, product descriptions and (later) articles.

Raw HTML in the Markdown is escaped, not passed through, so a pasted description can't
inject scripts into a page. Page files may start with a simple front-matter block:

    ---
    title: Shipping
    description: How and when we ship.
    ---
"""

from __future__ import annotations

import re
from html.parser import HTMLParser

from markdown_it import MarkdownIt

_md = MarkdownIt("commonmark", {"html": False, "linkify": False, "typographer": False})


def render(text: str) -> str:
    return _md.render(text or "")


# Squarespace's bold sometimes ends with a space inside it ("**Card Text: **Each ..."), which
# Markdown won't render as bold. Only an opening "**" (at the start of a line or after a space
# or "(") followed by text that starts with a non-space counts, so the gap between two good
# bolds ("**ex**, **Trainer cards**") is never mistaken for one.
_BOLD_INNER_SPACE = re.compile(r"(?<![^\s(])\*\*(\S(?:[^*\n]*?\S)?)\s+\*\*(?=\w)")
# A heading at the very top that only says "Details" or "Product Details": the product page
# already puts a "Details" heading above the description.
_LEADING_DETAILS_HEADING = re.compile(r"\A\s*#{1,6}[ \t]*(?:\*\*)?(?:product[ \t]+)?details:?(?:\*\*)?[ \t]*(?:\n|\Z)", re.I)


def render_description(text: str) -> str:
    """Render a product description, tidying two leftovers from the Squarespace import."""
    text = _BOLD_INNER_SPACE.sub(r"**\1** ", text or "")
    return render(_LEADING_DETAILS_HEADING.sub("", text, count=1))


def split_front_matter(text: str) -> tuple[dict[str, str], str]:
    """Return ({key: value}, body). Only flat `key: value` lines are supported."""
    if not text.startswith("---"):
        return {}, text
    lines = text.splitlines()
    meta: dict[str, str] = {}
    for index, line in enumerate(lines[1:], start=1):
        if line.strip() == "---":
            return meta, "\n".join(lines[index + 1:]).lstrip("\n")
        if ":" in line:
            key, value = line.split(":", 1)
            meta[key.strip()] = value.strip()
    return {}, text  # no closing line: treat the whole file as body


class _HtmlToMarkdown(HTMLParser):
    """Turn Squarespace's description HTML (p, br, strong, em, ul/ol/li, a, h1-h4) into Markdown.

    Anything else is reduced to its text. Good enough for product descriptions; not a
    general converter.
    """

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.out: list[str] = []
        self.list_stack: list[str] = []
        self.href: str | None = None

    def handle_starttag(self, tag, attrs):
        if tag in {"strong", "b"}:
            self.out.append("**")
        elif tag in {"em", "i"}:
            self.out.append("*")
        elif tag == "br":
            self.out.append("  \n")
        elif tag in {"ul", "ol"}:
            self.list_stack.append(tag)
            self.out.append("\n")
        elif tag == "li":
            self.out.append("\n- " if not self.list_stack or self.list_stack[-1] == "ul" else "\n1. ")
        elif tag in {"h1", "h2", "h3", "h4"}:
            self.out.append("\n\n### ")
        elif tag == "a":
            self.href = dict(attrs).get("href")
            if self.href:
                self.out.append("[")

    def handle_endtag(self, tag):
        if tag in {"strong", "b"}:
            self.out.append("**")
        elif tag in {"em", "i"}:
            self.out.append("*")
        elif tag in {"p", "div", "h1", "h2", "h3", "h4"}:
            self.out.append("\n\n")
        elif tag in {"ul", "ol"}:
            if self.list_stack:
                self.list_stack.pop()
            self.out.append("\n\n")
        elif tag == "a" and self.href:
            self.out.append(f"]({self.href})")
            self.href = None

    def handle_data(self, data):
        self.out.append(data.replace("\xa0", " "))


def html_to_markdown(html: str) -> str:
    if not html or "<" not in html:
        return (html or "").strip()
    parser = _HtmlToMarkdown()
    parser.feed(html)
    parser.close()
    text = "".join(parser.out)
    # Tidy: no trailing spaces except hard breaks, at most one blank line in a row,
    # and no empty emphasis left over from empty <strong></strong> tags.
    text = text.replace("****", "")
    lines = [line if line.endswith("  ") else line.rstrip() for line in text.splitlines()]
    tidy: list[str] = []
    for line in lines:
        if not line.strip() and tidy and not tidy[-1].strip():
            continue
        tidy.append(line)
    return "\n".join(tidy).strip()
