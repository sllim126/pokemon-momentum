#!/usr/bin/env python3
"""Best-effort text extraction for simple compressed PDF streams.

This is intentionally small and dependency-free; it is only meant to inspect
card-list PDFs when a full PDF parser is not available.
"""

from __future__ import annotations

import re
import sys
import zlib
from pathlib import Path


def unescape_pdf_string(value: bytes) -> str:
    out = bytearray()
    i = 0
    while i < len(value):
        ch = value[i]
        if ch == 0x5C and i + 1 < len(value):
            nxt = value[i + 1]
            if nxt in b"nrtbf":
                out.append({ord("n"): 10, ord("r"): 13, ord("t"): 9, ord("b"): 8, ord("f"): 12}[nxt])
                i += 2
                continue
            if nxt in b"()\\":
                out.append(nxt)
                i += 2
                continue
            octal = re.match(rb"[0-7]{1,3}", value[i + 1 : i + 4])
            if octal:
                out.append(int(octal.group(0), 8))
                i += 1 + len(octal.group(0))
                continue
        out.append(ch)
        i += 1
    return out.decode("utf-8", errors="replace")


def extract_streams(data: bytes) -> list[bytes]:
    streams: list[bytes] = []
    for match in re.finditer(rb"stream\r?\n(.*?)\r?\nendstream", data, re.S):
        stream = match.group(1)
        prefix = data[max(0, match.start() - 300) : match.start()]
        if b"FlateDecode" in prefix:
            try:
                stream = zlib.decompress(stream)
            except zlib.error:
                continue
        streams.append(stream)
    return streams


def extract_text(stream: bytes) -> list[str]:
    parts: list[str] = []
    for array in re.finditer(rb"\[(.*?)\]\s*TJ", stream, re.S):
        strings = re.findall(rb"\(((?:\\.|[^\\)])*)\)", array.group(1), re.S)
        if strings:
            parts.append("".join(unescape_pdf_string(item) for item in strings))
    for string in re.finditer(rb"\(((?:\\.|[^\\)])*)\)\s*Tj", stream, re.S):
        parts.append(unescape_pdf_string(string.group(1)))
    for hex_string in re.finditer(rb"<([0-9A-Fa-f]{4,})>\s*Tj", stream):
        raw = bytes.fromhex(hex_string.group(1).decode("ascii"))
        for encoding in ("utf-16-be", "utf-8", "latin-1"):
            try:
                parts.append(raw.decode(encoding).strip("\x00"))
                break
            except UnicodeDecodeError:
                pass
    return [part for part in parts if part.strip()]


def main() -> int:
    if len(sys.argv) != 2:
        print("Usage: extract_pdf_text.py PDF", file=sys.stderr)
        return 2
    data = Path(sys.argv[1]).read_bytes()
    for stream in extract_streams(data):
        for text in extract_text(stream):
            print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
