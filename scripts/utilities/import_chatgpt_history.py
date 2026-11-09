"""Copy this project's ChatGPT conversations into docs/chat_history/chatgpt/.

Source: a ChatGPT data export already converted to one Markdown file per
conversation (YYYY/YYYY-MM-DD - Title - <id>.md, with YAML front matter). Pass that folder
as the only argument:
    python3 scripts/utilities/import_chatgpt_history.py <converted export folder>

Which conversations belong to the project is decided by hand in a selection
file, one tab-separated line per conversation:
    <category>\t<path relative to the source folder>\t<title>
where category is "website", "business" (Poke6s store, selling, content) or
"exclude". The export holds every personal chat too, so the selection keeps
those out. It lives at docs/chat_history/chatgpt/selection.tsv, which is
git-ignored with the rest of the folder because the titles alone say a lot
about the business.

Secrets from .env are redacted the same way as the Codex export.

    python3 scripts/utilities/import_chatgpt_history.py [SOURCE_DIR]
"""

from __future__ import annotations

import sys
from pathlib import Path

from export_codex_history import REPO, load_env_secrets, redact

OUT_DIR = REPO / "docs" / "chat_history" / "chatgpt"
SELECTION = OUT_DIR / "selection.tsv"


def main() -> None:
    if len(sys.argv) != 2:
        sys.exit("Usage: import_chatgpt_history.py <converted ChatGPT export folder>")
    source = Path(sys.argv[1])
    secrets = load_env_secrets()
    index = ["# ChatGPT chat history", "",
             "Pre-Codex conversations (2025 to mid-2026) from the ChatGPT export.", "",
             "| Date | Category | Conversation | Size |", "|---|---|---|---|"]
    copied = 0
    for line in SELECTION.read_text().splitlines():
        category, rel, title = line.split("\t")
        if category == "exclude":
            continue
        src = source / rel
        if not src.exists():
            print(f"missing: {rel}")
            continue
        dest_dir = OUT_DIR / category
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest = dest_dir / src.name
        text = src.read_text(errors="ignore")
        # Tag the front matter so agents can filter by category.
        if text.startswith("---\n"):
            text = text.replace("---\n", f"---\ncategory: {category}\n", 1)
        dest.write_text(redact(text, secrets))
        copied += 1
        rel_out = f"{category}/{src.name}"
        index.append(f"| {src.name[:10]} | {category} | [{title}]({rel_out.replace(' ', '%20')}) "
                     f"| {len(text) // 1000}k |")
    (OUT_DIR / "README.md").write_text("\n".join(index) + "\n")
    print(f"copied {copied} conversations to {OUT_DIR}")


if __name__ == "__main__":
    main()
