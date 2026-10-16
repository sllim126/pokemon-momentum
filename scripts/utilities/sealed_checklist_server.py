"""A small local page for filling in data/sealed_pack_checklist.csv one product at a time.

The checklist lists sealed products whose pack counts are guesses (see the CSV's `notes`
column for what to check). This page shows one row at a time with links to research it,
and a box for the real pack count plus your own notes. "Save & next" writes the answer
straight into the CSV (columns `your_pack_count` and `your_notes`) and shows the next row.

It is a separate tool, not part of the website: it runs on the host, uses only the
standard library, and changes nothing but the checklist CSV. Nothing is sent anywhere.

    python3 scripts/utilities/sealed_checklist_server.py            # http://localhost:8099
    python3 scripts/utilities/sealed_checklist_server.py --port 8100

It listens on all addresses so another computer on the home network can open
http://<this server's LAN address>:8099; the public site's proxy doesn't forward to it.
Stop it with Ctrl+C. Applying the answers to the pack-count rules in api.py is a
separate step, done afterwards.
"""

import argparse
import csv
import html
import json
import os
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CSV_PATH = ROOT / "data" / "sealed_pack_checklist.csv"
ANSWER_COLUMNS = ["your_pack_count", "your_notes"]
LOCK = threading.Lock()


def read_rows() -> tuple[list[str], list[dict]]:
    with CSV_PATH.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        fields = list(reader.fieldnames or [])
        rows = list(reader)
    for column in ANSWER_COLUMNS:
        if column not in fields:
            fields.append(column)
    return fields, rows


def write_rows(fields: list[str], rows: list[dict]) -> None:
    """Write via a temp file and rename, so a crash never leaves half a CSV."""
    fd, tmp = tempfile.mkstemp(dir=CSV_PATH.parent, prefix=".sealed_checklist_", suffix=".csv")
    with os.fdopen(fd, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows({f: row.get(f, "") for f in fields} for row in rows)
    os.chmod(tmp, 0o664)
    os.replace(tmp, CSV_PATH)


PAGE = """<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Sealed Pack Checklist</title>
<style>
 :root { --bg:#f6f7f9; --card:#fff; --ink:#1d2330; --muted:#667085; --line:#d9dee7; --accent:#2f6fde; --ok:#1f8a4c; }
 @media (prefers-color-scheme: dark) { :root { --bg:#14171c; --card:#1d2129; --ink:#e7eaf0; --muted:#9aa3b2; --line:#323846; --accent:#6c9cff; --ok:#4cc27e; } }
 * { box-sizing:border-box; }
 body { margin:0; background:var(--bg); color:var(--ink); font:16px/1.45 system-ui, sans-serif; }
 main { max-width:760px; margin:24px auto; padding:0 16px; }
 .bar { display:flex; justify-content:space-between; align-items:center; gap:12px; color:var(--muted); font-size:14px; }
 .progress { height:6px; background:var(--line); border-radius:3px; margin:8px 0 16px; overflow:hidden; }
 .progress div { height:100%; background:var(--ok); }
 .card { background:var(--card); border:1px solid var(--line); border-radius:10px; padding:20px; display:grid; grid-template-columns:150px 1fr; gap:20px; }
 .card img { width:150px; border-radius:6px; background:var(--line); min-height:150px; object-fit:contain; }
 h1 { font-size:21px; margin:0 0 4px; }
 .meta { color:var(--muted); font-size:14px; margin:0 0 12px; }
 .question { background:var(--bg); border-left:3px solid var(--accent); padding:8px 12px; border-radius:4px; margin:0 0 12px; }
 .links a { color:var(--accent); margin-right:14px; font-size:14px; }
 form { margin-top:16px; display:grid; gap:10px; }
 label { font-size:14px; color:var(--muted); display:grid; gap:4px; }
 input, textarea { font:inherit; color:var(--ink); background:var(--card); border:1px solid var(--line); border-radius:6px; padding:8px 10px; }
 input { width:160px; }
 textarea { min-height:90px; resize:vertical; }
 .buttons { display:flex; gap:8px; flex-wrap:wrap; }
 button { font:inherit; border:1px solid var(--line); background:var(--card); color:var(--ink); border-radius:6px; padding:8px 14px; cursor:pointer; }
 button.primary { background:var(--accent); border-color:var(--accent); color:#fff; }
 .saved { color:var(--ok); font-size:14px; min-height:20px; }
 .done { color:var(--ok); font-weight:600; }
 @media (max-width:560px) { .card { grid-template-columns:1fr; } }
</style></head>
<body><main>
 <div class="bar"><span id="pos"></span><span id="count"></span></div>
 <div class="progress"><div id="fill"></div></div>
 <div class="card">
  <img id="img" alt="">
  <div>
   <p class="meta" id="section"></p>
   <h1 id="product"></h1>
   <p class="meta" id="details"></p>
   <p class="question" id="question"></p>
   <div class="links" id="links"></div>
   <form id="form">
    <label>Number of packs <input id="packs" inputmode="decimal" autocomplete="off" placeholder="e.g. 10"></label>
    <label>Notes <textarea id="notes" placeholder="Where you found it, what's inside, anything odd"></textarea></label>
    <div class="buttons">
     <button type="button" id="back">← Back</button>
     <button type="submit" class="primary">Save &amp; next →</button>
     <button type="button" id="skip">Skip</button>
     <button type="button" id="todo">Next unanswered</button>
    </div>
    <div class="saved" id="saved"></div>
   </form>
  </div>
 </div>
</main>
<script>
const $ = (id) => document.getElementById(id);
let rows = [], i = 0;

function answered(r) { return (r.your_pack_count || "").trim() !== ""; }
function nextUnanswered(from) {
  for (let k = 1; k <= rows.length; k++) { const j = (from + k) % rows.length; if (!answered(rows[j])) return j; }
  return -1;
}
function show(index, note) {
  i = Math.max(0, Math.min(rows.length - 1, index));
  const r = rows[i], done = rows.filter(answered).length;
  $("pos").textContent = `Item ${i + 1} of ${rows.length}`;
  $("count").innerHTML = done === rows.length ? '<span class="done">All answered</span>' : `${done} answered`;
  $("fill").style.width = `${(100 * done / rows.length).toFixed(1)}%`;
  $("section").textContent = `Section ${r.section}`;
  $("product").textContent = r.product;
  $("details").textContent = `${r.set} · counted now as ${r.counted_as} packs · $${r.price} ($${r.price_per_pack} a pack)`;
  $("question").textContent = r.notes || "Check how many packs are inside.";
  $("img").src = `https://tcgplayer-cdn.tcgplayer.com/product/${encodeURIComponent(r.product_id)}_200w.jpg`;
  const q = encodeURIComponent(r.product);
  $("links").innerHTML = "";
  [["TCGplayer", `https://www.tcgplayer.com/product/${encodeURIComponent(r.product_id)}`],
   ["Google", `https://www.google.com/search?q=${q}+how+many+packs`],
   ["Bulbapedia", `https://bulbapedia.bulbagarden.net/w/index.php?search=${q}`],
   ["PriceCharting", `https://www.pricecharting.com/search-products?q=${q}&type=prices`]]
    .forEach(([text, href]) => { const a = document.createElement("a"); a.textContent = text + " ↗"; a.href = href; a.target = "_blank"; a.rel = "noopener"; $("links").append(a); });
  $("packs").value = r.your_pack_count || "";
  $("notes").value = r.your_notes || "";
  $("saved").textContent = note || "";
  history.replaceState(null, "", `#${i + 1}`);
  $("packs").focus();
}
async function save() {
  const r = rows[i], packs = $("packs").value.trim(), notes = $("notes").value.trim();
  if (packs === (r.your_pack_count || "") && notes === (r.your_notes || "")) return true;
  const res = await fetch("/save", { method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ product_id: r.product_id, your_pack_count: packs, your_notes: notes }) });
  if (!res.ok) { $("saved").textContent = "Couldn't save: " + (await res.text()); return false; }
  r.your_pack_count = packs; r.your_notes = notes;
  return true;
}
$("form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const name = rows[i].product;
  if (await save()) {
    if (i === rows.length - 1) show(i, "Saved. That was the last item.");
    else show(i + 1, `Saved: ${name}`);
  }
});
$("back").onclick = async () => { if (await save()) show(i - 1); };
$("skip").onclick = () => show(i + 1);
$("todo").onclick = async () => { if (await save()) { const j = nextUnanswered(i); show(j < 0 ? i : j, j < 0 ? "Everything has an answer." : ""); } };
fetch("/rows").then((r) => r.json()).then((data) => {
  rows = data;
  const fromHash = parseInt(location.hash.slice(1), 10);
  const first = rows.findIndex((r) => !answered(r));
  show(fromHash > 0 ? fromHash - 1 : (first < 0 ? 0 : first));
});
</script></body></html>
"""


class Handler(BaseHTTPRequestHandler):
    def send(self, status: int, body: str, kind: str = "text/plain; charset=utf-8") -> None:
        data = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", kind)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:
        if self.path in ("/", "/index.html") or self.path.startswith("/#"):
            self.send(200, PAGE, "text/html; charset=utf-8")
        elif self.path == "/rows":
            with LOCK:
                _, rows = read_rows()
            self.send(200, json.dumps(rows), "application/json")
        else:
            self.send(404, "Not found")

    def do_POST(self) -> None:
        if self.path != "/save":
            return self.send(404, "Not found")
        try:
            length = min(int(self.headers.get("Content-Length", "0")), 20_000)
            answer = json.loads(self.rfile.read(length))
            product_id = str(answer["product_id"])
        except (ValueError, KeyError, TypeError):
            return self.send(400, "Bad request")
        with LOCK:
            fields, rows = read_rows()
            matches = [row for row in rows if row.get("product_id") == product_id]
            if not matches:
                return self.send(404, f"product {html.escape(product_id)} isn't in the checklist")
            for row in matches:
                for column in ANSWER_COLUMNS:
                    row[column] = str(answer.get(column, "")).strip()[:2000]
            write_rows(fields, rows)
        self.send(200, "ok")

    def log_message(self, fmt: str, *args) -> None:  # keep the terminal quiet except for saves
        if self.command == "POST":
            print(f"saved answer ({self.address_string()})")


def main() -> None:
    parser = argparse.ArgumentParser(description="Fill in the sealed pack checklist one item at a time.")
    parser.add_argument("--port", type=int, default=8099)
    args = parser.parse_args()
    server = ThreadingHTTPServer(("0.0.0.0", args.port), Handler)
    print(f"Sealed pack checklist: http://localhost:{args.port}  (Ctrl+C to stop)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
