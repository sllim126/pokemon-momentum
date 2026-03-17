import json
import math
import pandas as pd
from pathlib import Path

# ---------------- CONFIG ----------------
DATA_DIR = Path("/app/data/extracted")
TIMESERIES_CSV = DATA_DIR / "top200_timeseries.csv"
ROC_SNAPSHOT_CSV = DATA_DIR / "roc_snapshot_7_30_90.csv"
LOOKUP_CSV = DATA_DIR / "top200_lookup.csv"
GROUPS_CSV = DATA_DIR / "pokemon_groups.csv"  # optional
OUT_HTML = Path("/app/output/roc_dashboard_v3.html")

COL_DATE = "date"
COL_PID = "productId"
COL_SUB = "subTypeName"
COL_PRICE = "price"
COL_SMA7 = "sma_7"
COL_SMA30 = "sma_30"
# ----------------------------------------

def require_file(p: Path):
    if not p.exists():
        raise FileNotFoundError(f"Missing file: {p}")

def require_cols(df: pd.DataFrame, cols, label: str):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise KeyError(f"{label} missing columns: {missing}. Found: {df.columns.tolist()}")

def clean_for_json(x):
    if x is None:
        return None
    if isinstance(x, float) and (math.isnan(x) or math.isinf(x)):
        return None
    return x

def safe_json_for_html(obj):
    s = json.dumps(obj, ensure_ascii=False, allow_nan=False)
    return s.replace("</", "<\\/")

def make_key(pid, sub):
    return f"{pid}||{sub}"

def is_sealed_name(name: str) -> bool:
    n = (name or "").lower()
    sealed_words = [
        "booster box", "booster pack", "elite trainer box", "etb",
        "collection", "tin", "bundle", "blister", "box", "display",
        "premium", "set", "deck", "starter", "theme deck", "sleeves",
    ]
    # This is intentionally simple. You can refine later.
    return any(w in n for w in sealed_words)

def parse_number_fraction(num_str: str):
    # Returns (numerator, denominator) if "125/094" style, else (None, None)
    if not isinstance(num_str, str):
        return (None, None)
    s = num_str.strip()
    if "/" not in s:
        return (None, None)
    a, b = s.split("/", 1)
    a = a.strip()
    b = b.strip()
    try:
        return (int(a), int(b))
    except Exception:
        return (None, None)

print("Checking files...")
require_file(TIMESERIES_CSV)
require_file(ROC_SNAPSHOT_CSV)
require_file(LOOKUP_CSV)


print("Loading timeseries...")
ts = pd.read_csv(TIMESERIES_CSV)
require_cols(ts, [COL_PID, COL_SUB, COL_DATE, COL_PRICE, COL_SMA7, COL_SMA30], "Timeseries")
ts[COL_DATE] = ts[COL_DATE].astype(str)
ts["key"] = ts.apply(lambda r: make_key(r[COL_PID], r[COL_SUB]), axis=1)

print("Loading ROC snapshot...")
roc = pd.read_csv(ROC_SNAPSHOT_CSV)
require_cols(roc, ["productId", "subTypeName", "roc_7d_pct", "roc_30d_pct", "roc_90d_pct", "price_now"], "ROC snapshot")
roc["key"] = roc.apply(lambda r: make_key(r["productId"], r["subTypeName"]), axis=1)

print("Loading lookup...")
lk = pd.read_csv(LOOKUP_CSV)
require_cols(lk, ["productId"], "Lookup")

# Ensure lookup columns exist
for col in ["productName", "imageUrl", "groupId", "rarity", "number", "name"]:
    if col not in lk.columns:
        lk[col] = ""

# Prefer productName, fallback to name
if lk["productName"].astype(str).str.len().sum() == 0 and "name" in lk.columns:
    lk["productName"] = lk["name"]

# Group name map (optional)
group_name_map = {}
if GROUPS_CSV.exists():
    try:
        g = pd.read_csv(GROUPS_CSV)
        if "groupId" in g.columns and "name" in g.columns:
            group_name_map = dict(zip(g["groupId"].astype(str), g["name"].astype(str)))
            print("Loaded group names:", len(group_name_map))
    except Exception as e:
        print("Warning: could not load group names:", e)

# Merge lookup into roc
roc = roc.merge(
    lk[["productId", "groupId", "productName", "imageUrl", "rarity", "number"]],
    on="productId",
    how="left"
)
roc["groupId"] = roc["groupId"].astype("Int64", errors="ignore").astype(str)
roc["groupName"] = roc["groupId"].map(lambda x: group_name_map.get(x, ""))

# Derived fields for screener
roc["accel_7v30"] = roc["roc_7d_pct"] - roc["roc_30d_pct"]
roc["sealedFlag"] = roc["productName"].map(lambda x: is_sealed_name(str(x)))

def secret_flag(num):
    a, b = parse_number_fraction(str(num))
    if a is None or b is None:
        return False
    return a > b

roc["secretFlag"] = roc["number"].map(secret_flag)

# Build series map for chart rendering
series_map = {}
ts_keys = ts["key"].unique().tolist()

roc_by_key = {row["key"]: row for _, row in roc.iterrows()}

for k in ts_keys:
    chunk = ts[ts["key"] == k].sort_values(COL_DATE)
    if chunk.empty:
        continue

    rr = roc_by_key.get(k, None)
    if rr is None:
        pid, sub = k.split("||", 1)
        meta = {
            "productId": int(pid),
            "subTypeName": sub,
            "productName": "",
            "groupId": "",
            "groupName": "",
            "imageUrl": "",
            "rarity": "",
            "number": "",
            "roc7": None,
            "roc30": None,
            "roc90": None,
            "accel": None,
            "sealed": False,
            "secret": False,
        }
        label = k
    else:
        meta = {
            "productId": int(rr["productId"]),
            "subTypeName": "" if pd.isna(rr["subTypeName"]) else str(rr["subTypeName"]),
            "productName": "" if pd.isna(rr["productName"]) else str(rr["productName"]),
            "groupId": "" if pd.isna(rr["groupId"]) else str(rr["groupId"]),
            "groupName": "" if pd.isna(rr["groupName"]) else str(rr["groupName"]),
            "imageUrl": "" if pd.isna(rr["imageUrl"]) else str(rr["imageUrl"]),
            "rarity": "" if pd.isna(rr["rarity"]) else str(rr["rarity"]),
            "number": "" if pd.isna(rr["number"]) else str(rr["number"]),
            "roc7": clean_for_json(float(rr["roc_7d_pct"])) if pd.notna(rr["roc_7d_pct"]) else None,
            "roc30": clean_for_json(float(rr["roc_30d_pct"])) if pd.notna(rr["roc_30d_pct"]) else None,
            "roc90": clean_for_json(float(rr["roc_90d_pct"])) if pd.notna(rr["roc_90d_pct"]) else None,
            "accel": clean_for_json(float(rr["accel_7v30"])) if pd.notna(rr["accel_7v30"]) else None,
            "sealed": bool(rr["sealedFlag"]) if pd.notna(rr["sealedFlag"]) else False,
            "secret": bool(rr["secretFlag"]) if pd.notna(rr["secretFlag"]) else False,
        }
        label = (meta["productName"] + " | " + meta["subTypeName"]).strip(" |") or k

    series_map[k] = {
        "label": label,
        **meta,
        "d": chunk[COL_DATE].tolist(),
        "price": [clean_for_json(v) for v in chunk[COL_PRICE].tolist()],
        "sma7":  [clean_for_json(v) for v in chunk[COL_SMA7].tolist()],
        "sma30": [clean_for_json(v) for v in chunk[COL_SMA30].tolist()],
    }

if not series_map:
    raise RuntimeError("No chart series built. Check your timeseries file.")

# Screener rows based on roc snapshot but only include keys that exist in series_map
rows = []
for _, r in roc.iterrows():
    k = r["key"]
    if k not in series_map:
        continue
    rows.append({
        "key": k,
        "productId": int(r["productId"]),
        "subTypeName": "" if pd.isna(r["subTypeName"]) else str(r["subTypeName"]),
        "productName": "" if pd.isna(r["productName"]) else str(r["productName"]),
        "groupId": "" if pd.isna(r["groupId"]) else str(r["groupId"]),
        "groupName": "" if pd.isna(r["groupName"]) else str(r["groupName"]),
        "rarity": "" if pd.isna(r["rarity"]) else str(r["rarity"]),
        "number": "" if pd.isna(r["number"]) else str(r["number"]),
        "priceNow": clean_for_json(float(r["price_now"])) if pd.notna(r["price_now"]) else None,
        "roc7": clean_for_json(float(r["roc_7d_pct"])) if pd.notna(r["roc_7d_pct"]) else None,
        "roc30": clean_for_json(float(r["roc_30d_pct"])) if pd.notna(r["roc_30d_pct"]) else None,
        "roc90": clean_for_json(float(r["roc_90d_pct"])) if pd.notna(r["roc_90d_pct"]) else None,
        "accel": clean_for_json(float(r["accel_7v30"])) if pd.notna(r["accel_7v30"]) else None,
        "sealed": bool(r["sealedFlag"]) if pd.notna(r["sealedFlag"]) else False,
        "secret": bool(r["secretFlag"]) if pd.notna(r["secretFlag"]) else False,
        "imageUrl": "" if pd.isna(r["imageUrl"]) else str(r["imageUrl"]),
    })

# Default selection: best 30d ROC
rows_sorted = sorted([x for x in rows if x["roc30"] is not None], key=lambda x: x["roc30"], reverse=True)
default_key = rows_sorted[0]["key"] if rows_sorted else list(series_map.keys())[0]

html = """<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Pokemon Momentum Screener</title>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <style>
    body { font-family: Arial, sans-serif; margin: 16px; background: #0f172a; color: #e5e7eb; }
    .app { display: grid; grid-template-columns: 460px 1fr; gap: 14px; }
    .panel { background: #111c33; border: 1px solid #1f2a44; border-radius: 14px; padding: 12px; }
    .muted { color: #94a3b8; }
    input, select, button { background: #0b1224; color: #e5e7eb; border: 1px solid #1f2a44; border-radius: 10px; padding: 8px; }
    button { cursor: pointer; }
    button.active { border-color: #60a5fa; }
    .row { display: flex; gap: 8px; flex-wrap: wrap; align-items: center; }
    .pill { display: inline-block; padding: 4px 10px; border-radius: 999px; background: #0b1224; border: 1px solid #1f2a44; margin-right: 6px; }
    .green { background: rgba(34,197,94,0.12); border-color: rgba(34,197,94,0.35); color: #86efac; }
    .red { background: rgba(239,68,68,0.12); border-color: rgba(239,68,68,0.35); color: #fca5a5; }
    .tableWrap { max-height: 72vh; overflow: auto; border-radius: 12px; border: 1px solid #1f2a44; }
    table { width: 100%; border-collapse: collapse; font-size: 12px; }
    th, td { padding: 8px; border-bottom: 1px solid #1f2a44; }
    th { position: sticky; top: 0; background: #0b1224; z-index: 2; text-align: left; }
    tr:hover { background: rgba(96,165,250,0.08); }
    tr.selected { background: rgba(96,165,250,0.16); }
    #chart { width: 100%; height: 640px; }
    img { max-height: 110px; border-radius: 12px; border: 1px solid #1f2a44; }
    .title { font-size: 18px; font-weight: bold; }
    .subtitle { margin-top: 2px; }
    .topRight { display: grid; grid-template-columns: 1fr auto; gap: 12px; align-items: start; }
  </style>
</head>
<body>

<h2 style="margin: 0 0 10px 0;">Pokemon Momentum Screener</h2>
<div class="muted" style="margin-bottom: 14px;">Rank and filter items. Click a row to load the chart. Use timeframe buttons to zoom.</div>

<div class="app">

  <div class="panel">
    <div class="row" style="margin-bottom: 10px;">
      <input id="search" placeholder="Search name or set..." style="flex: 1; min-width: 220px;" />
      <select id="sortBy" title="Sort by">
        <option value="roc30">ROC 30d</option>
        <option value="roc7">ROC 7d</option>
        <option value="roc90">ROC 90d</option>
        <option value="accel">Acceleration</option>
        <option value="priceNow">Price</option>
      </select>
    </div>

    <div class="row" style="margin-bottom: 10px;">
      <select id="typeFilter" title="Type">
        <option value="all">All</option>
        <option value="singles">Singles</option>
        <option value="sealed">Sealed</option>
      </select>

      <select id="secretFilter" title="Secret">
        <option value="all">All numbers</option>
        <option value="secret">Secret only (125/094)</option>
        <option value="nonsecret">Normal only</option>
      </select>

      <select id="setFilter" title="Set">
        <option value="all">All sets</option>
      </select>
    </div>

    <div class="muted" id="countLine" style="margin: 6px 0 10px 0;"></div>

    <div class="tableWrap">
      <table id="tbl">
        <thead>
          <tr>
            <th>Item</th>
            <th>Set</th>
            <th style="text-align:right;">Price</th>
            <th style="text-align:right;">7d%</th>
            <th style="text-align:right;">30d%</th>
            <th style="text-align:right;">90d%</th>
            <th style="text-align:right;">Accel</th>
          </tr>
        </thead>
        <tbody></tbody>
      </table>
    </div>
  </div>

  <div class="panel">
    <div class="topRight">
      <div>
        <div class="title" id="title"></div>
        <div class="muted subtitle" id="subtitle"></div>
        <div id="stats" style="margin-top: 10px;"></div>
      </div>
      <div id="imgCard" style="display:none;">
        <img id="prodImg" src="" alt="Product image"/>
      </div>
    </div>

    <div class="row" style="margin: 12px 0 8px 0;">
      <button class="tf active" data-tf="30">30d</button>
      <button class="tf" data-tf="7">7d</button>
      <button class="tf" data-tf="90">90d</button>
      <button class="tf" data-tf="365">1y</button>
      <button class="tf" data-tf="all">All</button>
    </div>

    <div id="chart"></div>
  </div>

</div>

<script id="series-data" type="application/json">__SERIES_MAP__</script>
<script id="rows-data" type="application/json">__ROWS__</script>
<script id="default-key" type="application/json">__DEFAULT_KEY__</script>

<script>
const seriesMap = JSON.parse(document.getElementById("series-data").textContent);
const rowsAll = JSON.parse(document.getElementById("rows-data").textContent);
const defaultKey = JSON.parse(document.getElementById("default-key").textContent);

const titleEl = document.getElementById("title");
const subtitleEl = document.getElementById("subtitle");
const statsEl = document.getElementById("stats");
const imgCard = document.getElementById("imgCard");
const prodImg = document.getElementById("prodImg");

const searchEl = document.getElementById("search");
const sortEl = document.getElementById("sortBy");
const typeEl = document.getElementById("typeFilter");
const secretEl = document.getElementById("secretFilter");
const setEl = document.getElementById("setFilter");
const countLine = document.getElementById("countLine");

let selectedKey = defaultKey;
let timeframe = 30;

function safeNum(x) {
  if (x === "" || x === null || x === undefined) return null;
  const n = Number(x);
  return Number.isFinite(n) ? n : null;
}

function pctPill(label, value) {
  if (value === null || value === undefined) return `<span class="pill">${label}: n/a</span>`;
  const cls = value >= 0 ? "pill green" : "pill red";
  const sign = value >= 0 ? "+" : "";
  return `<span class="${cls}">${label}: ${sign}${value.toFixed(2)}%</span>`;
}

function setSelectedRow() {
  const trs = document.querySelectorAll("#tbl tbody tr");
  for (const tr of trs) {
    if (tr.dataset.key === selectedKey) tr.classList.add("selected");
    else tr.classList.remove("selected");
  }
}

function sliceByTimeframe(d, arr, tf) {
  if (tf === "all") return { d, arr };
  const n = Number(tf);
  if (!Number.isFinite(n) || n <= 0) return { d, arr };
  const start = Math.max(0, d.length - n);
  return { d: d.slice(start), arr: arr.slice(start) };
}

function renderChart(key) {
  selectedKey = key;
  const s = seriesMap[key];

  titleEl.textContent = s.productName ? s.productName : ("productId " + s.productId);
  const groupTxt = s.groupName ? (s.groupName + " | ") : "";
  subtitleEl.textContent = groupTxt + (s.subTypeName || "") + " | productId " + s.productId;

  if (s.imageUrl) {
    prodImg.src = s.imageUrl;
    imgCard.style.display = "block";
  } else {
    imgCard.style.display = "none";
  }

  const d0 = s.d;
  const price0 = s.price.map(safeNum);
  const sma70 = s.sma7.map(safeNum);
  const sma300 = s.sma30.map(safeNum);

  const d = sliceByTimeframe(d0, d0, timeframe).d;
  const price = sliceByTimeframe(d0, price0, timeframe).arr;
  const sma7 = sliceByTimeframe(d0, sma70, timeframe).arr;
  const sma30 = sliceByTimeframe(d0, sma300, timeframe).arr;

  const lastIdx = d0.length - 1;
  const lastPrice = price0[lastIdx];
  const lastSma30 = sma300[lastIdx];
  const pctVsSma30 = (lastPrice !== null && lastSma30 !== null && lastSma30 !== 0)
    ? ((lastPrice - lastSma30) / lastSma30 * 100)
    : null;

  statsEl.innerHTML = `
    <div><b>Latest date:</b> ${d0[lastIdx]}</div>
    <div style="margin-top: 6px;">
      ${pctPill("ROC 7d", safeNum(s.roc7))}
      ${pctPill("ROC 30d", safeNum(s.roc30))}
      ${pctPill("ROC 90d", safeNum(s.roc90))}
      ${pctPill("% vs SMA30", pctVsSma30)}
      ${pctPill("Acceleration", safeNum(s.accel))}
    </div>
    <div style="margin-top: 6px;"><b>Latest price:</b> ${lastPrice === null ? "n/a" : lastPrice}</div>
  `;

  const traces = [
    { x: d, y: price, name: "Price", mode: "lines" },
    { x: d, y: sma7, name: "SMA 7", mode: "lines" },
    { x: d, y: sma30, name: "SMA 30", mode: "lines" },
  ];

  const layout = {
    margin: { t: 30, r: 20, b: 40, l: 60 },
    legend: { orientation: "h" },
    hovermode: "x unified",
    xaxis: { title: "Date" },
    yaxis: { title: "Price" },
    paper_bgcolor: "rgba(0,0,0,0)",
    plot_bgcolor: "rgba(0,0,0,0)",
    font: { color: "#e5e7eb" }
  };

  Plotly.react("chart", traces, layout, { displaylogo: false });
  setSelectedRow();
}

function normalize(s) {
  return (s || "").toString().toLowerCase();
}

function rebuildSetOptions(rows) {
  const setNames = new Map();
  for (const r of rowsAll) {
    const name = r.groupName || "";
    if (name) setNames.set(name, true);
  }
  const names = Array.from(setNames.keys()).sort((a,b) => a.localeCompare(b));
  for (const n of names) {
    const opt = document.createElement("option");
    opt.value = n;
    opt.textContent = n;
    setEl.appendChild(opt);
  }
}

function filteredRows() {
  const q = normalize(searchEl.value);
  const sortBy = sortEl.value;
  const type = typeEl.value;
  const sec = secretEl.value;
  const setName = setEl.value;

  let rows = rowsAll.filter(r => {
    const text = normalize(r.productName) + " " + normalize(r.groupName) + " " + normalize(r.subTypeName);
    if (q && !text.includes(q)) return false;

    if (type === "sealed" && !r.sealed) return false;
    if (type === "singles" && r.sealed) return false;

    if (sec === "secret" && !r.secret) return false;
    if (sec === "nonsecret" && r.secret) return false;

    if (setName !== "all" && (r.groupName || "") !== setName) return false;

    return true;
  });

  function val(r) {
    const x = r[sortBy];
    return (x === null || x === undefined) ? -Infinity : Number(x);
  }
  rows.sort((a,b) => val(b) - val(a));
  return rows;
}

function fmtPct(x) {
  if (x === null || x === undefined) return "";
  const n = Number(x);
  if (!Number.isFinite(n)) return "";
  const sign = n >= 0 ? "+" : "";
  return sign + n.toFixed(2);
}

function fmtPrice(x) {
  if (x === null || x === undefined) return "";
  const n = Number(x);
  if (!Number.isFinite(n)) return "";
  return n.toFixed(2);
}

function rebuildTable() {
  const rows = filteredRows();
  countLine.textContent = `Showing ${rows.length} items`;

  const tbody = document.querySelector("#tbl tbody");
  tbody.innerHTML = "";

  const top = rows.slice(0, 250);
  for (const r of top) {
    const tr = document.createElement("tr");
    tr.dataset.key = r.key;

    const item = `${r.productName || ("productId " + r.productId)}${r.subTypeName ? (" | " + r.subTypeName) : ""}`;
    const setName = r.groupName || "";

    tr.innerHTML = `
      <td>${item}</td>
      <td>${setName}</td>
      <td style="text-align:right;">${fmtPrice(r.priceNow)}</td>
      <td style="text-align:right;">${fmtPct(r.roc7)}</td>
      <td style="text-align:right;">${fmtPct(r.roc30)}</td>
      <td style="text-align:right;">${fmtPct(r.roc90)}</td>
      <td style="text-align:right;">${fmtPct(r.accel)}</td>
    `;

    tr.addEventListener("click", () => renderChart(r.key));
    tbody.appendChild(tr);
  }

  setSelectedRow();
}

for (const b of document.querySelectorAll("button.tf")) {
  b.addEventListener("click", () => {
    for (const x of document.querySelectorAll("button.tf")) x.classList.remove("active");
    b.classList.add("active");
    timeframe = b.dataset.tf;
    renderChart(selectedKey);
  });
}

searchEl.addEventListener("input", rebuildTable);
sortEl.addEventListener("change", rebuildTable);
typeEl.addEventListener("change", rebuildTable);
secretEl.addEventListener("change", rebuildTable);
setEl.addEventListener("change", rebuildTable);

rebuildSetOptions(rowsAll);
rebuildTable();
renderChart(defaultKey);
</script>

</body>
</html>
"""

OUT_HTML.parent.mkdir(parents=True, exist_ok=True)
html = html.replace("__SERIES_MAP__", safe_json_for_html(series_map))
html = html.replace("__ROWS__", safe_json_for_html(rows))
html = html.replace("__DEFAULT_KEY__", safe_json_for_html(default_key))

print("Series built:", len(series_map))
print("Rows built:", len(rows))
print("About to write:", OUT_HTML)

print("Writing:", OUT_HTML)
OUT_HTML.write_text(html, encoding="utf-8")
print("Wrote bytes:", OUT_HTML.stat().st_size)
print("Done.")