import json
import pandas as pd
from pathlib import Path
import math

# ---------------- CONFIG ----------------
DATA_DIR = Path("/app/data/extracted")
TIMESERIES_CSV = DATA_DIR / "top200_timeseries.csv"
ROC_SNAPSHOT_CSV = DATA_DIR / "roc_snapshot_7_30_90.csv"
LOOKUP_CSV = DATA_DIR / "top200_lookup.csv"  # or pokemon_products.csv
GROUPS_CSV = DATA_DIR / "pokemon_groups.csv"  # optional
OUT_HTML = Path("/app/output/roc_dashboard.html")

# Timeseries columns (edit if yours differ)
COL_DATE = "date"
COL_PID = "productId"
COL_SUB = "subTypeName"
COL_PRICE = "price"
COL_SMA7 = "sma_7"
COL_SMA30 = "sma_30"
# ----------------------------------------

def clean_for_json(x):
    # Turn NaN/inf into None (=> null in JSON)
    if x is None:
        return None
    if isinstance(x, float) and (math.isnan(x) or math.isinf(x)):
        return None
    return x

def require_file(p: Path):
    if not p.exists():
        raise FileNotFoundError(f"Missing file: {p}")

def require_cols(df: pd.DataFrame, cols, label: str):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise KeyError(f"{label} is missing columns: {missing}. Found: {df.columns.tolist()}")

def safe_str(x):
    return "" if pd.isna(x) else str(x)

def safe_num(x):
    if pd.isna(x):
        return None
    try:
        return float(x)
    except Exception:
        return None

def make_key(pid, sub):
    return f"{pid}||{sub}"

print("Checking files...")
require_file(TIMESERIES_CSV)
require_file(ROC_SNAPSHOT_CSV)
require_file(LOOKUP_CSV)

print("Loading timeseries:", TIMESERIES_CSV)
ts = pd.read_csv(TIMESERIES_CSV)
require_cols(ts, [COL_PID, COL_SUB, COL_DATE, COL_PRICE, COL_SMA7, COL_SMA30], "Timeseries CSV")
ts[COL_DATE] = ts[COL_DATE].astype(str)

print("Loading ROC snapshot:", ROC_SNAPSHOT_CSV)
roc = pd.read_csv(ROC_SNAPSHOT_CSV)
require_cols(roc, ["productId", "subTypeName", "roc_7d_pct", "roc_30d_pct", "roc_90d_pct", "price_now"], "ROC snapshot")

print("Loading lookup:", LOOKUP_CSV)
lk = pd.read_csv(LOOKUP_CSV)
# lookup minimum columns
require_cols(lk, ["productId"], "Lookup CSV")

# Make sure lookup has these, fill if missing
for col in ["productName", "imageUrl", "groupId", "rarity", "number", "name"]:
    if col not in lk.columns:
        lk[col] = ""

# Normalize productName from name if needed
if lk["productName"].astype(str).str.len().sum() == 0 and "name" in lk.columns:
    lk["productName"] = lk["name"]

# Group name map optional
group_name_map = {}
if GROUPS_CSV.exists():
    try:
        g = pd.read_csv(GROUPS_CSV)
        if "groupId" in g.columns and "name" in g.columns:
            group_name_map = dict(zip(g["groupId"].astype(str), g["name"].astype(str)))
            print("Loaded group names:", len(group_name_map))
    except Exception as e:
        print("Warning: could not load groups CSV:", e)

# Join lookup into roc
lk2 = lk.copy()
roc = roc.merge(
    lk2[["productId", "groupId", "productName", "imageUrl", "rarity", "number"]],
    on="productId",
    how="left"
)

roc["groupId"] = roc["groupId"].astype("Int64", errors="ignore")
roc["groupId"] = roc["groupId"].astype(str)

roc["key"] = roc.apply(lambda r: make_key(r["productId"], r["subTypeName"]), axis=1)

# Prepare keys in ts
ts["key"] = ts.apply(lambda r: make_key(r[COL_PID], r[COL_SUB]), axis=1)
ts_keys = ts["key"].unique().tolist()
print("Unique series keys in timeseries:", len(ts_keys))

roc_by_key = {row["key"]: row for _, row in roc.iterrows()}

series_map = {}
print("Building series map...")
for k in ts_keys:
    chunk = ts[ts["key"] == k].sort_values(COL_DATE)
    if chunk.empty:
        continue

    roc_row = roc_by_key.get(k, None)

    if roc_row is None:
        pid, sub = k.split("||", 1)
        meta = {
            "productId": int(pid),
            "subTypeName": sub,
            "productName": "",
            "groupId": "",
            "groupName": "",
            "imageUrl": "",
            "roc7": None, "roc30": None, "roc90": None,
        }
        label = k
    else:
        gid = safe_str(roc_row.get("groupId", ""))
        gname = group_name_map.get(gid, "")
        pname = safe_str(roc_row.get("productName", ""))
        sub = safe_str(roc_row.get("subTypeName", ""))

        label = f"{pname} | {sub}".strip(" |") if pname or sub else k
        meta = {
            "productId": int(roc_row["productId"]),
            "subTypeName": sub,
            "productName": pname,
            "groupId": gid,
            "groupName": gname,
            "imageUrl": safe_str(roc_row.get("imageUrl", "")),
            "roc7": safe_num(roc_row.get("roc_7d_pct")),
            "roc30": safe_num(roc_row.get("roc_30d_pct")),
            "roc90": safe_num(roc_row.get("roc_90d_pct")),
            "priceNow": safe_num(roc_row.get("price_now")),
        }

    series_map[k] = {
        "label": label if label else k,
        **meta,
        "d": chunk[COL_DATE].tolist(),
        "price": [clean_for_json(v) for v in chunk[COL_PRICE].tolist()],
        "sma7":  [clean_for_json(v) for v in chunk[COL_SMA7].tolist()],
        "sma30": [clean_for_json(v) for v in chunk[COL_SMA30].tolist()],
    }

def safe_json_for_html(obj):
    s = json.dumps(obj, ensure_ascii=False, allow_nan=False)
    return s.replace("</", "<\\/")

if not series_map:
    raise RuntimeError("No series built. Timeseries file may be empty or keys do not match ROC/lookup keys.")

# Default key: highest 30d ROC that exists in series_map
default_key = None
roc_sorted = roc.sort_values("roc_30d_pct", ascending=False)
for _, r in roc_sorted.iterrows():
    k = r["key"]
    if k in series_map:
        default_key = k
        break
if default_key is None:
    default_key = list(series_map.keys())[0]

html = """<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Pokemon Momentum Dashboard (ROC 7/30/90)</title>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <style>
    body { font-family: Arial, sans-serif; margin: 16px; }
    .row { display: flex; gap: 16px; flex-wrap: wrap; align-items: center; }
    .card { border: 1px solid #ddd; border-radius: 10px; padding: 12px; }
    #chart { width: 100%; height: 600px; }
    select { padding: 8px; min-width: 360px; }
    img { max-height: 110px; border-radius: 10px; }
    .muted { color: #666; }
    .pill { display: inline-block; padding: 4px 10px; border-radius: 999px; background: #f2f2f2; margin-right: 6px; }
    .green { background: #e8f7ee; color: #166534; }
    .red { background: #fdecec; color: #991b1b; }
  </style>
</head>
<body>

<h2>Pokemon Momentum Dashboard</h2>
<div class="muted">Select an item. Stats include ROC 7d, 30d, 90d.</div>

<div class="row" style="margin-top: 12px;">
  <div class="card">
    <div class="muted">Select product</div>
    <select id="picker"></select>
  </div>

  <div class="card" style="flex: 1;">
    <div id="title" style="font-weight: bold; font-size: 18px;"></div>
    <div class="muted" id="subtitle"></div>
    <div id="stats" style="margin-top: 8px;"></div>
  </div>

  <div class="card" id="imgCard" style="display:none;">
    <img id="prodImg" src="" alt="Product image"/>
  </div>
</div>

<div id="chart" style="margin-top: 12px;"></div>

<script id="series-data" type="application/json">__SERIES_MAP__</script>
<script id="default-key" type="application/json">__DEFAULT_KEY__</script>

<script>
const seriesMap = JSON.parse(document.getElementById("series-data").textContent);
const defaultKey = JSON.parse(document.getElementById("default-key").textContent);

const picker = document.getElementById("picker");
const titleEl = document.getElementById("title");
const subtitleEl = document.getElementById("subtitle");
const statsEl = document.getElementById("stats");
const imgCard = document.getElementById("imgCard");
const prodImg = document.getElementById("prodImg");

function safeNum(x) {
  if (x === "" || x === null || x === undefined) return null;
  const n = Number(x);
  return Number.isFinite(n) ? n : null;
}

function pill(label, value) {
  if (value === null || value === undefined) return `<span class="pill">${label}: n/a</span>`;
  const cls = value >= 0 ? "pill green" : "pill red";
  const sign = value >= 0 ? "+" : "";
  return `<span class="${cls}">${label}: ${sign}${value.toFixed(2)}%</span>`;
}

const keys = Object.keys(seriesMap);
keys.sort((a,b) => seriesMap[a].label.localeCompare(seriesMap[b].label));
for (const k of keys) {
  const opt = document.createElement("option");
  opt.value = k;
  opt.textContent = seriesMap[k].label;
  picker.appendChild(opt);
}
picker.value = defaultKey;

function render(key) {
  const s = seriesMap[key];

  const title = s.productName ? s.productName : ("productId " + s.productId);
  titleEl.textContent = title;

  const groupTxt = s.groupName ? (s.groupName + " | ") : "";
  subtitleEl.textContent = groupTxt + (s.subTypeName || "") + " | productId " + s.productId;

  if (s.imageUrl) {
    prodImg.src = s.imageUrl;
    imgCard.style.display = "block";
  } else {
    imgCard.style.display = "none";
  }

  const d = s.d;
  const price = s.price.map(safeNum);
  const sma7 = s.sma7.map(safeNum);
  const sma30 = s.sma30.map(safeNum);

  const lastIdx = d.length - 1;
  const lastPrice = price[lastIdx];
  const lastSma30 = sma30[lastIdx];
  const pctVsSma30 = (lastPrice !== null && lastSma30 !== null && lastSma30 !== 0)
    ? ((lastPrice - lastSma30) / lastSma30 * 100)
    : null;

  statsEl.innerHTML = `
    <div><b>Latest date:</b> ${d[lastIdx]}</div>
    <div style="margin-top: 6px;">
      ${pill("ROC 7d", safeNum(s.roc7))}
      ${pill("ROC 30d", safeNum(s.roc30))}
      ${pill("ROC 90d", safeNum(s.roc90))}
      ${pill("% vs SMA30", pctVsSma30)}
    </div>
    <div style="margin-top: 6px;"><b>Latest price:</b> ${lastPrice === null ? "n/a" : lastPrice}</div>
  `;

  const traces = [
    { x: d, y: price, name: "Price", mode: "lines" },
    { x: d, y: sma7, name: "SMA 7", mode: "lines" },
    { x: d, y: sma30, name: "SMA 30", mode: "lines" },
  ];

  const layout = {
    margin: { t: 40, r: 20, b: 40, l: 60 },
    legend: { orientation: "h" },
    hovermode: "x unified",
    xaxis: { title: "Date" },
    yaxis: { title: "Price" },
  };

  Plotly.react("chart", traces, layout, { displaylogo: false });
}

picker.addEventListener("change", () => render(picker.value));
render(defaultKey);
</script>

</body>
</html>
"""

OUT_HTML.parent.mkdir(parents=True, exist_ok=True)
def safe_json_for_html(obj):
    # Prevent </script> from breaking the script tag
    s = json.dumps(obj, ensure_ascii=False)
    return s.replace("</", "<\\/")

html = html.replace("__SERIES_MAP__", safe_json_for_html(series_map))
html = html.replace("__DEFAULT_KEY__", safe_json_for_html(default_key))

print("Writing:", OUT_HTML)
OUT_HTML.write_text(html, encoding="utf-8")

size = OUT_HTML.stat().st_size
print("Wrote HTML bytes:", size)
print("Done.")