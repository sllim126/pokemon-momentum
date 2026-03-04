import json
import pandas as pd
from pathlib import Path

INDICATORS = Path(r"F:\Pokemon historical data extracted\group_23237_indicators.csv")
LOOKUP     = Path(r"F:\Pokemon historical data extracted\group_23237_lookup.csv")
OUT_HTML   = Path(r"F:\Pokemon historical data extracted\151_dashboard.html")

# Load data
df = pd.read_csv(INDICATORS)
lk = pd.read_csv(LOOKUP)

# Normalize types
df["d"] = pd.to_datetime(df["d"]).dt.strftime("%Y-%m-%d")
df["productId"] = df["productId"].astype("int64")
lk["productId"] = lk["productId"].astype("int64")

# Join names
df = df.merge(lk, on=["productId", "subTypeName"], how="left")

# Build a label for the dropdown
def make_label(row):
    name = row.get("productName")
    group = row.get("groupName")
    st = row.get("subTypeName")
    if pd.isna(name):
        name = f"productId {row['productId']}"
    if pd.isna(group):
        group = ""
    if group:
        return f"{name} | {group} | {st}"
    return f"{name} | {st}"

keys = df[["productId", "subTypeName", "productName"]].drop_duplicates()
keys["label"] = keys.apply(make_label, axis=1)

# Group into compact per-series arrays for the browser
series_map = {}
for _, k in keys.iterrows():
    pid = int(k["productId"])
    st = k["subTypeName"]
    label = k["label"]

    sub = df[(df["productId"] == pid) & (df["subTypeName"] == st)].sort_values("d")

    series_map[f"{pid}::{st}"] = {
        "productId": pid,
        "subTypeName": st,
        "label": label,
        "productName": "" if pd.isna(k["productName"]) else str(k["productName"]),
        "imageUrl": "" if "imageUrl" not in sub.columns or sub["imageUrl"].isna().all() else str(sub["imageUrl"].dropna().iloc[0]),
        "d": sub["d"].tolist(),
        "price": sub["price"].astype("float").tolist(),
        "sma7": sub["sma_7"].astype("float").fillna("").replace({pd.NA:""}).tolist(),
        "sma30": sub["sma_30"].astype("float").fillna("").replace({pd.NA:""}).tolist(),
    }

# Default selection: first item
default_key = next(iter(series_map.keys()))

# HTML template
html = """<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Top 200 Momentum Dashboard</title>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <style>
    body { font-family: Arial, sans-serif; margin: 16px; }
    .row { display: flex; gap: 16px; flex-wrap: wrap; align-items: center; }
    .card { border: 1px solid #ddd; border-radius: 10px; padding: 12px; }
    #chart { width: 100%; height: 600px; }
    select { padding: 8px; min-width: 320px; }
    img { max-height: 110px; border-radius: 10px; }
    .muted { color: #666; }
  </style>
</head>
<body>

<h2>Top 200 Momentum Dashboard</h2>

<div class="row">
  <div class="card">
    <div class="muted">Select product</div>
    <select id="picker"></select>
  </div>

  <div class="card" style="flex: 1;">
    <div id="title" style="font-weight: bold;"></div>
    <div class="muted" id="subtitle"></div>
    <div id="stats" style="margin-top: 8px;"></div>
  </div>

  <div class="card" id="imgCard" style="display:none;">
    <img id="prodImg" src="" alt="Product image"/>
  </div>
</div>

<div id="chart"></div>

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

  // Populate dropdown
  const keys = Object.keys(seriesMap);
  keys.sort((a,b) => seriesMap[a].label.localeCompare(seriesMap[b].label));
  for (const k of keys) {
    const opt = document.createElement("option");
    opt.value = k;
    opt.textContent = seriesMap[k].label;
    picker.appendChild(opt);
  }
  picker.value = defaultKey;

  function safeNum(x) {
    if (x === "" || x === null || x === undefined) return null;
    const n = Number(x);
    return Number.isFinite(n) ? n : null;
  }

  function render(key) {
    const s = seriesMap[key];

    titleEl.textContent = s.productName ? s.productName : ("productId " + s.productId);
    subtitleEl.textContent = (s.groupName ? s.groupName + " | " : "") + s.subTypeName + " | productId " + s.productId;

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

    statsEl.innerHTML =
      '<div><b>Latest date:</b> ' + d[lastIdx] + '</div>' +
      '<div><b>Latest price:</b> ' + (lastPrice ?? "n/a") + '</div>' +
      '<div><b>SMA 7:</b> ' + (sma7[lastIdx] ?? "n/a") + '</div>' +
      '<div><b>SMA 30:</b> ' + (lastSma30 ?? "n/a") + '</div>' +
      '<div><b>% vs SMA 30:</b> ' + (pctVsSma30 === null ? "n/a" : pctVsSma30.toFixed(2) + "%") + '</div>';

    const traces = [
      { x: d, y: price, name: "Price", mode: "lines" },
      { x: d, y: sma7, name: "SMA 7", mode: "lines" },
      { x: d, y: sma30, name: "SMA 30", mode: "lines" }
    ];

    const layout = {
      margin: { t: 40, r: 20, b: 40, l: 60 },
      legend: { orientation: "h" },
      hovermode: "x unified",
      xaxis: { title: "Date" },
      yaxis: { title: "Price" }
    };

    Plotly.react("chart", traces, layout, { displaylogo: false });
  }

  picker.addEventListener("change", () => render(picker.value));
  render(defaultKey);
</script>

</body>
</html>
"""

html = html.replace("__SERIES_MAP__", json.dumps(series_map))
html = html.replace("__DEFAULT_KEY__", json.dumps(default_key))

OUT_HTML.write_text(html, encoding="utf-8")
print("Wrote:", OUT_HTML)
print("Open it in a browser to test.")