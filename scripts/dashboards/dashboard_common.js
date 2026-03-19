(function () {
  function withCategory(apiBase, categoryId, path) {
    const url = new URL(path, apiBase);
    if (!url.searchParams.has("category_id")) {
      url.searchParams.set("category_id", String(categoryId));
    }
    return `${url.pathname}${url.search}`;
  }

  function syncCategoryUrl(categoryId) {
    const url = new URL(window.location.href);
    url.searchParams.set("category_id", String(categoryId));
    history.replaceState({}, "", `${url.pathname}${url.search}`);
  }

  async function fetchCategories(apiBase) {
    const res = await fetch(apiBase + "/categories");
    if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
    return res.json();
  }

  function populateCategoryPicker(selectEl, items, currentCategoryId) {
    selectEl.innerHTML = "";
    for (const item of items) {
      const opt = document.createElement("option");
      opt.value = String(item.category_id);
      opt.textContent = item.label;
      selectEl.appendChild(opt);
    }
    if (!items.some((item) => Number(item.category_id) === currentCategoryId)) {
      currentCategoryId = Number(items[0]?.category_id || 3);
    }
    selectEl.value = String(currentCategoryId);
    return currentCategoryId;
  }

  function keyOf(productId, subTypeName) {
    return `${productId}||${subTypeName || ""}`;
  }

  function formatNumber(value, digits) {
    const resolvedDigits = digits ?? 2;
    const num = Number(value);
    if (value === null || value === undefined || Number.isNaN(num)) return "n/a";
    return num.toLocaleString(undefined, {
      minimumFractionDigits: resolvedDigits,
      maximumFractionDigits: resolvedDigits,
    });
  }

  function formatPercentMeta(value, digits) {
    const resolvedDigits = digits ?? 2;
    const num = Number(value);
    if (value === null || value === undefined || Number.isNaN(num)) {
      return { text: "n/a", cls: "neutral", value: null };
    }
    return {
      text: `${num >= 0 ? "+" : ""}${formatNumber(num, resolvedDigits)}%`,
      cls: num >= 0 ? "good" : "warn",
      value: num,
    };
  }

  function formatProductClassLabel(item, fallback) {
    if (item.number) return `#${item.number}`;
    const cls = String(item.productClass || item.productKind || "").trim();
    if (!cls) return fallback || "Catalog product";
    return cls.replaceAll("_", " ").replace(/\b\w/g, (ch) => ch.toUpperCase());
  }

  function formatProductBadgeLabel(item) {
    const cls = String(item.productClass || "").trim();
    if (!cls) return String(item.productKind || "Unclassified");
    if (cls === "sealed_booster_box") return "Booster Box";
    if (cls === "sealed_booster_pack") return "Booster Pack";
    if (cls === "mcap") return "MCAP";
    if (cls === "card") return item.rarity || "Card";
    if (cls === "other") return "Other";
    return cls.replaceAll("_", " ").replace(/\b\w/g, (ch) => ch.toUpperCase());
  }

  function classifySegment(item, cardLabel, sealedLabel) {
    return item.productKind === "sealed" ? sealedLabel : cardLabel;
  }

  function rowObjects(payload) {
    const columns = payload.columns || [];
    return (payload.rows || []).map((row) =>
      Object.fromEntries(columns.map((col, i) => [col, row[i]]))
    );
  }

  function enrichRows(payload, universeByKey, cardLabel, sealedLabel) {
    return rowObjects(payload).map((item) => {
      item.key = keyOf(item.productId, item.subTypeName);
      const meta = universeByKey.get(item.key) || {};
      item.productName = meta.productName || item.productName || `productId ${item.productId}`;
      item.groupName = meta.groupName || item.groupName || "";
      item.imageUrl = meta.imageUrl || item.imageUrl || "";
      item.rarity = meta.rarity || item.rarity || "";
      item.number = meta.number || item.number || "";
      item.productClass = meta.productClass || item.productClass || null;
      item.productKind = meta.productKind || item.productKind || null;
      item.segment = classifySegment(item, cardLabel, sealedLabel);
      return item;
    });
  }

  window.DashboardCommon = {
    withCategory,
    syncCategoryUrl,
    fetchCategories,
    populateCategoryPicker,
    keyOf,
    formatNumber,
    formatPercentMeta,
    formatProductClassLabel,
    formatProductBadgeLabel,
    classifySegment,
    rowObjects,
    enrichRows,
  };
})();
