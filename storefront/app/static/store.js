// Small progressive enhancements. Every page works without this file.
// - sort menu submits on change;
// - product photos: thumbnails swap the main photo, clicking the main photo opens a zoom dialog.
// - the cart (Phase 2), kept in localStorage as SKUs and quantities only. Prices, stock and
//   shipping always come from the server (POST /cart/quote), never from what is stored here.
"use strict";

document.querySelectorAll("select[data-autosubmit]").forEach((select) => {
  select.addEventListener("change", () => select.form.submit());
});

const zoom = document.getElementById("zoom");
const mainLink = document.querySelector(".main-img[data-zoom]");
if (zoom && mainLink && typeof zoom.showModal === "function") {
  const mainImg = mainLink.querySelector("img");
  const thumbs = [...document.querySelectorAll(".thumbs a[data-swap]")];
  if (thumbs.length) thumbs[0].setAttribute("aria-current", "true");
  thumbs.forEach((thumb) => {
    thumb.addEventListener("click", (event) => {
      event.preventDefault();
      mainLink.href = thumb.href;
      mainImg.src = thumb.href;
      mainImg.alt = thumb.querySelector("img").alt;
      thumbs.forEach((t) => t.removeAttribute("aria-current"));
      thumb.setAttribute("aria-current", "true");
    });
  });
  mainLink.addEventListener("click", (event) => {
    event.preventDefault();
    // The <img> is created on first open so the page has no empty image before then.
    const img = zoom.querySelector("img") || zoom.appendChild(document.createElement("img"));
    img.src = mainLink.href;
    img.alt = mainImg.alt;
    zoom.showModal();
  });
  // Clicking the dark area around the photo closes it, like most shops.
  zoom.addEventListener("click", (event) => { if (event.target === zoom) zoom.close(); });
}

// --- cart -------------------------------------------------------------------------------
const CART_KEY = "poke6s-cart";

function readCart() {
  try {
    const items = JSON.parse(localStorage.getItem(CART_KEY) || "[]");
    return Array.isArray(items) ? items.filter((i) => i && typeof i.sku === "string" && Number.isInteger(i.qty) && i.qty > 0) : [];
  } catch (error) {
    return [];
  }
}

function writeCart(items) {
  localStorage.setItem(CART_KEY, JSON.stringify(items));
  showCartCount();
}

function showCartCount() {
  const badge = document.querySelector("[data-cart-count]");
  if (!badge) return;
  const count = readCart().reduce((sum, item) => sum + item.qty, 0);
  badge.textContent = count ? String(count) : "";
  badge.hidden = !count;
}

function money(cents) {
  return "$" + (cents / 100).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function el(tag, attrs = {}, ...children) {
  const node = document.createElement(tag);
  Object.entries(attrs).forEach(([key, value]) => {
    if (key === "class") node.className = value;
    else node.setAttribute(key, value);
  });
  children.forEach((child) => node.append(child));
  return node;
}

showCartCount();

// Product page: "Add to cart".
document.querySelectorAll("form[data-add-to-cart]").forEach((form) => {
  form.addEventListener("submit", (event) => {
    event.preventDefault();
    const sku = form.dataset.sku;
    const max = Number(form.dataset.max) || 1;
    const qty = Number(form.querySelector("select").value) || 1;
    const items = readCart();
    const found = items.find((i) => i.sku === sku);
    const before = found ? found.qty : 0;
    if (found) found.qty = Math.min(found.qty + qty, max);
    else items.push({ sku, qty: Math.min(qty, max) });
    writeCart(items);
    const now = found ? found.qty : Math.min(qty, max);
    const status = document.querySelector("[data-added]");
    if (status) {
      status.textContent = now === before ? `You already have the most we can sell (${max}) in your cart.`
        : `Added. ${now} in your cart.`;
    }
    const view = form.querySelector("[data-view-cart]");
    if (view) view.hidden = false;
  });
});

// Cart page.
const cartRoot = document.querySelector("[data-cart]");
if (cartRoot) {
  const body = cartRoot.querySelector("[data-cart-body]");
  const enabled = cartRoot.dataset.enabled === "1";

  async function quote(items) {
    const response = await fetch("/cart/quote", {
      method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ items }),
    });
    if (!response.ok) throw new Error("quote failed");
    return response.json();
  }

  // Keep the stored cart in line with what the server can sell (sold out, fewer left).
  function syncCart(q) {
    writeCart(q.lines.map((line) => ({ sku: line.sku, qty: line.quantity })));
  }

  function setQty(sku, qty) {
    const items = readCart().map((i) => (i.sku === sku ? { sku, qty } : i)).filter((i) => i.qty > 0);
    writeCart(items);
    refresh();
  }

  function render(q, message) {
    body.replaceChildren();
    const notes = [...(q.problems || [])];
    if (message) notes.unshift(message);
    if (notes.length) body.append(el("ul", { class: "notice", role: "alert" }, ...notes.map((n) => el("li", {}, n))));
    if (!q.lines.length) {
      body.append(el("p", {}, "Your cart is empty. ", el("a", { href: "/shop" }, "Browse the shop")));
      return;
    }
    const rows = q.lines.map((line) => {
      const select = el("select", { "aria-label": `Quantity of ${line.title}` });
      for (let n = 1; n <= line.max_qty; n += 1) {
        const option = el("option", { value: String(n) }, String(n));
        if (n === line.quantity) option.selected = true;
        select.append(option);
      }
      select.addEventListener("change", () => setQty(line.sku, Number(select.value)));
      const remove = el("button", { type: "button", class: "btn link" }, "Remove");
      remove.addEventListener("click", () => setQty(line.sku, 0));
      const img = line.image
        ? el("img", { src: `/media/480/${line.image}.webp`, alt: "", width: "64", height: "64", loading: "lazy" })
        : el("img", { src: "/static/img/placeholder.svg", alt: "", width: "64", height: "64" });
      return el("li", { class: "cart-line" },
        el("a", { href: `/p/${line.slug}`, class: "thumb" }, img),
        el("div", { class: "what" }, el("a", { href: `/p/${line.slug}` }, line.title),
          el("span", { class: "muted small" }, `${money(line.unit_cents)} each`)),
        el("div", { class: "qty" }, select, remove),
        el("strong", { class: "num" }, money(line.total_cents)));
    });
    body.append(el("ul", { class: "cart-lines" }, ...rows));

    const shipping = q.shipping_cents ? money(q.shipping_cents) : "Free";
    const summary = el("div", { class: "cart-summary" },
      el("p", {}, el("span", {}, "Subtotal"), el("span", {}, money(q.subtotal_cents))),
      el("p", {}, el("span", {}, "Shipping"), el("span", {}, shipping)),
      el("p", { class: "total" }, el("span", {}, "Total"), el("span", {}, money(q.total_cents))));
    if (q.to_free_shipping_cents > 0) {
      summary.append(el("p", { class: "small good" }, `Add ${money(q.to_free_shipping_cents)} more for free shipping.`));
    }
    // The tax line comes from the server (orders.tax_note), so it matches the tax mode.
    const taxNote = cartRoot.dataset.taxNote;
    summary.append(el("p", { class: "small muted" },
      (taxNote ? taxNote + " " : "") + "Discount codes are applied on the next page."));
    if (enabled) {
      const button = el("button", { type: "button", class: "btn primary wide" }, "Checkout");
      button.addEventListener("click", () => checkout(button));
      summary.append(button, el("p", { class: "small muted" }, "Payment is handled securely by Stripe. US shipping only."));
    } else {
      summary.append(el("p", { class: "small" }, "Checkout opens soon. ",
        el("a", { href: cartRoot.dataset.tcgplayer }, "Our TCGplayer store")));
    }
    body.append(summary);
  }

  async function checkout(button) {
    button.disabled = true;
    button.textContent = "Opening secure checkout…";
    try {
      const response = await fetch("/checkout", {
        method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ items: readCart() }),
      });
      const data = await response.json().catch(() => ({}));
      if (response.ok && data.url) {
        window.location.assign(data.url);
        return;
      }
      if (data.quote) {
        // The cart changed (sold out, fewer left): show the new cart before trying again.
        syncCart(data.quote);
        render(data.quote, data.error);
        return;
      }
      render(await quote(readCart()), data.error || "Checkout didn't start. Please try again.");
    } catch (error) {
      button.disabled = false;
      button.textContent = "Checkout";
      body.prepend(el("p", { class: "notice", role: "alert" }, "Couldn't reach the shop. Check your connection and try again."));
    }
  }

  async function refresh() {
    try {
      const q = await quote(readCart());
      syncCart(q);
      render(q);
    } catch (error) {
      body.replaceChildren(el("p", { class: "notice", role: "alert" }, "Couldn't load your cart. Please reload the page."));
    }
  }

  refresh();
}
