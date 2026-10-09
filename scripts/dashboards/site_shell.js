/*
 * Shared Poke6s site shell: one header and one navigation for every page, organized
 * around the five task areas. Pages opt in with:
 *
 *   <script src="/site-shell.js" defer></script>
 *
 * Pages that already have their own logo and account menu (the dashboards) use
 * data-shell="nav" on that tag to get only the area navigation row.
 *
 * The header is inserted at the top of <body> (or into <div id="site-shell"> when a
 * page provides one). The same AREAS config renders the area landing pages, so the
 * navigation and the hubs can never disagree.
 */
(function () {
  const AREAS = [
    {
      key: "buy",
      label: "Buy",
      question: "Is this worth buying, and at what price?",
      tools: [
        { label: "Card & Price Check", href: "/dashboard", text: "Search any card or sealed product for its market price, trend, and buy targets." },
        { label: "Sealed Deals", href: "/sealed-deals", text: "Price per pack and value for booster boxes, ETBs, and bundles." },
        { label: "Budget Builder", href: "/budget-builder", text: "The best cards to buy for a set budget, spread across sets." },
      ],
    },
    {
      key: "discover",
      label: "Discover",
      question: "What's moving right now?",
      tools: [
        { label: "Pullbacks", href: "/dashboard?tab=good_buys", text: "Premium cards that have dipped and may be good entries." },
        { label: "Early Movers", href: "/dashboard?tab=early_uptrends", text: "Cards just starting to trend up." },
        { label: "Breakouts", href: "/dashboard?tab=breakouts", text: "Cards breaking above their 90-day high with real activity." },
        { label: "Top Movers", href: "/dashboard?tab=top_movers", text: "The biggest steady gains over the last 30 days." },
        { label: "Under the Radar", href: "/dashboard?tab=under_the_radar", text: "Quiet risers that haven't been noticed yet." },
        { label: "Confirmed Movers", href: "/dashboard?tab=confirmed_uptrends", text: "Uptrends confirmed by moving averages." },
        { label: "Consistent Movers", href: "/dashboard?tab=sma30_holds", text: "Cards holding above their 30-day average." },
      ],
    },
    {
      key: "research",
      label: "Research",
      question: "How is this set, Pokémon, or market doing?",
      tools: [
        { label: "Set Explorer", href: "/set-explorer", text: "What a set costs, where its value sits, and how deep it goes." },
        { label: "Browse a Set", href: "/dashboard?tab=group_products", text: "Every card in a set with prices and signals." },
        { label: "Browse a Pokémon", href: "/dashboard?tab=browse_species", text: "Every print of one Pokémon across all sets." },
        { label: "Set Strength", href: "/dashboard?tab=group_signals", text: "Which sets are strengthening or weakening." },
        { label: "Time to Buy", href: "/dashboard?tab=time_to_buy", text: "Where each card in a set sits in its price range." },
        { label: "Market Indexes", href: "/index-overview", text: "Era and market-wide indexes for English and Japanese cards." },
      ],
    },
    {
      key: "track",
      label: "Track",
      question: "What am I buying and watching?",
      tools: [
        { label: "Buy List", href: "/mobile?mode=tracked&tracked_tag=owned", text: "Cards and sealed products you plan to pick up." },
        { label: "Watchlist", href: "/mobile?mode=tracked&tracked_tag=watchlist", text: "Items you're waiting on a price for." },
        { label: "Favorites", href: "/mobile?mode=tracked&tracked_tag=favorite", text: "Cards you've starred." },
        { label: "All Tracked Items", href: "/mobile?mode=tracked&tracked_tag=all", text: "Everything you've saved, in one list." },
        { label: "Account & Saved Views", href: "/account-settings", text: "Sign-in, synced tags, and saved dashboard views." },
      ],
    },
    {
      key: "learn",
      label: "Learn",
      question: "How does the market work, and what helps me collect?",
      tools: [
        { label: "Market Indexes", href: "/index-overview", text: "How each era of Pokémon cards is performing over time." },
        { label: "Collector Hub", href: "/collector-hub", text: "Checklists, print tools, and collection workflows." },
        { label: "Placeholder Library", href: "/placeholders", text: "Printable binder placeholders and checklist downloads." },
      ],
    },
  ];

  const SCRIPT_MODE = (document.currentScript && document.currentScript.dataset.shell) || "full";

  // Dashboard tabs and mobile modes map onto areas so the nav highlights the right one.
  const RESEARCH_TABS = new Set(["group_products", "browse_species", "group_signals", "time_to_buy"]);

  function dashboardArea() {
    const params = new URLSearchParams(window.location.search);
    if (params.get("mode") === "tracked" || params.get("tab") === "tracked_items") return "track";
    if (RESEARCH_TABS.has(params.get("tab") || "")) return "research";
    return "discover";
  }

  // Which area a page belongs to, for highlighting the nav.
  const PATH_AREAS = [
    [/^\/(buy|sealed-deals|budget-builder)\b/, "buy"],
    [/^\/discover\b/, "discover"],
    [/^\/(research|set-explorer|index-overview)/, "research"],
    [/^\/(track|account-settings)\b/, "track"],
    [/^\/(learn|collector-hub|placeholders)\b/, "learn"],
  ];

  const CSS = `
    .ps-shell { position: relative; z-index: 1000; font-family: Inter, system-ui, -apple-system, "Segoe UI", sans-serif;
      background: rgba(8, 17, 29, 0.94); backdrop-filter: blur(8px); border-bottom: 1px solid rgba(136, 162, 205, 0.2); }
    .ps-shell-inner { max-width: 1440px; margin: 0 auto; padding: 8px 16px; display: flex; align-items: center; gap: 16px; }
    .ps-brand { display: flex; align-items: center; gap: 8px; color: #eaf1ff; text-decoration: none; font-weight: 700; letter-spacing: 0.02em; }
    .ps-brand img { height: 28px; width: auto; display: block; }
    .ps-shell, .ps-shell-inner { max-width: 100%; min-width: 0; box-sizing: border-box; }
    .ps-nav { display: flex; gap: 4px; flex: 1; min-width: 0; overflow-x: auto; scrollbar-width: none; }
    .ps-nav::-webkit-scrollbar { display: none; }
    .ps-nav a, .ps-side a { color: #b8c7e3; text-decoration: none; font-size: 14px; font-weight: 600; padding: 8px 12px; border-radius: 8px; white-space: nowrap; }
    .ps-nav a:hover, .ps-side a:hover { color: #fff; background: rgba(136, 162, 205, 0.14); }
    .ps-nav a[aria-current="page"] { color: #fff; background: rgba(77, 163, 255, 0.22); box-shadow: inset 0 0 0 1px rgba(77, 163, 255, 0.5); }
    .ps-side { display: flex; gap: 4px; margin-left: auto; }
    .ps-side a.ps-admin { color: #ffcf7a; }
    .ps-side a.ps-admin[hidden] { display: none; }
    .ps-shell.ps-nav-only .ps-shell-inner { padding: 6px 16px; }
    .ps-shell.ps-nav-only .ps-nav a { font-size: 13px; padding: 6px 12px; }
    @media (max-width: 640px) {
      .ps-shell-inner { flex-wrap: wrap; gap: 4px 8px; padding: 8px 12px; }
      .ps-brand span { display: none; }
      .ps-nav { order: 3; flex-basis: 100%; }
      .ps-nav a, .ps-side a { padding: 8px 10px; font-size: 13px; }
      .ps-shell.ps-nav-only .ps-shell-inner { padding: 6px 6px; }
      .ps-shell.ps-nav-only .ps-nav { gap: 2px; justify-content: space-between; }
      .ps-shell.ps-nav-only .ps-nav a { padding: 6px 8px; }
    }
  `;

  function currentArea() {
    const forced = document.body && document.body.dataset.area;
    if (forced) return forced;
    const path = window.location.pathname;
    if (/^\/(dashboard|mobile)?$/.test(path) || path === "/dashboard-dev") return dashboardArea();
    const match = PATH_AREAS.find(([pattern]) => pattern.test(path));
    return match ? match[1] : "";
  }

  function trackingToken() {
    const match = document.cookie.match(/(?:^|;\s*)pm_tracking_token=([^;]+)/);
    return match ? decodeURIComponent(match[1]) : "";
  }

  async function revealAdminLink(link) {
    const token = trackingToken();
    if (!token) return;
    try {
      const res = await fetch("/tracking/session", { headers: { Authorization: `Bearer ${token}` } });
      if (!res.ok) return;
      const payload = await res.json();
      if (payload && payload.user && payload.user.is_admin) link.hidden = false;
    } catch (_err) {
      // Signed-out or offline: the admin link simply stays hidden.
    }
  }

  function buildHeader() {
    const area = currentArea();
    const header = document.createElement("header");
    header.className = "ps-shell";
    const navLinks = AREAS.map((a) => `<a href="/${a.key}"${a.key === area ? ' aria-current="page"' : ""}>${a.label}</a>`).join("");
    if (SCRIPT_MODE === "nav") {
      header.classList.add("ps-nav-only");
      header.innerHTML = `<div class="ps-shell-inner"><nav class="ps-nav" aria-label="Main">${navLinks}</nav></div>`;
      return header;
    }
    header.innerHTML = `
      <div class="ps-shell-inner">
        <a class="ps-brand" href="/" aria-label="Poke6s home">
          <img src="/images/Logo.png" alt="Poke6s" /><span>Market</span>
        </a>
        <nav class="ps-nav" aria-label="Main">${navLinks}</nav>
        <div class="ps-side">
          <a class="ps-admin" href="/admin" hidden>Admin</a>
          <a href="/account-settings">Account</a>
        </div>
      </div>`;
    revealAdminLink(header.querySelector(".ps-admin"));
    return header;
  }

  function mount() {
    if (document.querySelector(".ps-shell")) return;
    const style = document.createElement("style");
    style.textContent = CSS;
    document.head.appendChild(style);
    const slot = document.getElementById("site-shell");
    const header = buildHeader();
    if (slot) slot.replaceWith(header);
    else document.body.insertBefore(header, document.body.firstChild);
  }

  window.Poke6sShell = { AREAS, currentArea };
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", mount);
  else mount();
})();
