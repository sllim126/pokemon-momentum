# Responsive QA Snapshot

Reviewed: 2026-09-10

This pass covers the FastAPI-served HTML pages at phone (approximately 390 px), tablet
(768–1024 px), and laptop (1280–1440 px) widths. It is a source-level and route-contract
audit: this server image does not contain Chromium, Playwright, or Selenium, so screenshot
comparison and real-device interaction remain a release check rather than a completed claim.

## Results

| Area | Phone | Tablet | Laptop | Notes |
| --- | --- | --- | --- | --- |
| Main dashboard | Dedicated mobile route below 760 px | Coarse-pointer rules present | Primary layout | Route and frontend contracts pass |
| Index hub | Single-column breakpoint | Responsive card grid | Multi-column grid | Viewport and breakpoint present |
| Index details | Single-column cards and stats | One/two-column transition | Four-column holdings | Shared page now has 44 px controls and narrow-screen padding |
| Set Explorer | Scrollable wide data region | Breakpoint present | Full data layout | Needs screenshot verification |
| Budget Builder | Scrollable 820 px comparison region | Breakpoint present | Full comparison layout | Duplicate-name behavior is regression-tested |
| Sealed Deals | Scrollable 980 px table region | Breakpoint present | Full table | Needs touch/scroll verification |
| Collector and account pages | Narrow-screen breakpoints present | Responsive panels | Full panels | Route contracts pass |
| Operator upload/supplier pages | 640/920/960 px breakpoints present | Responsive forms | Full forms/tables | Authentication behavior covered separately |

Every dashboard HTML document has a viewport meta tag and at least one responsive media
query. Static duplicate-ID scanning found no repeated IDs after correcting the shared index
template. Wide tables intentionally retain a minimum width inside horizontally scrollable
containers; these should be checked for discoverability and sticky-column behavior on real
touch devices.

## Index-page fixes from this pass

- Removed hard-coded example market values and the fake chart fallback. An API failure now
  shows an explicit unavailable state instead of plausible stale numbers.
- Made all 100/151 holdings and methodology labels derive from `constituent_limit`.
- Added phone-specific panel/chart/card spacing and 44 px minimum button height.
- Reused the shared HTML escaping utility and kept exactly one included-set grid.

## Release-device checklist

- Load the main dashboard and every secondary route at 390, 768, 1024, and 1440 px.
- Confirm no page-level horizontal overflow; test intended table scrolling independently.
- Open filters, account menus, methodology folds, chart controls, and card/set links by touch.
- Confirm Plotly mode controls remain reachable and do not cover chart labels.
- Test long Japanese set/card names and missing card/set images.
- Record screenshots or defects here before calling the visual pass complete.
