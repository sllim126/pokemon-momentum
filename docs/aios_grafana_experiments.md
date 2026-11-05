# AIOS Experiments in Grafana

Last reviewed: 2026-11-05

An experimental way to *show* the AIOS task registry in Grafana. It sits at the bottom of the
**Pokemon Momentum: Overview** dashboard as a collapsed row named
"AIOS Experiments (placeholders, not the task system)".

**Grafana is not the task system.** The Markdown-based AIOS task registry is the source of
truth. Grafana only displays what the registry says, and nothing should be entered in Grafana.
Today the panels are placeholders: no data is connected.

## What was added (2026-11-05)

- **One collapsed row** at the bottom of the Overview. It starts closed, so the panels above it
  don't move, and its panels don't load until you open it.
- **Six placeholder panels** inside it (Grafana text panels). Each one says what it will list,
  its feed key and its columns:

  | Panel | Feed key |
  | --- | --- |
  | Today's priorities | `priorities` |
  | Active projects | `active_projects` |
  | Blocked or waiting tasks | `waiting` |
  | Decisions requiring attention | `decisions` |
  | Recently completed work | `completed` |
  | Documentation and BookStack follow-ups | `docs_followups` |

- **Where it's defined:** `deploy/monitoring/build_dashboards.py`, in the section headed
  "AIOS Experiments". The six sections are listed in `AIOS_SECTIONS`, and each panel carries
  its key as `options.aiosFeedKey` so a script can find it.

The rest of the Overview is unchanged: the 24 existing panels are identical before and after.

## Switching it off, or removing it

- **Hide it:** leave the row collapsed. It's closed by default.
- **Remove it:** set `AIOS_EXPERIMENTS = False` in `build_dashboards.py`, then run
  `python3 deploy/monitoring/build_dashboards.py`. Grafana drops the row within 30 seconds.
- **Undo completely:** revert the commit that added it.

## The feed format (`aios-feed/1`)

Something on the registry side (not built yet) reads the Markdown registry and writes one JSON
file in this shape:

```json
{
  "schema": "aios-feed/1",
  "generated_at": "2026-11-05T10:00:00Z",
  "source": "where the registry lives, and its commit or file date",
  "sections": {
    "priorities":      [ { "title": "Example task", "project": "Example", "status": "next",
                           "owner": "owner", "due": "2026-11-06", "updated": "2026-11-05",
                           "link": "https://…", "note": "" } ],
    "active_projects": [],
    "waiting":         [],
    "decisions":       [],
    "completed":       [],
    "docs_followups":  []
  }
}
```

Rules:

- Each section is a list of items. Every item may have the same eight fields: `title`,
  `project`, `status`, `owner`, `due`, `updated`, `link` and `note`. Only `title` is required.
  Dates use `YYYY-MM-DD`.
- A section can be empty, but its key should always be present, so a panel can tell "nothing
  to show" from "feed broken".
- Keep the keys stable. Panel titles can change freely; the keys are the contract.
- The registry stays the only place items are created and edited. The feed is regenerated
  from it, never edited by hand.
- **The GitHub repository is public**, so the feed file must not be committed to it. Keep it
  outside the repo, or in a git-ignored path.

## How to connect real data later

There are two steps, and both are reversible.

1. **Generate the feed.** A small script turns the registry into the JSON above and writes it
   somewhere Grafana can read. That script belongs with the registry, on the machine that
   holds the ISI-Knowledge repository; see "Planned design" below for the agreed locations.

2. **Show it in Grafana.** Choose one of these:
   - **Lists (recommended):** install the free, open-source **Infinity** data source plugin
     (`sudo grafana-cli plugins install yesoreyeram-infinity-datasource`, then restart
     grafana-server), which reads JSON from a URL. In `build_dashboards.py`, replace each
     placeholder with a table panel: source the feed URL, root selector `sections.<key>`, and
     the eight fields as columns. A `generated_at` stat tile shows whether the feed is fresh.
     Ask the owner first: it adds a Grafana plugin, though no cost.
   - **Counts only, no plugin:** have `scripts/monitoring/collect_status.py` read the feed and
     publish `momentum_aios_items{section="<key>"}` (with optional status labels), the same way
     it already counts `docs/todo.txt`. The panels then become stat tiles: for example "3
     decisions waiting". Items can't be listed this way, only counted.

Either way, make the change in `build_dashboards.py` (not in the Grafana editor, whose
changes are lost at the next rebuild), and keep the `AIOS_EXPERIMENTS` switch working.

## Planned design (owner, 2026-11-05)

Nothing below exists yet; the placeholders stay disconnected and unchanged until it does.

- **Registry (source of truth):** `05-Projects/AIOS/Task-Registry.md` in the ISI-Knowledge
  repository, currently at `/home/adam/Downloads/ISI-Knowledge`. The repository's existing
  `TODO.md` holds only knowledgebase tasks and is not the task system.
- **Feed:** `reports/aios/grafana-feed.json` in the same repository, generated on the machine
  that holds the repository, in the `aios-feed/1` format above.
- **Into Grafana:** exposed or synced to Grafana **read-only**. Grafana never writes back.
- **No second registry** on the Grafana server: it only ever holds a read-only copy of the
  generated feed, or reads it over the network.

Still to decide when it's built: how the feed reaches Grafana (a read-only file sync or a
read-only URL), and whether the section moves to its own dashboard once it has real data.
