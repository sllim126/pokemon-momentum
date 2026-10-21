# Working on Pokemon Momentum

Instructions for anyone changing this repository: people, Claude Code, Codex or any other
coding agent. Read this first, then `docs/todo.txt`.

## Where the work is tracked

- **`docs/todo.txt` is the work list.** Start there to find what to do next, and keep it
  current while you work. Don't rely on chat history, agent memory or a tool's own task
  list: the next agent won't see them.
- When you start an item, mark it `[in progress]` and note where you are if you stop
  partway. When you finish, move it to "Recently Completed" with the date. When you spot
  new work, add it under the right section with a status tag (the tags are explained at
  the top of the file).
- Finished items move to `docs/todo_archive.txt` at the next milestone (see below).
- Project map: `docs/project_status.md`. Desktop app guide: `docs/desktop_app.md`.
  Operating steps: `docs/operator_runbook.txt`. Data flow: `docs/data_flow.txt`.

## Ground rules

- **Secrets** live in `.env` (Squarespace, Discord webhook and bot token, PriceCharting, Google, PSA).
  Never print, paste, log or commit them. When you need to show `.env`, list names only.
  Never log the PriceCharting download URL; it contains the token.
- **Data files are not commits.** Don't commit changes under `data/` or exported CSVs
  (`data/market_prices_latest.csv`, `products_*.csv`,
  `data/archive/squarespace_exports/*`) unless the owner asks.
- **The live store.** Ask before any change that reaches the live Squarespace store.
- **Outside data sources:**
  - TCGCSV: one fetch per file per day, at least 100 ms between requests (the code uses
    150 ms), and stop on HTTP 403 or 429.
  - PriceCharting: one CSV download a day, and every price shown links back to
    PriceCharting.
- **The old mobile page is frozen.** Don't change `/mobile` or the mobile handling on `/`
  without the owner's go-ahead. Its replacement, the new mobile app at `/m-next`
  (`docs/mobile_app_plan.md`), is being built with the owner's go-ahead (2026-10-21).
- **Bug reports go to Discord** (`#bug-reports`). Don't add on-site bug forms to new pages.
- **Documentation is part of the job.** A human must be able to review the code: comment
  the non-obvious parts and update the docs a change affects, in the same commit.

## Running things

- The site runs in the Docker container `pokemon-momentum`, with the repo mounted at `/app`.
- After Python changes: `docker-compose restart pokemon-momentum`. HTML, CSS and JavaScript
  changes need only a browser reload.
- Tests: `docker-compose exec -T pokemon-momentum python -m pytest -q -p no:cacheprovider tests`
  (or `make test-all`).
- Desktop pages: check at 1920×1080 and 1366×768 with no page scroll and no console errors
  (see `docs/desktop_app.md`).

## Git

- Work on a branch, not on `main`.
- Commit messages: a short imperative summary line, then a body explaining why if needed.
  End with the co-author line your tool uses (Claude uses
  `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`).
- Merge to `main` and push only when the owner asks, or when they say the codeword below.
  Never force-push `main`.

## The codeword: "milestone"

When the owner says **milestone** (or "milestone: <name>"), finish and file the work
without asking further questions. Saying the word is permission to commit, merge to
`main` and push. It is not permission for anything else, such as live store changes or
committing data files.

1. **Settle in-progress work.** Finish what's small enough to finish. For anything left
   partway, make sure it works or is switched off, and record in `docs/todo.txt` exactly
   where it stopped.
2. **Run the tests** (command above). If they fail, stop here and report the failures; do
   not merge or push a failing build.
3. **Comments.** Check that the code changed since the last milestone has clear comments
   and docstrings, and that none of them are now wrong.
4. **Docs.** Update every doc the changes affect: usually `docs/project_status.md`, and
   `docs/desktop_app.md`, `docs/operator_runbook.txt`, `docs/data_flow.txt` or `README.md`
   when behavior, routes, settings or steps changed. Update their "Last reviewed" dates.
5. **Todo list.**
   - Done items go to "Recently Completed" with the date.
   - Items finished before the previous milestone move to `docs/todo_archive.txt`, newest
     month first.
   - Add the next steps you discovered, with status tags.
   - Update the "Last updated" line with today's date and the milestone name.
6. **Commit.** Run `git status`. Stage only project files, never `.env` or the data files
   listed above. If you're on `main`, first create a branch named
   `milestone/<date>-<short-name>`.
7. **Merge and push.** Merge the branch into `main` with `--no-ff`, push `main` to
   `origin`, and return to `main`.
8. **Report back** briefly: what was committed (hashes), the test result, anything skipped
   and why, and the top `[next]` items in `docs/todo.txt`.
