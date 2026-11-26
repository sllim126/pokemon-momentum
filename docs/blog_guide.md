# Writing blog posts for poke6s.com

How any agent (Claude Code, Codex, others) drafts a post for the owner. The mechanics of
articles (front matter, `[[market]]` boxes, images) are in `storefront/README.md` "Articles";
this file covers how to write one and get it out. Topics and dates: `docs/todo.txt`, "Blog posts".

Last reviewed: 2026-11-26.

## The arrangement

- The agent drafts from fresh price data; the owner proofreads, adds jokes and their own voice,
  and decides when it goes live. 2–3 posts a week through the end of 2026.
- Draft a few days before the post's date, as `draft: true`, on a branch (`blog/<short-name>`).
- Merge to `main` and deploy only when the owner says so ("merge and deploy"). A draft is
  readable by anyone with its link (the owner is fine with that), but it isn't listed.
- If the data turns out boring, say so and suggest swapping the topic instead of padding it.

## Writing

- Plain, direct sentences in the owner's voice: first person, a collector talking to
  collectors. Lead with the answer ("Short answer: no"), then the numbers behind it.
- **No filler.** Cut lines that sound written by an AI: "That changes how you'd plan it.",
  "Let's dive in", "It's worth noting", "In conclusion", tidy summaries of what was just said.
  If a sentence only adds tone, delete it. The owner adds the humour; leave room, don't fake it.
- Headings that match the question being answered. If the intro promises two questions, each
  section heading states its question ("Question 1: Do you need the bulk, or just the secret
  rares?"), in the order the intro gave them.
- Short lists of numbers beat long paragraphs of them. Round sensibly ($372, −17%, about 1,000).
- No Markdown tables: the article renderer is plain CommonMark and prints them as raw `|` text.
  Use a list, one item per row ("**Name:** $166, $4.60 a pack, 46¢ a card.").
- Say what the data is, once, in a short italic note: the source (TCGplayer market price), the
  date range, what was included. Readers trust the post more when they can see the method.
- Link to the site's other posts where they're relevant, with the article address
  (`/articles/<slug>`), and add a link back from the older post if it helps the reader.
- The description (front matter) is one or two sentences, under about 160 characters: what the
  post found, not what it's about.

## Prices in the text vs. the boxes

- Numbers in the text are fixed: give the date they're from.
- `[[market ids]]` rows show **today's** price and 30-day change on every page view. They have
  no caption, so the post says once, near its first prices, something like: *"The prices in the
  text are TCGplayer market prices as of November 20, 2026. The boxes below show today's prices
  and update every day."* Don't repeat that note under each row.
- Use `[[market]]` for products the post talks about (sealed or singles), `[[cards]]` when the
  point is the art (pictures, names and sets only).
- Rows of 3, 4 or 6 products lay out cleanly (3 and 6 go three to a row, 4 fit on one row).
  Five leaves a gap; add or drop one if it makes sense.
- Product names can link to TCGplayer through the affiliate link: `[Perfect Order](tcgplayer:672401)`.
  Add one line to the italic data note saying they're affiliate links (Poke6s may earn a small
  commission). Products we have in stock at or below TCGplayer's market price link to our own shop page
  automatically (whichever is cheaper wins), so the tag can be used for any product.
- Single cards must be in `data/storefront/market_reference.json` to show. The nightly build
  picks up IDs from article tags; to see them at once, rebuild it:
  `docker-compose exec -T pokemon-momentum python scripts/build_storefront_market_reference.py`.

## Data

- Prices: `/app/data/processed/prices_db.duckdb` in the `pokemon-momentum` container. Main
  tables: `pokemon_prices_named` (productId, subTypeName, date, marketPrice, groupName,
  productName), `pokemon_products`, `pokemon_groups`, and `pokemon_jp_*` for Japanese. Daily
  history from 2024-02-08. Query it by writing a small `.py` file in the repo folder and running
  it in the container (`docker-compose exec -T pokemon-momentum python <file>`); delete the
  file afterwards. Don't commit query scratch files or anything under `data/`.
- Graded prices (PriceCharting) must link back to PriceCharting wherever they're shown.
- Graded prices don't rise steadily with grade: PSA 1s often sell above mid grades. That's
  real, not an outlier.
- Check the newest date in the data before writing "today" or "as of".
- Facts from outside the data (promotion dates, release dates) come from the source, and the
  owner may correct them; e.g. TCGplayer Cyber Weekend 2026 is Nov 27–30.

## Pictures and charts

- No pictures unless the owner asks. When they do:
  - **Official Pokémon art never goes in the public repo.** The owner saves art to
    `storefront/content/images/` (git-ignored); store it with `storefront/app/images.py`
    `store_original(Path('/app/data/storefront/images'), base, bytes, ext)` inside the
    container, which returns the name to use (`/media/1200/<name>.webp`, or `cover: <name>`).
    Covers show at 16:9.
  - Card and product pictures come from the `[[market]]` / `[[cards]]` boxes (TCGplayer's own
    images), not uploaded copies.
- Charts: write a script in `scripts/blog/` (start from `master_sets_part2_charts.py`), render
  with matplotlib in the container, store the PNGs as above. **No chart titles or subtitles**:
  the text around the chart says what it shows. Use the site's dark colours and check the
  palette (see that script's docstring). Give each chart alt text.

## Getting it out

1. Branch, write `storefront/content/articles/<slug>.md` with `draft: true` and the planned
   date, commit (stage only the article and any script; never `git commit -a`).
2. Tell the owner it's ready, with the address (`https://poke6s.com/articles/<slug>`), the main
   findings, and anything you weren't sure about.
3. On "merge and deploy": merge to `main` with `--no-ff`, push, run
   `deploy/update_storefront.sh`, then check the live page with curl (it answers, the draft
   note shows, the boxes render).
4. The owner reads the draft (each draft page lists the other drafts and scheduled posts in a
   "Not live yet" box) and edits it, or asks for changes.
5. When the owner approves: set `draft: false`, keep the date, merge and deploy. A post with a
   future date is scheduled and goes live by itself on that date (Mountain time); check the page
   says "Scheduled: goes live on <date>". Move the item to Recently Completed in `docs/todo.txt`.
   Discord #general-site-updates gets a "New on the blog" message by itself at 8:05 am Mountain
   on the day it goes live, or the next morning if it went live later in the day ( `scripts/utilities/announce_articles_discord.py`).
6. Reader feedback (often pasted from Discord) is applied the same way: branch, edit, wait for
   "merge and deploy".
