# Poland Power Pulse — News & Signal Monitor

The dashboard is a single static page (`index.html`) served by GitHub Pages. A GitHub Actions workflow refreshes the live feed every 30 minutes.

## Repository layout
| Path | Purpose |
|---|---|
| `index.html` | The dashboard: News Feed, Signal Monitor, Project Milestones, Newsletter and Sources panel (v111) |
| `.github/workflows/update-feed.yml` | Runs every 30 min: fetch → enrich → commit `data/` |
| `scripts/feed_fetcher.py` | v10: reads every source (built-in + `config/additional_feed_sources.json`) and writes `data/signals.json`, `data/feed_status.json` and `data/site_watch.json` |
| `scripts/enrich_signals.py` | Opens each new article once and adds the lead text, the "What changed" line, the capacity and any delay timeline |
| `scripts/embed_config.py` | Copies `config/*.json` into `index.html` (run it after editing the config files) |
| `config/additional_feed_sources.json` | 110 additional sources (98 live, 12 reference-only) |
| `config/acronyms.json`, `search_concepts.json`, `milestone_stages.json`, `projects.json`, `curated_items.json` | Tooltips, EN/PL search, project timelines (the dashboard loads these at runtime) |
| `data/signals.json` | Live items (rewritten by the workflow) |
| `data/feed_status.json` | Created by the first run: proof of what was checked; see `additional_sources` |
| `data/site_watch.json` | Workflow memory: discovered feeds and links already seen |
| `data/headlines.json`, `data/analyst_reviews.json` | Optional: verified headlines and shared analyst reviews (start empty) |
| `.nojekyll` | Makes GitHub Pages serve the files exactly as they are |

## Setup (one time)
1. Upload every file in this folder to the repository root, keeping the folder structure. The `.github` folder and `.nojekyll` are hidden on some computers, so check they are included.
2. **Settings → Pages:** set Source to *Deploy from a branch* and Branch to `main` / root.
3. **Settings → Actions → General → Workflow permissions:** choose *Read and write*.
4. *(Recommended)* **Settings → Secrets → Actions:** add `AZURE_TRANSLATOR_KEY` and `AZURE_TRANSLATOR_REGION`. Without them, Polish live items are collected but not shown.
5. **Actions → Update live news feed → Run workflow** to start the first run now.

## Checking that it works
- **Actions tab:** the "Show run summary" step prints the number of websites read OK and the line `Additional sources checked: N / 98`.
- **`data/feed_status.json`:** `additional_sources.sources` shows, for each source, whether it was checked, how many reads worked, any errors and the items it found.
- **Dashboard header:** shows "Updated · <date>" once `data/signals.json` has been written.

## Known limits
- The Sejm API committees and Monitor Polski sources need an API integration and are not fetched.
- Paywalled sources (Argus, Montel, S&P, Parkiet, Energy Monitor, bne IntelliNews) only provide headlines.
- Some sites block automated readers or need JavaScript; the Google News searches act as a fallback for these.
- The exports work without a logo; `newsletter_assets/logo.png` is optional.
