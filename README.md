# Poland Power Pulse — News & Signal Monitor (v34 — news to 23 Oct 2026)

Static dashboard hosted on GitHub Pages.

- `index.html` – the dashboard
- `data/signals.json` – live items, refreshed every 30 min by `.github/workflows/update-feed.yml`
- `data/headlines.json` – optional verified-headline overrides
- `scripts/feed_fetcher.py` – feed fetcher (Python standard library only)
- `newsletter_assets/logo.png` – optional logo for newsletter exports
