# paperika

paperika is a standalone Python MVP for two paper-handling skills:

1. Remote-only paper discovery/lookup.
2. Local Chrome-assisted downloading.

The repo is intentionally standalone first, with runtime state kept outside the repository so it stays git-clean and can later be synced to GitHub safely.

## Runtime defaults

- PDF download directory: `/home/agastya/Downloads/papers/`
- SQLite database: `~/.hermes/paper_pipeline/papers.db`
- Failure/match screenshots and artifacts: `~/.hermes/paper_pipeline/screenshots/`
- Local Chrome CDP endpoint: `http://127.0.0.1:9222`

Override with environment variables:

- `PAPERIKA_DB_PATH`
- `PAPERIKA_DOWNLOAD_DIR`
- `PAPERIKA_SCREENSHOT_DIR`
- `PAPERIKA_CHROME_CDP_URL`
- `PAPERIKA_DISCOVERY_SHORTLIST_SIZE`

## Architecture

### 1) Locator skill

`paperika.locator` defines:

- `LocatorAdapter` interface
- `MockLocatorAdapter` for usable offline/local testing
- `CrossrefAdapter` for practical remote metadata lookup over HTTP
- `LocatorService` returning sparse/nullable structured results

Modes supported by CLI and result model:

- `lookup`
- `discover`
- `auto`

Discovery shortlist size defaults to 8.

Returned fields are intentionally sparse and nullable. Canonical URL prefers publisher-style landing pages when available, while alternate/open/PDF links are stored separately. Output includes `best_first_paper`.

### 2) Downloader skill

`paperika.downloader.Downloader` accepts raw free-form input and infers:

- DOI
- URL
- title residue
- probable PDF/viewer status
- publisher hint

It does not do broad remote discovery. Allowed lightweight resolution includes DOI resolution and URL normalization.

Fallback ladder:

1. Soft dedupe against verified local PDFs in DB.
2. Inspect existing local Chrome tabs over CDP using Playwright.
3. If a matching Chrome PDF viewer tab is found, verify identity quickly and click the viewer download button.
4. If no matching tab exists but an input URL is present, open that target in local Chrome and try the viewer download button.
5. If DOI exists, resolve DOI to a landing page/resource and retry the local Chrome route.
6. If all fail, schedule retry with backoff or move to manual intervention / permanent failure.

The downloader scans existing Chrome tabs first and uses local Chrome for the actual download flow. It does not perform broad remote search/discovery or raw PDF scraping in the downloader path.

## Retry model

Backoff schedule:

- attempt 1 -> 5 minutes
- attempt 2 -> 10 minutes
- attempt 3 -> 20 minutes
- ... doubling up to attempt 10 max

State is tracked in relational tables:

- `papers`
- `paper_links`
- `paper_requests`
- `paper_attempts`

Manual intervention state stores:

- reason
- screenshot/artifact path
- page title
- current URL
- suggested next action

## Installation

```bash
cd /home/agastya/paperika
python3 -m venv .venv
. .venv/bin/activate
pip install -e .[dev]
python -m playwright install chromium
```

For local Chrome control, launch Chrome with remote debugging enabled, for example:

```bash
google-chrome --remote-debugging-port=9222
```

## CLI usage

Initialize DB:

```bash
paperika init-db
```

Locate papers:

```bash
paperika locate "transformer interpretability" --mode discover --provider mock
paperika locate "attention is all you need" --provider crossref
```

Queue a download request:

```bash
paperika enqueue-download "10.1145/3544548.3580875"
paperika enqueue-download "https://arxiv.org/pdf/1706.03762.pdf"
paperika enqueue-download "Attention Is All You Need https://arxiv.org/abs/1706.03762"
```

Process one request:

```bash
paperika process-request 1
```

Retry pending work:

```bash
paperika retry-pending
```

## Repository layout

```text
src/paperika/
  cli.py
  config.py
  db.py
  downloader.py
  locator.py
  models.py
  normalize.py
  retry.py
tests/
```

## Safety / git-clean model

- Runtime state defaults outside the repo.
- DBs, downloads, screenshots, virtualenvs, caches, and environment files are gitignored.
- This MVP avoids broad scraping/discovery inside the downloader path.
- Locator and downloader are separated so local browser automation stays scoped.

## Current MVP status

Working now:

- project scaffold and install metadata
- SQLite init/migration layer
- runtime config helpers
- structured locator service with mock + Crossref adapters
- downloader enqueue/process flow
- soft dedupe against verified PDFs
- retry scheduling/state transitions
- failure artifact capture
- Playwright-over-CDP local Chrome path for existing-tab inspection and PDF viewer download attempt
- test coverage for core non-live logic

Conservative/stubbed or next-step:

- richer live discovery providers beyond Crossref
- more site-specific viewer/download button handlers
- actual notification delivery plumbing
- smarter title/identity verification heuristics
- daemonized background worker
