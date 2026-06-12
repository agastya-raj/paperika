# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Runtime

Use the repo-local `.venv` for every real run. It is created by `./scripts/bootstrap_runtime.sh` (idempotent: installs `-e .[dev]` and Playwright Chromium). Activate with `. .venv/bin/activate` before invoking `python -m paperika ...`. `python -m paperika doctor` is the non-mutating readiness check (reports Playwright import, CDP reachability, runtime dirs, DB state).

Python is pinned `>=3.11`. Per user preference, prefer `uv` for ad-hoc installs, but this project's canonical flow is the bootstrap script + activated `.venv` because Playwright browsers must be installed against that interpreter.

## Common commands

```bash
./scripts/bootstrap_runtime.sh           # provision .venv
. .venv/bin/activate
python -m paperika doctor                # readiness JSON
python -m paperika init-db
python -m paperika locate "<query>" --provider crossref
python -m paperika enqueue-download "<doi|url|freeform>"
python -m paperika process-request <id>
python -m paperika run-worker-once       # scheduler-friendly batch
python -m paperika retry-pending         # raw outcomes list
pytest                                   # full suite (testpaths=tests, pythonpath=src)
pytest tests/test_downloader_logic.py::<name>  # single test
```

Hermes wrappers in `scripts/hermes_*.sh` only select the repo `.venv` and forward to `python -m paperika`; do not duplicate business logic there.

## Runtime state (outside repo)

Defaults, overridable via env vars:

- `PAPERIKA_DB_PATH` → `~/.hermes/paper_pipeline/papers.db`
- `PAPERIKA_DOWNLOAD_DIR` → `/home/agastya/Downloads/papers/`
- `PAPERIKA_SCREENSHOT_DIR` → `~/.hermes/paper_pipeline/screenshots/`
- `PAPERIKA_NOTIFICATION_DIR` → `~/.hermes/paper_pipeline/events/`
- `PAPERIKA_CHROME_CDP_URL` → `http://127.0.0.1:9222`
- `PAPERIKA_DISCOVERY_SHORTLIST_SIZE`

The downloader attaches to an already-running local Chrome (`google-chrome --remote-debugging-port=9222`); it must never close the user's Chrome session.

## Architecture

Two workflows share one SQLite-backed request/attempt model:

1. **Locate** (`locator.py`) — metadata lookup via pluggable providers (`mock`, `crossref`). Returns normalized candidates; `normalize.py` owns identifier/title canonicalization used by both locator and downloader.
2. **Download** (`downloader.py`, ~970 lines — the heart of the project) — local-Chrome-assisted PDF fetch with a strict priority order: dedupe against verified PDFs in DB → inspect existing Chrome tabs → identity verification (DOI/URL/title/identifier overlap) → click PDF viewer controls (including viewer-frame selector scanning) → open target URL → DOI resolution retry. Includes CDP download-behavior setup, fallback materialization when Playwright `save_as()` yields an empty file after a native Chrome download, and PNG failure screenshots (with `.txt` fallback when browser automation is unavailable).

Orchestration layers on top:

- `models.py` / `db.py` — request, attempt, and verified-PDF schema; dedupe lookups.
- `retry.py` — due-work selection by backoff.
- `worker.py` — `run_worker_once` glues retry selection → `process_request` → notification emission; this is the scheduler entry point.
- `notifications.py` — emits structured JSON event files (`first_failure`, `first_success_after_failure`, `manual_intervention_needed`, `final_failure`) to the notification dir. Deliberately decoupled from any Telegram/Hermes transport.
- `runtime_check.py` — backs `doctor`; pure inspection, no side effects.
- `cli.py` / `__main__.py` — thin argparse dispatcher over the modules above.

When modifying the downloader, preserve the priority order and the "never close Chrome" invariant. When adding new state transitions, consider whether a new notification event type is warranted and update `notifications.py` + `test_notifications.py` together.

## Git-clean model

Runtime state (DB, downloads, screenshots, events) lives outside the repo. `.venv/` is inside the repo but gitignored. Keep the downloader scoped to local-Chrome-assisted flows — no broad scraping.

## Validation notes

`docs/e2e-notes.md` holds the latest real-run notes. Playwright/CDP flows emit upstream Node deprecation warnings during live runs; these are expected.
