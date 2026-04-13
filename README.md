# paperika

paperika is a standalone Python tool for two related paper-handling workflows:

1. Remote paper discovery / metadata lookup.
2. Local Chrome-assisted PDF downloading.

The repo stays standalone and git-clean. Runtime state lives outside the repo by default, while the dedicated Python runtime lives in `./.venv`.

## Runtime defaults

- PDF download directory: `/home/agastya/Downloads/papers/`
- SQLite database: `~/.hermes/paper_pipeline/papers.db`
- Failure / match screenshots: `~/.hermes/paper_pipeline/screenshots/`
- Notification event spool: `~/.hermes/paper_pipeline/events/`
- Local Chrome CDP endpoint: `http://127.0.0.1:9222`

Override with environment variables:

- `PAPERIKA_DB_PATH`
- `PAPERIKA_DOWNLOAD_DIR`
- `PAPERIKA_SCREENSHOT_DIR`
- `PAPERIKA_NOTIFICATION_DIR`
- `PAPERIKA_CHROME_CDP_URL`
- `PAPERIKA_DISCOVERY_SHORTLIST_SIZE`

## Dedicated runtime bootstrap

Use the dedicated repo-local runtime for all real runs:

```bash
cd /home/agastya/paperika
./scripts/bootstrap_runtime.sh
. .venv/bin/activate
python -m paperika doctor
```

The bootstrap script is idempotent and does all of the following:

- creates `/home/agastya/paperika/.venv` if needed
- upgrades pip / setuptools / wheel
- installs `-e .[dev]`
- installs Playwright Chromium support

Verification command:

```bash
cd /home/agastya/paperika
. .venv/bin/activate
python -c "import playwright; print('ok')"
```

## Local Chrome setup

paperika expects to attach to an already running local Chrome session over CDP.

Example launch:

```bash
google-chrome --remote-debugging-port=9222
```

Design constraints:

- paperika inspects existing tabs first
- paperika uses local Chrome as the source of truth for the actual download action
- paperika is conservative about the attached session and does not intentionally close the whole Chrome session

## Doctor command

Check whether the runtime is really ready:

```bash
cd /home/agastya/paperika
. .venv/bin/activate
python -m paperika doctor
```

`paperika doctor` is safe and non-mutating. It reports JSON containing:

- Python executable path
- whether Playwright imports successfully
- whether the CDP endpoint is reachable
- whether runtime directories already exist
- whether the DB exists and is initialized
- an overall `ready` flag and concise summary

## Downloader behavior

The local Chrome downloader keeps this priority order:

1. dedupe against verified PDFs already in the DB
2. inspect existing local Chrome tabs first
3. quick identity verification using DOI / URL / title / identifier overlap
4. click PDF viewer download controls in the page or viewer frames
5. open the target URL in local Chrome if needed
6. resolve DOI and retry through local Chrome

Additional hardening added here:

- viewer-frame selector scanning for Chrome PDF viewer controls
- conservative CDP download-behavior setup
- fallback materialization logic when Chrome performs the native download but Playwright `save_as()` yields an empty file
- matched-page screenshots before download attempts
- PNG failure screenshots when browser automation is available, with `.txt` fallback only when it is not
- improved DOI-only and identifier-based tab matching

## Notification events

paperika now emits structured notification-event JSON files for important state changes.

Current event types:

- `first_failure`
- `first_success_after_failure`
- `manual_intervention_needed`
- `final_failure`

These are written to `~/.hermes/paper_pipeline/events/` and are kept separate from any future Telegram or Hermes delivery transport.

## One-shot worker flow

For scheduler / cron / Hermes usage:

```bash
cd /home/agastya/paperika
. .venv/bin/activate
python -m paperika run-worker-once
```

This command:

- finds due queued / retrying requests
- processes them in DB order
- returns structured JSON
- includes per-request outcomes and emitted notification events

`retry-pending` still exists and returns the raw list of outcomes. `run-worker-once` is the scheduler-friendly wrapper around that flow.

## Hermes wrapper scripts

Thin wrappers are provided so Hermes can call the dedicated runtime without duplicating business logic:

```bash
./scripts/hermes_locate.sh "attention is all you need" --provider crossref
./scripts/hermes_download.sh enqueue-download "10.1364/JOCN.533634"
./scripts/hermes_download.sh process-request 5
./scripts/hermes_retry_worker.sh
```

These wrappers only select the repo-local `.venv` and invoke `python -m paperika ...`.

## CLI usage

Initialize DB:

```bash
python -m paperika init-db
```

Locate papers:

```bash
python -m paperika locate "transformer interpretability" --mode discover --provider mock
python -m paperika locate "attention is all you need" --provider crossref
```

Queue a download request:

```bash
python -m paperika enqueue-download "10.1145/3544548.3580875"
python -m paperika enqueue-download "https://arxiv.org/pdf/1706.03762.pdf"
python -m paperika enqueue-download "Attention Is All You Need https://arxiv.org/abs/1706.03762"
```

Process one request:

```bash
python -m paperika process-request 1
```

Process due work in one shot:

```bash
python -m paperika run-worker-once
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
  notifications.py
  retry.py
  runtime_check.py
  worker.py
scripts/
  bootstrap_runtime.sh
  hermes_download.sh
  hermes_locate.sh
  hermes_retry_worker.sh
tests/
```

## Real validation notes

See `docs/e2e-notes.md` for the latest real-run notes.

Most important current result:

- the dedicated runtime now imports Playwright correctly
- `paperika doctor` reports the runtime as ready
- a real DOI-driven local-Chrome download path completed end-to-end and produced a valid PDF under `/home/agastya/Downloads/papers/`
- scheduler-friendly worker runs now emit structured notification events

## Safety / git-clean model

- runtime state stays outside the repo by default
- `.venv/` stays inside the repo but is gitignored
- downloads, DB state, screenshots, and notification events remain outside the repo
- downloader scope remains limited to local Chrome-assisted download flows rather than broad scraping

## Remaining pain points

- live browser behavior is still site-dependent and can vary across publisher viewers
- Playwright/CDP flows emit upstream Node deprecation warnings during live runs
- when Chrome saves natively, stable slugged filenames are produced by copying the native download artifact rather than replacing Chrome’s original file in-place
