# Real E2E validation notes

Date: 2026-04-13

## Runtime validation

Commands run:

```bash
cd /home/agastya/paperika
./scripts/bootstrap_runtime.sh
. .venv/bin/activate
python -m paperika doctor
```

Observed:

- Playwright imported successfully from the dedicated repo runtime
- CDP endpoint at `http://127.0.0.1:9222/json/version` was reachable
- runtime directories existed
- DB was initialized
- `doctor` returned `ready: true`

## Hermes wrapper validation

Commands run:

```bash
./scripts/hermes_locate.sh "attention is all you need" --provider mock
./scripts/hermes_retry_worker.sh
./scripts/hermes_download.sh doctor
```

Observed:

- wrapper scripts invoked the repo-local `.venv`
- locate output was clean JSON
- worker output was structured JSON with notification events
- doctor output was clean JSON

## Real-paper validation

Lookup command:

```bash
python -m paperika locate "Multi-span optical power spectrum prediction using cascaded learning with one-shot end-to-end measurement" --mode lookup --provider crossref
```

Result:

- Crossref returned DOI `10.1364/JOCN.533634`

Download commands:

```bash
python -m paperika enqueue-download "10.1364/JOCN.533634" --force-redownload
python -m paperika process-request 5
file /home/agastya/Downloads/papers/10_1364_jocn_533634.pdf
pdfinfo /home/agastya/Downloads/papers/10_1364_jocn_533634.pdf
```

Observed:

- request 5 completed with status `downloaded`
- file verification reported a real PDF document, version 1.4, 11 pages
- `pdfinfo` reported the expected title: `Multi-span optical power spectrum prediction using cascaded learning with one-shot end-to-end measurement`

## DB verification

Observed request state for request 5:

- request status: `downloaded`
- attempt_count: `1`
- paper status: `downloaded`
- `verified_pdf = 1`
- local path: `/home/agastya/Downloads/papers/10_1364_jocn_533634.pdf`

## Important bug fixed during validation

Initial live runs showed that Chrome sometimes completed the native download while Playwright `download.save_as()` produced an empty target file.

Hardening added:

- detect that case
- inspect the native download directory for a newly materialized non-empty file
- copy the real browser-downloaded PDF into the stable slugged target path

That fix is what made the final DOI-driven validation produce a valid PDF at the expected slugged destination path.

## Remaining pain points

- publisher-specific viewers can still require more selectors or identity heuristics over time
- live CDP runs still emit upstream Node deprecation warnings from Playwright internals
