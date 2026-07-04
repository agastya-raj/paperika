# paperika bridge — verified contracts (AGA-260, §2.1 contract gate)

Recorded on branch `wip/pre-aga260-snapshot` (the behavior contract — the live
`papers.db` was produced by the dirty working tree, not by `origin/main`).
Date: 2026-06-13. DB: `~/.hermes/paper_pipeline/papers.db`.

This document is the authority for `bridge/limits.py`, `bridge/chrome.py`, and the
locate+verify pass (§2.6). Where the design's sketch differed from the live code,
the adaptation is noted here.

---

## 1. sqlite schema (`.schema`, via venv python sqlite3 — no `sqlite3` CLI on the box)

### papers
```sql
CREATE TABLE papers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT,
    normalized_title TEXT,
    doi TEXT UNIQUE,
    status TEXT NOT NULL DEFAULT 'new',
    local_pdf_path TEXT,
    verified_pdf INTEGER NOT NULL DEFAULT 0,
    canonical_url TEXT,
    open_access_url TEXT,
    year INTEGER,
    venue TEXT,
    authors_json TEXT,
    abstract TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
)
```

### paper_requests
```sql
CREATE TABLE paper_requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    raw_input TEXT NOT NULL,
    paper_id INTEGER REFERENCES papers(id) ON DELETE SET NULL,
    inferred_title TEXT,
    inferred_doi TEXT,
    inferred_url TEXT,
    source_mode TEXT NOT NULL DEFAULT 'local_download',
    status TEXT NOT NULL DEFAULT 'queued',
    priority INTEGER NOT NULL DEFAULT 100,
    force_redownload INTEGER NOT NULL DEFAULT 0,
    next_retry_at TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    manual_reason TEXT,
    manual_screenshot_path TEXT,
    manual_page_title TEXT,
    manual_current_url TEXT,
    manual_suggested_next_action TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
)
```

### paper_attempts (AFTER the bridge migration)
Original live schema lacked `started_at/finished_at/outcome/run_dir`. The design
assumes those columns. Added by guarded idempotent `ALTER TABLE ... ADD COLUMN`
(re-run at bridge startup; column-existence guard ⇒ no-op when present).
`request_id` already existed.
```sql
CREATE TABLE paper_attempts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id INTEGER NOT NULL REFERENCES paper_requests(id) ON DELETE CASCADE,
    paper_id INTEGER REFERENCES papers(id) ON DELETE SET NULL,
    attempt_number INTEGER NOT NULL,
    status TEXT NOT NULL,          -- pre-existing, NOT NULL (paperika worker semantics)
    strategy TEXT,
    message TEXT,
    screenshot_path TEXT,
    page_title TEXT,
    current_url TEXT,
    created_at TEXT NOT NULL
    -- + added by AGA-260 migration:
    , started_at  TEXT             -- write-ahead launch timestamp; spacing/cap key
    , finished_at TEXT
    , outcome     TEXT             -- bridge taxonomy: running/completed/interrupted/timeout/...
    , run_dir     TEXT
)
```
Indexes: `idx_paper_attempts_request_id (request_id)`.

`status` is NOT NULL and pre-existing (live values `downloaded`/`failed`). The
bridge keeps it satisfied by writing a mirror value alongside `outcome`
(`running`→status `running`; terminal outcomes→`downloaded`/`failed`). The bridge
TAXONOMY lives in the new `outcome` column; `status` is never reinterpreted.

### paper_links
```sql
CREATE TABLE paper_links (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    paper_id INTEGER NOT NULL REFERENCES papers(id) ON DELETE CASCADE,
    url TEXT NOT NULL,
    link_type TEXT NOT NULL,       -- canonical/pdf/open_access/alternate
    source TEXT,
    is_canonical INTEGER NOT NULL DEFAULT 0,
    confidence REAL,
    created_at TEXT NOT NULL,
    UNIQUE(paper_id, url)
)
```

---

## 2. verify / normalize signatures (the bridge calls these)

### Identity verification (§2.6) — the canonical routine
`paperika.downloader.Downloader._verify_downloaded_pdf_identity(self, parsed: ParsedInput, pdf_path: Path) -> PdfIdentityCheck`

- `Downloader.__init__(self, config: PaperikaConfig, db: Database)` — cheap, no
  browser launch; safe to instantiate purely to call the verify method.
- `PdfIdentityCheck(ok: bool, reason: str, observed_title: str | None = None, observed_doi: str | None = None)`.
- Semantics (as implemented): if a requested DOI is present ⇒ normalized-DOI exact
  match wins; else the requested DOI appearing in extracted PDF text wins (handles
  Optica license-DOI-in-bytes case); else title fuzzy fallback
  (`_title_match_strength >= 3`, or normalized requested title substring in
  extracted text). DOI match is decisive; title is the fallback. No numeric
  "threshold" knob — it is token-coverage based (`>=3` distinctive tokens,
  coverage >= 0.6). **The bridge does NOT reimplement this — it calls it.**
- ParsedInput: `ParsedInput(raw_input, title=None, doi=None, url=None, probable_pdf=False, probable_viewer=False, publisher_hint=None)`.
  The bridge builds `ParsedInput(raw_input=<doi or title>, title=<title>, doi=<normalized doi>)`.

### Normalization helpers (module-level / methods)
- `paperika.db.normalize_title(title: str | None) -> str | None` — lowercase,
  alnum+space only, whitespace-collapsed.
- `paperika.normalize.infer_input(raw_input: str) -> ParsedInput` — DOI/url/title
  inference; lowercases DOI.
- `Downloader._normalize_doi_token(value) -> str | None` — strip + lower + rstrip
  punctuation. Bridge boundary DOI normalization mirrors this (lowercase, strip
  `https://doi.org/` prefix) and additionally enforces `^10\.\d{4,9}/\S+$`.
- `paperika.downloader.DOI_PATTERN = re.compile(r"10\.\d{4,9}/[-._;()/:a-z0-9]+", re.I)`.

---

## 3. db methods the bridge uses

- `Database.from_config(config) -> Database`; `.connect()` (row_factory=Row, FK on);
  `.transaction()` context manager.
- `find_verified_pdf(doi=None, title=None) -> Row | None` — dedupe source of truth
  (`papers WHERE verified_pdf=1 AND local_pdf_path IS NOT NULL`, by DOI then
  normalized title).
- `create_request(raw_input, inferred_title, inferred_doi, inferred_url, paper_id, force_redownload=False) -> int`
  (inserts status `'queued'`).
- `update_request_status(request_id, status, attempt_count=None, next_retry_at=None, manual=None, paper_id=None)`.
- `record_attempt(request_id, paper_id, attempt_number, status, strategy, message=None, screenshot_path=None, page_title=None, current_url=None) -> int`
  — note: does NOT write the new `started_at/outcome/run_dir` columns, so the
  bridge issues its own INSERT/UPDATE for write-ahead rows (it still uses
  `record_attempt`'s table; raw SQL through `db.transaction()` for the new cols).
- `mark_paper_downloaded(paper_id, local_pdf_path)` — sets
  `local_pdf_path/verified_pdf=1/status='downloaded'`.
- `upsert_located_paper(LocateCandidate) -> int` — upsert by DOI then normalized
  title; also inserts paper_links. The bridge uses this to register a downloaded
  paper (DOI + title + canonical/pdf url), then `mark_paper_downloaded`.
- `get_request(request_id) -> Row | None`.

---

## 4. status vocabulary (live db + code)

Live `paper_requests.status` values observed: `downloaded` (35), `completed_deduped`
(1), `invalidated_wrong_pdf` (2), `retrying` (1), `superseded_by_success` (1).
Live `paper_attempts.status`: `downloaded` (26), `failed` (45); `outcome` all NULL
(new column).

The design's handler vocabulary (`in_progress`/`completed`/`failed`) does NOT
collide with these. The bridge uses, on its OWN rows:
- request status: `in_progress` (write-ahead) → `completed` | `failed`.
- attempt `outcome` (new column): `running` → `completed` | `wrong_paper` |
  `throttled` | `no_access` | `bot_wall` | `executor_gave_up` | `auth_error` |
  `timeout` | `interrupted`.
- attempt `status` (NOT NULL mirror): `running` while in-flight; `downloaded` on
  success/salvage; `failed` otherwise.

Spacing/cap (§3) count `paper_attempts` rows by **`started_at`** of ANY non-null
`outcome` (running + interrupted included). Dedupe and pre-flight rejections never
write an attempt row, so they don't start the clock.

paperika's old worker is NOT run as a service (safety rule 4), so the bridge's new
status strings cannot race the worker state machine.

---

## 5. notifications

`paperika.notifications.NotificationEvent(event_type, request_id, paper_id,
status_before, status_after, message, screenshot_path=None, current_url=None,
page_title=None, emitted_at=<iso>)` + `emit_notification_event(config, event) -> Path`
(writes `<notification_dir>/<ts>_request_<id>_<event_type>.json`).

`build_notification_event` only emits for the worker's fixed vocabulary, so the
bridge constructs `NotificationEvent` DIRECTLY with new event-type strings
(`paper_downloaded`, `executor_failed`, `verification_failed`, `download_throttled`,
`no_institutional_access`, `bot_wall`) and calls `emit_notification_event`.

`notification_dir` = `~/.hermes/paper_pipeline/events` (config default).

---

## 6. config paths (PaperikaConfig)

- `db_path` = `~/.hermes/paper_pipeline/papers.db`
- `download_dir` = `~/Downloads/papers`
- `screenshot_dir` = `~/.hermes/paper_pipeline/screenshots`
- `notification_dir` = `~/.hermes/paper_pipeline/events`
- `chrome_cdp_url` default `http://127.0.0.1:9222` — **bridge overrides to
  `http://127.0.0.1:9224`** via `PAPERIKA_CHROME_CDP_URL` env (or by constructing
  the config directly). 9224 deliberately avoids the conventional 9222.

Run dirs: `~/.hermes/paper_pipeline/codex_runs/<request_id>/`. Bridge state:
`~/.hermes/paper_pipeline/bridge_state.json` (sandbox_mode, last_selftest_*,
last recovery action).

---

## 7. Tiered ladder (AGA-339)

`/download` no longer runs a codex session for every request. Inside the
serialization lock, `_download_locked` walks a three-tier ladder, cheapest first,
short-circuiting on the first success. The gpu host has IP-based institutional
access, so most papers land on a deterministic tier without an LLM.

**Order + budgets** (module constants `app.TIER1_BUDGET` / `TIER2_BUDGET`;
tier 3 is bounded by `executor.EXEC_WALL_SECONDS`):

| Tier | Strategy label | Mechanism | Budget |
|------|----------------|-----------|--------|
| 1 | `direct_fetch` | `direct_fetch.attempt_direct_fetch` — authenticated HTTP GET of the DOI redirect chain + a `citation_pdf_url`/per-publisher ruleset, Chrome cookies exported over CDP | 20 s |
| 2 | `scripted_browser` | `scripted_browser.attempt_scripted_fetch` — deterministic Playwright over the managed CDP Chrome (no LLM) | 45 s |
| 3 | `codex_bridge` | `executor.run_codex` — the LLM-steered browser session | 480 s |

**Per-strategy rate discipline** (`limits.RATE_RULES`, `(min_spacing_seconds,
daily_cap)`): `direct_fetch` `(30, 100)`, `scripted_browser` `(90, 40)`,
`codex_bridge` `(180, 20)`. `check_rate(conn, strategy)` and
`attempts_today(conn, strategy)` consult ONLY that strategy's rows; the clocks are
independent (a recent codex attempt never blocks a direct fetch). The module-level
`MIN_SPACING_SECONDS`/`DAILY_CAP` remain the `codex_bridge` values for backward
compat, and a bare `check_rate(conn)` still defaults to the codex strategy.

**Ladder semantics**
- A tier whose strategy is rate-blocked is silently SKIPPED (no attempt row). If
  ALL three are blocked, `/download` returns `429 rate_limited` with the smallest
  `retry_after_seconds` among them.
- Each tier that runs writes a strategy-tagged write-ahead `paper_attempts` row.
  Tier 1/2 outcomes resolve as `completed` / `verify_failed` / `no_pdf` / `wall` /
  `error`. A tier-1/2 miss NEVER terminates the request — it escalates.
- A tier-1/2 `downloaded` runs the EXACT same acceptance path as codex bytes:
  `_within_download_window` containment + `_verify_and_finish` (mechanical gates +
  identity verify + quarantine-on-mismatch + link + stream). On identity-verify
  failure the candidate is quarantined and the ladder CONTINUES (a deterministic
  URL rule can grab the wrong object; terminal `wrong_paper` stays codex-only).
- **Classification authority:** only tier 3 (codex) produces the terminal taxonomy
  (`wrong_paper` / `throttled` / `paywalled_no_access` / `bot_wall` / `gave_up` /
  `auth_error` / `timeout`, §2.7). The ONE exception: if BOTH tier 1 and tier 2
  independently returned `wall` and codex is unavailable (no persisted
  `sandbox_mode`, or Chrome down), the wall evidence maps to a retryable
  `502 bot_wall` instead of `selftest_required`.
- Successful responses carry `X-Paperika-Tier: direct_fetch | scripted_browser |
  codex_bridge`.

**Pre-flight gating changes**
- The sandbox gate (`503 selftest_required` when no persisted `sandbox_mode`) gates
  ONLY tier 3 — it is checked at the escalation point, after tiers 1/2 have had
  their turn.
- Chrome pre-flight still probes, but a sick Chrome no longer fails the whole
  request: tier 1 runs cookie-less anyway, tier 2 is skipped, and tier 3 keeps the
  legacy `503 chrome_down`.
- Write-ahead bookkeeping is lazy: the first tier that actually runs materializes
  the request + run_dir, so a fully-gated request leaves no dangling `in_progress`
  row.

`/healthz` `limits` gains a per-strategy `attempts_today` map
(`{"direct_fetch", "scripted_browser", "codex_bridge"}`) alongside the existing
total `downloads_today`.
