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
    revision INTEGER NOT NULL DEFAULT 0,
    manual_reason TEXT,
    manual_screenshot_path TEXT,
    manual_page_title TEXT,
    manual_current_url TEXT,
    manual_suggested_next_action TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
)
```
`revision` (AGA-596 review round 4) is a pure optimistic-concurrency-control
token — bumped by 1 on every `update_request_status` write that actually
applies (never on a guarded no-op); NOT present in the 2026-06-13 snapshot
above, retrofitted onto pre-existing databases by
`db._ensure_request_revision_column` (idempotent `ALTER TABLE ... ADD COLUMN`,
run from `Database.init()`). See §3's `update_request_status` entry for the
CAS contract that reads/writes it.

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
- `update_request_status(request_id, status, attempt_count=None, next_retry_at=None, manual=None, paper_id=None, expected_revision=None) -> bool`
  — `paper_requests.revision` is a pure CAS token, bumped by 1 on every write
  this method actually applies (never on a guarded no-op). Returns True if the
  row was actually updated, False if a guard turned it into a no-op (AGA-596
  review round 2/3/4): a non-success status can't overwrite a row already in a
  success status (`downloaded`/`completed_deduped`/`completed`) UNLESS
  `expected_revision` is given AND still matches the row's CURRENT revision
  (compare-and-set — downloader.py's `process_request` passes the revision it
  read at the top of its own call, so a legitimate force_redownload
  reopen-then-fail applies but a stale concurrent write from elsewhere
  no-ops). Revision-based CAS (not status-string CAS, used through round 3)
  because status-only comparison has an ABA hole: two force_redownload workers
  can both read status='downloaded'; the faster one re-downloads and writes
  'downloaded' again (same string, genuinely newer row); a slower worker's
  stale expected-status check would still match. The bridge never passes
  `expected_revision`; every bridge call site relies on the plain
  success-status guard and MUST check the returned bool before emitting a
  status-change notification for a "failed" transition (a guarded no-op means
  the persisted row did not actually change).
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
  `timeout` | `executor_died` | `codex_quota` | `claude_auth` | `claude_budget` |
  `interrupted`.
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
| 0 | `elsevier_api` | `elsevier_api.attempt_elsevier_fetch` — only for `10.1016/` DOIs, only with `ELSEVIER_API_KEY` (env, else `~/.cache/secrets_env`): `GET api.elsevier.com/content/article/doi/{doi}` (`X-ELS-APIKey`, `Accept: application/pdf`, no auto-redirect; a 302/303 Location is fetched WITHOUT the key through the SSRF guard) | 30 s |
| 1 | `direct_fetch` | `direct_fetch.attempt_direct_fetch` — authenticated HTTP GET of the DOI redirect chain + a `citation_pdf_url`/per-publisher ruleset, Chrome cookies exported over CDP | 60 s |
| 2 | `scripted_browser` | `scripted_browser.attempt_scripted_fetch` — deterministic Playwright over the managed CDP Chrome (no LLM) | 45 s |
| 3 | `codex_bridge` | `executor.run_codex` — the LLM-steered browser session (`codex exec -m gpt-6.1-sol`, high effort, fast tier) | 900 s |
| 3b | `claude_bridge` | `executor.run_claude` — FALLBACK driver: `claude -p --bare --model claude-sonnet-5-5 --tools Bash --max-budget-usd 0.60`, same prompt/schema | 900 s |

**`elsevier_api` (AGA-1462):** runs BEFORE tier 1, deterministic, rate rule `(3, 500)`, own
write-ahead attempt row. It only succeeds or misses: 403/404/202/429/non-PDF/network error/
identity-gate failure (quarantined) all fall through to tier 1 unchanged, never a verdict;
a 401 also falls through and latches `elsevier_key_invalid` in bridge state (cleared by the
next 2xx/3xx from api.elsevier.com itself, before the PDF gate; a 401 from a redirect
target never latches). The key must be a plain alphanumeric token (whitespace stripped at
load); anything else is treated as absent with a warning naming the variable. Any
exception inside the fetch is a miss, so the ladder always continues and the attempt row
is finalised. The step counts as an available tier in the all-tiers-rate-limited
pre-flight when it applies. A missing key or a rate-blocked strategy skips it silently. Success carries
`X-Paperika-Tier: elsevier_api`. **Provenance:** the `paper_attempts` row with strategy
`elsevier_api` + outcome `completed` is the ledger of API-fetched papers (for purge on
termination). `/healthz` gains `elsevier: {key_present, key_invalid, attempts_today}`.

**Per-strategy rate discipline** (`limits.RATE_RULES`, `(min_spacing_seconds,
daily_cap)`): `direct_fetch` `(30, 100)`, `scripted_browser` `(90, 40)`,
`codex_bridge` `(180, 20)`, `claude_bridge` `(180, 20)`, `elsevier_api` `(3, 500)`. `check_rate(conn, strategy)` and
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
  `502 bot_wall_unclassified` (AGA-596 P1 — distinct from codex-confirmed
  `bot_wall`) instead of `selftest_required`.
- An UNEXPECTED exception escaping the ladder returns `500 bridge_error` (JSON,
  `detail` = `ExcType: message`, truncated to 300 chars; AGA-1457) instead of a
  bare plain-text 500. Non-retryable, operator-attention. The lock is released; a
  request/attempt row left `in_progress` is reaped by the startup sweep.
- Successful responses carry `X-Paperika-Tier: elsevier_api | direct_fetch | scripted_browser |
  codex_bridge | claude_bridge`.

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

## 8. Tier-3 driver chain (AGA-1460)

Tier 3 = codex (`gpt-6.1-sol`) PRIMARY, Claude Sonnet 5.5 via the Anthropic API
FALLBACK. Same prompt, same `OUTCOME_SCHEMA`, same acceptance/identity gate, same
salvage and wall handoff; each driver writes its own write-ahead attempt row
(`codex_bridge` / `claude_bridge`) under its own rate rule.

- **When claude runs:** codex unusable (no persisted `sandbox_mode`, rate-blocked,
  `codex_auth`, `codex_quota`) or codex ended `executor_died` / `timeout` (including
  a claimed download whose file is not found) with no salvageable PDF. **Never**
  after a codex verdict (`downloaded`, `bot_wall`, `paywalled_no_access`,
  `throttled`, `gave_up`, `wrong_paper`).
- **Key:** `ANTHROPIC_API_KEY` from the process env, else `~/.cache/secrets_env`,
  resolved once per process, passed only in the claude subprocess env (never argv,
  state or logs; the claude run sets `CLAUDE_CODE_SUBPROCESS_ENV_SCRUB=1` so the
  model's Bash tool cannot read it, with `--allowedTools Bash` because the scrub
  resets the permission mode; the key value is redacted from everything retained).
  No key => claude is unavailable and the codex result stands.
- **claude outcomes:** one JSON envelope. ONLY a schema-valid `structured_output`
  on a non-error envelope is a verdict; free-text `result` is never parsed. Invalid/
  missing envelope, `is_error`, refusal, or no valid structured output => `executor_died`; budget exhausted => `claude_budget`; 401 =>
  `claude_auth` (latches claude off until `POST /selftest/claude` passes). A valid
  structured outcome maps exactly like codex's.
- **Infra failures are retryable:** a run reporting `gave_up` whose FINAL notes
  (not tool inputs/outputs or earlier recovered errors) contain `INFRA_FAILURE_MARKERS` (`ERR_NAME_NOT_RESOLVED`, `net::ERR_`,
  `Target closed`, `Browser has been closed`, `connection refused`) is
  `executor_died`, not terminal `codex_gave_up`.
- **`POST /selftest/claude`:** no-browse CLI+key+schema check; persists
  `claude_last_selftest_ok/at` in bridge state. `/healthz` gains a `claude` block
  (`key_present`, `claude_usable`, `last_selftest_*`, `claude_cost_usd` month-to-date
  from `paper_attempts.cost_usd`, a new additive REAL column).

New `error_code`s: `claude_auth` (503, operator), `claude_budget` (502, retryable),
`codex_quota` (502, retryable). `executor_died` now also covers AGA-490's "claimed
download not found" (was `codex_gave_up`).

**Request budget.** KC's client timeout is 1200 s, so `/download` carries one
deadline (`app.REQUEST_BUDGET_SECONDS` = 1140) set when the ladder starts. Elsevier (30 s) + tiers
1+2 (60+45 s) run first. Let `R` = `CLEANUP_RESERVE_SECONDS` (120) and `W` =
`CLAUDE_FALLBACK_WINDOW_SECONDS` (300), `remaining` = budget left at tier-3 start:

- **claude fallback ready** (key present, claude selftest latch not failed, its rate rule
  allows, Chrome ok): codex's wall is capped to leave claude a guaranteed window even after
  a full codex TIMEOUT: `codex_wall = min(900, remaining - R - (W + R))` (495 s after the
  usual ~105 s of deterministic tiers). The first `R` covers codex's own post-run salvage
  drain + verification + Chrome recovery, the second claude's. If that cap is below
  `FALLBACK_MIN_SECONDS` (180) codex is skipped and claude runs alone.
- **claude not ready**: codex keeps `min(900, remaining - R)`.
- **either way** a first driver whose wall would be below `FALLBACK_MIN_SECONDS` is not
  run; tier 3 ends retryably (`429 cooldown`, `retry_after_seconds` 30).

The claude fallback after a codex failure re-checks the budget right before its attempt
row is created (AFTER codex's salvage and Chrome recovery, which consume budget): it runs
only when `remaining - R >= W`, i.e. its wall is at least 300 s — the same window codex's cap
reserved, so the two agree; otherwise codex's retryable failure stands and no claude row is
written. Claude's wall is `min(900, remaining - R)`; `R` covers the salvage drain (10 s) +
PDF verification (~20 s) + Chrome recovery (~70 s). If a claude run ends past the deadline,
Chrome recovery is skipped (the next request's pre-flight recovers it) so the lock is
released promptly.
`claude_budget` runs go through the same salvage path as timeouts/`executor_died`.

## `POST /admin/trial` (AGA-1461, driver bake-off)

Bearer-authed like `/download`. Runs ONE named executor against one paper to compare
drivers/models, under the same global lock (`429 busy` if held) and leaves no production
state: no dedupe/cache lookup, no Elsevier step, no tiers 1-2, nothing written to
`papers`/`paper_links`/`paper_requests` (attempt rows hang off one reserved ledger request,
`raw_input = 'paperika:trial_ledger'`, status `completed`). The accepted PDF is sha256'd,
sized, then DELETED; a gate failure is quarantined under the trial's run dir. Every trial
file operation is confined to the trial's OWN download dir, `<state dir>/trials/<trial_id>/downloads`
(the prompt's download path is redirected there via `executor.render_prompt`; locate-in-window,
salvage and quarantine all use it), so production's dir and dedupe cache are never touched.
The ledger row is inserted already `completed` in one statement. Neither executor's spawn env
carries `PAPERIKA_BRIDGE_TOKEN`, `ANTHROPIC_API_KEY` (codex) or `ELSEVIER_API_KEY`.

Request: `{doi, title, driver: "codex"|"claude", model, effort: str|null,
max_budget_usd: float|null}`. doi/title are sanitized exactly like `/download`.
Allowlists: codex models `gpt-5.6-sol`, `gpt-6.1-sol`, effort `medium|high`; claude models
`claude-sonnet-5-5`, `claude-haiku-5-5`, `claude-opus-5-5`, effort `low|medium|high`
(claude CLI `--effort <level>`); `max_budget_usd` is claude only (default 1.50, hard max
3.00). Violations: `422 invalid_driver|invalid_model|invalid_effort|invalid_budget|invalid_doi`.
Overrides default to the pinned values in `build_argv` / `build_argv_claude`
(`/download` passes none).

Same prompt, `task.json`, Chrome pre-flight, and wall (`min(900, budget - R)`); same
acceptance + identity gate; same Chrome recovery latch after a bad outcome.

Rate rules: strategies `trial_codex` / `trial_claude`, own daily cap 60 each, plus a GLOBAL
LLM spacing shared with production: `429 cooldown` (+`retry_after_seconds`) when ANY of
`codex_bridge|claude_bridge|trial_codex|trial_claude` started < 180 s ago. Production rate
rules, `/healthz` `downloads_today`, and the production spacing never count trial rows
(`/healthz` `limits.attempts_today` lists the trial strategies; `claude.trial_cost_usd`
is month-to-date trial spend). Other refusals: `503 selftest_required` (codex unusable),
`503 claude_auth` (no key / latched), `503 chrome_down`.

Response (200 for every outcome that reached a run): `{trial_id, driver, model, effort,
kind, gate: "pass"|"fail"|"not_run", gate_reason, verified_by, file_sha256, file_size,
wall_seconds, cost_usd, tokens, final_url, notes (redacted, <= 500 chars), policy_flags,
salvaged}`. `kind` is the executor `ExecResult.kind`; `cost_usd` is claude's envelope
`total_cost_usd`; `tokens` sums codex JSONL `turn.completed`/`token_count` usage (null when
absent). `policy_flags` lists `"<pattern>@<file>"` for credential/cookie/CAPTCHA-solver
patterns (`captcha`, `solver`, `2captcha`, `anticaptcha`, `capsolver`, `password`,
`document.cookie`, `Network.getAllCookies`, `Network.getCookies`, `storage_state`) found in
the run dir's model-authored scripts/logs; reported, never blocking. No secret is ever in
the response. A startup sweep closes a `running` trial row as `interrupted` without touching
any request.

## `/healthz` pinned driver models (AGA-1461)

The authenticated `/healthz` reports the pinned driver models as plain constants (no
network, no codex `models_cache.json`, which is client-version filtered and omits
`gpt-6.1-sol`): `codex.model` (`executor.CODEX_MODEL_ID`, e.g. `"gpt-6.1-sol"`),
`codex.reasoning_effort` (`executor.CODEX_REASONING_EFFORT`, `"high"`) and `claude.model`
(`executor.CLAUDE_MODEL`). The daily KC watcher compares them with the Sol tier named in the
canon; on a bump, change the constant and `POST /selftest/codex`.

## Tier 3 through the TCD browser (AGA-1702)

Tier 3's executors (codex, then the claude fallback) can drive the **TCD browser** — a
long-lived real Chrome with a shared, signed-in profile on another university host, reached
only through the `tcd-browser` CLI at `/home/agastya/.local/bin/tcd-browser` — instead of the
local managed Chrome. Elsevier, tiers 1-2, the identity gate, the outcome taxonomy and the
request budget are unchanged.

- **Setting:** `PAPERIKA_TIER3_BROWSER` = `local` (default) | `tcd` (`PaperikaConfig.tier3_browser`).
  Readiness is **per driver**: a driver is TCD-ready when the CLI exists, a cached
  `tcd-browser status` (60 s cache, 5 s timeout) reports `gateway … ok=True` and
  `browser shared=up`, and that driver's OWN `POST /selftest/tcd` passed within
  `SELFTEST_MAX_AGE_DAYS` (14). With the setting on `tcd`, tier 3:
  - codex armed and TCD-ready: today's chain in TCD mode; claude takes over on the usual
    fallback kinds only if claude is TCD-ready too, else there is no fallback in that request
    (a request never mixes a TCD run with a local one);
  - codex not TCD-ready (or unable to run) but claude TCD-ready and able to run: claude alone
    in TCD mode, codex skipped;
  - no TCD-ready driver can run: the local Chrome exactly as before, the reason logged once.
- **Prompt:** `executor.TCD_PROMPT_TEMPLATE`, static apart from `{run_dir}` like the local one
  (task data stays in `task.json`). It keeps the goal, data rule, access judgement, PDF
  finding, per-paper budget, report and `final.png`, and swaps in the CLI mechanics: challenge
  pages are waited out (10 s, at most three times, never clicked); the PDF href is opened
  directly rather than fighting an overlay; a download is taken only if it appeared after the
  executor's own action, copied with `download --latest` into `{run_dir}/tcd_download`, then
  moved to `<download dir>/<sanitized-doi>.pdf` and checked for `%PDF-`. Hard rules: no login,
  credentials, CAPTCHA, terms or forms; only doi.org and the paper's publisher; no CLI
  subcommand besides `ab`, `downloads`, `download --latest`; never the key file or cookies;
  never other tabs; never close the session.
- **Spawn:** env `TCD_BROWSER_SESSION=paperika-<request id>-<driver>` (trials
  `paperika-trial<attempt id>-<driver>`) and `AGENT_BROWSER_SOCKET_DIR=<socket dir>`. The CLI's
  `ab` drives an agent-browser daemon whose unix socket otherwise lands in `~/.agent-browser`,
  which the codex sandbox cannot write; the bridge creates a private (0700) socket dir per run,
  exclusively (an existing dir is never reused; a collision picks a new name), `<run_dir>/ab`,
  or, when a socket in it would pass the 107-BYTE unix path limit (measured with
  `os.fsencode`; budget: the daemon's 34-byte `tcd-<session[:16]>-<lease[:8]>.sock` + 4 bytes
  margin; trial run dirs are too long), a short `~/.cache/tcd_browser/paperika-ab/<8 hex>`.
  Production creates it ONCE, in the tier-3 selection, and hands it on (a claude fallback gets
  a fresh one, or no fallback); if creation fails for any reason the request runs local with
  the reason logged, and a request that then never reaches an executor removes it. A trial or
  selftest that cannot have one answers `503 tcd_unavailable` before any ledger row. The
  allocation is a `SocketDir`: the dir is created relative to a descriptor of its parent opened
  `O_DIRECTORY | O_NOFOLLOW` before any executor runs (a symlinked or non-directory fallback
  root is refused), and its device and inode are recorded. Removal goes only through that kept
  descriptor (`shutil.rmtree(name, dir_fd=...)`, symlink-attack safe; where unavailable, nothing
  is removed and it is logged) and only if the name is still that dir; the descriptor is closed
  on every path. One owner from creation on: a failure before the executor's own close
  removes the still-empty dir. Every trusted bridge call for that session (the tab read, the
  release) passes the same dir. The codex `workspace-write` sandbox gains exactly one extra writable root,
  `~/.cache/tcd_browser` (the CLI's session files, and the short socket-dir fallback). Secret
  stripping is unchanged. Claude gets no new tool.
- **Lease:** the bridge runs `tcd-browser ab --session <name> --close` (20 s timeout, best
  effort, logged) after every TCD-mode executor run, whatever its outcome or exception.
  `ab --close` always exits 0, so the close is classified by its stderr line: `released lease`
  → remove the dir; `left to expire` (the gateway kept the lease until it idles out, ~10 min;
  the CLI stopped the local daemon) → one warning, no retry, remove the dir; `no active
  session` → remove the dir only if it has no entries at all (names listed through the kept
  descriptor, no content read), else leave it and warn; anything else (non-zero exit,
  timeout, no CLI, no such line) → one retry after 2 s, then leave the dir exactly as it is and
  warn with the session and the path. The bridge reads no file in the socket dir and signals
  no process. A TCD selftest records a pass only when its own close said `released lease`.
  Cancellation: a cancelled TCD-mode executor's process group is killed and reaped (10 s)
  before the cancellation propagates, so it cannot re-open the session after the release. The
  whole close (`tcd_browser.close_session`) is shielded. When the executor already returned a
  result, a cancellation that arrives during the close is held until that result is persisted
  (the attempt row, the latch, the driver's selftest fields), then re-raised; when the executor
  itself was cancelled, the close finishes and the cancellation is re-raised. The bridge's own
  CLI calls are killed on cancellation too. Local-mode cancellation is unchanged.
- **Local Chrome:** a TCD-mode tier 3 does not require it and is never followed by the local
  Chrome recovery; the pre-flight and tier 2 keep their Chrome rules.
- **Recording and limits:** attempts are `tcd_codex` / `tcd_claude` (trials `trial_tcd_codex`
  / `trial_tcd_claude`). `limits.RATE_POOLS` makes each share the spacing and daily cap of
  the local driver's strategy (`codex_bridge`, `claude_bridge`, `trial_codex`,
  `trial_claude`); all four join the global LLM spacing. No extra attempt is ever added.
- **`POST /selftest/tcd`** (token, lock; `?driver=codex|claude`, default codex when armed
  else claude): fresh status check (`503 tcd_unavailable` when down), then one executor opens
  https://example.org through the CLI and reports, through `OUTCOME_SCHEMA` (both drivers),
  `gave_up` with notes `TCD_TITLE=` / `TCD_URL=`. The result is recorded for the driver that
  ran only (`tcd_<driver>_last_selftest_ok/at`). It passes only when (a) the run completed
  normally (that `gave_up`; never timeout, `executor_died`, auth, quota or budget), (b) the
  pair is exactly `Example Domain` and `https://example.org/` (or without the slash), and
  (c) the bridge's own `tcd-browser ab --session <name> -- get url` (20 s, made after the run
  and before the release) reads the expected URL: the two strings are public constants, (c)
  is evidence a model cannot supply. The bridge releases the lease.
- **`/healthz` `tcd` block** (authenticated tier only): `configured_mode`, `cli_present`,
  `gateway {ok, shared_up, error, probe_age_seconds}` (probed only when the configured mode is
  `tcd` and the CLI exists; null otherwise), `tcd_usable` (true when the browser answers and
  at least one driver is TCD-ready; null in local mode), `drivers.{codex,claude}`
  `{last_selftest_ok, last_selftest_at, selftest_fresh}`, `attempts_today` per TCD strategy,
  `claude_cost_usd` and
  `trial_claude_cost_usd` month-to-date.
- **Trials:** `/admin/trial` takes `browser: "local"|"tcd"` (default local, `422
  invalid_browser`); `tcd` needs that trial's own driver to be TCD-ready (`503
  tcd_unavailable`), skips the Chrome
  pre-flight, uses the trial's private download dir, and the response carries `browser`.

### The codex latch re-arms itself (AGA-1702)

When a `/download` reaches the tier-3 gate with codex off or its selftest stale
(`codex_usable` false), a selftest has run before (`last_selftest_at` set), the persisted mode
is not `bypass`, no selftest (manual or automatic) started in the last 24 h, codex's rate rule
allows it, the codex pool has room today for the selftest AND the paper attempt after it, and
the local Chrome is up, the bridge runs the selftest's `workspace-write` variant alone
(`executor.run_selftest_sandboxed`, wall capped at 180 s and within the request budget; skipped
when tier 3 would be left under `FALLBACK_MIN_SECONDS`). It never runs the bypass variant and
never persists `bypass`: a host that needs bypass still needs the manual `POST
/selftest/codex`, which is unchanged. The launch is a ledger row on the current request,
strategy `codex_selftest`, pooled with `codex_bridge`: it spends the same DAILY CAP but is
left out of every spacing computation (`check_rate`'s pooled spacing and the global LLM
spacing; spacing is about paper attempts, `limits.SPACING_EXEMPT`), resolved
`selftest_ok` / `selftest_failed` (`interrupted` on a cancel); it never carries a screenshot,
is never wall evidence, and the startup sweep closes it without salvaging through it.
`codex_rearm_attempted_at` is written before it runs. A pass sets `sandbox_mode=workspace-write`
as `POST /selftest/codex` would; a failure records `last_selftest_ok=false` and leaves
`sandbox_mode` as it was, clearing it only on `codex_auth`. It never runs from `/healthz`.
