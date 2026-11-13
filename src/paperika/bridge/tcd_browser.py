"""The TCD browser for tier 3 (AGA-1702): a long-lived real Chrome on another
university host, reached ONLY through the ``tcd-browser`` CLI installed on the
bridge host. This module is the bridge's own use of that CLI: the cached
``status`` probe behind readiness and ``/healthz``, and the lease release that
follows every TCD-mode executor run. The executor itself drives the browser
through the same CLI, from the TCD prompt in ``executor.py``.

Every call is a subprocess with a timeout, in its own process group, with the
bridge's secrets stripped from its env. Nothing here raises to the caller.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import logging
import os
from pathlib import Path
import re
import secrets
import signal
import time

from .executor import ANTHROPIC_KEY_ENV, BRIDGE_TOKEN_ENV, ELSEVIER_KEY_ENV, TCD_CACHE_DIR

logger = logging.getLogger(__name__)

# The CLI as installed on the bridge host. The TCD prompt names this exact path
# (the prompt is static), so it is a constant, not a setting; tests monkeypatch it.
TCD_BROWSER_BIN = "/home/agastya/.local/bin/tcd-browser"
# The executor's tab: every `tcd-browser ab -- <command>` call with the same name
# reuses one leased tab in the shared profile.
SESSION_ENV = "TCD_BROWSER_SESSION"

# `tcd-browser ab` drives an agent-browser daemon that keeps its unix socket in
# AGENT_BROWSER_SOCKET_DIR, else XDG_RUNTIME_DIR, else ~/.agent-browser; the last is
# NOT writable inside codex's workspace-write sandbox. In TCD mode the bridge
# therefore creates one private (0700) socket dir per run and passes it to the
# executor AND to its own trusted calls for that session (read, release), so all of
# them reach the same daemon. No new sandbox root: the dir is inside the run dir
# (codex's -C workspace), or, when that path would be too long for a unix socket,
# under SOCKET_FALLBACK_ROOT inside TCD_CACHE_DIR (the one root TCD mode adds).
SOCKET_DIR_ENV = "AGENT_BROWSER_SOCKET_DIR"
# Linux sun_path holds 108 BYTES including the terminating NUL.
SOCKET_PATH_MAX = 107
# Measured on the bridge host: with AGENT_BROWSER_SOCKET_DIR set, the daemon writes
# "tcd-<session[:16]>-<lease id[:8]>" + .config/.pid/.sock/.stream/.target/.version
# there; the socket name is 34 bytes (the CLI keeps session names ASCII). Plus a
# small margin.
SOCKET_NAME_BYTES = len("tcd-") + 16 + 1 + 8 + len(".sock")
SOCKET_NAME_BUDGET = SOCKET_NAME_BYTES + 4
SOCKET_FALLBACK_ROOT = TCD_CACHE_DIR / "paperika-ab"
SOCKET_DIR_ATTEMPTS = 5  # exclusive-create retries on a name collision
# A failed release is tried once more after this pause (see close_session).
RELEASE_RETRY_DELAY_SECONDS = 2.0

STATUS_CACHE_TTL_SECONDS = 60
STATUS_TIMEOUT_SECONDS = 5
RELEASE_TIMEOUT_SECONDS = 20
REAP_SECONDS = 5
MONOTONIC = time.monotonic  # module attribute so tests can stub the clock

# `tcd-browser status` prints, among others:
#   gateway   https://…  ok=True
#   browser   shared=up chrome=… leases={'shared': 0, 'isolated': 0}
_GATEWAY_OK = re.compile(r"^\s*gateway\b.*\bok=True\b", re.MULTILINE)
_SHARED_UP = re.compile(r"^\s*browser\b.*\bshared=up\b", re.MULTILINE)


@dataclass(slots=True)
class StatusResult:
    gateway_ok: bool
    shared_up: bool
    checked_at: float | None = None  # MONOTONIC() at probe time
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.gateway_ok and self.shared_up and self.error is None

    def age_seconds(self) -> float | None:
        return None if self.checked_at is None else MONOTONIC() - self.checked_at


_cached_status: StatusResult | None = None


def cli_present() -> bool:
    return Path(TCD_BROWSER_BIN).is_file()


def session_name(key: str, driver: str) -> str:
    """``paperika-<key>-<driver>``; ``key`` is bridge-generated (a request id, a
    trial attempt id), never external data."""
    return f"paperika-{key}-{driver}"


def parse_status(text: str) -> tuple[bool, bool]:
    """(gateway ok, shared browser up) from ``tcd-browser status`` output."""
    return bool(_GATEWAY_OK.search(text or "")), bool(_SHARED_UP.search(text or ""))


def socket_dir_fits(path: Path) -> bool:
    """Whether a daemon socket inside ``path`` stays within SOCKET_PATH_MAX BYTES."""
    return len(os.fsencode(str(path))) + 1 + SOCKET_NAME_BUDGET <= SOCKET_PATH_MAX


def _open_dir_nofollow(path: Path) -> int | None:
    try:
        return os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    except OSError:
        return None


def _create_in(parent: Path, names) -> Path | None:
    """Create the first free name of ``names`` in ``parent``: the parent is opened
    O_DIRECTORY | O_NOFOLLOW (a symlink or non-directory is refused) and the child
    created EXCLUSIVELY relative to that descriptor, mode 0700 (a taken name is never
    reused). The descriptor is closed in this function on every path."""
    pfd = _open_dir_nofollow(parent)
    if pfd is None:
        return None
    try:
        for name in names:
            if not socket_dir_fits(parent / name):
                continue
            try:
                os.mkdir(name, 0o700, dir_fd=pfd)
            except FileExistsError:
                continue
            return parent / name
        return None
    finally:
        os.close(pfd)


def make_socket_dir(run_dir: Path) -> Path | None:
    """Create the run's private agent-browser socket dir: ``<run_dir>/ab`` (or
    ``ab-<4 hex>`` on a collision) when a socket in it fits the unix path limit, else a
    fresh ``SOCKET_FALLBACK_ROOT/<8 hex>``. None (never an exception) when no candidate
    fits, every attempt collided, or a parent is a symlink or not a directory. The
    bridge never removes these dirs (see docs/bridge_contracts.md)."""
    try:
        run_dir.mkdir(parents=True, exist_ok=True)
        primary = _create_in(run_dir, (
            "ab" if i == 0 else f"ab-{secrets.token_hex(2)}" for i in range(SOCKET_DIR_ATTEMPTS)
        )) if socket_dir_fits(run_dir / "ab") else None
        if primary is not None:
            return primary
        if not socket_dir_fits(SOCKET_FALLBACK_ROOT / ("0" * 8)):
            return None
        try:
            SOCKET_FALLBACK_ROOT.mkdir(parents=True, exist_ok=True, mode=0o700)
        except OSError:
            pass  # e.g. a file or a dangling symlink there: refused by the O_NOFOLLOW open
        made = _create_in(SOCKET_FALLBACK_ROOT,
                          (secrets.token_hex(4) for _ in range(SOCKET_DIR_ATTEMPTS)))
        if made is None:
            logger.warning("no agent-browser socket dir could be created under %s",
                           SOCKET_FALLBACK_ROOT)
        return made
    except OSError as exc:
        logger.warning("could not create an agent-browser socket dir for %s: %s",
                       run_dir, type(exc).__name__)
        return None


def session_env(session: str, socket_dir: Path) -> dict[str, str]:
    """The executor's TCD-mode env additions: its session and its socket dir."""
    return {SESSION_ENV: session, SOCKET_DIR_ENV: str(socket_dir)}


def _cli_env(socket_dir: Path | None = None) -> dict[str, str]:
    env = dict(os.environ)
    for name in (ANTHROPIC_KEY_ENV, ELSEVIER_KEY_ENV, BRIDGE_TOKEN_ENV, SESSION_ENV, SOCKET_DIR_ENV):
        env.pop(name, None)
    if socket_dir is not None:
        env[SOCKET_DIR_ENV] = str(socket_dir)
    return env


def _kill_group(proc: asyncio.subprocess.Process) -> None:
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


async def _run_cli(
    args: list[str], *, timeout: float, socket_dir: Path | None = None
) -> tuple[int, str, str]:
    """Run the CLI; on overrun killpg its group and raise TimeoutError. A
    cancellation also kills (and briefly reaps) the group before it propagates: a
    stray `ab` call finishing after the bridge's release would lease a new tab."""
    proc = await asyncio.create_subprocess_exec(
        TCD_BROWSER_BIN, *args,
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
        env=_cli_env(socket_dir),
    )
    try:
        out_b, err_b = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.CancelledError:
        _kill_group(proc)
        try:
            await asyncio.wait_for(proc.communicate(), timeout=REAP_SECONDS)
        except asyncio.TimeoutError:
            pass
        raise
    except asyncio.TimeoutError:
        _kill_group(proc)
        try:
            await asyncio.wait_for(proc.communicate(), timeout=REAP_SECONDS)
        except asyncio.TimeoutError:
            pass
        raise
    return (
        proc.returncode if proc.returncode is not None else -1,
        (out_b or b"").decode("utf-8", errors="replace"),
        (err_b or b"").decode("utf-8", errors="replace"),
    )


async def probe_status(*, fresh: bool = False) -> StatusResult:
    """``tcd-browser status``, cached STATUS_CACHE_TTL_SECONDS (a failure is cached
    too, so a dead gateway is not re-probed on every request or health check)."""
    global _cached_status
    if not fresh and _cached_status is not None:
        age = _cached_status.age_seconds()
        if age is not None and 0 <= age < STATUS_CACHE_TTL_SECONDS:
            return _cached_status
    if not cli_present():
        result = StatusResult(False, False, checked_at=MONOTONIC(), error="tcd-browser CLI not found")
    else:
        try:
            code, out, err = await _run_cli(["status"], timeout=STATUS_TIMEOUT_SECONDS)
        except asyncio.TimeoutError:
            result = StatusResult(False, False, checked_at=MONOTONIC(),
                                  error=f"status probe timed out after {STATUS_TIMEOUT_SECONDS}s")
        except Exception as exc:
            result = StatusResult(False, False, checked_at=MONOTONIC(),
                                  error=f"status probe failed: {type(exc).__name__}")
        else:
            gateway_ok, shared_up = parse_status(out)
            error = None if code == 0 else f"status exited {code}: {(err or out).strip()[:200]}"
            result = StatusResult(gateway_ok, shared_up, checked_at=MONOTONIC(), error=error)
    _cached_status = result
    return result


async def ask_human_check(
    url: str, *, reason: str = "", wait_seconds: int = 0, withdraw: bool = False,
) -> str:
    """The ask CLI adapter: done=0, pending=2/4, declined=3; all other exits,
    a missing/older CLI or subprocess errors mean unavailable. Only the human
    operates the check; this call files, waits for or withdraws their request."""
    args = (["ask", "--withdraw", url] if withdraw else
            ["ask", url, "--reason", reason, "--wait", str(wait_seconds)])
    try:
        code, _out, err = await _run_cli(args, timeout=max(0, wait_seconds) + 20)
    except Exception as exc:
        logger.warning("tcd-browser ask unavailable: %s", type(exc).__name__)
        return "unavailable"
    if "unknown command" in (err or "").lower():
        # A CLI from before `ask` answers "unknown command 'ask'" with exit 2, the
        # same code as "the wait ran out" (seen on the Mac and on gpu, 13 Nov 2026).
        # Reading that as pending would park every walled paper with nothing on his
        # screen.
        return "unavailable"
    return {0: "done", 2: "pending", 3: "declined", 4: "pending"}.get(code, "unavailable")


def reset_status_cache() -> None:
    global _cached_status
    _cached_status = None


# `tcd-browser ab --close` ALWAYS exits 0 and writes exactly one of these lines to
# stderr (CLI source `ab_close`, confirmed on the bridge host):
RELEASED = "released"            # "tcd-browser: released lease <id>"
LEFT_TO_EXPIRE = "left_to_expire"  # "tcd-browser: lease <id> left to expire": the gateway
#   release failed, but the CLI stopped the local daemon and dropped the session state
NO_SESSION = "no_session"        # "tcd-browser: no active session '<name>'"
UNCLASSIFIED = "unclassified"    # anything else
_RELEASED_LINE = re.compile(r"tcd-browser: released lease [0-9a-f]{16}")
_LEFT_LINE = re.compile(r"tcd-browser: lease [0-9a-f]{16} left to expire")


def classify_close(code: int, stderr: str, session: str) -> str:
    """The close's outcome from an EXACT, whole-output match: exit 0 and a stripped
    stderr of exactly one line that fully matches one of the three known lines, the
    no-session one naming exactly the session the bridge passed. Anything else (several
    lines, extra text, another session, a non-zero exit) is UNCLASSIFIED: lease
    metadata the executor can write must not be able to forge a line."""
    text = (stderr or "").strip()
    if code != 0 or not text or "\n" in text or "\r" in text:
        return UNCLASSIFIED
    if _RELEASED_LINE.fullmatch(text):
        return RELEASED
    if _LEFT_LINE.fullmatch(text):
        return LEFT_TO_EXPIRE
    if text == f"tcd-browser: no active session '{session}'":
        return NO_SESSION
    return UNCLASSIFIED


async def release(session: str, socket_dir: Path | None = None) -> str:
    """``tcd-browser ab --session <name> --close``, classified by ``classify_close``. Best effort and logged; never raises an Exception. There are only
    four shared leases for every agent, so the bridge closes after EVERY TCD-mode run."""
    try:
        code, out, err = await _run_cli(
            ["ab", "--session", session, "--close"], timeout=RELEASE_TIMEOUT_SECONDS,
            socket_dir=socket_dir,
        )
    except asyncio.TimeoutError:
        logger.warning("tcd-browser release of %s timed out after %ss", session,
                       RELEASE_TIMEOUT_SECONDS)
        return UNCLASSIFIED
    except Exception as exc:
        logger.warning("tcd-browser release of %s failed: %s", session, type(exc).__name__)
        return UNCLASSIFIED
    outcome = classify_close(code, err, session)
    if outcome == UNCLASSIFIED:
        logger.warning("tcd-browser release of %s unclassified (exit %s): %s", session, code,
                       (err or out).strip()[:200])
    else:
        logger.info("tcd-browser release of %s: %s", session, outcome)
    return outcome


@dataclass(slots=True)
class Closed:
    """How a TCD session's close ended (``RELEASED`` ...) and, with ``defer_cancel``,
    a cancellation that arrived during it and is the caller's to re-raise."""
    outcome: str
    cancelled: asyncio.CancelledError | None = None


async def _close(session: str, socket_dir: Path) -> str:
    """Release; an UNCLASSIFIED release is tried once more after
    RELEASE_RETRY_DELAY_SECONDS. The outcome only drives logging, that retry and the
    selftest rule: the socket dir is never removed, read or listed, and no process is
    signalled."""
    outcome = await release(session, socket_dir)
    if outcome == UNCLASSIFIED:
        await asyncio.sleep(RELEASE_RETRY_DELAY_SECONDS)
        outcome = await release(session, socket_dir)
    if outcome == LEFT_TO_EXPIRE:
        logger.warning("tcd-browser session %s: the gateway kept its lease, held until it "
                       "idles out (about 10 minutes); the local daemon was stopped", session)
    elif outcome == UNCLASSIFIED:
        logger.warning("tcd-browser session %s was not released after two tries (socket dir "
                       "%s)", session, socket_dir)
    return outcome


async def close_session(
    session: str, socket_dir: Path, *, defer_cancel: bool = False
) -> Closed:
    """After every TCD-mode run, shielded from cancellation as a whole (bounded by two
    release timeouts and the retry pause): ``_close``.

    A cancellation that arrives meanwhile does not interrupt any of it. It is
    re-raised once the close is done, or, with ``defer_cancel`` (the executor already
    returned a result), returned in ``Closed.cancelled`` so the caller can persist that
    result first and re-raise it afterwards."""
    task = asyncio.ensure_future(_close(session, socket_dir))
    cancelled: asyncio.CancelledError | None = None
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as exc:
            cancelled = exc
    outcome = task.result()
    if cancelled is not None and not defer_cancel:
        raise cancelled
    return Closed(outcome, cancelled)
