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
import shutil
import signal
import stat
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
SESSION_URL_TIMEOUT_SECONDS = 20
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


@dataclass(eq=False)
class SocketDir:
    """A socket dir the bridge created: its path, a descriptor of its parent opened
    (O_DIRECTORY | O_NOFOLLOW) BEFORE any executor ran, and the created dir's identity.
    Removal goes through that descriptor and checks the identity, so swapping the
    parent (or the dir itself) can never redirect it. ``close`` drops the descriptor."""
    path: Path
    parent_fd: int | None
    dev: int
    ino: int

    @property
    def name(self) -> str:
        return self.path.name

    def close(self) -> None:
        if self.parent_fd is not None:
            try:
                os.close(self.parent_fd)
            except OSError:
                pass
            self.parent_fd = None


def _open_dir_nofollow(path: Path) -> int | None:
    try:
        return os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    except OSError:
        return None


def _allocate_in(parent_fd: int, parent: Path, name: str) -> SocketDir | None:
    """mkdir ``name`` exclusively, relative to ``parent_fd``, mode 0700. None when the
    name is taken: an existing dir is never reused."""
    try:
        os.mkdir(name, 0o700, dir_fd=parent_fd)
    except FileExistsError:
        return None
    fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent_fd)
    try:
        os.fchmod(fd, 0o700)
        st = os.fstat(fd)
    finally:
        os.close(fd)
    return SocketDir(parent / name, parent_fd, st.st_dev, st.st_ino)


def make_socket_dir(run_dir: Path) -> SocketDir | None:
    """Create the run's private (0700) agent-browser socket dir, exclusively and
    relative to a kept parent descriptor: ``<run_dir>/ab`` (or ``ab-<4 hex>`` on a
    collision) when a socket in it fits the unix path limit, else a fresh
    ``SOCKET_FALLBACK_ROOT/<8 hex>``. None (never an exception) when no candidate fits,
    every attempt collided, or the fallback root is a symlink or not a directory (it
    sits in executor-writable territory: refused, never followed)."""
    try:
        run_dir.mkdir(parents=True, exist_ok=True)
        if socket_dir_fits(run_dir / "ab"):
            pfd = _open_dir_nofollow(run_dir)
            if pfd is not None:
                for i in range(SOCKET_DIR_ATTEMPTS):
                    name = "ab" if i == 0 else f"ab-{secrets.token_hex(2)}"
                    made = _allocate_in(pfd, run_dir, name) if socket_dir_fits(run_dir / name) else None
                    if made is not None:
                        return made
                os.close(pfd)
        if not socket_dir_fits(SOCKET_FALLBACK_ROOT / ("0" * 8)):
            return None
        try:
            SOCKET_FALLBACK_ROOT.mkdir(parents=True, exist_ok=True, mode=0o700)
        except OSError:
            pass  # e.g. a file or a dangling symlink there: refused just below
        pfd = _open_dir_nofollow(SOCKET_FALLBACK_ROOT)
        if pfd is None:
            logger.warning("refusing agent-browser socket dir root %s: not a plain directory",
                           SOCKET_FALLBACK_ROOT)
            return None
        for _ in range(SOCKET_DIR_ATTEMPTS):
            made = _allocate_in(pfd, SOCKET_FALLBACK_ROOT, secrets.token_hex(4))
            if made is not None:
                return made
        os.close(pfd)
        return None
    except OSError as exc:
        logger.warning("could not create an agent-browser socket dir under %s: %s",
                       run_dir, type(exc).__name__)
        return None


def _still_ours(sd: SocketDir) -> bool:
    """The child name, seen through the kept parent descriptor (never followed), is
    still the very directory the bridge created."""
    try:
        st = os.stat(sd.name, dir_fd=sd.parent_fd, follow_symlinks=False)
    except OSError:
        return False
    return stat.S_ISDIR(st.st_mode) and (st.st_dev, st.st_ino) == (sd.dev, sd.ino)


def remove_socket_dir(sd: SocketDir) -> None:
    """Remove the dir the bridge created, never by path: relative to the kept parent
    descriptor, only if the name is still that dir (device + inode), with the
    symlink-attack-safe rmtree; where that is unavailable, nothing is removed and it
    is logged. The descriptor is closed on every path."""
    try:
        if sd.parent_fd is None:
            return
        if not shutil.rmtree.avoids_symlink_attacks:
            logger.warning("not removing %s: rmtree is not symlink-attack safe here", sd.path)
            return
        try:
            os.stat(sd.name, dir_fd=sd.parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            return
        if not _still_ours(sd):
            logger.warning("not removing %s: it is no longer the directory the bridge created",
                           sd.path)
            return
        try:
            parent_now = os.stat(sd.path.parent, follow_symlinks=False)
            moved = (parent_now.st_dev, parent_now.st_ino) != (
                os.fstat(sd.parent_fd).st_dev, os.fstat(sd.parent_fd).st_ino)
        except OSError:
            moved = True
        if moved:
            logger.warning("the parent of %s was moved or replaced; removing only the directory "
                           "the bridge created, through its kept descriptor", sd.path)
        shutil.rmtree(sd.name, dir_fd=sd.parent_fd)
    except OSError as exc:
        logger.warning("could not remove %s: %s", sd.path, type(exc).__name__)
    finally:
        sd.close()


def _dir_is_empty(sd: SocketDir) -> bool:
    """Entry NAMES only (no file is opened), through the kept parent descriptor, and
    only for the dir the bridge created. Any doubt reads as not empty."""
    if sd.parent_fd is None:
        return False
    try:
        fd = os.open(sd.name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=sd.parent_fd)
    except OSError:
        return False
    try:
        st = os.fstat(fd)
        if (st.st_dev, st.st_ino) != (sd.dev, sd.ino):
            return False
        return not os.listdir(fd)
    except OSError:
        return False
    finally:
        os.close(fd)


def session_env(session: str, socket_dir: SocketDir) -> dict[str, str]:
    """The executor's TCD-mode env additions: its session and its socket dir."""
    return {SESSION_ENV: session, SOCKET_DIR_ENV: str(socket_dir.path)}


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


def reset_status_cache() -> None:
    global _cached_status
    _cached_status = None


# `tcd-browser ab --close` ALWAYS exits 0 and writes exactly one of these lines to
# stderr (CLI source `ab_close`, confirmed on the bridge host):
RELEASED = "released"            # "tcd-browser: released lease <id>"
LEFT_TO_EXPIRE = "left_to_expire"  # "tcd-browser: lease <id> left to expire": the gateway
#   release failed, but the CLI stopped the local daemon and dropped the session state
NO_SESSION = "no_session"        # "tcd-browser: no active session '<name>'"
UNCLASSIFIED = "unclassified"    # non-zero exit, timeout, no CLI, or none of the above
_CLOSE_LINES: tuple[tuple[str, re.Pattern[str]], ...] = (
    (RELEASED, re.compile(r"^tcd-browser: released lease \S+", re.MULTILINE)),
    (LEFT_TO_EXPIRE, re.compile(r"^tcd-browser: lease \S+ left to expire", re.MULTILINE)),
    (NO_SESSION, re.compile(r"^tcd-browser: no active session '", re.MULTILINE)),
)


async def release(session: str, socket_dir: Path | None = None) -> str:
    """``tcd-browser ab --session <name> --close``, classified by its stderr line, never
    by the exit code. Best effort and logged; never raises an Exception. There are only
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
    if code == 0:
        for outcome, line in _CLOSE_LINES:
            if line.search(err):
                logger.info("tcd-browser release of %s: %s", session, outcome)
                return outcome
    logger.warning("tcd-browser release of %s unclassified (exit %s): %s", session, code,
                   (err or out).strip()[:200])
    return UNCLASSIFIED


async def session_url(
    session: str, socket_dir: Path | None = None
) -> tuple[str | None, str | None]:
    """The bridge's OWN read of a session's tab: ``tcd-browser ab --session <name>
    -- get url``. Returns (url, None), or (None, why) on any error or timeout; never
    raises an Exception. A session that was never opened leases a fresh blank tab
    here, so it reads as about:blank (the caller's release then closes it)."""
    try:
        code, out, err = await _run_cli(
            ["ab", "--session", session, "--", "get", "url"], timeout=SESSION_URL_TIMEOUT_SECONDS,
            socket_dir=socket_dir,
        )
    except asyncio.TimeoutError:
        return None, f"get url timed out after {SESSION_URL_TIMEOUT_SECONDS}s"
    except Exception as exc:
        return None, f"get url failed: {type(exc).__name__}"
    if code != 0:
        return None, f"get url exited {code}: {(err or out).strip()[:200]}"
    lines = [line.strip() for line in out.splitlines() if line.strip()]
    if not lines:
        return None, "get url printed nothing"
    # agent-browser may wrap output in content-boundary marker lines; the URL is the
    # line that parses as one.
    urls = [line for line in lines if re.match(r"^[a-z][a-z0-9+.-]*:", line)]
    return (urls[-1] if urls else lines[-1]), None


@dataclass(slots=True)
class Closed:
    """How a TCD session's close ended (``RELEASED`` ...) and, with ``defer_cancel``,
    a cancellation that arrived during it and is the caller's to re-raise."""
    outcome: str
    cancelled: asyncio.CancelledError | None = None


async def _close(session: str, sd: SocketDir) -> str:
    """Release, classified by the CLI's line; an unclassified release is tried once
    more after RELEASE_RETRY_DELAY_SECONDS. The socket dir is removed after
    ``released`` or ``left to expire`` (the CLI stopped the local daemon either way),
    and after ``no active session`` only when it holds no entries at all. Otherwise it
    is left exactly as it is, with one warning. The bridge reads no file in it and
    signals no process. The parent descriptor is closed on every path."""
    try:
        outcome = await release(session, sd.path)
        if outcome == UNCLASSIFIED:
            await asyncio.sleep(RELEASE_RETRY_DELAY_SECONDS)
            outcome = await release(session, sd.path)
        if outcome == RELEASED:
            remove_socket_dir(sd)
        elif outcome == LEFT_TO_EXPIRE:
            logger.warning("tcd-browser session %s: the gateway kept its lease, held until it "
                           "idles out (about 10 minutes); the local daemon was stopped", session)
            remove_socket_dir(sd)
        elif outcome == NO_SESSION:
            if _dir_is_empty(sd):
                remove_socket_dir(sd)
            else:
                logger.warning("tcd-browser has no session %s, yet %s holds entries; leaving "
                               "it as it is", session, sd.path)
        else:
            logger.warning("tcd-browser session %s was not released after two tries; "
                           "leaving %s as it is", session, sd.path)
        return outcome
    finally:
        sd.close()


async def close_session(session: str, sd: SocketDir, *, defer_cancel: bool = False) -> Closed:
    """After every TCD-mode run, shielded from cancellation as a whole (bounded by two
    release timeouts and the retry pause): ``_close``.

    A cancellation that arrives meanwhile does not interrupt any of it. It is
    re-raised once the close is done, or, with ``defer_cancel`` (the executor already
    returned a result), returned in ``Closed.cancelled`` so the caller can persist that
    result first and re-raise it afterwards."""
    task = asyncio.ensure_future(_close(session, sd))
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
