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
import subprocess
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
# A daemon left behind by a failed close: SIGTERM, wait, then SIGKILL, wait.
DAEMON_TERM_WAIT_SECONDS = 3.0
DAEMON_KILL_WAIT_SECONDS = 2.0
AGENT_BROWSER_MARK = "agent-browser"

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


def socket_dir_possible(run_dir: Path) -> bool:
    """Whether ``make_socket_dir(run_dir)`` has a candidate that fits (the primary in
    the run dir, or the fixed-length fallback)."""
    return socket_dir_fits(run_dir / "ab") or socket_dir_fits(SOCKET_FALLBACK_ROOT / ("0" * 8))


def _create_private(path: Path) -> bool:
    """Create ``path`` exclusively, mode 0700. False when it already exists: a
    leftover directory (perhaps holding a daemon's files) is never reused."""
    try:
        path.mkdir(mode=0o700)
    except FileExistsError:
        return False
    os.chmod(path, 0o700)
    return True


def make_socket_dir(run_dir: Path) -> Path | None:
    """Create the run's private (0700) agent-browser socket dir, exclusively:
    ``<run_dir>/ab`` (or ``ab-<4 hex>`` on a collision) when a socket in it fits the
    unix path limit, else a fresh ``SOCKET_FALLBACK_ROOT/<8 hex>``. None when no
    candidate fits or every attempt collided: TCD mode cannot run. ``close_session``
    removes it once no daemon is left in it."""
    run_dir.mkdir(parents=True, exist_ok=True)
    primary = run_dir / "ab"
    if socket_dir_fits(primary):
        if _create_private(primary):
            return primary
        for _ in range(SOCKET_DIR_ATTEMPTS - 1):
            path = run_dir / f"ab-{secrets.token_hex(2)}"
            if socket_dir_fits(path) and _create_private(path):
                return path
    if not socket_dir_fits(SOCKET_FALLBACK_ROOT / ("0" * 8)):
        return None
    SOCKET_FALLBACK_ROOT.mkdir(parents=True, exist_ok=True, mode=0o700)
    for _ in range(SOCKET_DIR_ATTEMPTS):
        path = SOCKET_FALLBACK_ROOT / secrets.token_hex(4)
        if _create_private(path):
            return path
    return None


def remove_socket_dir(path: Path) -> None:
    shutil.rmtree(path, ignore_errors=True)


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


def reset_status_cache() -> None:
    global _cached_status
    _cached_status = None


async def release(session: str, socket_dir: Path | None = None) -> bool:
    """Close the executor's leased tab: ``tcd-browser ab --session <name> --close``.
    Best effort and logged; never raises an Exception. There are only four shared
    leases for every agent, so the bridge calls this after EVERY TCD-mode executor
    run and never relies on the model to close its own tab. A lease the bridge
    could not close still expires after about 10 idle minutes."""
    try:
        code, out, err = await _run_cli(
            ["ab", "--session", session, "--close"], timeout=RELEASE_TIMEOUT_SECONDS,
            socket_dir=socket_dir,
        )
    except asyncio.TimeoutError:
        logger.warning("tcd-browser release of %s timed out after %ss", session,
                       RELEASE_TIMEOUT_SECONDS)
        return False
    except Exception as exc:
        logger.warning("tcd-browser release of %s failed: %s", session, type(exc).__name__)
        return False
    if code == 0:
        logger.info("tcd-browser session %s released", session)
        return True
    logger.warning("tcd-browser release of %s exited %s: %s", session, code,
                   (err or out).strip()[:200])
    return False


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


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    stat = Path(f"/proc/{pid}/stat")
    if Path("/proc").is_dir():  # Linux: a zombie is dead
        try:
            return stat.read_text().rsplit(")", 1)[1].split()[0] != "Z"
        except (OSError, IndexError):
            return False
    return True


def _cmdline(pid: int) -> str | None:
    """The process's command line (Linux /proc, else ``ps``), or None."""
    if Path("/proc").is_dir():
        try:
            raw = Path(f"/proc/{pid}/cmdline").read_bytes()
        except OSError:
            return None
        return raw.replace(b"\0", b" ").decode("utf-8", errors="replace").strip() or None
    try:
        out = subprocess.run(["ps", "-p", str(pid), "-o", "command="],
                             capture_output=True, text=True, timeout=5, check=False)
    except (OSError, subprocess.SubprocessError):
        return None
    return out.stdout.strip() or None


async def _wait_dead(pid: int, seconds: float) -> bool:
    deadline = time.monotonic() + seconds
    while _pid_alive(pid):
        if time.monotonic() >= deadline:
            return False
        await asyncio.sleep(0.1)
    return True


async def stop_leftover_daemons(socket_dir: Path) -> list[int]:
    """After a release (successful or not): for every ``*.pid`` in the run's socket
    dir whose pid is alive AND whose command line names agent-browser, SIGTERM it,
    wait, then SIGKILL it; logged. A pid whose command line does not match is never
    signalled (the daemon is gone and the pid was reused). Returns the pids that are
    still alive afterwards."""
    remaining: list[int] = []
    for pid_file in sorted(socket_dir.glob("*.pid")):
        try:
            pid = int(pid_file.read_text().strip())
        except (OSError, ValueError):
            continue
        if pid <= 1 or pid == os.getpid() or not _pid_alive(pid):
            continue
        cmd = _cmdline(pid)
        if not cmd or AGENT_BROWSER_MARK not in cmd:
            logger.warning("%s names pid %d, which is not an agent-browser process; not signalled",
                           pid_file, pid)
            continue
        logger.warning("stopping a leftover agent-browser daemon, pid %d (%s)", pid, pid_file.name)
        try:
            os.kill(pid, signal.SIGTERM)
            stopped = await _wait_dead(pid, DAEMON_TERM_WAIT_SECONDS)
            if not stopped:
                os.kill(pid, signal.SIGKILL)
                stopped = await _wait_dead(pid, DAEMON_KILL_WAIT_SECONDS)
        except ProcessLookupError:
            stopped = True
        except PermissionError:
            stopped = False
        if not stopped:
            remaining.append(pid)
    return remaining


async def _close(session: str, socket_dir: Path) -> None:
    await release(session, socket_dir)
    remaining = await stop_leftover_daemons(socket_dir)
    if remaining:
        logger.warning("agent-browser pid(s) %s could not be stopped; leaving %s in place",
                       remaining, socket_dir)
    else:
        remove_socket_dir(socket_dir)


async def close_session(
    session: str, socket_dir: Path, *, defer_cancel: bool = False
) -> asyncio.CancelledError | None:
    """After every TCD-mode run, shielded from cancellation as a whole: the release
    (same socket dir, so it reaches and stops the run's daemon), then any daemon still
    named by a pid file is stopped, then the socket dir is removed if none is left.

    A cancellation that arrives meanwhile does not interrupt any of it. It is
    re-raised once the close is done, or, with ``defer_cancel`` (the executor already
    returned a result), RETURNED so the caller can persist that result first and
    re-raise it afterwards."""
    task = asyncio.ensure_future(_close(session, socket_dir))
    cancelled: asyncio.CancelledError | None = None
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as exc:
            cancelled = exc
    task.result()
    if cancelled is not None and not defer_cancel:
        raise cancelled
    return cancelled
