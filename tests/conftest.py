"""Suite-wide guards."""

from __future__ import annotations

from pathlib import Path
import shutil
import tempfile

import pytest

from paperika.bridge import tcd_browser


@pytest.fixture(autouse=True)
def _no_real_tcd_browser_cli(tmp_path, monkeypatch):
    """AGA-1702: no test may reach the real tcd-browser CLI, even on the bridge host
    where it is installed. Every test starts with the CLI path pointing at a file
    that does not exist (TCD tests install a stub there) and an empty status cache."""
    monkeypatch.setattr(tcd_browser, "TCD_BROWSER_BIN", str(tmp_path / "no-tcd-browser"))
    tcd_browser.reset_status_cache()
    yield
    tcd_browser.reset_status_cache()


@pytest.fixture(autouse=True)
def _no_socket_dirs_in_the_real_home(monkeypatch):
    """AGA-1702: the short agent-browser socket-dir fallback lives under the real
    ~/.cache/tcd_browser in production. Tests use their own private dir, as SHORT as
    the production one (pytest's tmp_path is often too long for a unix socket), and
    remove it afterwards."""
    root = Path(tempfile.mkdtemp(prefix="pab", dir="/tmp"))
    monkeypatch.setattr(tcd_browser, "SOCKET_FALLBACK_ROOT", root / "ab")
    yield
    shutil.rmtree(root, ignore_errors=True)
