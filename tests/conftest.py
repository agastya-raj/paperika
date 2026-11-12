"""Suite-wide guards."""

from __future__ import annotations

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
