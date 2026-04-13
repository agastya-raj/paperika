from pathlib import Path

from paperika.config import PaperikaConfig
from paperika.db import Database
from paperika.runtime_check import collect_runtime_report, format_runtime_summary


def test_runtime_doctor_reports_ready_when_dependencies_and_db_exist(tmp_path: Path, monkeypatch):
    config = PaperikaConfig(
        db_path=tmp_path / "papers.db",
        download_dir=tmp_path / "downloads",
        screenshot_dir=tmp_path / "shots",
        notification_dir=tmp_path / "events",
    )
    config.ensure_runtime_dirs()
    db = Database.from_config(config)
    db.init()

    monkeypatch.setattr(
        "paperika.runtime_check._playwright_importable",
        lambda: {"ok": True, "error": None},
    )
    monkeypatch.setattr(
        "paperika.runtime_check._check_cdp",
        lambda url: {"ok": True, "checked_url": url, "browser": "Chrome", "web_socket_debugger_url": "ws://ok", "error": None},
    )

    report = collect_runtime_report(config)
    assert report["ready"] is True
    assert report["db"]["initialized"] is True
    assert report["directories"]["notification_dir"]["exists"] is True
    assert "ready=ok" in format_runtime_summary(report)


def test_runtime_doctor_reports_uninitialized_db(tmp_path: Path, monkeypatch):
    config = PaperikaConfig(
        db_path=tmp_path / "papers.db",
        download_dir=tmp_path / "downloads",
        screenshot_dir=tmp_path / "shots",
        notification_dir=tmp_path / "events",
    )
    config.ensure_runtime_dirs()
    monkeypatch.setattr(
        "paperika.runtime_check._playwright_importable",
        lambda: {"ok": True, "error": None},
    )
    monkeypatch.setattr(
        "paperika.runtime_check._check_cdp",
        lambda url: {"ok": False, "checked_url": url, "browser": None, "web_socket_debugger_url": None, "error": "offline"},
    )

    report = collect_runtime_report(config)
    assert report["ready"] is False
    assert report["db"]["initialized"] is False
    assert report["chrome_cdp"]["ok"] is False
