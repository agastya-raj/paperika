from __future__ import annotations

import json
from pathlib import Path
import sqlite3
import sys
from urllib.error import URLError
from urllib.request import urlopen

from .config import PaperikaConfig


def collect_runtime_report(config: PaperikaConfig) -> dict[str, object]:
    playwright_importable = _playwright_importable()
    cdp_check = _check_cdp(config.chrome_cdp_url)
    db_check = _check_db(config.db_path)
    directories = {
        "db_parent": _directory_status(config.db_path.parent),
        "download_dir": _directory_status(config.download_dir),
        "screenshot_dir": _directory_status(config.screenshot_dir),
        "notification_dir": _directory_status(config.notification_dir),
    }
    ready = all(
        [
            playwright_importable["ok"],
            cdp_check["ok"],
            db_check["initialized"],
            all(item["exists"] for item in directories.values()),
        ]
    )
    return {
        "ready": ready,
        "python_executable": sys.executable,
        "playwright": playwright_importable,
        "chrome_cdp": cdp_check,
        "directories": directories,
        "db": db_check,
    }


def format_runtime_summary(report: dict[str, object]) -> str:
    checks = [
        ("playwright", report["playwright"]["ok"]),
        ("chrome_cdp", report["chrome_cdp"]["ok"]),
        ("db_initialized", report["db"]["initialized"]),
        (
            "runtime_dirs",
            all(item["exists"] for item in report["directories"].values()),
        ),
    ]
    rendered = ", ".join(f"{name}={'ok' if ok else 'fail'}" for name, ok in checks)
    return f"ready={'ok' if report['ready'] else 'fail'} ({rendered})"


def report_as_json(config: PaperikaConfig) -> str:
    report = collect_runtime_report(config)
    return json.dumps({**report, "summary": format_runtime_summary(report)}, indent=2)


def _playwright_importable() -> dict[str, object]:
    try:
        import playwright  # noqa: F401

        return {"ok": True, "error": None}
    except Exception as exc:  # pragma: no cover - exercised in tests via monkeypatch
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def _check_cdp(base_url: str) -> dict[str, object]:
    version_url = base_url.rstrip("/") + "/json/version"
    try:
        with urlopen(version_url, timeout=3) as response:
            payload = json.loads(response.read().decode("utf-8"))
        return {
            "ok": True,
            "checked_url": version_url,
            "browser": payload.get("Browser"),
            "web_socket_debugger_url": payload.get("webSocketDebuggerUrl"),
            "error": None,
        }
    except (URLError, TimeoutError, OSError, ValueError) as exc:
        return {
            "ok": False,
            "checked_url": version_url,
            "browser": None,
            "web_socket_debugger_url": None,
            "error": f"{type(exc).__name__}: {exc}",
        }


def _directory_status(path: Path) -> dict[str, object]:
    return {"path": str(path), "exists": path.exists(), "is_dir": path.is_dir()}


def _check_db(path: Path) -> dict[str, object]:
    exists = path.exists()
    initialized = False
    error = None
    tables: list[str] = []
    if exists:
        try:
            conn = sqlite3.connect(path)
            try:
                rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            finally:
                conn.close()
            tables = sorted(row[0] for row in rows)
            initialized = {"papers", "paper_requests", "paper_attempts", "paper_links"}.issubset(set(tables))
        except sqlite3.DatabaseError as exc:
            error = f"{type(exc).__name__}: {exc}"
    return {
        "path": str(path),
        "exists": exists,
        "initialized": initialized,
        "tables": tables,
        "error": error,
    }
