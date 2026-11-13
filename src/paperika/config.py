from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os


# Runtime state lives under XDG state, NOT under ~/.hermes: that directory
# belongs to the Hermes agent, and a Hermes update/cleanup on 2026-10-07 wiped
# the bridge's papers.db with it (every download 500'd for days, AGA-1250).
DEFAULT_STATE_DIR = Path.home() / ".local" / "state" / "paperika"
DEFAULT_DB_PATH = DEFAULT_STATE_DIR / "papers.db"
DEFAULT_DOWNLOAD_DIR = Path.home() / "Downloads" / "papers"
DEFAULT_SCREENSHOT_DIR = DEFAULT_STATE_DIR / "screenshots"
DEFAULT_NOTIFICATION_DIR = DEFAULT_STATE_DIR / "events"
DEFAULT_CDP_URL = "http://127.0.0.1:9222"
# AGA-1702: the browser the bridge's tier-3 executors drive: "local" (the managed
# Chrome over CDP) or "tcd" (the TCD browser through the tcd-browser CLI, used only
# while it is ready; otherwise the bridge falls back to local).
DEFAULT_TIER3_BROWSER = "local"


@dataclass(slots=True)
class PaperikaConfig:
    db_path: Path = DEFAULT_DB_PATH
    download_dir: Path = DEFAULT_DOWNLOAD_DIR
    screenshot_dir: Path = DEFAULT_SCREENSHOT_DIR
    notification_dir: Path = DEFAULT_NOTIFICATION_DIR
    chrome_cdp_url: str = DEFAULT_CDP_URL
    discovery_shortlist_size: int = 8
    tier3_browser: str = DEFAULT_TIER3_BROWSER
    human_check_wait_seconds: int = 180
    human_check_retry_seconds: int = 900
    human_check_max_age_hours: int = 24

    @classmethod
    def from_env(cls) -> "PaperikaConfig":
        return cls(
            db_path=Path(os.getenv("PAPERIKA_DB_PATH", DEFAULT_DB_PATH)),
            download_dir=Path(os.getenv("PAPERIKA_DOWNLOAD_DIR", DEFAULT_DOWNLOAD_DIR)),
            screenshot_dir=Path(os.getenv("PAPERIKA_SCREENSHOT_DIR", DEFAULT_SCREENSHOT_DIR)),
            notification_dir=Path(os.getenv("PAPERIKA_NOTIFICATION_DIR", DEFAULT_NOTIFICATION_DIR)),
            chrome_cdp_url=os.getenv("PAPERIKA_CHROME_CDP_URL", DEFAULT_CDP_URL),
            discovery_shortlist_size=int(os.getenv("PAPERIKA_DISCOVERY_SHORTLIST_SIZE", "8")),
            tier3_browser=os.getenv("PAPERIKA_TIER3_BROWSER", DEFAULT_TIER3_BROWSER),
            human_check_wait_seconds=int(os.getenv("PAPERIKA_HUMAN_CHECK_WAIT_SECONDS", "180")),
            human_check_retry_seconds=int(os.getenv("PAPERIKA_HUMAN_CHECK_RETRY_SECONDS", "900")),
            human_check_max_age_hours=int(os.getenv("PAPERIKA_HUMAN_CHECK_MAX_AGE_HOURS", "24")),
        )

    def ensure_runtime_dirs(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.screenshot_dir.mkdir(parents=True, exist_ok=True)
        self.notification_dir.mkdir(parents=True, exist_ok=True)


def get_default_config() -> PaperikaConfig:
    config = PaperikaConfig.from_env()
    config.ensure_runtime_dirs()
    return config
