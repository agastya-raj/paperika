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


@dataclass(slots=True)
class PaperikaConfig:
    db_path: Path = DEFAULT_DB_PATH
    download_dir: Path = DEFAULT_DOWNLOAD_DIR
    screenshot_dir: Path = DEFAULT_SCREENSHOT_DIR
    notification_dir: Path = DEFAULT_NOTIFICATION_DIR
    chrome_cdp_url: str = DEFAULT_CDP_URL
    discovery_shortlist_size: int = 8

    @classmethod
    def from_env(cls) -> "PaperikaConfig":
        return cls(
            db_path=Path(os.getenv("PAPERIKA_DB_PATH", DEFAULT_DB_PATH)),
            download_dir=Path(os.getenv("PAPERIKA_DOWNLOAD_DIR", DEFAULT_DOWNLOAD_DIR)),
            screenshot_dir=Path(os.getenv("PAPERIKA_SCREENSHOT_DIR", DEFAULT_SCREENSHOT_DIR)),
            notification_dir=Path(os.getenv("PAPERIKA_NOTIFICATION_DIR", DEFAULT_NOTIFICATION_DIR)),
            chrome_cdp_url=os.getenv("PAPERIKA_CHROME_CDP_URL", DEFAULT_CDP_URL),
            discovery_shortlist_size=int(os.getenv("PAPERIKA_DISCOVERY_SHORTLIST_SIZE", "8")),
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
