from __future__ import annotations

import os
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path


@dataclass
class Settings:
    db_url: str = "sqlite:///./card_printing.db"
    timezone: str = "America/Los_Angeles"
    export_dir: Path = field(default_factory=lambda: Path("exports"))
    fuzzy_threshold: float = 0.92

    @classmethod
    def from_env(cls) -> "Settings":
        db_url = os.getenv("CARDS_DB_URL", cls.db_url)
        timezone = os.getenv("CARDS_TIMEZONE", cls.timezone)
        default_export = Path("exports")
        export_dir = Path(os.getenv("CARDS_EXPORT_DIR", str(default_export)))
        fuzzy_threshold = float(os.getenv("CARDS_FUZZY_THRESHOLD", cls.fuzzy_threshold))
        settings = cls(
            db_url=db_url,
            timezone=timezone,
            export_dir=export_dir,
            fuzzy_threshold=fuzzy_threshold,
        )
        settings.export_dir.mkdir(parents=True, exist_ok=True)
        return settings


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings.from_env()


__all__ = ["Settings", "get_settings"]
