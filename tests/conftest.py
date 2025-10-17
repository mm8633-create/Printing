import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from cards import init_db
from cards.config import get_settings
from cards import db as db_module


@pytest.fixture(autouse=True)
def configure_env(tmp_path, monkeypatch):
    db_path = tmp_path / "test.db"
    export_path = tmp_path / "exports"
    monkeypatch.setenv("CARDS_DB_URL", f"sqlite:///{db_path}")
    monkeypatch.setenv("CARDS_EXPORT_DIR", str(export_path))
    try:
        get_settings.cache_clear()
    except AttributeError:
        pass
    init_db()
    yield
    try:
        get_settings.cache_clear()
    except AttributeError:
        pass
    db_module._connection_cache.clear()
