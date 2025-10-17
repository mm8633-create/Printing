from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from .config import get_settings

_connection_cache: dict[str, sqlite3.Connection] = {}


def get_database_path() -> Path:
    settings = get_settings()
    url = settings.db_url
    if url.startswith("sqlite:///"):
        path = url.replace("sqlite:///", "")
        return Path(path)
    raise ValueError("Only sqlite:/// URLs are supported in this lightweight build")


def get_connection() -> sqlite3.Connection:
    db_path = str(get_database_path())
    if db_path not in _connection_cache:
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        _connection_cache[db_path] = conn
    return _connection_cache[db_path]


@contextmanager
def session_scope() -> Iterator[sqlite3.Connection]:
    conn = get_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def init_db() -> None:
    conn = get_connection()
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS batches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            label TEXT,
            source TEXT DEFAULT 'upload',
            notes TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id INTEGER NOT NULL,
            list_source TEXT NOT NULL,
            date_time_local TEXT,
            raw_payload_json TEXT NOT NULL,
            normalized_payload_json TEXT NOT NULL,
            validation_status TEXT NOT NULL,
            uncertainty_reasons TEXT NOT NULL,
            FOREIGN KEY(batch_id) REFERENCES batches(id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS duplicate_matches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entry_id INTEGER,
            matched_entry_id INTEGER,
            rule TEXT NOT NULL,
            score REAL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS print_jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id INTEGER NOT NULL,
            started_at TEXT,
            finished_at TEXT,
            entry_count INTEGER,
            stamps_export_path TEXT,
            combined_export_path TEXT,
            FOREIGN KEY(batch_id) REFERENCES batches(id)
        )
        """
    )
    conn.commit()


__all__ = ["init_db", "session_scope", "get_connection"]
