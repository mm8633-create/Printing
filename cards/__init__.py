from .config import get_settings
from .db import init_db as _init_db


def init_db() -> None:
    _init_db()


__all__ = ["init_db", "get_settings"]
