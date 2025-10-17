from __future__ import annotations

from typing import List

from . import simple_pandas as pd

try:  # pragma: no cover
    from fastapi import FastAPI
except ImportError:  # pragma: no cover
    class FastAPI:  # type: ignore
        def __init__(self, *args, **kwargs):
            pass

        def on_event(self, *args, **kwargs):
            def decorator(func):
                return func

            return decorator

        def post(self, *args, **kwargs):
            def decorator(func):
                return func

            return decorator

        def get(self, *args, **kwargs):
            def decorator(func):
                return func

            return decorator


try:  # pragma: no cover
    from pydantic import BaseModel
except ImportError:  # pragma: no cover
    class BaseModel:  # type: ignore
        def __init__(self, **data):
            for key, value in data.items():
                setattr(self, key, value)

from . import init_db
from .config import get_settings
from .header_mapping import HeaderMappingResult
from .processing import DataProcessor

app = FastAPI(title="Card Printing Table API")


class UploadPayload(BaseModel):
    new_visits: List[dict]
    reprints: List[dict]
    new_visit_mapping: dict
    reprint_mapping: dict
    batch_label: str | None = None


@app.on_event("startup")
def startup_event() -> None:
    init_db()


@app.post("/process")
def process(payload: UploadPayload):
    processor = DataProcessor()
    new_df = pd.DataFrame(payload.new_visits)
    re_df = pd.DataFrame(payload.reprints)
    new_mapping = HeaderMappingResult(mapping=payload.new_visit_mapping, missing=[], extras=[])
    re_mapping = HeaderMappingResult(mapping=payload.reprint_mapping, missing=[], extras=[])
    report = processor.process(new_df, re_df, new_mapping, re_mapping, batch_label=payload.batch_label)
    return {
        "summary": report.summary,
        "issues": report.issues,
        "duplicates": report.duplicate_reports,
    }


@app.get("/health")
def health():
    settings = get_settings()
    return {"status": "ok", "db": settings.db_url}
