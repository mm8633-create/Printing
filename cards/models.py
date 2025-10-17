from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class Batch:
    id: int
    label: Optional[str]
    source: str
    notes: Optional[str]
    created_at: str


@dataclass
class Entry:
    id: int
    batch_id: int
    list_source: str
    date_time_local: Optional[str]
    raw_payload_json: str
    normalized_payload_json: str
    validation_status: str
    uncertainty_reasons: str


@dataclass
class DuplicateMatch:
    id: int
    entry_id: Optional[int]
    matched_entry_id: Optional[int]
    rule: str
    score: Optional[float]
    created_at: str


@dataclass
class PrintJob:
    id: int
    batch_id: int
    started_at: str
    finished_at: Optional[str]
    entry_count: int
    stamps_export_path: str
    combined_export_path: str
