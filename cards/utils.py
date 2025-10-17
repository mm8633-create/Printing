from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple

from difflib import SequenceMatcher
from zoneinfo import ZoneInfo

@dataclass
class NormalizedEntry:
    name: str
    clinic_name: str
    address: str
    city: str
    state: str
    zip_code: str
    heally_link: str
    heally_id: Optional[str]
    date_time: Optional[datetime]
    list_source: str
    raw: Dict
    normalized_payload: Dict
    validation_status: str
    uncertainty_reasons: List[str]


NAME_COMMA_RE = re.compile(r"^(?P<last>[^,]+),\s*(?P<first>[^,]+)(?:,?\s*(?P<middle>.+))?$")
HEALLY_ID_RE = re.compile(r"(\d{6,})")


USPS_STATE_RE = re.compile(r"^[A-Z]{2}$")

DATETIME_FORMATS = [
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d",
    "%m/%d/%Y %H:%M:%S",
    "%m/%d/%Y %H:%M",
    "%m/%d/%Y",
    "%Y/%m/%d %H:%M:%S",
    "%Y/%m/%d %H:%M",
    "%Y/%m/%d",
    "%m-%d-%Y %H:%M:%S",
    "%m-%d-%Y %H:%M",
    "%m-%d-%Y",
]


def normalize_name(name: str) -> Tuple[str, List[str]]:
    name = name.strip()
    reasons: List[str] = []
    if not name:
        return "", ["missing_name"]
    match = NAME_COMMA_RE.match(name)
    if match:
        first = match.group("first").title()
        last = match.group("last").title()
        middle = match.group("middle")
        parts = [first]
        if middle:
            parts.append(" ".join(token.title() for token in middle.split()))
        parts.append(last)
        normalized = " ".join(parts)
        reasons.append("name_flipped")
        return normalized, reasons
    parts = [token.title() for token in name.split()]
    return " ".join(parts), reasons


def extract_heally(link: Optional[str], raw: Dict) -> Tuple[str, Optional[str], List[str]]:
    reasons = []
    if link and link.strip():
        link = link.strip()
        match = HEALLY_ID_RE.search(link)
        heally_id = match.group(1) if match else None
        return link, heally_id, reasons
    # search in raw values
    search_space = " ".join(str(value) for value in raw.values() if isinstance(value, str))
    match = HEALLY_ID_RE.search(search_space)
    if match:
        heally_id = match.group(1)
        link = f"https://getheally.com/super_admin/patient_users/{heally_id}"
        reasons.append("constructed_heally_link")
        return link, heally_id, reasons
    return "N/A", None, ["missing_heally_link"]


def parse_datetime(value: Optional[str], timezone: str) -> Tuple[Optional[datetime], List[str]]:
    if not value:
        return None, ["missing_datetime"]
    parsed = None
    for fmt in DATETIME_FORMATS:
        try:
            parsed = datetime.strptime(value.strip(), fmt)
            break
        except (ValueError, TypeError, AttributeError):
            continue
    if parsed is None:
        return None, ["invalid_datetime"]
    try:
        tz = ZoneInfo(timezone)
        parsed = parsed.replace(tzinfo=tz)
        parsed = parsed.astimezone(tz)
        return parsed.replace(tzinfo=None), []
    except Exception:
        return parsed, ["timezone_error"]


def format_datetime(dt: Optional[datetime]) -> str:
    if not dt:
        return ""
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def normalized_key(entry: NormalizedEntry) -> Tuple[str, str, str, str, str]:
    return (
        entry.name.lower(),
        entry.address.lower(),
        entry.city.lower(),
        entry.state.lower(),
        entry.zip_code,
    )


def _ratio(left: str, right: str) -> float:
    if not left and not right:
        return 1.0
    return SequenceMatcher(None, left.lower(), right.lower()).ratio()


def fuzzy_match_score(a: NormalizedEntry, b: NormalizedEntry) -> float:
    name_score = _ratio(a.name, b.name)
    address_score = _ratio(a.address, b.address)
    return (name_score + address_score) / 2


def serialize_entry(entry: NormalizedEntry) -> Dict:
    payload = {
        "Name": entry.name,
        "Clinic Name": entry.clinic_name,
        "Address": entry.address,
        "City": entry.city,
        "State": entry.state,
        "Zip Code": entry.zip_code,
        "Heally Link": entry.heally_link,
        "Date and Time": format_datetime(entry.date_time),
        "List Source": entry.list_source,
        "Duplicate?": entry.normalized_payload.get("duplicate", "No"),
    }
    return payload


__all__ = [
    "NormalizedEntry",
    "normalize_name",
    "extract_heally",
    "parse_datetime",
    "format_datetime",
    "normalized_key",
    "fuzzy_match_score",
    "serialize_entry",
]
