from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple

# rapid fuzzy import removed; using built-in tools only

ABBREVIATIONS = {
    "st": "Street",
    "ave": "Avenue",
    "blvd": "Boulevard",
    "dr": "Drive",
    "ln": "Lane",
    "pl": "Place",
    "rd": "Road",
    "hwy": "Highway",
    "pkwy": "Parkway",
    "ct": "Court",
    "cir": "Circle",
    "trl": "Trail",
    "way": "Way",
}

STATE_ABBREVS = {
    "alabama": "AL",
    "alaska": "AK",
    "arizona": "AZ",
    "arkansas": "AR",
    "california": "CA",
    "colorado": "CO",
    "connecticut": "CT",
    "delaware": "DE",
    "florida": "FL",
    "georgia": "GA",
    "hawaii": "HI",
    "idaho": "ID",
    "illinois": "IL",
    "indiana": "IN",
    "iowa": "IA",
    "kansas": "KS",
    "kentucky": "KY",
    "louisiana": "LA",
    "maine": "ME",
    "maryland": "MD",
    "massachusetts": "MA",
    "michigan": "MI",
    "minnesota": "MN",
    "mississippi": "MS",
    "missouri": "MO",
    "montana": "MT",
    "nebraska": "NE",
    "nevada": "NV",
    "new hampshire": "NH",
    "new jersey": "NJ",
    "new mexico": "NM",
    "new york": "NY",
    "north carolina": "NC",
    "north dakota": "ND",
    "ohio": "OH",
    "oklahoma": "OK",
    "oregon": "OR",
    "pennsylvania": "PA",
    "rhode island": "RI",
    "south carolina": "SC",
    "south dakota": "SD",
    "tennessee": "TN",
    "texas": "TX",
    "utah": "UT",
    "vermont": "VT",
    "virginia": "VA",
    "washington": "WA",
    "west virginia": "WV",
    "wisconsin": "WI",
    "wyoming": "WY",
}

USPS_STATES = set(STATE_ABBREVS.values())

ZIP_RE = re.compile(r"^(?P<zip5>\d{5})(?:[- ]?(?P<zip4>\d{4}))?$")


@dataclass
class AddressValidationResult:
    address: str
    city: str
    state: str
    zip5: str
    zip4: Optional[str]
    inferred: bool = False
    reasons: Tuple[str, ...] = ()


@lru_cache(maxsize=1)
def load_zip_db() -> Dict[str, Tuple[str, str]]:
    dataset_path = Path(__file__).parent / "data" / "zipcodes.csv"
    mapping: Dict[str, Tuple[str, str]] = {}
    with dataset_path.open("r", encoding="utf-8") as fp:
        reader = csv.DictReader(fp)
        for row in reader:
            mapping[row["zip"].strip()] = (row["city"].strip(), row["state"].strip())
    return mapping


def normalize_address(address: str) -> str:
    parts = []
    tokens = re.split(r"\s+", address.strip())
    for token in tokens:
        lower = token.lower().strip(",.")
        if lower in ABBREVIATIONS:
            parts.append(ABBREVIATIONS[lower])
        else:
            parts.append(token.title())
    return " ".join(parts)


def normalize_state(state: str) -> Tuple[str, Tuple[str, ...]]:
    state = state.strip()
    reasons = []
    if len(state) == 2 and state.upper() in USPS_STATES:
        return state.upper(), tuple(reasons)
    normalized = state.lower()
    if normalized in STATE_ABBREVS:
        reasons.append("state_name_converted")
        return STATE_ABBREVS[normalized], tuple(reasons)
    return state.upper(), tuple(["invalid_state"])


def normalize_zip(zip_code: str) -> Tuple[Optional[str], Optional[str], Tuple[str, ...]]:
    if not zip_code:
        return None, None, ("missing_zip",)
    match = ZIP_RE.match(zip_code.strip())
    if not match:
        return None, None, ("invalid_zip",)
    zip5 = match.group("zip5")
    zip4 = match.group("zip4")
    return zip5, zip4, tuple()


def infer_city_state_from_zip(zip5: str) -> Optional[Tuple[str, str]]:
    zip_db = load_zip_db()
    return zip_db.get(zip5)


def normalize_city(city: str) -> str:
    return city.strip().title()


def validate_and_normalize_address(
    address: str,
    city: Optional[str],
    state: Optional[str],
    zip_code: Optional[str],
) -> AddressValidationResult:
    reasons = []
    normalized_address = normalize_address(address)
    zip5, zip4, zip_reasons = normalize_zip(zip_code or "")
    reasons.extend(zip_reasons)

    normalized_city = normalize_city(city or "") if city else ""
    normalized_state = (state or "").strip()

    if normalized_state:
        normalized_state, state_reasons = normalize_state(normalized_state)
        reasons.extend(state_reasons)
    else:
        normalized_state = ""
        reasons.append("missing_state")

    if not normalized_city and zip5:
        inferred = infer_city_state_from_zip(zip5)
        if inferred:
            normalized_city, inferred_state = inferred
            if not normalized_state:
                normalized_state = inferred_state
            elif normalized_state != inferred_state:
                reasons.append("zip_state_mismatch")
            reasons.append("inferred_city_state_from_zip")
        else:
            reasons.append("zip_not_found")
    elif not normalized_city:
        reasons.append("missing_city")
    else:
        normalized_city = normalize_city(normalized_city)

    if not normalized_state and zip5:
        inferred = infer_city_state_from_zip(zip5)
        if inferred:
            _, inferred_state = inferred
            normalized_state = inferred_state
            reasons.append("inferred_state_from_zip")
    if not normalized_state:
        reasons.append("missing_state")

    return AddressValidationResult(
        address=normalized_address,
        city=normalized_city,
        state=normalized_state,
        zip5=zip5 or "",
        zip4=zip4,
        inferred="inferred_city_state_from_zip" in reasons or "inferred_state_from_zip" in reasons,
        reasons=tuple(sorted(set(r for r in reasons if r))),
    )


__all__ = [
    "AddressValidationResult",
    "normalize_address",
    "validate_and_normalize_address",
    "normalize_state",
    "normalize_zip",
    "infer_city_state_from_zip",
    "load_zip_db",
]
