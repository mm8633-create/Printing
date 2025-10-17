from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Mapping, Optional, Sequence

import difflib

REQUIRED_FIELDS = {
    "new_visit": [
        "name",
        "clinic_name",
        "address",
        "city",
        "state",
        "zip",
        "date_time",
        "heally_link",
    ],
    "reprint": [
        "name",
        "clinic_name",
        "address",
        "city",
        "state",
        "zip",
        "heally_link",
        "date_time",
    ],
}

OPTIONAL_FIELDS = [
    "address_2",
    "heally_id",
]

CANONICAL_HEADERS = {
    "name": ["name", "patient", "full_name", "patient_name", "patient_full_name"],
    "clinic_name": ["clinic", "clinic_name", "clinicname"],
    "address": ["address", "address1", "addr", "street", "street_address"],
    "address_2": ["address2", "addr2", "suite"],
    "city": ["city", "town", "city_name"],
    "state": ["state", "province", "st"],
    "zip": ["zip", "zipcode", "zip_code", "postal", "postal_code"],
    "date_time": ["date", "datetime", "date_time", "appointment", "timestamp"],
    "heally_link": ["heally", "heally_link", "profile", "patient_portal"],
    "heally_id": ["heally_id", "patient_id", "id"],
}


@dataclass
class HeaderMappingResult:
    mapping: Dict[str, str]
    missing: List[str]
    extras: List[str]


class HeaderMapper:
    def __init__(self, headers: Sequence[str]):
        self.headers = [h.strip() for h in headers]
        self.normalized = [h.lower().replace(" ", "_") for h in self.headers]

    def suggest_mapping(self) -> Dict[str, str]:
        mapping: Dict[str, str] = {}
        for canonical, variants in CANONICAL_HEADERS.items():
            for header, normalized in zip(self.headers, self.normalized):
                if normalized in variants:
                    mapping[canonical] = header
                    break
            if canonical not in mapping:
                closest = difflib.get_close_matches(canonical, self.normalized, n=1, cutoff=0.85)
                if closest:
                    idx = self.normalized.index(closest[0])
                    mapping[canonical] = self.headers[idx]
        return mapping

    def resolve(self, required_for: str) -> HeaderMappingResult:
        required = REQUIRED_FIELDS[required_for]
        mapping = self.suggest_mapping()
        missing = [field for field in required if field not in mapping]
        extras = [h for h in self.headers if h not in mapping.values()]
        return HeaderMappingResult(mapping=mapping, missing=missing, extras=extras)


def map_headers(headers: Sequence[str], required_for: str) -> HeaderMappingResult:
    mapper = HeaderMapper(headers)
    return mapper.resolve(required_for)


__all__ = ["map_headers", "HeaderMappingResult", "HeaderMapper", "REQUIRED_FIELDS", "OPTIONAL_FIELDS"]
