from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from . import simple_pandas as pd

from .address import AddressValidationResult, validate_and_normalize_address
from .config import get_settings
from .db import session_scope, init_db
from .header_mapping import HeaderMappingResult, map_headers
from .utils import (
    NormalizedEntry,
    extract_heally,
    format_datetime,
    fuzzy_match_score,
    normalize_name,
    normalized_key,
    parse_datetime,
)


@dataclass
class ProcessingReport:
    normalized_entries: List[NormalizedEntry]
    header_mapping: HeaderMappingResult
    issues: Dict[str, List[str]]
    duplicate_reports: List[Dict]
    summary: Dict[str, int]
    batch_id: int


SUPPORTED_EXTENSIONS = {".csv", ".tsv", ".txt", ".xlsx"}


class DataProcessor:
    def __init__(self, timezone: Optional[str] = None):
        self.settings = get_settings()
        if timezone:
            self.settings.timezone = timezone
        init_db()
        self.existing_entries_cache: List[Dict[str, object]] = []
        self._load_history()

    def _load_history(self):
        with session_scope() as conn:
            cursor = conn.execute(
                "SELECT id, list_source, normalized_payload_json, validation_status, uncertainty_reasons FROM entries"
            )
            rows = cursor.fetchall()
            for row in rows:
                payload = json.loads(row["normalized_payload_json"])
                reasons = json.loads(row["uncertainty_reasons"])
                normalized = NormalizedEntry(
                    name=payload.get("name", ""),
                    clinic_name=payload.get("clinic_name", ""),
                    address=payload.get("address", ""),
                    city=payload.get("city", ""),
                    state=payload.get("state", ""),
                    zip_code=payload.get("zip", ""),
                    heally_link=payload.get("heally_link", "N/A"),
                    heally_id=payload.get("heally_id"),
                    date_time=None,
                    list_source=row["list_source"],
                    raw={},
                    normalized_payload=payload,
                    validation_status=row["validation_status"],
                    uncertainty_reasons=reasons,
                )
                self.existing_entries_cache.append({"entry": normalized, "entry_id": row["id"]})

    def read_input(self, source) -> pd.DataFrame:
        if isinstance(source, pd.DataFrame):
            return source.copy()
        path = Path(source)
        ext = path.suffix.lower()
        if ext not in SUPPORTED_EXTENSIONS:
            raise ValueError(f"Unsupported file extension: {ext}")
        if ext == ".xlsx":
            df = pd.read_excel(path)
        elif ext in {".tsv", ".txt"}:
            df = pd.read_csv(path, sep="\t")
        else:
            df = pd.read_csv(path)
        return df

    def map_headers(self, df: pd.DataFrame, list_type: str) -> HeaderMappingResult:
        result = map_headers(list(df.columns), list_type)
        return result

    def normalize_rows(
        self,
        df: pd.DataFrame,
        list_type: str,
        header_mapping: HeaderMappingResult,
    ) -> Tuple[List[NormalizedEntry], Dict[str, List[str]]]:
        timezone = self.settings.timezone
        normalized_entries: List[NormalizedEntry] = []
        issues: Dict[str, List[str]] = defaultdict(list)

        mapping = header_mapping.mapping
        missing = header_mapping.missing
        if missing:
            issues["missing_columns"] = missing

        for idx, row in df.iterrows():
            raw = row.to_dict()
            name_field = mapping.get("name")
            address_field = mapping.get("address")
            city_field = mapping.get("city")
            state_field = mapping.get("state")
            zip_field = mapping.get("zip")
            clinic_field = mapping.get("clinic_name")
            date_field = mapping.get("date_time")
            heally_field = mapping.get("heally_link")

            name = str(raw.get(name_field, "")) if name_field else ""
            address = str(raw.get(address_field, "")) if address_field else ""
            city = str(raw.get(city_field, "")) if city_field else ""
            state = str(raw.get(state_field, "")) if state_field else ""
            zip_code = str(raw.get(zip_field, "")) if zip_field else ""
            clinic_name = str(raw.get(clinic_field, "")) if clinic_field else ""
            date_value = raw.get(date_field) if date_field else None
            heally_value = raw.get(heally_field) if heally_field else None

            normalized_name, name_reasons = normalize_name(name)
            address_result: AddressValidationResult = validate_and_normalize_address(
                address=address,
                city=city,
                state=state,
                zip_code=zip_code,
            )
            heally_link, heally_id, heally_reasons = extract_heally(heally_value, raw)
            date_time, date_reasons = parse_datetime(str(date_value) if date_value is not None else None, timezone)

            reasons = []
            reasons.extend(name_reasons)
            reasons.extend(list(address_result.reasons))
            reasons.extend(heally_reasons)
            reasons.extend(date_reasons)

            validation_status = "ok" if not reasons else "attention"
            if not normalized_name or not address_result.address:
                validation_status = "rejected"
                reasons.append("missing_core_fields")

            normalized = NormalizedEntry(
                name=normalized_name,
                clinic_name=clinic_name.title() if clinic_name else "",
                address=address_result.address,
                city=address_result.city,
                state=address_result.state,
                zip_code="-".join(filter(None, [address_result.zip5, address_result.zip4]))
                if address_result.zip4
                else address_result.zip5,
                heally_link=heally_link,
                heally_id=heally_id,
                date_time=date_time,
                list_source="New Visit" if list_type == "new_visit" else "Reprint",
                raw=raw,
                normalized_payload={
                    "name": normalized_name,
                    "clinic_name": clinic_name.title() if clinic_name else "",
                    "address": address_result.address,
                    "city": address_result.city,
                    "state": address_result.state,
                    "zip": address_result.zip5,
                    "zip4": address_result.zip4,
                    "heally_link": heally_link,
                    "heally_id": heally_id,
                    "date_time": format_datetime(date_time),
                    "list_source": "New Visit" if list_type == "new_visit" else "Reprint",
                },
                validation_status=validation_status,
                uncertainty_reasons=sorted(set(reasons)),
            )
            normalized_entries.append(normalized)

        return normalized_entries, issues

    def detect_duplicates(self, entries: List[NormalizedEntry]) -> List[Dict]:
        duplicates = []
        existing = {normalized_key(item["entry"]): item for item in self.existing_entries_cache}
        for entry in entries:
            key = normalized_key(entry)
            if key in existing:
                matched = existing[key]
                duplicates.append(
                    {
                        "entry": entry,
                        "rule": "exact_key",
                        "score": 1.0,
                        "matched": matched["entry"],
                        "matched_entry_id": matched.get("entry_id"),
                    }
                )
                entry.normalized_payload["duplicate"] = "Yes"
                continue
            if entry.heally_id:
                for cached in self.existing_entries_cache:
                    other = cached["entry"]
                    if other.heally_id and other.heally_id == entry.heally_id:
                        duplicates.append(
                            {
                                "entry": entry,
                                "rule": "heally_id",
                                "score": 1.0,
                                "matched": other,
                                "matched_entry_id": cached.get("entry_id"),
                            }
                        )
                        entry.normalized_payload["duplicate"] = "Yes"
                        break
            if entry.normalized_payload.get("duplicate") == "Yes":
                continue
            for cached in self.existing_entries_cache:
                other = cached["entry"]
                score = fuzzy_match_score(entry, other)
                if score >= self.settings.fuzzy_threshold:
                    duplicates.append(
                        {
                            "entry": entry,
                            "rule": "fuzzy",
                            "score": score,
                            "matched": other,
                            "matched_entry_id": cached.get("entry_id"),
                        }
                    )
                    entry.normalized_payload["duplicate"] = "Yes"
                    break
            if entry.normalized_payload.get("duplicate") != "Yes":
                entry.normalized_payload["duplicate"] = "No"
        return duplicates

    def deduplicate_within_batch(self, entries: List[NormalizedEntry]) -> List[Dict]:
        duplicates = []
        seen_entries: List[NormalizedEntry] = []
        for entry in entries:
            duplicate_found = False
            for other in seen_entries:
                if normalized_key(entry) == normalized_key(other):
                    duplicates.append({
                        "entry": entry,
                        "rule": "exact_key_batch",
                        "score": 1.0,
                        "matched": other,
                        "matched_entry_id": None,
                    })
                    duplicate_found = True
                elif entry.heally_id and other.heally_id and entry.heally_id == other.heally_id:
                    duplicates.append({
                        "entry": entry,
                        "rule": "heally_id_batch",
                        "score": 1.0,
                        "matched": other,
                        "matched_entry_id": None,
                    })
                    duplicate_found = True
                else:
                    score = fuzzy_match_score(entry, other)
                    if score >= self.settings.fuzzy_threshold:
                        duplicates.append({
                            "entry": entry,
                            "rule": "fuzzy_batch",
                            "score": score,
                            "matched": other,
                            "matched_entry_id": None,
                        })
                        duplicate_found = True
                if duplicate_found:
                    entry.normalized_payload["duplicate"] = "Yes"
                    other.normalized_payload["duplicate"] = "Yes"
                    other.normalized_payload["list_source"] = "Both Lists"
                    entry.normalized_payload["list_source"] = "Both Lists"
                    other.list_source = "Both Lists"
                    entry.list_source = "Both Lists"
                    break
            if not duplicate_found:
                if entry.normalized_payload.get("duplicate") != "Yes":
                    entry.normalized_payload["duplicate"] = "No"
            seen_entries.append(entry)
        return duplicates

    def process(
        self,
        new_visit_df: pd.DataFrame,
        reprint_df: pd.DataFrame,
        new_visit_mapping: HeaderMappingResult,
        reprint_mapping: HeaderMappingResult,
        batch_label: Optional[str] = None,
    ) -> ProcessingReport:
        new_entries, new_issues = self.normalize_rows(new_visit_df, "new_visit", new_visit_mapping)
        re_entries, re_issues = self.normalize_rows(reprint_df, "reprint", reprint_mapping)

        combined = new_entries + re_entries
        cross_duplicates = self.detect_duplicates(combined)
        batch_duplicates = self.deduplicate_within_batch(combined)
        duplicate_reports_raw = cross_duplicates + batch_duplicates

        for entry in combined:
            if entry.normalized_payload.get("duplicate") == "Yes" and entry.list_source != "Both Lists":
                entry.list_source = entry.normalized_payload.get("list_source", entry.list_source)

        summary = {
            "new_visits": len(new_entries),
            "reprints": len(re_entries),
            "combined_total": len(combined),
            "duplicates": sum(1 for entry in combined if entry.normalized_payload.get("duplicate") == "Yes"),
        }

        issues = {**new_issues}
        for key, value in re_issues.items():
            issues.setdefault(key, []).extend(value)

        with session_scope() as conn:
            cursor = conn.execute(
                "INSERT INTO batches (label) VALUES (?)",
                (batch_label,),
            )
            batch_id = cursor.lastrowid
            entry_records: Dict[int, int] = {}
            for entry in combined:
                cursor = conn.execute(
                    """
                    INSERT INTO entries (
                        batch_id,
                        list_source,
                        date_time_local,
                        raw_payload_json,
                        normalized_payload_json,
                        validation_status,
                        uncertainty_reasons
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        batch_id,
                        entry.list_source,
                        format_datetime(entry.date_time) if entry.date_time else None,
                        json.dumps(entry.raw, default=str),
                        json.dumps(entry.normalized_payload, default=str),
                        entry.validation_status,
                        json.dumps(entry.uncertainty_reasons),
                    ),
                )
                entry_records[id(entry)] = cursor.lastrowid

            for duplicate in duplicate_reports_raw:
                entry_obj = duplicate["entry"]
                entry_id = entry_records.get(id(entry_obj))
                matched_id = duplicate.get("matched_entry_id")
                if not matched_id:
                    matched_entry = duplicate.get("matched")
                    if matched_entry:
                        matched_id = entry_records.get(id(matched_entry))
                conn.execute(
                    "INSERT INTO duplicate_matches (entry_id, matched_entry_id, rule, score) VALUES (?, ?, ?, ?)",
                    (entry_id, matched_id, duplicate["rule"], duplicate.get("score")),
                )

        for entry in combined:
            entry_id = entry_records.get(id(entry))
            if entry_id:
                self.existing_entries_cache.append({"entry": entry, "entry_id": entry_id})

        return ProcessingReport(
            normalized_entries=combined,
            header_mapping=HeaderMappingResult(
                mapping={**new_visit_mapping.mapping, **reprint_mapping.mapping},
                missing=list(set(new_visit_mapping.missing + reprint_mapping.missing)),
                extras=list(set(new_visit_mapping.extras + reprint_mapping.extras)),
            ),
            issues=issues,
            duplicate_reports=[
                {
                    "rule": report["rule"],
                    "score": report["score"],
                    "entry_name": report["entry"].name,
                    "matched_name": report["matched"].name,
                }
                for report in duplicate_reports_raw
            ],
            summary=summary,
            batch_id=batch_id,
        )

    def export(self, entries: List[NormalizedEntry], batch_id: int) -> Tuple[Path, Path]:
        settings = self.settings
        entries_sorted = sorted(
            entries,
            key=lambda e: (
                (e.name.split()[0].lower() if e.name else ""),
                (e.name.split()[-1].lower() if e.name else ""),
            ),
        )

        stamps_rows = [
            {
                "Name": entry.name,
                "Address": entry.address,
                "City": entry.city,
                "State": entry.state,
                "Zip": entry.zip_code,
            }
            for entry in entries_sorted
        ]

        combined_rows = [
            {
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
            for entry in entries_sorted
        ]

        stamps_path = settings.export_dir / f"stamps_batch_{batch_id}.csv"
        combined_path = settings.export_dir / f"combined_batch_{batch_id}.csv"

        pd.DataFrame(stamps_rows).to_csv(stamps_path, index=False)
        pd.DataFrame(combined_rows).to_csv(combined_path, index=False)

        with session_scope() as conn:
            now = datetime.utcnow().isoformat()
            conn.execute(
                """
                INSERT INTO print_jobs (
                    batch_id,
                    started_at,
                    finished_at,
                    entry_count,
                    stamps_export_path,
                    combined_export_path
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    batch_id,
                    now,
                    now,
                    len(entries_sorted),
                    str(stamps_path),
                    str(combined_path),
                ),
            )

        return stamps_path, combined_path

    def from_entry_record(self, entry_row: Dict[str, str]) -> NormalizedEntry:
        payload = json.loads(entry_row["normalized_payload_json"])
        raw_payload = json.loads(entry_row["raw_payload_json"])
        date_value = entry_row.get("date_time_local")
        date_time = datetime.fromisoformat(date_value) if date_value else None
        normalized = NormalizedEntry(
            name=payload.get("name", raw_payload.get("name", "")),
            clinic_name=payload.get("clinic_name", ""),
            address=payload.get("address", ""),
            city=payload.get("city", ""),
            state=payload.get("state", ""),
            zip_code="-".join(filter(None, [payload.get("zip"), payload.get("zip4")]))
            if payload.get("zip4")
            else payload.get("zip", ""),
            heally_link=payload.get("heally_link", "N/A"),
            heally_id=payload.get("heally_id"),
            date_time=date_time,
            list_source=entry_row.get("list_source", ""),
            raw=raw_payload,
            normalized_payload=payload,
            validation_status=entry_row.get("validation_status", "ok"),
            uncertainty_reasons=json.loads(entry_row.get("uncertainty_reasons", "[]")),
        )
        return normalized


__all__ = ["DataProcessor", "ProcessingReport"]
