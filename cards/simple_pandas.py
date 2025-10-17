from __future__ import annotations

import csv
import io
import json
import zipfile
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional
from xml.etree import ElementTree as ET

__all__ = [
    "DataFrame",
    "read_csv",
    "read_excel",
    "read_parquet",
]


class Row(dict):
    def to_dict(self) -> Dict[str, Any]:
        return dict(self)


class _ILoc:
    def __init__(self, df: "DataFrame") -> None:
        self.df = df

    def __getitem__(self, item):
        if isinstance(item, slice):
            rows = self.df._rows[item]
            return DataFrame([row.copy() for row in rows], columns=self.df.columns)
        if isinstance(item, list):
            rows = [self.df._rows[i] for i in item]
            return DataFrame([row.copy() for row in rows], columns=self.df.columns)
        return self.df._rows[item]


class DataFrame:
    def __init__(self, data: Optional[Any] = None, columns: Optional[List[str]] = None):
        self._rows: List[Dict[str, Any]] = []
        if data is None:
            self._columns = columns or []
            return
        if isinstance(data, list):
            for row in data:
                if not isinstance(row, dict):
                    raise TypeError("Rows must be dictionaries")
                self._rows.append(dict(row))
            self._columns = list(columns or (list(data[0].keys()) if data else []))
            if not self._columns and data:
                self._columns = list(data[0].keys())
        elif isinstance(data, dict):
            keys = list(data.keys())
            length = len(next(iter(data.values()))) if data else 0
            for key, values in data.items():
                if len(values) != length:
                    raise ValueError("All columns must be the same length")
            for idx in range(length):
                self._rows.append({key: data[key][idx] for key in keys})
            self._columns = columns or keys
        else:
            raise TypeError("Unsupported data type for DataFrame")

    @property
    def columns(self) -> List[str]:
        if hasattr(self, "_columns") and self._columns:
            return list(self._columns)
        if self._rows:
            self._columns = list(self._rows[0].keys())
        else:
            self._columns = []
        return list(self._columns)

    def iterrows(self) -> Iterator[tuple[int, Row]]:
        for idx, row in enumerate(self._rows):
            yield idx, Row(row)

    def copy(self) -> "DataFrame":
        return DataFrame([row.copy() for row in self._rows], columns=self.columns)

    def to_csv(self, path: Path | str | io.IOBase, index: bool = False) -> None:
        close = False
        if isinstance(path, (str, Path)):
            fh = open(path, "w", newline="", encoding="utf-8")
            close = True
        else:
            fh = path
        writer = csv.DictWriter(fh, fieldnames=self.columns)
        writer.writeheader()
        for row in self._rows:
            writer.writerow({col: row.get(col, "") for col in self.columns})
        if close:
            fh.close()

    def to_parquet(self, path: Path | str, index: bool = False) -> None:
        data = {
            "columns": self.columns,
            "rows": [[row.get(col) for col in self.columns] for row in self._rows],
        }
        with open(path, "w", encoding="utf-8") as fp:
            json.dump(data, fp)

    @property
    def iloc(self) -> _ILoc:
        return _ILoc(self)

    def __len__(self) -> int:
        return len(self._rows)

    def __getitem__(self, key: str) -> List[Any]:
        return [row.get(key) for row in self._rows]

    def append(self, row: Dict[str, Any]) -> None:
        self._rows.append(dict(row))

    def to_records(self) -> List[Dict[str, Any]]:
        return [row.copy() for row in self._rows]

    def __iter__(self):
        return iter(self._rows)


def _ensure_text(data: bytes | str) -> str:
    if isinstance(data, bytes):
        return data.decode("utf-8")
    return data


def read_csv(source, sep: str = ",") -> DataFrame:
    if hasattr(source, "read"):
        content = _ensure_text(source.read())
        stream = io.StringIO(content)
    else:
        stream = open(source, "r", encoding="utf-8")
    reader = csv.DictReader(stream, delimiter=sep)
    rows = [dict(row) for row in reader]
    df = DataFrame(rows)
    if not hasattr(source, "read"):
        stream.close()
    return df


def read_parquet(path) -> DataFrame:
    with open(path, "r", encoding="utf-8") as fp:
        data = json.load(fp)
    columns = data.get("columns", [])
    rows = [dict(zip(columns, values)) for values in data.get("rows", [])]
    return DataFrame(rows, columns=columns)


def read_excel(path) -> DataFrame:
    if hasattr(path, "read"):
        data = path.read()
        stream = io.BytesIO(data)
    else:
        stream = open(path, "rb")
    with zipfile.ZipFile(stream) as zf:
        shared_strings: List[str] = []
        if "xl/sharedStrings.xml" in zf.namelist():
            ss_root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
            ns = {"a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
            for si in ss_root.findall("a:si", ns):
                texts = [node.text or "" for node in si.findall("a:t", ns)]
                text = "".join(texts) if texts else "".join(si.itertext())
                shared_strings.append(text)
        sheet_root = ET.fromstring(zf.read("xl/worksheets/sheet1.xml"))
        ns = {"a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
        rows = []
        headers: List[str] = []
        for idx, row in enumerate(sheet_root.findall(".//a:row", ns)):
            values: List[str] = []
            for cell in row.findall("a:c", ns):
                value = ""
                v_node = cell.find("a:v", ns)
                if v_node is not None:
                    value = v_node.text or ""
                if cell.get("t") == "s" and value:
                    value = shared_strings[int(value)]
                values.append(value)
            if idx == 0:
                headers = values
            else:
                rows.append(dict(zip(headers, values)))
    if not hasattr(path, "read"):
        stream.close()
    return DataFrame(rows, columns=headers)


def DataFrame_from_records(records: Iterable[Dict[str, Any]]) -> DataFrame:  # pragma: no cover
    return DataFrame(list(records))
