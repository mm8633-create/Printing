"""Generate the sample Reprints XLSX file for Card Printing Table."""
from __future__ import annotations

from pathlib import Path

from openpyxl import Workbook


DATA_ROWS = [
    {
        "Full Name": "PARKER, OLIVIA",
        "Clinic": "420Recs",
        "Street": "3656 Summerset Pl",
        "Town": "Inglewood",
        "State": "California",
        "Postal": "90305-1420",
        "Patient ID": 1481480,
        "Recorded": "02/19/2025 10:22 AM",
    },
    {
        "Full Name": "Smith, Andrew",
        "Clinic": "HappyMD",
        "Street": "1711 245th St",
        "Town": "Lomita",
        "State": "CA",
        "Postal": "90717",
        "Heally URL": "https://getheally.com/super_admin/patient_users/1406723",
        "Recorded": "2-7-25 4:07 pm",
    },
]


def build_workbook() -> Workbook:
    wb = Workbook()
    ws = wb.active
    ws.title = "Reprints"
    headers = list(DATA_ROWS[0].keys())
    ws.append(headers)
    for row in DATA_ROWS:
        ws.append([row.get(header, "") for header in headers])
    return wb


def main() -> None:
    target_path = Path(__file__).resolve().parent.parent / "samples" / "reprints.xlsx"
    target_path.parent.mkdir(parents=True, exist_ok=True)
    wb = build_workbook()
    wb.save(target_path)
    print(f"Generated {target_path.relative_to(Path.cwd())}")


if __name__ == "__main__":
    main()
