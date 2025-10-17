from cards import simple_pandas as pd

from cards.config import get_settings
from cards.processing import DataProcessor
from cards.header_mapping import HeaderMappingResult


def build_dataframe(rows):
    return pd.DataFrame(rows)


def get_mappings(df):
    return HeaderMappingResult(
        mapping={
            "name": "name",
            "clinic_name": "clinic",
            "address": "address",
            "city": "city",
            "state": "state",
            "zip": "zip",
            "date_time": "datetime",
            "heally_link": "heally",
        },
        missing=[],
        extras=[],
    )


def test_process_and_duplicate_detection(tmp_path):
    processor = DataProcessor()
    new_df = build_dataframe(
        [
            {
                "name": "John Doe",
                "clinic": "HappyMD",
                "address": "28 Davison Hill Ln",
                "city": "Oroville",
                "state": "California",
                "zip": "95966",
                "datetime": "2025-02-20 11:13",
                "heally": "",
            }
        ]
    )
    re_df = build_dataframe(
        [
            {
                "name": "Jon Doe",
                "clinic": "HappyMD",
                "address": "28 Davison Hill Lane",
                "city": "Oroville",
                "state": "CA",
                "zip": "95966",
                "datetime": "2025/02/20 11:13",
                "heally": "https://getheally.com/super_admin/patient_users/1481480",
            }
        ]
    )

    report = processor.process(new_df, re_df, get_mappings(new_df), get_mappings(re_df))

    assert report.summary["combined_total"] == 2
    assert any("fuzzy" in d["rule"] for d in report.duplicate_reports)
    assert report.batch_id > 0


def test_export_sorting(tmp_path):
    settings = get_settings()
    settings.export_dir = tmp_path
    processor = DataProcessor()

    df = build_dataframe(
        [
            {
                "name": "Smith, John",
                "clinic": "HappyMD",
                "address": "1711 245th Street",
                "city": "Lomita",
                "state": "CA",
                "zip": "90717",
                "datetime": "2025-02-07 16:07",
                "heally": "",
            },
            {
                "name": "Christina Beatrice Diaz",
                "clinic": "HappyMD",
                "address": "1711 245th Street",
                "city": "Lomita",
                "state": "CA",
                "zip": "90717",
                "datetime": "2025-02-07 16:07",
                "heally": "",
            },
        ]
    )

    report = processor.process(df.iloc[:1], df.iloc[1:], get_mappings(df), get_mappings(df))
    stamps, combined = processor.export(report.normalized_entries, report.batch_id)

    stamps_df = pd.read_csv(stamps)
    assert list(stamps_df.columns) == ["Name", "Address", "City", "State", "Zip"]
    assert list(stamps_df["Name"]) == sorted(stamps_df["Name"], key=lambda n: n.split()[0].lower())


def test_heally_link_construction():
    processor = DataProcessor()
    df = build_dataframe(
        [
            {
                "name": "Jane Example 1234567",
                "clinic": "Clinic",
                "address": "123 Main St",
                "city": "Los Angeles",
                "state": "CA",
                "zip": "90001",
                "datetime": "2025-01-01",
                "heally": "",
            }
        ]
    )
    report = processor.process(df, df, get_mappings(df), get_mappings(df))
    entry = report.normalized_entries[0]
    assert entry.heally_link.endswith("1234567")
    assert "constructed_heally_link" in entry.uncertainty_reasons
