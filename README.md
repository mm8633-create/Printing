# Card Printing Table

Card Printing Table ingests New Visit and Reprint lists, normalises and validates patient addresses, merges the data into a combined printing list, flags uncertainties, detects duplicates against historical batches, and produces export files that are ready for Stamps.com.

## Features

* Two-phase ingest flow for New Visits and Reprints via Streamlit UI and Typer CLI.
* Header mapping wizard with auto-detection heuristics for messy source files.
* Address normalisation (street abbreviations, state conversion, ZIP parsing) and USPS ZIP-based city/state inference using a bundled lookup table.
* Robust Heally link resolution from raw numeric IDs.
* Duplicate detection via exact keys, Heally ID matching, and fuzzy scoring (RapidFuzz) across current uploads and history.
* Persistent SQLite database (PostgreSQL-ready) storing batches, entries, duplicates, and export jobs.
* Combined Master and Stamps.com-ready CSV exports sorted by first name, plus batch summaries and clinic totals.
* FastAPI backend, Streamlit UI, and Typer CLI for headless automation.

## Quickstart

### Prerequisites

* Python 3.11+
* Node is **not** required. Optional: Docker.

### Installation

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
cp .env.example .env
```

Edit `.env` if you want to switch to PostgreSQL (`CARDS_DB_URL`) or tweak the timezone/export directory.

### Database Setup

```bash
cards init
```

This creates the SQLite database (or the DB specified by `CARDS_DB_URL`).

### Streamlit UI

```bash
streamlit run cards/streamlit_app.py
```

Follow the on-screen stepper:

1. Upload the New Visits list. The app will confirm with: `List 1 (New Visits) received. Ready for List 2 (Reprints)?`
2. Upload the Reprints list. The app prompts: `List 2 (Reprints) received. Would you like me to process and merge both lists now?`
3. Run the merge, review issues and duplicate reports, and then export. The final prompt asks: `Would you like me to export this as a CSV or make any modifications?`

### CLI Usage

The CLI supports the staged workflow:

```bash
python scripts/generate_sample_reprints_xlsx.py  # one-time helper to materialise the Excel sample
cards import --list new-visits samples/new_visits.csv
cards import --list reprints samples/reprints.xlsx
cards merge --batch-label "2025-02 Wave A"
cards export --batch 1 --out-dir ./exports
cards history --clinic "HappyMD" --since 2025-01-01
```

`cards merge` prints the batch summary, duplicate report, and resulting batch ID. Use `cards export` to generate the Combined Master and Stamps.com CSVs into the requested directory.

### FastAPI Backend

Run with Uvicorn:

```bash
uvicorn cards.api:app --reload
```

`POST /process` accepts New Visit & Reprint payloads plus explicit header mappings and returns the merge summary. `GET /health` returns application status.

## Adding New Header Mappings

Mappings are controlled by `cards/header_mapping.py`. To add aliases, edit `CANONICAL_HEADERS` or provide presets in the Streamlit header wizard. Tests cover the mapping heuristics; run `pytest` after making changes.

## Address Validation & Rules

* Street abbreviations expanded via `cards/address.ABBREVIATIONS`.
* States converted to USPS 2-letter codes; full names are accepted.
* ZIP codes parsed and validated; ZIP+4 stored split into `zip5` and `zip4`.
* City/State inferred from ZIP using `cards/data/zipcodes.csv`; ambiguous or missing data is flagged in `uncertainty_reasons`.
* Dates normalised to `America/Los_Angeles` and emitted as `YYYY-MM-DD HH:MM:SS`.
* Uncertainties (missing city/zip, inferred state, constructed Heally link, etc.) are surfaced in the review UI and stored per entry.

## Duplicate Detection

Duplicates are flagged using three strategies:

1. Exact match on normalised name + address + city + state + ZIP.
2. Shared Heally ID.
3. Fuzzy matching (token sort ratio) on name and address with a default threshold of 0.92.

Duplicate matches are persisted in the `duplicate_matches` table with rule and score metadata. Batch-level duplicates update the `List Source` to `Both Lists`.

## Changing Rules & Thresholds

* Fuzzy threshold: set `CARDS_FUZZY_THRESHOLD` or change `Settings.fuzzy_threshold` in `cards/config.py`.
* Address abbreviation map: edit `ABBREVIATIONS` in `cards/address.py`.
* Zip lookup: update `cards/data/zipcodes.csv` (ship your own dataset).
* Timezone: `CARDS_TIMEZONE` environment variable.

## Plugging in USPS / Smarty / Lob APIs

`cards/address.py` encapsulates address validation in `validate_and_normalize_address`. Swap in an API client by extending or replacing this function with a service-backed implementation. Keep uncertainty reasons explicit when external responses are ambiguous.

## History & Exports

Batches, entries, duplicates, and print jobs are recorded via SQLAlchemy models (`cards/models.py`). Use `cards history` or build UI/queries against the SQLite DB. Exports are stored under `exports/` by default (ignored by Git).

## Samples

`samples/` contains:

* `new_visits.csv` – messy headers, mixed casing, sample New Visit rows.
* `reprints.xlsx` – Excel format with alternate headers (generate it with `python scripts/generate_sample_reprints_xlsx.py`).

Use them to test the mapping wizard and duplicate detection. The generation script keeps the repository binary-free while still
providing a realistic Excel input for local runs.

## Testing & CI

Run tests and linters locally:

```bash
ruff check .
mypy cards
pytest
```

GitHub Actions (`.github/workflows/ci.yml`) runs Ruff, Mypy, Pytest, and builds the Streamlit app artifact on every push.

## Docker

Build the application services with Docker:

```bash
docker compose up --build
```

The compose stack launches the FastAPI backend, Streamlit frontend, and SQLite volume for persistence.

## Security & Privacy

* No real data is committed.
* Exports and local databases are `.gitignore`'d.
* Provide secrets and DB credentials via environment variables (`.env` file for local development).
