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

### Absolute beginner walkthrough

The steps below assume you are on macOS or Windows and have never used a terminal before. Every command that appears inside a
`code block` should be typed into a terminal exactly as shown, then press Enter. Replace `C:\Users\you\Projects` (Windows) or
`~/Projects` (macOS/Linux) with any folder you like.

1. **Install Python 3.11**
   * Windows/macOS: download from [python.org/downloads](https://www.python.org/downloads/). During installation on Windows,
     tick “Add Python to PATH”.
   * Verify it works by opening *Command Prompt* (Windows) or *Terminal* (macOS) and running:

     ```bash
     python --version
     ```

     You should see something like `Python 3.11.8`.

2. **Download the project**
   * Click the green “Code” button on GitHub → “Download ZIP”.
   * Extract the ZIP to your projects folder.
   * Open the extracted folder in your terminal:

     ```bash
     cd C:\Users\you\Projects\Printing  # Windows
     # or
     cd ~/Projects/Printing                # macOS/Linux
     ```

3. **Create a virtual environment** (isolated Python install):

   ```bash
   python -m venv .venv
   ```

4. **Activate the virtual environment**
   * Windows (Command Prompt):

     ```bash
     .venv\Scripts\activate
     ```

   * macOS/Linux:

     ```bash
     source .venv/bin/activate
     ```

   When activated, your prompt shows `(.venv)` at the start.

5. **Install the project and tools**

   ```bash
   pip install -e .[dev]
   ```

6. **Copy the default settings**

   ```bash
   cp .env.example .env
   ```

   On Windows without Git Bash, use `copy .env.example .env` instead. The `.env` file tells the app where to put its database
   and export files. You can keep the defaults.

7. **Initialise the database** (creates a SQLite file in the project):

   ```bash
   cards init
   ```

8. **Generate the Excel sample once** (optional but useful for testing):

   ```bash
   python scripts/generate_sample_reprints_xlsx.py
   ```

At this point the application is ready to run.

### Prerequisites

* Python 3.11+
* Node is **not** required. Optional: Docker.

### Installation (condensed)

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

Keep your virtual environment activated (`(.venv)` visible) and run:

```bash
streamlit run cards/streamlit_app.py
```

What happens next:

1. A browser tab opens automatically. If it does not, copy the URL that appears in the terminal (usually `http://localhost:8501`) and paste it into your browser.
2. Click **Upload New Visits** and select `samples/new_visits.csv` (or your own file). When the upload finishes the app says: `List 1 (New Visits) received. Ready for List 2 (Reprints)?`
3. Click **Upload Reprints** and choose `samples/reprints.xlsx`. After it loads, you are asked: `List 2 (Reprints) received. Would you like me to process and merge both lists now?`
4. Press **Process & Merge**. The review screen shows:
   * a validation banner explaining how many rows were normalised and how many need attention,
   * a table with any issues flagged in red,
   * a duplicate panel listing matches with their rule and score.
5. Make inline edits if necessary (click a cell to edit). Use the **Re-run validation** button to re-check your fixes.
6. When you are satisfied, click **Export files**. The app produces two downloads—Combined Master CSV and Stamps.com CSV—and then asks: `Would you like me to export this as a CSV or make any modifications?`

### CLI Usage

You can perform the entire workflow from the terminal. Run each command separately, pressing Enter between them:

```bash
python scripts/generate_sample_reprints_xlsx.py  # one-time helper to materialise the Excel sample
cards import --list new-visits samples/new_visits.csv
cards import --list reprints samples/reprints.xlsx
cards merge --batch-label "2025-02 Wave A"
cards export --batch 1 --out-dir ./exports
cards history --clinic "HappyMD" --since 2025-01-01
```

What these commands do:

1. `cards import` stores each list in the database and reports validation warnings immediately.
2. `cards merge` combines the two lists into a batch, shows duplicate matches, and prints the batch ID (note this number).
3. `cards export` generates the Combined Master and Stamps.com CSV files in the folder you choose (create `exports` first if it does not exist).
4. `cards history` lets you review past batches and filter by clinic or date.

Tip: the CLI prints the exact file paths for the exports so you can open them in Excel or upload to Stamps.com.

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
