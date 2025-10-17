from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from . import simple_pandas as pd
import typer

from . import init_db
from .config import get_settings
from .db import session_scope
from .processing import DataProcessor

app = typer.Typer(help="Card Printing Table CLI")


@app.command()
def init(db_url: Optional[str] = typer.Option(None, help="Override database URL")) -> None:
    """Initialise the database."""
    if db_url:
        settings = get_settings()
        settings.db_url = db_url
    init_db()
    typer.echo("Database initialised.")


@app.command()
def import_list(
    list_type: str = typer.Option(..., "--list", help="List type: new-visits or reprints"),
    path: Path = typer.Argument(..., exists=True),
    batch_label: Optional[str] = typer.Option(None, help="Optional batch label"),
) -> None:
    """Import two lists sequentially and merge them."""
    if list_type not in {"new-visits", "reprints"}:
        raise typer.BadParameter("list must be 'new-visits' or 'reprints'")

    settings = get_settings()
    init_db()
    processor = DataProcessor()

    df = processor.read_input(path)
    mapping = processor.map_headers(df, "new_visit" if list_type == "new-visits" else "reprint")

    temp_path = settings.export_dir / f"pending_{list_type}.parquet"
    df.to_parquet(temp_path, index=False)
    mapping_path = settings.export_dir / f"pending_{list_type}_mapping.json"
    mapping_path.write_text(json.dumps(mapping.mapping), encoding="utf-8")

    typer.echo(f"Stored {list_type} data for later merge at {temp_path}")


@app.command()
def merge(
    batch_label: Optional[str] = typer.Option(None, help="Optional label for batch"),
) -> None:
    """Merge previously imported new visits and reprints."""
    settings = get_settings()
    init_db()
    processor = DataProcessor()

    new_path = settings.export_dir / "pending_new-visits.parquet"
    reprint_path = settings.export_dir / "pending_reprints.parquet"

    if not new_path.exists() or not reprint_path.exists():
        raise typer.BadParameter("Both new visits and reprints must be imported before merge.")

    new_df = pd.read_parquet(new_path)
    re_df = pd.read_parquet(reprint_path)

    new_mapping_data = json.loads((settings.export_dir / "pending_new-visits_mapping.json").read_text("utf-8"))
    re_mapping_data = json.loads((settings.export_dir / "pending_reprints_mapping.json").read_text("utf-8"))

    from .header_mapping import HeaderMappingResult

    new_mapping = HeaderMappingResult(mapping=new_mapping_data, missing=[], extras=[])
    re_mapping = HeaderMappingResult(mapping=re_mapping_data, missing=[], extras=[])

    report = processor.process(new_df, re_df, new_mapping, re_mapping, batch_label=batch_label)

    typer.echo(json.dumps(report.summary, indent=2))
    typer.echo("Duplicate reports:")
    typer.echo(json.dumps(report.duplicate_reports, indent=2))
    typer.echo(f"Batch ID: {report.batch_id}")

    with open(settings.export_dir / "latest_report.json", "w", encoding="utf-8") as fp:
        json.dump({
            "summary": report.summary,
            "issues": report.issues,
            "duplicates": report.duplicate_reports,
        }, fp, indent=2)

    typer.echo("Merge complete. Use `cards export` to create CSV outputs.")


@app.command()
def export(
    batch_id: int = typer.Option(..., "--batch", help="Batch ID to export"),
    out_dir: Optional[Path] = typer.Option(None, help="Override export directory"),
) -> None:
    """Export CSV files for a batch."""
    init_db()
    settings = get_settings()
    if out_dir:
        settings.export_dir = out_dir
        settings.export_dir.mkdir(parents=True, exist_ok=True)
    processor = DataProcessor()

    with session_scope() as conn:
        batch = conn.execute("SELECT id FROM batches WHERE id = ?", (batch_id,)).fetchone()
        if not batch:
            raise typer.BadParameter(f"Batch {batch_id} not found")
        rows = [dict(row) for row in conn.execute("SELECT * FROM entries WHERE batch_id = ?", (batch_id,)).fetchall()]
    entries = [processor.from_entry_record(row) for row in rows]

    stamps, combined = processor.export(entries, batch_id)
    typer.echo(f"Exports written to {stamps} and {combined}")


@app.command()
def history(
    clinic: Optional[str] = typer.Option(None),
    since: Optional[str] = typer.Option(None),
) -> None:
    """Display history of batches."""
    from .db import session_scope
    from .models import Batch, Entry

    init_db()

    with session_scope() as conn:
        params = []
        query = "SELECT id, label, created_at FROM batches"
        if since:
            query += " WHERE created_at >= ?"
            params.append(since)
        query += " ORDER BY created_at DESC"
        typer.echo("Batches:")
        for row in conn.execute(query, params).fetchall():
            label = row["label"] or "No label"
            created = row["created_at"]
            typer.echo(f"- {row['id']}: {label} ({created[:10]})")
            if clinic:
                entries = conn.execute(
                    "SELECT normalized_payload_json FROM entries WHERE batch_id = ?",
                    (row["id"],),
                ).fetchall()
                count = sum(
                    1
                    for entry_row in entries
                    if json.loads(entry_row["normalized_payload_json"]).get("clinic_name") == clinic
                )
                typer.echo(f"  Entries for clinic {clinic}: {count}")


if __name__ == "__main__":
    app()
