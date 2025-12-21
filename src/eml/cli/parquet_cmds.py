"""CLI commands for parquet-based UID storage."""

import click
from click import echo, option, style

from ..config import find_eml_root
from ..parquet import (
    UIDS_PARQUET,
    export_uids_to_parquet,
    import_uids_from_parquet,
    parquet_stats,
)
from .utils import require_init


@click.command("export-uids")
@require_init
@option('-o', '--output', type=click.Path(), help="Output parquet file path")
def export_uids(output: str | None):
    """Export pulled UIDs to parquet for Git tracking.

    Creates a minimal parquet file containing only the critical UID data:
    (account, folder, uidvalidity, uid, content_hash)

    This file should be Git-tracked. It's typically ~2-3MB for 65k messages.

    \\b
    Examples:
      eml export-uids                    # Export to .eml/uids.parquet
      eml export-uids -o uids.parquet    # Export to custom path
    """
    from pathlib import Path

    root = find_eml_root()
    eml_dir = root / ".eml"

    output_path = Path(output) if output else None

    echo("Exporting UIDs to parquet...")
    try:
        path = export_uids_to_parquet(eml_dir, output_path)
    except FileNotFoundError as e:
        echo(style(str(e), fg="red"))
        raise SystemExit(1)

    stats = parquet_stats(eml_dir if not output else path.parent)
    if stats:
        echo(f"Rows: {stats['rows']:,}")
        echo(f"Size: {stats['file_size_mb']:.2f} MB")
        echo(f"Folders: {len(stats['folders'])}")

    echo()
    echo(style(f"Exported to: {path}", fg="green"))
    echo()
    echo("Next steps:")
    echo(f"  git add {path}")


@click.command("import-uids")
@require_init
@option('-i', '--input', 'input_path', type=click.Path(exists=True), help="Input parquet file path")
@option('-f', '--force', is_flag=True, help="Overwrite existing uids.db")
def import_uids(input_path: str | None, force: bool):
    """Import UIDs from parquet into uids.db.

    Rebuilds uids.db from the parquet file. Use this after cloning
    a repo or when uids.db needs to be regenerated.

    \\b
    Examples:
      eml import-uids                    # Import from .eml/uids.parquet
      eml import-uids -i uids.parquet    # Import from custom path
    """
    from pathlib import Path

    root = find_eml_root()
    eml_dir = root / ".eml"

    parquet_path = Path(input_path) if input_path else (eml_dir / UIDS_PARQUET)
    db_path = eml_dir / "uids.db"

    if db_path.exists() and not force:
        echo(style(f"uids.db already exists. Use --force to overwrite.", fg="red"))
        raise SystemExit(1)

    echo(f"Importing from: {parquet_path}")

    try:
        count = import_uids_from_parquet(eml_dir, parquet_path)
    except FileNotFoundError as e:
        echo(style(str(e), fg="red"))
        raise SystemExit(1)

    echo(f"Imported: {count:,} rows")

    db_size = db_path.stat().st_size
    echo(f"Database size: {db_size / 1024 / 1024:.2f} MB")

    echo()
    echo(style("Import complete!", fg="green"))


@click.command("uids-stats")
@require_init
def uids_stats():
    """Show statistics about the UIDs parquet file.

    \\b
    Examples:
      eml uids-stats
    """
    root = find_eml_root()
    eml_dir = root / ".eml"

    stats = parquet_stats(eml_dir)
    if not stats:
        echo(style(f"No parquet file at {eml_dir / UIDS_PARQUET}", fg="yellow"))
        echo("Run 'eml export-uids' to create it.")
        return

    echo(f"Parquet file: {eml_dir / UIDS_PARQUET}")
    echo(f"Total rows: {stats['rows']:,}")
    echo(f"File size: {stats['file_size_mb']:.2f} MB")
    echo()
    echo("Folders:")
    for folder, count in sorted(stats['folders'].items(), key=lambda x: -x[1]):
        echo(f"  {folder}: {count:,}")
