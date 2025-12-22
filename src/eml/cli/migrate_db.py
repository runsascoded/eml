"""Database rebuild command for regenerating index.db from .eml files."""

import click
from click import echo, option, style

from ..config import find_eml_root
from ..index import FileIndex, INDEX_DB
from .utils import require_init


@click.command("rebuild-index")
@require_init
@option('-f', '--force', is_flag=True, help="Force rebuild even if index exists")
@option('-v', '--verbose', is_flag=True, help="Show each file being indexed")
def rebuild_index(force: bool, verbose: bool):
    """Rebuild index.db from .eml files on disk.

    This regenerates all metadata (subject, from, to, threading, FTS)
    by parsing the .eml files. Use this when the index becomes corrupt.

    \b
    Examples:
      eml rebuild-index     # Rebuild index
      eml rebuild-index -v  # Verbose output
    """
    root = find_eml_root()
    eml_dir = root / ".eml"

    echo("Rebuilding index.db from .eml files...")
    echo(f"Root: {root}")

    with FileIndex(eml_dir) as index:
        def progress(current: int, total: int):
            if verbose:
                echo(f"  [{current+1}/{total}]", nl=False)
            elif current % 1000 == 0 and current > 0:
                echo(f"  {current:,}...", nl=False)

        indexed, skipped, errors = index.rebuild(progress_callback=progress)

        echo()
        echo(f"Indexed: {indexed:,} files")
        if skipped:
            echo(f"Skipped: {skipped:,} files")
        if errors:
            echo(style(f"Errors: {errors:,} files", fg="red"))

    index_path = eml_dir / INDEX_DB
    if index_path.exists():
        size = index_path.stat().st_size
        echo(f"Index size: {size:,} bytes ({size / 1024 / 1024:.1f} MB)")

    echo()
    echo(style("Index rebuilt successfully!", fg="green"))
