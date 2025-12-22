"""Parquet-based UID storage for Git-friendly critical data.

The critical data that MUST be Git-tracked is just:
  (account, folder, uidvalidity, uid, content_hash)

This maps IMAP UIDs to content hashes, which link to .eml files on disk.
Everything else (message_id, local_path, metadata, FTS) is regenerable.

File: .eml/uids.parquet (~2.5MB for 65k messages)
"""

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


UIDS_PARQUET = "uids.parquet"

# Minimal schema: just what's needed for incremental pulls
SCHEMA = pa.schema([
    ("account", pa.string()),
    ("folder", pa.string()),
    ("uidvalidity", pa.int64()),
    ("uid", pa.int64()),
    ("content_hash", pa.string()),
])


def export_uids_to_parquet(eml_dir: Path, output_path: Path | None = None) -> Path:
    """Export pulled UIDs from uids.db to parquet.

    Args:
        eml_dir: Path to .eml directory
        output_path: Output path (defaults to eml_dir/uids.parquet)

    Returns:
        Path to the created parquet file
    """
    import sqlite3

    output_path = output_path or (eml_dir / UIDS_PARQUET)
    db_path = eml_dir / "uids.db"

    if not db_path.exists():
        # Fall back to pulls.db for migration
        db_path = eml_dir / "pulls.db"
        table_name = "pulled_messages"
    else:
        table_name = "pulled_uids"

    if not db_path.exists():
        raise FileNotFoundError(f"No UID database found at {db_path}")

    conn = sqlite3.connect(db_path)
    cur = conn.execute(f"""
        SELECT account, folder, uidvalidity, uid, content_hash
        FROM {table_name}
        ORDER BY account, folder, uidvalidity, uid
    """)
    rows = cur.fetchall()
    conn.close()

    # Build arrays
    accounts = []
    folders = []
    uidvalidities = []
    uids = []
    hashes = []

    for row in rows:
        accounts.append(row[0])
        folders.append(row[1])
        uidvalidities.append(row[2])
        uids.append(row[3])
        hashes.append(row[4])

    table = pa.table({
        "account": accounts,
        "folder": folders,
        "uidvalidity": uidvalidities,
        "uid": uids,
        "content_hash": hashes,
    }, schema=SCHEMA)

    pq.write_table(
        table,
        output_path,
        compression="zstd",
        compression_level=19,  # Max compression for Git
    )

    return output_path


def import_uids_from_parquet(eml_dir: Path, parquet_path: Path | None = None) -> int:
    """Import UIDs from parquet into uids.db.

    Args:
        eml_dir: Path to .eml directory
        parquet_path: Parquet file path (defaults to eml_dir/uids.parquet)

    Returns:
        Number of rows imported
    """
    import sqlite3
    from datetime import datetime

    parquet_path = parquet_path or (eml_dir / UIDS_PARQUET)

    if not parquet_path.exists():
        raise FileNotFoundError(f"No parquet file at {parquet_path}")

    # Read parquet
    table = pq.read_table(parquet_path)

    # Create/connect to uids.db
    db_path = eml_dir / "uids.db"
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")

    # Create schema
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS pulled_uids (
            account TEXT NOT NULL,
            folder TEXT NOT NULL,
            uidvalidity INTEGER NOT NULL,
            uid INTEGER NOT NULL,
            content_hash TEXT NOT NULL,
            message_id TEXT,
            local_path TEXT,
            pulled_at TEXT NOT NULL,
            PRIMARY KEY (account, folder, uidvalidity, uid)
        );

        CREATE INDEX IF NOT EXISTS idx_pulled_uids_hash
            ON pulled_uids(content_hash);
    """)

    # Insert rows
    now = datetime.now().isoformat()
    rows = [
        (
            table["account"][i].as_py(),
            table["folder"][i].as_py(),
            table["uidvalidity"][i].as_py(),
            table["uid"][i].as_py(),
            table["content_hash"][i].as_py(),
            None,  # message_id - will be populated from index
            None,  # local_path - will be populated from index
            now,   # pulled_at
        )
        for i in range(len(table))
    ]

    conn.executemany("""
        INSERT OR REPLACE INTO pulled_uids
            (account, folder, uidvalidity, uid, content_hash, message_id, local_path, pulled_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)
    conn.commit()

    count = len(rows)
    conn.close()

    return count


def get_pulled_uids_from_parquet(
    eml_dir: Path,
    account: str,
    folder: str,
    uidvalidity: int,
) -> set[int]:
    """Get pulled UIDs from parquet file directly (no SQLite needed).

    This is useful for quick lookups during pulls.
    """
    parquet_path = eml_dir / UIDS_PARQUET

    if not parquet_path.exists():
        return set()

    # Use predicate pushdown for efficient filtering
    filters = [
        ("account", "=", account),
        ("folder", "=", folder),
        ("uidvalidity", "=", uidvalidity),
    ]

    table = pq.read_table(parquet_path, filters=filters, columns=["uid"])
    return {row.as_py() for row in table["uid"]}


def get_all_content_hashes_from_parquet(eml_dir: Path) -> set[str]:
    """Get all content hashes from parquet (for dedup checks)."""
    parquet_path = eml_dir / UIDS_PARQUET

    if not parquet_path.exists():
        return set()

    table = pq.read_table(parquet_path, columns=["content_hash"])
    return {row.as_py() for row in table["content_hash"]}


def parquet_stats(eml_dir: Path) -> dict | None:
    """Get stats about the parquet file."""
    parquet_path = eml_dir / UIDS_PARQUET

    if not parquet_path.exists():
        return None

    table = pq.read_table(parquet_path)
    file_size = parquet_path.stat().st_size

    # Count by folder
    folders: dict[str, int] = {}
    for i in range(len(table)):
        folder = table["folder"][i].as_py()
        folders[folder] = folders.get(folder, 0) + 1

    return {
        "rows": len(table),
        "file_size": file_size,
        "file_size_mb": round(file_size / 1024 / 1024, 2),
        "folders": folders,
    }
