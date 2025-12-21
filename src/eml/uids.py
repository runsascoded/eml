"""Critical UID tracking database.

This module provides the UidsDB class for tracking which UIDs have been
pulled from IMAP servers. This data is CRITICAL for incremental pulls:
- Without it, we don't know which UIDs to skip on incremental pulls
- Losing it means re-pulling everything (expensive, slow)

The critical data is Git-tracked as uids.parquet (~2.3MB).
The SQLite database (uids.db) is auto-rebuilt from parquet when needed.

The actual email metadata (subject, from, to, threading, FTS) is stored
separately in index.db (regenerable from .eml files).
"""

import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


UIDS_DB = "uids.db"


@dataclass
class PulledUID:
    """Record of a successfully pulled message UID."""
    account: str
    folder: str
    uidvalidity: int
    uid: int
    content_hash: str
    message_id: str | None
    local_path: str | None
    pulled_at: datetime


class UidsDB:
    """Critical SQLite database tracking pulled UIDs.

    This is the authoritative record of which messages we've successfully
    fetched from each server. Git-tracked for durability.

    Tables:
    - pulled_uids: (account, folder, uidvalidity, uid) -> content_hash, message_id, local_path
    - server_uids: Snapshot of UIDs seen on server
    - server_folders: Folder metadata (uidvalidity, message_count)
    """

    def __init__(self, eml_dir: Path):
        """Initialize UidsDB.

        Args:
            eml_dir: Path to .eml directory (e.g., /path/to/project/.eml)
        """
        self._eml_dir = eml_dir
        self._db_path = eml_dir / UIDS_DB
        self._conn: sqlite3.Connection | None = None

    @property
    def db_path(self) -> Path:
        return self._db_path

    def connect(self) -> None:
        """Open database connection, rebuilding from parquet if needed."""
        self._db_path.parent.mkdir(parents=True, exist_ok=True)

        # Check if we need to rebuild from parquet
        if self._needs_rebuild_from_parquet():
            self._rebuild_from_parquet()

        self._conn = sqlite3.connect(self._db_path, timeout=30.0)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._create_schema()

    def _needs_rebuild_from_parquet(self) -> bool:
        """Check if uids.db needs to be rebuilt from parquet.

        Rebuild is needed if:
        1. parquet exists AND
        2. (db doesn't exist OR parquet is newer than db)
        """
        from .parquet import UIDS_PARQUET

        parquet_path = self._eml_dir / UIDS_PARQUET

        if not parquet_path.exists():
            return False

        if not self._db_path.exists():
            return True

        # Compare mtimes - rebuild if parquet is newer
        return parquet_path.stat().st_mtime > self._db_path.stat().st_mtime

    def _rebuild_from_parquet(self) -> None:
        """Rebuild uids.db from parquet file."""
        from .parquet import UIDS_PARQUET, import_uids_from_parquet

        parquet_path = self._eml_dir / UIDS_PARQUET
        print(f"Rebuilding {UIDS_DB} from {parquet_path.name}...", file=sys.stderr)

        # Remove existing db if present
        if self._db_path.exists():
            self._db_path.unlink()

        count = import_uids_from_parquet(self._eml_dir, parquet_path)
        print(f"Imported {count:,} UIDs from parquet", file=sys.stderr)

    def disconnect(self) -> None:
        """Close database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None

    @property
    def conn(self) -> sqlite3.Connection:
        if not self._conn:
            raise RuntimeError("Not connected")
        return self._conn

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.disconnect()

    def _create_schema(self) -> None:
        """Create database schema."""
        self.conn.executescript("""
            -- Core UID tracking: which messages we've pulled
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

            -- Index by content_hash for dedup queries
            CREATE INDEX IF NOT EXISTS idx_pulled_uids_hash
                ON pulled_uids(content_hash);

            -- Index by message_id for cross-reference
            CREATE INDEX IF NOT EXISTS idx_pulled_uids_message_id
                ON pulled_uids(message_id);

            -- Server UIDs: snapshot of what the server reports
            CREATE TABLE IF NOT EXISTS server_uids (
                account TEXT NOT NULL,
                folder TEXT NOT NULL,
                uidvalidity INTEGER NOT NULL,
                uid INTEGER NOT NULL,
                message_id TEXT,
                last_seen TEXT NOT NULL,
                PRIMARY KEY (account, folder, uidvalidity, uid)
            );

            CREATE INDEX IF NOT EXISTS idx_server_uids_folder
                ON server_uids(account, folder, uidvalidity);

            CREATE INDEX IF NOT EXISTS idx_server_uids_message_id
                ON server_uids(message_id);

            -- Server folder metadata
            CREATE TABLE IF NOT EXISTS server_folders (
                account TEXT NOT NULL,
                folder TEXT NOT NULL,
                uidvalidity INTEGER NOT NULL,
                message_count INTEGER,
                last_checked TEXT NOT NULL,
                PRIMARY KEY (account, folder)
            );
        """)
        self.conn.commit()

    # -------------------------------------------------------------------------
    # Pulled UIDs tracking
    # -------------------------------------------------------------------------

    def record_pull(
        self,
        account: str,
        folder: str,
        uidvalidity: int,
        uid: int,
        content_hash: str,
        message_id: str | None = None,
        local_path: str | None = None,
        pulled_at: datetime | None = None,
    ) -> None:
        """Record a successfully pulled message.

        Args:
            account: Account name (e.g., 'y' for Yahoo)
            folder: Folder name (e.g., 'Inbox')
            uidvalidity: IMAP UIDVALIDITY value
            uid: Message UID
            content_hash: SHA256 of raw message bytes
            message_id: Message-ID header (optional)
            local_path: Path where message was stored (optional)
            pulled_at: When the message was pulled (defaults to now)
        """
        ts = (pulled_at or datetime.now()).isoformat()
        self.conn.execute("""
            INSERT OR REPLACE INTO pulled_uids
                (account, folder, uidvalidity, uid, content_hash, message_id, local_path, pulled_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (account, folder, uidvalidity, uid, content_hash, message_id, local_path, ts))
        self.conn.commit()

    def get_pulled_uids(
        self,
        account: str,
        folder: str,
        uidvalidity: int,
    ) -> set[int]:
        """Get all UIDs we've pulled for this account/folder/uidvalidity.

        Returns:
            Set of UIDs that have been successfully pulled
        """
        cur = self.conn.execute("""
            SELECT uid FROM pulled_uids
            WHERE account = ? AND folder = ? AND uidvalidity = ?
        """, (account, folder, uidvalidity))
        return {row["uid"] for row in cur}

    def get_pulled_count(
        self,
        account: str,
        folder: str,
        uidvalidity: int | None = None,
    ) -> int:
        """Get count of pulled messages for account/folder."""
        if uidvalidity is not None:
            cur = self.conn.execute("""
                SELECT COUNT(*) FROM pulled_uids
                WHERE account = ? AND folder = ? AND uidvalidity = ?
            """, (account, folder, uidvalidity))
        else:
            cur = self.conn.execute("""
                SELECT COUNT(*) FROM pulled_uids
                WHERE account = ? AND folder = ?
            """, (account, folder))
        return cur.fetchone()[0]

    def has_content_hash(self, content_hash: str) -> bool:
        """Check if we've pulled a message with this content hash (any account/folder)."""
        cur = self.conn.execute(
            "SELECT 1 FROM pulled_uids WHERE content_hash = ? LIMIT 1",
            (content_hash,)
        )
        return cur.fetchone() is not None

    def get_all_content_hashes(self) -> set[str]:
        """Get all content hashes we've ever pulled."""
        cur = self.conn.execute("SELECT DISTINCT content_hash FROM pulled_uids")
        return {row["content_hash"] for row in cur}

    def get_uidvalidity(self, account: str, folder: str) -> int | None:
        """Get the UIDVALIDITY we have on record for this folder.

        Returns None if no records exist for this account/folder.
        """
        cur = self.conn.execute("""
            SELECT DISTINCT uidvalidity FROM pulled_uids
            WHERE account = ? AND folder = ?
        """, (account, folder))
        rows = cur.fetchall()
        if not rows:
            return None
        if len(rows) > 1:
            # Multiple UIDVALIDITYs - folder was reset at some point
            # Return the most recent one (highest count)
            cur = self.conn.execute("""
                SELECT uidvalidity, COUNT(*) as cnt FROM pulled_uids
                WHERE account = ? AND folder = ?
                GROUP BY uidvalidity
                ORDER BY cnt DESC
                LIMIT 1
            """, (account, folder))
            return cur.fetchone()["uidvalidity"]
        return rows[0]["uidvalidity"]

    def get_path_by_content_hash(self, content_hash: str) -> str | None:
        """Get local_path for a content hash (for dedup display)."""
        cur = self.conn.execute(
            "SELECT local_path FROM pulled_uids WHERE content_hash = ? AND local_path IS NOT NULL LIMIT 1",
            (content_hash,)
        )
        row = cur.fetchone()
        return row["local_path"] if row else None

    def get_folders_with_activity(self, account: str | None = None) -> list[tuple[str, str, int]]:
        """Get list of folders that have pull activity.

        Returns:
            List of (account, folder, pull_count) tuples
        """
        if account:
            cur = self.conn.execute("""
                SELECT account, folder, COUNT(*) as cnt
                FROM pulled_uids
                WHERE account = ?
                GROUP BY account, folder
                ORDER BY cnt DESC
            """, (account,))
        else:
            cur = self.conn.execute("""
                SELECT account, folder, COUNT(*) as cnt
                FROM pulled_uids
                GROUP BY account, folder
                ORDER BY cnt DESC
            """)
        return [(row["account"], row["folder"], row["cnt"]) for row in cur]

    # -------------------------------------------------------------------------
    # Server UIDs tracking
    # -------------------------------------------------------------------------

    def record_server_uids(
        self,
        account: str,
        folder: str,
        uidvalidity: int,
        uid_message_ids: list[tuple[int, str | None]],
    ) -> None:
        """Record UIDs seen on server (with optional Message-IDs)."""
        now = datetime.now().isoformat()
        self.conn.executemany("""
            INSERT OR REPLACE INTO server_uids
                (account, folder, uidvalidity, uid, message_id, last_seen)
            VALUES (?, ?, ?, ?, ?, ?)
        """, [(account, folder, uidvalidity, uid, mid, now) for uid, mid in uid_message_ids])
        self.conn.commit()

    def record_server_folder(
        self,
        account: str,
        folder: str,
        uidvalidity: int,
        message_count: int,
    ) -> None:
        """Record server folder metadata."""
        self.conn.execute("""
            INSERT OR REPLACE INTO server_folders
                (account, folder, uidvalidity, message_count, last_checked)
            VALUES (?, ?, ?, ?, ?)
        """, (account, folder, uidvalidity, message_count, datetime.now().isoformat()))
        self.conn.commit()

    def get_server_uids(
        self,
        account: str,
        folder: str,
        uidvalidity: int,
    ) -> set[int]:
        """Get all UIDs we've seen on server for this folder."""
        cur = self.conn.execute("""
            SELECT uid FROM server_uids
            WHERE account = ? AND folder = ? AND uidvalidity = ?
        """, (account, folder, uidvalidity))
        return {row["uid"] for row in cur}

    def get_server_uid_count(self, account: str, folder: str) -> int:
        """Get count of UIDs tracked for server folder."""
        cur = self.conn.execute("""
            SELECT COUNT(*) FROM server_uids
            WHERE account = ? AND folder = ?
        """, (account, folder))
        return cur.fetchone()[0]

    def get_server_folder_info(
        self,
        account: str,
        folder: str,
    ) -> tuple[int, int, str] | None:
        """Get server folder metadata (uidvalidity, message_count, last_checked).

        Returns:
            Tuple of (uidvalidity, message_count, last_checked) or None if not found.
        """
        cur = self.conn.execute("""
            SELECT uidvalidity, message_count, last_checked FROM server_folders
            WHERE account = ? AND folder = ?
        """, (account, folder))
        row = cur.fetchone()
        if row:
            return (row["uidvalidity"], row["message_count"], row["last_checked"])
        return None

    def get_unpulled_uids(
        self,
        account: str,
        folder: str,
        uidvalidity: int,
    ) -> set[int]:
        """Get UIDs that are on server but not pulled."""
        cur = self.conn.execute("""
            SELECT s.uid FROM server_uids s
            LEFT JOIN pulled_uids p
                ON s.account = p.account
                AND s.folder = p.folder
                AND s.uidvalidity = p.uidvalidity
                AND s.uid = p.uid
            WHERE s.account = ? AND s.folder = ? AND s.uidvalidity = ?
                AND p.uid IS NULL
        """, (account, folder, uidvalidity))
        return {row["uid"] for row in cur}

    def get_uids_without_message_id(
        self,
        account: str,
        folder: str,
        uidvalidity: int,
    ) -> set[int]:
        """Get server UIDs that have no Message-ID."""
        cur = self.conn.execute("""
            SELECT uid FROM server_uids
            WHERE account = ? AND folder = ? AND uidvalidity = ?
                AND (message_id IS NULL OR message_id = '')
        """, (account, folder, uidvalidity))
        return {row["uid"] for row in cur}

    def clear_folder(
        self,
        account: str,
        folder: str,
        uidvalidity: int | None = None,
    ) -> int:
        """Clear pull records for a folder.

        Returns:
            Number of records deleted
        """
        if uidvalidity is not None:
            cur = self.conn.execute("""
                DELETE FROM pulled_uids
                WHERE account = ? AND folder = ? AND uidvalidity = ?
            """, (account, folder, uidvalidity))
        else:
            cur = self.conn.execute("""
                DELETE FROM pulled_uids
                WHERE account = ? AND folder = ?
            """, (account, folder))
        self.conn.commit()
        return cur.rowcount

    def get_stats(self, account: str | None = None) -> dict:
        """Get statistics about pulled UIDs.

        Returns:
            Dict with counts per folder, total, etc.
        """
        stats: dict = {"total": 0, "folders": {}}

        if account:
            cur = self.conn.execute("""
                SELECT folder, uidvalidity, COUNT(*) as count
                FROM pulled_uids
                WHERE account = ?
                GROUP BY folder, uidvalidity
            """, (account,))
        else:
            cur = self.conn.execute("""
                SELECT account, folder, uidvalidity, COUNT(*) as count
                FROM pulled_uids
                GROUP BY account, folder, uidvalidity
            """)

        for row in cur:
            folder = row["folder"]
            count = row["count"]
            stats["total"] += count
            if folder not in stats["folders"]:
                stats["folders"][folder] = 0
            stats["folders"][folder] += count

        return stats


def get_uids_db(root: Path | None = None) -> UidsDB:
    """Get UidsDB instance for the current project.

    Args:
        root: Project root (auto-detected if None)

    Returns:
        UidsDB instance (not yet connected - use as context manager)
    """
    from .config import get_eml_root
    root = root or get_eml_root()
    return UidsDB(root / ".eml")
