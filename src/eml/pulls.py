"""Persistent tracking of pulled messages by UID.

This module provides robust per-UID tracking for email pulls, replacing the
fragile `last_uid` approach that lost track of gaps from failed fetches.

The pulls.db file is Git-tracked, enabling:
- Resume across sessions/machines
- Exact knowledge of which UIDs we've fetched
- Retry only truly missing UIDs (not re-fetch successes)
- Survive UIDVALIDITY changes via content_hash fallback
"""

import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


PULLS_DB = "pulls.db"


@dataclass
class PulledMessage:
    """Record of a successfully pulled message."""
    account: str
    folder: str
    uidvalidity: int
    uid: int
    content_hash: str
    message_id: str | None
    local_path: str | None
    pulled_at: datetime


class PullsDB:
    """Persistent SQLite database tracking pulled messages by UID.

    This is the authoritative record of which messages we've successfully
    fetched from each server. Git-tracked for durability.
    """

    def __init__(self, eml_dir: Path):
        """Initialize PullsDB.

        Args:
            eml_dir: Path to .eml directory (e.g., /path/to/project/.eml)
        """
        self._eml_dir = eml_dir
        self._db_path = eml_dir / PULLS_DB
        self._conn: sqlite3.Connection | None = None

    @property
    def db_path(self) -> Path:
        return self._db_path

    def connect(self) -> None:
        """Open database connection and create schema if needed."""
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self._db_path, timeout=30.0)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._create_schema()

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
            CREATE TABLE IF NOT EXISTS pulled_messages (
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

            -- Index by content_hash for "do we have this content anywhere?" queries
            CREATE INDEX IF NOT EXISTS idx_pulled_hash
                ON pulled_messages(content_hash);

            -- Index by message_id for cross-reference queries
            CREATE INDEX IF NOT EXISTS idx_pulled_message_id
                ON pulled_messages(message_id);

            -- Index for folder queries
            CREATE INDEX IF NOT EXISTS idx_pulled_folder
                ON pulled_messages(account, folder);
        """)

    def record_pull(
        self,
        account: str,
        folder: str,
        uidvalidity: int,
        uid: int,
        content_hash: str,
        message_id: str | None = None,
        local_path: str | None = None,
    ) -> None:
        """Record a successfully pulled message.

        Args:
            account: Account name (e.g., 'y' for Yahoo)
            folder: Folder name (e.g., 'Inbox')
            uidvalidity: IMAP UIDVALIDITY value
            uid: Message UID
            content_hash: SHA256 of raw message bytes
            message_id: Message-ID header (optional, for reference)
            local_path: Path where message was stored (optional, None if deduped)
        """
        self.conn.execute("""
            INSERT OR REPLACE INTO pulled_messages
                (account, folder, uidvalidity, uid, content_hash, message_id, local_path, pulled_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            account, folder, uidvalidity, uid, content_hash,
            message_id, local_path, datetime.now().isoformat()
        ))
        self.conn.commit()

    def record_pulls_batch(
        self,
        records: list[tuple[str, str, int, int, str, str | None, str | None]],
    ) -> None:
        """Batch record multiple pulled messages.

        Args:
            records: List of (account, folder, uidvalidity, uid, content_hash, message_id, local_path)
        """
        now = datetime.now().isoformat()
        self.conn.executemany("""
            INSERT OR REPLACE INTO pulled_messages
                (account, folder, uidvalidity, uid, content_hash, message_id, local_path, pulled_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, [(r[0], r[1], r[2], r[3], r[4], r[5], r[6], now) for r in records])
        self.conn.commit()

    def get_pulled_uids(
        self,
        account: str,
        folder: str,
        uidvalidity: int,
    ) -> set[int]:
        """Get all UIDs we've pulled for this account/folder/uidvalidity.

        Args:
            account: Account name
            folder: Folder name
            uidvalidity: IMAP UIDVALIDITY value

        Returns:
            Set of UIDs that have been successfully pulled
        """
        cur = self.conn.execute("""
            SELECT uid FROM pulled_messages
            WHERE account = ? AND folder = ? AND uidvalidity = ?
        """, (account, folder, uidvalidity))
        return {row["uid"] for row in cur}

    def get_pulled_count(
        self,
        account: str,
        folder: str,
        uidvalidity: int | None = None,
    ) -> int:
        """Get count of pulled messages for account/folder.

        Args:
            account: Account name
            folder: Folder name
            uidvalidity: Optional UIDVALIDITY filter

        Returns:
            Number of pulled messages
        """
        if uidvalidity is not None:
            cur = self.conn.execute("""
                SELECT COUNT(*) FROM pulled_messages
                WHERE account = ? AND folder = ? AND uidvalidity = ?
            """, (account, folder, uidvalidity))
        else:
            cur = self.conn.execute("""
                SELECT COUNT(*) FROM pulled_messages
                WHERE account = ? AND folder = ?
            """, (account, folder))
        return cur.fetchone()[0]

    def has_content_hash(self, content_hash: str) -> bool:
        """Check if we've pulled a message with this content hash (any account/folder)."""
        cur = self.conn.execute(
            "SELECT 1 FROM pulled_messages WHERE content_hash = ? LIMIT 1",
            (content_hash,)
        )
        return cur.fetchone() is not None

    def get_all_content_hashes(self) -> set[str]:
        """Get all content hashes we've ever pulled."""
        cur = self.conn.execute("SELECT DISTINCT content_hash FROM pulled_messages")
        return {row["content_hash"] for row in cur}

    def get_stats(self, account: str | None = None) -> dict:
        """Get statistics about pulled messages.

        Args:
            account: Optional account filter

        Returns:
            Dict with counts per folder, total, etc.
        """
        stats: dict = {"total": 0, "folders": {}}

        if account:
            cur = self.conn.execute("""
                SELECT folder, uidvalidity, COUNT(*) as count
                FROM pulled_messages
                WHERE account = ?
                GROUP BY folder, uidvalidity
            """, (account,))
        else:
            cur = self.conn.execute("""
                SELECT account, folder, uidvalidity, COUNT(*) as count
                FROM pulled_messages
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

    def get_uidvalidity(self, account: str, folder: str) -> int | None:
        """Get the UIDVALIDITY we have on record for this folder.

        Returns None if no records exist for this account/folder.
        """
        cur = self.conn.execute("""
            SELECT DISTINCT uidvalidity FROM pulled_messages
            WHERE account = ? AND folder = ?
        """, (account, folder))
        rows = cur.fetchall()
        if not rows:
            return None
        if len(rows) > 1:
            # Multiple UIDVALIDITYs - folder was reset at some point
            # Return the most recent one (highest count)
            cur = self.conn.execute("""
                SELECT uidvalidity, COUNT(*) as cnt FROM pulled_messages
                WHERE account = ? AND folder = ?
                GROUP BY uidvalidity
                ORDER BY cnt DESC
                LIMIT 1
            """, (account, folder))
            return cur.fetchone()["uidvalidity"]
        return rows[0]["uidvalidity"]

    def clear_folder(
        self,
        account: str,
        folder: str,
        uidvalidity: int | None = None,
    ) -> int:
        """Clear pull records for a folder.

        Args:
            account: Account name
            folder: Folder name
            uidvalidity: Optional - only clear this UIDVALIDITY

        Returns:
            Number of records deleted
        """
        if uidvalidity is not None:
            cur = self.conn.execute("""
                DELETE FROM pulled_messages
                WHERE account = ? AND folder = ? AND uidvalidity = ?
            """, (account, folder, uidvalidity))
        else:
            cur = self.conn.execute("""
                DELETE FROM pulled_messages
                WHERE account = ? AND folder = ?
            """, (account, folder))
        self.conn.commit()
        return cur.rowcount


def get_pulls_db(root: Path | None = None) -> PullsDB:
    """Get PullsDB instance for the current project.

    Args:
        root: Project root (auto-detected if None)

    Returns:
        PullsDB instance (not yet connected - use as context manager)
    """
    from .config import get_eml_root
    root = root or get_eml_root()
    return PullsDB(root / ".eml")
