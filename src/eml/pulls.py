"""Persistent tracking of pulled messages by UID.

This module provides robust per-UID tracking for email pulls, replacing the
fragile `last_uid` approach that lost track of gaps from failed fetches.

The pulls.db file is Git-tracked, enabling:
- Resume across sessions/machines
- Exact knowledge of which UIDs we've fetched
- Retry only truly missing UIDs (not re-fetch successes)
- Survive UIDVALIDITY changes via content_hash fallback
- Track server state for set operations (what's on server vs local)
"""

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
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


@dataclass
class RecentPull:
    """A recently pulled message with display info."""
    uid: int
    folder: str
    local_path: str
    pulled_at: datetime
    subject: str | None = None
    msg_date: str | None = None


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
            -- Add new columns to existing tables (safe to run multiple times)
            -- SQLite silently ignores if column already exists with pragma
            PRAGMA foreign_keys = OFF;
        """)
        # Migrate: add subject and msg_date columns if missing
        try:
            self.conn.execute("SELECT subject FROM pulled_messages LIMIT 1")
        except sqlite3.OperationalError:
            self.conn.execute("ALTER TABLE pulled_messages ADD COLUMN subject TEXT")
        try:
            self.conn.execute("SELECT msg_date FROM pulled_messages LIMIT 1")
        except sqlite3.OperationalError:
            self.conn.execute("ALTER TABLE pulled_messages ADD COLUMN msg_date TEXT")

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
                subject TEXT,
                msg_date TEXT,
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

            -- Index by pulled_at for "last N downloaded" queries
            CREATE INDEX IF NOT EXISTS idx_pulled_at
                ON pulled_messages(pulled_at DESC);

            -- Server UIDs: snapshot of what the server reports
            -- Updated each time we query the server
            CREATE TABLE IF NOT EXISTS server_uids (
                account TEXT NOT NULL,
                folder TEXT NOT NULL,
                uidvalidity INTEGER NOT NULL,
                uid INTEGER NOT NULL,
                message_id TEXT,
                last_seen TEXT NOT NULL,
                PRIMARY KEY (account, folder, uidvalidity, uid)
            );

            CREATE INDEX IF NOT EXISTS idx_server_folder
                ON server_uids(account, folder, uidvalidity);

            CREATE INDEX IF NOT EXISTS idx_server_message_id
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
        subject: str | None = None,
        msg_date: str | None = None,
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
            pulled_at: When the message was pulled (defaults to now, use file mtime for backfill)
            subject: Email subject (for display)
            msg_date: Original message date (for display)
        """
        ts = (pulled_at or datetime.now()).isoformat()
        self.conn.execute("""
            INSERT OR REPLACE INTO pulled_messages
                (account, folder, uidvalidity, uid, content_hash, message_id, local_path, pulled_at, subject, msg_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            account, folder, uidvalidity, uid, content_hash,
            message_id, local_path, ts, subject, msg_date
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
        """Record UIDs seen on server (with optional Message-IDs).

        Args:
            account: Account name
            folder: Folder name
            uidvalidity: IMAP UIDVALIDITY value
            uid_message_ids: List of (uid, message_id) tuples
        """
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

    def get_folders_with_activity(self, account: str | None = None) -> list[tuple[str, str, int]]:
        """Get list of folders that have pull activity.

        Returns:
            List of (account, folder, pull_count) tuples
        """
        if account:
            cur = self.conn.execute("""
                SELECT account, folder, COUNT(*) as cnt
                FROM pulled_messages
                WHERE account = ?
                GROUP BY account, folder
                ORDER BY cnt DESC
            """, (account,))
        else:
            cur = self.conn.execute("""
                SELECT account, folder, COUNT(*) as cnt
                FROM pulled_messages
                GROUP BY account, folder
                ORDER BY cnt DESC
            """)
        return [(row["account"], row["folder"], row["cnt"]) for row in cur]

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

    def get_unpulled_uids(
        self,
        account: str,
        folder: str,
        uidvalidity: int,
    ) -> set[int]:
        """Get UIDs that are on server but not pulled."""
        cur = self.conn.execute("""
            SELECT s.uid FROM server_uids s
            LEFT JOIN pulled_messages p
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

    # -------------------------------------------------------------------------
    # Recent pulls and analytics
    # -------------------------------------------------------------------------

    def get_recent_pulls(
        self,
        limit: int = 10,
        account: str | None = None,
        folder: str | None = None,
        with_path_only: bool = True,
    ) -> list[RecentPull]:
        """Get most recently pulled messages.

        Args:
            limit: Max number to return
            account: Optional account filter
            folder: Optional folder filter
            with_path_only: Only return pulls that saved a file (not deduped)

        Returns:
            List of RecentPull objects, most recent first
        """
        conditions = []
        params: list = []
        if account:
            conditions.append("account = ?")
            params.append(account)
        if folder:
            conditions.append("folder = ?")
            params.append(folder)
        if with_path_only:
            conditions.append("local_path IS NOT NULL")

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        params.append(limit)

        cur = self.conn.execute(f"""
            SELECT uid, folder, local_path, pulled_at, subject, msg_date
            FROM pulled_messages
            {where}
            ORDER BY pulled_at DESC
            LIMIT ?
        """, params)

        return [
            RecentPull(
                uid=row["uid"],
                folder=row["folder"],
                local_path=row["local_path"],
                pulled_at=datetime.fromisoformat(row["pulled_at"]),
                subject=row["subject"],
                msg_date=row["msg_date"],
            )
            for row in cur
        ]

    def get_pulls_by_hour(
        self,
        account: str | None = None,
        folder: str | None = None,
        limit_hours: int = 24,
    ) -> list[tuple[str, int]]:
        """Get pull counts grouped by hour.

        Args:
            account: Optional account filter
            folder: Optional folder filter
            limit_hours: How many hours back to include

        Returns:
            List of (hour_str, count) tuples, e.g. [("2025-12-17 14:00", 50), ...]
        """
        # Calculate cutoff time
        cutoff = (datetime.now() - timedelta(hours=limit_hours)).isoformat()

        conditions = [f"pulled_at >= ?"]
        params: list = [cutoff]
        if account:
            conditions.append("account = ?")
            params.append(account)
        if folder:
            conditions.append("folder = ?")
            params.append(folder)

        where = f"WHERE {' AND '.join(conditions)}"

        cur = self.conn.execute(f"""
            SELECT
                strftime('%Y-%m-%d %H:00', pulled_at) as hour,
                COUNT(*) as count
            FROM pulled_messages
            {where}
            GROUP BY hour
            ORDER BY hour DESC
        """, params)

        return [(row["hour"], row["count"]) for row in cur]

    def get_activity_by_hour(
        self,
        account: str | None = None,
        folder: str | None = None,
        limit_hours: int = 24,
    ) -> list[tuple[str, int, int]]:
        """Get activity counts grouped by hour, split by new vs deduped.

        Args:
            account: Optional account filter
            folder: Optional folder filter
            limit_hours: How many hours back to include

        Returns:
            List of (hour_str, new_count, deduped_count) tuples
        """
        cutoff = (datetime.now() - timedelta(hours=limit_hours)).isoformat()

        conditions = ["pulled_at >= ?"]
        params: list = [cutoff]
        if account:
            conditions.append("account = ?")
            params.append(account)
        if folder:
            conditions.append("folder = ?")
            params.append(folder)

        where = f"WHERE {' AND '.join(conditions)}"

        cur = self.conn.execute(f"""
            SELECT
                strftime('%Y-%m-%d %H:00', pulled_at) as hour,
                SUM(CASE WHEN local_path IS NOT NULL THEN 1 ELSE 0 END) as new_count,
                SUM(CASE WHEN local_path IS NULL THEN 1 ELSE 0 END) as deduped_count
            FROM pulled_messages
            {where}
            GROUP BY hour
            ORDER BY hour DESC
        """, params)

        return [(row["hour"], row["new_count"], row["deduped_count"]) for row in cur]

    def get_pulls_by_day(
        self,
        account: str | None = None,
        folder: str | None = None,
        limit_days: int = 30,
    ) -> list[tuple[str, int]]:
        """Get pull counts grouped by day.

        Returns:
            List of (date_str, count) tuples, e.g. [("2025-12-17", 500), ...]
        """
        conditions = []
        params: list = []
        if account:
            conditions.append("account = ?")
            params.append(account)
        if folder:
            conditions.append("folder = ?")
            params.append(folder)

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        cur = self.conn.execute(f"""
            SELECT
                date(pulled_at) as day,
                COUNT(*) as count
            FROM pulled_messages
            {where}
            GROUP BY day
            ORDER BY day DESC
            LIMIT ?
        """, params + [limit_days])

        return [(row["day"], row["count"]) for row in cur]

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
