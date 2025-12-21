"""Persistent tracking of pulled messages by UID.

This module provides robust per-UID tracking for email pulls, replacing the
fragile `last_uid` approach that lost track of gaps from failed fetches.

Database architecture (post-migration):
- uids.db: Critical UID tracking (Git-tracked, ~40MB for 65k messages)
- pulls.db: Legacy database with metadata+FTS (being phased out)
- index.db: File-based index (regenerable from .eml files)

For new installations, use `eml split-db` to migrate from pulls.db to uids.db.
"""

import base64
import hashlib
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from .uids import UidsDB, UIDS_DB

PULLS_DB = "pulls.db"


def compute_thread_slug(thread_id: str) -> str:
    """Compute an 8-char base64url slug from a thread_id.

    Uses first 6 bytes (48 bits) of SHA256 hash, encoded as base64url.
    This is the "base" slug - collision handling happens at insert time.

    Args:
        thread_id: The canonical thread_id (Message-ID of thread root)

    Returns:
        8-character base64url string (no padding)
    """
    hash_bytes = hashlib.sha256(thread_id.encode()).digest()[:6]
    return base64.urlsafe_b64encode(hash_bytes).decode().rstrip('=')


def compute_thread_id(
    message_id: str | None,
    references: str | None,
    in_reply_to: str | None,
) -> str | None:
    """Compute canonical thread_id from message headers.

    Thread ID is the first message-id in the references chain (the thread root),
    or the message's own ID if it has no references (it IS the root).

    Args:
        message_id: This message's Message-ID header
        references: References header (space-separated message-ids)
        in_reply_to: In-Reply-To header

    Returns:
        The thread root message-id, or None if no message_id available
    """
    # Extract first message-id from references (the thread root)
    if references:
        matches = re.findall(r'<[^>]+>', references)
        if matches:
            return matches[0]

    # Fall back to in_reply_to (makes this message part of parent's thread)
    if in_reply_to:
        return in_reply_to

    # No threading info - this message IS the thread root
    return message_id


@dataclass
class SyncRun:
    """Record of a sync operation (pull or push command)."""
    id: int
    operation: str  # 'pull' or 'push'
    account: str
    folder: str
    started_at: datetime
    ended_at: datetime | None
    status: str  # 'running', 'completed', 'aborted', 'failed'
    total: int  # total messages to process
    fetched: int  # new messages fetched
    skipped: int  # duplicates skipped
    failed: int  # failures
    error_message: str | None  # if aborted/failed, why


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
    status: str | None = None  # 'new', 'skipped', 'failed'
    sync_run_id: int | None = None  # FK to sync_runs
    subject: str | None = None  # Email subject
    msg_date: str | None = None  # Original message date
    error_message: str | None = None  # error message for failed pulls
    # Threading fields
    in_reply_to: str | None = None  # In-Reply-To header
    references: str | None = None  # References header (space-separated message-ids)
    thread_id: str | None = None  # Canonical thread ID (first in references chain)
    thread_slug: str | None = None  # 8-char base64url slug for URLs
    # Search fields
    from_addr: str | None = None  # From header
    to_addr: str | None = None  # To header


@dataclass
class RecentPull:
    """A recently pulled message with display info."""
    uid: int
    folder: str
    local_path: str
    pulled_at: datetime
    subject: str | None = None
    msg_date: str | None = None
    status: str | None = None


class PullsDB:
    """Persistent SQLite database tracking pulled messages by UID.

    This class manages both legacy pulls.db and the new split database
    architecture (uids.db + pulls.db for metadata).

    When uids.db exists, UID operations are delegated to UidsDB.
    When only pulls.db exists, it operates in legacy mode.
    """

    def __init__(self, eml_dir: Path):
        """Initialize PullsDB.

        Args:
            eml_dir: Path to .eml directory (e.g., /path/to/project/.eml)
        """
        self._eml_dir = eml_dir
        self._db_path = eml_dir / PULLS_DB
        self._uids_db_path = eml_dir / UIDS_DB
        self._conn: sqlite3.Connection | None = None
        self._uids_db: UidsDB | None = None

    @property
    def db_path(self) -> Path:
        return self._db_path

    @property
    def has_uids_db(self) -> bool:
        """Check if split database architecture is in use."""
        return self._uids_db_path.exists()

    def connect(self) -> None:
        """Open database connection and create schema if needed."""
        self._db_path.parent.mkdir(parents=True, exist_ok=True)

        # Use uids.db for UID operations if it exists
        if self._uids_db_path.exists():
            self._uids_db = UidsDB(self._eml_dir)
            self._uids_db.connect()

        # Still connect to pulls.db for metadata/FTS (if it exists)
        if self._db_path.exists():
            self._conn = sqlite3.connect(self._db_path, timeout=30.0)
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._create_schema()

    def disconnect(self) -> None:
        """Close database connection."""
        if self._uids_db:
            self._uids_db.disconnect()
            self._uids_db = None
        if self._conn:
            self._conn.close()
            self._conn = None

    @property
    def conn(self) -> sqlite3.Connection:
        if not self._conn:
            raise RuntimeError("Not connected to pulls.db")
        return self._conn

    @property
    def uids_db(self) -> UidsDB | None:
        """Get UidsDB instance if split architecture is in use."""
        return self._uids_db

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.disconnect()

    def _create_schema(self) -> None:
        """Create database schema."""
        # First create all tables (CREATE TABLE IF NOT EXISTS is idempotent)
        self.conn.executescript("""
            PRAGMA foreign_keys = OFF;
            -- Sync runs: first-class record of each pull/push operation
            CREATE TABLE IF NOT EXISTS sync_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                operation TEXT NOT NULL,  -- 'pull' or 'push'
                account TEXT NOT NULL,
                folder TEXT NOT NULL,
                started_at TEXT NOT NULL,
                ended_at TEXT,
                status TEXT NOT NULL DEFAULT 'running',  -- 'running', 'completed', 'aborted', 'failed'
                total INTEGER DEFAULT 0,
                fetched INTEGER DEFAULT 0,
                skipped INTEGER DEFAULT 0,
                failed INTEGER DEFAULT 0,
                error_message TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_sync_runs_started
                ON sync_runs(started_at DESC);

            CREATE INDEX IF NOT EXISTS idx_sync_runs_account_folder
                ON sync_runs(account, folder);

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
                status TEXT,  -- 'new', 'skipped', 'failed'
                sync_run_id INTEGER,  -- FK to sync_runs
                error_message TEXT,  -- error message for failed pulls
                -- Threading fields
                in_reply_to TEXT,  -- In-Reply-To header (message-id)
                references_ TEXT,  -- References header (space-separated message-ids)
                -- Search fields
                from_addr TEXT,  -- From header
                to_addr TEXT,  -- To header
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

            -- Index for threading by In-Reply-To (find replies to a message)
            CREATE INDEX IF NOT EXISTS idx_pulled_in_reply_to
                ON pulled_messages(in_reply_to);

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

        # Create FTS5 virtual table for full-text search
        # Using regular FTS5 (not external content) for simplicity and reliability.
        # This duplicates searchable text but avoids sync complexity and corruption issues.
        self._ensure_fts_table()

        # Migrations: add columns to existing tables (for databases created before these columns existed)
        # These run AFTER CREATE TABLE to avoid errors on fresh databases
        def _add_column_if_missing(table: str, column: str, col_type: str = "TEXT") -> None:
            try:
                self.conn.execute(f"SELECT {column} FROM {table} LIMIT 1")
            except sqlite3.OperationalError:
                self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")

        # Migrate pulled_messages table
        _add_column_if_missing("pulled_messages", "subject")
        _add_column_if_missing("pulled_messages", "msg_date")
        _add_column_if_missing("pulled_messages", "status")
        _add_column_if_missing("pulled_messages", "sync_run_id", "INTEGER")
        _add_column_if_missing("pulled_messages", "error_message")
        # Threading columns
        _add_column_if_missing("pulled_messages", "in_reply_to")
        _add_column_if_missing("pulled_messages", "references_")
        _add_column_if_missing("pulled_messages", "thread_id")
        _add_column_if_missing("pulled_messages", "thread_slug")
        # Search columns
        _add_column_if_missing("pulled_messages", "from_addr")
        _add_column_if_missing("pulled_messages", "to_addr")

        # Add thread_id index
        try:
            self.conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_pulled_thread_id
                ON pulled_messages(thread_id)
            """)
        except sqlite3.OperationalError:
            pass

        # Add thread_slug index (unique per thread)
        try:
            self.conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_pulled_thread_slug
                ON pulled_messages(thread_slug)
            """)
        except sqlite3.OperationalError:
            pass

    def _ensure_fts_table(self) -> None:
        """Ensure FTS5 table exists and is the correct type (regular, not external content).

        Migrates from external content FTS to regular FTS if needed.
        """
        # Check if messages_fts exists and what type it is
        cur = self.conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='messages_fts'"
        )
        row = cur.fetchone()

        needs_recreate = False
        if row:
            sql = row[0] or ""
            # Need to recreate if: external content table OR missing message_id column
            if "content=" in sql or "content_rowid=" in sql:
                needs_recreate = True
            elif "message_id" not in sql:
                needs_recreate = True

        if needs_recreate:
            self.conn.execute("DROP TABLE IF EXISTS messages_fts")
            row = None

        if not row or needs_recreate:
            # Create regular FTS5 table with message_id for joining back to pulled_messages
            self.conn.execute("""
                CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(
                    message_id,
                    subject,
                    body_text,
                    from_addr,
                    to_addr
                )
            """)
            self.conn.commit()

    def insert_fts(
        self,
        message_id: str | None,
        subject: str | None,
        body_text: str | None,
        from_addr: str | None,
        to_addr: str | None,
    ) -> None:
        """Insert a message into the FTS index.

        Args:
            message_id: Message-ID header (for joining back to pulled_messages)
            subject: Email subject
            body_text: Plain text body
            from_addr: From header
            to_addr: To header
        """
        if not message_id:
            return  # Can't index without message_id for join
        self.conn.execute("""
            INSERT INTO messages_fts(message_id, subject, body_text, from_addr, to_addr)
            VALUES (?, ?, ?, ?, ?)
        """, (message_id, subject, body_text, from_addr, to_addr))

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
        status: str | None = None,
        sync_run_id: int | None = None,
        error_message: str | None = None,
        # Threading fields
        in_reply_to: str | None = None,
        references: str | None = None,
        # Search fields
        from_addr: str | None = None,
        to_addr: str | None = None,
        body_text: str | None = None,  # For FTS only (not stored in pulled_messages)
    ) -> None:
        """Record a pulled message (success or failure).

        Args:
            account: Account name (e.g., 'y' for Yahoo)
            folder: Folder name (e.g., 'Inbox')
            uidvalidity: IMAP UIDVALIDITY value
            uid: Message UID
            content_hash: SHA256 of raw message bytes (empty string for failures)
            message_id: Message-ID header (optional, for reference)
            local_path: Path where message was stored (optional, None if deduped or failed)
            pulled_at: When the message was pulled (defaults to now, use file mtime for backfill)
            subject: Email subject (for display)
            msg_date: Original message date (for display)
            status: 'new', 'skipped', or 'failed'
            sync_run_id: FK to sync_runs table
            error_message: Error message for failed pulls
            in_reply_to: In-Reply-To header (for threading)
            references: References header (space-separated message-ids, for threading)
            from_addr: From header (for search)
            to_addr: To header (for search)
            body_text: Plain text body (for FTS only, not stored in pulled_messages)
        """
        ts_dt = pulled_at or datetime.now()
        ts = ts_dt.isoformat()

        # Record to UidsDB if available (critical UID tracking)
        if self._uids_db and status != "failed":
            self._uids_db.record_pull(
                account=account,
                folder=folder,
                uidvalidity=uidvalidity,
                uid=uid,
                content_hash=content_hash,
                message_id=message_id,
                local_path=local_path,
                pulled_at=ts_dt,
            )

        # Also record to pulls.db for metadata/FTS if it exists
        if self._conn:
            thread_id = compute_thread_id(message_id, references, in_reply_to)
            thread_slug = self._get_or_create_thread_slug(thread_id) if thread_id else None
            self.conn.execute("""
                INSERT OR REPLACE INTO pulled_messages
                    (account, folder, uidvalidity, uid, content_hash, message_id, local_path, pulled_at,
                     subject, msg_date, status, sync_run_id, error_message,
                     in_reply_to, references_, thread_id, thread_slug, from_addr, to_addr)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                account, folder, uidvalidity, uid, content_hash,
                message_id, local_path, ts, subject, msg_date, status, sync_run_id, error_message,
                in_reply_to, references, thread_id, thread_slug, from_addr, to_addr
            ))

            # Incremental FTS indexing - add to search index immediately
            if status != "failed":
                self.insert_fts(message_id, subject, body_text, from_addr, to_addr)

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
        # Delegate to UidsDB if available
        if self._uids_db:
            return self._uids_db.get_pulled_uids(account, folder, uidvalidity)
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
        # Delegate to UidsDB if available
        if self._uids_db:
            return self._uids_db.get_pulled_count(account, folder, uidvalidity)
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
        # Delegate to UidsDB if available
        if self._uids_db:
            return self._uids_db.has_content_hash(content_hash)
        cur = self.conn.execute(
            "SELECT 1 FROM pulled_messages WHERE content_hash = ? LIMIT 1",
            (content_hash,)
        )
        return cur.fetchone() is not None

    def get_all_content_hashes(self) -> set[str]:
        """Get all content hashes we've ever pulled."""
        # Delegate to UidsDB if available
        if self._uids_db:
            return self._uids_db.get_all_content_hashes()
        cur = self.conn.execute("SELECT DISTINCT content_hash FROM pulled_messages")
        return {row["content_hash"] for row in cur}

    def get_stats(self, account: str | None = None) -> dict:
        """Get statistics about pulled messages.

        Args:
            account: Optional account filter

        Returns:
            Dict with counts per folder, total, etc.
        """
        # Delegate to UidsDB if available
        if self._uids_db:
            return self._uids_db.get_stats(account)
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
        # Delegate to UidsDB if available
        if self._uids_db:
            return self._uids_db.get_uidvalidity(account, folder)
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
        # Delegate to UidsDB if available
        if self._uids_db:
            self._uids_db.record_server_uids(account, folder, uidvalidity, uid_message_ids)
            return
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
        # Delegate to UidsDB if available
        if self._uids_db:
            self._uids_db.record_server_folder(account, folder, uidvalidity, message_count)
            return
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
        # Delegate to UidsDB if available
        if self._uids_db:
            return self._uids_db.get_folders_with_activity(account)
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
        # Delegate to UidsDB if available
        if self._uids_db:
            return self._uids_db.get_server_uids(account, folder, uidvalidity)
        cur = self.conn.execute("""
            SELECT uid FROM server_uids
            WHERE account = ? AND folder = ? AND uidvalidity = ?
        """, (account, folder, uidvalidity))
        return {row["uid"] for row in cur}

    def get_server_uid_count(self, account: str, folder: str) -> int:
        """Get count of UIDs tracked for server folder."""
        # Delegate to UidsDB if available
        if self._uids_db:
            return self._uids_db.get_server_uid_count(account, folder)
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
        # Delegate to UidsDB if available
        if self._uids_db:
            return self._uids_db.get_server_folder_info(account, folder)
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
        # Delegate to UidsDB if available
        if self._uids_db:
            return self._uids_db.get_unpulled_uids(account, folder, uidvalidity)
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
        # Delegate to UidsDB if available
        if self._uids_db:
            return self._uids_db.get_uids_without_message_id(account, folder, uidvalidity)
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
            SELECT uid, folder, local_path, pulled_at, subject, msg_date, status
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
                status=row["status"],
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
    ) -> list[tuple[str, int, int, int]]:
        """Get activity counts grouped by hour, split by new vs deduped vs failed.

        Args:
            account: Optional account filter
            folder: Optional folder filter
            limit_hours: How many hours back to include

        Returns:
            List of (hour_str, new_count, deduped_count, failed_count) tuples
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
                SUM(CASE WHEN status IS NULL OR status = 'new' THEN 1 ELSE 0 END) as new_count,
                SUM(CASE WHEN status = 'skipped' THEN 1 ELSE 0 END) as deduped_count,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_count
            FROM pulled_messages
            {where}
            GROUP BY hour
            ORDER BY hour DESC
        """, params)

        return [(row["hour"], row["new_count"], row["deduped_count"], row["failed_count"]) for row in cur]

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
        # Delegate to UidsDB if available
        if self._uids_db:
            return self._uids_db.clear_folder(account, folder, uidvalidity)
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

    # -------------------------------------------------------------------------
    # Sync runs - first-class tracking of pull/push operations
    # -------------------------------------------------------------------------

    def start_sync_run(
        self,
        operation: str,
        account: str,
        folder: str,
        total: int = 0,
    ) -> int:
        """Start a new sync run and return its ID.

        Args:
            operation: 'pull' or 'push'
            account: Account name
            folder: Folder name
            total: Total messages to process

        Returns:
            Sync run ID
        """
        now = datetime.now().isoformat()
        cur = self.conn.execute("""
            INSERT INTO sync_runs (operation, account, folder, started_at, status, total)
            VALUES (?, ?, ?, ?, 'running', ?)
        """, (operation, account, folder, now, total))
        self.conn.commit()
        return cur.lastrowid

    def update_sync_run(
        self,
        sync_run_id: int,
        total: int | None = None,
        fetched: int | None = None,
        skipped: int | None = None,
        failed: int | None = None,
    ) -> None:
        """Update sync run progress.

        Args:
            sync_run_id: Sync run ID
            total: Total messages (if updated)
            fetched: New messages fetched
            skipped: Duplicates skipped
            failed: Failures
        """
        updates = []
        params = []
        if total is not None:
            updates.append("total = ?")
            params.append(total)
        if fetched is not None:
            updates.append("fetched = ?")
            params.append(fetched)
        if skipped is not None:
            updates.append("skipped = ?")
            params.append(skipped)
        if failed is not None:
            updates.append("failed = ?")
            params.append(failed)

        if updates:
            params.append(sync_run_id)
            self.conn.execute(f"""
                UPDATE sync_runs SET {', '.join(updates)} WHERE id = ?
            """, params)
            self.conn.commit()

    def end_sync_run(
        self,
        sync_run_id: int,
        status: str,
        error_message: str | None = None,
    ) -> None:
        """End a sync run.

        Args:
            sync_run_id: Sync run ID
            status: Final status ('completed', 'aborted', 'failed')
            error_message: Error message if aborted/failed
        """
        now = datetime.now().isoformat()
        self.conn.execute("""
            UPDATE sync_runs
            SET ended_at = ?, status = ?, error_message = ?
            WHERE id = ?
        """, (now, status, error_message, sync_run_id))
        self.conn.commit()

    def get_sync_run(self, sync_run_id: int) -> SyncRun | None:
        """Get a sync run by ID."""
        cur = self.conn.execute("""
            SELECT id, operation, account, folder, started_at, ended_at,
                   status, total, fetched, skipped, failed, error_message
            FROM sync_runs WHERE id = ?
        """, (sync_run_id,))
        row = cur.fetchone()
        if not row:
            return None
        return SyncRun(
            id=row["id"],
            operation=row["operation"],
            account=row["account"],
            folder=row["folder"],
            started_at=datetime.fromisoformat(row["started_at"]),
            ended_at=datetime.fromisoformat(row["ended_at"]) if row["ended_at"] else None,
            status=row["status"],
            total=row["total"] or 0,
            fetched=row["fetched"] or 0,
            skipped=row["skipped"] or 0,
            failed=row["failed"] or 0,
            error_message=row["error_message"],
        )

    def get_recent_sync_runs(
        self,
        limit: int = 20,
        offset: int = 0,
        account: str | None = None,
        folder: str | None = None,
        operation: str | None = None,
    ) -> list[SyncRun]:
        """Get recent sync runs.

        Args:
            limit: Max number to return
            offset: Number to skip (for pagination)
            account: Optional account filter
            folder: Optional folder filter
            operation: Optional operation filter ('pull' or 'push')

        Returns:
            List of SyncRun objects, most recent first
        """
        conditions = []
        params: list = []
        if account:
            conditions.append("account = ?")
            params.append(account)
        if folder:
            conditions.append("folder = ?")
            params.append(folder)
        if operation:
            conditions.append("operation = ?")
            params.append(operation)

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        params.extend([limit, offset])

        cur = self.conn.execute(f"""
            SELECT id, operation, account, folder, started_at, ended_at,
                   status, total, fetched, skipped, failed, error_message
            FROM sync_runs
            {where}
            ORDER BY started_at DESC
            LIMIT ? OFFSET ?
        """, params)

        return [
            SyncRun(
                id=row["id"],
                operation=row["operation"],
                account=row["account"],
                folder=row["folder"],
                started_at=datetime.fromisoformat(row["started_at"]),
                ended_at=datetime.fromisoformat(row["ended_at"]) if row["ended_at"] else None,
                status=row["status"],
                total=row["total"] or 0,
                fetched=row["fetched"] or 0,
                skipped=row["skipped"] or 0,
                failed=row["failed"] or 0,
                error_message=row["error_message"],
            )
            for row in cur
        ]

    def count_sync_runs(
        self,
        account: str | None = None,
        folder: str | None = None,
        operation: str | None = None,
    ) -> int:
        """Count total sync runs matching filters.

        Args:
            account: Optional account filter
            folder: Optional folder filter
            operation: Optional operation filter ('pull' or 'push')

        Returns:
            Total count of matching sync runs
        """
        conditions = []
        params: list = []
        if account:
            conditions.append("account = ?")
            params.append(account)
        if folder:
            conditions.append("folder = ?")
            params.append(folder)
        if operation:
            conditions.append("operation = ?")
            params.append(operation)

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        cur = self.conn.execute(f"SELECT COUNT(*) FROM sync_runs {where}", params)
        return cur.fetchone()[0]

    def cleanup_stale_runs(self, max_age_minutes: int = 60) -> int:
        """Mark stale running sync runs as aborted.

        Args:
            max_age_minutes: Consider runs stale if started more than this many minutes ago

        Returns:
            Number of runs marked as aborted
        """
        from datetime import timedelta
        cutoff = (datetime.now() - timedelta(minutes=max_age_minutes)).isoformat()
        cur = self.conn.execute("""
            UPDATE sync_runs
            SET status = 'aborted', ended_at = datetime('now'), error_message = 'Marked as stale (no completion)'
            WHERE status = 'running' AND started_at < ?
        """, (cutoff,))
        self.conn.commit()
        return cur.rowcount

    def get_sync_run_messages(
        self,
        sync_run_id: int,
        status: str | None = None,
        limit: int = 100,
    ) -> list[PulledMessage]:
        """Get messages for a sync run.

        Args:
            sync_run_id: Sync run ID
            status: Optional status filter ('new', 'skipped', 'failed')
            limit: Max number to return

        Returns:
            List of PulledMessage objects
        """
        conditions = ["sync_run_id = ?"]
        params: list = [sync_run_id]
        if status:
            conditions.append("status = ?")
            params.append(status)

        where = f"WHERE {' AND '.join(conditions)}"
        params.append(limit)

        cur = self.conn.execute(f"""
            SELECT account, folder, uidvalidity, uid, content_hash, message_id,
                   local_path, pulled_at, status, sync_run_id, subject, msg_date, error_message
            FROM pulled_messages
            {where}
            ORDER BY pulled_at DESC
            LIMIT ?
        """, params)

        return [
            PulledMessage(
                account=row["account"],
                folder=row["folder"],
                uidvalidity=row["uidvalidity"],
                uid=row["uid"],
                content_hash=row["content_hash"],
                message_id=row["message_id"],
                local_path=row["local_path"],
                pulled_at=datetime.fromisoformat(row["pulled_at"]),
                status=row["status"],
                sync_run_id=row["sync_run_id"],
                error_message=row["error_message"],
            )
            for row in cur
        ]

    # -------------------------------------------------------------------------
    # Threading methods
    # -------------------------------------------------------------------------

    def _get_or_create_thread_slug(self, thread_id: str) -> str:
        """Get existing thread_slug for a thread_id, or create a new one.

        If another message with the same thread_id already has a slug, return that.
        Otherwise, compute a new slug with collision handling.

        Args:
            thread_id: The canonical thread_id (Message-ID of thread root)

        Returns:
            8-character base64url slug
        """
        # Check if any message with this thread_id already has a slug
        cur = self.conn.execute("""
            SELECT thread_slug FROM pulled_messages
            WHERE thread_id = ? AND thread_slug IS NOT NULL
            LIMIT 1
        """, (thread_id,))
        row = cur.fetchone()
        if row and row[0]:
            return row[0]

        # Compute base slug
        base_slug = compute_thread_slug(thread_id)

        # Check for collision with different thread_id
        cur = self.conn.execute("""
            SELECT thread_id FROM pulled_messages
            WHERE thread_slug = ? AND thread_id != ?
            LIMIT 1
        """, (base_slug, thread_id))

        if not cur.fetchone():
            # No collision
            return base_slug

        # Collision - increment until we find a free slug
        # Decode the slug back to bytes, increment as integer, re-encode
        slug_bytes = base64.urlsafe_b64decode(base_slug + '==')
        slug_int = int.from_bytes(slug_bytes, 'big')

        for _ in range(1000):  # Safety limit
            slug_int += 1
            new_bytes = slug_int.to_bytes(6, 'big')
            new_slug = base64.urlsafe_b64encode(new_bytes).decode().rstrip('=')

            cur = self.conn.execute("""
                SELECT 1 FROM pulled_messages
                WHERE thread_slug = ? AND thread_id != ?
                LIMIT 1
            """, (new_slug, thread_id))

            if not cur.fetchone():
                return new_slug

        # Fallback: use full hash (should never happen)
        return hashlib.sha256(thread_id.encode()).hexdigest()[:16]

    def get_thread(self, message_id: str, limit: int = 100) -> list[PulledMessage]:
        """Get all messages in a thread using thread_id index.

        First computes the thread_id for the given message, then fetches all
        messages with the same thread_id.

        Args:
            message_id: The Message-ID to find thread for
            limit: Max messages to return

        Returns:
            List of PulledMessage objects in the thread, ordered by msg_date
        """
        # First, get the thread_id for this message
        cur = self.conn.execute("""
            SELECT thread_id, in_reply_to, references_
            FROM pulled_messages
            WHERE message_id = ?
        """, (message_id,))
        row = cur.fetchone()

        if not row:
            return []

        thread_id = row[0]

        # If thread_id is not populated, compute it (for backwards compat with unbackfilled data)
        if not thread_id:
            thread_id = compute_thread_id(message_id, row[2], row[1])

        if not thread_id:
            # No thread info, return just this message
            cur = self.conn.execute("""
                SELECT account, folder, uidvalidity, uid, content_hash, message_id,
                       local_path, pulled_at, status, sync_run_id, subject, msg_date,
                       error_message, in_reply_to, references_, thread_id, thread_slug, from_addr, to_addr
                FROM pulled_messages
                WHERE message_id = ?
            """, (message_id,))
            row = cur.fetchone()
            return [self._row_to_pulled_message(row)] if row else []

        return self.get_thread_by_id(thread_id, limit)

    def get_thread_by_id(self, thread_id: str, limit: int = 100) -> list[PulledMessage]:
        """Get all messages in a thread by thread_id directly.

        Args:
            thread_id: The thread_id to look up
            limit: Max messages to return

        Returns:
            List of PulledMessage objects in the thread, ordered by msg_date
        """
        cur = self.conn.execute("""
            SELECT account, folder, uidvalidity, uid, content_hash, message_id,
                   local_path, pulled_at, status, sync_run_id, subject, msg_date,
                   error_message, in_reply_to, references_, thread_id, thread_slug, from_addr, to_addr
            FROM pulled_messages
            WHERE thread_id = ?
            ORDER BY msg_date
            LIMIT ?
        """, (thread_id, limit))

        return [self._row_to_pulled_message(row) for row in cur]

    def get_thread_by_slug(self, slug: str, limit: int = 100) -> list[PulledMessage]:
        """Get all messages in a thread by thread_slug.

        Args:
            slug: The 8-char base64url thread slug
            limit: Max messages to return

        Returns:
            List of PulledMessage objects in the thread, ordered by msg_date
        """
        cur = self.conn.execute("""
            SELECT account, folder, uidvalidity, uid, content_hash, message_id,
                   local_path, pulled_at, status, sync_run_id, subject, msg_date,
                   error_message, in_reply_to, references_, thread_id, thread_slug, from_addr, to_addr
            FROM pulled_messages
            WHERE thread_slug = ?
            ORDER BY msg_date
            LIMIT ?
        """, (slug, limit))

        return [self._row_to_pulled_message(row) for row in cur]

    def get_replies(self, message_id: str, limit: int = 100) -> list[PulledMessage]:
        """Get direct replies to a message.

        Args:
            message_id: The Message-ID to find replies to
            limit: Max messages to return

        Returns:
            List of PulledMessage objects that reply to this message
        """
        cur = self.conn.execute("""
            SELECT account, folder, uidvalidity, uid, content_hash, message_id,
                   local_path, pulled_at, status, sync_run_id, subject, msg_date,
                   error_message, in_reply_to, references_, thread_id, thread_slug, from_addr, to_addr
            FROM pulled_messages
            WHERE in_reply_to = ?
            ORDER BY msg_date
            LIMIT ?
        """, (message_id, limit))

        return [self._row_to_pulled_message(row) for row in cur]

    def _row_to_pulled_message(self, row: sqlite3.Row) -> PulledMessage:
        """Convert a database row to a PulledMessage object."""
        keys = row.keys()
        return PulledMessage(
            account=row["account"],
            folder=row["folder"],
            uidvalidity=row["uidvalidity"],
            uid=row["uid"],
            content_hash=row["content_hash"],
            message_id=row["message_id"],
            local_path=row["local_path"],
            pulled_at=datetime.fromisoformat(row["pulled_at"]),
            status=row["status"],
            sync_run_id=row["sync_run_id"],
            subject=row["subject"] if "subject" in keys else None,
            msg_date=row["msg_date"] if "msg_date" in keys else None,
            error_message=row["error_message"] if "error_message" in keys else None,
            in_reply_to=row["in_reply_to"] if "in_reply_to" in keys else None,
            references=row["references_"] if "references_" in keys else None,
            thread_id=row["thread_id"] if "thread_id" in keys else None,
            thread_slug=row["thread_slug"] if "thread_slug" in keys else None,
            from_addr=row["from_addr"] if "from_addr" in keys else None,
            to_addr=row["to_addr"] if "to_addr" in keys else None,
        )

    # -------------------------------------------------------------------------
    # Full-text search methods
    # -------------------------------------------------------------------------

    def search(
        self,
        query: str,
        limit: int = 50,
        offset: int = 0,
        account: str | None = None,
        folder: str | None = None,
    ) -> list[PulledMessage]:
        """Full-text search over email subject and body.

        Args:
            query: FTS5 search query (supports AND, OR, NOT, phrases, etc.)
            limit: Max results to return
            account: Optional account filter
            folder: Optional folder filter

        Returns:
            List of PulledMessage objects matching the query
        """
        # Build the query - join FTS results with pulled_messages via message_id
        conditions = ["messages_fts MATCH ?"]
        params: list = [query]

        if account:
            conditions.append("p.account = ?")
            params.append(account)
        if folder:
            conditions.append("p.folder = ?")
            params.append(folder)

        where = " AND ".join(conditions)
        params.extend([limit, offset])

        cur = self.conn.execute(f"""
            SELECT p.account, p.folder, p.uidvalidity, p.uid, p.content_hash, p.message_id,
                   p.local_path, p.pulled_at, p.status, p.sync_run_id,
                   COALESCE(messages_fts.subject, p.subject) as subject,
                   p.msg_date,
                   p.error_message, p.in_reply_to, p.references_,
                   COALESCE(messages_fts.from_addr, p.from_addr) as from_addr,
                   COALESCE(messages_fts.to_addr, p.to_addr) as to_addr,
                   bm25(messages_fts) as rank
            FROM messages_fts
            JOIN pulled_messages p ON messages_fts.message_id = p.message_id
            WHERE {where}
            ORDER BY p.msg_date DESC NULLS LAST
            LIMIT ? OFFSET ?
        """, params)

        return [self._row_to_pulled_message(row) for row in cur]

    def search_count(
        self,
        query: str,
        account: str | None = None,
        folder: str | None = None,
    ) -> int:
        """Get total count of search results (for pagination)."""
        conditions = ["messages_fts MATCH ?"]
        params: list = [query]

        if account:
            conditions.append("p.account = ?")
            params.append(account)
        if folder:
            conditions.append("p.folder = ?")
            params.append(folder)

        where = " AND ".join(conditions)

        cur = self.conn.execute(f"""
            SELECT COUNT(*) as cnt
            FROM messages_fts
            JOIN pulled_messages p ON messages_fts.message_id = p.message_id
            WHERE {where}
        """, params)
        row = cur.fetchone()
        return row["cnt"] if row else 0

    def rebuild_fts_index(self) -> int:
        """Rebuild the FTS5 index from pulled_messages (subject/from/to only).

        Note: body_text is not stored in pulled_messages, so this only rebuilds
        from subject/from_addr/to_addr. Use `eml index-fts` to rebuild with
        full body text by re-reading .eml files.

        Returns:
            Number of messages indexed
        """
        # Clear existing FTS data
        self.conn.execute("DELETE FROM messages_fts")

        # Re-insert all messages that have a message_id (required for join)
        # body_text will be NULL since we don't store it in pulled_messages
        cur = self.conn.execute("""
            INSERT INTO messages_fts(message_id, subject, body_text, from_addr, to_addr)
            SELECT message_id, subject, NULL, from_addr, to_addr
            FROM pulled_messages
            WHERE message_id IS NOT NULL
              AND subject IS NOT NULL
        """)
        count = cur.rowcount
        self.conn.commit()

        return count

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
