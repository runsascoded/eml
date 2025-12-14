"""SQLite-based storage layout."""

import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Iterator

from .base import StorageLayout, StoredMessage


class SqliteLayout:
    """Store emails as blobs in SQLite database.

    This is the V2 sqlite layout, storing in .eml/msgs.db.
    Schema is simplified from V1 (sync/push state now in YAML files).
    """

    def __init__(self, root: Path):
        self._root = root
        self._db_path = root / ".eml" / "msgs.db"
        self._conn: sqlite3.Connection | None = None

    @property
    def root(self) -> Path:
        return self._root

    def connect(self) -> None:
        """Open database connection."""
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
            raise RuntimeError("Not connected. Call connect() first.")
        return self._conn

    def _create_schema(self) -> None:
        """Create database schema."""
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY,
                message_id TEXT UNIQUE NOT NULL,
                folder TEXT NOT NULL,
                date TEXT,
                from_addr TEXT,
                to_addr TEXT,
                cc_addr TEXT,
                subject TEXT,
                raw BLOB NOT NULL,
                source_uid TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_messages_message_id ON messages(message_id);
            CREATE INDEX IF NOT EXISTS idx_messages_folder ON messages(folder);
            CREATE INDEX IF NOT EXISTS idx_messages_date ON messages(date);
        """)
        self.conn.commit()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.disconnect()

    def iter_messages(
        self,
        folder: str | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> Iterator[StoredMessage]:
        """Iterate over messages with optional filters."""
        query = "SELECT * FROM messages WHERE 1=1"
        params: list = []

        if folder:
            query += " AND folder = ?"
            params.append(folder)
        if start_date:
            query += " AND date >= ?"
            params.append(start_date.isoformat())
        if end_date:
            query += " AND date <= ?"
            params.append(end_date.isoformat())

        query += " ORDER BY date DESC"

        cur = self.conn.execute(query, params)
        for row in cur:
            yield self._row_to_message(row)

    def get_message(self, message_id: str) -> StoredMessage | None:
        """Get a message by Message-ID."""
        cur = self.conn.execute(
            "SELECT * FROM messages WHERE message_id = ?",
            (message_id,)
        )
        row = cur.fetchone()
        if not row:
            return None
        return self._row_to_message(row)

    def add_message(
        self,
        message_id: str,
        raw: bytes,
        folder: str,
        date: datetime | None = None,
        from_addr: str = "",
        to_addr: str = "",
        cc_addr: str = "",
        subject: str = "",
        source_uid: str | None = None,
    ) -> Path:
        """Add a message to storage. Returns db path."""
        date_str = date.isoformat() if date else None
        self.conn.execute(
            """INSERT OR IGNORE INTO messages
               (message_id, folder, date, from_addr, to_addr, cc_addr, subject, raw, source_uid)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (message_id, folder, date_str, from_addr, to_addr, cc_addr, subject, raw, source_uid)
        )
        self.conn.commit()
        return self._db_path

    def has_message(self, message_id: str) -> bool:
        """Check if a message exists by Message-ID."""
        cur = self.conn.execute(
            "SELECT 1 FROM messages WHERE message_id = ?",
            (message_id,)
        )
        return cur.fetchone() is not None

    def count(self, folder: str | None = None) -> int:
        """Count messages, optionally filtered by folder."""
        if folder:
            cur = self.conn.execute(
                "SELECT COUNT(*) FROM messages WHERE folder = ?",
                (folder,)
            )
        else:
            cur = self.conn.execute("SELECT COUNT(*) FROM messages")
        return cur.fetchone()[0]

    def _row_to_message(self, row: sqlite3.Row) -> StoredMessage:
        """Convert database row to StoredMessage."""
        date = None
        if row["date"]:
            try:
                date = datetime.fromisoformat(row["date"])
            except ValueError:
                pass
        return StoredMessage(
            message_id=row["message_id"],
            raw=row["raw"],
            folder=row["folder"],
            date=date,
            from_addr=row["from_addr"] or "",
            to_addr=row["to_addr"] or "",
            cc_addr=row["cc_addr"] or "",
            subject=row["subject"] or "",
            source_uid=row["source_uid"],
        )
