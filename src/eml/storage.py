"""Local email storage using SQLite."""

import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterator


@dataclass
class StoredMessage:
    """A message stored in local storage."""
    id: int
    message_id: str
    date: datetime | None
    from_addr: str
    to_addr: str
    cc_addr: str
    subject: str
    raw: bytes
    source_folder: str | None = None
    source_uid: str | None = None


class EmailStorage:
    """SQLite-based local email storage."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self._conn: sqlite3.Connection | None = None

    def connect(self) -> None:
        """Open database connection and create schema if needed."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.path)
        self._conn.row_factory = sqlite3.Row
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

    def _create_schema(self) -> None:
        """Create database schema."""
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY,
                message_id TEXT UNIQUE NOT NULL,
                date TEXT,
                from_addr TEXT,
                to_addr TEXT,
                cc_addr TEXT,
                subject TEXT,
                raw BLOB NOT NULL,
                source_folder TEXT,
                source_uid TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_messages_date ON messages(date);
            CREATE INDEX IF NOT EXISTS idx_messages_from ON messages(from_addr);
            CREATE INDEX IF NOT EXISTS idx_messages_source ON messages(source_folder, source_uid);

            CREATE TABLE IF NOT EXISTS sync_state (
                id INTEGER PRIMARY KEY,
                source_type TEXT NOT NULL,
                source_user TEXT NOT NULL,
                folder TEXT NOT NULL,
                uidvalidity INTEGER,
                last_uid INTEGER,
                last_sync TEXT,
                UNIQUE(source_type, source_user, folder)
            );

            CREATE TABLE IF NOT EXISTS push_state (
                id INTEGER PRIMARY KEY,
                message_id TEXT NOT NULL,
                dest_type TEXT NOT NULL,
                dest_user TEXT NOT NULL,
                dest_folder TEXT NOT NULL,
                pushed_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(message_id, dest_type, dest_user, dest_folder)
            );

            CREATE INDEX IF NOT EXISTS idx_push_state_dest ON push_state(dest_type, dest_user, dest_folder);
        """)
        self.conn.commit()

    def has_message(self, message_id: str) -> bool:
        """Check if a message exists by Message-ID."""
        cur = self.conn.execute(
            "SELECT 1 FROM messages WHERE message_id = ?",
            (message_id,)
        )
        return cur.fetchone() is not None

    def add_message(
        self,
        message_id: str,
        raw: bytes,
        date: datetime | None = None,
        from_addr: str = "",
        to_addr: str = "",
        cc_addr: str = "",
        subject: str = "",
        source_folder: str | None = None,
        source_uid: str | None = None,
    ) -> int:
        """Add a message to storage. Returns row ID."""
        date_str = date.isoformat() if date else None
        cur = self.conn.execute(
            """INSERT INTO messages
               (message_id, date, from_addr, to_addr, cc_addr, subject, raw, source_folder, source_uid)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (message_id, date_str, from_addr, to_addr, cc_addr, subject, raw, source_folder, source_uid)
        )
        self.conn.commit()
        return cur.lastrowid

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

    def iter_messages(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        from_addr: str | None = None,
        limit: int | None = None,
    ) -> Iterator[StoredMessage]:
        """Iterate over messages with optional filters."""
        query = "SELECT * FROM messages WHERE 1=1"
        params: list = []

        if start_date:
            query += " AND date >= ?"
            params.append(start_date.isoformat())
        if end_date:
            query += " AND date <= ?"
            params.append(end_date.isoformat())
        if from_addr:
            query += " AND from_addr LIKE ?"
            params.append(f"%{from_addr}%")

        query += " ORDER BY date DESC"

        if limit:
            query += " LIMIT ?"
            params.append(limit)

        cur = self.conn.execute(query, params)
        for row in cur:
            yield self._row_to_message(row)

    def count(self) -> int:
        """Count total messages."""
        cur = self.conn.execute("SELECT COUNT(*) FROM messages")
        return cur.fetchone()[0]

    def get_sync_state(
        self,
        source_type: str,
        source_user: str,
        folder: str,
    ) -> tuple[int | None, int | None]:
        """Get sync state for a source folder. Returns (uidvalidity, last_uid)."""
        cur = self.conn.execute(
            """SELECT uidvalidity, last_uid FROM sync_state
               WHERE source_type = ? AND source_user = ? AND folder = ?""",
            (source_type, source_user, folder)
        )
        row = cur.fetchone()
        if not row:
            return None, None
        return row["uidvalidity"], row["last_uid"]

    def set_sync_state(
        self,
        source_type: str,
        source_user: str,
        folder: str,
        uidvalidity: int,
        last_uid: int,
    ) -> None:
        """Update sync state for a source folder."""
        self.conn.execute(
            """INSERT INTO sync_state (source_type, source_user, folder, uidvalidity, last_uid, last_sync)
               VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
               ON CONFLICT(source_type, source_user, folder)
               DO UPDATE SET uidvalidity = ?, last_uid = ?, last_sync = CURRENT_TIMESTAMP""",
            (source_type, source_user, folder, uidvalidity, last_uid, uidvalidity, last_uid)
        )
        self.conn.commit()

    def clear_sync_state(
        self,
        source_type: str,
        source_user: str,
        folder: str,
    ) -> None:
        """Clear sync state (e.g., when UIDVALIDITY changes)."""
        self.conn.execute(
            """DELETE FROM sync_state
               WHERE source_type = ? AND source_user = ? AND folder = ?""",
            (source_type, source_user, folder)
        )
        self.conn.commit()

    def is_pushed(
        self,
        message_id: str,
        dest_type: str,
        dest_user: str,
        dest_folder: str,
    ) -> bool:
        """Check if a message has been pushed to a destination."""
        cur = self.conn.execute(
            """SELECT 1 FROM push_state
               WHERE message_id = ? AND dest_type = ? AND dest_user = ? AND dest_folder = ?""",
            (message_id, dest_type, dest_user, dest_folder)
        )
        return cur.fetchone() is not None

    def mark_pushed(
        self,
        message_id: str,
        dest_type: str,
        dest_user: str,
        dest_folder: str,
    ) -> None:
        """Mark a message as pushed to a destination."""
        self.conn.execute(
            """INSERT OR IGNORE INTO push_state (message_id, dest_type, dest_user, dest_folder)
               VALUES (?, ?, ?, ?)""",
            (message_id, dest_type, dest_user, dest_folder)
        )
        self.conn.commit()

    def count_pushed(
        self,
        dest_type: str,
        dest_user: str,
        dest_folder: str,
    ) -> int:
        """Count messages pushed to a destination."""
        cur = self.conn.execute(
            """SELECT COUNT(*) FROM push_state
               WHERE dest_type = ? AND dest_user = ? AND dest_folder = ?""",
            (dest_type, dest_user, dest_folder)
        )
        return cur.fetchone()[0]

    def iter_unpushed(
        self,
        dest_type: str,
        dest_user: str,
        dest_folder: str,
    ) -> Iterator[StoredMessage]:
        """Iterate over messages not yet pushed to a destination."""
        cur = self.conn.execute(
            """SELECT m.* FROM messages m
               WHERE NOT EXISTS (
                   SELECT 1 FROM push_state p
                   WHERE p.message_id = m.message_id
                   AND p.dest_type = ? AND p.dest_user = ? AND p.dest_folder = ?
               )
               ORDER BY m.date""",
            (dest_type, dest_user, dest_folder)
        )
        for row in cur:
            yield self._row_to_message(row)

    def _row_to_message(self, row: sqlite3.Row) -> StoredMessage:
        """Convert a database row to StoredMessage."""
        date = None
        if row["date"]:
            try:
                date = datetime.fromisoformat(row["date"])
            except ValueError:
                pass
        return StoredMessage(
            id=row["id"],
            message_id=row["message_id"],
            date=date,
            from_addr=row["from_addr"] or "",
            to_addr=row["to_addr"] or "",
            cc_addr=row["cc_addr"] or "",
            subject=row["subject"] or "",
            raw=row["raw"],
            source_folder=row["source_folder"],
            source_uid=row["source_uid"],
        )

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.disconnect()
