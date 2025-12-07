"""Local email storage using SQLite."""

import sqlite3
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Iterator

# Default paths
EML_DIR = ".eml"
MSGS_DB = "msgs.db"
ACCTS_DB = "accts.db"
GLOBAL_CONFIG_DIR = Path.home() / ".config" / "eml"


def find_eml_dir(start: Path | None = None) -> Path | None:
    """Find .eml directory, searching upward from start (or cwd)."""
    path = (start or Path.cwd()).resolve()
    while path != path.parent:
        eml_dir = path / EML_DIR
        if eml_dir.is_dir():
            return eml_dir
        path = path.parent
    return None


def get_eml_dir(require: bool = True) -> Path:
    """Get .eml directory, raising if not found and require=True."""
    eml_dir = find_eml_dir()
    if not eml_dir and require:
        raise FileNotFoundError(
            "Not in an eml project. Run 'eml init' first."
        )
    return eml_dir or (Path.cwd() / EML_DIR)


def get_msgs_db_path() -> Path:
    """Get path to messages database."""
    return get_eml_dir() / MSGS_DB


def get_accts_db_path(local_only: bool = False) -> Path | None:
    """Get path to accounts database (local, then global fallback)."""
    eml_dir = find_eml_dir()
    if eml_dir:
        local_accts = eml_dir / ACCTS_DB
        if local_accts.exists():
            return local_accts
    if local_only:
        return eml_dir / ACCTS_DB if eml_dir else None
    # Global fallback
    global_accts = GLOBAL_CONFIG_DIR / ACCTS_DB
    if global_accts.exists():
        return global_accts
    # Return local path for creation
    return eml_dir / ACCTS_DB if eml_dir else None


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
    tags: list[str] = field(default_factory=list)


@dataclass
class Account:
    """An IMAP account configuration."""
    name: str
    type: str  # "gmail", "zoho", or hostname
    user: str
    password: str
    created_at: datetime | None = None


class BaseStorage:
    """Base class for SQLite storage."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self._conn: sqlite3.Connection | None = None

    def connect(self) -> None:
        """Open database connection and create schema if needed."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.path, timeout=30.0)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")  # better concurrency
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
        """Create database schema. Override in subclasses."""
        raise NotImplementedError

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.disconnect()


class AccountStorage(BaseStorage):
    """SQLite storage for IMAP accounts."""

    def _create_schema(self) -> None:
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS accounts (
                name TEXT PRIMARY KEY,
                type TEXT NOT NULL,
                user TEXT NOT NULL,
                password TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
        """)
        self.conn.commit()

    def add(self, name: str, type: str, user: str, password: str) -> None:
        """Add or update an account."""
        self.conn.execute(
            """INSERT INTO accounts (name, type, user, password)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(name) DO UPDATE SET type = ?, user = ?, password = ?""",
            (name, type, user, password, type, user, password)
        )
        self.conn.commit()

    def get(self, name: str) -> Account | None:
        """Get an account by name."""
        cur = self.conn.execute(
            "SELECT * FROM accounts WHERE name = ?", (name,)
        )
        row = cur.fetchone()
        if not row:
            return None
        created_at = None
        if row["created_at"]:
            try:
                created_at = datetime.fromisoformat(row["created_at"])
            except ValueError:
                pass
        return Account(
            name=row["name"],
            type=row["type"],
            user=row["user"],
            password=row["password"],
            created_at=created_at,
        )

    def list(self) -> list[Account]:
        """List all accounts."""
        cur = self.conn.execute("SELECT * FROM accounts ORDER BY name")
        accounts = []
        for row in cur:
            created_at = None
            if row["created_at"]:
                try:
                    created_at = datetime.fromisoformat(row["created_at"])
                except ValueError:
                    pass
            accounts.append(Account(
                name=row["name"],
                type=row["type"],
                user=row["user"],
                password=row["password"],
                created_at=created_at,
            ))
        return accounts

    def remove(self, name: str) -> bool:
        """Remove an account. Returns True if it existed."""
        cur = self.conn.execute("DELETE FROM accounts WHERE name = ?", (name,))
        self.conn.commit()
        return cur.rowcount > 0


def get_account(name: str) -> Account | None:
    """Get account by name, checking local then global."""
    # Check local first
    eml_dir = find_eml_dir()
    if eml_dir:
        local_accts = eml_dir / ACCTS_DB
        if local_accts.exists():
            with AccountStorage(local_accts) as storage:
                acct = storage.get(name)
                if acct:
                    return acct
    # Check global
    global_accts = GLOBAL_CONFIG_DIR / ACCTS_DB
    if global_accts.exists():
        with AccountStorage(global_accts) as storage:
            return storage.get(name)
    return None


class MessageStorage(BaseStorage):
    """SQLite storage for email messages."""

    def _create_schema(self) -> None:
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

            CREATE TABLE IF NOT EXISTS message_tags (
                message_id TEXT NOT NULL,
                tag TEXT NOT NULL,
                PRIMARY KEY (message_id, tag)
            );

            CREATE INDEX IF NOT EXISTS idx_tags_tag ON message_tags(tag);

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
        tags: list[str] | None = None,
    ) -> int:
        """Add a message to storage. Returns row ID."""
        date_str = date.isoformat() if date else None
        cur = self.conn.execute(
            """INSERT INTO messages
               (message_id, date, from_addr, to_addr, cc_addr, subject, raw, source_folder, source_uid)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (message_id, date_str, from_addr, to_addr, cc_addr, subject, raw, source_folder, source_uid)
        )
        row_id = cur.lastrowid
        if tags:
            for tag in tags:
                self.conn.execute(
                    "INSERT OR IGNORE INTO message_tags (message_id, tag) VALUES (?, ?)",
                    (message_id, tag)
                )
        self.conn.commit()
        return row_id

    def add_tag(self, message_id: str, tag: str) -> None:
        """Add a tag to a message."""
        self.conn.execute(
            "INSERT OR IGNORE INTO message_tags (message_id, tag) VALUES (?, ?)",
            (message_id, tag)
        )
        self.conn.commit()

    def remove_tag(self, message_id: str, tag: str) -> None:
        """Remove a tag from a message."""
        self.conn.execute(
            "DELETE FROM message_tags WHERE message_id = ? AND tag = ?",
            (message_id, tag)
        )
        self.conn.commit()

    def get_tags(self, message_id: str) -> list[str]:
        """Get tags for a message."""
        cur = self.conn.execute(
            "SELECT tag FROM message_tags WHERE message_id = ? ORDER BY tag",
            (message_id,)
        )
        return [row["tag"] for row in cur]

    def list_tags(self) -> list[tuple[str, int]]:
        """List all tags with counts."""
        cur = self.conn.execute(
            "SELECT tag, COUNT(*) as count FROM message_tags GROUP BY tag ORDER BY tag"
        )
        return [(row["tag"], row["count"]) for row in cur]

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
        tag: str | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        from_addr: str | None = None,
        limit: int | None = None,
    ) -> Iterator[StoredMessage]:
        """Iterate over messages with optional filters."""
        if tag:
            query = """SELECT m.* FROM messages m
                       JOIN message_tags t ON m.message_id = t.message_id
                       WHERE t.tag = ?"""
            params: list = [tag]
        else:
            query = "SELECT * FROM messages WHERE 1=1"
            params = []

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

    def count(self, tag: str | None = None) -> int:
        """Count total messages, optionally filtered by tag."""
        if tag:
            cur = self.conn.execute(
                """SELECT COUNT(*) FROM messages m
                   JOIN message_tags t ON m.message_id = t.message_id
                   WHERE t.tag = ?""",
                (tag,)
            )
        else:
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
        tag: str | None = None,
    ) -> Iterator[StoredMessage]:
        """Iterate over messages not yet pushed to a destination."""
        if tag:
            query = """SELECT m.* FROM messages m
                       JOIN message_tags t ON m.message_id = t.message_id
                       WHERE t.tag = ?
                       AND NOT EXISTS (
                           SELECT 1 FROM push_state p
                           WHERE p.message_id = m.message_id
                           AND p.dest_type = ? AND p.dest_user = ? AND p.dest_folder = ?
                       )
                       ORDER BY m.date"""
            params = (tag, dest_type, dest_user, dest_folder)
        else:
            query = """SELECT m.* FROM messages m
                       WHERE NOT EXISTS (
                           SELECT 1 FROM push_state p
                           WHERE p.message_id = m.message_id
                           AND p.dest_type = ? AND p.dest_user = ? AND p.dest_folder = ?
                       )
                       ORDER BY m.date"""
            params = (dest_type, dest_user, dest_folder)
        cur = self.conn.execute(query, params)
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
        tags = self.get_tags(row["message_id"])
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
            tags=tags,
        )
