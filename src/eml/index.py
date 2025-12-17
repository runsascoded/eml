"""Persistent index for .eml files."""

import email
import sqlite3
import subprocess
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterator

from .layouts.path_template import content_hash


INDEX_DB = "index.db"


@dataclass
class IndexedFile:
    """An indexed .eml file."""
    id: int
    path: str  # relative to repo root
    content_hash: str
    message_id: str | None
    date: datetime | None
    from_addr: str
    to_addr: str
    subject: str
    size: int
    mtime: float
    indexed_at: datetime


class FileIndex:
    """Persistent SQLite index for .eml files.

    Provides O(1) lookups by message_id or content_hash instead of
    scanning all files on each operation.
    """

    def __init__(self, eml_dir: Path):
        """Initialize index.

        Args:
            eml_dir: Path to .eml directory (e.g., /path/to/project/.eml)
        """
        self._eml_dir = eml_dir
        self._root = eml_dir.parent
        self._db_path = eml_dir / INDEX_DB
        self._conn: sqlite3.Connection | None = None

    @property
    def db_path(self) -> Path:
        return self._db_path

    @property
    def root(self) -> Path:
        return self._root

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
            CREATE TABLE IF NOT EXISTS files (
                id INTEGER PRIMARY KEY,
                path TEXT UNIQUE NOT NULL,
                content_hash TEXT NOT NULL,
                message_id TEXT,
                date TEXT,
                from_addr TEXT,
                to_addr TEXT,
                subject TEXT,
                size INTEGER,
                mtime REAL,
                indexed_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_files_message_id ON files(message_id);
            CREATE INDEX IF NOT EXISTS idx_files_content_hash ON files(content_hash);
            CREATE INDEX IF NOT EXISTS idx_files_date ON files(date);
            CREATE INDEX IF NOT EXISTS idx_files_from ON files(from_addr);

            CREATE TABLE IF NOT EXISTS index_meta (
                key TEXT PRIMARY KEY,
                value TEXT
            );
        """)
        self.conn.commit()

    def get_meta(self, key: str) -> str | None:
        """Get metadata value."""
        cur = self.conn.execute(
            "SELECT value FROM index_meta WHERE key = ?", (key,)
        )
        row = cur.fetchone()
        return row["value"] if row else None

    def set_meta(self, key: str, value: str) -> None:
        """Set metadata value."""
        self.conn.execute(
            "INSERT OR REPLACE INTO index_meta (key, value) VALUES (?, ?)",
            (key, value)
        )
        self.conn.commit()

    def get_git_head(self) -> str | None:
        """Get current git HEAD sha."""
        try:
            result = subprocess.run(
                ["git", "-C", str(self._root), "rev-parse", "HEAD"],
                capture_output=True,
                text=True,
                check=True,
            )
            return result.stdout.strip()
        except (subprocess.CalledProcessError, FileNotFoundError):
            return None

    def get_indexed_sha(self) -> str | None:
        """Get git sha the index was built at."""
        return self.get_meta("git_sha")

    def is_stale(self) -> bool:
        """Check if index may be stale (HEAD changed since indexing)."""
        indexed_sha = self.get_indexed_sha()
        if not indexed_sha:
            return True
        head_sha = self.get_git_head()
        return indexed_sha != head_sha

    def file_count(self) -> int:
        """Get number of indexed files."""
        cur = self.conn.execute("SELECT COUNT(*) FROM files")
        return cur.fetchone()[0]

    def has_message_id(self, message_id: str) -> bool:
        """Check if a message_id exists in the index."""
        cur = self.conn.execute(
            "SELECT 1 FROM files WHERE message_id = ?", (message_id,)
        )
        return cur.fetchone() is not None

    def has_content_hash(self, sha: str) -> bool:
        """Check if a content hash exists in the index."""
        cur = self.conn.execute(
            "SELECT 1 FROM files WHERE content_hash = ?", (sha,)
        )
        return cur.fetchone() is not None

    def get_by_message_id(self, message_id: str) -> IndexedFile | None:
        """Get file by message_id."""
        cur = self.conn.execute(
            "SELECT * FROM files WHERE message_id = ?", (message_id,)
        )
        row = cur.fetchone()
        return self._row_to_file(row) if row else None

    def get_by_content_hash(self, sha: str) -> IndexedFile | None:
        """Get file by content hash."""
        cur = self.conn.execute(
            "SELECT * FROM files WHERE content_hash = ?", (sha,)
        )
        row = cur.fetchone()
        return self._row_to_file(row) if row else None

    def get_by_path(self, path: str) -> IndexedFile | None:
        """Get file by path."""
        cur = self.conn.execute(
            "SELECT * FROM files WHERE path = ?", (path,)
        )
        row = cur.fetchone()
        return self._row_to_file(row) if row else None

    def all_message_ids(self) -> set[str]:
        """Get all message_ids in the index."""
        cur = self.conn.execute(
            "SELECT message_id FROM files WHERE message_id IS NOT NULL"
        )
        return {row["message_id"] for row in cur}

    def all_content_hashes(self) -> set[str]:
        """Get all content hashes in the index."""
        cur = self.conn.execute("SELECT content_hash FROM files")
        return {row["content_hash"] for row in cur}

    def iter_files(
        self,
        folder: str | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> Iterator[IndexedFile]:
        """Iterate over indexed files with optional filters."""
        query = "SELECT * FROM files WHERE 1=1"
        params: list = []

        if folder:
            # Match files in folder (path starts with folder/)
            query += " AND path LIKE ?"
            params.append(f"{folder}/%")

        if start_date:
            query += " AND date >= ?"
            params.append(start_date.isoformat())

        if end_date:
            query += " AND date <= ?"
            params.append(end_date.isoformat())

        query += " ORDER BY date DESC"

        cur = self.conn.execute(query, params)
        for row in cur:
            yield self._row_to_file(row)

    def add_file(
        self,
        path: str,
        content_hash: str,
        message_id: str | None,
        date: datetime | None,
        from_addr: str,
        to_addr: str,
        subject: str,
        size: int,
        mtime: float,
    ) -> int:
        """Add or update a file in the index. Returns row id."""
        date_str = date.isoformat() if date else None
        cur = self.conn.execute(
            """INSERT INTO files (path, content_hash, message_id, date, from_addr, to_addr, subject, size, mtime)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(path) DO UPDATE SET
                   content_hash = excluded.content_hash,
                   message_id = excluded.message_id,
                   date = excluded.date,
                   from_addr = excluded.from_addr,
                   to_addr = excluded.to_addr,
                   subject = excluded.subject,
                   size = excluded.size,
                   mtime = excluded.mtime,
                   indexed_at = CURRENT_TIMESTAMP""",
            (path, content_hash, message_id, date_str, from_addr, to_addr, subject, size, mtime)
        )
        return cur.lastrowid or 0

    def remove_file(self, path: str) -> bool:
        """Remove a file from the index. Returns True if it existed."""
        cur = self.conn.execute("DELETE FROM files WHERE path = ?", (path,))
        return cur.rowcount > 0

    def clear(self) -> None:
        """Clear the entire index."""
        self.conn.execute("DELETE FROM files")
        self.conn.execute("DELETE FROM index_meta")
        self.conn.commit()

    def rebuild(self, progress_callback=None) -> tuple[int, int, int]:
        """Rebuild entire index from scratch.

        Args:
            progress_callback: Optional callback(current, total) for progress

        Returns:
            (indexed_count, skipped_count, error_count)
        """
        # Clear existing
        self.clear()

        # Find all .eml files
        eml_files = []
        for eml_path in self._root.rglob("*.eml"):
            # Skip .eml directory itself
            if ".eml" in eml_path.parts[:-1]:
                continue
            eml_files.append(eml_path)

        total = len(eml_files)
        indexed = 0
        skipped = 0
        errors = 0

        for i, eml_path in enumerate(eml_files):
            if progress_callback:
                progress_callback(i, total)

            try:
                result = self._index_file(eml_path)
                if result:
                    indexed += 1
                else:
                    skipped += 1
            except Exception:
                errors += 1

        self.conn.commit()

        # Save metadata
        git_sha = self.get_git_head()
        if git_sha:
            self.set_meta("git_sha", git_sha)
        self.set_meta("indexed_at", datetime.now().isoformat())
        self.set_meta("file_count", str(indexed))

        return indexed, skipped, errors

    def update(self, progress_callback=None) -> tuple[int, int, int]:
        """Incrementally update index based on git changes.

        Returns:
            (added, modified, deleted)
        """
        indexed_sha = self.get_indexed_sha()

        if not indexed_sha:
            # No previous index, do full rebuild
            indexed, _, _ = self.rebuild(progress_callback)
            return indexed, 0, 0

        # Get changed files since last index
        try:
            result = subprocess.run(
                ["git", "-C", str(self._root), "diff", "--name-status",
                 indexed_sha + "..HEAD", "--", "*.eml"],
                capture_output=True,
                text=True,
                check=True,
            )
        except subprocess.CalledProcessError:
            # Git error, fall back to full rebuild
            indexed, _, _ = self.rebuild(progress_callback)
            return indexed, 0, 0

        added = 0
        modified = 0
        deleted = 0

        for line in result.stdout.strip().split("\n"):
            if not line:
                continue

            parts = line.split("\t", 1)
            if len(parts) != 2:
                continue

            status, path = parts
            full_path = self._root / path

            if status == "D":
                # Deleted
                if self.remove_file(path):
                    deleted += 1
            elif status == "A":
                # Added
                if full_path.exists():
                    try:
                        if self._index_file(full_path):
                            added += 1
                    except Exception:
                        pass
            elif status in ("M", "R"):
                # Modified or renamed
                if full_path.exists():
                    try:
                        if self._index_file(full_path):
                            modified += 1
                    except Exception:
                        pass

        # Also check for untracked files
        try:
            result = subprocess.run(
                ["git", "-C", str(self._root), "ls-files", "--others",
                 "--exclude-standard", "*.eml"],
                capture_output=True,
                text=True,
                check=True,
            )
            for path in result.stdout.strip().split("\n"):
                if not path:
                    continue
                full_path = self._root / path
                if full_path.exists() and not self.get_by_path(path):
                    try:
                        if self._index_file(full_path):
                            added += 1
                    except Exception:
                        pass
        except subprocess.CalledProcessError:
            pass

        self.conn.commit()

        # Update metadata
        git_sha = self.get_git_head()
        if git_sha:
            self.set_meta("git_sha", git_sha)
        self.set_meta("indexed_at", datetime.now().isoformat())
        self.set_meta("file_count", str(self.file_count()))

        return added, modified, deleted

    def _index_file(self, path: Path) -> bool:
        """Index a single .eml file. Returns True if indexed."""
        try:
            stat = path.stat()
            raw = path.read_bytes()
            msg = email.message_from_bytes(raw)
        except Exception:
            return False

        rel_path = str(path.relative_to(self._root))
        sha = content_hash(raw)
        message_id = msg.get("Message-ID", "").strip() or None

        # Parse date
        date = None
        date_str = msg.get("Date", "")
        if date_str:
            try:
                import email.utils
                date = email.utils.parsedate_to_datetime(date_str)
            except Exception:
                pass

        self.add_file(
            path=rel_path,
            content_hash=sha,
            message_id=message_id,
            date=date,
            from_addr=msg.get("From", ""),
            to_addr=msg.get("To", ""),
            subject=msg.get("Subject", ""),
            size=stat.st_size,
            mtime=stat.st_mtime,
        )
        return True

    def _row_to_file(self, row: sqlite3.Row) -> IndexedFile:
        """Convert database row to IndexedFile."""
        date = None
        if row["date"]:
            try:
                date = datetime.fromisoformat(row["date"])
            except ValueError:
                pass

        indexed_at = datetime.now()
        if row["indexed_at"]:
            try:
                indexed_at = datetime.fromisoformat(row["indexed_at"])
            except ValueError:
                pass

        return IndexedFile(
            id=row["id"],
            path=row["path"],
            content_hash=row["content_hash"],
            message_id=row["message_id"],
            date=date,
            from_addr=row["from_addr"] or "",
            to_addr=row["to_addr"] or "",
            subject=row["subject"] or "",
            size=row["size"] or 0,
            mtime=row["mtime"] or 0.0,
            indexed_at=indexed_at,
        )

    def stats(self) -> dict:
        """Get index statistics."""
        cur = self.conn.execute("""
            SELECT
                COUNT(*) as total_files,
                COUNT(message_id) as with_message_id,
                COUNT(*) - COUNT(message_id) as without_message_id,
                SUM(size) as total_size,
                MIN(date) as oldest_date,
                MAX(date) as newest_date
            FROM files
        """)
        row = cur.fetchone()

        # Folder breakdown
        cur = self.conn.execute("""
            SELECT
                SUBSTR(path, 1, INSTR(path || '/', '/') - 1) as folder,
                COUNT(*) as count
            FROM files
            GROUP BY folder
            ORDER BY count DESC
            LIMIT 20
        """)
        folders = {r["folder"]: r["count"] for r in cur}

        return {
            "total_files": row["total_files"],
            "with_message_id": row["with_message_id"],
            "without_message_id": row["without_message_id"],
            "total_size": row["total_size"],
            "oldest_date": row["oldest_date"],
            "newest_date": row["newest_date"],
            "indexed_at": self.get_meta("indexed_at"),
            "git_sha": self.get_meta("git_sha"),
            "folders": folders,
        }
