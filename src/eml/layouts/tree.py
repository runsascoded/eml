"""Tree-based storage layout using .eml files in directories."""

import email
import email.utils
import sqlite3
from datetime import datetime, timezone
from email.message import Message
from pathlib import Path
from typing import Iterator

from .base import StorageLayout, StoredMessage
from .path_template import PathTemplate, MessageVars, content_hash


class TreeLayout:
    """Store emails as .eml files in a directory tree.

    Uses path templates for flexible sharding:
    - "default": $folder/$yyyy/$mm/${sha8}_${subj}.eml
    - "flat": $folder/${sha8}_${subj}.eml
    - "daily": $folder/$yyyy/$mm/$dd/${sha8}_${subj}.eml
    - Or custom: "$folder/$yyyy/$mm/$dd/${hhmm}_${sha8}.eml"

    Legacy sharding schemes (tree:month, etc.) are supported via aliases.
    """

    def __init__(self, root: Path, template: str = "default"):
        """Initialize tree layout.

        Args:
            root: Root directory for storage
            template: Template string or preset name (default, flat, daily, etc.)
        """
        self._root = root
        self._template = PathTemplate(template)
        self._index: dict[str, Path] | None = None  # message_id -> path
        self._hash_index: dict[str, Path] | None = None  # content_hash -> path

    @property
    def root(self) -> Path:
        return self._root

    @property
    def template(self) -> PathTemplate:
        return self._template

    def _message_path(
        self,
        folder: str,
        raw: bytes,
        date: datetime | None = None,
        subject: str = "",
        from_addr: str = "",
        uid: int | None = None,
    ) -> Path:
        """Get full path for a message using the template."""
        path_str = self._template.render_message(
            folder=folder,
            raw=raw,
            date=date,
            subject=subject,
            from_addr=from_addr,
            uid=uid,
        )
        return self._root / path_str

    def _load_index_from_db(self) -> tuple[dict[str, Path], dict[str, Path]] | None:
        """Try to load indices from persistent index.db (created by `eml index`).

        Returns (message_id_index, content_hash_index) if index.db exists and is valid,
        or None if we need to fall back to scanning.
        """
        index_path = self._root / ".eml" / "index.db"
        if not index_path.exists():
            return None

        try:
            conn = sqlite3.connect(index_path)
            cur = conn.cursor()

            mid_index: dict[str, Path] = {}
            hash_index: dict[str, Path] = {}

            # Load message_id -> path mappings
            cur.execute("SELECT path, message_id, content_hash FROM files WHERE message_id IS NOT NULL OR content_hash IS NOT NULL")
            for row in cur.fetchall():
                path_str, message_id, content_hash = row
                path = self._root / path_str
                if message_id:
                    mid_index[message_id] = path
                if content_hash:
                    hash_index[content_hash] = path

            conn.close()
            return mid_index, hash_index
        except Exception:
            return None

    def _build_index(self) -> tuple[dict[str, Path], dict[str, Path]]:
        """Build indices by scanning .eml files.

        Returns:
            (message_id_index, content_hash_index)
        """
        mid_index: dict[str, Path] = {}
        hash_index: dict[str, Path] = {}

        for eml_path in self._root.rglob("*.eml"):
            # Skip .eml directory
            if ".eml" in eml_path.parts[:-1]:
                continue
            try:
                raw = eml_path.read_bytes()
                msg = email.message_from_bytes(raw)

                # Index by message-id if present
                message_id = msg.get("Message-ID", "").strip()
                if message_id:
                    mid_index[message_id] = eml_path

                # Always index by content hash
                sha = content_hash(raw)
                hash_index[sha] = eml_path
            except Exception:
                pass

        return mid_index, hash_index

    def _get_indices(self) -> tuple[dict[str, Path], dict[str, Path]]:
        """Get or build the message indices.

        Tries to load from persistent index.db first (O(1) lookup),
        falls back to scanning all files if not available.
        """
        if self._index is None or self._hash_index is None:
            # Try persistent index first
            result = self._load_index_from_db()
            if result:
                self._index, self._hash_index = result
            else:
                self._index, self._hash_index = self._build_index()
        return self._index, self._hash_index

    def _parse_eml(self, path: Path) -> StoredMessage | None:
        """Parse a .eml file into a StoredMessage."""
        try:
            raw = path.read_bytes()
            msg = email.message_from_bytes(raw)
        except Exception:
            return None

        message_id = msg.get("Message-ID", "").strip()
        # Allow messages without Message-ID (use content hash)
        if not message_id:
            message_id = f"<{content_hash(raw)}@content-hash>"

        # Extract folder from path (relative to root, minus template subdirs)
        rel_path = path.relative_to(self._root)
        folder = self._extract_folder(rel_path)

        return StoredMessage(
            message_id=message_id,
            raw=raw,
            folder=folder,
            date=self._parse_date(msg),
            from_addr=msg.get("From", ""),
            to_addr=msg.get("To", ""),
            cc_addr=msg.get("Cc", ""),
            subject=msg.get("Subject", ""),
        )

    def _extract_folder(self, rel_path: Path) -> str:
        """Extract IMAP folder from relative path.

        For path like 'INBOX/2024/01/a1b2c3d4_meeting.eml', returns 'INBOX'.
        """
        parts = rel_path.parts[:-1]  # Remove filename
        if not parts:
            return "INBOX"

        # Folder is everything before sharding subdirs
        # Heuristic: year dirs are 4-digit, hash dirs are 2-char hex
        folder_parts = []
        for part in parts:
            if len(part) == 4 and part.isdigit():
                break  # Year shard
            if len(part) == 2 and all(c in "0123456789abcdef" for c in part.lower()):
                # Could be month (01-12) or hash shard
                if part.isdigit() and 1 <= int(part) <= 12:
                    break  # Month shard
                # Hash shard - break too
                break
            if part == "_undated":
                break
            folder_parts.append(part)

        return "/".join(folder_parts) if folder_parts else "INBOX"

    def _parse_date(self, msg: Message) -> datetime | None:
        """Parse Date header into datetime."""
        date_str = msg.get("Date", "")
        if not date_str:
            return None
        try:
            parsed = email.utils.parsedate_to_datetime(date_str)
            # Ensure timezone-aware
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed
        except Exception:
            return None

    def iter_messages(
        self,
        folder: str | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> Iterator[StoredMessage]:
        """Iterate over messages with optional filters."""
        for eml_path in self._root.rglob("*.eml"):
            # Skip .eml directory
            if ".eml" in eml_path.parts[:-1]:
                continue

            msg = self._parse_eml(eml_path)
            if not msg:
                continue

            # Filter by folder
            if folder and msg.folder != folder:
                continue

            # Filter by date
            if msg.date:
                if start_date and msg.date < start_date:
                    continue
                if end_date and msg.date > end_date:
                    continue

            yield msg

    def get_message(self, message_id: str) -> StoredMessage | None:
        """Get a message by Message-ID."""
        mid_index, _ = self._get_indices()
        path = mid_index.get(message_id)
        if path and path.exists():
            return self._parse_eml(path)
        return None

    def get_message_by_hash(self, sha: str) -> StoredMessage | None:
        """Get a message by content hash."""
        _, hash_index = self._get_indices()
        path = hash_index.get(sha)
        if path and path.exists():
            return self._parse_eml(path)
        return None

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
        """Add a message to storage. Returns path where stored."""
        uid = int(source_uid) if source_uid else None
        path = self._message_path(
            folder=folder,
            raw=raw,
            date=date,
            subject=subject,
            from_addr=from_addr,
            uid=uid,
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)

        # Update indices
        sha = content_hash(raw)
        if self._index is not None:
            self._index[message_id] = path
        if self._hash_index is not None:
            self._hash_index[sha] = path

        return path

    def has_message(self, message_id: str) -> bool:
        """Check if a message exists by Message-ID."""
        mid_index, _ = self._get_indices()
        path = mid_index.get(message_id)
        return path is not None and path.exists()

    def has_content(self, raw: bytes) -> bool:
        """Check if content already exists (by hash)."""
        sha = content_hash(raw)
        _, hash_index = self._get_indices()
        path = hash_index.get(sha)
        return path is not None and path.exists()

    def count(self, folder: str | None = None) -> int:
        """Count messages, optionally filtered by folder."""
        if folder:
            return sum(1 for _ in self.iter_messages(folder=folder))
        # Count all .eml files (excluding .eml directory)
        count = 0
        for eml_path in self._root.rglob("*.eml"):
            if ".eml" not in eml_path.parts[:-1]:
                count += 1
        return count

    def invalidate_index(self) -> None:
        """Clear cached index (e.g., after external changes)."""
        self._index = None
        self._hash_index = None
