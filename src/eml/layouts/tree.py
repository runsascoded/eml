"""Tree-based storage layout using .eml files in directories."""

import email
import email.utils
from datetime import datetime, timezone
from email.message import Message
from pathlib import Path
from typing import Iterator, Literal

from .base import StorageLayout, StoredMessage, message_id_to_filename


ShardingScheme = Literal["flat", "year", "month", "day", "hash2"]


class TreeLayout:
    """Store emails as .eml files in a directory tree.

    Supports different sharding schemes:
    - flat: INBOX/a1b2c3d4.eml
    - year: INBOX/2024/a1b2c3d4.eml
    - month: INBOX/2024/01/a1b2c3d4.eml
    - day: INBOX/2024/01/15/a1b2c3d4.eml
    - hash2: INBOX/a1/b2c3d4e5.eml
    """

    def __init__(self, root: Path, sharding: ShardingScheme = "month"):
        self._root = root
        self.sharding = sharding
        self._index: dict[str, Path] | None = None

    @property
    def root(self) -> Path:
        return self._root

    def _get_shard_path(
        self,
        folder: str,
        message_id: str,
        date: datetime | None,
    ) -> Path:
        """Get the directory path for a message based on sharding scheme."""
        base = self._root / folder

        if self.sharding == "flat":
            return base
        elif self.sharding == "hash2":
            filename = message_id_to_filename(message_id)
            return base / filename[:2]
        elif date:
            if self.sharding == "year":
                return base / str(date.year)
            elif self.sharding == "month":
                return base / str(date.year) / f"{date.month:02d}"
            elif self.sharding == "day":
                return base / str(date.year) / f"{date.month:02d}" / f"{date.day:02d}"

        # Fallback for date-based sharding without date
        return base / "_undated"

    def _message_path(
        self,
        folder: str,
        message_id: str,
        date: datetime | None,
    ) -> Path:
        """Get full path for a message."""
        shard = self._get_shard_path(folder, message_id, date)
        filename = message_id_to_filename(message_id)
        return shard / filename

    def _build_index(self) -> dict[str, Path]:
        """Build index of message_id -> path by scanning .eml files."""
        index: dict[str, Path] = {}
        for eml_path in self._root.rglob("*.eml"):
            # Skip .eml directory
            if ".eml" in eml_path.parts[:-1]:
                continue
            try:
                msg = self._parse_eml(eml_path)
                if msg and msg.message_id:
                    index[msg.message_id] = eml_path
            except Exception:
                pass
        return index

    def _get_index(self) -> dict[str, Path]:
        """Get or build the message index."""
        if self._index is None:
            self._index = self._build_index()
        return self._index

    def _parse_eml(self, path: Path) -> StoredMessage | None:
        """Parse a .eml file into a StoredMessage."""
        try:
            raw = path.read_bytes()
            msg = email.message_from_bytes(raw)
        except Exception:
            return None

        message_id = msg.get("Message-ID", "").strip()
        if not message_id:
            return None

        # Extract folder from path (relative to root, minus sharding dirs)
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

        For path like 'INBOX/2024/01/a1b2c3d4.eml', returns 'INBOX'.
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
        index = self._get_index()
        path = index.get(message_id)
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
        path = self._message_path(folder, message_id, date)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)

        # Update index
        if self._index is not None:
            self._index[message_id] = path

        return path

    def has_message(self, message_id: str) -> bool:
        """Check if a message exists by Message-ID."""
        index = self._get_index()
        path = index.get(message_id)
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
