"""Storage layout protocol for eml v2."""

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Iterator, Protocol, runtime_checkable


@dataclass
class StoredMessage:
    """A message in local storage."""
    message_id: str
    raw: bytes
    folder: str
    date: datetime | None = None
    from_addr: str = ""
    to_addr: str = ""
    cc_addr: str = ""
    subject: str = ""
    source_uid: str | None = None
    tags: list[str] = field(default_factory=list)


@runtime_checkable
class StorageLayout(Protocol):
    """Protocol for email storage layouts.

    Implementations:
    - TreeLayout: .eml files in directories (tree:flat, tree:month, etc.)
    - SqliteLayout: blobs in .eml/msgs.db
    """

    @property
    def root(self) -> Path:
        """Root directory of the eml project."""
        ...

    def iter_messages(
        self,
        folder: str | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> Iterator[StoredMessage]:
        """Iterate over messages with optional filters."""
        ...

    def get_message(self, message_id: str) -> StoredMessage | None:
        """Get a message by Message-ID."""
        ...

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
        ...

    def has_message(self, message_id: str) -> bool:
        """Check if a message exists by Message-ID."""
        ...

    def count(self, folder: str | None = None) -> int:
        """Count messages, optionally filtered by folder."""
        ...


def message_id_to_filename(message_id: str) -> str:
    """Convert Message-ID to deterministic filename.

    Uses first 8 chars of SHA-256 hash of Message-ID.
    """
    import hashlib
    h = hashlib.sha256(message_id.encode()).hexdigest()
    return f"{h[:8]}.eml"
