"""IMAP client wrappers for Gmail and Zoho."""

import imaplib
import email
from email.policy import default as email_policy
from email.utils import parsedate_to_datetime
from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterator


GMAIL_IMAP_HOST = "imap.gmail.com"
GMAIL_IMAP_PORT = 993
ZOHO_IMAP_HOST = "imap.zoho.com"
ZOHO_IMAP_PORT = 993


@dataclass
class EmailInfo:
    """Lightweight email metadata for listing/filtering."""
    uid: bytes
    message_id: str
    date: datetime | None
    from_addr: str
    to_addr: str
    cc_addr: str
    subject: str


@dataclass
class FilterConfig:
    """Email filter configuration."""
    addresses: list[str] = field(default_factory=list)
    domains: list[str] = field(default_factory=list)
    from_addresses: list[str] = field(default_factory=list)
    from_domains: list[str] = field(default_factory=list)

    def is_empty(self) -> bool:
        return (
            not self.addresses
            and not self.domains
            and not self.from_addresses
            and not self.from_domains
        )

    def build_imap_query(self) -> str:
        """Build IMAP search query from filters.

        addresses/domains match To/From/Cc; from_* only match From.
        """
        terms: list[str] = []

        for addr in self.addresses:
            terms.append(f'TO "{addr}"')
            terms.append(f'FROM "{addr}"')
            terms.append(f'CC "{addr}"')

        for domain in self.domains:
            terms.append(f'TO "{domain}"')
            terms.append(f'FROM "{domain}"')
            terms.append(f'CC "{domain}"')

        for addr in self.from_addresses:
            terms.append(f'FROM "{addr}"')

        for domain in self.from_domains:
            terms.append(f'FROM "{domain}"')

        if not terms:
            return "ALL"

        if len(terms) == 1:
            return f"({terms[0]})"

        # Build nested OR: (OR (OR (OR a b) c) d)
        result = terms[0]
        for term in terms[1:]:
            result = f"OR {result} {term}"
        return f"({result})"


class IMAPClient:
    """Base IMAP client with common operations."""

    def __init__(self, host: str, port: int = 993):
        self.host = host
        self.port = port
        self._conn: imaplib.IMAP4_SSL | None = None

    def connect(self, user: str, password: str) -> None:
        self._conn = imaplib.IMAP4_SSL(self.host, self.port)
        self._conn.login(user, password)

    def disconnect(self) -> None:
        if self._conn:
            try:
                self._conn.logout()
            except Exception:
                pass
            self._conn = None

    def list_folders(self) -> list[tuple[str, str, int | None]]:
        """List all folders. Returns [(flags, delimiter, name), ...]."""
        typ, data = self.conn.list()
        if typ != "OK":
            raise RuntimeError(f"Failed to list folders: {data}")

        folders = []
        for item in data:
            if item is None:
                continue
            # Parse: b'(\\HasNoChildren) "/" "INBOX"'
            decoded = item.decode() if isinstance(item, bytes) else item
            # Extract flags, delimiter, and name
            import re
            match = re.match(r'\(([^)]*)\)\s+"([^"]+)"\s+"?([^"]+)"?', decoded)
            if match:
                flags, delim, name = match.groups()
                # Get message count for folder
                try:
                    typ2, data2 = self.conn.select(name, readonly=True)
                    count = int(data2[0]) if typ2 == "OK" else None
                except Exception:
                    count = None
                folders.append((flags, delim, name, count))
        return folders

    @property
    def conn(self) -> imaplib.IMAP4_SSL:
        if not self._conn:
            raise RuntimeError("Not connected")
        return self._conn

    def select_folder(self, folder: str, readonly: bool = True) -> int:
        """Select a folder, return message count."""
        typ, data = self.conn.select(folder, readonly=readonly)
        if typ != "OK":
            raise RuntimeError(f"Failed to select folder {folder}: {data}")
        return int(data[0])

    def search(self, criteria: str) -> list[bytes]:
        """Search for messages matching criteria, return UIDs."""
        typ, data = self.conn.uid("SEARCH", None, criteria)
        if typ != "OK":
            raise RuntimeError(f"Search failed: {data}")
        return data[0].split()

    def fetch_info(self, uid: bytes) -> EmailInfo:
        """Fetch lightweight email info (headers only)."""
        typ, data = self.conn.uid(
            "FETCH", uid, "(BODY.PEEK[HEADER.FIELDS (MESSAGE-ID DATE FROM TO CC SUBJECT)])"
        )
        if typ != "OK" or not data or not data[0]:
            raise RuntimeError(f"Failed to fetch headers for UID {uid}")

        header_data = data[0][1]
        msg = email.message_from_bytes(header_data, policy=email_policy)

        date = None
        if msg["Date"]:
            try:
                date = parsedate_to_datetime(msg["Date"])
            except Exception:
                pass

        return EmailInfo(
            uid=uid,
            message_id=msg.get("Message-ID", ""),
            date=date,
            from_addr=msg.get("From", ""),
            to_addr=msg.get("To", ""),
            cc_addr=msg.get("Cc", ""),
            subject=msg.get("Subject", ""),
        )

    def fetch_raw(self, uid: bytes) -> bytes:
        """Fetch full raw message by UID."""
        typ, data = self.conn.uid("FETCH", uid, "(RFC822)")
        if typ != "OK" or not data or not data[0]:
            raise RuntimeError(f"Failed to fetch message for UID {uid}")
        return data[0][1]

    def get_message_ids(self, folder: str) -> set[str]:
        """Get all Message-IDs in a folder (for deduplication)."""
        self.select_folder(folder, readonly=True)
        uids = self.search("ALL")
        message_ids = set()
        for uid in uids:
            try:
                info = self.fetch_info(uid)
                if info.message_id:
                    message_ids.add(info.message_id)
            except Exception:
                continue
        return message_ids

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.disconnect()


class GmailClient(IMAPClient):
    """Gmail-specific IMAP client."""

    def __init__(self):
        super().__init__(GMAIL_IMAP_HOST, GMAIL_IMAP_PORT)
        self.all_mail_folder = "[Gmail]/All Mail"

    def search_by_filters(self, filters: FilterConfig) -> list[bytes]:
        """Search for emails matching filter config."""
        self.select_folder(self.all_mail_folder, readonly=True)
        query = filters.build_imap_query()
        return self.search(query)

    def iter_messages(
        self,
        filters: FilterConfig,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> Iterator[EmailInfo]:
        """Iterate over messages matching filters."""
        uids = self.search_by_filters(filters)
        for uid in uids:
            try:
                info = self.fetch_info(uid)
                if start_date and info.date and info.date < start_date:
                    continue
                if end_date and info.date and info.date > end_date:
                    continue
                yield info
            except Exception:
                continue


class ZohoClient(IMAPClient):
    """Zoho-specific IMAP client."""

    def __init__(self):
        super().__init__(ZOHO_IMAP_HOST, ZOHO_IMAP_PORT)

    def append_message(
        self,
        folder: str,
        raw_message: bytes,
        date: datetime | None = None,
    ) -> bool:
        """Append a message to a folder, preserving original date."""
        flags = None
        if date:
            internal_date = imaplib.Time2Internaldate(date.timetuple())
        else:
            internal_date = None

        typ, data = self.conn.append(folder, flags, internal_date, raw_message)
        return typ == "OK"

    def create_folder(self, folder: str) -> bool:
        """Create a folder if it doesn't exist."""
        typ, data = self.conn.create(folder)
        return typ == "OK" or b"ALREADYEXISTS" in (data[0] if data else b"")
