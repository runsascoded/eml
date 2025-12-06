"""Email migration logic."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable

from .imap import EmailInfo, FilterConfig, GmailClient, ZohoClient


@dataclass
class MigrationStats:
    """Track migration progress."""
    total_found: int = 0
    skipped_duplicate: int = 0
    skipped_date: int = 0
    migrated: int = 0
    failed: int = 0
    errors: list[str] = field(default_factory=list)


@dataclass
class MigrationConfig:
    """Migration configuration."""
    gmail_user: str
    gmail_password: str
    zoho_user: str
    zoho_password: str
    filters: FilterConfig
    dest_folder: str = "INBOX"
    start_date: datetime | None = None
    end_date: datetime | None = None
    dry_run: bool = False
    limit: int | None = None


class EmailMigrator:
    """Migrate emails from Gmail to Zoho."""

    def __init__(self, config: MigrationConfig):
        self.config = config
        self.stats = MigrationStats()
        self._gmail: GmailClient | None = None
        self._zoho: ZohoClient | None = None
        self._existing_ids: set[str] = set()

    def connect(self) -> None:
        """Connect to both Gmail and Zoho."""
        self._gmail = GmailClient()
        self._gmail.connect(self.config.gmail_user, self.config.gmail_password)

        if not self.config.dry_run:
            self._zoho = ZohoClient()
            self._zoho.connect(self.config.zoho_user, self.config.zoho_password)
            self._zoho.create_folder(self.config.dest_folder)
            self._existing_ids = self._zoho.get_message_ids(self.config.dest_folder)

    def disconnect(self) -> None:
        """Disconnect from both servers."""
        if self._gmail:
            self._gmail.disconnect()
        if self._zoho:
            self._zoho.disconnect()

    @property
    def gmail(self) -> GmailClient:
        if not self._gmail:
            raise RuntimeError("Not connected to Gmail")
        return self._gmail

    @property
    def zoho(self) -> ZohoClient:
        if not self._zoho:
            raise RuntimeError("Not connected to Zoho")
        return self._zoho

    def _should_skip(self, info: EmailInfo) -> str | None:
        """Check if message should be skipped. Returns reason or None."""
        if info.message_id in self._existing_ids:
            return "duplicate"

        if self.config.start_date and info.date:
            if info.date < self.config.start_date:
                return "before_start_date"

        if self.config.end_date and info.date:
            if info.date > self.config.end_date:
                return "after_end_date"

        return None

    def run(
        self,
        progress_callback: Callable[[EmailInfo, str], None] | None = None,
    ) -> MigrationStats:
        """Run the migration."""
        self.stats = MigrationStats()

        uids = self.gmail.search_by_filters(self.config.filters)
        self.stats.total_found = len(uids)

        processed = 0
        for uid in uids:
            if self.config.limit and processed >= self.config.limit:
                break

            try:
                info = self.gmail.fetch_info(uid)
            except Exception as e:
                self.stats.failed += 1
                self.stats.errors.append(f"Failed to fetch UID {uid}: {e}")
                continue

            skip_reason = self._should_skip(info)
            if skip_reason:
                if skip_reason == "duplicate":
                    self.stats.skipped_duplicate += 1
                else:
                    self.stats.skipped_date += 1
                if progress_callback:
                    progress_callback(info, f"skipped:{skip_reason}")
                continue

            if self.config.dry_run:
                if progress_callback:
                    progress_callback(info, "would_migrate")
                processed += 1
                continue

            try:
                raw = self.gmail.fetch_raw(uid)
                success = self.zoho.append_message(
                    self.config.dest_folder,
                    raw,
                    info.date,
                )
                if success:
                    self.stats.migrated += 1
                    self._existing_ids.add(info.message_id)
                    if progress_callback:
                        progress_callback(info, "migrated")
                else:
                    self.stats.failed += 1
                    self.stats.errors.append(f"Failed to append: {info.subject[:50]}")
                    if progress_callback:
                        progress_callback(info, "failed")
            except Exception as e:
                self.stats.failed += 1
                self.stats.errors.append(f"Error migrating {info.message_id}: {e}")
                if progress_callback:
                    progress_callback(info, "failed")

            processed += 1

        return self.stats

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.disconnect()
