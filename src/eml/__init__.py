"""Email migration and archival tool."""

from .imap import EmailInfo, FilterConfig, GmailClient, ZohoClient
from .migrate import EmailMigrator, MigrationConfig, MigrationStats
from .storage import EmailStorage, StoredMessage

__all__ = [
    "EmailInfo",
    "EmailMigrator",
    "EmailStorage",
    "FilterConfig",
    "GmailClient",
    "MigrationConfig",
    "MigrationStats",
    "StoredMessage",
    "ZohoClient",
]
