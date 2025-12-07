"""Email migration and archival tool."""

from .imap import EmailInfo, FilterConfig, GmailClient, ZohoClient
from .migrate import EmailMigrator, MigrationConfig, MigrationStats
from .storage import Account, AccountStorage, MessageStorage, StoredMessage

__all__ = [
    "Account",
    "AccountStorage",
    "EmailInfo",
    "EmailMigrator",
    "FilterConfig",
    "GmailClient",
    "MessageStorage",
    "MigrationConfig",
    "MigrationStats",
    "StoredMessage",
    "ZohoClient",
]
