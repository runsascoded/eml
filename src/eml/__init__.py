"""Email migration library for Gmail → Zoho."""

from .imap import EmailInfo, FilterConfig, GmailClient, ZohoClient
from .migrate import EmailMigrator, MigrationConfig, MigrationStats

__all__ = [
    "EmailInfo",
    "EmailMigrator",
    "FilterConfig",
    "GmailClient",
    "MigrationConfig",
    "MigrationStats",
    "ZohoClient",
]
