"""CLI for email migration."""

import os
import re
import sys
from datetime import datetime
from pathlib import Path

import yaml
from click import group, command, option, argument, echo, style
from dotenv import load_dotenv

from .imap import EmailInfo, FilterConfig, GmailClient, ZohoClient, IMAPClient
from .migrate import EmailMigrator, MigrationConfig


def err(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def format_date(dt: datetime | None) -> str:
    return dt.strftime("%Y-%m-%d") if dt else "?"


def load_config_file(path: str) -> dict:
    """Load config from YAML file."""
    with open(path) as f:
        return yaml.safe_load(f) or {}


def get_imap_client(host: str) -> IMAPClient:
    """Get appropriate IMAP client for host."""
    if "gmail" in host.lower():
        return GmailClient()
    elif "zoho" in host.lower():
        return ZohoClient()
    else:
        return IMAPClient(host)


def progress_handler(info: EmailInfo, status: str) -> None:
    """Print progress for each email processed."""
    date_str = format_date(info.date)
    from_short = info.from_addr[:30] if info.from_addr else "?"
    subj_short = info.subject[:50] if info.subject else "(no subject)"

    if status == "migrated":
        icon = style("✓", fg="green")
    elif status == "would_migrate":
        icon = style("○", fg="yellow")
    elif status.startswith("skipped"):
        icon = style("·", fg="bright_black")
    else:
        icon = style("✗", fg="red")

    echo(f"{icon} {date_str} | {from_short:30} | {subj_short}")


@group()
def main():
    """Email migration tools."""
    load_dotenv()


@main.command()
@option('-h', '--host', default="gmail", help="IMAP host (gmail, zoho, or hostname)")
@option('-u', '--user', envvar="GMAIL_USER", help="IMAP username (or GMAIL_USER env)")
@option('-p', '--password', envvar="GMAIL_APP_PASSWORD", help="IMAP password (or GMAIL_APP_PASSWORD env)")
def folders(host: str, user: str | None, password: str | None):
    """List folders/labels for an IMAP account.

    \b
    Examples:
      emails folders                     # Uses GMAIL_USER/GMAIL_APP_PASSWORD
      emails folders -h zoho -u you@example.com
    """
    if not user or not password:
        err("Missing credentials. Set GMAIL_USER/GMAIL_APP_PASSWORD or use -u/-p flags.")
        sys.exit(1)

    # Resolve host aliases
    if host == "gmail":
        client = GmailClient()
    elif host == "zoho":
        client = ZohoClient()
    else:
        client = IMAPClient(host)

    try:
        client.connect(user, password)
        folders_list = client.list_folders()

        echo(f"Folders for {user}:\n")
        for flags, delim, name, count in sorted(folders_list, key=lambda x: x[2]):
            count_str = f"({count:,})" if count is not None else ""
            special = ""
            if "\\Noselect" in flags:
                special = " [not selectable]"
            echo(f"  {name:40} {count_str:>10}{special}")

    except Exception as e:
        err(f"Error: {e}")
        sys.exit(1)
    finally:
        client.disconnect()


@main.command()
@option('-a', '--address', 'addresses', multiple=True, help="Match To/From/Cc address (repeatable)")
@option('-c', '--config', 'config_file', type=str, help="YAML config file")
@option('-d', '--from-domain', 'from_domains', multiple=True, help="Match From domain only (repeatable)")
@option('-D', '--domain', 'domains', multiple=True, help="Match To/From/Cc domain (repeatable)")
@option('-e', '--end-date', type=str, help="End date (YYYY-MM-DD)")
@option('-f', '--folder', type=str, help="Destination folder")
@option('-F', '--from-address', 'from_addresses', multiple=True, help="Match From address only (repeatable)")
@option('-l', '--limit', type=int, help="Max emails to process")
@option('-n', '--dry-run', is_flag=True, help="List matching emails without migrating")
@option('-s', '--start-date', type=str, help="Start date (YYYY-MM-DD)")
@option('-v', '--verbose', is_flag=True, help="Show skipped messages too")
def migrate(
    addresses: tuple[str, ...],
    config_file: str | None,
    domains: tuple[str, ...],
    from_domains: tuple[str, ...],
    end_date: str | None,
    folder: str | None,
    from_addresses: tuple[str, ...],
    limit: int | None,
    dry_run: bool,
    start_date: str | None,
    verbose: bool,
):
    """Migrate emails between IMAP mailboxes (e.g., Gmail to Zoho).

    \b
    Config file (YAML):
      emails migrate -c config.yml -n

    \b
    Example config.yml (see example.yml):
      filters:
        addresses:          # Match To/From/Cc (full address)
          - team@googlegroups.com
        domains:            # Match To/From/Cc (domain)
          - company.com
        from_addresses:     # Match From only (full address)
          - person@example.com
        from_domains:       # Match From only (domain)
          - partner.org
      folder: INBOX
      start_date: 2020-01-01

    \b
    CLI options extend/override config file values.

    \b
    Filter options (at least one required, via -c or flags):
      -a, --address       Match To/From/Cc (full address)
      -D, --domain        Match To/From/Cc (domain)
      -F, --from-address  Match From only (full address)
      -d, --from-domain   Match From only (domain)

    \b
    Requires environment variables (or .env file):
      GMAIL_USER          Source Gmail address
      GMAIL_APP_PASSWORD  Gmail app password (requires 2FA)
      ZOHO_USER           Destination Zoho address
      ZOHO_PASSWORD       Zoho password or app password
    """
    # Load config file if provided
    cfg: dict = {}
    if config_file:
        if not Path(config_file).exists():
            err(f"Config file not found: {config_file}")
            sys.exit(1)
        cfg = load_config_file(config_file)

    # Build filters: CLI args override/extend config file
    cfg_filters = cfg.get("filters", {})
    all_addresses = list(cfg_filters.get("addresses", [])) + list(addresses)
    all_domains = list(cfg_filters.get("domains", [])) + list(domains)
    all_from_addresses = list(cfg_filters.get("from_addresses", [])) + list(from_addresses)
    all_from_domains = list(cfg_filters.get("from_domains", [])) + list(from_domains)

    filters = FilterConfig(
        addresses=all_addresses,
        domains=all_domains,
        from_addresses=all_from_addresses,
        from_domains=all_from_domains,
    )

    if filters.is_empty():
        err("Error: At least one filter required (-a, -D, -F, -d, or via -c config)")
        sys.exit(1)

    # Other options: CLI overrides config
    dest_folder = folder or cfg.get("folder", "INBOX")
    start_date_str = start_date or cfg.get("start_date")
    end_date_str = end_date or cfg.get("end_date")
    limit = limit if limit is not None else cfg.get("limit")

    gmail_user = os.environ.get("GMAIL_USER")
    gmail_password = os.environ.get("GMAIL_APP_PASSWORD")
    zoho_user = os.environ.get("ZOHO_USER")
    zoho_password = os.environ.get("ZOHO_PASSWORD")

    missing = []
    if not gmail_user:
        missing.append("GMAIL_USER")
    if not gmail_password:
        missing.append("GMAIL_APP_PASSWORD")
    if not dry_run:
        if not zoho_user:
            missing.append("ZOHO_USER")
        if not zoho_password:
            missing.append("ZOHO_PASSWORD")

    if missing:
        err(f"Missing required environment variables: {', '.join(missing)}")
        err("Set them in .env or export them.")
        sys.exit(1)

    # Parse dates (handle both string and date objects from YAML)
    def parse_date(val) -> datetime | None:
        if val is None:
            return None
        if isinstance(val, datetime):
            return val
        if hasattr(val, 'isoformat'):  # date object
            return datetime.combine(val, datetime.min.time())
        return datetime.fromisoformat(str(val))

    parsed_start = parse_date(start_date_str)
    parsed_end = parse_date(end_date_str)

    config = MigrationConfig(
        gmail_user=gmail_user,
        gmail_password=gmail_password,
        zoho_user=zoho_user or "",
        zoho_password=zoho_password or "",
        filters=filters,
        dest_folder=dest_folder,
        start_date=parsed_start,
        end_date=parsed_end,
        dry_run=dry_run,
        limit=limit,
    )

    def filtered_progress(info: EmailInfo, status: str) -> None:
        if verbose or not status.startswith("skipped"):
            progress_handler(info, status)

    echo("Filters:")
    if all_addresses:
        echo(f"  Addresses (To/From/Cc): {', '.join(all_addresses)}")
    if all_domains:
        echo(f"  Domains (To/From/Cc): {', '.join(all_domains)}")
    if all_from_addresses:
        echo(f"  From addresses: {', '.join(all_from_addresses)}")
    if all_from_domains:
        echo(f"  From domains: {', '.join(all_from_domains)}")
    if parsed_start or parsed_end:
        echo(f"  Date range: {format_date(parsed_start)} to {format_date(parsed_end)}")
    if dry_run:
        echo(style("DRY RUN - no changes will be made", fg="yellow"))
    echo()

    try:
        with EmailMigrator(config) as migrator:
            stats = migrator.run(progress_callback=filtered_progress)
    except Exception as e:
        err(f"Error: {e}")
        sys.exit(1)

    echo()
    echo(f"Found: {stats.total_found}")
    echo(f"Skipped (duplicate): {stats.skipped_duplicate}")
    echo(f"Skipped (date): {stats.skipped_date}")
    if dry_run:
        echo(f"Would migrate: {stats.total_found - stats.skipped_duplicate - stats.skipped_date}")
    else:
        echo(f"Migrated: {stats.migrated}")
        echo(f"Failed: {stats.failed}")

    if stats.errors:
        echo()
        echo(style("Errors:", fg="red"))
        for error in stats.errors[:10]:
            err(f"  {error}")
        if len(stats.errors) > 10:
            err(f"  ... and {len(stats.errors) - 10} more")


if __name__ == "__main__":
    main()
