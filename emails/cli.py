"""CLI for email migration."""

import os
import sys
from datetime import datetime

from click import command, option, echo, style
from dotenv import load_dotenv

from .imap import EmailInfo, FilterConfig
from .migrate import EmailMigrator, MigrationConfig


def err(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def format_date(dt: datetime | None) -> str:
    return dt.strftime("%Y-%m-%d") if dt else "?"


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


@command()
@option('-a', '--address', 'addresses', multiple=True, help="Match To/From/Cc (repeatable)")
@option('-d', '--from-domain', 'from_domains', multiple=True, help="Match From domain (repeatable)")
@option('-e', '--end-date', type=str, help="End date (YYYY-MM-DD)")
@option('-f', '--folder', default="INBOX", help="Destination folder in Zoho")
@option('-F', '--from-address', 'from_addresses', multiple=True, help="Match From address only (repeatable)")
@option('-l', '--limit', type=int, help="Max emails to process")
@option('-n', '--dry-run', is_flag=True, help="List matching emails without migrating")
@option('-s', '--start-date', type=str, help="Start date (YYYY-MM-DD)")
@option('-v', '--verbose', is_flag=True, help="Show skipped messages too")
def main(
    addresses: tuple[str, ...],
    from_domains: tuple[str, ...],
    end_date: str | None,
    folder: str,
    from_addresses: tuple[str, ...],
    limit: int | None,
    dry_run: bool,
    start_date: str | None,
    verbose: bool,
):
    """Migrate emails from Gmail to Zoho.

    \b
    Filter options (at least one required):
      -a, --address       Match To, From, or Cc
      -d, --from-domain   Match From domain only
      -F, --from-address  Match From address only

    \b
    Example:
      emails \\
             -a address1 \\
             -a address2 \\
             -d domain1 \\
             -d domain2 \\
             -F address3 \\
             -n

    \b
    Requires environment variables (or .env file):
      GMAIL_USER          Gmail address
      GMAIL_APP_PASSWORD  Gmail app password (not regular password)
      ZOHO_USER           Zoho email address
      ZOHO_PASSWORD       Zoho password
    """
    load_dotenv()

    filters = FilterConfig(
        addresses=list(addresses),
        from_domains=list(from_domains),
        from_addresses=list(from_addresses),
    )

    if filters.is_empty():
        err("Error: At least one filter required (-a, -d, or -F)")
        sys.exit(1)

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

    parsed_start = datetime.fromisoformat(start_date) if start_date else None
    parsed_end = datetime.fromisoformat(end_date) if end_date else None

    config = MigrationConfig(
        gmail_user=gmail_user,
        gmail_password=gmail_password,
        zoho_user=zoho_user or "",
        zoho_password=zoho_password or "",
        filters=filters,
        dest_folder=folder,
        start_date=parsed_start,
        end_date=parsed_end,
        dry_run=dry_run,
        limit=limit,
    )

    def filtered_progress(info: EmailInfo, status: str) -> None:
        if verbose or not status.startswith("skipped"):
            progress_handler(info, status)

    echo("Filters:")
    if addresses:
        echo(f"  To/From/Cc: {', '.join(addresses)}")
    if from_domains:
        echo(f"  From domains: {', '.join(from_domains)}")
    if from_addresses:
        echo(f"  From addresses: {', '.join(from_addresses)}")
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
