"""Miscellaneous commands: init, folders, ls, tags, convert, serve, migrate, ingest."""

import email
import os
import subprocess
import sys
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

import click
import humanize
import yaml
from click import argument, echo, option, style
from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
)

from ..config import (
    AccountConfig,
    EmlConfig,
    find_eml_root,
    get_eml_root,
    load_config,
    save_config,
)
from ..imap import EmailInfo, FilterConfig, GmailClient, IMAPClient, ZohoClient
from ..layouts import PRESETS, SqliteLayout, TreeLayout, resolve_preset
from ..migrate import EmailMigrator, MigrationConfig
from ..storage import (
    ACCTS_DB,
    AccountStorage,
    EML_DIR,
    GLOBAL_CONFIG_DIR,
    MessageStorage,
    find_eml_dir,
    get_msgs_db_path,
)

from .utils import (
    err,
    format_date,
    get_account_any,
    get_imap_client,
    get_storage_layout,
    has_config,
    require_init,
    tag_option,
    validate_layout,
)


def load_config_file(path: str) -> dict:
    """Load config from YAML file."""
    with open(path) as f:
        return yaml.safe_load(f) or {}


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


# =============================================================================
# init
# =============================================================================


@click.command()
@option('-g', '--global', 'use_global', is_flag=True, help="Initialize global config (~/.config/eml)")
@option('-L', '--layout', default="default", callback=validate_layout,
        help="Layout preset (default, flat, daily, compact, hash2, sqlite) or template")
def init(use_global: bool, layout: str):
    """Initialize eml project directory.

    \b
    Layout presets:
      default  $folder/$yyyy/$mm/$dd/${hhmmss}_${sha8}_${subj}.eml
      monthly  $folder/$yyyy/$mm/${sha8}_${subj}.eml
      flat     $folder/${sha8}_${subj}.eml
      daily    $folder/$yyyy/$mm/$dd/${sha8}_${subj}.eml
      compact  $folder/$yyyy$mm$dd_${sha8}.eml
      hash2    $folder/${sha2}/${sha8}_${subj}.eml
      sqlite   Store messages in .eml/msgs.db

    \b
    Or use a custom template:
      eml init -L '$folder/$yyyy/$mm/$dd/${hhmmss}_${sha8}_${subj20}.eml'

    \b
    Examples:
      eml init                       # Default layout
      eml init -L flat               # Flat directory structure
      eml init -L sqlite             # SQLite blob storage
      eml init -g                    # Create ~/.config/eml/ for global accounts
    """
    if use_global:
        target = GLOBAL_CONFIG_DIR
        target.mkdir(parents=True, exist_ok=True)
        accts_path = target / ACCTS_DB
        with AccountStorage(accts_path) as storage:
            pass  # Just create schema
        echo(f"Initialized global config: {target}")
        return

    # Initialize project
    root = Path.cwd()
    eml_dir = root / EML_DIR
    config_path = eml_dir / "config.yaml"

    if config_path.exists():
        echo(f"Already initialized: {eml_dir}")
        return

    # Create .eml directory structure
    eml_dir.mkdir(parents=True, exist_ok=True)
    (eml_dir / "sync-state").mkdir(exist_ok=True)
    (eml_dir / "pushed").mkdir(exist_ok=True)

    # Create config.yaml
    config = EmlConfig(layout=layout)
    save_config(config, root)

    # Initialize sqlite db if using sqlite layout
    if layout == "sqlite":
        with SqliteLayout(root) as storage:
            pass

    # Initialize git if not already a repo
    git_dir = root / ".git"
    if not git_dir.exists():
        subprocess.run(["git", "init"], cwd=root, capture_output=True)
        echo("Initialized git repository")

    # Show resolved template if using a preset
    resolved = resolve_preset(layout)
    layout_display = f"{layout}" if layout == resolved else f"{layout} → {resolved}"

    echo(f"Initialized: {eml_dir}")
    echo(f"  config.yaml   - accounts and layout")
    echo(f"  Layout: {layout_display}")
    echo(f"  sync-state/   - pull progress per account")
    echo(f"  pushed/       - push manifests per account")
    echo()
    echo("Next steps:")
    echo("  eml account add y/user imap user@example.com --host imap.example.com")
    echo("  eml account add g/user gmail user@gmail.com")
    echo("  eml pull y/user -f INBOX")


# =============================================================================
# folders
# =============================================================================


@click.command(no_args_is_help=True)
@option('-p', '--password', help="IMAP password")
@option('-s', '--size', is_flag=True, help="Show total size of messages")
@option('-u', '--user', help="IMAP username")
@argument('account_or_folder', required=False)
@argument('folder', required=False)
def folders(password: str | None, size: bool, user: str | None, account_or_folder: str | None, folder: str | None):
    """List folders/labels, or show count for a specific folder.

    \b
    Examples:
      eml folders gmail                  # List all folders for gmail account
      eml folders gmail INBOX            # Show count for INBOX
      eml folders gmail -s "Work"        # Show count and size
    """
    # Parse arguments
    acct = None
    if account_or_folder:
        acct = get_account_any(account_or_folder)
        if not acct:
            # Maybe it's a folder name with explicit creds?
            if user and password:
                folder = account_or_folder
            else:
                err(f"Account '{account_or_folder}' not found.")
                err("  eml account add gmail user@gmail.com")
                sys.exit(1)

    if acct:
        src_type = acct.type
        src_user = user or acct.user
        src_password = password or acct.password
    else:
        src_type = "gmail"
        src_user = user or os.environ.get("SRC_USER")
        src_password = password or os.environ.get("SRC_PASS")

    if not src_user or not src_password:
        err("Missing credentials. Use an account name or -u/-p flags.")
        sys.exit(1)

    # Create IMAP client - use host for generic imap accounts
    if isinstance(acct, AccountConfig) and acct.host:
        client = IMAPClient(acct.host, acct.port)
    else:
        client = get_imap_client(src_type)

    try:
        client.connect(src_user, src_password)

        if folder:
            msg_count, _ = client.select_folder(folder, readonly=True)
            if size:
                total_size = client.get_folder_size()
                echo(f"{folder}: {msg_count:,} messages ({humanize.naturalsize(total_size)})")
            else:
                echo(f"{folder}: {msg_count:,} messages")
        else:
            folders_list = client.list_folders()
            echo(f"Folders for {src_user}:\n")
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


# =============================================================================
# ls
# =============================================================================


@click.command()
@require_init
@option('-f', '--from', 'from_filter', type=str, help="Filter by From address")
@option('-l', '--limit', default=20, help="Max messages to show")
@option('-s', '--subject', 'subject_filter', type=str, help="Filter by subject")
@tag_option
@argument('query', required=False)
def ls(
    from_filter: str | None,
    limit: int,
    subject_filter: str | None,
    tag: str | None,
    query: str | None,
):
    """List messages in local storage.

    \b
    Examples:
      eml ls                              # List recent messages
      eml ls -t work                      # List 'work' tagged messages
      eml ls -l 50                        # Show 50 messages
      eml ls -f "john@"                   # Filter by From
      eml ls "search term"                # Search in From/Subject
    """
    try:
        msgs_path = get_msgs_db_path()
        storage = MessageStorage(msgs_path)
        storage.connect()

        total = storage.count(tag=tag)
        tag_info = f" (tag: {tag})" if tag else ""
        echo(f"Total messages{tag_info}: {total:,}\n")

        # Build query
        if tag:
            sql = """SELECT m.* FROM messages m
                     JOIN message_tags t ON m.message_id = t.message_id
                     WHERE t.tag = ?"""
            params: list = [tag]
        else:
            sql = "SELECT * FROM messages WHERE 1=1"
            params = []

        if from_filter:
            sql += " AND from_addr LIKE ?"
            params.append(f"%{from_filter}%")

        if subject_filter:
            sql += " AND subject LIKE ?"
            params.append(f"%{subject_filter}%")

        if query:
            sql += " AND (from_addr LIKE ? OR subject LIKE ?)"
            params.extend([f"%{query}%", f"%{query}%"])

        sql += " ORDER BY date DESC LIMIT ?"
        params.append(limit)

        cur = storage.conn.execute(sql, params)
        rows = cur.fetchall()

        if not rows:
            echo("No messages found.")
            return

        for row in rows:
            date_str = row["date"][:10] if row["date"] else "?"
            from_short = (row["from_addr"] or "?")[:35]
            subj_short = (row["subject"] or "(no subject)")[:45]
            echo(f"{date_str} | {from_short:35} | {subj_short}")

        if len(rows) == limit:
            echo(f"\n(showing first {limit}, use -l to see more)")

        storage.disconnect()

    except FileNotFoundError as e:
        err(str(e))
        sys.exit(1)
    except Exception as e:
        err(f"Error: {e}")
        sys.exit(1)


# =============================================================================
# tags
# =============================================================================


@click.command()
@require_init
def tags():
    """List all tags with message counts.

    \b
    Examples:
      eml tags
    """
    try:
        msgs_path = get_msgs_db_path()
        storage = MessageStorage(msgs_path)
        storage.connect()

        tag_counts = storage.list_tags()

        if not tag_counts:
            echo("No tags found.")
            return

        echo("Tags:\n")
        for tag, count in tag_counts:
            echo(f"  {tag:20} {count:,} messages")

        storage.disconnect()

    except FileNotFoundError as e:
        err(str(e))
        sys.exit(1)


# =============================================================================
# convert
# =============================================================================


@click.command(no_args_is_help=True)
@require_init
@option('-n', '--dry-run', is_flag=True, help="Show what would be converted")
@option('-D', '--delete-old', is_flag=True, help="Delete old storage after conversion")
@argument('target_layout', callback=validate_layout)
def convert(dry_run: bool, delete_old: bool, target_layout: str):
    """Convert between storage layouts.

    \b
    Examples:
      eml convert default             # Convert to default template
      eml convert flat                # Flatten to single directory
      eml convert sqlite              # Pack into SQLite database
      eml convert '$folder/$yyyy/${sha8}.eml'  # Custom template
    """
    root = find_eml_root()
    if not root or not has_config(root):
        err("Convert requires an initialized project. Run 'eml init' first.")
        sys.exit(1)

    config = load_config(root)
    current_layout = config.layout

    if current_layout == target_layout:
        echo(f"Already using {target_layout} layout.")
        return

    echo(f"Converting: {current_layout} → {target_layout}")
    if dry_run:
        echo(style("DRY RUN - no changes will be made", fg="yellow"))
    echo()

    # Get source layout
    source = get_storage_layout(root)

    # Count messages
    messages = list(source.iter_messages())
    echo(f"Messages to convert: {len(messages):,}")

    if not messages:
        echo("No messages to convert.")
        return

    if dry_run:
        echo(f"\nWould convert {len(messages):,} messages to {target_layout}")
        return

    # Create target layout
    if target_layout == "sqlite":
        target = SqliteLayout(root)
        target.connect()
    else:
        target = TreeLayout(root, template=target_layout)

    console = Console()
    converted = 0
    failed = 0

    with Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]Converting"),
        BarColumn(),
        TaskProgressColumn(),
        TextColumn("{task.completed}/{task.total}"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("convert", total=len(messages))

        for msg in messages:
            try:
                target.add_message(
                    message_id=msg.message_id,
                    raw=msg.raw,
                    folder=msg.folder,
                    date=msg.date,
                    from_addr=msg.from_addr,
                    to_addr=msg.to_addr,
                    cc_addr=msg.cc_addr,
                    subject=msg.subject,
                    source_uid=msg.source_uid,
                )
                converted += 1
            except Exception as e:
                failed += 1
                console.print(f"  [red]✗[/] {msg.message_id[:40]}: {e}")

            progress.advance(task)

    # Update config
    config.layout = target_layout
    save_config(config, root)

    echo()
    echo(f"Converted: {converted:,}")
    if failed:
        echo(style(f"Failed: {failed}", fg="red"))

    # Cleanup old storage
    if delete_old and converted > 0:
        echo()
        if current_layout == "sqlite":
            old_db = root / ".eml" / "msgs.db"
            if old_db.exists():
                old_db.unlink()
                echo(f"Deleted: {old_db}")
        else:
            # Delete .eml files (but not .eml/ directory)
            deleted = 0
            for eml_file in root.rglob("*.eml"):
                if ".eml" not in eml_file.parts[:-1]:  # Skip .eml/ dir
                    eml_file.unlink()
                    deleted += 1
                    # Remove empty parent dirs
                    parent = eml_file.parent
                    while parent != root:
                        try:
                            parent.rmdir()
                            parent = parent.parent
                        except OSError:
                            break
            echo(f"Deleted: {deleted:,} .eml files")

    # Cleanup
    if hasattr(source, 'disconnect'):
        source.disconnect()
    if hasattr(target, 'disconnect'):
        target.disconnect()

    echo(f"\nLayout updated to: {target_layout}")


# =============================================================================
# ingest
# =============================================================================


@click.command()
@argument("eml_paths", nargs=-1, type=click.Path(exists=True))
@option('-f', '--folder', default="Inbox", help="Target folder (default: Inbox)")
@option('-M', '--move', is_flag=True, help="Move (delete original) instead of copy")
@option('-N', '--dry-run', is_flag=True, help="Show what would happen without doing it")
def ingest(eml_paths: tuple[str, ...], folder: str, move: bool, dry_run: bool):
    """Import .eml files into the repo with proper naming.

    Parses each .eml file to extract metadata (date, subject, from),
    then generates the proper filename based on the configured layout
    and copies/moves the file to the correct location.
    """
    if not eml_paths:
        err("No .eml files specified")
        sys.exit(1)

    config = load_config()
    root = find_eml_root()
    if config.layout.startswith("sqlite"):
        layout = SqliteLayout(root)
    else:
        layout = TreeLayout(root, template=config.layout)
    root = layout.root

    for eml_path in eml_paths:
        path = Path(eml_path)
        if not path.suffix.lower() == ".eml":
            err(f"Skipping non-.eml file: {path}")
            continue

        # Read and parse the message
        raw = path.read_bytes()
        msg = email.message_from_bytes(raw)

        # Extract metadata
        date_str = msg.get("Date", "")
        date = None
        if date_str:
            try:
                date = parsedate_to_datetime(date_str)
                if date.tzinfo is None:
                    date = date.replace(tzinfo=timezone.utc)
            except Exception:
                pass

        subject = msg.get("Subject", "")
        from_addr = msg.get("From", "")
        message_id = msg.get("Message-ID", "")

        # Check for duplicates
        if message_id and layout.has_message(message_id):
            echo(f"Skipped (duplicate): {path.name}")
            continue

        # Generate output path using the layout
        dest_path = layout.add_message(
            message_id=message_id or f"<ingest-{path.name}>",
            raw=raw,
            folder=folder,
            date=date,
            from_addr=from_addr,
            subject=subject,
        )

        rel_dest = dest_path.relative_to(root) if dest_path.is_relative_to(root) else dest_path

        if dry_run:
            echo(f"Would {'move' if move else 'copy'}: {path} -> {rel_dest}")
            # Clean up the file we just wrote in dry-run mode
            dest_path.unlink()
        else:
            echo(f"{'Moved' if move else 'Copied'}: {path.name} -> {rel_dest}")
            if move:
                path.unlink()
