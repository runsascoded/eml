"""CLI for email migration."""

import imaplib
import os
import re
import sys
import time
from datetime import datetime
from functools import wraps
from pathlib import Path

import click
import humanize
import yaml
from click import argument, echo, option, prompt, style
from dotenv import load_dotenv
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TaskProgressColumn, TimeElapsedColumn, TimeRemainingColumn

from .imap import EmailInfo, FilterConfig, GmailClient, ZohoClient, IMAPClient
from .migrate import EmailMigrator, MigrationConfig
from .storage import (
    Account, AccountStorage, MessageStorage,
    EML_DIR, MSGS_DB, ACCTS_DB, GLOBAL_CONFIG_DIR,
    find_eml_dir, get_eml_dir, get_msgs_db_path, get_account,
)


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


def get_password(password_opt: str | None) -> str:
    """Get password from option, stdin (if piped), or prompt."""
    if password_opt:
        return password_opt
    elif not sys.stdin.isatty():
        return sys.stdin.readline().rstrip("\n")
    else:
        return prompt("Password", hide_input=True)


def require_init(f):
    """Decorator that requires .eml directory to exist."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not find_eml_dir():
            err("Not in an eml project. Run 'eml init' first.")
            sys.exit(1)
        return f(*args, **kwargs)
    return wrapper


# Shared options
tag_option = option('-t', '--tag', help="Tag for organizing messages")


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


class AliasGroup(click.Group):
    """Click Group that supports command aliases."""

    def __init__(self, *args, aliases: dict[str, str] | None = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.aliases = aliases or {}

    def get_command(self, ctx, cmd_name):
        # Check for alias
        cmd_name = self.aliases.get(cmd_name, cmd_name)
        return super().get_command(ctx, cmd_name)

    def resolve_command(self, ctx, args):
        # Resolve alias before dispatching
        _, cmd_name, args = super().resolve_command(ctx, args)
        cmd_name = self.aliases.get(cmd_name, cmd_name)
        return _, cmd_name, args


# Main group with aliases
@click.group(cls=AliasGroup, aliases={
    'a': 'account',
    'f': 'folders',
    'i': 'init',
    'p': 'pull',
    'ps': 'push',
    'st': 'stats',
    's': 'serve',
})
def main():
    """Email migration tools."""
    load_dotenv()


# ============================================================================
# init
# ============================================================================

@main.command()
@option('-g', '--global', 'use_global', is_flag=True, help="Initialize global config (~/.config/eml)")
def init(use_global: bool):
    """Initialize eml project directory.

    \b
    Examples:
      eml init              # Create .eml/ in current directory
      eml init -g           # Create ~/.config/eml/ for global accounts
    """
    if use_global:
        target = GLOBAL_CONFIG_DIR
        target.mkdir(parents=True, exist_ok=True)
        accts_path = target / ACCTS_DB
        with AccountStorage(accts_path) as storage:
            pass  # Just create schema
        echo(f"Initialized global config: {target}")
    else:
        eml_dir = Path.cwd() / EML_DIR
        if eml_dir.exists():
            echo(f"Already initialized: {eml_dir}")
            return
        eml_dir.mkdir(parents=True)
        # Create empty databases with schema
        with MessageStorage(eml_dir / MSGS_DB) as storage:
            pass
        with AccountStorage(eml_dir / ACCTS_DB) as storage:
            pass
        echo(f"Initialized: {eml_dir}")
        echo(f"  {MSGS_DB}   - message storage")
        echo(f"  {ACCTS_DB}  - account credentials")
        echo()
        echo("Next steps:")
        echo("  eml account add gmail user@gmail.com")
        echo("  eml pull gmail -f INBOX")


# ============================================================================
# account (with aliases)
# ============================================================================

@main.group(cls=AliasGroup, aliases={
    'a': 'add',
    'l': 'ls',
    'r': 'rm',
})
def account():
    """Manage IMAP accounts."""
    pass


@account.command("add")
@option('-g', '--global', 'use_global', is_flag=True, help="Add to global config")
@option('-p', '--password', 'password_opt', help="Password (prompts if not provided)")
@option('-t', '--type', 'acct_type', help="Account type (gmail, zoho, or hostname)")
@argument('name')
@argument('user')
def account_add(use_global: bool, password_opt: str | None, acct_type: str | None, name: str, user: str):
    """Add or update an account.

    \b
    Examples:
      eml account add gmail user@gmail.com
      eml a a gmail user@gmail.com              # using aliases
      echo "$PASS" | eml account add zoho user@example.com
      eml account add gmail user@gmail.com -g   # global account
    """
    password = get_password(password_opt)

    # Infer type from name if not specified
    if not acct_type:
        if "gmail" in name.lower():
            acct_type = "gmail"
        elif "zoho" in name.lower():
            acct_type = "zoho"
        else:
            err(f"Cannot infer account type from '{name}'. Use -t to specify.")
            sys.exit(1)

    # Determine where to store
    if use_global:
        accts_path = GLOBAL_CONFIG_DIR / ACCTS_DB
        GLOBAL_CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        location = "global"
    else:
        eml_dir = find_eml_dir()
        if not eml_dir:
            err("Not in an eml project. Run 'eml init' first, or use -g for global.")
            sys.exit(1)
        accts_path = eml_dir / ACCTS_DB
        location = "local"

    with AccountStorage(accts_path) as storage:
        storage.add(name, acct_type, user, password)

    echo(f"Account '{name}' saved ({acct_type}: {user}) [{location}]")


@account.command("ls")
@option('-a', '--all', 'show_all', is_flag=True, help="Show both local and global accounts")
@option('-g', '--global', 'use_global', is_flag=True, help="Show global accounts only")
def account_ls(show_all: bool, use_global: bool):
    """List accounts.

    \b
    Examples:
      eml account ls        # local accounts (with global fallback info)
      eml a l               # using aliases
      eml account ls -g     # global accounts only
      eml account ls -a     # both local and global
    """
    eml_dir = find_eml_dir()
    local_accts_path = eml_dir / ACCTS_DB if eml_dir else None
    global_accts_path = GLOBAL_CONFIG_DIR / ACCTS_DB

    accounts_found = False

    # Local accounts
    if not use_global and local_accts_path and local_accts_path.exists():
        with AccountStorage(local_accts_path) as storage:
            accounts = storage.list()
        if accounts:
            accounts_found = True
            echo(f"Local accounts ({local_accts_path}):\n")
            for acct in accounts:
                echo(f"  {acct.name:15} {acct.type:10} {acct.user}")
            echo()

    # Global accounts
    if (use_global or show_all or not accounts_found) and global_accts_path.exists():
        with AccountStorage(global_accts_path) as storage:
            accounts = storage.list()
        if accounts:
            accounts_found = True
            echo(f"Global accounts ({global_accts_path}):\n")
            for acct in accounts:
                echo(f"  {acct.name:15} {acct.type:10} {acct.user}")
            echo()

    if not accounts_found:
        echo("No accounts configured.")
        echo("  eml account add gmail user@gmail.com")


@account.command("rm")
@option('-g', '--global', 'use_global', is_flag=True, help="Remove from global config")
@argument('name')
def account_rm(use_global: bool, name: str):
    """Remove an account.

    \b
    Examples:
      eml account rm gmail
      eml a r gmail           # using aliases
    """
    if use_global:
        accts_path = GLOBAL_CONFIG_DIR / ACCTS_DB
    else:
        eml_dir = find_eml_dir()
        if not eml_dir:
            err("Not in an eml project. Run 'eml init' first, or use -g for global.")
            sys.exit(1)
        accts_path = eml_dir / ACCTS_DB

    if not accts_path.exists():
        err(f"No accounts database: {accts_path}")
        sys.exit(1)

    with AccountStorage(accts_path) as storage:
        removed = storage.remove(name)

    if removed:
        echo(f"Account '{name}' removed.")
    else:
        err(f"Account '{name}' not found.")
        sys.exit(1)


# ============================================================================
# folders
# ============================================================================

@main.command()
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
        acct = get_account(account_or_folder)
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


# ============================================================================
# pull
# ============================================================================

@main.command()
@require_init
@option('-b', '--batch', 'checkpoint_interval', default=100, help="Save progress every N messages")
@option('-f', '--folder', type=str, help="Source folder")
@option('-l', '--limit', type=int, help="Max emails to fetch")
@option('-n', '--dry-run', is_flag=True, help="Show what would be fetched")
@option('-p', '--password', help="IMAP password (overrides account)")
@tag_option
@option('-u', '--user', help="IMAP username (overrides account)")
@option('-v', '--verbose', is_flag=True, help="Show each message")
@argument('account')
def pull(
    checkpoint_interval: int,
    folder: str | None,
    limit: int | None,
    dry_run: bool,
    password: str | None,
    tag: str | None,
    user: str | None,
    verbose: bool,
    account: str,
):
    """Pull emails from IMAP to local storage.

    \b
    Examples:
      eml pull gmail                      # Pull from Gmail All Mail
      eml pull gmail -f "Work" -t work    # Pull Work label, tag as 'work'
      eml p gmail -f INBOX -l 100         # Pull first 100 from INBOX
      eml pull gmail -n                   # Dry run
    """
    # Look up account
    acct = get_account(account)
    if not acct:
        err(f"Account '{account}' not found.")
        err("  eml account add gmail user@gmail.com")
        sys.exit(1)

    src_type = acct.type
    src_user = user or acct.user
    src_password = password or acct.password

    # Create IMAP client
    client = get_imap_client(src_type)
    src_folder = folder or (client.all_mail_folder if hasattr(client, 'all_mail_folder') else "INBOX")

    echo(f"Source: {src_type} ({src_user})")
    echo(f"Folder: {src_folder}")
    if tag:
        echo(f"Tag: {tag}")
    if dry_run:
        echo(style("DRY RUN - no changes will be made", fg="yellow"))
    echo()

    try:
        client.connect(src_user, src_password)
        count, uidvalidity = client.select_folder(src_folder, readonly=True)
        echo(f"Folder has {count:,} messages (UIDVALIDITY: {uidvalidity})")

        # Open storage
        msgs_path = get_msgs_db_path()
        storage = MessageStorage(msgs_path)
        if not dry_run:
            storage.connect()

        # Check sync state
        stored_uidvalidity, last_uid = (None, None)
        if not dry_run:
            stored_uidvalidity, last_uid = storage.get_sync_state(src_type, src_user, src_folder)

        if stored_uidvalidity and stored_uidvalidity != uidvalidity:
            echo(style(f"UIDVALIDITY changed ({stored_uidvalidity} → {uidvalidity}), doing full sync", fg="yellow"))
            if not dry_run:
                storage.clear_sync_state(src_type, src_user, src_folder)
            last_uid = None

        # Get UIDs to fetch
        if last_uid:
            echo(f"Incremental sync from UID {last_uid}")
            uids = client.search_uids_after(last_uid)
        else:
            echo("Full sync")
            uids = client.search("ALL")

        if limit:
            uids = uids[:limit]

        echo(f"Fetching {len(uids)} messages...")
        echo()

        fetched = 0
        skipped = 0
        failed = 0
        max_uid = last_uid or 0
        total = len(uids)
        console = Console()

        def save_checkpoint():
            if not dry_run and max_uid > 0:
                storage.set_sync_state(src_type, src_user, src_folder, uidvalidity, max_uid)

        def print_result(status: str, subj: str, detail: str | None = None):
            """Print a result line (scrollable) above the progress bar."""
            if status == "ok":
                console.print(f"  [green]✓[/] {subj}")
            elif status == "dry":
                console.print(f"  [dim]○ {subj}[/]")
            elif status == "skip":
                console.print(f"  [dim]· {subj}[/]")
            else:
                msg = f"  [red]✗[/] {subj}"
                if verbose and detail:
                    msg += f" [dim red]: {detail[:60]}[/]"
                console.print(msg)

        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]Pulling"),
            BarColumn(),
            TaskProgressColumn(),
            TextColumn("{task.completed}/{task.total}"),
            TimeElapsedColumn(),
            TextColumn("ETA"),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("pull", total=total)

            for i, uid in enumerate(uids):
                uid_int = int(uid)
                max_uid = max(max_uid, uid_int)

                try:
                    info = client.fetch_info(uid)
                except Exception as e:
                    failed += 1
                    if verbose:
                        print_result("fail", f"UID {uid}", str(e))
                    progress.advance(task)
                    continue

                subj = (info.subject or "(no subject)")[:60]

                # Check if already stored
                if not dry_run and storage.has_message(info.message_id):
                    skipped += 1
                    if verbose:
                        print_result("skip", subj)
                    progress.advance(task)
                    continue

                if dry_run:
                    if verbose:
                        print_result("dry", subj)
                    fetched += 1
                    progress.advance(task)
                    continue

                # Fetch full message and store
                try:
                    raw = client.fetch_raw(uid)
                    storage.add_message(
                        message_id=info.message_id,
                        raw=raw,
                        date=info.date,
                        from_addr=info.from_addr,
                        to_addr=info.to_addr,
                        cc_addr=info.cc_addr,
                        subject=info.subject,
                        source_folder=src_folder,
                        source_uid=str(uid_int),
                        tags=[tag] if tag else None,
                    )
                    fetched += 1
                    if verbose:
                        print_result("ok", subj)
                except Exception as e:
                    failed += 1
                    if verbose:
                        print_result("fail", subj, str(e))

                progress.advance(task)

                # Save checkpoint periodically
                if (i + 1) % checkpoint_interval == 0:
                    save_checkpoint()

        # Final sync state update
        save_checkpoint()

        echo()
        if dry_run:
            echo(f"Would fetch: {fetched}")
        else:
            echo(f"Fetched: {fetched}")
            if skipped:
                echo(f"Skipped (duplicate): {skipped}")
            echo(f"Total in storage: {storage.count():,}")
        if failed:
            echo(style(f"Failed: {failed}", fg="red"))

        if not dry_run:
            storage.disconnect()

    except Exception as e:
        err(f"Error: {e}")
        sys.exit(1)
    finally:
        client.disconnect()


# ============================================================================
# push
# ============================================================================

@main.command()
@require_init
@option('-b', '--batch', 'checkpoint_interval', default=100, help="Mark progress every N messages")
@option('-d', '--delay', type=float, default=0, help="Delay between messages (seconds)")
@option('-e', '--max-errors', default=10, help="Abort after N consecutive errors")
@option('-f', '--folder', default="INBOX", help="Destination folder")
@option('-l', '--limit', type=int, help="Max emails to push")
@option('-n', '--dry-run', is_flag=True, help="Show what would be pushed")
@option('-p', '--password', help="IMAP password (overrides account)")
@option('-S', '--max-size', type=int, default=25, help="Skip messages larger than N MB")
@tag_option
@option('-u', '--user', help="IMAP username (overrides account)")
@option('-v', '--verbose', is_flag=True, help="Show each message")
@argument('account')
def push(
    checkpoint_interval: int,
    delay: float,
    max_errors: int,
    folder: str,
    limit: int | None,
    dry_run: bool,
    max_size: int,
    password: str | None,
    tag: str | None,
    user: str | None,
    verbose: bool,
    account: str,
):
    """Push emails from local storage to IMAP destination.

    \b
    Examples:
      eml push zoho                       # Push all to Zoho INBOX
      eml push zoho -t work -f Work       # Push 'work' tagged to Work folder
      eml ps zoho -n                      # Dry run
      eml push zoho -l 10 -v              # Push 10, verbose
    """
    # Look up account
    acct = get_account(account)
    if not acct:
        err(f"Account '{account}' not found.")
        err("  eml account add zoho user@example.com")
        sys.exit(1)

    dst_type = acct.type
    dst_user = user or acct.user
    dst_password = password or acct.password
    dst_folder = folder

    echo(f"Destination: {dst_type} ({dst_user})")
    echo(f"Folder: {dst_folder}")
    if tag:
        echo(f"Tag filter: {tag}")
    if dry_run:
        echo(style("DRY RUN - no changes will be made", fg="yellow"))
    echo()

    client = None
    try:
        # Open storage
        msgs_path = get_msgs_db_path()
        storage = MessageStorage(msgs_path)
        storage.connect()

        total = storage.count(tag=tag)
        already_pushed = storage.count_pushed(dst_type, dst_user, dst_folder)
        echo(f"Messages in storage{f' (tag: {tag})' if tag else ''}: {total:,}")
        echo(f"Already pushed to destination: {already_pushed:,}")

        # Get unpushed messages
        unpushed = list(storage.iter_unpushed(dst_type, dst_user, dst_folder, tag=tag))
        if limit:
            unpushed = unpushed[:limit]

        echo(f"To push: {len(unpushed):,}")
        echo()

        if not unpushed:
            echo("Nothing to push.")
            return

        client = get_imap_client(dst_type)
        if not dry_run:
            client.connect(dst_user, dst_password)
            if hasattr(client, 'create_folder'):
                client.create_folder(dst_folder)

        pushed = 0
        failed = 0
        skipped = 0
        consecutive_errors = 0
        aborted = False
        errors = []  # collect errors for reporting
        total = len(unpushed)
        max_size_bytes = max_size * 1024 * 1024
        console = Console()

        def print_result(status: str, subj: str, detail: str | None = None):
            """Print a result line (scrollable) above the progress bar."""
            if status == "ok":
                console.print(f"  [green]✓[/] {subj}")
            elif status == "dry":
                console.print(f"  [dim]○ {subj}[/]")
            elif status == "skip":
                msg = f"  [yellow]⊘[/] {subj}"
                if detail:
                    msg += f" [dim yellow]({detail})[/]"
                console.print(msg)
            else:
                msg = f"  [red]✗[/] {subj}"
                if verbose and detail:
                    msg += f" [dim red]: {detail[:60]}[/]"
                console.print(msg)

        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]Pushing"),
            BarColumn(),
            TaskProgressColumn(),
            TextColumn("{task.completed}/{task.total}"),
            TimeElapsedColumn(),
            TextColumn("ETA"),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("push", total=total)

            for msg in unpushed:
                subj = (msg.subject or "(no subject)")[:60]
                msg_size = len(msg.raw)

                # Skip oversized messages
                if msg_size > max_size_bytes:
                    size_mb = msg_size / 1024 / 1024
                    skipped += 1
                    if verbose:
                        print_result("skip", subj, f"{size_mb:.1f}MB > {max_size}MB")
                    progress.advance(task)
                    continue

                if dry_run:
                    if verbose:
                        print_result("dry", subj)
                    pushed += 1
                else:
                    try:
                        success = client.conn.append(
                            dst_folder,
                            None,
                            imaplib.Time2Internaldate(msg.date.timestamp()) if msg.date else None,
                            msg.raw,
                        )
                        if success[0] == "OK":
                            storage.mark_pushed(msg.message_id, dst_type, dst_user, dst_folder)
                            pushed += 1
                            consecutive_errors = 0  # reset on success
                            if verbose:
                                print_result("ok", subj)
                        else:
                            failed += 1
                            consecutive_errors += 1
                            err_msg = f"IMAP returned: {success}"
                            errors.append((msg, err_msg))
                            if verbose:
                                print_result("fail", subj, err_msg)
                    except Exception as e:
                        failed += 1
                        consecutive_errors += 1
                        errors.append((msg, str(e)))
                        if verbose:
                            print_result("fail", subj, str(e))

                progress.advance(task)

                # Delay between requests (for rate limiting)
                if delay > 0 and not dry_run:
                    time.sleep(delay)

                # Abort after too many consecutive errors
                if consecutive_errors >= max_errors:
                    console.print(f"\n[bold red]Aborting: {consecutive_errors} consecutive errors (likely rate limited)[/]")
                    aborted = True
                    break

        # Final summary
        echo()
        if dry_run:
            echo(f"Would push: {pushed}")
            if skipped:
                echo(style(f"Would skip: {skipped} (over {max_size}MB)", fg="yellow"))
        else:
            echo(f"Pushed: {pushed}")
            if skipped:
                echo(style(f"Skipped: {skipped} (over {max_size}MB)", fg="yellow"))
            if failed:
                echo(style(f"Failed: {failed}", fg="red"))
                if not verbose and errors:
                    echo("\nErrors:")
                    for msg, error in errors[:5]:
                        subj = (msg.subject or "(no subject)")[:40]
                        echo(f"  {subj}: {error}")

        storage.disconnect()

    except Exception as e:
        err(f"Error: {e}")
        sys.exit(1)
    finally:
        if client and client._conn:
            client.disconnect()


# ============================================================================
# ls
# ============================================================================

@main.command()
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


# ============================================================================
# tags
# ============================================================================

@main.command()
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


# ============================================================================
# stats
# ============================================================================

@main.command()
@require_init
def stats():
    """Show aggregate statistics about stored messages.

    \b
    Examples:
      eml stats
    """
    from rich.table import Table

    try:
        msgs_path = get_msgs_db_path()
        storage = MessageStorage(msgs_path)
        storage.connect()
        console = Console()

        # Basic counts
        total = storage.count()
        if total == 0:
            echo("No messages in storage.")
            return

        # Run aggregate queries
        cursor = storage.conn.execute("""
            SELECT
                COUNT(*) as count,
                SUM(length(raw)) as total_bytes,
                MIN(date) as oldest,
                MAX(date) as newest,
                AVG(length(raw)) as avg_size
            FROM messages
        """)
        row = cursor.fetchone()
        total_bytes = row["total_bytes"] or 0
        oldest = row["oldest"]
        newest = row["newest"]
        avg_size = row["avg_size"] or 0

        # Size distribution
        size_dist = storage.conn.execute("""
            SELECT
                CASE
                    WHEN length(raw) > 30*1024*1024 THEN '>30MB'
                    WHEN length(raw) > 25*1024*1024 THEN '25-30MB'
                    WHEN length(raw) > 20*1024*1024 THEN '20-25MB'
                    WHEN length(raw) > 15*1024*1024 THEN '15-20MB'
                    WHEN length(raw) > 10*1024*1024 THEN '10-15MB'
                    WHEN length(raw) > 5*1024*1024 THEN '5-10MB'
                    WHEN length(raw) > 1*1024*1024 THEN '1-5MB'
                    WHEN length(raw) > 100*1024 THEN '100KB-1MB'
                    ELSE '<100KB'
                END as size_range,
                COUNT(*) as count,
                SUM(length(raw)) as total_bytes
            FROM messages
            GROUP BY 1
            ORDER BY MAX(length(raw)) DESC
        """).fetchall()

        # Tag counts
        tag_counts = storage.list_tags()

        # Push state (get unique destinations)
        push_stats = storage.conn.execute("""
            SELECT dest_type, dest_user, dest_folder, COUNT(*) as count
            FROM push_state
            GROUP BY dest_type, dest_user, dest_folder
        """).fetchall()

        # Display
        console.print()
        console.print(f"[bold]Messages:[/] {total:,}")
        console.print(f"[bold]Total size:[/] {humanize.naturalsize(total_bytes)}")
        console.print(f"[bold]Avg size:[/] {humanize.naturalsize(avg_size)}")
        if oldest:
            console.print(f"[bold]Date range:[/] {oldest[:10]} → {newest[:10]}")
        console.print()

        # Size distribution table
        table = Table(title="Size Distribution")
        table.add_column("Size", style="cyan")
        table.add_column("Count", justify="right")
        table.add_column("Total", justify="right")
        table.add_column("%", justify="right")

        for row in size_dist:
            pct = (row["count"] / total) * 100
            table.add_row(
                row["size_range"],
                f"{row['count']:,}",
                humanize.naturalsize(row["total_bytes"]),
                f"{pct:.1f}%",
            )
        console.print(table)

        # Tags
        if tag_counts:
            console.print()
            tag_table = Table(title="Tags")
            tag_table.add_column("Tag", style="cyan")
            tag_table.add_column("Count", justify="right")
            for tag, count in tag_counts:
                tag_table.add_row(tag, f"{count:,}")
            console.print(tag_table)

        # Push destinations
        if push_stats:
            console.print()
            push_table = Table(title="Pushed To")
            push_table.add_column("Destination", style="cyan")
            push_table.add_column("Folder")
            push_table.add_column("Count", justify="right")
            for row in push_stats:
                push_table.add_row(
                    f"{row['dest_type']} ({row['dest_user']})",
                    row["dest_folder"],
                    f"{row['count']:,}",
                )
            console.print(push_table)

        storage.disconnect()

    except FileNotFoundError as e:
        err(str(e))
        sys.exit(1)


# ============================================================================
# serve
# ============================================================================

@main.command()
@require_init
@option('-h', '--host', default="127.0.0.1", help="Host to bind to")
@option('-p', '--port', default=5000, help="Port to run on")
def serve(host: str, port: int):
    """Start pmail web UI for browsing emails.

    \b
    Examples:
      eml serve                           # Start on http://127.0.0.1:5000
      eml s -p 8080                       # Use different port
    """
    msgs_path = get_msgs_db_path()

    www_path = Path(__file__).parent.parent.parent / "www"
    sys.path.insert(0, str(www_path))

    try:
        from app import app
        import app as app_module
        app_module.DB_PATH = msgs_path.absolute()

        echo(f"Starting pmail on http://{host}:{port}")
        echo(f"Database: {msgs_path}")
        app.run(host=host, port=port, debug=True)
    except ImportError as e:
        err(f"Failed to import pmail app: {e}")
        err("Make sure www/app.py exists")
        sys.exit(1)


# ============================================================================
# migrate (legacy)
# ============================================================================

@main.command()
@option('-a', '--address', 'addresses', multiple=True, help="Match To/From/Cc address")
@option('-c', '--config', 'config_file', type=str, help="YAML config file")
@option('-d', '--from-domain', 'from_domains', multiple=True, help="Match From domain only")
@option('-D', '--domain', 'domains', multiple=True, help="Match To/From/Cc domain")
@option('-e', '--end-date', type=str, help="End date (YYYY-MM-DD)")
@option('-f', '--folder', type=str, help="Destination folder")
@option('-F', '--from-address', 'from_addresses', multiple=True, help="Match From address only")
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
    """Migrate emails between IMAP mailboxes (legacy direct mode).

    \b
    This is the original direct IMAP-to-IMAP migration.
    For the new workflow, use: eml pull, eml push

    \b
    Requires environment variables:
      GMAIL_USER, GMAIL_APP_PASSWORD
      ZOHO_USER, ZOHO_PASSWORD
    """
    # Load config file if provided
    cfg: dict = {}
    if config_file:
        if not Path(config_file).exists():
            err(f"Config file not found: {config_file}")
            sys.exit(1)
        cfg = load_config_file(config_file)

    # Build filters
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
        err("Error: At least one filter required")
        sys.exit(1)

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
        sys.exit(1)

    def parse_date(val) -> datetime | None:
        if val is None:
            return None
        if isinstance(val, datetime):
            return val
        if hasattr(val, 'isoformat'):
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
        echo(f"  Addresses: {', '.join(all_addresses)}")
    if all_domains:
        echo(f"  Domains: {', '.join(all_domains)}")
    if all_from_addresses:
        echo(f"  From addresses: {', '.join(all_from_addresses)}")
    if all_from_domains:
        echo(f"  From domains: {', '.join(all_from_domains)}")
    if parsed_start or parsed_end:
        echo(f"  Date range: {format_date(parsed_start)} to {format_date(parsed_end)}")
    if dry_run:
        echo(style("DRY RUN", fg="yellow"))
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
