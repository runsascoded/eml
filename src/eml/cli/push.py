"""Push command for uploading emails to IMAP."""

import atexit
import imaplib
import sys
import time

import click
from click import argument, echo, option, style
from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from ..config import AccountConfig, find_eml_root, load_config, load_pushed, mark_pushed
from ..imap import IMAPClient
from ..storage import MessageStorage, get_msgs_db_path

from .utils import (
    clear_sync_status,
    err,
    get_account_any,
    get_imap_client,
    get_storage_layout,
    has_config,
    log_pushed_message,
    read_sync_status,
    require_init,
    tag_option,
    update_sync_progress,
    write_sync_status,
)


@click.command(no_args_is_help=True)
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
      eml push g/user -f INBOX            # Push to destination account
      eml ps zoho -n                      # Dry run
      eml push zoho -l 10 -v              # Push 10, verbose
    """
    # Look up account
    acct = get_account_any(account)
    if not acct:
        err(f"Account '{account}' not found.")
        err("  eml account add g/user gmail user@example.com")
        sys.exit(1)

    dst_type = acct.type
    dst_user = user or acct.user
    dst_password = password or acct.password
    dst_folder = folder

    # Check for config
    root = find_eml_root()
    has_cfg = root and has_config(root)

    echo(f"Destination: {dst_type} ({dst_user})")
    echo(f"Folder: {dst_folder}")
    if has_cfg:
        echo(f"Layout: {load_config(root).layout}")
    if dry_run:
        echo(style("DRY RUN - no changes will be made", fg="yellow"))
    echo()

    client = None
    layout = None
    storage = None
    try:
        if has_cfg:
            #  use layout and pushed/<account>.txt
            layout = get_storage_layout(root)
            pushed_set = load_pushed(account, root)

            # Get all messages and filter out already pushed
            all_msgs = list(layout.iter_messages())
            total_count = len(all_msgs)
            already_pushed_count = len(pushed_set)
            unpushed = [m for m in all_msgs if m.message_id not in pushed_set]
        else:
            # Legacy:  use SQL storage
            msgs_path = get_msgs_db_path()
            storage = MessageStorage(msgs_path)
            storage.connect()
            total_count = storage.count(tag=tag)
            already_pushed_count = storage.count_pushed(dst_type, dst_user, dst_folder)
            unpushed = list(storage.iter_unpushed(dst_type, dst_user, dst_folder, tag=tag))

        if limit:
            unpushed = unpushed[:limit]

        echo(f"Messages in storage: {total_count:,}")
        echo(f"Already pushed to destination: {already_pushed_count:,}")
        echo(f"To push: {len(unpushed):,}")
        echo()

        if not unpushed:
            echo("Nothing to push.")
            return

        # Create IMAP client
        if has_cfg and isinstance(acct, AccountConfig) and acct.host:
            client = IMAPClient(acct.host, acct.port)
        else:
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
        errors = []
        total = len(unpushed)
        max_size_bytes = max_size * 1024 * 1024
        console = Console()

        # Check if another sync is already running in this worktree
        if has_cfg and not dry_run:
            existing = read_sync_status(root)
            if existing:
                pid = existing.get("pid")
                op = existing.get("operation", "sync")
                acct_name = existing.get("account", "?")
                fldr = existing.get("folder", "?")
                err(f"Another {op} is already running: {acct_name}/{fldr} [PID {pid}]")
                err(f"Wait for it to finish or kill it with: kill {pid}")
                sys.exit(1)

        # Write push status file (for `eml status` to read)
        if has_cfg and not dry_run:
            write_sync_status("push", account, dst_folder, total, 0, root)
            atexit.register(clear_sync_status, root)

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
                            if has_cfg:
                                mark_pushed(account, msg.message_id, root)
                                # Log for "Last 10 uploaded" feature
                                log_pushed_message(account, msg.message_id, str(msg.path) if hasattr(msg, 'path') else None, msg.subject, root)
                            else:
                                storage.mark_pushed(msg.message_id, dst_type, dst_user, dst_folder)
                            pushed += 1
                            consecutive_errors = 0
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

                # Update sync status for real-time progress
                if has_cfg and not dry_run:
                    update_sync_progress(
                        completed=pushed + failed + skipped,
                        skipped=skipped,
                        failed=failed,
                        current_subject=subj,
                        root=root,
                    )

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

        # Clear push status file (we're done)
        if has_cfg and not dry_run:
            clear_sync_status(root)

        # Cleanup
        if has_cfg and layout and hasattr(layout, 'disconnect'):
            layout.disconnect()
        elif not has_cfg and storage:
            storage.disconnect()

    except Exception as e:
        err(f"Error: {e}")
        sys.exit(1)
    finally:
        if client and client._conn:
            client.disconnect()
