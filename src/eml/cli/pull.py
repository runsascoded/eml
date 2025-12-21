"""Pull command for fetching emails from IMAP."""

import atexit
import sys
from datetime import datetime

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

from ..config import AccountConfig, PullFailure, find_eml_root, load_config, load_failures, save_failures, get_failures_path
from ..imap import IMAPClient
from ..index import FileIndex
from ..layouts.path_template import content_hash
from ..parsing import extract_body_text
from ..pulls import PullsDB, get_pulls_db
from ..storage import MessageStorage, get_msgs_db_path

from .utils import (
    clear_sync_status,
    err,
    get_account_any,
    get_imap_client,
    get_storage_layout,
    has_config,
    read_sync_status,
    require_init,
    tag_option,
    update_sync_progress,
    write_sync_status,
)


@click.command(no_args_is_help=True)
@require_init
@option('-b', '--batch', 'checkpoint_interval', default=100, help="Save progress every N messages")
@option('-e', '--max-errors', default=10, help="Abort after N consecutive errors (rate limit detection)")
@option('-f', '--folder', type=str, help="Source folder")
@option('-F', '--full', is_flag=True, help="Ignore sync-state, fetch all messages")
@option('-l', '--limit', type=int, help="Max emails to fetch")
@option('-n', '--dry-run', is_flag=True, help="Show what would be fetched")
@option('-p', '--password', help="IMAP password (overrides account)")
@option('-r', '--retry', is_flag=True, help="Retry previously failed UIDs only")
@tag_option
@option('-T', '--cache-ttl', type=int, default=60, help="UID cache TTL in minutes (default 60, 0 = always refresh)")
@option('-u', '--user', help="IMAP username (overrides account)")
@option('-v', '--verbose', is_flag=True, help="Show each message")
@argument('account')
def pull(
    cache_ttl: int,
    checkpoint_interval: int,
    max_errors: int,
    folder: str | None,
    full: bool,
    limit: int | None,
    dry_run: bool,
    password: str | None,
    retry: bool,
    tag: str | None,
    user: str | None,
    verbose: bool,
    account: str,
):
    """Pull emails from IMAP to local storage.

    \b
    Examples:
      eml pull gmail                      # Pull from Gmail All Mail
      eml pull y/user -f INBOX -l 100     # Pull first 100 from INBOX
      eml pull gmail -n                   # Dry run
    """
    # Look up account
    acct = get_account_any(account)
    if not acct:
        err(f"Account '{account}' not found.")
        err("  eml account add g/user gmail user@gmail.com")
        sys.exit(1)

    src_type = acct.type
    src_user = user or acct.user
    src_password = password or acct.password

    # Check for config
    root = find_eml_root()
    has_cfg = root and has_config(root)

    # Check if another pull is already running in this worktree
    if has_cfg and not dry_run:
        existing = read_sync_status(root)
        if existing:
            pid = existing.get("pid")
            acct_name = existing.get("account", "?")
            fldr = existing.get("folder", "?")
            err(f"Another pull is already running: {acct_name}/{fldr} [PID {pid}]")
            err(f"Wait for it to finish or kill it with: kill {pid}")
            sys.exit(1)

    # Create IMAP client
    if has_cfg and isinstance(acct, AccountConfig) and acct.host:
        client = IMAPClient(acct.host, acct.port)
    else:
        client = get_imap_client(src_type)
    src_folder = folder or (client.all_mail_folder if hasattr(client, 'all_mail_folder') else "INBOX")

    echo(f"Source: {src_type} ({src_user})")
    echo(f"Folder: {src_folder}")
    if has_cfg:
        echo(f"Layout: {load_config(root).layout}")
    if dry_run:
        echo(style("DRY RUN - no changes will be made", fg="yellow"))
    echo()

    try:
        client.connect(src_user, src_password)
        count, uidvalidity = client.select_folder(src_folder, readonly=True)
        echo(f"Folder has {count:,} messages (UIDVALIDITY: {uidvalidity})")

        # Open storage (V1 vs V2)
        layout = None
        storage = None
        file_index: FileIndex | None = None
        if has_cfg:
            layout = get_storage_layout(root) if not dry_run else None
            # Open FileIndex for incremental indexing of new messages
            if not dry_run:
                eml_dir = root / ".eml"
                file_index = FileIndex(eml_dir)
        else:
            msgs_path = get_msgs_db_path()
            storage = MessageStorage(msgs_path)
            if not dry_run:
                storage.connect()

        # Open pulls.db for tracking (Git-tracked, per-UID records)
        pulls_db: PullsDB | None = None
        pulled_uids: set[int] = set()
        stored_uidvalidity: int | None = None
        sync_run_id: int | None = None

        if has_cfg and not dry_run:
            pulls_db = get_pulls_db(root)
            pulls_db.connect()
            # Check for UIDVALIDITY change
            stored_uidvalidity = pulls_db.get_uidvalidity(account, src_folder)
            if stored_uidvalidity and stored_uidvalidity != uidvalidity:
                echo(style(f"UIDVALIDITY changed ({stored_uidvalidity} → {uidvalidity})", fg="yellow"))
                echo("  Previous pull records are invalid (UIDs reassigned by server)")
                # Note: We keep the old records for reference but they won't match
                # Could optionally clear them: pulls_db.clear_folder(account, src_folder, stored_uidvalidity)
            # Get UIDs we've already pulled for this UIDVALIDITY
            pulled_uids = pulls_db.get_pulled_uids(account, src_folder, uidvalidity)
            if pulled_uids:
                echo(f"Already pulled: {len(pulled_uids):,} UIDs (from pulls.db)")

        # Load previous failures for this account/folder
        failures = {}
        if has_cfg and not dry_run:
            failures = load_failures(account, src_folder, root)

        # Get UIDs to fetch - use cached server_uids if available and fresh
        cached_server_uids: set[int] = set()
        cache_is_fresh = False
        if pulls_db and uidvalidity:
            cached_server_uids = pulls_db.get_server_uids(account, src_folder, uidvalidity)
            if cached_server_uids and cache_ttl > 0:
                # Check if cache is fresh based on TTL
                folder_info = pulls_db.get_server_folder_info(account, src_folder)
                if folder_info:
                    _, _, last_checked_str = folder_info
                    last_checked = datetime.fromisoformat(last_checked_str)
                    cache_age_mins = (datetime.now() - last_checked).total_seconds() / 60
                    cache_is_fresh = cache_age_mins < cache_ttl
                    if not cache_is_fresh:
                        echo(f"UID cache expired ({cache_age_mins:.0f}m > {cache_ttl}m TTL)")

        # Determine which UIDs to fetch
        if retry:
            if not failures:
                echo(style("No failures to retry", fg="yellow"))
                if pulls_db:
                    pulls_db.disconnect()
                return
            # Convert int UIDs to bytes (as returned by IMAP search)
            uids = [str(uid).encode() for uid in sorted(failures.keys())]
            echo(f"Retrying {len(uids)} failed UIDs")
        elif cached_server_uids and cache_is_fresh and not full:
            # Use cached UIDs - much faster than IMAP SEARCH
            echo(f"Using cached server UIDs: {len(cached_server_uids):,}")
            unpulled = cached_server_uids - pulled_uids
            uids = [str(uid).encode() for uid in sorted(unpulled)]
            echo(f"Unpulled: {len(uids):,} UIDs")
        else:
            # No cache, stale cache, or --full: fetch from server
            echo("Fetching UID list from server...")
            all_server_uids = client.search("ALL")
            echo(f"Server has {len(all_server_uids):,} messages")

            # Cache the UIDs for next time
            if pulls_db and uidvalidity and not dry_run:
                uid_list = [(int(u), None) for u in all_server_uids]
                pulls_db.record_server_uids(account, src_folder, uidvalidity, uid_list)
                pulls_db.record_server_folder(account, src_folder, uidvalidity, len(all_server_uids))
                echo(f"Cached {len(all_server_uids):,} UIDs (TTL: {cache_ttl}m)")

            if full:
                echo("Full sync (--full) - will check all UIDs")
                uids = all_server_uids
            else:
                # Normal sync: fetch UIDs we haven't pulled yet
                uids = [u for u in all_server_uids if int(u) not in pulled_uids]
                if len(uids) < len(all_server_uids):
                    echo(f"Incremental sync: {len(uids):,} new UIDs to check")
                else:
                    echo("Full sync (no prior pulls)")

        if limit:
            uids = uids[:limit]

        total_candidates = len(uids)
        echo(f"Found {total_candidates} candidate messages")
        echo()

        fetched = 0
        skipped = 0
        failed = 0
        consecutive_errors = 0
        aborted = False
        total_for_loop = len(uids)
        console = Console()

        # Start sync run record
        if pulls_db and not dry_run:
            sync_run_id = pulls_db.start_sync_run(
                operation="pull",
                account=account,
                folder=src_folder,
                total=total_for_loop,
            )

        # Write pull status file (for `eml status` to read)
        if has_cfg and not dry_run:
            write_sync_status("pull", account, src_folder, total_for_loop, 0, root)
            # Register cleanup on exit (normal or abnormal)
            atexit.register(clear_sync_status, root)

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
            task = progress.add_task("pull", total=total_for_loop)

            for i, uid in enumerate(uids):
                uid_int = int(uid)

                # Fetch headers first (lightweight)
                try:
                    info = client.fetch_info(uid)
                except Exception as e:
                    failed += 1
                    consecutive_errors += 1
                    if has_cfg and not dry_run:
                        failures[uid_int] = e
                        # Record failure in pulls.db for activity tracking
                        if pulls_db:
                            pulls_db.record_pull(
                                account=account,
                                folder=src_folder,
                                uidvalidity=uidvalidity,
                                uid=uid_int,
                                content_hash="",
                                status="failed",
                                sync_run_id=sync_run_id,
                                error_message=str(e),
                            )
                    if verbose:
                        print_result("fail", f"UID {uid}", str(e))
                    progress.advance(task)
                    # Check for rate limit (consecutive errors)
                    if consecutive_errors >= max_errors:
                        console.print(f"\n[bold red]Aborting: {consecutive_errors} consecutive errors (likely rate limited)[/]")
                        aborted = True
                        break
                    continue

                subj = (info.subject or "(no subject)")[:60]

                if dry_run:
                    if verbose:
                        print_result("dry", subj)
                    fetched += 1
                    progress.advance(task)
                    continue

                # Fetch full message
                try:
                    raw = client.fetch_raw(uid)
                    raw_hash = content_hash(raw)

                    # Content-hash dedup - check if we already have this exact content
                    local_path: str | None = None
                    existing_path = layout.get_path_by_content(raw) if has_cfg else None
                    if existing_path:
                        # Duplicate - set local_path to existing file
                        local_path = str(existing_path.relative_to(root))
                        skipped += 1
                        if verbose:
                            print_result("skip", subj)
                    else:
                        # Store the message
                        if has_cfg:
                            stored_path = layout.add_message(
                                message_id=info.message_id,
                                raw=raw,
                                folder=src_folder,
                                date=info.date,
                                from_addr=info.from_addr,
                                to_addr=info.to_addr,
                                cc_addr=info.cc_addr,
                                subject=info.subject,
                                source_uid=str(uid_int),
                            )
                            local_path = str(stored_path.relative_to(root)) if stored_path else None
                            # Incrementally index the new file
                            if file_index and stored_path:
                                file_index._index_file(stored_path)
                        else:
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

                    # Record successful pull in pulls.db (even for dupes - we pulled it)
                    if pulls_db:
                        msg_date = info.date.isoformat() if info.date else None
                        msg_status = "skipped" if existing_path else "new"
                        body_text = extract_body_text(raw) if raw else None
                        pulls_db.record_pull(
                            account=account,
                            folder=src_folder,
                            uidvalidity=uidvalidity,
                            uid=uid_int,
                            content_hash=raw_hash,
                            message_id=info.message_id or None,
                            local_path=local_path,
                            subject=info.subject,
                            msg_date=msg_date,
                            status=msg_status,
                            sync_run_id=sync_run_id,
                            from_addr=info.from_addr or None,
                            to_addr=info.to_addr or None,
                            body_text=body_text,
                            in_reply_to=info.in_reply_to or None,
                            references=info.references or None,
                        )

                    # Clear from failures if previously failed
                    if uid_int in failures:
                        del failures[uid_int]

                    consecutive_errors = 0  # Reset on success

                except Exception as e:
                    failed += 1
                    consecutive_errors += 1
                    if has_cfg and not dry_run:
                        failures[uid_int] = e
                        # Record failure in pulls.db for activity tracking
                        if pulls_db:
                            msg_date = info.date.isoformat() if info.date else None
                            pulls_db.record_pull(
                                account=account,
                                folder=src_folder,
                                uidvalidity=uidvalidity,
                                uid=uid_int,
                                content_hash="",
                                message_id=info.message_id or None,
                                subject=info.subject,
                                msg_date=msg_date,
                                status="failed",
                                sync_run_id=sync_run_id,
                                error_message=str(e),
                            )
                    if verbose:
                        print_result("fail", subj, str(e))

                progress.advance(task)

                # Update sync progress for real-time status display
                if has_cfg and not dry_run:
                    update_sync_progress(
                        completed=fetched + skipped + failed,
                        skipped=skipped,
                        failed=failed,
                        current_subject=subj,
                        root=root,
                    )
                    # Also update sync_runs table for UI consistency
                    if pulls_db and sync_run_id:
                        pulls_db.update_sync_run(
                            sync_run_id,
                            fetched=fetched,
                            skipped=skipped,
                            failed=failed,
                        )

                # Check for rate limit (consecutive errors)
                if consecutive_errors >= max_errors:
                    console.print(f"\n[bold red]Aborting: {consecutive_errors} consecutive errors (likely rate limited)[/]")
                    aborted = True
                    break

        # Clear sync status file (we're done)
        if has_cfg and not dry_run:
            clear_sync_status(root)

        # End sync run record
        if pulls_db and sync_run_id:
            if aborted:
                run_status = "aborted"
                error_msg = f"{consecutive_errors} consecutive errors (rate limited)"
            elif failed > 0:
                run_status = "completed"  # Still completed, just with some failures
                error_msg = None
            else:
                run_status = "completed"
                error_msg = None
            pulls_db.update_sync_run(
                sync_run_id,
                fetched=fetched,
                skipped=skipped,
                failed=failed,
            )
            pulls_db.end_sync_run(sync_run_id, run_status, error_msg)

        # Save failures to disk
        if has_cfg and not dry_run:
            # Convert exception objects to PullFailure objects
            # (failures dict may contain both Exception objects and PullFailure objects from load_failures)
            # Use duck typing (hasattr) instead of isinstance to avoid module reloading issues
            failure_records = {}
            for uid, exc in failures.items():
                if hasattr(exc, 'error') and hasattr(exc, 'uid'):
                    # It's a PullFailure-like object - use its error field
                    failure_records[uid] = PullFailure(uid=uid, error=exc.error)
                else:
                    # It's an Exception - convert to string
                    failure_records[uid] = PullFailure(uid=uid, error=str(exc))
            save_failures(account, src_folder, failure_records, root)

        echo()
        if dry_run:
            echo(f"Would fetch: {fetched}")
        else:
            echo(f"Fetched: {fetched}")
            if skipped:
                echo(f"Skipped (duplicate): {skipped}")
            msg_count = layout.count() if has_cfg else storage.count()
            echo(f"Total in storage: {msg_count:,}")
            # Show pulls.db stats
            if pulls_db:
                pulled_count = pulls_db.get_pulled_count(account, src_folder, uidvalidity)
                echo(f"Pulled UIDs tracked: {pulled_count:,}")
        if failed:
            echo(style(f"Failed: {failed}", fg="red"))
            if has_cfg and not dry_run:
                failures_path = get_failures_path(account, src_folder, root)
                echo(f"  Failures logged: {failures_path}")
                echo("  Retry with: eml pull " + account + " -f '" + src_folder + "' --retry")
        if aborted:
            echo(style("Note: Aborted early due to rate limiting. Retry later.", fg="yellow"))

        # Cleanup
        if not dry_run:
            if pulls_db:
                pulls_db.disconnect()
            if file_index:
                file_index.conn.commit()
                file_index.close()
            if has_cfg and layout and hasattr(layout, 'disconnect'):
                layout.disconnect()
            elif not has_cfg and storage:
                storage.disconnect()

    except Exception as e:
        err(f"Error: {e}")
        sys.exit(1)
    finally:
        client.disconnect()
