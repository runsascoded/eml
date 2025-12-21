"""Index, uids, and fsck commands."""

import email
import re
import sys
from datetime import datetime

import click
import humanize
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

from ..config import AccountConfig, find_eml_root, get_eml_root, load_config
from ..imap import GmailClient, IMAPClient, ZohoClient
from ..pulls import get_pulls_db

from .utils import err, get_account_any, get_imap_client, require_init


@click.command()
@require_init
@option('-u', '--update', 'update_only', is_flag=True, help="Incremental update (only new/changed files)")
@option('-s', '--stats', 'show_stats', is_flag=True, help="Show index statistics")
@option('-c', '--check', 'check_only', is_flag=True, help="Check index freshness")
def index(update_only: bool, show_stats: bool, check_only: bool):
    """Build or update persistent file index.

    \b
    Examples:
      eml index                    # Full rebuild
      eml index -u                 # Incremental update
      eml index -s                 # Show statistics
      eml index -c                 # Check if stale

    The index enables O(1) lookups by Message-ID or content hash,
    instead of scanning all files on each operation.
    """
    from ..index import FileIndex

    root = get_eml_root()
    eml_dir = root / ".eml"

    with FileIndex(eml_dir) as idx:
        if check_only:
            # Check freshness
            indexed_sha = idx.get_indexed_sha()
            head_sha = idx.get_git_head()
            file_count = idx.file_count()

            if not indexed_sha:
                echo("No index built yet. Run 'eml index' to create.")
                sys.exit(1)

            echo(f"Index: {file_count:,} files at {indexed_sha[:8]}")

            if head_sha:
                if indexed_sha == head_sha:
                    echo(style("✓ Index is up to date", fg="green"))
                else:
                    echo(f"HEAD:  {head_sha[:8]}")
                    echo(style("Index may be stale. Run 'eml index -u' to update.", fg="yellow"))
            return

        if show_stats:
            # Show statistics
            if idx.file_count() == 0:
                echo("Index is empty. Run 'eml index' to build.")
                return

            stats = idx.stats()
            echo(f"Total files:        {stats['total_files']:,}")
            echo(f"With Message-ID:    {stats['with_message_id']:,}")
            echo(f"Without Message-ID: {stats['without_message_id']:,}")
            if stats['total_size']:
                echo(f"Total size:         {humanize.naturalsize(stats['total_size'])}")
            if stats['oldest_date']:
                echo(f"Date range:         {stats['oldest_date'][:10]} to {stats['newest_date'][:10]}")
            if stats['indexed_at']:
                echo(f"Indexed at:         {stats['indexed_at'][:19]}")
            if stats['git_sha']:
                echo(f"Git SHA:            {stats['git_sha'][:8]}")

            if stats['folders']:
                echo()
                echo("By folder:")
                for folder, count in sorted(stats['folders'].items(), key=lambda x: -x[1])[:10]:
                    echo(f"  {folder:20} {count:>8,}")
            return

        console = Console()

        if update_only:
            # Incremental update
            echo("Updating index...")
            with Progress(
                SpinnerColumn(),
                TextColumn("[bold blue]Scanning"),
                console=console,
            ) as progress:
                progress.add_task("scan", total=None)
                added, modified, deleted = idx.update()

            echo(f"Added:    {added:,}")
            echo(f"Modified: {modified:,}")
            echo(f"Deleted:  {deleted:,}")
        else:
            # Full rebuild
            echo("Building index...")

            # Count files first
            file_count = sum(1 for _ in root.rglob("*.eml")
                            if ".eml" not in _.parts[:-1])

            with Progress(
                SpinnerColumn(),
                TextColumn("[bold blue]Indexing"),
                BarColumn(),
                TaskProgressColumn(),
                TextColumn("{task.completed}/{task.total}"),
                TimeElapsedColumn(),
                console=console,
            ) as progress:
                task = progress.add_task("index", total=file_count)

                def progress_cb(current, total):
                    progress.update(task, completed=current)

                indexed, skipped, errors = idx.rebuild(progress_cb)

            echo()
            echo(f"Indexed:  {indexed:,}")
            if skipped:
                echo(f"Skipped:  {skipped:,}")
            if errors:
                echo(style(f"Errors:   {errors}", fg="red"))

        git_sha = idx.get_git_head()
        if git_sha:
            echo(f"Git SHA:  {git_sha[:8]}")


@click.command(no_args_is_help=True)
@require_init
@option('-f', '--folder', default="Inbox", help="IMAP folder")
@option('-j', '--json', 'output_json', is_flag=True, help="Output as JSON")
@option('-l', '--limit', type=int, help="Limit output")
@option('--no-mid', is_flag=True, help="Show UIDs without Message-ID")
@option('--pulled', is_flag=True, help="Show pulled UIDs")
@option('--server', is_flag=True, help="Show server UIDs (from last backfill)")
@option('--unpulled', is_flag=True, help="Show server UIDs not yet pulled")
@argument('account')
def uids(
    folder: str,
    output_json: bool,
    limit: int | None,
    no_mid: bool,
    pulled: bool,
    server: bool,
    unpulled: bool,
    account: str,
):
    """Query UID sets from pulls.db.

    \b
    Examples:
      eml uids y --server             # Show all server UIDs
      eml uids y --unpulled           # Show UIDs needing pull
      eml uids y --no-mid             # Show UIDs without Message-ID
      eml uids y --pulled -l 20       # Show last 20 pulled UIDs

    Requires backfill to have been run first (to populate server_uids).
    """
    import json as json_module

    root = get_eml_root()
    pulls_db_path = root / ".eml" / "pulls.db"
    if not pulls_db_path.exists():
        err("No pulls.db found. Run 'eml backfill' first.")
        sys.exit(1)

    # Need UIDVALIDITY - get from pulls.db
    pulls_db = get_pulls_db(root)
    pulls_db.connect()

    uidvalidity = pulls_db.get_uidvalidity(account, folder)
    if not uidvalidity:
        # Try server_folders table
        cur = pulls_db.conn.execute("""
            SELECT uidvalidity FROM server_folders
            WHERE account = ? AND folder = ?
        """, (account, folder))
        row = cur.fetchone()
        if row:
            uidvalidity = row["uidvalidity"]
        else:
            err(f"No UIDVALIDITY found for {account}/{folder}. Run backfill first.")
            pulls_db.disconnect()
            sys.exit(1)

    result_uids: set[int] = set()
    query_name = ""

    if no_mid:
        result_uids = pulls_db.get_uids_without_message_id(account, folder, uidvalidity)
        query_name = "UIDs without Message-ID"
    elif unpulled:
        result_uids = pulls_db.get_unpulled_uids(account, folder, uidvalidity)
        query_name = "Unpulled UIDs (on server, not pulled)"
    elif server:
        result_uids = pulls_db.get_server_uids(account, folder, uidvalidity)
        query_name = "Server UIDs"
    elif pulled:
        result_uids = pulls_db.get_pulled_uids(account, folder, uidvalidity)
        query_name = "Pulled UIDs"
    else:
        # Default: show summary
        server_count = pulls_db.get_server_uid_count(account, folder)
        pulled_count = pulls_db.get_pulled_count(account, folder, uidvalidity)
        unpulled_uids = pulls_db.get_unpulled_uids(account, folder, uidvalidity)
        no_mid_uids = pulls_db.get_uids_without_message_id(account, folder, uidvalidity)

        if output_json:
            print(json_module.dumps({
                "account": account,
                "folder": folder,
                "uidvalidity": uidvalidity,
                "server_uids": server_count,
                "pulled_uids": pulled_count,
                "unpulled_uids": len(unpulled_uids),
                "no_message_id": len(no_mid_uids),
            }, indent=2))
        else:
            echo(f"Account: {account}")
            echo(f"Folder: {folder}")
            echo(f"UIDVALIDITY: {uidvalidity}")
            echo()
            echo(f"Server UIDs:    {server_count:,}")
            echo(f"Pulled UIDs:    {pulled_count:,}")
            echo(f"Unpulled UIDs:  {len(unpulled_uids):,}")
            echo(f"No Message-ID:  {len(no_mid_uids):,}")
        pulls_db.disconnect()
        return

    pulls_db.disconnect()

    # Output
    uid_list = sorted(result_uids)
    if limit:
        uid_list = uid_list[:limit]

    if output_json:
        print(json_module.dumps({
            "query": query_name,
            "count": len(result_uids),
            "uids": uid_list,
        }, indent=2))
    else:
        echo(f"{query_name}: {len(result_uids):,}")
        if uid_list:
            echo()
            for uid in uid_list:
                echo(f"  {uid}")
            if limit and len(result_uids) > limit:
                echo(f"  ... and {len(result_uids) - limit:,} more")


@click.command(no_args_is_help=True)
@require_init
@option('-f', '--folder', default="[Gmail]/All Mail", help="IMAP folder to check")
@option('-j', '--json', 'output_json', is_flag=True, help="Output as JSON")
@option('-m', '--show-missing', is_flag=True, help="List truly missing messages")
@option('-v', '--verbose', is_flag=True, help="Show detailed progress")
@argument('account')
def fsck(folder: str, output_json: bool, show_missing: bool, verbose: bool, account: str):
    """Check local storage against IMAP server.

    \b
    Examples:
      eml fsck y -f Inbox           # Check Inbox folder
      eml fsck y -f Sent -m         # Show missing from Sent
      eml fsck y -j                 # Output as JSON

    Compares Message-IDs on server against local index to identify:
    - Truly missing messages (not in any local folder)
    - Cross-folder duplicates (same Message-ID in different folder)
    """
    from ..index import FileIndex

    root = get_eml_root()
    eml_dir = root / ".eml"
    config = load_config(root)

    # Resolve account
    if account in config.accounts:
        acct = config.accounts[account]
    else:
        err(f"Account not found: {account}")
        err(f"Available: {', '.join(config.accounts.keys())}")
        sys.exit(1)

    # Count local files in this folder
    local_folder_path = root / folder
    local_folder_count = 0
    if local_folder_path.exists():
        local_folder_count = sum(1 for _ in local_folder_path.rglob("*.eml") if _.is_file())

    # Load or build index
    with FileIndex(eml_dir) as idx:
        if idx.file_count() == 0:
            echo("Building index first...")
            idx.rebuild()
            echo()

        local_message_ids = idx.all_message_ids()
        local_hashes = idx.all_content_hashes()

    echo(f"Local index: {len(local_message_ids):,} message IDs, {len(local_hashes):,} content hashes")
    echo(f"Local '{folder}' files: {local_folder_count:,}")

    # Connect to IMAP
    echo(f"Connecting to {acct.host or acct.type}...")

    if acct.type == "gmail":
        client = GmailClient()
    elif acct.type == "zoho":
        client = ZohoClient()
    elif acct.host:
        client = IMAPClient(acct.host, acct.port)
    else:
        err(f"Unknown account type: {acct.type}")
        sys.exit(1)

    try:
        client.connect(acct.user, acct.password)
        echo(f"Connected as {acct.user}")

        # Select folder
        typ, data = client.conn.select(folder, readonly=True)
        if typ != "OK":
            err(f"Failed to select folder: {folder}")
            sys.exit(1)

        folder_count = int(data[0])
        echo(f"Server folder '{folder}': {folder_count:,} messages")

        # Fetch all Message-IDs from server
        echo("Fetching Message-IDs from server...")
        console = Console()

        server_ids: dict[str, dict] = {}  # message_id -> {uid, date, subject, from}

        # Fetch in batches for large folders
        batch_size = 1000
        total_fetched = 0

        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]Fetching"),
            BarColumn(),
            TaskProgressColumn(),
            TextColumn("{task.completed}/{task.total}"),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("fetch", total=folder_count)

            # Use SEARCH to get all UIDs
            typ, data = client.conn.uid("SEARCH", None, "ALL")
            if typ != "OK":
                err("Failed to search folder")
                sys.exit(1)

            all_uids = data[0].split() if data[0] else []

            for i in range(0, len(all_uids), batch_size):
                batch = all_uids[i:i + batch_size]
                uid_set = b",".join(batch)

                # Fetch headers for this batch
                typ, data = client.conn.uid(
                    "FETCH", uid_set,
                    "(BODY.PEEK[HEADER.FIELDS (MESSAGE-ID DATE FROM SUBJECT)])"
                )

                if typ != "OK":
                    continue

                for item in data:
                    if isinstance(item, tuple) and len(item) >= 2:
                        # Parse UID from response
                        uid_match = re.search(rb"UID (\d+)", item[0])
                        if not uid_match:
                            continue
                        uid = int(uid_match.group(1))

                        try:
                            msg = email.message_from_bytes(item[1])
                            mid = msg.get("Message-ID", "").strip()
                            if mid:
                                server_ids[mid] = {
                                    "uid": uid,
                                    "date": msg.get("Date", ""),
                                    "from": msg.get("From", ""),
                                    "subject": msg.get("Subject", ""),
                                }
                                total_fetched += 1
                        except Exception:
                            pass

                progress.update(task, completed=min(i + batch_size, len(all_uids)))

        # Count messages without Message-ID
        no_mid_count = folder_count - len(server_ids)
        echo(f"Server messages with Message-ID: {len(server_ids):,}")
        echo(f"Server messages without Message-ID: {no_mid_count:,}")

        # Compare by Message-ID
        server_set = set(server_ids.keys())
        local_set = local_message_ids

        missing_by_mid = server_set - local_set
        extra_local = local_set - server_set

        # Summary
        echo()
        file_diff = folder_count - local_folder_count
        echo(f"File count diff:    {file_diff:,} (server - local)")
        echo(f"Missing by Msg-ID:  {len(missing_by_mid):,}")
        if no_mid_count > 0:
            echo(f"  (+ {no_mid_count:,} server msgs have no Message-ID to compare)")
        echo(f"Extra in local:     {len(extra_local):,}")

        if output_json:
            import json as json_mod
            result = {
                "folder": folder,
                "server_total": folder_count,
                "server_with_mid": len(server_ids),
                "server_without_mid": no_mid_count,
                "local_files": local_folder_count,
                "file_diff": file_diff,
                "missing_by_mid": len(missing_by_mid),
                "extra_local": len(extra_local),
            }
            if show_missing:
                result["missing"] = [
                    {
                        "message_id": mid,
                        "uid": server_ids[mid]["uid"],
                        "date": server_ids[mid]["date"],
                        "from": server_ids[mid]["from"],
                        "subject": server_ids[mid]["subject"][:50],
                    }
                    for mid in sorted(missing_by_mid)[:100]
                ]
            print(json_mod.dumps(result, indent=2))
        elif show_missing and missing_by_mid:
            echo()
            echo("Missing messages (by Message-ID):")
            for mid in sorted(missing_by_mid)[:50]:
                info = server_ids[mid]
                date_str = info["date"][:16] if info["date"] else "?"
                from_str = info["from"][:30] if info["from"] else "?"
                subj_str = info["subject"][:40] if info["subject"] else "?"
                echo(f"  UID {info['uid']:>8}  {date_str}  {from_str}  {subj_str}")

            if len(missing_by_mid) > 50:
                echo(f"  ... and {len(missing_by_mid) - 50} more")

    finally:
        client.disconnect()


@click.command(name="index-fts")
@require_init
@option('-j', '--jobs', type=int, default=8, help="Number of parallel workers (default: 8)")
@option('-l', '--limit', type=int, help="Limit number of messages to process")
@option('-R', '--rebuild', is_flag=True, help="Rebuild entire FTS index from scratch")
@option('-v', '--verbose', is_flag=True, help="Show progress for each message")
def index_fts(jobs: int, limit: int | None, rebuild: bool, verbose: bool):
    """Build or update FTS (full-text search) index.

    \b
    Examples:
      eml index-fts           # Update FTS for messages with local_path but no body_text
      eml index-fts -R        # Rebuild entire FTS index from scratch
      eml index-fts -l 100    # Process at most 100 messages

    The FTS index enables full-text search across subject, body, from, and to fields.
    New messages pulled after this update will be indexed automatically.
    """
    from email import policy
    from email.parser import BytesParser
    from pathlib import Path

    from ..parsing import extract_body_text

    root = get_eml_root()
    pulls_db = get_pulls_db(root)
    pulls_db.connect()

    try:
        if rebuild:
            echo("Rebuilding FTS index from scratch...")
            count = pulls_db.rebuild_fts_index()
            echo(f"Indexed {count:,} messages")
            return

        # Find messages with local_path but missing FTS fields
        cur = pulls_db.conn.execute("""
            SELECT rowid, message_id, local_path, subject, from_addr, to_addr, body_text
            FROM pulled_messages
            WHERE local_path IS NOT NULL
              AND message_id IS NOT NULL
              AND (body_text IS NULL OR body_text = '')
            ORDER BY rowid DESC
        """ + (f" LIMIT {limit}" if limit else ""))

        rows = cur.fetchall()
        if not rows:
            echo("No messages need FTS indexing")
            return

        echo(f"Processing {len(rows):,} messages with {jobs} workers...")

        console = Console()
        indexed = 0
        skipped = 0
        errors = 0

        # Helper function for parallel file reading/parsing (I/O-bound)
        def process_file(row):
            """Read and parse a single .eml file. Returns extracted data or error."""
            rowid = row["rowid"]
            message_id = row["message_id"]
            local_path = row["local_path"]
            subject = row["subject"]
            from_addr = row["from_addr"]
            to_addr = row["to_addr"]

            eml_path = root / local_path
            if not eml_path.exists():
                return {"status": "skipped", "local_path": local_path}

            try:
                raw = eml_path.read_bytes()
                body_text = extract_body_text(raw)

                # Also extract from/to if missing
                if not from_addr or not to_addr:
                    msg = BytesParser(policy=policy.default).parsebytes(raw)
                    if not from_addr:
                        from_addr = msg.get("From", "")
                    if not to_addr:
                        to_addr = msg.get("To", "")

                return {
                    "status": "ok",
                    "rowid": rowid,
                    "message_id": message_id,
                    "subject": subject,
                    "from_addr": from_addr,
                    "to_addr": to_addr,
                    "body_text": body_text,
                    "local_path": local_path,
                }
            except Exception as e:
                return {"status": "error", "local_path": local_path, "error": str(e)}

        from concurrent.futures import ThreadPoolExecutor, as_completed

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Indexing FTS...", total=len(rows))

            # Process files in parallel, write to DB sequentially
            with ThreadPoolExecutor(max_workers=jobs) as executor:
                futures = {executor.submit(process_file, row): row for row in rows}

                batch = []
                for future in as_completed(futures):
                    result = future.result()
                    progress.advance(task)

                    if result["status"] == "skipped":
                        skipped += 1
                        if verbose:
                            console.print(f"[yellow]Skip[/] {result['local_path']} (file not found)")
                    elif result["status"] == "error":
                        errors += 1
                        if verbose:
                            console.print(f"[red]Error[/] {result['local_path']}: {result['error']}")
                    else:
                        batch.append(result)
                        indexed += 1
                        if verbose:
                            console.print(f"[green]OK[/] {result['local_path'][:60]}")

                    # Batch write every 100 results
                    if len(batch) >= 100:
                        for r in batch:
                            # Update from/to in pulled_messages (body_text only stored in FTS)
                            pulls_db.conn.execute("""
                                UPDATE pulled_messages
                                SET from_addr = ?, to_addr = ?
                                WHERE rowid = ?
                            """, (r["from_addr"], r["to_addr"], r["rowid"]))
                            pulls_db.conn.execute("DELETE FROM messages_fts WHERE message_id = ?", (r["message_id"],))
                            pulls_db.conn.execute("""
                                INSERT INTO messages_fts(message_id, subject, body_text, from_addr, to_addr)
                                VALUES (?, ?, ?, ?, ?)
                            """, (r["message_id"], r["subject"], r["body_text"], r["from_addr"], r["to_addr"]))
                        pulls_db.conn.commit()
                        batch = []

                # Write remaining batch
                for r in batch:
                    # Update from/to in pulled_messages (body_text only stored in FTS)
                    pulls_db.conn.execute("""
                        UPDATE pulled_messages
                        SET from_addr = ?, to_addr = ?
                        WHERE rowid = ?
                    """, (r["from_addr"], r["to_addr"], r["rowid"]))
                    pulls_db.conn.execute("DELETE FROM messages_fts WHERE message_id = ?", (r["message_id"],))
                    pulls_db.conn.execute("""
                        INSERT INTO messages_fts(message_id, subject, body_text, from_addr, to_addr)
                        VALUES (?, ?, ?, ?, ?)
                    """, (r["message_id"], r["subject"], r["body_text"], r["from_addr"], r["to_addr"]))
                pulls_db.conn.commit()

        echo()
        echo(f"Indexed: {indexed:,}")
        if skipped:
            echo(style(f"Skipped: {skipped:,}", fg="yellow"))
        if errors:
            echo(style(f"Errors:  {errors:,}", fg="red"))

    finally:
        pulls_db.disconnect()
