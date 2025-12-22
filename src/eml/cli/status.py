"""Status, web dashboard, and stats commands."""

import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import click
import humanize
from click import echo, option
from rich.console import Console
from rich.table import Table

from ..config import get_eml_root
from ..pulls import get_pulls_db
from ..storage import MessageStorage, get_msgs_db_path

from .utils import (
    err,
    get_recent_pushed,
    read_sync_status,
    require_init,
)


@click.command()
@require_init
@option('-c', '--color', is_flag=True, help="Force color output (for use with watch)")
@option('-f', '--folder', multiple=True, help="Filter to specific folder(s)")
def status(color: bool, folder: tuple[str, ...]):
    """Show pull progress and recent activity.

    \b
    Examples:
      eml status                    # Show current status
      eml status -f Sent            # Show only Sent folder
      watch -c -n5 eml status -c    # Monitor with colors

    Shows total files, pending failures, hourly download histogram,
    and the 10 most recently downloaded files.
    """
    root = get_eml_root()
    folder_filter = set(folder) if folder else None

    # Colors
    use_color = color or (sys.stdout.isatty() and not os.environ.get('NO_COLOR'))
    if use_color:
        BOLD, DIM, GREEN, YELLOW, CYAN, RESET = '\033[1m', '\033[2m', '\033[32m', '\033[33m', '\033[36m', '\033[0m'
    else:
        BOLD = DIM = GREEN = YELLOW = CYAN = RESET = ''

    now = datetime.now()

    # Collect all .eml files with mtime in single pass
    files = []
    for dirpath, dirnames, filenames in os.walk(root):
        # Check folder filter
        if folder_filter:
            rel_dir = os.path.relpath(dirpath, root)
            top_folder = rel_dir.split(os.sep)[0] if rel_dir != '.' else ''
            if top_folder not in folder_filter:
                continue
        for fname in filenames:
            if fname.endswith('.eml'):
                path = os.path.join(dirpath, fname)
                try:
                    mtime = os.stat(path).st_mtime
                    files.append((mtime, path))
                except OSError:
                    pass

    total = len(files)

    # Count failures (filter by folder if specified)
    failures_dir = root / ".eml" / "failures"
    total_failures = 0
    if failures_dir.exists():
        for fail_file in failures_dir.glob("*.yaml"):
            # Filter: filename is like y_Inbox.yaml - check if folder matches
            if folder_filter:
                file_folder = fail_file.stem.split('_', 1)[1] if '_' in fail_file.stem else ''
                if file_folder not in folder_filter:
                    continue
            try:
                for line in fail_file.read_text().splitlines():
                    if line and line[0].isdigit():
                        total_failures += 1
            except Exception:
                pass

    # Header
    folder_str = f" ({', '.join(folder)})" if folder else ""
    print(f"{BOLD}EML Pull Status{folder_str}{RESET} - {now:%Y-%m-%d %H:%M:%S}")
    print()
    print(f"{CYAN}Total files:{RESET}     {total}")
    print(f"{YELLOW}Pending retry:{RESET}  {total_failures}")

    # Show pulls.db stats if available
    pulls_db_path = root / ".eml" / "pulls.db"
    if pulls_db_path.exists():
        try:
            pulls_db = get_pulls_db(root)
            pulls_db.connect()
            stats = pulls_db.get_stats()
            pulled_total = stats.get("total", 0)
            print(f"{GREEN}Pulled UIDs:{RESET}    {pulled_total:,}")
            if stats.get("folders"):
                for folder_name, count in sorted(stats["folders"].items()):
                    if not folder_filter or folder_name in folder_filter:
                        print(f"  {folder_name}: {count:,}")
            pulls_db.disconnect()
        except Exception:
            pass
    print()

    # Hourly distribution (last 24h) - prefer pulls.db, fallback to filesystem
    print(f"{BOLD}Downloads by hour (last 24h):{RESET}")
    hourly_data: list[tuple[str, int]] = []
    pulls_db_path = root / ".eml" / "pulls.db"
    if pulls_db_path.exists():
        try:
            pulls_db = get_pulls_db(root)
            pulls_db.connect()
            hourly_data = pulls_db.get_pulls_by_hour(limit_hours=24)
            pulls_db.disconnect()
        except Exception:
            pass

    if hourly_data:
        # From pulls.db - data is [(hour_str, count), ...] most recent first
        for hour_str, count in reversed(hourly_data):
            bar = '█' * (count // 50)
            print(f"  {hour_str}  {count:4d} {bar}")
    else:
        # Fallback to filesystem mtime
        hourly = defaultdict(int)
        cutoff = now - timedelta(hours=24)
        for mtime, _ in files:
            if mtime >= cutoff.timestamp():
                dt = datetime.fromtimestamp(mtime)
                hour_key = dt.replace(minute=0, second=0, microsecond=0)
                hourly[hour_key] += 1

        for hour_key in sorted(hourly.keys()):
            count = hourly[hour_key]
            bar = '█' * (count // 50)
            print(f"  {hour_key:%Y-%m-%d %H}:00  {count:4d} {bar}")
    print()

    # Last 10 downloaded (oldest first, most recent at bottom) - prefer pulls.db
    print(f"{BOLD}Last 10 downloaded:{RESET}")
    recent_pulls: list = []
    if pulls_db_path.exists():
        try:
            pulls_db = get_pulls_db(root)
            pulls_db.connect()
            recent_pulls = pulls_db.get_recent_pulls(limit=10, with_path_only=True)
            pulls_db.disconnect()
        except Exception:
            pass

    if recent_pulls:
        # From pulls.db - data is most recent first, reverse for display
        for rp in reversed(recent_pulls):
            folder_name = os.path.dirname(rp.local_path)
            fname = os.path.basename(rp.local_path).removesuffix('.eml')
            if len(fname) > 45:
                fname = fname[:42] + '...'
            print(f"  {DIM}{rp.pulled_at:%Y-%m-%d %H:%M:%S}{RESET} {GREEN}{folder_name}/{RESET}{fname}")
    else:
        # Fallback to filesystem mtime
        files.sort(reverse=True)
        for mtime, path in reversed(files[:10]):
            dt = datetime.fromtimestamp(mtime)
            rel_path = os.path.relpath(path, root)
            folder_name = os.path.dirname(rel_path)
            fname = os.path.basename(path).removesuffix('.eml')
            if len(fname) > 45:
                fname = fname[:42] + '...'
            print(f"  {DIM}{dt:%Y-%m-%d %H:%M:%S}{RESET} {GREEN}{folder_name}/{RESET}{fname}")
    print()

    # Last 10 uploaded (if any)
    recent_pushed = get_recent_pushed(10, root)
    if recent_pushed:
        print(f"{BOLD}Last 10 uploaded:{RESET}")
        for entry in reversed(recent_pushed):  # oldest first, most recent at bottom
            pushed_at = entry.get("pushed_at", "")
            account = entry.get("account", "?")
            subject = entry.get("subject") or "(no subject)"
            if len(subject) > 50:
                subject = subject[:47] + "..."
            # Parse ISO timestamp
            try:
                dt = datetime.fromisoformat(pushed_at)
                dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                dt_str = pushed_at[:19] if len(pushed_at) >= 19 else pushed_at
            print(f"  {DIM}{dt_str}{RESET} {YELLOW}{account}{RESET} {subject}")
        print()

    # Check if sync (pull or push) running by reading local status file
    sync_status = read_sync_status(root)
    if sync_status:
        op = sync_status.get("operation", "sync")
        acct = sync_status.get("account", "?")
        fldr = sync_status.get("folder", "?")
        completed = sync_status.get("completed", 0)
        skipped = sync_status.get("skipped", 0)
        failed = sync_status.get("failed", 0)
        total_msgs = sync_status.get("total", 0)
        current_subject = sync_status.get("current_subject")
        pid = sync_status.get("pid")
        pid_str = f" [PID {pid}]" if pid else ""
        op_label = op.capitalize()

        # Build progress string
        if total_msgs > 0:
            pct = completed * 100 // total_msgs
            progress_str = f"{completed}/{total_msgs} ({pct}%)"
        else:
            progress_str = str(completed) if completed else "starting..."

        # Build details string
        details = []
        if skipped > 0:
            details.append(f"{skipped} skipped")
        if failed > 0:
            details.append(f"{failed} failed")
        details_str = f" [{', '.join(details)}]" if details else ""

        print(f"{GREEN}● {op_label} in progress: {acct}/{fldr} {progress_str}{details_str}{pid_str}{RESET}")
        if current_subject:
            print(f"  {DIM}Current: {current_subject}{RESET}")
    else:
        print(f"{DIM}○ No sync running{RESET}")


@click.command()
@require_init
@option('-h', '--host', default="127.0.0.1", help="Host to bind to")
@option('-p', '--port', default=8765, type=int, help="Port to run on")
@option('-r', '--reload', 'reload_', is_flag=True, help="Enable hot reload for development")
def web(host: str, port: int, reload_: bool):
    """Start status dashboard web UI.

    \b
    Examples:
      eml web                    # Start on http://127.0.0.1:8765
      eml web -p 8080            # Use different port
      eml web -h 0.0.0.0         # Listen on all interfaces

    The dashboard shows:
    - UID summary (server/pulled/unpulled)
    - Hourly download histogram
    - Recent downloads
    - Current sync status
    """
    try:
        from ..web import main as web_main
        web_main(host=host, port=port, reload=reload_)
    except ImportError as e:
        err(f"Failed to import web module: {e}")
        err("Make sure fastapi and uvicorn are installed: pip install fastapi uvicorn")
        sys.exit(1)


@click.command()
@require_init
def stats():
    """Show aggregate statistics about stored messages.

    \b
    Examples:
      eml stats
    """
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
