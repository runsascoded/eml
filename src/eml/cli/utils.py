"""Shared CLI utilities and helpers."""

import os
import sys
from datetime import datetime
from functools import wraps
from pathlib import Path

import click
from click import echo, prompt, style

from ..config import (
    AccountConfig,
    find_eml_root,
    get_eml_root,
    is_valid_layout,
    load_config,
)
from ..imap import GmailClient, IMAPClient, ZohoClient
from ..layouts import PRESETS, SqliteLayout, StorageLayout, TreeLayout
from ..storage import (
    ACCTS_DB,
    Account,
    AccountStorage,
    GLOBAL_CONFIG_DIR,
    find_eml_dir,
    get_account,
)


def err(*args, **kwargs):
    """Print to stderr."""
    print(*args, file=sys.stderr, **kwargs)


def has_config(root: Path | None = None) -> bool:
    """Check if project has config.yaml."""
    root = root or find_eml_root()
    if not root:
        return False
    return (root / ".eml" / "config.yaml").exists()


# =============================================================================
# Sync status tracking (SQLite-based for real-time updates)
# =============================================================================

SYNC_STATUS_DB = "sync-status.db"


def get_sync_status_db_path(root: Path | None = None) -> Path:
    """Get path to sync status database."""
    root = root or get_eml_root()
    return root / ".eml" / SYNC_STATUS_DB


def _get_sync_db(root: Path | None = None):
    """Get connection to sync status database, creating schema if needed."""
    import sqlite3

    db_path = get_sync_status_db_path(root)
    conn = sqlite3.connect(db_path, isolation_level=None)  # autocommit for real-time updates
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sync_status (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            pid INTEGER NOT NULL,
            operation TEXT NOT NULL,
            account TEXT NOT NULL,
            folder TEXT NOT NULL,
            total INTEGER NOT NULL,
            completed INTEGER NOT NULL DEFAULT 0,
            skipped INTEGER NOT NULL DEFAULT 0,
            failed INTEGER NOT NULL DEFAULT 0,
            started TEXT NOT NULL,
            current_subject TEXT
        )
    """)
    # Push log for "Last 10 uploaded" feature
    conn.execute("""
        CREATE TABLE IF NOT EXISTS push_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account TEXT NOT NULL,
            message_id TEXT NOT NULL,
            path TEXT,
            subject TEXT,
            pushed_at TEXT NOT NULL
        )
    """)
    return conn


def log_pushed_message(
    account: str,
    message_id: str,
    path: str | None,
    subject: str | None,
    root: Path | None = None,
) -> None:
    """Log a pushed message for 'Last 10 uploaded' display."""
    conn = _get_sync_db(root)
    conn.execute("""
        INSERT INTO push_log (account, message_id, path, subject, pushed_at)
        VALUES (?, ?, ?, ?, ?)
    """, (account, message_id, path, subject, datetime.now().isoformat()))
    conn.close()


def get_recent_pushed(limit: int = 10, root: Path | None = None) -> list[dict]:
    """Get recently pushed messages for display."""
    import sqlite3

    db_path = get_sync_status_db_path(root)
    if not db_path.exists():
        return []
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT account, message_id, path, subject, pushed_at
            FROM push_log
            ORDER BY pushed_at DESC
            LIMIT ?
        """, (limit,))
        results = [dict(row) for row in cur.fetchall()]
    except sqlite3.OperationalError:
        results = []
    conn.close()
    return results


def write_sync_status(
    operation: str,  # "pull" or "push"
    account: str,
    folder: str,
    total: int,
    completed: int = 0,
    root: Path | None = None,
) -> None:
    """Initialize sync status in database."""
    conn = _get_sync_db(root)
    conn.execute("DELETE FROM sync_status")
    conn.execute("""
        INSERT INTO sync_status (id, pid, operation, account, folder, total, completed, started)
        VALUES (1, ?, ?, ?, ?, ?, ?, ?)
    """, (os.getpid(), operation, account, folder, total, completed, datetime.now().isoformat()))
    conn.close()


def update_sync_status(completed: int, root: Path | None = None) -> None:
    """Update completed count in sync status database."""
    try:
        conn = _get_sync_db(root)
        conn.execute("UPDATE sync_status SET completed = ? WHERE id = 1", (completed,))
        conn.close()
    except Exception:
        pass


def update_sync_progress(
    completed: int | None = None,
    skipped: int | None = None,
    failed: int | None = None,
    current_subject: str | None = None,
    root: Path | None = None,
) -> None:
    """Update sync progress with fine-grained fields. Only updates non-None fields."""
    try:
        conn = _get_sync_db(root)
        updates = []
        params = []
        if completed is not None:
            updates.append("completed = ?")
            params.append(completed)
        if skipped is not None:
            updates.append("skipped = ?")
            params.append(skipped)
        if failed is not None:
            updates.append("failed = ?")
            params.append(failed)
        if current_subject is not None:
            updates.append("current_subject = ?")
            params.append(current_subject[:100] if current_subject else None)
        if updates:
            conn.execute(f"UPDATE sync_status SET {', '.join(updates)} WHERE id = 1", params)
        conn.close()
    except Exception:
        pass


def clear_sync_status(root: Path | None = None) -> None:
    """Clear sync status from database."""
    try:
        conn = _get_sync_db(root)
        conn.execute("DELETE FROM sync_status")
        conn.close()
    except Exception:
        pass


def read_sync_status(root: Path | None = None) -> dict | None:
    """Read sync status from database. Returns None if no active sync or stale."""
    root = root or get_eml_root()
    db_path = get_sync_status_db_path(root)
    if not db_path.exists():
        return None
    try:
        import sqlite3

        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.execute("SELECT * FROM sync_status WHERE id = 1")
        row = cur.fetchone()
        conn.close()
        if not row:
            return None
        status = dict(row)
        pid = status.get("pid")
        # Check if process is still running
        if pid:
            try:
                os.kill(pid, 0)  # Signal 0 just checks if process exists
            except OSError:
                # Process not running, stale status
                clear_sync_status(root)
                return None
        return status
    except Exception:
        return None


# =============================================================================
# Storage and account helpers
# =============================================================================


def get_storage_layout(root: Path | None = None) -> StorageLayout:
    """Get the storage layout for the current project."""
    root = root or get_eml_root()
    config = load_config(root)

    if config.layout == "sqlite":
        layout = SqliteLayout(root)
        layout.connect()
        return layout
    else:
        # Use template-based TreeLayout
        return TreeLayout(root, template=config.layout)


def get_account_any(name: str) -> Account | AccountConfig | None:
    """Get account by name from config.yaml or global accts.db."""
    root = find_eml_root()
    if root and has_config(root):
        config = load_config(root)
        if name in config.accounts:
            return config.accounts[name]
    # Fall back to global
    return get_account(name)


def format_date(dt: datetime | None) -> str:
    return dt.strftime("%Y-%m-%d") if dt else "?"


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


# =============================================================================
# Decorators and Click helpers
# =============================================================================


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
tag_option = click.option('-t', '--tag', help="Tag for organizing messages")


def validate_layout(ctx, param, value):
    """Validate layout is a preset name or valid template."""
    if not is_valid_layout(value):
        raise click.BadParameter(
            f"Invalid layout. Use a preset ({', '.join(PRESETS.keys())}, sqlite) "
            "or a template containing $variables"
        )
    return value


class AliasGroup(click.Group):
    """Click Group that supports command aliases."""

    def __init__(self, *args, aliases: dict[str, str] | None = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.aliases = aliases or {}
        # Build reverse mapping: command -> list of aliases
        self._cmd_aliases: dict[str, list[str]] = {}
        for alias, cmd in self.aliases.items():
            self._cmd_aliases.setdefault(cmd, []).append(alias)

    def get_command(self, ctx, cmd_name):
        # Check for alias
        cmd_name = self.aliases.get(cmd_name, cmd_name)
        return super().get_command(ctx, cmd_name)

    def resolve_command(self, ctx, args):
        # Resolve alias before dispatching
        _, cmd_name, args = super().resolve_command(ctx, args)
        cmd_name = self.aliases.get(cmd_name, cmd_name)
        return _, cmd_name, args

    def format_commands(self, ctx, formatter):
        """Write all commands with their aliases to the formatter."""
        commands = []
        for subcommand in self.list_commands(ctx):
            cmd = self.get_command(ctx, subcommand)
            if cmd is None or cmd.hidden:
                continue
            # Get aliases for this command
            aliases = self._cmd_aliases.get(subcommand, [])
            if aliases:
                name = f"{subcommand} ({', '.join(sorted(aliases))})"
            else:
                name = subcommand
            help_text = cmd.get_short_help_str(limit=formatter.width)
            commands.append((name, help_text))

        if commands:
            with formatter.section("Commands"):
                formatter.write_dl(commands)
