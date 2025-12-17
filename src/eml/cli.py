"""CLI for email migration."""

import atexit
import email
import imaplib
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta
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
from .config import (
    EmlConfig, AccountConfig, PullFailure, is_valid_layout,
    find_eml_root, get_eml_root, load_config, save_config,
    get_folder_sync_state, set_folder_sync_state,
    load_pushed, mark_pushed, is_pushed,
    load_failures, save_failures, add_failure, clear_failure, clear_failures,
    get_failures_path,
)
from .layouts import (
    StorageLayout, StoredMessage, TreeLayout, SqliteLayout,
    PRESETS, LEGACY_PRESETS, resolve_preset,
)


def err(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def has_config(root: Path | None = None) -> bool:
    """Check if project has config.yaml."""
    root = root or find_eml_root()
    if not root:
        return False
    return (root / ".eml" / "config.yaml").exists()


# Pull status file helpers - for tracking active pulls in this worktree
PULL_STATUS_FILE = "pull-status.json"

def get_pull_status_path(root: Path | None = None) -> Path:
    """Get path to pull status file."""
    root = root or get_eml_root()
    return root / ".eml" / PULL_STATUS_FILE

def write_pull_status(
    account: str,
    folder: str,
    total: int,
    completed: int = 0,
    root: Path | None = None,
) -> None:
    """Write pull status to file."""
    root = root or get_eml_root()
    status_path = get_pull_status_path(root)
    status = {
        "pid": os.getpid(),
        "account": account,
        "folder": folder,
        "total": total,
        "completed": completed,
        "started": datetime.now().isoformat(),
    }
    status_path.write_text(json.dumps(status, indent=2) + "\n")

def update_pull_status(completed: int, root: Path | None = None) -> None:
    """Update completed count in pull status file."""
    root = root or get_eml_root()
    status_path = get_pull_status_path(root)
    if not status_path.exists():
        return
    try:
        status = json.loads(status_path.read_text())
        status["completed"] = completed
        status_path.write_text(json.dumps(status, indent=2) + "\n")
    except Exception:
        pass

def clear_pull_status(root: Path | None = None) -> None:
    """Remove pull status file."""
    root = root or get_eml_root()
    status_path = get_pull_status_path(root)
    if status_path.exists():
        status_path.unlink()

def read_pull_status(root: Path | None = None) -> dict | None:
    """Read pull status from file. Returns None if no active pull or stale."""
    root = root or get_eml_root()
    status_path = get_pull_status_path(root)
    if not status_path.exists():
        return None
    try:
        status = json.loads(status_path.read_text())
        pid = status.get("pid")
        # Check if process is still running
        if pid:
            try:
                os.kill(pid, 0)  # Signal 0 just checks if process exists
            except OSError:
                # Process not running, stale status file
                status_path.unlink()
                return None
        return status
    except Exception:
        return None


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


# Main group with aliases
@click.group(cls=AliasGroup, aliases={
    'a': 'account',
    'cv': 'convert',
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

def validate_layout(ctx, param, value):
    """Validate layout is a preset name or valid template."""
    if not is_valid_layout(value):
        raise click.BadParameter(
            f"Invalid layout. Use a preset ({', '.join(PRESETS.keys())}, sqlite) "
            "or a template containing $variables"
        )
    return value


@main.command()
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
        import subprocess
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


# ============================================================================
# account (with aliases)
# ============================================================================

@main.group(cls=AliasGroup, aliases={
    'a': 'add',
    'l': 'ls',
    'r': 'rename',
})
def account():
    """Manage IMAP accounts."""
    pass


@account.command("add", no_args_is_help=True)
@option('-g', '--global', 'use_global', is_flag=True, help="Add to global config")
@option('-H', '--host', help="IMAP host (for generic imap type)")
@option('-p', '--password', 'password_opt', help="Password (prompts if not provided)")
@option('-P', '--port', type=int, default=993, help="IMAP port")
@option('-t', '--type', 'acct_type', help="Account type (gmail, zoho, imap)")
@argument('name')
@argument('user')
def account_add(
    use_global: bool,
    host: str | None,
    password_opt: str | None,
    port: int,
    acct_type: str | None,
    name: str,
    user: str,
):
    """Add or update an account.

    \b
    Examples:
      eml account add g/user gmail user@gmail.com
      eml account add y/user imap user@example.com --host imap.example.com
      eml a a gmail user@gmail.com              # using aliases
      echo "$PASS" | eml account add zoho user@example.com
      eml account add gmail user@gmail.com -g   # global account (V1 only)
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

    # Global accounts always use V1 SQLite
    if use_global:
        accts_path = GLOBAL_CONFIG_DIR / ACCTS_DB
        GLOBAL_CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        with AccountStorage(accts_path) as storage:
            storage.add(name, acct_type, user, password)
        echo(f"Account '{name}' saved ({acct_type}: {user}) [global]")
        return

    # Check for V2 project
    root = find_eml_root()
    if root and has_config(root):
        #  store in config.yaml
        config = load_config(root)
        config.accounts[name] = AccountConfig(
            name=name,
            type=acct_type,
            user=user,
            password=password,
            host=host,
            port=port,
        )
        save_config(config, root)
        echo(f"Account '{name}' saved ({acct_type}: {user}) [config.yaml]")
    else:
        # Legacy:  store in accts.db
        eml_dir = find_eml_dir()
        if not eml_dir:
            err("Not in an eml project. Run 'eml init' first, or use -g for global.")
            sys.exit(1)
        with AccountStorage(eml_dir / ACCTS_DB) as storage:
            storage.add(name, acct_type, user, password)
        echo(f"Account '{name}' saved ({acct_type}: {user}) [local]")


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
    accounts_found = False

    # V2 local accounts (config.yaml)
    root = find_eml_root()
    if not use_global and root and has_config(root):
        config = load_config(root)
        if config.accounts:
            accounts_found = True
            config_path = root / ".eml" / "config.yaml"
            echo(f"Accounts ({config_path}):\n")
            for name, acct in sorted(config.accounts.items()):
                host_info = f" ({acct.host})" if acct.host else ""
                echo(f"  {name:20} {acct.type:10} {acct.user}{host_info}")
            echo()

    # V1 local accounts (accts.db)
    eml_dir = find_eml_dir()
    if not use_global and eml_dir and not has_config():
        local_accts_path = eml_dir / ACCTS_DB
        if local_accts_path.exists():
            with AccountStorage(local_accts_path) as storage:
                accounts = storage.list()
            if accounts:
                accounts_found = True
                echo(f"Local accounts ({local_accts_path}):\n")
                for acct in accounts:
                    echo(f"  {acct.name:20} {acct.type:10} {acct.user}")
                echo()

    # Global accounts (V1 SQLite)
    global_accts_path = GLOBAL_CONFIG_DIR / ACCTS_DB
    if (use_global or show_all or not accounts_found) and global_accts_path.exists():
        with AccountStorage(global_accts_path) as storage:
            accounts = storage.list()
        if accounts:
            accounts_found = True
            echo(f"Global accounts ({global_accts_path}):\n")
            for acct in accounts:
                echo(f"  {acct.name:20} {acct.type:10} {acct.user}")
            echo()

    if not accounts_found:
        echo("No accounts configured.")
        echo("  eml account add g/user gmail user@gmail.com")


@account.command("rm", no_args_is_help=True)
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
        if not accts_path.exists():
            err(f"No accounts database: {accts_path}")
            sys.exit(1)
        with AccountStorage(accts_path) as storage:
            removed = storage.remove(name)
        if removed:
            echo(f"Account '{name}' removed [global].")
        else:
            err(f"Account '{name}' not found.")
            sys.exit(1)
        return

    # Check for V2 project
    root = find_eml_root()
    if root and has_config(root):
        config = load_config(root)
        if name in config.accounts:
            del config.accounts[name]
            save_config(config, root)
            echo(f"Account '{name}' removed [config.yaml].")
        else:
            err(f"Account '{name}' not found.")
            sys.exit(1)
        return

    # Legacy:  remove from accts.db
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


@account.command("rename", no_args_is_help=True)
@option('-g', '--global', 'use_global', is_flag=True, help="Rename in global config")
@argument('old_name')
@argument('new_name')
def account_rename(use_global: bool, old_name: str, new_name: str):
    """Rename an account.

    \b
    Examples:
      eml account rename gmail g/user
      eml a mv y/old y/new           # using aliases
    """
    if use_global:
        accts_path = GLOBAL_CONFIG_DIR / ACCTS_DB
        if not accts_path.exists():
            err(f"No accounts database: {accts_path}")
            sys.exit(1)
        with AccountStorage(accts_path) as storage:
            acct = storage.get(old_name)
            if not acct:
                err(f"Account '{old_name}' not found.")
                sys.exit(1)
            storage.remove(old_name)
            storage.add(new_name, acct.type, acct.user, acct.password)
        echo(f"Account renamed: '{old_name}' → '{new_name}' [global]")
        return

    # Check for V2 project
    root = find_eml_root()
    if root and has_config(root):
        config = load_config(root)
        if old_name not in config.accounts:
            err(f"Account '{old_name}' not found.")
            sys.exit(1)
        if new_name in config.accounts:
            err(f"Account '{new_name}' already exists.")
            sys.exit(1)
        # Rename by moving the entry
        config.accounts[new_name] = config.accounts.pop(old_name)
        config.accounts[new_name].name = new_name
        save_config(config, root)
        echo(f"Account renamed: '{old_name}' → '{new_name}' [config.yaml]")
        return

    # Legacy:  rename in accts.db
    eml_dir = find_eml_dir()
    if not eml_dir:
        err("Not in an eml project. Run 'eml init' first, or use -g for global.")
        sys.exit(1)
    accts_path = eml_dir / ACCTS_DB
    if not accts_path.exists():
        err(f"No accounts database: {accts_path}")
        sys.exit(1)
    with AccountStorage(accts_path) as storage:
        acct = storage.get(old_name)
        if not acct:
            err(f"Account '{old_name}' not found.")
            sys.exit(1)
        storage.remove(old_name)
        storage.add(new_name, acct.type, acct.user, acct.password)
    echo(f"Account renamed: '{old_name}' → '{new_name}'")


# ============================================================================
# folders
# ============================================================================

@main.command(no_args_is_help=True)
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


# ============================================================================
# pull
# ============================================================================

@main.command(no_args_is_help=True)
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
@option('-u', '--user', help="IMAP username (overrides account)")
@option('-v', '--verbose', is_flag=True, help="Show each message")
@argument('account')
def pull(
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
        existing = read_pull_status(root)
        if existing:
            pid = existing.get("pid")
            acct = existing.get("account", "?")
            fldr = existing.get("folder", "?")
            err(f"Another pull is already running: {acct}/{fldr} [PID {pid}]")
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
        if has_cfg:
            layout = get_storage_layout(root) if not dry_run else None
        else:
            msgs_path = get_msgs_db_path()
            storage = MessageStorage(msgs_path)
            if not dry_run:
                storage.connect()

        # Check sync state (unless --full or --retry)
        stored_uidvalidity, last_uid = (None, None)
        if not dry_run and not full and not retry:
            if has_cfg:
                sync_state = get_folder_sync_state(account, src_folder, root)
                if sync_state:
                    stored_uidvalidity = sync_state.uidvalidity
                    last_uid = sync_state.last_uid
            else:
                stored_uidvalidity, last_uid = storage.get_sync_state(src_type, src_user, src_folder)

        if stored_uidvalidity and stored_uidvalidity != uidvalidity:
            echo(style(f"UIDVALIDITY changed ({stored_uidvalidity} → {uidvalidity}), doing full sync", fg="yellow"))
            last_uid = None

        # Load previous failures for this account/folder 
        failures = {}
        if has_cfg and not dry_run:
            failures = load_failures(account, src_folder, root)

        # Get UIDs to fetch
        if retry:
            if not failures:
                echo(style("No failures to retry", fg="yellow"))
                return
            # Convert int UIDs to bytes (as returned by IMAP search)
            uids = [str(uid).encode() for uid in sorted(failures.keys())]
            echo(f"Retrying {len(uids)} failed UIDs")
        elif full:
            echo("Full sync (--full)")
            uids = client.search("ALL")
        elif last_uid:
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
        consecutive_errors = 0
        aborted = False
        max_uid = last_uid or 0
        total = len(uids)
        console = Console()

        # Write pull status file (for `eml status` to read)
        if has_cfg and not dry_run:
            write_pull_status(account, src_folder, total, 0, root)
            # Register cleanup on exit (normal or abnormal)
            atexit.register(clear_pull_status, root)

        def save_checkpoint():
            if not dry_run and max_uid > 0:
                if has_cfg:
                    set_folder_sync_state(account, src_folder, uidvalidity, max_uid, root)
                else:
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
                    consecutive_errors += 1
                    if has_cfg and not dry_run:
                        failures[uid_int] = e
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

                # Check if already stored (by Message-ID if present)
                # Skip this check for empty Message-ID - will check content hash after fetch
                if not dry_run and info.message_id:
                    has_msg = layout.has_message(info.message_id) if has_cfg else storage.has_message(info.message_id)
                    if has_msg:
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

                    # Content-hash dedup (catches messages without Message-ID)
                    if has_cfg and layout.has_content(raw):
                        skipped += 1
                        if verbose:
                            print_result("skip", subj)
                        progress.advance(task)
                        continue

                    if has_cfg:
                        layout.add_message(
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
                        # Clear from failures if previously failed
                        if uid_int in failures:
                            del failures[uid_int]
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
                    consecutive_errors = 0  # Reset on success
                    if verbose:
                        print_result("ok", subj)
                except Exception as e:
                    failed += 1
                    consecutive_errors += 1
                    if has_cfg and not dry_run:
                        failures[uid_int] = e
                    if verbose:
                        print_result("fail", subj, str(e))

                progress.advance(task)

                # Check for rate limit (consecutive errors)
                if consecutive_errors >= max_errors:
                    console.print(f"\n[bold red]Aborting: {consecutive_errors} consecutive errors (likely rate limited)[/]")
                    aborted = True
                    break

                # Save checkpoint periodically
                if (i + 1) % checkpoint_interval == 0:
                    save_checkpoint()
                    if has_cfg and not dry_run:
                        update_pull_status(i + 1, root)

        # Final sync state update
        save_checkpoint()

        # Clear pull status file (we're done)
        if has_cfg and not dry_run:
            clear_pull_status(root)

        # Save failures to disk 
        if has_cfg and not dry_run:
            # Convert exception objects to PullFailure objects
            failure_records = {}
            for uid, exc in failures.items():
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
            if has_cfg and hasattr(layout, 'disconnect'):
                layout.disconnect()
            elif not has_cfg:
                storage.disconnect()

    except Exception as e:
        err(f"Error: {e}")
        sys.exit(1)
    finally:
        client.disconnect()


# ============================================================================
# push
# ============================================================================

@main.command(no_args_is_help=True)
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

        # Cleanup
        if has_cfg and hasattr(layout, 'disconnect'):
            layout.disconnect()
        elif not has_cfg:
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
# status
# ============================================================================

@main.command()
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
    from collections import defaultdict

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
    print()

    # Hourly distribution (last 24h) - bucket by wall clock hour
    print(f"{BOLD}Downloads by hour (last 24h):{RESET}")
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

    # Last 10 downloaded (oldest first, most recent at bottom)
    print(f"{BOLD}Last 10 downloaded:{RESET}")
    files.sort(reverse=True)
    for mtime, path in reversed(files[:10]):
        dt = datetime.fromtimestamp(mtime)
        rel_path = os.path.relpath(path, root)
        folder = os.path.dirname(rel_path)
        fname = os.path.basename(path).removesuffix('.eml')
        if len(fname) > 45:
            fname = fname[:42] + '...'
        print(f"  {DIM}{dt:%Y-%m-%d %H:%M:%S}{RESET} {GREEN}{folder}/{RESET}{fname}")
    print()

    # Check if pull running by reading local status file
    pull_status = read_pull_status(root)
    if pull_status:
        acct = pull_status.get("account", "?")
        fldr = pull_status.get("folder", "?")
        completed = pull_status.get("completed", 0)
        total_msgs = pull_status.get("total", 0)
        pid = pull_status.get("pid")
        pid_str = f" [PID {pid}]" if pid else ""
        if total_msgs > 0:
            pct = completed * 100 // total_msgs
            print(f"{GREEN}● Pull in progress: {acct}/{fldr} ({completed}/{total_msgs}, {pct}%){pid_str}{RESET}")
        else:
            print(f"{GREEN}● Pull in progress: {acct}/{fldr}{pid_str}{RESET}")
    else:
        print(f"{DIM}○ No pull running{RESET}")


# ============================================================================
# convert
# ============================================================================

@main.command(no_args_is_help=True)
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


# ============================================================================
# index
# ============================================================================

@main.command()
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
    from .index import FileIndex

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


# ============================================================================
# fsck
# ============================================================================

@main.command(no_args_is_help=True)
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
    from .index import FileIndex

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

    # Load or build index
    with FileIndex(eml_dir) as idx:
        if idx.file_count() == 0:
            echo("Building index first...")
            idx.rebuild()
            echo()

        local_message_ids = idx.all_message_ids()
        local_hashes = idx.all_content_hashes()

    echo(f"Local index: {len(local_message_ids):,} message IDs, {len(local_hashes):,} content hashes")

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

        echo(f"Server Message-IDs: {len(server_ids):,}")

        # Compare
        server_set = set(server_ids.keys())
        local_set = local_message_ids

        missing_from_local = server_set - local_set
        extra_local = local_set - server_set

        echo()
        echo(f"Missing from local: {len(missing_from_local):,}")
        echo(f"Extra in local:     {len(extra_local):,}")

        if output_json:
            import json as json_mod
            result = {
                "folder": folder,
                "server_count": len(server_ids),
                "local_count": len(local_message_ids),
                "missing_count": len(missing_from_local),
                "extra_count": len(extra_local),
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
                    for mid in sorted(missing_from_local)[:100]
                ]
            print(json_mod.dumps(result, indent=2))
        elif show_missing and missing_from_local:
            echo()
            echo("Missing messages:")
            for mid in sorted(missing_from_local)[:50]:
                info = server_ids[mid]
                date_str = info["date"][:16] if info["date"] else "?"
                from_str = info["from"][:30] if info["from"] else "?"
                subj_str = info["subject"][:40] if info["subject"] else "?"
                echo(f"  UID {info['uid']:>8}  {date_str}  {from_str}  {subj_str}")

            if len(missing_from_local) > 50:
                echo(f"  ... and {len(missing_from_local) - 50} more")

    finally:
        client.disconnect()


if __name__ == "__main__":
    main()
