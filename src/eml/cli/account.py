"""Account management commands."""

import sys

import click
from click import argument, echo, option

from ..config import AccountConfig, find_eml_root, load_config, save_config
from ..storage import ACCTS_DB, AccountStorage, GLOBAL_CONFIG_DIR, find_eml_dir

from .utils import AliasGroup, err, get_password, has_config


@click.group(cls=AliasGroup, aliases={
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
