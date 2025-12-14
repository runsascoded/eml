"""V2 configuration and state management via YAML files."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

import yaml

LayoutType = Literal[
    "tree:flat", "tree:year", "tree:month", "tree:day", "tree:hash2", "sqlite"
]

EML_DIR = ".eml"
CONFIG_FILE = "config.yaml"
SYNC_STATE_DIR = "sync-state"
PUSHED_DIR = "pushed"


@dataclass
class AccountConfig:
    """An IMAP account configuration."""
    name: str
    type: str  # "gmail", "zoho", "imap"
    user: str
    password: str
    host: str | None = None
    port: int = 993


@dataclass
class EmlConfig:
    """Top-level eml project configuration."""
    layout: LayoutType = "tree:month"
    accounts: dict[str, AccountConfig] = field(default_factory=dict)


@dataclass
class FolderSyncState:
    """Sync state for a single IMAP folder."""
    uidvalidity: int
    last_uid: int


def find_eml_root(start: Path | None = None) -> Path | None:
    """Find eml project root (directory containing .eml/)."""
    path = (start or Path.cwd()).resolve()
    while path != path.parent:
        if (path / EML_DIR).is_dir():
            return path
        path = path.parent
    return None


def get_eml_root(require: bool = True) -> Path:
    """Get eml project root, raising if not found and require=True."""
    root = find_eml_root()
    if not root and require:
        raise FileNotFoundError(
            "Not in an eml project. Run 'eml init' first."
        )
    return root or Path.cwd()


def get_config_path(root: Path | None = None) -> Path:
    """Get path to config.yaml."""
    root = root or get_eml_root()
    return root / EML_DIR / CONFIG_FILE


def load_config(root: Path | None = None) -> EmlConfig:
    """Load config from config.yaml."""
    config_path = get_config_path(root)
    if not config_path.exists():
        return EmlConfig()

    with open(config_path) as f:
        data = yaml.safe_load(f) or {}

    accounts = {}
    for name, acct_data in data.get("accounts", {}).items():
        accounts[name] = AccountConfig(
            name=name,
            type=acct_data.get("type", "imap"),
            user=acct_data.get("user", ""),
            password=acct_data.get("password", ""),
            host=acct_data.get("host"),
            port=acct_data.get("port", 993),
        )

    return EmlConfig(
        layout=data.get("layout", "tree:month"),
        accounts=accounts,
    )


def save_config(config: EmlConfig, root: Path | None = None) -> None:
    """Save config to config.yaml."""
    config_path = get_config_path(root)
    config_path.parent.mkdir(parents=True, exist_ok=True)

    data = {"layout": config.layout}
    if config.accounts:
        data["accounts"] = {}
        for name, acct in config.accounts.items():
            acct_data = {
                "type": acct.type,
                "user": acct.user,
                "password": acct.password,
            }
            if acct.host:
                acct_data["host"] = acct.host
            if acct.port != 993:
                acct_data["port"] = acct.port
            data["accounts"][name] = acct_data

    with open(config_path, "w") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False)


def get_account(name: str, root: Path | None = None) -> AccountConfig | None:
    """Get account by name from config."""
    config = load_config(root)
    return config.accounts.get(name)


# --- Sync State ---


def get_sync_state_path(account: str, root: Path | None = None) -> Path:
    """Get path to sync state file for an account."""
    root = root or get_eml_root()
    # Replace / with _ in account name for filesystem safety
    safe_name = account.replace("/", "_")
    return root / EML_DIR / SYNC_STATE_DIR / f"{safe_name}.yaml"


def load_sync_state(account: str, root: Path | None = None) -> dict[str, FolderSyncState]:
    """Load sync state for an account. Returns folder -> FolderSyncState."""
    path = get_sync_state_path(account, root)
    if not path.exists():
        return {}

    with open(path) as f:
        data = yaml.safe_load(f) or {}

    result = {}
    for folder, state in data.items():
        if isinstance(state, dict):
            result[folder] = FolderSyncState(
                uidvalidity=state.get("uidvalidity", 0),
                last_uid=state.get("last_uid", 0),
            )
    return result


def save_sync_state(
    account: str,
    state: dict[str, FolderSyncState],
    root: Path | None = None,
) -> None:
    """Save sync state for an account."""
    path = get_sync_state_path(account, root)
    path.parent.mkdir(parents=True, exist_ok=True)

    data = {}
    for folder, folder_state in state.items():
        data[folder] = {
            "uidvalidity": folder_state.uidvalidity,
            "last_uid": folder_state.last_uid,
        }

    with open(path, "w") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False)


def get_folder_sync_state(
    account: str,
    folder: str,
    root: Path | None = None,
) -> FolderSyncState | None:
    """Get sync state for a specific folder."""
    state = load_sync_state(account, root)
    return state.get(folder)


def set_folder_sync_state(
    account: str,
    folder: str,
    uidvalidity: int,
    last_uid: int,
    root: Path | None = None,
) -> None:
    """Update sync state for a specific folder."""
    state = load_sync_state(account, root)
    state[folder] = FolderSyncState(uidvalidity=uidvalidity, last_uid=last_uid)
    save_sync_state(account, state, root)


# --- Push State ---


def get_pushed_path(account: str, root: Path | None = None) -> Path:
    """Get path to pushed manifest for an account."""
    root = root or get_eml_root()
    safe_name = account.replace("/", "_")
    return root / EML_DIR / PUSHED_DIR / f"{safe_name}.txt"


def load_pushed(account: str, root: Path | None = None) -> set[str]:
    """Load set of Message-IDs that have been pushed to account."""
    path = get_pushed_path(account, root)
    if not path.exists():
        return set()

    message_ids = set()
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                message_ids.add(line)
    return message_ids


def save_pushed(
    account: str,
    message_ids: set[str],
    root: Path | None = None,
) -> None:
    """Save pushed manifest for an account (sorted for stable diffs)."""
    path = get_pushed_path(account, root)
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w") as f:
        for msg_id in sorted(message_ids):
            f.write(f"{msg_id}\n")


def mark_pushed(
    account: str,
    message_id: str,
    root: Path | None = None,
) -> None:
    """Mark a single message as pushed to account."""
    pushed = load_pushed(account, root)
    pushed.add(message_id)
    save_pushed(account, pushed, root)


def is_pushed(
    account: str,
    message_id: str,
    root: Path | None = None,
) -> bool:
    """Check if a message has been pushed to account."""
    pushed = load_pushed(account, root)
    return message_id in pushed
