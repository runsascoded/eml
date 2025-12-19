"""V2 configuration and state management via YAML files."""

import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from .layouts.path_template import PRESETS, LEGACY_PRESETS, resolve_preset


def is_valid_layout(layout: str) -> bool:
    """Check if a layout string is valid (preset name or template)."""
    # Preset names
    if layout in PRESETS or layout in LEGACY_PRESETS:
        return True
    # "sqlite" is special
    if layout == "sqlite":
        return True
    # Otherwise must be a template (contain $)
    return "$" in layout

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
    layout: str = "default"  # Preset name or template string
    accounts: dict[str, AccountConfig] = field(default_factory=dict)


@dataclass
class FolderSyncState:
    """Sync state for a single IMAP folder."""
    uidvalidity: int
    last_uid: int


def find_eml_root(start: Path | None = None) -> Path | None:
    """Find eml project root (directory containing .eml/).

    First checks EML_ROOT environment variable, then walks up from start/cwd.
    """
    # Check EML_ROOT env var first
    env_root = os.environ.get("EML_ROOT")
    if env_root:
        env_path = Path(env_root).resolve()
        if (env_path / EML_DIR).is_dir():
            return env_path

    # Fall back to walking up from start/cwd
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
        layout=data.get("layout", "default"),
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


# --- Failure Tracking ---

FAILURES_DIR = "failures"


@dataclass
class PullFailure:
    """A failed pull attempt for a specific UID."""
    uid: int
    error: str
    timestamp: str | None = None


def _sanitize_error(error: str) -> str:
    """Strip nested PullFailure(...) wrappers from error strings.

    This fixes a bug where PullFailure objects were accidentally stringified
    and stored, creating exponentially growing nested strings.
    """
    import re
    # Keep stripping PullFailure(...) wrappers until we get to the actual error
    pattern = r"^PullFailure\(uid=\d+,\s*error=['\"](.+)['\"](?:,\s*timestamp=.*)?\)$"
    max_iterations = 100  # Safety limit
    for _ in range(max_iterations):
        # Unescape the string first (handle \\' -> ' etc)
        unescaped = error.replace("\\'", "'").replace('\\"', '"').replace("\\\\", "\\")
        match = re.match(pattern, unescaped, re.DOTALL)
        if match:
            error = match.group(1)
        else:
            break
    return error


def get_failures_path(account: str, folder: str, root: Path | None = None) -> Path:
    """Get path to failures file for an account/folder."""
    root = root or get_eml_root()
    safe_account = account.replace("/", "_")
    safe_folder = folder.replace("/", "_")
    return root / EML_DIR / FAILURES_DIR / f"{safe_account}_{safe_folder}.yaml"


def load_failures(
    account: str,
    folder: str,
    root: Path | None = None,
) -> dict[int, PullFailure]:
    """Load failures for an account/folder. Returns {uid: PullFailure}."""
    path = get_failures_path(account, folder, root)
    if not path.exists():
        return {}

    with open(path) as f:
        data = yaml.safe_load(f) or {}

    failures = {}
    for uid, info in data.items():
        if isinstance(info, dict):
            error = _sanitize_error(info.get("error", ""))
            failures[int(uid)] = PullFailure(
                uid=int(uid),
                error=error,
                timestamp=info.get("timestamp"),
            )
        else:
            error = _sanitize_error(str(info))
            failures[int(uid)] = PullFailure(uid=int(uid), error=error)
    return failures


def save_failures(
    account: str,
    folder: str,
    failures: dict[int, PullFailure],
    root: Path | None = None,
) -> None:
    """Save failures for an account/folder."""
    path = get_failures_path(account, folder, root)

    if not failures:
        if path.exists():
            path.unlink()
        return

    path.parent.mkdir(parents=True, exist_ok=True)
    data = {}
    for uid, failure in sorted(failures.items()):
        data[uid] = {"error": failure.error}
        if failure.timestamp:
            data[uid]["timestamp"] = failure.timestamp
    with open(path, "w") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False)


def add_failure(
    account: str,
    folder: str,
    uid: int,
    error: str,
    root: Path | None = None,
) -> None:
    """Record a pull failure for a specific UID."""
    from datetime import datetime
    failures = load_failures(account, folder, root)
    failures[uid] = PullFailure(
        uid=uid,
        error=error,
        timestamp=datetime.now().isoformat(),
    )
    save_failures(account, folder, failures, root)


def clear_failure(
    account: str,
    folder: str,
    uid: int,
    root: Path | None = None,
) -> None:
    """Remove a failure record (e.g., after successful retry)."""
    failures = load_failures(account, folder, root)
    if uid in failures:
        del failures[uid]
        save_failures(account, folder, failures, root)


def clear_failures(
    account: str,
    folder: str,
    root: Path | None = None,
) -> None:
    """Clear all failures for an account/folder."""
    path = get_failures_path(account, folder, root)
    if path.exists():
        path.unlink()
