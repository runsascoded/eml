# EML Storage V2: Git-Based Email Archives

## Overview

EML v2 stores emails in a Git repository. Each branch represents a **migration pipeline** (source account → destination account). Git provides versioning, branching, and worktrees; EML provides IMAP pull/push operations.

```
my-email-archive/
  .git/                     # standard git
  .eml/
    config.yaml             # accounts with credentials, layout (tracked)
    sync-state/
      <account>.yaml        # UIDVALIDITY, last UID per folder (tracked)
    pushed/
      <account>.txt         # Message-IDs pushed (tracked)
  INBOX/                    # IMAP folders as directories
    a1b2c3d4.eml
    e5f6g7h8.eml
  Sent/
    ...
```

## Core Concepts

### Branch = Migration Pipeline

Each branch models a migration from one IMAP account to another:

```
branch: cdonnelly     y/cdonnelly (Yahoo) → g/cdonnelly (Gmail)
branch: asmith        y/asmith (Yahoo) → g/asmith (Gmail)
branch: bjones        y/bjones (Yahoo) → g/bjones (Gmail)
```

Use git worktrees to work on multiple migrations in parallel:

```bash
git worktree add ../asmith asmith
git worktree add ../bjones bjones
```

Each branch has its own:
- Account configurations (typically 2: source + destination)
- Credentials (tracked, since branch is the security boundary)
- Sync state (what's been pulled from source)
- Push state (what's been pushed to destination)
- Message files (.eml files in worktree)

### Storage Layouts

The `layout` setting controls how messages are stored:

| Layout | Storage |
|--------|---------|
| `tree:flat` | `INBOX/a1b2c3d4.eml` |
| `tree:year` | `INBOX/2024/a1b2c3d4.eml` |
| `tree:month` | `INBOX/2024/01/a1b2c3d4.eml` |
| `tree:day` | `INBOX/2024/01/15/a1b2c3d4.eml` |
| `tree:hash2` | `INBOX/a1/b2c3d4e5.eml` |
| `sqlite` | `.eml/msgs.db` (blobs) |

All layouts use the same metadata format (config.yaml, sync-state/, pushed/). Only message storage differs.

Convert between layouts:

```bash
eml convert tree:month    # convert to monthly sharding
eml convert sqlite        # pack into SQLite
```

### .eml/ Directory

Internal bookkeeping (all tracked in git):

| File | Purpose |
|------|---------|
| `config.yaml` | Accounts (with credentials) + layout setting |
| `sync-state/<account>.yaml` | UIDVALIDITY, last UID per folder |
| `pushed/<account>.txt` | Message-IDs already pushed |

Since these files are tracked, they're automatically branch-aware.

## Configuration

### config.yaml

```yaml
layout: tree:month    # or: tree:flat, tree:year, sqlite, etc.

accounts:
  y/cdonnelly:
    type: imap
    host: imap.turbify.com
    port: 993
    user: cdonnelly@embankment.org
    password: yahoo-app-password

  g/cdonnelly:
    type: gmail
    user: cdonnelly@embankment.org
    password: gmail-app-password
```

Account names can contain slashes for namespacing (e.g., `y/cdonnelly`, `g/cdonnelly`).

Passwords can also come from environment variables: `EML_<ACCOUNT>_PASSWORD` (with `/` replaced by `_`, uppercased).

### sync-state/<account>.yaml

Tracks pull progress per source account:

```yaml
INBOX:
  uidvalidity: 123456
  last_uid: 5000
Sent:
  uidvalidity: 789012
  last_uid: 300
```

### pushed/<account>.txt

Message-IDs already pushed to destination, one per line, sorted:

```
<abc123@example.com>
<def456@example.com>
<ghi789@example.com>
```

## File Layout (tree layouts)

### IMAP Folder Mapping

IMAP folders map directly to filesystem directories:

| IMAP Folder | Filesystem Path |
|-------------|-----------------|
| `INBOX` | `INBOX/` |
| `Sent` | `Sent/` |
| `[Gmail]/All Mail` | `[Gmail]/All Mail/` |
| `Work/Projects/2024` | `Work/Projects/2024/` |

### Filenames

Format: `<sha256(message-id)[:8]>.eml`

- Deterministic: same email always gets same filename
- Filesystem-safe: no special characters
- Short but collision-resistant for typical mailbox sizes
- Message-ID preserved in file content (authoritative)

Example: Message-ID `<abc123@example.com>` → `a1b2c3d4.eml`

### Sharding

Date-based sharding extracts the date from the email's `Date` header.
Hash-based sharding uses the Message-ID hash.

## Commands

### `eml init`

Initialize a new eml repository:

```bash
eml init                        # default layout: tree:month
eml init --layout sqlite        # use SQLite storage
eml init --layout tree:flat     # flat directory structure
```

Creates:
- `.eml/` directory structure
- Runs `git init` if not already a git repo

### `eml account`

Manage IMAP accounts:

```bash
eml account add y/cdonnelly imap cdonnelly@embankment.org --host imap.turbify.com
eml account add g/cdonnelly gmail cdonnelly@embankment.org

eml account ls                  # list configured accounts
eml account rm <name>           # remove account
```

### `eml pull`

Fetch from IMAP to local storage:

```bash
eml pull <account>
eml pull y/cdonnelly
eml pull y/cdonnelly --folder INBOX     # specific folder only
eml pull y/cdonnelly --commit           # auto-commit after pull
```

Behavior:
1. Connect to IMAP account
2. For each folder (or specified folder):
   - Fetch all messages (or incremental since last pull)
   - Store according to layout
   - Skip messages that already exist (by Message-ID)
3. Update sync-state
4. Optionally commit

### `eml push`

Upload from local storage to IMAP:

```bash
eml push <account>
eml push g/cdonnelly
eml push g/cdonnelly --folder INBOX     # push to specific folder
eml push g/cdonnelly --dry-run
eml push g/cdonnelly --delay 2          # rate limiting
```

Behavior:
1. Read `pushed/<account>.txt` (Message-IDs already pushed)
2. Iterate local messages, find unpushed
3. Upload each new message to IMAP, preserving original Date
4. Append pushed Message-IDs to manifest
5. Commit manifest update

### `eml convert`

Convert between storage layouts:

```bash
eml convert tree:month          # reorganize to monthly sharding
eml convert tree:flat           # flatten to single directory
eml convert sqlite              # pack into SQLite database
```

### `eml ls`

List/search messages:

```bash
eml ls                          # list recent messages
eml ls --from "john@"           # filter by From
eml ls "search term"            # search content
```

For tree layouts, this parses .eml files. For sqlite, queries the database.

### Git Pass-through

Users interact with Git directly for:

```bash
git log                         # history
git diff                        # changes since last commit
git grep "search term"          # search email content (tree layouts)
git worktree add ../other branch  # parallel migrations
```

## Push State Tracking

### Message-ID Manifest

`pushed/<account>.txt` is the source of truth for "what's on the remote".

### Git Ref (optional optimization)

`refs/eml/pushed/<account>` points to the commit that was last pushed. Used as hint for incremental diff.

### Handling Modifications

| Action | Effect on next push |
|--------|---------------------|
| Add messages | Uploaded (exists locally but not in manifest) |
| Remove messages | No effect (still in manifest, stays on remote) |
| `--sync` flag | Deletes from remote if removed locally (opt-in) |

## Edge Cases

### Reserved Folder Names

Error on pull if IMAP has folders named `.git` or `.eml`.

### IMAP Folder Normalization

| Issue | Handling |
|-------|----------|
| `INBOX` case | Normalize to uppercase |
| UTF-7 encoding | Decode to UTF-8 |
| Windows reserved names | Warn, allow with escape prefix |

### Gmail-Specific

Gmail's special folders use `[Gmail]/` prefix and are preserved as-is.

## Migration from V1

V1 uses SQLite with push_state/sync_state tables. Migration:

```bash
eml migrate-v2
```

1. Read messages from `msgs.db`
2. Write according to v2 layout
3. Generate sync-state from `sync_state` table
4. Generate push manifests from `push_state` table

## Security Considerations

### Credential Storage

Credentials are stored in `config.yaml` and tracked in git. This is intentional:
- Each branch is a migration pipeline (security boundary)
- Repo should never be pushed to public git remotes
- Use app-specific passwords, not main account passwords

### Sensitive Content Warning

Email archives are sensitive. Consider:
- Never push to github.com/gitlab.com (use private hosting or local only)
- `eml push` (IMAP) vs `git push` (git remote) are distinct operations
- Pre-push hook can warn about public remotes

## StorageLayout Interface

```python
class StorageLayout(Protocol):
    def iter_messages(self) -> Iterator[StoredMessage]
    def get_message(message_id: str) -> StoredMessage | None
    def add_message(message_id: str, raw: bytes, date: datetime, ...) -> None
    def has_message(message_id: str) -> bool
    def count() -> int
```

Implementations:
- `SqliteLayout` - stores in `.eml/msgs.db`
- `TreeLayout` - stores as `.eml` files with configurable sharding
