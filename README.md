# eml

Email migration and archival tool with local SQLite storage.

## Installation

```bash
pip install eml
# or
uv add eml
```

## Quick Start

```bash
# Set source credentials (or use -u/-p flags)
export SRC_USER=you@gmail.com
export SRC_PASS=xxxx-xxxx-xxxx-xxxx

# List folders
eml folders

# Pull emails to local storage
eml pull -f "MyLabel"

# Query local storage
eml ls

# Set destination credentials and push
export DST_USER=you@zoho.com
export DST_PASS=your-password
eml push
```

## Commands

### `eml folders [FOLDER]`

List folders/labels, or show info for a specific folder:

```bash
eml folders                              # List all folders
eml folders "MyLabel"                    # Show message count
eml folders -s "MyLabel"                 # Show count and total size
eml folders -h imap.other.com -u user    # Query different account
```

### `eml pull`

Pull emails from IMAP to local SQLite storage:

```bash
eml pull -f "Work"                       # Pull from label
eml pull -h imap.other.com -u user       # Pull from different account
eml pull -f "Work" -o work.db            # Custom database path
eml pull -b 50                           # Checkpoint every 50 messages
eml pull -n                              # Dry run
```

Features:
- Progress bar with message count
- Incremental sync (only fetches new messages)
- Checkpoint saves every N messages (crash-safe)

### `eml push`

Push emails from local storage to IMAP destination:

```bash
eml push                                 # Push to destination
eml push -h imap.other.com -u user       # Push to different account
eml push -f "Archive"                    # Push to specific folder
eml push -n                              # Dry run
```

### `eml ls`

Query local storage:

```bash
eml ls                                   # List recent messages
eml ls -l 50                             # Show 50 messages
eml ls -f "john@"                        # Filter by From
eml ls -s "invoice"                      # Filter by subject
eml ls "search term"                     # Search From/Subject
```

### `eml migrate`

Direct IMAP-to-IMAP migration (without local storage):

```bash
eml migrate -c migrate.yml -n            # Dry run with config
eml migrate -a addr@example.com -n       # Filter by address
```

## Environment Variables

Credentials can be set via environment or `-u`/`-p` flags:

| Variable | Used by | Description |
|----------|---------|-------------|
| `SRC_USER` | `folders`, `pull` | Source IMAP username |
| `SRC_PASS` | `folders`, `pull` | Source IMAP password |
| `DST_USER` | `push` | Destination IMAP username |
| `DST_PASS` | `push` | Destination IMAP password |

## Architecture

```
Source IMAP
    ↓ pull
Local Storage (SQLite)
    ↓ push
Destination IMAP
```

## Features

- **Incremental sync**: Tracks UIDVALIDITY and last UID per folder
- **Checkpointing**: Saves progress periodically, crash-safe
- **Deduplication**: By Message-ID across sources
- **Preserves**: Original dates, threading, attachments
- **Progress bar**: Visual feedback for long operations

## Planned

- `eml serve` - Web UI for browsing emails (pmail)

## License

MIT
