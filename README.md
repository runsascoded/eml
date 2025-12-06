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
# Set credentials (or use -u/-p flags)
export GMAIL_USER=you@gmail.com
export GMAIL_APP_PASSWORD=xxxx-xxxx-xxxx-xxxx

# List folders
eml folders

# Pull emails to local storage
eml pull -f "MyLabel"

# Query local storage
eml ls

# Push to destination
eml push -h zoho -u you@zoho.com -p yourpass
```

## Commands

### `eml folders [FOLDER]`

List folders/labels, or show info for a specific folder:

```bash
eml folders                              # List all folders (uses GMAIL_* env)
eml folders "MyLabel"                    # Show message count
eml folders -s "MyLabel"                 # Show count and total size
eml folders -h zoho -u you@zoho.com      # Query different account
eml folders -h imap.example.com -u user  # Any IMAP server
```

### `eml pull`

Pull emails from IMAP to local SQLite storage:

```bash
eml pull -f "Work"                       # Pull from label (uses GMAIL_* env)
eml pull -h zoho -u you@zoho.com         # Pull from different account
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
eml push -h zoho -u you@zoho.com         # Push to Zoho (uses ZOHO_* env)
eml push -h gmail -u other@gmail.com     # Push to Gmail
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
| `GMAIL_USER` | `folders`, `pull` | Gmail/source username |
| `GMAIL_APP_PASSWORD` | `folders`, `pull` | Gmail/source password |
| `ZOHO_USER` | `push` | Zoho/destination username |
| `ZOHO_PASSWORD` | `push` | Zoho/destination password |

Override with `-u`/`-p` for any IMAP server.

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
