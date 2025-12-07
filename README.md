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
# Initialize project
eml init

# Add accounts
eml account add gmail user@gmail.com      # prompts for password
eml account add zoho user@example.com

# Pull emails to local storage
eml pull gmail -f "Work" -t work          # tag as 'work'

# Push to destination
eml push zoho -t work -f Work             # push 'work' tagged messages
```

## Project Structure

```
.eml/
  msgs.db     # messages, sync_state, push_state, tags
  accts.db    # account credentials (local)

~/.config/eml/
  accts.db    # account credentials (global fallback)
```

## Commands

All commands have short aliases shown in parentheses.

### `eml init` (`i`)

Initialize project directory:

```bash
eml init                    # create .eml/ in current directory
eml init -g                 # create ~/.config/eml/ for global accounts
```

### `eml account` (`a`)

Manage IMAP accounts:

```bash
eml account add gmail user@gmail.com      # add local account (a a)
eml account add zoho user@example.com -g  # add global account
eml account ls                            # list accounts (a l)
eml account rm gmail                      # remove account (a r)
```

### `eml folders` (`f`)

List folders/labels:

```bash
eml folders gmail                # list all folders
eml folders gmail INBOX          # show message count
eml folders gmail -s "Work"      # show count and total size
```

### `eml pull` (`p`)

Pull emails from IMAP to local storage:

```bash
eml pull gmail                   # pull from All Mail
eml pull gmail -f "Work"         # pull from label
eml pull gmail -f "Work" -t work # pull and tag as 'work'
eml pull gmail -l 100            # limit to 100 messages
eml pull gmail -n                # dry run
```

Features:
- Progress bar with message count
- Incremental sync (only fetches new messages)
- Checkpoint saves every N messages (crash-safe)
- Optional tagging with `-t`

### `eml push` (`ps`)

Push emails from local storage to IMAP destination:

```bash
eml push zoho                    # push all messages
eml push zoho -t work            # push only 'work' tagged
eml push zoho -t work -f Work    # push to specific folder
eml push zoho -l 10 -v           # push 10, verbose
eml push zoho -n                 # dry run
```

### `eml ls`

Query local storage:

```bash
eml ls                           # list recent messages
eml ls -t work                   # list 'work' tagged
eml ls -l 50                     # show 50 messages
eml ls -f "john@"                # filter by From
eml ls "search term"             # search From/Subject
```

### `eml tags`

List all tags with counts:

```bash
eml tags
```

### `eml serve` (`s`)

Start pmail web UI:

```bash
eml serve                        # http://127.0.0.1:5000
eml serve -p 8080                # different port
```

### `eml migrate`

Direct IMAP-to-IMAP migration (legacy, without local storage):

```bash
eml migrate -c migrate.yml -n    # dry run with config
```

## Account Lookup

Accounts are looked up in order:
1. Local `.eml/accts.db`
2. Global `~/.config/eml/accts.db`

This allows project-specific credentials with global fallback.

## Architecture

```
Source IMAP
    ↓ pull (with optional tag)
Local Storage (.eml/msgs.db)
    ↓ push (filter by tag)
Destination IMAP
```

## Features

- **Project-based**: `.eml/` directory like `.git/`
- **Account management**: Local and global credential storage
- **Incremental sync**: Tracks UIDVALIDITY and last UID per folder
- **Tagging**: Organize pulled messages, filter on push
- **Checkpointing**: Saves progress periodically, crash-safe
- **Deduplication**: By Message-ID across sources
- **Preserves**: Original dates, threading, attachments

## License

MIT
