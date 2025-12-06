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
# Create .env with credentials
cat > .env << 'EOF'
GMAIL_USER=you@gmail.com
GMAIL_APP_PASSWORD=xxxx-xxxx-xxxx-xxxx
EOF

# List Gmail folders/labels
eml folders

# Pull emails from a Gmail label to local storage
eml pull -f "MyLabel" -o emails.db

# Pull with config file
eml pull -c pull.yml
```

## Commands

### `eml folders`

List folders/labels for an IMAP account:

```bash
eml folders                          # Gmail (uses GMAIL_USER env)
eml folders -h zoho -u you@zoho.com  # Zoho
eml folders -h imap.example.com      # Custom IMAP
```

### `eml pull`

Pull emails from IMAP to local SQLite storage:

```bash
eml pull                             # Pull from Gmail All Mail
eml pull -f "Work" -o work.db        # Pull specific label
eml pull -c pull.yml                 # Use config file
eml pull -n                          # Dry run
eml pull -v                          # Verbose output
```

Config file (`pull.yml`):

```yaml
src:
  type: gmail
  folder: "Work"
storage: emails.db
```

### `eml migrate`

Direct IMAP-to-IMAP migration (Gmail → Zoho):

```bash
eml migrate -c migrate.yml -n        # Dry run
eml migrate -c migrate.yml           # Actually migrate
eml migrate -a addr@example.com -n   # Filter by address
```

Config file (`migrate.yml`):

```yaml
filters:
  addresses:
    - team@googlegroups.com
  domains:
    - company.com
folder: INBOX
```

## Architecture

```
Sources (IMAP, mbox, .eml)
    ↓ pull
Local Storage (SQLite)
    ↓ push
Destinations (IMAP, static HTML)
```

## Features

- **Pull**: Fetch from IMAP to local SQLite with incremental sync (UIDVALIDITY tracking)
- **Deduplication**: By Message-ID across sources
- **Preserves**: Original dates, threading, attachments
- **Dry-run mode**: Test before committing changes

## Planned

- `eml push` - Send from local storage to IMAP destination
- `eml ls` - Query local storage
- `eml serve` - Web UI for browsing emails (pmail)

## License

MIT
