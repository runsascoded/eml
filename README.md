# eml

Migrate emails between IMAP mailboxes with flexible filtering.

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
ZOHO_USER=you@example.com
ZOHO_PASSWORD=your-password
EOF

# List Gmail folders/labels
eml folders

# Dry run migration with config file
eml migrate -c config.yml -n

# Migrate with CLI flags
eml migrate -a team@googlegroups.com -D company.com -n
```

## Commands

### `eml folders`

List folders/labels for an IMAP account:

```bash
eml folders                          # Gmail (uses GMAIL_USER env)
eml folders -h zoho -u you@zoho.com  # Zoho
eml folders -h imap.example.com      # Custom IMAP
```

### `eml migrate`

Migrate emails between mailboxes:

```bash
eml migrate -c config.yml -n         # Dry run with config
eml migrate -c config.yml            # Actually migrate
eml migrate -a addr@example.com -n   # Filter by address
```

## Configuration

Create a YAML config file (see `example.yml`):

```yaml
filters:
  addresses:          # Match To/From/Cc (full address)
    - team@googlegroups.com
  domains:            # Match To/From/Cc (domain)
    - company.com
  from_addresses:     # Match From only (full address)
    - person@example.com
  from_domains:       # Match From only (domain)
    - vendor.com

folder: INBOX
start_date: 2020-01-01
end_date: 2024-12-31
```

## Filter Types

| Flag | Config Key | Matches |
|------|------------|---------|
| `-a` | `addresses` | To/From/Cc (full address) |
| `-D` | `domains` | To/From/Cc (domain) |
| `-F` | `from_addresses` | From only (full address) |
| `-d` | `from_domains` | From only (domain) |

## Features

- Preserves original dates, threading, and attachments
- Deduplication by Message-ID
- Date range filtering
- Dry-run mode for testing
- Progress display with batch limits

## Roadmap

Planned architecture separating pull/push operations with local storage:

```
Sources (IMAP, mbox, .eml)
    ↓ pull
Local Storage (SQLite, Maildir)
    ↓ push
Destinations (IMAP, static HTML)
    ↓ read
Public Readers (pmail webapp, JSON API)
```

Planned commands:
- `eml pull` - Fetch from IMAP to local SQLite/Maildir
- `eml push` - Send from local storage to destination
- `eml ls` - Query local storage
- `eml serve` - Serve web UI for browsing emails (pmail)

## License

MIT
