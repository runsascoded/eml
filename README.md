# emails

Migrate emails between IMAP mailboxes with flexible filtering.

## Installation

```bash
pip install emails
# or
uv add emails
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
emails folders

# Dry run migration with config file
emails migrate -c config.yml -n

# Migrate with CLI flags
emails migrate -a team@googlegroups.com -D company.com -n
```

## Commands

### `emails folders`

List folders/labels for an IMAP account:

```bash
emails folders                          # Gmail (uses GMAIL_USER env)
emails folders -h zoho -u you@zoho.com  # Zoho
emails folders -h imap.example.com      # Custom IMAP
```

### `emails migrate`

Migrate emails between mailboxes:

```bash
emails migrate -c config.yml -n         # Dry run with config
emails migrate -c config.yml            # Actually migrate
emails migrate -a addr@example.com -n   # Filter by address
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
- `emails pull` - Fetch from IMAP to local SQLite/Maildir
- `emails push` - Send from local storage to destination
- `emails ls` - Query local storage
- `emails serve` - Serve web UI for browsing emails (pmail)

## License

MIT
