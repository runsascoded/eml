# emails

Migrate emails between IMAP mailboxes with flexible filtering.

## Installation

```bash
pip install emails
# or
uv add emails
```

## Usage

```bash
# Create .env with credentials
cat > .env << 'EOF'
GMAIL_USER=you@gmail.com
GMAIL_APP_PASSWORD=xxxx-xxxx-xxxx-xxxx
ZOHO_USER=you@example.com
ZOHO_PASSWORD=your-password
EOF

# Dry run with config file
emails -c config.yml -n

# Migrate with CLI flags
emails -a team@googlegroups.com -d company.com -n
```

## Configuration

Create a YAML config file (see `example.yml`):

```yaml
filters:
  addresses:          # Match To/From/Cc
    - team@googlegroups.com
  from_domains:       # Match From domain only
    - company.com
  from_addresses:     # Match From address only
    - person@example.com

folder: INBOX
start_date: 2020-01-01
end_date: 2024-12-31
```

## Filter Types

| Flag | Config Key | Matches |
|------|------------|---------|
| `-a` | `addresses` | To, From, or Cc |
| `-d` | `from_domains` | From domain only |
| `-F` | `from_addresses` | From address only |

## Features

- Preserves original dates, threading, and attachments
- Deduplication by Message-ID
- Date range filtering
- Dry-run mode for testing
- Progress display with batch limits

## License

MIT
