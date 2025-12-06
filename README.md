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
emails -a team@googlegroups.com -D company.com -n
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

## License

MIT
