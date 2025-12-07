# eml - Email Migration Tool

## Project Overview

CLI tool for migrating emails between IMAP servers via local SQLite storage.

## Architecture

```
Source IMAP → pull → .eml/msgs.db → push → Destination IMAP
```

- `.eml/msgs.db` - messages, sync_state, push_state, message_tags
- `.eml/accts.db` - local account credentials
- `~/.config/eml/accts.db` - global account credentials (fallback)

## Key Files

- `src/eml/cli.py` - Click CLI with command aliases
- `src/eml/storage.py` - `MessageStorage`, `AccountStorage` classes
- `src/eml/imap.py` - `GmailClient`, `ZohoClient`, `IMAPClient`
- `www/app.py` - Flask pmail web UI

## Command Aliases

| Alias | Command |
|-------|---------|
| `i` | `init` |
| `a` | `account` (`a a`=add, `a l`=ls, `a r`=rm) |
| `f` | `folders` |
| `p` | `pull` |
| `ps` | `push` |
| `st` | `stats` |
| `s` | `serve` |

## Common Patterns

- Shared options: `tag_option = option('-t', '--tag', ...)`
- `@require_init` decorator for commands needing `.eml/`
- `AliasGroup` class for command aliases
- Account cascade: `get_account()` checks local then global

## Testing

```bash
eml init
eml a a gmail user@gmail.com
eml p gmail -f INBOX -l 10
eml ls
eml ps zoho -n
```
