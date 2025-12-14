# eml - Email Migration Tool

## Project Overview

CLI tool for migrating emails between IMAP servers via local storage.

## Architecture (V2)

```
Source IMAP → pull → .eml files / sqlite → push → Destination IMAP
```

V2 uses Git for versioning with branch = migration pipeline model:
- `.eml/config.yaml` - accounts, credentials, layout setting
- `.eml/sync-state/<account>.yaml` - UIDVALIDITY, last UID per folder
- `.eml/pushed/<account>.txt` - Message-IDs already pushed
- `INBOX/*.eml`, `Sent/*.eml`, etc. (tree layouts) or `.eml/msgs.db` (sqlite)

## Key Files

- `src/eml/cli.py` - Click CLI with command aliases
- `src/eml/config.py` - V2 config/state via YAML files
- `src/eml/layouts/` - `StorageLayout` protocol, `TreeLayout`, `SqliteLayout`
- `src/eml/storage.py` - V1 `MessageStorage`, `AccountStorage` classes
- `src/eml/imap.py` - `GmailClient`, `ZohoClient`, `IMAPClient`
- `www/app.py` - Flask pmail web UI

## Command Aliases

| Alias | Command |
|-------|---------|
| `i` | `init` |
| `a` | `account` (`a a`=add, `a l`=ls, `a r`=rm) |
| `cv` | `convert` |
| `f` | `folders` |
| `p` | `pull` |
| `ps` | `push` |
| `st` | `stats` |
| `s` | `serve` |

## Storage Layouts

| Layout | Storage |
|--------|---------|
| `tree:flat` | `INBOX/a1b2c3d4.eml` |
| `tree:year` | `INBOX/2024/a1b2c3d4.eml` |
| `tree:month` | `INBOX/2024/01/a1b2c3d4.eml` (default) |
| `tree:day` | `INBOX/2024/01/15/a1b2c3d4.eml` |
| `tree:hash2` | `INBOX/a1/b2c3d4e5.eml` |
| `sqlite` | `.eml/msgs.db` |

## Common Patterns

- `is_v2_project()` / `get_storage_layout()` for V1/V2 detection
- `StorageLayout` protocol with `iter_messages()`, `add_message()`, `has_message()`
- Account names support `/` for namespacing (e.g., `y/user`, `g/user`)
- `@require_init` decorator for commands needing `.eml/`

## Testing

```bash
eml init                                    # V2 with tree:month
eml init -L sqlite                          # V2 with SQLite
eml init -V                                 # V1 legacy

eml a a -t imap y/user user@example.com --host imap.example.com
eml a a -t gmail g/user user@gmail.com
eml p y/user -f INBOX -l 10
eml ls
eml ps g/user -n
eml cv tree:flat                            # convert layout
```
