# eml - Email Migration Tool

## Project Overview

CLI tool for migrating emails between IMAP servers via local `.eml` file storage.

## Architecture

```
Source IMAP → pull → .eml files (or sqlite) → push → Destination IMAP
```

Project structure:
- `.eml/config.yaml` - accounts, credentials, layout setting
- `.eml/sync-state/<account>.yaml` - UIDVALIDITY, last UID per folder
- `.eml/pushed/<account>.txt` - Message-IDs already pushed
- `.eml/failures/<account>_<folder>.yaml` - failed UIDs for retry
- `INBOX/*.eml`, `Sent/*.eml`, etc. (tree layouts) or `.eml/msgs.db` (sqlite)

## Key Files

- `src/eml/cli.py` - Click CLI with command aliases
- `src/eml/config.py` - Config/state via YAML files
- `src/eml/layouts/` - `StorageLayout` protocol, `TreeLayout`, `SqliteLayout`
- `src/eml/layouts/path_template.py` - `PathTemplate` for flexible file paths
- `src/eml/imap.py` - `GmailClient`, `ZohoClient`, `IMAPClient`
- `www/app.py` - Flask pmail web UI

## Command Aliases

| Alias | Command |
|-------|---------|
| `i` | `init` |
| `a` | `account` (`a a`=add, `a l`=ls, `a r`=rename) |
| `cv` | `convert` |
| `f` | `folders` |
| `p` | `pull` |
| `ps` | `push` |
| `st` | `stats` |
| `s` | `serve` |

## Storage Layouts

Path templates with `$var` / `${var}` interpolation:

| Preset | Template |
|--------|----------|
| `default` | `$folder/$yyyy/$mm/$dd/${hhmmss}_${sha8}_${subj}.eml` |
| `monthly` | `$folder/$yyyy/$mm/${sha8}_${subj}.eml` |
| `flat` | `$folder/${sha8}_${subj}.eml` |
| `daily` | `$folder/$yyyy/$mm/$dd/${sha8}_${subj}.eml` |
| `sqlite` | `.eml/msgs.db` |

Legacy `tree:*` names still work (`tree:month` → `monthly`).

### Template Variables

- Date: `$yyyy`, `$mm`, `$dd`, `$hh`, `$MM`, `$ss`, `$hhmmss`
- Content: `$sha`, `$sha8`, `$sha16` (content hash)
- Metadata: `$folder`, `$subj`, `$subj20`, `$from`, `$uid`

## Common Patterns

- `has_config()` / `get_storage_layout()` for project detection
- `StorageLayout` protocol: `iter_messages()`, `add_message()`, `has_message()`, `has_content()`
- `PathTemplate` for path generation from message metadata
- Account names support `/` for namespacing (e.g., `y/user`, `g/user`)
- `@require_init` decorator for commands needing `.eml/`

## Testing

```bash
pytest tests/ -v                # run all tests

# Manual testing
eml init
eml init -L flat
eml init -L sqlite
eml a a -t imap y/user user@example.com -H imap.example.com
eml a a -t gmail g/user user@gmail.com
eml p g/user -f INBOX -l 10
eml ls
eml ps y/user -n
eml cv flat
```
