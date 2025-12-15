# eml

Email migration and archival tool with flexible local storage.

## Installation

```bash
pip install eml-cli
# or
uv add eml-cli
```

## Quick Start

```bash
# Initialize project
eml init

# Add accounts
eml account add -t gmail g/user user@gmail.com
eml account add -t imap y/user user@example.com -H imap.example.com

# Pull emails to local storage
eml pull g/user -f INBOX

# Push to destination
eml push y/user -f Archive
```

## Project Structure

```
myproject/
  .eml/
    config.yaml      # accounts, layout setting
    sync-state/      # pull progress per account
    pushed/          # push manifests per account
  INBOX/
    2024/12/15/
      143022_a1b2c3d4_meeting_notes.eml
      ...
```

## Storage Layouts

Emails are stored as `.eml` files with configurable path templates:

| Preset | Template | Example |
|--------|----------|---------|
| `default` | `$folder/$yyyy/$mm/$dd/${hhmmss}_${sha8}_${subj}.eml` | `INBOX/2024/12/15/143022_a1b2c3d4_meeting.eml` |
| `monthly` | `$folder/$yyyy/$mm/${sha8}_${subj}.eml` | `INBOX/2024/12/a1b2c3d4_meeting.eml` |
| `flat` | `$folder/${sha8}_${subj}.eml` | `INBOX/a1b2c3d4_meeting.eml` |
| `sqlite` | `.eml/msgs.db` | SQLite blob storage |

Or use a custom template:

```bash
eml init -L '$folder/$yyyy/$mm/${sha8}.eml'
```

### Template Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `$folder` | IMAP folder | `INBOX`, `Sent` |
| `$yyyy`, `$mm`, `$dd` | Date components | `2024`, `12`, `15` |
| `$hh`, `$MM`, `$ss` | Time components | `14`, `30`, `22` |
| `$hhmmss` | Time combined | `143022` |
| `$sha8` | Content hash (8 chars) | `a1b2c3d4` |
| `$subj` | Sanitized subject | `meeting_notes` |
| `$from` | Sanitized sender | `john_smith` |

## Commands

All commands have short aliases shown in parentheses.

### `eml init` (`i`)

```bash
eml init                    # default layout
eml init -L flat            # flat layout
eml init -L sqlite          # SQLite storage
eml init -g                 # global config (~/.config/eml/)
```

### `eml account` (`a`)

```bash
eml account add -t gmail g/user user@gmail.com      # Gmail (a a)
eml account add -t imap y/user user@ex.com -H imap.ex.com
eml account ls                                       # list (a l)
eml account rm g/user                                # remove
eml account rename g/old g/new                       # rename (a r)
```

### `eml folders` (`f`)

```bash
eml folders g/user              # list all folders
eml folders g/user INBOX        # show message count
eml folders g/user -s INBOX     # show count and size
```

### `eml pull` (`p`)

```bash
eml pull g/user                 # pull from All Mail
eml pull g/user -f INBOX        # pull from specific folder
eml pull g/user -l 100          # limit to 100 messages
eml pull g/user -n              # dry run
eml pull g/user -F              # full sync (ignore state)
eml pull g/user -r              # retry failed UIDs
```

### `eml push` (`ps`)

```bash
eml push y/user -f Archive      # push to folder
eml push y/user -l 10 -v        # push 10, verbose
eml push y/user -d 2            # 2 sec delay (rate limiting)
eml push y/user -n              # dry run
```

### `eml convert` (`cv`)

```bash
eml convert flat                # convert to flat layout
eml convert sqlite              # convert to SQLite
eml convert '$folder/$yyyy/${sha8}.eml'  # custom template
```

### `eml ls`

```bash
eml ls                          # list recent messages
eml ls -l 50                    # show 50 messages
eml ls "search term"            # search From/Subject
```

### `eml stats` (`st`)

```bash
eml stats                       # size distribution, date range, etc.
```

### `eml serve` (`s`)

```bash
eml serve                       # web UI at http://127.0.0.1:5000
eml serve -p 8080               # different port
```

## Features

- **Flexible storage**: Path templates with date/hash/subject variables
- **Content-hash dedup**: Works even for emails without Message-ID
- **Incremental sync**: Tracks UIDVALIDITY and last UID per folder
- **Failure tracking**: Retry failed pulls with `--retry`
- **Rate limiting**: Configurable delay between pushes
- **Preserves**: Original dates, headers, threading, attachments

## License

MIT
