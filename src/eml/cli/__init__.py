"""CLI package for eml - email migration tools.

This package organizes CLI commands into modules:
- account.py: Account management (add, ls, rm, rename)
- pull.py: Pull emails from IMAP
- push.py: Push emails to IMAP
- status.py: Status, web dashboard, stats
- index_cmds.py: Index, backfill, uids, fsck
- attachments.py: Attachment manipulation
- misc.py: init, folders, ls, tags, convert, migrate, ingest
- utils.py: Shared utilities and helpers
"""

import click
from dotenv import load_dotenv

from .utils import AliasGroup

# Import command groups and commands
from .account import account
from .attachments import attachments
from .index_cmds import backfill, fsck, index, uids
from .misc import convert, folders, init, ingest, ls, migrate, tags
from .pull import pull
from .push import push
from .status import stats, status, web


# Main group with aliases
@click.group(cls=AliasGroup, aliases={
    'a': 'account',
    'at': 'attachments',
    'cv': 'convert',
    'f': 'folders',
    'i': 'init',
    'p': 'pull',
    'ps': 'push',
    'st': 'stats',
    's': 'status',
    'w': 'web',
})
def main():
    """Email migration tools."""
    load_dotenv()


# Register command groups
main.add_command(account)
main.add_command(attachments)

# Register individual commands
main.add_command(backfill)
main.add_command(convert)
main.add_command(folders)
main.add_command(fsck)
main.add_command(index)
main.add_command(ingest)
main.add_command(init)
main.add_command(ls)
main.add_command(migrate)
main.add_command(pull)
main.add_command(push)
main.add_command(stats)
main.add_command(status)
main.add_command(tags)
main.add_command(uids)
main.add_command(web)


# Export for convenience
__all__ = [
    'main',
    'account',
    'attachments',
    'backfill',
    'convert',
    'folders',
    'fsck',
    'index',
    'ingest',
    'init',
    'ls',
    'migrate',
    'pull',
    'push',
    'stats',
    'status',
    'tags',
    'uids',
    'web',
]
