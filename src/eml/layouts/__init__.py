"""Storage layout abstraction for eml v2."""

from .base import StorageLayout, StoredMessage
from .tree import TreeLayout
from .sqlite import SqliteLayout

__all__ = [
    "StorageLayout",
    "StoredMessage",
    "TreeLayout",
    "SqliteLayout",
]
