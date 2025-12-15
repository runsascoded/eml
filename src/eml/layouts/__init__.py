"""Storage layout abstraction for eml v2."""

from .base import StorageLayout, StoredMessage
from .tree import TreeLayout
from .sqlite import SqliteLayout
from .path_template import (
    PathTemplate,
    MessageVars,
    PRESETS,
    LEGACY_PRESETS,
    resolve_preset,
    sanitize_for_path,
    content_hash,
)

__all__ = [
    "StorageLayout",
    "StoredMessage",
    "TreeLayout",
    "SqliteLayout",
    "PathTemplate",
    "MessageVars",
    "PRESETS",
    "LEGACY_PRESETS",
    "resolve_preset",
    "sanitize_for_path",
    "content_hash",
]
