"""Path template system for flexible email storage layouts."""

import hashlib
import re
from dataclasses import dataclass
from datetime import datetime
from string import Template
from typing import Any


# Preset templates - simple names that expand to full templates
PRESETS: dict[str, str] = {
    # Default: daily dirs with timestamp, hash, and subject for chronological ordering
    "default": "$folder/$yyyy/$mm/$dd/${hhmmss}_${sha8}_${subj}.eml",
    "flat": "$folder/${sha8}_${subj}.eml",
    "monthly": "$folder/$yyyy/$mm/${sha8}_${subj}.eml",
    "daily": "$folder/$yyyy/$mm/$dd/${sha8}_${subj}.eml",
    "compact": "$folder/$yyyy$mm$dd_${sha8}.eml",
    "hash2": "$folder/${sha2}/${sha8}_${subj}.eml",
    # Verbose with sender
    "verbose": "$folder/$yyyy/$mm/$dd/${hhmm}_${from}_${subj}_${sha8}.eml",
}

# Legacy aliases (backwards compat)
LEGACY_PRESETS: dict[str, str] = {
    "tree:flat": "flat",
    "tree:year": "$folder/$yyyy/${sha8}_${subj}.eml",
    "tree:month": "monthly",
    "tree:day": "daily",
    "tree:hash2": "hash2",
}


def resolve_preset(layout: str) -> str:
    """Resolve a preset name to its template string.

    If layout is a preset name, returns the template.
    If layout is a legacy name, resolves through LEGACY_PRESETS.
    Otherwise returns layout unchanged (assumed to be a template).
    """
    # Check legacy first
    if layout in LEGACY_PRESETS:
        resolved = LEGACY_PRESETS[layout]
        # Legacy might point to another preset name
        if resolved in PRESETS:
            return PRESETS[resolved]
        return resolved

    # Check presets
    if layout in PRESETS:
        return PRESETS[layout]

    # Assume it's a raw template
    return layout


def sanitize_for_path(s: str, max_len: int = 30) -> str:
    """Sanitize a string for use in filesystem paths.

    - Lowercase
    - Replace spaces/punctuation with underscore
    - Remove non-ASCII
    - Collapse multiple underscores
    - Strip leading/trailing underscores
    - Truncate to max_len
    """
    if not s:
        return "_"

    # Lowercase
    s = s.lower()

    # Remove common prefixes (keep looping until no more prefixes)
    prefixes = ["re:", "fwd:", "fw:"]
    changed = True
    while changed:
        changed = False
        for prefix in prefixes:
            if s.startswith(prefix):
                s = s[len(prefix):].lstrip()
                changed = True

    # Replace non-alphanumeric with underscore
    s = re.sub(r"[^a-z0-9]", "_", s)

    # Collapse multiple underscores
    s = re.sub(r"_+", "_", s)

    # Strip leading/trailing underscores
    s = s.strip("_")

    # Truncate
    if len(s) > max_len:
        s = s[:max_len].rstrip("_")

    return s or "_"


def content_hash(raw: bytes) -> str:
    """Compute SHA-256 hash of raw email content."""
    return hashlib.sha256(raw).hexdigest()


@dataclass
class MessageVars:
    """Variables available for path template interpolation."""

    folder: str
    raw: bytes
    date: datetime | None = None
    subject: str = ""
    from_addr: str = ""
    uid: int | None = None

    def to_dict(self) -> dict[str, str]:
        """Convert to dict of template variables."""
        result: dict[str, str] = {}

        # Folder
        result["folder"] = self.folder

        # Content hash variants
        sha = content_hash(self.raw)
        result["sha"] = sha
        result["sha2"] = sha[:2]
        result["sha4"] = sha[:4]
        result["sha8"] = sha[:8]
        result["sha16"] = sha[:16]
        result["sha32"] = sha[:32]

        # Date/time (use epoch if missing)
        dt = self.date or datetime.now()
        result["yyyy"] = dt.strftime("%Y")
        result["yy"] = dt.strftime("%y")
        result["mm"] = dt.strftime("%m")
        result["dd"] = dt.strftime("%d")
        result["hh"] = dt.strftime("%H")
        result["MM"] = dt.strftime("%M")
        result["ss"] = dt.strftime("%S")
        result["hhmm"] = dt.strftime("%H%M")
        result["hhmmss"] = dt.strftime("%H%M%S")

        # Subject variants
        subj = sanitize_for_path(self.subject, max_len=30)
        result["subj"] = subj
        result["subj10"] = sanitize_for_path(self.subject, max_len=10)
        result["subj20"] = sanitize_for_path(self.subject, max_len=20)
        result["subj40"] = sanitize_for_path(self.subject, max_len=40)
        result["subj60"] = sanitize_for_path(self.subject, max_len=60)

        # From variants
        from_clean = sanitize_for_path(self.from_addr, max_len=20)
        result["from"] = from_clean
        result["from10"] = sanitize_for_path(self.from_addr, max_len=10)
        result["from30"] = sanitize_for_path(self.from_addr, max_len=30)

        # UID (if available)
        if self.uid is not None:
            result["uid"] = str(self.uid)
        else:
            result["uid"] = "0"

        return result


class PathTemplate:
    """Template for generating storage paths from message metadata."""

    def __init__(self, template: str):
        """Initialize with a template string or preset name.

        Args:
            template: Either a preset name (e.g., "default", "flat", "tree:month")
                     or a template string (e.g., "$folder/$yyyy/$mm/${sha8}.eml")
        """
        self.original = template
        self.template_str = resolve_preset(template)
        self._template = Template(self.template_str)

    @property
    def variables(self) -> list[str]:
        """List of variable names used in this template."""
        return self._template.get_identifiers()

    def render(self, vars: MessageVars | dict[str, str]) -> str:
        """Render the template with the given variables.

        Args:
            vars: Either a MessageVars object or a dict of variable values

        Returns:
            The rendered path string
        """
        if isinstance(vars, MessageVars):
            var_dict = vars.to_dict()
        else:
            var_dict = vars

        return self._template.substitute(var_dict)

    def render_message(
        self,
        folder: str,
        raw: bytes,
        date: datetime | None = None,
        subject: str = "",
        from_addr: str = "",
        uid: int | None = None,
    ) -> str:
        """Convenience method to render from individual message attributes."""
        vars = MessageVars(
            folder=folder,
            raw=raw,
            date=date,
            subject=subject,
            from_addr=from_addr,
            uid=uid,
        )
        return self.render(vars)

    def __repr__(self) -> str:
        if self.original != self.template_str:
            return f"PathTemplate({self.original!r} -> {self.template_str!r})"
        return f"PathTemplate({self.template_str!r})"


# Default template instance
DEFAULT_TEMPLATE = PathTemplate("default")
