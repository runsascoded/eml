"""Email parsing utilities for FTS indexing."""

from email import policy
from email.parser import BytesParser


def extract_body_text(raw: bytes) -> str:
    """Extract plain text body from raw email bytes for FTS indexing.

    Prefers text/plain parts. Falls back to empty string if no text found.
    """
    try:
        msg = BytesParser(policy=policy.default).parsebytes(raw)
    except Exception:
        return ""

    body_plain = ""
    if msg.is_multipart():
        for part in msg.walk():
            ct = part.get_content_type()
            if ct == "text/plain" and not body_plain:
                try:
                    body_plain = part.get_content()
                except Exception:
                    pass
    else:
        ct = msg.get_content_type()
        if ct != "text/html":
            try:
                body_plain = msg.get_content()
            except Exception:
                pass

    return body_plain if isinstance(body_plain, str) else ""
