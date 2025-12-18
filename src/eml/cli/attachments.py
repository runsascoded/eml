"""Attachment manipulation commands for .eml files."""

import email
import mimetypes
import re
import sys
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import click
import humanize
from click import argument, echo, option

from .utils import AliasGroup, err


def get_attachments(msg: email.message.Message) -> list[dict]:
    """Get list of attachments from an email message.

    Returns list of dicts with keys: filename, content_type, size, part
    """
    attachments = []
    for part in msg.walk():
        content_disposition = part.get("Content-Disposition", "")
        if "attachment" in content_disposition or (
            part.get_content_maintype() not in ("text", "multipart")
            and part.get_filename()
        ):
            filename = part.get_filename() or "unnamed"
            payload = part.get_payload(decode=True)
            size = len(payload) if payload else 0
            attachments.append({
                "filename": filename,
                "content_type": part.get_content_type(),
                "size": size,
                "part": part,
            })
    return attachments


def compute_eml_output_path(
    original_path: Path,
    new_content: bytes,
    keep: bool = False,
) -> tuple[Path, bool]:
    """Compute output path for modified .eml file.

    If filename contains a SHA-like pattern (8+ hex chars), replace it with new SHA.
    Returns (output_path, should_delete_original).
    """
    import hashlib

    new_sha = hashlib.sha256(new_content).hexdigest()[:8]
    name = original_path.name

    # Pattern: 8+ consecutive hex characters (likely SHA)
    sha_pattern = re.compile(r'[0-9a-f]{8,}', re.IGNORECASE)
    match = sha_pattern.search(name)

    if match:
        # Replace SHA in filename
        old_sha = match.group()
        new_name = name[:match.start()] + new_sha + name[match.end():]
        new_path = original_path.parent / new_name

        if new_path == original_path:
            # SHA didn't change (unlikely but possible)
            return original_path, False
        elif keep:
            # Keep both files
            return new_path, False
        else:
            # Replace: write new, delete old
            return new_path, True
    else:
        # No SHA in filename
        if keep:
            # Generate a modified filename
            stem = original_path.stem
            suffix = original_path.suffix
            # Check for existing _v# suffix
            v_match = re.search(r'_v(\d+)$', stem)
            if v_match:
                num = int(v_match.group(1)) + 1
                new_stem = stem[:v_match.start()] + f"_v{num}"
            else:
                new_stem = stem + "_v2"
            return original_path.parent / (new_stem + suffix), False
        else:
            # Overwrite in place
            return original_path, False


def rebuild_message_with_attachments(
    original: email.message.Message,
    attachments: list[tuple[str, str, bytes]],
) -> email.message.Message:
    """Rebuild a message with new/modified attachments.

    attachments is a list of (filename, content_type, data) tuples.
    """
    # Create new multipart message
    new_msg = MIMEMultipart()

    # Copy headers (except content-type which will be set by MIMEMultipart)
    skip_headers = {"content-type", "content-transfer-encoding", "mime-version"}
    for key, value in original.items():
        if key.lower() not in skip_headers:
            new_msg[key] = value

    # Find and copy the text body from original
    body_added = False
    for part in original.walk():
        if part.get_content_maintype() == "text" and not body_added:
            content_disposition = part.get("Content-Disposition", "")
            if "attachment" not in content_disposition:
                text_part = MIMEText(
                    part.get_payload(decode=True).decode(
                        part.get_content_charset() or "utf-8", errors="replace"
                    ),
                    part.get_content_subtype(),
                    part.get_content_charset() or "utf-8",
                )
                new_msg.attach(text_part)
                body_added = True

    # Add attachments
    for filename, content_type, data in attachments:
        maintype, subtype = content_type.split("/", 1) if "/" in content_type else (content_type, "octet-stream")
        attachment = MIMEBase(maintype, subtype)
        attachment.set_payload(data)
        encoders.encode_base64(attachment)
        attachment.add_header(
            "Content-Disposition",
            "attachment",
            filename=filename,
        )
        new_msg.attach(attachment)

    return new_msg


@click.group(cls=AliasGroup, aliases={'l': 'list', 'x': 'extract', 'r': 'replace'})
def attachments():
    """Manipulate attachments in .eml files."""
    pass


@attachments.command("list")
@argument("eml_path", type=click.Path(exists=True))
@option('-j', '--json', 'as_json', is_flag=True, help="Output as JSON")
def attachments_list(eml_path: str, as_json: bool):
    """List attachments in an .eml file."""
    import json as json_mod

    path = Path(eml_path)
    with open(path, "rb") as f:
        msg = email.message_from_binary_file(f)

    atts = get_attachments(msg)

    if as_json:
        result = [
            {"filename": a["filename"], "content_type": a["content_type"], "size": a["size"]}
            for a in atts
        ]
        print(json_mod.dumps(result, indent=2))
    else:
        if not atts:
            echo("No attachments")
            return
        echo(f"Attachments ({len(atts)}):")
        for a in atts:
            size_str = humanize.naturalsize(a["size"], binary=True)
            echo(f"  {a['filename']:<40} {a['content_type']:<30} {size_str:>10}")


@attachments.command("extract")
@argument("eml_path", type=click.Path(exists=True))
@argument("attachment_name")
@option('-o', '--output', 'out_path', type=click.Path(), help="Output path (default: attachment filename)")
def attachments_extract(eml_path: str, attachment_name: str, out_path: str | None):
    """Extract an attachment from an .eml file."""
    path = Path(eml_path)
    with open(path, "rb") as f:
        msg = email.message_from_binary_file(f)

    atts = get_attachments(msg)

    # Find matching attachment
    matches = [a for a in atts if a["filename"] == attachment_name]
    if not matches:
        # Try partial match
        matches = [a for a in atts if attachment_name.lower() in a["filename"].lower()]

    if not matches:
        err(f"Attachment not found: {attachment_name}")
        err("Available attachments:")
        for a in atts:
            err(f"  {a['filename']}")
        sys.exit(1)

    if len(matches) > 1:
        err(f"Multiple matches for '{attachment_name}':")
        for a in matches:
            err(f"  {a['filename']}")
        err("Please specify exact filename")
        sys.exit(1)

    att = matches[0]
    data = att["part"].get_payload(decode=True)

    output = Path(out_path) if out_path else Path(att["filename"])
    output.write_bytes(data)
    echo(f"Extracted: {output} ({humanize.naturalsize(len(data), binary=True)})")


@attachments.command("add")
@argument("eml_path", type=click.Path(exists=True))
@argument("file_path", type=click.Path(exists=True))
@option('-k', '--keep', is_flag=True, help="Keep original file (don't delete when SHA changes)")
@option('-n', '--name', 'att_name', help="Attachment filename (default: file basename)")
@option('-o', '--output', 'out_path', type=click.Path(), help="Output .eml path (overrides SHA logic)")
def attachments_add(eml_path: str, file_path: str, keep: bool, att_name: str | None, out_path: str | None):
    """Add an attachment to an .eml file."""
    eml = Path(eml_path)
    file = Path(file_path)

    with open(eml, "rb") as f:
        msg = email.message_from_binary_file(f)

    # Read new attachment
    data = file.read_bytes()
    filename = att_name or file.name
    content_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"

    # Get existing attachments
    existing = get_attachments(msg)
    attachments_list = [
        (a["filename"], a["content_type"], a["part"].get_payload(decode=True))
        for a in existing
    ]
    attachments_list.append((filename, content_type, data))

    # Rebuild message
    new_msg = rebuild_message_with_attachments(msg, attachments_list)
    new_content = new_msg.as_bytes()

    # Determine output path
    if out_path:
        output = Path(out_path)
        delete_original = False
    else:
        output, delete_original = compute_eml_output_path(eml, new_content, keep)

    # Write output
    with open(output, "wb") as f:
        f.write(new_content)

    # Delete original if needed (SHA-based filename changed)
    if delete_original and output != eml:
        eml.unlink()
        echo(f"Added {filename} ({humanize.naturalsize(len(data), binary=True)})")
        echo(f"  {eml.name} -> {output.name}")
    else:
        echo(f"Added {filename} ({humanize.naturalsize(len(data), binary=True)}) to {output}")


@attachments.command("replace")
@argument("eml_path", type=click.Path(exists=True))
@argument("attachment_name")
@argument("file_path", type=click.Path(exists=True))
@option('-k', '--keep', is_flag=True, help="Keep original file (don't delete when SHA changes)")
@option('-n', '--name', 'new_name', help="New attachment filename (default: keep original)")
@option('-o', '--output', 'out_path', type=click.Path(), help="Output .eml path (overrides SHA logic)")
def attachments_replace(
    eml_path: str,
    attachment_name: str,
    file_path: str,
    keep: bool,
    new_name: str | None,
    out_path: str | None,
):
    """Replace an attachment in an .eml file."""
    eml = Path(eml_path)
    file = Path(file_path)

    with open(eml, "rb") as f:
        msg = email.message_from_binary_file(f)

    atts = get_attachments(msg)

    # Find matching attachment
    found_idx = None
    for i, a in enumerate(atts):
        if a["filename"] == attachment_name:
            found_idx = i
            break
        if attachment_name.lower() in a["filename"].lower():
            found_idx = i

    if found_idx is None:
        err(f"Attachment not found: {attachment_name}")
        err("Available attachments:")
        for a in atts:
            err(f"  {a['filename']}")
        sys.exit(1)

    # Read replacement file
    data = file.read_bytes()
    filename = new_name or atts[found_idx]["filename"]
    content_type = mimetypes.guess_type(filename)[0] or atts[found_idx]["content_type"]

    # Build new attachments list
    attachments_list = []
    for i, a in enumerate(atts):
        if i == found_idx:
            attachments_list.append((filename, content_type, data))
        else:
            attachments_list.append((
                a["filename"],
                a["content_type"],
                a["part"].get_payload(decode=True),
            ))

    # Rebuild message
    new_msg = rebuild_message_with_attachments(msg, attachments_list)
    new_content = new_msg.as_bytes()

    # Determine output path
    if out_path:
        output = Path(out_path)
        delete_original = False
    else:
        output, delete_original = compute_eml_output_path(eml, new_content, keep)

    # Write output
    with open(output, "wb") as f:
        f.write(new_content)

    old_size = atts[found_idx]["size"]
    size_change = f"{humanize.naturalsize(old_size, binary=True)} -> {humanize.naturalsize(len(data), binary=True)}"

    # Delete original if needed (SHA-based filename changed)
    if delete_original and output != eml:
        eml.unlink()
        echo(f"Replaced {attachment_name} ({size_change})")
        echo(f"  {eml.name} -> {output.name}")
    else:
        echo(f"Replaced {attachment_name} ({size_change}) in {output}")


@attachments.command("remove")
@argument("eml_path", type=click.Path(exists=True))
@argument("attachment_name")
@option('-k', '--keep', is_flag=True, help="Keep original file (don't delete when SHA changes)")
@option('-o', '--output', 'out_path', type=click.Path(), help="Output .eml path (overrides SHA logic)")
def attachments_remove(eml_path: str, attachment_name: str, keep: bool, out_path: str | None):
    """Remove an attachment from an .eml file."""
    eml = Path(eml_path)

    with open(eml, "rb") as f:
        msg = email.message_from_binary_file(f)

    atts = get_attachments(msg)

    # Find matching attachment
    found_idx = None
    for i, a in enumerate(atts):
        if a["filename"] == attachment_name:
            found_idx = i
            break
        if attachment_name.lower() in a["filename"].lower():
            found_idx = i

    if found_idx is None:
        err(f"Attachment not found: {attachment_name}")
        err("Available attachments:")
        for a in atts:
            err(f"  {a['filename']}")
        sys.exit(1)

    # Build new attachments list without the removed one
    attachments_list = [
        (a["filename"], a["content_type"], a["part"].get_payload(decode=True))
        for i, a in enumerate(atts) if i != found_idx
    ]

    # Rebuild message
    new_msg = rebuild_message_with_attachments(msg, attachments_list)
    new_content = new_msg.as_bytes()

    # Determine output path
    if out_path:
        output = Path(out_path)
        delete_original = False
    else:
        output, delete_original = compute_eml_output_path(eml, new_content, keep)

    # Write output
    with open(output, "wb") as f:
        f.write(new_content)

    removed = atts[found_idx]
    removed_info = f"{removed['filename']} ({humanize.naturalsize(removed['size'], binary=True)})"

    # Delete original if needed (SHA-based filename changed)
    if delete_original and output != eml:
        eml.unlink()
        echo(f"Removed {removed_info}")
        echo(f"  {eml.name} -> {output.name}")
    else:
        echo(f"Removed {removed_info} from {output}")
