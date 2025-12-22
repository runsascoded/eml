#!/usr/bin/env -S uv run
# /// script
# dependencies = ["fastapi", "uvicorn", "sse-starlette"]
# ///
"""Web UI for EML status monitoring.

Run with: eml web
Or directly: python -m eml.web
"""

import asyncio
import email
import json
import os
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response
import uvicorn

from .config import get_eml_root
from .index import FileIndex
from .pulls import get_pulls_db


app = FastAPI(title="EML Status")


def get_index_db(root: Path) -> FileIndex:
    """Get FileIndex for the project."""
    return FileIndex(root / ".eml")


def extract_folder(path: str) -> str:
    """Extract folder name from path (e.g., 'Inbox/2023/...' -> 'Inbox')."""
    parts = path.split("/")
    return parts[0] if parts else ""


def get_root() -> Path:
    """Get project root, with fallback for when not in project dir."""
    try:
        return get_eml_root()
    except Exception:
        return Path.cwd()


@app.get("/api/health")
def api_health():
    """Check database health and provide rebuild suggestions."""
    root = get_root()
    eml_dir = root / ".eml"
    warnings = []

    # Check index.db
    index_db = eml_dir / "index.db"
    if not index_db.exists():
        warnings.append({
            "type": "missing_index",
            "message": "index.db not found",
            "fix": "Run: eml rebuild-index",
        })

    # Check uids.db and parquet
    uids_db = eml_dir / "uids.db"
    uids_parquet = eml_dir / "uids.parquet"
    if uids_parquet.exists() and not uids_db.exists():
        warnings.append({
            "type": "uids_pending_rebuild",
            "message": "uids.db will auto-rebuild from parquet on next access",
        })

    return {
        "ok": len(warnings) == 0,
        "root": str(root),
        "warnings": warnings,
        "databases": {
            "index_db": {"exists": index_db.exists(), "size": index_db.stat().st_size if index_db.exists() else 0},
            "uids_db": {"exists": uids_db.exists(), "size": uids_db.stat().st_size if uids_db.exists() else 0},
            "uids_parquet": {"exists": uids_parquet.exists(), "size": uids_parquet.stat().st_size if uids_parquet.exists() else 0},
        },
    }


@app.get("/api/folders")
def api_folders(account: str | None = None):
    """Get list of folders with activity."""
    root = get_root()
    with get_pulls_db(root) as db:
        folders = db.get_folders_with_activity(account=account)
        return {
            "folders": [
                {"account": acct, "folder": fld, "count": cnt}
                for acct, fld, cnt in folders
            ]
        }


@app.get("/api/status")
def api_status(account: str = "y", folder: str | None = None):
    """Get UID status summary. If folder is None, aggregate across all folders."""
    root = get_root()
    pulls_db_path = root / ".eml" / "pulls.db"
    if not pulls_db_path.exists():
        return JSONResponse({"error": "No pulls.db found"}, status_code=404)

    with get_pulls_db(root) as db:
        if folder is None:
            # Aggregate across all folders for this account
            cur = db.conn.execute(
                "SELECT SUM(cnt) FROM (SELECT COUNT(*) as cnt FROM server_uids WHERE account = ? GROUP BY folder)",
                (account,)
            )
            server_count = cur.fetchone()[0] or 0

            cur = db.conn.execute(
                "SELECT SUM(cnt) FROM (SELECT COUNT(*) as cnt FROM pulled_messages WHERE account = ? GROUP BY folder)",
                (account,)
            )
            pulled_count = cur.fetchone()[0] or 0

            # For aggregated view, we can't easily compute unpulled without iterating folders
            # Just show the difference
            unpulled_count = max(0, server_count - pulled_count)

            return {
                "account": account,
                "folder": None,
                "uidvalidity": None,
                "server_uids": server_count,
                "pulled_uids": pulled_count,
                "unpulled_uids": unpulled_count,
                "no_message_id": 0,
                "timestamp": datetime.now().isoformat(),
            }

        # Single folder case
        uidvalidity = db.get_uidvalidity(account, folder)
        if not uidvalidity:
            # Try server_folders
            cur = db.conn.execute(
                "SELECT uidvalidity FROM server_folders WHERE account = ? AND folder = ?",
                (account, folder)
            )
            row = cur.fetchone()
            uidvalidity = row["uidvalidity"] if row else None

        if not uidvalidity:
            return JSONResponse({"error": f"No data for {account}/{folder}"}, status_code=404)

        server_count = db.get_server_uid_count(account, folder)
        pulled_count = db.get_pulled_count(account, folder, uidvalidity)
        unpulled_uids = db.get_unpulled_uids(account, folder, uidvalidity)
        no_mid_uids = db.get_uids_without_message_id(account, folder, uidvalidity)

        return {
            "account": account,
            "folder": folder,
            "uidvalidity": uidvalidity,
            "server_uids": server_count,
            "pulled_uids": pulled_count,
            "unpulled_uids": len(unpulled_uids),
            "no_message_id": len(no_mid_uids),
            "timestamp": datetime.now().isoformat(),
        }


@app.get("/api/histogram")
def api_histogram(account: str | None = None, folder: str | None = None, hours: int = 24):
    """Get hourly activity histogram with new vs deduped vs failed breakdown."""
    root = get_root()
    with get_pulls_db(root) as db:
        data = db.get_activity_by_hour(account=account, folder=folder, limit_hours=hours)
        return {
            "hours": hours,
            "data": [{"hour": h, "new": new, "deduped": deduped, "failed": failed} for h, new, deduped, failed in data],
        }


@app.get("/api/recent")
def api_recent(limit: int = 20, account: str | None = None, folder: str | None = None):
    """Get recent activity (all pulls, including skipped/deduped and failures)."""
    root = get_root()
    with get_pulls_db(root) as db:
        # Get recent pulls - new files, deduped, and failures
        # with_path_only=False includes skipped (deduped) and failed entries
        pulls = db.get_recent_pulls(limit=limit, account=account, folder=folder, with_path_only=False)
        return {
            "pulls": [
                {
                    "uid": p.uid,
                    "folder": p.folder,
                    "path": p.local_path,
                    "pulled_at": p.pulled_at.isoformat(),
                    "status": p.status,  # 'new', 'skipped', or 'failed'
                    "subject": p.subject,
                    "msg_date": p.msg_date,
                }
                for p in pulls
            ]
        }


@app.get("/api/email/{path:path}")
def api_email(path: str):
    """Get email content as JSON."""
    from email import policy

    root = get_root()
    file_path = root / path

    # Security: ensure path is within root
    try:
        file_path = file_path.resolve()
        root_resolved = root.resolve()
        if not str(file_path).startswith(str(root_resolved)):
            return JSONResponse({"error": "Access denied"}, status_code=403)
    except Exception:
        return JSONResponse({"error": "Invalid path"}, status_code=400)

    if not file_path.exists() or not file_path.suffix == ".eml":
        return JSONResponse({"error": "Email not found"}, status_code=404)

    # Parse email
    with open(file_path, "rb") as f:
        msg = email.message_from_binary_file(f, policy=policy.default)

    # Extract headers
    headers = {
        "from": msg.get("From", ""),
        "to": msg.get("To", ""),
        "cc": msg.get("Cc", ""),
        "date": msg.get("Date", ""),
        "subject": msg.get("Subject", "(no subject)"),
        "message_id": msg.get("Message-ID", ""),
        "in_reply_to": msg.get("In-Reply-To", ""),
        "references": msg.get("References", ""),
    }

    # Get body (prefer HTML, fall back to plain)
    body_html = ""
    body_plain = ""
    if msg.is_multipart():
        for part in msg.walk():
            ct = part.get_content_type()
            if ct == "text/html" and not body_html:
                try:
                    body_html = part.get_content()
                except Exception:
                    pass
            elif ct == "text/plain" and not body_plain:
                try:
                    body_plain = part.get_content()
                except Exception:
                    pass
    else:
        ct = msg.get_content_type()
        try:
            content = msg.get_content()
            if ct == "text/html":
                body_html = content
            else:
                body_plain = content
        except Exception:
            body_plain = "(could not decode body)"

    # Get attachments and build cid map for inline images
    attachments = []
    cid_map: dict[str, str] = {}  # cid -> filename
    if msg.is_multipart():
        for part in msg.walk():
            filename = part.get_filename()
            if filename:
                payload = part.get_payload(decode=True)
                attachments.append({
                    "filename": filename,
                    "content_type": part.get_content_type(),
                    "size": len(payload) if payload else 0,
                })
                # Extract Content-ID for cid: URL mapping
                content_id = part.get("Content-ID", "")
                if content_id:
                    # Content-ID is usually <xxx>, strip angle brackets
                    cid = content_id.strip("<>")
                    cid_map[cid] = filename

    # Rewrite cid: URLs in HTML to use our attachment API
    if body_html and cid_map:
        def replace_cid(match: re.Match) -> str:
            cid = match.group(1)
            if cid in cid_map:
                filename = cid_map[cid]
                return f"/api/attachment/{path}/{quote(filename)}"
            return match.group(0)  # Return unchanged if not found

        body_html = re.sub(r'cid:([^"\'>\s]+)', replace_cid, body_html)

        # Wrap images with our attachment URLs in clickable links
        def wrap_img_in_link(match: re.Match) -> str:
            img_tag = match.group(0)
            src = match.group(1)
            # Only wrap images served from our attachment API
            if src.startswith('/api/attachment/'):
                return f'<a href="{src}" target="_blank" rel="noopener noreferrer">{img_tag}</a>'
            return img_tag

        body_html = re.sub(r'<img[^>]+src="(/api/attachment/[^"]+)"[^>]*>', wrap_img_in_link, body_html)

    return {
        "path": path,
        "headers": headers,
        "body_html": body_html,
        "body_plain": body_plain,
        "attachments": attachments,
    }


@app.get("/api/attachment/{path:path}/{filename}")
def api_attachment(path: str, filename: str):
    """Get an attachment from an email."""
    root = get_root()
    eml_path = root / path
    if not eml_path.exists():
        return JSONResponse({"error": f"Email not found: {path}"}, status_code=404)

    with open(eml_path, "rb") as f:
        msg = email.message_from_binary_file(f)

    # Find the attachment
    if msg.is_multipart():
        for part in msg.walk():
            part_filename = part.get_filename()
            if part_filename == filename:
                payload = part.get_payload(decode=True)
                content_type = part.get_content_type()
                if payload:
                    return Response(
                        content=payload,
                        media_type=content_type,
                        headers={
                            "Content-Disposition": f'inline; filename="{filename}"',
                        },
                    )

    return JSONResponse({"error": f"Attachment not found: {filename}"}, status_code=404)


@app.get("/api/sync-runs")
def api_sync_runs(
    limit: int = 20,
    offset: int = 0,
    account: str | None = None,
    folder: str | None = None,
    operation: str | None = None,
):
    """Get recent sync runs (pull/push operations) with pagination."""
    root = get_root()
    with get_pulls_db(root) as db:
        runs = db.get_recent_sync_runs(
            limit=limit,
            offset=offset,
            account=account,
            folder=folder,
            operation=operation,
        )
        total = db.count_sync_runs(account=account, folder=folder, operation=operation)
        return {
            "runs": [
                {
                    "id": r.id,
                    "operation": r.operation,
                    "account": r.account,
                    "folder": r.folder,
                    "started_at": r.started_at.isoformat(),
                    "ended_at": r.ended_at.isoformat() if r.ended_at else None,
                    "status": r.status,
                    "total": r.total,
                    "fetched": r.fetched,
                    "skipped": r.skipped,
                    "failed": r.failed,
                    "error_message": r.error_message,
                }
                for r in runs
            ],
            "total": total,
            "limit": limit,
            "offset": offset,
        }


@app.get("/api/sync-runs/{run_id}")
def api_sync_run_detail(run_id: int, message_status: str | None = None, limit: int = 100):
    """Get details of a specific sync run, including messages processed."""
    root = get_root()
    with get_pulls_db(root) as db:
        run = db.get_sync_run(run_id)
        if not run:
            return JSONResponse({"error": f"Sync run {run_id} not found"}, status_code=404)

        messages = db.get_sync_run_messages(run_id, status=message_status, limit=limit)

        return {
            "run": {
                "id": run.id,
                "operation": run.operation,
                "account": run.account,
                "folder": run.folder,
                "started_at": run.started_at.isoformat(),
                "ended_at": run.ended_at.isoformat() if run.ended_at else None,
                "status": run.status,
                "total": run.total,
                "fetched": run.fetched,
                "skipped": run.skipped,
                "failed": run.failed,
                "error_message": run.error_message,
            },
            "messages": [
                {
                    "uid": m.uid,
                    "folder": m.folder,
                    "message_id": m.message_id,
                    "local_path": m.local_path,
                    "pulled_at": m.pulled_at.isoformat(),
                    "status": m.status,
                    "content_hash": m.content_hash[:16] + "..." if m.content_hash else None,
                    "error_message": m.error_message,
                }
                for m in messages
            ],
        }


@app.get("/api/folder/{account}/{folder}")
def api_folder_detail(account: str, folder: str, recent_limit: int = 50, runs_limit: int = 10):
    """Get folder detail: recent messages and sync runs for a specific folder."""
    root = get_root()
    with get_pulls_db(root) as db:
        # Get status
        uidvalidity = db.get_uidvalidity(account, folder)
        if not uidvalidity:
            cur = db.conn.execute(
                "SELECT uidvalidity FROM server_folders WHERE account = ? AND folder = ?",
                (account, folder)
            )
            row = cur.fetchone()
            uidvalidity = row["uidvalidity"] if row else None

        server_count = db.get_server_uid_count(account, folder) if uidvalidity else 0
        pulled_count = db.get_pulled_count(account, folder, uidvalidity) if uidvalidity else 0

        # Get recent messages
        pulls = db.get_recent_pulls(
            limit=recent_limit, account=account, folder=folder, with_path_only=False
        )

        # Get sync runs for this folder
        runs = db.get_recent_sync_runs(
            limit=runs_limit, account=account, folder=folder
        )

        return {
            "account": account,
            "folder": folder,
            "uidvalidity": uidvalidity,
            "server_uids": server_count,
            "pulled_uids": pulled_count,
            "messages": [
                {
                    "uid": p.uid,
                    "folder": p.folder,
                    "path": p.local_path,
                    "pulled_at": p.pulled_at.isoformat(),
                    "is_new": p.status != "skipped",
                    "subject": p.subject,
                    "msg_date": p.msg_date,
                }
                for p in pulls
            ],
            "sync_runs": [
                {
                    "id": r.id,
                    "operation": r.operation,
                    "started_at": r.started_at.isoformat(),
                    "ended_at": r.ended_at.isoformat() if r.ended_at else None,
                    "status": r.status,
                    "total": r.total,
                    "fetched": r.fetched,
                    "skipped": r.skipped,
                    "failed": r.failed,
                }
                for r in runs
            ],
        }


@app.get("/api/search")
def api_search(
    q: str,
    limit: int = 50,
    offset: int = 0,
    account: str | None = None,
    folder: str | None = None,
):
    """Full-text search over emails using index.db.

    The query supports FTS5 syntax:
    - Simple terms: `meeting report`
    - Phrases: `"board meeting"`
    - AND/OR: `budget OR finance`
    - NOT: `report NOT draft`
    - Column-specific: `from_addr:john subject:budget`

    Returns paginated results with total count for pagination UI.
    """
    root = get_root()
    with get_index_db(root) as db:
        try:
            total = db.search_count(query=q, folder=folder)
            results = db.search(query=q, limit=limit, offset=offset, folder=folder)
        except Exception as e:
            return JSONResponse({"error": f"Search error: {e}"}, status_code=400)

        return {
            "query": q,
            "total": total,
            "count": len(results),
            "offset": offset,
            "limit": limit,
            "results": [
                {
                    "folder": extract_folder(m.path),
                    "message_id": m.message_id,
                    "subject": m.subject,
                    "local_path": m.path,
                    "msg_date": m.date.isoformat() if m.date else None,
                    "from_addr": m.from_addr,
                    "to_addr": m.to_addr,
                    "thread_id": m.thread_id,
                    "thread_slug": m.thread_slug,
                }
                for m in results
            ],
        }


@app.get("/api/thread/{message_id:path}")
def api_thread(message_id: str, limit: int = 100):
    """Get all messages in a thread by Message-ID.

    First looks up the thread_id from message_id, then retrieves all messages
    in that thread using index.db.
    """
    root = get_root()
    with get_index_db(root) as db:
        # First, find the message by message_id to get its thread_id
        file = db.get_by_message_id(message_id)
        if not file:
            return JSONResponse({"error": f"Message not found: {message_id}"}, status_code=404)

        thread_id = file.thread_id
        if not thread_id:
            # No thread, return just this message
            messages = [file]
        else:
            messages = db.get_thread(thread_id=thread_id, limit=limit)

        thread_slug = messages[0].thread_slug if messages else None
        return {
            "message_id": message_id,
            "thread_id": thread_id,
            "thread_slug": thread_slug,
            "count": len(messages),
            "messages": [
                {
                    "folder": extract_folder(m.path),
                    "subject": m.subject,
                    "message_id": m.message_id,
                    "thread_id": m.thread_id,
                    "thread_slug": m.thread_slug,
                    "local_path": m.path,
                    "msg_date": m.date.isoformat() if m.date else None,
                    "in_reply_to": m.in_reply_to,
                    "references": m.references,
                    "from_addr": m.from_addr,
                    "to_addr": m.to_addr,
                }
                for m in messages
            ],
        }


@app.get("/api/thread-by-id/{thread_id:path}")
def api_thread_by_id(thread_id: str, limit: int = 100):
    """Get all messages in a thread by thread_id directly using index.db.

    This is more efficient than /api/thread which looks up by message_id first.
    """
    root = get_root()
    with get_index_db(root) as db:
        messages = db.get_thread(thread_id=thread_id, limit=limit)
        thread_slug = messages[0].thread_slug if messages else None
        return {
            "thread_id": thread_id,
            "thread_slug": thread_slug,
            "count": len(messages),
            "messages": [
                {
                    "folder": extract_folder(m.path),
                    "subject": m.subject,
                    "message_id": m.message_id,
                    "thread_id": m.thread_id,
                    "thread_slug": m.thread_slug,
                    "local_path": m.path,
                    "msg_date": m.date.isoformat() if m.date else None,
                    "in_reply_to": m.in_reply_to,
                    "references": m.references,
                    "from_addr": m.from_addr,
                    "to_addr": m.to_addr,
                }
                for m in messages
            ],
        }


def count_attachments(root: Path, local_path: str | None) -> int:
    """Count attachments in an .eml file."""
    if not local_path:
        return 0
    eml_path = root / local_path
    if not eml_path.exists():
        return 0
    try:
        with open(eml_path, "rb") as f:
            msg = email.message_from_binary_file(f)
        count = 0
        if msg.is_multipart():
            for part in msg.walk():
                if part.get_filename():
                    count += 1
        return count
    except Exception:
        return 0


@app.get("/api/thread-by-slug/{slug}")
def api_thread_by_slug(slug: str, limit: int = 100):
    """Get all messages in a thread by thread_slug using index.db.

    This is the preferred endpoint for thread URLs.
    """
    root = get_root()
    with get_index_db(root) as db:
        messages = db.get_thread_by_slug(slug=slug, limit=limit)
        if not messages:
            return JSONResponse({"error": "Thread not found"}, status_code=404)
        return {
            "thread_slug": slug,
            "thread_id": messages[0].thread_id,
            "count": len(messages),
            "messages": [
                {
                    "folder": extract_folder(m.path),
                    "subject": m.subject,
                    "message_id": m.message_id,
                    "thread_id": m.thread_id,
                    "thread_slug": m.thread_slug,
                    "local_path": m.path,
                    "msg_date": m.date.isoformat() if m.date else None,
                    "in_reply_to": m.in_reply_to,
                    "references": m.references,
                    "from_addr": m.from_addr,
                    "to_addr": m.to_addr,
                    "attachment_count": count_attachments(root, m.path),
                }
                for m in messages
            ],
        }


@app.get("/api/replies/{message_id:path}")
def api_replies(message_id: str, limit: int = 100):
    """Get direct replies to a message using index.db."""
    root = get_root()
    with get_index_db(root) as db:
        messages = db.get_replies(message_id=message_id, limit=limit)
        return {
            "message_id": message_id,
            "count": len(messages),
            "replies": [
                {
                    "folder": extract_folder(m.path),
                    "message_id": m.message_id,
                    "local_path": m.path,
                    "msg_date": m.date.isoformat() if m.date else None,
                    "in_reply_to": m.in_reply_to,
                    "from_addr": m.from_addr,
                    "to_addr": m.to_addr,
                }
                for m in messages
            ],
        }


@app.post("/api/fts/rebuild")
def api_rebuild_fts():
    """Rebuild the full-text search index from index.db."""
    root = get_root()
    with get_index_db(root) as db:
        count = db.rebuild_fts()
        return {"status": "ok", "indexed": count}


@app.post("/api/sync-runs/cleanup-stale")
def api_cleanup_stale_runs(max_age_minutes: int = 60):
    """Mark stale running sync runs as aborted.

    Args:
        max_age_minutes: Consider runs stale if started more than this many minutes ago (default: 60)
    """
    root = get_root()
    with get_pulls_db(root) as db:
        count = db.cleanup_stale_runs(max_age_minutes)
        return {"status": "ok", "cleaned": count}


def _is_year_dir(name: str) -> bool:
    """Check if directory name looks like a year (YYYY)."""
    return len(name) == 4 and name.isdigit() and 1990 <= int(name) <= 2100


def _find_folder_roots(account_path: Path) -> list[Path]:
    """Find IMAP folder root directories within an account.

    A folder root is a directory that contains year (YYYY) subdirectories,
    indicating the start of our date-organized .eml tree.

    This handles nested IMAP folders like "Inbox/Subfolder" where the
    folder path is "Inbox/Subfolder" and it contains "2023/09/11/*.eml".
    """
    folder_roots = []

    def walk(path: Path, depth: int = 0):
        if depth > 10:  # Safety limit
            return

        if not path.is_dir():
            return

        # Check if this directory contains year subdirectories
        has_year_child = any(
            _is_year_dir(child.name) and child.is_dir()
            for child in path.iterdir()
            if not child.name.startswith(".")
        )

        if has_year_child:
            # This is a folder root
            folder_roots.append(path)
        else:
            # Keep looking in subdirectories
            for child in path.iterdir():
                if child.is_dir() and not child.name.startswith("."):
                    walk(child, depth + 1)

    walk(account_path)
    return folder_roots


@app.get("/api/fs-folders")
def api_fs_folders(account: str | None = None):
    """Get folders from filesystem layout (not pulls.db).

    This lists folders that exist on disk, which may differ from
    what's tracked in pulls.db (e.g., v1 pulls without db tracking).

    Folders are identified by finding directories that contain YYYY
    subdirectories (our date-organized .eml tree structure).

    Supports two layouts:
    1. Single-account: <root>/<folder>/YYYY/... (account defaults to "_")
    2. Multi-account: <root>/<account>/<folder>/YYYY/...
    """
    root = get_root()

    folders = []

    # First check if root itself contains folder roots (single-account layout)
    # by looking for directories with YYYY children directly under root
    root_folder_roots = _find_folder_roots(root)
    if root_folder_roots:
        # Single-account layout - folders are directly under root
        default_account = "_"
        if account and account != default_account:
            return {"folders": []}

        for folder_path in root_folder_roots:
            folder_name = str(folder_path.relative_to(root))
            eml_count = len(list(folder_path.rglob("*.eml")))
            if eml_count > 0:
                folders.append({
                    "account": default_account,
                    "folder": folder_name,
                    "path": str(folder_path.relative_to(root)),
                    "eml_count": eml_count,
                })
    else:
        # Multi-account layout - look for account directories
        for path in root.iterdir():
            if not path.is_dir() or path.name.startswith("."):
                continue
            acct = path.name
            if account and acct != account:
                continue

            # Find folder roots (directories with YYYY children)
            for folder_path in _find_folder_roots(path):
                folder_name = str(folder_path.relative_to(path))
                eml_count = len(list(folder_path.rglob("*.eml")))
                if eml_count > 0:
                    folders.append({
                        "account": acct,
                        "folder": folder_name,
                        "path": str(folder_path.relative_to(root)),
                        "eml_count": eml_count,
                    })

    # Sort by account, then folder name
    folders.sort(key=lambda x: (x["account"], x["folder"]))

    return {"folders": folders}


@app.get("/api/fs-emails/{account}/{folder:path}")
def api_fs_emails(
    account: str,
    folder: str,
    limit: int = 100,
    offset: int = 0,
    sort: str = "date_desc",
):
    """List emails in a folder from filesystem.

    Args:
        account: Account name (e.g., "y" or "_" for single-account repos)
        folder: Folder path, may contain slashes (e.g., "Inbox" or "Inbox/Subfolder")
        limit: Max emails to return
        offset: Offset for pagination
        sort: Sort order ("date_desc", "date_asc", "name")
    """
    from email import policy

    root = get_root()
    # For single-account repos, account="_" means folder is directly under root
    if account == "_":
        folder_path = root / folder
    else:
        folder_path = root / account / folder

    # Security check
    try:
        folder_path = folder_path.resolve()
        if not str(folder_path).startswith(str(root.resolve())):
            return JSONResponse({"error": "Access denied"}, status_code=403)
    except Exception:
        return JSONResponse({"error": "Invalid path"}, status_code=400)

    if not folder_path.exists() or not folder_path.is_dir():
        return JSONResponse({"error": "Folder not found"}, status_code=404)

    # Find all .eml files recursively
    eml_files = list(folder_path.rglob("*.eml"))

    # Sort files
    if sort == "date_desc":
        eml_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    elif sort == "date_asc":
        eml_files.sort(key=lambda p: p.stat().st_mtime)
    else:  # name
        eml_files.sort(key=lambda p: p.name)

    total = len(eml_files)
    eml_files = eml_files[offset : offset + limit]

    # Parse headers from each email
    emails = []
    for path in eml_files:
        rel_path = str(path.relative_to(root))
        try:
            with open(path, "rb") as f:
                # Only parse headers for speed
                msg = email.message_from_binary_file(f, policy=policy.default)

            emails.append({
                "path": rel_path,
                "subject": msg.get("Subject", "(no subject)"),
                "from": msg.get("From", ""),
                "to": msg.get("To", ""),
                "date": msg.get("Date", ""),
                "size": path.stat().st_size,
            })
        except Exception as e:
            emails.append({
                "path": rel_path,
                "subject": f"(error: {e})",
                "from": "",
                "to": "",
                "date": "",
                "size": path.stat().st_size,
            })

    return {
        "account": account,
        "folder": folder,
        "total": total,
        "offset": offset,
        "limit": limit,
        "emails": emails,
    }


@app.get("/api/fs-threads/{account}/{folder:path}")
def api_fs_threads(
    account: str,
    folder: str,
    limit: int = 50,
    offset: int = 0,
):
    """List threads (conversations) in a folder from index.db.

    Returns threads grouped by thread_id, showing only the latest message
    from each thread with a count of messages in that thread.

    Args:
        account: Account name (e.g., "y" or "_" for single-account repos)
        folder: Folder path (e.g., "Inbox")
        limit: Max threads to return
        offset: Offset for pagination
    """
    root = get_root()
    index_db = root / ".eml" / "index.db"

    if not index_db.exists():
        return JSONResponse(
            {"error": "index.db not found. Run 'eml rebuild-index' first."},
            status_code=404
        )

    # Build folder path pattern for SQL LIKE
    if account == "_":
        folder_pattern = f"{folder}/%"
    else:
        folder_pattern = f"{account}/{folder}/%"

    conn = sqlite3.connect(index_db)
    conn.row_factory = sqlite3.Row

    # Get total thread count
    count_sql = """
        SELECT COUNT(DISTINCT COALESCE(thread_id, path)) as cnt
        FROM files
        WHERE path LIKE ?
    """
    total = conn.execute(count_sql, (folder_pattern,)).fetchone()["cnt"]

    # Get threads: latest message per thread_id, ordered by max date
    # For messages without thread_id, treat each as its own thread
    threads_sql = """
        WITH thread_stats AS (
            SELECT
                COALESCE(thread_id, path) as tid,
                COUNT(*) as msg_count,
                MAX(date) as latest_date,
                GROUP_CONCAT(DISTINCT from_addr) as participants
            FROM files
            WHERE path LIKE ?
            GROUP BY COALESCE(thread_id, path)
        ),
        latest_messages AS (
            SELECT
                f.*,
                ts.msg_count,
                ts.participants,
                ROW_NUMBER() OVER (
                    PARTITION BY COALESCE(f.thread_id, f.path)
                    ORDER BY f.date DESC
                ) as rn
            FROM files f
            JOIN thread_stats ts ON COALESCE(f.thread_id, f.path) = ts.tid
            WHERE f.path LIKE ?
        )
        SELECT
            path, subject, from_addr, to_addr, date, size,
            thread_id, thread_slug, msg_count, participants
        FROM latest_messages
        WHERE rn = 1
        ORDER BY date DESC
        LIMIT ? OFFSET ?
    """

    rows = conn.execute(threads_sql, (folder_pattern, folder_pattern, limit, offset)).fetchall()
    conn.close()

    threads = []
    for row in rows:
        threads.append({
            "path": row["path"],
            "subject": row["subject"] or "(no subject)",
            "from": row["from_addr"] or "",
            "to": row["to_addr"] or "",
            "date": row["date"] or "",
            "size": row["size"] or 0,
            "thread_id": row["thread_id"],
            "thread_slug": row["thread_slug"],
            "msg_count": row["msg_count"],
            "participants": row["participants"] or "",
        })

    return {
        "account": account,
        "folder": folder,
        "total": total,
        "offset": offset,
        "limit": limit,
        "threads": threads,
    }


@app.get("/api/sync-status")
def api_sync_status():
    """Get current sync operation status from SQLite."""
    root = get_root()
    db_path = root / ".eml" / "sync-status.db"
    if not db_path.exists():
        return {"running": False}

    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.execute("SELECT * FROM sync_status WHERE id = 1")
        row = cur.fetchone()
        conn.close()

        if not row:
            return {"running": False}

        status = dict(row)
        pid = status.get("pid")

        # Check if process is still running
        if pid:
            try:
                os.kill(pid, 0)  # Signal 0 just checks if process exists
            except OSError:
                # Process not running, stale status
                return {"running": False}

        return {
            "running": True,
            "operation": status.get("operation"),
            "account": status.get("account"),
            "folder": status.get("folder"),
            "total": status.get("total", 0),
            "completed": status.get("completed", 0),
            "skipped": status.get("skipped", 0),
            "failed": status.get("failed", 0),
            "current_subject": status.get("current_subject"),
            "started": status.get("started"),
            "pid": pid,
        }
    except Exception as e:
        return {"running": False, "error": str(e)}


@app.get("/api/stream")
async def api_stream(request: Request):
    """Server-Sent Events stream for real-time updates."""
    from sse_starlette.sse import EventSourceResponse

    async def event_generator():
        last_pulled_at = None
        last_sync_hash = None

        while True:
            if await request.is_disconnected():
                break

            root = get_root()
            events = []

            # Check for new pulls by comparing max(pulled_at)
            try:
                with get_pulls_db(root) as db:
                    cur = db.conn.execute("SELECT MAX(pulled_at) as max_at FROM pulled_messages")
                    row = cur.fetchone()
                    current_max = row["max_at"] if row else None

                    if current_max and current_max != last_pulled_at:
                        last_pulled_at = current_max
                        # Get latest stats
                        events.append({
                            "event": "status",
                            "data": json.dumps(api_status())
                        })
                        events.append({
                            "event": "recent",
                            "data": json.dumps(api_recent())
                        })
            except Exception:
                pass

            # Check for sync status changes
            try:
                sync = api_sync_status()
                sync_hash = f"{sync.get('completed', 0)}:{sync.get('skipped', 0)}:{sync.get('running', False)}"
                if sync_hash != last_sync_hash:
                    last_sync_hash = sync_hash
                    events.append({
                        "event": "sync",
                        "data": json.dumps(sync)
                    })
            except Exception:
                pass

            for event in events:
                yield event

            await asyncio.sleep(1)  # Poll every second

    return EventSourceResponse(event_generator())


@app.get("/", response_class=HTMLResponse)
def dashboard():
    """Serve the React build."""
    ui_dist = Path(__file__).parent.parent.parent / "ui" / "dist" / "index.html"
    if not ui_dist.exists():
        return HTMLResponse(
            "<h1>UI not built</h1><p>Run <code>cd ui && pnpm build</code> to build the React frontend.</p>",
            status_code=503,
        )
    return HTMLResponse(ui_dist.read_text())


# Serve static assets from ui/dist if available
_ui_dist_dir = Path(__file__).parent.parent.parent / "ui" / "dist"
if _ui_dist_dir.exists():
    from fastapi.staticfiles import StaticFiles
    app.mount("/assets", StaticFiles(directory=_ui_dist_dir / "assets"), name="assets")


def main(host: str = "127.0.0.1", port: int = 8765, reload: bool = False):
    """Run the web server."""
    ui_dist = Path(__file__).parent.parent.parent / "ui" / "dist"
    if not ui_dist.exists():
        print(f"Warning: UI not built. Run 'cd ui && pnpm build' first.")
    print(f"Starting EML web UI at http://{host}:{port}")
    if reload:
        # Use string import for reload to work properly
        uvicorn.run("eml.web:app", host=host, port=port, log_level="warning", reload=True, reload_dirs=[str(Path(__file__).parent)])
    else:
        uvicorn.run(app, host=host, port=port, log_level="warning")


if __name__ == "__main__":
    main()
