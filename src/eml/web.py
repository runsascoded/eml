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
import sqlite3
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
import uvicorn

from .config import get_eml_root
from .pulls import get_pulls_db


app = FastAPI(title="EML Status")


def get_root() -> Path:
    """Get project root, with fallback for when not in project dir."""
    try:
        return get_eml_root()
    except Exception:
        return Path.cwd()


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
def api_status(account: str = "y", folder: str = "Inbox"):
    """Get UID status summary."""
    root = get_root()
    pulls_db_path = root / ".eml" / "pulls.db"
    if not pulls_db_path.exists():
        return JSONResponse({"error": "No pulls.db found"}, status_code=404)

    with get_pulls_db(root) as db:
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
    """Get hourly activity histogram with new vs deduped breakdown."""
    root = get_root()
    with get_pulls_db(root) as db:
        data = db.get_activity_by_hour(account=account, folder=folder, limit_hours=hours)
        return {
            "hours": hours,
            "data": [{"hour": h, "new": new, "deduped": deduped} for h, new, deduped in data],
        }


@app.get("/api/recent")
def api_recent(limit: int = 20, account: str | None = None, folder: str | None = None):
    """Get recent activity (all pulls, including skipped/deduped)."""
    root = get_root()
    with get_pulls_db(root) as db:
        # Get recent pulls - both new files and deduped
        # with_path_only=False includes skipped (deduped) entries
        pulls = db.get_recent_pulls(limit=limit, account=account, folder=folder, with_path_only=False)
        return {
            "pulls": [
                {
                    "uid": p.uid,
                    "folder": p.folder,
                    "path": p.local_path,
                    "pulled_at": p.pulled_at.isoformat(),
                    "is_new": p.status != "skipped",  # True if new file, False if deduped
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

    # Get attachments
    attachments = []
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

    return {
        "path": path,
        "headers": headers,
        "body_html": body_html,
        "body_plain": body_plain,
        "attachments": attachments,
    }


@app.get("/api/sync-runs")
def api_sync_runs(
    limit: int = 20,
    account: str | None = None,
    folder: str | None = None,
    operation: str | None = None,
):
    """Get recent sync runs (pull/push operations)."""
    root = get_root()
    with get_pulls_db(root) as db:
        runs = db.get_recent_sync_runs(
            limit=limit,
            account=account,
            folder=folder,
            operation=operation,
        )
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
            ]
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
    account: str | None = None,
    folder: str | None = None,
):
    """Full-text search over emails.

    The query supports FTS5 syntax:
    - Simple terms: `meeting report`
    - Phrases: `"board meeting"`
    - AND/OR: `budget OR finance`
    - NOT: `report NOT draft`
    - Column-specific: `from_addr:john subject:budget`
    """
    root = get_root()
    with get_pulls_db(root) as db:
        try:
            results = db.search(query=q, limit=limit, account=account, folder=folder)
        except Exception as e:
            return JSONResponse({"error": f"Search error: {e}"}, status_code=400)

        return {
            "query": q,
            "count": len(results),
            "results": [
                {
                    "account": m.account,
                    "folder": m.folder,
                    "uid": m.uid,
                    "message_id": m.message_id,
                    "subject": m.subject,
                    "local_path": m.local_path,
                    "msg_date": m.msg_date,
                    "from_addr": m.from_addr,
                    "to_addr": m.to_addr,
                }
                for m in results
            ],
        }


@app.get("/api/thread/{message_id:path}")
def api_thread(message_id: str, limit: int = 100):
    """Get all messages in a thread by Message-ID.

    Returns messages that:
    - Have this message_id
    - Reply to this message_id (via In-Reply-To)
    - Reference this message_id (via References)
    """
    root = get_root()
    with get_pulls_db(root) as db:
        messages = db.get_thread(message_id=message_id, limit=limit)
        return {
            "message_id": message_id,
            "count": len(messages),
            "messages": [
                {
                    "account": m.account,
                    "folder": m.folder,
                    "uid": m.uid,
                    "message_id": m.message_id,
                    "local_path": m.local_path,
                    "msg_date": m.msg_date,
                    "in_reply_to": m.in_reply_to,
                    "references": m.references,
                    "from_addr": m.from_addr,
                    "to_addr": m.to_addr,
                }
                for m in messages
            ],
        }


@app.get("/api/replies/{message_id:path}")
def api_replies(message_id: str, limit: int = 100):
    """Get direct replies to a message."""
    root = get_root()
    with get_pulls_db(root) as db:
        messages = db.get_replies(message_id=message_id, limit=limit)
        return {
            "message_id": message_id,
            "count": len(messages),
            "replies": [
                {
                    "account": m.account,
                    "folder": m.folder,
                    "uid": m.uid,
                    "message_id": m.message_id,
                    "local_path": m.local_path,
                    "msg_date": m.msg_date,
                    "in_reply_to": m.in_reply_to,
                    "from_addr": m.from_addr,
                    "to_addr": m.to_addr,
                }
                for m in messages
            ],
        }


@app.post("/api/fts/rebuild")
def api_rebuild_fts():
    """Rebuild the full-text search index from pulled_messages."""
    root = get_root()
    with get_pulls_db(root) as db:
        count = db.rebuild_fts_index()
        return {"status": "ok", "indexed": count}


@app.get("/api/fs-folders")
def api_fs_folders(account: str | None = None):
    """Get folders from filesystem layout (not pulls.db).

    This lists folders that exist on disk, which may differ from
    what's tracked in pulls.db (e.g., v1 pulls without db tracking).
    """
    root = get_root()

    folders = []
    # List top-level directories (accounts)
    for path in root.iterdir():
        if not path.is_dir() or path.name.startswith("."):
            continue
        acct = path.name
        if account and acct != account:
            continue

        # List subfolders (IMAP folders)
        for folder_path in path.iterdir():
            if not folder_path.is_dir() or folder_path.name.startswith("."):
                continue

            # Count .eml files (recursively for date-organized layouts)
            eml_count = len(list(folder_path.rglob("*.eml")))
            if eml_count > 0:
                folders.append({
                    "account": acct,
                    "folder": folder_path.name,
                    "path": str(folder_path.relative_to(root)),
                    "eml_count": eml_count,
                })

    # Sort by account, then folder name
    folders.sort(key=lambda x: (x["account"], x["folder"]))

    return {"folders": folders}


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


def main(host: str = "127.0.0.1", port: int = 8765):
    """Run the web server."""
    ui_dist = Path(__file__).parent.parent.parent / "ui" / "dist"
    if not ui_dist.exists():
        print(f"Warning: UI not built. Run 'cd ui && pnpm build' first.")
    print(f"Starting EML web UI at http://{host}:{port}")
    uvicorn.run(app, host=host, port=port, log_level="warning")


if __name__ == "__main__":
    main()
