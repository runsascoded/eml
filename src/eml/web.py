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
                    "is_new": p.local_path is not None,  # True if new file, False if deduped
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
