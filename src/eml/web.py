#!/usr/bin/env -S uv run
# /// script
# dependencies = ["fastapi", "uvicorn", "sse-starlette"]
# ///
"""Web UI for EML status monitoring.

Run with: eml web
Or directly: python -m eml.web
"""

import asyncio
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


DASHBOARD_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>EML Status</title>
    <meta charset="utf-8">
    <style>
        * { box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, monospace;
            background: #1a1a2e;
            color: #eee;
            margin: 0;
            padding: 20px;
        }
        h1 { color: #00d4aa; margin-top: 0; }
        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; }
        .card {
            background: #16213e;
            border-radius: 8px;
            padding: 20px;
            border: 1px solid #0f3460;
        }
        .card h2 { margin-top: 0; color: #00d4aa; font-size: 1.1em; }
        .stat { font-size: 2em; font-weight: bold; color: #fff; }
        .stat-label { color: #888; font-size: 0.9em; }
        .bar-chart { margin-top: 10px; }
        .bar-row { display: flex; align-items: center; margin: 2px 0; font-size: 0.85em; }
        .bar-label { width: 140px; color: #888; }
        .bar-value { width: 50px; text-align: right; margin-right: 10px; }
        .bar-container { display: flex; height: 18px; }
        .bar { height: 18px; }
        .bar-new { background: #00d4aa; border-radius: 2px 0 0 2px; }
        .bar-deduped { background: #666; border-radius: 0 2px 2px 0; }
        .bar-legend { display: flex; gap: 15px; margin-bottom: 10px; font-size: 0.8em; }
        .legend-item { display: flex; align-items: center; gap: 5px; }
        .legend-color { width: 12px; height: 12px; border-radius: 2px; }
        .recent-list { font-size: 0.85em; max-height: 600px; overflow-y: auto; }
        .recent-item { padding: 5px 0; border-bottom: 1px solid #0f3460; }
        .recent-item.new { }
        .recent-item.skipped { opacity: 0.6; }
        .recent-time { color: #888; }
        .recent-path { color: #00d4aa; word-break: break-all; }
        .recent-path.skipped { color: #666; }
        .recent-badge { font-size: 0.7em; padding: 2px 6px; border-radius: 3px; margin-left: 5px; }
        .badge-new { background: #00d4aa; color: #000; }
        .badge-skip { background: #444; color: #888; }
        .sync-status { padding: 10px; border-radius: 4px; margin-bottom: 20px; }
        .sync-status code { background: #1a1a2e; padding: 2px 6px; border-radius: 3px; font-family: monospace; }
        .sync-running { background: #0f3460; border: 1px solid #00d4aa; }
        .sync-idle { background: #1a1a2e; border: 1px solid #333; color: #666; }
        .progress-bar { background: #333; height: 8px; border-radius: 4px; margin-top: 5px; }
        .progress-fill { background: #00d4aa; height: 100%; border-radius: 4px; transition: width 0.3s; }
        #last-update { color: #666; font-size: 0.8em; }
        .live-indicator { display: inline-block; width: 8px; height: 8px; background: #00d4aa; border-radius: 50%; margin-right: 5px; animation: pulse 2s infinite; }
        @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.5; } }
        .folder-nav { display: flex; gap: 10px; margin-bottom: 15px; flex-wrap: wrap; }
        .folder-btn { padding: 5px 12px; border-radius: 4px; border: 1px solid #0f3460; background: #16213e; color: #888; cursor: pointer; font-size: 0.85em; }
        .folder-btn:hover { border-color: #00d4aa; color: #ccc; }
        .folder-btn.active { background: #0f3460; border-color: #00d4aa; color: #00d4aa; }
        .folder-count { font-size: 0.8em; color: #666; margin-left: 5px; }
    </style>
</head>
<body>
    <h1><span class="live-indicator"></span>EML Pull Status</h1>
    <div id="folder-nav" class="folder-nav">Loading folders...</div>
    <div id="sync-status" class="sync-status sync-idle">Connecting...</div>
    <div class="grid">
        <div class="card">
            <h2>UID Summary</h2>
            <div id="uid-summary">Loading...</div>
        </div>
        <div class="card">
            <h2>Activity by Hour (last 24h)</h2>
            <div class="bar-legend">
                <div class="legend-item"><div class="legend-color" style="background:#00d4aa"></div> New</div>
                <div class="legend-item"><div class="legend-color" style="background:#666"></div> Deduped</div>
            </div>
            <div id="histogram" class="bar-chart">Loading...</div>
        </div>
        <div class="card">
            <h2>Recent Activity</h2>
            <div id="recent" class="recent-list">Loading...</div>
        </div>
    </div>
    <p id="last-update"></p>

    <script>
        function formatNumber(n) {
            return n.toLocaleString();
        }

        function updateStatus(data) {
            if (data.error) {
                document.getElementById('uid-summary').innerHTML = `<p style="color:#f66">${data.error}</p>`;
                return;
            }
            document.getElementById('uid-summary').innerHTML = `
                <div><span class="stat">${formatNumber(data.server_uids)}</span> <span class="stat-label">server UIDs</span></div>
                <div><span class="stat">${formatNumber(data.pulled_uids)}</span> <span class="stat-label">pulled</span></div>
                <div><span class="stat" style="color:#f90">${formatNumber(data.unpulled_uids)}</span> <span class="stat-label">unpulled</span></div>
                <div><span class="stat-label">${formatNumber(data.no_message_id)} without Message-ID</span></div>
            `;
            document.getElementById('last-update').textContent = `Last updated: ${new Date().toLocaleTimeString()}`;
        }

        function updateHistogram(data) {
            const maxCount = Math.max(...data.data.map(d => d.new + d.deduped), 1);
            document.getElementById('histogram').innerHTML = data.data.map(d => {
                const total = d.new + d.deduped;
                const newWidth = (d.new / maxCount) * 200;
                const dedupedWidth = (d.deduped / maxCount) * 200;
                return `
                <div class="bar-row">
                    <span class="bar-label">${d.hour}</span>
                    <span class="bar-value">${formatNumber(total)}</span>
                    <div class="bar-container">
                        ${d.new > 0 ? `<div class="bar bar-new" style="width: ${newWidth}px" title="${d.new} new"></div>` : ''}
                        ${d.deduped > 0 ? `<div class="bar bar-deduped" style="width: ${dedupedWidth}px" title="${d.deduped} deduped"></div>` : ''}
                    </div>
                </div>
            `}).join('');
        }

        function updateRecent(data) {
            document.getElementById('recent').innerHTML = data.pulls.map(p => {
                const badge = p.is_new
                    ? '<span class="recent-badge badge-new">NEW</span>'
                    : '<span class="recent-badge badge-skip">SKIP</span>';
                const itemClass = p.is_new ? 'recent-item new' : 'recent-item skipped';
                const subject = p.subject || '(no subject)';
                const msgDate = p.msg_date ? p.msg_date.slice(0, 10) : '';
                return `
                    <div class="${itemClass}">
                        <span class="recent-time">${p.pulled_at.replace('T', ' ').slice(0, 19)}</span>${badge}
                        ${msgDate ? `<span style="color:#888;margin-left:10px">${msgDate}</span>` : ''}<br>
                        <span style="color:#00d4aa">${subject.slice(0, 60)}${subject.length > 60 ? '...' : ''}</span>
                    </div>
                `;
            }).join('') || '<p style="color:#666">No recent activity</p>';
        }

        function updateSync(data) {
            const syncEl = document.getElementById('sync-status');
            if (data.running) {
                const pct = data.total ? Math.round(data.completed / data.total * 100) : 0;
                const details = [];
                if (data.skipped > 0) details.push(`${formatNumber(data.skipped)} skipped`);
                if (data.failed > 0) details.push(`${formatNumber(data.failed)} failed`);
                const detailsStr = details.length ? ` [${details.join(', ')}]` : '';

                syncEl.className = 'sync-status sync-running';
                syncEl.innerHTML = `
                    <strong>● ${data.operation} in progress:</strong> <code>${data.account}/${data.folder}</code>
                    ${formatNumber(data.completed)} / ${formatNumber(data.total)} (${pct}%)${detailsStr}
                    ${data.current_subject ? `<br><small>Current: ${data.current_subject}</small>` : ''}
                    <div class="progress-bar"><div class="progress-fill" style="width:${pct}%"></div></div>
                `;
            } else {
                syncEl.className = 'sync-status sync-idle';
                syncEl.innerHTML = '○ No sync running';
            }
        }

        // Current folder state
        let currentAccount = 'y';
        let currentFolder = 'Inbox';

        function updateFolderNav(data) {
            const nav = document.getElementById('folder-nav');
            if (!data.folders || data.folders.length === 0) {
                nav.innerHTML = '<span style="color:#666">No folders found</span>';
                return;
            }
            nav.innerHTML = data.folders.map(f => {
                const isActive = f.folder === currentFolder && f.account === currentAccount;
                return `<button class="folder-btn ${isActive ? 'active' : ''}"
                    onclick="selectFolder('${f.account}', '${f.folder}')">${f.folder}
                    <span class="folder-count">${formatNumber(f.count)}</span></button>`;
            }).join('');
        }

        async function selectFolder(account, folder) {
            currentAccount = account;
            currentFolder = folder;
            await refreshAll();
            // Update nav buttons
            document.querySelectorAll('.folder-btn').forEach(btn => {
                btn.classList.toggle('active', btn.textContent.trim().startsWith(folder));
            });
        }

        async function refreshAll() {
            try {
                const params = `account=${currentAccount}&folder=${currentFolder}`;
                const [status, hist, recent] = await Promise.all([
                    fetch(`/api/status?${params}`).then(r => r.json()),
                    fetch(`/api/histogram?${params}`).then(r => r.json()),
                    fetch(`/api/recent?${params}`).then(r => r.json()),
                ]);
                updateStatus(status);
                updateHistogram(hist);
                updateRecent(recent);
            } catch (e) {
                console.error('Refresh failed:', e);
            }
        }

        // Initial fetch
        async function initialFetch() {
            try {
                // First get folders
                const folders = await fetch('/api/folders').then(r => r.json());
                updateFolderNav(folders);

                const params = `account=${currentAccount}&folder=${currentFolder}`;
                const [status, hist, recent, sync] = await Promise.all([
                    fetch(`/api/status?${params}`).then(r => r.json()),
                    fetch(`/api/histogram?${params}`).then(r => r.json()),
                    fetch(`/api/recent?${params}`).then(r => r.json()),
                    fetch('/api/sync-status').then(r => r.json()),
                ]);
                updateStatus(status);
                updateHistogram(hist);
                updateRecent(recent);
                updateSync(sync);
            } catch (e) {
                console.error('Initial fetch failed:', e);
            }
        }

        // SSE for real-time updates
        function connectSSE() {
            const evtSource = new EventSource('/api/stream');

            evtSource.addEventListener('status', (e) => {
                updateStatus(JSON.parse(e.data));
            });

            evtSource.addEventListener('recent', (e) => {
                updateRecent(JSON.parse(e.data));
            });

            evtSource.addEventListener('sync', (e) => {
                updateSync(JSON.parse(e.data));
            });

            evtSource.onerror = () => {
                console.log('SSE connection lost, reconnecting...');
                evtSource.close();
                setTimeout(connectSSE, 2000);
            };
        }

        // Refresh histogram periodically (less frequent)
        setInterval(async () => {
            try {
                const params = `account=${currentAccount}&folder=${currentFolder}`;
                const hist = await fetch(`/api/histogram?${params}`).then(r => r.json());
                updateHistogram(hist);
            } catch (e) {}
        }, 30000);

        // Refresh folder list periodically (for new folders)
        setInterval(async () => {
            try {
                const folders = await fetch('/api/folders').then(r => r.json());
                updateFolderNav(folders);
            } catch (e) {}
        }, 60000);

        initialFetch();
        connectSSE();
    </script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def dashboard():
    """Serve the dashboard HTML."""
    return DASHBOARD_HTML


def main(host: str = "127.0.0.1", port: int = 8765):
    """Run the web server."""
    print(f"Starting EML web UI at http://{host}:{port}")
    uvicorn.run(app, host=host, port=port, log_level="warning")


if __name__ == "__main__":
    main()
