#!/usr/bin/env python3
"""pmail - Simple web UI for browsing local email storage."""

import email
import sys
from email.policy import default as email_policy
from pathlib import Path

from flask import Flask, render_template, request, abort

# Add parent to path for eml imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from eml.storage import MessageStorage

app = Flask(__name__)

# Default database path (can override with ENV)
DB_PATH = Path(__file__).parent.parent / "emails.db"


def get_storage():
    """Get connected storage instance."""
    storage = MessageStorage(DB_PATH)
    storage.connect()
    return storage


@app.route("/")
def index():
    """List emails with optional search."""
    storage = get_storage()
    try:
        q = request.args.get("q", "")
        page = int(request.args.get("page", 1))
        per_page = 50
        offset = (page - 1) * per_page

        # Build query
        sql = "SELECT * FROM messages WHERE 1=1"
        params = []

        if q:
            sql += " AND (from_addr LIKE ? OR subject LIKE ? OR to_addr LIKE ?)"
            params.extend([f"%{q}%", f"%{q}%", f"%{q}%"])

        # Get total count
        count_sql = sql.replace("SELECT *", "SELECT COUNT(*)")
        total = storage.conn.execute(count_sql, params).fetchone()[0]

        # Get page of results
        sql += " ORDER BY date DESC LIMIT ? OFFSET ?"
        params.extend([per_page, offset])
        rows = storage.conn.execute(sql, params).fetchall()

        total_pages = (total + per_page - 1) // per_page

        return render_template(
            "index.html",
            messages=rows,
            q=q,
            page=page,
            total=total,
            total_pages=total_pages,
        )
    finally:
        storage.disconnect()


@app.route("/message/<int:msg_id>")
def message(msg_id: int):
    """View a single message."""
    storage = get_storage()
    try:
        row = storage.conn.execute(
            "SELECT * FROM messages WHERE id = ?", (msg_id,)
        ).fetchone()

        if not row:
            abort(404)

        # Parse the raw message for display
        raw = row["raw"]
        msg = email.message_from_bytes(raw, policy=email_policy)

        # Get body (prefer plain text)
        body = ""
        if msg.is_multipart():
            for part in msg.walk():
                if part.get_content_type() == "text/plain":
                    body = part.get_content()
                    break
                elif part.get_content_type() == "text/html" and not body:
                    body = part.get_content()
        else:
            body = msg.get_content()

        # Get attachments
        attachments = []
        if msg.is_multipart():
            for part in msg.walk():
                filename = part.get_filename()
                if filename:
                    attachments.append({
                        "filename": filename,
                        "content_type": part.get_content_type(),
                        "size": len(part.get_payload(decode=True) or b""),
                    })

        return render_template(
            "message.html",
            msg=row,
            body=body,
            attachments=attachments,
            headers={
                "From": msg.get("From", ""),
                "To": msg.get("To", ""),
                "Cc": msg.get("Cc", ""),
                "Date": msg.get("Date", ""),
                "Subject": msg.get("Subject", ""),
            },
        )
    finally:
        storage.disconnect()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="pmail web UI")
    parser.add_argument("-d", "--database", default="emails.db", help="SQLite database path")
    parser.add_argument("-p", "--port", type=int, default=5000, help="Port to run on")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind to")
    args = parser.parse_args()

    DB_PATH = Path(args.database)
    if not DB_PATH.exists():
        print(f"Database not found: {DB_PATH}")
        sys.exit(1)

    print(f"Starting pmail on http://{args.host}:{args.port}")
    print(f"Database: {DB_PATH}")
    app.run(host=args.host, port=args.port, debug=True)
