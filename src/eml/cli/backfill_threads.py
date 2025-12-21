#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Backfill threading fields (in_reply_to, references, thread_id, thread_slug) for existing pulled messages.

Reads the .eml files on disk and updates the pulls.db with threading headers.
"""

import email
import re
import sqlite3
from email.policy import default as email_policy
from pathlib import Path

import click

from eml.pulls import compute_thread_id, compute_thread_slug


@click.command()
@click.option('-n', '--dry-run', is_flag=True, help='Show what would be updated without making changes')
@click.option('-v', '--verbose', is_flag=True, help='Show each update')
@click.option('-l', '--limit', type=int, default=0, help='Limit number of messages to process (0 = all)')
@click.option('--thread-id-only', is_flag=True, help='Only backfill thread_id (skip in_reply_to/references)')
@click.option('--thread-slug-only', is_flag=True, help='Only backfill thread_slug from existing thread_id')
@click.pass_context
def backfill_threads(ctx, dry_run: bool, verbose: bool, limit: int, thread_id_only: bool, thread_slug_only: bool):
    """Backfill threading fields from .eml files into pulls.db."""
    from eml.config import find_eml_root

    root = find_eml_root()
    if not root:
        raise click.ClickException("Not in an eml project (no .eml directory found)")

    db_path = root / ".eml" / "pulls.db"
    if not db_path.exists():
        raise click.ClickException(f"Database not found: {db_path}")

    conn = sqlite3.connect(db_path, timeout=30.0)
    conn.row_factory = sqlite3.Row

    if thread_id_only:
        # Just compute thread_id from existing in_reply_to/references data
        query = """
            SELECT account, folder, uidvalidity, uid, message_id, in_reply_to, references_
            FROM pulled_messages
            WHERE (thread_id IS NULL OR thread_id = '')
              AND message_id IS NOT NULL
        """
        if limit > 0:
            query += f" LIMIT {limit}"

        cur = conn.execute(query)
        rows = cur.fetchall()

        total = len(rows)
        click.echo(f"Found {total} messages needing thread_id")

        if total == 0:
            return

        updated = 0
        for i, row in enumerate(rows):
            thread_id = compute_thread_id(row["message_id"], row["references_"], row["in_reply_to"])

            if thread_id and not dry_run:
                conn.execute("""
                    UPDATE pulled_messages
                    SET thread_id = ?
                    WHERE account = ? AND folder = ? AND uidvalidity = ? AND uid = ?
                """, (thread_id, row["account"], row["folder"], row["uidvalidity"], row["uid"]))

            updated += 1

            if not dry_run and updated % 5000 == 0:
                conn.commit()
                click.echo(f"  Progress: {updated}/{total}...")

        if not dry_run:
            conn.commit()

        click.echo(f"\nDone: {updated} thread_ids computed")
        if dry_run:
            click.echo("(dry run - no changes made)")
        return

    if thread_slug_only:
        # Compute thread_slug from existing thread_id, with collision handling
        # First get distinct thread_ids that need slugs
        query = """
            SELECT DISTINCT thread_id
            FROM pulled_messages
            WHERE thread_id IS NOT NULL
              AND thread_id != ''
              AND (thread_slug IS NULL OR thread_slug = '')
        """
        if limit > 0:
            query += f" LIMIT {limit}"

        cur = conn.execute(query)
        thread_ids = [row[0] for row in cur.fetchall()]

        total = len(thread_ids)
        click.echo(f"Found {total} distinct threads needing thread_slug")

        if total == 0:
            return

        # Build a cache of existing slugs to detect collisions
        slug_to_thread: dict[str, str] = {}
        cur = conn.execute("""
            SELECT DISTINCT thread_slug, thread_id
            FROM pulled_messages
            WHERE thread_slug IS NOT NULL AND thread_slug != ''
        """)
        for row in cur:
            slug_to_thread[row[0]] = row[1]

        updated = 0
        collisions = 0

        import base64
        import hashlib

        for thread_id in thread_ids:
            # Compute base slug
            base_slug = compute_thread_slug(thread_id)
            slug = base_slug

            # Check for collision
            if slug in slug_to_thread and slug_to_thread[slug] != thread_id:
                collisions += 1
                # Increment until we find a free slug
                slug_bytes = base64.urlsafe_b64decode(base_slug + '==')
                slug_int = int.from_bytes(slug_bytes, 'big')
                for _ in range(1000):
                    slug_int += 1
                    new_bytes = slug_int.to_bytes(6, 'big')
                    slug = base64.urlsafe_b64encode(new_bytes).decode().rstrip('=')
                    if slug not in slug_to_thread or slug_to_thread[slug] == thread_id:
                        break
                else:
                    slug = hashlib.sha256(thread_id.encode()).hexdigest()[:16]
                if verbose:
                    click.echo(f"  collision: {thread_id[:40]}... -> {slug}")

            # Register this slug
            slug_to_thread[slug] = thread_id

            if not dry_run:
                conn.execute("""
                    UPDATE pulled_messages
                    SET thread_slug = ?
                    WHERE thread_id = ?
                """, (slug, thread_id))

            updated += 1

            if not dry_run and updated % 5000 == 0:
                conn.commit()
                click.echo(f"  Progress: {updated}/{total}...")

        if not dry_run:
            conn.commit()

        click.echo(f"\nDone: {updated} thread_slugs computed, {collisions} collisions resolved")
        if dry_run:
            click.echo("(dry run - no changes made)")
        return

    # Full backfill from .eml files
    query = """
        SELECT account, folder, uidvalidity, uid, local_path, message_id
        FROM pulled_messages
        WHERE local_path IS NOT NULL
          AND (in_reply_to IS NULL OR in_reply_to = '')
          AND (references_ IS NULL OR references_ = '')
    """
    if limit > 0:
        query += f" LIMIT {limit}"

    cur = conn.execute(query)
    rows = cur.fetchall()

    total = len(rows)
    click.echo(f"Found {total} messages to backfill")

    if total == 0:
        return

    updated = 0
    skipped = 0
    errors = 0

    for i, row in enumerate(rows):
        local_path = row["local_path"]
        eml_path = root / local_path

        if not eml_path.exists():
            if verbose:
                click.echo(f"  skip: {local_path} (file not found)")
            skipped += 1
            continue

        try:
            with open(eml_path, "rb") as f:
                msg = email.message_from_binary_file(f, policy=email_policy)

            in_reply_to = msg.get("In-Reply-To", "") or ""
            references = msg.get("References", "") or ""
            message_id = row["message_id"]

            # Compute thread_id
            thread_id = compute_thread_id(message_id, references, in_reply_to)

            # Only update if we found threading info
            if not in_reply_to and not references:
                # Still update thread_id if we have message_id
                if thread_id and not dry_run:
                    conn.execute("""
                        UPDATE pulled_messages
                        SET thread_id = ?
                        WHERE account = ? AND folder = ? AND uidvalidity = ? AND uid = ?
                    """, (thread_id, row["account"], row["folder"], row["uidvalidity"], row["uid"]))
                skipped += 1
                continue

            if verbose:
                click.echo(f"  update: {local_path}")
                if in_reply_to:
                    click.echo(f"    In-Reply-To: {in_reply_to[:60]}...")
                if references:
                    ref_count = len(references.split())
                    click.echo(f"    References: {ref_count} message(s)")

            if not dry_run:
                conn.execute("""
                    UPDATE pulled_messages
                    SET in_reply_to = ?, references_ = ?, thread_id = ?
                    WHERE account = ? AND folder = ? AND uidvalidity = ? AND uid = ?
                """, (
                    in_reply_to or None,
                    references or None,
                    thread_id,
                    row["account"],
                    row["folder"],
                    row["uidvalidity"],
                    row["uid"],
                ))

            updated += 1

            # Commit periodically
            if not dry_run and updated % 1000 == 0:
                conn.commit()
                click.echo(f"  Progress: {updated}/{total} updated...")

        except Exception as e:
            if verbose:
                click.echo(f"  error: {local_path}: {e}")
            errors += 1

    if not dry_run:
        conn.commit()

    conn.close()

    click.echo(f"\nDone: {updated} updated, {skipped} skipped, {errors} errors")
    if dry_run:
        click.echo("(dry run - no changes made)")


if __name__ == "__main__":
    backfill_threads()
