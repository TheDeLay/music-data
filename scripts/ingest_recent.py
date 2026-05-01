"""CLI: pull recent plays from Spotify's API and load them into the DB.

Designed to be cron- or n8n-friendly: returns nonzero exit on hard errors,
emits a one-line summary on success.

Usage:
    python scripts/ingest_recent.py
"""
from __future__ import annotations

import sys

from rich.console import Console

from .db import connect, init_schema, start_run, finish_run
from .extractors import from_recently_played_item, safe_extract
from .loader import LoadStats, load_play
from .spotify_client import SpotifyClient

console = Console()


def main() -> int:
    conn = connect()
    init_schema(conn)
    run_id = start_run(conn, source="recently_played_api")
    stats = LoadStats()

    try:
        client = SpotifyClient()
        items = client.recently_played(limit=50)
    except Exception as e:
        finish_run(conn, run_id, status="failed", notes=str(e))
        console.print(f"[red]Spotify API error:[/red] {e}")
        return 1

    conn.execute("BEGIN")
    try:
        for item in items:
            rec, err = safe_extract(from_recently_played_item, item)
            if err is not None:
                stats.quarantined += 1
                continue
            outcome = load_play(conn, rec, run_id)
            if outcome == "added":
                stats.added += 1
            else:
                stats.skipped += 1
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        finish_run(conn, run_id, status="failed", notes="batch error")
        raise

    finish_run(conn, run_id, status="completed",
               rows_added=stats.added, rows_skipped=stats.skipped, rows_failed=stats.failed)

    console.print(
        f"recently_played: {len(items)} items | "
        f"added={stats.added} skipped={stats.skipped} quarantined={stats.quarantined}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
