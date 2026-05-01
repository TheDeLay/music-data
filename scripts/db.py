"""Database connection and schema management.

This module owns all interaction with the SQLite database file. Every other
module gets its connection from here so configuration (path, pragmas, row
factory) lives in exactly one place.

Usage:
    # As a library:
    from scripts.db import connect, init_schema
    conn = connect()

    # As a CLI:
    python scripts/db.py init      # create or migrate schema
    python scripts/db.py info      # print DB stats
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from dotenv import load_dotenv

load_dotenv()

# -----------------------------------------------------------------------------
# Paths
# -----------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCHEMA_PATH = PROJECT_ROOT / "sql" / "schema.sql"

DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "music.db"
DB_PATH = Path(os.environ.get("MUSIC_DB_PATH", DEFAULT_DB_PATH))


# -----------------------------------------------------------------------------
# Connection
# -----------------------------------------------------------------------------
def connect(db_path: Path | str | None = None) -> sqlite3.Connection:
    """Open a connection with sane defaults.

    - foreign_keys ON (enforces FK constraints)
    - journal_mode WAL (better concurrent read perf, safer crash recovery)
    - row_factory = sqlite3.Row (rows accessible by column name)
    """
    path = Path(db_path) if db_path else DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), isolation_level=None)  # autocommit; we manage txns explicitly
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")  # WAL + NORMAL is the recommended pairing
    return conn


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Run a block in a transaction. Commits on success, rolls back on error.

    Because we opened the connection with isolation_level=None (autocommit),
    we manage transactions explicitly with BEGIN/COMMIT/ROLLBACK. This gives
    us clean batch boundaries without sqlite3's implicit transaction quirks.
    """
    conn.execute("BEGIN")
    try:
        yield conn
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


# -----------------------------------------------------------------------------
# Schema management
# -----------------------------------------------------------------------------
def init_schema(conn: sqlite3.Connection) -> None:
    """Apply schema.sql to the connection. Idempotent (CREATE IF NOT EXISTS)."""
    schema_sql = SCHEMA_PATH.read_text()
    conn.executescript(schema_sql)


def get_schema_version(conn: sqlite3.Connection) -> str | None:
    """Return the schema version recorded in schema_meta, or None if not initialized."""
    try:
        row = conn.execute("SELECT value FROM schema_meta WHERE key = 'version'").fetchone()
        return row["value"] if row else None
    except sqlite3.OperationalError:
        return None  # schema_meta doesn't exist yet


def db_info(conn: sqlite3.Connection) -> dict:
    """Quick stats about the database for the `info` CLI."""
    info = {
        "db_path": str(DB_PATH),
        "schema_version": get_schema_version(conn),
        "size_bytes": DB_PATH.stat().st_size if DB_PATH.exists() else 0,
        "counts": {},
    }
    tables = [
        "plays", "tracks", "artists", "albums",
        "shows", "episodes", "audiobooks", "audiobook_chapters",
        "track_labels", "album_labels", "artist_labels",
        "ingestion_runs", "rejected_rows",
    ]
    for t in tables:
        try:
            row = conn.execute(f"SELECT COUNT(*) AS c FROM {t}").fetchone()
            info["counts"][t] = row["c"]
        except sqlite3.OperationalError:
            info["counts"][t] = "(missing)"
    return info


# -----------------------------------------------------------------------------
# Ingestion run helpers
# -----------------------------------------------------------------------------
def start_run(conn: sqlite3.Connection, source: str, input_path: str | None = None,
              notes: str | None = None) -> int:
    """Insert a new ingestion_runs row with status='running' and return run_id."""
    cur = conn.execute(
        """
        INSERT INTO ingestion_runs (source, status, input_path, notes)
        VALUES (?, 'running', ?, ?)
        """,
        (source, input_path, notes),
    )
    return cur.lastrowid


def finish_run(conn: sqlite3.Connection, run_id: int, *, status: str = "completed",
               rows_added: int = 0, rows_skipped: int = 0, rows_failed: int = 0,
               notes: str | None = None) -> None:
    """Mark an ingestion run finished with final counts."""
    conn.execute(
        """
        UPDATE ingestion_runs
        SET status = ?, completed_at = datetime('now'),
            rows_added = ?, rows_skipped = ?, rows_failed = ?,
            notes = COALESCE(?, notes)
        WHERE run_id = ?
        """,
        (status, rows_added, rows_skipped, rows_failed, notes, run_id),
    )


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="music-data DB management")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("init", help="Initialize or migrate the schema")
    sub.add_parser("info", help="Print database statistics")
    args = parser.parse_args(argv)

    conn = connect()
    try:
        if args.cmd == "init":
            before = get_schema_version(conn)
            init_schema(conn)
            after = get_schema_version(conn)
            if before is None:
                print(f"Initialized schema at {DB_PATH} (version {after})")
            elif before == after:
                print(f"Schema already at version {after}, no changes needed")
            else:
                print(f"Migrated schema from {before} to {after}")
            return 0
        elif args.cmd == "info":
            info = db_info(conn)
            print(f"DB path:        {info['db_path']}")
            print(f"Schema version: {info['schema_version']}")
            print(f"Size:           {info['size_bytes']:,} bytes")
            print("Row counts:")
            for table, count in info["counts"].items():
                print(f"  {table:<20} {count:>10}")
            return 0
    finally:
        conn.close()
    return 1


if __name__ == "__main__":
    sys.exit(main())
