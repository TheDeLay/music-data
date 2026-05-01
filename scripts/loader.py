"""Loader: write PlayRecord objects into the database.

Responsibilities:
- Resolve names (artist, album) into entity rows, creating them if needed
- Insert plays with full FK linkage
- Batch in transactions for performance
- Idempotent: re-running on the same input adds no duplicates
- Track per-run counters (added, skipped, failed)

Entity dedup at ingest time uses normalized name matching for artist/album
because the dump only gives us names. Once enrichment runs and populates
spotify_*_uri columns, those become the canonical identity.
"""
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import Iterable

from .models import PlayRecord


# Batch size: large enough to amortize transaction overhead, small enough to
# keep memory usage trivial and crash-recovery cheap.
DEFAULT_BATCH_SIZE = 1000


@dataclass
class LoadStats:
    added: int = 0
    skipped: int = 0       # dedup hits — already in DB
    failed: int = 0        # validation/constraint errors
    quarantined: int = 0   # written to rejected_rows


def _normalize_name(name: str | None) -> str | None:
    if name is None:
        return None
    return name.strip().lower()


def _get_or_create_artist(conn: sqlite3.Connection, name: str) -> int:
    """Return artist_id for `name`, inserting if necessary."""
    norm = _normalize_name(name)
    row = conn.execute(
        "SELECT artist_id FROM artists WHERE name_normalized = ? LIMIT 1",
        (norm,),
    ).fetchone()
    if row:
        return row["artist_id"]
    cur = conn.execute(
        "INSERT INTO artists (name, name_normalized) VALUES (?, ?)",
        (name, norm),
    )
    return cur.lastrowid


def _get_or_create_album(conn: sqlite3.Connection, name: str) -> int:
    norm = _normalize_name(name)
    row = conn.execute(
        "SELECT album_id FROM albums WHERE name_normalized = ? LIMIT 1",
        (norm,),
    ).fetchone()
    if row:
        return row["album_id"]
    cur = conn.execute(
        "INSERT INTO albums (name, name_normalized) VALUES (?, ?)",
        (name, norm),
    )
    return cur.lastrowid


def _get_or_create_track(conn: sqlite3.Connection, uri: str, name: str | None,
                         album_id: int | None, artist_id: int | None) -> int:
    """Look up a track by URI; insert if missing. Wires the artist link too."""
    row = conn.execute(
        "SELECT track_id FROM tracks WHERE spotify_track_uri = ? LIMIT 1",
        (uri,),
    ).fetchone()
    if row:
        return row["track_id"]
    cur = conn.execute(
        "INSERT INTO tracks (spotify_track_uri, name, album_id) VALUES (?, ?, ?)",
        (uri, name or "(unknown)", album_id),
    )
    track_id = cur.lastrowid
    if artist_id is not None:
        conn.execute(
            "INSERT OR IGNORE INTO track_artists (track_id, artist_id, position) VALUES (?, ?, 0)",
            (track_id, artist_id),
        )
    return track_id


def _get_or_create_show(conn: sqlite3.Connection, name: str | None) -> int | None:
    if not name:
        return None
    row = conn.execute("SELECT show_id FROM shows WHERE name = ? LIMIT 1", (name,)).fetchone()
    if row:
        return row["show_id"]
    cur = conn.execute("INSERT INTO shows (name) VALUES (?)", (name,))
    return cur.lastrowid


def _get_or_create_episode(conn: sqlite3.Connection, uri: str, name: str | None,
                           show_id: int | None) -> int:
    row = conn.execute(
        "SELECT episode_id FROM episodes WHERE spotify_episode_uri = ? LIMIT 1",
        (uri,),
    ).fetchone()
    if row:
        return row["episode_id"]
    cur = conn.execute(
        "INSERT INTO episodes (spotify_episode_uri, name, show_id) VALUES (?, ?, ?)",
        (uri, name or "(unknown)", show_id),
    )
    return cur.lastrowid


def _get_or_create_audiobook(conn: sqlite3.Connection, uri: str | None, title: str | None) -> int | None:
    if not (uri or title):
        return None
    if uri:
        row = conn.execute(
            "SELECT audiobook_id FROM audiobooks WHERE spotify_audiobook_uri = ? LIMIT 1",
            (uri,),
        ).fetchone()
        if row:
            return row["audiobook_id"]
    cur = conn.execute(
        "INSERT INTO audiobooks (spotify_audiobook_uri, title) VALUES (?, ?)",
        (uri, title or "(unknown)"),
    )
    return cur.lastrowid


def _get_or_create_chapter(conn: sqlite3.Connection, uri: str, title: str | None,
                           audiobook_id: int | None) -> int:
    row = conn.execute(
        "SELECT audiobook_chapter_id FROM audiobook_chapters WHERE spotify_chapter_uri = ? LIMIT 1",
        (uri,),
    ).fetchone()
    if row:
        return row["audiobook_chapter_id"]
    cur = conn.execute(
        "INSERT INTO audiobook_chapters (spotify_chapter_uri, title, audiobook_id) VALUES (?, ?, ?)",
        (uri, title, audiobook_id),
    )
    return cur.lastrowid


# -----------------------------------------------------------------------------
# Source priority for dedup-on-conflict resolution
# -----------------------------------------------------------------------------
_SOURCE_PRIORITY = {
    "extended_dump": 3,        # most authoritative
    "recently_played_api": 2,
    "top_tracks_api": 1,
}


def _maybe_upgrade_source(conn: sqlite3.Connection, ts: str, content_uri: str,
                          ms_played: int, new_source: str) -> None:
    """If an existing play row has a less-authoritative source, upgrade it."""
    new_pri = _SOURCE_PRIORITY.get(new_source, 0)
    row = conn.execute(
        "SELECT play_id, source FROM plays WHERE ts = ? AND content_uri = ? AND ms_played = ?",
        (ts, content_uri, ms_played),
    ).fetchone()
    if row and _SOURCE_PRIORITY.get(row["source"], 0) < new_pri:
        conn.execute("UPDATE plays SET source = ? WHERE play_id = ?", (new_source, row["play_id"]))


# -----------------------------------------------------------------------------
# Main loader
# -----------------------------------------------------------------------------
def load_play(conn: sqlite3.Connection, rec: PlayRecord, run_id: int) -> str:
    """Load one PlayRecord. Returns 'added' | 'skipped' | 'upgraded'.

    Caller is responsible for transaction boundaries (we don't BEGIN/COMMIT here,
    so callers can batch).
    """
    # Resolve content-specific entities
    track_id = episode_id = chapter_id = None

    if rec.content_type == "track":
        artist_id = _get_or_create_artist(conn, rec.artist_name) if rec.artist_name else None
        album_id = _get_or_create_album(conn, rec.album_name) if rec.album_name else None
        track_id = _get_or_create_track(conn, rec.content_uri, rec.track_name, album_id, artist_id)
    elif rec.content_type == "episode":
        show_id = _get_or_create_show(conn, rec.show_name)
        episode_id = _get_or_create_episode(conn, rec.content_uri, rec.episode_name, show_id)
    elif rec.content_type == "audiobook_chapter":
        audiobook_id = _get_or_create_audiobook(conn, rec.audiobook_uri, rec.audiobook_title)
        chapter_id = _get_or_create_chapter(conn, rec.content_uri, rec.chapter_title, audiobook_id)

    # Insert with INSERT OR IGNORE; the unique index (ts, content_uri, ms_played)
    # handles dedup. lastrowid is 0/None if nothing was inserted.
    cur = conn.execute(
        """
        INSERT OR IGNORE INTO plays (
            ts, ms_played, content_type, content_uri,
            track_id, episode_id, audiobook_chapter_id,
            platform, conn_country, reason_start, reason_end,
            shuffle, skipped, offline, incognito_mode,
            source, ingestion_run_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            rec.ts, rec.ms_played, rec.content_type, rec.content_uri,
            track_id, episode_id, chapter_id,
            rec.platform, rec.conn_country, rec.reason_start, rec.reason_end,
            int(rec.shuffle) if rec.shuffle is not None else None,
            int(rec.skipped) if rec.skipped is not None else None,
            int(rec.offline) if rec.offline is not None else None,
            int(rec.incognito_mode) if rec.incognito_mode is not None else None,
            rec.source, run_id,
        ),
    )

    if cur.rowcount == 1:
        return "added"

    # Dedup hit: maybe upgrade source provenance
    _maybe_upgrade_source(conn, rec.ts, rec.content_uri, rec.ms_played, rec.source)
    return "skipped"


def quarantine(conn: sqlite3.Connection, run_id: int, raw: dict, reason: str) -> None:
    """Write a bad row to rejected_rows for later inspection."""
    conn.execute(
        "INSERT INTO rejected_rows (ingestion_run_id, raw_record, error_reason) VALUES (?, ?, ?)",
        (run_id, json.dumps(raw, default=str)[:50_000], reason[:1000]),
    )


def load_batch(conn: sqlite3.Connection, records: Iterable[PlayRecord],
               run_id: int, stats: LoadStats) -> None:
    """Load a batch of already-validated PlayRecord objects in one transaction.

    Stats are mutated in place. The caller is responsible for managing the
    overall stats lifecycle and for the transaction boundary (we BEGIN/COMMIT
    one batch at a time).
    """
    conn.execute("BEGIN")
    try:
        for rec in records:
            try:
                outcome = load_play(conn, rec, run_id)
                if outcome == "added":
                    stats.added += 1
                else:
                    stats.skipped += 1
            except sqlite3.IntegrityError as e:
                # Schema-level reject (e.g. CHECK constraint failure).
                stats.failed += 1
                # Don't quarantine here — we don't have the raw dict, and the
                # PlayRecord that produced this should be exceptional.
                # Just log via stats; ingest CLIs print summaries.
                _ = e
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
