"""Phase 1B: enrich tracks with MusicBrainz tags + genres.

Walks tracks that already have an MBID (from `enrich_acousticbrainz` Phase 1)
and have no tag-fetch record yet. For each, fetches the MB recording endpoint
with tags + genres included, then persists every tag and genre into
`track_labels` with `set_by='mb'`.

A sentinel row `(track_id, 'mb-tags-fetched', 'true', set_by='mb')` is written
for every attempted MBID — including misses (200 with empty arrays, or 404).
This is what makes re-runs idempotent: the candidate query joins on the
sentinel's absence, not on whether the tag itself exists.

Reuses ThrottledClient + LongPenaltyError + watchdog from
`enrich_acousticbrainz` so a single client polite to MB is shared.

Usage:
    # Smoke
    python -m scripts.enrich_mb_tags --max 10 --dry-run
    python -m scripts.enrich_mb_tags --max 10

    # Full run (~10 min for 585 cached MBIDs)
    python -m scripts.enrich_mb_tags

Exit codes:
    0  clean completion
    2  long-penalty / sustained-rate-limit abort
"""
from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from dataclasses import dataclass
from typing import Optional

from scripts.db import connect
from scripts.enrich_acousticbrainz import (
    DEFAULT_LONG_PENALTY_THRESHOLD,
    DEFAULT_MAX_NO_PROGRESS,
    DEFAULT_RATE_INTERVAL,
    DEFAULT_USER_AGENT,
    LongPenaltyError,
    MB_API,
    RateLimitError,
    SustainedRateLimitError,
    ThrottledClient,
)


log = logging.getLogger(__name__)

# Marker row that says "we've already fetched MB tags for this track —
# don't refetch even if zero tags came back." Lives in track_labels alongside
# the actual tags but with a distinct label_key so it's cheap to filter on.
SENTINEL_KEY = "mb-tags-fetched"
SENTINEL_VALUE = "true"
SET_BY = "mb"

# label_key used for actual data
TAG_KEY = "tag"      # folksonomy tags (user-submitted)
GENRE_KEY = "genre"  # curated genre vocabulary


@dataclass
class TagsResult:
    tags: list[tuple[str, int]]   # [(name, count), ...]
    genres: list[tuple[str, int]]
    not_found: bool = False       # 404 from MB — record exists no more


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------
def fetch_tags(client: ThrottledClient, mbid: str) -> TagsResult:
    """Fetch tags + genres for a single MBID from MusicBrainz.

    Returns TagsResult with either populated tags/genres (possibly empty)
    or not_found=True for a 404. Empty arrays are valid (the recording
    exists in MB, nobody has tagged it).

    Raises RateLimitError on long penalties / watchdog — caller aborts.
    """
    data = client.get(f"/recording/{mbid}", params={"inc": "tags+genres", "fmt": "json"})
    if data is None:
        return TagsResult(tags=[], genres=[], not_found=True)

    raw_tags = data.get("tags") or []
    raw_genres = data.get("genres") or []
    return TagsResult(
        tags=[(t.get("name", ""), int(t.get("count") or 0)) for t in raw_tags if t.get("name")],
        genres=[(g.get("name", ""), int(g.get("count") or 0)) for g in raw_genres if g.get("name")],
        not_found=False,
    )


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------
@dataclass
class PhaseStats:
    candidates: int = 0
    attempted: int = 0
    hits: int = 0          # got at least one tag or genre
    empty: int = 0         # 200 OK but empty arrays (recording exists, untagged)
    not_found: int = 0     # 404 from MB
    tag_rows_written: int = 0
    aborted_at_index: Optional[int] = None
    error: Optional[str] = None


def _candidates(conn: sqlite3.Connection) -> list[tuple[int, str]]:
    """Tracks with a non-null MBID and no MB-tags sentinel row yet."""
    rows = conn.execute(
        """
        SELECT m.track_id, m.mb_recording_id
        FROM mb_recordings m
        WHERE m.mb_recording_id IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM track_labels tl
              WHERE tl.track_id = m.track_id
                AND tl.label_key = ?
                AND tl.set_by = ?
          )
        ORDER BY m.track_id
        """,
        (SENTINEL_KEY, SET_BY),
    ).fetchall()
    return [(r["track_id"], r["mb_recording_id"]) for r in rows]


def _persist(conn: sqlite3.Connection, track_id: int, result: TagsResult) -> int:
    """Write tags + genres + sentinel for one track in a single transaction.

    Returns number of TAG/GENRE rows inserted (excluding sentinel).
    INSERT OR IGNORE makes this safe to call twice on the same data.
    """
    rows_written = 0
    conn.execute("BEGIN")
    try:
        for name, count in result.tags:
            cur = conn.execute(
                "INSERT OR IGNORE INTO track_labels "
                "(track_id, label_key, label_value, set_by, note) "
                "VALUES (?, ?, ?, ?, ?)",
                (track_id, TAG_KEY, name, SET_BY, f"count={count}"),
            )
            rows_written += cur.rowcount
        for name, count in result.genres:
            cur = conn.execute(
                "INSERT OR IGNORE INTO track_labels "
                "(track_id, label_key, label_value, set_by, note) "
                "VALUES (?, ?, ?, ?, ?)",
                (track_id, GENRE_KEY, name, SET_BY, f"count={count}"),
            )
            rows_written += cur.rowcount
        # Sentinel — distinguishes "no data" from "not yet fetched"
        conn.execute(
            "INSERT OR IGNORE INTO track_labels "
            "(track_id, label_key, label_value, set_by, note) "
            "VALUES (?, ?, ?, ?, ?)",
            (track_id, SENTINEL_KEY, SENTINEL_VALUE, SET_BY,
             "404" if result.not_found else f"tags={len(result.tags)} genres={len(result.genres)}"),
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return rows_written


def run(
    conn: sqlite3.Connection,
    client: ThrottledClient,
    max_n: Optional[int] = None,
    dry_run: bool = False,
) -> PhaseStats:
    """Walk candidate MBIDs and persist tags. RateLimitError aborts cleanly."""
    candidates = _candidates(conn)
    if max_n is not None:
        candidates = candidates[:max_n]
    stats = PhaseStats(candidates=len(candidates))

    for i, (track_id, mbid) in enumerate(candidates):
        try:
            result = fetch_tags(client, mbid)
        except RateLimitError as e:
            stats.aborted_at_index = i
            stats.error = str(e)
            return stats

        stats.attempted += 1
        if result.not_found:
            stats.not_found += 1
        elif result.tags or result.genres:
            stats.hits += 1
        else:
            stats.empty += 1

        if not dry_run:
            stats.tag_rows_written += _persist(conn, track_id, result)

    return stats


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------
def print_summary(stats: PhaseStats, dry_run: bool) -> None:
    verb = "would persist" if dry_run else "persisted"
    print("\n=== MB tags + genres ===")
    print(f"  candidates: {stats.candidates}")
    print(f"  attempted:  {stats.attempted}")
    print(f"  hits:       {stats.hits}    (recording with at least 1 tag/genre)")
    print(f"  empty:      {stats.empty}    (recording exists, nothing tagged)")
    print(f"  not_found:  {stats.not_found}    (404 from MB)")
    print(f"  tag rows {verb}: {stats.tag_rows_written}")
    if stats.aborted_at_index is not None:
        print(f"  ABORTED at index {stats.aborted_at_index}: {stats.error}")


def print_coverage(conn: sqlite3.Connection) -> None:
    n_with_mbid = conn.execute(
        "SELECT COUNT(*) FROM mb_recordings WHERE mb_recording_id IS NOT NULL"
    ).fetchone()[0]
    n_fetched = conn.execute(
        "SELECT COUNT(DISTINCT track_id) FROM track_labels "
        "WHERE label_key = ? AND set_by = ?",
        (SENTINEL_KEY, SET_BY),
    ).fetchone()[0]
    n_with_data = conn.execute(
        "SELECT COUNT(DISTINCT track_id) FROM track_labels "
        "WHERE label_key IN (?, ?) AND set_by = ?",
        (TAG_KEY, GENRE_KEY, SET_BY),
    ).fetchone()[0]

    def pct(n, d): return f"{100*n/d:.1f}%" if d else "n/a"

    print("\n=== MB tags coverage ===")
    print(f"  tracks w/ MBID:        {n_with_mbid:>5}")
    print(f"  tracks fetched:        {n_fetched:>5}  ({pct(n_fetched, n_with_mbid)})")
    print(f"  tracks w/ tag data:    {n_with_data:>5}  ({pct(n_with_data, n_fetched)} of fetched)")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Enrich tracks with MusicBrainz tags + genres (Phase 1B).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--max", type=int, default=None,
                        help="Cap candidates this run (smoke testing).")
    parser.add_argument("--dry-run", action="store_true",
                        help="Hit MB and parse responses but don't write to DB.")
    parser.add_argument("--rate-interval", type=float, default=DEFAULT_RATE_INTERVAL,
                        help="Seconds between requests. MB asks for ~1 req/sec; "
                             "default 1.1s is friendly.")
    parser.add_argument("--long-penalty-threshold", type=float,
                        default=DEFAULT_LONG_PENALTY_THRESHOLD,
                        help="429 Retry-After above this triggers immediate abort.")
    parser.add_argument("--max-no-progress", type=float, default=DEFAULT_MAX_NO_PROGRESS,
                        help="Abort if no successful response in this many seconds.")
    parser.add_argument("--user-agent", type=str, default=DEFAULT_USER_AGENT,
                        help="HTTP User-Agent (MB requires identifiable UA).")
    parser.add_argument("--db", type=str, default=None,
                        help="Path to music.db (default: auto-detect).")

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    conn = connect(args.db)
    try:
        client = ThrottledClient(
            MB_API,
            user_agent=args.user_agent,
            min_request_interval=args.rate_interval,
            long_penalty_threshold_seconds=args.long_penalty_threshold,
            max_no_progress_seconds=args.max_no_progress,
        )
        stats = run(conn, client, max_n=args.max, dry_run=args.dry_run)
        print_summary(stats, args.dry_run)
        print_coverage(conn)
        if stats.aborted_at_index is not None:
            return 2
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
