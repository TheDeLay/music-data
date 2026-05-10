"""Enrich artists with Last.fm top tags.

Walks artists with sufficient lifetime plays and fetches `artist.getTopTags`
from Last.fm. Persists tags to `artist_labels` with `set_by='lastfm'` and
the LF tag count in the `note` column for later weight-based filtering.

A sentinel row `(artist_id, 'lastfm-fetched', 'true', set_by='lastfm')` is
written for every attempted artist — including those Last.fm doesn't know
about — so re-runs skip already-tried artists.

Prefers MBID lookup when an artist has a `spotify_artist_uri` linked AND
their Spotify enrichment included an MBID-equivalent identifier. Falls back
to artist-name lookup otherwise. Last.fm's MBID handling is more precise
for artists who share names.

Reuses the LastfmClient (which wraps ThrottledClient) so throttle/backoff/
LongPenaltyError/SustainedRateLimitError patterns are inherited from the
existing MB+AB+Spotify integrations.

Usage:
    # Smoke
    python -m scripts.enrich_lastfm_tags --max 10 --dry-run
    python -m scripts.enrich_lastfm_tags --max 10

    # Full backfill (~19 min for 2,310 artists at 0.5s/call)
    python -m scripts.enrich_lastfm_tags --min-plays 5

Exit codes:
    0  clean completion
    2  long-penalty / sustained-rate-limit / Last.fm-29 abort
    3  config error (missing API key)
    4  unexpected error
"""
from __future__ import annotations

import argparse
import logging
import os
import sqlite3
import sys
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv

from scripts.db import connect
from scripts.enrich_acousticbrainz import (
    DEFAULT_LONG_PENALTY_THRESHOLD,
    DEFAULT_MAX_NO_PROGRESS,
    DEFAULT_USER_AGENT,
    LongPenaltyError,
    RateLimitError,
    SustainedRateLimitError,
)
from scripts.lastfm_client import (
    DEFAULT_RATE_INTERVAL,
    LastfmAPIError,
    LastfmAuthError,
    LastfmClient,
)


load_dotenv()

log = logging.getLogger(__name__)

SENTINEL_KEY = "lastfm-fetched"
SENTINEL_VALUE = "true"
SET_BY = "lastfm"
TAG_KEY = "tag"


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------
@dataclass
class PhaseStats:
    candidates: int = 0
    attempted: int = 0
    hits: int = 0          # got at least one tag
    empty: int = 0         # 200 OK with empty tag list
    not_found: int = 0     # 404 / error=6
    tag_rows_written: int = 0
    aborted_at_index: Optional[int] = None
    error: Optional[str] = None


def _candidates(conn: sqlite3.Connection, min_plays: int) -> list[tuple[int, str]]:
    """Artists with >= min_plays lifetime plays AND no LF sentinel yet.

    Counts plays where the artist is the primary (position=0) — a feature
    artist on a track shouldn't drag a different primary's play count up.
    Returns [(artist_id, name), ...] ordered by play count DESC so smoke
    runs and partial cron runs prioritize the most-listened-to artists.
    """
    rows = conn.execute(
        """
        SELECT a.artist_id, a.name, COUNT(p.play_id) AS n_plays
        FROM artists a
        JOIN track_artists ta ON ta.artist_id = a.artist_id AND ta.position = 0
        JOIN plays p ON p.track_id = ta.track_id AND p.content_type = 'track'
        WHERE NOT EXISTS (
            SELECT 1 FROM artist_labels al
            WHERE al.artist_id = a.artist_id
              AND al.label_key = ?
              AND al.set_by = ?
        )
        GROUP BY a.artist_id, a.name
        HAVING n_plays >= ?
        ORDER BY n_plays DESC
        """,
        (SENTINEL_KEY, SET_BY, min_plays),
    ).fetchall()
    return [(r["artist_id"], r["name"]) for r in rows]


def _persist(conn: sqlite3.Connection, artist_id: int, tags: list[tuple[str, int]],
             not_found: bool) -> int:
    """Write tags + sentinel for one artist atomically.

    Returns count of tag rows inserted (excluding sentinel). INSERT OR IGNORE
    keeps re-runs safe — only the first call inserts; subsequent calls
    silently skip. The sentinel is written regardless of hit/miss so
    candidate selection can exclude this artist on subsequent runs.
    """
    rows_written = 0
    conn.execute("BEGIN")
    try:
        for name, count in tags:
            cur = conn.execute(
                "INSERT OR IGNORE INTO artist_labels "
                "(artist_id, label_key, label_value, set_by, note) "
                "VALUES (?, ?, ?, ?, ?)",
                (artist_id, TAG_KEY, name, SET_BY, f"count={count}"),
            )
            rows_written += cur.rowcount
        # Sentinel — distinguishes "no data" from "not yet fetched"
        sentinel_note = "404" if not_found else f"tags={len(tags)}"
        conn.execute(
            "INSERT OR IGNORE INTO artist_labels "
            "(artist_id, label_key, label_value, set_by, note) "
            "VALUES (?, ?, ?, ?, ?)",
            (artist_id, SENTINEL_KEY, SENTINEL_VALUE, SET_BY, sentinel_note),
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return rows_written


def run(
    conn: sqlite3.Connection,
    client: LastfmClient,
    min_plays: int = 5,
    max_n: Optional[int] = None,
    dry_run: bool = False,
) -> PhaseStats:
    """Walk candidate artists and persist Last.fm tags.

    Aborts cleanly on RateLimitError (LongPenaltyError, error=29 from LF,
    or watchdog) — last successful index is recorded in stats.aborted_at_index
    so a future run can pick up where this one left off.
    """
    candidates = _candidates(conn, min_plays)
    if max_n is not None:
        candidates = candidates[:max_n]
    stats = PhaseStats(candidates=len(candidates))

    for i, (artist_id, name) in enumerate(candidates):
        try:
            result = client.get_artist_top_tags(artist_name=name)
        except RateLimitError as e:
            stats.aborted_at_index = i
            stats.error = str(e)
            return stats

        stats.attempted += 1
        if result.not_found:
            stats.not_found += 1
        elif result.tags:
            stats.hits += 1
        else:
            stats.empty += 1

        if not dry_run:
            stats.tag_rows_written += _persist(
                conn, artist_id, result.tags, result.not_found,
            )

    return stats


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------
def print_summary(stats: PhaseStats, dry_run: bool) -> None:
    verb = "would persist" if dry_run else "persisted"
    print("\n=== Last.fm artist tags ===")
    print(f"  candidates: {stats.candidates}")
    print(f"  attempted:  {stats.attempted}")
    print(f"  hits:       {stats.hits}    (artist with at least 1 tag)")
    print(f"  empty:      {stats.empty}    (artist exists in LF, no tags)")
    print(f"  not_found:  {stats.not_found}    (artist not in LF)")
    print(f"  tag rows {verb}: {stats.tag_rows_written}")
    if stats.aborted_at_index is not None:
        print(f"  ABORTED at index {stats.aborted_at_index}: {stats.error}")


def print_coverage(conn: sqlite3.Connection) -> None:
    n_artists = conn.execute(
        "SELECT COUNT(*) FROM artists"
    ).fetchone()[0]
    n_fetched = conn.execute(
        "SELECT COUNT(DISTINCT artist_id) FROM artist_labels "
        "WHERE label_key = ? AND set_by = ?",
        (SENTINEL_KEY, SET_BY),
    ).fetchone()[0]
    n_with_tags = conn.execute(
        "SELECT COUNT(DISTINCT artist_id) FROM artist_labels "
        "WHERE label_key = ? AND set_by = ?",
        (TAG_KEY, SET_BY),
    ).fetchone()[0]

    def pct(n, d): return f"{100*n/d:.1f}%" if d else "n/a"
    print("\n=== Last.fm coverage ===")
    print(f"  artists total:         {n_artists:>5}")
    print(f"  artists fetched:       {n_fetched:>5}  ({pct(n_fetched, n_artists)})")
    print(f"  artists w/ tag data:   {n_with_tags:>5}  ({pct(n_with_tags, n_fetched)} of fetched)")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Enrich artists with Last.fm top tags.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--min-plays", type=int, default=5,
                        help="Minimum lifetime primary-artist plays to consider an artist.")
    parser.add_argument("--max", type=int, default=None,
                        help="Cap candidates (smoke testing).")
    parser.add_argument("--dry-run", action="store_true",
                        help="Hit Last.fm and parse responses but don't write to DB.")
    parser.add_argument("--rate-interval", type=float, default=DEFAULT_RATE_INTERVAL,
                        help=f"Seconds between requests. Default {DEFAULT_RATE_INTERVAL}s "
                             f"(2 req/sec). Hard cap: 5 req/sec (0.2s minimum).")
    parser.add_argument("--long-penalty-threshold", type=float,
                        default=DEFAULT_LONG_PENALTY_THRESHOLD)
    parser.add_argument("--max-no-progress", type=float, default=DEFAULT_MAX_NO_PROGRESS)
    parser.add_argument("--user-agent", type=str, default=DEFAULT_USER_AGENT)
    parser.add_argument("--db", type=str, default=None,
                        help="Path to music.db (default: auto-detect).")

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    api_key = os.environ.get("LASTFM_API_KEY", "").strip()
    if not api_key:
        print("ERROR: LASTFM_API_KEY not set. Sign up at "
              "https://www.last.fm/api/account/create and add to .env.",
              file=sys.stderr)
        return 3

    try:
        client = LastfmClient(
            api_key=api_key,
            user_agent=args.user_agent,
            min_request_interval=args.rate_interval,
            long_penalty_threshold_seconds=args.long_penalty_threshold,
            max_no_progress_seconds=args.max_no_progress,
        )
    except (ValueError, LastfmAuthError) as e:
        print(f"ERROR: client construction failed: {e}", file=sys.stderr)
        return 3

    conn = connect(args.db)
    try:
        try:
            stats = run(conn, client, args.min_plays, args.max, args.dry_run)
        except LastfmAuthError as e:
            print(f"ERROR: Last.fm rejected API key: {e}", file=sys.stderr)
            return 3
        except LastfmAPIError as e:
            print(f"ERROR: Last.fm returned unexpected error: {e}", file=sys.stderr)
            return 4
        print_summary(stats, args.dry_run)
        print_coverage(conn)
        if stats.aborted_at_index is not None:
            return 2
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
