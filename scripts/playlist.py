"""Playlist generator for music-data — engagement-model layer 3.

Combines the love score (scripts/score.py) with listening-context affinity
(scripts/cluster_modes.py + scripts/label_modes.py) to produce playlists
filtered by both *quality* (how much you love the track) and *fit* (does
it belong to a particular time-of-day / day-of-week mode).

Usage:
    # Top 30 by love score, no mode filter (close to score.py behavior)
    python -m scripts.playlist --top 30

    # Top 30 with love >= 5, only "weekday morning" primary tracks
    python -m scripts.playlist --love-min 5 --mode "weekday morning" --top 30

    # Loose match: any track with affinity >= 0.4 in the mode
    python -m scripts.playlist --mode "late night" --min-affinity 0.4

    # Output formats for actually using the playlist
    python -m scripts.playlist --mode "weekend daytime" --format uris > playlist.txt
    python -m scripts.playlist --mode "weekend daytime" --format json
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from dataclasses import dataclass
from typing import Optional

from scripts.db import connect
from scripts.score import ScoreConfig, TrackScore, score_tracks


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
@dataclass
class PlaylistConfig:
    score_config: ScoreConfig
    mode: Optional[str] = None             # raw user input — label or cluster_id
    love_min: float = 0.0
    top: int = 30
    min_affinity: Optional[float] = None   # None = strict (is_primary=1 only)


class ModeNotFoundError(Exception):
    """--mode value didn't match any cluster_id or labeled context."""


# ---------------------------------------------------------------------------
# Mode resolution
# ---------------------------------------------------------------------------
def resolve_context(conn: sqlite3.Connection, mode: Optional[str]) -> Optional[int]:
    """Look up context_id for a --mode argument.

    Resolution order:
      1. mode is None or empty string → no mode filter (returns None)
      2. mode parses as integer → match cluster_id
      3. mode is text → case-insensitive exact match against user_label
                        (skips empty/unlabeled contexts)

    Raises ModeNotFoundError with available options if no match found.
    """
    if mode is None or mode == "":
        return None

    try:
        cid = int(mode)
        row = conn.execute(
            "SELECT context_id FROM listening_contexts WHERE cluster_id = ?",
            (cid,),
        ).fetchone()
        if row:
            return row["context_id"]
    except ValueError:
        pass

    row = conn.execute(
        "SELECT context_id FROM listening_contexts "
        "WHERE LOWER(user_label) = LOWER(?) AND user_label != ''",
        (mode,),
    ).fetchone()
    if row:
        return row["context_id"]

    available = conn.execute(
        "SELECT cluster_id, user_label FROM listening_contexts ORDER BY cluster_id"
    ).fetchall()
    msg_parts = [f"Mode {mode!r} not found."]
    if available:
        msg_parts.append("Available modes:")
        for r in available:
            label = r["user_label"] or "<unlabeled>"
            msg_parts.append(f"  cluster_id={r['cluster_id']}  label={label!r}")
    else:
        msg_parts.append("No clusters found — run scripts.cluster_modes first.")
    raise ModeNotFoundError("\n".join(msg_parts))


# ---------------------------------------------------------------------------
# Filtering
# ---------------------------------------------------------------------------
def filter_by_context(
    tracks: list[TrackScore],
    conn: sqlite3.Connection,
    context_id: int,
    min_affinity: Optional[float],
) -> list[TrackScore]:
    """Return only tracks belonging to the given context.

    Strict mode (min_affinity is None): only tracks with is_primary=1 in
    this context. Cleanest playlists, no overlap between modes.

    Loose mode (min_affinity is float): any track with affinity ≥ threshold,
    regardless of primary status. Tracks can appear in multiple modes.
    """
    if min_affinity is None:
        rows = conn.execute(
            "SELECT track_id FROM track_context_affinity "
            "WHERE context_id = ? AND is_primary = 1",
            (context_id,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT track_id FROM track_context_affinity "
            "WHERE context_id = ? AND affinity >= ?",
            (context_id, min_affinity),
        ).fetchall()
    eligible = {r["track_id"] for r in rows}
    return [t for t in tracks if t.track_id in eligible]


def build_playlist(
    conn: sqlite3.Connection,
    config: PlaylistConfig,
) -> list[TrackScore]:
    """Run the full pipeline: resolve mode → score → filter → top-N."""
    context_id = resolve_context(conn, config.mode)

    tracks = score_tracks(conn, config.score_config)

    if context_id is not None:
        tracks = filter_by_context(tracks, conn, context_id, config.min_affinity)

    if config.love_min > 0:
        tracks = [t for t in tracks if t.love_score >= config.love_min]

    return tracks[:config.top]


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------
EMPTY_MSG = "No tracks match — try lowering --love-min or removing --mode."


def print_table(tracks: list[TrackScore], mode: Optional[str]) -> None:
    if not tracks:
        print(EMPTY_MSG)
        return
    header = "\nPlaylist"
    if mode:
        header += f" — mode: {mode}"
    header += f"  ({len(tracks)} tracks)"
    print(header)
    print(f"{'#':>3}  {'Love':>5}  {'Track':<40} {'Artist':<25} "
          f"{'Plays':>5} {'Rcnt':>4} {'Back':>4} {'Avg%':>5}")
    print("-" * 100)
    for i, t in enumerate(tracks, 1):
        print(f"{i:>3}  {t.love_score:>5.0f}  {t.track_name[:39]:<40} "
              f"{(t.primary_artist_name or '?')[:24]:<25} "
              f"{t.total_plays:>5} {t.recent_quality:>4} "
              f"{t.backbutton_count:>4} {t.avg_pct_played:>5.1f}")
    print()


def print_json(tracks: list[TrackScore], mode: Optional[str]) -> None:
    output = {
        "mode": mode,
        "track_count": len(tracks),
        "tracks": [
            {
                "track_id": t.track_id,
                "spotify_track_uri": t.spotify_track_uri,
                "track_name": t.track_name,
                "primary_artist_name": t.primary_artist_name,
                "album_name": t.album_name,
                "release_year": t.release_year,
                "love_score": round(t.love_score, 2),
                "total_plays": t.total_plays,
                "quality_plays": t.quality_plays,
                "recent_quality": t.recent_quality,
                "backbutton_count": t.backbutton_count,
                "avg_pct_played": round(t.avg_pct_played, 1),
            }
            for t in tracks
        ],
    }
    json.dump(output, sys.stdout, indent=2)
    print()


def print_uris(tracks: list[TrackScore], mode: Optional[str]) -> None:
    """One spotify:track:... URI per line. Empty result → message to stderr only.

    The empty-case message is on stderr so `--format uris > playlist.txt`
    produces a truly empty file rather than one with a complaint in it.
    """
    if not tracks:
        print(EMPTY_MSG, file=sys.stderr)
        return
    for t in tracks:
        print(t.spotify_track_uri)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate a playlist from love score + listening context.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument("--mode", type=str, default=None,
                        help="Listening context: user_label (e.g. 'weekday morning') "
                             "or cluster_id integer. Omit for no mode filter.")
    parser.add_argument("--love-min", type=float, default=0.0,
                        help="Minimum love score to include (0 = no filter)")
    parser.add_argument("--top", type=int, default=30,
                        help="Number of tracks in the playlist")
    parser.add_argument("--min-affinity", type=float, default=None,
                        help="Loose mode: include tracks with affinity >= this "
                             "(0.0-1.0). Without this flag, strict mode "
                             "(is_primary=1 only) is used.")
    parser.add_argument("--format", choices=["table", "json", "uris"], default="table",
                        help="Output format. uris = paste-into-Spotify lines.")
    parser.add_argument("--db", type=str, default=None,
                        help="Path to music.db (default: auto-detect)")

    parser.add_argument("--quality-threshold", type=float, default=0.80,
                        help="Min pct_played to count as a quality play (0.0-1.0)")
    parser.add_argument("--recency-days", type=int, default=90,
                        help="Days for the recency window")
    parser.add_argument("--recent-weight", type=float, default=3.0)
    parser.add_argument("--backbutton-weight", type=float, default=5.0)
    parser.add_argument("--lifetime-weight", type=float, default=1.0)
    parser.add_argument("--skip-forgiveness", type=int, default=2)
    parser.add_argument("--skip-penalty", type=float, default=2.0)
    parser.add_argument("--streak-window", type=int, default=10)
    parser.add_argument("--deliberate-weight", type=float, default=2.0)

    args = parser.parse_args(argv)

    score_config = ScoreConfig(
        quality_threshold=args.quality_threshold,
        recency_days=args.recency_days,
        recent_weight=args.recent_weight,
        backbutton_weight=args.backbutton_weight,
        lifetime_weight=args.lifetime_weight,
        skip_forgiveness=args.skip_forgiveness,
        skip_penalty=args.skip_penalty,
        streak_window=args.streak_window,
        deliberate_weight=args.deliberate_weight,
    )
    config = PlaylistConfig(
        score_config=score_config,
        mode=args.mode,
        love_min=args.love_min,
        top=args.top,
        min_affinity=args.min_affinity,
    )

    conn = connect(args.db)
    try:
        try:
            tracks = build_playlist(conn, config)
        except ModeNotFoundError as e:
            print(str(e), file=sys.stderr)
            return 2

        if args.format == "json":
            print_json(tracks, args.mode)
        elif args.format == "uris":
            print_uris(tracks, args.mode)
        else:
            print_table(tracks, args.mode)
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
