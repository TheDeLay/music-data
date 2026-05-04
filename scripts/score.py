"""Love-score engine for music-data.

Computes a per-track "love score" that captures genuine engagement rather
than raw play count.  The formula rewards completion behavior, forgives
mood-driven skips, treats the back-button as the strongest love signal,
and weights recent listening over ancient history.

The SQL view `v_track_engagement` handles the base aggregates; this script
layers on the parts that need Python: skip-streak detection and the final
weighted score.

Usage:
    python -m scripts.score                          # top 50, defaults
    python -m scripts.score --top 20                 # top 20
    python -m scripts.score --recency-days 30        # tighter recency window
    python -m scripts.score --quality-threshold 0.90 # stricter quality bar
    python -m scripts.score --format json            # machine-readable output
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from scripts.db import connect


# ---------------------------------------------------------------------------
# Configuration (all defaults from the engagement-model spec)
# ---------------------------------------------------------------------------
@dataclass
class ScoreConfig:
    quality_threshold: float = 0.80       # pct_played >= this = "quality play"
    recency_days: int = 90                # window for "recent" plays
    recent_weight: float = 3.0            # multiplier for recent quality plays
    backbutton_weight: float = 5.0        # multiplier for back-button replays
    lifetime_weight: float = 1.0          # multiplier for all-time quality plays
    skip_forgiveness: int = 2             # free skips before penalty kicks in
    skip_penalty: float = 2.0             # per-skip penalty beyond forgiveness
    streak_window: int = 10               # last N plays to check for skip streaks
    deliberate_weight: float = 2.0        # multiplier for deliberate quality plays
                                          # (clickrow + finished — "I know when I want this")


@dataclass
class TrackScore:
    track_id: int
    spotify_track_uri: str
    track_name: str
    primary_artist_name: Optional[str]
    album_name: Optional[str]
    release_year: Optional[int]
    duration_ms: int
    total_plays: int
    quality_plays: int
    recent_quality: int
    backbutton_count: int
    recent_plays: int
    skip_count: int
    avg_pct_played: float
    deliberate_quality: int = 0
    # Computed by this script:
    skip_streak: int = 0
    love_score: float = 0.0


# ---------------------------------------------------------------------------
# Skip-streak detection
# ---------------------------------------------------------------------------
def compute_skip_streak(plays: list[dict], config: ScoreConfig) -> int:
    """Find the longest recent consecutive skip run for a track.

    Args:
        plays: list of play dicts with 'ms_played', 'duration_ms', 'ts',
               ordered by ts DESC (most recent first).
        config: scoring configuration.

    Returns:
        Length of the longest consecutive skip streak in the last
        `config.streak_window` plays.
    """
    if not plays:
        return 0

    window = plays[:config.streak_window]
    max_streak = 0
    current_streak = 0

    for p in window:
        pct = p["ms_played"] / p["duration_ms"] if p["duration_ms"] else 0
        if pct < config.quality_threshold:
            current_streak += 1
            max_streak = max(max_streak, current_streak)
        else:
            current_streak = 0

    return max_streak


# ---------------------------------------------------------------------------
# Love-score computation
# ---------------------------------------------------------------------------
def compute_love_score(track: TrackScore, config: ScoreConfig) -> float:
    """Apply the love-score formula.

    Signals and their weights (all configurable):
      - recent_quality  × 3.0  — what you love NOW
      - backbutton      × 5.0  — strongest love signal (mid-play replay)
      - deliberate_qual × 2.0  — "I know when I want this" (chose it AND finished)
      - lifetime_quality× 1.0  — historical engagement
      - skip_streak penalty     — consistent recent skipping = falling out of love
    """
    score = (
        track.recent_quality * config.recent_weight
        + track.backbutton_count * config.backbutton_weight
        + track.deliberate_quality * config.deliberate_weight
        + track.quality_plays * config.lifetime_weight
        - max(0, track.skip_streak - config.skip_forgiveness) * config.skip_penalty
    )

    return score


# ---------------------------------------------------------------------------
# Main scoring pipeline
# ---------------------------------------------------------------------------
def _compute_threshold_columns(
    conn: sqlite3.Connection, config: ScoreConfig
) -> dict[int, sqlite3.Row]:
    """Recompute the threshold/recency-dependent counts using runtime config.

    The SQL view `v_track_engagement` bakes in 0.80 and 90 days. When the
    user overrides `--quality-threshold` or `--recency-days`, those columns
    are stale. This bulk query produces fresh counts so the love score is
    internally consistent regardless of which knobs were turned.
    """
    rows = conn.execute("""
        SELECT
            p.track_id,
            SUM(CASE WHEN p.ms_played * 1.0 / t.duration_ms >= ?
                     THEN 1 ELSE 0 END) AS quality_plays,
            SUM(CASE WHEN p.ts >= datetime('now', ?)
                     AND p.ms_played * 1.0 / t.duration_ms >= ?
                     THEN 1 ELSE 0 END) AS recent_quality,
            SUM(CASE WHEN p.reason_start = 'clickrow'
                     AND p.ms_played * 1.0 / t.duration_ms >= ?
                     THEN 1 ELSE 0 END) AS deliberate_quality
        FROM plays p
        JOIN tracks t ON p.track_id = t.track_id
        WHERE t.duration_ms IS NOT NULL AND t.duration_ms > 0
          AND p.content_type = 'track'
        GROUP BY p.track_id
    """, (
        config.quality_threshold,
        f"-{config.recency_days} days",
        config.quality_threshold,
        config.quality_threshold,
    )).fetchall()
    return {r["track_id"]: r for r in rows}


def score_tracks(conn: sqlite3.Connection, config: ScoreConfig) -> list[TrackScore]:
    """Score all tracks that have engagement data.

    1. Read base aggregates from v_track_engagement (metadata + threshold-
       independent counts: total_plays, backbutton_count, skip_count, etc.).
    2. Recompute threshold-dependent counts (quality_plays, recent_quality,
       deliberate_quality) using runtime config — single source of truth.
    3. For each track, fetch individual plays for skip-streak detection.
    4. Compute final love_score.
    """
    # Step 1: base aggregates from the view
    rows = conn.execute("""
        SELECT track_id, spotify_track_uri, track_name, duration_ms,
               album_name, release_year, primary_artist_name,
               total_plays, quality_plays, recent_quality,
               backbutton_count, deliberate_quality,
               recent_plays, skip_count, avg_pct_played
        FROM v_track_engagement
    """).fetchall()

    if not rows:
        return []

    tracks: list[TrackScore] = []
    for r in rows:
        ts = TrackScore(
            track_id=r["track_id"],
            spotify_track_uri=r["spotify_track_uri"],
            track_name=r["track_name"],
            primary_artist_name=r["primary_artist_name"],
            album_name=r["album_name"],
            release_year=r["release_year"],
            duration_ms=r["duration_ms"],
            total_plays=r["total_plays"],
            quality_plays=r["quality_plays"],
            recent_quality=r["recent_quality"],
            backbutton_count=r["backbutton_count"],
            deliberate_quality=r["deliberate_quality"],
            recent_plays=r["recent_plays"],
            skip_count=r["skip_count"],
            avg_pct_played=r["avg_pct_played"] or 0.0,
        )
        tracks.append(ts)

    # Step 2: recompute threshold/recency-dependent counts with runtime config
    threshold_data = _compute_threshold_columns(conn, config)
    for ts in tracks:
        td = threshold_data.get(ts.track_id)
        if td is not None:
            ts.quality_plays = td["quality_plays"]
            ts.recent_quality = td["recent_quality"]
            ts.deliberate_quality = td["deliberate_quality"]

    # Step 3: per-play data for skip-streak detection
    # One query per track is fine for ~907 tracks
    for ts in tracks:
        plays = conn.execute("""
            SELECT p.ms_played, t.duration_ms, p.ts
            FROM plays p
            JOIN tracks t ON p.track_id = t.track_id
            WHERE p.track_id = ? AND p.content_type = 'track'
            ORDER BY p.ts DESC
        """, (ts.track_id,)).fetchall()

        play_dicts = [{"ms_played": p["ms_played"],
                        "duration_ms": p["duration_ms"],
                        "ts": p["ts"]} for p in plays]

        ts.skip_streak = compute_skip_streak(play_dicts, config)

    # Step 4: compute love_score
    for ts in tracks:
        ts.love_score = compute_love_score(ts, config)

    # Sort descending by love_score, then by recent_quality as tiebreaker
    tracks.sort(key=lambda t: (t.love_score, t.recent_quality), reverse=True)
    return tracks


# ---------------------------------------------------------------------------
# Output formatters
# ---------------------------------------------------------------------------
def print_table(tracks: list[TrackScore], top: int) -> None:
    """Pretty-print the top-N scored tracks."""
    display = tracks[:top]
    if not display:
        print("No tracks with engagement data found.")
        return

    print(f"\n{'#':>3}  {'Love':>5}  {'Track':<40} {'Artist':<25} "
          f"{'Plays':>5} {'Qual':>4} {'Rcnt':>4} {'Back':>4} {'Dlib':>4} {'Skip':>4} {'Avg%':>5}")
    print("-" * 145)
    for i, t in enumerate(display, 1):
        print(f"{i:>3}  {t.love_score:>5.0f}  {t.track_name[:39]:<40} "
              f"{(t.primary_artist_name or '?')[:24]:<25} "
              f"{t.total_plays:>5} {t.quality_plays:>4} {t.recent_quality:>4} "
              f"{t.backbutton_count:>4} {t.deliberate_quality:>4} {t.skip_streak:>4} {t.avg_pct_played:>5.1f}")

    print(f"\n  Dlib = deliberate quality (chose it + finished it)  |  {len(tracks)} tracks scored  |  showing top {len(display)}")


def print_json(tracks: list[TrackScore], top: int) -> None:
    """Output top-N as JSON for piping to other tools."""
    display = tracks[:top]
    output = []
    for t in display:
        output.append({
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
            "deliberate_quality": t.deliberate_quality,
            "skip_streak": t.skip_streak,
            "avg_pct_played": round(t.avg_pct_played, 1),
        })
    json.dump(output, sys.stdout, indent=2)
    print()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Compute love scores for tracks in music-data.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--top", type=int, default=50,
                        help="Number of top tracks to display")
    parser.add_argument("--quality-threshold", type=float, default=0.80,
                        help="Min pct_played to count as a quality play (0.0-1.0)")
    parser.add_argument("--recency-days", type=int, default=90,
                        help="Days for the recency window")
    parser.add_argument("--recent-weight", type=float, default=3.0,
                        help="Weight for recent quality plays")
    parser.add_argument("--backbutton-weight", type=float, default=5.0,
                        help="Weight for back-button replays")
    parser.add_argument("--lifetime-weight", type=float, default=1.0,
                        help="Weight for all-time quality plays")
    parser.add_argument("--skip-forgiveness", type=int, default=2,
                        help="Free skips before penalty")
    parser.add_argument("--skip-penalty", type=float, default=2.0,
                        help="Per-skip penalty beyond forgiveness")
    parser.add_argument("--streak-window", type=int, default=10,
                        help="Last N plays to check for skip streaks")
    parser.add_argument("--deliberate-weight", type=float, default=2.0,
                        help="Weight for deliberate quality plays (chose it + finished)")
    parser.add_argument("--format", choices=["table", "json"], default="table",
                        help="Output format")
    parser.add_argument("--db", type=str, default=None,
                        help="Path to music.db (default: auto-detect)")

    args = parser.parse_args(argv)

    config = ScoreConfig(
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

    conn = connect(args.db)
    try:
        tracks = score_tracks(conn, config)
        if args.format == "json":
            print_json(tracks, args.top)
        else:
            print_table(tracks, args.top)
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
