"""One-shot reports surfacing non-obvious patterns in your listening data.

All queries are pure SQL against existing tables/views. Zero API, zero
state mutation, safe to run anytime.

Reports:
  forgotten       — "What did I used to love and stopped playing?"
  backbutton      — "What did I rewind to hear again?" (pure love signal)
  skipped         — "What did I love and now skip?" (taste shift detector)
  deliberate      — "When I sought this out, I finished it" (high-conviction tracks)
  obsessions      — "What am I currently overplaying?" (recent-spike detector)
  spam            — "Spotify keeps queueing these and I keep skipping" (dislike worklist)
  hidden_loves    — "Low play count but I always finish it" (didn't-know surface)
  tags            — "What genres dominate my library?" (LF + MB unified)

Usage:
    python -m scripts.reports --list
    python -m scripts.reports --report forgotten
    python -m scripts.reports --all
    python -m scripts.reports --report tags --limit 50

Reports work on whatever data you have today. As the cron fills in more
audio features and Last.fm tags over time, the same reports get richer
without any code changes.
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from dataclasses import dataclass
from typing import Callable, Optional

from scripts.db import connect


# ---------------------------------------------------------------------------
# Report registry
# ---------------------------------------------------------------------------
@dataclass
class Report:
    name: str
    description: str
    fn: Callable[..., list[sqlite3.Row]]  # (conn, limit, **kwargs) → rows
    columns: list[str]                    # column labels for table output
    column_widths: list[int]              # per-column display width
    column_aligns: list[str]              # 'l' (left) or 'r' (right)


def _report_forgotten(conn: sqlite3.Connection, limit: int, **_kw) -> list[sqlite3.Row]:
    """High lifetime plays + zero recent plays. 'You used to love these.'

    Sorted by total_plays so the most-loved-but-forgotten surfaces first.
    """
    return conn.execute(
        """
        SELECT track_name, primary_artist_name, total_plays,
               recent_plays, ROUND(avg_pct_played) AS avg_pct,
               substr(last_played, 1, 10) AS last_played
        FROM v_track_engagement
        WHERE total_plays >= 20
          AND recent_plays = 0
        ORDER BY total_plays DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()


def _report_backbutton(conn: sqlite3.Connection, limit: int, **_kw) -> list[sqlite3.Row]:
    """High backbutton_count = 'I rewound to hear this again.' Purest love signal."""
    return conn.execute(
        """
        SELECT track_name, primary_artist_name, total_plays,
               backbutton_count, ROUND(avg_pct_played) AS avg_pct
        FROM v_track_engagement
        WHERE backbutton_count >= 1
        ORDER BY backbutton_count DESC, total_plays DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()


def _report_skipped(conn: sqlite3.Connection, limit: int, **_kw) -> list[sqlite3.Row]:
    """Tracks with strong lifetime engagement but high RECENT skip rate.

    Joins plays directly to compute recent_skip_count (last 90 days). The
    view's skip_count is lifetime and can't be safely divided by
    recent_plays — a track skipped 60 times across 12 years and played
    5 times recently would otherwise show as 1200% skip rate.

    The life_avg_pct >= 50 gate enforces "actually loved historically" —
    without it, tracks that Spotify recently autoplayed (and you immediately
    skipped) leak in even though they were never loved in the first place.
    """
    return conn.execute(
        """
        SELECT v.track_name, v.primary_artist_name, v.total_plays,
               v.recent_plays,
               COALESCE(rs.recent_skips, 0) AS recent_skips,
               ROUND(100.0 * COALESCE(rs.recent_skips, 0) / v.recent_plays) AS skip_pct,
               ROUND(v.avg_pct_played) AS life_avg_pct
        FROM v_track_engagement v
        LEFT JOIN (
            SELECT track_id,
                   SUM(CASE WHEN skipped = 1 THEN 1 ELSE 0 END) AS recent_skips
            FROM plays
            WHERE ts >= datetime('now', '-90 days')
            GROUP BY track_id
        ) rs ON rs.track_id = v.track_id
        WHERE v.total_plays >= 10
          AND v.recent_plays >= 5
          AND v.avg_pct_played >= 50
          AND COALESCE(rs.recent_skips, 0) >= 1
        ORDER BY (CAST(COALESCE(rs.recent_skips, 0) AS REAL) / v.recent_plays) DESC,
                 v.total_plays DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()


def _report_deliberate(conn: sqlite3.Connection, limit: int, **_kw) -> list[sqlite3.Row]:
    """High deliberate_quality:total_plays ratio = 'I sought this out and finished it.'

    deliberate_quality counts plays where reason_start='clickrow' (intentional
    pick) AND the track played to completion. Indicates conviction beyond
    autoplay or shuffle inertia.
    """
    return conn.execute(
        """
        SELECT track_name, primary_artist_name, total_plays,
               deliberate_quality,
               ROUND(100.0 * deliberate_quality / total_plays) AS deliberate_pct,
               ROUND(avg_pct_played) AS avg_pct
        FROM v_track_engagement
        WHERE total_plays >= 5
          AND deliberate_quality >= 1
        ORDER BY (CAST(deliberate_quality AS REAL) / total_plays) DESC,
                 total_plays DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()


def _report_obsessions(conn: sqlite3.Connection, limit: int,
                       min_avg_pct: int = 20, **_kw) -> list[sqlite3.Row]:
    """Tracks where recent plays dominate lifetime plays — current obsessions.

    Filters: at least 5 recent plays (to exclude one-off discoveries),
    recent makes up >=30% of lifetime, AND avg_pct_played >= min_avg_pct
    (default 20). The avg_pct gate filters out tracks Spotify autoplay
    keeps queueing that you immediately skip — those are "served, not
    chosen" and don't belong in an obsessions list.
    """
    return conn.execute(
        """
        SELECT track_name, primary_artist_name, total_plays, recent_plays,
               ROUND(100.0 * recent_plays / total_plays) AS recency_pct,
               ROUND(avg_pct_played) AS avg_pct
        FROM v_track_engagement
        WHERE recent_plays >= 5
          AND total_plays >= recent_plays
          AND (CAST(recent_plays AS REAL) / total_plays) >= 0.3
          AND avg_pct_played >= ?
        ORDER BY (CAST(recent_plays AS REAL) / total_plays) DESC,
                 recent_plays DESC
        LIMIT ?
        """,
        (min_avg_pct, limit),
    ).fetchall()


def _report_spam(conn: sqlite3.Connection, limit: int, **_kw) -> list[sqlite3.Row]:
    """Tracks Spotify keeps autoplaying that you keep skipping.

    Useful as a worklist for "Don't Play This For Me Again" in the
    Spotify app. If avg_pct < 15 AND you've never deliberately picked it,
    the track has only ever entered your stream via algorithm — and you
    have only ever rejected it. Marking these as disliked tightens up
    Spotify's recommender for everything else.
    """
    return conn.execute(
        """
        SELECT v.track_name, v.primary_artist_name, v.total_plays,
               v.recent_plays, ROUND(v.avg_pct_played) AS avg_pct
        FROM v_track_engagement v
        WHERE v.total_plays >= 5
          AND v.avg_pct_played < 15
          AND v.deliberate_quality = 0
        ORDER BY v.total_plays DESC, v.recent_plays DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()


def _report_hidden_loves(conn: sqlite3.Connection, limit: int, **_kw) -> list[sqlite3.Row]:
    """Low-play-count tracks with high engagement and at least one deliberate pick.

    The "you didn't realize you loved this" surface. Tracks you've only
    played a handful of times, but at least once you clicked play on
    purpose AND your overall finish-rate is high. Recency-blind on purpose —
    these can be months or years old; the point is to surface what's been
    hiding behind the high-play-count noise.
    """
    return conn.execute(
        """
        SELECT v.track_name, v.primary_artist_name, v.total_plays,
               v.deliberate_quality, ROUND(v.avg_pct_played) AS avg_pct,
               substr(v.last_played, 1, 10) AS last_played
        FROM v_track_engagement v
        WHERE v.total_plays BETWEEN 5 AND 20
          AND v.avg_pct_played >= 70
          AND v.deliberate_quality >= 1
        ORDER BY v.avg_pct_played DESC, v.deliberate_quality DESC,
                 v.total_plays DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()


def _report_tags(conn: sqlite3.Connection, limit: int, **_kw) -> list[sqlite3.Row]:
    """Genre/tag distribution across the library.

    Counts DISTINCT tracks per tag (a track with the tag from both MB and LF
    only counts once). Source column shows which providers contributed.
    """
    return conn.execute(
        """
        SELECT tag,
               COUNT(DISTINCT track_id) AS n_tracks,
               GROUP_CONCAT(DISTINCT source) AS sources
        FROM v_track_tags
        GROUP BY tag
        ORDER BY n_tracks DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()


REPORTS: dict[str, Report] = {
    "forgotten": Report(
        name="forgotten",
        description="High lifetime plays + zero recent plays. 'You used to love these.'",
        fn=_report_forgotten,
        columns=["Track", "Artist", "Plays", "Recent", "Avg%", "Last played"],
        column_widths=[40, 25, 6, 6, 5, 12],
        column_aligns=["l", "l", "r", "r", "r", "l"],
    ),
    "backbutton": Report(
        name="backbutton",
        description="High back-button count. 'I rewound to hear it again.' Purest love.",
        fn=_report_backbutton,
        columns=["Track", "Artist", "Plays", "BackBtn", "Avg%"],
        column_widths=[40, 25, 6, 7, 5],
        column_aligns=["l", "l", "r", "r", "r"],
    ),
    "skipped": Report(
        name="skipped",
        description="High lifetime love + high RECENT skip rate. Taste shift detector.",
        fn=_report_skipped,
        columns=["Track", "Artist", "Plays", "Recent", "Skips", "Skip%", "Life Avg%"],
        column_widths=[40, 25, 6, 6, 5, 6, 9],
        column_aligns=["l", "l", "r", "r", "r", "r", "r"],
    ),
    "deliberate": Report(
        name="deliberate",
        description="High deliberate-quality ratio. 'When I picked this, I always finished.'",
        fn=_report_deliberate,
        columns=["Track", "Artist", "Plays", "Deliberate", "Delib%", "Avg%"],
        column_widths=[40, 25, 6, 10, 6, 5],
        column_aligns=["l", "l", "r", "r", "r", "r"],
    ),
    "obsessions": Report(
        name="obsessions",
        description="High recent-to-lifetime ratio. What you're currently overplaying.",
        fn=_report_obsessions,
        columns=["Track", "Artist", "Plays", "Recent", "Recent%", "Avg%"],
        column_widths=[40, 25, 6, 6, 7, 5],
        column_aligns=["l", "l", "r", "r", "r", "r"],
    ),
    "tags": Report(
        name="tags",
        description="Genre/tag distribution (MB + Last.fm + manual + classifier).",
        fn=_report_tags,
        columns=["Tag", "Tracks", "Sources"],
        column_widths=[40, 7, 30],
        column_aligns=["l", "r", "l"],
    ),
    "spam": Report(
        name="spam",
        description="Spotify keeps queueing these. You keep skipping. Mark as disliked.",
        fn=_report_spam,
        columns=["Track", "Artist", "Plays", "Recent", "Avg%"],
        column_widths=[40, 25, 6, 6, 5],
        column_aligns=["l", "l", "r", "r", "r"],
    ),
    "hidden_loves": Report(
        name="hidden_loves",
        description="Low play count, high engagement, deliberate picks. 'I didn't know.'",
        fn=_report_hidden_loves,
        columns=["Track", "Artist", "Plays", "Deliberate", "Avg%", "Last played"],
        column_widths=[40, 25, 6, 10, 5, 12],
        column_aligns=["l", "l", "r", "r", "r", "l"],
    ),
}


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------
def _truncate(s, width: int) -> str:
    s = "" if s is None else str(s)
    return s if len(s) <= width else s[: width - 1] + "…"


def print_report(report: Report, rows: list[sqlite3.Row]) -> None:
    """Render a report as a fixed-width text table to stdout."""
    print()
    print(f"=== {report.name}: {report.description}")
    print()
    if not rows:
        print("  (no rows)")
        return

    # Header
    header_parts = []
    for col, w, a in zip(report.columns, report.column_widths, report.column_aligns):
        h = _truncate(col, w)
        header_parts.append(f"{h:<{w}}" if a == "l" else f"{h:>{w}}")
    print("  " + "  ".join(header_parts))
    print("  " + "  ".join("-" * w for w in report.column_widths))

    # Rows — assume row positional access matches column order
    for r in rows:
        parts = []
        for i, (w, a) in enumerate(zip(report.column_widths, report.column_aligns)):
            v = _truncate(r[i], w)
            parts.append(f"{v:<{w}}" if a == "l" else f"{v:>{w}}")
        print("  " + "  ".join(parts))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="One-shot reports against the music-data DB.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    g = parser.add_mutually_exclusive_group(required=True)
    g.add_argument("--list", action="store_true",
                   help="List available reports + descriptions.")
    g.add_argument("--report", choices=sorted(REPORTS.keys()),
                   help="Run a single named report.")
    g.add_argument("--all", action="store_true",
                   help="Run every report in sequence.")
    parser.add_argument("--limit", type=int, default=20,
                        help="Max rows per report.")
    parser.add_argument("--db", type=str, default=None,
                        help="Path to music.db (default: auto-detect).")
    parser.add_argument("--min-avg-pct", type=int, default=20,
                        help="Quality gate for the obsessions report — "
                             "drop tracks with avg_pct_played below this. "
                             "Lower to 0 to see everything Spotify is "
                             "currently feeding you.")
    args = parser.parse_args(argv)

    if args.list:
        print("Available reports:")
        for name, r in sorted(REPORTS.items()):
            print(f"  {name:12} {r.description}")
        return 0

    kwargs = {"min_avg_pct": args.min_avg_pct}
    conn = connect(args.db)
    try:
        if args.all:
            for name in sorted(REPORTS.keys()):
                rows = REPORTS[name].fn(conn, args.limit, **kwargs)
                print_report(REPORTS[name], rows)
        else:
            report = REPORTS[args.report]
            rows = report.fn(conn, args.limit, **kwargs)
            print_report(report, rows)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
