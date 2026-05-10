"""Generate a clickable worklist for marking spam tracks/artists in Spotify.

The `spam` report in reports.py surfaces tracks Spotify autoplay keeps
queueing that the user has never deliberately picked and never finishes.
Since Spotify's dev-mode wall blocks programmatic dislike (PUT/DELETE on
/me/tracks returned 403 in the May 10 2026 smoke), the only way to feed
this back to Spotify's recommender is manual action in the desktop app.

This script generates a markdown worklist with clickable open.spotify.com
links — one section ranked by artist (best for "Don't play this artist"
actions on artists with multiple spam tracks) and a detailed track list
(for finer-grained "Hide song" actions in playlist context).

Usage:
    python -m scripts.spam_worklist
    python -m scripts.spam_worklist --top 50 --output spam-worklist.md
    python -m scripts.spam_worklist --max-avg-pct 10  # tighter spam threshold
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from collections import defaultdict
from typing import Optional

from scripts.db import connect


SPAM_QUERY = """
    SELECT v.track_name,
           v.spotify_track_uri,
           ar.name AS artist_name,
           ar.spotify_artist_uri,
           v.total_plays,
           v.recent_plays,
           ROUND(v.avg_pct_played) AS avg_pct
    FROM v_track_engagement v
    JOIN track_artists ta ON ta.track_id = v.track_id AND ta.position = 0
    JOIN artists ar ON ar.artist_id = ta.artist_id
    WHERE v.total_plays >= ?
      AND v.avg_pct_played < ?
      AND v.deliberate_quality = 0
    ORDER BY v.total_plays DESC, v.recent_plays DESC
    LIMIT ?
"""


def _uri_to_web_url(uri: str | None, kind: str) -> str | None:
    """Convert spotify:track:ID → https://open.spotify.com/track/ID."""
    if not uri or not uri.startswith(f"spotify:{kind}:"):
        return None
    return f"https://open.spotify.com/{kind}/{uri.split(':')[2]}"


def fetch_spam(conn: sqlite3.Connection, min_plays: int, max_avg: int,
               limit: int) -> list[sqlite3.Row]:
    return conn.execute(SPAM_QUERY, (min_plays, max_avg, limit)).fetchall()


def render_markdown(rows: list[sqlite3.Row], min_plays: int,
                    max_avg: int) -> str:
    if not rows:
        return f"# Spam Worklist\n\n(No tracks matched: min_plays>={min_plays}, avg_pct<{max_avg}, deliberate_quality=0)\n"

    by_artist: dict[str, list[sqlite3.Row]] = defaultdict(list)
    for r in rows:
        by_artist[r["artist_name"]].append(r)

    artists_ranked = sorted(
        by_artist.items(),
        key=lambda kv: (-len(kv[1]), -sum(r["total_plays"] for r in kv[1])),
    )

    lines: list[str] = []
    lines.append("# Spam Worklist")
    lines.append("")
    lines.append(f"**{len(rows)} tracks across {len(by_artist)} unique artists.** "
                 f"Filter: total_plays >= {min_plays}, avg_pct_played < {max_avg}, "
                 f"never deliberately picked.")
    lines.append("")
    lines.append("## How to use this list")
    lines.append("")
    lines.append("Spotify's dev-mode API blocks programmatic dislike. This is the manual path.")
    lines.append("")
    lines.append("**For artists with multiple spam tracks** (highest-leverage cleanup):")
    lines.append("1. Click the artist link.")
    lines.append("2. In the Spotify desktop app, click the `…` menu next to the artist name.")
    lines.append("3. Select **\"Don't play this artist\"**.")
    lines.append("4. Spotify will stop autoplaying ANY track from that artist.")
    lines.append("")
    lines.append("**For mixed-feelings artists** (you like SOME of their stuff):")
    lines.append("1. Skip the \"don't play artist\" step. Use per-track Hide instead.")
    lines.append("2. Find the track in a playlist or queue context.")
    lines.append("3. Right-click → **\"Hide song\"**. Spotify won't autoplay it again.")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## By artist (sorted by spam-track count, then total plays)")
    lines.append("")

    for artist_name, artist_rows in artists_ranked:
        artist_uri = artist_rows[0]["spotify_artist_uri"]
        artist_url = _uri_to_web_url(artist_uri, "artist")
        total = sum(r["total_plays"] for r in artist_rows)
        track_count = len(artist_rows)

        header = (f"### {artist_name} — "
                  f"{track_count} spam track{'s' if track_count > 1 else ''}, "
                  f"{total} lifetime plays")
        lines.append(header)
        if artist_url:
            lines.append(f"- Artist: {artist_url}")
        else:
            lines.append("- Artist: (URI not enriched)")
        lines.append("- Tracks:")
        for r in sorted(artist_rows, key=lambda x: -x["total_plays"]):
            track_url = _uri_to_web_url(r["spotify_track_uri"], "track")
            lines.append(f"  - **{r['track_name']}** "
                         f"— {r['total_plays']} plays, {r['avg_pct']}% avg finish")
            if track_url:
                lines.append(f"    - {track_url}")
        lines.append("")

    lines.append("---")
    lines.append("")
    lines.append(f"_Generated by `scripts/spam_worklist.py`. "
                 f"Re-run after marking artists/tracks to confirm the spam set has shrunk._")
    lines.append("")
    return "\n".join(lines)


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate a clickable worklist for marking Spotify spam.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--top", type=int, default=30,
                        help="Max number of spam tracks to include.")
    parser.add_argument("--min-plays", type=int, default=5,
                        help="Minimum lifetime plays to qualify as spam.")
    parser.add_argument("--max-avg-pct", type=int, default=15,
                        help="Drop tracks with avg_pct_played at or above this.")
    parser.add_argument("--output", type=str, default=None,
                        help="Write to a file instead of stdout.")
    parser.add_argument("--db", type=str, default=None,
                        help="Path to music.db (default: auto-detect).")
    args = parser.parse_args(argv)

    conn = connect(args.db)
    try:
        rows = fetch_spam(conn, args.min_plays, args.max_avg_pct, args.top)
    finally:
        conn.close()

    md = render_markdown(rows, args.min_plays, args.max_avg_pct)

    if args.output:
        with open(args.output, "w") as f:
            f.write(md)
        print(f"Wrote {len(rows)} tracks ({len(set(r['artist_name'] for r in rows))} artists) to {args.output}")
    else:
        print(md)
    return 0


if __name__ == "__main__":
    sys.exit(main())
