"""Interactive labeler for listening contexts.

Reads cluster summaries from the database, prints each cluster's centroid
+ top primary tracks, and prompts the user for a name. Writes the result
to listening_contexts.user_label.

Run after scripts/cluster_modes.py — that script populates the contexts;
this one names them.

Usage:
    python -m scripts.label_modes              # label any unlabeled clusters
    python -m scripts.label_modes --relabel    # re-prompt all clusters
    python -m scripts.label_modes --list       # print labels and exit
    python -m scripts.label_modes --top-n 15   # show 15 top tracks per cluster
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from dataclasses import dataclass
from typing import Callable

import numpy as np

from scripts.cluster_modes import describe_cluster
from scripts.db import connect


@dataclass
class ClusterInfo:
    context_id: int
    cluster_id: int
    user_label: str
    play_count: int
    centroid_hour_cos: float
    centroid_hour_sin: float
    centroid_is_weekend: float


@dataclass
class TopTrack:
    track_name: str
    artist_name: str | None
    plays_in_cluster: int
    affinity: float


def load_clusters(conn: sqlite3.Connection, only_unlabeled: bool) -> list[ClusterInfo]:
    """Return all clusters in cluster_id order, optionally filtered."""
    sql = """
        SELECT context_id, cluster_id, user_label, play_count,
               centroid_hour_cos, centroid_hour_sin, centroid_is_weekend
        FROM listening_contexts
        ORDER BY cluster_id
    """
    rows = conn.execute(sql).fetchall()
    out = []
    for r in rows:
        if only_unlabeled and r["user_label"]:
            continue
        out.append(ClusterInfo(
            context_id=r["context_id"],
            cluster_id=r["cluster_id"],
            user_label=r["user_label"] or "",
            play_count=r["play_count"],
            centroid_hour_cos=r["centroid_hour_cos"] if r["centroid_hour_cos"] is not None else 0.0,
            centroid_hour_sin=r["centroid_hour_sin"] if r["centroid_hour_sin"] is not None else 0.0,
            centroid_is_weekend=r["centroid_is_weekend"] if r["centroid_is_weekend"] is not None else 0.0,
        ))
    return out


def top_tracks_for_context(conn: sqlite3.Connection, ctx_id: int, n: int) -> list[TopTrack]:
    """Top-N tracks where this context is the track's primary mode.

    Sorted by total plays of the track, descending. Tracks where this is the
    primary cluster but with very few plays still surface; the count gives
    the user enough context to decide if the cluster's "personality" matches
    a real listening mode.
    """
    rows = conn.execute("""
        SELECT
            t.name AS track_name,
            ar.name AS artist_name,
            tca.affinity AS affinity,
            (SELECT COUNT(*) FROM plays p
             WHERE p.track_id = t.track_id AND p.content_type = 'track') AS total_plays
        FROM track_context_affinity tca
        JOIN tracks t ON tca.track_id = t.track_id
        LEFT JOIN track_artists ta ON ta.track_id = t.track_id AND ta.position = 0
        LEFT JOIN artists ar ON ar.artist_id = ta.artist_id
        WHERE tca.context_id = ? AND tca.is_primary = 1
        ORDER BY total_plays DESC
        LIMIT ?
    """, (ctx_id, n)).fetchall()
    return [
        TopTrack(
            track_name=r["track_name"] or "(unknown)",
            artist_name=r["artist_name"],
            plays_in_cluster=r["total_plays"] or 0,
            affinity=float(r["affinity"]),
        )
        for r in rows
    ]


def total_play_count(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT COUNT(*) FROM plays WHERE content_type = 'track'"
    ).fetchone()
    return row[0] if row else 0


def format_cluster_block(info: ClusterInfo, top_tracks: list[TopTrack],
                         total_plays: int) -> str:
    """Render one cluster as a multi-line text block."""
    centroid = np.array([info.centroid_hour_cos, info.centroid_hour_sin, info.centroid_is_weekend])
    centroid_summary = describe_cluster(centroid)
    pct = 100.0 * info.play_count / total_plays if total_plays > 0 else 0.0
    lines = [
        f"Cluster {info.cluster_id} (context {info.context_id})",
        f"  Time pattern: {centroid_summary}",
        f"  Plays: {info.play_count:,} ({pct:.1f}% of your listening)",
    ]
    if info.user_label:
        lines.append(f"  Current label: {info.user_label!r}")
    else:
        lines.append("  Current label: <unlabeled>")

    if top_tracks:
        lines.append(f"  Top {len(top_tracks)} tracks (primary in this cluster, by play count):")
        for i, tt in enumerate(top_tracks, 1):
            artist = tt.artist_name or "(unknown artist)"
            track = tt.track_name
            lines.append(f"    {i:>2}. {track[:50]:50s}  {artist[:25]:25s}  ({tt.plays_in_cluster} plays, aff={tt.affinity:.2f})")
    else:
        lines.append("  Top tracks: none with primary affinity above threshold")
    return "\n".join(lines)


def prompt_label(input_fn: Callable[[str], str], current_label: str) -> str | None:
    """Prompt for a label. Empty input → None (skip, don't change current label).

    Whitespace-only input is treated as empty.
    """
    hint = " (Enter to keep current)" if current_label else " (Enter to skip)"
    raw = input_fn(f"  What do you call this?{hint} > ")
    stripped = raw.strip()
    if not stripped:
        return None
    return stripped


def update_label(conn: sqlite3.Connection, ctx_id: int, label: str) -> None:
    conn.execute(
        "UPDATE listening_contexts SET user_label = ? WHERE context_id = ?",
        (label, ctx_id),
    )


def list_labels(conn: sqlite3.Connection) -> list[tuple[int, int, str, int]]:
    """Return (context_id, cluster_id, user_label, play_count) for all clusters."""
    rows = conn.execute("""
        SELECT context_id, cluster_id, user_label, play_count
        FROM listening_contexts
        ORDER BY cluster_id
    """).fetchall()
    return [(r[0], r[1], r[2] or "", r[3]) for r in rows]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main(argv: list[str] | None = None,
         input_fn: Callable[[str], str] = input,
         conn: sqlite3.Connection | None = None) -> int:
    parser = argparse.ArgumentParser(description="Interactively label listening contexts.")
    parser.add_argument("--top-n", type=int, default=10,
                        help="Top tracks to display per cluster")
    parser.add_argument("--relabel", action="store_true",
                        help="Re-prompt for already-labeled clusters")
    parser.add_argument("--list", dest="list_mode", action="store_true",
                        help="Print current labels and exit")
    parser.add_argument("--db", type=str, default=None, help="Path to music.db")
    args = parser.parse_args(argv)

    own_conn = conn is None
    if own_conn:
        conn = connect(args.db)
    try:
        if args.list_mode:
            labels = list_labels(conn)
            if not labels:
                print("No clusters found. Run `python -m scripts.cluster_modes` first.")
                return 0
            print(f"{'Cluster':<8} {'Plays':<10} Label")
            for ctx_id, cluster_id, label, play_count in labels:
                shown = label if label else "<unlabeled>"
                print(f"  {cluster_id:<6} {play_count:>8,}  {shown}")
            return 0

        clusters = load_clusters(conn, only_unlabeled=not args.relabel)
        if not clusters:
            existing = list_labels(conn)
            if not existing:
                print("No clusters found. Run `python -m scripts.cluster_modes` first.")
            else:
                labeled = sum(1 for _, _, lbl, _ in existing if lbl)
                print(f"All {labeled} clusters already labeled. Use --relabel to rename "
                      f"or --list to view.")
            return 0

        total_plays = total_play_count(conn)
        print(f"Found {len(clusters)} cluster(s) to label. Total plays in DB: {total_plays:,}.")
        print("Enter a name (e.g. 'morning workout', 'late night chill'). "
              "Empty input skips a cluster without changing it.\n")

        labeled_count = 0
        for cluster in clusters:
            top_tracks = top_tracks_for_context(conn, cluster.context_id, args.top_n)
            print(format_cluster_block(cluster, top_tracks, total_plays))
            try:
                new_label = prompt_label(input_fn, cluster.user_label)
            except (EOFError, KeyboardInterrupt):
                print("\nAborted. Labels written so far have been saved.")
                return 0
            if new_label is None:
                print("  (skipped)\n")
                continue
            update_label(conn, cluster.context_id, new_label)
            labeled_count += 1
            print(f"  Saved: {new_label!r}\n")

        print(f"Done. Updated {labeled_count} cluster label(s).")
        return 0
    finally:
        if own_conn:
            conn.close()


if __name__ == "__main__":
    sys.exit(main())
