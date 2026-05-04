"""Tests for the label_modes interactive labeler.

Stdin is injected via the input_fn parameter so prompts can be tested
without real terminal interaction.
"""
from __future__ import annotations

import sqlite3

import numpy as np
import pytest

from scripts.label_modes import (
    ClusterInfo,
    TopTrack,
    format_cluster_block,
    list_labels,
    load_clusters,
    main,
    prompt_label,
    top_tracks_for_context,
    update_label,
)
from scripts.db import init_schema


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _make_synth_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:", isolation_level=None)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.row_factory = sqlite3.Row
    init_schema(conn)
    return conn


def _seed_two_clusters(conn: sqlite3.Connection,
                       label_first: str = "",
                       label_second: str = "") -> None:
    """Two clusters; cluster_id 0 unlabeled by default, 1 unlabeled by default."""
    conn.execute(
        """
        INSERT INTO listening_contexts
            (cluster_id, user_label, centroid_hour_cos, centroid_hour_sin,
             centroid_is_weekend, play_count)
        VALUES (0, ?, 0.5, 0.866, 0.0, 1000)
        """,
        (label_first,),
    )
    conn.execute(
        """
        INSERT INTO listening_contexts
            (cluster_id, user_label, centroid_hour_cos, centroid_hour_sin,
             centroid_is_weekend, play_count)
        VALUES (1, ?, -0.5, -0.866, 1.0, 500)
        """,
        (label_second,),
    )


def _add_track_with_plays(conn: sqlite3.Connection, track_id: int, name: str,
                          artist_name: str, n_plays: int) -> None:
    uri = f"spotify:track:t{track_id}"
    conn.execute(
        "INSERT INTO tracks (track_id, spotify_track_uri, name) VALUES (?, ?, ?)",
        (track_id, uri, name),
    )
    conn.execute(
        "INSERT INTO artists (artist_id, spotify_artist_uri, name, name_normalized) VALUES (?, ?, ?, ?)",
        (track_id, f"spotify:artist:a{track_id}", artist_name, artist_name.lower()),
    )
    conn.execute(
        "INSERT INTO track_artists (track_id, artist_id, position) VALUES (?, ?, 0)",
        (track_id, track_id),
    )
    for i in range(n_plays):
        # Vary ms_played to dodge plays' UNIQUE(ts, content_uri, ms_played).
        conn.execute(
            """INSERT INTO plays (track_id, content_type, content_uri, ts, ms_played, source)
               VALUES (?, 'track', ?, '2026-04-01 12:00:00', ?, 'test')""",
            (track_id, uri, 60000 + i),
        )


# ---------------------------------------------------------------------------
# load_clusters
# ---------------------------------------------------------------------------
class TestLoadClusters:
    def test_loads_all_when_only_unlabeled_false(self):
        conn = _make_synth_db()
        _seed_two_clusters(conn, label_first="morning", label_second="evening")
        clusters = load_clusters(conn, only_unlabeled=False)
        assert [c.cluster_id for c in clusters] == [0, 1]
        assert [c.user_label for c in clusters] == ["morning", "evening"]

    def test_filters_to_unlabeled(self):
        conn = _make_synth_db()
        _seed_two_clusters(conn, label_first="morning", label_second="")
        clusters = load_clusters(conn, only_unlabeled=True)
        assert len(clusters) == 1
        assert clusters[0].cluster_id == 1
        assert clusters[0].user_label == ""

    def test_handles_null_centroids(self):
        # Older clustering runs may not have populated centroid columns.
        conn = _make_synth_db()
        conn.execute(
            "INSERT INTO listening_contexts (cluster_id, play_count) VALUES (0, 100)"
        )
        clusters = load_clusters(conn, only_unlabeled=False)
        assert len(clusters) == 1
        # Should default to 0.0 instead of crashing.
        assert clusters[0].centroid_hour_cos == 0.0
        assert clusters[0].centroid_hour_sin == 0.0
        assert clusters[0].centroid_is_weekend == 0.0


# ---------------------------------------------------------------------------
# top_tracks_for_context
# ---------------------------------------------------------------------------
class TestTopTracks:
    def test_returns_only_primary_tracks(self):
        conn = _make_synth_db()
        _seed_two_clusters(conn)
        ctx = conn.execute("SELECT context_id FROM listening_contexts ORDER BY cluster_id").fetchall()
        ctx_a, ctx_b = ctx[0]["context_id"], ctx[1]["context_id"]

        _add_track_with_plays(conn, 1, "Primary in A", "Artist A", n_plays=10)
        _add_track_with_plays(conn, 2, "Primary in B", "Artist B", n_plays=5)
        _add_track_with_plays(conn, 3, "Non-primary", "Artist C", n_plays=20)

        # Track 1 primary in A
        conn.execute("INSERT INTO track_context_affinity VALUES (1, ?, 0.9, 1)", (ctx_a,))
        # Track 2 primary in B
        conn.execute("INSERT INTO track_context_affinity VALUES (2, ?, 0.8, 1)", (ctx_b,))
        # Track 3 has affinity to A but is_primary=0
        conn.execute("INSERT INTO track_context_affinity VALUES (3, ?, 0.4, 0)", (ctx_a,))

        a_tracks = top_tracks_for_context(conn, ctx_a, n=10)
        b_tracks = top_tracks_for_context(conn, ctx_b, n=10)

        assert [t.track_name for t in a_tracks] == ["Primary in A"]
        assert [t.track_name for t in b_tracks] == ["Primary in B"]

    def test_orders_by_total_plays_desc(self):
        conn = _make_synth_db()
        _seed_two_clusters(conn)
        ctx_a = conn.execute("SELECT context_id FROM listening_contexts WHERE cluster_id = 0").fetchone()[0]

        _add_track_with_plays(conn, 1, "Few plays", "X", n_plays=3)
        _add_track_with_plays(conn, 2, "Many plays", "Y", n_plays=50)
        _add_track_with_plays(conn, 3, "Medium plays", "Z", n_plays=20)
        for tid in (1, 2, 3):
            conn.execute("INSERT INTO track_context_affinity VALUES (?, ?, 0.9, 1)", (tid, ctx_a))

        tracks = top_tracks_for_context(conn, ctx_a, n=10)
        assert [t.track_name for t in tracks] == ["Many plays", "Medium plays", "Few plays"]

    def test_respects_limit(self):
        conn = _make_synth_db()
        _seed_two_clusters(conn)
        ctx_a = conn.execute("SELECT context_id FROM listening_contexts WHERE cluster_id = 0").fetchone()[0]
        for i in range(1, 6):
            _add_track_with_plays(conn, i, f"Track {i}", "X", n_plays=10 - i)
            conn.execute("INSERT INTO track_context_affinity VALUES (?, ?, 0.9, 1)", (i, ctx_a))
        tracks = top_tracks_for_context(conn, ctx_a, n=3)
        assert len(tracks) == 3


# ---------------------------------------------------------------------------
# format_cluster_block
# ---------------------------------------------------------------------------
class TestFormat:
    def test_includes_unlabeled_marker(self):
        info = ClusterInfo(
            context_id=1, cluster_id=0, user_label="",
            play_count=1000,
            centroid_hour_cos=0.5, centroid_hour_sin=0.866, centroid_is_weekend=0.0,
        )
        block = format_cluster_block(info, top_tracks=[], total_plays=10000)
        assert "<unlabeled>" in block
        assert "10.0%" in block

    def test_includes_current_label(self):
        info = ClusterInfo(
            context_id=1, cluster_id=0, user_label="morning routine",
            play_count=500,
            centroid_hour_cos=0.5, centroid_hour_sin=0.866, centroid_is_weekend=0.0,
        )
        block = format_cluster_block(info, top_tracks=[], total_plays=1000)
        assert "morning routine" in block

    def test_lists_top_tracks(self):
        info = ClusterInfo(
            context_id=1, cluster_id=0, user_label="",
            play_count=1000,
            centroid_hour_cos=0.5, centroid_hour_sin=0.866, centroid_is_weekend=0.0,
        )
        tracks = [
            TopTrack("Song A", "Artist X", plays_in_cluster=42, affinity=0.85),
            TopTrack("Song B", None, plays_in_cluster=20, affinity=0.55),
        ]
        block = format_cluster_block(info, top_tracks=tracks, total_plays=10000)
        assert "Song A" in block
        assert "Artist X" in block
        assert "Song B" in block
        assert "(unknown artist)" in block
        assert "42 plays" in block
        assert "aff=0.85" in block


# ---------------------------------------------------------------------------
# prompt_label
# ---------------------------------------------------------------------------
class TestPrompt:
    def test_returns_stripped_input(self):
        inp = lambda _: "  morning workout  "
        assert prompt_label(inp, current_label="") == "morning workout"

    def test_empty_returns_none(self):
        assert prompt_label(lambda _: "", current_label="") is None
        assert prompt_label(lambda _: "   ", current_label="") is None

    def test_empty_does_not_overwrite_current(self):
        # A None return tells main() to skip — current label stays untouched.
        assert prompt_label(lambda _: "", current_label="morning") is None

    def test_explicit_label_overwrites_current(self):
        assert prompt_label(lambda _: "evening", current_label="morning") == "evening"


# ---------------------------------------------------------------------------
# update_label
# ---------------------------------------------------------------------------
class TestUpdate:
    def test_writes_to_db(self):
        conn = _make_synth_db()
        _seed_two_clusters(conn)
        ctx_a = conn.execute("SELECT context_id FROM listening_contexts WHERE cluster_id = 0").fetchone()[0]
        update_label(conn, ctx_a, "morning routine")
        row = conn.execute(
            "SELECT user_label FROM listening_contexts WHERE context_id = ?", (ctx_a,)
        ).fetchone()
        assert row["user_label"] == "morning routine"


# ---------------------------------------------------------------------------
# list_labels
# ---------------------------------------------------------------------------
class TestList:
    def test_returns_all_clusters(self):
        conn = _make_synth_db()
        _seed_two_clusters(conn, label_first="morning", label_second="")
        out = list_labels(conn)
        assert len(out) == 2
        assert out[0][1] == 0  # cluster_id
        assert out[0][2] == "morning"
        assert out[1][1] == 1
        assert out[1][2] == ""

    def test_empty_db(self):
        conn = _make_synth_db()
        assert list_labels(conn) == []


# ---------------------------------------------------------------------------
# main flow (integration-ish via input_fn injection)
# ---------------------------------------------------------------------------
class TestMain:
    def test_list_mode_prints_labels(self, capsys):
        conn = _make_synth_db()
        _seed_two_clusters(conn, label_first="morning", label_second="evening")
        rc = main(argv=["--list"], conn=conn)
        captured = capsys.readouterr()
        assert rc == 0
        assert "morning" in captured.out
        assert "evening" in captured.out

    def test_labels_unlabeled_clusters(self, capsys):
        conn = _make_synth_db()
        _seed_two_clusters(conn, label_first="", label_second="")
        # Add one play so total_play_count > 0 (CHECK constraint requires real track_id)
        _add_track_with_plays(conn, 999, "Dummy", "Dummy Artist", n_plays=1)
        responses = iter(["weekday morning", "weekend afternoon"])
        rc = main(argv=[], input_fn=lambda _: next(responses), conn=conn)
        assert rc == 0
        rows = conn.execute(
            "SELECT cluster_id, user_label FROM listening_contexts ORDER BY cluster_id"
        ).fetchall()
        assert rows[0]["user_label"] == "weekday morning"
        assert rows[1]["user_label"] == "weekend afternoon"

    def test_skip_does_not_overwrite(self, capsys):
        conn = _make_synth_db()
        _seed_two_clusters(conn, label_first="existing-label", label_second="")
        _add_track_with_plays(conn, 999, "Dummy", "Dummy Artist", n_plays=1)
        # In --relabel mode, both clusters prompt. Skip the first (empty input),
        # set the second.
        responses = iter(["", "evening"])
        rc = main(argv=["--relabel"], input_fn=lambda _: next(responses), conn=conn)
        assert rc == 0
        rows = conn.execute(
            "SELECT cluster_id, user_label FROM listening_contexts ORDER BY cluster_id"
        ).fetchall()
        assert rows[0]["user_label"] == "existing-label"  # unchanged
        assert rows[1]["user_label"] == "evening"

    def test_no_clusters_message(self, capsys):
        conn = _make_synth_db()
        rc = main(argv=[], conn=conn)
        captured = capsys.readouterr()
        assert rc == 0
        assert "cluster_modes" in captured.out
