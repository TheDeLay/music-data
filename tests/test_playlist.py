"""Tests for the playlist generator (engagement-model layer 3).

Verifies:
  - --mode resolution by cluster_id, user_label, case-insensitive matching
  - Unknown --mode raises ModeNotFoundError with helpful options listing
  - Strict filtering (is_primary=1 only) vs loose filtering (--min-affinity)
  - --love-min filtering and --top truncation
  - End-to-end build_playlist pipeline
  - Output formats: table (empty + populated), json (parseable), uris
"""
from __future__ import annotations

import io
import json
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from scripts.playlist import (
    EMPTY_MSG,
    ModeNotFoundError,
    PlaylistConfig,
    build_playlist,
    filter_by_context,
    print_json,
    print_table,
    print_uris,
    resolve_context,
)
from scripts.score import ScoreConfig, TrackScore, score_tracks


SCHEMA_PATH = Path(__file__).resolve().parent.parent / "sql" / "schema.sql"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
def _new_db(tmp_path) -> sqlite3.Connection:
    db_path = tmp_path / "test_playlist.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(SCHEMA_PATH.read_text())
    return conn


def _insert_contexts(conn: sqlite3.Connection) -> None:
    """Three contexts: two labeled, one unlabeled."""
    conn.executemany(
        """INSERT INTO listening_contexts
           (context_id, cluster_id, user_label, play_count) VALUES (?, ?, ?, ?)""",
        [
            (1, 0, "weekday morning", 100),
            (2, 1, "late night", 80),
            (3, 2, "", 50),
        ],
    )
    conn.commit()


@pytest.fixture
def contexts_only_db(tmp_path):
    """DB with only listening_contexts populated (for resolve_context tests)."""
    conn = _new_db(tmp_path)
    _insert_contexts(conn)
    yield conn
    conn.close()


@pytest.fixture
def empty_contexts_db(tmp_path):
    """DB with NO listening_contexts (for unhelpful-error tests)."""
    conn = _new_db(tmp_path)
    yield conn
    conn.close()


@pytest.fixture
def full_db(tmp_path):
    """Synthetic DB with tracks + plays + contexts + affinities.

    Track layout (track_id : description):
      1  "Morning Anthem"  primary in ctx 1, lots of recent quality plays
      2  "Night Owl"       primary in ctx 2, medium engagement
      3  "All Context"     no primary (aff=0.34 each), high lifetime plays
      4  "Rare Gem"        primary in ctx 1, few but excellent plays (backbtn)
      5  "Falling Out"     primary in ctx 1, recent skip streak
    """
    conn = _new_db(tmp_path)
    now = datetime.utcnow()
    recent = now - timedelta(days=30)
    old = now - timedelta(days=180)

    conn.execute("INSERT INTO artists (artist_id, name, name_normalized) "
                 "VALUES (1, 'Test Artist', 'test artist')")
    conn.execute("INSERT INTO albums (album_id, name, name_normalized, release_year) "
                 "VALUES (1, 'Test Album', 'test album', 2020)")
    conn.execute("INSERT INTO ingestion_runs (run_id, source, status) "
                 "VALUES (1, 'test', 'completed')")

    tracks = [
        (1, "spotify:track:morning",  "Morning Anthem", 240000),
        (2, "spotify:track:night",    "Night Owl",      240000),
        (3, "spotify:track:allctx",   "All Context",    240000),
        (4, "spotify:track:rare",     "Rare Gem",       240000),
        (5, "spotify:track:falling",  "Falling Out",    240000),
    ]
    for tid, uri, name, dur in tracks:
        conn.execute(
            "INSERT INTO tracks (track_id, spotify_track_uri, name, album_id, duration_ms) "
            "VALUES (?, ?, ?, 1, ?)", (tid, uri, name, dur))
        conn.execute("INSERT INTO track_artists (track_id, artist_id, position) "
                     "VALUES (?, 1, 0)", (tid,))

    def _play(ts, uri, tid, ms_played=230000, reason_start=None, reason_end=None):
        conn.execute(
            "INSERT INTO plays (ts, ms_played, content_type, content_uri, track_id, "
            "reason_start, reason_end, source, ingestion_run_id) "
            "VALUES (?, ?, 'track', ?, ?, ?, ?, 'extended_dump', 1)",
            (ts, ms_played, uri, tid, reason_start, reason_end))

    # Track 1: Morning Anthem — many recent quality plays, all clickrow
    for i in range(10):
        _play((recent + timedelta(days=i)).strftime("%Y-%m-%dT%H:%M:%SZ"),
              "spotify:track:morning", 1, reason_start="clickrow")
    # Track 2: Night Owl — modest engagement
    for i in range(5):
        _play((recent + timedelta(days=i)).strftime("%Y-%m-%dT%H:%M:%SZ"),
              "spotify:track:night", 2)
    # Track 3: All Context — high lifetime, low recent
    for i in range(20):
        _play((old + timedelta(days=i)).strftime("%Y-%m-%dT%H:%M:%SZ"),
              "spotify:track:allctx", 3)
    # Track 4: Rare Gem — few plays, all clickrow + backbtn (strong love signal)
    for i in range(3):
        _play((recent + timedelta(days=i)).strftime("%Y-%m-%dT%H:%M:%SZ"),
              "spotify:track:rare", 4, reason_start="clickrow", reason_end="backbtn")
    # Track 5: Falling Out — old quality + recent skips
    for i in range(15):
        _play((old + timedelta(days=i)).strftime("%Y-%m-%dT%H:%M:%SZ"),
              "spotify:track:falling", 5)
    for i in range(8):
        _play((recent + timedelta(days=i)).strftime("%Y-%m-%dT%H:%M:%SZ"),
              "spotify:track:falling", 5, ms_played=20000)

    _insert_contexts(conn)

    # Affinities — design matching the docstring above
    affinities = [
        (1, 1, 0.85, 1),  # Morning Anthem primary in ctx 1
        (1, 2, 0.15, 0),
        (2, 2, 0.90, 1),  # Night Owl primary in ctx 2
        (2, 1, 0.10, 0),
        (3, 1, 0.34, 0),  # All Context — no primary
        (3, 2, 0.33, 0),
        (3, 3, 0.33, 0),
        (4, 1, 1.00, 1),  # Rare Gem primary in ctx 1
        (5, 1, 0.70, 1),  # Falling Out primary in ctx 1
        (5, 2, 0.30, 0),
    ]
    conn.executemany(
        "INSERT INTO track_context_affinity "
        "(track_id, context_id, affinity, is_primary) VALUES (?, ?, ?, ?)",
        affinities,
    )
    conn.commit()
    yield conn
    conn.close()


def _make_score_config(**overrides) -> ScoreConfig:
    return ScoreConfig(**overrides)


def _make_playlist_config(score_overrides=None, **overrides) -> PlaylistConfig:
    sc = _make_score_config(**(score_overrides or {}))
    return PlaylistConfig(score_config=sc, **overrides)


# ---------------------------------------------------------------------------
# resolve_context
# ---------------------------------------------------------------------------
class TestResolveContext:

    def test_none_returns_none(self, contexts_only_db):
        assert resolve_context(contexts_only_db, None) is None

    def test_empty_string_returns_none(self, contexts_only_db):
        assert resolve_context(contexts_only_db, "") is None

    def test_by_cluster_id_int_string(self, contexts_only_db):
        # cluster_id 0 → context_id 1 per fixture
        assert resolve_context(contexts_only_db, "0") == 1
        assert resolve_context(contexts_only_db, "1") == 2

    def test_by_user_label_exact(self, contexts_only_db):
        assert resolve_context(contexts_only_db, "weekday morning") == 1
        assert resolve_context(contexts_only_db, "late night") == 2

    def test_by_user_label_case_insensitive(self, contexts_only_db):
        assert resolve_context(contexts_only_db, "Weekday Morning") == 1
        assert resolve_context(contexts_only_db, "LATE NIGHT") == 2

    def test_skips_unlabeled_context(self, contexts_only_db):
        """Empty user_label must NOT match an empty-string mode arg lookup."""
        # cluster_id 2 has no label — empty-string match should skip it
        # We can still reach it by cluster_id
        assert resolve_context(contexts_only_db, "2") == 3
        # But not via empty-string label lookup (already covered by None test;
        # this guards against a future regression where '' matches '')

    def test_unknown_label_raises_with_available_options(self, contexts_only_db):
        with pytest.raises(ModeNotFoundError) as exc_info:
            resolve_context(contexts_only_db, "morning workout")
        msg = str(exc_info.value)
        assert "'morning workout'" in msg
        assert "weekday morning" in msg
        assert "late night" in msg
        assert "<unlabeled>" in msg  # the empty-label cluster shows up

    def test_unknown_cluster_id_raises_with_available_options(self, contexts_only_db):
        with pytest.raises(ModeNotFoundError) as exc_info:
            resolve_context(contexts_only_db, "99")
        assert "Available modes" in str(exc_info.value)

    def test_no_clusters_raises_with_helpful_message(self, empty_contexts_db):
        with pytest.raises(ModeNotFoundError) as exc_info:
            resolve_context(empty_contexts_db, "anything")
        assert "scripts.cluster_modes" in str(exc_info.value)


# ---------------------------------------------------------------------------
# filter_by_context
# ---------------------------------------------------------------------------
class TestFilterByContext:

    def test_strict_only_includes_primary(self, full_db):
        config = _make_score_config()
        all_tracks = score_tracks(full_db, config)
        # ctx 1: primaries are tracks 1, 4, 5 (Morning Anthem, Rare Gem, Falling Out)
        result = filter_by_context(all_tracks, full_db, context_id=1, min_affinity=None)
        names = {t.track_name for t in result}
        assert names == {"Morning Anthem", "Rare Gem", "Falling Out"}

    def test_strict_excludes_non_primary_even_at_high_affinity(self, full_db):
        """Track 1 has aff=0.15 in ctx 2 but is_primary=0 — must be excluded."""
        config = _make_score_config()
        all_tracks = score_tracks(full_db, config)
        result = filter_by_context(all_tracks, full_db, context_id=2, min_affinity=None)
        names = {t.track_name for t in result}
        assert names == {"Night Owl"}  # only primary in ctx 2

    def test_loose_includes_high_affinity_non_primary(self, full_db):
        """Loose mode: aff >= threshold regardless of is_primary."""
        config = _make_score_config()
        all_tracks = score_tracks(full_db, config)
        # ctx 1 with min_affinity=0.3: tracks 1 (0.85), 3 (0.34), 4 (1.0), 5 (0.70)
        result = filter_by_context(all_tracks, full_db, context_id=1, min_affinity=0.3)
        names = {t.track_name for t in result}
        assert names == {"Morning Anthem", "All Context", "Rare Gem", "Falling Out"}

    def test_loose_excludes_below_threshold(self, full_db):
        config = _make_score_config()
        all_tracks = score_tracks(full_db, config)
        # ctx 1 with min_affinity=0.5: tracks 1 (0.85), 4 (1.0), 5 (0.70) only
        result = filter_by_context(all_tracks, full_db, context_id=1, min_affinity=0.5)
        names = {t.track_name for t in result}
        assert names == {"Morning Anthem", "Rare Gem", "Falling Out"}

    def test_empty_context_returns_empty(self, full_db):
        """Context 3 has only Track 3 with affinity 0.33, is_primary=0."""
        config = _make_score_config()
        all_tracks = score_tracks(full_db, config)
        # Strict mode → no primary, empty result
        assert filter_by_context(all_tracks, full_db, 3, min_affinity=None) == []
        # Loose with high threshold → also empty
        assert filter_by_context(all_tracks, full_db, 3, min_affinity=0.5) == []
        # Loose with permissive threshold → includes Track 3
        result = filter_by_context(all_tracks, full_db, 3, min_affinity=0.1)
        assert {t.track_name for t in result} == {"All Context"}


# ---------------------------------------------------------------------------
# build_playlist
# ---------------------------------------------------------------------------
class TestBuildPlaylist:

    def test_no_mode_returns_all_scored_sorted(self, full_db):
        config = _make_playlist_config(top=10)
        result = build_playlist(full_db, config)
        # Should include all 5 tracks since no mode filter and no love-min
        assert len(result) == 5
        # Should be sorted love_score descending
        scores = [t.love_score for t in result]
        assert scores == sorted(scores, reverse=True)

    def test_mode_filters_to_context(self, full_db):
        config = _make_playlist_config(mode="late night", top=10)
        result = build_playlist(full_db, config)
        assert {t.track_name for t in result} == {"Night Owl"}

    def test_love_min_filters_low_scorers(self, full_db):
        """Falling Out has recent skip streak → low or negative score.

        With love_min above its score it should drop out, but Morning Anthem
        and Rare Gem (high signals) should remain.
        """
        config = _make_playlist_config(love_min=20.0, top=10)
        result = build_playlist(full_db, config)
        names = {t.track_name for t in result}
        assert "Morning Anthem" in names
        # Every kept track must clear the threshold
        for t in result:
            assert t.love_score >= 20.0

    def test_top_truncates(self, full_db):
        config = _make_playlist_config(top=2)
        result = build_playlist(full_db, config)
        assert len(result) == 2

    def test_unknown_mode_raises(self, full_db):
        config = _make_playlist_config(mode="not a real mode")
        with pytest.raises(ModeNotFoundError):
            build_playlist(full_db, config)

    def test_mode_plus_love_min_combined(self, full_db):
        """Both filters apply: tracks must be in the mode AND clear love_min."""
        config = _make_playlist_config(
            mode="weekday morning", love_min=1.0, top=10,
        )
        result = build_playlist(full_db, config)
        names = {t.track_name for t in result}
        # Falling Out is primary in ctx 1 but has bad love score — should drop
        # Morning Anthem and Rare Gem should remain (high love + primary in ctx 1)
        assert "Morning Anthem" in names
        for t in result:
            assert t.love_score >= 1.0


# ---------------------------------------------------------------------------
# Output formats
# ---------------------------------------------------------------------------
class TestOutputFormats:

    def test_table_empty_message(self, capsys):
        print_table([], mode="weekday morning")
        captured = capsys.readouterr()
        assert EMPTY_MSG in captured.out

    def test_table_includes_track_data(self, full_db, capsys):
        config = _make_playlist_config(mode="late night", top=5)
        tracks = build_playlist(full_db, config)
        print_table(tracks, mode="late night")
        captured = capsys.readouterr()
        assert "Night Owl" in captured.out
        assert "late night" in captured.out
        assert "Test Artist" in captured.out

    def test_json_parseable_and_includes_mode(self, full_db, capsys):
        config = _make_playlist_config(mode="late night", top=5)
        tracks = build_playlist(full_db, config)
        print_json(tracks, mode="late night")
        captured = capsys.readouterr()
        parsed = json.loads(captured.out)
        assert parsed["mode"] == "late night"
        assert parsed["track_count"] == 1
        assert parsed["tracks"][0]["track_name"] == "Night Owl"
        assert parsed["tracks"][0]["spotify_track_uri"] == "spotify:track:night"
        assert "love_score" in parsed["tracks"][0]

    def test_json_empty_result_still_valid_json(self, capsys):
        print_json([], mode="anything")
        captured = capsys.readouterr()
        parsed = json.loads(captured.out)
        assert parsed["track_count"] == 0
        assert parsed["tracks"] == []

    def test_uris_one_per_line(self, full_db, capsys):
        config = _make_playlist_config(top=10)
        tracks = build_playlist(full_db, config)
        print_uris(tracks, mode=None)
        captured = capsys.readouterr()
        lines = [l for l in captured.out.split("\n") if l]
        assert len(lines) == len(tracks)
        for line in lines:
            assert line.startswith("spotify:track:")

    def test_uris_empty_writes_to_stderr_not_stdout(self, capsys):
        """`--format uris > file` should produce an empty file on no match.

        The complaint goes to stderr so the redirected stdout stays clean.
        """
        print_uris([], mode="weekday morning")
        captured = capsys.readouterr()
        assert captured.out == ""             # stdout clean — file would be empty
        assert EMPTY_MSG in captured.err      # message visible in terminal


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
