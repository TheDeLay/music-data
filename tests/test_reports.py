"""Tests for the one-shot reports script.

Each report is exercised against a synthetic DB seeded to deliberately
trigger or NOT trigger that report's filters. Pure SQL — no API.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from scripts.reports import (
    REPORTS,
    _report_backbutton,
    _report_deliberate,
    _report_forgotten,
    _report_obsessions,
    _report_skipped,
    _report_tags,
)


SCHEMA_PATH = Path(__file__).resolve().parent.parent / "sql" / "schema.sql"


# ---------------------------------------------------------------------------
# Fixture: a DB with hand-crafted rows that exercise each report's filter
# ---------------------------------------------------------------------------
@pytest.fixture
def report_db(tmp_path):
    """Seed v_track_engagement-equivalent data via direct INSERTs.

    Each track exists for a specific report's filter:
      1 — Forgotten Hit:   total=30, recent=0  -> forgotten
      2 — Currently Loved: total=10, recent=8  -> obsessions
      3 — Skip Champion:   total=15, recent=5, skip_count=4  -> skipped
      4 — Back-Btn Star:   total=20, backbutton_count=3      -> backbutton
      5 — Deliberate Pick: total=10, deliberate_quality=5    -> deliberate
      6 — Excluded Noise:  total=2 (below all min_plays bars) -> none
    """
    conn = sqlite3.connect(str(tmp_path / "reports.db"), isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(SCHEMA_PATH.read_text())

    conn.execute(
        "INSERT INTO ingestion_runs (run_id, source, status) VALUES (1, 'test', 'completed')"
    )
    conn.execute(
        "INSERT INTO albums (album_id, name, name_normalized) VALUES (1, 'A', 'a')"
    )
    conn.execute(
        "INSERT INTO artists (artist_id, name, name_normalized) "
        "VALUES (1, 'TestArtist', 'testartist')"
    )

    tracks = [
        (1, "spotify:track:t1", "Forgotten Hit"),
        (2, "spotify:track:t2", "Currently Loved"),
        (3, "spotify:track:t3", "Skip Champion"),
        (4, "spotify:track:t4", "Back-Btn Star"),
        (5, "spotify:track:t5", "Deliberate Pick"),
        (6, "spotify:track:t6", "Excluded Noise"),
    ]
    for tid, uri, name in tracks:
        conn.execute(
            "INSERT INTO tracks (track_id, spotify_track_uri, name, album_id, "
            "duration_ms, last_enriched_at) "
            "VALUES (?, ?, ?, 1, 240000, datetime('now'))",
            (tid, uri, name),
        )
        conn.execute(
            "INSERT INTO track_artists (track_id, artist_id, position) "
            "VALUES (?, 1, 0)", (tid,),
        )

    def _play(track_id, uri, days_ago, ms_played=230000, *,
              reason_start=None, reason_end=None, skipped=0, ms_jitter=0):
        # ms_played offset for UNIQUE constraint. `skipped` mirrors
        # Spotify's own boolean flag (the v_track_engagement view sums on it).
        conn.execute(
            "INSERT INTO plays (ts, ms_played, content_type, content_uri, "
            "track_id, reason_start, reason_end, skipped, source, ingestion_run_id) "
            "VALUES (datetime('now', ? || ' days'), ?, 'track', ?, ?, ?, ?, ?, 'test', 1)",
            (f"-{days_ago}", ms_played + ms_jitter, uri, track_id,
             reason_start, reason_end, skipped),
        )

    # Track 1: 30 OLD plays (200 days ago), 0 recent
    for i in range(30):
        _play(1, "spotify:track:t1", days_ago=200, ms_jitter=i)

    # Track 2: 2 old + 8 recent (>=30% ratio, >=5 recent)
    for i in range(2):
        _play(2, "spotify:track:t2", days_ago=200, ms_jitter=i)
    for i in range(8):
        _play(2, "spotify:track:t2", days_ago=10, ms_jitter=i)

    # Track 3: 10 old + 5 recent, 4 of recent flagged as skipped by Spotify
    for i in range(10):
        _play(3, "spotify:track:t3", days_ago=200, ms_jitter=i)
    for i in range(4):
        _play(3, "spotify:track:t3", days_ago=10,
              ms_played=20000, skipped=1, ms_jitter=i)
    _play(3, "spotify:track:t3", days_ago=11)   # 1 non-skip recent

    # Track 4: 20 plays, 3 of which are back-button (reason_end='backbtn')
    for i in range(17):
        _play(4, "spotify:track:t4", days_ago=100, ms_jitter=i)
    for i in range(3):
        _play(4, "spotify:track:t4", days_ago=99,
              reason_start="clickrow", reason_end="backbtn", ms_jitter=i)

    # Track 5: 10 plays, 5 deliberate-quality (clickrow start + completed)
    for i in range(5):
        _play(5, "spotify:track:t5", days_ago=50, ms_jitter=i)
    for i in range(5):
        _play(5, "spotify:track:t5", days_ago=51,
              reason_start="clickrow", reason_end="trackdone", ms_jitter=i)

    # Track 6: noise — 2 plays only
    for i in range(2):
        _play(6, "spotify:track:t6", days_ago=10, ms_jitter=i)

    # Tags for the tags report
    conn.execute(
        "INSERT INTO track_labels (track_id, label_key, label_value, set_by) "
        "VALUES (1, 'genre', 'rock', 'mb')"
    )
    conn.execute(
        "INSERT INTO artist_labels (artist_id, label_key, label_value, set_by) "
        "VALUES (1, 'tag', 'rock', 'lastfm')"
    )
    conn.execute(
        "INSERT INTO artist_labels (artist_id, label_key, label_value, set_by) "
        "VALUES (1, 'tag', 'classic rock', 'lastfm')"
    )

    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# Per-report tests
# ---------------------------------------------------------------------------
class TestForgotten:

    def test_surfaces_high_lifetime_zero_recent(self, report_db):
        rows = _report_forgotten(report_db, limit=20)
        names = [r["track_name"] for r in rows]
        assert "Forgotten Hit" in names

    def test_excludes_tracks_with_recent_plays(self, report_db):
        rows = _report_forgotten(report_db, limit=20)
        names = [r["track_name"] for r in rows]
        assert "Currently Loved" not in names
        assert "Excluded Noise" not in names    # also below total>=20 bar


class TestBackbutton:

    def test_surfaces_tracks_with_backbtn_count(self, report_db):
        rows = _report_backbutton(report_db, limit=20)
        names = [r["track_name"] for r in rows]
        assert "Back-Btn Star" in names

    def test_orders_by_backbtn_then_total(self, report_db):
        rows = _report_backbutton(report_db, limit=20)
        # Back-Btn Star has backbutton_count=3, no other track has any
        assert rows[0]["track_name"] == "Back-Btn Star"
        assert rows[0]["backbutton_count"] >= 1


class TestSkipped:

    def test_surfaces_recent_skip_offender(self, report_db):
        rows = _report_skipped(report_db, limit=20)
        names = [r["track_name"] for r in rows]
        assert "Skip Champion" in names

    def test_excludes_low_recent_play_count(self, report_db):
        # Forgotten Hit has 30 lifetime + 0 recent — recent_plays<5 bar excludes it
        rows = _report_skipped(report_db, limit=20)
        names = [r["track_name"] for r in rows]
        assert "Forgotten Hit" not in names


class TestDeliberate:

    def test_surfaces_high_deliberate_ratio(self, report_db):
        rows = _report_deliberate(report_db, limit=20)
        names = [r["track_name"] for r in rows]
        assert "Deliberate Pick" in names

    def test_orders_by_ratio(self, report_db):
        rows = _report_deliberate(report_db, limit=20)
        # Deliberate Pick has 5/10 = 50% ratio, the highest
        assert rows[0]["track_name"] == "Deliberate Pick"


class TestObsessions:

    def test_surfaces_high_recent_ratio(self, report_db):
        rows = _report_obsessions(report_db, limit=20)
        names = [r["track_name"] for r in rows]
        assert "Currently Loved" in names

    def test_excludes_below_threshold(self, report_db):
        rows = _report_obsessions(report_db, limit=20)
        names = [r["track_name"] for r in rows]
        # Forgotten Hit has 0 recent — fails recent>=5 bar
        assert "Forgotten Hit" not in names


class TestTags:

    def test_aggregates_by_tag(self, report_db):
        rows = _report_tags(report_db, limit=20)
        tags = {r["tag"]: r["n_tracks"] for r in rows}
        # 'rock' is on every track via the artist (LF tag)
        # Track 1 also has it via track-level MB
        assert "rock" in tags
        # 'classic rock' is artist-level only
        assert "classic rock" in tags

    def test_lists_distinct_sources(self, report_db):
        rows = _report_tags(report_db, limit=20)
        # Find the 'rock' row
        rock_row = next(r for r in rows if r["tag"] == "rock")
        sources = set(rock_row["sources"].split(","))
        # rock comes from BOTH mb (track 1) and lastfm (all tracks via artist)
        assert "mb" in sources
        assert "lastfm" in sources


# ---------------------------------------------------------------------------
# Registry sanity
# ---------------------------------------------------------------------------
class TestRegistry:

    def test_all_reports_have_columns_matching_widths_and_aligns(self):
        for name, r in REPORTS.items():
            n = len(r.columns)
            assert len(r.column_widths) == n, f"{name}: column_widths length mismatch"
            assert len(r.column_aligns) == n, f"{name}: column_aligns length mismatch"
            for a in r.column_aligns:
                assert a in ("l", "r"), f"{name}: bad align {a!r}"
