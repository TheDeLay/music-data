"""Tests for Last.fm artist-tag enrichment.

Mocked LastfmClient throughout — no live Last.fm calls.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path
from unittest import mock

import pytest

from scripts.enrich_acousticbrainz import RateLimitError, LongPenaltyError
from scripts.enrich_lastfm_tags import (
    SENTINEL_KEY,
    SENTINEL_VALUE,
    SET_BY,
    TAG_KEY,
    PhaseStats,
    _candidates,
    _persist,
    main,
    run,
)
from scripts.lastfm_client import TagsResult


SCHEMA_PATH = Path(__file__).resolve().parent.parent / "sql" / "schema.sql"


# ---------------------------------------------------------------------------
# DB fixture
# ---------------------------------------------------------------------------
def _new_db(tmp_path) -> sqlite3.Connection:
    db = sqlite3.connect(str(tmp_path / "lf.db"), isolation_level=None)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA foreign_keys = ON")
    db.executescript(SCHEMA_PATH.read_text())
    return db


def _seed_artist_with_plays(conn, artist_id, name, n_plays):
    """Insert an artist + a track for them + n_plays primary-artist plays."""
    conn.execute(
        "INSERT OR IGNORE INTO ingestion_runs (run_id, source, status) "
        "VALUES (1, 'test', 'completed')"
    )
    conn.execute(
        "INSERT OR IGNORE INTO albums (album_id, name, name_normalized) "
        "VALUES (1, 'A', 'a')"
    )
    norm = name.lower()
    conn.execute(
        "INSERT INTO artists (artist_id, name, name_normalized) VALUES (?, ?, ?)",
        (artist_id, name, norm),
    )
    track_id = artist_id * 100   # unique per artist
    conn.execute(
        "INSERT INTO tracks (track_id, spotify_track_uri, name, album_id, duration_ms) "
        "VALUES (?, ?, ?, 1, 240000)",
        (track_id, f"spotify:track:t{track_id}", f"Track for {name}"),
    )
    conn.execute(
        "INSERT INTO track_artists (track_id, artist_id, position) VALUES (?, ?, 0)",
        (track_id, artist_id),
    )
    for i in range(n_plays):
        conn.execute(
            "INSERT INTO plays (ts, ms_played, content_type, content_uri, "
            "track_id, source, ingestion_run_id) "
            "VALUES (datetime('now'), ?, 'track', ?, ?, 'test', 1)",
            (200000 + i, f"spotify:track:t{track_id}", track_id),
        )


@pytest.fixture
def synth_db(tmp_path):
    """Three artists with varying play counts."""
    conn = _new_db(tmp_path)
    _seed_artist_with_plays(conn, 1, "Megafake", 20)    # qualifies at any min_plays
    _seed_artist_with_plays(conn, 2, "Popstar", 10)     # qualifies at min_plays<=10
    _seed_artist_with_plays(conn, 3, "Rare", 2)         # excluded at min_plays>=5
    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# _candidates
# ---------------------------------------------------------------------------
class TestCandidates:

    def test_filters_by_min_plays(self, synth_db):
        cands = _candidates(synth_db, min_plays=5)
        ids = [aid for aid, _ in cands]
        # Artist 3 has only 2 plays, below 5 — excluded
        assert sorted(ids) == [1, 2]

    def test_lowering_min_plays_admits_more(self, synth_db):
        cands = _candidates(synth_db, min_plays=1)
        ids = [aid for aid, _ in cands]
        assert sorted(ids) == [1, 2, 3]

    def test_excludes_already_fetched(self, synth_db):
        # Artist 1 already has the LF sentinel — should be excluded.
        synth_db.execute(
            "INSERT INTO artist_labels (artist_id, label_key, label_value, set_by) "
            "VALUES (?, ?, ?, ?)",
            (1, SENTINEL_KEY, SENTINEL_VALUE, SET_BY),
        )
        cands = _candidates(synth_db, min_plays=1)
        assert sorted(aid for aid, _ in cands) == [2, 3]

    def test_orders_by_play_count_desc(self, synth_db):
        cands = _candidates(synth_db, min_plays=1)
        # Order should be Megafake (20) > Popstar (10) > Rare (2)
        assert [aid for aid, _ in cands] == [1, 2, 3]

    def test_returns_artist_name_for_lf_lookup(self, synth_db):
        cands = _candidates(synth_db, min_plays=10)
        names = [name for _, name in cands]
        assert "Megafake" in names
        assert "Popstar" in names


# ---------------------------------------------------------------------------
# _persist
# ---------------------------------------------------------------------------
class TestPersist:

    def test_writes_tags_and_sentinel(self, synth_db):
        n = _persist(synth_db, 1, [("metal", 100), ("thrash", 80)], not_found=False)
        assert n == 2
        rows = synth_db.execute(
            "SELECT label_key, label_value, set_by, note FROM artist_labels "
            "WHERE artist_id = 1 ORDER BY label_key, label_value"
        ).fetchall()
        keys_values = [(r["label_key"], r["label_value"], r["set_by"]) for r in rows]
        assert (TAG_KEY, "metal", SET_BY) in keys_values
        assert (TAG_KEY, "thrash", SET_BY) in keys_values
        assert (SENTINEL_KEY, SENTINEL_VALUE, SET_BY) in keys_values

    def test_count_in_note(self, synth_db):
        _persist(synth_db, 1, [("metal", 100)], not_found=False)
        row = synth_db.execute(
            "SELECT note FROM artist_labels "
            "WHERE artist_id = 1 AND label_key = ? AND label_value = ?",
            (TAG_KEY, "metal"),
        ).fetchone()
        assert row["note"] == "count=100"

    def test_not_found_writes_only_sentinel(self, synth_db):
        n = _persist(synth_db, 1, [], not_found=True)
        assert n == 0
        rows = synth_db.execute(
            "SELECT label_key, note FROM artist_labels WHERE artist_id = 1"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0]["label_key"] == SENTINEL_KEY
        assert rows[0]["note"] == "404"

    def test_idempotent_double_persist(self, synth_db):
        _persist(synth_db, 1, [("rock", 50)], not_found=False)
        # Second call returns 0 new rows; doesn't error
        n = _persist(synth_db, 1, [("rock", 50)], not_found=False)
        assert n == 0
        count = synth_db.execute(
            "SELECT COUNT(*) FROM artist_labels WHERE artist_id = 1"
        ).fetchone()[0]
        # 1 tag + 1 sentinel
        assert count == 2


# ---------------------------------------------------------------------------
# run() pipeline
# ---------------------------------------------------------------------------
def _fake_client(tag_results):
    """Return a mock LastfmClient whose get_artist_top_tags yields tag_results in order.

    Each entry can be a TagsResult OR an Exception to raise.
    """
    client = mock.MagicMock()
    client.stats = {"calls_total": 0}
    client.get_artist_top_tags.side_effect = tag_results
    return client


class TestRun:

    def test_dry_run_no_writes(self, synth_db):
        client = _fake_client([
            TagsResult(tags=[("metal", 100)], not_found=False),
            TagsResult(tags=[("pop", 50)], not_found=False),
        ])
        stats = run(synth_db, client, min_plays=5, dry_run=True)
        assert stats.attempted == 2
        assert stats.hits == 2
        assert stats.tag_rows_written == 0
        assert synth_db.execute(
            "SELECT COUNT(*) FROM artist_labels"
        ).fetchone()[0] == 0

    def test_max_caps_candidates(self, synth_db):
        client = _fake_client([
            TagsResult(tags=[("x", 1)], not_found=False),
        ])
        stats = run(synth_db, client, min_plays=5, max_n=1)
        assert stats.candidates == 1
        assert stats.attempted == 1

    def test_long_penalty_aborts_at_index(self, synth_db):
        # First call OK, second raises long-penalty (RateLimitError subclass)
        client = _fake_client([
            TagsResult(tags=[("metal", 100)], not_found=False),
            LongPenaltyError("Retry-After 300s"),
        ])
        stats = run(synth_db, client, min_plays=5)
        assert stats.aborted_at_index == 1
        assert "300s" in stats.error or "penalty" in stats.error.lower()
        # Artist 1 should be persisted before abort
        rows_a1 = synth_db.execute(
            "SELECT COUNT(*) FROM artist_labels WHERE artist_id = 1"
        ).fetchone()[0]
        assert rows_a1 >= 2   # tag + sentinel

    def test_not_found_persists_sentinel_and_continues(self, synth_db):
        client = _fake_client([
            TagsResult(tags=[], not_found=True),
            TagsResult(tags=[("indie", 10)], not_found=False),
        ])
        stats = run(synth_db, client, min_plays=5)
        assert stats.attempted == 2
        assert stats.not_found == 1
        assert stats.hits == 1
        # Both artists should have a sentinel so they don't get re-tried
        for aid in (1, 2):
            row = synth_db.execute(
                "SELECT note FROM artist_labels "
                "WHERE artist_id = ? AND label_key = ? AND set_by = ?",
                (aid, SENTINEL_KEY, SET_BY),
            ).fetchone()
            assert row is not None

    def test_empty_tags_distinct_from_not_found(self, synth_db):
        # Artist exists in LF but has no tags — should count as 'empty', not 'not_found'
        client = _fake_client([
            TagsResult(tags=[], not_found=False),
            TagsResult(tags=[("rock", 5)], not_found=False),
        ])
        stats = run(synth_db, client, min_plays=5)
        assert stats.empty == 1
        assert stats.hits == 1
        assert stats.not_found == 0


# ---------------------------------------------------------------------------
# main() integration
# ---------------------------------------------------------------------------
class TestMain:

    def test_missing_api_key_returns_3(self, monkeypatch):
        monkeypatch.setenv("LASTFM_API_KEY", "")
        rc = main([])
        assert rc == 3

    def test_long_penalty_returns_2(self, tmp_path, monkeypatch):
        # Synthesize an isolated DB with one candidate
        db_path = tmp_path / "main.db"
        conn = sqlite3.connect(str(db_path))
        conn.executescript(SCHEMA_PATH.read_text())
        conn.execute(
            "INSERT INTO ingestion_runs (run_id, source, status) "
            "VALUES (1, 'test', 'completed')"
        )
        conn.execute(
            "INSERT INTO albums (album_id, name, name_normalized) VALUES (1, 'A', 'a')"
        )
        conn.execute(
            "INSERT INTO artists (artist_id, name, name_normalized) "
            "VALUES (1, 'Megafake', 'megafake')"
        )
        conn.execute(
            "INSERT INTO tracks (track_id, spotify_track_uri, name, album_id, duration_ms) "
            "VALUES (1, 'spotify:track:t1', 'T1', 1, 200000)"
        )
        conn.execute(
            "INSERT INTO track_artists (track_id, artist_id, position) VALUES (1, 1, 0)"
        )
        for i in range(10):
            conn.execute(
                "INSERT INTO plays (ts, ms_played, content_type, content_uri, "
                "track_id, source, ingestion_run_id) "
                "VALUES (datetime('now'), ?, 'track', ?, 1, 'test', 1)",
                (200000 + i, f"spotify:track:t1"),
            )
        conn.commit()
        conn.close()

        monkeypatch.setenv("LASTFM_API_KEY", "dummy-key")

        fake_client = mock.MagicMock()
        fake_client.get_artist_top_tags.side_effect = LongPenaltyError("Retry-After 300s")
        fake_client.stats = {"calls_total": 0}

        with mock.patch(
            "scripts.enrich_lastfm_tags.LastfmClient", return_value=fake_client
        ):
            rc = main(["--db", str(db_path), "--rate-interval", "0.2"])
        assert rc == 2
