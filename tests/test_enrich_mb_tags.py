"""Tests for MB tags + genres enrichment (Phase 1B).

Mocked HTTP throughout — no live MusicBrainz calls.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

from scripts.enrich_acousticbrainz import (
    LongPenaltyError,
    ThrottledClient,
)
from scripts.enrich_mb_tags import (
    GENRE_KEY,
    SENTINEL_KEY,
    SENTINEL_VALUE,
    SET_BY,
    TAG_KEY,
    PhaseStats,
    TagsResult,
    _candidates,
    _persist,
    fetch_tags,
    main,
    run,
)


SCHEMA_PATH = Path(__file__).resolve().parent.parent / "sql" / "schema.sql"


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------
@dataclass
class FakeResponse:
    status_code: int
    payload: Any = None
    headers: dict = field(default_factory=dict)

    def json(self):
        return self.payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


def _make_client(responses, *, base_url="https://musicbrainz.org/ws/2"):
    """Build a ThrottledClient whose session.get returns from a queue."""
    session = mock.MagicMock()
    session.headers = {}
    session.get.side_effect = responses
    clock = [1000.0]

    def fake_time():
        return clock[0]

    def fake_sleep(s):
        clock[0] += s

    client = ThrottledClient(
        base_url,
        min_request_interval=0,
        long_penalty_threshold_seconds=60.0,
        max_no_progress_seconds=600.0,
        sleep_fn=fake_sleep,
        time_fn=fake_time,
        session=session,
    )
    return client, session


# ---------------------------------------------------------------------------
# DB fixtures
# ---------------------------------------------------------------------------
def _new_db(tmp_path) -> sqlite3.Connection:
    db = sqlite3.connect(str(tmp_path / "test.db"), isolation_level=None)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA foreign_keys = ON")
    db.executescript(SCHEMA_PATH.read_text())
    return db


def _seed_track_with_mbid(conn, track_id, mbid):
    conn.execute(
        "INSERT OR IGNORE INTO ingestion_runs (run_id, source, status) "
        "VALUES (1, 'test', 'completed')"
    )
    conn.execute(
        "INSERT OR IGNORE INTO albums (album_id, name, name_normalized) "
        "VALUES (1, 'A', 'a')"
    )
    conn.execute(
        "INSERT INTO tracks (track_id, spotify_track_uri, name, album_id, "
        "duration_ms, isrc, last_enriched_at) "
        "VALUES (?, ?, ?, 1, 200000, ?, datetime('now'))",
        (track_id, f"spotify:track:t{track_id}", f"Track {track_id}", f"ISRC{track_id:04d}"),
    )
    if mbid is not None:
        conn.execute(
            "INSERT INTO mb_recordings (track_id, mb_recording_id) VALUES (?, ?)",
            (track_id, mbid),
        )


@pytest.fixture
def synth_db(tmp_path):
    """Three tracks: two with MBIDs, one without (excluded from candidates)."""
    conn = _new_db(tmp_path)
    _seed_track_with_mbid(conn, 1, "mbid-aaaa")
    _seed_track_with_mbid(conn, 2, "mbid-bbbb")
    _seed_track_with_mbid(conn, 3, None)   # MB phase 1 already failed; exclude
    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# fetch_tags parsing
# ---------------------------------------------------------------------------
class TestFetchTags:

    def test_extracts_tags_and_genres(self):
        client, _ = _make_client([
            FakeResponse(200, {
                "tags":   [{"name": "thrash metal", "count": 12},
                           {"name": "metal", "count": 8}],
                "genres": [{"name": "heavy metal", "count": 5}],
            }),
        ])
        r = fetch_tags(client, "mbid-x")
        assert r.not_found is False
        assert ("thrash metal", 12) in r.tags
        assert ("metal", 8) in r.tags
        assert ("heavy metal", 5) in r.genres

    def test_404_marks_not_found(self):
        client, _ = _make_client([FakeResponse(404)])
        r = fetch_tags(client, "mbid-missing")
        assert r.not_found is True
        assert r.tags == [] and r.genres == []

    def test_empty_arrays_are_valid_not_404(self):
        """200 with empty tags + genres is "exists, untagged" — not a miss."""
        client, _ = _make_client([
            FakeResponse(200, {"tags": [], "genres": []}),
        ])
        r = fetch_tags(client, "mbid-empty")
        assert r.not_found is False
        assert r.tags == [] and r.genres == []

    def test_missing_count_field_defaults_to_zero(self):
        client, _ = _make_client([
            FakeResponse(200, {"tags": [{"name": "indie"}], "genres": []}),
        ])
        r = fetch_tags(client, "mbid-x")
        assert r.tags == [("indie", 0)]

    def test_blank_name_skipped(self):
        client, _ = _make_client([
            FakeResponse(200, {
                "tags":   [{"name": "", "count": 5}, {"name": "rock", "count": 3}],
                "genres": [],
            }),
        ])
        r = fetch_tags(client, "mbid-x")
        assert r.tags == [("rock", 3)]


# ---------------------------------------------------------------------------
# _persist
# ---------------------------------------------------------------------------
class TestPersist:

    def test_writes_tags_genres_and_sentinel(self, synth_db):
        result = TagsResult(
            tags=[("thrash metal", 12)],
            genres=[("heavy metal", 5)],
            not_found=False,
        )
        n = _persist(synth_db, 1, result)
        assert n == 2  # one tag + one genre, not counting sentinel
        rows = synth_db.execute(
            "SELECT label_key, label_value, set_by FROM track_labels "
            "WHERE track_id = 1 ORDER BY label_key, label_value"
        ).fetchall()
        keys_values = [(r["label_key"], r["label_value"], r["set_by"]) for r in rows]
        assert (TAG_KEY, "thrash metal", SET_BY) in keys_values
        assert (GENRE_KEY, "heavy metal", SET_BY) in keys_values
        assert (SENTINEL_KEY, SENTINEL_VALUE, SET_BY) in keys_values

    def test_404_writes_only_sentinel(self, synth_db):
        result = TagsResult(tags=[], genres=[], not_found=True)
        n = _persist(synth_db, 1, result)
        assert n == 0
        rows = synth_db.execute(
            "SELECT label_key, note FROM track_labels WHERE track_id = 1"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0]["label_key"] == SENTINEL_KEY
        assert rows[0]["note"] == "404"

    def test_idempotent_on_double_persist(self, synth_db):
        result = TagsResult(
            tags=[("metal", 10)], genres=[("rock", 3)], not_found=False,
        )
        _persist(synth_db, 1, result)
        # Second call returns 0 new rows (INSERT OR IGNORE) and doesn't error
        n = _persist(synth_db, 1, result)
        assert n == 0
        # Still exactly 3 rows (1 tag + 1 genre + 1 sentinel)
        count = synth_db.execute(
            "SELECT COUNT(*) FROM track_labels WHERE track_id = 1"
        ).fetchone()[0]
        assert count == 3


# ---------------------------------------------------------------------------
# _candidates
# ---------------------------------------------------------------------------
class TestCandidates:

    def test_excludes_tracks_without_mbid(self, synth_db):
        cands = _candidates(synth_db)
        # Track 3 has no MBID row; track 1 + 2 do.
        ids = [tid for tid, _ in cands]
        assert ids == [1, 2]

    def test_excludes_tracks_already_fetched(self, synth_db):
        # Track 1 already has the sentinel — should be excluded.
        synth_db.execute(
            "INSERT INTO track_labels (track_id, label_key, label_value, set_by) "
            "VALUES (?, ?, ?, ?)",
            (1, SENTINEL_KEY, SENTINEL_VALUE, SET_BY),
        )
        cands = _candidates(synth_db)
        assert [tid for tid, _ in cands] == [2]

    def test_sentinel_from_different_set_by_does_not_block(self, synth_db):
        # Hypothetical: someone else writes to track_labels with set_by='manual'
        # and the same key. Our query is set_by='mb', so it should not match.
        synth_db.execute(
            "INSERT INTO track_labels (track_id, label_key, label_value, set_by) "
            "VALUES (?, ?, ?, ?)",
            (1, SENTINEL_KEY, SENTINEL_VALUE, "manual-typo"),
        )
        cands = _candidates(synth_db)
        assert 1 in [tid for tid, _ in cands]


# ---------------------------------------------------------------------------
# run() — full pipeline
# ---------------------------------------------------------------------------
class TestRun:

    def test_dry_run_no_writes(self, synth_db):
        client, _ = _make_client([
            FakeResponse(200, {"tags": [{"name": "metal", "count": 10}], "genres": []}),
            FakeResponse(200, {"tags": [{"name": "rock", "count": 5}], "genres": []}),
        ])
        stats = run(synth_db, client, dry_run=True)
        assert stats.attempted == 2
        assert stats.hits == 2
        assert stats.tag_rows_written == 0
        assert synth_db.execute(
            "SELECT COUNT(*) FROM track_labels"
        ).fetchone()[0] == 0

    def test_max_caps_candidates(self, synth_db):
        client, _ = _make_client([
            FakeResponse(200, {"tags": [{"name": "x", "count": 1}], "genres": []}),
        ])
        stats = run(synth_db, client, max_n=1)
        assert stats.candidates == 1
        assert stats.attempted == 1

    def test_long_penalty_aborts_at_index(self, synth_db):
        # First call succeeds; second call returns 429 with long Retry-After.
        client, _ = _make_client([
            FakeResponse(200, {"tags": [{"name": "metal", "count": 10}], "genres": []}),
            FakeResponse(429, headers={"Retry-After": "300"}),
        ])
        stats = run(synth_db, client, dry_run=False)
        assert stats.aborted_at_index == 1
        assert "300" in stats.error or "penalty" in stats.error.lower()
        # Track 1 should have been persisted before the abort
        rows = synth_db.execute(
            "SELECT COUNT(*) FROM track_labels WHERE track_id = 1"
        ).fetchone()[0]
        assert rows >= 2  # at least 1 tag + 1 sentinel

    def test_404_persists_sentinel_and_continues(self, synth_db):
        client, _ = _make_client([
            FakeResponse(404),
            FakeResponse(200, {"tags": [{"name": "metal", "count": 3}], "genres": []}),
        ])
        stats = run(synth_db, client)
        assert stats.attempted == 2
        assert stats.not_found == 1
        assert stats.hits == 1
        # Both tracks have sentinel rows so neither will be re-fetched.
        for tid in (1, 2):
            row = synth_db.execute(
                "SELECT note FROM track_labels "
                "WHERE track_id = ? AND label_key = ? AND set_by = ?",
                (tid, SENTINEL_KEY, SET_BY),
            ).fetchone()
            assert row is not None


# ---------------------------------------------------------------------------
# main() integration
# ---------------------------------------------------------------------------
class TestMain:

    def test_long_penalty_returns_2(self, tmp_path):
        # Build an isolated on-disk DB with one MBID candidate.
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
            "INSERT INTO tracks (track_id, spotify_track_uri, name, album_id, "
            "duration_ms, last_enriched_at) "
            "VALUES (1, 'spotify:track:t1', 'T1', 1, 200000, datetime('now'))"
        )
        conn.execute(
            "INSERT INTO mb_recordings (track_id, mb_recording_id) VALUES (1, 'm-1')"
        )
        conn.commit()
        conn.close()

        fake_client = mock.MagicMock()
        fake_client.get.side_effect = LongPenaltyError("Retry-After 300s")

        with mock.patch(
            "scripts.enrich_mb_tags.ThrottledClient", return_value=fake_client
        ):
            rc = main(["--db", str(db_path), "--rate-interval", "0.0"])

        assert rc == 2
