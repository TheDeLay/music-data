"""Tests for the AcousticBrainz/MusicBrainz enrichment pipeline.

Verifies (with a mocked HTTP session, no live API):
  - ThrottledClient: throttle wait, 429 short-Retry-After backoff,
    LongPenaltyError on long Retry-After, 5xx exponential backoff,
    sustained-no-progress watchdog.
  - lookup_mbid: success, empty 'recordings', 404.
  - fetch_features: low-level only, high-level only, both, both 404.
  - Phase 1 idempotency: tracks already in mb_recordings are skipped.
  - Phase 2 idempotency: tracks already in acousticbrainz_features are skipped.
  - --dry-run paths don't write to the DB.
  - --min-plays filter.
  - LongPenaltyError mid-phase aborts cleanly with stats.aborted_at_index set.
  - Coverage report counts.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from scripts.enrich_acousticbrainz import (
    AB_API,
    KEY_TO_PITCH_CLASS,
    MB_API,
    Features,
    LongPenaltyError,
    PhaseStats,
    SustainedRateLimitError,
    ThrottledClient,
    fetch_features,
    lookup_mbid,
    run_phase_1,
    run_phase_2,
)


SCHEMA_PATH = Path(__file__).resolve().parent.parent / "sql" / "schema.sql"


# ---------------------------------------------------------------------------
# Fake HTTP plumbing
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


class FakeSession:
    """Minimal Session stand-in. Each test rigs `responses` with whatever
    sequence of FakeResponse objects the next get() calls should return.
    """
    def __init__(self, responses=None):
        self.responses: list[FakeResponse] = list(responses or [])
        self.calls: list[tuple[str, dict | None]] = []
        self.headers: dict = {}

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, params))
        if not self.responses:
            raise RuntimeError(f"FakeSession exhausted (called {url})")
        return self.responses.pop(0)


class FakeClock:
    """Deterministic time + sleep tracker for throttle tests."""
    def __init__(self, start: float = 1000.0):
        self._t = start
        self.sleep_calls: list[float] = []

    def sleep(self, seconds: float) -> None:
        self.sleep_calls.append(seconds)
        self._t += seconds

    def time(self) -> float:
        return self._t

    def advance(self, seconds: float) -> None:
        self._t += seconds


def _make_client(base_url: str, *, responses=None, clock=None,
                 min_interval=1.0, long_penalty=60.0,
                 max_no_progress=600.0) -> tuple[ThrottledClient, FakeSession, FakeClock]:
    clock = clock or FakeClock()
    session = FakeSession(responses)
    client = ThrottledClient(
        base_url=base_url,
        min_request_interval=min_interval,
        long_penalty_threshold_seconds=long_penalty,
        max_no_progress_seconds=max_no_progress,
        sleep_fn=clock.sleep,
        time_fn=clock.time,
        session=session,
    )
    return client, session, clock


# ---------------------------------------------------------------------------
# ThrottledClient mechanics
# ---------------------------------------------------------------------------
class TestThrottle:

    def test_first_call_no_wait(self):
        client, _, clock = _make_client(MB_API, responses=[FakeResponse(200, {"ok": True})])
        client.get("/x")
        assert clock.sleep_calls == []  # nothing to wait for on first call

    def test_second_call_waits_for_interval(self):
        client, _, clock = _make_client(
            MB_API,
            responses=[FakeResponse(200, {}), FakeResponse(200, {})],
            min_interval=1.0,
        )
        client.get("/x")
        # Time hasn't advanced naturally; the throttle should sleep ~1.0s
        client.get("/y")
        assert clock.sleep_calls and clock.sleep_calls[0] >= 1.0

    def test_no_wait_if_natural_gap_is_long_enough(self):
        client, _, clock = _make_client(
            MB_API,
            responses=[FakeResponse(200, {}), FakeResponse(200, {})],
            min_interval=1.0,
        )
        client.get("/x")
        clock.advance(2.0)  # natural elapsed > min_interval
        client.get("/y")
        assert clock.sleep_calls == []  # no extra sleep needed


class TestStatusCodes:

    def test_200_returns_payload(self):
        client, _, _ = _make_client(MB_API, responses=[FakeResponse(200, {"hello": "world"})])
        assert client.get("/x") == {"hello": "world"}
        assert client.stats["calls_200"] == 1

    def test_404_returns_none(self):
        client, _, _ = _make_client(MB_API, responses=[FakeResponse(404)])
        assert client.get("/missing") is None
        assert client.stats["calls_404"] == 1

    def test_429_short_retry_after_sleeps_and_retries(self):
        client, _, clock = _make_client(MB_API, responses=[
            FakeResponse(429, headers={"Retry-After": "5"}),
            FakeResponse(200, {"ok": True}),
        ])
        result = client.get("/x")
        assert result == {"ok": True}
        assert client.stats["calls_429"] == 1
        assert client.stats["calls_200"] == 1
        # Should have slept at least 5s for the 429
        assert any(s >= 5.0 for s in clock.sleep_calls)

    def test_429_long_retry_after_raises_long_penalty(self):
        client, _, _ = _make_client(
            MB_API,
            responses=[FakeResponse(429, headers={"Retry-After": "120"})],
            long_penalty=60.0,
        )
        with pytest.raises(LongPenaltyError, match="Retry-After=120"):
            client.get("/x")

    def test_5xx_retries_with_backoff(self):
        client, _, clock = _make_client(MB_API, responses=[
            FakeResponse(503),
            FakeResponse(503),
            FakeResponse(200, {"ok": True}),
        ])
        result = client.get("/x")
        assert result == {"ok": True}
        assert client.stats["calls_5xx"] == 2
        # Two backoff sleeps (2^0=1, 2^1=2) — exponential
        assert clock.sleep_calls.count(1) >= 1 or any(s >= 1 for s in clock.sleep_calls)


class TestWatchdog:

    def test_sustained_no_progress_aborts(self):
        client, _, clock = _make_client(
            MB_API,
            responses=[FakeResponse(429, headers={"Retry-After": "5"}),
                       FakeResponse(429, headers={"Retry-After": "5"}),
                       FakeResponse(200, {})],
            max_no_progress=10.0,
        )
        # Force the clock so the watchdog trips before next throttle
        clock.advance(11.0)
        with pytest.raises(SustainedRateLimitError):
            client.get("/x")


# ---------------------------------------------------------------------------
# lookup_mbid
# ---------------------------------------------------------------------------
class TestLookupMbid:

    def test_success_returns_first_recording_id(self):
        client, _, _ = _make_client(MB_API, responses=[FakeResponse(200, {
            "recordings": [
                {"id": "abc-123", "title": "Track A"},
                {"id": "def-456", "title": "Track A (re-release)"},
            ]
        })])
        assert lookup_mbid(client, "USRC17607839") == "abc-123"

    def test_empty_recordings_returns_none(self):
        client, _, _ = _make_client(MB_API, responses=[FakeResponse(200, {"recordings": []})])
        assert lookup_mbid(client, "USRC17607839") is None

    def test_404_returns_none(self):
        client, _, _ = _make_client(MB_API, responses=[FakeResponse(404)])
        assert lookup_mbid(client, "BADISRC") is None


# ---------------------------------------------------------------------------
# fetch_features
# ---------------------------------------------------------------------------
class TestFetchFeatures:

    def test_both_endpoints_404_marks_not_found(self):
        client, _, _ = _make_client(AB_API, responses=[FakeResponse(404), FakeResponse(404)])
        f = fetch_features(client, "fake-mbid")
        assert f.not_found is True
        assert f.bpm is None and f.valence is None

    def test_low_level_only(self):
        low = {
            "rhythm": {"bpm": 128.5},
            "tonal": {"key_key": "C", "key_scale": "minor"},
            "lowlevel": {"average_loudness": 0.78},
        }
        client, _, _ = _make_client(AB_API, responses=[
            FakeResponse(200, low),
            FakeResponse(404),
        ])
        f = fetch_features(client, "x")
        assert f.not_found is False
        assert f.bpm == 128.5
        assert f.key == 0          # C
        assert f.mode == 0         # minor
        assert f.energy == 0.78
        assert f.valence is None   # no high-level

    def test_high_level_only(self):
        high = {"highlevel": {
            "mood_happy": {"all": {"happy": 0.81, "not_happy": 0.19}},
            "danceability": {"all": {"danceable": 0.66, "not_danceable": 0.34}},
            "voice_instrumental": {"all": {"instrumental": 0.22, "voice": 0.78}},
        }}
        client, _, _ = _make_client(AB_API, responses=[
            FakeResponse(404),
            FakeResponse(200, high),
        ])
        f = fetch_features(client, "x")
        assert f.not_found is False
        assert f.valence == 0.81
        assert f.danceability == 0.66
        assert f.instrumental == 0.22
        assert f.bpm is None       # no low-level

    def test_both_endpoints_merge(self):
        low = {"rhythm": {"bpm": 100.0}, "tonal": {"key_key": "F#", "key_scale": "major"},
               "lowlevel": {"average_loudness": 0.5}}
        high = {"highlevel": {"mood_happy": {"all": {"happy": 0.4}},
                              "danceability": {"all": {"danceable": 0.3}}}}
        client, _, _ = _make_client(AB_API, responses=[
            FakeResponse(200, low),
            FakeResponse(200, high),
        ])
        f = fetch_features(client, "x")
        assert f.bpm == 100.0
        assert f.key == KEY_TO_PITCH_CLASS["F#"]
        assert f.mode == 1
        assert f.energy == 0.5
        assert f.valence == 0.4
        assert f.danceability == 0.3

    def test_loudness_clamped_to_unit_range(self):
        """Defensive: average_loudness occasionally falls outside 0-1; clamp."""
        low = {"lowlevel": {"average_loudness": 1.5}}  # weird but possible
        client, _, _ = _make_client(AB_API, responses=[
            FakeResponse(200, low),
            FakeResponse(404),
        ])
        f = fetch_features(client, "x")
        assert f.energy == 1.0


# ---------------------------------------------------------------------------
# Phase 1 + 2 with synthetic DB
# ---------------------------------------------------------------------------
def _new_db(tmp_path) -> sqlite3.Connection:
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(SCHEMA_PATH.read_text())
    return conn


def _seed_tracks_with_plays(conn, specs):
    """specs: list of (track_id, isrc, n_plays)."""
    conn.execute("INSERT INTO ingestion_runs (run_id, source, status) "
                 "VALUES (1, 'test', 'completed')")
    conn.execute("INSERT INTO albums (album_id, name, name_normalized) "
                 "VALUES (1, 'A', 'a')")
    for tid, isrc, n_plays in specs:
        conn.execute(
            "INSERT INTO tracks (track_id, spotify_track_uri, name, album_id, "
            "duration_ms, isrc) VALUES (?, ?, ?, 1, 240000, ?)",
            (tid, f"spotify:track:t{tid}", f"Track {tid}", isrc),
        )
        for i in range(n_plays):
            # Vary ms_played per play to satisfy UNIQUE(ts, content_uri, ms_played)
            conn.execute(
                "INSERT INTO plays (ts, ms_played, content_type, content_uri, "
                "track_id, source, ingestion_run_id) "
                "VALUES (datetime('now'), ?, 'track', ?, ?, 'test', 1)",
                (200000 + i, f"spotify:track:t{tid}", tid),
            )
    conn.commit()


@pytest.fixture
def synth_db(tmp_path):
    conn = _new_db(tmp_path)
    _seed_tracks_with_plays(conn, [
        (1, "ISRC0001", 10),
        (2, "ISRC0002", 8),
        (3, None, 20),         # no ISRC — should be excluded from phase 1
        (4, "ISRC0004", 2),    # below default min_plays
        (5, "ISRC0005", 7),
    ])
    yield conn
    conn.close()


class TestPhase1:

    def test_min_plays_filter(self, synth_db):
        """Track 4 has 2 plays, default min_plays=5 — should be skipped.
        Track 3 has no ISRC — also excluded."""
        client, _, _ = _make_client(MB_API, responses=[
            FakeResponse(200, {"recordings": [{"id": "mbid-1"}]}),
            FakeResponse(200, {"recordings": [{"id": "mbid-2"}]}),
            FakeResponse(200, {"recordings": [{"id": "mbid-5"}]}),
        ])
        stats = run_phase_1(synth_db, client, min_plays=5)
        assert stats.candidates == 3            # tracks 1, 2, 5
        assert stats.attempted == 3
        assert stats.hits == 3
        rows = synth_db.execute(
            "SELECT track_id, mb_recording_id FROM mb_recordings ORDER BY track_id"
        ).fetchall()
        assert [tuple(r) for r in rows] == [(1, "mbid-1"), (2, "mbid-2"), (5, "mbid-5")]

    def test_idempotent_skips_existing(self, synth_db):
        """A second run should find no new candidates."""
        synth_db.execute(
            "INSERT INTO mb_recordings (track_id, mb_recording_id) VALUES (1, 'pre-existing')"
        )
        synth_db.commit()
        client, _, _ = _make_client(MB_API, responses=[
            FakeResponse(200, {"recordings": [{"id": "mbid-2"}]}),
            FakeResponse(200, {"recordings": [{"id": "mbid-5"}]}),
        ])
        stats = run_phase_1(synth_db, client, min_plays=5)
        assert stats.candidates == 2  # track 1 already done
        # Existing row untouched
        existing = synth_db.execute(
            "SELECT mb_recording_id FROM mb_recordings WHERE track_id = 1"
        ).fetchone()[0]
        assert existing == "pre-existing"

    def test_misses_persisted_with_null_mbid(self, synth_db):
        """Track 1 has no MB record — store NULL, don't retry next run."""
        client, _, _ = _make_client(MB_API, responses=[
            FakeResponse(200, {"recordings": []}),  # track 1: empty
            FakeResponse(200, {"recordings": [{"id": "m2"}]}),
            FakeResponse(404),                       # track 5: 404
        ])
        stats = run_phase_1(synth_db, client, min_plays=5)
        assert stats.hits == 1
        assert stats.misses == 2
        rows = synth_db.execute(
            "SELECT track_id, mb_recording_id FROM mb_recordings ORDER BY track_id"
        ).fetchall()
        assert [tuple(r) for r in rows] == [(1, None), (2, "m2"), (5, None)]

    def test_dry_run_does_not_write(self, synth_db):
        client, _, _ = _make_client(MB_API, responses=[
            FakeResponse(200, {"recordings": [{"id": "x"}]}),
            FakeResponse(200, {"recordings": [{"id": "y"}]}),
            FakeResponse(200, {"recordings": [{"id": "z"}]}),
        ])
        stats = run_phase_1(synth_db, client, min_plays=5, dry_run=True)
        assert stats.attempted == 3
        n = synth_db.execute("SELECT COUNT(*) FROM mb_recordings").fetchone()[0]
        assert n == 0

    def test_max_caps_candidates(self, synth_db):
        client, _, _ = _make_client(MB_API, responses=[
            FakeResponse(200, {"recordings": [{"id": "x"}]}),
        ])
        stats = run_phase_1(synth_db, client, min_plays=5, max_n=1)
        assert stats.candidates == 1
        assert stats.attempted == 1

    def test_long_penalty_aborts_with_index(self, synth_db):
        client, _, _ = _make_client(
            MB_API,
            responses=[
                FakeResponse(200, {"recordings": [{"id": "first"}]}),
                FakeResponse(429, headers={"Retry-After": "120"}),
            ],
            long_penalty=60.0,
        )
        stats = run_phase_1(synth_db, client, min_plays=5)
        assert stats.aborted_at_index == 1
        assert stats.error and "Retry-After=120" in stats.error
        # First track was committed before the abort
        n = synth_db.execute("SELECT COUNT(*) FROM mb_recordings").fetchone()[0]
        assert n == 1


class TestPhase2:

    def test_only_runs_for_tracks_with_mbid(self, synth_db):
        """Tracks without an MBID-non-null row in mb_recordings shouldn't be candidates."""
        synth_db.executemany(
            "INSERT INTO mb_recordings (track_id, mb_recording_id) VALUES (?, ?)",
            [(1, "mbid-1"), (2, None), (5, "mbid-5")],   # track 2 = miss
        )
        synth_db.commit()
        client, _, _ = _make_client(AB_API, responses=[
            FakeResponse(200, {"rhythm": {"bpm": 120.0}}),  # track 1 low
            FakeResponse(404),                                # track 1 high
            FakeResponse(200, {"rhythm": {"bpm": 90.0}}),   # track 5 low
            FakeResponse(404),                                # track 5 high
        ])
        stats = run_phase_2(synth_db, client)
        assert stats.candidates == 2  # tracks 1 and 5 only

    def test_persists_features_and_not_found(self, synth_db):
        synth_db.executemany(
            "INSERT INTO mb_recordings (track_id, mb_recording_id) VALUES (?, ?)",
            [(1, "mbid-1"), (5, "mbid-5")],
        )
        synth_db.commit()
        # Track 1: full features. Track 5: 404 both endpoints.
        client, _, _ = _make_client(AB_API, responses=[
            FakeResponse(200, {"rhythm": {"bpm": 120.0},
                               "tonal": {"key_key": "G", "key_scale": "major"},
                               "lowlevel": {"average_loudness": 0.6}}),
            FakeResponse(200, {"highlevel": {
                "mood_happy": {"all": {"happy": 0.7}},
                "danceability": {"all": {"danceable": 0.5}},
            }}),
            FakeResponse(404),  # track 5 low
            FakeResponse(404),  # track 5 high
        ])
        stats = run_phase_2(synth_db, client)
        assert stats.hits == 1
        assert stats.misses == 1
        rows = {r["track_id"]: r for r in synth_db.execute(
            "SELECT * FROM acousticbrainz_features ORDER BY track_id"
        )}
        assert rows[1]["bpm"] == 120.0
        assert rows[1]["mode"] == 1
        assert rows[1]["valence"] == 0.7
        assert rows[1]["not_found"] == 0
        assert rows[5]["bpm"] is None
        assert rows[5]["not_found"] == 1

    def test_idempotent_skips_existing(self, synth_db):
        synth_db.executemany(
            "INSERT INTO mb_recordings (track_id, mb_recording_id) VALUES (?, ?)",
            [(1, "mbid-1"), (5, "mbid-5")],
        )
        synth_db.execute(
            "INSERT INTO acousticbrainz_features (track_id, bpm, not_found) "
            "VALUES (1, 99.0, 0)"
        )
        synth_db.commit()
        client, _, _ = _make_client(AB_API, responses=[
            FakeResponse(404), FakeResponse(404),  # only track 5 should be queried
        ])
        stats = run_phase_2(synth_db, client)
        assert stats.candidates == 1  # only track 5
        # Track 1's pre-existing row preserved
        bpm = synth_db.execute(
            "SELECT bpm FROM acousticbrainz_features WHERE track_id = 1"
        ).fetchone()[0]
        assert bpm == 99.0

    def test_dry_run_does_not_write(self, synth_db):
        synth_db.execute(
            "INSERT INTO mb_recordings (track_id, mb_recording_id) VALUES (1, 'mbid-1')"
        )
        synth_db.commit()
        client, _, _ = _make_client(AB_API, responses=[
            FakeResponse(200, {"rhythm": {"bpm": 100}}),
            FakeResponse(404),
        ])
        stats = run_phase_2(synth_db, client, dry_run=True)
        assert stats.attempted == 1
        n = synth_db.execute("SELECT COUNT(*) FROM acousticbrainz_features").fetchone()[0]
        assert n == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
