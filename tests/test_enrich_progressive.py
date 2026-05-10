"""Tests for the progressive (tier-laddered) Spotify enrichment.

What's covered (no live API):
  - select_tier_candidates: window filter, min_plays bar, last_enriched_at
    exclusion, ORDER BY plays_in_window DESC.
  - plan_tonight: walks the ladder until daily_quota is spent; respects
    --max-age horizon; 0-quota / empty cases.
  - _format_dry_run: includes tier names, totals, and "all up to date" path.
  - _parse_max_age: '365d', '1095d', 'all', invalid input.
  - main(): --dry-run makes zero API calls; --max-age 'banana' returns code 3.
  - End-to-end: synthetic DB + mocked SpotifyClient runs a small batch and
    persists duration_ms.
  - LongPenaltyError mid-run -> exit code 2.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path
from unittest import mock

import pytest

from scripts import enrich_progressive as ep
from scripts.enrich_progressive import (
    DEFAULT_TIERS,
    Tier,
    TierPlan,
    _format_dry_run,
    _parse_max_age,
    main,
    plan_tonight,
    select_tier_candidates,
)
from scripts.spotify_client import LongPenaltyError


SCHEMA_PATH = Path(__file__).resolve().parent.parent / "sql" / "schema.sql"


# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------
def _new_db(tmp_path) -> sqlite3.Connection:
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path), isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(SCHEMA_PATH.read_text())
    return conn


def _seed_track_with_plays(
    conn: sqlite3.Connection,
    track_id: int,
    days_ago: int,
    n_plays: int,
    *,
    enriched: bool = False,
) -> None:
    """Insert one track + n plays at `days_ago` days in the past.

    Plays use sub-second offsets to avoid violating UNIQUE(ts, content_uri,
    ms_played). The track row optionally has duration_ms + last_enriched_at
    set when `enriched=True` (simulating already-enriched).
    """
    # Reuse a dummy ingestion run + album.
    conn.execute(
        "INSERT OR IGNORE INTO ingestion_runs (run_id, source, status) "
        "VALUES (1, 'test', 'completed')"
    )
    conn.execute(
        "INSERT OR IGNORE INTO albums (album_id, name, name_normalized) "
        "VALUES (1, 'A', 'a')"
    )
    duration = 240000 if enriched else None
    last_enriched = "datetime('now')" if enriched else "NULL"
    conn.execute(
        f"""
        INSERT INTO tracks (track_id, spotify_track_uri, name, album_id,
                            duration_ms, last_enriched_at)
        VALUES (?, ?, ?, 1, ?, {last_enriched})
        """,
        (track_id, f"spotify:track:t{track_id}", f"Track {track_id}", duration),
    )
    for i in range(n_plays):
        conn.execute(
            """
            INSERT INTO plays (ts, ms_played, content_type, content_uri,
                               track_id, source, ingestion_run_id)
            VALUES (datetime('now', ? || ' days'), ?, 'track', ?, ?, 'test', 1)
            """,
            (f"-{days_ago}", 200000 + i, f"spotify:track:t{track_id}", track_id),
        )


@pytest.fixture
def synth_db(tmp_path):
    """A DB seeded with a deliberate spread across all default tiers + extras."""
    conn = _new_db(tmp_path)
    # Tier 'recent' (0-30d, >=3 plays): track 1 qualifies, track 2 doesn't (2 plays).
    _seed_track_with_plays(conn, 1, days_ago=10, n_plays=5)
    _seed_track_with_plays(conn, 2, days_ago=10, n_plays=2)
    # Tier '30-90d' (>=3 plays): track 3 qualifies.
    _seed_track_with_plays(conn, 3, days_ago=60, n_plays=4)
    # Tier '90-365d' (>=5 plays): track 4 qualifies; track 5 doesn't (4 plays).
    _seed_track_with_plays(conn, 4, days_ago=180, n_plays=8)
    _seed_track_with_plays(conn, 5, days_ago=180, n_plays=4)
    # Tier '1-2y' (>=8 plays): track 6 qualifies.
    _seed_track_with_plays(conn, 6, days_ago=500, n_plays=10)
    # Already-enriched track in 'recent' window — must be filtered out.
    _seed_track_with_plays(conn, 7, days_ago=15, n_plays=20, enriched=True)
    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# select_tier_candidates
# ---------------------------------------------------------------------------
class TestSelectTierCandidates:

    def test_recent_tier_picks_engaged_unenriched_track(self, synth_db):
        rows = select_tier_candidates(synth_db, DEFAULT_TIERS[0])  # recent
        ids = [r["id"] for r in rows]
        assert ids == [1]   # 5 plays in 0-30d, not enriched

    def test_min_plays_filters_low_engagement(self, synth_db):
        # Track 2 has 2 plays in 0-30d window; tier needs >=3.
        rows = select_tier_candidates(synth_db, DEFAULT_TIERS[0])
        assert 2 not in [r["id"] for r in rows]

    def test_already_enriched_track_excluded(self, synth_db):
        # Track 7: 20 plays in window, but last_enriched_at IS NOT NULL.
        rows = select_tier_candidates(synth_db, DEFAULT_TIERS[0])
        assert 7 not in [r["id"] for r in rows]

    def test_30_90d_tier_isolates_correct_window(self, synth_db):
        rows = select_tier_candidates(synth_db, DEFAULT_TIERS[1])  # 30-90d
        ids = [r["id"] for r in rows]
        assert ids == [3]

    def test_higher_min_plays_for_older_tier(self, synth_db):
        # Track 5 has 4 plays at 180d; '90-365d' tier needs >=5.
        rows = select_tier_candidates(synth_db, DEFAULT_TIERS[2])  # 90-365d
        ids = [r["id"] for r in rows]
        assert ids == [4] and 5 not in ids

    def test_ordered_by_plays_in_window_desc(self, tmp_path):
        conn = _new_db(tmp_path)
        _seed_track_with_plays(conn, 100, days_ago=5, n_plays=3)
        _seed_track_with_plays(conn, 101, days_ago=5, n_plays=10)
        _seed_track_with_plays(conn, 102, days_ago=5, n_plays=6)
        rows = select_tier_candidates(conn, DEFAULT_TIERS[0])
        assert [r["id"] for r in rows] == [101, 102, 100]


# ---------------------------------------------------------------------------
# plan_tonight
# ---------------------------------------------------------------------------
class TestPlanTonight:

    def test_walks_ladder_within_quota(self, synth_db):
        plans, total = plan_tonight(synth_db, daily_quota=10, max_age_days=10**9)
        # Tracks 1,3,4,6 qualify across tiers. Quota 10 covers all 4.
        assert total == 4
        taken_per_tier = {p.tier.name: p.take for p in plans}
        assert taken_per_tier["recent"] == 1
        assert taken_per_tier["30-90d"] == 1
        assert taken_per_tier["90-365d"] == 1
        assert taken_per_tier["1-2y"] == 1
        assert taken_per_tier["2-3y"] == 0   # nothing in window

    def test_quota_exhausts_mid_ladder(self, synth_db):
        plans, total = plan_tonight(synth_db, daily_quota=2, max_age_days=10**9)
        assert total == 2
        # Walks top-down, so 'recent' first then '30-90d' — both have 1 each.
        assert plans[0].take == 1   # recent
        assert plans[1].take == 1   # 30-90d
        assert plans[2].take == 0   # 90-365d (quota gone)
        assert plans[2].candidates_total == 1   # but it knows there's work

    def test_max_age_skips_older_tiers(self, synth_db):
        # max_age=100d. Tracks 1 (recent) + 3 (30-90d) qualify -> total=2.
        # Tier '90-365d' overlaps the 100d boundary so it's clipped to 90-100d:
        # no track in synth_db has plays in that narrow window -> 0 candidates.
        # Tiers '1-2y' (min=365) + '2-3y' (min=730) are entirely beyond -> skipped.
        plans, total = plan_tonight(synth_db, daily_quota=100, max_age_days=100)
        assert total == 2
        by_name = {p.tier.name: p for p in plans}
        assert by_name["recent"].take == 1
        assert by_name["30-90d"].take == 1
        assert by_name["90-365d"].skipped_reason is None       # clipped, not skipped
        assert by_name["90-365d"].candidates_total == 0
        assert by_name["1-2y"].skipped_reason == "beyond --max-age"
        assert by_name["2-3y"].skipped_reason == "beyond --max-age"

    def test_max_age_clips_boundary_tier(self, tmp_path):
        # A track with plays at 80d should be enriched when max_age=100;
        # a track with plays at 200d in the same tier ('90-365d') should not.
        conn = _new_db(tmp_path)
        _seed_track_with_plays(conn, 10, days_ago=80, n_plays=5)    # in 30-90d
        _seed_track_with_plays(conn, 11, days_ago=95, n_plays=10)   # in 90-100d clipped slice
        _seed_track_with_plays(conn, 12, days_ago=200, n_plays=10)  # in 100-365d, beyond horizon
        plans, total = plan_tonight(conn, daily_quota=100, max_age_days=100)
        ids_taken = [r["id"] for p in plans for r in p.rows_taken]
        assert 10 in ids_taken
        assert 11 in ids_taken
        assert 12 not in ids_taken
        assert total == 2

    def test_zero_quota_returns_zero_planned(self, synth_db):
        plans, total = plan_tonight(synth_db, daily_quota=0, max_age_days=10**9)
        assert total == 0
        # Candidates still computed — useful for dry-run-with-zero-quota previews.
        assert any(p.candidates_total > 0 for p in plans)

    def test_empty_db_yields_empty_plan(self, tmp_path):
        conn = _new_db(tmp_path)
        plans, total = plan_tonight(conn, daily_quota=600, max_age_days=10**9)
        assert total == 0
        assert all(p.candidates_total == 0 for p in plans)


# ---------------------------------------------------------------------------
# _format_dry_run
# ---------------------------------------------------------------------------
class TestFormatDryRun:

    def test_lists_each_tier(self, synth_db):
        plans, total = plan_tonight(synth_db, daily_quota=10, max_age_days=10**9)
        out = _format_dry_run(plans, total, 10, 10**9, 35.0)
        for tier in DEFAULT_TIERS:
            assert tier.name in out

    def test_up_to_date_message_when_zero_planned(self, tmp_path):
        conn = _new_db(tmp_path)
        plans, total = plan_tonight(conn, daily_quota=100, max_age_days=365)
        out = _format_dry_run(plans, total, 100, 365, 35.0)
        assert "Nothing to do" in out

    def test_wall_clock_estimate_present_when_work_planned(self, synth_db):
        plans, total = plan_tonight(synth_db, daily_quota=10, max_age_days=10**9)
        out = _format_dry_run(plans, total, 10, 10**9, 35.0)
        assert "Estimated wall clock" in out


# ---------------------------------------------------------------------------
# _parse_max_age
# ---------------------------------------------------------------------------
class TestParseMaxAge:

    def test_accepts_days(self):
        assert _parse_max_age("365d") == 365
        assert _parse_max_age("1095d") == 1095

    def test_all_returns_huge_number(self):
        assert _parse_max_age("all") >= 10**8

    def test_invalid_raises(self):
        with pytest.raises(ValueError):
            _parse_max_age("banana")
        with pytest.raises(ValueError):
            _parse_max_age("365")
        with pytest.raises(ValueError):
            _parse_max_age("xd")


# ---------------------------------------------------------------------------
# main() integration
# ---------------------------------------------------------------------------
class TestMain:

    def test_dry_run_makes_zero_api_calls(self, synth_db, tmp_path, monkeypatch):
        """--dry-run must never instantiate SpotifyClient or hit the network."""
        db_path = tmp_path / "main_dry.db"
        # Clone synth_db rows into a real on-disk file (main() opens its own conn).
        target = sqlite3.connect(str(db_path))
        synth_db.backup(target)
        target.close()

        # Sentinel: if SpotifyClient is constructed, the test fails loudly.
        with mock.patch("scripts.enrich_progressive.SpotifyClient") as m:
            rc = main([
                "--db", str(db_path),
                "--dry-run",
                "--log-file", str(tmp_path / "dry.log"),
            ])
        assert rc == 0
        m.assert_not_called()

    def test_bad_max_age_returns_3(self, tmp_path):
        rc = main([
            "--max-age", "banana",
            "--log-file", str(tmp_path / "bad.log"),
        ])
        assert rc == 3

    def test_long_penalty_returns_2(self, synth_db, tmp_path):
        """A LongPenaltyError mid-run must surface as exit code 2."""
        db_path = tmp_path / "main_lp.db"
        target = sqlite3.connect(str(db_path))
        synth_db.backup(target)
        target.close()

        # Fake client whose get_tracks raises LongPenaltyError on first call.
        fake_client = mock.MagicMock()
        fake_client.get_tracks.side_effect = LongPenaltyError("Retry-After 300s")
        fake_client.stats = {"calls_total": 0}

        with mock.patch(
            "scripts.enrich_progressive.SpotifyClient", return_value=fake_client
        ):
            rc = main([
                "--db", str(db_path),
                "--daily-quota", "5",
                "--rate-interval", "0.0",
                "--log-file", str(tmp_path / "lp.log"),
            ])
        assert rc == 2

    def test_end_to_end_persists_duration_ms(self, synth_db, tmp_path):
        """Happy path: mocked client returns a track payload; DB row updates."""
        db_path = tmp_path / "main_ok.db"
        target = sqlite3.connect(str(db_path))
        synth_db.backup(target)
        target.close()

        # Build a payload that get_tracks([uri]) -> [payload] would return.
        def fake_get_tracks(uris):
            return [
                {
                    "name": f"Updated {uri}",
                    "duration_ms": 200000,
                    "explicit": False,
                    "popularity": 50,
                    "external_ids": {"isrc": "ISRC0001"},
                    "album": {
                        "uri": "spotify:album:fake-album",
                        "name": "Fake Album",
                        "release_date": "2020",
                    },
                    "artists": [{
                        "uri": "spotify:artist:fake-artist",
                        "name": "Fake Artist",
                    }],
                }
                for uri in uris
            ]
        fake_client = mock.MagicMock()
        fake_client.get_tracks.side_effect = fake_get_tracks
        fake_client.stats = {"calls_total": 1}

        with mock.patch(
            "scripts.enrich_progressive.SpotifyClient", return_value=fake_client
        ):
            rc = main([
                "--db", str(db_path),
                "--daily-quota", "1",   # only enrich top track
                "--rate-interval", "0.0",
                "--log-file", str(tmp_path / "ok.log"),
            ])
        assert rc == 0

        # Verify track 1 (highest plays in 'recent') got duration_ms.
        check = sqlite3.connect(str(db_path))
        check.row_factory = sqlite3.Row
        row = check.execute(
            "SELECT duration_ms, last_enriched_at FROM tracks WHERE track_id = 1"
        ).fetchone()
        check.close()
        assert row["duration_ms"] == 200000
        assert row["last_enriched_at"] is not None
