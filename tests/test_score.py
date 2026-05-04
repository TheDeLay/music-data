"""Tests for the love-score engine.

Verifies:
  - The updated formula (with deliberate-quality replacing variance bonus)
  - Skip-streak detection (consecutive skips counted correctly)
  - Skip forgiveness (≤2 consecutive skips = no penalty)
  - Back-button weight (back-button replays are the strongest signal)
  - Deliberate-quality weight (chose it + finished = intent signal)
  - Recency weighting (recent quality plays outweigh historical)
  - Integration test with synthetic DB
"""
from __future__ import annotations

import sqlite3
import pytest
from datetime import datetime, timedelta

from scripts.score import (
    ScoreConfig,
    TrackScore,
    compute_love_score,
    compute_skip_streak,
    score_tracks,
)
from scripts.db import init_schema


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def make_config(**overrides) -> ScoreConfig:
    return ScoreConfig(**overrides)


def make_track(**overrides) -> TrackScore:
    defaults = dict(
        track_id=1,
        spotify_track_uri="spotify:track:test1",
        track_name="Test Track",
        primary_artist_name="Test Artist",
        album_name="Test Album",
        release_year=2020,
        duration_ms=240000,
        total_plays=10,
        quality_plays=5,
        recent_quality=2,
        backbutton_count=0,
        deliberate_quality=0,
        recent_plays=3,
        skip_count=2,
        avg_pct_played=65.0,
        skip_streak=0,
    )
    defaults.update(overrides)
    return TrackScore(**defaults)


def make_play(ms_played: int, duration_ms: int = 240000, ts: str = "2026-04-01T12:00:00Z") -> dict:
    return {"ms_played": ms_played, "duration_ms": duration_ms, "ts": ts}


# ---------------------------------------------------------------------------
# Core formula tests
# ---------------------------------------------------------------------------
class TestFormula:
    """Verify the love-score formula with known inputs."""

    def test_basic_score(self):
        """Simple case: recent + backbutton + deliberate + lifetime - skip penalty."""
        config = make_config()
        track = make_track(
            quality_plays=20, recent_quality=5, backbutton_count=2,
            deliberate_quality=8, skip_streak=0,
        )
        score = compute_love_score(track, config)
        # 5*3 + 2*5 + 8*2 + 20*1 - 0 = 15 + 10 + 16 + 20 = 61
        assert score == 61.0

    def test_beloved_track(self):
        """High engagement across all signals."""
        config = make_config()
        track = make_track(
            total_plays=30, quality_plays=28, recent_quality=12,
            backbutton_count=4, deliberate_quality=10, skip_streak=0,
        )
        score = compute_love_score(track, config)
        # 12*3 + 4*5 + 10*2 + 28*1 = 36 + 20 + 20 + 28 = 104
        assert score == 104.0

    def test_falling_out_track(self):
        """High lifetime but recent skip streak and no deliberate plays."""
        config = make_config()
        track = make_track(
            total_plays=25, quality_plays=20, recent_quality=1,
            backbutton_count=0, deliberate_quality=0, skip_streak=4,
        )
        score = compute_love_score(track, config)
        # 1*3 + 0*5 + 0*2 + 20*1 - (4-2)*2 = 3 + 0 + 0 + 20 - 4 = 19
        assert score == 19.0

    def test_ranking_order(self):
        """Beloved > Passive-good > Falling-out."""
        config = make_config()
        beloved = make_track(quality_plays=28, recent_quality=12,
                             backbutton_count=4, deliberate_quality=10, skip_streak=0)
        passive = make_track(quality_plays=15, recent_quality=5,
                             backbutton_count=0, deliberate_quality=0, skip_streak=0)
        falling = make_track(quality_plays=20, recent_quality=1,
                             backbutton_count=0, deliberate_quality=0, skip_streak=4)

        scores = [
            compute_love_score(beloved, config),
            compute_love_score(passive, config),
            compute_love_score(falling, config),
        ]
        assert scores[0] > scores[1] > scores[2]


# ---------------------------------------------------------------------------
# Skip-streak detection
# ---------------------------------------------------------------------------
class TestSkipStreak:

    def test_no_plays(self):
        assert compute_skip_streak([], make_config()) == 0

    def test_all_quality(self):
        plays = [make_play(200000) for _ in range(5)]
        assert compute_skip_streak(plays, make_config()) == 0

    def test_all_skips(self):
        plays = [make_play(50000) for _ in range(10)]
        assert compute_skip_streak(plays, make_config()) == 10

    def test_streak_in_middle(self):
        plays = [
            make_play(200000),  # quality
            make_play(200000),  # quality
            make_play(50000),   # skip
            make_play(50000),   # skip
            make_play(50000),   # skip
            make_play(50000),   # skip
            make_play(200000),  # quality
        ]
        assert compute_skip_streak(plays, make_config()) == 4

    def test_streak_at_start(self):
        plays = [
            make_play(50000),   # skip (most recent)
            make_play(50000),   # skip
            make_play(50000),   # skip
            make_play(200000),  # quality
            make_play(200000),  # quality
        ]
        assert compute_skip_streak(plays, make_config()) == 3

    def test_window_limit(self):
        plays = [make_play(200000)] * 10 + [make_play(50000)] * 20
        config = make_config(streak_window=10)
        assert compute_skip_streak(plays, config) == 0


# ---------------------------------------------------------------------------
# Skip forgiveness
# ---------------------------------------------------------------------------
class TestSkipForgiveness:

    def test_no_penalty_under_forgiveness(self):
        config = make_config()
        track = make_track(quality_plays=10, recent_quality=5,
                           backbutton_count=0, deliberate_quality=0, skip_streak=2)
        score_with = compute_love_score(track, config)

        track_no_skip = make_track(quality_plays=10, recent_quality=5,
                                   backbutton_count=0, deliberate_quality=0, skip_streak=0)
        score_without = compute_love_score(track_no_skip, config)

        assert score_with == score_without

    def test_penalty_above_forgiveness(self):
        config = make_config()
        track = make_track(quality_plays=10, recent_quality=5,
                           backbutton_count=0, deliberate_quality=0, skip_streak=5)
        score = compute_love_score(track, config)
        # 5*3 + 0 + 0 + 10*1 - (5-2)*2 = 15 + 10 - 6 = 19
        assert score == 19.0


# ---------------------------------------------------------------------------
# Back-button is the strongest signal
# ---------------------------------------------------------------------------
class TestBackButton:

    def test_backbutton_outweighs_plays(self):
        config = make_config()
        track_with_bb = make_track(quality_plays=5, recent_quality=0,
                                   backbutton_count=2, deliberate_quality=0, skip_streak=0)
        track_more_plays = make_track(quality_plays=10, recent_quality=0,
                                     backbutton_count=0, deliberate_quality=0, skip_streak=0)

        score_bb = compute_love_score(track_with_bb, config)
        score_plays = compute_love_score(track_more_plays, config)
        # bb: 0 + 2*5 + 0 + 5*1 = 15
        # plays: 0 + 0 + 0 + 10*1 = 10
        assert score_bb > score_plays


# ---------------------------------------------------------------------------
# Deliberate-quality signal
# ---------------------------------------------------------------------------
class TestDeliberateQuality:

    def test_deliberate_adds_score(self):
        """Deliberate quality plays should boost the score."""
        config = make_config()
        track_with = make_track(quality_plays=10, recent_quality=0,
                                backbutton_count=0, deliberate_quality=5, skip_streak=0)
        track_without = make_track(quality_plays=10, recent_quality=0,
                                   backbutton_count=0, deliberate_quality=0, skip_streak=0)

        score_with = compute_love_score(track_with, config)
        score_without = compute_love_score(track_without, config)
        # Difference should be 5 * 2.0 = 10
        assert score_with - score_without == 10.0

    def test_deliberate_weight_configurable(self):
        """Custom weight should change the contribution."""
        config = make_config(deliberate_weight=4.0)
        track = make_track(quality_plays=10, recent_quality=0,
                           backbutton_count=0, deliberate_quality=5, skip_streak=0)
        score = compute_love_score(track, config)
        # 0 + 0 + 5*4 + 10*1 = 30
        assert score == 30.0

    def test_zero_deliberate_no_penalty(self):
        """Tracks with zero deliberate plays just don't get the bonus — no penalty."""
        config = make_config()
        track = make_track(quality_plays=10, recent_quality=3,
                           backbutton_count=0, deliberate_quality=0, skip_streak=0)
        score = compute_love_score(track, config)
        # 3*3 + 0 + 0 + 10*1 = 19
        assert score == 19.0

    def test_deliberate_between_lifetime_and_recent(self):
        """Deliberate (2x) should be stronger than lifetime (1x) but weaker than recent (3x)."""
        config = make_config()
        # 1 deliberate quality play = +2
        # 1 recent quality play = +3
        # 1 lifetime quality play = +1
        t1 = make_track(quality_plays=0, recent_quality=0, backbutton_count=0,
                        deliberate_quality=1, skip_streak=0)
        t2 = make_track(quality_plays=0, recent_quality=1, backbutton_count=0,
                        deliberate_quality=0, skip_streak=0)
        t3 = make_track(quality_plays=1, recent_quality=0, backbutton_count=0,
                        deliberate_quality=0, skip_streak=0)

        s1 = compute_love_score(t1, config)
        s2 = compute_love_score(t2, config)
        s3 = compute_love_score(t3, config)

        assert s2 > s1 > s3  # recent > deliberate > lifetime


# ---------------------------------------------------------------------------
# Recency weighting
# ---------------------------------------------------------------------------
class TestRecency:

    def test_recent_beats_historical(self):
        config = make_config()
        recent = make_track(quality_plays=10, recent_quality=8,
                            backbutton_count=0, deliberate_quality=0, skip_streak=0)
        historical = make_track(quality_plays=20, recent_quality=0,
                                backbutton_count=0, deliberate_quality=0, skip_streak=0)

        assert compute_love_score(recent, config) > compute_love_score(historical, config)


# ---------------------------------------------------------------------------
# Integration: end-to-end with a test database
# ---------------------------------------------------------------------------
class TestIntegration:

    @pytest.fixture
    def test_db(self, tmp_path):
        """Fresh test DB with synthetic data."""
        db_path = tmp_path / "test_music.db"
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")

        from pathlib import Path
        schema_path = Path(__file__).resolve().parent.parent / "sql" / "schema.sql"
        conn.executescript(schema_path.read_text())

        now = datetime.utcnow()
        recent = now - timedelta(days=30)
        old = now - timedelta(days=180)

        # Artist + Album + Run
        conn.execute("INSERT INTO artists (artist_id, name, name_normalized) VALUES (1, 'Test Artist', 'test artist')")
        conn.execute("INSERT INTO albums (album_id, name, name_normalized, release_year) VALUES (1, 'Test Album', 'test album', 2020)")
        conn.execute("INSERT INTO ingestion_runs (run_id, source, status) VALUES (1, 'test', 'completed')")

        # Track A: "Beloved" — high quality, deliberate plays
        conn.execute("INSERT INTO tracks (track_id, spotify_track_uri, name, album_id, duration_ms) VALUES (1, 'spotify:track:aaa', 'Beloved Track', 1, 240000)")
        conn.execute("INSERT INTO track_artists (track_id, artist_id, position) VALUES (1, 1, 0)")
        for i in range(15):
            ts = (old + timedelta(days=i)).strftime("%Y-%m-%dT%H:%M:%SZ")
            conn.execute("INSERT INTO plays (ts, ms_played, content_type, content_uri, track_id, reason_start, source, ingestion_run_id) VALUES (?, 220000, 'track', 'spotify:track:aaa', 1, 'trackdone', 'extended_dump', 1)", (ts,))
        for i in range(8):
            ts = (recent + timedelta(days=i)).strftime("%Y-%m-%dT%H:%M:%SZ")
            conn.execute("INSERT INTO plays (ts, ms_played, content_type, content_uri, track_id, reason_start, source, ingestion_run_id) VALUES (?, 230000, 'track', 'spotify:track:aaa', 1, 'clickrow', 'extended_dump', 1)", (ts,))

        # Track B: "Skipper" — lots of plays but mostly recent skips
        conn.execute("INSERT INTO tracks (track_id, spotify_track_uri, name, album_id, duration_ms) VALUES (2, 'spotify:track:bbb', 'Skipper Track', 1, 240000)")
        conn.execute("INSERT INTO track_artists (track_id, artist_id, position) VALUES (2, 1, 0)")
        for i in range(15):
            ts = (old + timedelta(days=i)).strftime("%Y-%m-%dT%H:%M:%SZ")
            conn.execute("INSERT INTO plays (ts, ms_played, content_type, content_uri, track_id, source, ingestion_run_id) VALUES (?, 200000, 'track', 'spotify:track:bbb', 2, 'extended_dump', 1)", (ts,))
        for i in range(10):
            ts = (recent + timedelta(days=i)).strftime("%Y-%m-%dT%H:%M:%SZ")
            conn.execute("INSERT INTO plays (ts, ms_played, content_type, content_uri, track_id, source, ingestion_run_id) VALUES (?, 30000, 'track', 'spotify:track:bbb', 2, 'extended_dump', 1)", (ts,))

        # Track C: "Back-button gold" — few plays, all back-button + clickrow
        conn.execute("INSERT INTO tracks (track_id, spotify_track_uri, name, album_id, duration_ms) VALUES (3, 'spotify:track:ccc', 'Back Button Gold', 1, 240000)")
        conn.execute("INSERT INTO track_artists (track_id, artist_id, position) VALUES (3, 1, 0)")
        for i in range(5):
            ts = (recent + timedelta(days=i)).strftime("%Y-%m-%dT%H:%M:%SZ")
            conn.execute("INSERT INTO plays (ts, ms_played, content_type, content_uri, track_id, reason_start, reason_end, source, ingestion_run_id) VALUES (?, 230000, 'track', 'spotify:track:ccc', 3, 'clickrow', 'backbtn', 'extended_dump', 1)", (ts,))

        conn.commit()
        yield conn
        conn.close()

    def test_beloved_beats_skipper(self, test_db):
        config = make_config()
        tracks = score_tracks(test_db, config)
        names = [t.track_name for t in tracks]
        assert names.index("Beloved Track") < names.index("Skipper Track")

    def test_backbutton_gold_ranks_well(self, test_db):
        config = make_config()
        tracks = score_tracks(test_db, config)
        bb_track = next(t for t in tracks if t.track_name == "Back Button Gold")
        assert bb_track.backbutton_count == 5
        assert bb_track.deliberate_quality == 5  # clickrow + finished
        assert bb_track.love_score > 0

    def test_beloved_has_deliberate_quality(self, test_db):
        """Beloved track's recent clickrow plays should count as deliberate quality."""
        config = make_config()
        tracks = score_tracks(test_db, config)
        beloved = next(t for t in tracks if t.track_name == "Beloved Track")
        assert beloved.deliberate_quality == 8  # the 8 clickrow + quality plays

    def test_skipper_penalized(self, test_db):
        config = make_config()
        tracks = score_tracks(test_db, config)
        skipper = next(t for t in tracks if t.track_name == "Skipper Track")
        assert skipper.skip_streak > 0

    def test_quality_threshold_override_propagates(self, test_db):
        """Raising --quality-threshold must actually raise the bar.

        Beloved Track's recent plays are 230000/240000 = 95.8% played.
        At threshold 0.80 they all count as deliberate quality (8 plays).
        At threshold 0.97 none of them clear the bar, so deliberate
        quality drops to 0. If the override doesn't propagate to the
        threshold-dependent columns, this test fails.
        """
        loose = score_tracks(test_db, make_config(quality_threshold=0.80))
        strict = score_tracks(test_db, make_config(quality_threshold=0.97))

        loose_beloved = next(t for t in loose if t.track_name == "Beloved Track")
        strict_beloved = next(t for t in strict if t.track_name == "Beloved Track")

        assert loose_beloved.deliberate_quality == 8
        assert strict_beloved.deliberate_quality == 0
        # quality_plays should also drop — old plays at 220000 = 91.7% no longer qualify
        assert strict_beloved.quality_plays < loose_beloved.quality_plays

    def test_recency_override_propagates(self, test_db):
        """Tightening --recency-days must shrink recent_quality."""
        wide = score_tracks(test_db, make_config(recency_days=90))
        tight = score_tracks(test_db, make_config(recency_days=7))

        wide_beloved = next(t for t in wide if t.track_name == "Beloved Track")
        tight_beloved = next(t for t in tight if t.track_name == "Beloved Track")

        # 8 recent plays span 8 days starting 30 days ago — none in last 7
        assert wide_beloved.recent_quality == 8
        assert tight_beloved.recent_quality == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
