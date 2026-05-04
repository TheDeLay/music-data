"""Tests for the mode-clustering engine.

Covers:
  - Cyclical feature extraction (sin/cos encoding + weekend flag)
  - K-means determinism under fixed seed
  - K-means recovers known cluster structure
  - Silhouette picks the correct k on seeded data
  - Track affinity computation (sums to 1, min-plays filter)
  - Primary-mode marking respects threshold
  - End-to-end on a synthetic in-memory DB
  - has_user_labels detects existing labeled clusters
"""
from __future__ import annotations

import sqlite3

import numpy as np
import pytest

from scripts.cluster_modes import (
    ClusterConfig,
    compute_track_affinity,
    describe_cluster,
    features_from_hours,
    has_user_labels,
    kmeans,
    pick_best_k,
    run_clustering,
    silhouette_score,
    write_results,
)
from scripts.db import init_schema


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _seed_three_clusters(seed: int = 0, n_per: int = 200) -> tuple[np.ndarray, np.ndarray]:
    """Synthetic plays in three obvious clusters.

    A: weekday morning  (hour 6-9, dow 1-5)
    B: weekday evening  (hour 18-22, dow 1-5)
    C: weekend afternoon (hour 12-17, dow 0/6)
    """
    rng = np.random.default_rng(seed)
    weekdays = np.array([1, 2, 3, 4, 5])
    weekend = np.array([0, 6])
    hA = rng.integers(6, 10, size=n_per); dA = rng.choice(weekdays, size=n_per)
    hB = rng.integers(18, 23, size=n_per); dB = rng.choice(weekdays, size=n_per)
    hC = rng.integers(12, 18, size=n_per); dC = rng.choice(weekend, size=n_per)
    hours = np.concatenate([hA, hB, hC])
    dows = np.concatenate([dA, dB, dC])
    return hours, dows


def _make_synth_db() -> sqlite3.Connection:
    """In-memory DB matching production connect() semantics.

    Production uses isolation_level=None (autocommit) so transaction() can
    issue explicit BEGIN/COMMIT. Tests must mirror this or transaction()
    breaks with "cannot start a transaction within a transaction".
    """
    conn = sqlite3.connect(":memory:", isolation_level=None)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.row_factory = sqlite3.Row
    init_schema(conn)
    return conn


def _insert_track(conn: sqlite3.Connection, track_id: int, name: str) -> None:
    conn.execute(
        "INSERT INTO tracks (track_id, spotify_track_uri, name) VALUES (?, ?, ?)",
        (track_id, f"spotify:track:test{track_id}", name),
    )


def _insert_play(conn: sqlite3.Connection, track_id: int, ts: str) -> None:
    conn.execute(
        """
        INSERT INTO plays (track_id, content_type, ts, ms_played)
        VALUES (?, 'track', ?, 60000)
        """,
        (track_id, ts),
    )


# ---------------------------------------------------------------------------
# Feature extraction
# ---------------------------------------------------------------------------
class TestFeatures:
    def test_shape(self):
        hours = np.array([0, 6, 12, 18])
        dows = np.array([0, 1, 2, 6])
        X = features_from_hours(hours, dows)
        assert X.shape == (4, 3)

    def test_cyclical_encoding_wraps(self):
        # hour 0 and hour 24 (=0) should produce identical features.
        # hour 23 and hour 1 should be roughly equidistant from hour 0.
        X = features_from_hours(np.array([0, 23, 1]), np.array([1, 1, 1]))
        d_23_to_0 = np.linalg.norm(X[1] - X[0])
        d_1_to_0 = np.linalg.norm(X[2] - X[0])
        assert pytest.approx(d_23_to_0, rel=1e-6) == d_1_to_0

    def test_weekend_flag(self):
        X = features_from_hours(np.array([12, 12, 12]), np.array([0, 3, 6]))  # Sun, Wed, Sat
        assert X[0, 2] == 1.0
        assert X[1, 2] == 0.0
        assert X[2, 2] == 1.0


# ---------------------------------------------------------------------------
# K-means
# ---------------------------------------------------------------------------
class TestKmeans:
    def test_deterministic_with_seed(self):
        hours, dows = _seed_three_clusters()
        X = features_from_hours(hours, dows)
        labels1, centers1, _ = kmeans(X, 3, n_init=5, max_iter=50, seed=42)
        labels2, centers2, _ = kmeans(X, 3, n_init=5, max_iter=50, seed=42)
        assert np.array_equal(labels1, labels2)
        assert np.allclose(centers1, centers2)

    def test_recovers_three_clusters(self):
        # If we seeded 3 clean clusters, k-means with k=3 should put each
        # synthetic group into its own cluster (allowing for label permutation).
        hours, dows = _seed_three_clusters()
        X = features_from_hours(hours, dows)
        labels, _, _ = kmeans(X, 3, n_init=10, max_iter=100, seed=42)
        # Each true cluster occupies a contiguous span of 200 rows.
        # All rows in a span should share the same predicted label.
        for span_start in (0, 200, 400):
            span = labels[span_start:span_start + 200]
            assert np.unique(span).size == 1, (
                f"Cluster starting at {span_start} got split: {np.bincount(span)}"
            )

    def test_inertia_is_finite(self):
        hours, dows = _seed_three_clusters()
        X = features_from_hours(hours, dows)
        _, _, inertia = kmeans(X, 3, n_init=5, max_iter=50, seed=42)
        assert np.isfinite(inertia) and inertia >= 0


# ---------------------------------------------------------------------------
# Silhouette
# ---------------------------------------------------------------------------
class TestSilhouette:
    def test_picks_correct_k(self):
        hours, dows = _seed_three_clusters()
        X = features_from_hours(hours, dows)
        config = ClusterConfig(min_modes=2, max_modes=5, seed=42)
        best_k, _, _, score, all_scores = pick_best_k(X, config)
        assert best_k == 3, f"Expected k=3, got k={best_k}. Scores: {all_scores}"
        assert all_scores[3] > all_scores[2]
        assert score > 0.5

    def test_single_cluster_returns_minus_one(self):
        # All points in one cluster — silhouette is undefined.
        X = np.zeros((10, 3))
        labels = np.zeros(10, dtype=int)
        s = silhouette_score(X, labels, sample_size=10, seed=42)
        assert s == -1.0


# ---------------------------------------------------------------------------
# Track affinity
# ---------------------------------------------------------------------------
class TestAffinity:
    def test_sums_to_one(self):
        track_ids = np.array([1, 1, 1, 2, 2, 2, 2])
        play_labels = np.array([0, 0, 1, 0, 1, 1, 2])
        aff, totals = compute_track_affinity(track_ids, play_labels, k=3, min_plays=1)
        assert pytest.approx(aff[1].sum(), abs=1e-9) == 1.0
        assert pytest.approx(aff[2].sum(), abs=1e-9) == 1.0
        assert totals[1] == 3
        assert totals[2] == 4

    def test_min_plays_filter(self):
        # Track 3 has only 2 plays; should be excluded with min_plays=3.
        track_ids = np.array([1, 1, 1, 2, 2, 2, 3, 3])
        play_labels = np.array([0, 0, 1, 0, 1, 2, 0, 1])
        aff, totals = compute_track_affinity(track_ids, play_labels, k=3, min_plays=3)
        assert 1 in aff and 2 in aff
        assert 3 not in aff
        assert 3 not in totals

    def test_known_distribution(self):
        # Track 1: 3 plays in cluster 0, 1 play in cluster 1 → [0.75, 0.25, 0.0]
        track_ids = np.array([1, 1, 1, 1])
        play_labels = np.array([0, 0, 0, 1])
        aff, _ = compute_track_affinity(track_ids, play_labels, k=3, min_plays=1)
        assert pytest.approx(aff[1][0], abs=1e-9) == 0.75
        assert pytest.approx(aff[1][1], abs=1e-9) == 0.25
        assert aff[1][2] == 0.0


# ---------------------------------------------------------------------------
# Primary marking via write_results
# ---------------------------------------------------------------------------
class TestPrimaryMarking:
    def test_threshold_is_inclusive(self):
        conn = _make_synth_db()
        _insert_track(conn, 1, "Above threshold")
        _insert_track(conn, 2, "Below threshold")
        from scripts.cluster_modes import ClusterResult
        result = ClusterResult(
            k=2,
            silhouette=0.5,
            centers=np.array([[1.0, 0.0, 0.0], [-1.0, 0.0, 1.0]]),
            play_labels=np.array([0, 0, 1, 0, 1]),
            play_count_per_cluster=np.array([3, 2]),
            track_affinity={
                1: np.array([0.50, 0.50]),  # tied — argmax picks 0; 0.50 >= 0.50 → primary
                2: np.array([0.49, 0.51]),  # 0.51 in cluster 1; 0.51 >= 0.50 → primary
            },
            track_total_plays={1: 10, 2: 10},
        )
        write_results(conn, result, primary_threshold=0.50)
        rows = conn.execute(
            "SELECT track_id, context_id, affinity, is_primary FROM track_context_affinity ORDER BY track_id, context_id"
        ).fetchall()
        # Track 1: tied 0.50/0.50 → cluster 0 wins argmax, 0.50 >= 0.50 → is_primary=1 on cluster 0
        # Track 2: 0.51 in cluster 1 → is_primary=1 on cluster 1
        track1 = [r for r in rows if r["track_id"] == 1]
        track2 = [r for r in rows if r["track_id"] == 2]
        assert {r["is_primary"] for r in track1} == {0, 1}
        assert sum(r["is_primary"] for r in track1) == 1
        assert sum(r["is_primary"] for r in track2) == 1

    def test_no_primary_below_threshold(self):
        conn = _make_synth_db()
        _insert_track(conn, 1, "Mood-fluid")
        from scripts.cluster_modes import ClusterResult
        result = ClusterResult(
            k=3,
            silhouette=0.4,
            centers=np.eye(3),
            play_labels=np.array([0, 1, 2, 0, 1, 2, 0, 1, 2]),
            play_count_per_cluster=np.array([3, 3, 3]),
            track_affinity={
                1: np.array([0.34, 0.33, 0.33]),  # max = 0.34 < 0.50
            },
            track_total_plays={1: 9},
        )
        write_results(conn, result, primary_threshold=0.50)
        rows = conn.execute(
            "SELECT is_primary FROM track_context_affinity WHERE track_id=1"
        ).fetchall()
        assert sum(r["is_primary"] for r in rows) == 0


# ---------------------------------------------------------------------------
# has_user_labels
# ---------------------------------------------------------------------------
class TestUserLabels:
    def test_no_labels_returns_false(self):
        conn = _make_synth_db()
        conn.execute("INSERT INTO listening_contexts (cluster_id) VALUES (0)")
        conn.execute("INSERT INTO listening_contexts (cluster_id) VALUES (1)")
        assert has_user_labels(conn) is False

    def test_any_label_returns_true(self):
        conn = _make_synth_db()
        conn.execute("INSERT INTO listening_contexts (cluster_id, user_label) VALUES (0, '')")
        conn.execute("INSERT INTO listening_contexts (cluster_id, user_label) VALUES (1, 'morning workout')")
        assert has_user_labels(conn) is True


# ---------------------------------------------------------------------------
# describe_cluster
# ---------------------------------------------------------------------------
class TestDescribe:
    def test_morning_centroid(self):
        # cos(2π * 8/24), sin(2π * 8/24), is_weekend=0
        angle = 2 * np.pi * 8 / 24
        centroid = np.array([np.cos(angle), np.sin(angle), 0.0])
        s = describe_cluster(centroid)
        assert "8.0:00" in s
        assert "0.0%" in s

    def test_weekend_centroid(self):
        angle = 2 * np.pi * 14 / 24
        centroid = np.array([np.cos(angle), np.sin(angle), 1.0])
        s = describe_cluster(centroid)
        assert "100.0%" in s


# ---------------------------------------------------------------------------
# End-to-end
# ---------------------------------------------------------------------------
class TestEndToEnd:
    def test_full_pipeline_on_synth_db(self):
        """Insert plays for 3 tracks across 3 time-of-day patterns; verify
        clustering finds 3 contexts, each track gets a primary mode, and
        rows make it into the DB."""
        conn = _make_synth_db()
        # Three tracks, each strongly biased to one time pattern.
        # Use UTC timestamps that map to the desired hours when read back as
        # localtime — but for the test we want determinism regardless of
        # system TZ. SQLite's strftime('%H', ts, 'localtime') returns
        # whatever the host TZ converts to. To dodge TZ-dependence, we'll
        # pre-compute features and bypass the SQL/TZ layer by calling
        # run_clustering's internals directly.
        rng = np.random.default_rng(0)
        # Track 1: 50 morning weekday plays
        # Track 2: 50 evening weekday plays
        # Track 3: 50 weekend afternoon plays
        for tid in (1, 2, 3):
            _insert_track(conn, tid, f"Track {tid}")
        # Build feature data manually to dodge TZ
        weekdays = np.array([1, 2, 3, 4, 5])
        weekend = np.array([0, 6])
        h1 = rng.integers(6, 10, size=50); d1 = rng.choice(weekdays, size=50)
        h2 = rng.integers(18, 23, size=50); d2 = rng.choice(weekdays, size=50)
        h3 = rng.integers(12, 18, size=50); d3 = rng.choice(weekend, size=50)
        hours = np.concatenate([h1, h2, h3])
        dows = np.concatenate([d1, d2, d3])
        track_ids = np.concatenate([np.full(50, 1), np.full(50, 2), np.full(50, 3)])

        X = features_from_hours(hours, dows)
        config = ClusterConfig(min_modes=2, max_modes=4, min_plays=5, seed=42)
        best_k, labels, centers, score, _ = pick_best_k(X, config)
        assert best_k == 3
        affinity, totals = compute_track_affinity(track_ids, labels, best_k, config.min_plays)
        assert set(affinity.keys()) == {1, 2, 3}
        # Each track should have an obvious primary cluster (>= 0.9 affinity).
        for tid, vec in affinity.items():
            assert vec.max() > 0.9, f"Track {tid} affinity vector too flat: {vec}"

        from scripts.cluster_modes import ClusterResult
        play_count_per_cluster = np.bincount(labels, minlength=best_k)
        result = ClusterResult(
            k=best_k, silhouette=score, centers=centers,
            play_labels=labels, play_count_per_cluster=play_count_per_cluster,
            track_affinity=affinity, track_total_plays=totals,
        )
        ctx_n, aff_n = write_results(conn, result, primary_threshold=0.50)
        assert ctx_n == 3
        # 3 tracks * 1 nonzero cluster each (since affinity is ~1.0 in one) = 3 rows minimum.
        # Could be more if any track has a small fraction in another cluster — fine.
        assert aff_n >= 3
        # Each track should have exactly one is_primary=1 row.
        rows = conn.execute(
            "SELECT track_id, COUNT(*) FROM track_context_affinity WHERE is_primary=1 GROUP BY track_id"
        ).fetchall()
        assert {r[0] for r in rows} == {1, 2, 3}
        for r in rows:
            assert r[1] == 1

    def test_writes_play_counts_to_contexts(self):
        conn = _make_synth_db()
        _insert_track(conn, 1, "T1")
        from scripts.cluster_modes import ClusterResult
        result = ClusterResult(
            k=2,
            silhouette=0.6,
            centers=np.array([[1.0, 0.0, 0.0], [0.0, 1.0, 1.0]]),
            play_labels=np.array([0, 0, 0, 1, 1]),
            play_count_per_cluster=np.array([3, 2]),
            track_affinity={1: np.array([0.6, 0.4])},
            track_total_plays={1: 5},
        )
        write_results(conn, result, primary_threshold=0.5)
        rows = conn.execute(
            "SELECT cluster_id, play_count FROM listening_contexts ORDER BY cluster_id"
        ).fetchall()
        assert [(r["cluster_id"], r["play_count"]) for r in rows] == [(0, 3), (1, 2)]
