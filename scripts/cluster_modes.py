"""Mode clustering — derive listening contexts from play-time patterns.

Clusters all plays by (hour-of-day, weekend-vs-weekday) features, picks the
best k via silhouette score over a configurable range, and writes per-track
affinity to the database. The clusters are unnamed at this stage — run
scripts/label_modes.py afterward for the interactive labeling step.

The algorithm is hand-rolled k-means + silhouette on numpy. We avoid sklearn
to keep the public-repo install lightweight (numpy alone is ~30 MB; sklearn
+ scipy is ~500 MB).

Usage:
    python -m scripts.cluster_modes                          # cluster + write
    python -m scripts.cluster_modes --dry-run                # plan only
    python -m scripts.cluster_modes --max-modes 4            # cap k at 4
    python -m scripts.cluster_modes --primary-threshold 0.6  # stricter primary
    python -m scripts.cluster_modes --min-plays 10           # exclude rare tracks
    python -m scripts.cluster_modes --force                  # overwrite labels
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from dataclasses import dataclass

import numpy as np

from scripts.db import connect, transaction


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
@dataclass
class ClusterConfig:
    min_modes: int = 2
    max_modes: int = 5
    primary_threshold: float = 0.50
    min_plays: int = 5
    listener: str = "primary"   # Phase-0 stub; not yet enforced
    seed: int = 42
    silhouette_sample: int = 5000
    n_init: int = 10
    max_iter: int = 100
    force: bool = False


@dataclass
class ClusterResult:
    k: int
    silhouette: float
    centers: np.ndarray              # (k, d) feature-space centroids
    play_labels: np.ndarray          # (n_plays,) cluster id per play
    play_count_per_cluster: np.ndarray  # (k,)
    track_affinity: dict[int, np.ndarray]  # track_id -> (k,) affinity vector
    track_total_plays: dict[int, int]


# ---------------------------------------------------------------------------
# Feature extraction
# ---------------------------------------------------------------------------
def features_from_hours(hours: np.ndarray, dows: np.ndarray) -> np.ndarray:
    """Cyclical encoding of hour-of-day plus weekend flag.

    Returns shape (n, 3): [hour_cos, hour_sin, is_weekend].

    Cyclical encoding avoids the 23h/0h discontinuity that plain hour values
    would introduce — k-means treats midnight and 1 AM as neighbors only when
    the feature space wraps. is_weekend is a hard binary because Sat/Sun
    listening differs from weekdays in ways the hour alone doesn't capture.
    """
    h = hours.astype(np.float64)
    angles = 2.0 * np.pi * h / 24.0
    is_weekend = ((dows == 0) | (dows == 6)).astype(np.float64)
    return np.column_stack([np.cos(angles), np.sin(angles), is_weekend])


# ---------------------------------------------------------------------------
# K-means (hand-rolled)
# ---------------------------------------------------------------------------
def _pairwise_sq_dist(X: np.ndarray, Y: np.ndarray) -> np.ndarray:
    """Squared Euclidean distance matrix, shape (X.shape[0], Y.shape[0])."""
    xx = (X * X).sum(axis=1, keepdims=True)
    yy = (Y * Y).sum(axis=1, keepdims=True).T
    return np.maximum(xx + yy - 2.0 * X @ Y.T, 0.0)


def _kmeans_pp_init(X: np.ndarray, k: int, rng: np.random.Generator) -> np.ndarray:
    """k-means++ seeding — picks centers spread across the data."""
    n = X.shape[0]
    centers = np.empty((k, X.shape[1]), dtype=X.dtype)
    centers[0] = X[rng.integers(0, n)]
    closest_sq = _pairwise_sq_dist(X, centers[:1]).ravel()
    for i in range(1, k):
        total = closest_sq.sum()
        if total <= 0:
            centers[i] = X[rng.integers(0, n)]
        else:
            probs = closest_sq / total
            idx = rng.choice(n, p=probs)
            centers[i] = X[idx]
        d_new = _pairwise_sq_dist(X, centers[i:i + 1]).ravel()
        closest_sq = np.minimum(closest_sq, d_new)
    return centers


def _kmeans_one_run(X: np.ndarray, k: int, max_iter: int,
                    rng: np.random.Generator) -> tuple[np.ndarray, np.ndarray, float]:
    """Single k-means run. Returns (labels, centers, inertia)."""
    centers = _kmeans_pp_init(X, k, rng)
    labels = np.zeros(X.shape[0], dtype=np.int64)
    for _ in range(max_iter):
        d = _pairwise_sq_dist(X, centers)
        new_labels = d.argmin(axis=1)
        if np.array_equal(new_labels, labels):
            labels = new_labels
            break
        labels = new_labels
        new_centers = np.empty_like(centers)
        for j in range(k):
            mask = labels == j
            if mask.any():
                new_centers[j] = X[mask].mean(axis=0)
            else:
                # Empty cluster — re-seed from a random point.
                new_centers[j] = X[rng.integers(0, X.shape[0])]
        if np.allclose(new_centers, centers):
            centers = new_centers
            break
        centers = new_centers
    d = _pairwise_sq_dist(X, centers)
    labels = d.argmin(axis=1)
    inertia = d[np.arange(X.shape[0]), labels].sum()
    return labels, centers, inertia


def kmeans(X: np.ndarray, k: int, *, n_init: int, max_iter: int,
           seed: int) -> tuple[np.ndarray, np.ndarray, float]:
    """Run k-means n_init times, return the best result by inertia.

    Deterministic given the same seed: each restart uses a child Generator
    derived from a fixed-seed parent.
    """
    parent = np.random.default_rng(seed)
    best_inertia = np.inf
    best_labels: np.ndarray | None = None
    best_centers: np.ndarray | None = None
    for _ in range(n_init):
        # Spawn a child stream so each restart is independent and reproducible.
        child = np.random.default_rng(parent.integers(0, 2**31 - 1))
        labels, centers, inertia = _kmeans_one_run(X, k, max_iter, child)
        if inertia < best_inertia:
            best_inertia = inertia
            best_labels = labels
            best_centers = centers
    assert best_labels is not None and best_centers is not None
    return best_labels, best_centers, best_inertia


# ---------------------------------------------------------------------------
# Silhouette score (sample-based)
# ---------------------------------------------------------------------------
def silhouette_score(X: np.ndarray, labels: np.ndarray, *,
                     sample_size: int, seed: int) -> float:
    """Mean silhouette coefficient over a random sample.

    Full silhouette is O(n^2); we sample down to keep it tractable for the
    100k-play range. Returns -1.0 if there's only one populated cluster
    (silhouette is undefined there).
    """
    rng = np.random.default_rng(seed)
    n = X.shape[0]
    if n > sample_size:
        idx = rng.choice(n, sample_size, replace=False)
        Xs = X[idx]
        ls = labels[idx]
    else:
        Xs = X
        ls = labels
    unique = np.unique(ls)
    if unique.size < 2:
        return -1.0

    sq = _pairwise_sq_dist(Xs, Xs)
    dist = np.sqrt(sq)
    np.fill_diagonal(dist, 0.0)

    s = np.zeros(Xs.shape[0])
    for i in range(Xs.shape[0]):
        own_mask = ls == ls[i]
        own_mask[i] = False
        if own_mask.any():
            a_i = dist[i, own_mask].mean()
        else:
            a_i = 0.0
        b_i = np.inf
        for c in unique:
            if c == ls[i]:
                continue
            other_mask = ls == c
            if other_mask.any():
                b_i = min(b_i, dist[i, other_mask].mean())
        denom = max(a_i, b_i)
        s[i] = 0.0 if denom == 0 else (b_i - a_i) / denom
    return float(s.mean())


# ---------------------------------------------------------------------------
# k selection
# ---------------------------------------------------------------------------
def pick_best_k(X: np.ndarray, config: ClusterConfig) -> tuple[int, np.ndarray, np.ndarray, float, dict[int, float]]:
    """Sweep k in [min_modes, max_modes], pick the k with best silhouette.

    Returns (best_k, labels, centers, best_score, all_scores_by_k).
    """
    scores: dict[int, float] = {}
    best_k = config.min_modes
    best_score = -np.inf
    best_labels: np.ndarray | None = None
    best_centers: np.ndarray | None = None
    for k in range(config.min_modes, config.max_modes + 1):
        labels, centers, _ = kmeans(
            X, k,
            n_init=config.n_init,
            max_iter=config.max_iter,
            seed=config.seed,
        )
        score = silhouette_score(
            X, labels,
            sample_size=config.silhouette_sample,
            seed=config.seed,
        )
        scores[k] = score
        if score > best_score:
            best_score = score
            best_k = k
            best_labels = labels
            best_centers = centers
    assert best_labels is not None and best_centers is not None
    return best_k, best_labels, best_centers, best_score, scores


# ---------------------------------------------------------------------------
# Affinity computation
# ---------------------------------------------------------------------------
def compute_track_affinity(track_ids: np.ndarray, play_labels: np.ndarray,
                           k: int, min_plays: int) -> tuple[dict[int, np.ndarray], dict[int, int]]:
    """Per-track affinity vectors.

    For each track with at least min_plays, compute the fraction of its plays
    that fall in each cluster. Tracks with fewer plays are dropped.

    Returns:
      affinity[track_id] -> ndarray shape (k,) summing to 1.0
      total_plays[track_id] -> int
    """
    unique_tracks, inverse, counts = np.unique(track_ids, return_inverse=True, return_counts=True)
    affinity: dict[int, np.ndarray] = {}
    total_plays: dict[int, int] = {}
    for ti, tid in enumerate(unique_tracks):
        if counts[ti] < min_plays:
            continue
        mask = inverse == ti
        track_labels = play_labels[mask]
        bincount = np.bincount(track_labels, minlength=k).astype(np.float64)
        affinity[int(tid)] = bincount / bincount.sum()
        total_plays[int(tid)] = int(counts[ti])
    return affinity, total_plays


# ---------------------------------------------------------------------------
# DB I/O
# ---------------------------------------------------------------------------
def load_plays(conn: sqlite3.Connection) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Load plays as numpy arrays.

    Returns (track_ids, hours, dows) — each (n_plays,) int arrays. Hours and
    days-of-week are computed in SQLite via the system local timezone, so
    cluster boundaries match the user's felt experience of when they listen.
    """
    rows = conn.execute("""
        SELECT
            track_id,
            CAST(strftime('%H', ts, 'localtime') AS INTEGER) AS hour,
            CAST(strftime('%w', ts, 'localtime') AS INTEGER) AS dow
        FROM plays
        WHERE content_type = 'track'
          AND ts IS NOT NULL
          AND track_id IS NOT NULL
        ORDER BY play_id
    """).fetchall()
    if not rows:
        return np.empty(0, dtype=np.int64), np.empty(0, dtype=np.int64), np.empty(0, dtype=np.int64)
    track_ids = np.fromiter((r[0] for r in rows), dtype=np.int64, count=len(rows))
    hours = np.fromiter((r[1] for r in rows), dtype=np.int64, count=len(rows))
    dows = np.fromiter((r[2] for r in rows), dtype=np.int64, count=len(rows))
    return track_ids, hours, dows


def has_user_labels(conn: sqlite3.Connection) -> bool:
    row = conn.execute(
        "SELECT COUNT(*) FROM listening_contexts WHERE user_label != ''"
    ).fetchone()
    return (row[0] if row else 0) > 0


def write_results(conn: sqlite3.Connection, result: ClusterResult,
                  primary_threshold: float) -> tuple[int, int]:
    """Wipe and rewrite listening_contexts + track_context_affinity.

    Returns (contexts_written, affinity_rows_written).
    """
    with transaction(conn):
        conn.execute("DELETE FROM listening_contexts")
        # CASCADE handles track_context_affinity.
        cluster_to_context: dict[int, int] = {}
        for cid in range(result.k):
            cur = conn.execute(
                """
                INSERT INTO listening_contexts
                    (cluster_id, centroid_hour_cos, centroid_hour_sin,
                     centroid_is_weekend, play_count)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    cid,
                    float(result.centers[cid, 0]),
                    float(result.centers[cid, 1]),
                    float(result.centers[cid, 2]),
                    int(result.play_count_per_cluster[cid]),
                ),
            )
            cluster_to_context[cid] = cur.lastrowid

        rows: list[tuple[int, int, float, int]] = []
        for track_id, vec in result.track_affinity.items():
            primary_cluster = int(vec.argmax())
            primary_score = float(vec[primary_cluster])
            qualifies_primary = primary_score >= primary_threshold
            for cid in range(result.k):
                aff = float(vec[cid])
                if aff <= 0.0:
                    continue
                is_primary = 1 if (cid == primary_cluster and qualifies_primary) else 0
                rows.append((track_id, cluster_to_context[cid], aff, is_primary))
        if rows:
            conn.executemany(
                """
                INSERT INTO track_context_affinity
                    (track_id, context_id, affinity, is_primary)
                VALUES (?, ?, ?, ?)
                """,
                rows,
            )
    return result.k, len(rows)


# ---------------------------------------------------------------------------
# Cluster summary (for human display)
# ---------------------------------------------------------------------------
def describe_cluster(centroid: np.ndarray) -> str:
    """One-line human-readable summary of a centroid in feature space.

    centroid = [hour_cos, hour_sin, is_weekend].
    """
    hour_cos, hour_sin, is_weekend = centroid
    angle = np.arctan2(hour_sin, hour_cos)
    if angle < 0:
        angle += 2 * np.pi
    dominant_hour = (angle * 24.0 / (2 * np.pi)) % 24.0
    weekend_pct = max(0.0, min(1.0, float(is_weekend))) * 100.0
    return f"~{dominant_hour:4.1f}:00 local, weekend share {weekend_pct:4.1f}%"


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------
def run_clustering(conn: sqlite3.Connection, config: ClusterConfig) -> ClusterResult:
    track_ids, hours, dows = load_plays(conn)
    if track_ids.size == 0:
        raise SystemExit("No plays found in database — ingest data first.")
    if track_ids.size < config.min_modes * 10:
        raise SystemExit(
            f"Too few plays ({track_ids.size}) to support clustering with "
            f"min_modes={config.min_modes}. Ingest more data or lower --min-modes."
        )

    X = features_from_hours(hours, dows)
    best_k, labels, centers, score, all_scores = pick_best_k(X, config)

    play_count_per_cluster = np.bincount(labels, minlength=best_k)
    affinity, total_plays = compute_track_affinity(track_ids, labels, best_k, config.min_plays)

    print(f"Loaded {track_ids.size:,} plays; tested k={config.min_modes}..{config.max_modes}.")
    for k, s in sorted(all_scores.items()):
        marker = " <-- best" if k == best_k else ""
        print(f"  k={k}  silhouette={s:+.4f}{marker}")
    print(f"\nSelected k={best_k} (silhouette={score:+.4f}).")
    print(f"Clusters (centroid summaries):")
    for cid in range(best_k):
        n_plays = int(play_count_per_cluster[cid])
        pct = 100.0 * n_plays / track_ids.size
        print(f"  cluster {cid}: {n_plays:>6,} plays ({pct:5.1f}%) — {describe_cluster(centers[cid])}")
    print(f"\nTracks eligible for affinity (>= {config.min_plays} plays): {len(affinity):,}")
    primaries = sum(1 for v in affinity.values() if float(v.max()) >= config.primary_threshold)
    print(f"Tracks with primary mode (affinity >= {config.primary_threshold}): {primaries:,} "
          f"({100.0 * primaries / max(1, len(affinity)):.1f}%)")

    return ClusterResult(
        k=best_k,
        silhouette=score,
        centers=centers,
        play_labels=labels,
        play_count_per_cluster=play_count_per_cluster,
        track_affinity=affinity,
        track_total_plays=total_plays,
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Cluster plays into listening contexts.")
    parser.add_argument("--min-modes", type=int, default=2, help="Minimum k to consider")
    parser.add_argument("--max-modes", type=int, default=5, help="Maximum k to consider")
    parser.add_argument("--primary-threshold", type=float, default=0.50,
                        help="Affinity bar for is_primary=1 (0.0-1.0)")
    parser.add_argument("--min-plays", type=int, default=5,
                        help="Minimum plays per track to be eligible for affinity")
    parser.add_argument("--listener", type=str, default="primary",
                        help="Listener identity (Phase-0 stub; not yet enforced)")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    parser.add_argument("--silhouette-sample", type=int, default=5000,
                        help="Sample size for silhouette computation")
    parser.add_argument("--n-init", type=int, default=10, help="K-means random restarts")
    parser.add_argument("--max-iter", type=int, default=100, help="K-means iterations per restart")
    parser.add_argument("--dry-run", action="store_true", help="Plan only; do not write to DB")
    parser.add_argument("--force", action="store_true",
                        help="Overwrite existing clusters even if user labels exist")
    parser.add_argument("--db", type=str, default=None, help="Path to music.db")

    args = parser.parse_args(argv)
    if args.min_modes < 2:
        print("error: --min-modes must be >= 2", file=sys.stderr)
        return 2
    if args.max_modes < args.min_modes:
        print("error: --max-modes must be >= --min-modes", file=sys.stderr)
        return 2

    config = ClusterConfig(
        min_modes=args.min_modes,
        max_modes=args.max_modes,
        primary_threshold=args.primary_threshold,
        min_plays=args.min_plays,
        listener=args.listener,
        seed=args.seed,
        silhouette_sample=args.silhouette_sample,
        n_init=args.n_init,
        max_iter=args.max_iter,
        force=args.force,
    )

    conn = connect(args.db)
    try:
        if not args.dry_run and not args.force and has_user_labels(conn):
            print("error: existing listening_contexts have user labels. Re-run with "
                  "--force to overwrite (you'll lose the labels), or run --dry-run "
                  "to inspect first.", file=sys.stderr)
            return 2

        result = run_clustering(conn, config)

        if args.dry_run:
            print("\n--dry-run: skipping DB writes.")
            return 0

        ctx_n, aff_n = write_results(conn, result, config.primary_threshold)
        print(f"\nWrote {ctx_n} listening_contexts rows and {aff_n:,} track_context_affinity rows.")
        print("Next: run `python -m scripts.label_modes` to name the clusters.")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
