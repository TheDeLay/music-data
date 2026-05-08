"""Two-phase audio-feature enrichment via MusicBrainz + AcousticBrainz.

Phase 1 — ISRC -> MBID lookup via MusicBrainz
    For each track with an ISRC and no row yet in mb_recordings, fetch the
    MusicBrainz Recording UUID. Misses (no recording for that ISRC) are
    persisted as a row with mb_recording_id=NULL so we don't retry.

Phase 2 — MBID -> audio features via AcousticBrainz
    For each track that has an MBID and no row yet in acousticbrainz_features,
    fetch low-level (BPM, key, mode, loudness) and high-level (mood_happy,
    danceability, voice_instrumental) features. 404 = not in AB's frozen
    dataset; persisted as a row with not_found=1.

Both APIs are open data, no auth required. Rate limits documented as
1 req/sec/IP for both — we default to 1.1s to be friendly. Identifiable
User-Agent is required by MusicBrainz and encouraged by AcousticBrainz.

AcousticBrainz stopped accepting new submissions in Feb 2022, so coverage
is good for pre-2022 recordings and sparse for newer releases.

Usage:
    # Smoke test: small batch, no writes
    python -m scripts.enrich_acousticbrainz --max 5 --dry-run

    # Phase 1 only (ISRC -> MBID)
    python -m scripts.enrich_acousticbrainz --phase 1 --min-plays 5

    # Phase 2 only (MBID -> features)
    python -m scripts.enrich_acousticbrainz --phase 2

    # Both phases, full library run
    python -m scripts.enrich_acousticbrainz --min-plays 5
"""
from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
import time
from dataclasses import dataclass, field
from typing import Optional

import requests

from scripts.db import connect


log = logging.getLogger(__name__)

DEFAULT_USER_AGENT = "music-data/0.3 (https://github.com/TheDeLay/music-data)"
DEFAULT_RATE_INTERVAL = 1.1
DEFAULT_LONG_PENALTY_THRESHOLD = 60.0
DEFAULT_MAX_NO_PROGRESS = 600.0

MB_API = "https://musicbrainz.org/ws/2"
AB_API = "https://acousticbrainz.org/api/v1"


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------
class RateLimitError(Exception):
    """Base — top-level run loop should catch and exit cleanly."""


class LongPenaltyError(RateLimitError):
    """Single 429 with Retry-After above the threshold. Hard-stop."""


class SustainedRateLimitError(RateLimitError):
    """No successful API response in max_no_progress_seconds — abort."""


# ---------------------------------------------------------------------------
# HTTP client
# ---------------------------------------------------------------------------
class ThrottledClient:
    """Polite GET client with per-instance throttle, 429 backoff, watchdog.

    Each instance has its own throttle clock — use one per API host so a
    cross-API run doesn't share a single 1 req/sec budget across two
    independent rate-limit buckets.
    """

    def __init__(
        self,
        base_url: str,
        user_agent: str = DEFAULT_USER_AGENT,
        min_request_interval: float = DEFAULT_RATE_INTERVAL,
        long_penalty_threshold_seconds: float = DEFAULT_LONG_PENALTY_THRESHOLD,
        max_no_progress_seconds: float = DEFAULT_MAX_NO_PROGRESS,
        sleep_fn=time.sleep,
        time_fn=time.time,
        session=None,
    ):
        self.base_url = base_url.rstrip("/")
        self._session = session if session is not None else requests.Session()
        self._session.headers["User-Agent"] = user_agent
        self._min_interval = float(min_request_interval)
        self._long_penalty_threshold = float(long_penalty_threshold_seconds)
        self._max_no_progress = float(max_no_progress_seconds)
        self._sleep = sleep_fn
        self._now = time_fn
        self._last_request_at = 0.0
        self._backoff_until = 0.0
        self._last_progress_at = self._now()
        self.stats = {
            "calls_total": 0,
            "calls_200": 0,
            "calls_404": 0,
            "calls_429": 0,
            "calls_5xx": 0,
        }

    def _throttle(self) -> None:
        now = self._now()
        elapsed = now - self._last_progress_at
        if self._max_no_progress > 0 and elapsed > self._max_no_progress:
            raise SustainedRateLimitError(
                f"No successful response in {elapsed:.0f}s "
                f"(threshold {self._max_no_progress:.0f}s) on {self.base_url}. "
                f"Aborting."
            )
        wait = max(
            self._min_interval - (now - self._last_request_at)
            if self._min_interval > 0 else 0,
            self._backoff_until - now,
        )
        if wait > 0:
            self._sleep(wait)

    def get(self, path: str, params: dict | None = None) -> Optional[dict]:
        """GET base_url+path. Returns dict on 200, None on 404, raises otherwise.

        Handles 429 with Retry-After (long → LongPenaltyError, short → sleep
        and retry up to 5 times). 5xx → exponential backoff retry.
        """
        url = path if path.startswith("http") else f"{self.base_url}{path}"
        for attempt in range(5):
            self._throttle()
            resp = self._session.get(url, params=params, timeout=30)
            self._last_request_at = self._now()
            self.stats["calls_total"] += 1

            if resp.status_code == 200:
                self._last_progress_at = self._last_request_at
                self.stats["calls_200"] += 1
                return resp.json()

            if resp.status_code == 404:
                self._last_progress_at = self._last_request_at
                self.stats["calls_404"] += 1
                return None

            if resp.status_code == 429:
                self.stats["calls_429"] += 1
                retry_after = int(resp.headers.get("Retry-After", "10"))
                if retry_after > self._long_penalty_threshold:
                    raise LongPenaltyError(
                        f"429 on {url} with Retry-After={retry_after}s "
                        f"(>{self._long_penalty_threshold:.0f}s). Aborting."
                    )
                wait = min(retry_after, 300)
                log.warning("429 on %s  Retry-After=%ds  attempt=%d/5",
                            path, retry_after, attempt + 1)
                self._backoff_until = max(self._backoff_until, self._now() + wait)
                self._sleep(wait)
                continue

            if 500 <= resp.status_code < 600:
                self.stats["calls_5xx"] += 1
                backoff = 2 ** attempt
                log.warning("%d on %s  attempt=%d/5  (sleeping %ds)",
                            resp.status_code, path, attempt + 1, backoff)
                self._sleep(backoff)
                continue

            resp.raise_for_status()
        raise RuntimeError(f"giving up on {url} after retries")


# ---------------------------------------------------------------------------
# Phase 1 — MusicBrainz: ISRC -> MBID
# ---------------------------------------------------------------------------
def lookup_mbid(client: ThrottledClient, isrc: str) -> Optional[str]:
    """Resolve an ISRC to a MusicBrainz Recording UUID.

    Returns the first recording's MBID, or None if MB has no record. When
    multiple recordings share an ISRC (re-issues, multi-region releases),
    we just take the first — for audio-feature lookup this is good enough,
    since AB features are per-recording but the actual audio is usually
    the same across re-releases.
    """
    data = client.get(f"/isrc/{isrc}", params={"fmt": "json"})
    if data is None:
        return None
    recs = data.get("recordings") or []
    if not recs:
        return None
    return recs[0].get("id")


# ---------------------------------------------------------------------------
# Phase 2 — AcousticBrainz: MBID -> features
# ---------------------------------------------------------------------------
KEY_TO_PITCH_CLASS = {
    "C": 0, "C#": 1, "Db": 1, "D": 2, "D#": 3, "Eb": 3,
    "E": 4, "F": 5, "F#": 6, "Gb": 6, "G": 7, "G#": 8,
    "Ab": 8, "A": 9, "A#": 10, "Bb": 10, "B": 11,
}


@dataclass
class Features:
    bpm: Optional[float] = None
    energy: Optional[float] = None       # normalized 0-1 (loudness proxy)
    valence: Optional[float] = None      # high-level mood_happy probability
    danceability: Optional[float] = None
    instrumental: Optional[float] = None
    key: Optional[int] = None
    mode: Optional[int] = None
    not_found: bool = False              # both endpoints 404 → don't retry


def _extract_lowlevel(low: dict) -> dict:
    """Pull bpm/key/mode/energy from AcousticBrainz low-level response."""
    out: dict = {}
    rhythm = low.get("rhythm") or {}
    tonal = low.get("tonal") or {}
    lowlevel = low.get("lowlevel") or {}

    bpm = rhythm.get("bpm")
    if isinstance(bpm, (int, float)):
        out["bpm"] = float(bpm)

    key_str = tonal.get("key_key")
    if key_str in KEY_TO_PITCH_CLASS:
        out["key"] = KEY_TO_PITCH_CLASS[key_str]

    scale = tonal.get("key_scale")
    if scale == "major":
        out["mode"] = 1
    elif scale == "minor":
        out["mode"] = 0

    loudness = lowlevel.get("average_loudness")
    if isinstance(loudness, (int, float)):
        out["energy"] = max(0.0, min(1.0, float(loudness)))

    return out


def _extract_highlevel(high: dict) -> dict:
    """Pull mood/danceability/voice probabilities from high-level response."""
    out: dict = {}
    hl = high.get("highlevel") or {}

    happy = hl.get("mood_happy") or {}
    happy_all = happy.get("all") or {}
    if "happy" in happy_all:
        out["valence"] = float(happy_all["happy"])

    dance = hl.get("danceability") or {}
    dance_all = dance.get("all") or {}
    if "danceable" in dance_all:
        out["danceability"] = float(dance_all["danceable"])

    voice = hl.get("voice_instrumental") or {}
    voice_all = voice.get("all") or {}
    if "instrumental" in voice_all:
        out["instrumental"] = float(voice_all["instrumental"])

    return out


def fetch_features(client: ThrottledClient, mbid: str) -> Features:
    """Fetch low-level + high-level features for a single MBID.

    Both endpoints can independently 404 — AB's frozen dataset has gaps.
    We fetch both, merge what we get. If both are 404, returns a Features
    with not_found=True so the caller can persist a "looked up, nothing
    there" row.
    """
    low = client.get(f"/{mbid}/low-level")
    high = client.get(f"/{mbid}/high-level")

    if low is None and high is None:
        return Features(not_found=True)

    f = Features()
    if low is not None:
        for k, v in _extract_lowlevel(low).items():
            setattr(f, k, v)
    if high is not None:
        for k, v in _extract_highlevel(high).items():
            setattr(f, k, v)
    return f


# ---------------------------------------------------------------------------
# Pipeline phases
# ---------------------------------------------------------------------------
@dataclass
class PhaseStats:
    candidates: int = 0
    attempted: int = 0
    hits: int = 0
    misses: int = 0
    aborted_at_index: Optional[int] = None
    error: Optional[str] = None


def _phase_1_candidates(conn: sqlite3.Connection, min_plays: int) -> list[tuple[int, str]]:
    """Tracks with ISRC and no mb_recordings row yet, filtered by min_plays.

    Returns [(track_id, isrc), ...] ordered by total plays descending so
    a partial run prioritizes the user's most-listened-to tracks.
    """
    rows = conn.execute(
        """
        SELECT t.track_id, t.isrc, COUNT(p.play_id) AS n_plays
        FROM tracks t
        LEFT JOIN plays p ON p.track_id = t.track_id AND p.content_type = 'track'
        WHERE t.isrc IS NOT NULL AND t.isrc != ''
          AND t.track_id NOT IN (SELECT track_id FROM mb_recordings)
        GROUP BY t.track_id
        HAVING n_plays >= ?
        ORDER BY n_plays DESC
        """,
        (min_plays,),
    ).fetchall()
    return [(r["track_id"], r["isrc"]) for r in rows]


def _phase_2_candidates(conn: sqlite3.Connection) -> list[tuple[int, str]]:
    """Tracks that have a non-null MBID and no acousticbrainz_features row."""
    rows = conn.execute(
        """
        SELECT m.track_id, m.mb_recording_id
        FROM mb_recordings m
        WHERE m.mb_recording_id IS NOT NULL
          AND m.track_id NOT IN (SELECT track_id FROM acousticbrainz_features)
        """,
    ).fetchall()
    return [(r["track_id"], r["mb_recording_id"]) for r in rows]


def run_phase_1(
    conn: sqlite3.Connection,
    client: ThrottledClient,
    min_plays: int = 5,
    max_n: Optional[int] = None,
    dry_run: bool = False,
) -> PhaseStats:
    """ISRC -> MBID for tracks above min_plays. Returns PhaseStats.

    Persists a row per track regardless of hit/miss (NULL mb_recording_id
    on miss) so resume logic can skip retries.
    """
    candidates = _phase_1_candidates(conn, min_plays)
    if max_n is not None:
        candidates = candidates[:max_n]
    stats = PhaseStats(candidates=len(candidates))

    for i, (track_id, isrc) in enumerate(candidates):
        try:
            mbid = lookup_mbid(client, isrc)
        except RateLimitError as e:
            stats.aborted_at_index = i
            stats.error = str(e)
            return stats
        stats.attempted += 1
        if mbid:
            stats.hits += 1
        else:
            stats.misses += 1
        if not dry_run:
            conn.execute(
                "INSERT INTO mb_recordings (track_id, mb_recording_id) VALUES (?, ?)",
                (track_id, mbid),
            )
            conn.commit()
    return stats


def run_phase_2(
    conn: sqlite3.Connection,
    client: ThrottledClient,
    max_n: Optional[int] = None,
    dry_run: bool = False,
) -> PhaseStats:
    """MBID -> features. Persists a row per attempted MBID."""
    candidates = _phase_2_candidates(conn)
    if max_n is not None:
        candidates = candidates[:max_n]
    stats = PhaseStats(candidates=len(candidates))

    for i, (track_id, mbid) in enumerate(candidates):
        try:
            f = fetch_features(client, mbid)
        except RateLimitError as e:
            stats.aborted_at_index = i
            stats.error = str(e)
            return stats
        stats.attempted += 1
        if f.not_found:
            stats.misses += 1
        else:
            stats.hits += 1
        if not dry_run:
            conn.execute(
                """
                INSERT INTO acousticbrainz_features
                    (track_id, bpm, energy, valence, danceability,
                     instrumental, key, mode, not_found)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (track_id, f.bpm, f.energy, f.valence, f.danceability,
                 f.instrumental, f.key, f.mode, 1 if f.not_found else 0),
            )
            conn.commit()
    return stats


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------
def print_phase_summary(name: str, stats: PhaseStats, dry_run: bool) -> None:
    verb = "would write" if dry_run else "wrote"
    print(f"\n=== {name} ===")
    print(f"  candidates: {stats.candidates}")
    print(f"  attempted:  {stats.attempted}")
    print(f"  hits:       {stats.hits}  ({verb} as data rows)")
    print(f"  misses:     {stats.misses}  ({verb} as 'looked-up, none found')")
    if stats.aborted_at_index is not None:
        print(f"  ABORTED at index {stats.aborted_at_index}: {stats.error}")


def print_coverage(conn: sqlite3.Connection) -> None:
    """End-of-run snapshot of overall coverage in the DB."""
    n_tracks = conn.execute("SELECT COUNT(*) FROM tracks").fetchone()[0]
    n_isrc = conn.execute(
        "SELECT COUNT(*) FROM tracks WHERE isrc IS NOT NULL AND isrc != ''"
    ).fetchone()[0]
    n_mb = conn.execute(
        "SELECT COUNT(*) FROM mb_recordings WHERE mb_recording_id IS NOT NULL"
    ).fetchone()[0]
    n_ab = conn.execute(
        "SELECT COUNT(*) FROM acousticbrainz_features WHERE not_found = 0"
    ).fetchone()[0]

    def pct(n, d): return f"{100*n/d:.1f}%" if d else "n/a"
    print("\n=== Coverage in DB ===")
    print(f"  tracks total:        {n_tracks:>7,}")
    print(f"  with ISRC:           {n_isrc:>7,}  ({pct(n_isrc, n_tracks)})")
    print(f"  with MBID:           {n_mb:>7,}  ({pct(n_mb, n_isrc)} of ISRC tracks)")
    print(f"  with AB features:    {n_ab:>7,}  ({pct(n_ab, n_mb)} of MBID tracks)")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Enrich tracks with audio features via MusicBrainz + AcousticBrainz.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--phase", choices=["1", "2", "both"], default="both",
                        help="Run phase 1 (ISRC->MBID), phase 2 (MBID->features), or both.")
    parser.add_argument("--min-plays", type=int, default=5,
                        help="Phase 1 only: minimum total plays to consider a track.")
    parser.add_argument("--max", type=int, default=None,
                        help="Cap candidates per phase (useful for smoke tests).")
    parser.add_argument("--dry-run", action="store_true",
                        help="Make HTTP calls but don't write to the DB.")
    parser.add_argument("--rate-interval", type=float, default=DEFAULT_RATE_INTERVAL,
                        help="Seconds between requests per API. Both APIs ask "
                             "for ~1 req/sec; default 1.1s is friendly.")
    parser.add_argument("--long-penalty-threshold", type=float,
                        default=DEFAULT_LONG_PENALTY_THRESHOLD,
                        help="429 Retry-After above this triggers immediate abort.")
    parser.add_argument("--user-agent", type=str, default=DEFAULT_USER_AGENT,
                        help="HTTP User-Agent (MusicBrainz requires identifiable UA).")
    parser.add_argument("--db", type=str, default=None,
                        help="Path to music.db (default: auto-detect)")

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    conn = connect(args.db)
    try:
        if args.phase in ("1", "both"):
            mb_client = ThrottledClient(
                MB_API,
                user_agent=args.user_agent,
                min_request_interval=args.rate_interval,
                long_penalty_threshold_seconds=args.long_penalty_threshold,
            )
            stats = run_phase_1(conn, mb_client, args.min_plays, args.max, args.dry_run)
            print_phase_summary("Phase 1: ISRC -> MBID (MusicBrainz)", stats, args.dry_run)
            if stats.aborted_at_index is not None:
                print_coverage(conn)
                return 2

        if args.phase in ("2", "both"):
            ab_client = ThrottledClient(
                AB_API,
                user_agent=args.user_agent,
                min_request_interval=args.rate_interval,
                long_penalty_threshold_seconds=args.long_penalty_threshold,
            )
            stats = run_phase_2(conn, ab_client, args.max, args.dry_run)
            print_phase_summary("Phase 2: MBID -> features (AcousticBrainz)", stats, args.dry_run)
            if stats.aborted_at_index is not None:
                print_coverage(conn)
                return 2

        print_coverage(conn)
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
