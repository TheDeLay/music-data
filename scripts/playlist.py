"""Playlist generator for music-data — engagement-model layer 3.

Combines the love score (scripts/score.py) with listening-context affinity
(scripts/cluster_modes.py + scripts/label_modes.py) and AcousticBrainz
audio features (scripts/enrich_acousticbrainz.py) to produce playlists
filtered by quality (love score), fit (time-of-day / weekend mode), and
acoustic character (BPM, key, mode, valence, energy, danceability,
instrumental).

Usage:
    # Top 30 by love score, no filters (close to score.py behavior)
    python -m scripts.playlist --top 30

    # Top 30 with love >= 5, only "weekday morning" primary tracks
    python -m scripts.playlist --love-min 5 --mode "weekday morning" --top 30

    # Upbeat happy tracks I love, fit late night
    python -m scripts.playlist --mode "late night" --love-min 5 \\
        --bpm-min 120 --valence-min 0.6

    # Slow + minor-key tracks for a specific mood
    python -m scripts.playlist --bpm-max 90 --key-mode minor --top 20

    # Output formats for actually using the playlist
    python -m scripts.playlist --mode "weekend daytime" --format uris > playlist.txt
    python -m scripts.playlist --mode "weekend daytime" --format json

Note on NULL handling: any active audio-feature filter EXCLUDES tracks
that don't have AcousticBrainz data for that field. Asking for BPM>=120
on a track with unknown BPM means "this track doesn't satisfy the
constraint" — same as standard SQL WHERE semantics.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from dataclasses import dataclass, field, fields
from typing import Optional

from scripts.db import connect
from scripts.score import ScoreConfig, TrackScore, score_tracks


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
@dataclass
class FeatureFilter:
    """Audio-feature filters from acousticbrainz_features.

    All None = no filter active. Combining filters is logical AND.
    NULL feature values fail any active filter (standard WHERE semantics).
    """
    bpm_min: Optional[float] = None
    bpm_max: Optional[float] = None
    valence_min: Optional[float] = None
    valence_max: Optional[float] = None
    energy_min: Optional[float] = None
    energy_max: Optional[float] = None
    danceability_min: Optional[float] = None
    danceability_max: Optional[float] = None
    instrumental_min: Optional[float] = None
    key: Optional[int] = None              # exact match, 0-11 pitch class
    key_mode: Optional[int] = None         # 0=minor, 1=major

    def is_active(self) -> bool:
        return any(getattr(self, f.name) is not None for f in fields(self))


@dataclass
class PlaylistConfig:
    score_config: ScoreConfig
    mode: Optional[str] = None             # raw user input — label or cluster_id
    love_min: float = 0.0
    top: int = 30
    min_affinity: Optional[float] = None   # None = strict (is_primary=1 only)
    feature_filter: FeatureFilter = field(default_factory=FeatureFilter)


class ModeNotFoundError(Exception):
    """--mode value didn't match any cluster_id or labeled context."""


# ---------------------------------------------------------------------------
# Mode resolution
# ---------------------------------------------------------------------------
def resolve_context(conn: sqlite3.Connection, mode: Optional[str]) -> Optional[int]:
    """Look up context_id for a --mode argument.

    Resolution order:
      1. mode is None or empty string → no mode filter (returns None)
      2. mode parses as integer → match cluster_id
      3. mode is text → case-insensitive exact match against user_label
                        (skips empty/unlabeled contexts)

    Raises ModeNotFoundError with available options if no match found.
    """
    if mode is None or mode == "":
        return None

    try:
        cid = int(mode)
        row = conn.execute(
            "SELECT context_id FROM listening_contexts WHERE cluster_id = ?",
            (cid,),
        ).fetchone()
        if row:
            return row["context_id"]
    except ValueError:
        pass

    row = conn.execute(
        "SELECT context_id FROM listening_contexts "
        "WHERE LOWER(user_label) = LOWER(?) AND user_label != ''",
        (mode,),
    ).fetchone()
    if row:
        return row["context_id"]

    available = conn.execute(
        "SELECT cluster_id, user_label FROM listening_contexts ORDER BY cluster_id"
    ).fetchall()
    msg_parts = [f"Mode {mode!r} not found."]
    if available:
        msg_parts.append("Available modes:")
        for r in available:
            label = r["user_label"] or "<unlabeled>"
            msg_parts.append(f"  cluster_id={r['cluster_id']}  label={label!r}")
    else:
        msg_parts.append("No clusters found — run scripts.cluster_modes first.")
    raise ModeNotFoundError("\n".join(msg_parts))


# ---------------------------------------------------------------------------
# Filtering
# ---------------------------------------------------------------------------
def filter_by_context(
    tracks: list[TrackScore],
    conn: sqlite3.Connection,
    context_id: int,
    min_affinity: Optional[float],
) -> list[TrackScore]:
    """Return only tracks belonging to the given context.

    Strict mode (min_affinity is None): only tracks with is_primary=1 in
    this context. Cleanest playlists, no overlap between modes.

    Loose mode (min_affinity is float): any track with affinity ≥ threshold,
    regardless of primary status. Tracks can appear in multiple modes.
    """
    if min_affinity is None:
        rows = conn.execute(
            "SELECT track_id FROM track_context_affinity "
            "WHERE context_id = ? AND is_primary = 1",
            (context_id,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT track_id FROM track_context_affinity "
            "WHERE context_id = ? AND affinity >= ?",
            (context_id, min_affinity),
        ).fetchall()
    eligible = {r["track_id"] for r in rows}
    return [t for t in tracks if t.track_id in eligible]


def filter_by_features(
    tracks: list[TrackScore],
    conn: sqlite3.Connection,
    flt: FeatureFilter,
) -> list[TrackScore]:
    """Filter tracks against acousticbrainz_features predicates.

    Builds one parameterized SELECT into the features table for whichever
    fields are constrained, returns the set of track_ids that pass, and
    keeps only those tracks. NULL feature values fail any active predicate
    (standard SQL WHERE behavior — `NULL >= 120` is NULL, treated as false).

    If no filter is active, returns the input unchanged without querying.
    """
    if not flt.is_active():
        return tracks

    clauses: list[str] = []
    params: list = []
    if flt.bpm_min is not None:
        clauses.append("bpm >= ?"); params.append(flt.bpm_min)
    if flt.bpm_max is not None:
        clauses.append("bpm <= ?"); params.append(flt.bpm_max)
    if flt.valence_min is not None:
        clauses.append("valence >= ?"); params.append(flt.valence_min)
    if flt.valence_max is not None:
        clauses.append("valence <= ?"); params.append(flt.valence_max)
    if flt.energy_min is not None:
        clauses.append("energy >= ?"); params.append(flt.energy_min)
    if flt.energy_max is not None:
        clauses.append("energy <= ?"); params.append(flt.energy_max)
    if flt.danceability_min is not None:
        clauses.append("danceability >= ?"); params.append(flt.danceability_min)
    if flt.danceability_max is not None:
        clauses.append("danceability <= ?"); params.append(flt.danceability_max)
    if flt.instrumental_min is not None:
        clauses.append("instrumental >= ?"); params.append(flt.instrumental_min)
    if flt.key is not None:
        clauses.append("key = ?"); params.append(flt.key)
    if flt.key_mode is not None:
        clauses.append("mode = ?"); params.append(flt.key_mode)

    where = " AND ".join(clauses)
    rows = conn.execute(
        f"SELECT track_id FROM acousticbrainz_features WHERE {where}",
        tuple(params),
    ).fetchall()
    eligible = {r["track_id"] for r in rows}
    return [t for t in tracks if t.track_id in eligible]


def load_features_for_tracks(
    conn: sqlite3.Connection, track_ids: list[int],
) -> dict[int, sqlite3.Row]:
    """Bulk-fetch audio features for the given track_ids. Returns dict
    keyed by track_id; missing tracks simply aren't in the result."""
    if not track_ids:
        return {}
    placeholders = ",".join("?" * len(track_ids))
    rows = conn.execute(
        f"SELECT track_id, bpm, energy, valence, danceability, "
        f"       instrumental, key, mode "
        f"FROM acousticbrainz_features WHERE track_id IN ({placeholders})",
        track_ids,
    ).fetchall()
    return {r["track_id"]: r for r in rows}


def build_playlist(
    conn: sqlite3.Connection,
    config: PlaylistConfig,
) -> list[TrackScore]:
    """Run the full pipeline: resolve mode → score → filter (context,
    love, features) → top-N."""
    context_id = resolve_context(conn, config.mode)

    tracks = score_tracks(conn, config.score_config)

    if context_id is not None:
        tracks = filter_by_context(tracks, conn, context_id, config.min_affinity)

    if config.love_min > 0:
        tracks = [t for t in tracks if t.love_score >= config.love_min]

    tracks = filter_by_features(tracks, conn, config.feature_filter)

    return tracks[:config.top]


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------
EMPTY_MSG = "No tracks match — try lowering --love-min or removing --mode."

KEY_NAMES = ["C", "C#", "D", "D#", "E", "F", "F#", "G", "G#", "A", "A#", "B"]
MODE_NAMES = {0: "min", 1: "maj"}


def _fmt_feature(value, fmt: str = "5.0f", missing: str = "  -  ") -> str:
    """Render a feature value, or a fixed-width placeholder if NULL."""
    if value is None:
        return missing
    return f"{value:{fmt}}"


def print_table(tracks: list[TrackScore], mode: Optional[str],
                features_by_id: Optional[dict[int, sqlite3.Row]] = None) -> None:
    if not tracks:
        print(EMPTY_MSG)
        return
    show_features = features_by_id is not None
    header = "\nPlaylist"
    if mode:
        header += f" — mode: {mode}"
    header += f"  ({len(tracks)} tracks)"
    print(header)
    if show_features:
        print(f"{'#':>3}  {'Love':>5}  {'Track':<35} {'Artist':<22} "
              f"{'Plays':>5} {'BPM':>5} {'Vlnc':>4} {'Eng':>4} {'Dnc':>4} {'Key':>4}")
        print("-" * 110)
        for i, t in enumerate(tracks, 1):
            f = features_by_id.get(t.track_id)
            if f is not None:
                bpm = _fmt_feature(f["bpm"], "5.0f")
                val = _fmt_feature(f["valence"], "4.2f")
                eng = _fmt_feature(f["energy"], "4.2f")
                dnc = _fmt_feature(f["danceability"], "4.2f")
                k = f["key"]; m = f["mode"]
                key_str = (f"{KEY_NAMES[k]}{MODE_NAMES.get(m, '')}"
                           if k is not None else "  -  ")
            else:
                bpm = val = eng = dnc = "  -  "
                key_str = "  -  "
            print(f"{i:>3}  {t.love_score:>5.0f}  {t.track_name[:34]:<35} "
                  f"{(t.primary_artist_name or '?')[:21]:<22} "
                  f"{t.total_plays:>5} {bpm:>5} {val:>4} {eng:>4} {dnc:>4} {key_str:>4}")
    else:
        print(f"{'#':>3}  {'Love':>5}  {'Track':<40} {'Artist':<25} "
              f"{'Plays':>5} {'Rcnt':>4} {'Back':>4} {'Avg%':>5}")
        print("-" * 100)
        for i, t in enumerate(tracks, 1):
            print(f"{i:>3}  {t.love_score:>5.0f}  {t.track_name[:39]:<40} "
                  f"{(t.primary_artist_name or '?')[:24]:<25} "
                  f"{t.total_plays:>5} {t.recent_quality:>4} "
                  f"{t.backbutton_count:>4} {t.avg_pct_played:>5.1f}")
    print()


def print_json(tracks: list[TrackScore], mode: Optional[str],
               features_by_id: Optional[dict[int, sqlite3.Row]] = None) -> None:
    """JSON always includes features when provided — machine-readable, no
    width concern. Tracks without features get null fields."""
    output_tracks = []
    for t in tracks:
        entry = {
            "track_id": t.track_id,
            "spotify_track_uri": t.spotify_track_uri,
            "track_name": t.track_name,
            "primary_artist_name": t.primary_artist_name,
            "album_name": t.album_name,
            "release_year": t.release_year,
            "love_score": round(t.love_score, 2),
            "total_plays": t.total_plays,
            "quality_plays": t.quality_plays,
            "recent_quality": t.recent_quality,
            "backbutton_count": t.backbutton_count,
            "avg_pct_played": round(t.avg_pct_played, 1),
        }
        if features_by_id is not None:
            f = features_by_id.get(t.track_id)
            entry["audio_features"] = {
                "bpm": f["bpm"] if f else None,
                "energy": f["energy"] if f else None,
                "valence": f["valence"] if f else None,
                "danceability": f["danceability"] if f else None,
                "instrumental": f["instrumental"] if f else None,
                "key": f["key"] if f else None,
                "mode": f["mode"] if f else None,
            }
        output_tracks.append(entry)
    output = {"mode": mode, "track_count": len(tracks), "tracks": output_tracks}
    json.dump(output, sys.stdout, indent=2)
    print()


def print_uris(tracks: list[TrackScore], mode: Optional[str]) -> None:
    """One spotify:track:... URI per line. Empty result → message to stderr only.

    The empty-case message is on stderr so `--format uris > playlist.txt`
    produces a truly empty file rather than one with a complaint in it.
    """
    if not tracks:
        print(EMPTY_MSG, file=sys.stderr)
        return
    for t in tracks:
        print(t.spotify_track_uri)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate a playlist from love score + listening context.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument("--mode", type=str, default=None,
                        help="Listening context: user_label (e.g. 'weekday morning') "
                             "or cluster_id integer. Omit for no mode filter.")
    parser.add_argument("--love-min", type=float, default=0.0,
                        help="Minimum love score to include (0 = no filter)")
    parser.add_argument("--top", type=int, default=30,
                        help="Number of tracks in the playlist")
    parser.add_argument("--min-affinity", type=float, default=None,
                        help="Loose mode: include tracks with affinity >= this "
                             "(0.0-1.0). Without this flag, strict mode "
                             "(is_primary=1 only) is used.")
    parser.add_argument("--format", choices=["table", "json", "uris"], default="table",
                        help="Output format. uris = paste-into-Spotify lines.")
    parser.add_argument("--db", type=str, default=None,
                        help="Path to music.db (default: auto-detect)")

    # Audio-feature filters (require AcousticBrainz enrichment to have run).
    # Any active filter excludes tracks without that feature populated.
    parser.add_argument("--bpm-min", type=float, default=None,
                        help="Minimum BPM (tempo)")
    parser.add_argument("--bpm-max", type=float, default=None,
                        help="Maximum BPM (tempo)")
    parser.add_argument("--valence-min", type=float, default=None,
                        help="Minimum valence — mood_happy probability (0.0-1.0)")
    parser.add_argument("--valence-max", type=float, default=None,
                        help="Maximum valence (0.0-1.0)")
    parser.add_argument("--energy-min", type=float, default=None,
                        help="Minimum energy — loudness proxy (0.0-1.0)")
    parser.add_argument("--energy-max", type=float, default=None,
                        help="Maximum energy (0.0-1.0)")
    parser.add_argument("--danceability-min", type=float, default=None,
                        help="Minimum danceability (0.0-1.0)")
    parser.add_argument("--danceability-max", type=float, default=None,
                        help="Maximum danceability (0.0-1.0)")
    parser.add_argument("--instrumental-min", type=float, default=None,
                        help="Minimum instrumental probability (0.0-1.0); "
                             "0.5+ = likely instrumental")
    parser.add_argument("--key", type=int, default=None, choices=range(12),
                        metavar="0..11",
                        help="Exact pitch class: 0=C 1=C# 2=D 3=D# 4=E 5=F "
                             "6=F# 7=G 8=G# 9=A 10=A# 11=B")
    parser.add_argument("--key-mode", choices=["minor", "major"], default=None,
                        help="Tonality: 'minor' or 'major' (separate from --mode)")
    parser.add_argument("--show-features", action="store_true",
                        help="Show audio-feature columns in the table format. "
                             "Auto-on when any feature filter is active.")

    parser.add_argument("--quality-threshold", type=float, default=0.80,
                        help="Min pct_played to count as a quality play (0.0-1.0)")
    parser.add_argument("--recency-days", type=int, default=90,
                        help="Days for the recency window")
    parser.add_argument("--recent-weight", type=float, default=3.0)
    parser.add_argument("--backbutton-weight", type=float, default=5.0)
    parser.add_argument("--lifetime-weight", type=float, default=1.0)
    parser.add_argument("--skip-forgiveness", type=int, default=2)
    parser.add_argument("--skip-penalty", type=float, default=2.0)
    parser.add_argument("--streak-window", type=int, default=10)
    parser.add_argument("--deliberate-weight", type=float, default=2.0)

    args = parser.parse_args(argv)

    score_config = ScoreConfig(
        quality_threshold=args.quality_threshold,
        recency_days=args.recency_days,
        recent_weight=args.recent_weight,
        backbutton_weight=args.backbutton_weight,
        lifetime_weight=args.lifetime_weight,
        skip_forgiveness=args.skip_forgiveness,
        skip_penalty=args.skip_penalty,
        streak_window=args.streak_window,
        deliberate_weight=args.deliberate_weight,
    )
    feature_filter = FeatureFilter(
        bpm_min=args.bpm_min, bpm_max=args.bpm_max,
        valence_min=args.valence_min, valence_max=args.valence_max,
        energy_min=args.energy_min, energy_max=args.energy_max,
        danceability_min=args.danceability_min, danceability_max=args.danceability_max,
        instrumental_min=args.instrumental_min,
        key=args.key,
        key_mode={"minor": 0, "major": 1}.get(args.key_mode),
    )
    config = PlaylistConfig(
        score_config=score_config,
        mode=args.mode,
        love_min=args.love_min,
        top=args.top,
        min_affinity=args.min_affinity,
        feature_filter=feature_filter,
    )

    conn = connect(args.db)
    try:
        try:
            tracks = build_playlist(conn, config)
        except ModeNotFoundError as e:
            print(str(e), file=sys.stderr)
            return 2

        # Side-channel: pull features for the final playlist for output.
        # Auto-show features in the table if a feature filter was active OR
        # the user explicitly asked.
        show_features = args.show_features or feature_filter.is_active()
        features_by_id = (load_features_for_tracks(conn, [t.track_id for t in tracks])
                          if show_features or args.format == "json"
                          else None)

        if args.format == "json":
            # JSON always includes features (cheap, no width concern)
            if features_by_id is None:
                features_by_id = load_features_for_tracks(
                    conn, [t.track_id for t in tracks])
            print_json(tracks, args.mode, features_by_id)
        elif args.format == "uris":
            print_uris(tracks, args.mode)
        else:
            print_table(tracks, args.mode,
                        features_by_id if show_features else None)
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
