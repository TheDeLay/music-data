"""Extractors: turn raw input into validated PlayRecord objects.

Two paths today, more possible later:
- from_dump_record: a single record from the extended streaming history JSON
- from_recently_played_item: a single item from /me/player/recently-played

Both produce the same PlayRecord shape, so the loader doesn't care where data
came from. Validation errors raise; callers should catch and quarantine.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Iterator

from pydantic import ValidationError

from .models import PlayRecord


# -----------------------------------------------------------------------------
# Extended streaming history (JSON files from the privacy export)
# -----------------------------------------------------------------------------
def from_dump_record(rec: dict) -> PlayRecord:
    """Convert one dict from a Spotify extended streaming history JSON file.

    Raises ValueError or pydantic.ValidationError on bad input. Caller is
    expected to catch and route to rejected_rows.
    """
    # Determine content_type from which URI is populated
    track_uri = rec.get("spotify_track_uri")
    episode_uri = rec.get("spotify_episode_uri")
    chapter_uri = rec.get("audiobook_chapter_uri")

    if track_uri:
        content_type = "track"
        content_uri = track_uri
    elif episode_uri:
        content_type = "episode"
        content_uri = episode_uri
    elif chapter_uri:
        content_type = "audiobook_chapter"
        content_uri = chapter_uri
    else:
        # Local files, podcasts that crashed mid-stream, weird edge cases.
        # Reject — we can't anchor this to anything stable.
        raise ValueError("no track/episode/chapter URI present")

    return PlayRecord(
        ts=rec["ts"],
        ms_played=rec.get("ms_played", 0),
        content_type=content_type,
        content_uri=content_uri,
        track_name=rec.get("master_metadata_track_name"),
        artist_name=rec.get("master_metadata_album_artist_name"),
        album_name=rec.get("master_metadata_album_album_name"),
        episode_name=rec.get("episode_name"),
        show_name=rec.get("episode_show_name"),
        audiobook_title=rec.get("audiobook_title"),
        audiobook_uri=rec.get("audiobook_uri"),
        chapter_title=rec.get("audiobook_chapter_title"),
        platform=rec.get("platform"),
        conn_country=rec.get("conn_country"),
        reason_start=rec.get("reason_start"),
        reason_end=rec.get("reason_end"),
        shuffle=rec.get("shuffle"),
        skipped=rec.get("skipped"),
        offline=rec.get("offline"),
        incognito_mode=rec.get("incognito_mode"),
        source="extended_dump",
    )


def iter_dump_files(dump_dir: Path) -> Iterator[Path]:
    """Yield the JSON files in a dump directory, in lexical order.

    Spotify names them like 'Streaming_History_Audio_2018-2019_0.json',
    'Streaming_History_Audio_2019-2020_1.json', etc. Lexical order is
    chronological, which is what we want for stable run logs.
    """
    if not dump_dir.is_dir():
        raise NotADirectoryError(f"dump dir not found: {dump_dir}")
    for path in sorted(dump_dir.glob("Streaming_History_*.json")):
        yield path
    # Also allow plain .json files (in case the user pre-extracted/renamed)
    for path in sorted(dump_dir.glob("*.json")):
        if not path.name.startswith("Streaming_History_"):
            yield path


def iter_dump_records(dump_dir: Path) -> Iterator[tuple[Path, int, dict]]:
    """Yield (file_path, index_within_file, raw_record) tuples.

    The file_path + index pair is useful for error messages so quarantined
    rows can be traced back to their origin.
    """
    for path in iter_dump_files(dump_dir):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception as e:
            # Whole-file failure: yield a single sentinel so the caller logs it
            yield (path, -1, {"_file_error": str(e)})
            continue
        if not isinstance(data, list):
            yield (path, -1, {"_file_error": f"expected JSON array, got {type(data).__name__}"})
            continue
        for i, rec in enumerate(data):
            yield (path, i, rec)


# -----------------------------------------------------------------------------
# Recently played API (/me/player/recently-played)
# -----------------------------------------------------------------------------
def from_recently_played_item(item: dict) -> PlayRecord:
    """Convert one item from Spotify's /me/player/recently-played response.

    Note: this endpoint returns a *strict subset* of fields compared to the
    dump. We don't get reason_start/end, shuffle, skipped, offline, etc.
    We ALSO don't get ms_played — the API returns total track duration,
    which is wrong. We approximate by setting ms_played = duration_ms,
    flagging via reason_end='api_estimate'. This is the unavoidable limitation
    of the recently-played endpoint; if you need real engagement data, the
    dump is the only source.
    """
    track = item.get("track") or {}
    track_uri = track.get("uri")
    if not track_uri or not track_uri.startswith("spotify:track:"):
        # The recently-played endpoint only returns tracks today; episodes need a different endpoint
        raise ValueError(f"unexpected/missing track uri: {track_uri!r}")

    artists = track.get("artists") or []
    primary_artist_name = artists[0].get("name") if artists else None
    album = track.get("album") or {}

    return PlayRecord(
        ts=item["played_at"],
        ms_played=track.get("duration_ms", 0),  # see docstring caveat
        content_type="track",
        content_uri=track_uri,
        track_name=track.get("name"),
        artist_name=primary_artist_name,
        album_name=album.get("name"),
        platform=None,
        conn_country=None,
        reason_start=None,
        reason_end="api_estimate",  # marker that ms_played is duration, not actual
        shuffle=None,
        skipped=None,
        offline=None,
        incognito_mode=None,
        source="recently_played_api",
    )


# -----------------------------------------------------------------------------
# Validation helper (for the loader's quarantine path)
# -----------------------------------------------------------------------------
def safe_extract(extractor, raw: dict) -> tuple[PlayRecord | None, str | None]:
    """Run an extractor, return (record, None) on success or (None, error_msg)."""
    try:
        return extractor(raw), None
    except (ValueError, ValidationError, KeyError, TypeError) as e:
        return None, f"{type(e).__name__}: {e}"
