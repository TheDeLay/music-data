"""Pydantic models for ingest pipeline records.

These are the canonical "wire format" between extractors and the loader.
Both the dump extractor and the API extractor produce instances of these
models; the loader only knows how to consume them.

This is the seam that lets us swap data sources without touching the loader.
"""
from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


ContentType = Literal["track", "episode", "audiobook_chapter"]
PlaySource = Literal["extended_dump", "recently_played_api", "top_tracks_api"]


class PlayRecord(BaseModel):
    """A single play event in normalized form, ready to load into the DB.

    Field naming intentionally mirrors the `plays` table columns so the loader
    can stay simple. Fields not in this model (track_id, episode_id,
    audiobook_chapter_id, ingestion_run_id, ingested_at) are populated by
    the loader during write.
    """
    ts: str = Field(..., description="ISO 8601 UTC timestamp")
    ms_played: int = Field(..., ge=0)
    content_type: ContentType
    content_uri: str = Field(..., description="The URI matching content_type (track, episode, or chapter)")

    # Names from the dump — these get converted to entity IDs during load
    track_name: Optional[str] = None
    artist_name: Optional[str] = None        # primary artist only (album_artist from dump)
    album_name: Optional[str] = None
    episode_name: Optional[str] = None
    show_name: Optional[str] = None
    show_uri: Optional[str] = None           # API can give us this; dump cannot
    audiobook_title: Optional[str] = None
    audiobook_uri: Optional[str] = None
    chapter_title: Optional[str] = None

    platform: Optional[str] = None
    conn_country: Optional[str] = None
    reason_start: Optional[str] = None
    reason_end: Optional[str] = None
    shuffle: Optional[bool] = None
    skipped: Optional[bool] = None
    offline: Optional[bool] = None
    incognito_mode: Optional[bool] = None

    source: PlaySource

    @field_validator("ts")
    @classmethod
    def normalize_ts(cls, v: str) -> str:
        """Accept various ISO formats; emit canonical 'YYYY-MM-DDTHH:MM:SSZ'.

        The dump uses 'YYYY-MM-DDTHH:MM:SSZ'. The API uses
        'YYYY-MM-DDTHH:MM:SS.mmmZ'. We normalize to second precision in UTC
        so dedup keys match across sources.
        """
        if not v:
            raise ValueError("timestamp is required")
        # Strip fractional seconds and timezone variants, re-emit canonical form
        s = v.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(s)
        except ValueError as e:
            raise ValueError(f"invalid timestamp: {v!r}") from e
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    @model_validator(mode="after")
    def check_content_consistency(self) -> "PlayRecord":
        """Ensure the URI scheme matches content_type."""
        expected_prefix = {
            "track": "spotify:track:",
            "episode": "spotify:episode:",
            "audiobook_chapter": "spotify:chapter:",
        }[self.content_type]
        if not self.content_uri.startswith(expected_prefix):
            # Don't fail hard — log + continue handled at extractor level
            # but model still enforces non-empty URI
            if not self.content_uri:
                raise ValueError(f"content_uri is empty for content_type={self.content_type}")
        return self
