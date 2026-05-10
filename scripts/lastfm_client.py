"""Last.fm API client for tag fetching.

Wraps `ThrottledClient` (from `enrich_acousticbrainz`) with Last.fm-specific
URL/parameter conventions and the API's quirky "HTTP 200 with `error` in body"
error semantics.

We only use read endpoints: `artist.getTopTags`, `track.getTopTags`. Per the
TOS analysis in `lastfm-integration-notes.md`, we never call `user.*`
endpoints (clause 5.1.6 forbids personally-identifying use of Last.fm data).

Design choices documented in `lastfm-integration-notes.md`:
- Default rate-interval 0.5s (2 req/sec) — under "several per second" limit
- Hard upper bound 5 req/sec enforced at construction
- Same User-Agent, watchdog, LongPenaltyError pattern as MB/AB integration
- API-key-only auth; no OAuth, no per-user tokens

Usage:
    from scripts.lastfm_client import LastfmClient

    client = LastfmClient(api_key=os.environ["LASTFM_API_KEY"])
    result = client.get_artist_top_tags(artist_name="Metallica")
    if result.not_found:
        print("artist not in Last.fm")
    else:
        for name, count in result.tags:
            print(f"  {name}: {count}")
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

from scripts.enrich_acousticbrainz import (
    DEFAULT_LONG_PENALTY_THRESHOLD,
    DEFAULT_MAX_NO_PROGRESS,
    DEFAULT_USER_AGENT,
    LongPenaltyError,
    RateLimitError,
    SustainedRateLimitError,
    ThrottledClient,
)


log = logging.getLogger(__name__)

LASTFM_BASE_URL = "https://ws.audioscrobbler.com/2.0/"

# Conservative defaults per lastfm-integration-notes.md
DEFAULT_RATE_INTERVAL = 0.5     # 2 req/sec
HARD_MAX_RATE = 5.0             # never go faster than 5 req/sec
HARD_MIN_INTERVAL = 1.0 / HARD_MAX_RATE   # = 0.2s

# Last.fm error codes that we map to specific behaviors.
ERROR_INVALID_PARAMS = 6        # artist/track not found, or bad params
ERROR_INVALID_API_KEY = 10
ERROR_RATE_LIMIT = 29


class LastfmAuthError(Exception):
    """Invalid API key or auth-related Last.fm error. Don't retry; surface."""


class LastfmAPIError(Exception):
    """Last.fm returned an unrecognized error code. Surface for investigation."""


@dataclass
class TagsResult:
    """Result of an `artist.getTopTags` or `track.getTopTags` call."""
    tags: list[tuple[str, int]] = field(default_factory=list)   # [(name, count), ...]
    not_found: bool = False     # True when the entity doesn't exist in Last.fm


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------
class LastfmClient:
    """Polite Last.fm client over ThrottledClient.

    Construct with an API key; reuses ThrottledClient for throttle, backoff,
    watchdog. Translates Last.fm's "200 OK with error-in-body" into proper
    Python exceptions.
    """

    def __init__(
        self,
        api_key: str,
        *,
        user_agent: str = DEFAULT_USER_AGENT,
        min_request_interval: float = DEFAULT_RATE_INTERVAL,
        long_penalty_threshold_seconds: float = DEFAULT_LONG_PENALTY_THRESHOLD,
        max_no_progress_seconds: float = DEFAULT_MAX_NO_PROGRESS,
        throttled_client: Optional[ThrottledClient] = None,
    ):
        if not api_key:
            raise LastfmAuthError(
                "LASTFM_API_KEY is empty. Sign up at "
                "https://www.last.fm/api/account/create and set it in .env."
            )
        if min_request_interval < HARD_MIN_INTERVAL:
            raise ValueError(
                f"min_request_interval={min_request_interval} violates the "
                f"5 req/sec hard cap (min interval {HARD_MIN_INTERVAL}s) "
                f"set per Last.fm TOS analysis. Use a value >= {HARD_MIN_INTERVAL}."
            )
        self.api_key = api_key
        self._client = throttled_client if throttled_client is not None else ThrottledClient(
            LASTFM_BASE_URL,
            user_agent=user_agent,
            min_request_interval=min_request_interval,
            long_penalty_threshold_seconds=long_penalty_threshold_seconds,
            max_no_progress_seconds=max_no_progress_seconds,
        )

    @property
    def stats(self) -> dict:
        """Surface the underlying ThrottledClient's stats dict for reporting."""
        return self._client.stats

    # -------------------------------------------------------------------
    # Internal: parameterized GET with error-body translation
    # -------------------------------------------------------------------
    def _call(self, method: str, **params) -> Optional[dict]:
        """GET ws.audioscrobbler.com/2.0/?method=<method>&api_key=...&format=json&...

        Returns parsed dict on success, None on "not found" (HTTP 404 OR
        Last.fm error=6). Raises LastfmAuthError / RateLimitError /
        LastfmAPIError for other error conditions.

        Propagates RateLimitError from the underlying ThrottledClient
        (long-penalty / sustained-no-progress).
        """
        full_params = {
            "method": method,
            "api_key": self.api_key,
            "format": "json",
            **params,
        }
        data = self._client.get("", params=full_params)
        if data is None:
            return None   # HTTP 404

        # Last.fm sometimes returns HTTP 200 with an error field in the body.
        # Translate the documented codes into proper Python exceptions; opaque
        # codes surface as LastfmAPIError so we don't silently swallow problems.
        if isinstance(data, dict) and "error" in data:
            try:
                err = int(data["error"])
            except (TypeError, ValueError):
                raise LastfmAPIError(
                    f"Last.fm response had non-integer error field: {data!r}"
                )
            message = data.get("message", "<no message>")
            if err == ERROR_INVALID_PARAMS:
                return None
            if err == ERROR_INVALID_API_KEY:
                raise LastfmAuthError(f"error=10: {message}")
            if err == ERROR_RATE_LIMIT:
                raise RateLimitError(f"Last.fm error=29 (rate limit): {message}")
            raise LastfmAPIError(f"Last.fm error={err}: {message}")
        return data

    # -------------------------------------------------------------------
    # Public: tag fetchers
    # -------------------------------------------------------------------
    def get_artist_top_tags(
        self,
        artist_name: Optional[str] = None,
        mbid: Optional[str] = None,
    ) -> TagsResult:
        """Fetch top tags for an artist.

        Provide either `artist_name` OR `mbid` (mbid is more precise — it
        disambiguates artists who share a name). When both are provided,
        mbid wins (Last.fm's documented precedence).

        Returns a TagsResult with tags=[] and not_found=True when Last.fm
        has no record of the artist. An artist who exists but is untagged
        returns tags=[] with not_found=False.
        """
        if not (artist_name or mbid):
            raise ValueError("must provide artist_name or mbid")

        params: dict = {}
        if mbid:
            params["mbid"] = mbid
        else:
            params["artist"] = artist_name

        data = self._call("artist.getTopTags", **params)
        return _parse_tags_response(data)

    def get_track_top_tags(
        self,
        artist_name: str,
        track_name: str,
    ) -> TagsResult:
        """Fetch top tags for a specific track.

        Both args required — Last.fm's track endpoint needs both artist and
        track name to disambiguate. Returns same shape as artist tags.
        """
        if not (artist_name and track_name):
            raise ValueError("both artist_name and track_name are required")

        data = self._call(
            "track.getTopTags",
            artist=artist_name,
            track=track_name,
        )
        return _parse_tags_response(data)


# ---------------------------------------------------------------------------
# Response parsing (module-level so it's testable in isolation)
# ---------------------------------------------------------------------------
def _parse_tags_response(data: Optional[dict]) -> TagsResult:
    """Extract (name, count) pairs from a Last.fm getTopTags response.

    The Last.fm API returns:
      - None / null for "not found" (we translate _call's None into not_found=True)
      - {"toptags": {"tag": [{"name": "metal", "count": 100}, ...]}}     normal
      - {"toptags": {"tag": {"name": "metal", "count": 100}}}             single tag (dict not list)
      - {"toptags": {"tag": []}}                                          tagged-but-no-data
      - {"toptags": {"@attr": {...}}}                                     no `tag` key at all
    All are valid; only the first is "not_found".
    """
    if data is None:
        return TagsResult(tags=[], not_found=True)

    toptags = data.get("toptags") or {}
    raw = toptags.get("tag")
    if raw is None:
        return TagsResult(tags=[], not_found=False)

    # Last.fm collapses single-element arrays to a bare dict.
    if isinstance(raw, dict):
        raw = [raw]

    tags: list[tuple[str, int]] = []
    for entry in raw:
        if not isinstance(entry, dict):
            continue
        name = entry.get("name") or ""
        if not name:
            continue
        try:
            count = int(entry.get("count", 0))
        except (TypeError, ValueError):
            count = 0
        tags.append((name, count))

    return TagsResult(tags=tags, not_found=False)
