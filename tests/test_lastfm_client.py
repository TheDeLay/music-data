"""Tests for the Last.fm API client.

Mocked HTTP throughout — no live Last.fm calls. Verifies:
  - LastfmClient construction (api-key required, hard rate cap enforced)
  - _call: success, HTTP 404, error=6 (treated as 404), error=10 (auth),
    error=29 (rate limit), unknown error code
  - get_artist_top_tags: artist_name path, mbid path, mbid precedence,
    missing-arg ValueError
  - get_track_top_tags: happy path, missing-arg ValueError
  - _parse_tags_response: list of tags, single-tag dict (Last.fm collapse),
    empty array, missing 'tag' key, malformed entries, None passthrough
"""
from __future__ import annotations

from unittest import mock

import pytest

from scripts.enrich_acousticbrainz import RateLimitError, ThrottledClient
from scripts.lastfm_client import (
    HARD_MIN_INTERVAL,
    LASTFM_BASE_URL,
    LastfmAPIError,
    LastfmAuthError,
    LastfmClient,
    TagsResult,
    _parse_tags_response,
)


# ---------------------------------------------------------------------------
# _parse_tags_response (pure, easy to test)
# ---------------------------------------------------------------------------
class TestParseTagsResponse:

    def test_none_means_not_found(self):
        r = _parse_tags_response(None)
        assert r.not_found is True
        assert r.tags == []

    def test_normal_array(self):
        r = _parse_tags_response({
            "toptags": {"tag": [
                {"name": "metal", "count": 100},
                {"name": "thrash metal", "count": 80},
            ]}
        })
        assert r.not_found is False
        assert r.tags == [("metal", 100), ("thrash metal", 80)]

    def test_single_tag_collapsed_to_dict(self):
        # Last.fm collapses single-element arrays to bare dicts.
        r = _parse_tags_response({
            "toptags": {"tag": {"name": "indie", "count": 5}}
        })
        assert r.tags == [("indie", 5)]

    def test_empty_tag_array_is_valid_not_404(self):
        r = _parse_tags_response({"toptags": {"tag": []}})
        assert r.not_found is False
        assert r.tags == []

    def test_missing_tag_key(self):
        # Some responses come back with @attr only and no tag list.
        r = _parse_tags_response({"toptags": {"@attr": {"artist": "X"}}})
        assert r.not_found is False
        assert r.tags == []

    def test_malformed_entries_skipped(self):
        r = _parse_tags_response({"toptags": {"tag": [
            {"name": "rock", "count": 10},
            {"name": ""},                        # blank name — skipped
            "not-a-dict",                        # wrong type — skipped
            {"name": "blues", "count": "garbage"},  # bad count → 0
        ]}})
        # rock OK; blank skipped; non-dict skipped; "garbage" count → 0
        assert r.tags == [("rock", 10), ("blues", 0)]


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------
class TestConstruction:

    def test_empty_api_key_rejected(self):
        with pytest.raises(LastfmAuthError):
            LastfmClient(api_key="")

    def test_rate_below_hard_cap_rejected(self):
        # min_request_interval=0.1 = 10 req/sec, above 5 req/sec hard cap
        with pytest.raises(ValueError, match="hard cap"):
            LastfmClient(api_key="test", min_request_interval=0.1)

    def test_rate_at_hard_cap_accepted(self):
        c = LastfmClient(api_key="test", min_request_interval=HARD_MIN_INTERVAL)
        # Should not raise; underlying ThrottledClient was constructed
        assert c.api_key == "test"

    def test_default_construction_uses_lastfm_base_url(self):
        c = LastfmClient(api_key="test")
        assert c._client.base_url.startswith("https://ws.audioscrobbler.com")


# ---------------------------------------------------------------------------
# _call (using injected fake ThrottledClient)
# ---------------------------------------------------------------------------
def _client_with_fake(returns=None, raises=None):
    """Build a LastfmClient whose _client.get is mocked to return/raise."""
    fake = mock.MagicMock(spec=ThrottledClient)
    fake.stats = {"calls_total": 0}
    if raises is not None:
        fake.get.side_effect = raises
    else:
        fake.get.return_value = returns
    c = LastfmClient(api_key="testkey", throttled_client=fake)
    return c, fake


class TestCall:

    def test_404_returns_none(self):
        c, _ = _client_with_fake(returns=None)
        assert c._call("artist.getTopTags", artist="X") is None

    def test_error_6_invalid_params_returns_none(self):
        # error=6 means "not found" / bad input. Treat as 404.
        c, _ = _client_with_fake(returns={"error": 6, "message": "not found"})
        assert c._call("artist.getTopTags", artist="X") is None

    def test_error_10_invalid_api_key_raises_auth(self):
        c, _ = _client_with_fake(returns={"error": 10, "message": "Invalid API key"})
        with pytest.raises(LastfmAuthError, match="Invalid API key"):
            c._call("artist.getTopTags", artist="X")

    def test_error_29_rate_limit_raises_rate_limit(self):
        c, _ = _client_with_fake(returns={"error": 29, "message": "rate limit hit"})
        with pytest.raises(RateLimitError, match="error=29"):
            c._call("artist.getTopTags", artist="X")

    def test_unknown_error_raises_api_error(self):
        c, _ = _client_with_fake(returns={"error": 99, "message": "unfamiliar"})
        with pytest.raises(LastfmAPIError, match="error=99"):
            c._call("artist.getTopTags", artist="X")

    def test_success_returns_payload(self):
        payload = {"toptags": {"tag": [{"name": "metal", "count": 5}]}}
        c, _ = _client_with_fake(returns=payload)
        assert c._call("artist.getTopTags", artist="X") == payload

    def test_call_includes_method_apikey_format(self):
        c, fake = _client_with_fake(returns={"toptags": {"tag": []}})
        c._call("artist.getTopTags", artist="Megafake")
        args, kwargs = fake.get.call_args
        assert args[0] == ""        # path = "" (single Last.fm endpoint)
        params = kwargs["params"]
        assert params["method"] == "artist.getTopTags"
        assert params["api_key"] == "testkey"
        assert params["format"] == "json"
        assert params["artist"] == "Megafake"

    def test_long_penalty_propagates_from_underlying_client(self):
        from scripts.enrich_acousticbrainz import LongPenaltyError
        c, _ = _client_with_fake(raises=LongPenaltyError("Retry-After 300s"))
        with pytest.raises(LongPenaltyError):
            c._call("artist.getTopTags", artist="X")


# ---------------------------------------------------------------------------
# get_artist_top_tags
# ---------------------------------------------------------------------------
class TestGetArtistTopTags:

    def test_artist_name_path(self):
        c, fake = _client_with_fake(returns={
            "toptags": {"tag": [{"name": "metal", "count": 100}]}
        })
        r = c.get_artist_top_tags(artist_name="Metallica")
        assert r.tags == [("metal", 100)]
        assert r.not_found is False
        # Ensure 'artist' was sent, not 'mbid'
        params = fake.get.call_args.kwargs["params"]
        assert params.get("artist") == "Metallica"
        assert "mbid" not in params

    def test_mbid_path(self):
        c, fake = _client_with_fake(returns={"toptags": {"tag": []}})
        c.get_artist_top_tags(mbid="abc-123")
        params = fake.get.call_args.kwargs["params"]
        assert params.get("mbid") == "abc-123"
        assert "artist" not in params

    def test_mbid_takes_precedence_over_artist_name(self):
        c, fake = _client_with_fake(returns={"toptags": {"tag": []}})
        c.get_artist_top_tags(artist_name="N", mbid="abc-123")
        params = fake.get.call_args.kwargs["params"]
        assert params.get("mbid") == "abc-123"
        assert "artist" not in params

    def test_neither_arg_raises(self):
        c, _ = _client_with_fake(returns=None)
        with pytest.raises(ValueError, match="artist_name or mbid"):
            c.get_artist_top_tags()

    def test_artist_not_found_returns_not_found_result(self):
        c, _ = _client_with_fake(returns={"error": 6, "message": "not found"})
        r = c.get_artist_top_tags(artist_name="ZZZ")
        assert r.not_found is True
        assert r.tags == []


# ---------------------------------------------------------------------------
# get_track_top_tags
# ---------------------------------------------------------------------------
class TestGetTrackTopTags:

    def test_happy_path(self):
        c, fake = _client_with_fake(returns={
            "toptags": {"tag": [{"name": "thrash", "count": 50}]}
        })
        r = c.get_track_top_tags(artist_name="Metallica", track_name="One")
        assert r.tags == [("thrash", 50)]
        params = fake.get.call_args.kwargs["params"]
        assert params["method"] == "track.getTopTags"
        assert params["artist"] == "Metallica"
        assert params["track"] == "One"

    def test_missing_args_raises(self):
        c, _ = _client_with_fake(returns=None)
        with pytest.raises(ValueError):
            c.get_track_top_tags(artist_name="", track_name="One")
        with pytest.raises(ValueError):
            c.get_track_top_tags(artist_name="Metallica", track_name="")
