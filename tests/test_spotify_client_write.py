"""Tests for the SpotifyClient write surface added 2026-05-09 late evening:

  - authorize_headless: paste-back OAuth flow (no browser needed)
  - post: throttle/retry/backoff for POST endpoints
  - create_playlist: requires non-empty name; caches user_id
  - add_tracks_to_playlist: chunks at 100 URIs/request

All HTTP is mocked. The existing browser-based _authorize() and the rich
get() retry behavior are NOT covered here (they're shipped + working;
adding regression coverage is a future task).
"""
from __future__ import annotations

import os
from unittest import mock

import pytest

from scripts.spotify_client import (
    SpotifyAuthError,
    SpotifyClient,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _make_client(monkeypatch, **overrides):
    """Build a SpotifyClient with auth='user' and a fake cached token.

    Skips the browser auth path entirely. Real network calls would still
    fail; tests that hit the network mock _session.post / _session.get.
    """
    monkeypatch.setenv("SPOTIFY_CLIENT_ID", "test-client-id")
    monkeypatch.setenv("SPOTIFY_CLIENT_SECRET", "test-client-secret")
    monkeypatch.setenv("SPOTIFY_REDIRECT_URI", "http://127.0.0.1:8888/callback")
    # Avoid touching the real token file
    with mock.patch("scripts.spotify_client.SpotifyClient._load_token", return_value=None):
        client = SpotifyClient(auth="user", **overrides)
    # Inject a never-expires fake token so _ensure_token doesn't trigger auth
    client._token = {
        "access_token": "fake-access",
        "refresh_token": "fake-refresh",
        "expires_at": 9_999_999_999,
        "scope": "playlist-modify-private playlist-modify-public user-read-private",
    }
    return client


def _fake_response(status_code: int, json_payload=None, headers=None):
    r = mock.MagicMock()
    r.status_code = status_code
    r.headers = headers or {}
    r.json.return_value = json_payload if json_payload is not None else {}
    r.content = b"x" if json_payload else b""
    return r


# ---------------------------------------------------------------------------
# authorize_headless — paste-back flow
# ---------------------------------------------------------------------------
class TestAuthorizeHeadless:

    def test_full_url_paste_extracts_code_and_state(self, monkeypatch):
        client = _make_client(monkeypatch)
        printed: list[str] = []

        # Capture the auth URL so we can extract the state we expect back
        def fake_print(*args, **kwargs):
            printed.append(" ".join(str(a) for a in args))

        # We need to know the state the client generated. Easiest: pre-seed
        # a deterministic random so we can reconstruct the URL. Alternatively,
        # parse it out of what gets printed.
        def fake_input(prompt):
            # Find the state= in the printed URL
            for line in printed:
                if "state=" in line:
                    state = line.split("state=")[1].split("&")[0]
                    return f"http://127.0.0.1:8888/callback?code=ABC123&state={state}"
            raise AssertionError("no auth URL printed")

        # Mock the token-exchange POST so it succeeds
        with mock.patch("scripts.spotify_client.requests.post") as mp, \
             mock.patch.object(client, "_save_token") as msave:
            mp.return_value = _fake_response(200, {
                "access_token": "new-tok", "refresh_token": "new-ref",
                "expires_in": 3600,
            })
            client.authorize_headless(input_fn=fake_input, print_fn=fake_print)

        # Token-exchange POST happened with the pasted code + the verifier
        # the client generated for this session.
        assert mp.called
        post_data = mp.call_args.kwargs["data"]
        assert post_data["grant_type"] == "authorization_code"
        assert post_data["code"] == "ABC123"
        assert "code_verifier" in post_data
        msave.assert_called_once()

    def test_just_query_string_paste_works(self, monkeypatch):
        """If user pastes only the query string (after '?'), still works."""
        client = _make_client(monkeypatch)
        printed: list[str] = []

        def fake_print(*args, **kwargs):
            printed.append(" ".join(str(a) for a in args))

        def fake_input(prompt):
            for line in printed:
                if "state=" in line:
                    state = line.split("state=")[1].split("&")[0]
                    return f"code=ABC123&state={state}"
            raise AssertionError("no auth URL printed")

        with mock.patch("scripts.spotify_client.requests.post") as mp, \
             mock.patch.object(client, "_save_token"):
            mp.return_value = _fake_response(200, {
                "access_token": "x", "expires_in": 3600,
            })
            client.authorize_headless(input_fn=fake_input, print_fn=fake_print)

        assert mp.call_args.kwargs["data"]["code"] == "ABC123"

    def test_missing_code_raises(self, monkeypatch):
        client = _make_client(monkeypatch)

        def fake_input(prompt):
            return "http://127.0.0.1:8888/callback?state=xyz"   # no 'code'

        with pytest.raises(SpotifyAuthError, match="no 'code' parameter"):
            client.authorize_headless(input_fn=fake_input, print_fn=lambda *a, **k: None)

    def test_state_mismatch_raises(self, monkeypatch):
        """CSRF guard: the state pasted back must match what was generated."""
        client = _make_client(monkeypatch)

        def fake_input(prompt):
            # state value that definitely doesn't match the generated one
            return "code=X&state=THIS_IS_NOT_THE_GENERATED_STATE"

        with pytest.raises(SpotifyAuthError, match="state mismatch"):
            client.authorize_headless(input_fn=fake_input, print_fn=lambda *a, **k: None)


# ---------------------------------------------------------------------------
# post — throttle/retry/backoff for POST
# ---------------------------------------------------------------------------
class TestPost:

    def test_201_returns_json(self, monkeypatch):
        client = _make_client(monkeypatch)
        with mock.patch.object(client._session, "post") as ms:
            ms.return_value = _fake_response(201, {"id": "new-thing"})
            result = client.post("/users/u1/playlists", json_body={"name": "x"})
        assert result == {"id": "new-thing"}

    def test_204_returns_empty_dict(self, monkeypatch):
        client = _make_client(monkeypatch)
        with mock.patch.object(client._session, "post") as ms:
            ms.return_value = _fake_response(204)
            result = client.post("/playlists/x/tracks", json_body={"uris": ["u1"]})
        assert result == {}

    def test_401_refreshes_token_and_retries(self, monkeypatch):
        client = _make_client(monkeypatch)

        # On 401 the loop nulls self._token; the next _ensure_token would
        # otherwise call _authorize() which tries to bind localhost:8888.
        # Stub _authorize to drop in a fresh fake token instead.
        def fake_reauth():
            client._token = {
                "access_token": "refreshed", "refresh_token": "r",
                "expires_at": 9_999_999_999,
            }

        with mock.patch.object(client._session, "post") as ms, \
             mock.patch.object(client, "_authorize", side_effect=fake_reauth):
            ms.side_effect = [
                _fake_response(401),
                _fake_response(200, {"ok": True}),
            ]
            result = client.post("/x")
        assert result == {"ok": True}
        assert ms.call_count == 2

    def test_500_then_200_succeeds(self, monkeypatch):
        client = _make_client(monkeypatch)
        with mock.patch.object(client._session, "post") as ms, \
             mock.patch("scripts.spotify_client.time.sleep"):
            ms.side_effect = [
                _fake_response(500),
                _fake_response(200, {"ok": True}),
            ]
            result = client.post("/x")
        assert result == {"ok": True}

    def test_4xx_raises_no_retry(self, monkeypatch):
        client = _make_client(monkeypatch)
        with mock.patch.object(client._session, "post") as ms:
            ms.return_value = _fake_response(400)
            ms.return_value.raise_for_status.side_effect = RuntimeError("HTTP 400")
            with pytest.raises(RuntimeError, match="HTTP 400"):
                client.post("/x")
            assert ms.call_count == 1   # no retry on 4xx-other


# ---------------------------------------------------------------------------
# create_playlist
# ---------------------------------------------------------------------------
class TestCreatePlaylist:

    def test_empty_name_rejected(self, monkeypatch):
        client = _make_client(monkeypatch)
        with pytest.raises(ValueError, match="non-empty"):
            client.create_playlist("")

    def test_posts_to_me_playlists(self, monkeypatch):
        client = _make_client(monkeypatch)
        with mock.patch.object(client, "post") as mp:
            mp.return_value = {"id": "playlist-42",
                               "external_urls": {"spotify": "https://x"}}
            result = client.create_playlist(
                "My Playlist", description="desc", public=True,
            )
        # Modern endpoint — NOT the legacy /users/{id}/playlists
        # (deprecated for new dev apps post-Nov 2024 — see method docstring).
        mp.assert_called_once_with(
            "/me/playlists",
            json_body={
                "name": "My Playlist",
                "description": "desc",
                "public": True,
                "collaborative": False,
            },
        )
        assert result["id"] == "playlist-42"

    def test_does_not_call_me_endpoint(self, monkeypatch):
        """No /me lookup needed — /me/playlists encodes the auth'd user."""
        client = _make_client(monkeypatch)
        with mock.patch.object(client, "get") as mg, \
             mock.patch.object(client, "post") as mp:
            mp.return_value = {"id": "p"}
            client.create_playlist("a")
            client.create_playlist("b")
            client.create_playlist("c")
        assert mg.call_count == 0   # no wasted /me calls


# ---------------------------------------------------------------------------
# add_tracks_to_playlist
# ---------------------------------------------------------------------------
class TestAddTracksToPlaylist:

    def test_empty_playlist_id_rejected(self, monkeypatch):
        client = _make_client(monkeypatch)
        with pytest.raises(ValueError, match="playlist_id"):
            client.add_tracks_to_playlist("", ["spotify:track:x"])

    def test_single_chunk(self, monkeypatch):
        client = _make_client(monkeypatch)
        uris = [f"spotify:track:t{i}" for i in range(50)]
        with mock.patch.object(client, "post") as mp:
            mp.return_value = {}
            n = client.add_tracks_to_playlist("p1", uris)
        assert n == 50
        assert mp.call_count == 1
        assert mp.call_args.args[0] == "/playlists/p1/tracks"
        assert len(mp.call_args.kwargs["json_body"]["uris"]) == 50

    def test_chunks_at_100_per_request(self, monkeypatch):
        client = _make_client(monkeypatch)
        uris = [f"spotify:track:t{i}" for i in range(250)]
        with mock.patch.object(client, "post") as mp:
            mp.return_value = {}
            n = client.add_tracks_to_playlist("p1", uris)
        assert n == 250
        assert mp.call_count == 3   # 100 + 100 + 50
        # Verify chunk sizes
        chunk_sizes = [len(c.kwargs["json_body"]["uris"]) for c in mp.call_args_list]
        assert chunk_sizes == [100, 100, 50]

    def test_empty_uris_no_calls(self, monkeypatch):
        client = _make_client(monkeypatch)
        with mock.patch.object(client, "post") as mp:
            n = client.add_tracks_to_playlist("p1", [])
        assert n == 0
        assert mp.call_count == 0
