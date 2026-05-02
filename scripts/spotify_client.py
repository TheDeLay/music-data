"""Thin wrapper around the Spotify Web API.

Handles:
- OAuth 2.0 Authorization Code flow with PKCE
- Token caching to .spotify_token.json
- Automatic refresh when access tokens expire
- Rate-limit handling with backoff on 429
- Convenience methods for the endpoints we actually use

We deliberately don't pull in spotipy or similar — the surface area we need
is small, and rolling our own keeps deps minimal and behavior auditable.
"""
from __future__ import annotations

import base64
import hashlib
import http.server
import json
import os
import secrets
import socketserver
import threading
import time
import urllib.parse
import webbrowser
from pathlib import Path
from typing import Any, Iterator

import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
TOKEN_PATH = PROJECT_ROOT / ".spotify_token.json"

AUTH_URL = "https://accounts.spotify.com/authorize"
TOKEN_URL = "https://accounts.spotify.com/api/token"
API_BASE = "https://api.spotify.com/v1"

# Scopes we need:
# - user-read-recently-played: /me/player/recently-played
# - user-read-private: basic profile (helpful for sanity checks)
# - playlist-modify-private + playlist-modify-public: future playlist creation
DEFAULT_SCOPES = (
    "user-read-recently-played "
    "user-read-private "
    "user-top-read "
    "playlist-modify-private "
    "playlist-modify-public"
)


class SpotifyAuthError(Exception):
    pass


class SpotifyClient:
    def __init__(self, client_id: str | None = None, client_secret: str | None = None,
                 redirect_uri: str | None = None, scopes: str = DEFAULT_SCOPES,
                 auth: str = "user",
                 user_agent: str = "TheDeLay-Music-Data/0.1 (personal listening archive)",
                 min_request_interval: float = 1.0):
        # auth="user": Authorization Code w/ PKCE — needed for /me/* and playlist mod
        # auth="app":  Client Credentials — public catalog only, no browser, unattended
        if auth not in ("user", "app"):
            raise ValueError(f"auth must be 'user' or 'app', got {auth!r}")
        self.auth_mode = auth
        self.client_id = client_id or os.environ.get("SPOTIFY_CLIENT_ID", "")
        self.client_secret = client_secret or os.environ.get("SPOTIFY_CLIENT_SECRET", "")
        self.redirect_uri = redirect_uri or os.environ.get(
            "SPOTIFY_REDIRECT_URI", "http://127.0.0.1:8888/callback"
        )
        self.scopes = scopes
        if not self.client_id or not self.client_secret:
            raise SpotifyAuthError(
                "Spotify credentials missing. Set SPOTIFY_CLIENT_ID and "
                "SPOTIFY_CLIENT_SECRET in .env (or env vars)."
            )
        self._token: dict[str, Any] | None = self._load_token() if auth == "user" else None
        self._app_token: dict[str, Any] | None = None  # in-memory only, 1h TTL
        self._session = requests.Session()
        self._session.headers["User-Agent"] = user_agent
        # Throttle: minimum seconds between requests. 1.0 = 60 req/min sustained,
        # well under any plausible rate limit and friendly under TOS §VI.4.
        self._min_interval = float(min_request_interval)
        self._last_request_at = 0.0
        # Global 429 backoff: when Spotify returns 429, ALL subsequent calls wait
        # until this timestamp. Without this, after a 429 we'd retry the one URL
        # but immediately hammer the next URL — same bucket, same problem.
        self._backoff_until = 0.0

    # -------------------------------------------------------------------------
    # Token management
    # -------------------------------------------------------------------------
    def _load_token(self) -> dict | None:
        if not TOKEN_PATH.exists():
            return None
        try:
            return json.loads(TOKEN_PATH.read_text())
        except Exception:
            return None

    def _save_token(self, tok: dict) -> None:
        # Add absolute expiry for easier refresh logic
        if "expires_at" not in tok and "expires_in" in tok:
            tok["expires_at"] = int(time.time()) + int(tok["expires_in"]) - 60
        TOKEN_PATH.write_text(json.dumps(tok, indent=2))
        TOKEN_PATH.chmod(0o600)
        self._token = tok

    def _ensure_token(self) -> str:
        """Return a valid access token, refreshing or re-authing as needed."""
        if self.auth_mode == "app":
            return self._ensure_app_token()
        if self._token and self._token.get("expires_at", 0) > time.time():
            return self._token["access_token"]
        if self._token and "refresh_token" in self._token:
            try:
                self._refresh()
                return self._token["access_token"]
            except SpotifyAuthError:
                pass  # fall through to full re-auth
        self._authorize()
        return self._token["access_token"]

    def _ensure_app_token(self) -> str:
        """Client Credentials grant — no user, no browser, no refresh token.

        Tokens last 1h. We just request a new one when expired. Stays in
        memory only — no disk cache, since the secret can mint a fresh one
        any time and a stale token file would only be a leak risk.
        """
        if self._app_token and self._app_token.get("expires_at", 0) > time.time():
            return self._app_token["access_token"]
        basic = base64.b64encode(f"{self.client_id}:{self.client_secret}".encode()).decode()
        resp = requests.post(
            TOKEN_URL,
            headers={"Authorization": f"Basic {basic}"},
            data={"grant_type": "client_credentials"},
            timeout=30,
        )
        if resp.status_code != 200:
            raise SpotifyAuthError(f"client_credentials grant failed: {resp.status_code} {resp.text}")
        tok = resp.json()
        tok["expires_at"] = int(time.time()) + int(tok.get("expires_in", 3600)) - 60
        self._app_token = tok
        return tok["access_token"]

    def _refresh(self) -> None:
        if not self._token or "refresh_token" not in self._token:
            raise SpotifyAuthError("no refresh token available")
        basic = base64.b64encode(f"{self.client_id}:{self.client_secret}".encode()).decode()
        resp = requests.post(
            TOKEN_URL,
            headers={"Authorization": f"Basic {basic}"},
            data={"grant_type": "refresh_token", "refresh_token": self._token["refresh_token"]},
            timeout=30,
        )
        if resp.status_code != 200:
            raise SpotifyAuthError(f"refresh failed: {resp.status_code} {resp.text}")
        new_tok = resp.json()
        # Spotify sometimes omits refresh_token in refresh responses; preserve old one.
        new_tok.setdefault("refresh_token", self._token["refresh_token"])
        self._save_token(new_tok)

    # -------------------------------------------------------------------------
    # OAuth Authorization Code with PKCE (one-time, opens browser)
    # -------------------------------------------------------------------------
    def _authorize(self) -> None:
        verifier = secrets.token_urlsafe(64)
        challenge = base64.urlsafe_b64encode(
            hashlib.sha256(verifier.encode()).digest()
        ).decode().rstrip("=")
        state = secrets.token_urlsafe(16)

        params = {
            "response_type": "code",
            "client_id": self.client_id,
            "redirect_uri": self.redirect_uri,
            "scope": self.scopes,
            "state": state,
            "code_challenge_method": "S256",
            "code_challenge": challenge,
        }
        url = f"{AUTH_URL}?{urllib.parse.urlencode(params)}"

        # Tiny callback server to catch the redirect
        parsed = urllib.parse.urlparse(self.redirect_uri)
        port = parsed.port or 8888
        captured: dict[str, str] = {}

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802
                qs = urllib.parse.urlparse(self.path).query
                params = urllib.parse.parse_qs(qs)
                if "code" in params:
                    captured["code"] = params["code"][0]
                    captured["state"] = params.get("state", [""])[0]
                self.send_response(200)
                self.send_header("Content-Type", "text/html")
                self.end_headers()
                self.wfile.write(b"<h2>Auth complete. You can close this tab.</h2>")
            def log_message(self, *a, **kw): pass  # silence

        server = socketserver.TCPServer(("localhost", port), Handler)
        thread = threading.Thread(target=server.handle_request, daemon=True)
        thread.start()

        print(f"Opening browser for Spotify authorization: {url}")
        webbrowser.open(url)
        thread.join(timeout=300)
        server.server_close()

        if not captured.get("code") or captured.get("state") != state:
            raise SpotifyAuthError("authorization callback did not return a valid code")

        # Exchange code for tokens
        resp = requests.post(
            TOKEN_URL,
            data={
                "grant_type": "authorization_code",
                "code": captured["code"],
                "redirect_uri": self.redirect_uri,
                "client_id": self.client_id,
                "code_verifier": verifier,
            },
            auth=(self.client_id, self.client_secret),
            timeout=30,
        )
        if resp.status_code != 200:
            raise SpotifyAuthError(f"token exchange failed: {resp.status_code} {resp.text}")
        self._save_token(resp.json())

    # -------------------------------------------------------------------------
    # Generic GET with retry/backoff
    # -------------------------------------------------------------------------
    def _throttle(self) -> None:
        """Block until both the per-call interval AND any global 429 backoff have elapsed."""
        now = time.time()
        wait = max(
            self._min_interval - (now - self._last_request_at) if self._min_interval > 0 else 0,
            self._backoff_until - now,
        )
        if wait > 0:
            time.sleep(wait)

    def get(self, path: str, params: dict | None = None,
            allow_404: bool = False) -> dict | None:
        """GET an endpoint, with throttling, retry/backoff, and optional 404-as-None.

        Set allow_404=True for entity-by-id calls where 'not found' is a valid
        outcome (deleted track, removed artist), and you want to keep going.
        """
        url = path if path.startswith("http") else f"{API_BASE}{path}"
        for attempt in range(5):
            self._throttle()
            tok = self._ensure_token()
            resp = self._session.get(
                url,
                headers={"Authorization": f"Bearer {tok}"},
                params=params,
                timeout=30,
            )
            self._last_request_at = time.time()
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 204:
                return {}
            if resp.status_code == 404 and allow_404:
                return None
            if resp.status_code == 401:
                # Token expired between check and use; force-refresh and retry
                if self.auth_mode == "app":
                    self._app_token = None
                else:
                    self._token = None
                continue
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", "10"))
                # Global backoff: every subsequent call sees this too.
                # Cap at 5 minutes per single 429 to prevent a single bad
                # response from stalling forever.
                wait = min(wait, 300)
                self._backoff_until = max(self._backoff_until, time.time() + wait)
                time.sleep(wait)
                continue
            if 500 <= resp.status_code < 600:
                time.sleep(2 ** attempt)
                continue
            resp.raise_for_status()
        raise RuntimeError(f"giving up on {url} after retries")

    # -------------------------------------------------------------------------
    # Convenience endpoints
    # -------------------------------------------------------------------------
    def me(self) -> dict:
        return self.get("/me")

    def recently_played(self, limit: int = 50) -> list[dict]:
        """Return up to 50 most recent plays (the API max)."""
        data = self.get("/me/player/recently-played", params={"limit": min(limit, 50)})
        return data.get("items", [])

    # -------------------------------------------------------------------------
    # Single-ID fetches.
    #
    # Spotify's batch endpoints (/tracks?ids=, /artists?ids=, /albums?ids=)
    # return 403 Forbidden for newly-created Development-Mode apps as of the
    # February 2026 API tightening. Single-ID GETs (/tracks/{id} etc.) still
    # work, so we use those. If/when this app is approved for Extended Quota
    # Mode, batch endpoints unlock and these can be re-batched for speed.
    #
    # Returned list is parallel to `uris`: index i is the entity for uris[i],
    # or None if Spotify returned 404 (deleted/removed/regional unavailability)
    # or if retries were exhausted (rate limit / transient 5xx). The caller
    # treats None as "skip this one for now, try again on a later run" — this
    # preserves chunk-level partial progress instead of failing the whole batch.
    # -------------------------------------------------------------------------
    def _fetch_one(self, path: str) -> dict | None:
        """Single-ID fetch that swallows retry-exhaustion as None.

        404 already returns None (allow_404=True). RuntimeError from giving up
        after 5 retries is also coerced to None so a single bad ID can't kill
        a whole enrichment chunk.
        """
        try:
            return self.get(path, allow_404=True)
        except RuntimeError:
            return None

    def get_tracks(self, uris: list[str]) -> list[dict | None]:
        return [self._fetch_one(f"/tracks/{uri.split(':')[-1]}") for uri in uris]

    def get_artists(self, uris: list[str]) -> list[dict | None]:
        return [self._fetch_one(f"/artists/{uri.split(':')[-1]}") for uri in uris]

    def get_albums(self, uris: list[str]) -> list[dict | None]:
        return [self._fetch_one(f"/albums/{uri.split(':')[-1]}") for uri in uris]

    def search_artist(self, name: str) -> dict | None:
        """Return the first matching artist for a name, or None."""
        data = self.get("/search", params={"q": name, "type": "artist", "limit": 1})
        items = (data.get("artists") or {}).get("items") or []
        return items[0] if items else None


def _chunks(items: list, size: int) -> Iterator[list]:
    for i in range(0, len(items), size):
        yield items[i:i + size]
