"""Smoke test for /me/tracks PUT/DELETE under Spotify's March 2026 dev-mode wall.

VERDICT (verified 2026-05-10 against TheDeLay-Music-Data app):
  GET  /me                         → 200
  GET  /me/tracks?limit=N          → 200
  GET  /me/tracks/contains?ids=... → 403 (blocked)
  PUT  /me/tracks?ids=...          → 403 (blocked)
  DELETE /me/tracks?ids=...        → 403 (blocked)
  GET  /me/player/recently-played  → 200
  GET  /me/top/tracks              → 200

Spotify's dev-mode wall (post-Nov-2024 apps) extends to ALL user-state
writes — PUT/DELETE on the Liked-Songs library and the /contains lookup —
not just the documented POST /v1/playlists/{id}/tracks migration.

The Nov 2024 official restriction list does NOT enumerate library writes
as restricted; this is undocumented. Empirical only. See memory entry
`reference_spotify_user_state_writes_blocked_2026.md`.

Practical implication: a "feed engagement-DB picks into Spotify Liked
Songs to teach their recommender" feature is not buildable under the
current dev-mode tier. The TuneMyMusic text-import bridge (`playlist.py
--format text`) remains the only path to push curated tracks into a
Spotify account.

This script is preserved as documented evidence of the wall, not as a
working tool. The 5-call probe has been reshaped to use list-based
verification instead of /me/tracks/contains (which 403s) so it can run
to completion and produce a clean 'VERDICT: blocked' report rather than
crashing on the first call.

Run:
    python -m scripts.smoke_library
"""
from __future__ import annotations

import sys

import requests
from dotenv import load_dotenv

from scripts.spotify_client import API_BASE, SpotifyClient


CANARY_TRACK_ID = "4cOdK2wGLETKBW3PvgPWqT"  # Rick Astley — Never Gonna Give You Up
LIBRARY_PATH = "/me/tracks"


def _list_recent_saved_ids(tok: str, limit: int = 50) -> tuple[list[str] | None, int]:
    """Return (ids, status_code). ids is None if the call failed."""
    r = requests.get(
        f"{API_BASE}{LIBRARY_PATH}",
        headers={"Authorization": f"Bearer {tok}"},
        params={"limit": limit},
        timeout=30,
    )
    if r.status_code != 200:
        return None, r.status_code
    items = r.json().get("items", [])
    return [it["track"]["id"] for it in items if it.get("track")], r.status_code


def _put_save(tok: str, track_id: str) -> int:
    r = requests.put(
        f"{API_BASE}{LIBRARY_PATH}",
        headers={"Authorization": f"Bearer {tok}",
                 "Content-Type": "application/json"},
        params={"ids": track_id},
        timeout=30,
    )
    return r.status_code


def _delete_unsave(tok: str, track_id: str) -> int:
    r = requests.delete(
        f"{API_BASE}{LIBRARY_PATH}",
        headers={"Authorization": f"Bearer {tok}",
                 "Content-Type": "application/json"},
        params={"ids": track_id},
        timeout=30,
    )
    return r.status_code


def main() -> int:
    load_dotenv()
    client = SpotifyClient(auth="user")
    tok = client._ensure_token()

    print(f"Canary: spotify:track:{CANARY_TRACK_ID} (Rick Astley — Never Gonna Give You Up)")
    print()

    print("[1/5] Pre-check (list 50 most-recently-saved)...")
    ids, code = _list_recent_saved_ids(tok)
    if ids is None:
        print(f"      HTTP {code} — list endpoint also blocked. Cannot proceed.")
        return 3
    pre = CANARY_TRACK_ID in ids
    print(f"      HTTP {code}, canary present = {pre}")
    if pre:
        print()
        print("ABORT: canary is already in your library. Pick a different track")
        print("       or unsave it manually first. No changes made.")
        return 2

    print()
    print("[2/5] PUT /me/tracks (save)...")
    put_code = _put_save(tok, CANARY_TRACK_ID)
    print(f"      HTTP {put_code}")

    print()
    print("[3/5] Verify (list 50 most-recently-saved)...")
    ids_mid, code_mid = _list_recent_saved_ids(tok)
    mid = ids_mid is not None and CANARY_TRACK_ID in ids_mid
    print(f"      HTTP {code_mid}, canary present = {mid}")

    print()
    print("[4/5] DELETE /me/tracks (unsave)...")
    del_code = _delete_unsave(tok, CANARY_TRACK_ID)
    print(f"      HTTP {del_code}")

    print()
    print("[5/5] Verify removed (list 50 most-recently-saved)...")
    ids_post, code_post = _list_recent_saved_ids(tok)
    post = ids_post is not None and CANARY_TRACK_ID in ids_post
    print(f"      HTTP {code_post}, canary present = {post}")

    print()
    print("=" * 60)
    ok = (put_code in (200, 204)
          and del_code in (200, 204)
          and mid is True
          and post is False)
    if ok:
        print("VERDICT: Liked-Songs feedback loop IS UNBLOCKED.")
        print("         (This would be a surprise — historical verdict is BLOCKED.)")
        return 0
    else:
        print("VERDICT: BLOCKED. Spotify dev-mode wall extends to library writes.")
        print(f"         put={put_code} del={del_code} after_save={mid} after_del={post}")
        print()
        print("This matches the wall pattern documented in the file header and")
        print("in memory entry `reference_spotify_user_state_writes_blocked_2026.md`.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
