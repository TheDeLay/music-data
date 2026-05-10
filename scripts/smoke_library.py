"""Smoke test: confirm PUT/DELETE /v1/me/tracks works under Spotify's
March 2026 dev-mode restrictions.

The Feb/March 2026 wall blocked POST /v1/playlists/{id}/tracks but the
Library scope is a separate codepath. This script proves (or disproves)
that the Liked-Songs feedback loop is unblocked for our app credentials.

Five API calls, fully reversible. Uses Rick Astley's "Never Gonna Give
You Up" as a canary URI. If pre-check shows the canary is already in
your library, the script aborts rather than risk an accidental delete.

Run:
    python -m scripts.smoke_library
"""
from __future__ import annotations

import sys

import requests
from dotenv import load_dotenv

from scripts.spotify_client import API_BASE, SpotifyClient


CANARY_TRACK_ID = "4cOdK2wGLETKBW3PvgPWqT"  # Rick Astley — Never Gonna Give You Up
CONTAINS_PATH = "/me/tracks/contains"
LIBRARY_PATH = "/me/tracks"


def _contains(client: SpotifyClient, track_id: str) -> bool:
    body = client.get(CONTAINS_PATH, params={"ids": track_id})
    return bool(body and body[0])


def _put_save(client: SpotifyClient, track_id: str) -> int:
    tok = client._ensure_token()
    r = requests.put(
        f"{API_BASE}{LIBRARY_PATH}",
        headers={"Authorization": f"Bearer {tok}",
                 "Content-Type": "application/json"},
        params={"ids": track_id},
        timeout=30,
    )
    return r.status_code


def _delete_unsave(client: SpotifyClient, track_id: str) -> int:
    tok = client._ensure_token()
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

    print(f"Canary: spotify:track:{CANARY_TRACK_ID} (Rick Astley — Never Gonna Give You Up)")
    print()

    print("[1/5] Pre-check contains...")
    pre = _contains(client, CANARY_TRACK_ID)
    print(f"      contains = {pre}")
    if pre:
        print()
        print("ABORT: canary is already in your library. Pick a different track")
        print("       or unsave it manually first. No changes made.")
        return 2

    print()
    print("[2/5] PUT /me/tracks (save)...")
    put_code = _put_save(client, CANARY_TRACK_ID)
    print(f"      HTTP {put_code}")

    print()
    print("[3/5] Verify contains flipped to True...")
    mid = _contains(client, CANARY_TRACK_ID)
    print(f"      contains = {mid}")

    print()
    print("[4/5] DELETE /me/tracks (unsave)...")
    del_code = _delete_unsave(client, CANARY_TRACK_ID)
    print(f"      HTTP {del_code}")

    print()
    print("[5/5] Verify contains flipped back to False...")
    post = _contains(client, CANARY_TRACK_ID)
    print(f"      contains = {post}")

    print()
    print("=" * 60)
    ok = (put_code in (200, 204)
          and del_code in (200, 204)
          and mid is True
          and post is False)
    if ok:
        print("VERDICT: Liked-Songs feedback loop is unblocked.")
        print("         PUT 200/204, DELETE 200/204, contains flips correctly.")
        return 0
    else:
        print("VERDICT: something is off. Investigate before building like_tracks.py.")
        print(f"         put={put_code} del={del_code} mid={mid} post={post}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
