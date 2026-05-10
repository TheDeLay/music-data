"""One-time CLI to bootstrap Spotify user OAuth.

Two modes:
  - default: open a browser, listen on localhost for the redirect (works
    on machines with a desktop + browser + ability to bind 8888)
  - --headless: print the auth URL, wait for the user to paste back the
    redirect URL from any browser (works on headless servers, over SSH,
    or on hosts where the local 8888 listener can't bind)

Usage:
    python -m scripts.auth_spotify              # browser flow (default)
    python -m scripts.auth_spotify --headless   # paste-back flow

Either way, on success the token is saved to .spotify_token.json (gitignored
+ SyncThing-excluded as of 2026-05-09). Subsequent SpotifyClient(auth='user')
constructions pick it up automatically and refresh as needed.
"""
from __future__ import annotations

import argparse
import sys

from dotenv import load_dotenv

from scripts.spotify_client import (
    SpotifyAuthError,
    SpotifyClient,
    TOKEN_PATH,
)


load_dotenv()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Bootstrap Spotify user-OAuth (one-time).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Use paste-back flow instead of opening a browser locally. "
             "Useful for SSH'd-in sessions and headless servers.",
    )
    args = parser.parse_args(argv)

    print(f"Token path: {TOKEN_PATH}")
    if TOKEN_PATH.exists():
        print(f"Note: a token already exists at the path above. Re-running this "
              f"script will overwrite it.")

    try:
        client = SpotifyClient(auth="user")
    except SpotifyAuthError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 3

    try:
        if args.headless:
            client.authorize_headless()
        else:
            client._authorize()
    except SpotifyAuthError as e:
        print(f"ERROR: auth failed: {e}", file=sys.stderr)
        return 1

    # Sanity-check by hitting /me
    try:
        me = client.me()
    except Exception as e:
        print(f"WARNING: token saved but /me sanity check failed: {e}",
              file=sys.stderr)
        return 0
    print(f"OK Authenticated as {me.get('display_name', '?')!r} "
          f"(id={me.get('id', '?')!r}, country={me.get('country', '?')!r})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
