"""CLI: enrich entities with Spotify Web API metadata.

This fills in:
- artists: spotify_artist_uri, genres_json, popularity, followers
- albums:  spotify_album_uri, release_date, release_year, album_type, total_tracks
- tracks:  duration_ms (critical for percent_played!), explicit, popularity, isrc,
           proper album linkage, full track_artists list

Strategy:
1. Track enrichment first — uses the spotify_track_uri we already have.
   This populates duration_ms and gives us authoritative album/artist URIs.
2. Album enrichment — for albums newly linked from track enrichment.
3. Artist enrichment — same.

Run repeatedly: it picks up where it left off (last_enriched_at IS NULL).

Usage:
    python scripts/enrich.py --tracks
    python scripts/enrich.py --albums
    python scripts/enrich.py --artists
    python scripts/enrich.py --all
    python scripts/enrich.py --all --refresh-older-than 90d
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta

from rich.console import Console
from rich.progress import Progress, BarColumn, MofNCompleteColumn, TextColumn, TimeElapsedColumn

from .db import connect, init_schema, start_run, finish_run
from .spotify_client import SpotifyClient

console = Console()
# Single-ID per call (batch endpoints 403 for new Dev apps post Feb 2026).
# These constants control how many we process per DB transaction, not API batch.
TRACK_BATCH = 25
ALBUM_BATCH = 25
ARTIST_BATCH = 25


def _parse_release_year(release_date: str | None) -> int | None:
    if not release_date:
        return None
    try:
        return int(release_date[:4])
    except ValueError:
        return None


def _parse_age(spec: str | None) -> timedelta | None:
    """Parse '90d', '12h', '2w' into a timedelta. Returns None if spec is None."""
    if not spec:
        return None
    spec = spec.strip().lower()
    if spec.endswith("d"):
        return timedelta(days=int(spec[:-1]))
    if spec.endswith("h"):
        return timedelta(hours=int(spec[:-1]))
    if spec.endswith("w"):
        return timedelta(weeks=int(spec[:-1]))
    raise ValueError(f"can't parse age spec: {spec!r}")


def _age_clause(refresh_older_than: timedelta | None, col: str = "last_enriched_at") -> tuple[str, list]:
    """Build an SQL fragment for the staleness filter. Returns (sql, params)."""
    if refresh_older_than is None:
        return f"{col} IS NULL", []
    cutoff = (datetime.utcnow() - refresh_older_than).strftime("%Y-%m-%d %H:%M:%S")
    return f"({col} IS NULL OR {col} < ?)", [cutoff]


def _select_track_targets(conn, refresh_older_than, min_plays: int):
    """Tracks needing enrichment, filtered to those with at least min_plays."""
    age_sql, params = _age_clause(refresh_older_than, "t.last_enriched_at")
    if min_plays <= 1:
        return conn.execute(
            f"SELECT t.track_id AS id, t.spotify_track_uri AS uri "
            f"FROM tracks t WHERE {age_sql}",
            params,
        ).fetchall()
    return conn.execute(
        f"""
        SELECT t.track_id AS id, t.spotify_track_uri AS uri
        FROM tracks t
        JOIN (
            SELECT track_id, COUNT(*) AS c
            FROM plays WHERE content_type = 'track'
            GROUP BY track_id
            HAVING c >= ?
        ) tp ON t.track_id = tp.track_id
        WHERE {age_sql}
        """,
        [min_plays] + params,
    ).fetchall()


def _select_album_targets(conn, refresh_older_than, min_plays: int):
    """Albums with a URI and unenriched, where any track on the album has >= min_plays plays.

    Note: only albums whose URI has been populated (by track enrichment) are
    candidates. Albums without a URI can't be looked up and stay unenriched.
    """
    age_sql, params = _age_clause(refresh_older_than, "al.last_enriched_at")
    base = f"al.spotify_album_uri IS NOT NULL AND {age_sql}"
    if min_plays <= 1:
        return conn.execute(
            f"SELECT al.album_id AS id, al.spotify_album_uri AS uri FROM albums al WHERE {base}",
            params,
        ).fetchall()
    return conn.execute(
        f"""
        SELECT al.album_id AS id, al.spotify_album_uri AS uri
        FROM albums al
        WHERE {base}
          AND EXISTS (
            SELECT 1 FROM tracks t
            JOIN plays p ON p.track_id = t.track_id AND p.content_type = 'track'
            WHERE t.album_id = al.album_id
            GROUP BY t.album_id
            HAVING COUNT(*) >= ?
          )
        """,
        params + [min_plays],
    ).fetchall()


def _select_artist_uri_targets(conn, refresh_older_than, min_plays: int):
    """Artists with a URI and unenriched, where the artist's total play count >= min_plays."""
    age_sql, params = _age_clause(refresh_older_than, "ar.last_enriched_at")
    base = f"ar.spotify_artist_uri IS NOT NULL AND {age_sql}"
    if min_plays <= 1:
        return conn.execute(
            f"SELECT ar.artist_id AS id, ar.spotify_artist_uri AS uri FROM artists ar WHERE {base}",
            params,
        ).fetchall()
    return conn.execute(
        f"""
        SELECT ar.artist_id AS id, ar.spotify_artist_uri AS uri
        FROM artists ar
        WHERE {base}
          AND (
            SELECT COUNT(*) FROM plays p
            JOIN tracks t ON p.track_id = t.track_id
            JOIN track_artists ta ON t.track_id = ta.track_id AND ta.position = 0
            WHERE ta.artist_id = ar.artist_id AND p.content_type = 'track'
          ) >= ?
        """,
        params + [min_plays],
    ).fetchall()


def _select_artist_name_targets(conn, min_plays: int):
    """Name-only artists (no URI yet) whose total play count >= min_plays.

    These are the ones worth resolving via /search. The long tail (single plays
    by random artists) is intentionally skipped to stay polite to the API.
    """
    if min_plays <= 1:
        return conn.execute(
            "SELECT artist_id, name FROM artists WHERE spotify_artist_uri IS NULL"
        ).fetchall()
    return conn.execute(
        """
        SELECT ar.artist_id, ar.name
        FROM artists ar
        WHERE ar.spotify_artist_uri IS NULL
          AND (
            SELECT COUNT(*) FROM plays p
            JOIN tracks t ON p.track_id = t.track_id
            JOIN track_artists ta ON t.track_id = ta.track_id AND ta.position = 0
            WHERE ta.artist_id = ar.artist_id AND p.content_type = 'track'
          ) >= ?
        """,
        (min_plays,),
    ).fetchall()


# -----------------------------------------------------------------------------
# Track enrichment
# -----------------------------------------------------------------------------
def enrich_tracks(conn, client: SpotifyClient, run_id: int, refresh_older_than, min_plays: int = 1) -> int:
    rows = _select_track_targets(conn, refresh_older_than, min_plays)
    if not rows:
        console.print("[dim]No tracks need enrichment.[/dim]")
        return 0

    console.print(f"Enriching {len(rows):,} track(s)...")
    updated = 0
    with Progress(TextColumn("Tracks"), BarColumn(), MofNCompleteColumn(),
                  TimeElapsedColumn(), console=console) as progress:
        task = progress.add_task("", total=len(rows))
        for i in range(0, len(rows), TRACK_BATCH):
            chunk = rows[i:i + TRACK_BATCH]
            uris = [r["uri"] for r in chunk]
            try:
                api_tracks = client.get_tracks(uris)
            except Exception as e:
                console.print(f"[yellow]Track batch failed: {e}[/yellow]")
                progress.advance(task, len(chunk))
                continue

            conn.execute("BEGIN")
            try:
                for uri, api_track in zip(uris, api_tracks):
                    if api_track is None:
                        # None = either 404 (deleted) or retries exhausted (rate limit /
                        # transient 5xx). In either case, leave last_enriched_at NULL
                        # so a future run picks it up. The cost of re-trying a truly
                        # deleted track is one wasted call per run — fine. The cost of
                        # marking an exhausted-retry track as 'enriched' would be
                        # losing it forever from the resume queue.
                        continue

                    # Resolve / create album
                    album_id = None
                    api_album = api_track.get("album") or {}
                    album_uri = api_album.get("uri")
                    if album_uri:
                        album_id = _upsert_album_from_api(conn, api_album)

                    # Update the track row
                    conn.execute(
                        """
                        UPDATE tracks
                        SET name = ?, album_id = COALESCE(?, album_id),
                            duration_ms = ?, explicit = ?, popularity = ?,
                            isrc = ?, last_enriched_at = datetime('now')
                        WHERE spotify_track_uri = ?
                        """,
                        (
                            api_track.get("name"),
                            album_id,
                            api_track.get("duration_ms"),
                            int(bool(api_track.get("explicit"))) if api_track.get("explicit") is not None else None,
                            api_track.get("popularity"),
                            (api_track.get("external_ids") or {}).get("isrc"),
                            uri,
                        ),
                    )

                    # Resolve / create artists, then sync track_artists join rows
                    track_id_row = conn.execute(
                        "SELECT track_id FROM tracks WHERE spotify_track_uri = ?", (uri,)
                    ).fetchone()
                    if track_id_row:
                        track_id = track_id_row["track_id"]
                        api_artists = api_track.get("artists") or []
                        # Replace track_artists rows for this track (positions may have changed)
                        conn.execute("DELETE FROM track_artists WHERE track_id = ?", (track_id,))
                        for pos, ar in enumerate(api_artists):
                            artist_id = _upsert_artist_from_api(conn, ar)
                            conn.execute(
                                "INSERT OR IGNORE INTO track_artists (track_id, artist_id, position) "
                                "VALUES (?, ?, ?)",
                                (track_id, artist_id, pos),
                            )
                    updated += 1
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

            progress.advance(task, len(chunk))
    return updated


def _upsert_album_from_api(conn, api_album: dict) -> int:
    """Insert or update an album from API data; return album_id."""
    uri = api_album.get("uri")
    name = api_album.get("name") or "(unknown)"
    norm = name.strip().lower()
    release_date = api_album.get("release_date")

    row = conn.execute(
        "SELECT album_id FROM albums WHERE spotify_album_uri = ? LIMIT 1", (uri,)
    ).fetchone()
    if not row:
        # Try to attach to an existing albums row by normalized name (created at ingest)
        row = conn.execute(
            "SELECT album_id FROM albums WHERE spotify_album_uri IS NULL AND name_normalized = ? LIMIT 1",
            (norm,),
        ).fetchone()

    if row:
        conn.execute(
            """
            UPDATE albums
            SET spotify_album_uri = COALESCE(spotify_album_uri, ?),
                name = ?, name_normalized = ?,
                release_date = ?, release_year = ?,
                album_type = ?, total_tracks = ?,
                last_enriched_at = datetime('now')
            WHERE album_id = ?
            """,
            (uri, name, norm, release_date, _parse_release_year(release_date),
             api_album.get("album_type"), api_album.get("total_tracks"), row["album_id"]),
        )
        return row["album_id"]

    cur = conn.execute(
        """
        INSERT INTO albums (spotify_album_uri, name, name_normalized, release_date,
                            release_year, album_type, total_tracks, last_enriched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
        """,
        (uri, name, norm, release_date, _parse_release_year(release_date),
         api_album.get("album_type"), api_album.get("total_tracks")),
    )
    return cur.lastrowid


def _upsert_artist_from_api(conn, api_artist: dict) -> int:
    """Insert or update an artist; return artist_id.

    The simplified artist object on a track only has uri+name. Fuller details
    (genres, popularity, followers) come from enrich_artists() which queries
    /artists directly.
    """
    uri = api_artist.get("uri")
    name = api_artist.get("name") or "(unknown)"
    norm = name.strip().lower()

    row = conn.execute(
        "SELECT artist_id FROM artists WHERE spotify_artist_uri = ? LIMIT 1", (uri,)
    ).fetchone()
    if not row:
        row = conn.execute(
            "SELECT artist_id FROM artists WHERE spotify_artist_uri IS NULL AND name_normalized = ? LIMIT 1",
            (norm,),
        ).fetchone()

    if row:
        conn.execute(
            "UPDATE artists SET spotify_artist_uri = COALESCE(spotify_artist_uri, ?), "
            "name = ?, name_normalized = ? WHERE artist_id = ?",
            (uri, name, norm, row["artist_id"]),
        )
        return row["artist_id"]

    cur = conn.execute(
        "INSERT INTO artists (spotify_artist_uri, name, name_normalized) VALUES (?, ?, ?)",
        (uri, name, norm),
    )
    return cur.lastrowid


# -----------------------------------------------------------------------------
# Artist enrichment (genres + popularity + followers)
# -----------------------------------------------------------------------------
def enrich_artists(conn, client: SpotifyClient, run_id: int, refresh_older_than, min_plays: int = 1) -> int:
    """Fill in genres/popularity/followers for artists with a URI but missing detail."""
    rows = list(_select_artist_uri_targets(conn, refresh_older_than, min_plays))

    # Also: artists with NO URI yet whose play-count meets the threshold —
    # resolve via /search. (Long-tail single-play artists are intentionally skipped.)
    name_only = _select_artist_name_targets(conn, min_plays)

    # Resolve name-only artists by search
    if name_only:
        console.print(f"Resolving {len(name_only)} name-only artist(s) via search...")
        with Progress(TextColumn("Search"), BarColumn(), MofNCompleteColumn(),
                      TimeElapsedColumn(), console=console) as progress:
            task = progress.add_task("", total=len(name_only))
            for r in name_only:
                try:
                    found = client.search_artist(r["name"])
                except Exception:
                    progress.advance(task)
                    continue
                if found and found.get("uri"):
                    new_uri = found["uri"]
                    conn.execute("BEGIN")
                    try:
                        # Did track enrichment already create a URI-bearing row for this
                        # same artist? If yes, merge the orphan into the existing row.
                        existing = conn.execute(
                            "SELECT artist_id FROM artists WHERE spotify_artist_uri = ?",
                            (new_uri,),
                        ).fetchone()
                        if existing and existing["artist_id"] != r["artist_id"]:
                            target_id = existing["artist_id"]
                            orphan_id = r["artist_id"]
                            # Repoint track_artists. Use UPDATE OR IGNORE because the
                            # same track may already link to the URI-bearing row, in
                            # which case we'd hit the (track_id, artist_id) PK; OR
                            # IGNORE skips those, then we DELETE any stragglers.
                            conn.execute(
                                "UPDATE OR IGNORE track_artists SET artist_id = ? WHERE artist_id = ?",
                                (target_id, orphan_id),
                            )
                            conn.execute(
                                "DELETE FROM track_artists WHERE artist_id = ?",
                                (orphan_id,),
                            )
                            # Repoint labels (in case the user labeled the orphan).
                            conn.execute(
                                "UPDATE OR IGNORE artist_labels SET artist_id = ? WHERE artist_id = ?",
                                (target_id, orphan_id),
                            )
                            conn.execute(
                                "DELETE FROM artist_labels WHERE artist_id = ?",
                                (orphan_id,),
                            )
                            conn.execute(
                                "DELETE FROM artist_labels_history WHERE artist_id = ?",
                                (orphan_id,),
                            )
                            # Delete the orphan row itself.
                            conn.execute(
                                "DELETE FROM artists WHERE artist_id = ?",
                                (orphan_id,),
                            )
                            merged_id = target_id
                        else:
                            conn.execute(
                                "UPDATE artists SET spotify_artist_uri = ? WHERE artist_id = ?",
                                (new_uri, r["artist_id"]),
                            )
                            merged_id = r["artist_id"]
                        conn.execute("COMMIT")
                    except Exception:
                        conn.execute("ROLLBACK")
                        raise
                    rows.append({"id": merged_id, "uri": new_uri})
                progress.advance(task)

    if not rows:
        console.print("[dim]No artists need enrichment.[/dim]")
        return 0

    console.print(f"Enriching {len(rows):,} artist(s)...")
    updated = 0
    with Progress(TextColumn("Artists"), BarColumn(), MofNCompleteColumn(),
                  TimeElapsedColumn(), console=console) as progress:
        task = progress.add_task("", total=len(rows))
        for i in range(0, len(rows), ARTIST_BATCH):
            chunk = rows[i:i + ARTIST_BATCH]
            uris = [r["uri"] for r in chunk]
            try:
                api_artists = client.get_artists(uris)
            except Exception as e:
                console.print(f"[yellow]Artist batch failed: {e}[/yellow]")
                progress.advance(task, len(chunk))
                continue

            conn.execute("BEGIN")
            try:
                for uri, api in zip(uris, api_artists):
                    if api is None:
                        # 404 or retries exhausted — leave for future run (see
                        # comment in enrich_tracks for full rationale).
                        continue
                    conn.execute(
                        """
                        UPDATE artists
                        SET name = ?, name_normalized = ?,
                            genres_json = ?, popularity = ?, followers = ?,
                            last_enriched_at = datetime('now')
                        WHERE spotify_artist_uri = ?
                        """,
                        (
                            api.get("name"),
                            (api.get("name") or "").strip().lower(),
                            json.dumps(api.get("genres") or []),
                            api.get("popularity"),
                            (api.get("followers") or {}).get("total"),
                            uri,
                        ),
                    )
                    updated += 1
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

            progress.advance(task, len(chunk))
    return updated


# -----------------------------------------------------------------------------
# Album enrichment (for albums that came in via track enrichment but lack details)
# -----------------------------------------------------------------------------
def enrich_albums(conn, client: SpotifyClient, run_id: int, refresh_older_than, min_plays: int = 1) -> int:
    rows = _select_album_targets(conn, refresh_older_than, min_plays)
    if not rows:
        console.print("[dim]No albums need enrichment.[/dim]")
        return 0

    console.print(f"Enriching {len(rows):,} album(s)...")
    updated = 0
    with Progress(TextColumn("Albums"), BarColumn(), MofNCompleteColumn(),
                  TimeElapsedColumn(), console=console) as progress:
        task = progress.add_task("", total=len(rows))
        for i in range(0, len(rows), ALBUM_BATCH):
            chunk = rows[i:i + ALBUM_BATCH]
            uris = [r["uri"] for r in chunk]
            try:
                api_albums = client.get_albums(uris)
            except Exception as e:
                console.print(f"[yellow]Album batch failed: {e}[/yellow]")
                progress.advance(task, len(chunk))
                continue

            conn.execute("BEGIN")
            try:
                for uri, api in zip(uris, api_albums):
                    if api is None:
                        # 404 or retries exhausted — leave for future run.
                        continue
                    release_date = api.get("release_date")
                    conn.execute(
                        """
                        UPDATE albums
                        SET name = ?, name_normalized = ?,
                            release_date = ?, release_year = ?,
                            album_type = ?, total_tracks = ?,
                            last_enriched_at = datetime('now')
                        WHERE spotify_album_uri = ?
                        """,
                        (
                            api.get("name"),
                            (api.get("name") or "").strip().lower(),
                            release_date, _parse_release_year(release_date),
                            api.get("album_type"), api.get("total_tracks"),
                            uri,
                        ),
                    )
                    updated += 1
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

            progress.advance(task, len(chunk))
    return updated


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Enrich entities with Spotify Web API metadata")
    parser.add_argument("--tracks", action="store_true", help="Enrich tracks (do this first)")
    parser.add_argument("--albums", action="store_true", help="Enrich albums")
    parser.add_argument("--artists", action="store_true", help="Enrich artists")
    parser.add_argument("--all", action="store_true", help="Run all enrichment passes in correct order")
    parser.add_argument("--refresh-older-than", default=None,
                        help="Re-enrich rows older than this (e.g. 90d, 4w, 24h)")
    parser.add_argument("--min-plays", type=int, default=1,
                        help="Only enrich tracks (and their albums/artists) with at least this "
                             "many plays. Default 1 (all). Useful values: 20 (top engagement, "
                             "~45 min), 5 (covers ~65%% of plays, ~2-3 hours).")
    parser.add_argument("--rate-interval", type=float, default=1.0,
                        help="Minimum seconds between API calls. Default 1.0 (60 req/min).")
    args = parser.parse_args(argv)

    if not (args.tracks or args.albums or args.artists or args.all):
        parser.error("must specify at least one of --tracks/--albums/--artists/--all")

    refresh = _parse_age(args.refresh_older_than)

    conn = connect()
    init_schema(conn)
    run_id = start_run(conn, source="enrichment",
                       notes=f"min_plays={args.min_plays} rate={args.rate_interval}s")
    # Enrichment hits public catalog endpoints only — Client Credentials grant
    # is correct here (no user context required, no browser/OAuth dance).
    client = SpotifyClient(auth="app", min_request_interval=args.rate_interval)

    total_updated = 0
    try:
        if args.all or args.tracks:
            total_updated += enrich_tracks(conn, client, run_id, refresh, args.min_plays)
        if args.all or args.albums:
            total_updated += enrich_albums(conn, client, run_id, refresh, args.min_plays)
        if args.all or args.artists:
            total_updated += enrich_artists(conn, client, run_id, refresh, args.min_plays)
    except Exception as e:
        finish_run(conn, run_id, status="failed", notes=str(e))
        console.print(f"[red]Enrichment failed:[/red] {e}")
        return 1

    finish_run(conn, run_id, status="completed", rows_added=total_updated)
    console.print(f"[green]Enrichment complete.[/green] Updated {total_updated:,} row(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
