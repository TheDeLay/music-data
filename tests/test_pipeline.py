"""Integration test: generate synthetic Spotify dump JSON, ingest it, verify."""
from __future__ import annotations

import json
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scripts.db import connect, init_schema  # noqa: E402


def make_synthetic_dump(out_dir: Path) -> int:
    """Create a fake Streaming_History JSON file with diverse record types."""
    records = [
        # Normal track plays
        {
            "ts": "2021-08-14T03:24:11Z",
            "platform": "OS X 11.5.2",
            "ms_played": 515000,
            "conn_country": "US",
            "ip_addr": "192.0.2.1",  # we should NOT store this
            "master_metadata_track_name": "Master of Puppets",
            "master_metadata_album_artist_name": "Metallica",
            "master_metadata_album_album_name": "Master of Puppets",
            "spotify_track_uri": "spotify:track:fake000001",
            "episode_name": None,
            "episode_show_name": None,
            "spotify_episode_uri": None,
            "audiobook_title": None,
            "audiobook_uri": None,
            "audiobook_chapter_uri": None,
            "audiobook_chapter_title": None,
            "reason_start": "trackdone",
            "reason_end": "trackdone",
            "shuffle": False,
            "skipped": False,
            "offline": False,
            "offline_timestamp": None,
            "incognito_mode": False,
        },
        # Same artist, different track, partial listen (skip)
        {
            "ts": "2021-08-14T03:33:00Z",
            "platform": "OS X 11.5.2",
            "ms_played": 12000,
            "conn_country": "US",
            "ip_addr": "192.0.2.1",
            "master_metadata_track_name": "Battery",
            "master_metadata_album_artist_name": "Metallica",
            "master_metadata_album_album_name": "Master of Puppets",
            "spotify_track_uri": "spotify:track:fake000002",
            "episode_name": None,
            "episode_show_name": None,
            "spotify_episode_uri": None,
            "audiobook_title": None,
            "audiobook_uri": None,
            "audiobook_chapter_uri": None,
            "audiobook_chapter_title": None,
            "reason_start": "trackdone",
            "reason_end": "fwdbtn",
            "shuffle": False,
            "skipped": True,
            "offline": False,
            "offline_timestamp": None,
            "incognito_mode": False,
        },
        # Different artist, repeat play of same track later
        {
            "ts": "2022-03-15T19:01:00Z",
            "platform": "Android 12",
            "ms_played": 200000,
            "conn_country": "US",
            "master_metadata_track_name": "Master of Puppets",
            "master_metadata_album_artist_name": "Metallica",
            "master_metadata_album_album_name": "Master of Puppets",
            "spotify_track_uri": "spotify:track:fake000001",
            "spotify_episode_uri": None,
            "audiobook_chapter_uri": None,
            "reason_start": "clickrow",
            "reason_end": "endplay",
            "shuffle": True,
            "skipped": False,
            "offline": False,
            "incognito_mode": False,
        },
        # Podcast episode
        {
            "ts": "2023-06-01T08:00:00Z",
            "platform": "iOS 16.1",
            "ms_played": 2700000,
            "conn_country": "US",
            "master_metadata_track_name": None,
            "master_metadata_album_artist_name": None,
            "master_metadata_album_album_name": None,
            "spotify_track_uri": None,
            "episode_name": "The State of AI",
            "episode_show_name": "Hard Fork",
            "spotify_episode_uri": "spotify:episode:fake_ep_001",
            "reason_start": "trackdone",
            "reason_end": "trackdone",
            "shuffle": False,
            "skipped": False,
            "offline": True,
            "incognito_mode": False,
        },
        # Bad record: no URIs at all (should be quarantined)
        {
            "ts": "2023-07-04T12:00:00Z",
            "ms_played": 30000,
            "master_metadata_track_name": "Local file",
            "master_metadata_album_artist_name": "Unknown",
            "spotify_track_uri": None,
            "spotify_episode_uri": None,
            "audiobook_chapter_uri": None,
        },
        # EXACT duplicate of first record (should be deduplicated by unique index)
        {
            "ts": "2021-08-14T03:24:11Z",
            "platform": "OS X 11.5.2",
            "ms_played": 515000,
            "master_metadata_track_name": "Master of Puppets",
            "master_metadata_album_artist_name": "Metallica",
            "master_metadata_album_album_name": "Master of Puppets",
            "spotify_track_uri": "spotify:track:fake000001",
            "spotify_episode_uri": None,
            "audiobook_chapter_uri": None,
            "reason_start": "trackdone",
            "reason_end": "trackdone",
            "shuffle": False,
            "skipped": False,
            "offline": False,
            "incognito_mode": False,
        },
    ]
    out_path = out_dir / "Streaming_History_Audio_2021-2023_0.json"
    out_path.write_text(json.dumps(records, indent=2))
    return len(records)


def run_test() -> int:
    tmp = Path(tempfile.mkdtemp(prefix="music-data-test-"))
    try:
        dump_dir = tmp / "dump"
        dump_dir.mkdir()
        n_records = make_synthetic_dump(dump_dir)
        print(f"Created synthetic dump with {n_records} records")

        # Use a temp DB for the test
        test_db = tmp / "test.db"
        env = {
            **dict(__import__("os").environ),
            "MUSIC_DB_PATH": str(test_db),
        }

        # Run ingest
        result = subprocess.run(
            [sys.executable, "-m", "scripts.ingest_dump", str(dump_dir)],
            cwd=ROOT, env=env, capture_output=True, text=True,
        )
        print("--- ingest stdout ---")
        print(result.stdout)
        if result.returncode != 0:
            print("--- ingest stderr ---")
            print(result.stderr)
            return 1

        # Verify expected outcomes
        conn = connect(test_db)
        plays = conn.execute("SELECT COUNT(*) AS c FROM plays").fetchone()["c"]
        tracks = conn.execute("SELECT COUNT(*) AS c FROM tracks").fetchone()["c"]
        artists = conn.execute("SELECT COUNT(*) AS c FROM artists").fetchone()["c"]
        albums = conn.execute("SELECT COUNT(*) AS c FROM albums").fetchone()["c"]
        episodes = conn.execute("SELECT COUNT(*) AS c FROM episodes").fetchone()["c"]
        rejected = conn.execute("SELECT COUNT(*) AS c FROM rejected_rows").fetchone()["c"]
        runs = conn.execute(
            "SELECT source, status, rows_added, rows_skipped FROM ingestion_runs"
        ).fetchall()

        print(f"\nplays:       {plays}    (expected 4: 3 tracks + 1 episode, 1 dup deduped, 1 rejected)")
        print(f"tracks:      {tracks}    (expected 2: master, battery)")
        print(f"artists:     {artists}   (expected 1: metallica)")
        print(f"albums:      {albums}    (expected 1: master of puppets)")
        print(f"episodes:    {episodes}  (expected 1)")
        print(f"rejected:    {rejected}  (expected 1: the no-URI record)")
        print("ingestion_runs:")
        for r in runs:
            print(f"  source={r['source']} status={r['status']} added={r['rows_added']} skipped={r['rows_skipped']}")

        # Verify ip_addr is NOT stored anywhere
        plays_with_ip = conn.execute(
            "SELECT * FROM plays LIMIT 1"
        ).fetchone()
        cols = plays_with_ip.keys() if plays_with_ip else []
        assert "ip_addr" not in cols, "ip_addr leaked into plays table!"
        print("✓ ip_addr correctly NOT stored")

        # Verify the views compute correctly
        view_rows = conn.execute(
            "SELECT track_name, primary_artist_name, ms_played, engagement, reason_end "
            "FROM v_track_plays ORDER BY ts"
        ).fetchall()
        print("\nv_track_plays output:")
        for r in view_rows:
            print(f"  {r['track_name']:<20} {r['primary_artist_name']:<10} "
                  f"ms={r['ms_played']:<7} engagement={r['engagement']:<10} {r['reason_end']}")

        # Re-run ingest and verify nothing new added (idempotency!)
        print("\n--- Re-running ingest to verify idempotency ---")
        result2 = subprocess.run(
            [sys.executable, "-m", "scripts.ingest_dump", str(dump_dir)],
            cwd=ROOT, env=env, capture_output=True, text=True,
        )
        print(result2.stdout)

        plays_after = conn.execute("SELECT COUNT(*) AS c FROM plays").fetchone()["c"]
        assert plays_after == plays, f"Idempotency broken! plays {plays} -> {plays_after}"
        print(f"✓ Idempotent: plays count unchanged at {plays_after}")

        # Quick assertion suite
        assert plays == 4, f"plays count: expected 4, got {plays}"
        assert tracks == 2, f"tracks count: expected 2, got {tracks}"
        assert artists == 1, f"artists count: expected 1, got {artists}"
        assert albums == 1, f"albums count: expected 1, got {albums}"
        assert episodes == 1, f"episodes count: expected 1, got {episodes}"
        assert rejected == 1, f"rejected count: expected 1, got {rejected}"

        print("\n✓ ALL CHECKS PASSED")
        return 0
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(run_test())
