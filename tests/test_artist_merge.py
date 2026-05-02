"""Offline test: re-create the artist-merge race in a fresh DB and confirm the patch handles it.

No Spotify API calls. Pure SQL + the patched logic.
"""
import sqlite3
import sys
from pathlib import Path

# Resolve schema path relative to this test file: tests/ -> project root -> sql/schema.sql
ROOT = Path(__file__).resolve().parent.parent
SCHEMA = (ROOT / "sql" / "schema.sql").read_text()

# Inline a stripped version of the merge logic from enrich.py so we don't need
# the Spotify client. The logic must match what the real code does.
def resolve_orphan_to_uri(conn, orphan_id: int, new_uri: str) -> int:
    conn.execute("BEGIN")
    try:
        existing = conn.execute(
            "SELECT artist_id FROM artists WHERE spotify_artist_uri = ?",
            (new_uri,),
        ).fetchone()
        if existing and existing["artist_id"] != orphan_id:
            target_id = existing["artist_id"]
            conn.execute(
                "UPDATE OR IGNORE track_artists SET artist_id = ? WHERE artist_id = ?",
                (target_id, orphan_id),
            )
            conn.execute("DELETE FROM track_artists WHERE artist_id = ?", (orphan_id,))
            conn.execute(
                "UPDATE OR IGNORE artist_labels SET artist_id = ? WHERE artist_id = ?",
                (target_id, orphan_id),
            )
            conn.execute("DELETE FROM artist_labels WHERE artist_id = ?", (orphan_id,))
            conn.execute("DELETE FROM artist_labels_history WHERE artist_id = ?", (orphan_id,))
            conn.execute("DELETE FROM artists WHERE artist_id = ?", (orphan_id,))
            merged_id = target_id
        else:
            conn.execute(
                "UPDATE artists SET spotify_artist_uri = ? WHERE artist_id = ?",
                (new_uri, orphan_id),
            )
            merged_id = orphan_id
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return merged_id


def main() -> int:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(SCHEMA)

    # Set up the bug scenario:
    # - Orphan name-artist row from ingest: artist_id=1, "Test Artist", no URI
    # - URI-bearing row from track enrichment: artist_id=2, same artist, URI present
    # - One track linked to both rows (the conflict that triggers UPDATE OR IGNORE)
    # - One track linked only to the orphan (clean migration)
    fake_uri = "spotify:artist:" + "A" * 22
    conn.execute(
        "INSERT INTO artists (artist_id, name, name_normalized) VALUES (1, 'Test Artist', 'test artist')"
    )
    conn.execute(
        "INSERT INTO artists (artist_id, name, name_normalized, spotify_artist_uri, last_enriched_at) "
        "VALUES (2, 'Test Artist', 'test artist', ?, datetime('now'))",
        (fake_uri,),
    )
    conn.execute(
        "INSERT INTO albums (album_id, name, name_normalized) VALUES (10, 'Sample Album', 'sample album')"
    )
    # Track 100: linked to BOTH artists (the conflict case)
    conn.execute(
        "INSERT INTO tracks (track_id, spotify_track_uri, name, album_id) "
        "VALUES (100, 'spotify:track:" + "B" * 22 + "', 'Sample Track', 10)"
    )
    conn.execute("INSERT INTO track_artists (track_id, artist_id, position) VALUES (100, 1, 0)")
    conn.execute("INSERT INTO track_artists (track_id, artist_id, position) VALUES (100, 2, 0)")
    # Track 101: linked only to orphan (clean migration case)
    conn.execute(
        "INSERT INTO tracks (track_id, spotify_track_uri, name, album_id) "
        "VALUES (101, 'spotify:track:" + "C" * 22 + "', 'Other Track', 10)"
    )
    conn.execute("INSERT INTO track_artists (track_id, artist_id, position) VALUES (101, 1, 0)")
    # Add a label on the orphan to test label migration
    conn.execute(
        "INSERT INTO artist_labels (artist_id, label_key, label_value, set_by) "
        "VALUES (1, 'sample_label', 'y', 'manual')"
    )
    conn.commit()

    print("Before merge:")
    for r in conn.execute("SELECT artist_id, name, spotify_artist_uri FROM artists ORDER BY artist_id"):
        print(f"  artist {r['artist_id']}: {r['name']!r} uri={r['spotify_artist_uri']}")
    print(f"  track_artists rows: {conn.execute('SELECT COUNT(*) c FROM track_artists').fetchone()['c']}")
    print(f"  artist_labels rows: {conn.execute('SELECT COUNT(*) c FROM artist_labels').fetchone()['c']}")

    # The bug scenario: orphan_id=1 just resolved via /search to URI already in row 2
    merged = resolve_orphan_to_uri(conn, orphan_id=1, new_uri=fake_uri)

    print(f"\nMerged into artist_id={merged}")
    print("\nAfter merge:")
    for r in conn.execute("SELECT artist_id, name, spotify_artist_uri FROM artists ORDER BY artist_id"):
        print(f"  artist {r['artist_id']}: {r['name']!r} uri={r['spotify_artist_uri']}")
    print(f"  track_artists rows: {conn.execute('SELECT COUNT(*) c FROM track_artists').fetchone()['c']}")
    for r in conn.execute("SELECT track_id, artist_id FROM track_artists ORDER BY track_id, artist_id"):
        print(f"    track {r['track_id']} <-> artist {r['artist_id']}")
    labels = conn.execute("SELECT artist_id, label_key, label_value FROM artist_labels").fetchall()
    print(f"  artist_labels rows: {len(labels)}")
    for r in labels:
        print(f"    artist {r['artist_id']} {r['label_key']}={r['label_value']}")

    # ---- Assertions ----
    artists = conn.execute("SELECT artist_id FROM artists ORDER BY artist_id").fetchall()
    assert [r["artist_id"] for r in artists] == [2], f"orphan should be deleted, got {artists}"
    rows_100 = conn.execute("SELECT artist_id FROM track_artists WHERE track_id=100 ORDER BY artist_id").fetchall()
    assert [r["artist_id"] for r in rows_100] == [2], f"track 100 should have just artist 2, got {rows_100}"
    rows_101 = conn.execute("SELECT artist_id FROM track_artists WHERE track_id=101 ORDER BY artist_id").fetchall()
    assert [r["artist_id"] for r in rows_101] == [2], f"track 101 should be repointed to artist 2, got {rows_101}"
    label_rows = conn.execute("SELECT artist_id FROM artist_labels").fetchall()
    assert [r["artist_id"] for r in label_rows] == [2], f"label should be repointed to artist 2, got {label_rows}"
    # No FK orphans
    fk_check = conn.execute("PRAGMA foreign_key_check").fetchall()
    assert not fk_check, f"FK violations: {fk_check}"

    # ---- Second test: clean case (no existing URI), should fall through to UPDATE ----
    conn.execute("INSERT INTO artists (artist_id, name, name_normalized) VALUES (3, 'Third Artist', 'third artist')")
    conn.commit()
    fresh_uri = "spotify:artist:" + "Z" * 22
    merged2 = resolve_orphan_to_uri(conn, orphan_id=3, new_uri=fresh_uri)
    assert merged2 == 3, f"clean case should keep orphan_id, got {merged2}"
    r = conn.execute("SELECT spotify_artist_uri FROM artists WHERE artist_id=3").fetchone()
    assert r["spotify_artist_uri"] == fresh_uri, f"URI should be set on orphan: {r['spotify_artist_uri']}"

    print("\n  All assertions passed. Patch is correct.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
