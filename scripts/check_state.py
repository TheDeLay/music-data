"""Inspect DB enrichment progress and resume work counters. Uses a TEMP TABLE
to pre-aggregate artist plays once instead of correlated subqueries."""
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "music.db"
conn = sqlite3.connect(str(DB_PATH))
conn.row_factory = sqlite3.Row

# Pre-compute artist play counts ONCE.
conn.execute("DROP TABLE IF EXISTS temp.artist_plays")
conn.execute("""
    CREATE TEMP TABLE artist_plays AS
    SELECT ta.artist_id, COUNT(*) AS c
    FROM plays p
    JOIN tracks t ON p.track_id = t.track_id
    JOIN track_artists ta ON t.track_id = ta.track_id AND ta.position = 0
    WHERE p.content_type = 'track'
    GROUP BY ta.artist_id
""")
conn.execute("CREATE INDEX temp.idx_artist_plays_c ON artist_plays(c)")

print("=== Top-line counts ===")
for label, sql in [
    ("plays",                     "SELECT COUNT(*) c FROM plays"),
    ("tracks total",              "SELECT COUNT(*) c FROM tracks"),
    ("tracks enriched",           "SELECT COUNT(*) c FROM tracks WHERE last_enriched_at IS NOT NULL"),
    ("tracks w/ duration_ms",     "SELECT COUNT(*) c FROM tracks WHERE duration_ms IS NOT NULL"),
    ("albums total",              "SELECT COUNT(*) c FROM albums"),
    ("albums w/ URI",             "SELECT COUNT(*) c FROM albums WHERE spotify_album_uri IS NOT NULL"),
    ("albums w/ release_year",    "SELECT COUNT(*) c FROM albums WHERE release_year IS NOT NULL"),
    ("artists total",             "SELECT COUNT(*) c FROM artists"),
    ("artists w/ URI",            "SELECT COUNT(*) c FROM artists WHERE spotify_artist_uri IS NOT NULL"),
    ("artists w/ genres_json",    "SELECT COUNT(*) c FROM artists WHERE genres_json IS NOT NULL"),
    ("artists last_enriched",     "SELECT COUNT(*) c FROM artists WHERE last_enriched_at IS NOT NULL"),
]:
    print(f"  {label:<28} {conn.execute(sql).fetchone()['c']:>6}")

print("\n=== Resume work counters (tier-20) ===")
# Tracks still needing enrichment (the 13 that 429'd)
unenriched = conn.execute("""
    SELECT COUNT(*) c FROM tracks t
    JOIN (SELECT track_id, COUNT(*) c FROM plays WHERE content_type='track'
          GROUP BY track_id HAVING c >= 20) tp ON t.track_id = tp.track_id
    WHERE t.last_enriched_at IS NULL
""").fetchone()['c']
print(f"  Tracks still need enrichment:    {unenriched}")
# Artists with URI but unenriched
ar_uri = conn.execute("""
    SELECT COUNT(*) c FROM artists ar
    JOIN artist_plays ap ON ap.artist_id = ar.artist_id
    WHERE ar.spotify_artist_uri IS NOT NULL
      AND ar.last_enriched_at IS NULL
      AND ap.c >= 20
""").fetchone()['c']
print(f"  Tier-20 artists w/URI need detail: {ar_uri}")
# Artists in tier-20 still needing name search
ar_name = conn.execute("""
    SELECT COUNT(*) c FROM artists ar
    JOIN artist_plays ap ON ap.artist_id = ar.artist_id
    WHERE ar.spotify_artist_uri IS NULL AND ap.c >= 20
""").fetchone()['c']
print(f"  Tier-20 artists need name-search: {ar_name}")

total_calls = unenriched + ar_uri + ar_name
print(f"\n  Total resume calls: {total_calls}  (~{total_calls*2/60:.1f} min @ 2.0s throttle)")

print("\n=== Potential dup-URI conflicts (the bug scenario) ===")
dups = conn.execute("""
    SELECT a1.artist_id AS orphan_id, a1.name AS orphan_name,
           a2.artist_id AS uri_id
    FROM artists a1
    JOIN artists a2 ON a1.name_normalized = a2.name_normalized
    WHERE a1.spotify_artist_uri IS NULL
      AND a2.spotify_artist_uri IS NOT NULL
      AND a1.artist_id != a2.artist_id
""").fetchall()
print(f"  Orphan rows that will trigger merge logic: {len(dups)}")
for r in dups[:8]:
    print(f"    artist {r['orphan_id']:>5} {r['orphan_name']!r:<35} -> merge into artist {r['uri_id']}")
if len(dups) > 8:
    print(f"    ... and {len(dups) - 8} more")
