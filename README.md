# music-data

Personal Spotify listening archive. SQLite-backed, normalized, with manual
labeling support for editorial playlists.

## What this is

A pipeline that ingests Spotify listening history from two sources:

1. **Extended streaming history** — one-shot import of the full historical
   dump from Spotify's privacy export (the file you request from
   <https://www.spotify.com/account/privacy>; arrives ~30 days after request).
2. **Recently played API** — incremental sync of the last 50 plays via
   Spotify's Web API. Run on a cron / n8n schedule to keep the archive fresh.

After ingest, an enrichment pass calls the Spotify API to fill in metadata
the dump doesn't include: artist URIs and genres, album release dates,
track durations and popularity.

The end state is a SQLite database you can query for things like:

- "What 90s metal was I actually listening to in 2021?" (genre + year filtering)
- "Which tracks do I always let finish vs. which do I skip?" (engagement metrics)
- "Build me a playlist from my top-25 most-committed-to tracks since June" (data-driven curation)

## Project layout

```
music-data/
├── README.md                    # This file
├── requirements.txt             # Python dependencies
├── .gitignore
├── sql/
│   └── schema.sql              # Canonical DDL — source of truth for the DB
├── scripts/
│   ├── db.py                   # Connection + schema management
│   ├── models.py               # Pydantic data models
│   ├── spotify_client.py       # Thin Spotify Web API wrapper
│   ├── extractors.py           # JSON / API → normalized records
│   ├── loader.py               # Records → DB (idempotent, batched)
│   ├── ingest_dump.py          # CLI: load extended history dump
│   ├── ingest_recent.py        # CLI: pull last 50 plays from API
│   └── enrich.py               # CLI: fill in metadata via API
├── data/
│   ├── raw/                    # Original JSON dumps (archived, gitignored)
│   └── music.db                # SQLite database (gitignored)
├── summaries/                  # Auto-generated markdown context packs
└── tests/
    └── ...                     # Synthetic-data tests for extractors/loader
```

## Setup

```bash
# 1. Clone, then create a venv
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 2. Configure (copy and fill in)
cp .env.example .env
# Edit .env: SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET, SPOTIFY_REDIRECT_URI

# 3. Initialize the database
python scripts/db.py init

# 4. (When the dump arrives) Load historical plays
python scripts/ingest_dump.py /path/to/Spotify\ Extended\ Streaming\ History/

# 5. Pull recent plays (also good for testing without the dump)
python scripts/ingest_recent.py

# 6. Enrich: fill in artist genres, album years, track durations
python scripts/enrich.py --all
```

## Spotify Developer App

To use the API ingest and enrichment, register a Spotify Developer App at
<https://developer.spotify.com/dashboard>. Set the redirect URI to
`http://localhost:8888/callback` (or whatever you prefer). Copy the Client
ID and Client Secret into `.env`.

The first run of any API-using script will open a browser for one-time OAuth
consent and cache the refresh token in `.spotify_token.json`. After that,
scripts run unattended.

## Schema overview

See `sql/schema.sql` for the full definition with comments. Highlights:

- **`plays`** — one row per play event (track, podcast episode, or audiobook chapter)
- **`tracks`, `albums`, `artists`** — normalized entities, populated incrementally
- **`track_labels`, `album_labels`, `artist_labels`** — user editorial layer
  with audit history; supports inheritance via `v_track_effective_labels`
- **`v_track_plays`, `v_track_summary`, `v_artist_summary`** — analytical views
  with computed engagement metrics (`percent_played`, `engagement` bucket,
  `finish_rate`)

## Operational notes

- All ingest is **idempotent**. Re-running on the same input produces no
  duplicates (enforced by unique index on `(ts, content_uri, ms_played)`).
- Failed records are quarantined to `rejected_rows` rather than crashing
  the run. Check there if row counts are surprising.
- Every play row carries `ingestion_run_id` linking back to the run that
  created it (see `ingestion_runs` table for run audit log).
