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
chmod 600 .env
# Edit .env: SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET (redirect URI defaults are fine)

# 3. Initialize the database
python -m scripts.db init

# 4. Load historical plays (when your privacy export arrives)
python -m scripts.ingest_dump /path/to/Spotify\ Extended\ Streaming\ History/

# 5. Pull recent plays (works without the dump, good for testing)
python -m scripts.ingest_recent

# 6. Enrich most-played tracks first; long tail can wait
python -m scripts.enrich --all --min-plays 20 --rate-interval 2.0
```

The `--min-plays N` flag enriches only tracks with at least N plays — useful
because most listening concentrates on a small fraction of unique tracks
(Pareto distribution). For a typical heavy listener, `--min-plays 20` covers
the meaningful engagement signal in ~40 minutes of throttled API calls;
`--min-plays 5` covers ~65% of plays in a few hours; `--min-plays 1` covers
everything but takes much longer.

## Spotify Developer App

Register at <https://developer.spotify.com/dashboard>:

- **Redirect URI**: `http://127.0.0.1:8888/callback` — note the literal IP. Spotify rejects `localhost` since its April 2025 policy update.
- **API**: Web API.
- Copy Client ID and Client Secret into `.env` (chmod 600 it).

`enrich.py` uses the Client Credentials grant (no browser, public catalog
endpoints only). `ingest_recent.py` and any future playlist mutation use
Authorization Code w/ PKCE — those open a browser once and cache a refresh
token in `.spotify_token.json`.

## A note on rate limits and Spotify's API constraints

This pipeline is built for **personal use under Spotify's Development Mode**:

- **Single-ID GETs only.** Spotify's batch endpoints (`/tracks?ids=...`,
  `/artists?ids=...`, `/albums?ids=...`) return 403 for new Development-Mode
  apps as of February 2026. Single-ID endpoints (`/tracks/{id}`) still work,
  so the client uses those exclusively. This is the project's permanent
  architecture, not a workaround.
- **Throttle and global 429 backoff.** Default 1s between calls (configurable
  via `--rate-interval`); 2s recommended for tier-1 enrichment runs. When
  Spotify returns 429, all subsequent calls wait per the `Retry-After`
  header — not just the one URL that 429'd.
- **Be a good API citizen.** Read [Spotify's Developer Terms](https://developer.spotify.com/terms)
  before running a full enrichment pass. §VI.4 prohibits "excessive service
  calls"; §IV.2.2 prohibits crawler/spider behavior. This pipeline stays
  defensible by enriching only what the user has personally listened to (no
  catalog crawling), filtering by a play-count threshold, throttling well
  below any plausible rate limit, and identifying itself with a User-Agent.
- **Extended Quota Mode is not available** to individual hobbyist developers
  as of May 15, 2025. Don't apply unless you have a registered business with
  250k+ MAUs.

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
