# music-data

> **Spotify is wrong about your favorites.**
> Your most-played track might be one you skip half the time. The song you actually love might have four plays and two back-button rewinds.
> This is the database that knows the difference.

Open-source companion to the article [**Spotify Has 12 Years of My Data. I Just Took It Back.**](https://thedelay.com/spotify-music-archive) on TheDeLay.com.

It ingests your full Spotify listening history into a local SQLite database, enriches it with the metadata Spotify's privacy export *doesn't* include (track durations, album release years, artist genres), and exposes engagement signals — what percent of a song you actually finish, your back-button rewind list, your skip rate over time — that turn raw play count into something useful.

The thesis from the article, in one line:

> A song queued four times and skipped in eight seconds each time is not a hit. It's four rejections.

This database tells you which is which.

---

## What this answers

A few question-shapes this database makes trivial. The synthetic numbers below are illustrative.

### "Which 'favorites' am I lying about?"

Your top tracks by play count, with the engagement signal that play count hides:

```
track                       artist            plays  avg_pct  finish_pct
--------------------------  ----------------  -----  -------  ----------
[Synthetic Track A]         [Artist X]        210    43       38
[Synthetic Track B]         [Artist Y]        175    69       57
[Synthetic Track C]         [Artist Z]        141    52       37
```

Track A is queued the most. Finished 38% of the time. By raw play count it's "a favorite." By engagement, it's a song you keep skipping past. Track B is the real one — fewer plays, but you let it run.

### "What did I commit to when nothing was getting in the way?"

Per-year skip and finish rates over your full listening history:

```
year   plays   skip_pct   finish_pct
----   -----   --------   ----------
2018    9893         26           18
2020    2891          4           81
2024   11609         49           39
```

Three different humans across the same Spotify account. The shifts are measurable.

### "Show me the rewinds — the songs that grabbed me mid-play."

Tracks where you hit the back button mid-play. The rarest signal in the dataset; arguably the most truthful — your conscious self never chose to enshrine these tracks, but your listening behavior keeps voting for them.

```
track                       artist               backs   total_plays
--------------------------  -------------------  -----   -----------
[Synthetic Track Q]         [Heavy Artist]           2             4
[Synthetic Track R]         [Indie Artist]           2             3
```

Track Q has four total plays and two of them ended with you rewinding. That's a track you keep skipping past *and* keep being grabbed by. Surface it.

### "What was I actually listening to in the 90s?" / "Vocal-light tracks for working hours, sorted by tempo." / etc.

The schema supports decade filtering (`albums.release_year`), genre tags (`artists.genres_json`), and any custom dimensions you want to label tracks/albums/artists with (`track_labels`, etc.). The point isn't a fixed query catalog — it's that you have the data shape to ask whatever question you want.

---

## Two ways to set this up

### Path 1 — Hand it to your AI

Download [`setup-with-ai.md`](./setup-with-ai.md) and paste it into Claude Code, Cursor, ChatGPT desktop with code interpreter, or whatever AI assistant you trust on your machine. Say "set this up for me." The AI clones the repo, sets up the venv, walks you through registering a Spotify Developer App, configures `.env`, runs the database init, and verifies the API path. You handle the OAuth consent and request your own data dump — only you can do those. Everything else, the AI does.

### Path 2 — Read the script before running it

Clone the repo and follow [`INSTALL.md`](./INSTALL.md). Same destination, manual journey. Plain terminal commands. No AI involvement. If you've spent any time in InfoSec you already know which path you'll pick, and that's fine. Both are first-class citizens here.

---

## Quick start (Path 2, condensed)

```bash
git clone https://github.com/TheDeLay/music-data.git
cd music-data
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Configure: register a Spotify Developer App at https://developer.spotify.com/dashboard
# Redirect URI MUST be http://127.0.0.1:8888/callback (Spotify rejects 'localhost' since Apr 2025)
cp .env.example .env && chmod 600 .env
# Edit .env with your Client ID and Client Secret

# Initialize
python -m scripts.db init

# Request your data export from https://www.spotify.com/account/privacy
# (Spotify says "up to 30 days" — in practice, often a few days)
# Then load it:
python -m scripts.ingest_dump /path/to/Spotify\ Extended\ Streaming\ History/

# Pull recent plays from the API (works without the dump too — useful for testing)
python -m scripts.ingest_recent

# Enrich most-played tracks first; long tail can wait
python -m scripts.enrich --all --min-plays 20 --rate-interval 2.0
```

The `--min-plays N` flag enriches only tracks with at least N plays. Listening concentrates on a small fraction of unique tracks (Pareto distribution), so this is the difference between 40 minutes and 16 hours of throttled API calls. Useful values: `20` for the engagement-signal tier, `5` for ~65% of all plays covered, `1` for the long tail.

---

## How it works

Three layers, by design:

1. **Raw layer** — JSON dumps from Spotify's privacy export, archived in `data/raw/` (gitignored). This is the source of truth; the database can be rebuilt from here at any time.
2. **Query layer** — `data/music.db` (SQLite, also gitignored). Single file, zero infrastructure, easy to copy/back up. Lives in your filesystem; your AI can query it with `sqlite3` or any wrapper.
3. **Context layer** — auto-generated markdown summaries in `summaries/` (also gitignored — these contain your taste). When you start an AI session, drop these into context so the assistant doesn't have to query from scratch.

Everything else is plumbing:

- **Idempotent ingest.** Re-run on the same input, get no duplicates. Unique index on `(ts, content_uri, ms_played)` is the dedup key.
- **Quarantine, don't crash.** Bad rows go to `rejected_rows` with a reason; ingest keeps going.
- **Provenance.** Every play row carries an `ingestion_run_id`; every entity row carries `last_enriched_at`. If something looks off six months from now, you can trace it back.
- **Polymorphic plays.** Tracks, podcast episodes, audiobook chapters all share one `plays` table. `content_type` discriminates; a CHECK constraint enforces the FK invariant at the DB level.
- **Engagement metrics as views, not stored columns.** `v_track_plays`, `v_track_summary`, `v_artist_summary` compute `percent_played`, `engagement` bucket, `finish_rate` on the fly. Refresh-free.

For the why behind the schema choices — what got considered and rejected, and why this is SQLite instead of Postgres — see the companion piece [music-data: The Schema Story](https://thedelay.com/spotify-music-archive-nerd-stuff) (publishing soon).

---

## Schema overview

See [`sql/schema.sql`](sql/schema.sql) for the full DDL with comments. Highlights:

- **`plays`** — one row per play event (track, podcast episode, or audiobook chapter)
- **`tracks`, `albums`, `artists`** — normalized entities, populated incrementally during ingest and enrichment
- **`track_labels`, `album_labels`, `artist_labels`** — your editorial layer with audit history; supports inheritance via the `v_track_effective_labels` view (track > album > artist > NULL)
- **`v_track_plays`, `v_track_summary`, `v_artist_summary`** — analytical views with computed engagement metrics

Roughly 20 tables, a handful of triggers for label history, and a few indexes tuned for the common queries.

---

## A note on rate limits and Spotify's API

This pipeline is built for **personal use under Spotify's Development Mode**. As of February 2026, that's the only mode hobbyist developers can use:

- **Single-ID GETs only.** Spotify's batch endpoints (`/tracks?ids=...`, `/artists?ids=...`, `/albums?ids=...`) return 403 for new Development-Mode apps. Single-ID endpoints (`/tracks/{id}`) still work, so the client uses those exclusively. This is the project's permanent architecture, not a workaround.
- **Throttle and global 429 backoff.** Default 1s between calls (configurable via `--rate-interval`); 2-3s recommended for tier-1 enrichment runs. When Spotify returns 429, *all* subsequent calls wait per the `Retry-After` header — not just the one URL that 429'd.
- **Be a good API citizen.** Read [Spotify's Developer Terms](https://developer.spotify.com/terms) before running a full enrichment pass. §VI.4 prohibits "excessive service calls"; §IV.2.2 prohibits crawler/spider behavior. This pipeline stays defensible by enriching only what *you* have personally listened to (no catalog crawling), filtering by a play-count threshold, throttling well below any plausible rate limit, and identifying itself with a User-Agent.
- **Extended Quota Mode is not available** to individual hobbyist developers as of May 15, 2025 (legally-registered businesses with 250k+ MAUs only).
- **Cooldowns can be long.** In testing, ~875 calls in 15 minutes triggered a `Retry-After` of about 20 hours. Plan accordingly. If you 429, walk away for a day.

---

## What's not in this repo

- Your listening data (the JSON dumps and the SQLite DB are gitignored — they live on your machine, not GitHub).
- Your Spotify credentials (`.env` is gitignored too).
- The auto-generated summaries (also gitignored — they contain personal taste data).

This is a clean toolkit. The data stays yours.

---

## License

MIT. See [`LICENSE`](LICENSE). Copyright © 2026 John DeLay.

---

## See also

- [Spotify Has 12 Years of My Data. I Just Took It Back.](https://thedelay.com/spotify-music-archive) — the why
- [music-data: The Schema Story](https://thedelay.com/spotify-music-archive-nerd-stuff) — the how (publishing soon)
- [TheDeLay.com](https://thedelay.com) — homelab, AI integration, and InfoSec writing
