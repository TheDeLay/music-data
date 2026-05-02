# Manual install — music-data

> **v0.1 stub.** Functionally complete. A polished version with screenshots of the Spotify Developer Dashboard, OS-specific notes, and troubleshooting flowcharts will replace it when the article-publish session ships its full version.

## When to use this file

You're following the manual path from [the article](https://thedelay.com/spotify-music-archive/). You'd rather read every command before running it than have an AI drive things. Or you've spent any time in InfoSec and, correctly, don't let an AI agent run shell on your machine. Either way, this is your file.

If you'd rather hand setup to your AI, see [`setup-with-ai.md`](./setup-with-ai.md). Both paths land in the same place.

## Prerequisites

- **Python 3.10 or newer** — `python3 --version`
- **`git`**
- **A Spotify account.** Premium isn't required for ingest or enrichment, but it'll be required for playlist creation in future versions of the project.
- **About 1 GB of disk space.** Your data dump is bigger than you'd think — JSON is verbose.

## Step 1 — Register a Spotify Developer App

1. Go to https://developer.spotify.com/dashboard and sign in.
2. Click **Create app**.
3. Fill in:
   - **App name** — anything (`music-data` is fine)
   - **App description** — anything
   - **Redirect URI** — `http://127.0.0.1:8888/callback`
     (Note the literal `127.0.0.1`. Spotify rejects `localhost` since their April 2025 policy update. Use the exact value above.)
   - **APIs used** — check **Web API**.
4. Save. On the app page, click **Settings** to find:
   - **Client ID** — visible.
   - **Client Secret** — click "View client secret." Treat this like a password.

You'll be in **Development Mode**. That's expected and fine — Extended Quota Mode is no longer available to individual hobbyist developers as of May 15, 2025. The pipeline is built around Development Mode constraints.

## Step 2 — Clone and set up the venv

```bash
git clone https://github.com/TheDeLay/music-data.git
cd music-data
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Step 3 — Configure credentials

```bash
cp .env.example .env
chmod 600 .env
```

Then edit `.env` with your Client ID and Client Secret. The redirect URI default (`http://127.0.0.1:8888/callback`) is already correct — leave it.

> **Heads-up if you use SyncThing or similar replication:** `.env` is gitignored, but file-sync tools don't honor `.gitignore`. If you don't want this secret syncing across machines, add `.env` to your sync ignore list (`.stignore` for SyncThing) or use shell-exported env vars instead.

## Step 4 — Initialize the database

```bash
python -m scripts.db init
python -m scripts.db info
```

Expected: schema version 2; 20 tables, all row counts 0. The DB file lives at `data/music.db` (gitignored).

## Step 5 — Request your data export

Go to https://www.spotify.com/account/privacy and request **Extended streaming history**. Spotify says "up to 30 days." In practice, it often arrives in a few days — they email you a download link when ready.

When the link arrives, unzip the file somewhere convenient. Note the path to the `Spotify Extended Streaming History` directory inside — you'll need it next.

## Step 6 — Ingest the dump

```bash
python -m scripts.ingest_dump "/path/to/Spotify Extended Streaming History/"
```

Expected: a few thousand to a few hundred thousand plays, processed in seconds. No rate limit applies — this is local-only work. Bad rows go to `rejected_rows` for later inspection rather than crashing the run.

Re-running the same command on the same files is safe — the unique index on `(ts, content_uri, ms_played)` deduplicates.

## Step 7 — Smoke-test the API path

Before any longer enrichment job, verify your credentials work:

```bash
python -m scripts.ingest_recent
```

This pulls your last 50 plays from Spotify's API. The first run opens a browser for one-time OAuth consent and caches a refresh token in `.spotify_token.json` (also gitignored). After that, scripts run unattended.

## Step 8 — Enrich, starting small

```bash
python -m scripts.enrich --tracks --min-plays 100 --rate-interval 2.0
```

This enriches only your top ~25 tracks (ones with 100+ plays each). Takes about a minute. Zero rate-limit risk. Once it completes:

```bash
python -m scripts.check_state
```

You should see ~25 tracks with `duration_ms`, ~25 albums with `release_year`, and a handful of artists with Spotify URIs.

## Step 9 — Scale up enrichment carefully

```bash
python -m scripts.enrich --all --min-plays 20 --rate-interval 2.0
```

This enriches the top ~900 tracks (≥20 plays each), plus their albums and artists. About 40 minutes of throttled API calls if uninterrupted.

> **Cooldown caveat — this matters.** In testing, ~875 calls in 15 minutes triggered a `Retry-After: 74,512` cooldown from Spotify (about 20 hours). The pipeline backs off globally on 429s and won't crash, but it also won't make progress until the cooldown clears. **If you 429, walk away for a day.** Don't keep retrying — that just extends the cooldown.

For higher coverage:

| Flag | Coverage | Wall-clock |
|---|---|---|
| `--min-plays 20` | top ~900 tracks; ~33% of total plays | ~40 min |
| `--min-plays 5` | top ~5,000 tracks; ~65% of total plays | several hours |
| `--min-plays 1` | every unique track ever | many hours; not recommended |

## Step 10 — Query

You have a SQLite database at `data/music.db`. Open it however you like:

```bash
# Coverage stats and resume planning
python -m scripts.check_state

# Interactive (if you have the sqlite3 CLI installed)
sqlite3 data/music.db

# Or from Python directly
.venv/bin/python -c "
import sqlite3
conn = sqlite3.connect('data/music.db')
for r in conn.execute('SELECT * FROM v_track_summary LIMIT 5'):
    print(r)
"
```

The views `v_track_plays`, `v_track_summary`, and `v_artist_summary` compute engagement metrics on the fly. See [`sql/schema.sql`](sql/schema.sql) for the full DDL with comments.

## Common gotchas

- **`localhost` rejected as redirect URI.** Spotify enforces loopback IP literals (`127.0.0.1`) since April 2025. The `.env.example` has the right value.
- **Batch endpoints return 403.** Spotify's `/v1/tracks?ids=` (and `/artists?ids=`, `/albums?ids=`) return 403 for new Development-Mode apps as of February 2026. The pipeline already routes around this with single-ID GETs. If you're rolling your own client, don't use the batch form.
- **Long cooldowns are real.** When Spotify rate-limits you, the `Retry-After` can be 20+ hours. Plan accordingly.
- **`ip_addr` in the dump.** Spotify's privacy export *includes* your historical IP addresses in each play record. The ingest pipeline drops this field at the boundary — it's never written to the database. If you ever publish or share JSONs from `data/raw/`, sanitize that field.

## Verifying the install

```bash
python tests/test_pipeline.py        # Synthetic integration test (no API)
python tests/test_artist_merge.py    # Offline unit test for merge logic (no API)
```

Both should print "ALL CHECKS PASSED" / "All assertions passed."

## Where to go next

- **The "what does this answer?" examples in the [README](./README.md#what-this-answers).**
- **The companion article on schema choices:** **music-data: The Schema Story** (publishing shortly on TheDeLay.com).
- **The original article:** [Spotify Has 12 Years of My Data. I Just Took It Back.](https://thedelay.com/spotify-music-archive/)

---

*Stub v0.1 — John DeLay, 2026-05-01. Slot for replacement: a full install guide with screenshots of the Spotify Developer Dashboard, OS-specific notes (macOS vs Linux vs Windows-in-WSL), troubleshooting flowcharts, and verified expected outputs for every step. The article-publish session is the canonical owner of the final version.*
