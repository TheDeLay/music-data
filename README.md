# music-data

> **Spotify is wrong about your favorites.**
> Your most-played track might be one you skip half the time. The song you actually love might have four plays and two back-button rewinds.
> This is the database that knows the difference.

Open-source companion to the article [Spotify Has 12 Years of My Data. I Just Took It Back.](https://thedelay.com/spotify-music-archive/) on TheDeLay.com.

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
year   plays    skip_pct   finish_pct
----   ------   --------   ----------
2018   ~6500        ~25         ~30
2020   ~2800         ~5         ~80
2024   ~9000        ~50         ~35
```

Numbers above are illustrative shapes, not anyone's real data. But the *pattern* is the point: a same-account listener can look like three different humans across the same six years. The shifts are measurable, even when the trend isn't obvious from inside it.

### "Show me the rewinds — the songs that grabbed me mid-play."

Tracks where you hit the back button mid-play. The rarest signal in the dataset; arguably the most truthful — your conscious self never chose to enshrine these tracks, but your listening behavior keeps voting for them.

```
track                       artist               backs   total_plays
--------------------------  -------------------  -----   -----------
[Synthetic Track Q]         [Heavy Artist]           2             4
[Synthetic Track R]         [Indie Artist]           2             3
```

Track Q has four total plays and two of them ended with you rewinding. That's a track you keep skipping past *and* keep being grabbed by. Surface it.

### "What was I actually listening to in the 90s?" / "tracks I want to label as 'workout', sorted by length" / etc.

The schema supports decade filtering (`albums.release_year` populates fine for new Dev Mode apps), and any custom dimensions you want to label tracks/albums/artists with (`track_labels`, etc.).

Genre filtering via `artists.genres_json` is in the schema but **will be empty for new Dev Mode apps post-Feb-2026** (see Limitations below). Multi-source enrichment via MusicBrainz/AcousticBrainz is planned to fill that gap.

The point isn't a fixed query catalog — it's that you have the data shape to ask whatever question you want.

---

## Two ways to set this up

### Path 1 — Hand it to your AI

Download [`setup-with-ai.md`](./setup-with-ai.md) and paste it into Claude Code, Cursor, ChatGPT desktop with code interpreter, or whatever AI assistant you trust on your machine. Say "set this up for me." The AI clones the repo, sets up the venv, walks you through registering a Spotify Developer App, configures `.env`, runs the database init, and verifies the API path. You handle the OAuth consent and request your own data dump — only you can do those. Everything else, the AI does.

### Path 2 — Read the script before running it

Clone the repo and follow [`INSTALL.md`](./INSTALL.md). Same destination, manual journey. Plain terminal commands. No AI involvement. If you've spent any time in InfoSec you already know which path you'll pick, and that's fine. Both are first-class citizens here.

---

## Before you commit: known limitations

Spotify tightened its Web API significantly between November 2024 and February 2026. This pipeline works within the new constraints, but you should know what you're agreeing to before you start.

### Spotify Premium is required (for you, the developer)

As of February 2026, all Spotify Web API Development Mode apps require the **app owner** to have an active Spotify Premium subscription. If your Premium lapses, the dev app stops working. **No Premium → this pipeline will not run.**

This is a Spotify policy change, not a project decision. Source: [Spotify's February 2026 policy update](https://developer.spotify.com/blog/2026-02-06-update-on-developer-access-and-platform-security).

### These metadata fields will be empty

For new Dev Mode apps registered after February 6, 2026, Spotify's API returns NULL or empty for:

- `track.popularity` — no global popularity score
- `track.available_markets` — no regional availability
- `artist.popularity` — no artist popularity score
- `artist.followers.total` — no follower count
- `artist.genres` — empty array; **no genre data at all**

The schema still includes these columns (so existing pre-policy apps work, and so the data slot is ready if Spotify ever restores access), but if you're an individual developer running this in 2026 — **expect them to be empty**.

The good news: the article's data-driven analyses don't rely on any of these. Engagement %, finish rate, back-button gold, decade distribution — all work because they use `duration_ms`, `album.release_date`, and the existing play-event signals, none of which are restricted.

If you want genre, tempo, energy, or valence data, the planned multi-source enrichment will pull it from MusicBrainz and AcousticBrainz instead — open data sources that don't depend on Spotify's policy direction.

### Daily API quota: ~700-900 calls

Empirically observed during this project's development:

| Pacing | Calls before wall | Resulting cooldown |
|---|---|---|
| Fast (~1,200/hr) | ~875 | **20.7 hours** |
| Slow (~100/hr) | ~732 | **7.9 hours** |

The wall hits in roughly the same place regardless of pacing — that's a daily/cumulative bucket, not a rolling window. But **slower pacing reduces the penalty severity** by ~3×.

Plan for **~600 calls per night** as the sustainable target with the default settings. Full enrichment of a multi-year listening archive will span multiple nights.

The pipeline's defaults are calibrated to these observations:

- **35-second throttle** between calls (`--rate-interval 35.0`)
- **60-second hard-stop** on a single 429 with a long Retry-After (`--long-penalty-threshold 60`) — bails immediately rather than retrying into a known-blocked state
- **10-minute no-progress watchdog** (`--max-no-progress 600`) — aborts if no successful response in that window, instead of silently sleeping for hours
- **Per-run log file** in `logs/enrich-{timestamp}.log` capturing every 429, watchdog warning, and end-of-run stats summary

Run `python -m scripts.enrich --help` for full flag documentation. Defaults are tuned for unattended overnight runs — designed assuming you might kick the script off and go to bed.

### Extended Quota Mode is not available to individual developers

Since May 15, 2025, Extended Quota Mode requires:

1. A legally registered business or organization (not an individual)
2. Minimum 250,000 monthly active users
3. Active, launched service in key Spotify markets
4. Application via company email; up to 6-week review

There is no upgrade path for individual developers. Dev Mode is the ceiling. If your use case requires volume the daily quota can't support, you need either a registered business entity at scale or a different data source.

### What this means in practice

- **Keep Premium active.** As long as you want to run this pipeline.
- **Plan to span enrichment across multiple nights.** A 12-year listening archive isn't a one-evening job.
- **Don't expect genre-aware features to work out of the box.** They'll come back via MusicBrainz/AcousticBrainz integration in a future version.
- **The engagement queries work today.** Everything the companion article demonstrates is fully data-backed by what Dev Mode still returns.

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

# Enrich most-played tracks first; long tail can wait.
# 35s/call is the SAFE default — well below Spotify's daily quota threshold.
python -m scripts.enrich --all --min-plays 20 --rate-interval 35.0
```

The `--min-plays N` flag enriches only tracks with at least N plays. Listening concentrates on a small fraction of unique tracks (Pareto distribution), so this is the difference between one overnight run and many. Useful values: `20` for the engagement-signal tier, `5` for ~65% of all plays covered, `1` for the long tail (will take many nights — see Limitations above).

The defaults are deliberately conservative. If you're impatient, you can lower `--rate-interval` — but read the Limitations section above first. Spotify's daily quota for Dev Mode apps is real and a fast burst hits it hard.

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

For the why behind the schema choices — what got considered and rejected, and why this is SQLite instead of Postgres — see the companion piece **music-data: The Schema Story** (publishing shortly on TheDeLay.com).

---

## Schema overview

See [`sql/schema.sql`](sql/schema.sql) for the full DDL with comments. Highlights:

- **`plays`** — one row per play event (track, podcast episode, or audiobook chapter)
- **`tracks`, `albums`, `artists`** — normalized entities, populated incrementally during ingest and enrichment
- **`track_labels`, `album_labels`, `artist_labels`** — your editorial layer with audit history; supports inheritance via the `v_track_effective_labels` view (track > album > artist > NULL)
- **`v_track_plays`, `v_track_summary`, `v_artist_summary`** — analytical views with computed engagement metrics

Roughly 20 tables, a handful of triggers for label history, and a few indexes tuned for the common queries.

---

## API architecture (the technical why)

A few things to know about how the pipeline talks to Spotify, beyond the limits in the section above:

- **Single-ID GETs only.** Spotify's batch endpoints return 403 for Dev Mode apps post-Feb-2026. Single-ID endpoints (`/tracks/{id}`) still work, so the client uses those exclusively. This is the project's permanent architecture, not a workaround.
- **Client Credentials grant** for catalog reads — no user OAuth, no browser dance, runs unattended.
- **Global 429 backoff.** When Spotify returns 429 on any call, *all* subsequent calls wait per the `Retry-After` header — not just the one URL that 429'd. Without this, after a 429 we'd retry the one URL but immediately hammer the next URL using the same bucket.
- **Long-Retry-After hard stop.** If a single 429 returns Retry-After above 60 seconds, the run aborts immediately rather than sleeping through it. This matters: making more calls during a penalty bucket can extend or worsen the penalty.
- **Be a good API citizen.** Read [Spotify's Developer Terms](https://developer.spotify.com/terms) before running a full enrichment pass. §VI.4 prohibits "excessive service calls"; §IV.2.2 prohibits crawler/spider behavior. This pipeline stays defensible by enriching only what *you* have personally listened to (no catalog crawling), filtering by play-count threshold, throttling well below any plausible rate limit, and identifying itself with a clear User-Agent.

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

- [Spotify Has 12 Years of My Data. I Just Took It Back.](https://thedelay.com/spotify-music-archive/) — the why
- [music-data: The Schema Story](https://thedelay.com/spotify-music-archive-nerd-stuff/) — the how (publishing soon)
- [TheDeLay.com](https://thedelay.com/) — homelab, AI integration, and InfoSec writing
