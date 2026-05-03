# Set up music-data with your AI assistant

> **v0.1 stub.** This file is functionally complete — every step below is real and current. A more polished version (with sample dialogue, error-recovery patterns, and OS-specific edge cases) will replace it when the article-publish session ships its full version. Until then, this gets you running.

## What this file is

A self-contained instruction set you can hand to any AI assistant with shell access — Claude Code, Cursor, ChatGPT Desktop with code interpreter, Cowork, etc. The AI reads this file, asks you the questions only you can answer, and drives the rest of setup itself.

The thesis behind this approach is the same one in the [companion article](https://thedelay.com/spotify-music-archive/): give your AI structured context, and the conversation becomes work — not chat. This file is the structured context for setup.

## Before you start: critical limitations

**Read the [README's "Known limitations" section](./README.md#before-you-commit-known-limitations) before letting your AI drive setup.** Two things matter most:

1. **Spotify Premium is required.** As of February 2026, every Spotify Web API Development Mode app requires the app owner to have an active Premium subscription. No Premium → this pipeline will not function. Don't spend 30 minutes setting up only to discover this at the smoke test.
2. **Several Spotify metadata fields will be empty for you.** `track.popularity`, `artist.popularity`, `artist.followers`, and `artist.genres` all return NULL/empty for new Dev Mode apps post-Feb-2026. The engagement queries (the article's central reveals) work today; genre-aware features will require future MusicBrainz integration.

Tell your AI assistant: *"Read the README's Limitations section first and confirm I have Premium before starting setup."* The good ones will respect that.

## The prompt to paste

Open your AI assistant in your shell (or paired with your editor) and paste:

> I want to set up the `music-data` project from https://github.com/TheDeLay/music-data. I have `setup-with-ai.md` open in this folder (or you can fetch it from the repo). Read it, ask me what you need to know, and walk me through setup end to end. Check with me before any destructive operation. When you're done, run a smoke test and report results.

That's the whole interaction. The AI will take it from there.

## What the AI will need from you

The AI will pause and ask you for these:

1. **Confirmation that you have Spotify Premium.** Required since February 2026 (see Limitations above). The AI should ask before doing anything else; if it doesn't, prompt it.
2. **Where to clone the repo.** Default: `~/git/music-data` or wherever you keep code.
3. **Spotify Developer App credentials.** If you don't have an app yet, the AI walks you through registering one at https://developer.spotify.com/dashboard. The redirect URI must be exactly `http://127.0.0.1:8888/callback` — Spotify rejects `localhost` since their April 2025 policy update.
4. **Where your Spotify data dump lives.** You request it from https://www.spotify.com/account/privacy ("Extended streaming history" — Spotify says up to 30 days; in practice, it often arrives in a few). Point the AI at the unzipped folder when it's ready.
5. **How chatty you want it.** Some users want the AI to narrate every command; others want "just do it, tell me when it's done." Tell it which up front.

## What the AI will run

These are the same steps in [`INSTALL.md`](./INSTALL.md) — your AI is just driving them for you.

1. Clone the repo.
2. Create a Python venv and `pip install -r requirements.txt`.
3. Copy `.env.example` to `.env`, prompt you for credentials, lock to `chmod 600`.
4. Initialize the SQLite database with `python -m scripts.db init`.
5. (When dump arrives) Run `python -m scripts.ingest_dump <your-path>`.
6. Run a smoke-test enrichment with the safe default throttle: `python -m scripts.enrich --tracks --min-plays 100 --rate-interval 35.0` — about 25 tracks, ~15 minutes. The `35.0` matches the README's recommended default; tells you the API works without burning meaningful daily quota.
7. Verify with `python -m scripts.check_state` and a quick query against `v_track_summary`.

## Things only you can do

- Click through Spotify OAuth consent in your browser (the AI shouldn't be doing this for you).
- Request your privacy export from Spotify and wait for the email.
- Decide whether to scale enrichment beyond the smoke test. The full top-tier (`--min-plays 20` at the safe `--rate-interval 35.0`, ~9-12 hours throttled depending on your library size) is a real API workload — your AI shouldn't auto-launch it without your explicit go-ahead. Plan for an overnight run.
- Read [Spotify's Developer Terms](https://developer.spotify.com/terms). §VI.4 (excessive calls) and §IV.2.2 (no crawling) matter for any external-API project.

## When your AI gets stuck

Fall back to the manual path: open [`INSTALL.md`](./INSTALL.md) and walk through it yourself. It covers the same ground in human-readable form. Or skim the [Quick start section in the README](./README.md#quick-start-path-2-condensed) — that's the condensed version.

## Why this file exists

The article makes a claim: the difference between an AI that *generally helps* and one that *actually knows you* is whether you've given it structured access to your real data. This setup file is that claim applied to setup itself — a structured contract between human and AI about who does what. The AI handles the plumbing; you handle the consent, the credentials, and the data ownership decisions.

---

*Stub v0.1 — John DeLay, 2026-05-01. Slot for replacement: an AI-optimized walkthrough with example transcripts, error-recovery patterns, and per-assistant notes (Claude Code vs Cursor vs ChatGPT). The article-publish session is the canonical owner of the final version.*
