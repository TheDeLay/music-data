-- =============================================================================
-- music-data schema
-- =============================================================================
-- Personal Spotify listening archive.
-- Designed for SQLite first; portable to Postgres with minimal changes.
--
-- Source data:
--   1. Spotify "Extended streaming history" data dump (one-shot, historical)
--   2. Spotify Web API "recently played" endpoint (incremental, ongoing)
--   3. Spotify Web API track/artist/album lookups (enrichment)
--
-- Design principles:
--   - Normalized: tracks, artists, albums each get their own entity tables
--   - Polymorphic plays: one plays table covers tracks, episodes, audiobooks
--   - Idempotent ingest: every insert is safe to retry
--   - Provenance: every row knows where it came from and when
--   - Stable URIs as natural keys for entities
-- =============================================================================

PRAGMA foreign_keys = ON;

-- -----------------------------------------------------------------------------
-- ARTISTS
-- -----------------------------------------------------------------------------
-- The dump gives us only artist NAMES (strings). Artist URIs and genres come
-- from the API enrichment pass that runs after ingest.
--
-- Why artist_id (integer) and not just spotify_artist_uri as PK?
--   - At ingest time, we know the name but NOT the URI yet (dump limitation).
--   - We create the artist row immediately, fill URI later when we enrich.
--   - Stable integer keys also produce smaller indexes than string keys.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS artists (
    artist_id           INTEGER PRIMARY KEY,
    spotify_artist_uri  TEXT UNIQUE,                    -- nullable until enriched
    name                TEXT NOT NULL,
    name_normalized     TEXT NOT NULL,                  -- lowercase, trimmed; for dedup-by-name pre-enrichment
    genres_json         TEXT,                           -- JSON array: ["thrash metal", "heavy metal"]
    popularity          INTEGER,                        -- 0-100, from API
    followers           INTEGER,                        -- from API
    last_enriched_at    TEXT,                           -- ISO 8601 timestamp; NULL = never enriched
    first_seen_at       TEXT NOT NULL DEFAULT (datetime('now'))
);

-- We deduplicate artists by normalized name during initial ingest.
-- Once enriched and given a URI, that becomes the stable identity.
CREATE INDEX IF NOT EXISTS idx_artists_name_normalized ON artists(name_normalized);
CREATE INDEX IF NOT EXISTS idx_artists_last_enriched ON artists(last_enriched_at);

-- -----------------------------------------------------------------------------
-- ALBUMS
-- -----------------------------------------------------------------------------
-- Similar story to artists: dump gives album NAME only. URI + release date
-- from API enrichment.
--
-- release_year is denormalized from release_date for fast indexed queries.
-- ("90s metal" => WHERE release_year BETWEEN 1990 AND 1999)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS albums (
    album_id            INTEGER PRIMARY KEY,
    spotify_album_uri   TEXT UNIQUE,                    -- nullable until enriched
    name                TEXT NOT NULL,
    name_normalized     TEXT NOT NULL,
    release_date        TEXT,                           -- "1986" or "1986-03-03" or "1986-03"
    release_year        INTEGER,                        -- parsed from release_date for indexing
    album_type          TEXT,                           -- album | single | compilation
    total_tracks        INTEGER,
    last_enriched_at    TEXT,
    first_seen_at       TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_albums_name_normalized ON albums(name_normalized);
CREATE INDEX IF NOT EXISTS idx_albums_release_year ON albums(release_year);
CREATE INDEX IF NOT EXISTS idx_albums_last_enriched ON albums(last_enriched_at);

-- -----------------------------------------------------------------------------
-- TRACKS
-- -----------------------------------------------------------------------------
-- spotify_track_uri is in the dump — this is the ONLY entity we can identify
-- with certainty at ingest time. Everything else (album_id, duration, popularity)
-- comes from API enrichment.
--
-- duration_ms is critical: it's how we compute "what % of this song did I play?"
-- which you flagged as a priceless metric. Worth getting right during enrichment.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tracks (
    track_id            INTEGER PRIMARY KEY,
    spotify_track_uri   TEXT UNIQUE NOT NULL,           -- always present from dump
    name                TEXT NOT NULL,
    album_id            INTEGER REFERENCES albums(album_id),
    duration_ms         INTEGER,                        -- from API; needed for percent_played
    explicit            INTEGER,                        -- 0/1, from API
    popularity          INTEGER,                        -- 0-100, from API
    isrc                TEXT,                           -- International Standard Recording Code, from API
    last_enriched_at    TEXT,
    first_seen_at       TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_tracks_album ON tracks(album_id);
CREATE INDEX IF NOT EXISTS idx_tracks_last_enriched ON tracks(last_enriched_at);

-- -----------------------------------------------------------------------------
-- TRACK_ARTISTS  (many-to-many join)
-- -----------------------------------------------------------------------------
-- Spotify tracks frequently have multiple artists (collabs, features).
-- The dump only gives the album_artist string (typically the primary artist).
-- After enrichment we populate ALL artists with their position.
--
-- position 0 = primary, 1+ = features, in display order.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS track_artists (
    track_id    INTEGER NOT NULL REFERENCES tracks(track_id),
    artist_id   INTEGER NOT NULL REFERENCES artists(artist_id),
    position    INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (track_id, artist_id)
);

CREATE INDEX IF NOT EXISTS idx_track_artists_artist ON track_artists(artist_id);

-- -----------------------------------------------------------------------------
-- SHOWS  (podcast shows)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS shows (
    show_id             INTEGER PRIMARY KEY,
    spotify_show_uri    TEXT UNIQUE,
    name                TEXT NOT NULL,
    publisher           TEXT,
    last_enriched_at    TEXT,
    first_seen_at       TEXT NOT NULL DEFAULT (datetime('now'))
);

-- -----------------------------------------------------------------------------
-- EPISODES  (podcast episodes)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS episodes (
    episode_id          INTEGER PRIMARY KEY,
    spotify_episode_uri TEXT UNIQUE NOT NULL,
    name                TEXT NOT NULL,
    show_id             INTEGER REFERENCES shows(show_id),
    duration_ms         INTEGER,
    release_date        TEXT,
    description         TEXT,
    last_enriched_at    TEXT,
    first_seen_at       TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_episodes_show ON episodes(show_id);
CREATE INDEX IF NOT EXISTS idx_episodes_last_enriched ON episodes(last_enriched_at);

-- -----------------------------------------------------------------------------
-- AUDIOBOOKS  (audiobook works)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS audiobooks (
    audiobook_id            INTEGER PRIMARY KEY,
    spotify_audiobook_uri   TEXT UNIQUE,
    title                   TEXT NOT NULL,
    last_enriched_at        TEXT,
    first_seen_at           TEXT NOT NULL DEFAULT (datetime('now'))
);

-- -----------------------------------------------------------------------------
-- AUDIOBOOK_CHAPTERS
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS audiobook_chapters (
    audiobook_chapter_id    INTEGER PRIMARY KEY,
    spotify_chapter_uri     TEXT UNIQUE NOT NULL,
    title                   TEXT,
    audiobook_id            INTEGER REFERENCES audiobooks(audiobook_id),
    duration_ms             INTEGER,
    last_enriched_at        TEXT,
    first_seen_at           TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_audiobook_chapters_book ON audiobook_chapters(audiobook_id);

-- -----------------------------------------------------------------------------
-- PLAYS  (the fact table — everything you've ever listened to)
-- -----------------------------------------------------------------------------
-- Polymorphic: exactly one of (track_id, episode_id, audiobook_chapter_id)
-- is non-NULL, determined by content_type. Enforced via CHECK constraint.
--
-- Dedup key: (ts, content_uri, ms_played) — covered by unique index below.
-- "content_uri" varies by type, so the unique index uses three nullable
-- columns; SQLite's unique index treats multiple NULLs as distinct unless
-- we use a partial index. We solve this with a single content_uri column
-- that ALWAYS holds the relevant URI regardless of type. Cheap and effective.
--
-- ms_played + reason_end together let us answer the "how engaged was I?"
-- question. percent_played is computed at query time as:
--     MIN(ms_played, duration_ms) * 100.0 / duration_ms
-- (capped at 100% to handle back-button replays inflating ms_played)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS plays (
    play_id                 INTEGER PRIMARY KEY,
    ts                      TEXT NOT NULL,                  -- ISO 8601 UTC, e.g. "2021-08-14T03:24:11Z"
    ms_played               INTEGER NOT NULL,
    content_type            TEXT NOT NULL CHECK (content_type IN ('track', 'episode', 'audiobook_chapter')),
    content_uri             TEXT NOT NULL,                  -- denormalized URI for fast dedup; matches one of the FKs
    track_id                INTEGER REFERENCES tracks(track_id),
    episode_id              INTEGER REFERENCES episodes(episode_id),
    audiobook_chapter_id    INTEGER REFERENCES audiobook_chapters(audiobook_chapter_id),

    platform                TEXT,                           -- e.g. "OS X 11.5.2 [arm 0]"
    conn_country            TEXT,                           -- ISO country code at time of play
    reason_start            TEXT,                           -- trackdone | clickrow | fwdbtn | backbtn | ...
    reason_end              TEXT,                           -- trackdone | fwdbtn | backbtn | endplay | logout | ...
    shuffle                 INTEGER,                        -- 0/1
    skipped                 INTEGER,                        -- 0/1 (Spotify's own skip flag, distinct from inferable from ms_played)
    offline                 INTEGER,                        -- 0/1
    incognito_mode          INTEGER,                        -- 0/1

    source                  TEXT NOT NULL,                  -- 'extended_dump' | 'recently_played_api' | 'top_tracks_api'
    ingested_at             TEXT NOT NULL DEFAULT (datetime('now')),
    ingestion_run_id        INTEGER REFERENCES ingestion_runs(run_id),

    -- Polymorphic invariant: exactly one content FK matches the content_type.
    CHECK (
        (content_type = 'track'             AND track_id             IS NOT NULL AND episode_id IS NULL AND audiobook_chapter_id IS NULL) OR
        (content_type = 'episode'           AND episode_id           IS NOT NULL AND track_id   IS NULL AND audiobook_chapter_id IS NULL) OR
        (content_type = 'audiobook_chapter' AND audiobook_chapter_id IS NOT NULL AND track_id   IS NULL AND episode_id           IS NULL)
    )
);

-- Dedup unique index: same play event from multiple sources should collapse to one row.
CREATE UNIQUE INDEX IF NOT EXISTS uix_plays_dedup ON plays(ts, content_uri, ms_played);

-- Hot-path indexes for analytical queries.
CREATE INDEX IF NOT EXISTS idx_plays_ts ON plays(ts);
CREATE INDEX IF NOT EXISTS idx_plays_track ON plays(track_id) WHERE track_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_plays_episode ON plays(episode_id) WHERE episode_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_plays_content_type ON plays(content_type);
CREATE INDEX IF NOT EXISTS idx_plays_reason_end ON plays(reason_end);

-- -----------------------------------------------------------------------------
-- INGESTION_RUNS  (audit log for ingest operations)
-- -----------------------------------------------------------------------------
-- Every time we run an ingest script (dump load, API sync, enrichment),
-- we insert a row here. plays.ingestion_run_id back-references the run that
-- created it. This is your audit trail for "when did this data arrive and why?"
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ingestion_runs (
    run_id          INTEGER PRIMARY KEY,
    source          TEXT NOT NULL,                          -- 'extended_dump' | 'recently_played_api' | 'enrichment_artists' | ...
    started_at      TEXT NOT NULL DEFAULT (datetime('now')),
    completed_at    TEXT,
    status          TEXT NOT NULL DEFAULT 'running',        -- running | completed | failed
    rows_added      INTEGER NOT NULL DEFAULT 0,
    rows_skipped    INTEGER NOT NULL DEFAULT 0,
    rows_failed     INTEGER NOT NULL DEFAULT 0,
    notes           TEXT,                                   -- free-form: file paths, errors, params
    input_path      TEXT                                    -- source file or API endpoint
);

CREATE INDEX IF NOT EXISTS idx_ingestion_runs_source ON ingestion_runs(source);

-- -----------------------------------------------------------------------------
-- REJECTED_ROWS  (quarantine for malformed input records)
-- -----------------------------------------------------------------------------
-- When ingest encounters a record it can't validate (missing required fields,
-- bad types, weird timestamps), we DON'T crash the whole run. We park the
-- offending record here with the error reason and continue. Auditable later.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS rejected_rows (
    rejected_id         INTEGER PRIMARY KEY,
    ingestion_run_id    INTEGER NOT NULL REFERENCES ingestion_runs(run_id),
    raw_record          TEXT NOT NULL,                      -- JSON of the bad record
    error_reason        TEXT NOT NULL,
    rejected_at         TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_rejected_rows_run ON rejected_rows(ingestion_run_id);

-- =============================================================================
-- VIEWS  (convenience layer for common analytical queries)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- v_track_plays
-- -----------------------------------------------------------------------------
-- Flattened view of every track play with computed engagement metrics.
-- This is the workhorse view for "what did I actually listen to?" questions.
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS v_track_plays;
CREATE VIEW v_track_plays AS
SELECT
    p.play_id,
    p.ts,
    p.ms_played,
    t.duration_ms,
    -- percent_played, capped at 100% so back-button replays don't break aggregates
    CASE
        WHEN t.duration_ms IS NULL OR t.duration_ms = 0 THEN NULL
        ELSE ROUND(100.0 * MIN(p.ms_played, t.duration_ms) / t.duration_ms, 1)
    END AS percent_played,
    -- engagement bucket: useful for "love vs tolerate vs skip" analysis
    CASE
        WHEN t.duration_ms IS NULL OR t.duration_ms = 0 THEN 'unknown'
        WHEN p.ms_played * 1.0 / t.duration_ms < 0.25 THEN 'skipped'
        WHEN p.ms_played * 1.0 / t.duration_ms < 0.75 THEN 'partial'
        ELSE 'committed'
    END AS engagement,
    p.reason_start,
    p.reason_end,
    p.shuffle,
    p.skipped,
    p.platform,
    p.conn_country,
    t.track_id,
    t.spotify_track_uri,
    t.name AS track_name,
    a.album_id,
    a.name AS album_name,
    a.release_year,
    -- primary artist (position 0); other artists available via track_artists join
    pa.artist_id AS primary_artist_id,
    pa.name AS primary_artist_name,
    pa.genres_json AS primary_artist_genres,
    p.source,
    p.ingested_at
FROM plays p
JOIN tracks t ON p.track_id = t.track_id
LEFT JOIN albums a ON t.album_id = a.album_id
LEFT JOIN track_artists ta ON t.track_id = ta.track_id AND ta.position = 0
LEFT JOIN artists pa ON ta.artist_id = pa.artist_id
WHERE p.content_type = 'track';

-- -----------------------------------------------------------------------------
-- v_track_summary
-- -----------------------------------------------------------------------------
-- Per-track rollup: total plays, avg engagement, finish rate.
-- Answers "what tracks do I actually love vs. tolerate?"
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS v_track_summary;
CREATE VIEW v_track_summary AS
SELECT
    t.track_id,
    t.spotify_track_uri,
    t.name AS track_name,
    a.name AS album_name,
    a.release_year,
    pa.name AS primary_artist_name,
    COUNT(p.play_id) AS play_count,
    SUM(p.ms_played) AS total_ms_played,
    AVG(
        CASE
            WHEN t.duration_ms IS NULL OR t.duration_ms = 0 THEN NULL
            ELSE 100.0 * MIN(p.ms_played, t.duration_ms) / t.duration_ms
        END
    ) AS avg_percent_played,
    SUM(CASE WHEN p.reason_end = 'trackdone' THEN 1 ELSE 0 END) * 1.0 / COUNT(p.play_id) AS finish_rate,
    SUM(CASE WHEN p.reason_end = 'backbtn' THEN 1 ELSE 0 END) AS backbtn_count,
    MIN(p.ts) AS first_played_at,
    MAX(p.ts) AS last_played_at
FROM tracks t
JOIN plays p ON p.track_id = t.track_id
LEFT JOIN albums a ON t.album_id = a.album_id
LEFT JOIN track_artists ta ON t.track_id = ta.track_id AND ta.position = 0
LEFT JOIN artists pa ON ta.artist_id = pa.artist_id
WHERE p.content_type = 'track'
GROUP BY t.track_id, t.spotify_track_uri, t.name, a.name, a.release_year, pa.name;

-- -----------------------------------------------------------------------------
-- v_artist_summary
-- -----------------------------------------------------------------------------
-- Per-artist rollup across all their tracks you've played.
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS v_artist_summary;
CREATE VIEW v_artist_summary AS
SELECT
    ar.artist_id,
    ar.spotify_artist_uri,
    ar.name AS artist_name,
    ar.genres_json,
    COUNT(DISTINCT t.track_id) AS unique_tracks_played,
    COUNT(p.play_id) AS total_plays,
    SUM(p.ms_played) AS total_ms_played,
    AVG(
        CASE
            WHEN t.duration_ms IS NULL OR t.duration_ms = 0 THEN NULL
            ELSE 100.0 * MIN(p.ms_played, t.duration_ms) / t.duration_ms
        END
    ) AS avg_percent_played,
    MIN(p.ts) AS first_played_at,
    MAX(p.ts) AS last_played_at
FROM artists ar
JOIN track_artists ta ON ar.artist_id = ta.artist_id
JOIN tracks t ON ta.track_id = t.track_id
JOIN plays p ON p.track_id = t.track_id
WHERE p.content_type = 'track'
GROUP BY ar.artist_id, ar.spotify_artist_uri, ar.name, ar.genres_json;

-- -----------------------------------------------------------------------------
-- v_track_engagement
-- -----------------------------------------------------------------------------
-- Per-track engagement aggregates for the love-score engine.
-- Computes quality plays (≥ threshold % listened), backbutton replays,
-- recent quality plays, and raw engagement stats. Only includes tracks
-- that have duration_ms (i.e. have been enriched via the Spotify API).
--
-- The score.py script reads from this view and layers on the more complex
-- computations (skip-streak detection) that need Python.
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS v_track_engagement;
CREATE VIEW v_track_engagement AS
SELECT
    t.track_id,
    t.spotify_track_uri,
    t.name AS track_name,
    t.duration_ms,
    a.name AS album_name,
    a.release_year,
    pa.name AS primary_artist_name,
    COUNT(p.play_id) AS total_plays,
    -- Quality plays: listened to ≥ 80% of the track
    SUM(CASE WHEN p.ms_played * 1.0 / t.duration_ms >= 0.80 THEN 1 ELSE 0 END) AS quality_plays,
    -- Recent quality plays: quality plays in last 90 days
    SUM(CASE WHEN p.ts >= datetime('now', '-90 days')
              AND p.ms_played * 1.0 / t.duration_ms >= 0.80 THEN 1 ELSE 0 END) AS recent_quality,
    -- Back-button replays: strongest love signal
    SUM(CASE WHEN p.reason_end = 'backbtn' THEN 1 ELSE 0 END) AS backbutton_count,
    -- Recent plays (any engagement level, last 90 days)
    SUM(CASE WHEN p.ts >= datetime('now', '-90 days') THEN 1 ELSE 0 END) AS recent_plays,
    -- Deliberate quality: you chose it (clickrow) AND finished it (≥80%)
    -- This is the "I know when I want this song" signal
    SUM(CASE WHEN p.reason_start = 'clickrow'
              AND p.ms_played * 1.0 / t.duration_ms >= 0.80 THEN 1 ELSE 0 END) AS deliberate_quality,
    -- Skips (Spotify's own flag)
    SUM(CASE WHEN p.skipped = 1 THEN 1 ELSE 0 END) AS skip_count,
    -- Average percent played (capped at 100%)
    AVG(ROUND(100.0 * MIN(p.ms_played, t.duration_ms) / t.duration_ms, 1)) AS avg_pct_played,
    MIN(p.ts) AS first_played,
    MAX(p.ts) AS last_played
FROM tracks t
JOIN plays p ON p.track_id = t.track_id AND p.content_type = 'track'
LEFT JOIN albums a ON t.album_id = a.album_id
LEFT JOIN track_artists ta ON t.track_id = ta.track_id AND ta.position = 0
LEFT JOIN artists pa ON ta.artist_id = pa.artist_id
WHERE t.duration_ms IS NOT NULL AND t.duration_ms > 0
GROUP BY t.track_id, t.spotify_track_uri, t.name, t.duration_ms,
         a.name, a.release_year, pa.name;

-- =============================================================================
-- SCHEMA VERSION  (for future migrations)
-- =============================================================================
CREATE TABLE IF NOT EXISTS schema_meta (
    key         TEXT PRIMARY KEY,
    value       TEXT NOT NULL,
    updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

INSERT OR REPLACE INTO schema_meta (key, value) VALUES ('version', '1');
INSERT OR REPLACE INTO schema_meta (key, value) VALUES ('created_at', datetime('now'));

-- =============================================================================
-- LABELS  (user editorial layer)
-- =============================================================================
-- A generic labeling system for tracks, albums, and artists. Labels are
-- arbitrary key/value annotations the user applies to express judgment that
-- Spotify's metadata can't capture.
--
-- Examples:
--   ('workout', 'y')         -- works for workouts
--   ('workout', 'm')         -- "kinda" — usable but not ideal
--   ('workout', 'n')         -- not for workouts
--   ('family_safe', 'n')     -- skip when family is around
--   ('mood', 'melancholy')   -- multi-valued labels also work
--
-- Label keys are arbitrary; the schema doesn't constrain what dimensions
-- you track. Keep keys lowercase + snake_case for consistency.
--
-- NULL semantics:
--   Absence of a row for a (track, label_key) pair means UNKNOWN, not 'n'.
--   Strict queries must use explicit value matching.
--
-- Inheritance (resolved by v_track_effective_labels view):
--   track_label > album_label > artist_label > NULL
--   (track-level overrides everything; artist-level is the broadest fallback)
--
-- Audit trail:
--   Every change writes to *_history via triggers. Includes old + new value,
--   timestamp, who/what made the change, and an optional note. This is the
--   safety net for "messing this up sucks" \u2014 you can always see what changed.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- TRACK_LABELS
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS track_labels (
    track_id    INTEGER NOT NULL REFERENCES tracks(track_id),
    label_key   TEXT NOT NULL,
    label_value TEXT NOT NULL,
    set_at      TEXT NOT NULL DEFAULT (datetime('now')),
    set_by      TEXT,                       -- 'manual', 'rule:genre_match', 'bulk:2026_05_03', etc.
    note        TEXT,
    PRIMARY KEY (track_id, label_key)
);

CREATE INDEX IF NOT EXISTS idx_track_labels_key_value ON track_labels(label_key, label_value);

CREATE TABLE IF NOT EXISTS track_labels_history (
    history_id  INTEGER PRIMARY KEY,
    track_id    INTEGER NOT NULL,
    label_key   TEXT NOT NULL,
    old_value   TEXT,                       -- NULL on INSERT (first time)
    new_value   TEXT,                       -- NULL on DELETE
    op          TEXT NOT NULL CHECK (op IN ('INSERT','UPDATE','DELETE')),
    changed_at  TEXT NOT NULL DEFAULT (datetime('now')),
    changed_by  TEXT,
    note        TEXT
);

CREATE INDEX IF NOT EXISTS idx_track_labels_history_track ON track_labels_history(track_id);
CREATE INDEX IF NOT EXISTS idx_track_labels_history_key ON track_labels_history(label_key);

-- Audit triggers: capture every change to track_labels.
DROP TRIGGER IF EXISTS trg_track_labels_insert;
CREATE TRIGGER trg_track_labels_insert
AFTER INSERT ON track_labels
BEGIN
    INSERT INTO track_labels_history (track_id, label_key, old_value, new_value, op, changed_by, note)
    VALUES (NEW.track_id, NEW.label_key, NULL, NEW.label_value, 'INSERT', NEW.set_by, NEW.note);
END;

DROP TRIGGER IF EXISTS trg_track_labels_update;
CREATE TRIGGER trg_track_labels_update
AFTER UPDATE ON track_labels
WHEN OLD.label_value IS NOT NEW.label_value
BEGIN
    INSERT INTO track_labels_history (track_id, label_key, old_value, new_value, op, changed_by, note)
    VALUES (NEW.track_id, NEW.label_key, OLD.label_value, NEW.label_value, 'UPDATE', NEW.set_by, NEW.note);
END;

DROP TRIGGER IF EXISTS trg_track_labels_delete;
CREATE TRIGGER trg_track_labels_delete
AFTER DELETE ON track_labels
BEGIN
    INSERT INTO track_labels_history (track_id, label_key, old_value, new_value, op, changed_by, note)
    VALUES (OLD.track_id, OLD.label_key, OLD.label_value, NULL, 'DELETE', OLD.set_by, OLD.note);
END;

-- -----------------------------------------------------------------------------
-- ALBUM_LABELS
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS album_labels (
    album_id    INTEGER NOT NULL REFERENCES albums(album_id),
    label_key   TEXT NOT NULL,
    label_value TEXT NOT NULL,
    set_at      TEXT NOT NULL DEFAULT (datetime('now')),
    set_by      TEXT,
    note        TEXT,
    PRIMARY KEY (album_id, label_key)
);

CREATE INDEX IF NOT EXISTS idx_album_labels_key_value ON album_labels(label_key, label_value);

CREATE TABLE IF NOT EXISTS album_labels_history (
    history_id  INTEGER PRIMARY KEY,
    album_id    INTEGER NOT NULL,
    label_key   TEXT NOT NULL,
    old_value   TEXT,
    new_value   TEXT,
    op          TEXT NOT NULL CHECK (op IN ('INSERT','UPDATE','DELETE')),
    changed_at  TEXT NOT NULL DEFAULT (datetime('now')),
    changed_by  TEXT,
    note        TEXT
);

CREATE INDEX IF NOT EXISTS idx_album_labels_history_album ON album_labels_history(album_id);
CREATE INDEX IF NOT EXISTS idx_album_labels_history_key ON album_labels_history(label_key);

DROP TRIGGER IF EXISTS trg_album_labels_insert;
CREATE TRIGGER trg_album_labels_insert
AFTER INSERT ON album_labels
BEGIN
    INSERT INTO album_labels_history (album_id, label_key, old_value, new_value, op, changed_by, note)
    VALUES (NEW.album_id, NEW.label_key, NULL, NEW.label_value, 'INSERT', NEW.set_by, NEW.note);
END;

DROP TRIGGER IF EXISTS trg_album_labels_update;
CREATE TRIGGER trg_album_labels_update
AFTER UPDATE ON album_labels
WHEN OLD.label_value IS NOT NEW.label_value
BEGIN
    INSERT INTO album_labels_history (album_id, label_key, old_value, new_value, op, changed_by, note)
    VALUES (NEW.album_id, NEW.label_key, OLD.label_value, NEW.label_value, 'UPDATE', NEW.set_by, NEW.note);
END;

DROP TRIGGER IF EXISTS trg_album_labels_delete;
CREATE TRIGGER trg_album_labels_delete
AFTER DELETE ON album_labels
BEGIN
    INSERT INTO album_labels_history (album_id, label_key, old_value, new_value, op, changed_by, note)
    VALUES (OLD.album_id, OLD.label_key, OLD.label_value, NULL, 'DELETE', OLD.set_by, OLD.note);
END;

-- -----------------------------------------------------------------------------
-- ARTIST_LABELS
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS artist_labels (
    artist_id   INTEGER NOT NULL REFERENCES artists(artist_id),
    label_key   TEXT NOT NULL,
    label_value TEXT NOT NULL,
    set_at      TEXT NOT NULL DEFAULT (datetime('now')),
    set_by      TEXT,
    note        TEXT,
    PRIMARY KEY (artist_id, label_key)
);

CREATE INDEX IF NOT EXISTS idx_artist_labels_key_value ON artist_labels(label_key, label_value);

CREATE TABLE IF NOT EXISTS artist_labels_history (
    history_id  INTEGER PRIMARY KEY,
    artist_id   INTEGER NOT NULL,
    label_key   TEXT NOT NULL,
    old_value   TEXT,
    new_value   TEXT,
    op          TEXT NOT NULL CHECK (op IN ('INSERT','UPDATE','DELETE')),
    changed_at  TEXT NOT NULL DEFAULT (datetime('now')),
    changed_by  TEXT,
    note        TEXT
);

CREATE INDEX IF NOT EXISTS idx_artist_labels_history_artist ON artist_labels_history(artist_id);
CREATE INDEX IF NOT EXISTS idx_artist_labels_history_key ON artist_labels_history(label_key);

DROP TRIGGER IF EXISTS trg_artist_labels_insert;
CREATE TRIGGER trg_artist_labels_insert
AFTER INSERT ON artist_labels
BEGIN
    INSERT INTO artist_labels_history (artist_id, label_key, old_value, new_value, op, changed_by, note)
    VALUES (NEW.artist_id, NEW.label_key, NULL, NEW.label_value, 'INSERT', NEW.set_by, NEW.note);
END;

DROP TRIGGER IF EXISTS trg_artist_labels_update;
CREATE TRIGGER trg_artist_labels_update
AFTER UPDATE ON artist_labels
WHEN OLD.label_value IS NOT NEW.label_value
BEGIN
    INSERT INTO artist_labels_history (artist_id, label_key, old_value, new_value, op, changed_by, note)
    VALUES (NEW.artist_id, NEW.label_key, OLD.label_value, NEW.label_value, 'UPDATE', NEW.set_by, NEW.note);
END;

DROP TRIGGER IF EXISTS trg_artist_labels_delete;
CREATE TRIGGER trg_artist_labels_delete
AFTER DELETE ON artist_labels
BEGIN
    INSERT INTO artist_labels_history (artist_id, label_key, old_value, new_value, op, changed_by, note)
    VALUES (OLD.artist_id, OLD.label_key, OLD.label_value, NULL, 'DELETE', OLD.set_by, OLD.note);
END;

-- -----------------------------------------------------------------------------
-- v_track_effective_labels
-- -----------------------------------------------------------------------------
-- Resolves the effective label for every (track, label_key) pair using the
-- inheritance chain: track > album > artist > NULL.
--
-- "source_level" tells you WHERE the label came from, so queries can ask
-- "show me only tracks I've explicitly labeled" by filtering source_level='track'.
--
-- This view emits one row per (track_id, label_key) that has ANY label set
-- somewhere in the hierarchy. Tracks/labels with no setting at any level
-- simply don't appear \u2014 they're "unknown."
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS v_track_effective_labels;
CREATE VIEW v_track_effective_labels AS
WITH all_keys AS (
    -- Every (track_id, label_key) pair where SOMETHING is set in the chain
    SELECT t.track_id, tl.label_key FROM tracks t JOIN track_labels tl ON t.track_id = tl.track_id
    UNION
    SELECT t.track_id, al.label_key FROM tracks t
        JOIN album_labels al ON t.album_id = al.album_id
    UNION
    SELECT t.track_id, arl.label_key FROM tracks t
        JOIN track_artists ta ON t.track_id = ta.track_id AND ta.position = 0
        JOIN artist_labels arl ON ta.artist_id = arl.artist_id
)
SELECT
    ak.track_id,
    ak.label_key,
    COALESCE(tl.label_value, al.label_value, arl.label_value)  AS label_value,
    CASE
        WHEN tl.label_value  IS NOT NULL THEN 'track'
        WHEN al.label_value  IS NOT NULL THEN 'album'
        WHEN arl.label_value IS NOT NULL THEN 'artist'
    END AS source_level,
    COALESCE(tl.set_at, al.set_at, arl.set_at) AS set_at,
    COALESCE(tl.set_by, al.set_by, arl.set_by) AS set_by,
    COALESCE(tl.note,   al.note,   arl.note)   AS note
FROM all_keys ak
JOIN tracks t ON ak.track_id = t.track_id
LEFT JOIN track_labels tl
    ON tl.track_id = ak.track_id AND tl.label_key = ak.label_key
LEFT JOIN album_labels al
    ON al.album_id = t.album_id AND al.label_key = ak.label_key
LEFT JOIN track_artists ta
    ON ta.track_id = t.track_id AND ta.position = 0
LEFT JOIN artist_labels arl
    ON arl.artist_id = ta.artist_id AND arl.label_key = ak.label_key;

-- =============================================================================
-- LISTENING CONTEXTS (mode classification)
-- =============================================================================
-- Algorithm-discovered + user-named clusters of listening behavior.
-- Each cluster groups plays with similar time-of-day / day-of-week patterns
-- (e.g. "weekday mornings", "weekend evenings"). Tracks are associated with
-- one or more contexts via track_context_affinity.
--
-- Populated by scripts/cluster_modes.py (algorithm) and scripts/label_modes.py
-- (interactive labeling). The clustering is per-listener — see Phase-0 of
-- the engagement-model spec for shared-account handling.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS listening_contexts (
    context_id INTEGER PRIMARY KEY,
    cluster_id INTEGER NOT NULL UNIQUE,    -- algorithm-assigned label (0, 1, 2, ...)
    user_label TEXT NOT NULL DEFAULT '',   -- user-provided name; empty until labeled
    centroid_hour_cos REAL,                -- cluster centroid in feature space; nullable
    centroid_hour_sin REAL,                --   for backward compat with older runs
    centroid_is_weekend REAL,
    play_count INTEGER NOT NULL DEFAULT 0, -- # plays assigned to this cluster (denormalized)
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- -----------------------------------------------------------------------------
-- track_context_affinity
-- -----------------------------------------------------------------------------
-- Per-track, per-context score of how strongly a track belongs to that
-- listening context. affinity = (plays of track in cluster) / (total plays
-- of track). is_primary marks the highest-affinity context if it clears the
-- primary-threshold (default 0.50). Tracks below threshold are "all-context"
-- (no row marked primary).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS track_context_affinity (
    track_id INTEGER NOT NULL,
    context_id INTEGER NOT NULL,
    affinity REAL NOT NULL,
    is_primary INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (track_id, context_id),
    FOREIGN KEY (track_id) REFERENCES tracks(track_id) ON DELETE CASCADE,
    FOREIGN KEY (context_id) REFERENCES listening_contexts(context_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_tca_track ON track_context_affinity(track_id);
CREATE INDEX IF NOT EXISTS idx_tca_context ON track_context_affinity(context_id);

-- Bump schema version
INSERT OR REPLACE INTO schema_meta (key, value) VALUES ('version', '4');
