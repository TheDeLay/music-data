"""Progressive Spotify enrichment — tier-laddered, cron-friendly.

Walks the user's listening history in tiers, newest first, enriching only what
matters most until the per-night API quota is spent. Designed for unattended
nightly runs.

Tier ladder (default):
    recent      0-30d        >=3 plays in window
    30-90d      30-90d       >=3 plays
    90-365d     90-365d      >=5 plays
    1-2y        365-730d     >=8 plays
    2-3y        730-1095d    >=12 plays
    >3y         opt-in via --max-age

A track that's been enriched (last_enriched_at IS NOT NULL) drops out of every
tier automatically. New tracks crossing a tier's min_plays bar become candidates
on the next run.

Usage:
    # Preview tonight's plan, no API calls
    python -m scripts.enrich_progressive --dry-run

    # Default: up to 600 calls, walking down the ladder
    python -m scripts.enrich_progressive

    # Custom quota
    python -m scripts.enrich_progressive --daily-quota 400

    # Extend horizon
    python -m scripts.enrich_progressive --max-age 1095d   # 3 years
    python -m scripts.enrich_progressive --max-age all     # entire archive

Exit codes:
    0  clean completion (tier(s) advanced or already up to date)
    2  Spotify cooldown / watchdog abort
    3  config error (bad --max-age, etc.)
    4  unexpected error
"""
from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
)

from .db import connect, init_schema, start_run, finish_run
from .enrich import (
    TRACK_BATCH,
    _upsert_album_from_api,
    _upsert_artist_from_api,
)
from .spotify_client import (
    LongPenaltyError,
    RateLimitError,
    SpotifyClient,
)

console = Console()
log = logging.getLogger("enrich-progressive")


# ---------------------------------------------------------------------------
# Tier ladder
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Tier:
    name: str
    min_days: int  # window start (older bound, in days ago)
    max_days: int  # window end (younger bound, in days ago)
    min_plays: int


DEFAULT_TIERS: tuple[Tier, ...] = (
    Tier("recent",   0,    30,   3),
    Tier("30-90d",   30,   90,   3),
    Tier("90-365d",  90,   365,  5),
    Tier("1-2y",     365,  730,  8),
    Tier("2-3y",     730,  1095, 12),
)

DEFAULT_DAILY_QUOTA = 600
DEFAULT_RATE_INTERVAL = 35.0          # per-call sleep; 600 calls -> ~5h50m
DEFAULT_MAX_AGE_DAYS = 365            # spec default — opt in to extend
DEFAULT_LONG_PENALTY = 60.0
DEFAULT_MAX_NO_PROGRESS = 600.0


# ---------------------------------------------------------------------------
# Candidate selection
# ---------------------------------------------------------------------------
def select_tier_candidates(conn: sqlite3.Connection, tier: Tier) -> list[sqlite3.Row]:
    """Tracks in this tier's window with >=min_plays plays AND not yet enriched.

    Ordered by plays_in_window DESC so per-night truncation always takes the
    most-engaged candidates first.
    """
    return conn.execute(
        """
        SELECT t.track_id AS id,
               t.spotify_track_uri AS uri,
               COUNT(p.play_id) AS plays_in_window
        FROM tracks t
        JOIN plays p
          ON p.track_id = t.track_id
         AND p.content_type = 'track'
        WHERE t.last_enriched_at IS NULL
          AND t.spotify_track_uri IS NOT NULL
          AND p.ts >= datetime('now', '-' || ? || ' days')
          AND p.ts <  datetime('now', '-' || ? || ' days')
        GROUP BY t.track_id
        HAVING COUNT(p.play_id) >= ?
        ORDER BY plays_in_window DESC, t.track_id ASC
        """,
        (tier.max_days, tier.min_days, tier.min_plays),
    ).fetchall()


@dataclass
class TierPlan:
    tier: Tier
    candidates_total: int
    rows_taken: list[sqlite3.Row]    # actual rows enriched this run (may be subset)
    skipped_reason: str | None = None  # set when tier is skipped (e.g., beyond --max-age)

    @property
    def take(self) -> int:
        return len(self.rows_taken)


def plan_tonight(
    conn: sqlite3.Connection,
    daily_quota: int,
    max_age_days: int,
    tiers: Iterable[Tier] = DEFAULT_TIERS,
) -> tuple[list[TierPlan], int]:
    """Walk the tier ladder, taking up to daily_quota candidates total.

    Returns (per-tier plans, total tracks to enrich tonight).
    """
    plans: list[TierPlan] = []
    budget = daily_quota
    for tier in tiers:
        if tier.min_days >= max_age_days:
            plans.append(TierPlan(tier, 0, [], skipped_reason="beyond --max-age"))
            continue
        # If --max-age cuts through the middle of this tier's window, clip
        # max_days down so we don't enrich anything older than the horizon.
        # The tier name + min_plays bar are preserved; only the window shrinks.
        effective = (
            tier if tier.max_days <= max_age_days
            else Tier(tier.name, tier.min_days, max_age_days, tier.min_plays)
        )
        rows = select_tier_candidates(conn, effective)
        total = len(rows)
        if budget <= 0:
            plans.append(TierPlan(tier, total, []))
            continue
        take = min(total, budget)
        plans.append(TierPlan(tier, total, rows[:take]))
        budget -= take
    return plans, daily_quota - budget


# ---------------------------------------------------------------------------
# Per-chunk enrichment (slim mirror of enrich.enrich_tracks loop body)
# ---------------------------------------------------------------------------
def enrich_chunk(
    conn: sqlite3.Connection,
    client: SpotifyClient,
    chunk: list[sqlite3.Row],
) -> int:
    """Fetch + persist a chunk of tracks. Returns rows updated.

    RateLimitError (LongPenaltyError / SustainedRateLimitError) propagates so
    the top-level run loop aborts cleanly.
    """
    uris = [r["uri"] for r in chunk]
    api_tracks = client.get_tracks(uris)  # may raise RateLimitError

    updated = 0
    conn.execute("BEGIN")
    try:
        for uri, api_track in zip(uris, api_tracks):
            if api_track is None:
                # 404 or retries exhausted — leave last_enriched_at NULL so
                # a future run can pick it up.
                continue

            album_id = None
            api_album = api_track.get("album") or {}
            if api_album.get("uri"):
                album_id = _upsert_album_from_api(conn, api_album)

            conn.execute(
                """
                UPDATE tracks
                SET name = ?,
                    album_id = COALESCE(?, album_id),
                    duration_ms = ?,
                    explicit = ?,
                    popularity = ?,
                    isrc = ?,
                    last_enriched_at = datetime('now')
                WHERE spotify_track_uri = ?
                """,
                (
                    api_track.get("name"),
                    album_id,
                    api_track.get("duration_ms"),
                    int(bool(api_track.get("explicit"))) if api_track.get("explicit") is not None else None,
                    api_track.get("popularity"),
                    (api_track.get("external_ids") or {}).get("isrc"),
                    uri,
                ),
            )

            tid_row = conn.execute(
                "SELECT track_id FROM tracks WHERE spotify_track_uri = ?", (uri,)
            ).fetchone()
            if tid_row:
                track_id = tid_row["track_id"]
                api_artists = api_track.get("artists") or []
                conn.execute("DELETE FROM track_artists WHERE track_id = ?", (track_id,))
                for pos, ar in enumerate(api_artists):
                    artist_id = _upsert_artist_from_api(conn, ar)
                    conn.execute(
                        "INSERT OR IGNORE INTO track_artists (track_id, artist_id, position) "
                        "VALUES (?, ?, ?)",
                        (track_id, artist_id, pos),
                    )
            updated += 1
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return updated


# ---------------------------------------------------------------------------
# Dry-run preview
# ---------------------------------------------------------------------------
def _format_dry_run(
    plans: list[TierPlan],
    total_planned: int,
    daily_quota: int,
    max_age_days: int,
    rate_interval: float,
) -> str:
    """Return a human-readable plan preview. Pure function for testability."""
    lines = []
    lines.append("Progressive Enrichment — Dry Run")
    lines.append("=" * 32)
    lines.append("")
    lines.append("Tier Status:")
    for p in plans:
        t = p.tier
        bar = f"({t.min_days}-{t.max_days}d, >={t.min_plays} plays)"
        if p.skipped_reason:
            lines.append(f"  {t.name:<10} {bar:<22} skipped — {p.skipped_reason}")
        else:
            status = "in plan" if p.take > 0 else (
                "queued (no quota left)" if p.candidates_total > 0 else "up to date"
            )
            lines.append(
                f"  {t.name:<10} {bar:<22} {p.candidates_total:>5} candidates  [{status}]"
            )
    lines.append("")
    lines.append(f"Tonight's Plan (daily quota: {daily_quota} calls):")
    if total_planned == 0:
        lines.append("  Nothing to do — all in-horizon tiers already up to date.")
    else:
        for p in plans:
            if p.take > 0:
                tail = " (tier completes)" if p.take == p.candidates_total else " (tier continues tomorrow)"
                lines.append(f"  -> {p.take:>4} calls on tier '{p.tier.name}'{tail}")
        lines.append(f"  = {total_planned} total calls")
    if total_planned > 0:
        secs = int(total_planned * rate_interval)
        h, rem = divmod(secs, 3600)
        m = rem // 60
        lines.append("")
        lines.append(f"Estimated wall clock: ~{h}h {m}m at {rate_interval:.0f}s/call")
    if max_age_days < 10**8:
        lines.append("")
        lines.append(f"Horizon: {max_age_days} days. Pass --max-age 'all' to include older tracks.")
    lines.append("")
    lines.append("To proceed: re-run without --dry-run.")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def _configure_logging(log_file: Path | None, level: str) -> Path:
    """Per-run log file plus WARNING+ to stderr. Mirrors enrich.py's setup but
    with a distinct filename prefix so cron tail is unambiguous."""
    if log_file is None:
        ts = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
        log_dir = Path(__file__).resolve().parent.parent / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f"enrich-progressive-{ts}.log"
    else:
        log_file.parent.mkdir(parents=True, exist_ok=True)

    fmt = logging.Formatter(
        "[%(asctime)s] %(levelname)-5s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_h = logging.FileHandler(log_file)
    file_h.setFormatter(fmt)
    file_h.setLevel(getattr(logging, level.upper()))

    stream_h = logging.StreamHandler(sys.stderr)
    stream_h.setFormatter(fmt)
    stream_h.setLevel(logging.WARNING)

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.handlers.clear()
    root.addHandler(file_h)
    root.addHandler(stream_h)
    return log_file


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
def _parse_max_age(spec: str) -> int:
    """Parse --max-age into days. Accepts '365d', '1095d', 'all'."""
    if spec == "all":
        return 10**9
    if spec.endswith("d"):
        try:
            return int(spec[:-1])
        except ValueError:
            pass
    raise ValueError(
        f"--max-age must be like '365d' or 'all'; got {spec!r}"
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Progressive Spotify enrichment (tier-laddered, cron-friendly)",
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Print tonight's plan; make zero API calls.")
    parser.add_argument("--daily-quota", type=int, default=DEFAULT_DAILY_QUOTA,
                        help=f"Max API calls tonight. Default {DEFAULT_DAILY_QUOTA} "
                             f"(under empirical Spotify ~700-900/day ceiling).")
    parser.add_argument("--max-age", default=f"{DEFAULT_MAX_AGE_DAYS}d",
                        help=f"Tier ladder horizon. e.g. '{DEFAULT_MAX_AGE_DAYS}d' "
                             f"(default), '1095d' (3y), 'all'. Tracks older than this "
                             f"are not candidates.")
    parser.add_argument("--rate-interval", type=float, default=DEFAULT_RATE_INTERVAL,
                        help=f"Seconds between API calls. Default {DEFAULT_RATE_INTERVAL}s. "
                             f"35s x 600 calls fits in one overnight window.")
    parser.add_argument("--max-no-progress", type=float, default=DEFAULT_MAX_NO_PROGRESS,
                        help="Abort if no successful API response in this many seconds.")
    parser.add_argument("--long-penalty-threshold", type=float, default=DEFAULT_LONG_PENALTY,
                        help="If a single 429 returns Retry-After above this, abort.")
    parser.add_argument("--log-file", type=Path, default=None,
                        help="Path to per-run log file. Default: auto-generated.")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    parser.add_argument("--db", type=Path, default=None,
                        help="Path to music.db (default: project's data/music.db)")
    return parser


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

    try:
        max_age_days = _parse_max_age(args.max_age)
    except ValueError as e:
        console.print(f"[red]Config error:[/red] {e}")
        return 3

    log_path = _configure_logging(args.log_file, args.log_level)

    log.info("=" * 60)
    log.info("Progressive enrichment starting")
    log.info(
        "  dry_run=%s daily_quota=%d max_age=%dd rate=%.1fs",
        args.dry_run, args.daily_quota, max_age_days, args.rate_interval,
    )
    log.info("  log file: %s", log_path)

    conn = connect(args.db) if args.db else connect()
    init_schema(conn)

    try:
        plans, total_planned = plan_tonight(conn, args.daily_quota, max_age_days)
    except sqlite3.OperationalError as e:
        log.exception("plan_tonight failed: %s", e)
        console.print(f"[red]DB error during planning:[/red] {e}")
        return 4

    # Always log the plan summary, dry-run or not.
    for p in plans:
        if p.skipped_reason:
            log.info("Tier %-10s skipped (%s)", p.tier.name, p.skipped_reason)
        else:
            log.info(
                "Tier %-10s candidates=%d  taking=%d",
                p.tier.name, p.candidates_total, p.take,
            )
    log.info("Total tonight: %d calls", total_planned)

    if args.dry_run:
        console.print(_format_dry_run(
            plans, total_planned, args.daily_quota, max_age_days, args.rate_interval,
        ))
        return 0

    if total_planned == 0:
        console.print("[green]Nothing to enrich — all in-horizon tiers up to date.[/green]")
        return 0

    run_id = start_run(
        conn,
        source="enrichment-progressive",
        notes=f"daily_quota={args.daily_quota} max_age={max_age_days}d "
              f"rate={args.rate_interval}s",
    )
    client = SpotifyClient(
        auth="app",
        min_request_interval=args.rate_interval,
        max_no_progress_seconds=args.max_no_progress,
        long_penalty_threshold_seconds=args.long_penalty_threshold,
    )

    started_at = datetime.now()
    total_updated = 0

    try:
        with Progress(
            TextColumn("[bold]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            for p in plans:
                if p.take == 0:
                    continue
                task = progress.add_task(f"Tier {p.tier.name}", total=p.take)
                log.info(
                    "Tier '%s' begin: %d / %d candidates this run",
                    p.tier.name, p.take, p.candidates_total,
                )
                for i in range(0, p.take, TRACK_BATCH):
                    chunk = p.rows_taken[i:i + TRACK_BATCH]
                    total_updated += enrich_chunk(conn, client, chunk)
                    progress.advance(task, len(chunk))
                log.info(
                    "Tier '%s' end: cumulative updated=%d  stats=%s",
                    p.tier.name, total_updated, client.stats,
                )
    except RateLimitError as e:
        elapsed = (datetime.now() - started_at).total_seconds()
        kind = "long-penalty" if isinstance(e, LongPenaltyError) else "watchdog"
        log.error("Aborted (%s) after %.0fs: %s", kind, elapsed, e)
        log.error("  final stats: %s", client.stats)
        log.error("  rows updated before abort: %d", total_updated)
        finish_run(
            conn, run_id, status="aborted",
            notes=f"{kind}: {e}", rows_added=total_updated,
        )
        console.print(f"[yellow]Aborted ({kind}):[/yellow] {e}")
        console.print(
            f"[yellow]Updated {total_updated:,} row(s) before abort. "
            f"Log: {log_path}[/yellow]"
        )
        return 2
    except Exception as e:
        log.exception("Progressive enrichment failed unexpectedly: %s", e)
        finish_run(conn, run_id, status="failed", notes=str(e))
        console.print(f"[red]Failed:[/red] {e}")
        return 4

    elapsed = (datetime.now() - started_at).total_seconds()
    log.info(
        "Complete. updated=%d elapsed=%.0fs stats=%s",
        total_updated, elapsed, client.stats,
    )
    finish_run(conn, run_id, status="completed", rows_added=total_updated)
    console.print(
        f"[green]Complete.[/green] Updated {total_updated:,} row(s) "
        f"in {elapsed:.0f}s."
    )
    console.print(f"[dim]Stats: {client.stats}  log: {log_path}[/dim]")
    return 0


if __name__ == "__main__":
    sys.exit(main())
