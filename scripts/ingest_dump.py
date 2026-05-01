"""CLI: load the Spotify extended streaming history dump into the database.

Usage:
    python scripts/ingest_dump.py /path/to/dump-directory/
    python scripts/ingest_dump.py /path/to/dump-directory/ --dry-run
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, MofNCompleteColumn, TimeElapsedColumn, TextColumn

from .db import connect, init_schema, start_run, finish_run
from .extractors import iter_dump_records, from_dump_record, safe_extract
from .loader import LoadStats, load_play, quarantine

BATCH_SIZE = 1000
console = Console()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Ingest Spotify extended streaming history dump")
    parser.add_argument("dump_dir", type=Path, help="Directory containing Streaming_History_*.json files")
    parser.add_argument("--dry-run", action="store_true", help="Parse and validate but do not write to DB")
    parser.add_argument("--limit", type=int, default=None, help="Stop after this many records (for testing)")
    args = parser.parse_args(argv)

    if not args.dump_dir.is_dir():
        console.print(f"[red]Not a directory:[/red] {args.dump_dir}")
        return 2

    conn = connect()
    init_schema(conn)

    run_id = start_run(conn, source="extended_dump", input_path=str(args.dump_dir),
                       notes="dry-run" if args.dry_run else None)
    stats = LoadStats()

    # First pass: count files & approximate total records (cheap; we'll just count files)
    files = list(args.dump_dir.glob("Streaming_History_*.json"))
    if not files:
        files = list(args.dump_dir.glob("*.json"))
    console.print(f"Found {len(files)} JSON file(s) in {args.dump_dir}")

    batch: list = []
    processed = 0

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Loading dump", total=None)

        for path, idx, raw in iter_dump_records(args.dump_dir):
            if "_file_error" in raw:
                console.print(f"[yellow]Skipping {path.name}: {raw['_file_error']}[/yellow]")
                continue

            rec, err = safe_extract(from_dump_record, raw)
            if err is not None:
                if not args.dry_run:
                    quarantine(conn, run_id, raw, f"{path.name}#{idx}: {err}")
                stats.quarantined += 1
                processed += 1
                progress.advance(task)
                continue

            batch.append(rec)
            processed += 1
            progress.advance(task)

            if len(batch) >= BATCH_SIZE:
                if not args.dry_run:
                    _flush(conn, batch, run_id, stats)
                batch.clear()

            if args.limit and processed >= args.limit:
                break

        # Final flush
        if batch and not args.dry_run:
            _flush(conn, batch, run_id, stats)

    final_status = "completed" if not args.dry_run else "completed"
    finish_run(
        conn, run_id, status=final_status,
        rows_added=stats.added, rows_skipped=stats.skipped, rows_failed=stats.failed,
        notes=(f"dry-run; would have processed {processed} records" if args.dry_run else None),
    )

    console.print()
    console.print("[bold]Ingest summary[/bold]")
    console.print(f"  Records seen:    {processed:,}")
    console.print(f"  Added:           {stats.added:,}")
    console.print(f"  Skipped (dup):   {stats.skipped:,}")
    console.print(f"  Quarantined:     {stats.quarantined:,}")
    console.print(f"  Failed:          {stats.failed:,}")
    if args.dry_run:
        console.print("  [yellow]Dry run — no changes written.[/yellow]")
    return 0


def _flush(conn, batch, run_id: int, stats: LoadStats) -> None:
    """Apply a batch in a single transaction."""
    conn.execute("BEGIN")
    try:
        for rec in batch:
            outcome = load_play(conn, rec, run_id)
            if outcome == "added":
                stats.added += 1
            else:
                stats.skipped += 1
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


if __name__ == "__main__":
    sys.exit(main())
