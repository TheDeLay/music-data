"""Generic tag-based artist classifier.

Reads a YAML file of (pattern, tag) rules and writes one row per match to
the artist_classifications table. The taxonomy is entirely user-driven —
this engine treats tag names as opaque strings. Use it for label-based
classification, era flags, ensemble-type tagging, mood categories, or any
other artist-level annotation you want to feed into playlist filtering.

The YAML format is a list of rule objects, each with a pattern and a tag:

    rules:
      - pattern: "Symphony Orchestra"
        tag: "orchestral"
      - pattern: "Trio"
        tag: "ensemble"

Patterns are matched case-insensitively as substrings against
artists.name_normalized (which is `name.strip().lower()`). One artist can
receive multiple tags from multiple matching rules.

Re-running the script with method='label_match' (default) is idempotent:
all previous label_match rows are deleted and replaced with the current
ruleset's output. Rows from other methods (e.g. 'manual') are preserved.

Usage:
    python -m scripts.classify_artists --rules config/my-tags.yaml
    python -m scripts.classify_artists --rules config/my-tags.yaml --dry-run
    python -m scripts.classify_artists --list-tags
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import yaml

from scripts.db import connect


DEFAULT_METHOD = "label_match"
DEFAULT_CONFIDENCE = 1.0


@dataclass(frozen=True)
class Rule:
    pattern: str       # substring to match against name_normalized
    tag: str           # user-defined tag to assign


@dataclass
class ClassifyStats:
    rules_loaded: int = 0
    artists_matched: int = 0          # distinct artists with >= 1 tag
    classifications_written: int = 0  # total (artist, tag) pairs


# ---------------------------------------------------------------------------
# YAML loading
# ---------------------------------------------------------------------------
class RulesError(Exception):
    """YAML loaded successfully but the contents are not a valid ruleset."""


def load_rules(path: Path) -> list[Rule]:
    """Parse a YAML file into Rule objects. Validates structure.

    Expected schema:
        rules:
          - pattern: str (non-empty, post-strip)
            tag: str (non-empty, post-strip)
          - ...

    Raises RulesError on missing/empty fields, FileNotFoundError on missing
    file, yaml.YAMLError on invalid YAML.
    """
    raw = yaml.safe_load(path.read_text())
    if raw is None:
        raise RulesError(f"{path}: file is empty")
    if not isinstance(raw, dict) or "rules" not in raw:
        raise RulesError(f"{path}: top-level 'rules:' key required")
    rules_raw = raw["rules"]
    if not isinstance(rules_raw, list):
        raise RulesError(f"{path}: 'rules' must be a list, got {type(rules_raw).__name__}")

    rules: list[Rule] = []
    for i, item in enumerate(rules_raw, 1):
        if not isinstance(item, dict):
            raise RulesError(f"{path}: rule #{i} is not a mapping")
        pattern = (item.get("pattern") or "").strip()
        tag = (item.get("tag") or "").strip()
        if not pattern:
            raise RulesError(f"{path}: rule #{i} has empty/missing 'pattern'")
        if not tag:
            raise RulesError(f"{path}: rule #{i} has empty/missing 'tag'")
        rules.append(Rule(pattern=pattern, tag=tag))
    return rules


# ---------------------------------------------------------------------------
# Matching
# ---------------------------------------------------------------------------
def match_artists(
    conn: sqlite3.Connection,
    rules: Iterable[Rule],
) -> dict[int, set[str]]:
    """Run each rule against the artists table; return {artist_id: {tag, ...}}.

    Matching is case-insensitive substring against artists.name_normalized.
    A single artist receiving the same tag from multiple rules is deduped
    via the set.
    """
    by_artist: dict[int, set[str]] = defaultdict(set)
    for rule in rules:
        like_pattern = f"%{rule.pattern.lower()}%"
        rows = conn.execute(
            "SELECT artist_id FROM artists WHERE name_normalized LIKE ?",
            (like_pattern,),
        ).fetchall()
        for r in rows:
            by_artist[r["artist_id"]].add(rule.tag)
    return by_artist


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------
def write_classifications(
    conn: sqlite3.Connection,
    matches: dict[int, set[str]],
    method: str,
    confidence: float,
) -> int:
    """Replace all rows for the given method, then insert the new matches.

    Wraps the delete+insert in a single transaction so a partial failure
    leaves the table in its previous state. Returns the number of rows
    written.
    """
    rows_to_insert = [
        (aid, tag, method, confidence)
        for aid, tags in matches.items()
        for tag in tags
    ]
    with conn:
        conn.execute(
            "DELETE FROM artist_classifications WHERE method = ?",
            (method,),
        )
        if rows_to_insert:
            conn.executemany(
                "INSERT INTO artist_classifications "
                "(artist_id, classification, method, confidence) "
                "VALUES (?, ?, ?, ?)",
                rows_to_insert,
            )
    return len(rows_to_insert)


def classify(
    conn: sqlite3.Connection,
    rules: list[Rule],
    method: str = DEFAULT_METHOD,
    confidence: float = DEFAULT_CONFIDENCE,
    dry_run: bool = False,
) -> tuple[ClassifyStats, dict[int, set[str]]]:
    """Run the full pipeline: match → (optionally) write → return stats.

    Returns (stats, matches). matches is keyed by artist_id even in dry-run
    mode so callers can preview what would be written.
    """
    matches = match_artists(conn, rules)
    written = 0
    if not dry_run:
        written = write_classifications(conn, matches, method, confidence)
    else:
        written = sum(len(tags) for tags in matches.values())

    stats = ClassifyStats(
        rules_loaded=len(rules),
        artists_matched=len(matches),
        classifications_written=written,
    )
    return stats, matches


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------
def list_tags(conn: sqlite3.Connection) -> list[tuple[str, str, int]]:
    """Return [(method, classification, count), ...] for what's in the DB."""
    return [
        (r["method"], r["classification"], r["n"])
        for r in conn.execute(
            "SELECT method, classification, COUNT(*) AS n "
            "FROM artist_classifications "
            "GROUP BY method, classification "
            "ORDER BY method, n DESC, classification"
        )
    ]


def print_run_summary(stats: ClassifyStats, matches: dict[int, set[str]],
                      conn: sqlite3.Connection, dry_run: bool) -> None:
    """Print a human-readable summary of what just happened (or would have)."""
    verb = "Would write" if dry_run else "Wrote"
    print(f"\n{verb} {stats.classifications_written} classification(s) "
          f"across {stats.artists_matched} artist(s) "
          f"(from {stats.rules_loaded} rule(s)).")

    tag_counts = Counter()
    for tags in matches.values():
        tag_counts.update(tags)
    if tag_counts:
        print("\nBy tag:")
        for tag, n in tag_counts.most_common():
            print(f"  {tag:30s}  {n:5d}")


def print_tags_summary(rows: list[tuple[str, str, int]]) -> None:
    if not rows:
        print("No classifications in DB. Run with --rules to populate.")
        return
    print(f"{'method':<15} {'classification':<35} {'count':>6}")
    print("-" * 60)
    for method, tag, n in rows:
        print(f"{method:<15} {tag:<35} {n:>6}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Tag artists in the DB based on a curated YAML ruleset.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--rules", type=str, default=None,
                        help="Path to a YAML rules file. Required unless "
                             "--list-tags is used.")
    parser.add_argument("--method", type=str, default=DEFAULT_METHOD,
                        help="Method tag stored alongside each row. "
                             "Re-runs replace all rows with this method.")
    parser.add_argument("--confidence", type=float, default=DEFAULT_CONFIDENCE,
                        help="Confidence value (0.0-1.0) recorded for each "
                             "classification. label_match defaults to 1.0.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Compute matches and print summary, but don't "
                             "write to the DB.")
    parser.add_argument("--list-tags", action="store_true",
                        help="Print what's currently in artist_classifications "
                             "and exit. Ignores --rules.")
    parser.add_argument("--db", type=str, default=None,
                        help="Path to music.db (default: auto-detect)")

    args = parser.parse_args(argv)

    conn = connect(args.db)
    try:
        if args.list_tags:
            print_tags_summary(list_tags(conn))
            return 0

        if not args.rules:
            parser.error("--rules is required (or pass --list-tags)")

        rules_path = Path(args.rules)
        try:
            rules = load_rules(rules_path)
        except FileNotFoundError:
            print(f"Rules file not found: {rules_path}", file=sys.stderr)
            return 2
        except (RulesError, yaml.YAMLError) as e:
            print(f"Invalid rules file: {e}", file=sys.stderr)
            return 2

        if not rules:
            print("Rules file contains no rules; nothing to do.", file=sys.stderr)
            return 0

        stats, matches = classify(
            conn, rules,
            method=args.method,
            confidence=args.confidence,
            dry_run=args.dry_run,
        )
        print_run_summary(stats, matches, conn, args.dry_run)
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
