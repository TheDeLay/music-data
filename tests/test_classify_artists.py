"""Tests for the generic artist classifier.

Verifies:
  - YAML loading + structural validation
  - Case-insensitive substring matching against name_normalized
  - Multiple tags per artist (multiple matching rules)
  - Same tag from multiple rules deduplicates
  - Idempotency: re-runs replace label_match rows
  - Method scoping: 'manual' rows survive a label_match re-run
  - --dry-run does not write to the DB
  - --list-tags reports current state
"""
from __future__ import annotations

import sqlite3
import textwrap
from pathlib import Path

import pytest
import yaml

from scripts.classify_artists import (
    Rule,
    RulesError,
    classify,
    list_tags,
    load_rules,
    match_artists,
    write_classifications,
)


SCHEMA_PATH = Path(__file__).resolve().parent.parent / "sql" / "schema.sql"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
def _new_db(tmp_path) -> sqlite3.Connection:
    db_path = tmp_path / "test_classify.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(SCHEMA_PATH.read_text())
    return conn


@pytest.fixture
def db_with_artists(tmp_path):
    """DB with several artists covering different naming patterns."""
    conn = _new_db(tmp_path)
    artists = [
        (1, "London Symphony Orchestra", "london symphony orchestra"),
        (2, "Boston Symphony Orchestra", "boston symphony orchestra"),
        (3, "Bill Evans Trio",          "bill evans trio"),
        (4, "Kronos Quartet",            "kronos quartet"),
        (5, "Live at the Apollo Band",   "live at the apollo band"),
        (6, "Sigur Rós",                  "sigur rós"),
        (7, "miles davis",                "miles davis"),  # already-lowercase edge case
    ]
    conn.executemany(
        "INSERT INTO artists (artist_id, name, name_normalized) VALUES (?, ?, ?)",
        artists,
    )
    conn.commit()
    yield conn
    conn.close()


def _write_yaml(tmp_path, contents: str) -> Path:
    p = tmp_path / "rules.yaml"
    p.write_text(textwrap.dedent(contents))
    return p


# ---------------------------------------------------------------------------
# YAML loading
# ---------------------------------------------------------------------------
class TestLoadRules:

    def test_basic_load(self, tmp_path):
        path = _write_yaml(tmp_path, """
            rules:
              - pattern: Symphony
                tag: orchestral
              - pattern: Quartet
                tag: ensemble
        """)
        rules = load_rules(path)
        assert rules == [
            Rule(pattern="Symphony", tag="orchestral"),
            Rule(pattern="Quartet", tag="ensemble"),
        ]

    def test_strips_whitespace(self, tmp_path):
        path = _write_yaml(tmp_path, """
            rules:
              - pattern: "  Symphony  "
                tag: "  orchestral  "
        """)
        rules = load_rules(path)
        assert rules[0] == Rule(pattern="Symphony", tag="orchestral")

    def test_empty_file_raises(self, tmp_path):
        path = tmp_path / "empty.yaml"
        path.write_text("")
        with pytest.raises(RulesError, match="empty"):
            load_rules(path)

    def test_missing_top_level_rules_key_raises(self, tmp_path):
        path = _write_yaml(tmp_path, "patterns: []")
        with pytest.raises(RulesError, match="'rules:' key required"):
            load_rules(path)

    def test_rules_not_a_list_raises(self, tmp_path):
        path = _write_yaml(tmp_path, "rules: not-a-list")
        with pytest.raises(RulesError, match="must be a list"):
            load_rules(path)

    def test_rule_missing_pattern_raises(self, tmp_path):
        path = _write_yaml(tmp_path, """
            rules:
              - tag: orchestral
        """)
        with pytest.raises(RulesError, match="empty/missing 'pattern'"):
            load_rules(path)

    def test_rule_missing_tag_raises(self, tmp_path):
        path = _write_yaml(tmp_path, """
            rules:
              - pattern: Symphony
        """)
        with pytest.raises(RulesError, match="empty/missing 'tag'"):
            load_rules(path)

    def test_invalid_yaml_raises_yaml_error(self, tmp_path):
        path = tmp_path / "bad.yaml"
        path.write_text("rules: [unclosed")
        with pytest.raises(yaml.YAMLError):
            load_rules(path)


# ---------------------------------------------------------------------------
# Matching
# ---------------------------------------------------------------------------
class TestMatchArtists:

    def test_substring_case_insensitive(self, db_with_artists):
        rules = [Rule(pattern="symphony", tag="orchestral")]
        result = match_artists(db_with_artists, rules)
        assert set(result.keys()) == {1, 2}  # London, Boston
        assert all("orchestral" in tags for tags in result.values())

    def test_uppercase_pattern_still_matches(self, db_with_artists):
        rules = [Rule(pattern="SYMPHONY", tag="orchestral")]
        result = match_artists(db_with_artists, rules)
        assert set(result.keys()) == {1, 2}

    def test_no_matches_returns_empty(self, db_with_artists):
        rules = [Rule(pattern="Nonexistent Pattern", tag="never")]
        assert match_artists(db_with_artists, rules) == {}

    def test_multiple_tags_per_artist(self, db_with_artists):
        rules = [
            Rule(pattern="Symphony Orchestra", tag="orchestral"),
            Rule(pattern="London", tag="city-london"),
        ]
        result = match_artists(db_with_artists, rules)
        assert result[1] == {"orchestral", "city-london"}  # London Symphony hits both
        assert result[2] == {"orchestral"}                  # Boston Symphony hits one

    def test_same_tag_from_multiple_rules_deduplicates(self, db_with_artists):
        """Two rules with the same tag matching one artist → one tag, not two."""
        rules = [
            Rule(pattern="Bill Evans", tag="jazz"),
            Rule(pattern="Trio", tag="jazz"),
        ]
        result = match_artists(db_with_artists, rules)
        assert result[3] == {"jazz"}

    def test_unicode_normalization(self, db_with_artists):
        """Pattern containing non-ASCII matches normalized name correctly."""
        rules = [Rule(pattern="Sigur Rós", tag="post-rock")]
        result = match_artists(db_with_artists, rules)
        assert 6 in result and "post-rock" in result[6]


# ---------------------------------------------------------------------------
# Persistence + idempotency
# ---------------------------------------------------------------------------
class TestWriteAndIdempotency:

    def test_basic_write(self, db_with_artists):
        matches = {1: {"orchestral"}, 2: {"orchestral"}}
        n = write_classifications(db_with_artists, matches, "label_match", 1.0)
        assert n == 2
        rows = db_with_artists.execute(
            "SELECT artist_id, classification, method, confidence "
            "FROM artist_classifications ORDER BY artist_id"
        ).fetchall()
        assert [tuple(r) for r in rows] == [
            (1, "orchestral", "label_match", 1.0),
            (2, "orchestral", "label_match", 1.0),
        ]

    def test_rerun_replaces_label_match_rows(self, db_with_artists):
        """Second run with different rules should not leave the first run's rows behind."""
        write_classifications(
            db_with_artists, {1: {"orchestral"}, 2: {"orchestral"}}, "label_match", 1.0,
        )
        write_classifications(
            db_with_artists, {3: {"jazz"}}, "label_match", 1.0,
        )
        rows = db_with_artists.execute(
            "SELECT artist_id, classification FROM artist_classifications "
            "ORDER BY artist_id"
        ).fetchall()
        assert [tuple(r) for r in rows] == [(3, "jazz")]

    def test_manual_rows_survive_label_match_rerun(self, db_with_artists):
        """A user's manually-set classifications must not be wiped by re-running label_match."""
        # User added a manual classification
        db_with_artists.execute(
            "INSERT INTO artist_classifications (artist_id, classification, method, confidence) "
            "VALUES (1, 'special-note', 'manual', 1.0)"
        )
        db_with_artists.commit()
        # Re-run label_match with no matches at all
        write_classifications(db_with_artists, {}, "label_match", 1.0)
        rows = db_with_artists.execute(
            "SELECT artist_id, classification, method FROM artist_classifications"
        ).fetchall()
        assert [tuple(r) for r in rows] == [(1, "special-note", "manual")]

    def test_empty_matches_clears_method(self, db_with_artists):
        """Calling with empty matches AND label_match method clears prior label_match rows."""
        write_classifications(
            db_with_artists, {1: {"orchestral"}}, "label_match", 1.0,
        )
        n = write_classifications(db_with_artists, {}, "label_match", 1.0)
        assert n == 0
        remaining = db_with_artists.execute(
            "SELECT COUNT(*) FROM artist_classifications WHERE method='label_match'"
        ).fetchone()[0]
        assert remaining == 0


# ---------------------------------------------------------------------------
# classify() pipeline
# ---------------------------------------------------------------------------
class TestClassifyPipeline:

    def test_dry_run_does_not_write(self, db_with_artists):
        rules = [Rule(pattern="Symphony", tag="orchestral")]
        stats, matches = classify(db_with_artists, rules, dry_run=True)
        assert stats.classifications_written == 2  # would-write count
        assert stats.artists_matched == 2
        # ...but the table is empty
        n_rows = db_with_artists.execute(
            "SELECT COUNT(*) FROM artist_classifications"
        ).fetchone()[0]
        assert n_rows == 0

    def test_real_run_writes_and_reports(self, db_with_artists):
        rules = [
            Rule(pattern="Symphony", tag="orchestral"),
            Rule(pattern="Trio", tag="ensemble"),
        ]
        stats, _ = classify(db_with_artists, rules)
        assert stats.rules_loaded == 2
        assert stats.artists_matched == 3      # 2 symphonies + 1 trio
        assert stats.classifications_written == 3
        n_rows = db_with_artists.execute(
            "SELECT COUNT(*) FROM artist_classifications"
        ).fetchone()[0]
        assert n_rows == 3

    def test_custom_confidence_persisted(self, db_with_artists):
        rules = [Rule(pattern="Symphony", tag="orchestral")]
        classify(db_with_artists, rules, confidence=0.7)
        confs = [r[0] for r in db_with_artists.execute(
            "SELECT confidence FROM artist_classifications"
        )]
        assert confs == [0.7, 0.7]


# ---------------------------------------------------------------------------
# list_tags reporting
# ---------------------------------------------------------------------------
class TestListTags:

    def test_empty_db(self, db_with_artists):
        assert list_tags(db_with_artists) == []

    def test_groups_by_method_and_tag(self, db_with_artists):
        # Two label_match rows + one manual row
        db_with_artists.executemany(
            "INSERT INTO artist_classifications (artist_id, classification, method, confidence) "
            "VALUES (?, ?, ?, ?)",
            [
                (1, "orchestral", "label_match", 1.0),
                (2, "orchestral", "label_match", 1.0),
                (3, "manual-flag", "manual", 1.0),
            ],
        )
        db_with_artists.commit()
        rows = list_tags(db_with_artists)
        # Sort key: method ASC, count DESC, tag ASC
        assert rows == [
            ("label_match", "orchestral", 2),
            ("manual", "manual-flag", 1),
        ]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
