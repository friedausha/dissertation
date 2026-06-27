#!/usr/bin/env python3
"""
Title-based deduplication — Stage 1c of the trafficking incident pipeline.

URL-level dedup already happens on INSERT via ON CONFLICT. This script
handles cross-site syndication: the same story (same title) republished
across multiple domains gets one canonical row; the rest are marked
is_duplicate = true and skipped by classifier.py.

Algorithm:
  1. Backfill title_hash for any rows that don't have one yet.
  2. For each (title_hash, seendate-window) cluster with >1 article,
     keep the row with the smallest id (first seen) and mark the rest
     is_duplicate = true.

title_hash is MD5 of the title after lowercasing and stripping all
non-alphanumeric characters. Short titles (<= 10 chars after normalisation)
are never hashed — too likely to collide across unrelated articles.

Usage:
    python3 dedup.py              # run full dedup (default)
    python3 dedup.py backfill     # only backfill title_hash, don't mark dupes
    python3 dedup.py stats        # show dedup coverage and duplicate counts
    python3 dedup.py reset        # un-mark all duplicates (for re-runs)
"""

import argparse
import hashlib
import logging
import os
import re
from pathlib import Path

import psycopg2
import psycopg2.extras

SCRIPT_DIR   = Path(__file__).parent
LOG_PATH     = SCRIPT_DIR.parent / "logs" / "dedup.log"
DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://frieda:localdev@localhost:5432/trafficking_db"
)

DEDUP_WINDOW_DAYS = 3   # articles within this many days count as "same window"
MIN_NORM_LEN      = 10  # ignore titles shorter than this after normalisation


def normalize_title(title: str | None) -> str | None:
    if not title:
        return None
    norm = re.sub(r"[^a-z0-9\s]", "", title.lower())
    norm = re.sub(r"\s+", " ", norm).strip()
    return norm if len(norm) >= MIN_NORM_LEN else None


def title_hash(title: str | None) -> str | None:
    norm = normalize_title(title)
    if not norm:
        return None
    return hashlib.md5(norm.encode()).hexdigest()


def get_conn() -> psycopg2.extensions.connection:
    return psycopg2.connect(DATABASE_URL)


# ── Backfill ──────────────────────────────────────────────────────────────────

def cmd_backfill(conn) -> int:
    """Compute and store title_hash for rows where it is NULL."""
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            "SELECT id, title FROM raw_articles WHERE title_hash IS NULL AND title IS NOT NULL"
        )
        rows = cur.fetchall()

    updated = 0
    with conn.cursor() as cur:
        for row in rows:
            h = title_hash(row["title"])
            if h is None:
                continue
            cur.execute(
                "UPDATE raw_articles SET title_hash = %s WHERE id = %s",
                (h, row["id"]),
            )
            updated += 1
    conn.commit()
    logging.info("backfill: set title_hash for %d articles", updated)
    return updated


# ── Mark duplicates ───────────────────────────────────────────────────────────

def cmd_mark_duplicates(conn) -> int:
    """
    For each (title_hash, date-window) group, keep the row with the
    smallest id (first seen) and mark the rest is_duplicate = true.

    Uses NOT IN against a subquery of canonical ids to avoid the NULL
    edge case that can affect window-function PARTITION BY with NULL dates.
    """
    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE raw_articles
            SET is_duplicate = TRUE
            WHERE title_hash IS NOT NULL
              AND is_duplicate = FALSE
              AND id NOT IN (
                  SELECT MIN(id)
                  FROM raw_articles
                  WHERE title_hash IS NOT NULL
                    AND is_duplicate = FALSE
                  GROUP BY title_hash,
                           COALESCE(
                               (EXTRACT(EPOCH FROM seendate)::BIGINT / ({DEDUP_WINDOW_DAYS} * 86400))::BIGINT,
                               -1
                           )
              )
            """
        )
        marked = cur.rowcount
    conn.commit()
    logging.info("mark_duplicates: marked %d articles as is_duplicate=true", marked)
    return marked


# ── Commands ──────────────────────────────────────────────────────────────────

def cmd_run() -> None:
    conn = get_conn()
    try:
        backfilled = cmd_backfill(conn)
        marked     = cmd_mark_duplicates(conn)
        logging.info("=== dedup done  backfilled=%d  marked=%d ===", backfilled, marked)
    finally:
        conn.close()


def cmd_stats() -> None:
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM raw_articles")
        total = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM raw_articles WHERE title_hash IS NOT NULL")
        with_hash = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM raw_articles WHERE is_duplicate = TRUE")
        dupes = cur.fetchone()[0]

        cur.execute(
            """SELECT query_label, COUNT(*) FILTER (WHERE is_duplicate) AS dupes,
                      COUNT(*) AS total
               FROM raw_articles
               GROUP BY query_label ORDER BY dupes DESC"""
        )
        label_rows = cur.fetchall()
    conn.close()

    print(f"\nTotal articles:       {total}")
    print(f"  with title_hash:    {with_hash}  ({100*with_hash//max(total,1)}%)")
    print(f"  is_duplicate=true:  {dupes}  ({100*dupes//max(total,1)}%)")

    print(f"\n{'Label':<28}  {'Dupes':>6}  {'Total':>7}")
    print("─" * 48)
    for label, d, t in label_rows:
        print(f"  {label:<26}  {d:>6}  {t:>7}")


def cmd_reset() -> None:
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("UPDATE raw_articles SET is_duplicate = FALSE WHERE is_duplicate = TRUE")
        n = cur.rowcount
    conn.commit()
    conn.close()
    print(f"Reset {n} articles to is_duplicate=false.")


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler(LOG_PATH),
            logging.StreamHandler(),
        ],
    )

    parser = argparse.ArgumentParser(description="Title-based deduplication — pipeline stage 1c")
    sub = parser.add_subparsers(dest="cmd")
    sub.add_parser("run",      help="Full dedup: backfill hashes then mark duplicates (default)")
    sub.add_parser("backfill", help="Only backfill title_hash, don't mark duplicates")
    sub.add_parser("stats",    help="Show dedup coverage and counts")
    sub.add_parser("reset",    help="Un-mark all duplicates (for re-runs after schema changes)")
    args = parser.parse_args()

    if args.cmd == "backfill":
        conn = get_conn()
        try:
            cmd_backfill(conn)
        finally:
            conn.close()
    elif args.cmd == "stats":
        cmd_stats()
    elif args.cmd == "reset":
        cmd_reset()
    else:
        cmd_run()


if __name__ == "__main__":
    main()
