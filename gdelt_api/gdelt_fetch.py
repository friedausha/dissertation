#!/usr/bin/env python3
"""
GDELT news fetcher — Stage 1 of the trafficking incident pipeline.

Queries GDELT's Document API with domain-specific keyword sets, deduplicates
by URL, and inserts new article metadata into the raw_articles table in
PostgreSQL. Full-text fetching is a separate stage.

Usage:
    python3 gdelt_fetch.py              # fetch last 2 hours (default)
    python3 gdelt_fetch.py show         # print 20 most recent raw articles
    python3 gdelt_fetch.py show 50      # print 50
    python3 gdelt_fetch.py stats        # per-label counts and top domains

Connection:
    Set DATABASE_URL env var, or rely on the defaults below.
    Default: postgresql://frieda:localdev@localhost:5432/trafficking_db
"""

import argparse
import hashlib
import json
import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras
import requests

# ── Configuration ─────────────────────────────────────────────────────────────

SCRIPT_DIR = Path(__file__).parent
LOG_PATH   = SCRIPT_DIR.parent / "logs" / "gdelt_fetch.log"

DATABASE_URL  = os.environ.get(
    "DATABASE_URL",
    "postgresql://frieda:localdev@localhost:5432/trafficking_db"
)

GDELT_DOC_API = "https://api.gdeltproject.org/api/v2/doc/doc"
GDELT_API_KEY = "gdelt_sk_2b04101c9c1a89631fd76427eb799e4c747fbd682389dbb2bf69271fa61d9c60"

# sourcelang:english restricts the DOC API to English-source articles —
# without it GDELT searches its translingual index (65 languages) and
# returns machine-translated titles/snippets for non-English originals.
QUERIES: dict[str, str] = {
    "human_trafficking": (
        '"human trafficking" OR "sex trafficking" OR "labor trafficking" '
        'OR "modern slavery" OR "forced labor" OR "trafficking victim" '
        'OR "trafficking survivor" sourcelang:english'
    ),
    "pig_butchering": (
        '"pig butchering" OR "pig-butchering" OR "sha zhu pan" '
        'OR "romance scam" OR "cryptocurrency fraud" OR "crypto scam" '
        'OR "investment fraud scam" sourcelang:english'
    ),
    "smuggling_debt": (
        '"human smuggling" OR "debt bondage" OR "smuggling network" '
        'OR "organ trafficking" OR "forced marriage" OR "child trafficking" '
        'sourcelang:english'
    ),
    "online_scam_camps": (
        '"scam compound" OR "scam center" OR "fraud compound" '
        'OR "cyber slavery" OR "online scam operation" OR "KK Park" '
        'OR "Myanmar scam" OR "Cambodia scam" sourcelang:english'
    ),
}

LOOKBACK_HOURS    = 2    # look back 2h per run to cover clock skew
MAX_RECORDS       = 250  # GDELT DOC API hard max per query
BACKFILL_WINDOW_H = 12   # window size for backfill sliding pass


def _title_hash(title: str | None) -> str | None:
    if not title:
        return None
    norm = re.sub(r"[^a-z0-9\s]", "", title.lower())
    norm = re.sub(r"\s+", " ", norm).strip()
    return hashlib.md5(norm.encode()).hexdigest() if len(norm) >= 10 else None


# ── Database ──────────────────────────────────────────────────────────────────

def get_conn() -> psycopg2.extensions.connection:
    return psycopg2.connect(DATABASE_URL)


def insert_articles(
    conn: psycopg2.extensions.connection,
    articles: list[dict],
    label: str,
    fetched_at: str,
) -> int:
    inserted = 0
    with conn.cursor() as cur:
        for a in articles:
            url = (a.get("url") or "").strip()
            if not url:
                continue
            cur.execute(
                """
                INSERT INTO raw_articles
                    (url, title, domain, source_country, language,
                     seendate, fetched_at, query_label, title_hash)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (url) DO NOTHING
                """,
                (
                    url,
                    a.get("title"),
                    a.get("domain"),
                    a.get("sourcecountry"),
                    a.get("language"),
                    _parse_gdelt_date(a.get("seendate")),
                    fetched_at,
                    label,
                    _title_hash(a.get("title")),
                ),
            )
            if cur.rowcount:
                inserted += 1
    conn.commit()
    return inserted


def _parse_gdelt_date(raw: str | None) -> str | None:
    """Convert GDELT's YYYYMMDDHHMMSS string to an ISO timestamp."""
    if not raw or len(raw) < 8:
        return None
    try:
        return datetime.strptime(raw[:14].ljust(14, "0"), "%Y%m%d%H%M%S").isoformat()
    except ValueError:
        return None


# ── GDELT API ─────────────────────────────────────────────────────────────────

def time_window(hours_back: int) -> tuple[str, str]:
    now   = datetime.now(timezone.utc)
    start = now - timedelta(hours=hours_back)
    fmt   = "%Y%m%d%H%M%S"
    return start.strftime(fmt), now.strftime(fmt)


def fetch_gdelt(query: str, start: str, end: str, retries: int = 3) -> list[dict]:
    params = {
        "query":         query,
        "mode":          "artlist",
        "maxrecords":    MAX_RECORDS,
        "format":        "json",
        "startdatetime": start,
        "enddatetime":   end,
        "sort":          "datedesc",
        "key":           GDELT_API_KEY,
    }
    for attempt in range(retries):
        try:
            resp = requests.get(GDELT_DOC_API, params=params, timeout=30)
            if resp.status_code == 429:
                wait = 10 * (attempt + 1)
                logging.warning(
                    "Rate limited (429), waiting %ds (attempt %d/%d)",
                    wait, attempt + 1, retries,
                )
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json().get("articles") or []
        except requests.RequestException as e:
            logging.error("GDELT request failed: %s", e)
            if attempt < retries - 1:
                time.sleep(6)
        except (json.JSONDecodeError, KeyError) as e:
            logging.error("GDELT parse error: %s", e)
    return []


# ── Pipeline run tracking ─────────────────────────────────────────────────────

def start_run(conn, stage: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO pipeline_runs (stage) VALUES (%s) RETURNING id", (stage,)
        )
        run_id = cur.fetchone()[0]
    conn.commit()
    return run_id


def finish_run(conn, run_id: int, processed: int, new: int, status: str, error: str | None = None) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE pipeline_runs
               SET finished_at = NOW(), articles_processed = %s,
                   articles_new = %s, status = %s, error_msg = %s
               WHERE id = %s""",
            (processed, new, status, error, run_id),
        )
    conn.commit()


# ── Commands ──────────────────────────────────────────────────────────────────

def cmd_fetch() -> None:
    fetched_at     = datetime.now(timezone.utc).isoformat()
    start, end     = time_window(LOOKBACK_HOURS)
    logging.info("=== gdelt_fetch start  window %s → %s ===", start, end)

    conn   = get_conn()
    run_id = start_run(conn, "gdelt_fetch")
    total_fetched = 0
    total_new     = 0

    try:
        for i, (label, query) in enumerate(QUERIES.items()):
            if i > 0:
                time.sleep(10)  # GDELT rate limit: 1 req / 5 s
            articles = fetch_gdelt(query, start, end)
            new      = insert_articles(conn, articles, label, fetched_at)
            logging.info(
                "  %-25s fetched=%-4d  new=%d", label, len(articles), new
            )
            total_fetched += len(articles)
            total_new     += new

        finish_run(conn, run_id, total_fetched, total_new, "success")
        logging.info("=== gdelt_fetch done   total new: %d ===", total_new)

    except Exception as exc:
        finish_run(conn, run_id, total_fetched, total_new, "failed", str(exc))
        logging.exception("gdelt_fetch failed")
        raise
    finally:
        conn.close()


def cmd_backfill(days: int) -> None:
    """
    Historical backfill: slide BACKFILL_WINDOW_H-hour windows across `days`
    days of GDELT DOC API data and insert into raw_articles.

    Each query returns up to MAX_RECORDS (250) articles per keyword bucket.
    With 12h windows across 90 days: 90×2×4 = 720 API calls (~2 hrs at 10s
    inter-query delay). This is a one-time operation — run it once, then let
    the hourly cron handle incremental updates going forward.
    """
    fetched_at = datetime.now(timezone.utc).isoformat()
    end   = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    start = end - timedelta(days=days)
    fmt   = "%Y%m%d%H%M%S"

    windows: list[tuple[str, str]] = []
    t = start
    while t < end:
        t_end = min(t + timedelta(hours=BACKFILL_WINDOW_H), end)
        windows.append((t.strftime(fmt), t_end.strftime(fmt)))
        t = t_end

    conn   = get_conn()
    run_id = start_run(conn, "gdelt_fetch")
    logging.info(
        "=== gdelt_fetch backfill start  days=%d  windows=%d  %s → %s ===",
        days, len(windows), windows[0][0], windows[-1][1],
    )

    total_fetched = 0
    total_new     = 0

    try:
        for wi, (w_start, w_end) in enumerate(windows, 1):
            for li, (label, query) in enumerate(QUERIES.items()):
                if wi > 1 or li > 0:
                    time.sleep(10)
                articles = fetch_gdelt(query, w_start, w_end)
                new      = insert_articles(conn, articles, label, fetched_at)
                total_fetched += len(articles)
                total_new     += new

            if wi % 10 == 0 or wi == len(windows):
                logging.info(
                    "  [%d/%d] window %s  cumulative: fetched=%d  new=%d",
                    wi, len(windows), w_start, total_fetched, total_new,
                )

        finish_run(conn, run_id, total_fetched, total_new, "success")
        logging.info(
            "=== gdelt_fetch backfill done  total_fetched=%d  total_new=%d ===",
            total_fetched, total_new,
        )

    except Exception as exc:
        finish_run(conn, run_id, total_fetched, total_new, "failed", str(exc))
        logging.exception("gdelt_fetch backfill failed")
        raise
    finally:
        conn.close()


def cmd_show(n: int) -> None:
    conn = get_conn()
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            """SELECT seendate, query_label, domain, source_country, title
               FROM raw_articles ORDER BY seendate DESC NULLS LAST LIMIT %s""",
            (n,),
        )
        rows = cur.fetchall()
        cur.execute("SELECT COUNT(*) FROM raw_articles")
        total = cur.fetchone()[0]
    conn.close()

    if not rows:
        print("No articles stored yet.")
        return

    print(f"{'─'*80}")
    print(f"  Total raw articles in DB: {total}")
    print(f"{'─'*80}")
    for row in rows:
        print(f"  {row['seendate']}  [{row['query_label']}]  {row['domain']} ({row['source_country']})")
        print(f"  {row['title']}")
        print()


def cmd_stats() -> None:
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(
            """SELECT query_label, COUNT(*) AS n,
                      MIN(seendate) AS oldest, MAX(seendate) AS newest
               FROM raw_articles GROUP BY query_label ORDER BY n DESC"""
        )
        label_rows = cur.fetchall()

        cur.execute("SELECT COUNT(*) FROM raw_articles")
        total = cur.fetchone()[0]

        cur.execute(
            """SELECT domain, COUNT(*) AS n FROM raw_articles
               GROUP BY domain ORDER BY n DESC LIMIT 10"""
        )
        domain_rows = cur.fetchall()

        cur.execute(
            """SELECT stage, status, COUNT(*) AS n, MAX(started_at) AS last_run
               FROM pipeline_runs GROUP BY stage, status ORDER BY stage, status"""
        )
        run_rows = cur.fetchall()
    conn.close()

    print(f"\nTotal raw articles: {total}\n")
    print(f"{'Label':<30} {'Count':>7}  {'Oldest':>22}  {'Newest':>22}")
    print("─" * 86)
    for label, n, oldest, newest in label_rows:
        print(f"  {label:<28} {n:>7}  {str(oldest or ''):>22}  {str(newest or ''):>22}")

    print(f"\nTop domains:")
    for domain, n in domain_rows:
        print(f"  {n:>6}  {domain}")

    if run_rows:
        print(f"\nPipeline run history:")
        for stage, status, n, last in run_rows:
            print(f"  {stage:<20} {status:<10} runs={n}  last={last}")


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

    parser = argparse.ArgumentParser(
        description="GDELT fetcher — pipeline stage 1"
    )
    sub = parser.add_subparsers(dest="cmd")
    sub.add_parser("fetch", help="Run a fetch now (default)")
    p_show = sub.add_parser("show", help="Print recent raw articles")
    p_show.add_argument("n", nargs="?", type=int, default=20)
    sub.add_parser("stats", help="Counts, top domains, run history")
    p_back = sub.add_parser("backfill", help="Historical backfill (sliding windows)")
    p_back.add_argument(
        "--days", type=int, default=90,
        help="How many days back to backfill (default 90 ≈ 3 months)"
    )
    args = parser.parse_args()

    if args.cmd == "show":
        cmd_show(args.n)
    elif args.cmd == "stats":
        cmd_stats()
    elif args.cmd == "backfill":
        cmd_backfill(args.days)
    else:
        cmd_fetch()


if __name__ == "__main__":
    main()
