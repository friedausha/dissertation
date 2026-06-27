#!/usr/bin/env python3
"""
Article text fetcher — Stage 1b of the trafficking incident pipeline.

Reads raw_articles rows where full_text_status = 'pending', fetches each
article URL, extracts the main body text using trafilatura, and writes the
result back to the database.

Per-domain rate limiting (MIN_DOMAIN_GAP_S) prevents the fetcher from
hammering a single news site. Articles that return 403/401/429 are marked
'blocked'; those with no extractable text after a successful HTTP response
are marked 'failed'. Paywalled articles are detected heuristically and
marked 'paywalled'.

Usage:
    python3 text_fetch.py              # process all pending articles
    python3 text_fetch.py --batch 50   # process at most 50 articles
    python3 text_fetch.py stats        # show status breakdown
"""

import argparse
import hashlib
import logging
import os
import re
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import psycopg2
import psycopg2.extras
import requests
import trafilatura
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")


def _title_hash(title: str | None) -> str | None:
    if not title:
        return None
    norm = re.sub(r"[^a-z0-9\s]", "", title.lower())
    norm = re.sub(r"\s+", " ", norm).strip()
    return hashlib.md5(norm.encode()).hexdigest() if len(norm) >= 10 else None

# ── Configuration ─────────────────────────────────────────────────────────────

SCRIPT_DIR = Path(__file__).parent
LOG_PATH   = SCRIPT_DIR.parent / "logs" / "text_fetch.log"

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://frieda:localdev@localhost:5432/trafficking_db"
)

MIN_DOMAIN_GAP_S = 2.0    # minimum seconds between requests to the same domain
REQUEST_TIMEOUT  = 15     # seconds per HTTP request
MAX_TEXT_CHARS   = 50_000 # truncate very long articles before storing
MIN_TEXT_CHARS   = 100    # below this → treat extraction as failed

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; ResearchBot/1.0; "
        "+https://www.ed.ac.uk) trafilatura"
    )
}

# Heuristic paywall signals found in page bodies
PAYWALL_SIGNALS = [
    "subscribe to read",
    "subscription required",
    "sign in to read",
    "create a free account",
    "this article is for subscribers",
    "become a member to read",
    "unlock this article",
]


# ── Text extraction ───────────────────────────────────────────────────────────

def _is_paywalled(html: str) -> bool:
    lower = html.lower()
    return any(signal in lower for signal in PAYWALL_SIGNALS)


def fetch_text(url: str) -> tuple[str, str, str | None]:
    """
    Fetch and extract article body text + title from url.

    Returns (status, text, title) where status is one of:
        'success'   — text extracted successfully
        'blocked'   — HTTP 401 / 403 / 429
        'paywalled' — page loaded but paywall detected
        'failed'    — any other failure (timeout, no text, etc.)
    title is None when extraction fails or the page has no title metadata.
    """
    try:
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)

        if resp.status_code in (401, 403, 429):
            return "blocked", "", None

        if resp.status_code != 200:
            return "failed", "", None

        if _is_paywalled(resp.text):
            return "paywalled", "", None

        doc = trafilatura.bare_extraction(
            resp.text,
            url=url,
            include_comments=False,
            include_tables=False,
            with_metadata=True,
        )

        text  = (doc.text if doc else None) or ""
        title = (doc.title if doc else None)

        if len(text.strip()) < MIN_TEXT_CHARS:
            return "failed", "", title

        return "success", text.strip()[:MAX_TEXT_CHARS], title

    except requests.exceptions.Timeout:
        return "failed", "", None
    except requests.exceptions.RequestException:
        return "failed", "", None
    except Exception:
        return "failed", "", None


# ── Database ──────────────────────────────────────────────────────────────────

def get_conn() -> psycopg2.extensions.connection:
    return psycopg2.connect(DATABASE_URL)


def get_pending(conn, batch_size: int) -> list[dict]:
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            """SELECT id, url, domain
               FROM raw_articles
               WHERE full_text_status = 'pending'
               ORDER BY seendate DESC NULLS LAST
               LIMIT %s""",
            (batch_size,),
        )
        return [dict(r) for r in cur.fetchall()]


def save_result(conn, article_id: int, status: str, text: str, title: str | None) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE raw_articles
               SET full_text            = %s,
                   full_text_status     = %s,
                   full_text_fetched_at = %s,
                   title                = COALESCE(title, %s),
                   title_hash           = COALESCE(title_hash, %s)
               WHERE id = %s""",
            (text or None, status, datetime.now(timezone.utc), title,
             _title_hash(title), article_id),
        )
    conn.commit()


def start_run(conn) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO pipeline_runs (stage) VALUES ('text_fetch') RETURNING id"
        )
        run_id = cur.fetchone()[0]
    conn.commit()
    return run_id


def finish_run(conn, run_id: int, processed: int, new: int,
               status: str, error: str | None = None) -> None:
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

def cmd_fetch(batch_size: int) -> None:
    conn   = get_conn()
    run_id = start_run(conn)
    logging.info("=== text_fetch start  batch_size=%d ===", batch_size)

    pending = get_pending(conn, batch_size)
    logging.info("  %d pending articles to fetch", len(pending))

    # Per-domain rate limiting: track when we last fetched from each domain
    last_fetch: dict[str, float] = defaultdict(float)

    counts: dict[str, int] = defaultdict(int)

    try:
        for i, article in enumerate(pending, 1):
            domain = article["domain"] or urlparse(article["url"]).netloc

            # Enforce per-domain gap
            gap = time.monotonic() - last_fetch[domain]
            if gap < MIN_DOMAIN_GAP_S:
                time.sleep(MIN_DOMAIN_GAP_S - gap)

            status, text, title = fetch_text(article["url"])
            last_fetch[domain] = time.monotonic()

            save_result(conn, article["id"], status, text, title)
            counts[status] += 1

            logging.info(
                "  [%d/%d] %-10s %s",
                i, len(pending), status, article["url"][:80],
            )

        total = sum(counts.values())
        finish_run(conn, run_id, total, counts["success"], "success")
        logging.info(
            "=== text_fetch done  success=%d blocked=%d paywalled=%d failed=%d ===",
            counts["success"], counts["blocked"],
            counts["paywalled"], counts["failed"],
        )

    except Exception as exc:
        finish_run(conn, run_id, sum(counts.values()), counts.get("success", 0),
                   "failed", str(exc))
        logging.exception("text_fetch failed")
        raise
    finally:
        conn.close()


def cmd_stats() -> None:
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(
            """SELECT full_text_status, COUNT(*) AS n
               FROM raw_articles
               GROUP BY full_text_status
               ORDER BY n DESC"""
        )
        rows = cur.fetchall()
        cur.execute("SELECT COUNT(*) FROM raw_articles")
        total = cur.fetchone()[0]
    conn.close()

    print(f"\nraw_articles full_text breakdown (total={total}):")
    for status, n in rows:
        bar = "█" * min(40, int(40 * n / max(total, 1)))
        print(f"  {status:<12} {n:>6}  {bar}")


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
        description="Article text fetcher — pipeline stage 1b"
    )
    sub = parser.add_subparsers(dest="cmd")

    p_fetch = sub.add_parser("fetch", help="Fetch pending articles (default)")
    p_fetch.add_argument(
        "--batch", type=int, default=500,
        help="Max articles to process per run (default 500)"
    )
    sub.add_parser("stats", help="Show full_text_status breakdown")

    args = parser.parse_args()

    if args.cmd == "stats":
        cmd_stats()
    elif args.cmd == "fetch":
        cmd_fetch(args.batch)
    else:
        # Default: fetch with standard batch size
        cmd_fetch(500)


if __name__ == "__main__":
    main()
