#!/usr/bin/env python3
"""
GDELT GKG firehose fetcher — Stage 1 (alternate feed) of the trafficking
incident pipeline.

Downloads GDELT's raw 15-minute Global Knowledge Graph (GKG) export zip
files, which cover EVERY news article GDELT indexed worldwide (not just
keyword matches), and filters them down to articles relevant to
scam-driven trafficking before inserting into raw_articles.

This is a separate ingestion path from gdelt_fetch.py (DOC 2.0 keyword
search API). Both write to the same raw_articles table and dedupe by URL
via ON CONFLICT, so they're safe to run side by side: the DOC API as the
primary targeted feed (returns up to 250 hits/query), this as a broader
recall net since GKG isn't capped.

GKG rows carry no article title or body text, only structured metadata
(themes, names, organizations, locations). Filtering matches the article
URL slug against KEYWORD_BUCKETS — the same phrases gdelt_fetch.py uses
against the DOC API.

GDELT's own V2Themes topic codes (e.g. "HUMAN_TRAFFICKING", "KIDNAP",
"ORGANIZED_CRIME") were tried and dropped: spot-checking matched URLs
showed near-zero precision (a bail bill, celebrity gossip, stock market
news, an FBI/UFC story all carried the "HUMAN_TRAFFICKING" tag). GDELT's
theme taxonomy isn't reliable enough for this without the verified
codebook, so this fetcher relies on keyword matching only, accepting
lower recall in exchange for not flooding the classifier with noise.

Only the base .gkg.csv.zip is fetched, not .translation.gkg.csv.zip —
GDELT publishes translated (non-English-source) documents in that
separate companion file, so this already restricts to English-source
articles without extra filtering.

Usage:
    python3 gdelt_gkg_fetch.py                  # fetch + filter last ~75 min of GKG files
    python3 gdelt_gkg_fetch.py backfill --days 7 # one-time historical backfill
    python3 gdelt_gkg_fetch.py stats            # counts by label and run history
"""

import argparse
import io
import logging
import os
import time
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv

# ── Configuration ─────────────────────────────────────────────────────────────

SCRIPT_DIR = Path(__file__).parent
LOG_PATH   = SCRIPT_DIR.parent / "logs" / "gdelt_gkg_fetch.log"

load_dotenv(SCRIPT_DIR.parent / ".env")

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://frieda:localdev@localhost:5432/trafficking_db"
)

GKG_BASE_URL     = "http://data.gdeltproject.org/gdeltv2"
LOOKBACK_MINUTES = 75   # 5 x 15-min intervals; hourly cron with overlap, dedup by URL
REQUEST_TIMEOUT  = 60

# Same keyword buckets as gdelt_fetch.py's DOC API queries, lowercased and
# hyphenated for substring matching against the article URL slug (GKG rows
# carry no title/body to match against).
KEYWORD_BUCKETS: dict[str, list[str]] = {
    "human_trafficking": [
        "human-trafficking", "human_trafficking", "sex-trafficking",
        "labor-trafficking", "modern-slavery", "forced-labor",
        "trafficking-victim", "trafficking-survivor",
    ],
    "pig_butchering": [
        "pig-butchering", "shazhupan", "sha-zhu-pan", "romance-scam",
        "crypto-scam", "cryptocurrency-fraud", "investment-fraud-scam",
    ],
    "smuggling_debt": [
        "human-smuggling", "debt-bondage", "smuggling-network",
        "organ-trafficking", "forced-marriage", "child-trafficking",
    ],
    "online_scam_camps": [
        "scam-compound", "scam-center", "scam-centre", "fraud-compound",
        "cyber-slavery", "online-scam", "kk-park", "myanmar-scam",
        "cambodia-scam",
    ],
}

# GKG 2.1 is 27 tab-separated fields, no header row.
GKG_FIELDS = [
    "record_id", "date", "source_collection_id", "source_common_name",
    "document_id", "counts", "v2_counts", "themes", "v2_themes",
    "locations", "v2_locations", "persons", "v2_persons",
    "organizations", "v2_organizations", "v2_tone", "dates", "gcam",
    "sharing_image", "related_images", "social_image_embeds",
    "social_video_embeds", "quotations", "all_names", "amounts",
    "translation_info", "extras",
]


# ── GKG file discovery ───────────────────────────────────────────────────────

def interval_timestamps(lookback_minutes: int) -> list[str]:
    """GDELT GKG files are published every 15 min, on the clock."""
    now = datetime.now(timezone.utc)
    floored_minute = (now.minute // 15) * 15
    latest = now.replace(minute=floored_minute, second=0, microsecond=0)
    n = lookback_minutes // 15
    return [
        (latest - timedelta(minutes=15 * i)).strftime("%Y%m%d%H%M%S")
        for i in range(n, -1, -1)
    ]


def download_gkg(ts: str, retries: int = 2) -> list[str]:
    """Download and unzip one GKG file, return list of raw CSV lines."""
    url = f"{GKG_BASE_URL}/{ts}.gkg.csv.zip"
    for attempt in range(retries):
        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 404:
                return []  # file not published yet, or no activity that interval
            resp.raise_for_status()
            with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
                name = zf.namelist()[0]
                raw = zf.read(name).decode("utf-8", errors="replace")
            return raw.splitlines()
        except (requests.RequestException, zipfile.BadZipFile) as e:
            logging.warning(
                "GKG download failed for %s (attempt %d/%d): %s",
                ts, attempt + 1, retries, e,
            )
            time.sleep(3)
    return []


# ── Parsing & filtering ──────────────────────────────────────────────────────

def parse_gkg_row(line: str) -> dict | None:
    parts = line.split("\t")
    if len(parts) < len(GKG_FIELDS):
        return None
    return dict(zip(GKG_FIELDS, parts))


def match_bucket(url_lower: str) -> str | None:
    for label, phrases in KEYWORD_BUCKETS.items():
        if any(p in url_lower for p in phrases):
            return label
    return None


def filter_row(row: dict) -> tuple[bool, str | None]:
    """Returns (matched, query_label)."""
    url = (row.get("document_id") or "").strip()
    if not url or not url.startswith("http"):
        return False, None

    bucket = match_bucket(url.lower())
    if bucket:
        return True, bucket
    return False, None


def _parse_gkg_date(raw: str | None) -> str | None:
    if not raw or len(raw) < 14:
        return None
    try:
        return datetime.strptime(raw[:14], "%Y%m%d%H%M%S").isoformat()
    except ValueError:
        return None


# ── Database ──────────────────────────────────────────────────────────────────

def get_conn() -> psycopg2.extensions.connection:
    return psycopg2.connect(DATABASE_URL)


def process_interval(conn, ts: str, fetched_at: str) -> tuple[int, int, int]:
    """Download, filter, and insert one GKG interval. Returns (scanned, matched, new)."""
    lines = download_gkg(ts)
    scanned = matched = new = 0

    for line in lines:
        row = parse_gkg_row(line)
        if not row:
            continue
        scanned += 1

        is_match, label = filter_row(row)
        if not is_match:
            continue
        matched += 1

        if insert_row(conn, row, label, fetched_at):
            new += 1

    conn.commit()
    return scanned, matched, new


def insert_row(conn, row: dict, label: str, fetched_at: str) -> bool:
    url = row["document_id"].strip()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO raw_articles
                (url, title, domain, source_country, language,
                 seendate, fetched_at, query_label)
            VALUES (%s, NULL, %s, NULL, 'English', %s, %s, %s)
            ON CONFLICT (url) DO NOTHING
            """,
            (
                url,
                row.get("source_common_name"),
                _parse_gkg_date(row.get("date")),
                fetched_at,
                label,
            ),
        )
        return bool(cur.rowcount)


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
    fetched_at = datetime.now(timezone.utc).isoformat()
    timestamps = interval_timestamps(LOOKBACK_MINUTES)
    logging.info(
        "=== gdelt_gkg_fetch start  %d intervals  %s..%s ===",
        len(timestamps), timestamps[0], timestamps[-1],
    )

    conn   = get_conn()
    run_id = start_run(conn, "gdelt_gkg_fetch")
    total_rows    = 0
    total_matched = 0
    total_new     = 0

    try:
        for ts in timestamps:
            scanned, matched, new = process_interval(conn, ts, fetched_at)
            logging.info("  %s  %d rows  matched=%d  new=%d", ts, scanned, matched, new)
            total_rows    += scanned
            total_matched += matched
            total_new     += new
            time.sleep(1)  # be polite to GDELT's static file server

        finish_run(conn, run_id, total_matched, total_new, "success")
        logging.info(
            "=== gdelt_gkg_fetch done  scanned=%d  matched=%d  new=%d ===",
            total_rows, total_matched, total_new,
        )

    except Exception as exc:
        finish_run(conn, run_id, total_matched, total_new, "failed", str(exc))
        logging.exception("gdelt_gkg_fetch failed")
        raise
    finally:
        conn.close()


def cmd_backfill(days: int) -> None:
    fetched_at = datetime.now(timezone.utc).isoformat()
    end   = datetime.now(timezone.utc)
    floored_minute = (end.minute // 15) * 15
    end   = end.replace(minute=floored_minute, second=0, microsecond=0)
    start = end - timedelta(days=days)

    timestamps = []
    t = start
    while t <= end:
        timestamps.append(t.strftime("%Y%m%d%H%M%S"))
        t += timedelta(minutes=15)

    logging.info(
        "=== gdelt_gkg_backfill start  %d intervals (%d days)  %s..%s ===",
        len(timestamps), days, timestamps[0], timestamps[-1],
    )

    conn   = get_conn()
    run_id = start_run(conn, "gdelt_gkg_fetch")
    total_rows    = 0
    total_matched = 0
    total_new     = 0

    try:
        for i, ts in enumerate(timestamps, 1):
            scanned, matched, new = process_interval(conn, ts, fetched_at)
            total_rows    += scanned
            total_matched += matched
            total_new     += new

            if i % 20 == 0 or i == len(timestamps):
                logging.info(
                    "  [%d/%d] %s  scanned=%d  matched=%d  new=%d  (running totals)",
                    i, len(timestamps), ts, total_rows, total_matched, total_new,
                )

            time.sleep(1)  # be polite to GDELT's static file server

        finish_run(conn, run_id, total_matched, total_new, "success")
        logging.info(
            "=== gdelt_gkg_backfill done  scanned=%d  matched=%d  new=%d ===",
            total_rows, total_matched, total_new,
        )

    except Exception as exc:
        finish_run(conn, run_id, total_matched, total_new, "failed", str(exc))
        logging.exception("gdelt_gkg_backfill failed")
        raise
    finally:
        conn.close()


def cmd_stats() -> None:
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(
            """SELECT query_label, COUNT(*) AS n
               FROM raw_articles
               WHERE query_label IN ('human_trafficking', 'pig_butchering',
                                      'smuggling_debt', 'online_scam_camps')
               GROUP BY query_label ORDER BY n DESC"""
        )
        rows = cur.fetchall()

        cur.execute(
            """SELECT status, COUNT(*) AS n, MAX(started_at) AS last_run
               FROM pipeline_runs WHERE stage = 'gdelt_gkg_fetch'
               GROUP BY status ORDER BY status"""
        )
        run_rows = cur.fetchall()
    conn.close()

    print("\nGKG-sourced article counts by keyword bucket:")
    for label, n in rows:
        print(f"  {label:<22} {n:>6}")

    if run_rows:
        print("\ngdelt_gkg_fetch run history:")
        for status, n, last in run_rows:
            print(f"  {status:<10} runs={n}  last={last}")
    else:
        print("\nNo gdelt_gkg_fetch runs yet.")


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
        description="GDELT GKG firehose fetcher — pipeline stage 1 (alternate feed)"
    )
    sub = parser.add_subparsers(dest="cmd")
    sub.add_parser("fetch", help="Run a fetch now (default)")
    p_backfill = sub.add_parser("backfill", help="Fetch an explicit historical range")
    p_backfill.add_argument("--days", type=int, default=7,
                             help="How many days back to backfill (default 7)")
    sub.add_parser("stats", help="Counts by label and run history")
    args = parser.parse_args()

    if args.cmd == "backfill":
        cmd_backfill(args.days)
    elif args.cmd == "stats":
        cmd_stats()
    else:
        cmd_fetch()


if __name__ == "__main__":
    main()
