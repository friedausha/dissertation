#!/usr/bin/env python3
"""
LLM relevance classifier — Stage 2a of the trafficking incident pipeline.

Reads raw_articles where:
  - full_text_status = 'success'
  - no row yet in article_classifications

Passes each article's title + body to an LLM and determines whether it
describes a genuine scam-driven trafficking incident. Stores the binary
decision and reasoning in article_classifications.

The classifier specifically targets the SUPPLY SIDE of scam operations:
people who are themselves trafficked and forced to run digital fraud,
NOT articles about fraud victims alone.

Usage:
    python3 classifier.py              # classify all unprocessed articles
    python3 classifier.py --batch 50   # process at most 50 articles
    python3 classifier.py stats        # show classification breakdown
    python3 classifier.py export       # export unclassified sample for annotation
    python3 classifier.py eval FILE    # evaluate against a labelled CSV
"""

import argparse
import csv
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv(Path(__file__).parent.parent / ".env")

# ── Configuration ─────────────────────────────────────────────────────────────

SCRIPT_DIR = Path(__file__).parent
LOG_PATH   = SCRIPT_DIR.parent / "logs" / "classifier.log"

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://frieda:localdev@localhost:5432/trafficking_db"
)

ELM_API_KEY  = os.environ["ELM_API_KEY"]
ELM_BASE_URL = os.environ.get("ELM_BASE_URL", "https://api.openai.com/v1")
ELM_MODEL    = os.environ.get("ELM_MODEL", "gpt-4.1-mini")

MAX_BODY_CHARS   = 2_000   # characters of article body sent to LLM
REQUEST_DELAY_S  = 0.5     # seconds between LLM calls (rate limit headroom)

# ── Prompt ────────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are a specialist research assistant supporting an academic
study of scam-driven human trafficking — a phenomenon in which people are lured
with fraudulent job offers and then held captive in guarded compounds, where they
are forced to carry out online fraud operations (such as romance scams,
pig-butchering cryptocurrency fraud, and impersonation scams).

Your task is to read a news article and decide whether it reports on a genuine
scam-driven trafficking incident.

Classify as RELEVANT (true) if the article describes:
- People trafficked, held, rescued, or escaped from scam compounds or cyber-fraud centres
- Workers forced or coerced into running online scams (pig-butchering, romance fraud, investment fraud)
- Arrests, raids, or prosecutions of operators who traffic workers into fraud operations
- Estimates of victims held in such compounds in specific locations
- Forced criminality at the intersection of human trafficking and cybercrime

Classify as NOT RELEVANT (false) if the article describes:
- Fraud or scam victims ONLY (people who lost money to a scam, with no mention of trafficked workers)
- Traditional human trafficking unconnected to online fraud operations
- General cybercrime, data breaches, or financial fraud with no trafficking component
- Policy, legislation, or statistics discussions without a specific incident
- Unrelated news that merely matched a keyword

Return ONLY a JSON object with exactly two keys:
{
  "relevant": true or false,
  "reason": "One sentence explaining the decision."
}
Do not include any other text."""

USER_TEMPLATE = """Article title: {title}

Article body (excerpt):
{body}"""


# ── LLM client ────────────────────────────────────────────────────────────────

def make_client() -> OpenAI:
    return OpenAI(api_key=ELM_API_KEY, base_url=ELM_BASE_URL)


def classify_article(client: OpenAI, title: str, body: str) -> tuple[bool, str]:
    """
    Call the LLM to classify a single article.
    Returns (is_relevant, reasoning).
    Raises on API error so the caller can decide whether to retry or skip.
    """
    user_msg = USER_TEMPLATE.format(
        title=title or "(no title)",
        body=(body or "")[:MAX_BODY_CHARS],
    )
    response = client.chat.completions.create(
        model=ELM_MODEL,
        temperature=0,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": user_msg},
        ],
    )
    raw = response.choices[0].message.content
    parsed = json.loads(raw)
    return bool(parsed["relevant"]), str(parsed.get("reason", ""))


# ── Database ──────────────────────────────────────────────────────────────────

def get_conn() -> psycopg2.extensions.connection:
    return psycopg2.connect(DATABASE_URL)


def get_unclassified(conn, batch_size: int) -> list[dict]:
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            """SELECT r.id, r.url, r.title, r.full_text
               FROM raw_articles r
               LEFT JOIN article_classifications c ON c.raw_article_id = r.id
               WHERE r.full_text_status = 'success'
                 AND c.id IS NULL
                 AND r.is_duplicate = FALSE
               ORDER BY r.seendate DESC NULLS LAST
               LIMIT %s""",
            (batch_size,),
        )
        return [dict(row) for row in cur.fetchall()]


def save_classification(
    conn, raw_article_id: int, is_relevant: bool,
    reasoning: str, model: str,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO article_classifications
                   (raw_article_id, is_relevant, reasoning, model_version, classified_at)
               VALUES (%s, %s, %s, %s, %s)
               ON CONFLICT (raw_article_id) DO NOTHING""",
            (raw_article_id, is_relevant, reasoning, model,
             datetime.now(timezone.utc)),
        )
    conn.commit()


def start_run(conn) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO pipeline_runs (stage) VALUES ('classification') RETURNING id"
        )
        run_id = cur.fetchone()[0]
    conn.commit()
    return run_id


def finish_run(conn, run_id: int, processed: int, relevant: int,
               status: str, error: str | None = None) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE pipeline_runs
               SET finished_at = NOW(), articles_processed = %s,
                   articles_new = %s, status = %s, error_msg = %s
               WHERE id = %s""",
            (processed, relevant, status, error, run_id),
        )
    conn.commit()


# ── Commands ──────────────────────────────────────────────────────────────────

def cmd_classify(batch_size: int) -> None:
    conn   = get_conn()
    client = make_client()
    run_id = start_run(conn)
    logging.info("=== classifier start  model=%s  batch=%d ===", ELM_MODEL, batch_size)

    articles = get_unclassified(conn, batch_size)
    logging.info("  %d unclassified articles to process", len(articles))

    processed = 0
    relevant_count = 0

    try:
        for i, article in enumerate(articles, 1):
            try:
                is_relevant, reason = classify_article(
                    client, article["title"], article["full_text"]
                )
            except Exception as exc:
                logging.warning(
                    "  [%d/%d] LLM error for id=%d — skipping: %s",
                    i, len(articles), article["id"], exc,
                )
                continue

            save_classification(conn, article["id"], is_relevant, reason, ELM_MODEL)
            processed += 1
            if is_relevant:
                relevant_count += 1

            logging.info(
                "  [%d/%d] %-5s  %s | %s",
                i, len(articles),
                "YES" if is_relevant else "no",
                (article["title"] or "")[:60],
                reason[:80],
            )

            time.sleep(REQUEST_DELAY_S)

        finish_run(conn, run_id, processed, relevant_count, "success")
        logging.info(
            "=== classifier done  processed=%d  relevant=%d  not_relevant=%d ===",
            processed, relevant_count, processed - relevant_count,
        )

    except Exception as exc:
        finish_run(conn, run_id, processed, relevant_count, "failed", str(exc))
        logging.exception("classifier failed")
        raise
    finally:
        conn.close()


def cmd_stats() -> None:
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM raw_articles WHERE full_text_status = 'success'")
        total_with_text = cur.fetchone()[0]

        cur.execute(
            """SELECT is_relevant, COUNT(*) AS n
               FROM article_classifications
               GROUP BY is_relevant ORDER BY is_relevant DESC"""
        )
        class_rows = cur.fetchall()

        cur.execute(
            """SELECT COUNT(*) FROM raw_articles r
               LEFT JOIN article_classifications c ON c.raw_article_id = r.id
               WHERE r.full_text_status = 'success' AND c.id IS NULL"""
        )
        pending = cur.fetchone()[0]

        cur.execute(
            """SELECT r.domain, COUNT(*) AS n
               FROM raw_articles r
               JOIN article_classifications c ON c.raw_article_id = r.id
               WHERE c.is_relevant = true
               GROUP BY r.domain ORDER BY n DESC LIMIT 10"""
        )
        top_domains = cur.fetchall()
    conn.close()

    print(f"\nArticles with full text: {total_with_text}")
    print(f"Pending classification:  {pending}")
    for is_relevant, n in class_rows:
        label = "relevant    " if is_relevant else "not_relevant"
        bar = "█" * min(40, int(40 * n / max(total_with_text, 1)))
        print(f"  {label}  {n:>6}  {bar}")

    if top_domains:
        print("\nTop domains in relevant articles:")
        for domain, n in top_domains:
            print(f"  {n:>5}  {domain}")


def cmd_export(out_path: str = "annotation_sample.csv") -> None:
    """
    Export a random sample of articles for manual annotation.
    The CSV has columns: id, url, title, body_excerpt, label (blank for human to fill).
    Used to build the ~100-article ground truth for precision/recall evaluation.
    """
    conn = get_conn()
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            """SELECT r.id, r.url, r.title,
                      LEFT(r.full_text, 500) AS body_excerpt
               FROM raw_articles r
               WHERE r.full_text_status = 'success'
               ORDER BY RANDOM()
               LIMIT 100"""
        )
        rows = cur.fetchall()
    conn.close()

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=["id", "url", "title", "body_excerpt", "label"]
        )
        writer.writeheader()
        for row in rows:
            writer.writerow({**dict(row), "label": ""})

    print(f"Exported {len(rows)} articles to {out_path}")
    print("Fill the 'label' column with 1 (relevant) or 0 (not relevant), then run:")
    print(f"  python3 classifier.py eval {out_path}")


def cmd_eval(csv_path: str) -> None:
    """
    Evaluate classifier performance against a manually annotated CSV.
    Expects columns: id, label (1=relevant, 0=not_relevant).
    Prints precision, recall, F1, and per-field confusion counts.
    """
    ground_truth: dict[int, bool] = {}
    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row["label"].strip() in ("0", "1"):
                ground_truth[int(row["id"])] = row["label"].strip() == "1"

    if not ground_truth:
        print("No labelled rows found. Fill the 'label' column with 0 or 1.")
        return

    conn = get_conn()
    with conn.cursor() as cur:
        ids = list(ground_truth.keys())
        cur.execute(
            "SELECT raw_article_id, is_relevant FROM article_classifications "
            "WHERE raw_article_id = ANY(%s)",
            (ids,),
        )
        predictions = {row[0]: row[1] for row in cur.fetchall()}
    conn.close()

    tp = fp = tn = fn = 0
    missing = 0
    for art_id, true_label in ground_truth.items():
        if art_id not in predictions:
            missing += 1
            continue
        pred = predictions[art_id]
        if true_label and pred:     tp += 1
        elif not true_label and pred: fp += 1
        elif true_label and not pred: fn += 1
        else:                         tn += 1

    total = tp + fp + tn + fn
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall    = tp / (tp + fn) if (tp + fn) else 0.0
    f1        = (2 * precision * recall / (precision + recall)
                 if (precision + recall) else 0.0)

    print(f"\nEvaluation against {total} labelled articles ({missing} not yet classified)\n")
    print(f"  True positives:  {tp}")
    print(f"  False positives: {fp}")
    print(f"  True negatives:  {tn}")
    print(f"  False negatives: {fn}")
    print(f"\n  Precision: {precision:.3f}")
    print(f"  Recall:    {recall:.3f}")
    print(f"  F1:        {f1:.3f}")


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
        description="LLM relevance classifier — pipeline stage 2a"
    )
    sub = parser.add_subparsers(dest="cmd")

    p_cls = sub.add_parser("classify", help="Classify unprocessed articles (default)")
    p_cls.add_argument("--batch", type=int, default=500,
                       help="Max articles per run (default 500)")

    sub.add_parser("stats",  help="Show classification breakdown")

    p_exp = sub.add_parser("export", help="Export sample for manual annotation")
    p_exp.add_argument("--out", default="annotation_sample.csv",
                       help="Output CSV path (default: annotation_sample.csv)")

    p_eval = sub.add_parser("eval", help="Evaluate against annotated CSV")
    p_eval.add_argument("file", help="Path to annotated CSV")

    args = parser.parse_args()

    if args.cmd == "stats":
        cmd_stats()
    elif args.cmd == "export":
        cmd_export(args.out)
    elif args.cmd == "eval":
        cmd_eval(args.file)
    else:
        batch = getattr(args, "batch", 500)
        cmd_classify(batch)


if __name__ == "__main__":
    main()
