#!/usr/bin/env python3
"""
LLM structured extractor — Stage 2b of the trafficking incident pipeline.

Reads articles where:
  - article_classifications.is_relevant = true
  - no row yet in incidents for that article

Passes each article to an LLM and extracts a structured incident record
(date, country, crime type, victim count, etc.) into the incidents table.

Core design rule: null over hallucination.
The LLM is explicitly instructed to return null for any field it cannot
confidently extract from the article text — never to guess or infer.

Usage:
    python3 extractor.py              # extract all unprocessed relevant articles
    python3 extractor.py --batch 50   # process at most 50 articles
    python3 extractor.py stats        # show extraction coverage breakdown
"""

import argparse
import json
import logging
import os
import re
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
LOG_PATH   = SCRIPT_DIR.parent / "logs" / "extractor.log"

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://frieda:localdev@localhost:5432/trafficking_db"
)

ELM_API_KEY  = os.environ["ELM_API_KEY"]
ELM_BASE_URL = os.environ.get("ELM_BASE_URL", "https://api.openai.com/v1")
ELM_MODEL    = os.environ.get("ELM_MODEL", "gpt-4.1-mini")

MAX_BODY_CHARS  = 3_000   # more context than classifier since we need specific facts
REQUEST_DELAY_S = 0.5

# Valid enum values — must match schema.sql exactly
VALID_REGIONS = {
    "Southeast Asia", "East Asia", "South Asia",
    "East Africa", "West Africa", "Central Africa", "Southern Africa",
    "Europe", "North America", "Central America", "South America",
    "Middle East", "Pacific", "Other",
}

VALID_CRIME_TYPES = {
    "pig_butchering", "scam_compound", "forced_labour",
    "sex_trafficking", "organ_trafficking", "debt_bondage",
    "smuggling", "other",
}


# ── Prompt ────────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are a specialist data extraction assistant for an academic
study of scam-driven human trafficking. You will be given a news article and must
extract specific factual fields into a structured JSON record.

CRITICAL RULE: Return null for any field you cannot extract with confidence
directly from the article text. Never guess, infer, or hallucinate values.
A null field is far more useful than a fabricated one.

Extract the following fields:

incident_date
  The date the incident occurred (not the publication date).
  Format: "YYYY-MM-DD". Return null if not stated.

location_country
  The full English name of the primary country where the incident occurred.
  Example: "Myanmar", "Cambodia", "Nigeria". Return null if unclear.

location_region
  The geographic region. Must be EXACTLY one of:
  "Southeast Asia", "East Asia", "South Asia", "East Africa", "West Africa",
  "Central Africa", "Southern Africa", "Europe", "North America",
  "Central America", "South America", "Middle East", "Pacific", "Other"
  Return null if location_country is null.

crime_type
  Must be EXACTLY one of:
  "pig_butchering"   — romance-investment hybrid fraud (sha zhu pan)
  "scam_compound"    — people held in guarded facilities and forced to run any online scam
  "forced_labour"    — non-digital forced labour exploitation
  "sex_trafficking"  — sexual exploitation
  "organ_trafficking"— trafficking for organ removal
  "debt_bondage"     — exploitation via inflated debt
  "smuggling"        — movement of people across borders for exploitation
  "other"            — trafficking-related but does not fit above categories
  If multiple apply, choose the most prominent one.

victim_count
  An integer: the number of victims, workers, or people rescued/held, if explicitly
  stated. Return null if not stated or if given as a vague range.

victim_nationality
  Free text describing the nationality of victims if explicitly stated.
  Example: "Filipino", "Chinese and Vietnamese". Return null if not stated.

perpetrator_nationality
  Free text describing the nationality of perpetrators/operators if explicitly
  stated. Return null if not stated.

summary
  A single factual sentence (max 40 words) summarising what happened, where,
  and to whom. This field is REQUIRED — always populate it.

Return ONLY a valid JSON object with these exact keys. No other text.

Example of a good response:
{
  "incident_date": "2024-03-10",
  "location_country": "Myanmar",
  "location_region": "Southeast Asia",
  "crime_type": "scam_compound",
  "victim_count": 3000,
  "victim_nationality": "Multiple nationalities",
  "perpetrator_nationality": "Chinese",
  "summary": "An estimated 3,000 people from multiple countries were held in guarded compounds in Myanmar's Shan State, forced to run online fraud operations."
}

Example of a response with appropriate nulls:
{
  "incident_date": null,
  "location_country": "United Kingdom",
  "location_region": "Europe",
  "crime_type": "pig_butchering",
  "victim_count": null,
  "victim_nationality": "British",
  "perpetrator_nationality": null,
  "summary": "UK Action Fraud reported a surge in pig-butchering complaints linked to Southeast Asian scam compounds where trafficked workers pose as romantic partners."
}"""

USER_TEMPLATE = """Article title: {title}

Article body (excerpt):
{body}"""


# ── Validation & confidence ───────────────────────────────────────────────────

def validate_and_clean(raw: dict) -> dict:
    """
    Validate enum fields against schema, coerce or null invalid values,
    and ensure summary is present. Returns the cleaned record.
    """
    # Validate region
    region = raw.get("location_region")
    if region and region not in VALID_REGIONS:
        # Try case-insensitive match
        match = next((v for v in VALID_REGIONS if v.lower() == region.lower()), None)
        raw["location_region"] = match  # None if no match

    # Validate crime_type
    crime = raw.get("crime_type")
    if crime and crime not in VALID_CRIME_TYPES:
        raw["crime_type"] = None

    # victim_count must be a positive integer
    vc = raw.get("victim_count")
    if vc is not None:
        try:
            raw["victim_count"] = int(vc)
            if raw["victim_count"] <= 0:
                raw["victim_count"] = None
        except (TypeError, ValueError):
            raw["victim_count"] = None

    # Truncate free-text fields to sane lengths
    for field in ("victim_nationality", "perpetrator_nationality"):
        if raw.get(field):
            raw[field] = str(raw[field])[:200]

    if raw.get("summary"):
        raw["summary"] = str(raw["summary"])[:500]

    # Validate incident_date format
    date_str = raw.get("incident_date")
    if date_str:
        try:
            datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            raw["incident_date"] = None

    return raw


def score_confidence(record: dict) -> str:
    """
    Assign confidence based on how many key fields were extracted.
      high   — country + crime_type + summary + at least one of (date, victim_count)
      medium — country + crime_type + summary
      low    — summary only, or missing country/crime_type
    """
    has_country     = bool(record.get("location_country"))
    has_crime_type  = bool(record.get("crime_type"))
    has_summary     = bool(record.get("summary"))
    has_extra       = bool(record.get("incident_date") or record.get("victim_count"))

    if has_country and has_crime_type and has_summary and has_extra:
        return "high"
    if has_country and has_crime_type and has_summary:
        return "medium"
    return "low"


# ── LLM client ────────────────────────────────────────────────────────────────

def make_client() -> OpenAI:
    return OpenAI(api_key=ELM_API_KEY, base_url=ELM_BASE_URL)


def extract_article(client: OpenAI, title: str, body: str) -> dict:
    """
    Call the LLM to extract structured fields from one article.
    Returns the validated extraction dict.
    Raises on API or parse error.
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
    raw = json.loads(response.choices[0].message.content)
    return validate_and_clean(raw)


# ── Database ──────────────────────────────────────────────────────────────────

def get_conn() -> psycopg2.extensions.connection:
    return psycopg2.connect(DATABASE_URL)


def get_unextracted(conn, batch_size: int) -> list[dict]:
    """Relevant articles that don't yet have an incidents row."""
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            """SELECT r.id AS raw_id, r.url, r.title, r.full_text,
                      r.domain, r.seendate
               FROM raw_articles r
               JOIN article_classifications c ON c.raw_article_id = r.id
               LEFT JOIN incidents i ON i.raw_article_id = r.id
               WHERE c.is_relevant = true
                 AND i.id IS NULL
               ORDER BY r.seendate DESC NULLS LAST
               LIMIT %s""",
            (batch_size,),
        )
        return [dict(row) for row in cur.fetchall()]


def insert_incident(conn, raw_id: int, article: dict, record: dict,
                    confidence: str) -> None:
    reported_date = (
        article["seendate"].date()
        if article.get("seendate") else None
    )

    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO incidents (
                   raw_article_id, article_url, article_title, article_domain,
                   reported_date, incident_date,
                   location_country, location_region, crime_type,
                   victim_count, victim_nationality, perpetrator_nationality,
                   summary, confidence, model_version, extracted_at
               ) VALUES (
                   %s, %s, %s, %s,
                   %s, %s,
                   %s, %s, %s,
                   %s, %s, %s,
                   %s, %s, %s, %s
               )
               ON CONFLICT (raw_article_id) DO NOTHING""",
            (
                raw_id,
                article["url"],
                article["title"],
                article.get("domain"),
                reported_date,
                record.get("incident_date"),
                record.get("location_country"),
                record.get("location_region"),
                record.get("crime_type"),
                record.get("victim_count"),
                record.get("victim_nationality"),
                record.get("perpetrator_nationality"),
                record.get("summary", ""),
                confidence,
                ELM_MODEL,
                datetime.now(timezone.utc),
            ),
        )
    conn.commit()


def start_run(conn) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO pipeline_runs (stage) VALUES ('extraction') RETURNING id"
        )
        run_id = cur.fetchone()[0]
    conn.commit()
    return run_id


def finish_run(conn, run_id: int, processed: int, inserted: int,
               status: str, error: str | None = None) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE pipeline_runs
               SET finished_at = NOW(), articles_processed = %s,
                   articles_new = %s, status = %s, error_msg = %s
               WHERE id = %s""",
            (processed, inserted, status, error, run_id),
        )
    conn.commit()


# ── Commands ──────────────────────────────────────────────────────────────────

def cmd_extract(batch_size: int) -> None:
    conn   = get_conn()
    client = make_client()
    run_id = start_run(conn)
    logging.info("=== extractor start  model=%s  batch=%d ===", ELM_MODEL, batch_size)

    articles = get_unextracted(conn, batch_size)
    logging.info("  %d relevant articles awaiting extraction", len(articles))

    processed = inserted = 0

    try:
        for i, article in enumerate(articles, 1):
            try:
                record = extract_article(
                    client, article["title"], article["full_text"]
                )
            except Exception as exc:
                logging.warning(
                    "  [%d/%d] LLM error for id=%d — skipping: %s",
                    i, len(articles), article["raw_id"], exc,
                )
                continue

            if not record.get("summary"):
                logging.warning(
                    "  [%d/%d] No summary extracted for id=%d — skipping",
                    i, len(articles), article["raw_id"],
                )
                continue

            confidence = score_confidence(record)
            insert_incident(conn, article["raw_id"], article, record, confidence)
            processed += 1
            inserted  += 1

            logging.info(
                "  [%d/%d] [%s] %s | %s → %s | %s",
                i, len(articles),
                confidence,
                record.get("location_country") or "?",
                record.get("crime_type") or "?",
                f"victims={record.get('victim_count') or '?'}",
                (article["title"] or "")[:50],
            )

            time.sleep(REQUEST_DELAY_S)

        finish_run(conn, run_id, processed, inserted, "success")
        logging.info(
            "=== extractor done  processed=%d  inserted=%d ===",
            processed, inserted,
        )

    except Exception as exc:
        finish_run(conn, run_id, processed, inserted, "failed", str(exc))
        logging.exception("extractor failed")
        raise
    finally:
        conn.close()


def cmd_stats() -> None:
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(
            """SELECT
                 COUNT(*) FILTER (WHERE c.is_relevant = true)           AS relevant,
                 COUNT(*) FILTER (WHERE c.is_relevant = true
                                    AND i.id IS NOT NULL)                AS extracted,
                 COUNT(*) FILTER (WHERE c.is_relevant = true
                                    AND i.id IS NULL)                    AS pending
               FROM article_classifications c
               JOIN raw_articles r ON r.id = c.raw_article_id
               LEFT JOIN incidents i ON i.raw_article_id = r.id"""
        )
        rel, ext, pend = cur.fetchone()

        cur.execute(
            """SELECT confidence, COUNT(*) AS n
               FROM incidents WHERE model_version != 'seed'
               GROUP BY confidence ORDER BY n DESC"""
        )
        conf_rows = cur.fetchall()

        cur.execute(
            """SELECT crime_type, COUNT(*) AS n
               FROM incidents
               GROUP BY crime_type ORDER BY n DESC"""
        )
        crime_rows = cur.fetchall()

        cur.execute(
            """SELECT location_region, COUNT(*) AS n
               FROM incidents WHERE location_region IS NOT NULL
               GROUP BY location_region ORDER BY n DESC"""
        )
        region_rows = cur.fetchall()

        cur.execute(
            """SELECT
                 COUNT(*) FILTER (WHERE location_country IS NOT NULL)   AS has_country,
                 COUNT(*) FILTER (WHERE incident_date IS NOT NULL)      AS has_date,
                 COUNT(*) FILTER (WHERE victim_count IS NOT NULL)       AS has_count,
                 COUNT(*) FILTER (WHERE victim_nationality IS NOT NULL) AS has_vnat,
                 COUNT(*)                                               AS total
               FROM incidents WHERE model_version != 'seed'"""
        )
        cov = cur.fetchone()
    conn.close()

    print(f"\nExtraction pipeline:")
    print(f"  Relevant articles:  {rel}")
    print(f"  Extracted → incidents: {ext}")
    print(f"  Pending extraction:    {pend}")

    if cov and cov[4]:
        total = cov[4]
        print(f"\nField coverage (LLM-extracted, n={total}):")
        print(f"  location_country:    {cov[0]:>4} / {total}  ({100*cov[0]//total}%)")
        print(f"  incident_date:       {cov[1]:>4} / {total}  ({100*cov[1]//total}%)")
        print(f"  victim_count:        {cov[2]:>4} / {total}  ({100*cov[2]//total}%)")
        print(f"  victim_nationality:  {cov[3]:>4} / {total}  ({100*cov[3]//total}%)")

    if conf_rows:
        print(f"\nConfidence breakdown (LLM-extracted):")
        for conf, n in conf_rows:
            print(f"  {conf:<8}  {n}")

    if crime_rows:
        print(f"\nCrime type breakdown (all incidents):")
        for crime, n in crime_rows:
            print(f"  {(crime or 'null'):<22}  {n}")

    if region_rows:
        print(f"\nRegion breakdown (all incidents):")
        for region, n in region_rows:
            print(f"  {region:<22}  {n}")


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
        description="LLM structured extractor — pipeline stage 2b"
    )
    sub = parser.add_subparsers(dest="cmd")

    p_ext = sub.add_parser("extract", help="Extract unprocessed relevant articles (default)")
    p_ext.add_argument("--batch", type=int, default=500,
                       help="Max articles per run (default 500)")

    sub.add_parser("stats", help="Show extraction coverage and breakdown")

    args = parser.parse_args()

    if args.cmd == "stats":
        cmd_stats()
    else:
        batch = getattr(args, "batch", 500)
        cmd_extract(batch)


if __name__ == "__main__":
    main()
