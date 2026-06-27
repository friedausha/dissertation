-- Migration 001: add deduplication support to raw_articles
--
-- title_hash: MD5 of normalized title (lowercase, punctuation stripped).
--             NULL for rows with no title. Populated on insert by pipeline
--             code; backfilled by: python3 pipeline/dedup.py backfill
--
-- is_duplicate: TRUE if a same-title article with an earlier ID already
--               exists in the DB (within a 3-day window of seendate).
--               Classifier and extractor skip duplicate rows.

ALTER TABLE raw_articles
    ADD COLUMN IF NOT EXISTS title_hash   TEXT,
    ADD COLUMN IF NOT EXISTS is_duplicate BOOLEAN NOT NULL DEFAULT FALSE;

CREATE INDEX IF NOT EXISTS idx_raw_articles_title_hash
    ON raw_articles (title_hash)
    WHERE title_hash IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_raw_articles_is_duplicate
    ON raw_articles (is_duplicate)
    WHERE is_duplicate = FALSE;
