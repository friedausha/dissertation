-- =============================================================================
-- Scam-Driven Human Trafficking Incident Database
-- Project 1-2 (Frieda) — Backend pipeline
-- Companion to Project 1-1 (Jonathan) — Generative visualisation dashboard
--
-- Two-layer design:
--   Layer 1  raw_articles + article_classifications  (pipeline internals)
--   Layer 2  incidents                               (exposed via REST API)
--   Layer 3  pipeline_runs                           (ops / Slurm tracking)
--
-- Roles:
--   frieda    — pipeline user, full read/write on all tables
--   api_user  — REST API user, SELECT on incidents only (defence against
--               SQL injection via POST /query)
-- =============================================================================

-- =============================================================================
-- LAYER 1 — PIPELINE TABLES (internal, not exposed to Jonathan's LLM)
-- =============================================================================

CREATE TABLE IF NOT EXISTS raw_articles (
    id                      SERIAL PRIMARY KEY,

    -- GDELT-provided metadata
    url                     TEXT        NOT NULL UNIQUE,
    title                   TEXT,
    domain                  TEXT,
    source_country          TEXT,               -- 2-char ISO from GDELT e.g. 'MM', 'KH', 'GB'
    language                TEXT,
    seendate                TIMESTAMPTZ,        -- when GDELT first indexed this article
    fetched_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    query_label             TEXT        NOT NULL, -- which keyword bucket matched
        -- valid values: 'human_trafficking' | 'pig_butchering' |
        --               'smuggling_debt' | 'online_scam_camps' |
        --               'gkg_theme_match' (matched via GDELT GKG theme code,
        --               not a keyword bucket — see gdelt_gkg_fetch.py)

    -- Full article text (fetched separately from the URL)
    full_text               TEXT,
    full_text_status        TEXT        NOT NULL DEFAULT 'pending'
        CHECK (full_text_status IN ('pending','success','failed','blocked','paywalled')),
    full_text_fetched_at    TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_raw_articles_seendate
    ON raw_articles (seendate DESC);
CREATE INDEX IF NOT EXISTS idx_raw_articles_query_label
    ON raw_articles (query_label);
CREATE INDEX IF NOT EXISTS idx_raw_articles_full_text_status
    ON raw_articles (full_text_status);


-- LLM relevance classification
-- One row per raw_article. is_relevant=TRUE gates entry into structured extraction.
CREATE TABLE IF NOT EXISTS article_classifications (
    id              SERIAL PRIMARY KEY,
    raw_article_id  INT         NOT NULL
                    REFERENCES raw_articles(id) ON DELETE CASCADE,
    is_relevant     BOOLEAN     NOT NULL,
    reasoning       TEXT,               -- LLM explanation; kept for prompt-refinement eval
    model_version   TEXT,               -- e.g. 'gpt-4o-2024-08-06'
    classified_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (raw_article_id)             -- one classification per article
);

CREATE INDEX IF NOT EXISTS idx_classifications_relevant
    ON article_classifications (is_relevant);
CREATE INDEX IF NOT EXISTS idx_classifications_classified_at
    ON article_classifications (classified_at DESC);


-- =============================================================================
-- LAYER 2 — DATA TABLE (exposed via REST API to Jonathan's frontend)
--
-- Design notes for Jonathan's LLM:
--   • Full country names (not ISO codes) — SQL like WHERE location_country = 'Myanmar'
--   • location_region uses a fixed enum injected into the system prompt
--   • crime_type uses a fixed enum injected into the system prompt
--   • Denormalised article fields so no joins are needed in generated SQL
--   • reported_date is almost always populated; incident_date is often NULL
-- =============================================================================

CREATE TABLE IF NOT EXISTS incidents (
    id                      SERIAL PRIMARY KEY,

    -- Source traceability (denormalised — no join needed from Jonathan's queries)
    raw_article_id          INT
                            REFERENCES raw_articles(id) ON DELETE SET NULL,
    article_url             TEXT        NOT NULL,
    article_title           TEXT,
    article_domain          TEXT,

    -- Temporal
    reported_date           DATE,       -- article publication date (use for time-series queries)
    incident_date           DATE,       -- when the incident occurred (frequently NULL)

    -- Geography
    location_country        TEXT,       -- full English name e.g. 'Myanmar', 'Cambodia', 'Nigeria'
    location_region         TEXT
        CHECK (location_region IN (
            'Southeast Asia',
            'East Asia',
            'South Asia',
            'East Africa',
            'West Africa',
            'Central Africa',
            'Southern Africa',
            'Europe',
            'North America',
            'Central America',
            'South America',
            'Middle East',
            'Pacific',
            'Other'
        )),

    -- Incident classification
    crime_type              TEXT
        CHECK (crime_type IN (
            'pig_butchering',       -- romance-investment hybrid fraud
            'scam_compound',        -- forced criminality in guarded compound
            'forced_labour',        -- non-digital forced labour exploitation
            'sex_trafficking',
            'organ_trafficking',
            'debt_bondage',
            'smuggling',
            'other'
        )),

    -- Victim / perpetrator details (often NULL — null preferred over hallucination)
    victim_count            INT,
    victim_nationality      TEXT,       -- free text, e.g. 'Filipino', 'Chinese'
    perpetrator_nationality TEXT,       -- free text

    -- Human-readable incident summary (always populated)
    summary                 TEXT        NOT NULL,

    -- Extraction provenance
    confidence              TEXT        NOT NULL DEFAULT 'medium'
        CHECK (confidence IN ('high', 'medium', 'low')),
    extracted_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    model_version           TEXT,

    UNIQUE (raw_article_id)             -- one incident record per source article
);

-- Indexes covering all columns Jonathan's LLM will filter or group on
CREATE INDEX IF NOT EXISTS idx_incidents_reported_date
    ON incidents (reported_date DESC);
CREATE INDEX IF NOT EXISTS idx_incidents_location_country
    ON incidents (location_country);
CREATE INDEX IF NOT EXISTS idx_incidents_location_region
    ON incidents (location_region);
CREATE INDEX IF NOT EXISTS idx_incidents_crime_type
    ON incidents (crime_type);
CREATE INDEX IF NOT EXISTS idx_incidents_region_date
    ON incidents (location_region, reported_date DESC);  -- common compound query
CREATE INDEX IF NOT EXISTS idx_incidents_country_date
    ON incidents (location_country, reported_date DESC);


-- =============================================================================
-- LAYER 3 — PIPELINE OPS (internal; useful for Slurm job monitoring)
-- =============================================================================

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id                  SERIAL PRIMARY KEY,
    stage               TEXT        NOT NULL
        CHECK (stage IN ('gdelt_fetch', 'gdelt_gkg_fetch', 'text_fetch', 'classification', 'extraction')),
    started_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at         TIMESTAMPTZ,
    articles_processed  INT         NOT NULL DEFAULT 0,
    articles_new        INT         NOT NULL DEFAULT 0,
    status              TEXT        NOT NULL DEFAULT 'running'
        CHECK (status IN ('running', 'success', 'failed')),
    error_msg           TEXT
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_stage_started
    ON pipeline_runs (stage, started_at DESC);


-- =============================================================================
-- ROLES — run once per environment (idempotent via DO block)
-- =============================================================================

DO $$
BEGIN
    -- Read-only role used by the REST API server for POST /query and all
    -- GET endpoints. Even if a malicious SQL string bypasses application-level
    -- checks, this role cannot write, drop, or read internal pipeline tables.
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'api_readonly') THEN
        CREATE ROLE api_readonly;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'api_user') THEN
        CREATE USER api_user WITH PASSWORD 'apilocaldev';
        GRANT api_readonly TO api_user;
    END IF;
END
$$;

-- Grant connect + schema access
GRANT CONNECT ON DATABASE trafficking_db TO api_readonly;
GRANT USAGE ON SCHEMA public TO api_readonly;

-- api_readonly can only SELECT on incidents — not raw_articles,
-- article_classifications, or pipeline_runs
GRANT SELECT ON incidents TO api_readonly;

-- Ensure future tables added to public schema do NOT inherit access
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    REVOKE ALL ON TABLES FROM api_readonly;
