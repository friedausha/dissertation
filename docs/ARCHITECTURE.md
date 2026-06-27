# Pipeline Architecture

Visual reference for everything in this repo. Open this file's preview in VS Code
(`Ctrl+Shift+V`) to render the diagrams — they're Mermaid, which VS Code's built-in
Markdown preview supports natively.

---

## 1. End-to-end flow

```mermaid
flowchart TD
    subgraph SOURCES["GDELT (external)"]
        DOC["DOC 2.0 API\nkeyword search, max 250 results/query"]
        GKG["GKG 15-min export files\nevery article GDELT scanned, no filter"]
    end

    subgraph S1["Stage 1 — Ingest"]
        F1["gdelt_fetch.py\n(DOC API, 4 keyword buckets)"]
        F2["gdelt_gkg_fetch.py\n(firehose + URL keyword filter)"]
        F3["text_fetch.py\n(trafilatura full-text scrape)"]
    end

    subgraph S2["Stage 2 — LLM (via ELM)"]
        C["classifier.py\nis this genuine scam-trafficking?"]
        E["extractor.py\nstructured fields: country, crime_type,\nvictim_count, summary..."]
    end

    subgraph DB["PostgreSQL — trafficking_db"]
        RA[(raw_articles)]
        AC[(article_classifications)]
        INC[(incidents)]
        PR[(pipeline_runs)]
    end

    subgraph API["Stage 3 — REST API (not yet built)"]
        REST["api.py\nGET /incidents, /query, /aggregate/*"]
    end

    JON["Jonathan's frontend\n(Project 1-1, agentic dashboard)"]

    DOC --> F1
    GKG --> F2
    F1 -->|insert, dedup by URL| RA
    F2 -->|insert, dedup by URL| RA
    RA -->|full_text_status='pending'| F3
    F3 -->|writes full_text + status| RA
    RA -->|full_text_status='success'| C
    C -->|writes is_relevant + reasoning| AC
    AC -->|is_relevant=true| E
    E -->|writes structured record| INC
    F1 & F2 & F3 & C & E -.->|logs every run| PR
    INC --> REST
    REST -->|api_readonly role,\nSELECT-only on incidents| JON
```

**Read it as:** two ingest paths feed the same `raw_articles` table → full text gets
scraped → an LLM filters for relevance → a second LLM call extracts structured fields
*only* for articles that passed the filter → those become `incidents` rows → the
(not-yet-built) REST API serves `incidents` to Jonathan's frontend, through a
database role that can't see or touch anything else.

---

## 2. Database schema (ERD)

```mermaid
erDiagram
    raw_articles ||--o| article_classifications : "classified by"
    raw_articles ||--o| incidents : "extracted into"

    raw_articles {
        serial id PK
        text url UK "deduped on this"
        text title
        text domain
        text source_country
        text language
        timestamptz seendate
        text query_label "which keyword bucket / gkg"
        text full_text
        text full_text_status "pending|success|failed|blocked|paywalled"
    }

    article_classifications {
        serial id PK
        int raw_article_id FK "UNIQUE - max 1 per article"
        boolean is_relevant
        text reasoning
        text model_version
    }

    incidents {
        serial id PK
        int raw_article_id FK "UNIQUE - max 1 per article"
        text article_url "denormalised, no join needed"
        date reported_date
        date incident_date
        text location_country
        text location_region "enum, 14 values"
        text crime_type "enum, 8 values"
        int victim_count
        text summary "NOT NULL"
        text confidence "high|medium|low"
    }

    pipeline_runs {
        serial id PK
        text stage "gdelt_fetch|gdelt_gkg_fetch|text_fetch|classification|extraction"
        text status "running|success|failed"
        int articles_processed
        int articles_new
    }
```

**Key thing to notice:** `incidents` and `article_classifications` are **not**
directly linked — both point independently to `raw_articles.raw_article_id`. An
`incidents` row only ever exists for an article whose classification says
`is_relevant = true`. `pipeline_runs` doesn't link to anything; it's just an
audit log every stage writes to.

---

## 3. What each file does

| File | Stage | Reads from | Writes to | Notes |
|---|---|---|---|---|
| [db/schema.sql](../db/schema.sql) | — | — | defines all tables | run once to set up / update the DB |
| [db/seed_lookups.sql](../db/seed_lookups.sql) | — | — | `raw_articles`, `article_classifications`, `incidents` | fake test data for development |
| [gdelt_api/gdelt_fetch.py](../gdelt_api/gdelt_fetch.py) | 1a | GDELT DOC API | `raw_articles` | keyword search, 4 buckets, `sourcelang:english` |
| [gdelt_gkg/gdelt_gkg_fetch.py](../gdelt_gkg/gdelt_gkg_fetch.py) | 1a (alt) | GDELT GKG zip files | `raw_articles` | firehose + URL keyword filter, broader recall |
| [pipeline/text_fetch.py](../pipeline/text_fetch.py) | 1b | `raw_articles` (status=`pending`) | `raw_articles.full_text` | trafilatura scrape, per-domain rate limit |
| [pipeline/classifier.py](../pipeline/classifier.py) | 2a | `raw_articles` (status=`success`) | `article_classifications` | ELM call #1 — relevant or not |
| [pipeline/extractor.py](../pipeline/extractor.py) | 2b | `article_classifications` (relevant=true) | `incidents` | ELM call #2 — structured fields |
| `api.py` | 3 | `incidents` | — | **not built yet** — REST API for Jonathan |
| [api_contract.md](api_contract.md) | — | — | — | spec doc for the API above |
| [cron/crontab.txt](../cron/crontab.txt) | — | — | — | checked-in copy of the live cron schedule |
| `.env` / `.gitignore` | — | — | — | secrets (ELM key, DB URLs), kept out of git |

Every script in stages 1–2 follows the same shape: `start_run()` logs to
`pipeline_runs`, does its work in a batch, `finish_run()` logs the result. Every
script also has a `stats` subcommand to inspect its own table without writing SQL.

---

## 4. One article's life

```mermaid
sequenceDiagram
    participant G as GDELT
    participant RA as raw_articles
    participant T as text_fetch.py
    participant C as classifier.py
    participant AC as article_classifications
    participant E as extractor.py
    participant I as incidents

    G->>RA: INSERT (url, title, domain... query_label)
    Note over RA: full_text_status = 'pending'

    T->>RA: SELECT WHERE status='pending'
    T->>T: scrape URL via trafilatura
    T->>RA: UPDATE full_text, status='success'/'failed'/'blocked'/'paywalled'

    C->>RA: SELECT WHERE status='success' AND unclassified
    C->>C: ELM call — relevant?
    C->>AC: INSERT is_relevant, reasoning

    alt is_relevant = true
        E->>AC: SELECT WHERE is_relevant=true AND not yet extracted
        E->>E: ELM call — extract structured fields
        E->>I: INSERT country, crime_type, summary, confidence...
    else is_relevant = false
        Note over E: article stops here — never reaches incidents
    end
```

Most articles die at one of two points: `text_fetch.py` can't get readable text
(paywall, 404, blocked), or `classifier.py` decides it's not actually about
scam-driven trafficking. Only the survivors become `incidents` rows.

---

## 5. Scheduling

```mermaid
gantt
    dateFormat HH:mm
    axisFormat %H:%M
    title Hourly cron (both run at :00, independently)
    section Ingest
    gdelt_fetch.py (DOC API)      :a1, 00:00, 2m
    gdelt_gkg_fetch.py (firehose) :a2, 00:00, 5m
```

Both jobs are cron'd for `0 * * * *` (every hour on the hour). `text_fetch.py`,
`classifier.py`, and `extractor.py` are **not yet cron'd** — you've been running
them manually after each ingest. Say the word if you want those automated too.

---

## 6. Security boundary

```mermaid
flowchart LR
    subgraph Pipeline["frieda role — full access"]
        RA2[(raw_articles)]
        AC2[(article_classifications)]
        INC2[(incidents)]
        PR2[(pipeline_runs)]
    end

    subgraph Public["api_readonly / api_user role"]
        INC3[(incidents only)]
    end

    REST2["POST /query\n(Jonathan's LLM-generated SQL)"] -->|SELECT-only,\ncan't see other tables| INC3
    INC2 -.same table.- INC3
```

Even if Jonathan's LLM-generated SQL in `POST /query` somehow bypassed the
application-level "SELECT only" string check, the `api_user` Postgres role
physically cannot read `raw_articles`, `article_classifications`, or
`pipeline_runs`, and cannot write anywhere — it only has `SELECT` on `incidents`.
