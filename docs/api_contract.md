# API Contract — Project 1-1 ↔ Project 1-2

**Project 1-1 (Frontend):** Jonathan  
**Project 1-2 (Backend):** Frieda  
**Last updated:** 2026-06-06  
**Status:** DRAFT — pending sign-off from both parties

---

## Overview

Frieda's backend exposes a REST API over the `incidents` table, populated from
GDELT's global news stream via an LLM extraction pipeline. Jonathan's frontend
sends queries to this API to populate dynamically generated Vega-Lite dashboards.

Base URL (development): `http://localhost:8000`  
Base URL (production): TBD — agree once deployed on DICE/Eddie

---

## 1. Authentication

None required for the MVP. Both projects run within the university network.
To be revisited if the API is deployed publicly.

---

## 2. Common conventions

### 2.1 Response envelope

Every successful response is a JSON object with a `data` key:

```json
{
  "data": [ ...array of row objects... ],
  "row_count": 42
}
```

Every error response uses the same shape regardless of HTTP status code:

```json
{
  "error": "Human-readable description of what went wrong."
}
```

### 2.2 Date format

All dates are returned as ISO 8601 strings: `"YYYY-MM-DD"`.  
Date filter parameters are also ISO 8601: `?date_from=2024-01-01&date_to=2025-12-31`.

### 2.3 Null values

Fields with no extracted value are returned as JSON `null`, never as empty
string or zero. Jonathan's vis-spec should handle nulls gracefully (e.g. by
filtering them out before plotting).

### 2.4 Column naming

Column names in all responses use **snake_case** matching the `incidents` table
exactly (e.g. `location_country`, `reported_date`, `crime_type`). Jonathan's
LLM should use these names verbatim in Vega-Lite `field` references, or alias
them explicitly in SQL using `AS`.

---

## 3. Endpoints

### 3.1 `GET /health`

Liveness check.

**Response 200:**
```json
{ "status": "ok" }
```

---

### 3.2 `GET /schema`

Returns the `incidents` table schema as structured JSON. Jonathan injects this
into his system prompt so the LLM knows what columns and enum values exist.

**Response 200:**
```json
{
  "data": {
    "table": "incidents",
    "columns": [
      { "name": "id",                    "type": "integer" },
      { "name": "article_url",           "type": "text" },
      { "name": "article_title",         "type": "text" },
      { "name": "article_domain",        "type": "text" },
      { "name": "reported_date",         "type": "date",    "note": "Use for time-series queries. Nearly always populated." },
      { "name": "incident_date",         "type": "date",    "note": "When the incident occurred. Frequently NULL." },
      { "name": "location_country",      "type": "text",    "note": "Full English country name, e.g. 'Myanmar'." },
      { "name": "location_region",       "type": "text",    "enum": ["Southeast Asia","East Asia","South Asia","East Africa","West Africa","Central Africa","Southern Africa","Europe","North America","Central America","South America","Middle East","Pacific","Other"] },
      { "name": "crime_type",            "type": "text",    "enum": ["pig_butchering","scam_compound","forced_labour","sex_trafficking","organ_trafficking","debt_bondage","smuggling","other"] },
      { "name": "victim_count",          "type": "integer", "note": "Frequently NULL." },
      { "name": "victim_nationality",    "type": "text",    "note": "Free text. Frequently NULL." },
      { "name": "perpetrator_nationality","type": "text",   "note": "Free text. Frequently NULL." },
      { "name": "summary",               "type": "text",    "note": "Always populated. One-sentence incident description." },
      { "name": "confidence",            "type": "text",    "enum": ["high","medium","low"] },
      { "name": "reported_date",         "type": "date" }
    ]
  },
  "row_count": 14
}
```

---

### 3.3 `POST /query`

Executes an arbitrary SELECT query against the `incidents` table. This is the
primary endpoint Jonathan's LLM uses to power custom dashboards.

**Request body:**
```json
{
  "sql": "SELECT location_country, COUNT(*) AS incident_count FROM incidents WHERE location_region = 'Southeast Asia' GROUP BY location_country ORDER BY incident_count DESC"
}
```

**Response 200:**
```json
{
  "data": [
    { "location_country": "Myanmar",  "incident_count": 34 },
    { "location_country": "Cambodia", "incident_count": 28 }
  ],
  "row_count": 2
}
```

**Constraints (enforced server-side):**
- Only `SELECT` statements are accepted. Any other statement returns `400`.
- Only the `incidents` table may be queried. References to other tables return `400`.
- Query execution is capped at **5 seconds**. Queries exceeding this return `408`.
- Maximum of **1 000 rows** returned. Add `LIMIT` in the SQL to control this.

**Error responses:**

| Status | Meaning |
|--------|---------|
| 400 | Not a SELECT, references a disallowed table, or SQL syntax error |
| 408 | Query timed out (> 5 s) |
| 500 | Unexpected server error |

---

### 3.4 `GET /incidents`

Returns a paginated list of individual incident records. Useful for "show me
recent incidents" or summary-card widgets.

**Query parameters:**

| Parameter    | Type   | Description |
|--------------|--------|-------------|
| `country`    | string | Filter by `location_country` (exact match) |
| `region`     | string | Filter by `location_region` (exact match) |
| `crime_type` | string | Filter by `crime_type` (exact match) |
| `date_from`  | date   | `reported_date >=` this value |
| `date_to`    | date   | `reported_date <=` this value |
| `limit`      | int    | Max rows returned (default 50, max 500) |

**Response 200:**
```json
{
  "data": [
    {
      "id": 3,
      "article_url": "https://...",
      "article_title": "...",
      "reported_date": "2025-02-28",
      "location_country": "Laos",
      "location_region": "Southeast Asia",
      "crime_type": "scam_compound",
      "victim_count": 5000,
      "victim_nationality": "Multiple nationalities",
      "perpetrator_nationality": "Chinese",
      "summary": "Reports indicate a major scam-compound operation...",
      "confidence": "medium"
    }
  ],
  "row_count": 1
}
```

---

### 3.5 `GET /incidents/aggregate/country`

Returns incident counts grouped by country.

**Query parameters:** `region`, `crime_type`, `date_from`, `date_to`

**Response 200:**
```json
{
  "data": [
    { "location_country": "Myanmar",  "incident_count": 34 },
    { "location_country": "Cambodia", "incident_count": 28 }
  ],
  "row_count": 2
}
```

---

### 3.6 `GET /incidents/aggregate/region`

Returns incident counts grouped by region.

**Query parameters:** `crime_type`, `date_from`, `date_to`

**Response 200:**
```json
{
  "data": [
    { "location_region": "Southeast Asia", "incident_count": 87 },
    { "location_region": "West Africa",    "incident_count": 14 }
  ],
  "row_count": 2
}
```

---

### 3.7 `GET /incidents/aggregate/crime-type`

Returns incident counts grouped by crime type.

**Query parameters:** `country`, `region`, `date_from`, `date_to`

**Response 200:**
```json
{
  "data": [
    { "crime_type": "scam_compound", "incident_count": 52 },
    { "crime_type": "pig_butchering","incident_count": 41 }
  ],
  "row_count": 2
}
```

---

### 3.8 `GET /incidents/aggregate/timeline`

Returns incident counts over time. Use for trend and time-series charts.

**Query parameters:**

| Parameter     | Type   | Description |
|---------------|--------|-------------|
| `country`     | string | Optional filter |
| `region`      | string | Optional filter |
| `crime_type`  | string | Optional filter |
| `granularity` | string | `day`, `week`, or `month` (default `month`) |
| `date_from`   | date   | Optional |
| `date_to`     | date   | Optional |

**Response 200:**
```json
{
  "data": [
    { "period": "2024-01-01", "incident_count": 4 },
    { "period": "2024-02-01", "incident_count": 7 },
    { "period": "2024-03-01", "incident_count": 12 }
  ],
  "row_count": 3
}
```

`period` is always the first day of the granularity bucket (first of the month,
first day of the ISO week, or the exact date).

---

## 4. The 10 benchmark queries

These are the agreed acceptance tests for the REST API. All 10 must return
correct, non-empty JSON responses against the production database before the
integration milestone (M2, Week 7) is considered complete.

| # | Description | Endpoint |
|---|-------------|---------|
| 1 | Total incident count (no filters) | `GET /incidents` |
| 2 | Top 10 countries by incident count | `GET /incidents/aggregate/country` |
| 3 | Incident count per region | `GET /incidents/aggregate/region` |
| 4 | Incident count per crime type, globally | `GET /incidents/aggregate/crime-type` |
| 5 | Monthly timeline for Southeast Asia | `GET /incidents/aggregate/timeline?region=Southeast+Asia&granularity=month` |
| 6 | All incidents in Myanmar (any date) | `GET /incidents?country=Myanmar` |
| 7 | Crime type breakdown within Europe | `GET /incidents/aggregate/crime-type?region=Europe` |
| 8 | Incident counts for both Southeast Asia and West Africa | `GET /incidents/aggregate/region` (client filters the two) |
| 9 | 20 most recent incidents with summaries | `GET /incidents?limit=20` |
| 10 | Custom: victim count by region via SQL | `POST /query` with aggregation SQL |

---

## 5. Open questions — to resolve before Week 3

The following must be agreed before Frieda begins building the API:

- [ ] **Base URL on DICE** — what hostname/port will the API run on in production?
- [ ] **`/query` row limit** — is 1 000 rows sufficient for Jonathan's chart data, or does he need more?
- [ ] **Timeline `period` type** — should `period` be a date string `"2024-01-01"` or a display label `"Jan 2024"`? Affects Vega-Lite temporal encoding.
- [ ] **Null handling in charts** — when `victim_count` is null, should aggregate endpoints omit those rows or count them as zero?
- [ ] **`POST /query` table scope** — should the LLM be allowed to join `raw_articles` for domain/source data, or is `incidents`-only sufficient?

---

## 6. Change log

| Date | Change | Author |
|------|--------|--------|
| 2026-06-06 | Initial draft | Frieda |
