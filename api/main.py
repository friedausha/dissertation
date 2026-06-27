#!/usr/bin/env python3
"""
REST API — Stage 3 of the trafficking incident pipeline.

Exposes the incidents table to Jonathan's frontend via a read-only API.
Runs using the api_user Postgres role which has SELECT-only on incidents,
so even malicious SQL in POST /query cannot read pipeline-internal tables.

Usage:
    uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload

Endpoints (see docs/api_contract.md for the full spec):
    GET  /health
    GET  /schema
    POST /query
    GET  /incidents
    GET  /incidents/aggregate/country
    GET  /incidents/aggregate/region
    GET  /incidents/aggregate/crime_type
"""

import json
import os
import re
from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

# ── Configuration ─────────────────────────────────────────────────────────────

API_DATABASE_URL = os.environ.get(
    "API_DATABASE_URL",
    "postgresql://api_user:apilocaldev@localhost:5432/trafficking_db"
)

QUERY_TIMEOUT_MS = 5_000
MAX_ROWS         = 1_000

# ── Schema definition (mirrors schema.sql) ────────────────────────────────────

INCIDENTS_SCHEMA = {
    "table": "incidents",
    "columns": [
        {"name": "id",                       "type": "integer"},
        {"name": "article_url",              "type": "text"},
        {"name": "article_title",            "type": "text"},
        {"name": "article_domain",           "type": "text"},
        {"name": "reported_date",            "type": "date",    "note": "Article publication date. Use for time-series queries. Nearly always populated."},
        {"name": "incident_date",            "type": "date",    "note": "When the incident occurred. Frequently NULL."},
        {"name": "location_country",         "type": "text",    "note": "Full English country name, e.g. 'Myanmar', 'Cambodia', 'Nigeria'."},
        {"name": "location_region",          "type": "text",    "enum": ["Southeast Asia","East Asia","South Asia","East Africa","West Africa","Central Africa","Southern Africa","Europe","North America","Central America","South America","Middle East","Pacific","Other"]},
        {"name": "crime_type",               "type": "text",    "enum": ["pig_butchering","scam_compound","forced_labour","sex_trafficking","organ_trafficking","debt_bondage","smuggling","other"]},
        {"name": "victim_count",             "type": "integer", "note": "Frequently NULL."},
        {"name": "victim_nationality",       "type": "text",    "note": "Free text. Frequently NULL."},
        {"name": "perpetrator_nationality",  "type": "text",    "note": "Free text. Frequently NULL."},
        {"name": "summary",                  "type": "text",    "note": "One-sentence incident description. Always populated."},
        {"name": "confidence",               "type": "text",    "enum": ["high","medium","low"]},
    ],
}

# ── App setup ─────────────────────────────────────────────────────────────────

app = FastAPI(
    title="Trafficking Incidents API",
    description="Read-only REST API over the incidents table. See /schema for column definitions.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


# ── JSON serialisation ────────────────────────────────────────────────────────

class _Encoder(json.JSONEncoder):
    """Handles psycopg2 types Python's json module doesn't know about."""
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


def _ok(data: Any, row_count: int) -> JSONResponse:
    return JSONResponse(
        content=json.loads(json.dumps({"data": data, "row_count": row_count}, cls=_Encoder))
    )


# ── Database helpers ──────────────────────────────────────────────────────────

@contextmanager
def get_conn():
    conn = psycopg2.connect(API_DATABASE_URL)
    try:
        yield conn
    finally:
        conn.close()


def _fetch(cur) -> list[dict]:
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


# ── SQL validation for POST /query ────────────────────────────────────────────

_COMMENT_RE    = re.compile(r"--[^\n]*|/\*.*?\*/", re.DOTALL)
_WHITESPACE_RE = re.compile(r"\s+")


def _validate_query(sql: str) -> str:
    cleaned = _COMMENT_RE.sub(" ", sql).strip()

    if cleaned.endswith(";"):
        cleaned = cleaned[:-1].rstrip()

    if ";" in cleaned:
        raise HTTPException(status_code=400, detail="Only a single SELECT statement is accepted.")

    if not _WHITESPACE_RE.sub(" ", cleaned).strip().upper().startswith("SELECT"):
        raise HTTPException(status_code=400, detail="Only SELECT statements are accepted.")

    if not re.search(r"\bLIMIT\b", cleaned, re.IGNORECASE):
        cleaned = f"{cleaned} LIMIT {MAX_ROWS}"

    return cleaned


def _run_query(sql: str) -> list[dict]:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(f"SET statement_timeout = {QUERY_TIMEOUT_MS}")
            try:
                cur.execute(sql)
            except psycopg2.errors.QueryCanceled:
                raise HTTPException(status_code=408, detail=f"Query timed out (>{QUERY_TIMEOUT_MS // 1000}s limit).")
            except psycopg2.errors.InsufficientPrivilege:
                raise HTTPException(status_code=400, detail="Query references a table or column not available via this API.")
            except psycopg2.Error as e:
                parts = [e.diag.message_primary or str(e)]
                if e.diag.message_detail:
                    parts.append(f"Detail: {e.diag.message_detail}")
                if e.diag.message_hint:
                    parts.append(f"Hint: {e.diag.message_hint}")
                if e.diag.statement_position:
                    parts.append(f"Position: {e.diag.statement_position}")
                raise HTTPException(status_code=400, detail=" | ".join(parts))
            return _fetch(cur)


# ── Request model ─────────────────────────────────────────────────────────────

class QueryRequest(BaseModel):
    sql: str


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/schema")
def schema():
    return _ok(INCIDENTS_SCHEMA, len(INCIDENTS_SCHEMA["columns"]))


@app.post("/query")
def query(body: QueryRequest):
    rows = _run_query(_validate_query(body.sql))
    return _ok(rows, len(rows))


@app.get("/incidents")
def get_incidents(
    country:    str | None  = Query(None, description="Filter by location_country (exact)"),
    region:     str | None  = Query(None, description="Filter by location_region (exact)"),
    crime_type: str | None  = Query(None, description="Filter by crime_type (exact)"),
    date_from:  date | None = Query(None, description="reported_date >= this date (YYYY-MM-DD)"),
    date_to:    date | None = Query(None, description="reported_date <= this date (YYYY-MM-DD)"),
    limit:      int         = Query(50, ge=1, le=500, description="Max rows (default 50, max 500)"),
):
    conditions: list[str] = []
    params: list[Any]     = []

    if country:
        conditions.append("location_country = %s")
        params.append(country)
    if region:
        conditions.append("location_region = %s")
        params.append(region)
    if crime_type:
        conditions.append("crime_type = %s")
        params.append(crime_type)
    if date_from:
        conditions.append("reported_date >= %s")
        params.append(date_from)
    if date_to:
        conditions.append("reported_date <= %s")
        params.append(date_to)

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    sql   = f"""
        SELECT id, article_url, article_title, article_domain,
               reported_date, incident_date,
               location_country, location_region, crime_type,
               victim_count, victim_nationality, perpetrator_nationality,
               summary, confidence
        FROM incidents
        {where}
        ORDER BY reported_date DESC NULLS LAST
        LIMIT %s
    """
    params.append(limit)

    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            try:
                cur.execute(sql, params)
            except psycopg2.Error as e:
                raise HTTPException(status_code=500, detail=str(e))
            rows = _fetch(cur)

    return _ok(rows, len(rows))


def _aggregate(group_col: str, region: str | None, crime_type: str | None,
               date_from: date | None, date_to: date | None) -> JSONResponse:
    conditions: list[str] = [f"{group_col} IS NOT NULL"]
    params: list[Any]     = []

    if region:
        conditions.append("location_region = %s")
        params.append(region)
    if crime_type:
        conditions.append("crime_type = %s")
        params.append(crime_type)
    if date_from:
        conditions.append("reported_date >= %s")
        params.append(date_from)
    if date_to:
        conditions.append("reported_date <= %s")
        params.append(date_to)

    sql = f"""
        SELECT {group_col}, COUNT(*) AS incident_count
        FROM incidents
        WHERE {" AND ".join(conditions)}
        GROUP BY {group_col}
        ORDER BY incident_count DESC
    """

    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            try:
                cur.execute(sql, params)
            except psycopg2.Error as e:
                raise HTTPException(status_code=500, detail=str(e))
            rows = _fetch(cur)

    return _ok(rows, len(rows))


@app.get("/incidents/aggregate/country")
def aggregate_country(
    region:     str | None  = Query(None),
    crime_type: str | None  = Query(None),
    date_from:  date | None = Query(None),
    date_to:    date | None = Query(None),
):
    return _aggregate("location_country", region, crime_type, date_from, date_to)


@app.get("/incidents/aggregate/region")
def aggregate_region(
    crime_type: str | None  = Query(None),
    date_from:  date | None = Query(None),
    date_to:    date | None = Query(None),
):
    return _aggregate("location_region", None, crime_type, date_from, date_to)


@app.get("/incidents/aggregate/crime_type")
def aggregate_crime_type(
    region:     str | None  = Query(None),
    date_from:  date | None = Query(None),
    date_to:    date | None = Query(None),
):
    return _aggregate("crime_type", region, None, date_from, date_to)
