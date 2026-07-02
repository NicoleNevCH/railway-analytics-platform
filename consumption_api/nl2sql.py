"""
Natural language -> SQL (text-to-SQL) over the ``train_events`` view.

Flow:
  question -> LLM (OpenAI API) writes ONE DuckDB SELECT
           -> guardrails validate it (read-only, single statement, LIMIT cap)
           -> the query runs on the same DuckDB engine as every other endpoint
           -> the caller receives the rows AND the generated SQL (auditable).

Safety model (defense in depth):
  * The LLM only returns TEXT — it has no tool or database access.
  * The generated SQL must pass ``validate_sql``: it must be a single
    SELECT/WITH statement, must not contain write/DDL/extension keywords, and
    gets a LIMIT appended when missing.
  * Every response includes the executed SQL, so the user can always audit it.
  * If no API key is configured the feature degrades gracefully (HTTP 503).
"""

from __future__ import annotations

import os
import re

import httpx

OPENAI_API_URL = os.getenv("OPENAI_API_URL", "https://api.openai.com/v1/chat/completions")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")


def api_key() -> str | None:
    """Read the key at call time so a container restart picks up .env changes."""
    key = os.getenv("OPENAI_API_KEY", "").strip()
    return key or None


# ---------------------------------------------------------------------------
# Schema shown to the model. Keep in sync with spark/jobs/bronze_to_silver.py.
# ---------------------------------------------------------------------------
SCHEMA_DOC = """
There is exactly ONE queryable view: train_events (one row per trip, latest state).

Columns:
  trip_id                 VARCHAR   -- unique trip identifier
  event_id                VARCHAR
  train_number            VARCHAR   -- e.g. 'RJX 568'
  operator                VARCHAR   -- e.g. 'ÖBB'
  origin_station_id       VARCHAR
  destination_station_id  VARCHAR
  current_station_id      VARCHAR
  current_station_name    VARCHAR   -- e.g. 'Salzburg Hbf'
  scheduled_departure     TIMESTAMP
  actual_departure        TIMESTAMP
  scheduled_arrival       TIMESTAMP
  actual_arrival          TIMESTAMP
  event_timestamp         TIMESTAMP
  passengers_count        INTEGER
  platform                VARCHAR
  delay_minutes           DOUBLE    -- actual vs scheduled arrival, in minutes
  is_delayed              BOOLEAN   -- true when delay_minutes > 5
  delay_status            VARCHAR   -- 'ON_TIME' | 'DELAYED' | 'UNKNOWN'
  trip_date               DATE
  processed_at            TIMESTAMP
"""

SYSTEM_PROMPT = f"""You translate questions about railway operations into DuckDB SQL.

{SCHEMA_DOC}

Rules — follow ALL of them:
1. Output ONLY the SQL. No explanations, no markdown fences, no comments.
2. Exactly ONE statement, and it must be a SELECT (a WITH ... SELECT is fine).
3. Query ONLY the train_events view. Never invent tables or columns.
4. Read-only: never use INSERT/UPDATE/DELETE/CREATE/DROP/ATTACH/COPY/INSTALL/LOAD/SET/PRAGMA.
5. Always include a LIMIT (at most 200) unless the query is a single-row aggregate.
6. Use DuckDB syntax (e.g. COUNT(*) FILTER (WHERE ...) is allowed).
7. If the question cannot be answered from this schema, output exactly: SELECT 'cannot answer from available data' AS error
"""

# Write/DDL/extension keywords that must never appear in the generated SQL.
_FORBIDDEN = re.compile(
    r"\b(insert|update|delete|drop|alter|create|replace|attach|detach|copy|export"
    r"|import|install|load|pragma|call|grant|revoke|vacuum|truncate|merge"
    r"|set|reset|begin|commit|rollback|use|checkpoint)\b",
    re.IGNORECASE,
)
_HAS_LIMIT = re.compile(r"\blimit\s+\d+\b", re.IGNORECASE)
# Dangerous function calls: settings mutation and container-filesystem access.
_FORBIDDEN_FUNCS = re.compile(r"\b(set_config|read_\w+|glob|getenv)\s*\(", re.IGNORECASE)
_MAX_LIMIT = 200


def strip_fences(text: str) -> str:
    """Remove ```sql fences and stray backticks the model might add."""
    text = text.strip()
    text = re.sub(r"^```(?:sql)?\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*```$", "", text)
    return text.strip()


def validate_sql(sql: str) -> str:
    """
    Enforce the guardrails. Returns the sanitized SQL or raises ValueError.

    Checks: non-empty; single statement (no ';' in the middle); starts with
    SELECT or WITH; no forbidden keywords; LIMIT present (appended if missing).
    """
    sql = strip_fences(sql).rstrip().rstrip(";").strip()
    if not sql:
        raise ValueError("the model returned empty SQL")
    if ";" in sql:
        raise ValueError("multiple statements are not allowed")

    head = sql.lstrip().split(None, 1)[0].lower()
    if head not in ("select", "with"):
        raise ValueError("only SELECT queries are allowed")

    if (m := _FORBIDDEN.search(sql)) is not None:
        raise ValueError(f"forbidden keyword in generated SQL: {m.group(0)!r}")

    if (m := _FORBIDDEN_FUNCS.search(sql)) is not None:
        raise ValueError(f"forbidden function in generated SQL: {m.group(1)!r}")

    if not _HAS_LIMIT.search(sql):
        sql = f"{sql}\nLIMIT {_MAX_LIMIT}"
    return sql


def generate_sql(question: str) -> str:
    """Call the OpenAI API and return the RAW model output (not yet validated)."""
    key = api_key()
    if key is None:
        raise RuntimeError("OPENAI_API_KEY is not configured")

    resp = httpx.post(
        OPENAI_API_URL,
        headers={
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
        },
        json={
            "model": OPENAI_MODEL,
            "max_tokens": 500,
            "temperature": 0,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": question},
            ],
        },
        timeout=60.0,
    )
    resp.raise_for_status()
    data = resp.json()
    return (data["choices"][0]["message"]["content"] or "").strip()


def ask_to_sql(question: str) -> str:
    """question -> validated, ready-to-run SQL (raises on any guardrail hit)."""
    return validate_sql(generate_sql(question))
