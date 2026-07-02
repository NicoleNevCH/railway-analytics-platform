"""
Guardrail tests for the text-to-SQL module (consumption_api/nl2sql.py).

These run offline — no API key or network needed — and protect the safety
contract: only a single read-only SELECT ever reaches DuckDB.
"""

from __future__ import annotations

import pytest

from consumption_api.nl2sql import validate_sql


def test_limit_appended_when_missing():
    sql = validate_sql("SELECT count(*) FROM train_events")
    assert sql.endswith("LIMIT 200")


def test_existing_limit_respected():
    sql = validate_sql("select * from train_events limit 10")
    assert "LIMIT 200" not in sql


def test_fences_and_trailing_semicolon_stripped():
    sql = validate_sql("```sql\nSELECT count(*) FROM train_events;\n```")
    assert sql.startswith("SELECT")
    assert ";" not in sql


def test_with_cte_accepted():
    validate_sql("WITH d AS (SELECT * FROM train_events) SELECT count(*) FROM d")


def test_offset_does_not_trip_set_keyword():
    validate_sql("SELECT * FROM train_events LIMIT 10 OFFSET 5")


@pytest.mark.parametrize(
    "bad_sql",
    [
        "DROP TABLE train_events",
        "SELECT 1; SELECT 2",
        "INSERT INTO train_events VALUES (1)",
        "UPDATE train_events SET delay_minutes = 0",
        "SELECT * FROM train_events; DROP TABLE x",
        "INSTALL httpfs",
        "PRAGMA database_list",
        "ATTACH 'x.db'",
        "COPY train_events TO 'out.csv'",
        "CREATE TABLE x AS SELECT 1",
        "SELECT set_config('memory_limit', '1TB')",
        "SELECT * FROM read_csv('/etc/passwd')",
        "SELECT glob('/*')",
        "SELECT getenv('OPENAI_API_KEY')",
        "",
    ],
)
def test_dangerous_sql_is_blocked(bad_sql):
    with pytest.raises(ValueError):
        validate_sql(bad_sql)
