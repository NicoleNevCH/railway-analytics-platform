"""
Consumption API (FastAPI) — analytical serving layer.

Exposes REST endpoints that, under the hood, fire SQL on DuckDB directly over
the Iceberg table in MinIO. It is the "Gold layer" served on demand: fast, with
no Spark in the request path.

Endpoints:
  GET  /health
  GET  /pipeline/status            -> Bronze/Silver/Errors counts (health board)
  GET  /stats/punctuality          -> punctuality KPIs (refund shielding)
  GET  /trips/delayed?min_delay=10 -> trips delayed above a threshold
  GET  /stats/by-station           -> average delay per current station
  GET  /stats/by-operator          -> volume and punctuality per operator
  GET  /trips/{trip_id}            -> the single consolidated row of a trip
  POST /refresh                    -> reloads the Iceberg metadata pointer
"""

from __future__ import annotations

import logging

import boto3
from botocore.client import Config as BotoConfig
from fastapi import FastAPI, HTTPException, Query

from .config import settings
from .duckdb_engine import IcebergQueryEngine

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s :: %(message)s")
logger = logging.getLogger("consumption.api")

app = FastAPI(title=settings.api_title, version=settings.api_version)
engine = IcebergQueryEngine()

# Friendly, actionable message when the Silver table has not been built yet.
_NOT_READY = {
    "error": "The Silver (Iceberg) table is not available yet.",
    "hint": "Send events to the ingestion API, then run the Spark job "
            "(`make process` / spark-submit), then POST /refresh and retry.",
}


def _safe_query(sql: str, refresh: bool = False):
    """Run SQL, turning the 'table not ready' case into a clear 503."""
    try:
        return engine.query(sql, refresh=refresh)
    except FileNotFoundError:
        raise HTTPException(status_code=503, detail=_NOT_READY)
    except Exception as exc:  # noqa: BLE001
        logger.exception("query error")
        raise HTTPException(
            status_code=500,
            detail={"error": "Query failed.", "reason": str(exc)},
        ) from exc


# ---------------------------------------------------------------------------
# Storage stats (boto3) — used by /pipeline/status to show what is where.
# ---------------------------------------------------------------------------
def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
        region_name=settings.s3_region,
        config=BotoConfig(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def _count_objects(client, bucket: str, prefix: str) -> int:
    """Count objects under a prefix (paginated). Missing bucket -> 0."""
    total = 0
    try:
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            total += page.get("KeyCount", 0)
    except Exception:  # noqa: BLE001 - bucket may not exist yet
        return 0
    return total


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "service": "consumption"}


@app.get("/pipeline/status")
def pipeline_status():
    """
    Health board across the layers — answers "is there data, and where?".

    Counts raw events in Bronze, rows in the Silver table, and rejects in each
    Dead Letter Queue. Never 500s: if a layer is empty it simply reports zero.
    """
    client = _s3_client()
    bronze_raw = _count_objects(client, settings.bronze_bucket, "train_events/")
    schema_rejected = _count_objects(client, settings.errors_bucket, "schema_rejected/")
    temporal_rejected = _count_objects(client, settings.errors_bucket, "temporal_rejected/")

    metadata_uri = engine.latest_metadata_uri()
    silver_rows = None
    if metadata_uri is not None:
        try:
            rows = engine.query("SELECT COUNT(*) AS n FROM train_events", refresh=True)
            silver_rows = rows[0]["n"] if rows else 0
        except Exception:  # noqa: BLE001
            silver_rows = None

    return {
        "bronze": {"raw_events": bronze_raw},
        "silver": {
            "ready": metadata_uri is not None,
            "rows": silver_rows,
            "latest_metadata": metadata_uri.split("/")[-1] if metadata_uri else None,
        },
        "errors": {
            "schema_rejected": schema_rejected,
            "temporal_rejected": temporal_rejected,
        },
    }


@app.get("/stats/punctuality")
def punctuality():
    """Global punctuality KPIs — the metric that shields refunds."""
    rows = _safe_query(
        """
        SELECT
            COUNT(*)                                            AS total_trips,
            COUNT(*) FILTER (WHERE delay_status = 'DELAYED')    AS delayed_trips,
            COUNT(*) FILTER (WHERE delay_status = 'ON_TIME')    AS on_time_trips,
            COUNT(*) FILTER (WHERE delay_status = 'UNKNOWN')    AS unknown_trips,
            ROUND(AVG(delay_minutes), 2)                        AS avg_delay_minutes,
            ROUND(MAX(delay_minutes), 2)                        AS max_delay_minutes,
            ROUND(100.0 * COUNT(*) FILTER (WHERE delay_status = 'ON_TIME')
                  / NULLIF(COUNT(*), 0), 2)                     AS on_time_pct
        FROM train_events
        """
    )
    return rows[0] if rows else {}


@app.get("/trips/delayed")
def delayed_trips(
    min_delay: float = Query(5.0, description="minimum delay in minutes"),
    limit: int = Query(50, ge=1, le=1000),
):
    """List trips delayed above a threshold, from highest to lowest."""
    return _safe_query(
        f"""
        SELECT trip_id, train_number, operator,
               current_station_name, delay_minutes, delay_status,
               scheduled_arrival, actual_arrival
        FROM train_events
        WHERE delay_minutes > {float(min_delay)}
        ORDER BY delay_minutes DESC
        LIMIT {int(limit)}
        """
    )


@app.get("/stats/by-station")
def by_station(limit: int = Query(20, ge=1, le=200)):
    """Average delay and count per the event's current station."""
    return _safe_query(
        f"""
        SELECT current_station_id,
               ANY_VALUE(current_station_name)                 AS station_name,
               COUNT(*)                                        AS trips,
               ROUND(AVG(delay_minutes), 2)                    AS avg_delay_minutes,
               COUNT(*) FILTER (WHERE delay_status='DELAYED')  AS delayed
        FROM train_events
        GROUP BY current_station_id
        ORDER BY avg_delay_minutes DESC NULLS LAST
        LIMIT {int(limit)}
        """
    )


@app.get("/stats/by-operator")
def by_operator():
    """Volume and punctuality per operator."""
    return _safe_query(
        """
        SELECT operator,
               COUNT(*)                                        AS trips,
               ROUND(AVG(delay_minutes), 2)                    AS avg_delay_minutes,
               ROUND(100.0 * COUNT(*) FILTER (WHERE delay_status='ON_TIME')
                     / NULLIF(COUNT(*), 0), 2)                 AS on_time_pct
        FROM train_events
        GROUP BY operator
        ORDER BY trips DESC
        """
    )


@app.get("/trips/{trip_id}")
def get_trip(trip_id: str):
    """Return the SINGLE consolidated row of a trip (proof of the UPSERT)."""
    safe = trip_id.replace("'", "''")
    rows = _safe_query(f"SELECT * FROM train_events WHERE trip_id = '{safe}'")
    if not rows:
        raise HTTPException(status_code=404, detail={"error": "trip not found", "trip_id": trip_id})
    if len(rows) > 1:  # should never happen — dedup guarantees 1 row
        logger.error("INTEGRITY: %d rows for trip_id=%s", len(rows), trip_id)
    return rows[0]


@app.post("/refresh")
def refresh():
    """Reload the Iceberg metadata pointer (after a new Spark load)."""
    try:
        engine.refresh()
    except FileNotFoundError:
        raise HTTPException(status_code=503, detail=_NOT_READY)
    return {"status": "refreshed"}
