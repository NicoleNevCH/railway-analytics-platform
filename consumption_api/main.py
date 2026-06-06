"""
Consumption API (FastAPI) — analytical serving layer.

Exposes REST endpoints that, under the hood, fire SQL on DuckDB directly over
the Iceberg table in MinIO. It is the "Gold layer" served on demand: fast, with
no Spark in the request path.

Endpoints:
  GET /health
  GET /stats/punctuality           -> punctuality KPIs (refund shielding)
  GET /trips/delayed?min_delay=10  -> trips delayed above a threshold
  GET /stats/by-station            -> average delay per current station
  GET /stats/by-operator           -> volume and punctuality per operator
  GET /trips/{trip_id}             -> the single consolidated row of a trip
  POST /refresh                    -> reloads the Iceberg metadata pointer
"""

from __future__ import annotations

import logging

from fastapi import FastAPI, HTTPException, Query

from .config import settings
from .duckdb_engine import IcebergQueryEngine

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s :: %(message)s")
logger = logging.getLogger("consumption.api")

app = FastAPI(title=settings.api_title, version=settings.api_version)
engine = IcebergQueryEngine()


def _safe_query(sql: str, refresh: bool = False):
    try:
        return engine.query(sql, refresh=refresh)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("query error")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "service": "consumption"}


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
    rows = _safe_query(
        f"SELECT * FROM train_events WHERE trip_id = '{safe}'"
    )
    if not rows:
        raise HTTPException(status_code=404, detail="trip not found")
    if len(rows) > 1:  # should never happen — dedup guarantees 1 row
        logger.error("INTEGRITY: %d rows for trip_id=%s", len(rows), trip_id)
    return rows[0]


@app.post("/refresh")
def refresh():
    """Reload the Iceberg metadata pointer (after a new Spark load)."""
    engine.refresh()
    return {"status": "refreshed"}
