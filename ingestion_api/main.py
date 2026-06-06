"""
Ingestion API (FastAPI).

SINGLE responsibility: be the gate of the lakehouse.
  - Receives train events (one or many).
  - Validates the CONTRACT with Pydantic.
  - Valid events    -> Bronze bucket (raw, no transformation).
  - Invalid events  -> Errors / DLQ bucket, with the original payload + reason.

This API does NOT transform data. Hygienization and business rules belong to
Spark. Keeping the gate "dumb" and fast is intentional: it only stores the raw
data and isolates the junk, ensuring nothing crashes the pipeline downstream.
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import Body, FastAPI
from pydantic import ValidationError

from .config import settings
from .minio_client import ObjectStore, bronze_key, dlq_key
from .models import IngestResponse, TrainEvent

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s :: %(message)s")
logger = logging.getLogger("ingestion.api")

app = FastAPI(title=settings.api_title, version=settings.api_version)
store = ObjectStore()


@app.on_event("startup")
def _startup() -> None:
    """Ensure the Bronze and Errors buckets exist at startup."""
    store.ensure_buckets([settings.bronze_bucket, settings.errors_bucket])
    logger.info("ingestion API ready — buckets ensured")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "service": "ingestion"}


def _route_one(raw: dict[str, Any], resp: IngestResponse) -> None:
    """Validate ONE raw event and route it to Bronze or the DLQ."""
    try:
        event = TrainEvent.model_validate(raw)
    except ValidationError as exc:
        # Contract violated: goes to the Dead Letter Queue with diagnostics.
        reasons = [e["msg"] for e in exc.errors()]
        envelope = {
            "rejected_at_stage": "ingestion_schema_validation",
            "reason_code": "SCHEMA_VALIDATION_FAILED",
            "errors": exc.errors(),
            "reasons_human": reasons,
            "original_payload": raw,
        }
        key = store.put_json(settings.errors_bucket, dlq_key("SCHEMA_VALIDATION_FAILED"), envelope)
        resp.rejected += 1
        resp.dlq_keys.append(key)
        logger.warning("event rejected -> DLQ (%s)", reasons)
        return

    # Valid: persist the normalized RAW JSON into Bronze.
    payload = event.model_dump(mode="json")
    key = store.put_json(
        settings.bronze_bucket,
        bronze_key(event.trip_id, event.event_id),
        payload,
    )
    resp.accepted += 1
    resp.bronze_keys.append(key)


@app.post("/ingest", response_model=IngestResponse)
def ingest(payload: Any = Body(...)) -> IngestResponse:
    """
    Accept a single event (object) or a batch (list of objects).

    Always responds 200 with the scoreboard (received/accepted/rejected). A
    contract rejection is NOT an HTTP error — it is expected business flow, and
    the record was preserved in the DLQ for auditing/reprocessing.
    """
    batch = payload if isinstance(payload, list) else [payload]
    resp = IngestResponse(received=len(batch), accepted=0, rejected=0)

    for raw in batch:
        if not isinstance(raw, dict):
            envelope = {
                "rejected_at_stage": "ingestion_schema_validation",
                "reason_code": "MALFORMED_PAYLOAD",
                "reasons_human": ["payload is not a JSON object"],
                "original_payload": raw,
            }
            key = store.put_json(settings.errors_bucket, dlq_key("MALFORMED_PAYLOAD"), envelope)
            resp.rejected += 1
            resp.dlq_keys.append(key)
            continue
        _route_one(raw, resp)

    logger.info(
        "batch processed: received=%d accepted=%d rejected=%d",
        resp.received, resp.accepted, resp.rejected,
    )
    return resp
