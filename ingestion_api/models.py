"""
Data Contract of the platform.

This module defines the "contract at the gate": the minimum structure and the
types a train event MUST have to be accepted into the Bronze layer.

Two-layer defense strategy (each layer with a distinct responsibility):

  1. Pydantic (here, at the gate)
        -> validates STRUCTURE, TYPES and the presence of the mandatory
           IDENTIFIERS.
        -> cheap, fast and STATELESS.
        -> on failure, the raw record goes to the DLQ with
           SCHEMA_VALIDATION_FAILED.

  2. Spark (processing layer)
        -> validates BUSINESS/TEMPORAL LOGIC (e.g. arrival before departure).
        -> needs cross-field context/coherence, so it lives in the pipeline.
        -> on failure, the record goes to Spark's rejected area with
           TEMPORAL_LOGIC_VIOLATION.

This separation is intentional: the gate does NOT know temporal business rules.
A structurally perfect event with inverted timestamps PASSES here on purpose —
so that the pipeline's logic guard (Spark) catches it. That keeps both gates
active and auditable.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class DelayStatus(str, Enum):
    """Official punctuality marker."""

    ON_TIME = "ON_TIME"
    DELAYED = "DELAYED"
    UNKNOWN = "UNKNOWN"


class TrainEvent(BaseModel):
    """
    Operational event of a train trip.

    MANDATORY identifier fields (the "contract"):
      - trip_id              : the TRIP identifier (UPSERT key)
      - current_station_id   : the STATION identifier of the event
      - event_timestamp      : when the telemetry emitted the event

    If any of these is missing/null, Pydantic raises a ValidationError and the
    API pushes the raw record to the Dead Letter Queue.
    """

    # ---- Mandatory identifiers --------------------------------------------
    trip_id: str = Field(..., min_length=1, description="Unique trip identifier")
    current_station_id: str = Field(
        ..., min_length=1, description="EVA/ID of the station the event refers to"
    )
    event_timestamp: datetime = Field(..., description="Emission timestamp")

    # ---- Descriptive attributes -------------------------------------------
    event_id: Optional[str] = Field(default=None, description="ID of the individual event")
    train_number: Optional[str] = Field(default=None, description="Service number, e.g. RJX 568")
    operator: str = Field(default="ÖBB", description="Railway operator")

    origin_station_id: Optional[str] = None
    destination_station_id: Optional[str] = None
    current_station_name: Optional[str] = None

    # ---- Temporal window ---------------------------------------------------
    scheduled_departure: Optional[datetime] = None
    actual_departure: Optional[datetime] = None
    scheduled_arrival: Optional[datetime] = None
    actual_arrival: Optional[datetime] = None

    # ---- Operational metrics ----------------------------------------------
    passengers_count: Optional[int] = Field(default=None, ge=0)
    platform: Optional[str] = None

    # Explicitly reject ID strings that are only whitespace.
    @field_validator("trip_id", "current_station_id")
    @classmethod
    def _not_blank(cls, value: str) -> str:
        if value is None or not value.strip():
            raise ValueError("mandatory identifier is empty or blank")
        return value.strip()

    # NOTE: we do NOT validate temporal coherence here on purpose. Events with
    # inverted timestamps are structurally valid and must PASS to the Bronze
    # layer, where Spark's logic guard catches them (TEMPORAL_LOGIC_VIOLATION).

    model_config = {
        "json_schema_extra": {
            "example": {
                "trip_id": "OEBB-2024-09-21-RJX568-0001",
                "current_station_id": "8100173",
                "current_station_name": "Salzburg Hbf",
                "event_id": "evt-7f3a",
                "train_number": "RJX 568",
                "operator": "ÖBB",
                "origin_station_id": "8103000",
                "destination_station_id": "8100002",
                "scheduled_departure": "2024-09-21T08:00:00+02:00",
                "actual_departure": "2024-09-21T08:03:00+02:00",
                "scheduled_arrival": "2024-09-21T10:30:00+02:00",
                "actual_arrival": "2024-09-21T10:41:00+02:00",
                "event_timestamp": "2024-09-21T10:41:05+02:00",
                "passengers_count": 412,
                "platform": "3A",
            }
        }
    }


class IngestResponse(BaseModel):
    """Response of the ingestion API for a batch of events."""

    received: int
    accepted: int
    rejected: int
    bronze_keys: list[str] = Field(default_factory=list)
    dlq_keys: list[str] = Field(default_factory=list)
