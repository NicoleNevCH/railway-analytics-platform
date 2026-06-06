"""
The Chaos Engine — Local Railway Telemetry Simulator.

Generates plausible trips across the Austrian network and sends them to the
Ingestion API. To stress the pipeline, it injects INTENTIONAL defects:

  * Null Injection   : erases the ``current_station_id`` of some events to check
                       that the DLQ isolates the error without crashing Spark.
  * Temporal Mess    : inverts the timestamps (arrival before departure) to test
                       the pipeline's logic guards.
  * Trip duplication : resends the SAME trip several times (repeated telemetry)
                       to validate the Iceberg UPSERT by trip_id.

Usage:
    python -m simulator.chaos_simulator --trips 50 --updates-per-trip 3 \
        --null-rate 0.12 --time-mess-rate 0.10
"""

from __future__ import annotations

import argparse
import logging
import random
import uuid
from datetime import datetime, timedelta, timezone

import httpx

from .stations import AUSTRIAN_STATIONS, Station

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s :: %(message)s")
logger = logging.getLogger("chaos")

# Austria timezone (CEST). Kept fixed for the simulator's simplicity.
TZ = timezone(timedelta(hours=2))

TRAIN_PREFIXES = ["RJX", "RJ", "IC", "EC", "WB", "REX", "CJX", "NJ"]


def _random_train_number() -> str:
    return f"{random.choice(TRAIN_PREFIXES)} {random.randint(100, 999)}"


def _pick_od() -> tuple[Station, Station]:
    """Draw a distinct origin and destination."""
    origin, dest = random.sample(AUSTRIAN_STATIONS, 2)
    return origin, dest


def build_trip_events(
    *,
    updates_per_trip: int,
    null_rate: float,
    time_mess_rate: float,
    delayed_rate: float,
) -> list[dict]:
    """
    Build N events for ONE trip (repeated telemetry of the same trip_id).

    The events share trip_id; each resend has a more recent ``event_timestamp``,
    simulating status updates. Spark must consolidate everything into ONE row
    (the most recent) via UPSERT.
    """
    origin, dest = _pick_od()
    trip_id = f"OEBB-{datetime.now(TZ):%Y%m%d}-{uuid.uuid4().hex[:8]}"
    train_number = _random_train_number()

    base_dep = datetime.now(TZ).replace(microsecond=0) - timedelta(
        minutes=random.randint(0, 180)
    )
    planned_duration = timedelta(minutes=random.randint(40, 240))
    scheduled_departure = base_dep
    scheduled_arrival = base_dep + planned_duration

    # Real delay of this trip (most are on time; some are genuinely delayed).
    if random.random() < delayed_rate:
        dep_delay = random.randint(6, 35)
        arr_delay = dep_delay + random.randint(-2, 20)
    else:
        dep_delay = random.randint(0, 4)
        arr_delay = random.randint(0, 5)

    actual_departure = scheduled_departure + timedelta(minutes=dep_delay)
    actual_arrival = scheduled_arrival + timedelta(minutes=max(arr_delay, 0))

    events: list[dict] = []
    for i in range(updates_per_trip):
        # The event's "current" station walks origin -> destination.
        current = origin if i == 0 else (dest if i == updates_per_trip - 1 else random.choice([origin, dest]))
        event = {
            "trip_id": trip_id,
            "event_id": f"evt-{uuid.uuid4().hex[:10]}",
            "train_number": train_number,
            "operator": "ÖBB",
            "origin_station_id": origin.eva_id,
            "destination_station_id": dest.eva_id,
            "current_station_id": current.eva_id,
            "current_station_name": current.name,
            "scheduled_departure": scheduled_departure.isoformat(),
            "actual_departure": actual_departure.isoformat(),
            "scheduled_arrival": scheduled_arrival.isoformat(),
            "actual_arrival": actual_arrival.isoformat(),
            # each update is slightly "newer"
            "event_timestamp": (actual_arrival + timedelta(seconds=i)).isoformat(),
            "passengers_count": random.randint(20, 600),
            "platform": f"{random.randint(1, 12)}{random.choice(['', 'A', 'B'])}",
        }

        # --- CHAOS 1: Null Injection -------------------------------------
        # Erase the station identifier -> must land in the DLQ (schema).
        if random.random() < null_rate:
            event["current_station_id"] = None
            event["_chaos"] = "NULL_STATION_ID"

        # --- CHAOS 2: Temporal Mess --------------------------------------
        # Swap actual arrival and departure -> the logic guard must block it.
        if random.random() < time_mess_rate:
            event["actual_departure"], event["actual_arrival"] = (
                event["actual_arrival"],
                event["actual_departure"],
            )
            event["_chaos"] = "TIME_INVERSION"

        events.append(event)

    return events


def run(
    *,
    api_url: str,
    trips: int,
    updates_per_trip: int,
    null_rate: float,
    time_mess_rate: float,
    delayed_rate: float,
    batch_size: int,
) -> None:
    all_events: list[dict] = []
    for _ in range(trips):
        all_events.extend(
            build_trip_events(
                updates_per_trip=updates_per_trip,
                null_rate=null_rate,
                time_mess_rate=time_mess_rate,
                delayed_rate=delayed_rate,
            )
        )

    random.shuffle(all_events)  # shuffle the telemetry arrival order
    logger.info(
        "generated %d events from %d trips (updates/trip=%d)",
        len(all_events), trips, updates_per_trip,
    )

    sent = accepted = rejected = 0
    with httpx.Client(timeout=30.0) as client:
        for start in range(0, len(all_events), batch_size):
            chunk = all_events[start : start + batch_size]
            # strip the internal chaos marker before sending (not part of the contract)
            wire = [{k: v for k, v in e.items() if k != "_chaos"} for e in chunk]
            resp = client.post(f"{api_url.rstrip('/')}/ingest", json=wire)
            resp.raise_for_status()
            body = resp.json()
            sent += body["received"]
            accepted += body["accepted"]
            rejected += body["rejected"]
            logger.info(
                "batch sent: +%d (accepted=%d, rejected=%d)",
                body["received"], body["accepted"], body["rejected"],
            )

    logger.info("================ CHAOS SUMMARY ================")
    logger.info("sent=%d | accepted=%d | rejected(DLQ)=%d", sent, accepted, rejected)
    logger.info("==============================================")


def main() -> None:
    p = argparse.ArgumentParser(description="Chaos Engine — ÖBB railway simulator")
    p.add_argument("--api-url", default="http://localhost:8000", help="ingestion API URL")
    p.add_argument("--trips", type=int, default=50, help="number of distinct trips")
    p.add_argument("--updates-per-trip", type=int, default=3, help="resends per trip (tests UPSERT)")
    p.add_argument("--null-rate", type=float, default=0.12, help="prob. of erasing station_id")
    p.add_argument("--time-mess-rate", type=float, default=0.10, help="prob. of inverting timestamps")
    p.add_argument("--delayed-rate", type=float, default=0.30, help="prob. the trip is genuinely delayed")
    p.add_argument("--batch-size", type=int, default=25, help="events per request")
    p.add_argument("--seed", type=int, default=None, help="seed for reproducibility")
    args = p.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    run(
        api_url=args.api_url,
        trips=args.trips,
        updates_per_trip=args.updates_per_trip,
        null_rate=args.null_rate,
        time_mess_rate=args.time_mess_rate,
        delayed_rate=args.delayed_rate,
        batch_size=args.batch_size,
    )


if __name__ == "__main__":
    main()
