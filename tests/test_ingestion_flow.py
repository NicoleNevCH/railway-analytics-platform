"""
Integration tests for the Ingestion API using an in-memory ObjectStore
(fake S3). They prove the Bronze vs DLQ routing end-to-end without needing
MinIO running.

Run:  pytest -q   (or)   python -m tests.test_ingestion_flow
"""

from __future__ import annotations

from fastapi.testclient import TestClient


class FakeStore:
    """In-memory store that mimics the ObjectStore (Bronze + DLQ)."""

    def __init__(self) -> None:
        self.objects: dict[str, dict] = {}

    def ensure_buckets(self, buckets):  # noqa: D401
        return None

    def put_json(self, bucket: str, key: str, payload: dict) -> str:
        uri = f"s3://{bucket}/{key}"
        self.objects[uri] = payload
        return uri


def _make_client():
    import ingestion_api.main as m

    fake = FakeStore()
    m.store = fake  # inject the fake into the app
    return TestClient(m.app), fake


VALID = {
    "trip_id": "T-OK-1",
    "current_station_id": "8100173",
    "current_station_name": "Salzburg Hbf",
    "event_timestamp": "2024-09-21T10:41:05+02:00",
    "scheduled_arrival": "2024-09-21T10:30:00+02:00",
    "actual_arrival": "2024-09-21T10:41:00+02:00",
    "scheduled_departure": "2024-09-21T08:00:00+02:00",
    "actual_departure": "2024-09-21T08:03:00+02:00",
}


def test_valid_event_goes_to_bronze():
    client, fake = _make_client()
    r = client.post("/ingest", json=VALID)
    assert r.status_code == 200
    body = r.json()
    assert body == {
        "received": 1, "accepted": 1, "rejected": 0,
        "bronze_keys": body["bronze_keys"], "dlq_keys": [],
    }
    assert any("s3://bronze/" in k for k in fake.objects)
    assert not any("s3://errors/" in k for k in fake.objects)


def test_null_station_goes_to_dlq():
    client, fake = _make_client()
    bad = dict(VALID)
    bad["current_station_id"] = None  # Null Injection
    r = client.post("/ingest", json=bad)
    assert r.status_code == 200
    body = r.json()
    assert body["accepted"] == 0 and body["rejected"] == 1
    dlq = [v for k, v in fake.objects.items() if "s3://errors/" in k]
    assert dlq and dlq[0]["reason_code"] == "SCHEMA_VALIDATION_FAILED"


def test_time_inversion_passes_ingestion():
    """A temporal inversion is structurally valid -> PASSES to Bronze (Spark blocks it)."""
    client, fake = _make_client()
    mess = dict(VALID)
    mess["actual_arrival"] = "2024-09-21T07:00:00+02:00"  # before departure
    r = client.post("/ingest", json=mess)
    body = r.json()
    assert body["accepted"] == 1 and body["rejected"] == 0


def test_mixed_batch():
    client, fake = _make_client()
    batch = [
        VALID,
        {**VALID, "trip_id": "T-OK-2"},
        {**VALID, "current_station_id": "   "},   # blank -> DLQ
        {"trip_id": "no-station", "event_timestamp": "2024-09-21T10:41:05+02:00"},  # missing station -> DLQ
        "not-a-dict",                              # malformed -> DLQ
    ]
    r = client.post("/ingest", json=batch)
    body = r.json()
    assert body["received"] == 5
    assert body["accepted"] == 2
    assert body["rejected"] == 3


if __name__ == "__main__":
    test_valid_event_goes_to_bronze()
    test_null_station_goes_to_dlq()
    test_time_inversion_passes_ingestion()
    test_mixed_batch()
    print("All ingestion tests passed")
