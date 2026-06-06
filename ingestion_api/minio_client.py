"""
Thin layer over boto3 to talk to MinIO.

MinIO exposes the S3 API, so we use the same boto3 client we would use against
AWS — only pointing ``endpoint_url`` at MinIO. That is what makes the project
portable to the cloud: swap the endpoint and credentials and you are done.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

import boto3
from botocore.client import Config as BotoConfig
from botocore.exceptions import ClientError

from .config import settings

logger = logging.getLogger("ingestion.s3")


class ObjectStore:
    """Wrapper for the S3 operations used by the ingestion API."""

    def __init__(self) -> None:
        self._client = boto3.client(
            "s3",
            endpoint_url=settings.s3_endpoint,
            aws_access_key_id=settings.s3_access_key,
            aws_secret_access_key=settings.s3_secret_key,
            region_name=settings.s3_region,
            # path-style is required on MinIO (no per-bucket virtual-host DNS)
            config=BotoConfig(signature_version="s3v4", s3={"addressing_style": "path"}),
        )

    # -- infrastructure -----------------------------------------------------
    def ensure_buckets(self, buckets: list[str]) -> None:
        """Create the buckets if they do not exist yet (idempotent)."""
        existing = {b["Name"] for b in self._client.list_buckets().get("Buckets", [])}
        for bucket in buckets:
            if bucket in existing:
                continue
            try:
                self._client.create_bucket(Bucket=bucket)
                logger.info("bucket created: %s", bucket)
            except ClientError as exc:  # pragma: no cover - creation race
                code = exc.response.get("Error", {}).get("Code", "")
                if code not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
                    raise

    # -- writing ------------------------------------------------------------
    def put_json(self, bucket: str, key: str, payload: dict[str, Any]) -> str:
        body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        self._client.put_object(
            Bucket=bucket, Key=key, Body=body, ContentType="application/json"
        )
        return f"s3://{bucket}/{key}"


def _now_parts() -> tuple[str, str]:
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%d"), now.strftime("%Y%m%dT%H%M%S%fZ")


def bronze_key(trip_id: str, event_id: str | None) -> str:
    """
    Path partitioned by ingestion date in the Bronze layer.

    E.g.: train_events/ingest_date=2024-09-21/trip_id=T-1/20240921T104105_evt.json
    """
    ingest_date, stamp = _now_parts()
    safe_trip = trip_id.replace("/", "_")
    suffix = (event_id or "noevt").replace("/", "_")
    return (
        f"{settings.bronze_prefix}/ingest_date={ingest_date}/"
        f"trip_id={safe_trip}/{stamp}_{suffix}.json"
    )


def dlq_key(reason: str) -> str:
    """Path in the Dead Letter Queue, partitioned by date and reason."""
    ingest_date, stamp = _now_parts()
    safe_reason = reason.replace("/", "_")
    return f"{settings.errors_prefix}/ingest_date={ingest_date}/reason={safe_reason}/{stamp}.json"
