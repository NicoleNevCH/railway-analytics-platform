"""Centralized configuration read from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    # --- MinIO connection (S3-compatible) ---
    s3_endpoint: str = os.getenv("S3_ENDPOINT", "http://minio:9000")
    s3_access_key: str = os.getenv("S3_ACCESS_KEY", "minioadmin")
    s3_secret_key: str = os.getenv("S3_SECRET_KEY", "minioadmin")
    s3_region: str = os.getenv("S3_REGION", "us-east-1")

    # --- Buckets / layers ---
    bronze_bucket: str = os.getenv("BRONZE_BUCKET", "bronze")
    errors_bucket: str = os.getenv("ERRORS_BUCKET", "errors")

    # Logical prefixes inside the buckets
    bronze_prefix: str = os.getenv("BRONZE_PREFIX", "train_events")
    errors_prefix: str = os.getenv("ERRORS_PREFIX", "schema_rejected")

    api_title: str = "Railway Analytics — Ingestion API"
    api_version: str = "1.0.0"


settings = Settings()
