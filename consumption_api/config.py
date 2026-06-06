"""Configuration of the consumption API (DuckDB over Iceberg)."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    s3_endpoint: str = os.getenv("S3_ENDPOINT", "http://minio:9000")
    s3_access_key: str = os.getenv("S3_ACCESS_KEY", "minioadmin")
    s3_secret_key: str = os.getenv("S3_SECRET_KEY", "minioadmin")
    s3_region: str = os.getenv("S3_REGION", "us-east-1")

    silver_bucket: str = os.getenv("SILVER_BUCKET", "silver")
    warehouse_prefix: str = os.getenv("ICEBERG_WAREHOUSE_PREFIX", "warehouse")
    namespace: str = os.getenv("ICEBERG_NAMESPACE", "railway")
    table: str = os.getenv("ICEBERG_TABLE", "train_events")

    api_title: str = "Railway Analytics — Consumption API"
    api_version: str = "1.0.0"


settings = Settings()
