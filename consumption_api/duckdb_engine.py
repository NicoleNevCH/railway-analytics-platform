"""
DuckDB query engine over the Apache Iceberg table (Gold/serving layer).

DuckDB reads the Iceberg files DIRECTLY from MinIO (S3) at very high speed,
without needing a Spark cluster in the query path. Strategy:

  1. Configures DuckDB's httpfs pointing at MinIO (s3:// scheme).
  2. Locates the most recent metadata.json of the table (listing the metadata/
     prefix via boto3 and picking the highest version) — robust and independent
     of the presence of version-hint.text.
  3. Uses ``iceberg_scan`` over that metadata to materialize a view and answers
     the analytical queries.

The Iceberg table was written in copy-on-write mode, so the current data files
are complete Parquet files — a clean read for DuckDB.
"""

from __future__ import annotations

import logging
import re
import threading

import boto3
import duckdb
from botocore.client import Config as BotoConfig

from .config import settings

logger = logging.getLogger("consumption.duckdb")

_METADATA_RE = re.compile(r"v?(\d+)[^/]*\.metadata\.json$")


class IcebergQueryEngine:
    """Reusable DuckDB connection with the ``train_events`` view registered."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._con: duckdb.DuckDBPyConnection | None = None
        self._view_ready = False

    # -- bootstrap ----------------------------------------------------------
    def _connect(self) -> duckdb.DuckDBPyConnection:
        con = duckdb.connect(database=":memory:")
        con.execute("INSTALL httpfs;  LOAD httpfs;")
        con.execute("INSTALL iceberg; LOAD iceberg;")
        # Endpoint without the http:// scheme for DuckDB.
        endpoint = settings.s3_endpoint.replace("http://", "").replace("https://", "")
        con.execute(f"SET s3_endpoint='{endpoint}';")
        con.execute("SET s3_url_style='path';")
        con.execute(f"SET s3_use_ssl={'true' if settings.s3_endpoint.startswith('https') else 'false'};")
        con.execute(f"SET s3_region='{settings.s3_region}';")
        con.execute(f"SET s3_access_key_id='{settings.s3_access_key}';")
        con.execute(f"SET s3_secret_access_key='{settings.s3_secret_key}';")
        # Recent DuckDB versions require enabling version detection via hint or metadata.
        try:
            con.execute("SET unsafe_enable_version_guessing=true;")
        except duckdb.Error:
            pass
        return con

    def _latest_metadata_uri(self) -> str:
        """Find the most recent metadata.json of the table in MinIO."""
        s3 = boto3.client(
            "s3",
            endpoint_url=settings.s3_endpoint,
            aws_access_key_id=settings.s3_access_key,
            aws_secret_access_key=settings.s3_secret_key,
            region_name=settings.s3_region,
            config=BotoConfig(signature_version="s3v4", s3={"addressing_style": "path"}),
        )
        prefix = f"{settings.warehouse_prefix}/{settings.namespace}/{settings.table}/metadata/"
        resp = s3.list_objects_v2(Bucket=settings.silver_bucket, Prefix=prefix)
        contents = resp.get("Contents", [])
        candidates: list[tuple[int, str]] = []
        for obj in contents:
            key = obj["Key"]
            m = _METADATA_RE.search(key)
            if m:
                candidates.append((int(m.group(1)), key))
        if not candidates:
            raise FileNotFoundError(
                f"no metadata.json found in s3://{settings.silver_bucket}/{prefix} "
                "(run the Spark pipeline first)"
            )
        candidates.sort()
        best_key = candidates[-1][1]
        return f"s3://{settings.silver_bucket}/{best_key}"

    def _ensure_view(self, refresh: bool = False) -> None:
        if self._con is None:
            self._con = self._connect()
        if self._view_ready and not refresh:
            return
        metadata_uri = self._latest_metadata_uri()
        logger.info("registering view over metadata: %s", metadata_uri)
        self._con.execute(
            f"""
            CREATE OR REPLACE VIEW train_events AS
            SELECT * FROM iceberg_scan('{metadata_uri}');
            """
        )
        self._view_ready = True

    # -- public API ---------------------------------------------------------
    def query(self, sql: str, refresh: bool = False) -> list[dict]:
        """Run arbitrary SQL against the ``train_events`` view."""
        with self._lock:
            self._ensure_view(refresh=refresh)
            assert self._con is not None
            rel = self._con.execute(sql)
            cols = [d[0] for d in rel.description]
            return [dict(zip(cols, row)) for row in rel.fetchall()]

    def refresh(self) -> None:
        with self._lock:
            self._view_ready = False
            self._ensure_view(refresh=True)
