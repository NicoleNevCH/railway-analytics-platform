"""
Spark Job: Bronze -> Silver (Apache Iceberg table).

Hygienization and analytical-load pipeline:

  1. Reads the raw events (JSON) from the Bronze layer in MinIO.
  2. TEMPORAL LOGIC GUARD: separates coherent events from incoherent ones
     (e.g. actual arrival before actual departure). The incoherent ones go to
     Spark's rejected area (errors bucket) with TEMPORAL_LOGIC_VIOLATION —
     without crashing the job.
  3. OFFICIAL DELAY RULE: computes the delay (actual arrival - scheduled
     arrival). > 5 minutes => is_delayed = true (DELAYED); otherwise ON_TIME.
     This shields the refund statistics.
  4. SINGLE-RECORD GUARANTEE: deduplicates the multiple updates of the SAME
     trip (same trip_id), keeping the most recent (highest event_timestamp).
  5. UPSERT (MERGE INTO) into the Iceberg table by trip_id: updates the
     existing row or inserts a new one — never duplicates the analytical history.

Submission (from the spark-master container):
    spark-submit /opt/bitnami/spark/jobs/bronze_to_silver.py
"""

from __future__ import annotations

import os
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# ----------------------------------------------------------------------------
# Configuration read from the environment (injected by docker-compose).
# ----------------------------------------------------------------------------
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
S3_REGION = os.getenv("S3_REGION", "us-east-1")

BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "bronze")
ERRORS_BUCKET = os.getenv("ERRORS_BUCKET", "errors")
# IMPORTANT: warehouse with the s3:// scheme (Iceberg S3FileIO). Metadata
# written with s3:// is read natively by DuckDB. Reading the raw Bronze, in
# turn, uses s3a:// (Hadoop S3AFileSystem). The two coexist in the same session.
WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "s3://silver/warehouse")

CATALOG = os.getenv("ICEBERG_CATALOG", "lakehouse")
NAMESPACE = os.getenv("ICEBERG_NAMESPACE", "railway")
TABLE = os.getenv("ICEBERG_TABLE", "train_events")
FQ_TABLE = f"{CATALOG}.{NAMESPACE}.{TABLE}"

DELAY_THRESHOLD_MIN = int(os.getenv("DELAY_THRESHOLD_MIN", "5"))


# ----------------------------------------------------------------------------
# Spark session with Iceberg + S3A pointing at MinIO.
# ----------------------------------------------------------------------------
def build_spark() -> SparkSession:
    """
    Configures TWO accesses to MinIO that coexist in the same session:

      * Iceberg via S3FileIO (``s3://`` scheme)  -> writes the table's
        metadata/data with ``s3://`` paths, which DuckDB reads natively in the
        consumption layer. A 'hadoop'-type catalog (metadata in the bucket
        itself), self-contained and simple. For multi-writer production, swap it
        for a REST/Nessie/Glue catalog.
      * Hadoop S3AFileSystem (``s3a://`` scheme) -> used by ``spark.read.json``
        to read the raw Bronze and by ``write.json`` for the rejected area.

    The jars (iceberg-spark-runtime, iceberg-aws-bundle, hadoop-aws and
    aws-java-sdk-bundle) are baked into the image (see spark/Dockerfile).
    """
    builder = (
        SparkSession.builder.appName("bronze_to_silver_iceberg")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        # --- Iceberg catalog (S3FileIO, s3:// scheme) ---
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", WAREHOUSE)
        .config(f"spark.sql.catalog.{CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{CATALOG}.s3.endpoint", S3_ENDPOINT)
        .config(f"spark.sql.catalog.{CATALOG}.s3.path-style-access", "true")
        .config(f"spark.sql.catalog.{CATALOG}.s3.access-key-id", S3_ACCESS_KEY)
        .config(f"spark.sql.catalog.{CATALOG}.s3.secret-access-key", S3_SECRET_KEY)
        .config(f"spark.sql.catalog.{CATALOG}.client.region", S3_REGION)
        # --- Hadoop S3A -> MinIO (s3a:// scheme, to read raw Bronze) ---
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.session.timeZone", "UTC")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# Explicit Bronze schema (we do not trust JSON schema inference).
BRONZE_SCHEMA = T.StructType(
    [
        T.StructField("trip_id", T.StringType(), False),
        T.StructField("current_station_id", T.StringType(), False),
        T.StructField("event_timestamp", T.StringType(), True),
        T.StructField("event_id", T.StringType(), True),
        T.StructField("train_number", T.StringType(), True),
        T.StructField("operator", T.StringType(), True),
        T.StructField("origin_station_id", T.StringType(), True),
        T.StructField("destination_station_id", T.StringType(), True),
        T.StructField("current_station_name", T.StringType(), True),
        T.StructField("scheduled_departure", T.StringType(), True),
        T.StructField("actual_departure", T.StringType(), True),
        T.StructField("scheduled_arrival", T.StringType(), True),
        T.StructField("actual_arrival", T.StringType(), True),
        T.StructField("passengers_count", T.LongType(), True),
        T.StructField("platform", T.StringType(), True),
    ]
)

TS_COLS = [
    "event_timestamp",
    "scheduled_departure",
    "actual_departure",
    "scheduled_arrival",
    "actual_arrival",
]


def read_bronze(spark: SparkSession) -> DataFrame:
    """Read all JSONs from Bronze (one object per file)."""
    path = f"s3a://{BRONZE_BUCKET}/train_events/*/*/*.json"
    df = (
        spark.read.option("multiLine", "true")
        .schema(BRONZE_SCHEMA)
        .json(path)
    )
    # Convert ISO-8601 strings to real timestamps.
    for col in TS_COLS:
        df = df.withColumn(col, F.to_timestamp(F.col(col)))
    return df


def split_temporal(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    TEMPORAL LOGIC GUARD.

    Coherent when, given both timestamps, the arrival is not earlier than the
    departure (both actual and scheduled). Otherwise, it is rejected.
    """
    bad_actual = (
        F.col("actual_departure").isNotNull()
        & F.col("actual_arrival").isNotNull()
        & (F.col("actual_arrival") < F.col("actual_departure"))
    )
    bad_sched = (
        F.col("scheduled_departure").isNotNull()
        & F.col("scheduled_arrival").isNotNull()
        & (F.col("scheduled_arrival") < F.col("scheduled_departure"))
    )
    is_bad = bad_actual | bad_sched

    rejected = df.filter(is_bad).withColumn(
        "reason_code", F.lit("TEMPORAL_LOGIC_VIOLATION")
    ).withColumn(
        "reason_human",
        F.when(bad_actual, F.lit("actual_arrival earlier than actual_departure"))
        .when(bad_sched, F.lit("scheduled_arrival earlier than scheduled_departure"))
        .otherwise(F.lit("temporal incoherence")),
    )
    valid = df.filter(~is_bad)
    return valid, rejected


def apply_business_rules(df: DataFrame) -> DataFrame:
    """
    OFFICIAL DELAY RULE + enrichment.

    delay_minutes:
        - if scheduled and actual arrival exist: (actual - scheduled) in minutes;
        - otherwise, falls back to the departure delay; otherwise, null.
    is_delayed = delay_minutes > DELAY_THRESHOLD_MIN  (default 5).
    """
    arr_delay = (
        F.col("actual_arrival").cast("long") - F.col("scheduled_arrival").cast("long")
    ) / 60.0
    dep_delay = (
        F.col("actual_departure").cast("long") - F.col("scheduled_departure").cast("long")
    ) / 60.0

    df = df.withColumn(
        "delay_minutes",
        F.when(
            F.col("actual_arrival").isNotNull() & F.col("scheduled_arrival").isNotNull(),
            F.round(arr_delay, 2),
        ).when(
            F.col("actual_departure").isNotNull() & F.col("scheduled_departure").isNotNull(),
            F.round(dep_delay, 2),
        ).otherwise(F.lit(None).cast("double")),
    )

    df = df.withColumn(
        "is_delayed",
        F.when(F.col("delay_minutes").isNull(), F.lit(None).cast("boolean")).otherwise(
            F.col("delay_minutes") > F.lit(DELAY_THRESHOLD_MIN)
        ),
    )
    df = df.withColumn(
        "delay_status",
        F.when(F.col("delay_minutes").isNull(), F.lit("UNKNOWN"))
        .when(F.col("is_delayed"), F.lit("DELAYED"))
        .otherwise(F.lit("ON_TIME")),
    )

    # Analytical partition column and processing metadata.
    df = df.withColumn("trip_date", F.to_date("event_timestamp"))
    df = df.withColumn("processed_at", F.current_timestamp())
    return df


def deduplicate(df: DataFrame) -> DataFrame:
    """
    SINGLE-RECORD GUARANTEE (within the batch).

    Keeps only the most recent update per trip_id, ordering by event_timestamp
    desc (tie-break by processed_at). That way, ten updates of the same trip
    become ONE row before the MERGE.
    """
    w = Window.partitionBy("trip_id").orderBy(
        F.col("event_timestamp").desc_nulls_last(), F.col("processed_at").desc()
    )
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def ensure_table(spark: SparkSession) -> None:
    """Create the namespace and the Iceberg table (idempotent)."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {FQ_TABLE} (
            trip_id               STRING,
            train_number          STRING,
            operator              STRING,
            origin_station_id     STRING,
            destination_station_id STRING,
            current_station_id    STRING,
            current_station_name  STRING,
            scheduled_departure   TIMESTAMP,
            actual_departure      TIMESTAMP,
            scheduled_arrival     TIMESTAMP,
            actual_arrival        TIMESTAMP,
            event_timestamp       TIMESTAMP,
            passengers_count      BIGINT,
            platform              STRING,
            delay_minutes         DOUBLE,
            is_delayed            BOOLEAN,
            delay_status          STRING,
            trip_date             DATE,
            processed_at          TIMESTAMP,
            event_id              STRING
        )
        USING iceberg
        PARTITIONED BY (trip_date)
        TBLPROPERTIES (
            'format-version' = '2',
            'write.update.mode' = 'copy-on-write',
            'write.merge.mode'  = 'copy-on-write',
            'write.delete.mode' = 'copy-on-write'
        )
        """
    )


SELECT_COLS = [
    "trip_id", "train_number", "operator", "origin_station_id",
    "destination_station_id", "current_station_id", "current_station_name",
    "scheduled_departure", "actual_departure", "scheduled_arrival",
    "actual_arrival", "event_timestamp", "passengers_count", "platform",
    "delay_minutes", "is_delayed", "delay_status", "trip_date",
    "processed_at", "event_id",
]


def upsert(spark: SparkSession, df: DataFrame) -> None:
    """
    UPSERT via MERGE INTO by trip_id (the trip's business key).

    On match: updates the row (the new one is, by construction, the most
    recent). On no match: inserts. Result: a single row per trip in the history.
    """
    src = df.select(*SELECT_COLS)
    src.createOrReplaceTempView("staged_updates")

    set_clause = ",\n            ".join(f"t.{c} = s.{c}" for c in SELECT_COLS)
    insert_cols = ", ".join(SELECT_COLS)
    insert_vals = ", ".join(f"s.{c}" for c in SELECT_COLS)

    spark.sql(
        f"""
        MERGE INTO {FQ_TABLE} t
        USING staged_updates s
        ON t.trip_id = s.trip_id
        WHEN MATCHED THEN UPDATE SET
            {set_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_cols})
        VALUES ({insert_vals})
        """
    )


def write_rejects(df: DataFrame) -> int:
    """Persist the temporal rejects as JSON in Spark's error area."""
    count = df.count()
    if count == 0:
        return 0
    out = f"s3a://{ERRORS_BUCKET}/temporal_rejected"
    # convert timestamps back to string for a human-readable audit JSON
    audit = df
    for col in TS_COLS:
        audit = audit.withColumn(col, F.col(col).cast("string"))
    audit.write.mode("append").json(out)
    return count


def main() -> int:
    spark = build_spark()
    print(f">> Reading Bronze from s3a://{BRONZE_BUCKET}/ ...")

    try:
        bronze = read_bronze(spark)
    except Exception as exc:  # noqa: BLE001
        print(f"!! No data in Bronze or read error: {exc}")
        spark.stop()
        return 0

    total = bronze.count()
    print(f">> Raw events read: {total}")
    if total == 0:
        print(">> Nothing to process. Exiting.")
        spark.stop()
        return 0

    valid, rejected = split_temporal(bronze)
    n_rejected = write_rejects(rejected)
    print(f">> Rejected by temporal logic (-> Spark DLQ): {n_rejected}")

    enriched = apply_business_rules(valid)
    deduped = deduplicate(enriched)
    print(f">> After dedup by trip_id: {deduped.count()} unique trips in the batch")

    ensure_table(spark)
    upsert(spark, deduped)

    final_count = spark.table(FQ_TABLE).count()
    delayed = spark.table(FQ_TABLE).filter(F.col("delay_status") == "DELAYED").count()
    print(">> ================= LOAD COMPLETE =================")
    print(f">> Rows in the Iceberg table {FQ_TABLE}: {final_count}")
    print(f">> Trips marked as DELAYED: {delayed}")
    print(">> =================================================")

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
