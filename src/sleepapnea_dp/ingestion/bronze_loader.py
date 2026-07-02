"""
Bronze layer ingestion: reads raw device export JSON-lines files from the ADLS Gen2 raw/
landing container using Databricks Auto Loader (cloudFiles), and writes append-only to a
Bronze Delta table — schema-enforced, with full lineage columns (source file, ingest time).

Auto Loader is used (rather than a plain batch read) because:
  - it incrementally + exactly-once processes new files as device vendors drop them,
  - it scales to millions of small files (typical for per-sync device exports),
  - schema drift can be tracked via the schema location instead of failing silently.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from sleepapnea_dp.schemas.device_session import DEVICE_SESSION_RAW_SCHEMA
from sleepapnea_dp.utils.spark_session import get_logger

logger = get_logger(__name__)


def read_raw_device_sessions_stream(
    spark: SparkSession,
    raw_path: str,
    schema_location: str,
) -> DataFrame:
    """Streaming read of raw JSON device exports via Auto Loader."""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_location)
        .option("cloudFiles.inferColumnTypes", "false")
        .schema(DEVICE_SESSION_RAW_SCHEMA)
        .load(raw_path)
    )


def read_raw_device_sessions_batch(spark: SparkSession, raw_path: str) -> DataFrame:
    """Batch read variant — used for backfills / local testing without a streaming checkpoint."""
    return spark.read.schema(DEVICE_SESSION_RAW_SCHEMA).json(raw_path)


def with_ingestion_metadata(df: DataFrame) -> DataFrame:
    """Adds lineage/audit columns required for HIPAA-grade traceability of every record."""
    return df.withColumn("_ingested_at_utc", F.current_timestamp()).withColumn(
        "_source_file", F.input_file_name()
    )


def write_bronze(df: DataFrame, bronze_table: str, checkpoint_location: str, trigger_once: bool = True):
    writer = (
        df.writeStream.format("delta")
        .option("checkpointLocation", checkpoint_location)
        .outputMode("append")
    )
    if trigger_once:
        writer = writer.trigger(availableNow=True)
    return writer.toTable(bronze_table)
