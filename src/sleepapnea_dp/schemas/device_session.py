"""
Explicit schemas for sleep apnea device data. Defining schemas explicitly (rather than
relying on inferSchema) is mandatory for healthcare-grade pipelines: it makes ingestion
deterministic, catches vendor schema drift immediately, and avoids silent type coercion
on clinical values (e.g., AHI silently becoming a string).
"""
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("event_type", StringType(), nullable=False),
        StructField("event_timestamp_utc", StringType(), nullable=False),  # cast to timestamp in Silver
        StructField("duration_seconds", DoubleType(), nullable=True),
        StructField("spo2_min_pct", DoubleType(), nullable=True),
    ]
)

DEVICE_SESSION_RAW_SCHEMA = StructType(
    [
        StructField("record_id", StringType(), nullable=False),
        StructField("schema_version", StringType(), nullable=False),
        StructField("patient_id", StringType(), nullable=False),  # PHI
        StructField("device_id", StringType(), nullable=False),
        StructField("device_model", StringType(), nullable=True),
        StructField("mask_type", StringType(), nullable=True),
        StructField("patient_timezone", StringType(), nullable=True),
        StructField("session_start_utc", StringType(), nullable=False),
        StructField("session_end_utc", StringType(), nullable=False),
        StructField("duration_minutes", IntegerType(), nullable=True),
        StructField("ahi", DoubleType(), nullable=True),
        StructField("severity_label_device", StringType(), nullable=True),
        StructField("avg_spo2_pct", DoubleType(), nullable=True),
        StructField("min_spo2_pct", DoubleType(), nullable=True),
        StructField("leak_rate_avg_lpm", DoubleType(), nullable=True),
        StructField("leak_large_event_count", IntegerType(), nullable=True),
        StructField("mask_off_minutes", DoubleType(), nullable=True),
        StructField("pressure_avg_cmh2o", DoubleType(), nullable=True),
        StructField("pressure_95th_cmh2o", DoubleType(), nullable=True),
        StructField("humidifier_setting", IntegerType(), nullable=True),
        StructField("events", ArrayType(EVENT_SCHEMA), nullable=True),
        StructField("ingestion_batch_id", StringType(), nullable=False),
        StructField("source_export_ts_utc", StringType(), nullable=True),
    ]
)

# Columns classified as PHI / quasi-identifiers — used by quality + transformation layers
# to drive masking, access control tagging, and Safe-Harbor de-identification logic.
PHI_COLUMNS = {"patient_id"}
QUASI_IDENTIFIER_COLUMNS = {"device_id", "patient_timezone"}
