"""
Silver layer: cleanses, types, de-duplicates, and de-identifies Bronze device session data.
Pure functions of DataFrame -> DataFrame so they're trivially unit-testable with a local
SparkSession and small fixture DataFrames (see tests/unit/test_silver_transform.py).
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from sleepapnea_dp.utils.phi import pseudonymize_column


def cast_timestamps(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("session_start_utc", F.to_timestamp("session_start_utc"))
        .withColumn("session_end_utc", F.to_timestamp("session_end_utc"))
        .withColumn("source_export_ts_utc", F.to_timestamp("source_export_ts_utc"))
    )


def deduplicate_sessions(df: DataFrame) -> DataFrame:
    """
    Devices may re-upload the same night (sync retries). Dedupe on the natural key
    (device_id, session_start_utc), keeping the most recently ingested record.
    """
    w = Window.partitionBy("device_id", "session_start_utc").orderBy(F.col("_ingested_at_utc").desc())
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def filter_invalid_sessions(df: DataFrame) -> DataFrame:
    """Drop physically impossible records rather than silently passing them downstream.
    Rejected rows should be captured by the quality module (quarantine), not just dropped —
    see quality/validators.py for the paired quarantine logic used in the orchestrating notebook.
    """
    return df.filter(
        (F.col("session_end_utc") > F.col("session_start_utc"))
        & (F.col("duration_minutes") > 0)
        & (F.col("ahi") >= 0)
        & (F.col("avg_spo2_pct").between(50, 100) | F.col("avg_spo2_pct").isNull())
    )


def pseudonymize_patient_id(df: DataFrame, salt: str) -> DataFrame:
    """
    Replaces the raw PHI patient_id with a stable pseudonym for all downstream analytical
    use. The reversible mapping is written separately to a restricted-access identity vault
    table (see transformation/identity_vault.py) — Silver/Gold never carry both columns.
    """
    return df.withColumn("patient_key", pseudonymize_column(F.col("patient_id"), salt)).drop(
        "patient_id"
    )


def explode_events(df: DataFrame) -> DataFrame:
    """Produces a separate event-grain Silver table from the nested `events` array —
    needed for per-event analytics (e.g., apnea event clustering) without duplicating
    session-level columns across every event row unnecessarily."""
    return (
        df.select(
            "record_id",
            "patient_key",
            "device_id",
            "session_start_utc",
            F.explode_outer("events").alias("event"),
        )
        .select(
            "record_id",
            "patient_key",
            "device_id",
            "session_start_utc",
            F.col("event.event_id").alias("event_id"),
            F.col("event.event_type").alias("event_type"),
            F.to_timestamp("event.event_timestamp_utc").alias("event_timestamp_utc"),
            F.col("event.duration_seconds").alias("duration_seconds"),
            F.col("event.spo2_min_pct").alias("spo2_min_pct"),
        )
    )


def build_silver_sessions(df: DataFrame, salt: str) -> DataFrame:
    """Composable end-to-end Silver session transform — the function notebooks actually call."""
    df = cast_timestamps(df)
    df = deduplicate_sessions(df)
    df = filter_invalid_sessions(df)
    df = pseudonymize_patient_id(df, salt)
    return df.drop("events")  # events go to their own exploded Silver table
