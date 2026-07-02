"""
Gold layer: curated, analytics/ML-ready aggregates built from Silver. Clinically meaningful
rollups — e.g., patient-night summary already exists at Silver grain (one row per session),
so Gold here focuses on patient-level trend/compliance metrics used by clinician dashboards
and AHI-trend ML features.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_patient_compliance_summary(silver_sessions: DataFrame) -> DataFrame:
    """
    CMS/insurance CPAP compliance rule of thumb: >=4 hours/night on >=70% of nights in a
    rolling 30-day window. This computes the building-block aggregates; the 70%-of-30-days
    windowing itself is best done with a date-spine join at the BI/semantic layer, but the
    per-night flag is produced here so it's reusable.
    """
    return silver_sessions.withColumn(
        "compliant_night", (F.col("duration_minutes") >= 240).cast("int")
    ).select(
        "patient_key",
        "device_id",
        F.to_date("session_start_utc").alias("session_date"),
        "duration_minutes",
        "ahi",
        "avg_spo2_pct",
        "min_spo2_pct",
        "leak_rate_avg_lpm",
        "compliant_night",
    )


def build_patient_ahi_trend(silver_sessions: DataFrame) -> DataFrame:
    """Rolling 7-night average AHI per patient — a standard clinical trend metric."""
    from pyspark.sql.window import Window

    w = (
        Window.partitionBy("patient_key")
        .orderBy(F.col("session_start_utc").cast("long"))
        .rangeBetween(-6 * 86400, 0)
    )
    return silver_sessions.withColumn(
        "ahi_7night_avg", F.round(F.avg("ahi").over(w), 2)
    ).select(
        "patient_key",
        F.to_date("session_start_utc").alias("session_date"),
        "ahi",
        "ahi_7night_avg",
        "severity_label_device",
    )


def build_device_fleet_health(silver_sessions: DataFrame) -> DataFrame:
    """Operational (non-clinical) view: device-level data freshness/quality for fleet ops."""
    return (
        silver_sessions.groupBy("device_id")
        .agg(
            F.max("session_start_utc").alias("last_session_start_utc"),
            F.count("*").alias("total_sessions"),
            F.avg("leak_rate_avg_lpm").alias("avg_leak_rate_lpm"),
        )
    )
