from datetime import datetime, timedelta

from sleepapnea_dp.transformation.silver_transform import (
    deduplicate_sessions,
    filter_invalid_sessions,
    pseudonymize_patient_id,
)


def _row(**overrides):
    base = dict(
        record_id="r1",
        patient_id="SYN-MRN-100001",
        device_id="DEV-1",
        session_start_utc=datetime(2026, 5, 1, 22, 0, 0),
        session_end_utc=datetime(2026, 5, 2, 4, 0, 0),
        duration_minutes=360,
        ahi=5.0,
        avg_spo2_pct=94.0,
        _ingested_at_utc=datetime(2026, 5, 2, 6, 0, 0),
    )
    base.update(overrides)
    return base


def test_filter_invalid_sessions_drops_negative_ahi(spark):
    df = spark.createDataFrame([_row(ahi=-1.0), _row(record_id="r2", ahi=5.0)])
    result = filter_invalid_sessions(df)
    assert result.count() == 1
    assert result.collect()[0]["record_id"] == "r2"


def test_filter_invalid_sessions_drops_bad_session_window(spark):
    bad = _row(
        record_id="r3",
        session_start_utc=datetime(2026, 5, 2, 4, 0, 0),
        session_end_utc=datetime(2026, 5, 1, 22, 0, 0),  # end before start
    )
    df = spark.createDataFrame([bad])
    result = filter_invalid_sessions(df)
    assert result.count() == 0


def test_deduplicate_sessions_keeps_latest_ingested(spark):
    older = _row(record_id="old", _ingested_at_utc=datetime(2026, 5, 2, 6, 0, 0))
    newer = _row(record_id="new", _ingested_at_utc=datetime(2026, 5, 2, 9, 0, 0))
    df = spark.createDataFrame([older, newer])
    result = deduplicate_sessions(df)
    assert result.count() == 1
    assert result.collect()[0]["record_id"] == "new"


def test_pseudonymize_patient_id_is_deterministic_and_drops_phi(spark):
    df = spark.createDataFrame([_row(), _row(record_id="r2")])
    result = pseudonymize_patient_id(df, salt="test-salt")
    assert "patient_id" not in result.columns
    assert "patient_key" in result.columns
    rows = result.select("patient_key").distinct().collect()
    assert len(rows) == 1  # same patient_id -> same patient_key
    assert len(rows[0]["patient_key"]) == 64  # sha256 hex digest length
