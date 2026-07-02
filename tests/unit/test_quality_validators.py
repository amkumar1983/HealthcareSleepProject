from datetime import datetime

from sleepapnea_dp.quality.validators import DEVICE_SESSION_RULES, split_clean_and_quarantine


def test_split_clean_and_quarantine(spark):
    good = dict(
        patient_id="p1",
        device_id="d1",
        session_start_utc="2026-05-01T22:00:00",
        session_end_utc="2026-05-02T04:00:00",
        ahi=5.0,
        avg_spo2_pct=95.0,
        duration_minutes=360,
    )
    bad_negative_ahi = dict(good, patient_id="p2", ahi=-3.0)
    bad_null_patient = dict(good, patient_id=None)

    df = spark.createDataFrame([good, bad_negative_ahi, bad_null_patient])
    clean, quarantined = split_clean_and_quarantine(df, DEVICE_SESSION_RULES)

    assert clean.count() == 1
    assert quarantined.count() == 2
    # quarantined rows retain the diagnostic flag columns
    assert "_dq_ahi_non_negative" in quarantined.columns
