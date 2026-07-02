"""
Generates synthetic CPAP / sleep-study device export data, mimicking what a real
device vendor (e.g., ResMed AirView, Philips DreamMapper-style export) would push
into the raw landing zone, as JSON-lines files — one file per device sync batch.

100% synthetic. No real patient data. Safe to commit to git / use in dev.

Usage:
    python generate_sleep_apnea_data.py --patients 25 --nights 30 --out ./output
"""
import argparse
import json
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

random.seed(42)

DEVICE_MODELS = ["ResMed AirSense 11", "Philips DreamStation 2", "Fisher&Paykel SleepStyle"]
MASK_TYPES = ["nasal_pillow", "full_face", "nasal_cradle"]
EVENT_TYPES = ["obstructive_apnea", "central_apnea", "hypopnea", "rera", "desaturation"]
TIMEZONES = ["America/New_York", "America/Chicago", "America/Los_Angeles", "America/Denver"]


def synthetic_patient_id(i: int) -> str:
    # Synthetic MRN-style identifier; in real life this is PHI and must be handled accordingly.
    return f"SYN-MRN-{100000 + i}"


def gen_events(session_start: datetime, duration_min: int, ahi_target: float):
    """Generate discrete respiratory events for a session based on a target AHI (events/hour)."""
    n_events = max(0, int(round(ahi_target * duration_min / 60)))
    events = []
    for _ in range(n_events):
        offset_min = random.uniform(0, duration_min)
        ts = session_start + timedelta(minutes=offset_min)
        etype = random.choices(
            EVENT_TYPES, weights=[0.45, 0.10, 0.30, 0.10, 0.05]
        )[0]
        events.append(
            {
                "event_id": str(uuid.uuid4()),
                "event_type": etype,
                "event_timestamp_utc": ts.isoformat(),
                "duration_seconds": round(random.uniform(10, 60), 1),
                "spo2_min_pct": round(random.uniform(78, 92), 1) if etype == "desaturation" else None,
            }
        )
    events.sort(key=lambda e: e["event_timestamp_utc"])
    return events


def gen_session(patient_id: str, device_id: str, night_date, tz_name: str):
    severity = random.choices(
        ["normal", "mild", "moderate", "severe"], weights=[0.30, 0.30, 0.25, 0.15]
    )[0]
    ahi_target = {
        "normal": random.uniform(0, 4.9),
        "mild": random.uniform(5, 14.9),
        "moderate": random.uniform(15, 29.9),
        "severe": random.uniform(30, 60),
    }[severity]

    session_start = datetime.combine(night_date, datetime.min.time(), tzinfo=timezone.utc) + timedelta(
        hours=random.uniform(21, 23.5)
    )
    duration_min = int(random.gauss(420, 45))  # ~7h average, in minutes
    duration_min = max(60, min(duration_min, 600))
    session_end = session_start + timedelta(minutes=duration_min)

    events = gen_events(session_start, duration_min, ahi_target)
    actual_ahi = round(len(events) / (duration_min / 60), 2) if duration_min else 0.0

    leak_events = random.randint(0, 5)
    mask_off_min = round(random.uniform(0, 25), 1)

    return {
        "record_id": str(uuid.uuid4()),
        "schema_version": "1.2",
        "patient_id": patient_id,           # PHI - pseudonymize downstream
        "device_id": device_id,
        "device_model": random.choice(DEVICE_MODELS),
        "mask_type": random.choice(MASK_TYPES),
        "patient_timezone": tz_name,
        "session_start_utc": session_start.isoformat(),
        "session_end_utc": session_end.isoformat(),
        "duration_minutes": duration_min,
        "ahi": actual_ahi,
        "severity_label_device": severity,
        "avg_spo2_pct": round(random.uniform(90, 98), 1),
        "min_spo2_pct": round(random.uniform(78, 94), 1),
        "leak_rate_avg_lpm": round(random.uniform(0, 35), 1),
        "leak_large_event_count": leak_events,
        "mask_off_minutes": mask_off_min,
        "pressure_avg_cmh2o": round(random.uniform(6, 14), 1),
        "pressure_95th_cmh2o": round(random.uniform(8, 18), 1),
        "humidifier_setting": random.randint(0, 5),
        "events": events,
        "ingestion_batch_id": str(uuid.uuid4()),
        "source_export_ts_utc": (session_end + timedelta(hours=random.uniform(1, 72))).isoformat(),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--patients", type=int, default=25)
    parser.add_argument("--nights", type=int, default=30)
    parser.add_argument("--out", type=str, default="./sample_data/output")
    parser.add_argument("--start-date", type=str, default="2026-05-01")
    args = parser.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()

    patients = [
        {
            "patient_id": synthetic_patient_id(i),
            "device_id": f"DEV-{uuid.uuid4().hex[:8].upper()}",
            "tz": random.choice(TIMEZONES),
        }
        for i in range(args.patients)
    ]

    # One file per simulated "sync batch" (a device syncing a few nights at once),
    # which mirrors how raw files actually land in ADLS from vendor exports.
    batch_idx = 0
    for p in patients:
        night = start_date
        nights_left = args.nights
        while nights_left > 0:
            batch_size = min(nights_left, random.randint(1, 4))
            records = [
                gen_session(p["patient_id"], p["device_id"], night + timedelta(days=d), p["tz"])
                for d in range(batch_size)
            ]
            fname = out_dir / f"device_export_{p['device_id']}_{night.isoformat()}_batch{batch_idx}.jsonl"
            with open(fname, "w") as f:
                for r in records:
                    f.write(json.dumps(r) + "\n")
            night += timedelta(days=batch_size)
            nights_left -= batch_size
            batch_idx += 1

    print(f"Generated synthetic data for {len(patients)} patients into {out_dir.resolve()}")


if __name__ == "__main__":
    main()
