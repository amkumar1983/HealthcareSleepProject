# Data Dictionary

## Bronze: `device_sessions` (raw, schema-enforced, append-only)
| Column | Type | Notes |
|---|---|---|
| record_id | string | Source record UUID |
| patient_id | string | **PHI** — synthetic MRN-style ID in sample data |
| device_id | string | Device serial/identifier |
| device_model | string | e.g. ResMed AirSense 11 |
| mask_type | string | nasal_pillow / full_face / nasal_cradle |
| patient_timezone | string | IANA tz name, e.g. America/Chicago |
| session_start_utc / session_end_utc | string (ISO8601) | Cast to timestamp in Silver |
| duration_minutes | int | Session length |
| ahi | double | Apnea-Hypopnea Index (events/hour) reported by device |
| severity_label_device | string | Device's own severity bucket (normal/mild/moderate/severe) |
| avg_spo2_pct / min_spo2_pct | double | Blood oxygen saturation |
| leak_rate_avg_lpm | double | Mask leak rate |
| events | array<struct> | Nested per-event detail; exploded into Silver `device_events` |
| _ingested_at_utc, _source_file | metadata | Lineage/audit columns added at ingestion |

## Silver: `device_sessions` (de-identified, deduped, validated)
Same as Bronze minus `patient_id` (replaced by `patient_key`, a salted SHA-256 pseudonym) and
minus `events` (moved to `device_events`). Only rows passing all `error`-severity quality
rules are present; rejects go to `quarantine.device_sessions_rejected`.

## Silver: `device_events`
Event-grain table: `event_id, event_type, event_timestamp_utc, duration_seconds, spo2_min_pct`
plus `patient_key, device_id, session_start_utc` for joinability back to sessions.

## Restricted: `phi_restricted.patient_identity_map`
`patient_id` (PHI) ↔ `patient_key` mapping. Separate Unity Catalog schema, separate (tighter)
grants. This is the only table where re-identification is possible.

## Gold
- `patient_compliance_summary` — per-patient-night compliant flag (>=240 min usage), feeds CMS-style compliance reporting.
- `patient_ahi_trend` — rolling 7-night AHI average per patient.
- `device_fleet_health` — operational device-level freshness/leak metrics (non-clinical, fleet ops use).
