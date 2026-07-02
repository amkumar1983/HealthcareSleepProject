# Gaps & Recommendations (read before going to production)

This is the expanded version of README §5 — things commonly missed in "first draft" healthcare
data platforms.

## Compliance & legal
- **BAA (Business Associate Agreement)** with Microsoft must be signed before any PHI touches
  Azure. Confirm every service used (ADLS, Databricks, Event Hubs, ADF, Key Vault, Monitor) is
  in Microsoft's HIPAA-covered services list for your subscription.
- **HIPAA Security Risk Assessment** — formal, documented, repeated annually.
- **Minimum necessary** principle — design Unity Catalog grants so each role sees only the
  columns/rows it needs (e.g., a data scientist building an AHI model should get de-identified
  Silver/Gold, not raw PHI Bronze).
- **De-identification strategy**: decide between HIPAA Safe Harbor (strip the 18 identifiers)
  vs. Expert Determination, especially if data feeds ML models or is shared with researchers.
- **Data residency** — confirm region (Azure region selection) matches regulatory requirements
  if patients are outside your default region.
- **State-level health data laws** (e.g., differing breach-notification timelines) on top of HIPAA.

## Data lifecycle
- **Retention policy**: HIPAA requires 6 years minimum for certain records; define ADLS
  lifecycle management rules (hot→cool→archive→delete) per data classification, not a single
  blanket policy.
- **Right to deletion / amendment requests** — build a process (and Delta `DELETE`/`MERGE`
  pattern with audit logging) since plain "delete the row" breaks Delta time-travel/audit trail
  guarantees; need a documented exception process for compliant deletion.
- **Consent tracking** — store consent scope/version per patient-device link; pipeline should
  refuse to process data outside consented use (e.g., research vs. clinical-only).

## Data engineering robustness
- **Late/out-of-order data**: CPAP devices often sync in batches days later — design Bronze/
  Silver merge logic (Delta `MERGE INTO`) to be idempotent and watermark-based, not append-only
  assuming arrival order.
- **Clock drift / timezone handling**: device local time vs. patient timezone vs. UTC — store
  all three explicitly, don't silently convert.
- **Schema evolution / contract testing**: vendor firmware updates change export schema. Add a
  schema-validation gate (`schemas/` + `quality/` checks) that fails loudly rather than silently
  dropping/mistyping new columns.
- **Duplicate/replay protection**: device re-uploads of the same night — dedupe key
  `(device_id, patient_id, session_start_ts)`.
- **Multi-device-per-patient** and **device-reassignment** (loaner devices passed between
  patients) — your patient↔device mapping needs a `valid_from/valid_to` (SCD Type 2), not a
  static foreign key.

## Platform/ops
- **Cluster policies & job clusters**: prod jobs should run on job clusters (auto-terminate),
  not shared all-purpose clusters; enforce via Databricks cluster policies + budget alerts.
- **DR/backup**: Git is the backup for code; Unity Catalog metastore config, secret scopes, and
  cluster policies should be captured as IaC (Bicep/Terraform) so a workspace can be rebuilt;
  document RTO/RPO targets explicitly.
- **Observability**: ship Databricks audit logs + cluster logs to Log Analytics; build a
  freshness/SLA dashboard (e.g., "Bronze table updated within X hours") with Azure Monitor
  alerting, not just a green checkmark in Jobs UI.
- **Cost governance**: tag all resources by env/team/cost-center; set Azure Budgets + alerts.

## Interoperability
- **FHIR export**: if results (e.g., AHI score, recommended titration) need to land back in an
  EHR, use **Azure Health Data Services (FHIR service)** rather than building a custom HL7/FHIR
  mapper from scratch.
- **HL7v2 ingestion**, if any source system still emits it, needs a dedicated adapter — not
  covered by the JSON device-export pattern in this repo.

## Testing
- Add edge-case synthetic data: mask leak spikes, mid-night device disconnect/reconnect,
  daylight-saving-time transition nights, simultaneous multi-device sessions, malformed/
  truncated export files, sessions spanning midnight UTC boundary.
