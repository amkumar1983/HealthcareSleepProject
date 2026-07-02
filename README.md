# Sleep Apnea Device Data Platform — Azure Databricks Reference Architecture

End-to-end, healthcare-grade data engineering pipeline for ingesting, validating, and
modeling sleep apnea device telemetry (e.g., CPAP/APAP machines, pulse-oximeters, home
sleep-test devices) on Azure, built with Databricks + PySpark and Medallion architecture.

## 1. Architecture

```
Device Vendor API/SFTP/Files
        │
        ▼
  Azure Data Factory (ADF)  ──orchestrates ingestion──▶  ADLS Gen2 (raw/landing zone)
        │                                                     │
        │  (event-driven option)                              ▼
  Azure Event Hubs ──▶ Azure Databricks Structured Streaming   Bronze (raw, immutable, append-only)
        │                                                     │
        ▼                                                     ▼
  Azure Key Vault (secrets, PHI encryption keys)        Silver (cleansed, conformed, de-identified)
        │                                                     │
        ▼                                                     ▼
  Unity Catalog (governance, PHI column tagging, RLS/CLS)  Gold (curated, analytics/ML-ready)
                                                                │
                                          ┌─────────────────────┼─────────────────────┐
                                          ▼                     ▼                     ▼
                                   Power BI / Synapse     ML models (AHI risk)   FHIR export /
                                   (clinician dashboards)  Azure Databricks ML   downstream EHR
```

### Core Azure services
| Layer | Service | Why |
|---|---|---|
| Landing/storage | **ADLS Gen2** | Hierarchical namespace, ACLs, lifecycle mgmt, HIPAA-eligible |
| Orchestration (batch) | **Azure Data Factory** | Pulls from SFTP/vendor APIs into raw zone, triggers Databricks Jobs |
| Orchestration (intra-pipeline) | **Databricks Workflows/Jobs** | Notebook-to-notebook DAG, retries, alerting |
| Streaming (optional) | **Azure Event Hubs** + Databricks Structured Streaming | Near-real-time device telemetry (e.g., apnea events, SpO2 drops) |
| Compute | **Azure Databricks** (Premium tier) | PySpark transformations, Delta Lake, Unity Catalog |
| Storage format | **Delta Lake** | ACID, time travel, schema evolution, audit trail (required for HIPAA) |
| Governance/catalog | **Unity Catalog** | Centralized access control, column-level masking for PHI, lineage, audit logs |
| Secrets | **Azure Key Vault** + Databricks secret scopes | No credentials in code/notebooks |
| Identity | **Microsoft Entra ID** + Databricks SCIM | RBAC, conditional access |
| Networking | **VNet injection / Private Link** for Databricks, ADLS firewall, Private Endpoints | No public exposure of PHI-bearing storage |
| Monitoring | **Azure Monitor / Log Analytics** + Databricks audit logs | Required for HIPAA audit-trail control |
| CI/CD | **Azure DevOps Pipelines** (or GitHub Actions) | Promote code/config dev→test→prod |
| IaC | **Bicep/ARM (or Terraform)** | Reproducible environment provisioning |
| Data quality | **Great Expectations / Databricks DLT expectations** (here: lightweight custom framework) | Validate device readings before Silver/Gold |
| Optional clinical interoperability | **Azure Health Data Services (FHIR service)** | If results need to integrate with EHR systems |

### HIPAA / healthcare-specific considerations baked in
- Data is classified at ingestion: **PHI columns** (patient_id, name, DOB, MRN) vs. **device telemetry** (AHI, SpO2, events) are tagged and separated.
- Silver layer **pseudonymizes** patient_id (salted hash) before wide analytical use; a separate restricted-access mapping table (Gold-Secure / "PHI vault") holds the re-identification key.
- All storage uses **encryption at rest** (Microsoft-managed or CMK via Key Vault) and **TLS in transit**.
- **Unity Catalog** access control + audit logging gives the "who accessed what PHI, when" trail required for HIPAA.
- ADLS containers use **private endpoints**; Databricks deployed with **VNet injection / No Public IP**.
- A **Business Associate Agreement (BAA)** with Microsoft is required for any Azure subscription processing PHI — this is an organizational/legal step, not a technical one, but it's the precondition for everything above.

## 2. Repository structure

```
sleepapnea-data-platform/
├── notebooks/                  # Databricks notebooks = orchestration only (thin)
│   ├── 00_setup/                # widget/config bootstrap, mount/Unity Catalog setup
│   ├── 10_bronze/                # raw ingestion notebooks
│   ├── 20_silver/                # cleansing/conform/de-identify notebooks
│   ├── 30_gold/                  # curated aggregates (AHI summaries, patient-night rollups)
│   ├── 40_quality/               # data quality gate notebooks
│   └── 90_utils/                 # ad hoc / one-off utility notebooks
├── src/sleepapnea_dp/           # all REAL logic lives here as an installable Python package (wheel)
│   ├── ingestion/                # readers (file, autoloader, eventhub)
│   ├── transformation/           # bronze→silver→gold business logic
│   ├── quality/                  # validation rules, expectations
│   ├── schemas/                  # explicit StructType schemas + Delta DDL
│   └── utils/                    # spark session, config loader, logging, security/hashing
├── config/                      # environment-specific settings (no secrets!)
│   ├── dev/  test/  prod/        # each has pipeline_config.yaml
│   └── common.yaml
├── pipelines/devops/            # azure-pipelines.yml (build, test, release dev→test→prod)
├── infra/
│   ├── databricks/               # DAB (Databricks Asset Bundles) + job JSON definitions
│   └── arm_bicep/                 # IaC for ADLS, Databricks workspace, Key Vault, ADF
├── tests/
│   ├── unit/                     # pytest, chispa/pyspark-test for transformation logic
│   └── integration/              # run against a dbconnect/test catalog
├── sample_data/                  # synthetic sleep apnea device data generator + samples
├── docs/                         # data dictionary, runbook, architecture decision records
├── requirements.txt
├── setup.py / pyproject.toml
└── databricks.yml                # Databricks Asset Bundle root config
```

**Why notebooks are thin:** notebooks under `notebooks/` only import the `sleepapnea_dp`
package, read the config, and call functions — they contain almost no business logic. This
makes the PySpark code unit-testable with plain `pytest` outside Databricks, reusable across
notebooks/jobs/streaming, and reviewable in pull requests as real diff-able Python rather than
notebook JSON.

## 3. Environments & promotion model

Three Databricks workspaces (or three sets of Unity Catalog catalogs in a single workspace —
see note below): **dev → test → prod**, each with its own ADLS containers, Key Vault, and
Unity Catalog catalog (`sleepapnea_dev`, `sleepapnea_test`, `sleepapnea_prod`).

```
feature/* branch → PR → main (dev auto-deploys on merge)
                              │
                         tag/release  → triggers Test deployment (manual approval gate)
                                            │
                                       triggers Prod deployment (manual approval gate + change ticket)
```

- Git: **Databricks Repos** integrated with the same Azure DevOps/GitHub repo — engineers
  develop in a Repos checkout in `dev` workspace, never edit notebooks directly in prod.
- CI: on PR — lint (`ruff`/`black`), unit tests (`pytest`), build wheel.
- CD: **Databricks Asset Bundles (DAB)** deploy notebooks, jobs, and the wheel to each target
  workspace using environment-specific `databricks.yml` targets; `azure-pipelines.yml`
  orchestrates the three stages with manual approval gates before Test and Prod.

> Note: Unity Catalog now supports a **single workspace, three catalogs** pattern (catalog-level
> isolation rather than workspace-level), which is increasingly the recommended pattern and
> cheaper to operate. The repo structure here works either way — only the `config/<env>/*.yaml`
> values change (catalog name, storage account, key vault name).

## 4. Sample data

`sample_data/generate_sleep_apnea_data.py` produces realistic synthetic CPAP/sleep-study
device export data: nightly session summaries + per-event (apnea/hypopnea/desaturation)
detail records, in JSON-lines (as a device would actually export), matching what lands in
the ADLS raw zone. No real patient data is used anywhere.

## 5. What you might be missing (read this)

See `docs/gaps_and_recommendations.md` for the full list — short version:

1. **BAA with Microsoft** + formal HIPAA risk assessment — legal precondition, not optional.
2. **Data retention & deletion policy** (HIPAA min. 6 years; right-to-deletion conflicts — need an approach).
3. **De-identification/Safe Harbor or Expert Determination** strategy if data is ever used for research/ML.
4. **Disaster recovery**: ADLS GRS/RA-GRS + Databricks workspace backup (jobs/notebooks are code — recoverable via Git; but Unity Catalog metastore and secrets need a documented DR runbook.
5. **Consent management** — sleep apnea data often tied to clinical consent scope; track consent state, not just data.
6. **Device data integrity**: checksum/signature validation from vendor, late-arriving/out-of-order data handling (devices sync in batches), clock-drift correction across timezones.
7. **Schema evolution governance** — vendors change firmware/export formats; need a contract testing step against `schemas/`.
8. **Cost governance**: Databricks cluster policies, autoscaling limits, job clusters (not all-purpose) for prod, budget alerts.
9. **Observability**: structured logging to Log Analytics, data quality metrics to a dashboard, SLA/freshness alerting (Azure Monitor alerts on job failure/duration).
10. **PHI access reviews**: periodic Unity Catalog permission audits (quarterly), break-glass access procedure.
11. **FHIR interoperability** if results need to reach an EHR — Azure Health Data Services FHIR service.
12. **Testing with synthetic-but-realistic edge cases**: leak mask events, device disconnects mid-night, daylight saving transitions, multiple devices per patient.
