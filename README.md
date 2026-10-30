# Unity Catalog DR Sync — Cross-Workspace Disaster Recovery

![Databricks](https://img.shields.io/badge/Databricks-SDK-FF3621)
![Unity Catalog](https://img.shields.io/badge/Unity_Catalog-00ADD8?style=flat)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-00ADD8?style=flat)
![Python](https://img.shields.io/badge/Python-3.9-blue?style=flat)
![Azure](https://img.shields.io/badge/Azure-ADLS_Gen2-0078D4?style=flat)

A disaster recovery (DR) framework that recreates a Unity Catalog workspace — its metadata, data, and grants — in a second Databricks workspace, so the DR workspace is ready to use if the primary workspace is unavailable.

It is deliberately split into two steps: build the metadata skeleton first and verify it, then sync the data and permissions. Every notebook is idempotent, so a failed or partial run can safely be re-run.

## The problem

Recreating a Unity Catalog workspace by hand is slow and error-prone. Catalogs, schemas, storage credentials, external locations, volumes, views, and grants all have to match the primary workspace, and Delta tables have to be copied without losing history or breaking external-table registrations. Miss a grant or a storage path translation and the DR workspace looks complete but does not work.

This framework automates that recreation from CSV mapping files, with no source-to-target names or URLs hardcoded in the notebooks.

## What it syncs

| Area | How |
|---|---|
| Storage credentials and external locations | Databricks SDK, compared by name, missing objects created in the target |
| Catalogs and schemas | Databricks SDK, shared/external catalogs skipped, system schemas excludable |
| Delta tables | DEEP CLONE to a shared DR landing zone, then DROP stale external tables and LOAD them in the target metastore |
| External volumes | Databricks SDK, with storage-path translation from source to DR |
| Views | `SHOW CREATE TABLE` DDL from the source, catalog name rewritten, `CREATE OR REPLACE VIEW` on the target |
| Grants / permissions | Unity Catalog REST API, effective permissions diffed per principal, only the delta PATCHed |

## Architecture

```
PRIMARY WORKSPACE (Source)                   DR TARGET WORKSPACE
━━━━━━━━━━━━━━━━━━━━━━━━━                   ━━━━━━━━━━━━━━━━━━━━

┌─────────────────────┐                     ┌─────────────────────┐
│  Unity Catalog      │                     │  Unity Catalog      │
│  ├── Credentials    │──── Workflow 1 ────►│  ├── Credentials    │
│  ├── Ext. Locations │  (metadata_sync)    │  ├── Ext. Locations │
│  ├── Catalogs       │                     │  ├── Catalogs       │
│  └── Schemas        │                     │  └── Schemas        │
└─────────────────────┘                     └─────────────────────┘
         │                                           │
         │  Workflow 2 (deepclone_dr)                │
         │                                           │
         ▼                                           ▼
┌─────────────────────┐  DEEP CLONE  ┌─────────────────────────┐
│  Delta Tables       │─────────────►│  DR Landing Zone (ADLS) │
│  External Volumes   │  path map    │  ├── tables/             │
│  Views (DDL copy)   │─────────────►│  ├── volumes/            │
│  Grants/Permissions │─────────────►│  └── views/              │
└─────────────────────┘              └─────────────────────────┘
```

Full design notes, including the ten design decisions behind this split, are in [docs/architecture.md](docs/architecture.md).

## Two workflows — run in order

| Step | Workflow | Notebooks | Purpose |
|---|---|---|---|
| 1 | `wf_metadata_sync_dr` | 01 → 02 | Sync credentials, external locations, catalogs, and schemas |
| 2 | `wf_deepclone_dr` | 03 → 04 → 05 → 06 | Deep clone tables, sync volumes and views, then sync permissions |

Run Workflow 1 first and check the target skeleton before running Workflow 2. The table clone is the expensive step; permissions run last so grants are applied to objects that already exist in the target.

## The six notebooks

| # | Notebook | Key pattern |
|---|---|---|
| 01 | `01_sync_creds_and_locations.py` | SDK `storage_credentials` + `external_locations` APIs; credential creation is optional, controlled by a flag |
| 02 | `02_sync_catalogs_and_schemas.py` | SDK `catalogs.list()` + `schemas.create()`; configurable schema ignore list |
| 03 | `03_sync_tables.py` | DEEP CLONE via the SQL warehouse statement-execution API, then DROP stale tables and LOAD; parallel with `ThreadPoolExecutor` |
| 04 | `04_sync_ext_volumes.py` | SDK `volumes.create()` with storage-path translation; ownership transferred to match the source |
| 05 | `05_sync_views.py` | `SHOW CREATE TABLE` → catalog replacement → `CREATE OR REPLACE VIEW` on the target, with statement polling |
| 06 | `06_sync_permissions.py` | Direct REST API for effective permissions; per-principal diff, PATCH delta only |

## Key design choices

- **CSV-driven mapping** — every source → target translation lives in `data/mapping_files/`. Changing the DR target means editing a CSV, not notebook code.
- **Compare by name, not by ID** — catalog, schema, credential, and location IDs differ between metastores; names are the stable identifier.
- **DEEP CLONE to a shared landing zone** — the DR landing container is accessible from both workspaces, so the clone is written from the primary and read from the DR workspace without extra data movement, and Delta history is preserved for point-in-time recovery.
- **Idempotent** — objects that already exist in the target are skipped, `CREATE OR REPLACE` is used where supported, and explicit existence checks are used elsewhere.
- **Delta-only grants** — effective (not just direct) grants are diffed per principal, so grants set directly on the target are not wiped out by a full overwrite.
- **Parallel, but tunable** — bulk work uses `ThreadPoolExecutor` with a configurable worker count, so throughput can be tuned against API rate limits.

## Prerequisites

- Two Databricks workspaces (primary and DR) with Unity Catalog enabled, on Azure.
- A service principal with permission to create the objects being synced in the target workspace, and to read them in the source. Run the workflows as that service principal.
- A Databricks Secret Scope named `adls{env}scope` (where `{env}` is `dev`, `uat`, or `prod`) holding four keys: `SourceHostKey`, `SourceSPTokenKey`, `TargetHostKey`, and `TargetSPTokenKey`. Hosts and tokens are never hardcoded.
- SQL warehouses in both workspaces for the statement-execution steps, and an ADLS Gen2 landing zone accessible from both workspaces.
- Databricks Runtime 14.3 (`14.3.x-scala2.12`, as in the workflow definitions), with the notebooks uploaded to the workspace paths used in the workflow YAMLs.
- The mapping CSVs uploaded to a Unity Catalog Volume, e.g. `/Volumes/uc_dev/default/dr_mapping_files/`.

## Security

- No workspace host, token, secret, or subscription ID is hardcoded anywhere in this repository. All values that differ by environment come from the Secret Scope or from the CSV mapping files.
- The mapping files in this repository are **examples with placeholders** (`yourstorageaccount`, `<your-service-principal-id>`, `<sub-id>`, warehouse IDs). Replace every placeholder before running anything against a real workspace.
- Use least-privilege access for the service principal, and test the full sequence in a dev pair of workspaces first. DEEP CLONE copies full tables, and Workflow 2 drops stale external tables in the **target** workspace before re-creating them — review the mapping files and run order before every run.

## How to deploy

1. Upload the mapping CSVs in `data/mapping_files/` to a Unity Catalog Volume, and edit every placeholder value for your own primary and DR workspaces.
2. Upload the notebooks in `notebooks/` to the Databricks workspace, under `/Workspace/projects/dr_metadatasync/` (01–02) and `/Workspace/projects/dr_deepclone/` (03–06), matching the paths in the workflow YAMLs.
3. Create the Secret Scope and keys listed under Prerequisites.
4. In `workflows/`, replace the placeholders: storage account names, the service principal ID in `run_as`, and the source/target SQL warehouse IDs. See Notes below for the `shared_functions` wheel reference.
5. Import the two workflow YAMLs as Databricks Jobs (or recreate them from the YAMLs in the Jobs UI) and set the job parameters from the YAML defaults.
6. Run `wf_metadata_sync_dr` first, verify catalogs, schemas, credentials, and external locations in the DR workspace, then run `wf_deepclone_dr`.

## Mapping files

| File | Purpose | Key columns |
|---|---|---|
| `catalog_mapping.csv` | Source catalog → target catalog, and the target storage root | `source_catalog`, `target_catalog`, `target_storage_root` |
| `schema_mapping.csv` | Schemas included in the table sync | `source_schema`, `target_schema` |
| `external_loc_mapping.csv` | External location name and URL translation | `source_loc_name`, `target_loc_name`, `target_url` |
| `volume_mapping.csv` | Storage-path prefix translation for volumes | `source_loc`, `target_loc` |
| `azure_cred_mapping.csv` | Credential auth config for the DR workspace | `source_cred_name`, `target_cred_name`, managed-identity / service-principal fields |

Only one auth method should be populated per credential row in `azure_cred_mapping.csv`: a managed-identity connector, a managed-identity identity, or a service principal. Credential creation itself is off by default (`create_credential_flag=false`).

## Notes and limitations

- **Shared-functions wheel:** the workflow YAMLs reference a `shared_functions-*.whl` library, but that wheel is not included in this repository and is not imported by any notebook in it. Remove that library entry, or point it at your own wheel, if your workspace does not need it.
- **Status widgets:** the notebooks accept `status_database` / `status_tbl` widget parameters, but in this version they report a run summary with `print` output only; they do not write rows to a status table.
- **Tables:** Notebook 03 syncs DELTA tables (including EXTERNAL Delta tables). Other table types are skipped and reported as such.
- **Views:** view DDL is copied with a straight catalog-name replacement. Views that reference objects outside the mapped catalogs need a manual check.
- **Permissions:** the REST API is used directly because the SDK does not expose effective permissions consistently across versions. Securable-type variants are tried in order for the same reason.

## Project structure

```
unity-catalog-dr-sync/
├── notebooks/
│   ├── 01_sync_creds_and_locations.py   # Workflow 1, Task 1
│   ├── 02_sync_catalogs_and_schemas.py  # Workflow 1, Task 2
│   ├── 03_sync_tables.py                # Workflow 2, Task 1
│   ├── 04_sync_ext_volumes.py           # Workflow 2, Task 2
│   ├── 05_sync_views.py                 # Workflow 2, Task 3
│   └── 06_sync_permissions.py           # Workflow 2, Task 4
├── workflows/
│   ├── wf_metadata_sync_dr.yml          # Run first
│   └── wf_deepclone_dr.yml              # Run second
├── data/
│   └── mapping_files/
│       ├── catalog_mapping.csv
│       ├── schema_mapping.csv
│       ├── external_loc_mapping.csv
│       ├── volume_mapping.csv
│       └── azure_cred_mapping.csv
├── docs/
│   └── architecture.md
├── requirements.txt
├── .gitignore
├── LICENSE
└── README.md
```

## Tech stack

Azure · Databricks · Unity Catalog · Delta Lake · PySpark / Spark SQL · Databricks SDK · SQL Warehouse statement-execution API · Unity Catalog REST API · ADLS Gen2 · Python

## Author

**Sanjeev Kumar Pandey** — Data Engineer → AI Engineer | Azure Databricks | PySpark | RAG | Agentic AI

[LinkedIn](https://www.linkedin.com/in/sanjeev-pandey-7a45831ba) · [GitHub](https://github.com/sanjeevpandeydata)
