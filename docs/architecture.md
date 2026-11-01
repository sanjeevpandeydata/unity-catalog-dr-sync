# Architecture — Unity Catalog DR Sync Framework

## Purpose

This document describes how the Unity Catalog disaster-recovery framework works, why it is split the way it is, and where it can fail. It is written so a reviewer can check each claim against the code in `notebooks/` and the two workflow definitions in `workflows/`.

The framework recreates a Unity Catalog workspace — its metadata, data, and grants — in a second Databricks workspace, so the DR workspace is ready to use if the primary workspace is unavailable.

> **Status of this document.** This is an improved rewrite of the architecture document previously in this repository. The notebooks, workflow YAMLs, and mapping files are unchanged. Where the previous document stated a behaviour that the code does not show in that form, this version corrects it and says so (see §7).

---

## 1. System overview

```
PRIMARY WORKSPACE (source)                      DR TARGET WORKSPACE
━━━━━━━━━━━━━━━━━━━━━━━━━━                      ━━━━━━━━━━━━━━━━━━━━━

Unity Catalog                                   Unity Catalog
  ├── Storage credentials                          ├── Storage credentials
  ├── External locations                           ├── External locations
  ├── Catalogs                                     ├── Catalogs
  ├── Schemas                                      ├── Schemas
  ├── Delta tables                                 ├── Delta tables (via landing zone)
  ├── External volumes                             ├── External volumes
  ├── Views                                        ├── Views
  └── Grants                                       └── Grants

                          ┌─────────────────────┐
                          │ DR Landing Zone     │
                          │ (ADLS Gen2, shared) │
                          │  └── tables/        │
                          └─────────────────────┘
```

**Reading the diagram.** Nothing is copied directly from one metastore to the other. Metadata is recreated through the Databricks SDK and REST API, table data is written to a shared ADLS landing zone by DEEP CLONE and then registered in the target, and grants are diffed and patched. The landing zone must be accessible from both workspaces.

---

## 2. End-to-end flow

Two workflows, run in order. Workflow 2 must not run before Workflow 1 has been checked.

### 2.1 Workflow 1 — `wf_metadata_sync_dr` (metadata skeleton first)

| Task | Notebook | What it does |
|---|---|---|
| 1. `sync_creds_and_locations` | `01_sync_creds_and_locations.py` | Lists storage credentials and external locations in the source via the SDK, compares by name, and creates only the missing ones in the target. URLs are translated with `external_loc_mapping.csv`; credential names with `azure_cred_mapping.csv`. Credential *creation* is off by default (`create_credential_flag=false`). |
| 2. `sync_catalogs_and_schemas` (depends on Task 1) | `02_sync_catalogs_and_schemas.py` | Lists catalogs and schemas in the source, compares by name, and creates the missing ones in the target. Shared and external catalogs are skipped. Schemas listed in the `schemas_to_ignore` widget parameter (default `default,information_schema`) are excluded. Mappings come from `catalog_mapping.csv` and `schema_mapping.csv`. |

**Why this order.** Credentials and external locations must exist in the target before catalogs and schemas that reference their storage roots can be created. Checking the skeleton at this point — before any data is copied — is the cheap place to catch a mapping error.

### 2.2 Workflow 2 — `wf_deepclone_dr` (data and objects second)

| Task | Notebook | What it does |
|---|---|---|
| 1. `sync_tables` | `03_sync_tables.py` | Phase 1: DEEP CLONE Delta tables to the DR landing zone (source warehouse). Phase 2: DROP stale external tables in the target. Phase 3: LOAD (register) tables in the target metastore from the landing zone. See §3.1 for the important scope limit in Phase 3. |
| 2. `sync_ext_volumes` (depends on Task 1) | `04_sync_ext_volumes.py` | Lists external volumes from the source `information_schema`, translates storage paths with `volume_mapping.csv`, creates them in the target, and transfers ownership to match the source. |
| 3. `sync_views` (depends on Task 2) | `05_sync_views.py` | Reads view DDL with `SHOW CREATE TABLE` on the source, rewrites the source catalog name to the target catalog name, converts it to `CREATE OR REPLACE VIEW`, and executes it on the target warehouse, polling until it succeeds or fails. |
| 4. `sync_permissions` (depends on Task 3) | `06_sync_permissions.py` | Reads effective permissions from source and target via the Unity Catalog REST API, diffs them per principal, and PATCHes only the delta on the target. Covers catalogs, schemas, tables, and volumes. |

**Why permissions run last.** Grants are applied to objects that must already exist in the target. Running permissions earlier would attempt to grant on objects that have not been created yet.

### 2.3 Common steps (every notebook)

1. **Secrets.** Source and target workspace hosts and tokens are read from the Databricks Secret Scope `adls{env}scope` (`SourceHostKey`, `SourceSPTokenKey`, `TargetHostKey`, `TargetSPTokenKey`). No host or token is hardcoded.
2. **Mapping files.** All source → target translations come from CSVs uploaded to a Unity Catalog Volume (default `/Volumes/uc_dev/default/dr_mapping_files/`). Changing the DR target means editing a CSV, not notebook code.
3. **Reporting.** Each notebook prints a run summary (counts, successes, failures, duration). In this version the notebooks do **not** write rows to a status table, even though `status_database` / `status_tbl` parameters are accepted. Operational visibility comes from workflow run history and printed output only.

---

## 3. What is synced, and what is not

| Area | Synced | How | Limit |
|---|---|---|---|
| Storage credentials, external locations | Yes | SDK, compare by name, create missing | Credential creation is off by default; see §2.1 |
| Catalogs, schemas | Yes | SDK, compare by name | Shared/external catalogs skipped; ignored schemas excluded |
| Delta tables | Partly — see §3.1 | DEEP CLONE to landing zone, then register | Only DELTA-format tables are selected; views are excluded from the table sync |
| External volumes | Yes | SDK, path translation, ownership transfer | External volumes only (`volume_type == "EXTERNAL"`) |
| Views | Yes | DDL copy with catalog-name replacement | Straight name replacement only — views referencing objects outside the mapped catalogs need a manual check |
| Grants | Yes | REST API, effective-permission diff, PATCH delta | Only the securable types the notebook can resolve; see §7 |

### 3.1 Table sync scope — read this before relying on it

Notebook 03 selects tables from `system.information_schema.tables` where the table type is not a view and `data_source_format == "DELTA"`. All such tables are DEEP CLONED to the landing zone in Phase 1. In Phase 3, however, `load_table` only registers a table when its type is `EXTERNAL`, using `CREATE TABLE … USING delta LOCATION …`; other types are printed as skipped and are not registered in the target metastore by that function.

That means, as the code stands, cloned data for non-external Delta tables lands in the DR landing zone but is not registered as a table in the target catalog by Phase 3. Verify this behaviour against your own catalogs before a DR test, and treat the previous architecture document's unqualified "DEEP CLONE then LOAD" description as incomplete. If managed-table registration is required, that step needs to be added or confirmed in the target workspace — it is not in Notebook 03 as supplied.

---

## 4. Design decisions

### 4.1 Two-workflow split: metadata first, data second

**Decision.** Two separate workflows, with an explicit human checkpoint between them, instead of one long job.

**Why.** The metadata skeleton is cheap to build and easy to verify. The table clone is the expensive, destructive-adjacent step (it drops stale external tables in the target). Splitting them lets an operator catch a wrong mapping before any data moves.

**Trade-off.** Two jobs to deploy, schedule, and monitor, and no automatic gate prevents Workflow 2 from being run first. The run order is a documented procedure, not an enforced dependency.

### 4.2 CSV-driven mapping, not hardcoded names

**Decision.** Every source → target translation lives in a CSV on a Volume.

**Why.** A DR target change (new catalog names, new storage URLs) is a data change, not a code change, and the same notebooks serve dev, UAT, and production pairs by pointing at different CSVs and a different Secret Scope environment.

**Trade-off.** The CSVs are now part of the deployment. A stale or partially edited CSV is a silent misconfiguration risk — hence the checkpoint after Workflow 1.

### 4.3 Compare by name, not by ID

**Decision.** All existence checks compare object names.

**Why.** Catalog, schema, credential, and location IDs differ between metastores. Names are the only stable identifier across workspaces.

**Trade-off.** Renaming an object in the source looks like a delete-plus-create to the sync. It will not be matched to the old target object by name.

### 4.4 DEEP CLONE to a shared landing zone

**Decision.** Clone to an ADLS container accessible from both workspaces, then register from there.

**Why.** The clone is written from the primary and read from the DR workspace without extra data movement, and Delta history is preserved in the cloned data for point-in-time recovery.

**Trade-off.** The landing zone becomes a shared dependency with its own access and lifecycle. Cloned data remains there after a run; retention and cleanup of old clones are not handled by these notebooks.

### 4.5 Parallelism with `ThreadPoolExecutor`, default 1 worker

**Decision.** Bulk operations (tables, volumes, views, permissions) submit work through `ThreadPoolExecutor` with a configurable `num_exec` worker count.

**Why.** Throughput can be tuned against warehouse and API rate limits without code changes.

**Trade-off.** The workflow default for `num_exec` is `"1"`, so parallelism is effectively off unless the operator raises it. Raising it increases concurrent warehouse statements and API calls — tune it against the warehouses in use, and note that the notebooks do not retry rate-limit errors beyond statement polling.

### 4.6 Idempotent re-runs (with limits)

**Decision.** Objects that already exist are skipped, `CREATE OR REPLACE` is used where supported, and explicit existence checks are used elsewhere, so a failed or partial run can be re-run.

**Why.** DR syncs are long-running and will be interrupted. A re-run should not duplicate objects or fail on objects that already exist.

**Trade-off.** Idempotency here means "safe to re-run", not "no-op on re-run". DEEP CLONE re-runs rewrite the landing-zone data, and Phase 2 drops and re-creates external tables. A re-run is a full re-sync of the tables in scope, not an incremental one.

### 4.7 Delta-only grants from effective permissions

**Decision.** Read *effective* (not direct) grants from both workspaces, diff per principal, and PATCH only the changed grants.

**Why.** A full overwrite would wipe grants set directly on the target. Diffing effective permissions preserves target-local grants while converging the rest.

**Trade-off.** Effective permissions are a computed view. If the source and target resolve the same principal differently (for example, group membership differences), the diff can include grants that look changed but are equivalent — review the printed change list on the first run.

### 4.8 Direct REST API for permissions

**Decision.** Call `/api/2.1/unity-catalog/effective-permissions/` and `/api/2.1/unity-catalog/permissions/` directly with `requests`, instead of using the SDK.

**Why.** The SDK does not expose effective permissions consistently across versions, and the API requires an exact securable-type string, so the notebook tries securable-type variants in order.

**Trade-off.** A future API change breaks this notebook in a way an SDK upgrade would have absorbed. Pin the behaviour by testing permissions sync after any Databricks runtime or API change.

### 4.9 Secrets via Secret Scope, never hardcoded

**Decision.** Hosts and tokens come from `adls{env}scope` at run time, keyed by environment (`dev`, `uat`, `prod`).

**Why.** The same notebooks run against different workspace pairs with no code change, and no credential value appears in the repository, the YAMLs, or the logs.

**Note.** The token *values* are read on the driver and then used by the SDK and REST clients. Rotating a secret requires a fresh run, not a code change — but a long-running clone that started before a rotation keeps using the token it started with.

### 4.10 Statement polling with a long backoff — confirm before running

**Decision.** Warehouse statements are polled in a `PENDING`/`RUNNING` loop, sleeping `response_backoff_param` between polls.

**Why.** DEEP CLONE and view creation are long-running statements; polling is the correct pattern.

**What to confirm.** The workflow default for `response_backoff_param` is `"300"`, and the code passes it directly to `time.sleep()`, i.e. 300 seconds between polls. That makes status checks very infrequent. Confirm whether that value is intended as written, or whether it should be a much smaller polling interval, before the first long run — a 300-second poll interval will make a clone look stalled in the logs for long stretches even while it is progressing.

---

## 5. Data and mapping files

| File | Purpose | Key columns |
|---|---|---|
| `catalog_mapping.csv` | Source catalog → target catalog, and the target storage root | `source_catalog`, `target_catalog`, `target_storage_root` |
| `schema_mapping.csv` | Schemas included in the table sync | `source_schema`, `target_schema` |
| `external_loc_mapping.csv` | External location name and URL translation | `source_loc_name`, `target_loc_name`, `target_url` |
| `volume_mapping.csv` | Storage-path prefix translation for volumes | `source_loc`, `target_loc` |
| `azure_cred_mapping.csv` | Credential auth config for the DR workspace | `source_cred_name`, `target_cred_name`, managed-identity / service-principal fields |

Rules that matter in practice:

- Only one auth method should be populated per credential row: a managed-identity connector, a managed-identity identity, or a service principal.
- The mapping files in this repository are **examples with placeholders** (`yourstorageaccount`, `<your-service-principal-id>`, `<sub-id>`, warehouse IDs). Replace every placeholder before running against a real workspace.
- Only schemas listed in `schema_mapping.csv` are copied by Notebook 03. A schema that exists in the source but is not in that file is silently out of scope for the table sync, even though Notebook 02 may have created it in the target.

---

## 6. Configuration

| Parameter | Where | Default | Notes |
|---|---|---|---|
| `env` | Both workflows | `dev` | Selects the Secret Scope suffix and, by convention, the workspace pair. |
| `create_credential_flag` | Workflow 1 | `"false"` | Credential creation in the target is off unless explicitly enabled. |
| `schemas_to_ignore` | Workflow 1 | `"default,information_schema"` | Comma-separated; no code change needed to exclude a schema. |
| `num_exec` | Workflow 2 | `"1"` | ThreadPool worker count. Raise deliberately; see §4.5. |
| `wait_timeout` | Workflow 2 | `30s` | Passed to the warehouse statement-execution API. |
| `response_backoff_param` | Workflow 2 | `"300"` | Sleep between statement polls — confirm the unit/intent; see §4.10. |
| `source_catalog` / `target_catalog` | Workflow 2 | `uc_dev` / `uc_dev_dr` | The catalog pair being synced. |
| `landing_zone_url`, `source_landing_url`, `target_landing_url` | Workflow 2 | Placeholder ADLS URLs | Replace with your own landing zones. |
| `w_source_id` / `w_target_id` | Workflow 2 | `<your-source-warehouse-id>` / `<your-target-warehouse-id>` | Replace with real SQL warehouse IDs. |
| `run_as` | Both workflows | `<your-service-principal-id>` | Replace with the service principal that runs the jobs. |
| `status_database` / `status_tbl`, `ops_database` / `ops_table` | Both workflows | `ops` / `tbl_run_status`, `ops` / `ops_application_metadata` | Accepted but not written to by these notebooks. |
| Cluster | Both workflows | Runtime `14.3.x-scala2.12`, `Standard_D4ds_v5`, Spot with fallback, autoscale 2–8 workers, dedicated data security mode | As defined in the YAMLs. |
| Schedule | Neither workflow defines one | — | These jobs are run on demand, in order. There is no schedule to pause or enable. |

---

## 7. Failure modes, gaps, and limitations

Read this section before operating the framework. Each item is visible in the supplied files.

1. **`shared_functions` wheel is referenced but not included — and its version differs.** The metadata workflow references `shared_functions-0.7`, the deep-clone workflow references `0.7` for three tasks and `0.4` for `sync_tables`. The wheel is not in this repository and is not imported by any notebook in it. Remove the library entries, or point them at your own wheel and confirm which version is intended — the version split across tasks looks like drift, not a deliberate choice.
2. **No status-table write.** The notebooks accept `status_database` / `status_tbl` but only print a summary. There is no queryable run history in a table; use workflow run history and logs.
3. **Table registration scope (§3.1).** Only `EXTERNAL` tables are registered in the target by Phase 3 as the code stands. Do not assume every cloned Delta table appears as a table in the target catalog until you have verified it.
4. **Views are name-replaced, not dependency-checked.** View DDL is copied with a straight catalog-name replacement. Views referencing objects outside the mapped catalogs, or depending on objects not yet synced, will fail or point at the wrong object. Check view failures in the printed output after Workflow 2.
5. **Permissions depend on securable-type resolution.** The notebook tries securable-type variants because the API requires an exact string. An object whose type cannot be determined is reported and skipped — it is not granted. Check the permissions output for skipped objects.
6. **Destructive step in the target.** Phase 2 drops stale external tables in the **target** workspace before re-creating them. Review the mapping files and run order before every run, and test in a dev pair of workspaces first.
7. **No retries beyond polling.** Statement polling waits for a statement to finish; it does not retry a failed statement, and API rate-limit or transient errors in the parallel loops surface as per-object failures in the printed summary. A run that reports failures needs a re-run, not a assumption that the objects eventually synced.
8. **No schedule, no automatic ordering.** Nothing in the YAMLs prevents Workflow 2 from running before Workflow 1, or from running twice. The order is a procedure to follow, not a system guarantee.
9. **Catalog scope in Notebook 03.** The notebook iterates over catalogs supplied to it and filters by the schema-mapping file. A catalog that is created by Notebook 02 but not passed into Notebook 03's scope will not have its tables cloned.

---

## 8. Performance — what is known and what is not

**Known from the code.** Bulk work is parallelised through `ThreadPoolExecutor` with a configurable worker count, but the workflow default is 1 worker, so the default run is effectively serial at the thread level (warehouse statements themselves still run on the warehouse). DEEP CLONE cost is dominated by the size of the tables being cloned and by warehouse capacity, neither of which is stated in this repository.

**Not known from the supplied files.** No timed run, table count, data volume, or duration was supplied. This document therefore claims no runtime, throughput, or cost figure for a DR sync. Do not quote one from this repository without running a timed DR test and recording the catalog count, table count, total data size, `num_exec` value, warehouse sizes, and wall-clock duration.

**How to measure it.** Run Workflow 1 and Workflow 2 against a dev pair of workspaces with a known table count and data size, record the printed per-notebook durations, and note any failures by object. Only then add a performance table, labelled with the data size, environment, and date of the run.

---

## 9. Deployment and operations checklist

- [ ] Mapping CSVs uploaded to a Unity Catalog Volume and every placeholder replaced for your own primary and DR workspaces.
- [ ] Secret Scope `adls{env}scope` created in the workspace that runs the jobs, with all four keys.
- [ ] `<your-service-principal-id>` replaced in both YAMLs, and the service principal has read access in the source and create access in the target.
- [ ] SQL warehouse IDs (`w_source_id`, `w_target_id`) replaced with real IDs.
- [ ] `shared_functions` wheel entries removed or corrected, and the version split (§7.1) resolved.
- [ ] `response_backoff_param` confirmed (§4.10) and `num_exec` set deliberately (§4.5).
- [ ] Notebooks uploaded to `/Workspace/projects/dr_metadatasync/` (01–02) and `/Workspace/projects/dr_deepclone/` (03–06), matching the YAML paths.
- [ ] Full sequence tested in a dev pair of workspaces first, including a check that cloned tables appear in the target catalog as expected (§3.1) and that views and grants landed.
- [ ] Workflow 1 run and skeleton verified in the DR workspace before Workflow 2 is run.
- [ ] After Workflow 2, check the printed summaries for failed tables, skipped views, and skipped permission objects — a run with failures is not a complete DR sync.

---

## 10. File inventory

| Path | What it is | Changed in this repository? |
|---|---|---|
| `notebooks/01_sync_creds_and_locations.py` … `06_sync_permissions.py` | The six sync notebooks | No — as supplied |
| `workflows/wf_metadata_sync_dr.yml`, `workflows/wf_deepclone_dr.yml` | Databricks Workflow definitions | No — as supplied, including placeholder service principal and warehouse IDs |
| `data/mapping_files/*.csv` | Example mapping files with placeholders | No — as supplied |
| `docs/architecture.md` | This document | **Yes — rewritten** at the author's request to be professional and checkable; see the status note at the top |
| `README.md`, `.gitignore`, `LICENSE`, `requirements.txt` | Project README and professional extras | No change in this step |

---

## Author

**Sanjeev Kumar Pandey** — Data Engineer → AI Engineer | Azure Databricks | PySpark | RAG | Agentic AI

[LinkedIn](https://www.linkedin.com/in/sanjeev-pandey-7a45831ba) · [GitHub](https://github.com/sanjeevpandeydata)
