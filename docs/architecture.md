# Architecture — Unity Catalog DR Sync Framework

## End-to-End Flow

```
┌──────────────────────────────────────────────────────────────────────┐
│                        WORKFLOW 1: wf_metadata_sync_dr               │
│                        (Run first — sets up the skeleton)            │
├──────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  Task 1: 01_sync_creds_and_locations                                 │
│  ──────────────────────────────────                                  │
│  SOURCE WS                          TARGET WS (DR)                   │
│  storage_credentials.list()  ──────►  storage_credentials.create()  │
│  external_locations.list()   ──────►  external_locations.create()   │
│                                                                      │
│  CSV inputs: azure_cred_mapping.csv, external_loc_mapping.csv        │
│  Logic: compare by name → create only missing objects                │
│         URL translation via loc_mapping['target_url']                │
│         Credential translation via cred_mapping['target_cred_name']  │
│                                                                      │
│  Task 2: 02_sync_catalogs_and_schemas  (depends_on Task 1)           │
│  ──────────────────────────────────────                              │
│  SOURCE WS                          TARGET WS (DR)                   │
│  catalogs.list()             ──────►  catalogs.create()              │
│  schemas.list(catalog)       ──────►  schemas.create()               │
│                                                                      │
│  CSV inputs: catalog_mapping.csv, schema_mapping.csv                 │
│  Logic: diff by name, skip shared/external catalogs                  │
│         schemas_to_ignore list (widget param, no code change)        │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────┐
│                        WORKFLOW 2: wf_deepclone_dr                   │
│                        (Run second — syncs data + objects)           │
├──────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  Task 1: 03_sync_tables                                              │
│  ──────────────────────                                              │
│  Phase A — DEEP CLONE (on SOURCE warehouse):                         │
│    CREATE OR REPLACE TABLE delta.`{dr_landing}/{schema}/{table}`     │
│    DEEP CLONE {source_catalog}.{schema}.{table}                      │
│    → Writes to DR ADLS region (accessible from both WSs)             │
│    → ThreadPoolExecutor (configurable num_exec)                      │
│                                                                      │
│  Phase B — DROP stale external tables (on TARGET warehouse):         │
│    DROP TABLE IF EXISTS {target_catalog}.{schema}.{table}            │
│    (Required before CREATE TABLE with LOCATION)                      │
│                                                                      │
│  Phase C — LOAD in target metastore:                                 │
│    CREATE TABLE {target_catalog}.{schema}.{table}                    │
│    USING delta LOCATION '{dr_landing}/{schema}/{table}'              │
│                                                                      │
│  Task 2: 04_sync_ext_volumes  (depends_on Task 1)                    │
│  ────────────────────────────                                        │
│  system.information_schema.volumes (source) → volumes.create()       │
│  Storage path translated: source_loc → target_loc (volume_mapping)  │
│  Owner transferred via volumes.update()                              │
│                                                                      │
│  Task 3: 05_sync_views  (depends_on Task 2)                          │
│  ───────────────────────                                             │
│  SHOW CREATE TABLE {source}.{schema}.{view}  →  extract DDL          │
│  Replace source catalog name with target catalog name                │
│  CREATE OR REPLACE VIEW executed via statement_execution API         │
│  Polls PENDING/RUNNING → SUCCEEDED/FAILED                            │
│                                                                      │
│  Task 4: 06_sync_permissions  (depends_on Task 3)                    │
│  ────────────────────────────                                        │
│  REST API: GET effective-permissions (source)                        │
│  REST API: GET effective-permissions (target)                        │
│  Diff per principal → build add/remove change list                   │
│  REST API: PATCH permissions (target) — delta only, not full replace │
│  Covers: catalogs, schemas, tables, volumes                          │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Key Design Decisions

### 1. CSV-driven mapping (not hardcoded)
All source→target name and URL translations live in CSV files uploaded to a Unity Catalog Volume. Changing the DR target requires updating the CSV only — no notebook edits.

### 2. Compare by name (not by ID)
Catalog, schema, credential, and location IDs change between metastores. All existence checks compare by **name** — the only stable identifier across workspaces.

### 3. DEEP CLONE to a shared landing zone
The DR landing ADLS container is accessible from **both** primary and secondary workspaces. This allows the clone to be written from the primary and read from the secondary without cross-workspace data movement.

### 4. ThreadPoolExecutor for parallelism
All bulk operations (tables, volumes, views, permissions) use `ThreadPoolExecutor` with a configurable `num_exec` worker count. This allows tuning throughput vs. API rate limits without code changes.

### 5. Idempotent design
Every notebook is safe to re-run. Objects that already exist in the target are skipped. `CREATE OR REPLACE` is used where supported; explicit existence checks are used elsewhere.

### 6. Secrets in Databricks Secret Scope
Source and target host/token values are never hardcoded. They are read from a scoped secret (`adls{env}scope`) keyed by environment — supporting dev/uat/prod without code changes.

### 7. Delta diff for permissions (not full overwrite)
The permissions notebook reads **effective** (not direct) grants from both source and target, diffs per principal, and PATCHes only the changed grants. This avoids wiping grants set directly on the target.

### 8. Schema ignore list via widget
The `schemas_to_ignore` parameter lets operators exclude system/internal schemas (e.g. `information_schema`, `default`) without modifying notebook code.

### 9. Direct REST API for permissions
The Unity Catalog SDK does not expose `effective-permissions` consistently across versions. Direct REST API calls to `/api/2.1/unity-catalog/effective-permissions/` and `/api/2.1/unity-catalog/permissions/` are used for reliability.

### 10. Two-workflow split (metadata first, data second)
Splitting into `wf_metadata_sync_dr` and `wf_deepclone_dr` allows the metadata skeleton (catalogs, schemas) to be verified before running the more expensive table clone operations.
