# Databricks notebook source
# ============================================================
# NOTEBOOK : sync_views.py
# PURPOSE  : Sync Unity Catalog views from primary workspace
#            to DR target workspace.
#            Reads the view DDL from source using SHOW CREATE
#            TABLE, rewrites the catalog reference, and
#            executes CREATE OR REPLACE VIEW on the target
#            workspace via SQL warehouse API.
# AUTHOR   : Sanjeev Kumar Pandey
# ============================================================

# COMMAND ----------
import time
import pandas as pd
from itertools import repeat
from databricks.sdk import WorkspaceClient
from concurrent.futures import ThreadPoolExecutor
from databricks.sdk.service.sql import StatementState

start = time.time()

# COMMAND ----------
# ── Widget Parameters ─────────────────────────────────────────
dbutils.widgets.text("env",                    "")
env                    = dbutils.widgets.get("env")

dbutils.widgets.text("catalog_mapping_file",   "")
catalog_mapping_path   = dbutils.widgets.get("catalog_mapping_file")

dbutils.widgets.text("num_exec",               "")
num_exec               = int(dbutils.widgets.get("num_exec"))

dbutils.widgets.text("w_target_id",            "")
warehouse_id           = dbutils.widgets.get("w_target_id")

dbutils.widgets.text("response_backoff_param", "")
response_backoff       = int(dbutils.widgets.get("response_backoff_param"))

dbutils.widgets.text("pl_or_wl_name",          "")
pl_or_wl_name          = dbutils.widgets.get("pl_or_wl_name")

dbutils.widgets.text("status_tbl",             "")
status_tbl             = dbutils.widgets.get("status_tbl")

# COMMAND ----------
# ── Secrets ───────────────────────────────────────────────────
target_host  = dbutils.secrets.get(scope=f"adls{env}scope", key="TargetHostKey")
target_token = dbutils.secrets.get(scope=f"adls{env}scope", key="TargetSPTokenKey")

# COMMAND ----------
# ── Helper: Create a single view in target workspace ──────────
# 1. Reads the view DDL from source using SHOW CREATE TABLE
# 2. Replaces the source catalog name with target catalog name
# 3. Converts CREATE VIEW → CREATE OR REPLACE VIEW for idempotency
# 4. Executes on the target workspace via statement_execution API
# 5. Polls until the statement completes (PENDING → RUNNING → SUCCEEDED/FAILED)

def create_view(w, target_catalog, source_catalog, schema, view_name, warehouse):
    try:
        # Read DDL from source and translate catalog reference
        view_stmt = (
            spark.sql(
                f"SHOW CREATE TABLE {source_catalog}.{schema}.{view_name}"
            ).collect()[0]["createtab_stmt"]
            .replace(source_catalog, target_catalog)
            .replace("CREATE", "CREATE OR REPLACE")
        )

        resp = w.statement_execution.execute_statement(
            warehouse_id=warehouse,
            statement=view_stmt)

        # Poll until complete
        while resp.status.state in {StatementState.PENDING, StatementState.RUNNING}:
            resp = w.statement_execution.get_statement(resp.statement_id)
            time.sleep(response_backoff)

        if resp.status.state != StatementState.SUCCEEDED:
            return {
                "catalog":       target_catalog,
                "schema":        schema,
                "view_name":     view_name,
                "status":        f"FAIL: {resp.status.error.message}",
                "creation_time": time.time_ns()
            }

        return {
            "catalog":       target_catalog,
            "schema":        schema,
            "view_name":     view_name,
            "status":        "SUCCESS",
            "creation_time": time.time_ns()
        }

    except Exception as e:
        return {
            "catalog":       target_catalog,
            "schema":        schema,
            "view_name":     view_name,
            "status":        f"FAIL: {e}",
            "creation_time": time.time_ns()
        }

# COMMAND ----------
# ── Main: Read mapping, query views, create in target ─────────
try:
    catalog_df = pd.read_csv(catalog_mapping_path, keep_default_na=False)

    if "source_catalog" not in catalog_df.columns or "target_catalog" not in catalog_df.columns:
        raise ValueError("catalog_mapping.csv must contain 'source_catalog' and 'target_catalog'")

    catalogs_to_copy = catalog_df["source_catalog"].tolist()

    # Query all views from source catalog(s) via system.information_schema
    catalog_filter = "', '".join(catalogs_to_copy)
    all_views      = spark.sql(
        f"SELECT * FROM system.information_schema.views "
        f"WHERE table_catalog IN ('{catalog_filter}')")

    w_target = WorkspaceClient(host=target_host, token=target_token)

    # Tracking lists
    loaded_view_names   = []
    loaded_view_schemas = []
    loaded_view_catalogs = []
    loaded_view_status  = []
    loaded_view_times   = []

    for cat in catalogs_to_copy:
        filtered_views = all_views.filter(
            (all_views.table_catalog == cat) &
            (all_views.table_schema  != "information_schema")
        ).collect()

        catalog_mapping = catalog_df[catalog_df["source_catalog"] == cat]
        if catalog_mapping.empty:
            print(f"No mapping for catalog '{cat}', skipping.")
            continue

        target_catalog_name = catalog_mapping["target_catalog"].iloc[0]
        source_catalog_name = catalog_mapping["source_catalog"].iloc[0]

        schemas    = [row["table_schema"] for row in filtered_views]
        view_names = [row["table_name"]   for row in filtered_views]

        print(f"Catalog '{cat}': {len(view_names)} views to sync")

        # Create views in parallel using ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=num_exec) as executor:
            threads = executor.map(
                create_view,
                repeat(w_target),
                repeat(target_catalog_name),
                repeat(source_catalog_name),
                schemas,
                view_names,
                repeat(warehouse_id))

            for thread in threads:
                loaded_view_names.append(thread["view_name"])
                loaded_view_schemas.append(thread["schema"])
                loaded_view_catalogs.append(thread["catalog"])
                loaded_view_status.append(thread["status"])
                loaded_view_times.append(thread["creation_time"])

                status_label = "✅" if thread["status"] == "SUCCESS" else "❌"
                print(f"{status_label} {thread['catalog']}.{thread['schema']}.{thread['view_name']} "
                      f"— {thread['status']}")

    # Summary DataFrame
    status_df = pd.DataFrame({
        "catalog":   loaded_view_catalogs,
        "schema":    loaded_view_schemas,
        "view":      loaded_view_names,
        "status":    loaded_view_status,
        "sync_time": loaded_view_times
    })

    success_count = len(status_df[status_df["status"] == "SUCCESS"])
    failed_count  = len(status_df[status_df["status"].str.contains("FAIL")])

    end          = time.time()
    run_duration = end - start

except Exception as e:
    raise SystemExit(f"sync_views failed: {str(e)}")

# COMMAND ----------
print(f"\nPipeline : {pl_or_wl_name}")
print(f"Views created : {success_count}")
print(f"Views failed  : {failed_count}")
print(f"Duration      : {run_duration:.2f}s")
print("Status        : SUCCESS")
