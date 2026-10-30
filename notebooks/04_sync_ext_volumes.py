# Databricks notebook source
# ============================================================
# NOTEBOOK : sync_ext_volumes.py
# PURPOSE  : Sync Unity Catalog external volumes from primary
#            workspace to DR target workspace using the
#            Databricks SDK WorkspaceClient.
#            Reads catalog and volume mapping CSVs, translates
#            storage paths, creates volumes in target workspace
#            using ThreadPoolExecutor for parallelism.
# AUTHOR   : Sanjeev Kumar Pandey
# ============================================================

# COMMAND ----------
from itertools import repeat
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import catalog
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import time

start = time.time()

# COMMAND ----------
# ── Widget Parameters ─────────────────────────────────────────
dbutils.widgets.text("catalog_mapping_file", "")
catalog_mapping_path = dbutils.widgets.get("catalog_mapping_file")

dbutils.widgets.text("volume_mapping_path",  "")
volume_mapping_path  = dbutils.widgets.get("volume_mapping_path")

dbutils.widgets.text("num_exec",             "")
num_exec             = int(dbutils.widgets.get("num_exec"))

dbutils.widgets.text("env",                  "")
env                  = dbutils.widgets.get("env")

dbutils.widgets.text("pl_or_wl_name",        "")
pl_or_wl_name        = dbutils.widgets.get("pl_or_wl_name")

dbutils.widgets.text("status_tbl",           "")
status_tbl           = dbutils.widgets.get("status_tbl")

# COMMAND ----------
# ── Secrets ───────────────────────────────────────────────────
target_host  = dbutils.secrets.get(scope=f"adls{env}scope", key="TargetHostKey")
target_token = dbutils.secrets.get(scope=f"adls{env}scope", key="TargetSPTokenKey")

# COMMAND ----------
# ── Helper: Create a single external volume in target ─────────
# If the volume already exists the exception is caught and
# logged — idempotent by design.

def create_volume(w, catalog_name, schema_name, volume_name, location, owner):
    print(f"Creating volume: {catalog_name}.{schema_name}.{volume_name}")
    try:
        vol = w.volumes.create(
            catalog_name=catalog_name,
            schema_name=schema_name,
            name=volume_name,
            storage_location=location,
            volume_type=catalog.VolumeType.EXTERNAL)

        # Transfer ownership to match source workspace
        w.volumes.update(vol.full_name, owner=owner)

        return {"volume": vol.full_name, "status": "success"}

    except Exception as e:
        return {
            "volume": f"{catalog_name}.{schema_name}.{volume_name}",
            "status": f"ERROR: {e}"
        }

# COMMAND ----------
# ── Main: Read mapping files + create volumes in parallel ──────
try:
    w_target = WorkspaceClient(host=target_host, token=target_token)

    # Read catalog mapping — controls which catalogs to process
    catalog_df = pd.read_csv(catalog_mapping_path, keep_default_na=False)

    if "source_catalog" not in catalog_df.columns or "target_catalog" not in catalog_df.columns:
        raise ValueError("catalog_mapping.csv must contain 'source_catalog' and 'target_catalog'")

    # Read volume mapping — controls storage path translation
    volume_mapping_df = pd.read_csv(volume_mapping_path, keep_default_na=False)
    print(f"Volume mapping rows: {len(volume_mapping_df)}")

    if "source_loc" not in volume_mapping_df.columns or "target_loc" not in volume_mapping_df.columns:
        raise ValueError("volume_mapping.csv must contain 'source_loc' and 'target_loc'")

    catalogs_to_copy = catalog_df["source_catalog"].tolist()
    print(f"Catalogs to process: {catalogs_to_copy}")

    # Query Unity Catalog system tables for all external volumes
    catalog_filter = "', '".join(catalogs_to_copy)
    system_info    = spark.sql(
        f"SELECT * FROM system.information_schema.volumes "
        f"WHERE volume_catalog IN ('{catalog_filter}')")

    # Path translation — replace source storage account URL with DR target URL
    source_replace = volume_mapping_df["source_loc"].iloc[0]
    target_replace = volume_mapping_df["target_loc"].iloc[0]
    print(f"Path replacement: '{source_replace}' → '{target_replace}'")

    results = []

    for cat in catalogs_to_copy:
        print(f"Processing catalog: {cat}")

        filtered_volumes = system_info.filter(
            (system_info.volume_catalog == cat) &
            (system_info.volume_schema  != "information_schema") &
            (system_info.volume_type    == "EXTERNAL")
        ).collect()

        catalog_mapping = catalog_df[catalog_df["source_catalog"] == cat]
        if catalog_mapping.empty:
            print(f"No mapping for catalog '{cat}', skipping.")
            continue

        catalog_name  = catalog_mapping["target_catalog"].iloc[0]
        schema_names  = [row["volume_schema"]  for row in filtered_volumes]
        volume_names  = [row["volume_name"]    for row in filtered_volumes]
        volume_locs   = [
            row["storage_location"].replace(source_replace, target_replace)
            for row in filtered_volumes]
        volume_owners = [row["volume_owner"]   for row in filtered_volumes]

        # Create all volumes for this catalog in parallel
        with ThreadPoolExecutor(max_workers=num_exec) as executor:
            threads = executor.map(
                create_volume,
                repeat(w_target),
                repeat(catalog_name),
                schema_names,
                volume_names,
                volume_locs,
                volume_owners)

        for result in threads:
            results.append({
                "catalog": catalog_name,
                "volume":  result["volume"],
                "status":  result["status"]
            })
            status_label = "✅" if result["status"] == "success" else "❌"
            print(f"{status_label} {result['volume']} — {result['status']}")

    # Build summary DataFrame
    df_results    = pd.DataFrame(results)
    success_count = len(df_results[df_results["status"] == "success"])
    error_count   = len(df_results[df_results["status"].str.contains("ERROR")])

    end          = time.time()
    run_duration = end - start

except Exception as e:
    raise SystemExit(f"sync_ext_volumes failed: {str(e)}")

# COMMAND ----------
print(f"\nPipeline : {pl_or_wl_name}")
print(f"Volumes created : {success_count}")
print(f"Volumes failed  : {error_count}")
print(f"Duration        : {run_duration:.2f}s")
print("Status          : SUCCESS")
