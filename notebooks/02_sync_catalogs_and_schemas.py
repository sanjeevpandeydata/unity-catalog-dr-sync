# Databricks notebook source
# ============================================================
# NOTEBOOK : sync_catalogs_and_schemas.py
# PURPOSE  : Sync Unity Catalog catalogs and schemas from a
#            primary workspace to a DR target workspace.
#            Reads catalog_mapping.csv and schema_mapping.csv,
#            compares source vs target by name, and creates
#            any missing catalogs and schemas in the target.
# AUTHOR   : Sanjeev Kumar Pandey
# ============================================================

# COMMAND ----------
from databricks.sdk import WorkspaceClient
import pandas as pd
import json
import time

start = time.time()

# COMMAND ----------
# ── Widget Parameters ─────────────────────────────────────────
dbutils.widgets.text("catalog_mapping_file",      "")
catalog_mapping_file      = dbutils.widgets.get("catalog_mapping_file")

dbutils.widgets.text("schema_mapping_file",       "")
schema_mapping_file       = dbutils.widgets.get("schema_mapping_file")

dbutils.widgets.text("env",                       "")
env                       = dbutils.widgets.get("env")

dbutils.widgets.text("status_database",           "")
status_database           = dbutils.widgets.get("status_database")

dbutils.widgets.text("status_tbl",                "")
status_tbl                = dbutils.widgets.get("status_tbl")

dbutils.widgets.text("schemas_to_ignore",         "")
schemas_to_ignore_raw     = dbutils.widgets.get("schemas_to_ignore")

dbutils.widgets.text("pipeline_or_workflow_name", "")
pl_or_wl_name             = dbutils.widgets.get("pipeline_or_workflow_name")

# COMMAND ----------
# ── Secrets ───────────────────────────────────────────────────
source_host = dbutils.secrets.get(scope=f"adls{env}scope", key="SourceHostKey")
source_pat  = dbutils.secrets.get(scope=f"adls{env}scope", key="SourceSPTokenKey")
target_host = dbutils.secrets.get(scope=f"adls{env}scope", key="TargetHostKey")
target_pat  = dbutils.secrets.get(scope=f"adls{env}scope", key="TargetSPTokenKey")

w_source = WorkspaceClient(host=source_host, token=source_pat)
w_target = WorkspaceClient(host=target_host, token=target_pat)

# COMMAND ----------
# ── Counters ──────────────────────────────────────────────────
catalogs_created_count        = 0
catalogs_skipped_count        = 0
catalogs_shared_skipped_count = 0
catalogs_failed_count         = 0
schemas_created_count         = 0
total_source_schemas_count    = 0
schemas_skipped_count         = 0
schemas_ignored_count         = 0
schemas_failed_count          = 0
schemas_already_in_target     = 0
total_target_schemas_count    = 0

# COMMAND ----------
# ── Step 1-3: Catalog sync ────────────────────────────────────
# Read mapping CSV, list catalogs in source and target,
# find the diff, and create any missing catalogs in target.

try:
    print("Step1: Starting catalog sync")

    catalog_df        = pd.read_csv(catalog_mapping_file, keep_default_na=False)
    catalogs_from_copy = catalog_df["source_catalog"].tolist()
    catalogs_to_copy   = catalog_df["target_catalog"].tolist()

    source_catalogs    = [x for x in w_source.catalogs.list()
                          if x.name in catalogs_from_copy]
    target_catalogs    = [x for x in w_target.catalogs.list()
                          if x.name in catalogs_to_copy]

    print(f"Step2: Source catalogs matched: {len(source_catalogs)} | "
          f"Target catalogs matched: {len(target_catalogs)}")

    target_catalog_names = [x.name for x in target_catalogs]
    catalog_diff         = list(set(catalogs_to_copy) - set(target_catalog_names))
    print(f"Step3: Catalogs to create: {len(catalog_diff)}")

    if not catalog_diff:
        print("Step4: All catalogs already exist in target metastore.")
        catalogs_skipped_count = len(target_catalogs)
    else:
        filtered_df       = catalog_df[catalog_df["target_catalog"].isin(catalog_diff)]
        newcat_in_source  = filtered_df["source_catalog"].tolist()
        catalogs_to_create = [x for x in source_catalogs if x.name in newcat_in_source]

        for cat in catalogs_to_create:
            catalog_name = catalog_df["target_catalog"].loc[
                catalog_df["source_catalog"] == cat.name].iloc[0]

            # Skip external/shared catalogs — they cannot be cloned directly
            if cat.share_name:
                print(f"Skipping shared/external catalog: {cat.name}")
                catalogs_shared_skipped_count += 1
                continue

            catalog_comment    = cat.comment
            catalog_properties = cat.properties
            print(f"Step5: Creating catalog '{catalog_name}'")

            try:
                storage_root = catalog_df["target_storage_root"].loc[
                    catalog_df["target_catalog"] == catalog_name].iloc[0]
            except (KeyError, IndexError):
                print(f"ERROR: No storage root found for '{catalog_name}' — check mapping file.")
                catalogs_failed_count += 1
                break

            if storage_root:
                w_target.catalogs.create(
                    name=catalog_name,
                    comment=catalog_comment,
                    properties=catalog_properties,
                    storage_root=storage_root)
            else:
                w_target.catalogs.create(
                    name=catalog_name,
                    comment=catalog_comment,
                    properties=catalog_properties)

            print(f"Step6: Created catalog '{catalog_name}'")
            catalogs_created_count += 1

    # COMMAND ----------
    # ── Step 8-12: Schema sync ────────────────────────────────
    # For each source catalog, list schemas, skip ignored ones,
    # skip already existing ones, create the rest in target.

    schemas_to_ignore = {
        s.strip()
        for s in schemas_to_ignore_raw.split(",")
        if s.strip()
    }
    print(f"Step8: Schemas to ignore: {schemas_to_ignore}")

    for cat in source_catalogs:
        source_schemas      = list(w_source.schemas.list(cat.name))
        source_schema_names = [x.name for x in source_schemas]
        total_source_schemas_count += len(source_schemas)

        catalog_name = catalog_df["target_catalog"].loc[
            catalog_df["source_catalog"] == cat.name].iloc[0]

        target_schemas      = list(w_target.schemas.list(catalog_name))
        target_schema_names = [x.name for x in target_schemas]
        total_target_schemas_count += len(target_schemas)

        target_storage_root = catalog_df["target_storage_root"].loc[
            catalog_df["source_catalog"] == cat.name].iloc[0]

        print(f"Step9: Catalog '{catalog_name}': "
              f"{len(source_schemas)} schema(s) in source")

        for schema in source_schemas:
            schema_name       = schema.name
            schema_comment    = schema.comment
            schema_properties = schema.properties

            # Skip system / internal schemas configured in the widget
            if schema_name in schemas_to_ignore:
                print(f"Step10: Skipping '{catalog_name}.{schema_name}' "
                      f"— in ignore list")
                schemas_ignored_count += 1
                continue

            # Skip schemas that already exist in target
            if schema_name in target_schema_names:
                print(f"Step10: Skipping '{catalog_name}.{schema_name}' "
                      f"— already in target")
                schemas_already_in_target += 1
                continue

            print(f"Step10: Creating schema '{catalog_name}.{schema_name}'")

            try:
                if target_storage_root:
                    w_target.schemas.create(
                        name=schema_name,
                        catalog_name=catalog_name,
                        comment=schema_comment,
                        properties=schema_properties,
                        storage_root=target_storage_root)
                else:
                    w_target.schemas.create(
                        name=schema_name,
                        catalog_name=catalog_name,
                        comment=schema_comment,
                        properties=schema_properties)

                print(f"Step11: Created schema '{catalog_name}.{schema_name}'")
                schemas_created_count += 1

            except Exception as e:
                print(f"ERROR creating schema "
                      f"'{catalog_name}.{schema_name}': {str(e)}")
                schemas_failed_count += 1
                continue

    end          = time.time()
    run_duration = end - start

    summary = {
        "run_duration_seconds":      round(run_duration, 2),
        "catalog_total_in_mapping":  len(catalogs_from_copy),
        "catalog_found_in_source":   len(source_catalogs),
        "catalog_already_in_target": catalogs_skipped_count,
        "catalog_created":           catalogs_created_count,
        "catalog_failed":            catalogs_failed_count,
        "catalog_shared_skipped":    catalogs_shared_skipped_count,
        "schema_found_in_source":    total_source_schemas_count,
        "schema_already_in_target":  schemas_already_in_target,
        "schema_ignored":            schemas_ignored_count,
        "schema_created":            schemas_created_count,
        "schema_failed":             schemas_failed_count,
    }

except Exception as e:
    raise SystemExit(f"Catalog/Schema sync failed: {str(e)}")

# COMMAND ----------
print(f"\nPipeline : {pl_or_wl_name}")
print(f"Catalogs created  : {catalogs_created_count}")
print(f"Schemas  created  : {schemas_created_count}")
print(f"Schemas  ignored  : {schemas_ignored_count}")
print(f"Duration : {run_duration:.2f}s")
print("Status   : SUCCESS")
print(json.dumps(summary, indent=2))
