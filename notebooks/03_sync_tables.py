# Databricks notebook source
# ============================================================
# NOTEBOOK : sync_tables.py
# PURPOSE  : Sync Delta tables from primary workspace to DR
#            target workspace using DEEP CLONE via Databricks
#            SQL warehouse statement execution API.
#            Three phases: DEEP CLONE to landing zone →
#            DROP stale external tables in target →
#            LOAD tables in target from cloned location.
# AUTHOR   : Sanjeev Kumar Pandey
# ============================================================

# COMMAND ----------
import time
import pandas as pd
from itertools import repeat
from databricks.sdk import WorkspaceClient
from concurrent.futures import ThreadPoolExecutor
from databricks.sdk.service.sql import StatementState
import json
import os

start = time.time()

# COMMAND ----------
# ── Widget Parameters ─────────────────────────────────────────
dbutils.widgets.text("env",                 "")
env                 = dbutils.widgets.get("env")

dbutils.widgets.text("num_exec",            "")
num_exec            = int(dbutils.widgets.get("num_exec"))

dbutils.widgets.text("schema_mapping_file", "")
schema_mapping      = dbutils.widgets.get("schema_mapping_file")

dbutils.widgets.text("source_catalog",      "")
source_catalog      = dbutils.widgets.get("source_catalog")
catalogs_to_copy    = source_catalog.split(",")

dbutils.widgets.text("target_catalog",      "")
target_catalog      = dbutils.widgets.get("target_catalog")

dbutils.widgets.text("landing_zone_url",    "")
landing_zone_url    = dbutils.widgets.get("landing_zone_url")

dbutils.widgets.text("w_source_id",         "")
w_source_id         = dbutils.widgets.get("w_source_id")

dbutils.widgets.text("w_target_id",         "")
w_target_id         = dbutils.widgets.get("w_target_id")

dbutils.widgets.text("wait_timeout",        "")
wait_timeout        = dbutils.widgets.get("wait_timeout")

dbutils.widgets.text("response_backoff_param", "")
response_backoff    = int(dbutils.widgets.get("response_backoff_param"))

dbutils.widgets.text("pl_or_wl_name",       "")
pl_or_wl_name       = dbutils.widgets.get("pl_or_wl_name")

dbutils.widgets.text("status_tbl",          "")
status_tbl          = dbutils.widgets.get("status_tbl")

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
DELTA_table_count        = 0
Table_skipped_count      = 0
Table_created_count      = 0
Table_copied_count       = 0
Table_dropped_count      = 0
Error_dropping_table_count = 0

# COMMAND ----------
# ── Helper: DEEP CLONE table to landing zone ──────────────────
# Runs via SQL warehouse on the SOURCE workspace.
# Writes cloned data to the DR landing zone (secondary region).
# Uses ThreadPoolExecutor for parallel submission.

def copy_table(w, catalog, schema, table_name, table_type, bucket, warehouse, wait_timeout):
    try:
        sql = (f"CREATE OR REPLACE TABLE delta.`{bucket}/{schema}/{table_name}` "
               f"DEEP CLONE {catalog}.{schema}.{table_name}")

        resp = w.statement_execution.execute_statement(
            warehouse_id=warehouse,
            wait_timeout=wait_timeout,
            statement=sql)

        while resp.status.state in {StatementState.PENDING, StatementState.RUNNING}:
            resp = w.statement_execution.get_statement(resp.statement_id)
            time.sleep(response_backoff)

        return {
            "schema":     schema,
            "table_name": table_name,
            "table_type": table_type,
            "location":   bucket,
            "status":     "SUCCESS" if resp.status.state == StatementState.SUCCEEDED else "FAIL"
        }
    except Exception as e:
        return {
            "schema":     schema,
            "table_name": table_name,
            "table_type": table_type,
            "location":   bucket,
            "status":     f"FAIL: {e}"
        }


# COMMAND ----------
# ── Helper: DROP external table in target ─────────────────────
# External tables must be dropped before re-creating because
# CREATE TABLE does not support REPLACE when specifying a location.

def drop_table(w, catalog, schema, table_name, warehouse):
    try:
        sql  = f"DROP TABLE IF EXISTS {catalog}.{schema}.{table_name}"
        resp = w.statement_execution.execute_statement(
            warehouse_id=warehouse, statement=sql)

        while resp.status.state in {StatementState.PENDING, StatementState.RUNNING}:
            resp = w.statement_execution.get_statement(resp.statement_id)
            time.sleep(response_backoff)

        status = 1 if resp.status.state == StatementState.SUCCEEDED else 0
        return {"status": status, "schema": schema, "table_name": table_name}

    except Exception:
        return {"status": 0, "schema": schema, "table_name": table_name}


# COMMAND ----------
# ── Helper: LOAD (register) table in target metastore ─────────
# Points to the DEEP CLONE location written by copy_table.
# EXTERNAL tables use CREATE TABLE ... LOCATION syntax.

def load_table(w, catalog, schema, table_name, table_type, location, warehouse, wait_timeout):
    if table_type == "EXTERNAL":
        try:
            sql  = (f"CREATE TABLE {catalog}.{schema}.{table_name} "
                    f"USING delta LOCATION '{location}'")
            resp = w.statement_execution.execute_statement(
                warehouse_id=warehouse,
                wait_timeout=wait_timeout,
                statement=sql)

            while resp.status.state in {StatementState.PENDING, StatementState.RUNNING}:
                resp = w.statement_execution.get_statement(resp.statement_id)
                time.sleep(response_backoff)

            if resp.status.state != StatementState.SUCCEEDED:
                return {
                    "schema": schema, "table_name": table_name,
                    "table_type": table_type, "location": location,
                    "status": f"FAIL: {resp.status.error.message}",
                    "creation_time": time.time_ns()
                }
            return {
                "schema": schema, "table_name": table_name,
                "table_type": table_type, "location": location,
                "status": "SUCCESS", "creation_time": time.time_ns()
            }
        except Exception as e:
            return {
                "schema": schema, "table_name": table_name,
                "table_type": table_type, "location": location,
                "status": f"FAIL: {e}", "creation_time": time.time_ns()
            }
    else:
        # Only DELTA/EXTERNAL tables are supported in this DR pattern
        print(f"Skipping {table_type} table {catalog}.{schema}.{table_name}")
        return {
            "schema": schema, "table_name": table_name,
            "table_type": table_type, "location": location,
            "status": "SKIPPED", "creation_time": "N/A"
        }


# COMMAND ----------
# ── Phase 1: DEEP CLONE all DELTA tables to landing zone ──────
try:
    print("sync_tables: Starting Phase 1 — DEEP CLONE to DR landing zone")

    copied_table_names     = []
    copied_table_types     = []
    copied_table_schemas   = []
    copied_table_locations = []

    # Read schema filter — only copy schemas listed in schema_mapping CSV
    schema_df     = pd.read_csv(schema_mapping, keep_default_na=False)
    schemas_filter = schema_df["source_schema"].tolist()

    system_info = spark.sql("SELECT * FROM system.information_schema.tables")

    for cat in catalogs_to_copy:
        filtered_tables = system_info.filter(
            (system_info.table_catalog == cat) &
            (system_info.table_schema != "information_schema") &
            (system_info.table_type != "VIEW") &
            system_info.table_schema.isin(schemas_filter) &
            (system_info.data_source_format == "DELTA")
        ).collect()

        DELTA_table_count = len(filtered_tables)
        print(f"Catalog '{cat}': {DELTA_table_count} DELTA tables found")

        schemas     = [row["table_schema"] for row in filtered_tables]
        table_names = [row["table_name"]   for row in filtered_tables]
        table_types = [row["table_type"]   for row in filtered_tables]

        # Parallel DEEP CLONE using ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=num_exec) as executor:
            threads = executor.map(
                copy_table,
                repeat(w_source), repeat(cat),
                schemas, table_names, table_types,
                repeat(landing_zone_url),
                repeat(w_source_id),
                repeat(wait_timeout))

            for thread in threads:
                copied_table_names.append(thread["table_name"])
                copied_table_types.append(thread["table_type"])
                copied_table_schemas.append(thread["schema"])
                copied_table_locations.append(
                    f"{thread['location']}/{thread['schema']}/{thread['table_name']}")
                Table_copied_count += 1

    # Build manifest of all cloned tables
    manifest_df = pd.DataFrame({
        "schema":   copied_table_schemas,
        "table":    copied_table_names,
        "location": copied_table_locations,
        "type":     copied_table_types
    })
    print(f"Phase 1 complete: {Table_copied_count} tables cloned to {landing_zone_url}")

    # COMMAND ----------
    # ── Phase 2: DROP stale external tables in target ─────────
    print("sync_tables: Starting Phase 2 — DROP stale external tables in target")

    external_df = manifest_df[manifest_df["type"] == "EXTERNAL"]

    with ThreadPoolExecutor(max_workers=num_exec) as executor:
        threads = executor.map(
            drop_table,
            repeat(w_target), repeat(target_catalog),
            list(external_df["schema"]),
            list(external_df["table"]),
            repeat(w_target_id))

        for thread in threads:
            if thread["status"]:
                print(f"Dropped: {thread['schema']}.{thread['table_name']}")
                Table_dropped_count += 1
            else:
                print(f"Drop failed: {thread['schema']}.{thread['table_name']}")
                Error_dropping_table_count += 1

    # COMMAND ----------
    # ── Phase 3: LOAD tables in target from DR location ───────
    print("sync_tables: Starting Phase 3 — LOAD tables in target metastore")

    loaded_table_names     = []
    loaded_table_types     = []
    loaded_table_schemas   = []
    loaded_table_locations = []
    loaded_table_status    = []
    loaded_table_times     = []

    with ThreadPoolExecutor(max_workers=num_exec) as executor:
        threads = executor.map(
            load_table,
            repeat(w_target), repeat(target_catalog),
            list(manifest_df["schema"]),
            list(manifest_df["table"]),
            list(manifest_df["type"]),
            list(manifest_df["location"]),
            repeat(w_target_id),
            repeat(wait_timeout))

        for thread in threads:
            loaded_table_names.append(thread["table_name"])
            loaded_table_types.append(thread["table_type"])
            loaded_table_schemas.append(thread["schema"])
            loaded_table_locations.append(thread["location"])
            loaded_table_status.append(thread["status"])
            loaded_table_times.append(thread["creation_time"])

    status_df = pd.DataFrame({
        "schema":    loaded_table_schemas,
        "table":     loaded_table_names,
        "location":  loaded_table_locations,
        "type":      loaded_table_types,
        "status":    loaded_table_status,
        "sync_time": loaded_table_times
    })

    Table_created_count = len(status_df[status_df["status"] == "SUCCESS"])

    end          = time.time()
    run_duration = end - start

    summary = {
        "run_duration":              run_duration,
        "total_delta_tables":        DELTA_table_count,
        "tables_deep_cloned":        Table_copied_count,
        "tables_dropped_in_target":  Table_dropped_count,
        "tables_drop_errors":        Error_dropping_table_count,
        "tables_loaded_in_target":   Table_created_count,
        "tables_skipped":            Table_skipped_count,
    }

except Exception as e:
    raise SystemExit(f"sync_tables failed: {str(e)}")

# COMMAND ----------
print(f"\nPipeline : {pl_or_wl_name}")
print(f"Tables cloned  : {Table_copied_count}")
print(f"Tables loaded  : {Table_created_count}")
print(f"Tables dropped : {Table_dropped_count}")
print(f"Duration : {run_duration:.2f}s")
print("Status   : SUCCESS")
print(json.dumps(summary, indent=2))
