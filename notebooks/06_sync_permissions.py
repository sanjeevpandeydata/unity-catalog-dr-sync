# Databricks notebook source
# ============================================================
# NOTEBOOK : sync_permissions.py
# PURPOSE  : Sync Unity Catalog grants (permissions) for
#            catalogs, schemas, tables, and volumes from
#            primary workspace to DR target workspace.
#            Uses direct Unity Catalog REST API calls
#            (effective-permissions + permissions endpoints)
#            to compare source vs target grants and apply
#            only the delta (add/remove) changes.
# AUTHOR   : Sanjeev Kumar Pandey
# ============================================================

# COMMAND ----------
from itertools import repeat
from databricks.sdk import WorkspaceClient
from concurrent.futures import ThreadPoolExecutor
import requests
import json
import pandas as pd
from collections import defaultdict
import time

start = time.time()

# COMMAND ----------
# ── Widget Parameters ─────────────────────────────────────────
dbutils.widgets.text("env",                  "")
env                  = dbutils.widgets.get("env")

dbutils.widgets.text("catalog_mapping_file", "")
catalog_mapping_path = dbutils.widgets.get("catalog_mapping_file")

dbutils.widgets.text("num_exec",             "")
num_exec             = int(dbutils.widgets.get("num_exec"))

dbutils.widgets.text("pl_or_wl_name",        "")
pl_or_wl_name        = dbutils.widgets.get("pl_or_wl_name")

dbutils.widgets.text("status_tbl",           "")
status_tbl           = dbutils.widgets.get("status_tbl")

# COMMAND ----------
# ── Secrets ───────────────────────────────────────────────────
source_host  = dbutils.secrets.get(scope=f"adls{env}scope", key="SourceHostKey")
source_token = dbutils.secrets.get(scope=f"adls{env}scope", key="SourceSPTokenKey")
target_host  = dbutils.secrets.get(scope=f"adls{env}scope", key="TargetHostKey")
target_token = dbutils.secrets.get(scope=f"adls{env}scope", key="TargetSPTokenKey")

# COMMAND ----------
# ── Load catalog mapping ──────────────────────────────────────
catalog_df      = pd.read_csv(catalog_mapping_path, keep_default_na=False)

if "source_catalog" not in catalog_df.columns or "target_catalog" not in catalog_df.columns:
    raise ValueError("catalog_mapping.csv must contain 'source_catalog' and 'target_catalog'")

catalog_mapping  = dict(zip(catalog_df["source_catalog"], catalog_df["target_catalog"]))
catalogs_to_copy = catalog_df["source_catalog"].tolist()

# Global counters
sync_stats = defaultdict(int)

# COMMAND ----------
# ── Helper: GET effective permissions via REST API ────────────
# Uses direct REST call instead of SDK — SDK does not expose
# effective-permissions in all versions.

def get_effective_permissions(host, token, securable_type, full_name):
    url     = f"{host.rstrip('/')}/api/2.1/unity-catalog/effective-permissions/{securable_type}/{full_name}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    resp    = requests.get(url, headers=headers)

    if resp.status_code == 200:
        return resp.json()
    elif resp.status_code == 404:
        raise Exception(f"NotFound: {full_name}")
    else:
        resp.raise_for_status()


# COMMAND ----------
# ── Helper: PATCH permissions via REST API ────────────────────
# Sends only the delta (add/remove) rather than full overwrite.

def update_permissions(host, token, securable_type, full_name, changes):
    url     = f"{host.rstrip('/')}/api/2.1/unity-catalog/permissions/{securable_type}/{full_name}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    resp    = requests.patch(url, headers=headers, json={"changes": changes})

    if resp.status_code not in [200, 204]:
        resp.raise_for_status()
    return resp.json() if resp.content else {}


# COMMAND ----------
# ── Helper: Translate source object name to target ────────────
# Replaces source catalog prefix with target catalog prefix
# using the catalog_mapping dict.

def map_object_name(source_name, catalog_mapping):
    for src_cat, tgt_cat in catalog_mapping.items():
        if source_name.startswith(src_cat + "."):
            return source_name.replace(src_cat + ".", tgt_cat + ".", 1)
        elif source_name == src_cat:
            return tgt_cat
    return source_name  # No mapping found — return unchanged


# COMMAND ----------
# ── Core: Sync grants for one object ─────────────────────────
# 1. Fetch effective permissions from source and target
# 2. Diff the privilege sets per principal
# 3. PATCH only the changed grants

def sync_grants(source_host, source_token, target_host, target_token,
                source_obj, target_obj, obj_type):
    try:
        source_grants = get_effective_permissions(
            source_host, source_token, obj_type, source_obj)

        try:
            target_grants = get_effective_permissions(
                target_host, target_token, obj_type, target_obj)
        except Exception as e:
            if "NotFound" in str(e) or "404" in str(e):
                sync_stats["not_found"] += 1
                return {"name": target_obj, "status": "NotFound"}
            raise e

        source_assignments = source_grants.get("privilege_assignments", [])
        target_assignments = target_grants.get("privilege_assignments", [])

        # Collect all unique principals across source and target
        all_principals = (
            {u["principal"] for u in source_assignments} |
            {u["principal"] for u in target_assignments}
        )

        changes = []
        for principal in all_principals:
            # Privileges this principal has in source
            source_privs = []
            for a in source_assignments:
                if a["principal"] == principal:
                    source_privs = [
                        p["privilege"] for p in a.get("privileges", [])
                        if p.get("privilege")]
                    break

            # Privileges this principal has in target
            target_privs = []
            for a in target_assignments:
                if a["principal"] == principal:
                    target_privs = [
                        p["privilege"] for p in a.get("privileges", [])
                        if p.get("privilege")]
                    break

            add_perms = list(set(source_privs) - set(target_privs))
            rem_perms = list(set(target_privs) - set(source_privs))

            if add_perms or rem_perms:
                change = {"principal": principal}
                if add_perms: change["add"]    = add_perms
                if rem_perms: change["remove"] = rem_perms
                changes.append(change)

        if changes:
            update_permissions(
                target_host, target_token, obj_type, target_obj, changes)
            sync_stats["success"] += 1
            return {"name": target_obj, "status": "SUCCESS", "changes": len(changes)}
        else:
            sync_stats["no_changes"] += 1
            return {"name": target_obj, "status": None}

    except Exception as e:
        sync_stats["error"] += 1
        return {"name": target_obj, "status": "ERROR", "error": str(e)}


# COMMAND ----------
# ── Helper: Process a batch of objects in parallel ────────────
# Tries securable type variants (API requires exact string).

def process_objects_batch(source_host, source_token, target_host, target_token,
                           source_objects, target_objects, obj_type, label):
    if not source_objects:
        return []

    print(f"\nSyncing {label} grants ({len(source_objects)} objects)...")

    # Try type name variants — API format varies by SDK version
    working_type = None
    for variant in [f"{obj_type.lower()}s", obj_type.lower(), obj_type.upper()]:
        try:
            test = sync_grants(source_host, source_token, target_host, target_token,
                               source_objects[0], target_objects[0], variant)
            if test["status"] != "ERROR":
                working_type = variant
                break
        except Exception:
            continue

    if not working_type:
        print(f"Could not determine securable type for {label}")
        return []

    with ThreadPoolExecutor(max_workers=num_exec) as executor:
        results = list(executor.map(
            lambda pair: sync_grants(
                source_host, source_token, target_host, target_token,
                pair[0], pair[1], working_type),
            zip(source_objects, target_objects)
        ))

    success   = sum(1 for r in results if r["status"] == "SUCCESS")
    no_change = sum(1 for r in results if r["status"] is None)
    not_found = sum(1 for r in results if r["status"] == "NotFound")
    errors    = sum(1 for r in results if r["status"] == "ERROR")

    print(f"{label}: ✅ {success} synced | ↔ {no_change} unchanged | "
          f"❓ {not_found} not found | ❌ {errors} errors")

    return results


# COMMAND ----------
# ── Main: Sync permissions for all catalogs ───────────────────
def main():
    print("Starting permission synchronization...")
    print(f"Catalog mapping: {catalog_mapping}")

    w_source = WorkspaceClient(host=source_host, token=source_token)
    w_target = WorkspaceClient(host=target_host, token=target_token)

    # Verify connectivity
    try:
        w_source.current_user.me()
        w_target.current_user.me()
        print("Connected to both source and target workspaces.")
    except Exception as e:
        raise SystemExit(f"Workspace connectivity check failed: {str(e)}")

    # Query all tables and volumes from source
    catalog_filter = "', '".join(catalogs_to_copy)
    table_info  = spark.sql(
        f"SELECT * FROM system.information_schema.tables "
        f"WHERE table_catalog IN ('{catalog_filter}')")
    volume_info = spark.sql(
        f"SELECT * FROM system.information_schema.volumes "
        f"WHERE volume_catalog IN ('{catalog_filter}')")

    for source_cat, target_cat in catalog_mapping.items():
        print(f"\nProcessing: {source_cat} → {target_cat}")

        try:
            filtered_tables  = table_info.filter(
                (table_info.table_catalog  == source_cat) &
                (table_info.table_schema   != "information_schema")
            ).collect()

            filtered_volumes = volume_info.filter(
                volume_info.volume_catalog == source_cat
            ).collect()

            # Sync catalog-level grants
            for cat_type in ["catalogs", "catalog", "CATALOG"]:
                try:
                    res = sync_grants(
                        source_host, source_token, target_host, target_token,
                        source_cat, target_cat, cat_type)
                    if res["status"] in ("SUCCESS", None, "NotFound"):
                        break
                except Exception:
                    continue

            # Build source/target object lists
            source_schemas = list({
                f"{source_cat}.{row['table_schema']}"
                for row in filtered_tables})
            target_schemas = [
                map_object_name(s, catalog_mapping) for s in source_schemas]

            source_tables  = [
                f"{source_cat}.{row['table_schema']}.{row['table_name']}"
                for row in filtered_tables]
            target_tables  = [
                map_object_name(t, catalog_mapping) for t in source_tables]

            source_volumes = [
                f"{source_cat}.{row['volume_schema']}.{row['volume_name']}"
                for row in filtered_volumes]
            target_volumes = [
                map_object_name(v, catalog_mapping) for v in source_volumes]

            # Sync grants for schemas, tables, volumes in parallel
            if source_schemas:
                process_objects_batch(
                    source_host, source_token, target_host, target_token,
                    source_schemas, target_schemas, "schema", "Schema")

            if source_tables:
                process_objects_batch(
                    source_host, source_token, target_host, target_token,
                    source_tables, target_tables, "table", "Table")

            if source_volumes:
                process_objects_batch(
                    source_host, source_token, target_host, target_token,
                    source_volumes, target_volumes, "volume", "Volume")

        except Exception as e:
            print(f"ERROR processing {source_cat} → {target_cat}: {str(e)}")
            continue

    end          = time.time()
    run_duration = end - start

    summary = {
        "run_duration":    run_duration,
        "grants_synced":   sync_stats["success"],
        "no_changes":      sync_stats["no_changes"],
        "not_found":       sync_stats["not_found"],
        "errors":          sync_stats["error"],
    }

    return summary


if __name__ == "__main__":
    summary = main()

# COMMAND ----------
print(f"\nPipeline : {pl_or_wl_name}")
print(f"Grants synced   : {sync_stats['success']}")
print(f"No changes      : {sync_stats['no_changes']}")
print(f"Not found       : {sync_stats['not_found']}")
print(f"Errors          : {sync_stats['error']}")
print(f"Duration        : {(time.time() - start):.2f}s")
print("Status          : SUCCESS")
