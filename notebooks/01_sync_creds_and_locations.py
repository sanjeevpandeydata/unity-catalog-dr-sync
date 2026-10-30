# Databricks notebook source
# ============================================================
# NOTEBOOK : sync_creds_and_locations.py
# PURPOSE  : Sync Unity Catalog storage credentials and
#            external locations from a primary workspace to a
#            DR (disaster recovery) target workspace.
#            Compares source vs target by name, creates missing
#            objects in target using CSV mapping files.
# AUTHOR   : Sanjeev Kumar Pandey
# ============================================================

# COMMAND ----------
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import catalog
import pandas as pd
import json
import time

start = time.time()

# COMMAND ----------
# ── Widget Parameters ─────────────────────────────────────────
dbutils.widgets.text("cred_mapping_file",          "")
cred_mapping_file          = dbutils.widgets.get("cred_mapping_file")

dbutils.widgets.text("loc_mapping_file",           "")
loc_mapping_file           = dbutils.widgets.get("loc_mapping_file")

dbutils.widgets.text("create_credential_flag",     "")
create_credential_flag     = dbutils.widgets.get("create_credential_flag")

dbutils.widgets.text("cloud_type",                 "")
cloud_type                 = dbutils.widgets.get("cloud_type")

dbutils.widgets.text("env",                        "")
env                        = dbutils.widgets.get("env")

dbutils.widgets.text("status_database",            "")
status_database            = dbutils.widgets.get("status_database")

dbutils.widgets.text("status_tbl",                 "")
status_tbl                 = dbutils.widgets.get("status_tbl")

dbutils.widgets.text("pipeline_or_workflow_name",  "")
pl_or_wl_name              = dbutils.widgets.get("pipeline_or_workflow_name")

# COMMAND ----------
# ── Secrets — fetched from Databricks Secret Scope ───────────
# Never hardcoded. Source and target PATs stored in scoped secrets.
source_host  = dbutils.secrets.get(scope=f"adls{env}scope", key="SourceHostKey")
source_pat   = dbutils.secrets.get(scope=f"adls{env}scope", key="SourceSPTokenKey")
target_host  = dbutils.secrets.get(scope=f"adls{env}scope", key="TargetHostKey")
target_pat   = dbutils.secrets.get(scope=f"adls{env}scope", key="TargetSPTokenKey")

# Initialise SDK clients for both workspaces
w_source = WorkspaceClient(host=source_host, token=source_pat)
w_target = WorkspaceClient(host=target_host, token=target_pat)

# COMMAND ----------
# ── Step 1-2: List external locations from both workspaces ────
source_extloc = [x for x in w_source.external_locations.list()]
target_extloc = [x for x in w_target.external_locations.list()]

cred_df = pd.read_csv(cred_mapping_file, keep_default_na=False)

# COMMAND ----------
# ── (Optional) Step: Sync storage credentials ─────────────────
# Controlled by create_credential_flag widget ("true"/"false").
# For most DR scenarios this is false — credentials already exist.

if create_credential_flag == "true":
    source_creds = [x for x in w_source.storage_credentials.list()]
    target_creds = [x for x in w_target.storage_credentials.list()]

    target_cred_names = [x.name for x in target_creds]
    creds_to_create   = cred_df["target_cred_name"].tolist()

    if not creds_to_create:
        print("All source credentials already exist in target metastore.")

    for cred_name in creds_to_create:
        print(f"Creating storage credential: {cred_name}")

        # Pull auth config from mapping CSV —
        # only ONE of managed_id_connector / managed_id_identity / SP should be populated
        try:
            managed_id_connector = cred_df["target_mgd_id_connector"].loc[
                cred_df["source_cred_name"] == cred_name].iloc[0]
            managed_id_identity  = cred_df["target_mgd_id_identity"].loc[
                cred_df["source_cred_name"] == cred_name].iloc[0]
            sp_directory         = cred_df["target_sp_directory"].loc[
                cred_df["source_cred_name"] == cred_name].iloc[0]
            sp_appid             = cred_df["target_sp_appid"].loc[
                cred_df["source_cred_name"] == cred_name].iloc[0]
            sp_secret            = cred_df["target_sp_secret"].loc[
                cred_df["source_cred_name"] == cred_name].iloc[0]
        except (KeyError, IndexError):
            print(f"Could not create credential {cred_name} — check mapping file.")
            break

        # Create credential using whichever auth method is provided
        if managed_id_connector:
            mgd_id = catalog.AzureManagedIdentityRequest(
                access_connector_id=managed_id_connector)
            w_target.storage_credentials.create(
                name=cred_name, azure_managed_identity=mgd_id)

        elif managed_id_identity:
            mgd_id = catalog.AzureManagedIdentityRequest(
                access_connector_id=managed_id_identity)
            w_target.storage_credentials.create(
                name=cred_name, azure_managed_identity=mgd_id)

        else:
            cred_sp = catalog.AzureServicePrincipal(
                directory_id=sp_directory,
                application_id=sp_appid,
                client_secret=sp_secret)
            w_target.storage_credentials.create(
                name=cred_name, azure_service_principal=cred_sp)

        print(f"Created storage credential: {cred_name}")
else:
    print("Skipping storage credential creation (create_credential_flag = false).")

# COMMAND ----------
# ── Step 3-7: Sync external locations ─────────────────────────
# Compare source vs target by name; create any that are missing.
# URL translation handled via the location mapping CSV.

try:
    extloc_created_count = 0
    extloc_skipped_count = 0

    source_extloc_names = [x.name for x in source_extloc]
    target_extloc_names = [x.name for x in target_extloc]
    print(f"Source external locations: {len(source_extloc_names)}")
    print(f"Target external locations: {len(target_extloc_names)}")

    loc_df        = pd.read_csv(loc_mapping_file, keep_default_na=False)
    locs_to_add   = loc_df["target_loc_name"].tolist()
    locs_to_sync  = loc_df["source_loc_name"].tolist()

    # Find locations that exist in mapping but not yet in target
    loc_diff      = list(set(locs_to_add) - set(target_extloc_names))
    filtered_df   = loc_df[loc_df["target_loc_name"].isin(loc_diff)]
    new_in_source = filtered_df["source_loc_name"].tolist()
    locs_to_create = [x for x in source_extloc if x.name in new_in_source]

    print(f"External locations to create: {len(loc_diff)}")

    if not loc_diff:
        print("All source external locations already exist in target metastore.")
    else:
        for loc in locs_to_create:
            # Translate name and credential using mapping CSV
            loc_name      = loc_df["target_loc_name"].loc[
                loc_df["source_loc_name"] == loc.name].iloc[0]
            loc_cred_name = cred_df["target_cred_name"].loc[
                cred_df["source_cred_name"] == loc.credential_name].iloc[0]
            loc_comment   = loc.comment
            loc_fallback  = None      # Not used in this DR pattern
            loc_read_only = loc.read_only

            print(f"Creating external location: {loc_name}")

            try:
                url = loc_df["target_url"].loc[
                    loc_df["target_loc_name"] == loc_name].iloc[0]
            except (KeyError, IndexError):
                print(f"ERROR: No target URL found for '{loc_name}' — check mapping file.")
                break

            w_target.external_locations.create(
                name=loc_name,
                credential_name=loc_cred_name,
                comment=loc_comment,
                fallback=loc_fallback,
                read_only=loc_read_only,
                url=url
            )

            print(f"Created external location: {loc_name}")
            extloc_created_count += 1

    extloc_skipped_count = len(locs_to_add) - extloc_created_count

except Exception as e:
    raise SystemExit(f"External location sync failed: {str(e)}")

# COMMAND ----------
end          = time.time()
run_duration = end - start

print(f"\nPipeline : {pl_or_wl_name}")
print(f"External locations created : {extloc_created_count}")
print(f"External locations skipped : {extloc_skipped_count}")
print(f"Duration : {run_duration:.2f}s")
print("Status   : SUCCESS")
