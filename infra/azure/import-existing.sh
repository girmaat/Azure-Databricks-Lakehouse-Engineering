#!/usr/bin/env bash
set -euo pipefail

# Imports the Azure + Unity Catalog objects that were created manually.
# Run from this infra/azure directory AFTER:
#   az login
#   terraform init
#   terraform.tfvars has a valid databricks_host
#
# This script intentionally does NOT run terraform apply.

RG="rg-adblh-dev-eus2-001"
SA="stadblhdeveus2001"
AC="ac-adblh-dev-eus2-001"
WS="dbw-adblh-dev-eus2-001"
BUDGET="budget-adblh-dev-monthly"

SUB_ID="$(az account show --query id -o tsv)"
RG_ID="/subscriptions/${SUB_ID}/resourceGroups/${RG}"
SA_ID="${RG_ID}/providers/Microsoft.Storage/storageAccounts/${SA}"
AC_ID="${RG_ID}/providers/Microsoft.Databricks/accessConnectors/${AC}"
WS_ID="${RG_ID}/providers/Microsoft.Databricks/workspaces/${WS}"
BUDGET_ID="${RG_ID}/providers/Microsoft.Consumption/budgets/${BUDGET}"

import_if_missing() {
  local address="$1"
  local id="$2"
  if terraform state show "$address" >/dev/null 2>&1; then
    echo "SKIP: $address already in Terraform state"
  else
    echo "IMPORT: $address"
    terraform import "$address" "$id"
  fi
}

import_if_missing azurerm_resource_group.dev "$RG_ID"
import_if_missing azurerm_storage_account.dev "$SA_ID"
import_if_missing azurerm_storage_container.landing "${SA_ID}/blobServices/default/containers/landing"
import_if_missing azurerm_storage_container.managed "${SA_ID}/blobServices/default/containers/managed"
import_if_missing azurerm_storage_container.checkpoints "${SA_ID}/blobServices/default/containers/checkpoints"
import_if_missing azurerm_storage_container.exports "${SA_ID}/blobServices/default/containers/exports"
import_if_missing azurerm_storage_container.recovery "${SA_ID}/blobServices/default/containers/recovery"
import_if_missing azurerm_databricks_access_connector.dev "$AC_ID"
import_if_missing azurerm_databricks_workspace.dev "$WS_ID"
import_if_missing azurerm_consumption_budget_resource_group.dev "$BUDGET_ID"

# Role assignment IDs are GUID-based. Discover the existing assignment and import it.
PRINCIPAL_ID="$(az databricks access-connector show -g "$RG" -n "$AC" --query identity.principalId -o tsv)"
ROLE_ID="$(az role assignment list --assignee-object-id "$PRINCIPAL_ID" --scope "$SA_ID" --role "Storage Blob Data Contributor" --query '[0].id' -o tsv)"
if [[ -n "$ROLE_ID" && "$ROLE_ID" != "null" ]]; then
  import_if_missing azurerm_role_assignment.dev_storage_blob_data_contributor "$ROLE_ID"
else
  echo "WARN: existing Storage Blob Data Contributor role assignment not found."
fi

# Unity Catalog objects use their names as import IDs with a workspace-level provider.
import_if_missing databricks_storage_credential.dev "sc-adblh-dev-storage"
import_if_missing databricks_external_location.landing "el-adblh-dev-landing"
import_if_missing databricks_external_location.managed "el-adblh-dev-managed"
import_if_missing databricks_catalog.dev "cat_adblh_dev"
import_if_missing databricks_schema.bronze "cat_adblh_dev.bronze"
import_if_missing databricks_schema.silver "cat_adblh_dev.silver"
import_if_missing databricks_schema.gold "cat_adblh_dev.gold"
import_if_missing databricks_schema.operations "cat_adblh_dev.operations"
import_if_missing databricks_schema.quarantine "cat_adblh_dev.quarantine"

# Import the baseline Unity Catalog grants that already exist in the portal.
# databricks_grant import ID format: <securable_type>/<securable_name>/<principal>.
import_if_missing databricks_grant.catalog_browse_account_users "catalog/cat_adblh_dev/account users"
import_if_missing databricks_grant.catalog_engineers "catalog/cat_adblh_dev/grp_adblh_dev_engineers"
import_if_missing databricks_grant.bronze_engineers "schema/cat_adblh_dev.bronze/grp_adblh_dev_engineers"

# Workspace entitlement resources are keyed by Databricks group ID. Import them
# when the Databricks CLI is installed and the selected workspace profile works.
# If not, the script warns and leaves them for a later explicit import.
DATABRICKS_PROFILE="${DATABRICKS_PROFILE:-DEFAULT}"

get_group_id_for_import() {
  local group_name="$1"
  local payload
  payload="$(databricks groups list --filter "displayName eq '$group_name'" -p "$DATABRICKS_PROFILE" -o json)"
  GROUP_NAME="$group_name" python -c '
import json, os, sys
name = os.environ["GROUP_NAME"]
data = json.load(sys.stdin)
items = data if isinstance(data, list) else (data.get("Resources") or data.get("resources") or [])
for item in items:
    if item.get("displayName") == name or item.get("display_name") == name:
        print(item.get("id", ""))
        raise SystemExit(0)
raise SystemExit(1)
' <<<"$payload"
}

if command -v databricks >/dev/null 2>&1 && command -v python >/dev/null 2>&1; then
  if databricks current-user me -p "$DATABRICKS_PROFILE" >/dev/null 2>&1; then
    OWNER_GID="$(get_group_id_for_import grp_adblh_dev_owners)"
    ENGINEER_GID="$(get_group_id_for_import grp_adblh_dev_engineers)"
    ANALYST_GID="$(get_group_id_for_import grp_adblh_dev_analysts)"

    import_if_missing databricks_entitlements.owners "group/${OWNER_GID}"
    import_if_missing databricks_entitlements.engineers "group/${ENGINEER_GID}"
    import_if_missing databricks_entitlements.analysts "group/${ANALYST_GID}"
  else
    echo "WARN: Databricks CLI profile '$DATABRICKS_PROFILE' is not authenticated; entitlement imports skipped."
  fi
else
  echo "WARN: Databricks CLI and/or Python not found; entitlement imports skipped."
fi

echo
echo "Import pass complete. Next run: terraform plan"
echo "Do not apply until the plan is reviewed and any manual-vs-code differences are reconciled."
