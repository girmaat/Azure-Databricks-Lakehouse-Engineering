#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/00-config.sh"

AC_ID="$(az databricks access-connector show -g "$RESOURCE_GROUP" -n "$ACCESS_CONNECTOR" --query id -o tsv)"
LANDING_URL="abfss://landing@${STORAGE_ACCOUNT}.dfs.core.windows.net/"
MANAGED_URL="abfss://managed@${STORAGE_ACCOUNT}.dfs.core.windows.net/"

if ! databricks storage-credentials get "$STORAGE_CREDENTIAL" -p "$DATABRICKS_PROFILE" >/dev/null 2>&1; then
  databricks storage-credentials create "$STORAGE_CREDENTIAL" \
    -p "$DATABRICKS_PROFILE" \
    --json "{\"azure_managed_identity\":{\"access_connector_id\":\"$AC_ID\"},\"comment\":\"DEV storage credential backed by Azure Databricks Access Connector managed identity.\"}"
else
  echo "SKIP: storage credential already exists: $STORAGE_CREDENTIAL"
fi

if ! databricks external-locations get "$LANDING_EXTERNAL_LOCATION" -p "$DATABRICKS_PROFILE" >/dev/null 2>&1; then
  databricks external-locations create "$LANDING_EXTERNAL_LOCATION" "$LANDING_URL" "$STORAGE_CREDENTIAL" \
    -p "$DATABRICKS_PROFILE" \
    --comment "DEV landing/source data external location."
else
  echo "SKIP: external location already exists: $LANDING_EXTERNAL_LOCATION"
fi

if ! databricks external-locations get "$MANAGED_EXTERNAL_LOCATION" -p "$DATABRICKS_PROFILE" >/dev/null 2>&1; then
  databricks external-locations create "$MANAGED_EXTERNAL_LOCATION" "$MANAGED_URL" "$STORAGE_CREDENTIAL" \
    -p "$DATABRICKS_PROFILE" \
    --comment "DEV managed-storage root external location for Unity Catalog managed objects."
else
  echo "SKIP: external location already exists: $MANAGED_EXTERNAL_LOCATION"
fi
