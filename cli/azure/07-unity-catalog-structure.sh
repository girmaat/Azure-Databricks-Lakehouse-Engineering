#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/00-config.sh"

MANAGED_URL="abfss://managed@${STORAGE_ACCOUNT}.dfs.core.windows.net/"

if ! databricks catalogs get "$CATALOG" -p "$DATABRICKS_PROFILE" >/dev/null 2>&1; then
  databricks catalogs create "$CATALOG" -p "$DATABRICKS_PROFILE" \
    --storage-root "$MANAGED_URL" \
    --comment "Development catalog for Azure Databricks Lakehouse Engineering project."
else
  echo "SKIP: catalog already exists: $CATALOG"
fi

databricks catalogs update "$CATALOG" -p "$DATABRICKS_PROFILE" --owner "$OWNER_GROUP"

for schema in bronze silver gold operations quarantine; do
  if ! databricks schemas get "${CATALOG}.${schema}" -p "$DATABRICKS_PROFILE" >/dev/null 2>&1; then
    databricks schemas create "$schema" "$CATALOG" -p "$DATABRICKS_PROFILE"
  else
    echo "SKIP: schema already exists: ${CATALOG}.${schema}"
  fi
  databricks schemas update "${CATALOG}.${schema}" -p "$DATABRICKS_PROFILE" --owner "$OWNER_GROUP"
done
