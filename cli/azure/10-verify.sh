#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/00-config.sh"

echo "=== Azure resources ==="
az group show -n "$RESOURCE_GROUP" --query '{name:name,location:location}' -o table
az storage account show -g "$RESOURCE_GROUP" -n "$STORAGE_ACCOUNT" --query '{name:name,hns:isHnsEnabled,publicNetworkAccess:publicNetworkAccess,minTls:minimumTlsVersion}' -o table
az databricks access-connector show -g "$RESOURCE_GROUP" -n "$ACCESS_CONNECTOR" --query '{name:name,principalId:identity.principalId}' -o table
az databricks workspace show -g "$RESOURCE_GROUP" -n "$DATABRICKS_WORKSPACE" --query '{name:name,sku:sku.name,workspaceUrl:workspaceUrl,enableNoPublicIp:parameters.enableNoPublicIp.value}' -o table

echo
echo "=== Unity Catalog objects ==="
databricks storage-credentials get "$STORAGE_CREDENTIAL" -p "$DATABRICKS_PROFILE" -o json
databricks external-locations get "$LANDING_EXTERNAL_LOCATION" -p "$DATABRICKS_PROFILE" -o json
databricks external-locations get "$MANAGED_EXTERNAL_LOCATION" -p "$DATABRICKS_PROFILE" -o json
databricks catalogs get "$CATALOG" -p "$DATABRICKS_PROFILE" -o json

echo
echo "=== Catalog grants ==="
databricks grants get catalog "$CATALOG" -p "$DATABRICKS_PROFILE" -o json

echo
echo "=== Effective engineer grants by schema ==="
for schema in bronze silver gold operations quarantine; do
  echo "--- ${CATALOG}.${schema}"
  databricks grants get-effective schema "${CATALOG}.${schema}" \
    -p "$DATABRICKS_PROFILE" \
    --principal "$ENGINEER_GROUP" \
    -o json
done

echo
echo "=== Effective analyst grants on Gold ==="
databricks grants get-effective schema "${CATALOG}.gold" \
  -p "$DATABRICKS_PROFILE" \
  --principal "$ANALYST_GROUP" \
  -o json
