#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/00-config.sh"

if ! az databricks access-connector show -g "$RESOURCE_GROUP" -n "$ACCESS_CONNECTOR" >/dev/null 2>&1; then
  az databricks access-connector create \
    --name "$ACCESS_CONNECTOR" \
    --resource-group "$RESOURCE_GROUP" \
    --location "$LOCATION" \
    --identity-type SystemAssigned \
    --tags \
      Environment=dev \
      Project=Azure-Databricks-Lakehouse-Engineering \
      ManagedBy=manual-first
else
  echo "SKIP: access connector already exists: $ACCESS_CONNECTOR"
fi

STORAGE_ID="$(az storage account show -g "$RESOURCE_GROUP" -n "$STORAGE_ACCOUNT" --query id -o tsv)"
PRINCIPAL_ID="$(az databricks access-connector show -g "$RESOURCE_GROUP" -n "$ACCESS_CONNECTOR" --query identity.principalId -o tsv)"

if [[ "$(az role assignment list --assignee-object-id "$PRINCIPAL_ID" --scope "$STORAGE_ID" --role "Storage Blob Data Contributor" --query 'length(@)' -o tsv)" == "0" ]]; then
  az role assignment create \
    --assignee-object-id "$PRINCIPAL_ID" \
    --assignee-principal-type ServicePrincipal \
    --role "Storage Blob Data Contributor" \
    --scope "$STORAGE_ID"
else
  echo "SKIP: Storage Blob Data Contributor already assigned to access connector identity"
fi
