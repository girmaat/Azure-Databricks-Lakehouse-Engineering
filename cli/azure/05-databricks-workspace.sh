#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/00-config.sh"

if az databricks workspace show -g "$RESOURCE_GROUP" -n "$DATABRICKS_WORKSPACE" >/dev/null 2>&1; then
  echo "SKIP: Databricks workspace already exists: $DATABRICKS_WORKSPACE"
  az databricks workspace show -g "$RESOURCE_GROUP" -n "$DATABRICKS_WORKSPACE" \
    --query '{name:name,location:location,sku:sku.name,workspaceUrl:workspaceUrl,managedResourceGroupId:managedResourceGroupId,enableNoPublicIp:parameters.enableNoPublicIp.value}' \
    -o jsonc
  exit 0
fi

az databricks workspace create \
  --resource-group "$RESOURCE_GROUP" \
  --name "$DATABRICKS_WORKSPACE" \
  --location "$LOCATION" \
  --sku trial \
  --compute-mode Hybrid \
  --managed-resource-group "$MANAGED_RESOURCE_GROUP" \
  --enable-no-public-ip true \
  --public-network-access Enabled \
  --tags \
    Environment=dev \
    Project=Azure-Databricks-Lakehouse-Engineering \
    ManagedBy=manual-first
