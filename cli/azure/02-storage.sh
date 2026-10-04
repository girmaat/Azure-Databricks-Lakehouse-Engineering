#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/00-config.sh"

if ! az storage account show -g "$RESOURCE_GROUP" -n "$STORAGE_ACCOUNT" >/dev/null 2>&1; then
  az storage account create \
    --name "$STORAGE_ACCOUNT" \
    --resource-group "$RESOURCE_GROUP" \
    --location "$LOCATION" \
    --sku Standard_LRS \
    --kind StorageV2 \
    --enable-hierarchical-namespace true \
    --https-only true \
    --min-tls-version TLS1_2 \
    --allow-blob-public-access false \
    --public-network-access Enabled \
    --routing-choice MicrosoftRouting \
    --tags \
      Environment=dev \
      Project=Azure-Databricks-Lakehouse-Engineering \
      ManagedBy=manual-first
else
  echo "SKIP: storage account already exists: $STORAGE_ACCOUNT"
fi

for container in landing managed checkpoints exports recovery; do
  az storage container create \
    --name "$container" \
    --account-name "$STORAGE_ACCOUNT" \
    --auth-mode login \
    --only-show-errors >/dev/null
  echo "OK: container $container"
done
