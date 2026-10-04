#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/00-config.sh"

az group create \
  --name "$RESOURCE_GROUP" \
  --location "$LOCATION" \
  --tags \
    Environment=dev \
    Project=Azure-Databricks-Lakehouse-Engineering \
    ManagedBy=manual-first
