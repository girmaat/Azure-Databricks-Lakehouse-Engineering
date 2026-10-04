#!/usr/bin/env bash
# Shared configuration for the CLI catch-up scripts.
# Source this file rather than executing it directly.

export RESOURCE_GROUP="${RESOURCE_GROUP:-rg-adblh-dev-eus2-001}"
export LOCATION="${LOCATION:-eastus2}"
export STORAGE_ACCOUNT="${STORAGE_ACCOUNT:-stadblhdeveus2001}"
export ACCESS_CONNECTOR="${ACCESS_CONNECTOR:-ac-adblh-dev-eus2-001}"
export DATABRICKS_WORKSPACE="${DATABRICKS_WORKSPACE:-dbw-adblh-dev-eus2-001}"
export MANAGED_RESOURCE_GROUP="${MANAGED_RESOURCE_GROUP:-managed-rg-adblh-dev-eus2-001}"

export STORAGE_CREDENTIAL="${STORAGE_CREDENTIAL:-sc-adblh-dev-storage}"
export LANDING_EXTERNAL_LOCATION="${LANDING_EXTERNAL_LOCATION:-el-adblh-dev-landing}"
export MANAGED_EXTERNAL_LOCATION="${MANAGED_EXTERNAL_LOCATION:-el-adblh-dev-managed}"
export CATALOG="${CATALOG:-cat_adblh_dev}"

export OWNER_GROUP="${OWNER_GROUP:-grp_adblh_dev_owners}"
export ENGINEER_GROUP="${ENGINEER_GROUP:-grp_adblh_dev_engineers}"
export ANALYST_GROUP="${ANALYST_GROUP:-grp_adblh_dev_analysts}"

export DATABRICKS_PROFILE="${DATABRICKS_PROFILE:-DEFAULT}"
export APPLY_REMAINING_UC_PERMISSIONS="${APPLY_REMAINING_UC_PERMISSIONS:-false}"
