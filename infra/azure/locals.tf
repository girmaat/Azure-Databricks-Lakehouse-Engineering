locals {
  resource_group_name = "rg-adblh-dev-eus2-001"
  location            = "East US 2"

  storage_account_name = "stadblhdeveus2001"
  access_connector_name = "ac-adblh-dev-eus2-001"
  workspace_name        = "dbw-adblh-dev-eus2-001"
  managed_resource_group_name = var.databricks_managed_resource_group_name

  project_tag = "Azure-Databricks-Lakehouse-Engineering"
  common_tags = {
    Environment = "dev"
    Project     = local.project_tag
    ManagedBy   = "manual-first"
  }

  storage_credential_name   = "sc-adblh-dev-storage"
  landing_external_location = "el-adblh-dev-landing"
  managed_external_location = "el-adblh-dev-managed"
  catalog_name              = "cat_adblh_dev"

  owner_group_name    = "grp_adblh_dev_owners"
  engineer_group_name = "grp_adblh_dev_engineers"
  analyst_group_name  = "grp_adblh_dev_analysts"

  landing_url = "abfss://landing@${local.storage_account_name}.dfs.core.windows.net/"
  managed_url = "abfss://managed@${local.storage_account_name}.dfs.core.windows.net/"
}
