locals {
  storage_credential_name = "sc-adblh-dev-storage"
  landing_external_location = "el-adblh-dev-landing"
  managed_external_location = "el-adblh-dev-managed"
  catalog_name = "cat_adblh_dev"
  owner_group_name = "grp_adblh_dev_owners"
  engineer_group_name = "grp_adblh_dev_engineers"
  analyst_group_name = "grp_adblh_dev_analysts"
  landing_url = data.terraform_remote_state.azure.outputs.landing_url
  managed_url = data.terraform_remote_state.azure.outputs.managed_url
}
