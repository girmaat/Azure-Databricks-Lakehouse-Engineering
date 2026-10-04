# Workspace entitlements are different from Unity Catalog data privileges.
# These resources mirror the toggles configured in the portal.
resource "databricks_entitlements" "owners" {
  group_id                    = data.databricks_group.owners.id
  workspace_access            = true
  databricks_sql_access       = false
  allow_cluster_create        = false
  allow_instance_pool_create  = false
}

resource "databricks_entitlements" "engineers" {
  group_id                    = data.databricks_group.engineers.id
  workspace_access            = true
  databricks_sql_access       = true
  allow_cluster_create        = false
  allow_instance_pool_create  = false
}

resource "databricks_entitlements" "analysts" {
  group_id                    = data.databricks_group.analysts.id
  workspace_access            = true
  databricks_sql_access       = true
  allow_cluster_create        = false
  allow_instance_pool_create  = false
}
