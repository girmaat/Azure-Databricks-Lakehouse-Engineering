# NOTE: databricks_grant manages one principal on one securable and is used here
# to avoid overwriting unrelated direct grants that might exist on the object.

# Existing catalog discoverability created by Databricks when the catalog was created.
resource "databricks_grant" "catalog_browse_account_users" {
  catalog    = databricks_catalog.dev.id
  principal  = "account users"
  privileges = ["BROWSE"]
}

# Existing engineer prerequisite confirmed in Step 10.
resource "databricks_grant" "catalog_engineers" {
  catalog    = databricks_catalog.dev.id
  principal  = data.databricks_group.engineers.display_name
  privileges = ["USE_CATALOG"]
}

# Bronze engineer grants are the baseline pattern already discussed/implemented.
resource "databricks_grant" "bronze_engineers" {
  schema     = "${databricks_catalog.dev.id}.${databricks_schema.bronze.name}"
  principal  = data.databricks_group.engineers.display_name
  privileges = ["USE_SCHEMA", "CREATE_TABLE", "CREATE_VOLUME", "SELECT", "MODIFY"]
}

# ---------------------- OPTIONAL REMAINING STEP-10 GRANTS ----------------------
# These resources are OFF by default. They are created only when:
#   enable_remaining_uc_permissions = true
#
# This gives you the automation ready now, while letting you skip manually
# repeating portal work unless a later interview question actually needs it.

resource "databricks_grant" "catalog_analysts" {
  count      = var.enable_remaining_uc_permissions ? 1 : 0
  catalog    = databricks_catalog.dev.id
  principal  = data.databricks_group.analysts.display_name
  privileges = ["USE_CATALOG"]
}

resource "databricks_grant" "silver_engineers" {
  count      = var.enable_remaining_uc_permissions ? 1 : 0
  schema     = "${databricks_catalog.dev.id}.${databricks_schema.silver.name}"
  principal  = data.databricks_group.engineers.display_name
  privileges = ["USE_SCHEMA", "CREATE_TABLE", "CREATE_VOLUME", "SELECT", "MODIFY"]
}

resource "databricks_grant" "gold_engineers" {
  count      = var.enable_remaining_uc_permissions ? 1 : 0
  schema     = "${databricks_catalog.dev.id}.${databricks_schema.gold.name}"
  principal  = data.databricks_group.engineers.display_name
  privileges = ["USE_SCHEMA", "CREATE_TABLE", "CREATE_VOLUME", "SELECT", "MODIFY"]
}

resource "databricks_grant" "operations_engineers" {
  count      = var.enable_remaining_uc_permissions ? 1 : 0
  schema     = "${databricks_catalog.dev.id}.${databricks_schema.operations.name}"
  principal  = data.databricks_group.engineers.display_name
  privileges = ["USE_SCHEMA", "CREATE_TABLE", "SELECT", "MODIFY"]
}

resource "databricks_grant" "quarantine_engineers" {
  count      = var.enable_remaining_uc_permissions ? 1 : 0
  schema     = "${databricks_catalog.dev.id}.${databricks_schema.quarantine.name}"
  principal  = data.databricks_group.engineers.display_name
  privileges = ["USE_SCHEMA", "CREATE_TABLE", "SELECT", "MODIFY"]
}

resource "databricks_grant" "gold_analysts" {
  count      = var.enable_remaining_uc_permissions ? 1 : 0
  schema     = "${databricks_catalog.dev.id}.${databricks_schema.gold.name}"
  principal  = data.databricks_group.analysts.display_name
  privileges = ["USE_SCHEMA", "SELECT"]
}
