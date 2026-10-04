# Groups already exist because they were created manually in the Databricks UI.
# We intentionally reference them as data sources here instead of recreating them.
# This avoids accidentally creating workspace-local duplicates. Their workspace
# entitlements are managed in 09-databricks-group-entitlements.tf.
data "databricks_group" "owners" {
  display_name = local.owner_group_name
}

data "databricks_group" "engineers" {
  display_name = local.engineer_group_name
}

data "databricks_group" "analysts" {
  display_name = local.analyst_group_name
}

resource "databricks_catalog" "dev" {
  name         = local.catalog_name
  storage_root = local.managed_url
  owner        = data.databricks_group.owners.display_name
  comment      = "Development catalog for Azure Databricks Lakehouse Engineering project."

  depends_on = [databricks_external_location.managed]
}

resource "databricks_schema" "bronze" {
  catalog_name = databricks_catalog.dev.id
  name         = "bronze"
  owner        = data.databricks_group.owners.display_name
  comment      = "Source-aligned/raw ingestion layer for DEV."
}

resource "databricks_schema" "silver" {
  catalog_name = databricks_catalog.dev.id
  name         = "silver"
  owner        = data.databricks_group.owners.display_name
  comment      = "Trusted, standardized, and canonical data for DEV."
}

resource "databricks_schema" "gold" {
  catalog_name = databricks_catalog.dev.id
  name         = "gold"
  owner        = data.databricks_group.owners.display_name
  comment      = "Curated analytical and serving layer for DEV."
}

resource "databricks_schema" "operations" {
  catalog_name = databricks_catalog.dev.id
  name         = "operations"
  owner        = data.databricks_group.owners.display_name
  comment      = "Operational control, audit, reconciliation, monitoring, and run metadata for DEV."
}

resource "databricks_schema" "quarantine" {
  catalog_name = databricks_catalog.dev.id
  name         = "quarantine"
  owner        = data.databricks_group.owners.display_name
  comment      = "Rejected or nonconforming records retained for traceability and remediation."
}
