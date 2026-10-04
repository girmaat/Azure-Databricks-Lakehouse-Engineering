# Unity Catalog storage credential using the Azure Databricks Access Connector's
# system-assigned managed identity.
resource "databricks_storage_credential" "dev" {
  name = local.storage_credential_name

  azure_managed_identity {
    access_connector_id = azurerm_databricks_access_connector.dev.id
  }

  comment = "DEV storage credential backed by Azure Databricks Access Connector managed identity."
}

resource "databricks_external_location" "landing" {
  name            = local.landing_external_location
  url             = local.landing_url
  credential_name = databricks_storage_credential.dev.id
  comment         = "DEV landing/source data external location."
}

resource "databricks_external_location" "managed" {
  name            = local.managed_external_location
  url             = local.managed_url
  credential_name = databricks_storage_credential.dev.id
  comment         = "DEV managed-storage root external location for Unity Catalog managed objects."
}
