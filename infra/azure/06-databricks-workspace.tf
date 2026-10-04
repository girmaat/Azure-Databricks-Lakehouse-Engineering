# Mirrors the DEV workspace created manually in Azure Portal:
# - Trial SKU
# - Hybrid compute mode in Portal
# - Managed VNet (no customer VNet injection yet)
# - Secure Cluster Connectivity / No Public IP enabled
resource "azurerm_databricks_workspace" "dev" {
  name                        = local.workspace_name
  resource_group_name         = azurerm_resource_group.dev.name
  location                    = azurerm_resource_group.dev.location
  sku                         = "trial"
  managed_resource_group_name = local.managed_resource_group_name

  public_network_access_enabled = true

  custom_parameters {
    no_public_ip = true
  }

  tags = local.common_tags
}
