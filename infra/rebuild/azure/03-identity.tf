resource "azurerm_databricks_access_connector" "dev" {
  name                = local.access_connector_name
  resource_group_name = azurerm_resource_group.dev.name
  location            = azurerm_resource_group.dev.location

  identity {
    type = "SystemAssigned"
  }

  tags = local.common_tags
}
