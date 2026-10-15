output "workspace_url" { value = "https://${azurerm_databricks_workspace.dev.workspace_url}" }
output "workspace_id" { value = azurerm_databricks_workspace.dev.workspace_id }
output "access_connector_id" { value = azurerm_databricks_access_connector.dev.id }
output "storage_account_name" { value = azurerm_storage_account.dev.name }
output "landing_url" { value = local.landing_url }
output "managed_url" { value = local.managed_url }
