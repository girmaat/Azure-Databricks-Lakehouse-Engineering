/*
Dependency chain:

azurerm_storage_account.dev
        ↑
azurerm_role_assignment.dev_storage_blob_data_contributor
        ↑
managed identity on azurerm_databricks_access_connector.dev

This is Azure RBAC. Unity Catalog grants are managed separately in
10-unity-catalog-permissions.tf.
*/
resource "azurerm_role_assignment" "dev_storage_blob_data_contributor" {
  scope                = azurerm_storage_account.dev.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.dev.identity[0].principal_id
  principal_type       = "ServicePrincipal"
}
