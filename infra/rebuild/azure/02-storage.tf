resource "azurerm_storage_account" "dev" {
  name                     = local.storage_account_name
  resource_group_name      = azurerm_resource_group.dev.name
  location                 = azurerm_resource_group.dev.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"

  is_hns_enabled = true

  https_traffic_only_enabled      = true
  min_tls_version                 = "TLS1_2"
  allow_nested_items_to_be_public = false
  public_network_access_enabled   = true

  routing {
    choice = "MicrosoftRouting"
  }

  tags = local.common_tags
}

resource "azurerm_storage_container" "landing" {
  name                  = "landing"
  storage_account_id    = azurerm_storage_account.dev.id
  container_access_type = "private"
}

resource "azurerm_storage_container" "managed" {
  name                  = "managed"
  storage_account_id    = azurerm_storage_account.dev.id
  container_access_type = "private"
}

resource "azurerm_storage_container" "checkpoints" {
  name                  = "checkpoints"
  storage_account_id    = azurerm_storage_account.dev.id
  container_access_type = "private"
}

resource "azurerm_storage_container" "exports" {
  name                  = "exports"
  storage_account_id    = azurerm_storage_account.dev.id
  container_access_type = "private"
}

resource "azurerm_storage_container" "recovery" {
  name                  = "recovery"
  storage_account_id    = azurerm_storage_account.dev.id
  container_access_type = "private"
}
