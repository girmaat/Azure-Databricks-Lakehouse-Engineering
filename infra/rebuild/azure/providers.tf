terraform {
  required_version = ">= 1.6.0"
  required_providers {
    azurerm = { source = "hashicorp/azurerm", version = ">= 5.0, < 6.0" }
    databricks = { source = "databricks/databricks", version = ">= 1.130, < 2.0" }
  }
}
provider "azurerm" {
  features {}
  subscription_id = var.subscription_id
  tenant_id = var.tenant_id
}
