terraform {
  required_version = ">= 1.6.0"
  required_providers {
    azurerm = { source = "hashicorp/azurerm", version = ">= 5.0, < 6.0" }
    databricks = { source = "databricks/databricks", version = ">= 1.130, < 2.0" }
  }
}
provider "databricks" {
  host = "https://accounts.azuredatabricks.net"
  account_id = var.databricks_account_id
}
variable "databricks_account_id" { type = string }
variable "new_user_email" { type = string }
