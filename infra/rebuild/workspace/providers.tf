terraform {
  required_version = ">= 1.6.0"
  required_providers {
    azurerm = { source = "hashicorp/azurerm", version = ">= 5.0, < 6.0" }
    databricks = { source = "databricks/databricks", version = ">= 1.130, < 2.0" }
  }
}
data "terraform_remote_state" "azure" {
  backend = "local"
  config = { path = "${path.module}/../azure/terraform.tfstate" }
}
data "terraform_remote_state" "account" {
  backend = "local"
  config = { path = "${path.module}/../account/terraform.tfstate" }
}
provider "databricks" { host = data.terraform_remote_state.azure.outputs.workspace_url }
