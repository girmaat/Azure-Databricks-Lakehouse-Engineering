terraform {
  required_version = ">= 1.6.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = ">= 5.0, < 6.0"
    }

    databricks = {
      source  = "databricks/databricks"
      version = ">= 1.130, < 2.0"
    }
  }
}

provider "azurerm" {
  features {}
}

# Workspace-level Databricks provider.
# Authentication is intentionally NOT hard-coded. For local development,
# sign in with Azure CLI first (az login) and set databricks_host in tfvars.
provider "databricks" {
  host = var.databricks_host
}
