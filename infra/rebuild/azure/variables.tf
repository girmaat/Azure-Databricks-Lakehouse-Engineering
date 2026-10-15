variable "subscription_id" {
  type = string
}
variable "tenant_id" {
  type = string
}
variable "resource_group_name" {
  type = string
  default = "rg-adblh-dev-eus2-001"
}
variable "location" {
  type = string
  default = "eastus2"
}
variable "storage_account_name" {
  type = string
}
variable "access_connector_name" {
  type = string
  default = "ac-adblh-dev-eus2-001"
}
variable "workspace_name" {
  type = string
  default = "dbw-adblh-dev-eus2-001"
}
variable "databricks_managed_resource_group_name" {
  type = string
  default = "managed-rg-adblh-dev-eus2-001"
}
variable "workspace_sku" {
  type = string
  default = "premium"
}
variable "enable_budget" {
  type = bool
  default = false
}
variable "dev_monthly_budget_amount" {
  type = number
  default = 20
}
variable "budget_start_date" {
  type = string
  default = "2026-10-01T00:00:00Z"
}
variable "budget_notification_emails" {
  type = list(string)
  default = []
}
