variable "databricks_host" {
  description = "Per-workspace Azure Databricks URL, for example https://adb-1234567890123456.7.azuredatabricks.net"
  type        = string
}


variable "databricks_managed_resource_group_name" {
  description = "Managed resource group name of the existing Azure Databricks workspace. Verify this against Azure before import/apply."
  type        = string
  default     = "managed-rg-adblh-dev-eus2-001"
}

variable "dev_monthly_budget_amount" {
  description = "Monthly Azure budget amount for the DEV resource group."
  type        = number
  default     = 20
}

variable "budget_notification_emails" {
  description = "Email addresses that receive Azure budget alerts."
  type        = list(string)
  default     = []
}

variable "budget_start_date" {
  description = "Start date of the existing monthly Azure budget. Keep this aligned with the Portal-created budget before import/apply."
  type        = string
  default     = "2026-10-01T00:00:00Z"
}

variable "enable_remaining_uc_permissions" {
  description = "When true, automate the remaining engineer/analyst Unity Catalog grants discussed in Step 10. Defaults false so nothing new is granted until you explicitly opt in."
  type        = bool
  default     = false
}
