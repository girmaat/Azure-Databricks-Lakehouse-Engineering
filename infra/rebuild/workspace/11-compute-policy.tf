# Step 11 - Databricks compute governance
# Mirrors the portal-created pol_adblh_dev_engineering policy.
# The policy inherits the Databricks Shared Compute family and overrides only
# project-specific DEV controls.

data "databricks_cluster_policy" "shared_compute_default" {
  name = var.shared_compute_policy_name
}

resource "databricks_cluster_policy" "dev_engineering" {
  name        = "pol_adblh_dev_engineering"
  description = "Governed classic interactive compute policy for Azure Databricks Lakehouse Engineering DEV workloads."

  policy_family_id = data.databricks_cluster_policy.shared_compute_default.policy_family_id

  policy_family_definition_overrides = jsonencode({
    spark_version = {
      type   = "fixed"
      value  = "auto:latest-lts"
      hidden = true
    }

    data_security_mode = {
      type   = "fixed"
      value  = "DATA_SECURITY_MODE_STANDARD"
      hidden = true
    }

    autotermination_minutes = {
      type   = "fixed"
      value  = 30
      hidden = true
    }

    dbus_per_hour = {
      type     = "range"
      maxValue = 10
    }

    "autoscale.max_workers" = {
      type     = "range"
      maxValue = 4
    }
  })
}

# Engineers do not have unrestricted cluster creation. CAN_USE on this governed
# policy is the intended path for classic interactive compute creation.
resource "databricks_permissions" "dev_engineering_policy" {
  cluster_policy_id = databricks_cluster_policy.dev_engineering.id

  access_control {
    group_name       = data.databricks_group.engineers.display_name
    permission_level = "CAN_USE"
  }
}
