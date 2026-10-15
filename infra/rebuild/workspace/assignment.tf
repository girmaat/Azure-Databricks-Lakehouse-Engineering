resource "databricks_permission_assignment" "groups" {
  for_each = data.terraform_remote_state.account.outputs.group_ids
  principal_id = each.value
  permissions = ["USER"]
}
