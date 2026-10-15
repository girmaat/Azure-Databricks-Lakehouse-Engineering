# Sign in to the new workspace once before applying: the user must exist.
data "databricks_user" "learner" { user_name = var.new_user_email }
resource "databricks_group" "owners" { display_name = "grp_adblh_dev_owners" }
resource "databricks_group" "engineers" { display_name = "grp_adblh_dev_engineers" }
resource "databricks_group" "analysts" { display_name = "grp_adblh_dev_analysts" }
resource "databricks_group_member" "learner_owner" {
  group_id = databricks_group.owners.id
  member_id = data.databricks_user.learner.id
}
resource "databricks_group_member" "learner_engineer" {
  group_id = databricks_group.engineers.id
  member_id = data.databricks_user.learner.id
}
output "group_ids" {
  value = { owners = databricks_group.owners.id, engineers = databricks_group.engineers.id, analysts = databricks_group.analysts.id }
}
