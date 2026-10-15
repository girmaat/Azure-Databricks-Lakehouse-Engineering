data "databricks_current_user" "me" {}
resource "databricks_notebook" "inspect_shipment_landing" {
  path = "${data.databricks_current_user.me.home}/Azure Databricks Lakehouse Engineering/notebooks/ingestion/inspect_shipment_landing"
  language = "PYTHON"
  content_base64 = base64encode(templatefile("${path.module}/templates/inspect_shipment_landing.py.tftpl", { landing_url = local.landing_url }))
  depends_on = [databricks_catalog.dev]
}
output "notebook_url" { value = databricks_notebook.inspect_shipment_landing.url }
