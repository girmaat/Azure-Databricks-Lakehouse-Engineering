# Old-environment preservation gate

Known project targets from your source: rg-adblh-dev-eus2-001; workspace dbw-adblh-dev-eus2-001; storage stadblhdeveus2001; Access Connector ac-adblh-dev-eus2-001. The managed resource-group name in tfvars is not live-verified.

Before deleting:
- Azure Portal: open the project resource group and export its resource list (including names, types, IDs and region). Verify the Databricks workspace's actual managed resource group. Check for project resources in other groups. Do not infer that the code is a full deployed inventory.
- Storage browser: inspect landing, managed, checkpoints, exports and recovery. Copy any cloud-only objects. The package already preserves the ten shipment API pages; it does not prove the other containers are empty.
- Databricks: confirm the exported Inspect_Shipment_Landing.py matches the current notebook. Export any additional notebooks. Record any jobs, warehouses and actual compute configurations not represented in this package.
- Catalog: record actual catalog/schema owners, groups and memberships, direct grants, metastore assignment, external-location settings. Record any tables/volumes and preserve their data if present.
- Save resource list and actual findings in docs/manual_resource_register.md (currently empty). Do not claim a backup is complete until these are checked.
- Keep folders.zip, this package and your Git repository. The package has no secrets; restore secrets and Git authentication separately.

Safe-to-delete decision is still pending those cloud-only checks. Resources can be recreated from reviewed code, but this package cannot recover unexported data or configuration. Cancelling the subscription is a separate billing action; deleting a Microsoft email/account is not necessary to remove project resources.
