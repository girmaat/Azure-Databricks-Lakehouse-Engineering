# Terraform catch-up package — Azure Databricks Lakehouse Engineering

This folder is designed to catch Terraform up to the resources already created manually in Azure Portal and Databricks.

## Important safety rule

**Do not run `terraform apply` against the manually-created environment until the existing resources have been imported and the plan has been reviewed.**

Recommended sequence:

1. Copy `terraform.tfvars.example` to `terraform.tfvars`.
2. Set the real Azure Databricks workspace URL.
3. Set budget notification email(s) and confirm the budget start date matches Azure Portal.
4. Run `az login` and select the correct subscription.
5. Run `terraform init`.
6. Run `terraform validate`.
7. Run `./import-existing.sh`.
8. Run `terraform plan` and reconcile any differences before applying.

## Optional remaining Step-10 Unity Catalog permissions

`enable_remaining_uc_permissions` defaults to `false`.

When left `false`, Terraform keeps the already-established baseline model but does not add the remaining Silver/Gold/Operations/Quarantine engineer grants or Gold analyst grants.

When you intentionally set it to `true`, Terraform will add:

- Engineers on Silver: USE_SCHEMA, CREATE_TABLE, CREATE_VOLUME, SELECT, MODIFY
- Engineers on Gold: USE_SCHEMA, CREATE_TABLE, CREATE_VOLUME, SELECT, MODIFY
- Engineers on Operations: USE_SCHEMA, CREATE_TABLE, SELECT, MODIFY
- Engineers on Quarantine: USE_SCHEMA, CREATE_TABLE, SELECT, MODIFY
- Analysts on catalog: USE_CATALOG
- Analysts on Gold: USE_SCHEMA, SELECT

This keeps the automation ready without forcing you to repeat portal exercises you already understand.

## Why groups are data sources, not created here

The three groups already exist and are usable by Unity Catalog. Referencing them as data sources avoids accidentally creating workspace-local duplicates. Their workspace entitlements are managed by `databricks_entitlements` resources.

## Reconciliation notes

The files intentionally avoid guessing values that were not conclusively verified in the portal. In particular:

- Verify `databricks_managed_resource_group_name` against the existing workspace before apply.
- The landing external location showed file events enabled in the portal even though automatic event-resource provisioning did not fully succeed. This package does not force a file-events setting yet; let the imported state and `terraform plan` reveal the current API value before deciding whether to manage it.
- The owner/engineer/analyst groups are referenced as existing account groups. Do not recreate them as workspace-local groups.

The import helper also imports the three baseline Unity Catalog grants that were already established: catalog BROWSE for `account users`, engineer USE_CATALOG, and the Bronze engineer grant set.
