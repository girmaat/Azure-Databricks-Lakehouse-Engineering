# CLI catch-up package — Azure Databricks Lakehouse Engineering

The scripts are designed to be safe to keep in Git and mostly idempotent for the current DEV environment.

## Authentication

Azure resources:

```bash
az login
az account set --subscription <your-subscription-id-or-name>
```

Databricks workspace commands:

Use a Databricks CLI workspace profile. Example:

```bash
databricks auth login https://<your-workspace-url> --profile DEFAULT
```

The scripts use `DATABRICKS_PROFILE=DEFAULT` unless you override it.

## Suggested usage

Because the current environment already exists, use the verification script first:

```bash
./10-verify.sh
```

The scripts 01–07 are primarily imperative equivalents / rebuild references for resources already created manually.

## Remaining Step-10 permissions are opt-in

By default, `09-unity-catalog-permissions.sh` applies/reasserts only the baseline grants and skips the remaining grants.

To apply the remaining engineer/analyst permissions later:

```bash
export APPLY_REMAINING_UC_PERMISSIONS=true
./09-unity-catalog-permissions.sh
```

That grants:

- Engineers: Silver/Gold build+read+modify, Operations/Quarantine build+read+modify
- Analysts: USE_CATALOG plus USE_SCHEMA/SELECT on Gold only

The script uses `databricks grants update`, which adds the specified privileges instead of replacing unrelated principals' grants.

## Workspace entitlements

`08-workspace-entitlements.sh` ensures the positive entitlements already selected in the portal:

- Owners: workspace access
- Engineers: workspace access + Databricks SQL access
- Analysts: workspace access + Databricks SQL access

For safety, the CLI script is additive and does not revoke unexpected entitlements. Terraform is the authoritative desired-state representation for disabled cluster/pool creation entitlements.
