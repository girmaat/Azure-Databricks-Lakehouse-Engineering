# Rebuild your Azure Databricks Lakehouse Engineering environment

Status: prepared from folders.zip; not deployed or Terraform-validated against Azure.
This is a NEW-ENVIRONMENT package, not an update to the old manually built environment.
Do not run import-existing.sh or use old Terraform state here. Keep all three state files locally backed up and out of Git.

## Scope preserved
East US 2; project resource group, HNS-enabled LRS storage with landing/managed/checkpoints/exports/recovery containers; system-assigned Access Connector and storage RBAC; Databricks workspace; sc-adblh-dev-storage; el-adblh-dev-landing and el-adblh-dev-managed; cat_adblh_dev and bronze/silver/gold/operations/quarantine; original entitlements, baseline grants, and engineering compute policy.

New-account additions: account groups, learner membership in owners and engineers, workspace group assignment, parameterized new storage name, three-stage deployment, inspection notebook imported with NEW landing URL, and verified restoration of completed shipment pages.

## Before deleting the old environment
See PRE_DELETE.md. This package contains no delete command and does not cancel subscriptions.

## Manual bootstrap that cannot be inferred from your ZIP
1. Create/identify the new Azure subscription, tenant, and new Databricks account. Confirm offer eligibility and costs separately.
2. Authenticate Azure locally (or use your chosen supported service-principal authentication). The earlier login failure must be resolved for local deployment; this package does not bypass it.
3. New Azure identity needs resource creation and role-assignment rights on the deployment scope. Configure ARM_SUBSCRIPTION_ID/ARM_TENANT_ID or tfvars as appropriate.
4. Authenticate Databricks account provider with account-admin access. Use unified authentication applicable to your account; no credentials are placed in source.

## Stage 1: Azure
Copy azure/terraform.tfvars.example to azure/terraform.tfvars; fill new subscription ID, tenant ID, globally unique lowercase storage name.
The workspace defaults to PREMIUM, not Trial: this avoids silently assuming a renewed trial. This incurs normal paid usage when workloads run. Change workspace_sku only after confirming what the new subscription supports.
Budget is disabled by default until its support and new notification address are known. Enable it explicitly when ready; amount and thresholds remain the original 20/50/75/90/100/forecast100.
From the project root:
    ./infra/rebuild/deploy.ps1 -Stage azure
Review the plan. Then:
    ./infra/rebuild/deploy.ps1 -Stage azure -Action apply
    terraform -chdir=infra/rebuild/azure output

## Stage 2: Account groups
Open the NEW workspace and sign in once with your new email so the user exists. Obtain the Databricks account ID and account-admin authentication.
Fill account/terraform.tfvars from its example.
    ./infra/rebuild/deploy.ps1 -Stage account
    ./infra/rebuild/deploy.ps1 -Stage account -Action apply
If same-named account groups already exist, STOP and reconcile/import into this account-stage state; do not create duplicates.
The learner belongs to owners and engineers; analysts is created empty because the ZIP provides no analyst identities.

## Stage 3: Workspace
Verify Unity Catalog metastore assignment (see workspace/metastore-check.md). Configure workspace-level Databricks authentication for the NEW workspace. Remove stale DATABRICKS_HOST/profile selections; the Terraform host is obtained from the Azure stage.
Confirm the built-in Shared Compute policy exists. If it has a different name, set shared_compute_policy_name in workspace/terraform.tfvars.
    ./infra/rebuild/deploy.ps1 -Stage workspace
    ./infra/rebuild/deploy.ps1 -Stage workspace -Action apply
Remaining grants are false by default, matching the uploaded baseline. File events are intentionally not enabled, reflecting the unresolved old File Events Read check.
Storage RBAC can take time to become effective; retry a failed workspace plan/apply only after examining its error.
The exported notebook is imported into the deploying user's personal project path with the new landing URL. This is deployment automation, not automatic ongoing Git synchronization.

## Restore API data
    python -m pip install -r infra/rebuild/requirements.txt
    python infra/rebuild/restore_shipments.py
This uploads only the completed checkpoint-success copy to landing/fulfillment/shipments/delivery_id=shipments-2026-10-01-v1/ and checks byte equality. AzureCliCredential requires a working login and your user needs Storage Blob Data Contributor on the new container/account (the connector role does not grant your user access).
If you prefer portal restoration, upload ONLY page_001.json through page_010.json from the checkpoint-success folder to the same destination. Exclude checkpoint.json.

## Completion check
Open imported inspect_shipment_landing and run its five cells on supported notebook compute. Expect 10 page rows and Page metadata validation: PASS. Check catalog/schema ownership, memberships and compute policy before continuing Sequence 13.

## Deliberately not replicated without evidence
No SQL warehouse/cluster/job resources were defined in the supplied code. No cluster is started. No account metastore ID is invented. No Git PAT or API secret is copied. PostgreSQL remains on your Windows machine at 5433 with its existing data and CDC configuration. No snapshot/incremental parquet is uploaded automatically. Existing local checkpoints are preserved.

## Known unresolved parity item
The old CLI explicitly requests Hybrid compute mode; the supplied Terraform only comments on it. This package does not guess a provider property. Verify the resulting workspace supports the notebook/classic compute choices you used before accepting full parity.

## Validation limits
Python syntax, source coverage and archive integrity checked locally. Terraform binary and authenticated Azure/Databricks access were unavailable, so init/validate/plan/apply must be run on your Windows machine. A successful plan is required before apply.
