param(
  [Parameter(Mandatory=$true)]
  [ValidateSet("azure","account","workspace")][string]$Stage,
  [ValidateSet("plan","apply")][string]$Action = "plan"
)
$ErrorActionPreference = "Stop"
$stagePath = Join-Path $PSScriptRoot $Stage
function Run-Terraform([string[]]$TfArgs) {
  & terraform "-chdir=$stagePath" @TfArgs
  if ($LASTEXITCODE -ne 0) { throw "Terraform failed. Stop here." }
}
if ($Stage -ne "workspace" -and !(Test-Path (Join-Path $stagePath "terraform.tfvars"))) {
  throw "Copy terraform.tfvars.example to terraform.tfvars and fill NEW account values first."
}
Run-Terraform @("init")
Run-Terraform @("validate")
if ($Action -eq "plan") {
  Run-Terraform @("plan", "-out=rebuild.tfplan")
} else {
  if (!(Test-Path (Join-Path $stagePath "rebuild.tfplan"))) { throw "Create and review a plan first." }
  Run-Terraform @("apply", "rebuild.tfplan")
}
