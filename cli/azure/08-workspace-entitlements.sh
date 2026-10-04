#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/00-config.sh"

# Workspace entitlements are separate from Unity Catalog data privileges.
# This script is intentionally additive: it ensures the required positive
# entitlements exist, but it does not remove unexpected entitlements.
# Terraform (09-databricks-group-entitlements.tf) is the authoritative
# desired-state definition for both true and false entitlement values.

get_group_id() {
  local group_name="$1"
  local payload
  payload="$(databricks groups list \
    --filter "displayName eq '$group_name'" \
    -p "$DATABRICKS_PROFILE" \
    -o json)"

  GROUP_NAME="$group_name" python -c '
import json, os, sys
name = os.environ["GROUP_NAME"]
data = json.load(sys.stdin)
if isinstance(data, list):
    items = data
elif isinstance(data, dict):
    items = data.get("Resources") or data.get("resources") or []
else:
    items = []
for item in items:
    if item.get("displayName") == name or item.get("display_name") == name:
        print(item.get("id", ""))
        raise SystemExit(0)
raise SystemExit(f"Group not found in workspace: {name}")
' <<<"$payload"
}

ensure_entitlements() {
  local group_name="$1"
  shift
  local desired=("$@")
  local group_id
  group_id="$(get_group_id "$group_name")"

  local current
  current="$(databricks groups get "$group_id" -p "$DATABRICKS_PROFILE" -o json)"

  local missing_json
  missing_json="$(DESIRED="$(IFS=,; echo "${desired[*]}")" python -c '
import json, os, sys
obj = json.load(sys.stdin)
current = set()
for item in obj.get("entitlements", []) or []:
    if isinstance(item, dict) and item.get("value"):
        current.add(item["value"])
desired = [x for x in os.environ.get("DESIRED", "").split(",") if x]
missing = [x for x in desired if x not in current]
print(json.dumps(missing))
' <<<"$current")"

  if [[ "$missing_json" == "[]" ]]; then
    echo "SKIP: required entitlements already present for $group_name"
    return 0
  fi

  local patch_json
  patch_json="$(MISSING_JSON="$missing_json" python -c '
import json, os
missing = json.loads(os.environ["MISSING_JSON"])
print(json.dumps({
  "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
  "Operations": [{
    "op": "add",
    "path": "entitlements",
    "value": [{"value": x} for x in missing]
  }]
}))
')"

  databricks groups patch "$group_id" \
    -p "$DATABRICKS_PROFILE" \
    --json "$patch_json"

  echo "OK: ensured required entitlements for $group_name"
}

# Portal state implemented in Step 10:
# owners    -> Workspace access only
# engineers -> Workspace access + Databricks SQL access
# analysts  -> Workspace access + Databricks SQL access
ensure_entitlements "$OWNER_GROUP" workspace-access
ensure_entitlements "$ENGINEER_GROUP" workspace-access databricks-sql-access
ensure_entitlements "$ANALYST_GROUP" workspace-access databricks-sql-access

echo
echo "NOTE: This CLI script does not revoke extra entitlements."
echo "Use Terraform plan/apply for authoritative false values on cluster/pool creation."
