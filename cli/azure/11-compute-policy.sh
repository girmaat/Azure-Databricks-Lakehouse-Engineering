#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/00-config.sh"

# Updates the existing portal-created Step 11 compute policy.
# Requires a working Databricks CLI profile. It does not create or start compute.

POLICIES_JSON="$(databricks cluster-policies list -p "$DATABRICKS_PROFILE" -o json)"

read -r POLICY_ID POLICY_FAMILY_ID < <(
  POLICY_NAME="$DEV_ENGINEERING_POLICY" python -c '
import json, os, sys
name = os.environ["POLICY_NAME"]
data = json.load(sys.stdin)
items = data if isinstance(data, list) else data.get("policies", [])
for item in items:
    if item.get("name") == name:
        print(item.get("policy_id", ""), item.get("policy_family_id", ""))
        raise SystemExit(0)
raise SystemExit(f"Compute policy not found: {name}")
' <<<"$POLICIES_JSON"
)

if [[ -z "$POLICY_ID" ]]; then
  echo "ERROR: policy ID was not returned for $DEV_ENGINEERING_POLICY" >&2
  exit 1
fi

if [[ -z "$POLICY_FAMILY_ID" ]]; then
  echo "ERROR: policy does not report a policy family ID; expected Shared Compute family." >&2
  exit 1
fi

REQUEST_JSON="$(POLICY_ID="$POLICY_ID" POLICY_FAMILY_ID="$POLICY_FAMILY_ID" POLICY_NAME="$DEV_ENGINEERING_POLICY" python -c '
import json, os

overrides = {
  "spark_version": {
    "type": "fixed",
    "value": "auto:latest-lts",
    "hidden": True
  },
  "data_security_mode": {
    "type": "fixed",
    "value": "DATA_SECURITY_MODE_STANDARD",
    "hidden": True
  },
  "autotermination_minutes": {
    "type": "fixed",
    "value": 30,
    "hidden": True
  },
  "dbus_per_hour": {
    "type": "range",
    "maxValue": 10
  },
  "autoscale.max_workers": {
    "type": "range",
    "maxValue": 4
  }
}

print(json.dumps({
  "policy_id": os.environ["POLICY_ID"],
  "name": os.environ["POLICY_NAME"],
  "description": "Governed classic interactive compute policy for Azure Databricks Lakehouse Engineering DEV workloads.",
  "policy_family_id": os.environ["POLICY_FAMILY_ID"],
  "policy_family_definition_overrides": json.dumps(overrides)
}))
')"

databricks cluster-policies edit "$POLICY_ID" \
  -p "$DATABRICKS_PROFILE" \
  --json "$REQUEST_JSON"

# Additive permission update: engineers may use the governed policy without
# granting unrestricted cluster creation.
databricks cluster-policies update-permissions "$POLICY_ID" \
  -p "$DATABRICKS_PROFILE" \
  --json "{\"access_control_list\":[{\"group_name\":\"$ENGINEER_GROUP\",\"permission_level\":\"CAN_USE\"}]}"

echo "OK: updated $DEV_ENGINEERING_POLICY and granted CAN_USE to $ENGINEER_GROUP"
