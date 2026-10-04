#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/00-config.sh"

add_grants() {
  local securable_type="$1"
  local full_name="$2"
  local principal="$3"
  shift 3
  local privileges=("$@")

  local json_privs=""
  local p
  for p in "${privileges[@]}"; do
    [[ -n "$json_privs" ]] && json_privs+=","
    json_privs+="\"$p\""
  done

  databricks grants update "$securable_type" "$full_name" \
    -p "$DATABRICKS_PROFILE" \
    --json "{\"changes\":[{\"principal\":\"$principal\",\"add\":[${json_privs}]}]}"
}

# Baseline grants already established/understood.
add_grants catalog "$CATALOG" "account users" BROWSE
add_grants catalog "$CATALOG" "$ENGINEER_GROUP" USE_CATALOG
add_grants schema "${CATALOG}.bronze" "$ENGINEER_GROUP" USE_SCHEMA CREATE_TABLE CREATE_VOLUME SELECT MODIFY

if [[ "$APPLY_REMAINING_UC_PERMISSIONS" != "true" ]]; then
  echo
  echo "Remaining Step-10 grants intentionally skipped."
  echo "To apply them later: export APPLY_REMAINING_UC_PERMISSIONS=true"
  exit 0
fi

# Remaining engineer access.
add_grants schema "${CATALOG}.silver" "$ENGINEER_GROUP" USE_SCHEMA CREATE_TABLE CREATE_VOLUME SELECT MODIFY
add_grants schema "${CATALOG}.gold" "$ENGINEER_GROUP" USE_SCHEMA CREATE_TABLE CREATE_VOLUME SELECT MODIFY
add_grants schema "${CATALOG}.operations" "$ENGINEER_GROUP" USE_SCHEMA CREATE_TABLE SELECT MODIFY
add_grants schema "${CATALOG}.quarantine" "$ENGINEER_GROUP" USE_SCHEMA CREATE_TABLE SELECT MODIFY

# Narrow analyst path: catalog prerequisite + Gold read access only.
add_grants catalog "$CATALOG" "$ANALYST_GROUP" USE_CATALOG
add_grants schema "${CATALOG}.gold" "$ANALYST_GROUP" USE_SCHEMA SELECT
