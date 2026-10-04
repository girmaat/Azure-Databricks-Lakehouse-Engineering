#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/00-config.sh"

BUDGET_NAME="${BUDGET_NAME:-budget-adblh-dev-monthly}"
BUDGET_AMOUNT="${BUDGET_AMOUNT:-20}"
CONTACT_EMAIL="${CONTACT_EMAIL:-}"

if [[ -z "$CONTACT_EMAIL" ]]; then
  echo "INFO: budget already exists manually."
  echo "To create/update from CLI later, export CONTACT_EMAIL before running this script."
  exit 0
fi

if az consumption budget show-with-rg --resource-group "$RESOURCE_GROUP" --budget-name "$BUDGET_NAME" >/dev/null 2>&1; then
  echo "SKIP: budget already exists: $BUDGET_NAME"
  exit 0
fi

az consumption budget create-with-rg \
  --budget-name "$BUDGET_NAME" \
  --resource-group "$RESOURCE_GROUP" \
  --amount "$BUDGET_AMOUNT" \
  --time-grain Monthly \
  --category Cost \
  --notifications '{
    "Actual_50":{"enabled":true,"operator":"GreaterThanOrEqualTo","threshold":50,"thresholdType":"Actual","contactEmails":["'"$CONTACT_EMAIL"'"]},
    "Actual_75":{"enabled":true,"operator":"GreaterThanOrEqualTo","threshold":75,"thresholdType":"Actual","contactEmails":["'"$CONTACT_EMAIL"'"]},
    "Actual_90":{"enabled":true,"operator":"GreaterThanOrEqualTo","threshold":90,"thresholdType":"Actual","contactEmails":["'"$CONTACT_EMAIL"'"]},
    "Actual_100":{"enabled":true,"operator":"GreaterThanOrEqualTo","threshold":100,"thresholdType":"Actual","contactEmails":["'"$CONTACT_EMAIL"'"]},
    "Forecast_100":{"enabled":true,"operator":"GreaterThanOrEqualTo","threshold":100,"thresholdType":"Forecasted","contactEmails":["'"$CONTACT_EMAIL"'"]}
  }'
