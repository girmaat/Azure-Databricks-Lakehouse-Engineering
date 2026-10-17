
    
  
import json
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(
    "/Workspace/Users/databricks-qa@hotmail.com/"
    "Azure-Databricks-Lakehouse-Engineering"
)
TARGET = (
    PROJECT_ROOT
    / "contracts/bronze/postgres/shipments_incremental_v1.json"
)

# BLOCK A — Define only the three agreed checks.
# Build directly: do not copy unrelated snapshot rules.
contract = {
    "contract_id": "postgres.fulfillment.shipments.incremental",
    "contract_version": "2.0.0",
    "fields": [
        {
            "name": "carrier_code",
            "required": False,
            "spark_type": "string",
        },
        {
            "name": "source_version",
            "required": True,
            "spark_type": "bigint",
        },
        {
            "name": "shipment_status",
            "required": True,
            "allowed_values": ["delivered", "in_transit"],
        },
    ],
    "validation": {
        "on_violation": "fail_consumer_gate",
    },
}

# BLOCK B — Prepare a regular JSON file.
TARGET.parent.mkdir(parents=True, exist_ok=True)
content = (json.dumps(contract, indent=2) + "\n").encode("utf-8")
existing = TARGET.read_bytes() if TARGET.exists() else None

# BLOCK C — Preserve the previous file before replacing it.
# Byte comparison also handles an empty or invalid existing JSON file.
if existing == content:
    print(f"Existing contract already matches: {TARGET}")
else:
    if existing is not None:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        backup = TARGET.with_name(f"{TARGET.name}.{stamp}.bak")
        with backup.open("xb") as handle:
            handle.write(existing)
        print(f"Previous contract backed up: {backup}")

    TARGET.write_bytes(content)
    print(f"Prepared: {TARGET}")

print("Configuration prepared; shipment validation has not run.")

