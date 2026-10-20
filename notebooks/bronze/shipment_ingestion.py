# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# Operational PostgreSQL shipment Landing -> Bronze ingestion.
# This notebook deliberately excludes historical resets, practice fixtures,
# demonstrations, and manual delivery selection.
# Intended for a SINGLE-WRITER Databricks Job (maximum concurrent runs = 1).

# COMMAND ----------

# CELL 1 — Configure inputs and the existing Bronze/operations tables.
# Purpose: Fix the exact processing boundary and load the repository contract.
# Output: Contract requirements and execution identifiers.
# Data impact: Read-only in this cell.

import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from pyspark.sql import functions as F

LANDING_ROOT = (
    "abfss://landing@stadblhdeveus2001.dfs.core.windows.net/"
    "postgres/shipments/"
)
BRONZE_TABLE = "cat_adblh_dev.bronze.shipments_postgres_raw"
RESULTS_TABLE = "cat_adblh_dev.operations.shipment_contract_results"
ATTEMPTS_TABLE = "cat_adblh_dev.operations.shipment_ingestion_attempts"
CONTRACT_RELATIVE_PATH = Path(
    "contracts/bronze/postgres/shipments_incremental_v1.json"
)

# Unlike the historical exercises, this can ingest into an EMPTY Bronze table.
# The old function read its _source_system label from existing Bronze records;
# that approach cannot work when the first baseline delivery arrives.
# The producer identity here comes from the contract ID's first segment:
# "postgres.fulfillment.shipments.incremental" -> "postgres".
# Confirm this label is consistent with your intended Bronze provenance.
search_roots = [Path.cwd(), *Path.cwd().parents]
contract_path = next(
    (root / CONTRACT_RELATIVE_PATH for root in search_roots
     if (root / CONTRACT_RELATIVE_PATH).is_file()),
    None,
)
if contract_path is None:
    raise FileNotFoundError(f"Contract not found: {CONTRACT_RELATIVE_PATH}")

# with automatically closes the file; json.load parses it into a dictionary.
with contract_path.open("r", encoding="utf-8-sig") as handle:
    contract = json.load(handle)

# Explicitly support only the narrow, agreed contract format.
if not isinstance(contract, dict) or set(contract) != {
    "contract_id", "contract_version", "fields", "validation"
}:
    raise ValueError("Unexpected contract top-level structure")
if contract["validation"] != {"on_violation": "fail_consumer_gate"}:
    raise ValueError("Unsupported validation policy")
if not isinstance(contract["fields"], list) or not contract["fields"]:
    raise ValueError("Contract fields must be a nonempty list")
if not all(isinstance(contract[key], str) and contract[key]
           for key in ("contract_id", "contract_version")):
    raise ValueError("Contract identity/version must be nonempty strings")

field_names = []
for rule in contract["fields"]:
    if not isinstance(rule, dict):
        raise ValueError("Each contract field must be a JSON object")
    # set(rule) takes dictionary KEYS; - returns unexpected keys.
    # Example: {name, required, max_length} - {name, required, spark_type,
    # allowed_values} = {max_length}, which fails closed.
    unexpected = set(rule) - {"name", "required", "spark_type", "allowed_values"}
    if unexpected:
        raise ValueError(f"Unsupported field keys: {sorted(unexpected)}")
    if (not isinstance(rule.get("name"), str) or not rule["name"]
            or not isinstance(rule.get("required"), bool)):
        raise ValueError("Invalid field name or required flag")
    if "spark_type" in rule and rule["spark_type"] not in {"string", "bigint"}:
        raise ValueError(f"Unsupported Spark type in {rule['name']}")
    if "allowed_values" in rule and (
        not isinstance(rule["allowed_values"], list)
        or not rule["allowed_values"]
        or not all(isinstance(v, str) for v in rule["allowed_values"])
        or len(set(rule["allowed_values"])) != len(rule["allowed_values"])
    ):
        raise ValueError(f"Invalid allowed_values for {rule['name']}")
    field_names.append(rule["name"])
if len(set(field_names)) != len(field_names):
    raise ValueError("Duplicate contract field names")

SOURCE_SYSTEM = contract["contract_id"].split(".", 1)[0]
RUN_ID = str(uuid4())

# Use EXACT columns from the historical operational evidence tables.
RESULT_SCHEMA = """
    run_id STRING, contract_id STRING, contract_version STRING,
    delivery_id STRING, source_path STRING, field STRING,
    rule STRING, expected STRING, observed STRING, status STRING,
    failed_rows BIGINT, checked_at TIMESTAMP
"""
ATTEMPT_SCHEMA = """
    run_id STRING, delivery_id STRING, source_path STRING,
    contract_id STRING, contract_version STRING, input_rows BIGINT,
    validation STRING, action STRING, completed_at TIMESTAMP
"""

# Fail before processing if any required existing target is missing.
for table_name in (BRONZE_TABLE, RESULTS_TABLE, ATTEMPTS_TABLE):
    if not spark.catalog.tableExists(table_name):
        raise RuntimeError(f"Required table is missing: {table_name}")

# Ensure the target can store the five original source fields, optional carrier,
# and the lineage fields used by the historical Bronze writer.
target_schema = spark.table(BRONZE_TABLE).schema
target_columns = set(target_schema.fieldNames())
required_target_columns = {
    "shipment_id", "order_id", "shipment_status", "updated_at",
    "source_version", "carrier_code", "_source_file", "_source_system",
    "_delivery_id", "_run_id", "_ingested_at", "_rescued_data",
    "_containment_reason",
}
missing_target_columns = required_target_columns - target_columns
if missing_target_columns:
    raise RuntimeError(f"Bronze target lacks columns: {sorted(missing_target_columns)}")

# COMMAND ----------

# CELL 2 — Discover ONLY published delivery folders.
# Purpose: Prevent incomplete upload folders from entering validation.
# Output: List of dictionaries with delivery IDs, paths, and Parquet files.
# Data impact: Read-only.

def discover_published_deliveries(root):
    try:
        folders = dbutils.fs.ls(root)
    except Exception as exc:
        # Only a genuinely absent folder counts as empty input. Permissions,
        # storage outages, and other errors must fail the Job.
        message = str(exc).lower()
        if any(token in message for token in (
            "filenotfoundexception", "pathnotfound", "path_not_found"
        )):
            return []
        raise

    discovered = []
    for folder in folders:
        if not folder.isDir() or not folder.name.startswith("delivery_id="):
            continue
        delivery_id = folder.name.rstrip("/").split("=", 1)[1]
        if not delivery_id:
            raise RuntimeError(f"Empty delivery ID: {folder.path}")
        entries = dbutils.fs.ls(folder.path)
        files = sorted(
            item.path for item in entries
            if not item.isDir() and item.name.startswith("part-")
            and item.name.endswith(".parquet")
        )
        marker = any(
            not item.isDir() and item.name == "_SUCCESS" for item in entries
        )
        # A _SUCCESS marker is meaningful only if the publisher creates it
        # AFTER all Parquet parts were successfully uploaded.
        if files and marker:
            discovered.append({
                "delivery_id": delivery_id,
                "delivery_path": folder.path,
                "parquet_files": files,
            })
        else:
            print(f"INCOMPLETE (not processed): {delivery_id}")
    return sorted(discovered, key=lambda d: d["delivery_id"])

published_deliveries = discover_published_deliveries(LANDING_ROOT)
print(f"Published deliveries: {len(published_deliveries)}")

# COMMAND ----------

# CELL 3 — Evaluate physical Parquet types and actual record values.
# Purpose: Enforce all currently declared contract rules per delivery.
# Output: Validated data and rule-by-rule evidence, kept in memory.
# Data impact: Read-only.

def evaluate_delivery(delivery):
    delivery_id = delivery["delivery_id"]
    checked_at = datetime.now(timezone.utc)
    evidence = []
    frames = []
    total_rows = 0

    def record(path, field, rule_name, expected, observed, passed, failed_rows):
        evidence.append((
            RUN_ID, contract["contract_id"], contract["contract_version"],
            delivery_id, path, field, rule_name, str(expected),
            str(observed), "PASS" if passed else "FAIL", int(failed_rows),
            checked_at,
        ))

    for path in delivery["parquet_files"]:
        # Examine each physical file independently: a merged schema can hide
        # an incompatible type present in just one part file.
        df = spark.read.parquet(path)
        row_count = df.count()
        total_rows += row_count
        actual_types = {
            col.name: col.dataType.simpleString() for col in df.schema.fields
        }
        file_has_errors = False
        for rule in contract["fields"]:
            name = rule["name"]
            required = rule["required"]
            if name not in actual_types:
                record(path, name, "column_presence",
                       "required" if required else "optional", "absent",
                       not required, row_count if required else 0)
                if required:
                    file_has_errors = True
                continue

            record(path, name, "column_presence",
                   "required" if required else "optional", "present", True, 0)
            if "spark_type" in rule:
                expected = rule["spark_type"]
                actual = actual_types[name]
                same_type = expected == actual
                record(path, name, "spark_type", expected, actual,
                       same_type, 0 if same_type else row_count)
                if not same_type:
                    file_has_errors = True

            # required=True means no null record values, in addition to
            # requiring that the column exists.
            if required:
                null_rows = df.filter(F.col(name).isNull()).count()
                record(path, name, "required_non_null", "non-null",
                       f"{null_rows} null rows", null_rows == 0, null_rows)
                if null_rows:
                    file_has_errors = True

            if "allowed_values" in rule:
                allowed = rule["allowed_values"]
                # ~ negates a Spark Boolean expression; & means AND.
                # Example: status='cancelled' is not among
                # ['delivered', 'in_transit'] -> invalid row.
                # Nulls are counted by required_non_null when required.
                invalid_rows = df.filter(
                    F.col(name).isNotNull() & ~F.col(name).isin(*allowed)
                ).count()
                record(path, name, "allowed_values", json.dumps(allowed),
                       f"{invalid_rows} invalid rows",
                       invalid_rows == 0, invalid_rows)
                if invalid_rows:
                    file_has_errors = True

        # This file has already been checked, but preserve its DataFrame for
        # the later write; the write happens only if EVERY delivery is valid.
        if not file_has_errors:
            frames.append(df.withColumn("_source_file", F.lit(path)))

    if total_rows == 0:
        record(delivery["delivery_path"], "*", "nonempty_delivery",
               "at least one row", "0 rows", False, 0)

    failed = [r for r in evidence if r[9] == "FAIL"]
    return {
        "delivery": delivery,
        "input_rows": total_rows,
        "status": "FAIL" if failed else "PASS",
        "evidence": evidence,
        "frames": frames,
        "failed_rules": [f"{r[5]}.{r[6]}" for r in failed],
    }

assessments = [evaluate_delivery(d) for d in published_deliveries]
print("Validation outcomes:", [
    (a["delivery"]["delivery_id"], a["status"]) for a in assessments
])

# COMMAND ----------

# CELL 4 — Record validation, enforce gate, and accept passing deliveries.
# Purpose: Persist rule results, record BLOCKED decisions, and write PASS
# deliveries once with the historical Bronze lineage columns.
# Output: WRITTEN / ALREADY_PRESENT / BLOCKED operational evidence.
# Data impact: Appends only to existing Bronze and operations tables.
# No TRUNCATE, DELETE, fixture generation, or checkpoint reset.

# Save contract-evaluation evidence BEFORE any Bronze write.
all_rule_rows = [row for item in assessments for row in item["evidence"]]
if all_rule_rows:
    (spark.createDataFrame(all_rule_rows, RESULT_SCHEMA)
     .write.format("delta").mode("append").saveAsTable(RESULTS_TABLE))


def save_attempt(item, action):
    delivery = item["delivery"]
    row = [(
        RUN_ID, delivery["delivery_id"], delivery["delivery_path"],
        contract["contract_id"], contract["contract_version"],
        item["input_rows"], item["status"], action,
        datetime.now(timezone.utc),
    )]
    (spark.createDataFrame(row, ATTEMPT_SCHEMA)
     .write.format("delta").mode("append").saveAsTable(ATTEMPTS_TABLE))


# The consumer gate is ALL-OR-NOTHING for the current Job execution.
# If a single published delivery FAILS, persist its BLOCKED evidence and
# stop BEFORE writing any passing delivery. This prevents partial release.
failures = [item for item in assessments if item["status"] == "FAIL"]
if failures:
    for item in failures:
        save_attempt(item, "BLOCKED")
        print("BLOCKED:", item["delivery"]["delivery_id"],
              item["failed_rules"])
    raise RuntimeError(
        f"Consumer gate blocked {len(failures)} published delivery(s)"
    )

for item in assessments:
    delivery_id = item["delivery"]["delivery_id"]
    # Check if this logical delivery was already committed in Bronze.
    # Example: a retry finds _delivery_id='snapshot_baseline', so it skips
    # appending the same delivery again and records ALREADY_PRESENT.
    present = (
        spark.table(BRONZE_TABLE)
        .filter(F.col("_delivery_id") == delivery_id)
        .limit(1).count() > 0
    )
    if present:
        action = "ALREADY_PRESENT"
    else:
        # Avoid ambiguous partial writes across several part files:
        # combine all valid DataFrames before a single Delta append.
        frames = item["frames"]
        if not frames:
            raise RuntimeError(f"No valid Parquet rows for {delivery_id}")
        data = frames[0]
        for part in frames[1:]:
            data = data.unionByName(part, allowMissingColumns=True)

        # Preserve the original Bronze column names and types; existing
        # historical exercises used these same lineage fields.
        output = (
            data.withColumn("_source_system", F.lit(SOURCE_SYSTEM))
                .withColumn("_delivery_id", F.lit(delivery_id))
                .withColumn("_run_id", F.lit(RUN_ID))
                .withColumn("_ingested_at", F.current_timestamp())
                .withColumn("_rescued_data", F.lit(None).cast("string"))
                .withColumn("_containment_reason", F.lit(None).cast("string"))
        )
        # Do not silently discard missing required source fields. Match
        # target source types rather than manufacturing values.
        for name in ("shipment_id", "order_id", "shipment_status",
                     "updated_at", "source_version"):
            if name not in output.columns:
                raise RuntimeError(f"Source missing Bronze column: {name}")
        for column in target_schema.fields:
            if column.name not in output.columns:
                output = output.withColumn(
                    column.name, F.lit(None).cast(column.dataType)
                )
        (output.select(*target_schema.fieldNames())
         .write.format("delta").mode("append").saveAsTable(BRONZE_TABLE))
        action = "WRITTEN"

    # Bronze Delta append is one commit per delivery. Audit and Bronze
    # commits are NOT atomic together; a retry after a successful Bronze
    # commit can record ALREADY_PRESENT if this attempt's audit write failed.
    save_attempt(item, action)
    print(f"{delivery_id}: PASS / {action} / {item['input_rows']} input rows")

if not assessments:
    print("IDLE: No published deliveries. No Bronze or audit writes.")
print(f"Run ID: {RUN_ID}")