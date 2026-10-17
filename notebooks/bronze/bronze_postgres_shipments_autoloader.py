# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Current Q70 walkthrough
# MAGIC Run only code cells 7 and 8, in that order. Do not use Run all: the retained historical cells include a full-table reset and ingestion without the contract gate.
# MAGIC Prerequisites: prepared three-rule incremental JSON, existing Bronze table with the approved carrier_code column, the three Q3 landing deliveries, and permission to create/write the results table in the existing operations schema. No local or cloud execution has been performed by preparing this file.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Code cell 1 — Original rebuild preparation
# MAGIC Historical Q2 setup. Captures the five source-field types and source-system label, and requires an unused checkpoint before a deliberate rebuild. Does not delete data. Do not run for the current Q70 replay; the checkpoint already exists.

# COMMAND ----------

from pyspark.sql.types import StructType

TABLE = "cat_adblh_dev.bronze.shipments_postgres_raw"

LANDING = (
    "abfss://landing@stadblhdeveus2001.dfs.core.windows.net/"
    "postgres/shipments"
)

CHECKPOINT_ROOT = (
    "abfss://checkpoints@stadblhdeveus2001.dfs.core.windows.net/"
)
CHECKPOINT_NAME = "q2_shipments_directory_listing_v1"
CHECKPOINT = CHECKPOINT_ROOT + CHECKPOINT_NAME

SOURCE_COLUMNS = [
    "shipment_id",
    "order_id",
    "shipment_status",
    "updated_at",
    "source_version",
]

# Preserve the actual source-field types before clearing Bronze.
existing = spark.table(TABLE)
SOURCE_SCHEMA = StructType([
    existing.schema[name] for name in SOURCE_COLUMNS
])

# Preserve the project's actual source-system label.
labels = [
    row["_source_system"]
    for row in existing.select("_source_system").distinct().collect()
]
if len(labels) != 1 or labels[0] is None:
    raise RuntimeError(
        "Expected one non-null source-system label before the reset."
    )

SOURCE_SYSTEM = labels[0]

# PROBLEM — Clearing Bronze while retaining completed progress
# could prevent already-processed files from refilling the table.
# Require an unused checkpoint for this deliberate rebuild.
checkpoint_entries = dbutils.fs.ls(CHECKPOINT_ROOT)

if any(
    item.name.rstrip("/") == CHECKPOINT_NAME
    for item in checkpoint_entries
):
    raise RuntimeError(
        "This checkpoint already exists. Stop before clearing anything."
    )

print("Source schema and source-system label captured.")
print(f"Checkpoint selected: {CHECKPOINT}")
print("No data has been changed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Code cell 2 — Original full Bronze reset
# MAGIC Historical Q2 reset. TRUNCATE removes every Bronze record while retaining the table definition. This was used before uploading the baseline. Do not run for Q70; code cell 7 performs the targeted deletion instead.

# COMMAND ----------

# Run once, after removing the five landed file copies.
# This clears table records, not the table definition or source files.
spark.sql(f"TRUNCATE TABLE {TABLE}")

print("Bronze records cleared. Upload the baseline, then run cell 3.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Code cell 3 — Directory listing ingestion
# MAGIC Historical Q2 ingestion. Auto Loader discovers Parquet files beneath LANDING using directory listing, adds provenance, and appends to Bronze using the saved checkpoint. availableNow processes the available input and then stops the query, not the compute. Does not execute the JSON contract. Do not run for this replay.

# COMMAND ----------

from uuid import uuid4
from pyspark.sql import functions as F

# Each execution has its own identifier.
run_id = str(uuid4())

# PROBLEM — Manual delivery selection and last-filename tracking.
# Auto Loader reads the landing root and maintains file-processing state.
source_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.useManagedFileEvents", "false")
    .option("cloudFiles.useNotifications", "false")
    .option("cloudFiles.includeExistingFiles", "true")
    .option("cloudFiles.partitionColumns", "")
    .schema(SOURCE_SCHEMA)
    .load(LANDING)
)

# Preserve source fields and record where each row came from.
bronze_stream = (
    source_stream.select(
        *SOURCE_COLUMNS,
        F.col("_metadata.file_path").alias("_source_file"),
    )
    .withColumn("_source_system", F.lit(SOURCE_SYSTEM))
    .withColumn(
        "_delivery_id",
        F.regexp_extract(
            F.col("_source_file"),
            r"/delivery_id=([^/]+)/",
            1,
        ),
    )
    .withColumn("_run_id", F.lit(run_id))
    .withColumn("_ingested_at", F.current_timestamp())
)

# PROBLEM — Duplicate ingestion during normal restart.
# Reuse this checkpoint and target for every arrival and retry.
query = (
    bronze_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .toTable(TABLE)
)

# Wait until this execution finishes before reading the result.
query.awaitTermination()

print(f"Current Bronze rows: {spark.table(TABLE).count()}")

display(
    spark.table(TABLE)
    .groupBy("_delivery_id")
    .agg(F.count("*").alias("rows"))
    .orderBy("_delivery_id")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Code cell 4 — Approved reader schema and learning deliveries
# MAGIC Historical Q3 setup. Reuses existing source types, adds optional carrier_code and the rescued-data column, ensures output columns exist, and creates missing demonstration deliveries from one baseline record. Existing fixture folders are retained. Do not run for this replay.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

TABLE = "cat_adblh_dev.bronze.shipments_postgres_raw"

LANDING = (
    "abfss://landing@stadblhdeveus2001.dfs.core.windows.net/"
    "postgres/shipments"
)

# Keep the existing checkpoint. Do not reset processing progress.
CHECKPOINT = (
    "abfss://checkpoints@stadblhdeveus2001.dfs.core.windows.net/"
    "q2_shipments_directory_listing_v1"
)

SOURCE_COLUMNS = [
    "shipment_id",
    "order_id",
    "shipment_status",
    "updated_at",
    "source_version",
]

DEMO_IDS = [
    "q3_added_field_v1",
    "q3_bad_version_v1",
    "q3_bad_status_v1",
]

current = spark.table(TABLE)

# Reuse the actual types of the original five source fields.
SOURCE_SCHEMA = StructType([
    current.schema[name] for name in SOURCE_COLUMNS
])

# PROBLEM — An approved field must survive a fresh notebook session.
# carrier_code is now an accepted, optional source field.
# _rescued_data preserves future unexpected fields and incompatible values.
READ_SCHEMA = StructType(
    list(SOURCE_SCHEMA.fields)
    + [
        StructField("carrier_code", StringType(), True),
        StructField("_rescued_data", StringType(), True),
    ]
)

# Preserve the existing project source-system label.
labels = [
    row["_source_system"]
    for row in current.select("_source_system").distinct().collect()
]

if len(labels) != 1 or labels[0] is None:
    raise RuntimeError(
        "Expected one existing non-null source-system label."
    )

SOURCE_SYSTEM = labels[0]

# Add missing output columns without clearing existing records.
for name in [
    "carrier_code",
    "_rescued_data",
    "_containment_reason",
]:
    if name not in spark.table(TABLE).columns:
        spark.sql(
            f"ALTER TABLE {TABLE} ADD COLUMNS (`{name}` STRING)"
        )

# Retain the original learning-fixture setup.
# Existing fixture folders are skipped, so completed deliveries
# are not overwritten or intentionally recreated.
existing_folders = {
    item.name.rstrip("/")
    for item in dbutils.fs.ls(LANDING)
}

missing_demo_ids = [
    delivery_id
    for delivery_id in DEMO_IDS
    if f"delivery_id={delivery_id}" not in existing_folders
]

if missing_demo_ids:
    # Freeze one baseline row before deriving demonstration records.
    seed_rows = (
        spark.table(TABLE)
        .filter(F.col("_delivery_id") == "snapshot_baseline")
        .select(*SOURCE_COLUMNS)
        .orderBy("shipment_id")
        .limit(1)
        .collect()
    )

    if not seed_rows:
        raise RuntimeError(
            "No baseline row is available to derive the fixtures."
        )

    seed = spark.createDataFrame(seed_rows, SOURCE_SCHEMA)

    fixtures = {
        "q3_added_field_v1": (
            seed.withColumn("carrier_code", F.lit("UPS"))
        ),
        "q3_bad_version_v1": (
            seed.withColumn("source_version", F.lit("N/A"))
        ),
        "q3_bad_status_v1": (
            seed.withColumn(
                "shipment_status",
                F.lit("UNKNOWN_STATUS"),
            )
        ),
    }

    # Illustrative platform-created deliveries, not PostgreSQL extracts.
    # The folder check is suitable for this single-writer exercise.
    for delivery_id in missing_demo_ids:
        (
            fixtures[delivery_id]
            .coalesce(1)
            .write.mode("append")
            .parquet(
                f"{LANDING}/delivery_id={delivery_id}/"
            )
        )

print(
    "Approved reader schema initialized. "
    "Existing Bronze records, fixture files, and checkpoint retained."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Code cell 5 — Rescue and label incoming records
# MAGIC Historical Q3 ingestion. Reads with the approved schema, preserves incompatible values in _rescued_data, assigns hard-coded containment reasons, and writes records including problematic ones. These labels are not the Q70 acceptance gate. Do not run for this replay.

# COMMAND ----------

from uuid import uuid4
from pyspark.sql import functions as F

before_count = spark.table(TABLE).count()
run_id = str(uuid4())

# PROBLEM — Unexpected fields and incompatible values must remain visible.
# Read carrier_code directly because it is now approved.
incoming = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.useManagedFileEvents", "false")
    .option("cloudFiles.useNotifications", "false")
    .option("cloudFiles.includeExistingFiles", "true")
    .option("cloudFiles.partitionColumns", "")
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .option("rescuedDataColumn", "_rescued_data")
    .schema(READ_SCHEMA)
    .load(LANDING)
    .select(
        *SOURCE_COLUMNS,
        "carrier_code",
        "_rescued_data",
        F.col("_metadata.file_path").alias("_source_file"),
    )
)

classified = (
    incoming
    .withColumn("_source_system", F.lit(SOURCE_SYSTEM))
    .withColumn(
        "_delivery_id",
        F.regexp_extract(
            F.col("_source_file"),
            r"/delivery_id=([^/]+)/",
            1,
        ),
    )
    .withColumn("_run_id", F.lit(run_id))
    .withColumn("_ingested_at", F.current_timestamp())

    # PROBLEM — Schema compatibility does not establish business validity.
    # Assign the first matching reason using explicit learning rules.
    .withColumn(
        "_containment_reason",
        F.when(
            F.col("source_version").isNull()
            | (F.col("source_version") < 1),
            F.lit("INVALID_SOURCE_VERSION"),
        )
        .when(
            F.col("shipment_status").isNull()
            | ~F.col("shipment_status").isin(
                "delivered",
                "in_transit",
            ),
            F.lit("INVALID_SHIPMENT_STATUS"),
        )
        .when(
            F.col("_rescued_data").isNotNull(),
            F.lit("UNAPPROVED_FIELDS_OR_TYPES"),
        )
        .otherwise(F.lit(None).cast("string")),
    )
)

# PROBLEM — Normal restarts must preserve processing memory.
# Use the same checkpoint and Delta target.
query = (
    classified.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .toTable(TABLE)
)

# Wait for available input to finish processing before measuring output.
query.awaitTermination()

after_count = spark.table(TABLE).count()

print({
    "rows_before_ingestion": before_count,
    "rows_after_ingestion": after_count,
    "rows_added": after_count - before_count,
})

# Display current persisted fixture results.
display(
    spark.table(TABLE)
    .filter(F.col("_delivery_id").isin(DEMO_IDS))
    .select(
        "_delivery_id",
        "carrier_code",
        "source_version",
        "shipment_status",
        "_rescued_data",
        "_containment_reason",
    )
    .orderBy("_delivery_id")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Code cell 6 — Apply the approved field update
# MAGIC Historical Q3 approval exercise. Rereads carrier_code from its source delivery and updates matching Bronze records without appending duplicates, then summarizes the earlier exercise. Requires earlier Q3 session variables, including before_approval; it is not a standalone fresh-session cell. Do not run for this replay.

# COMMAND ----------

from pyspark.sql import functions as F

# Explicit simulated approval for this learning exercise:
# carrier_code is approved as a string field.
if "carrier_code" not in spark.table(TABLE).columns:
    spark.sql(f"ALTER TABLE {TABLE} ADD COLUMNS (carrier_code STRING)")

ADDITION_PATH = f"{LANDING}/delivery_id=q3_added_field_v1/"

# PROBLEM — Reprocessing must not append a second copy.
# Reread only the affected delivery as a batch, then update its rows.
approved_source = (
    spark.read.parquet(ADDITION_PATH)
    .select(
        "shipment_id",
        "carrier_code",
        F.col("_metadata.file_path").alias("_source_file"),
    )
)
approved_source.createOrReplaceTempView("q3_approved_carrier")

spark.sql(f"""
    MERGE INTO {TABLE} AS target
    USING q3_approved_carrier AS source
    ON target._delivery_id = 'q3_added_field_v1'
       AND target.shipment_id = source.shipment_id
       AND target._source_file = source._source_file
    WHEN MATCHED
         AND target._containment_reason = 'UNAPPROVED_FIELDS_OR_TYPES'
    THEN UPDATE SET
        target.carrier_code = source.carrier_code,
        target._containment_reason = NULL
""")

# Update the notebook's expected reader schema for future executions.
# Cell 5 will use this updated schema when rerun in this session.
if "carrier_code" not in READ_SCHEMA.fieldNames():
    READ_SCHEMA = READ_SCHEMA.add("carrier_code", "string", True)

# Keep a complete source selection, including the approved addition.
# Apply the cell-5 reader adjustment explained below before future arrivals.

before_lookup = {
    row["_delivery_id"]: row["_containment_reason"]
    for row in before_approval
}

final_rows = (
    spark.table(TABLE)
    .filter(F.col("_delivery_id").isin(DEMO_IDS))
    .select(
        "_delivery_id",
        "carrier_code",
        "source_version",
        "shipment_status",
        "_rescued_data",
        "_containment_reason",
    )
    .orderBy("_delivery_id")
)

display(final_rows)

final_count = spark.table(TABLE).count()
print({
    "classification_before_approval": before_lookup,
    "rows_after_ingestion": after_count,
    "rows_after_approved_update": final_count,
    "rows_added_by_approved_update": final_count - after_count,
})

# One reconciliation for this exercise, using the persisted result.
counts = {
    r["_delivery_id"]: r["count"]
    for r in (
        spark.table(TABLE)
        .filter(F.col("_delivery_id").isin(DEMO_IDS))
        .groupBy("_delivery_id")
        .count()
        .collect()
    )
}
if counts != {delivery_id: 1 for delivery_id in DEMO_IDS}:
    raise RuntimeError(f"Unexpected fixture reconciliation: {counts}")

if final_count != after_count:
    raise RuntimeError("The approved update unexpectedly changed row count.")

print("Q3 fixture reconciliation: PASS")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Code cell 7 — Prepare and reset the three-delivery replay
# MAGIC Current Q70 action. Loads shipments_incremental_v1.json, captures Bronze schema and source label, and confirms that all three original landing deliveries are readable and nonempty. Then deletes only their Bronze rows. Source files and the Auto Loader checkpoint are retained. Run once before code cell 8. No backup files are created.

# COMMAND ----------

import json
from pathlib import Path
from uuid import uuid4
from datetime import datetime, timezone
from pyspark.sql import functions as F

TABLE = "cat_adblh_dev.bronze.shipments_postgres_raw"
RESULTS_TABLE = "cat_adblh_dev.operations.shipment_contract_results"
LANDING = (
    "abfss://landing@stadblhdeveus2001.dfs.core.windows.net/"
    "postgres/shipments"
)
CONTRACT_PATH = Path(
    "/Workspace/Users/databricks-qa@hotmail.com/"
    "Azure-Databricks-Lakehouse-Engineering/"
    "contracts/bronze/postgres/shipments_incremental_v1.json"
)
DEMO_IDS = [
    "q3_added_field_v1",
    "q3_bad_version_v1",
    "q3_bad_status_v1",
]

# BLOCK A — LOAD RULES
with CONTRACT_PATH.open("r", encoding="utf-8-sig") as handle:
    contract = json.load(handle)

if contract["validation"]["on_violation"] != "fail_consumer_gate":
    raise ValueError("This implementation supports fail_consumer_gate.")

target_schema = spark.table(TABLE).schema
labels = spark.table(TABLE).select("_source_system").distinct().collect()
if len(labels) != 1 or labels[0]["_source_system"] is None:
    raise RuntimeError("Expected one existing source-system label.")
source_system = labels[0]["_source_system"]

# Confirm readable, nonempty source deliveries before deleting their rows.
# No explicit schema: retain the actual source Parquet types.
replay_inputs = {}
for delivery_id in DEMO_IDS:
    path = f"{LANDING}/delivery_id={delivery_id}/"
    data = spark.read.parquet(path)
    if data.count() == 0:
        raise RuntimeError(f"No replay records found: {delivery_id}")
    replay_inputs[delivery_id] = data

# BLOCK B — TARGETED LEARNING RESET
demo_sql = ", ".join(f"'{value}'" for value in DEMO_IDS)
spark.sql(f"""
    DELETE FROM {TABLE}
    WHERE _delivery_id IN ({demo_sql})
""")
print(
    "Removed the three demonstration deliveries from Bronze. "
    "Landing files and the Auto Loader checkpoint are unchanged. "
    "Run code cell 8."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Code cell 8 — Validate and ingest in one execution
# MAGIC Current Q70 action. Uses the actual Parquet types, executes the three JSON field checks, and saves rule-level results to operations.shipment_contract_results. A failed rule blocks its entire delivery; passing input receives provenance and is appended to the existing Bronze table. Finishes with one summary of validation, action, and persisted row counts.
# MAGIC Q3 preserved and labeled problems; this cell makes JSON-driven validation control acceptance. It explicitly replays three selected deliveries and does not modify the Auto Loader streaming cells or use their checkpoint.
# MAGIC Block A loads rules in code cell 7. Block C executes required (column presence), spark_type (physical type), and allowed_values (membership including null rejection). Block D records rule evidence. Block E enforces fail_consumer_gate. Block F writes passing input. Block G shows the result.
# MAGIC Expected: added_field PASS with 1 Bronze row; bad_version FAIL with 0; bad_status FAIL with 0. These are expectations until execution. Assumes one writer and unchanged source files. An existing delivery is skipped on a normal cell-8 rerun; this is not correction processing. Validation and Bronze writes are separate commits, so a saved PASS does not prove a write committed. Original failed input remains in landing.

# COMMAND ----------

run_id = str(uuid4())
RESULT_SCHEMA = """
    run_id STRING,
    contract_id STRING,
    contract_version STRING,
    delivery_id STRING,
    source_path STRING,
    field STRING,
    rule STRING,
    expected STRING,
    observed STRING,
    status STRING,
    failed_rows BIGINT,
    checked_at TIMESTAMP
"""

def validate_and_ingest(delivery_id):
    data = replay_inputs[delivery_id]
    source_path = f"{LANDING}/delivery_id={delivery_id}/"
    row_count = data.count()
    actual_types = {
        field.name: field.dataType.simpleString()
        for field in data.schema.fields
    }
    results = []
    checked_at = datetime.now(timezone.utc)

    def record(field, rule, expected, observed, passed, failed_rows):
        results.append((
            run_id, contract["contract_id"], contract["contract_version"],
            delivery_id, source_path, field, rule, str(expected),
            str(observed), "PASS" if passed else "FAIL",
            int(failed_rows), checked_at,
        ))

    # BLOCK C — EXECUTE THE JSON RULES
    for field_rule in contract["fields"]:
        name = field_rule["name"]
        # "required" checks column presence, not null values.
        if name not in actual_types:
            required = field_rule["required"]
            record(
                name, "column_presence",
                "required" if required else "optional",
                "absent", not required, row_count if required else 0,
            )
            continue

        if "spark_type" in field_rule:
            expected_type = field_rule["spark_type"]
            observed_type = actual_types[name]
            passed = observed_type == expected_type
            record(
                name, "spark_type", expected_type, observed_type,
                passed, 0 if passed else row_count,
            )

        if "allowed_values" in field_rule:
            allowed = field_rule["allowed_values"]
            invalid = F.col(name).isNull() | ~F.col(name).isin(*allowed)
            failed_rows = data.filter(invalid).count()
            record(
                name, "allowed_values", json.dumps(allowed),
                f"{failed_rows} rows outside allowed values",
                failed_rows == 0, failed_rows,
            )

    # BLOCK D — PERSIST VALIDATION EVIDENCE
    # PASS describes validation, not proof of a committed Bronze write.
    (
        spark.createDataFrame(results, RESULT_SCHEMA)
        .write.format("delta").mode("append").saveAsTable(RESULTS_TABLE)
    )

    # BLOCK E — ENFORCE THE WHOLE-DELIVERY ACCEPTANCE GATE
    failed_rules = [
        f"{result[5]}.{result[6]}"
        for result in results if result[9] == "FAIL"
    ]
    if failed_rules:
        return (
            delivery_id, row_count, "FAIL",
            ", ".join(failed_rules), "BLOCKED",
        )

    # BLOCK F — WRITE ONLY A PASSING DELIVERY
    # This is a single-writer replay with unchanged source files.
    already_present = (
        spark.table(TABLE)
        .filter(F.col("_delivery_id") == delivery_id)
        .limit(1).count()
    )
    if already_present:
        return (delivery_id, row_count, "PASS", "", "ALREADY_PRESENT")

    output = (
        data
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_source_system", F.lit(source_system))
        .withColumn("_delivery_id", F.lit(delivery_id))
        .withColumn("_run_id", F.lit(run_id))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_rescued_data", F.lit(None).cast("string"))
        .withColumn("_containment_reason", F.lit(None).cast("string"))
    )
    for field in target_schema.fields:
        if field.name not in output.columns:
            output = output.withColumn(
                field.name, F.lit(None).cast(field.dataType)
            )
    (
        output.select(*target_schema.fieldNames())
        .write.format("delta").mode("append").saveAsTable(TABLE)
    )
    return (delivery_id, row_count, "PASS", "", "WRITTEN")

summary_rows = [
    validate_and_ingest(delivery_id) for delivery_id in DEMO_IDS
]

# BLOCK G — ONE CONSOLIDATED RESULT
summary = spark.createDataFrame(
    summary_rows,
    """
        delivery_id STRING, input_rows BIGINT, validation STRING,
        failed_rules STRING, action STRING
    """,
)
bronze_counts = (
    spark.table(TABLE)
    .filter(F.col("_delivery_id").isin(*DEMO_IDS))
    .groupBy("_delivery_id").count()
    .withColumnRenamed("_delivery_id", "delivery_id")
    .withColumnRenamed("count", "bronze_rows")
)
display(
    summary.join(bronze_counts, "delivery_id", "left")
    .fillna({"bronze_rows": 0}).orderBy("delivery_id")
)
print(f"Validation results saved in: {RESULTS_TABLE}")
print(f"Run ID: {run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Correct failed deliveries and release

# COMMAND ----------

# Correct failed deliveries and release
# Requires the completed Q70 preparation and validation cells.
# Do not rerun the targeted deletion cell.

CORRECTIONS = {
    "q3_bad_version_v1": "q70_corrected_version_v1",
    "q3_bad_status_v1": "q70_corrected_status_v1",
}

# BLOCK H — CREATE IDENTIFIABLE CORRECTED DELIVERIES
# Keep the failed originals unchanged.
existing_folders = {
    item.name.rstrip("/")
    for item in dbutils.fs.ls(LANDING)
}

for original_id, corrected_id in CORRECTIONS.items():
    corrected_path = f"{LANDING}/delivery_id={corrected_id}/"

    if f"delivery_id={corrected_id}" not in existing_folders:
        original = spark.read.parquet(
            f"{LANDING}/delivery_id={original_id}/"
        )

        if original_id == "q3_bad_version_v1":
            corrected = original.withColumn(
                "source_version",
                F.lit(1).cast("bigint"),
            )
        else:
            corrected = original.withColumn(
                "shipment_status",
                F.lit("delivered"),
            )

        (
            corrected.coalesce(1)
            .write.mode("append")
            .parquet(corrected_path)
        )

    # Read the saved correction, including its actual Parquet types.
    replay_inputs[corrected_id] = spark.read.parquet(corrected_path)


# BLOCK I — VALIDATE AND RELEASE THROUGH THE SAME FUNCTION
run_id = str(uuid4())
release_run_id = run_id

first_results = {
    corrected_id: validate_and_ingest(corrected_id)
    for corrected_id in CORRECTIONS.values()
}


# BLOCK J — REPEAT ACCEPTANCE WITHOUT DELETING ANY ROWS
run_id = str(uuid4())
repeat_run_id = run_id

repeat_results = {
    corrected_id: validate_and_ingest(corrected_id)
    for corrected_id in CORRECTIONS.values()
}


# BLOCK K — ONE CORRECTION AND REPLAY SUMMARY
summary_rows = []

for original_id, corrected_id in CORRECTIONS.items():
    first = first_results[corrected_id]
    repeated = repeat_results[corrected_id]

    summary_rows.append((
        original_id,
        corrected_id,
        first[1],       # Input row count
        first[2],       # First validation result
        first[4],       # First acceptance action
        repeated[2],    # Repeated validation result
        repeated[4],    # Repeated acceptance action
    ))

correction_summary = spark.createDataFrame(
    summary_rows,
    """
        original_delivery_id STRING,
        delivery_id STRING,
        input_rows BIGINT,
        first_validation STRING,
        first_action STRING,
        repeat_validation STRING,
        repeat_action STRING
    """,
)

corrected_counts = (
    spark.table(TABLE)
    .filter(
        F.col("_delivery_id").isin(*CORRECTIONS.values())
    )
    .groupBy("_delivery_id")
    .count()
    .withColumnRenamed("_delivery_id", "delivery_id")
    .withColumnRenamed("count", "bronze_rows")
)

display(
    correction_summary
    .join(corrected_counts, "delivery_id", "left")
    .fillna({"bronze_rows": 0})
    .orderBy("delivery_id")
)

print(f"Correction validation run: {release_run_id}")
print(f"Repeated acceptance run: {repeat_run_id}")

# COMMAND ----------

# Q4 — Explicit delivery acceptance and attempt recording
# Run this definition cell only. It does not ingest any deliveries.

import json
from pathlib import Path
from datetime import datetime, timezone

from pyspark.sql import functions as F

TABLE = "cat_adblh_dev.bronze.shipments_postgres_raw"
RESULTS_TABLE = "cat_adblh_dev.operations.shipment_contract_results"
ATTEMPTS_TABLE = "cat_adblh_dev.operations.shipment_ingestion_attempts"

LANDING = (
    "abfss://landing@stadblhdeveus2001.dfs.core.windows.net/"
    "postgres/shipments"
)

CONTRACT_PATH = Path(
    "/Workspace/Users/databricks-qa@hotmail.com/"
    "Azure-Databricks-Lakehouse-Engineering/"
    "contracts/bronze/postgres/shipments_incremental_v1.json"
)

RESULT_SCHEMA = """
    run_id STRING,
    contract_id STRING,
    contract_version STRING,
    delivery_id STRING,
    source_path STRING,
    field STRING,
    rule STRING,
    expected STRING,
    observed STRING,
    status STRING,
    failed_rows BIGINT,
    checked_at TIMESTAMP
"""

ATTEMPT_SCHEMA = """
    run_id STRING,
    delivery_id STRING,
    source_path STRING,
    contract_id STRING,
    contract_version STRING,
    input_rows BIGINT,
    validation STRING,
    action STRING,
    completed_at TIMESTAMP
"""


def validate_and_ingest(delivery_id, source_path, replay_run_id):
    # BLOCK A — EXPLICIT INPUTS AND CURRENT CONTRACT
    if not all(
        isinstance(value, str) and value.strip()
        for value in [delivery_id, source_path, replay_run_id]
    ):
        raise ValueError(
            "delivery_id, source_path, and replay_run_id "
            "must be nonempty strings."
        )

    with CONTRACT_PATH.open("r", encoding="utf-8-sig") as handle:
        active_contract = json.load(handle)

    if (
        active_contract["validation"]["on_violation"]
        != "fail_consumer_gate"
    ):
        raise ValueError("Only fail_consumer_gate is supported.")

    supported_field_keys = {
        "name", "required", "spark_type", "allowed_values"
    }
    for rule in active_contract["fields"]:
        unsupported = set(rule) - supported_field_keys
        if unsupported:
            raise ValueError(
                f"Unsupported field settings: {sorted(unsupported)}"
            )
        if not isinstance(rule["required"], bool):
            raise ValueError("Each required setting must be Boolean.")

    # Read actual Parquet types before any output conversion.
    data = spark.read.parquet(source_path)
    input_rows = data.count()

    if input_rows == 0:
        raise ValueError(f"Selected delivery is empty: {source_path}")

    target_schema = spark.table(TABLE).schema
    actual_types = {
        field.name: field.dataType.simpleString()
        for field in data.schema.fields
    }

    results = []
    checked_at = datetime.now(timezone.utc)

    def record(name, rule, expected, observed, passed, failed_rows):
        results.append((
            replay_run_id,
            active_contract["contract_id"],
            active_contract["contract_version"],
            delivery_id,
            source_path,
            name,
            rule,
            str(expected),
            str(observed),
            "PASS" if passed else "FAIL",
            int(failed_rows),
            checked_at,
        ))

    # BLOCK B — EXECUTE THE EXISTING CONTRACT RULES
    for field_rule in active_contract["fields"]:
        name = field_rule["name"]

        if name not in actual_types:
            required = field_rule["required"]
            record(
                name,
                "column_presence",
                "required" if required else "optional",
                "absent",
                not required,
                input_rows if required else 0,
            )
            continue

        if "spark_type" in field_rule:
            expected = field_rule["spark_type"]
            observed = actual_types[name]
            passed = observed == expected
            record(
                name, "spark_type", expected, observed,
                passed, 0 if passed else input_rows,
            )

        if "allowed_values" in field_rule:
            allowed = field_rule["allowed_values"]
            invalid = (
                F.col(name).isNull()
                | ~F.col(name).isin(*allowed)
            )
            failed_rows = data.filter(invalid).count()
            record(
                name,
                "allowed_values",
                json.dumps(allowed),
                f"{failed_rows} rows outside allowed values",
                failed_rows == 0,
                failed_rows,
            )

    # BLOCK C — SAVE RULE EVIDENCE
    (
        spark.createDataFrame(results, RESULT_SCHEMA)
        .write.format("delta")
        .mode("append")
        .saveAsTable(RESULTS_TABLE)
    )

    failed_rules = [
        f"{result[5]}.{result[6]}"
        for result in results
        if result[9] == "FAIL"
    ]

    # BLOCK D — GATE ACCEPTANCE AND PREVENT SAME-ID REAPPEND
    if failed_rules:
        validation = "FAIL"
        action = "BLOCKED"
    else:
        validation = "PASS"
        already_present = (
            spark.table(TABLE)
            .filter(F.col("_delivery_id") == delivery_id)
            .limit(1)
            .count()
        )

        if already_present:
            action = "ALREADY_PRESENT"
        else:
            labels = (
                spark.table(TABLE)
                .select("_source_system")
                .distinct()
                .collect()
            )
            if (
                len(labels) != 1
                or labels[0]["_source_system"] is None
            ):
                raise RuntimeError(
                    "Expected one existing source-system label."
                )

            output = (
                data
                .withColumn(
                    "_source_file", F.col("_metadata.file_path")
                )
                .withColumn(
                    "_source_system",
                    F.lit(labels[0]["_source_system"]),
                )
                .withColumn("_delivery_id", F.lit(delivery_id))
                .withColumn("_run_id", F.lit(replay_run_id))
                .withColumn(
                    "_ingested_at", F.current_timestamp()
                )
                .withColumn(
                    "_rescued_data", F.lit(None).cast("string")
                )
                .withColumn(
                    "_containment_reason",
                    F.lit(None).cast("string"),
                )
            )

            for field in target_schema.fields:
                if field.name not in output.columns:
                    output = output.withColumn(
                        field.name,
                        F.lit(None).cast(field.dataType),
                    )

            (
                output.select(*target_schema.fieldNames())
                .write.format("delta")
                .mode("append")
                .saveAsTable(TABLE)
            )
            action = "WRITTEN"

    # BLOCK E — RECORD THE COMPLETED ACCEPTANCE DECISION
    (
        spark.createDataFrame(
            [(
                replay_run_id,
                delivery_id,
                source_path,
                active_contract["contract_id"],
                active_contract["contract_version"],
                input_rows,
                validation,
                action,
                datetime.now(timezone.utc),
            )],
            ATTEMPT_SCHEMA,
        )
        .write.format("delta")
        .mode("append")
        .saveAsTable(ATTEMPTS_TABLE)
    )

    return {
        "run_id": replay_run_id,
        "delivery_id": delivery_id,
        "source_path": source_path,
        "input_rows": input_rows,
        "validation": validation,
        "failed_rules": ", ".join(failed_rules),
        "action": action,
    }


print("Q4 acceptance function ready. No deliveries processed.")

# COMMAND ----------

# Q4 — Renamed resend demonstration
# Requires the Q4 acceptance function definition cell.

from uuid import uuid4
from pyspark.sql import functions as F

# A previously accepted, one-row learning delivery.
delivery_id = "q3_added_field_v1"
original_path = f"{LANDING}/delivery_id={delivery_id}/"
resend_run_id = str(uuid4())

# Separate from the normal postgres/shipments Auto Loader input.
practice_root = (
    "abfss://landing@stadblhdeveus2001.dfs.core.windows.net/"
    "q4_practice/renamed_resends"
)
copy_folder = f"{practice_root}/{resend_run_id}"
renamed_path = f"{copy_folder}/vendor_resend_renamed.parquet"

# BLOCK A — CAPTURE THE SELECTED DELIVERY'S CURRENT BRONZE COUNT
before_rows = (
    spark.table(TABLE)
    .filter(F.col("_delivery_id") == delivery_id)
    .count()
)

if before_rows != 1:
    raise RuntimeError(
        f"Expected the accepted one-row delivery; found {before_rows}. "
        "Stop without resetting or reingesting anything."
    )

# BLOCK B — COPY THE SAME FILE UNDER A DIFFERENT NAME
source_files = [
    item.path
    for item in dbutils.fs.ls(original_path)
    if item.name.lower().endswith((".parquet", ".par"))
]

if len(source_files) != 1:
    raise RuntimeError(
        f"Expected one source data file; found {len(source_files)}."
    )

dbutils.fs.mkdirs(copy_folder)

if not dbutils.fs.cp(source_files[0], renamed_path):
    raise RuntimeError("The practice file copy did not succeed.")

# BLOCK C — ACCEPT USING THE ORIGINAL LOGICAL DELIVERY ID
result = validate_and_ingest(
    delivery_id=delivery_id,
    source_path=renamed_path,
    replay_run_id=resend_run_id,
)

# BLOCK D — SHOW THE PERSISTED ATTEMPT AND BRONZE EFFECT
after_rows = (
    spark.table(TABLE)
    .filter(F.col("_delivery_id") == delivery_id)
    .count()
)

display(
    spark.table(ATTEMPTS_TABLE)
    .filter(F.col("run_id") == resend_run_id)
    .select(
        "delivery_id",
        "source_path",
        "input_rows",
        "validation",
        "action",
    )
    .withColumn("bronze_before", F.lit(before_rows))
    .withColumn("bronze_after", F.lit(after_rows))
    .withColumn(
        "rows_added", F.lit(after_rows - before_rows)
    )
)

if (
    result["validation"] != "PASS"
    or result["action"] != "ALREADY_PRESENT"
    or after_rows != before_rows
):
    raise RuntimeError("Renamed-resend outcome differs from expectation.")

print("Renamed resend: PASS — no additional Bronze rows.")

# COMMAND ----------

# Q4 — Bounded backfill and repeat
# Requires the Q4 acceptance function definition cell.
# Artificial learning delivery; not a new PostgreSQL extract.

from uuid import uuid4
from pyspark.sql import functions as F

existing_id = "q3_added_field_v1"
backfill_id = "q4_historical_practice_v1"

original_path = f"{LANDING}/delivery_id={existing_id}/"

practice_path = (
    "abfss://landing@stadblhdeveus2001.dfs.core.windows.net/"
    "q4_practice/backfills/"
    f"delivery_id={backfill_id}/"
)
practice_file = f"{practice_path}historical_practice.parquet"

# BLOCK A — PREPARE ONE REUSABLE PRACTICE DELIVERY
source_files = [
    item.path
    for item in dbutils.fs.ls(original_path)
    if item.name.lower().endswith((".parquet", ".par"))
]

if len(source_files) != 1:
    raise RuntimeError("Expected one original learning data file.")

dbutils.fs.mkdirs(practice_path)

saved_files = [
    item.path
    for item in dbutils.fs.ls(practice_path)
    if item.name.lower().endswith((".parquet", ".par"))
]

if not saved_files:
    if not dbutils.fs.cp(source_files[0], practice_file):
        raise RuntimeError("Could not create the practice delivery.")
elif saved_files != [practice_file]:
    raise RuntimeError("Unexpected files in the practice delivery folder.")

# BLOCK B — DECLARE THE COMPLETE BACKFILL SCOPE
selected_deliveries = [
    (existing_id, original_path),
    (backfill_id, practice_file),
]
selected_ids = [
    delivery_id for delivery_id, _ in selected_deliveries
]


def selected_bronze_counts():
    counts = {
        row["_delivery_id"]: row["count"]
        for row in (
            spark.table(TABLE)
            .filter(F.col("_delivery_id").isin(*selected_ids))
            .groupBy("_delivery_id")
            .count()
            .collect()
        )
    }
    return {
        delivery_id: counts.get(delivery_id, 0)
        for delivery_id in selected_ids
    }


before = selected_bronze_counts()

# BLOCK C — PROCESS ONLY THE SELECTED DELIVERIES
first_run_id = str(uuid4())
first_results = {
    delivery_id: validate_and_ingest(
        delivery_id=delivery_id,
        source_path=source_path,
        replay_run_id=first_run_id,
    )
    for delivery_id, source_path in selected_deliveries
}
after_first = selected_bronze_counts()

# BLOCK D — REPEAT THE SAME SCOPE WITH A NEW EXECUTION ID
repeat_run_id = str(uuid4())
repeat_results = {
    delivery_id: validate_and_ingest(
        delivery_id=delivery_id,
        source_path=source_path,
        replay_run_id=repeat_run_id,
    )
    for delivery_id, source_path in selected_deliveries
}
after_repeat = selected_bronze_counts()

# BLOCK E — ONE SUMMARY USING THE PERSISTED ATTEMPT RECORDS
attempts = spark.table(ATTEMPTS_TABLE)

first_attempts = (
    attempts
    .filter(F.col("run_id") == first_run_id)
    .select(
        "delivery_id",
        "input_rows",
        F.col("validation").alias("first_validation"),
        F.col("action").alias("first_action"),
    )
)

repeat_attempts = (
    attempts
    .filter(F.col("run_id") == repeat_run_id)
    .select(
        "delivery_id",
        F.col("validation").alias("repeat_validation"),
        F.col("action").alias("repeat_action"),
    )
)

counts_summary = spark.createDataFrame(
    [
        (
            delivery_id,
            before[delivery_id],
            after_first[delivery_id],
            after_repeat[delivery_id],
            after_repeat[delivery_id] - after_first[delivery_id],
        )
        for delivery_id in selected_ids
    ],
    """
        delivery_id STRING,
        bronze_before BIGINT,
        bronze_after_first BIGINT,
        bronze_after_repeat BIGINT,
        repeat_rows_added BIGINT
    """,
)

display(
    first_attempts
    .join(repeat_attempts, "delivery_id")
    .join(counts_summary, "delivery_id")
    .orderBy("delivery_id")
)

# BLOCK F — CHECK THIS DEMONSTRATION'S EXPECTED OUTCOMES
for delivery_id in selected_ids:
    first = first_results[delivery_id]
    repeated = repeat_results[delivery_id]

    expected_action = (
        "ALREADY_PRESENT" if before[delivery_id] else "WRITTEN"
    )
    expected_rows = (
        before[delivery_id]
        if before[delivery_id]
        else first["input_rows"]
    )

    if (
        first["validation"] != "PASS"
        or first["action"] != expected_action
        or repeated["validation"] != "PASS"
        or repeated["action"] != "ALREADY_PRESENT"
        or after_first[delivery_id] != expected_rows
        or after_repeat[delivery_id] != expected_rows
    ):
        raise RuntimeError(
            f"Unexpected backfill outcome for {delivery_id}."
        )

print("Bounded backfill and repeat: PASS.")
print(f"First run: {first_run_id}")
print(f"Repeat run: {repeat_run_id}")