import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

# Tell Spark to use the same Python interpreter running this script.
# This addresses the earlier Windows warning about missing "python3".
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession, functions as F


# The script lives in ingestion/, so its parent's parent is the project root.
# Relative input, contract, and report paths are resolved from this location.
PROJECT_ROOT = Path(__file__).resolve().parents[1]


def project_path(value):
    """Convert a supplied path into an absolute project-based path."""
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path.resolve()


def load_contract(path):
    """Read the contract and confirm this validator supports its policies."""
    # utf-8-sig also accepts files containing a UTF-8 byte-order mark.
    with path.open("r", encoding="utf-8-sig") as handle:
        contract = json.load(handle)

    # Reject unsupported policy settings instead of silently ignoring them.
    # For example, this version supports reporting extra columns, not
    # rejecting them. Changing that policy requires validator changes too.
    supported = {
        "required_columns": "all_declared_fields",
        "type_policy": "exact_spark_types",
        "nullability_check": "inspect_actual_values",
        "extra_columns_policy": "allow_and_report",
        "on_violation": "fail_consumer_gate",
        "preserve_input": True,
        "report_contract_version": True,
        "report_rule_results": True,
    }

    validation = contract["validation"]
    for name, expected in supported.items():
        if validation.get(name) != expected:
            raise ValueError(
                f"Unsupported validation policy: {name}="
                f"{validation.get(name)!r}"
            )

    # This implementation checks Parquet snapshots. It does not yet
    # interpret API page envelopes or incremental-delivery contracts.
    if contract["dataset"]["delivery_kind"] != "snapshot":
        raise ValueError("This validator supports snapshot contracts only.")

    if contract["dataset"]["format"] != "parquet":
        raise ValueError("This validator supports Parquet input only.")

    # Confirm that delivery policies match the checks implemented below.
    # A non-null SLA or fixed row count would require additional checks.
    delivery = contract["delivery"]
    if delivery["empty_snapshot_policy"] != "fail":
        raise ValueError("Unsupported empty snapshot policy.")
    if delivery.get("freshness_sla") is not None:
        raise ValueError("Freshness SLA validation is not implemented.")
    if delivery.get("expected_row_count") is not None:
        raise ValueError("Fixed row-count validation is not implemented.")

    # Check the contract's field definitions before using them on data.
    # These are configuration checks, not shipment-record checks.
    fields = contract["fields"]
    names = [field["name"] for field in fields]
    if not names or len(names) != len(set(names)):
        raise ValueError("Contract field names must be nonempty and unique.")

    supported_types = {"string", "timestamp", "bigint"}
    for field in fields:
        if field["spark_type"] not in supported_types:
            raise ValueError(f"Unsupported type for {field['name']}.")
        if "allow_blank" in field and field["spark_type"] != "string":
            raise ValueError("allow_blank requires a string field.")
        if "minimum" in field and field["spark_type"] != "bigint":
            raise ValueError("minimum currently requires a bigint field.")
        if "allowed_values" in field and not field["allowed_values"]:
            raise ValueError("allowed_values must not be empty.")

    # The uniqueness check must reference declared columns and agree
    # with the primary key documented in the dataset section.
    keys = validation["unique_key"]
    if not keys or any(key not in names for key in keys):
        raise ValueError("Unique keys must reference declared fields.")
    if keys != contract["dataset"]["primary_key"]:
        raise ValueError("Dataset and validation keys disagree.")

    return contract


def validate_snapshot(df, contract, report):
    """Run contract checks and add their results to the shared report."""
    results = report["rules"]

    # These helpers give every rule a consistent report structure.
    # A check passes when its violation count is zero.
    def record(rule, status, **details):
        results.append({
            "rule": rule,
            "status": status,
            **details,
        })

    def check(rule, violations, **details):
        record(
            rule,
            "PASS" if violations == 0 else "FAIL",
            violations=int(violations),
            **details,
        )

    # Capture measured input evidence. simpleString() represents Spark
    # types using names matching our contract, such as string and bigint.
    row_count = df.count()
    report["row_count"] = row_count
    report["actual_schema"] = {
        field.name: field.dataType.simpleString()
        for field in df.schema.fields
    }

    check("snapshot_not_empty", int(row_count == 0))

    # Compare declared and actual columns. Missing required columns fail;
    # additional columns are permitted and listed for visibility.
    fields = contract["fields"]
    declared = {field["name"] for field in fields}
    actual = report["actual_schema"]

    missing = sorted(declared - set(actual))
    extras = sorted(set(actual) - declared)

    check("required_columns", len(missing), missing_columns=missing)
    record(
        "extra_columns",
        "PASS",
        extra_columns=extras,
        policy="allow_and_report",
    )

    for field in fields:
        name = field["name"]
        expected_type = field["spark_type"]

        # The required-columns rule already reports this failure.
        # Skip dependent checks because the column cannot be inspected.
        if name not in actual:
            record(
                f"{name}.value_checks",
                "SKIP",
                reason="Required column is missing.",
            )
            continue

        type_matches = actual[name] == expected_type
        check(
            f"{name}.type",
            int(not type_matches),
            expected=expected_type,
            actual=actual[name],
        )

        column = F.col(name)

        # Inspect actual null values rather than relying on the schema's
        # nullable flag: Parquet can report nullable fields even when
        # the source database uses NOT NULL constraints.
        if field["nullable"] is False:
            check(
                f"{name}.not_null",
                df.filter(column.isNull()).count(),
            )

        # Avoid automatic casts that could conceal a producer type change.
        # Null checks are still possible, but typed checks are skipped.
        if not type_matches:
            record(
                f"{name}.typed_value_checks",
                "SKIP",
                reason="Column type does not match the contract.",
            )
            continue

        # Null and blank are different problems. This rule catches
        # empty strings and strings consisting only of whitespace.
        if field.get("allow_blank") is False:
            blank = (
                column.isNotNull()
                & column.rlike(r"^\s*$")
            )
            check(f"{name}.not_blank", df.filter(blank).count())

        # Apply the allowed-value list from the contract, rather than
        # hard-coding shipment statuses in this Python script.
        if "allowed_values" in field:
            invalid = (
                column.isNotNull()
                & ~column.isin(field["allowed_values"])
            )
            check(
                f"{name}.allowed_values",
                df.filter(invalid).count(),
                allowed_values=field["allowed_values"],
            )

        # Apply numeric lower bounds, such as source_version >= 1.
        # Nulls are handled separately by the not-null rule.
        if "minimum" in field:
            invalid = (
                column.isNotNull()
                & (column < F.lit(field["minimum"]))
            )
            check(
                f"{name}.minimum",
                df.filter(invalid).count(),
                minimum=field["minimum"],
            )

    # Check uniqueness across the whole snapshot.
    # The violation count measures repeated key groups, not excess rows:
    # one ID occurring three times counts as one duplicate key group.
    keys = contract["validation"]["unique_key"]
    if all(key in actual for key in keys):
        duplicate_groups = (
            df.groupBy(*keys)
            .count()
            .filter(F.col("count") > 1)
        )
        check(
            "snapshot_unique_key",
            duplicate_groups.count(),
            keys=keys,
            counting_unit="duplicate_key_groups",
        )
    else:
        record(
            "snapshot_unique_key",
            "SKIP",
            reason="A key column is missing.",
        )

    # Any failed rule fails the snapshot. A dependent SKIP never cancels
    # the missing-column or type failure that caused it.
    report["status"] = (
        "FAIL"
        if any(result["status"] == "FAIL" for result in results)
        else "PASS"
    )


def main():
    # Command-line arguments let us reuse the validator with controlled
    # test copies later, without editing the script's input path.
    parser = argparse.ArgumentParser(
        description="Validate a Parquet snapshot against a JSON contract."
    )
    parser.add_argument(
        "--contract",
        default="contracts/postgres/shipments_snapshot_v1.json",
    )
    parser.add_argument(
        "--input",
        default="data/raw/postgres/shipments/snapshot_baseline",
    )
    parser.add_argument(
        "--report-dir",
        default="data/validation/postgres/shipments",
    )
    args = parser.parse_args()

    contract_path = project_path(args.contract)
    input_path = project_path(args.input)
    report_dir = project_path(args.report_dir)

    # Keep reports outside the snapshot so later Parquet reads do not
    # accidentally discover report files alongside the source data.
    if report_dir == input_path or input_path in report_dir.parents:
        raise ValueError("Report directory must be outside the input path.")

    report_dir.mkdir(parents=True, exist_ok=True)

    # Give each execution its own report, preserving earlier evidence.
    # UTC time plus a random suffix makes filename collisions unlikely.
    started = datetime.now(timezone.utc)
    run_id = (
        started.strftime("%Y%m%dT%H%M%S_%fZ")
        + "_"
        + uuid4().hex[:8]
    )
    report_path = report_dir / f"validation_{run_id}.json"

    # Begin with ERROR until validation completes successfully.
    # These limitations prevent a PASS from implying guarantees that
    # this validator cannot establish by inspecting one snapshot.
    report = {
        "run_id": run_id,
        "started_at_utc": started.isoformat(),
        "contract_path": str(contract_path),
        "input_path": str(input_path),
        "status": "ERROR",
        "rules": [],
        "limitations": [
            "Does not prove timestamp preservation during extraction.",
            "Does not prove that every update advances timestamp/version.",
            "Does not validate delivery SLAs or capture physical deletes.",
        ],
    }

    spark = None
    df = None
    exit_code = 2

    try:
        contract = load_contract(contract_path)
        report["contract_id"] = contract["contract_id"]
        report["contract_version"] = contract["contract_version"]

        if not input_path.is_dir():
            raise FileNotFoundError(
                f"Snapshot folder does not exist: {input_path}"
            )

        # Run locally with two worker threads. UTC controls Spark's
        # timestamp interpretation/display; it does not itself prove
        # that the earlier JDBC extraction preserved the source instant.
        spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("ShipmentSnapshotContractValidation")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")

        # Read the saved files directly: no PostgreSQL connection or
        # JDBC driver is needed. Cache because many checks reuse them.
        df = spark.read.parquet(str(input_path)).cache()
        validate_snapshot(df, contract, report)

        # Exit codes allow a future calling process to decide whether
        # to continue: 0 = valid, 1 = violations, 2 = execution error.
        exit_code = 0 if report["status"] == "PASS" else 1

    except Exception as error:
        # A missing input, invalid contract, or Spark error is different
        # from a successfully executed check that found bad data.
        report["status"] = "ERROR"
        report["error"] = {
            "type": type(error).__name__,
            "message": str(error),
        }
        exit_code = 2

    finally:
        # Release resources even if validation fails. Cleanup errors
        # are recorded separately and make the execution unsuccessful.
        try:
            if df is not None:
                df.unpersist()
            if spark is not None:
                spark.stop()
        except Exception as error:
            report["cleanup_error"] = str(error)
            report["status"] = "ERROR"
            exit_code = 2

        report["finished_at_utc"] = (
            datetime.now(timezone.utc).isoformat()
        )

        # Save the detailed report. Exclusive-create mode ("x") prevents
        # overwriting an existing report if a filename collision occurs.
        with report_path.open("x", encoding="utf-8") as handle:
            json.dump(report, handle, indent=2, ensure_ascii=False)

    # Print a concise console summary; the JSON report retains details
    # such as expected/actual types and the list of additional columns.
    print(f"\nContract: {report.get('contract_id', 'unavailable')}")
    print(f"Version: {report.get('contract_version', 'unavailable')}")
    print(f"Snapshot rows: {report.get('row_count', 'unavailable')}")

    for result in report["rules"]:
        count = result.get("violations")
        detail = f" | violations={count}" if count is not None else ""
        print(f"{result['status']}: {result['rule']}{detail}")

    if "error" in report:
        print(f"Error: {report['error']['message']}")

    print(f"Contract validation: {report['status']}")
    print(f"Report: {report_path}")

    return exit_code


# Run main() only when executing this file directly.
# Importing it from another script will not start validation automatically.
if __name__ == "__main__":
    sys.exit(main())