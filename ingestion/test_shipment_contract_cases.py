import json
import os
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession, functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[1]
BASELINE = PROJECT_ROOT / "data/raw/postgres/shipments/snapshot_baseline"
VALIDATOR = PROJECT_ROOT / "ingestion/validate_shipment_contract.py"
CONTRACT = PROJECT_ROOT / "contracts/postgres/shipments_snapshot_v1.json"

RUN_ROOT = (
    PROJECT_ROOT
    / "data/contract_tests/postgres/shipments"
    / f"suite_{uuid4().hex[:8]}"
)

TARGET = "SHP-000006"


def create_cases():
    """Create independent test deliveries from the unchanged baseline."""
    spark = None
    baseline = None

    try:
        if not BASELINE.is_dir():
            raise FileNotFoundError(f"Missing baseline: {BASELINE}")

        spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("ShipmentContractTestCases")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")

        baseline = spark.read.parquet(str(BASELINE)).cache()
        baseline_rows = baseline.count()

        target = baseline.filter(F.col("shipment_id") == TARGET)
        if target.count() != 1:
            raise ValueError("Expected exactly one target shipment.")

        is_target = F.col("shipment_id") == TARGET

        # Each case starts from the baseline, so failures are isolated.
        cases = [
            (
                "duplicate_key",
                baseline.unionByName(target),
                1,
                "snapshot_unique_key",
                "FAIL",
                baseline_rows + 1,
            ),
            (
                "null_key",
                baseline.withColumn(
                    "shipment_id",
                    F.when(is_target, F.lit(None).cast("string"))
                    .otherwise(F.col("shipment_id")),
                ),
                1,
                "shipment_id.not_null",
                "FAIL",
                baseline_rows,
            ),
            (
                "blank_key",
                baseline.withColumn(
                    "shipment_id",
                    F.when(is_target, F.lit("   "))
                    .otherwise(F.col("shipment_id")),
                ),
                1,
                "shipment_id.not_blank",
                "FAIL",
                baseline_rows,
            ),
            (
                "negative_version",
                baseline.withColumn(
                    "source_version",
                    F.when(is_target, F.lit(-1).cast("bigint"))
                    .otherwise(F.col("source_version")),
                ),
                1,
                "source_version.minimum",
                "FAIL",
                baseline_rows,
            ),
            (
                "missing_updated_at",
                baseline.drop("updated_at"),
                1,
                "required_columns",
                "FAIL",
                baseline_rows,
            ),
            (
                "wrong_version_type",
                baseline.withColumn(
                    "source_version",
                    F.col("source_version").cast("string"),
                ),
                1,
                "source_version.type",
                "FAIL",
                baseline_rows,
            ),
            (
                "empty_snapshot",
                baseline.limit(0),
                1,
                "snapshot_not_empty",
                "FAIL",
                0,
            ),
            (
                "extra_optional_field",
                baseline.withColumn(
                    "carrier_reference",
                    F.lit("REFERENCE-TEST"),
                ),
                0,
                "extra_columns",
                "PASS",
                baseline_rows,
            ),
        ]

        RUN_ROOT.mkdir(parents=True, exist_ok=False)
        manifest = []

        # Persist each case and independently verify its files exist.
        for name, df, code, rule, status, rows in cases:
            input_path = RUN_ROOT / name
            df.write.mode("errorifexists").parquet(str(input_path))

            if not list(input_path.glob("*.parquet")):
                raise RuntimeError(f"No Parquet files found: {input_path}")

            manifest.append({
                "case": name,
                "input": str(input_path),
                "expected_exit_code": code,
                "expected_rule": rule,
                "expected_rule_status": status,
                "expected_rows": rows,
            })
            print(f"Created: {name}")

        return manifest

    finally:
        if baseline is not None:
            baseline.unpersist()
        if spark is not None:
            spark.stop()


def run_validation(case):
    """Run the existing validator and inspect its machine-readable report."""
    report_dir = RUN_ROOT / "_reports" / case["case"]
    report_dir.mkdir(parents=True, exist_ok=False)

    # Use a separate process for each validator execution. Save its full
    # console output while printing only a compact suite result.
    command = [
        sys.executable,
        str(VALIDATOR),
        "--contract", str(CONTRACT),
        "--input", case["input"],
        "--report-dir", str(report_dir),
    ]

    with (report_dir / "console.log").open(
        "w", encoding="utf-8"
    ) as log:
        completed = subprocess.run(
            command,
            cwd=str(PROJECT_ROOT),
            stdout=log,
            stderr=subprocess.STDOUT,
            check=False,
        )

    reports = list(report_dir.glob("validation_*.json"))
    result = {
        **case,
        "actual_exit_code": completed.returncode,
        "report_directory": str(report_dir),
        "test_passed": False,
    }

    if len(reports) != 1:
        result["error"] = "Expected exactly one validation report."
        return result

    with reports[0].open("r", encoding="utf-8") as handle:
        report = json.load(handle)

    rule_results = {
        rule["rule"]: rule
        for rule in report["rules"]
    }
    expected_rule = rule_results.get(case["expected_rule"])
    expected_overall = (
        "PASS" if case["expected_exit_code"] == 0 else "FAIL"
    )

    # Check the expected rule, overall result, row count, and exit code.
    # Also reject unexpected failures, even if the expected rule failed.
    failed_rules = [
        rule["rule"]
        for rule in report["rules"]
        if rule["status"] == "FAIL"
    ]
    expected_failures = (
        [case["expected_rule"]]
        if case["expected_rule_status"] == "FAIL"
        else []
    )

    checks = [
        completed.returncode == case["expected_exit_code"],
        report["status"] == expected_overall,
        report.get("row_count") == case["expected_rows"],
        expected_rule is not None,
        (
            expected_rule is not None
            and expected_rule["status"] == case["expected_rule_status"]
        ),
        sorted(failed_rules) == sorted(expected_failures),
    ]

    # Confirm an extra field was actually discovered, not merely tolerated
    # in principle.
    if case["case"] == "extra_optional_field":
        checks.append(
            "carrier_reference"
            in rule_results.get("extra_columns", {}).get(
                "extra_columns", []
            )
        )

    # For the one-row value/key cases, verify exactly one violation.
    if case["case"] in {
        "duplicate_key", "null_key", "blank_key", "negative_version"
    }:
        checks.append(
            expected_rule is not None
            and expected_rule.get("violations") == 1
        )

    result.update({
        "actual_validation_status": report["status"],
        "actual_rule_status": (
            expected_rule["status"] if expected_rule else "MISSING"
        ),
        "failed_rules": failed_rules,
        "test_passed": all(checks),
    })
    return result


def main():
    for path in (VALIDATOR, CONTRACT):
        if not path.is_file():
            raise FileNotFoundError(f"Missing required file: {path}")

    manifest = create_cases()
    results = []

    # Spark used for creation is stopped before starting validators.
    for case in manifest:
        print(f"\nValidating: {case['case']}", flush=True)
        result = run_validation(case)
        results.append(result)
        outcome = "PASS" if result["test_passed"] else "FAIL"
        print(
            f"TEST {outcome}: {case['case']} | "
            f"validator exit={result['actual_exit_code']}"
        )

    summary_path = RUN_ROOT / "suite_summary.json"
    with summary_path.open("x", encoding="utf-8") as handle:
        json.dump(results, handle, indent=2)

    passed = sum(result["test_passed"] for result in results)
    print(f"\nContract tests passed: {passed}/{len(results)}")
    print(f"Summary: {summary_path}")

    # A negative case passes its TEST when the validator correctly fails.
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())