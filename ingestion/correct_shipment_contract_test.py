import argparse
import os
import sys
from pathlib import Path
from uuid import uuid4

# Use the current Python interpreter for Spark.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession, functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TARGET_SHIPMENT = "SHP-000006"
INVALID_STATUS = "unknown_test_status"


def project_path(value):
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path.resolve()


def main():
    parser = argparse.ArgumentParser(
        description="Correct the controlled invalid-status test delivery."
    )
    parser.add_argument("--input", required=True)
    args = parser.parse_args()

    input_path = project_path(args.input)
    baseline_path = (
        PROJECT_ROOT
        / "data/raw/postgres/shipments/snapshot_baseline"
    )
    output_path = (
        PROJECT_ROOT
        / "data/contract_tests/postgres/shipments"
        / f"corrected_status_{uuid4().hex[:8]}"
    )

    spark = None
    baseline = None
    invalid = None

    try:
        for path in (input_path, baseline_path):
            if not path.is_dir():
                raise FileNotFoundError(f"Folder does not exist: {path}")

        spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("CorrectShipmentContractTest")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")

        # Read the trusted baseline and the failed delivery separately.
        baseline = spark.read.parquet(str(baseline_path)).cache()
        invalid = spark.read.parquet(str(input_path)).cache()

        # Obtain the original status from evidence, rather than guessing it.
        original_records = (
            baseline
            .filter(F.col("shipment_id") == TARGET_SHIPMENT)
            .select("shipment_status")
            .collect()
        )

        if len(original_records) != 1:
            raise ValueError("Expected exactly one target in the baseline.")

        original_status = original_records[0]["shipment_status"]
        if original_status is None or original_status == INVALID_STATUS:
            raise ValueError("Baseline does not provide a usable correction.")

        target = invalid.filter(
            F.col("shipment_id") == TARGET_SHIPMENT
        )
        invalid_count = invalid.filter(
            F.col("shipment_status") == INVALID_STATUS
        ).count()

        # Confirm this is the intended one-row experiment.
        if target.count() != 1 or invalid_count != 1:
            raise ValueError(
                "Expected one target shipment and one invalid status."
            )

        if target.first()["shipment_status"] != INVALID_STATUS:
            raise ValueError("Target shipment does not have the test violation.")

        print("\nBefore correction:")
        target.show(truncate=False)

        # Restore only the target status. All other values are retained.
        corrected = invalid.withColumn(
            "shipment_status",
            F.when(
                F.col("shipment_id") == TARGET_SHIPMENT,
                F.lit(original_status),
            ).otherwise(F.col("shipment_status")),
        )

        output_path.parent.mkdir(parents=True, exist_ok=True)
        corrected.write.mode("errorifexists").parquet(str(output_path))

        # Verify the files using Python before reading them back with Spark.
        if not list(output_path.glob("*.parquet")):
            raise RuntimeError(
                f"No Parquet files found at: {output_path}"
            )

        saved = spark.read.parquet(str(output_path))
        input_rows = invalid.count()
        saved_rows = saved.count()

        if saved_rows != input_rows:
            raise RuntimeError("Correction changed the row count.")

        if saved.filter(
            F.col("shipment_status") == INVALID_STATUS
        ).count() != 0:
            raise RuntimeError("Invalid status remains in corrected output.")

        # Compare every saved value with the intended corrected DataFrame.
        # exceptAll preserves duplicate multiplicity during comparison.
        saved_ordered = saved.select(*corrected.columns)
        if (
            corrected.exceptAll(saved_ordered).limit(1).count()
            or saved_ordered.exceptAll(corrected).limit(1).count()
        ):
            raise RuntimeError("Saved output differs from intended correction.")

        print("After correction:")
        saved.filter(
            F.col("shipment_id") == TARGET_SHIPMENT
        ).show(truncate=False)

        print(f"Restored status: {original_status}")
        print(f"Input rows: {input_rows}")
        print(f"Corrected rows: {saved_rows}")
        print("Correction saved: PASS")

        relative_path = output_path.relative_to(PROJECT_ROOT).as_posix()
        print("\nRun contract validation:")
        print(
            'python .\\ingestion\\validate_shipment_contract.py '
            f'--input "{relative_path}"'
        )

    finally:
        if baseline is not None:
            baseline.unpersist()
        if invalid is not None:
            invalid.unpersist()
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()