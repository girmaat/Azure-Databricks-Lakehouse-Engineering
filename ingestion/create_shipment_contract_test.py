import os
import sys
from pathlib import Path
from uuid import uuid4

# Use the same Python interpreter for this script and Spark workers.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession, functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[1]

BASELINE_PATH = (
    PROJECT_ROOT
    / "data/raw/postgres/shipments/snapshot_baseline"
)

# Each execution creates a new folder, preserving earlier test evidence.
TEST_PATH = (
    PROJECT_ROOT
    / "data/contract_tests/postgres/shipments"
    / f"invalid_status_{uuid4().hex[:8]}"
)

TARGET_SHIPMENT = "SHP-000006"
INVALID_STATUS = "unknown_test_status"


def main():
    spark = None
    baseline = None

    try:
        if not BASELINE_PATH.is_dir():
            raise FileNotFoundError(
                f"Baseline folder does not exist: {BASELINE_PATH}"
            )

        spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("CreateShipmentContractTest")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")

        # Read and cache the baseline. All changes below create a new
        # DataFrame; they do not modify these source files.
        baseline = (
            spark.read
            .parquet(BASELINE_PATH.as_uri())
            .cache()
        )

        baseline_rows = baseline.count()
        target = baseline.filter(
            F.col("shipment_id") == TARGET_SHIPMENT
        )

        # Require exactly one target so the experiment changes one row.
        target_count = target.count()
        if target_count != 1:
            raise ValueError(
                f"Expected one {TARGET_SHIPMENT} record; "
                f"found {target_count}."
            )

        # Ensure this deliberately invalid value is absent initially.
        existing_invalid = baseline.filter(
            F.col("shipment_status") == INVALID_STATUS
        ).count()

        if existing_invalid != 0:
            raise ValueError(
                "Baseline already contains the intended invalid status."
            )

        print("\nOriginal record:")
        target.show(truncate=False)

        # Replace only the target shipment's status. All other columns
        # and rows retain their original values and types.
        test_df = baseline.withColumn(
            "shipment_status",
            F.when(
                F.col("shipment_id") == TARGET_SHIPMENT,
                F.lit(INVALID_STATUS),
            ).otherwise(F.col("shipment_status")),
        )

        print("Modified record:")
        test_df.filter(
            F.col("shipment_id") == TARGET_SHIPMENT
        ).show(truncate=False)

                # Create the parent using Python so its Windows location is explicit.
        TEST_PATH.parent.mkdir(parents=True, exist_ok=True)

        print(f"Intended test folder: {TEST_PATH}")

        # Use the native absolute Windows path for this local write.
        test_df.write.mode("errorifexists").parquet(
            str(TEST_PATH)
        )

        # Verify the output through Python's filesystem, independently
        # of Spark's interpretation of the path.
        parquet_files = list(TEST_PATH.glob("*.parquet"))

        if not TEST_PATH.is_dir() or not parquet_files:
            raise RuntimeError(
                "Spark finished writing, but Python cannot find Parquet "
                f"files at the intended location: {TEST_PATH}"
            )

        print(f"Parquet files found on disk: {len(parquet_files)}")

        # Read the verified location to check the saved contents.
        saved = spark.read.parquet(str(TEST_PATH))

        saved_rows = saved.count()
        invalid_rows = saved.filter(
            F.col("shipment_status") == INVALID_STATUS
        ).count()

        if saved_rows != baseline_rows:
            raise RuntimeError("Test output row count changed unexpectedly.")

        if invalid_rows != 1:
            raise RuntimeError(
                f"Expected one invalid status; found {invalid_rows}."
            )

        print(f"Baseline rows: {baseline_rows}")
        print(f"Saved test rows: {saved_rows}")
        print(f"Invalid status rows: {invalid_rows}")
        print("Test copy creation: PASS")

        # Print a ready-to-run command pointing to this specific copy.
        relative_path = TEST_PATH.relative_to(PROJECT_ROOT).as_posix()
        print("\nRun validation from your project root:")
        print(
            'python .\\ingestion\\validate_shipment_contract.py '
            f'--input "{relative_path}"'
        )

    finally:
        if baseline is not None:
            baseline.unpersist()
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()