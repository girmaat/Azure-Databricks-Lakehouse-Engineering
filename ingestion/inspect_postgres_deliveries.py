import json
from pathlib import Path

from pyspark.sql import SparkSession, functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[1]

RAW_ROOT = PROJECT_ROOT / "data" / "raw" / "postgres" / "shipments"

WATERMARK_FILE = (
    PROJECT_ROOT
    / "data"
    / "checkpoints"
    / "postgres"
    / "shipments_watermark.json"
)


def inspect_delivery(spark, folder):
    part_files = sorted(folder.glob("part-*.parquet"))

    result = {
        "delivery": folder.name,
        "parquet_files": len(part_files),
        "success_marker": (folder / "_SUCCESS").is_file(),
    }

    if not part_files:
        result["inspection"] = "ERROR"
        result["reason"] = "No part-*.parquet files found"
        return result

    try:
        # Read only Parquet data files, excluding markers and CRC files.
        df = spark.read.parquet(
            *[file.as_posix() for file in part_files]
        )

        if "updated_at" not in df.columns:
            result["rows"] = df.count()
            result["inspection"] = "ERROR"
            result["reason"] = "updated_at column is missing"
            return result

        updated_type = df.schema["updated_at"].dataType.simpleString()
        result["updated_at_type"] = updated_type

        expressions = [
            F.count(F.lit(1)).alias("rows"),
            F.sum(
                F.when(F.col("updated_at").isNull(), 1).otherwise(0)
            ).alias("null_updated_at"),
        ]

        # Avoid silently interpreting strings as timestamps.
        temporal_type = updated_type in (
            "timestamp",
            "timestamp_ntz",
            "date",
        )

        if temporal_type:
            expressions.extend([
                F.min("updated_at").alias("min_updated_at"),
                F.max("updated_at").alias("max_updated_at"),
            ])

        summary = df.agg(*expressions).first().asDict()

        result["rows"] = summary["rows"]
        result["null_updated_at"] = summary["null_updated_at"] or 0

        if temporal_type:
            for field in ("min_updated_at", "max_updated_at"):
                value = summary[field]
                result[field] = (
                    value.isoformat() if value is not None else None
                )
            result["inspection"] = "READ_OK"
        else:
            result["inspection"] = "REVIEW"
            result["reason"] = (
                "Timestamp ranges omitted: updated_at is not "
                "a timestamp or date column"
            )

    except Exception as error:
        result["inspection"] = "ERROR"
        result["reason"] = str(error)

    return result


def main():
    if not RAW_ROOT.is_dir():
        raise FileNotFoundError(
            f"PostgreSQL extract folder not found: {RAW_ROOT}"
        )

    baseline = RAW_ROOT / "snapshot_baseline"
    incremental_root = RAW_ROOT / "incremental"

    # Name sorting gives stable display order, not extraction order.
    batches = (
        sorted(
            path
            for path in incremental_root.glob("batch_*")
            if path.is_dir()
        )
        if incremental_root.is_dir()
        else []
    )

    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("Inspect Saved PostgreSQL Deliveries")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    try:
        results = [
            inspect_delivery(spark, folder)
            for folder in [baseline, *batches]
        ]

        print("\nPOSTGRES_DELIVERY_INVENTORY")
        print(json.dumps(results, indent=2))

        if WATERMARK_FILE.is_file():
            checkpoint = json.loads(
                WATERMARK_FILE.read_text(encoding="utf-8")
            )

            batch_id = checkpoint.get("last_batch_id")
            latest_name = f"batch_{batch_id}" if batch_id else None

            latest = next(
                (
                    item for item in results
                    if item["delivery"] == latest_name
                ),
                None,
            )

            expected_rows = checkpoint.get("last_batch_rows")
            actual_rows = latest.get("rows") if latest else None

            if expected_rows is None or actual_rows is None:
                comparison = "NOT_VERIFIED"
            elif expected_rows == actual_rows:
                comparison = "MATCH"
            else:
                comparison = "MISMATCH"

            print("\nLATEST_CHECKPOINT_COMPARISON")
            print(json.dumps({
                "last_successful_watermark": checkpoint.get(
                    "last_successful_watermark"
                ),
                "latest_batch": latest_name,
                "checkpoint_rows": expected_rows,
                "observed_rows": actual_rows,
                "row_count_comparison": comparison,
            }, indent=2))
        else:
            print("\nCheckpoint file not found; comparison omitted.")

        print(
            "\nRead-only inspection finished. "
            "No extraction data or checkpoint was changed."
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()