import json
import os
from pathlib import Path

from pyspark.sql import SparkSession, functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[1]

BASELINE_DIR = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "postgres"
    / "shipments"
    / "snapshot_baseline"
)

WATERMARK_FILE = (
    PROJECT_ROOT
    / "data"
    / "checkpoints"
    / "postgres"
    / "shipments_watermark.json"
)


def main():
    # Initialization must never reset existing extraction progress.
    if WATERMARK_FILE.exists():
        progress = json.loads(
            WATERMARK_FILE.read_text(encoding="utf-8")
        )
        print(f"Existing watermark: {progress['last_successful_watermark']}")
        print("Existing progress retained; no changes made.")
        return

    if not list(BASELINE_DIR.glob("part-*.parquet")):
        raise FileNotFoundError(
            f"No baseline Parquet files found: {BASELINE_DIR}"
        )

    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("Initialize Shipment Watermark")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    try:
        baseline_df = spark.read.parquet(BASELINE_DIR.as_posix())

        baseline_watermark = (
            baseline_df
            .agg(
                F.date_format(
                    F.max("updated_at"),
                    "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'",
                ).alias("watermark")
            )
            .first()["watermark"]
        )

        if baseline_watermark is None:
            raise RuntimeError("Baseline has no usable updated_at timestamp.")

        progress = {
            "source_table": "fulfillment.shipments",
            "watermark_column": "updated_at",
            "last_successful_watermark": baseline_watermark,
            "initialized_from": str(BASELINE_DIR),
        }

        WATERMARK_FILE.parent.mkdir(parents=True, exist_ok=True)
        temporary_file = WATERMARK_FILE.with_suffix(".json.tmp")

        with temporary_file.open("w", encoding="utf-8") as file:
            json.dump(progress, file, indent=2)
            file.write("\n")
            file.flush()
            os.fsync(file.fileno())

        os.replace(temporary_file, WATERMARK_FILE)

        print(f"Initial watermark: {baseline_watermark}")
        print(f"Watermark file: {WATERMARK_FILE}")
        print("Watermark initialization: PASS")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()