import json
from pathlib import Path

from pyspark.sql import SparkSession


PROJECT_ROOT = Path(__file__).resolve().parents[1]

SHIPMENT_ROOT = (
    PROJECT_ROOT / "data" / "raw" / "postgres" / "shipments"
)

BASELINE_DIR = SHIPMENT_ROOT / "snapshot_baseline"
INCREMENTAL_ROOT = SHIPMENT_ROOT / "incremental"

WATERMARK_FILE = (
    PROJECT_ROOT
    / "data"
    / "checkpoints"
    / "postgres"
    / "shipments_watermark.json"
)

VERSION_KEYS = ["shipment_id", "source_version"]


def main():
    progress = json.loads(
        WATERMARK_FILE.read_text(encoding="utf-8")
    )

    # Use the explicitly recorded batch, not directory-name ordering.
    current_dir = Path(progress["last_batch_path"]).resolve()

    if current_dir.parent != INCREMENTAL_ROOT.resolve():
        raise RuntimeError("Recorded batch is outside the incremental folder.")

    if not list(current_dir.glob("part-*.parquet")):
        raise FileNotFoundError(f"Batch Parquet files missing: {current_dir}")

    # For this exercise, all other complete batches must precede
    # the current batch. Do not run extraction concurrently.
    history_dirs = [BASELINE_DIR]

    history_dirs.extend(
        path
        for path in sorted(INCREMENTAL_ROOT.glob("batch_*"))
        if path.is_dir()
        and path.resolve() != current_dir
        and (path / "_SUCCESS").is_file()
    )

    for path in history_dirs:
        if not list(path.glob("part-*.parquet")):
            raise FileNotFoundError(f"History Parquet files missing: {path}")

    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("Inspect Shipment Batch Duplicates")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    try:
        current_df = spark.read.parquet(current_dir.as_posix())

        history_df = spark.read.parquet(
            *[path.as_posix() for path in history_dirs]
        )

        history_keys_df = (
            history_df
            .select(*VERSION_KEYS)
            .dropDuplicates(VERSION_KEYS)
        )

        current_versions_df = current_df.dropDuplicates(VERSION_KEYS)

        new_versions_df = current_versions_df.join(
            history_keys_df,
            on=VERSION_KEYS,
            how="left_anti",
        )

        repeated_versions_df = current_versions_df.join(
            history_keys_df,
            on=VERSION_KEYS,
            how="left_semi",
        )

        current_count = current_versions_df.count()
        repeated_count = repeated_versions_df.count()
        new_count = new_versions_df.count()

        print(f"Current batch: {current_dir.name}")
        print(f"Current versions: {current_count}")
        print(f"Repeated versions: {repeated_count}")
        print(f"Newly observed versions: {new_count}")

        print("\nRepeated versions:")
        repeated_versions_df.orderBy("shipment_id").show(
            10, truncate=False
        )

        print("\nNewly observed versions:")
        new_versions_df.orderBy("shipment_id").show(
            10, truncate=False
        )

        if current_count != repeated_count + new_count:
            raise RuntimeError("Version classification does not reconcile.")

        print("Version classification: PASS")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()