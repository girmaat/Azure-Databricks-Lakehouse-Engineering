import getpass
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

from pyspark.sql import SparkSession, functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[1]
JDBC_JAR = PROJECT_ROOT / "drivers" / "postgresql.jar"

JDBC_URL = "jdbc:postgresql://localhost:5433/lakehouse_source"
SOURCE_TABLE = "fulfillment.shipments"
OVERLAP_MINUTES = 5

WATERMARK_FILE = (
    PROJECT_ROOT
    / "data"
    / "checkpoints"
    / "postgres"
    / "shipments_watermark.json"
)

INCREMENTAL_ROOT = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "postgres"
    / "shipments"
    / "incremental"
)


def parse_timestamp(value):
    timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))

    if timestamp.tzinfo is None:
        raise ValueError("Watermark must include a timezone.")

    return timestamp.astimezone(timezone.utc)


def save_checkpoint(progress):
    temporary_file = WATERMARK_FILE.with_suffix(".json.tmp")

    with temporary_file.open("w", encoding="utf-8") as file:
        json.dump(progress, file, indent=2)
        file.write("\n")
        file.flush()
        os.fsync(file.fileno())

    os.replace(temporary_file, WATERMARK_FILE)


def main():
    if not JDBC_JAR.is_file():
        raise FileNotFoundError(f"JDBC driver not found: {JDBC_JAR}")

    progress = json.loads(
        WATERMARK_FILE.read_text(encoding="utf-8")
    )

    if (
        progress["source_table"] != SOURCE_TABLE
        or progress["watermark_column"] != "updated_at"
    ):
        raise RuntimeError("Checkpoint does not match this source.")

    previous_watermark = progress["last_successful_watermark"]
    lower_timestamp = parse_timestamp(previous_watermark)
    overlap_start = lower_timestamp - timedelta(
        minutes=OVERLAP_MINUTES
    )

    user = input("PostgreSQL username [postgres]: ").strip() or "postgres"
    password = getpass.getpass("PostgreSQL password: ")

    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("Shipment Watermark Extraction")
        .config("spark.jars", JDBC_JAR.as_uri())
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    properties = {
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver",
        "fetchsize": "1000",
        "connectTimeout": "10",
        "queryTimeout": "60",
    }

    try:
        bounds_df = spark.read.jdbc(
            url=JDBC_URL,
            table=(
                "(SELECT MAX(updated_at) AS upper_watermark "
                "FROM fulfillment.shipments) AS source_bounds"
            ),
            properties=properties,
        )

        upper_value = bounds_df.select(
            F.date_format(
                "upper_watermark",
                "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'",
            ).alias("upper_watermark")
        ).first()["upper_watermark"]

        print(f"Saved watermark: {previous_watermark}")
        print(f"Overlap starts at: {overlap_start.isoformat()}")

        if upper_value is None:
            print("Source is empty; watermark retained.")
            return

        upper_timestamp = parse_timestamp(upper_value)
        print(f"Upper watermark: {upper_value}")

        if upper_timestamp < lower_timestamp:
            raise RuntimeError(
                "Source maximum is behind the saved watermark. "
                "Checkpoint was not changed."
            )

        # Do not stop when the upper and saved watermarks are equal.
        # Boundary changes may exist inside the overlap window.
        lower_sql = overlap_start.isoformat()
        upper_sql = upper_timestamp.isoformat()

        source_query = f"""
            (
                SELECT
                    shipment_id,
                    order_id,
                    shipment_status,
                    updated_at,
                    source_version
                FROM fulfillment.shipments
                WHERE updated_at >= TIMESTAMPTZ '{lower_sql}'
                  AND updated_at <= TIMESTAMPTZ '{upper_sql}'
            ) AS shipment_changes
        """

        changes_df = spark.read.jdbc(
            url=JDBC_URL,
            table=source_query,
            properties=properties,
        )

        batch_id = uuid4().hex
        output_dir = INCREMENTAL_ROOT / f"batch_{batch_id}"
        output_dir.parent.mkdir(parents=True, exist_ok=True)

        changes_df.write.mode("errorifexists").parquet(
            output_dir.as_posix()
        )

        if not list(output_dir.glob("part-*.parquet")):
            raise RuntimeError(
                f"No local Parquet part files found: {output_dir}"
            )

        saved_df = spark.read.parquet(output_dir.as_posix())
        saved_count = saved_df.count()

        if saved_count == 0:
            print(f"Empty overlap batch saved: {output_dir}")
            print("Watermark retained; no progress recorded.")
            return

        saved_df.orderBy("shipment_id").show(10, truncate=False)

        progress["last_successful_watermark"] = upper_value
        progress["last_batch_id"] = batch_id
        progress["last_batch_path"] = str(output_dir)
        progress["last_batch_rows"] = saved_count
        progress["overlap_minutes"] = OVERLAP_MINUTES

        save_checkpoint(progress)

        print(f"Overlap rows saved: {saved_count}")
        print(f"Batch folder: {output_dir}")

        if upper_timestamp == lower_timestamp:
            print(f"Watermark retained at: {upper_value}")
        else:
            print(f"Watermark advanced to: {upper_value}")

        print("Overlap extraction: PASS")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()