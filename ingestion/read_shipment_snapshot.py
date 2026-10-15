import getpass
from pathlib import Path

from pyspark.sql import SparkSession


PROJECT_ROOT = Path(__file__).resolve().parents[1]
JDBC_JAR = PROJECT_ROOT / "drivers" / "postgresql.jar"

JDBC_URL = "jdbc:postgresql://localhost:5433/lakehouse_source"
SOURCE_TABLE = "fulfillment.shipments"

OUTPUT_DIR = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "postgres"
    / "shipments"
    / "snapshot_baseline"
)


def main():
    if not JDBC_JAR.is_file():
        raise FileNotFoundError(
            f"JDBC driver not found: {JDBC_JAR}"
        )

    if OUTPUT_DIR.exists():
        raise FileExistsError(
            f"Baseline folder already exists: {OUTPUT_DIR}. "
            "It has not been overwritten."
        )

    user = input(
        "PostgreSQL username [postgres]: "
    ).strip() or "postgres"

    password = getpass.getpass("PostgreSQL password: ")

    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("Shipment Database Snapshot")
        .config("spark.jars", JDBC_JAR.as_uri())
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    try:
        shipment_snapshot_df = spark.read.jdbc(
            url=JDBC_URL,
            table=SOURCE_TABLE,
            properties={
                "user": user,
                "password": password,
                "driver": "org.postgresql.Driver",
                "fetchsize": "1000",
                "connectTimeout": "10",
                "queryTimeout": "60",
            },
        )

        OUTPUT_DIR.parent.mkdir(parents=True, exist_ok=True)

        # Use a forward-slash Windows path without URI encoding.
        output_path = OUTPUT_DIR.as_posix()
        print(f"Writing snapshot to: {output_path}")

        shipment_snapshot_df.write.mode(
            "errorifexists"
        ).parquet(output_path)

        # Verify files exist at the intended Windows location.
        parquet_files = list(
            OUTPUT_DIR.glob("part-*.parquet")
        )

        if not parquet_files:
            raise RuntimeError(
                "No Parquet part files found at the intended "
                f"path: {OUTPUT_DIR}"
            )

        print(
            f"Local Parquet files found: {len(parquet_files)}"
        )

        # Read the persisted files, not PostgreSQL again.
        saved_snapshot_df = spark.read.parquet(output_path)
        saved_snapshot_df.printSchema()

        row_count = saved_snapshot_df.count()
        print(f"Saved snapshot rows: {row_count}")

        if row_count != 1000:
            raise RuntimeError(
                f"Expected 1,000 saved rows; found {row_count}. "
                "The output remains available for inspection."
            )

        print(f"Snapshot folder: {OUTPUT_DIR}")
        print("Persisted snapshot: PASS")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()