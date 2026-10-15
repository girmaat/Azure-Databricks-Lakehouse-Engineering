import getpass
import json
from pathlib import Path

import psycopg
from psycopg import sql


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DELIVERY_ID = "shipments-2026-10-01-v1"
DATABASE = "lakehouse_source"

SOURCE_DIR = (
    PROJECT_ROOT / "data" / "raw" / "shipments"
    / f"{DELIVERY_ID}-checkpoint-success"
)


def main():
    # Load the completed existing delivery before changing the database.
    checkpoint = json.loads(
        (SOURCE_DIR / "checkpoint.json").read_text(encoding="utf-8")
    )
    if checkpoint["all_pages_received"] is not True:
        raise RuntimeError("The selected local delivery is incomplete.")

    records = []
    for path in sorted(SOURCE_DIR.glob("page_*.json")):
        page = json.loads(path.read_text(encoding="utf-8"))
        if page["delivery_id"] != DELIVERY_ID:
            raise RuntimeError(f"Unexpected delivery in {path.name}")
        records.extend(page["records"])

    if (
        len(records) != 1000
        or len({record["shipment_id"] for record in records}) != 1000
    ):
        raise RuntimeError("Expected 1,000 unique local shipments.")

    user = input("PostgreSQL username [postgres]: ").strip() or "postgres"
    password = getpass.getpass("PostgreSQL password: ")

    connection = {
        "host": "localhost",
        "port": 5433,
        "user": user,
        "password": password,
        "connect_timeout": 10,
    }

    # CREATE DATABASE must run outside a transaction.
    with psycopg.connect(
        dbname="postgres", autocommit=True, **connection
    ) as conn:
        version = conn.execute("SELECT version()").fetchone()[0]
        print(f"Connection: PASS\n{version}")

        exists = conn.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s",
            (DATABASE,),
        ).fetchone()

        if not exists:
            conn.execute(
                sql.SQL("CREATE DATABASE {}").format(
                    sql.Identifier(DATABASE)
                )
            )
            print(f"Created database: {DATABASE}")
        else:
            print(f"Using existing database: {DATABASE}")

    with psycopg.connect(dbname=DATABASE, **connection) as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS fulfillment")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS fulfillment.shipments (
                shipment_id TEXT PRIMARY KEY,
                order_id TEXT NOT NULL,
                shipment_status TEXT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL,
                source_version BIGINT NOT NULL
            )
        """)

        with conn.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO fulfillment.shipments (
                    shipment_id, order_id, shipment_status,
                    updated_at, source_version
                )
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (shipment_id) DO NOTHING
                """,
                [
                    (
                        record["shipment_id"],
                        record["order_id"],
                        record["shipment_status"],
                        record["updated_at"],
                        record["source_version"],
                    )
                    for record in records
                ],
            )

        count = conn.execute(
            "SELECT COUNT(*) FROM fulfillment.shipments"
        ).fetchone()[0]

        if count != 1000:
            raise RuntimeError(
                f"Expected 1,000 source rows; found {count}. "
                "Existing data was not deleted."
            )

    print("\nPostgreSQL source ready")
    print("Host: localhost | Port: 5433")
    print(f"Database: {DATABASE}")
    print("Table: fulfillment.shipments")
    print(f"Shipment rows: {count}")
    print("Setup: PASS")


if __name__ == "__main__":
    main()