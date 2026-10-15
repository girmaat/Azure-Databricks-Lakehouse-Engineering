import json
import os
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import time
from urllib.error import HTTPError, URLError

API_URL = "http://127.0.0.1:5055/shipments"
DELIVERY_ID = "shipments-2026-10-01-v1"

PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = PROJECT_ROOT / "data" / "raw" / "shipments" / DELIVERY_ID

def fetch_page(request, max_attempts=4):
    retryable_statuses = {429, 500, 502, 503, 504}

    for attempt in range(1, max_attempts + 1):
        try:
            with urlopen(request, timeout=30) as response:
                return json.loads(response.read().decode("utf-8"))

        except HTTPError as error:
            status = error.code
            retry_after = error.headers.get("Retry-After")
            error.close()

            if status not in retryable_statuses:
                raise

            if attempt == max_attempts:
                raise

            delay = min(2 ** attempt, 30)

            # Our practice API supplies Retry-After as seconds.
            if retry_after:
                try:
                    delay = max(delay, int(retry_after))
                except ValueError:
                    pass

            print(
                f"HTTP {status}; retrying the same page in "
                f"{delay} seconds "
                f"(attempt {attempt + 1}/{max_attempts})"
            )
            time.sleep(delay)

        except (URLError, TimeoutError):
            if attempt == max_attempts:
                raise

            delay = min(2 ** attempt, 30)
            print(
                f"Connection failure; retrying the same page in "
                f"{delay} seconds "
                f"(attempt {attempt + 1}/{max_attempts})"
            )
            time.sleep(delay)

def write_json_atomic(path, content):
    temporary_path = path.with_suffix(path.suffix + ".tmp")

    with temporary_path.open("w", encoding="utf-8") as file:
        json.dump(content, file, indent=2)
        file.write("\n")
        file.flush()
        os.fsync(file.fileno())

    os.replace(temporary_path, path)

def restore_progress():
    progress = {
        "cursor": None,
        "seen_cursors": set(),
        "seen_page_ids": set(),
        "shipment_ids": set(),
        "expected_total": None,
        "records_received": 0,
        "pages_saved": 0,
        "all_pages_received": False,
    }

    files = sorted(OUTPUT_DIR.glob("page_*.json"))

    for number, path in enumerate(files, start=1):
        if path.name != f"page_{number:03d}.json":
            raise RuntimeError("Saved pages have a gap or unexpected filename.")

        if progress["all_pages_received"]:
            raise RuntimeError("A saved page follows the final page.")

        page = json.loads(path.read_text(encoding="utf-8"))

        if page["delivery_id"] != DELIVERY_ID:
            raise RuntimeError("Saved page belongs to another delivery.")
        if page["page_number"] != number:
            raise RuntimeError("Saved page number does not match its filename.")

        total = page["total_records"]
        if not isinstance(total, int) or total < 0:
            raise RuntimeError("Invalid saved source total.")

        if progress["expected_total"] is None:
            progress["expected_total"] = total
        elif total != progress["expected_total"]:
            raise RuntimeError("Saved pages disagree on source total.")

        page_id = page["page_id"]
        if page_id in progress["seen_page_ids"]:
            raise RuntimeError("Repeated saved page ID.")

        records = page["records"]
        if not isinstance(records, list):
            raise RuntimeError("Saved records must be a list.")
        if page["record_count"] != len(records):
            raise RuntimeError("Saved page count is incorrect.")

        for record in records:
            shipment_id = record["shipment_id"]
            if not isinstance(shipment_id, str) or not shipment_id.strip():
                raise RuntimeError("Invalid saved shipment ID.")
            if shipment_id in progress["shipment_ids"]:
                raise RuntimeError(f"Duplicate saved shipment: {shipment_id}")
            progress["shipment_ids"].add(shipment_id)

        next_cursor = page["next_cursor"]
        if next_cursor is not None:
            if not isinstance(next_cursor, str) or not next_cursor:
                raise RuntimeError("Invalid saved continuation token.")
            if next_cursor in progress["seen_cursors"]:
                raise RuntimeError("Repeated saved continuation token.")
            if not records:
                raise RuntimeError("Empty saved page has a continuation token.")
            progress["seen_cursors"].add(next_cursor)

        progress["seen_page_ids"].add(page_id)
        progress["records_received"] += len(records)
        progress["pages_saved"] = number
        progress["cursor"] = next_cursor
        progress["all_pages_received"] = next_cursor is None

        if progress["records_received"] > total:
            raise RuntimeError("Saved record count exceeds the source total.")

    checkpoint_path = OUTPUT_DIR / "checkpoint.json"

    if checkpoint_path.exists():
        checkpoint = json.loads(
            checkpoint_path.read_text(encoding="utf-8")
        )
        if checkpoint["delivery_id"] != DELIVERY_ID:
            raise RuntimeError("Checkpoint belongs to another delivery.")

        saved_page = checkpoint["last_saved_page"]
        if (
            not isinstance(saved_page, int)
            or saved_page < 1
            or saved_page > progress["pages_saved"]
        ):
            raise RuntimeError("Checkpoint references an unavailable page.")

        checkpoint_page = json.loads(
            (OUTPUT_DIR / f"page_{saved_page:03d}.json").read_text(
                encoding="utf-8"
            )
        )
        if (
            checkpoint["last_saved_page_id"] != checkpoint_page["page_id"]
            or checkpoint["next_cursor"] != checkpoint_page["next_cursor"]
        ):
            raise RuntimeError("Checkpoint disagrees with its saved page.")

    if files:
        last_page = json.loads(files[-1].read_text(encoding="utf-8"))
        write_json_atomic(
            checkpoint_path,
            {
                "delivery_id": DELIVERY_ID,
                "last_saved_page": progress["pages_saved"],
                "last_saved_page_id": last_page["page_id"],
                "next_cursor": progress["cursor"],
                "pages_saved": progress["pages_saved"],
                "records_received": progress["records_received"],
                "expected_total": progress["expected_total"],
                "all_pages_received": progress["all_pages_received"],
            },
        )

    return progress

def main():
    api_key = os.environ.get("SHIPMENT_API_KEY")
    if not api_key:
        raise RuntimeError("Set SHIPMENT_API_KEY before running.")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    progress = restore_progress()

    cursor = progress["cursor"]
    seen_cursors = progress["seen_cursors"]
    seen_page_ids = progress["seen_page_ids"]
    shipment_ids = progress["shipment_ids"]
    expected_total = progress["expected_total"]
    records_received = progress["records_received"]
    pages_saved = progress["pages_saved"]
    all_pages_received = progress["all_pages_received"]

    stop_after_page = int(os.environ.get("STOP_AFTER_PAGE", "0"))

    if pages_saved:
        print(
            f"Restored {pages_saved} pages and "
            f"{records_received} records"
        )

    while not all_pages_received:
        params = {"fail_once_page": 7}
        if cursor is not None:
            params["cursor"] = cursor
        url = API_URL
        if params:
            url += "?" + urlencode(params)

        request = Request(
            url,
            headers={
                "x-api-key": api_key,
                "Accept": "application/json",
            },
        )

        page = fetch_page(request)

        if page["delivery_id"] != DELIVERY_ID:
            raise RuntimeError("Unexpected delivery ID.")

        total = page["total_records"]
        if not isinstance(total, int) or total < 0:
            raise RuntimeError("Invalid source total.")

        if expected_total is None:
            expected_total = total
        elif total != expected_total:
            raise RuntimeError("Source total changed during extraction.")

        page_number = page["page_number"]
        if page_number != pages_saved + 1:
            raise RuntimeError("Unexpected page sequence.")

        page_id = page["page_id"]
        if page_id in seen_page_ids:
            raise RuntimeError("API returned a repeated page.")

        records = page["records"]
        if not isinstance(records, list):
            raise RuntimeError("Records must be a list.")
        if page["record_count"] != len(records):
            raise RuntimeError("Page record count does not match its records.")

        page_shipment_ids = set()
        for record in records:
            shipment_id = record["shipment_id"]
            if not isinstance(shipment_id, str) or not shipment_id.strip():
                raise RuntimeError("Missing or invalid shipment ID.")
            if (
                shipment_id in shipment_ids
                or shipment_id in page_shipment_ids
            ):
                raise RuntimeError(f"Duplicate shipment: {shipment_id}")
            page_shipment_ids.add(shipment_id)

        next_cursor = page["next_cursor"]
        if next_cursor is not None:
            if not isinstance(next_cursor, str) or not next_cursor:
                raise RuntimeError("Invalid continuation token.")
            if next_cursor == cursor or next_cursor in seen_cursors:
                raise RuntimeError("API returned a repeated continuation token.")
            if not records:
                raise RuntimeError("Empty page has a continuation token.")

        if records_received + len(records) > expected_total:
            raise RuntimeError("Received more records than the source total.")

        output_file = OUTPUT_DIR / f"page_{page_number:03d}.json"
        write_json_atomic(output_file, page)

        crash_after_page = int(
            os.environ.get("CRASH_AFTER_PAGE_WRITE", "0")
        )

        if page_number == crash_after_page:
            raise RuntimeError(
                f"Controlled failure: page {page_number} saved "
                "before checkpoint update."
            )

        pages_saved += 1
        records_received += len(records)
        shipment_ids.update(page_shipment_ids)
        seen_page_ids.add(page_id)
        checkpoint = {
            "delivery_id": DELIVERY_ID,
            "last_saved_page": page_number,
            "last_saved_page_id": page_id,
            "next_cursor": next_cursor,
            "pages_saved": pages_saved,
            "records_received": records_received,
            "expected_total": expected_total,
            "all_pages_received": next_cursor is None,
        }

        write_json_atomic(OUTPUT_DIR / "checkpoint.json", checkpoint)

        print(f"Saved page {page_number}: {len(records)} records")

        if stop_after_page == page_number and next_cursor is not None:
            print(f"Controlled stop after page {page_number}.")
            return

        if next_cursor is None:
            break

        if next_cursor is None:
            break

        seen_cursors.add(next_cursor)
        cursor = next_cursor

    passed = (
        records_received == expected_total
        and len(shipment_ids) == expected_total
    )

    print(f"\nPages saved: {pages_saved}")
    print(f"Records received: {records_received}")
    print(f"Distinct shipment IDs: {len(shipment_ids)}")
    print(f"Expected records: {expected_total}")
    print(f"Reconciliation: {'PASS' if passed else 'FAIL'}")
    print(f"Output folder: {OUTPUT_DIR}")

    if not passed:
        raise RuntimeError("Extraction is incomplete; reconciliation failed.")


if __name__ == "__main__":
    main()