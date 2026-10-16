import hashlib
import os
import time

from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.responses import JSONResponse

app = FastAPI(title="Project Shipment Source")

DELIVERY_ID = "shipments-2026-10-01-v1"
PAGE_SIZE = 100
RATE_LIMIT = 60

# A fixed delivery keeps pagination stable across requests and restarts.
SHIPMENTS = [
    {
        "shipment_id": f"SHP-{number:06d}",
        "order_id": f"ORD-{number:06d}",
        "shipment_status": "delivered" if number % 3 else "in_transit",
        "updated_at": "2026-10-01T12:00:00Z",
        "source_version": 1,
    }
    for number in range(1, 1001)
]

request_times = []
failed_once = set()


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/shipments")
def shipments(
    cursor: str | None = None,
    fail_once_page: int | None = Query(default=None, ge=1, le=10),
    x_api_key: str | None = Header(default=None),
):
    expected_key = os.environ.get("SHIPMENT_API_KEY")
    if not expected_key:
        raise HTTPException(503, "Source API key is not configured")
    if x_api_key != expected_key:
        raise HTTPException(401, "Invalid API key")

    now = time.monotonic()
    request_times[:] = [
        request_time
        for request_time in request_times
        if now - request_time < 60
    ]
    if len(request_times) >= RATE_LIMIT:
        return JSONResponse(
            status_code=429,
            content={"detail": "Request limit exceeded"},
            headers={"Retry-After": "60"},
        )
    request_times.append(now)

    offset = 0
    if cursor:
        try:
            delivery, offset_text = cursor.split(":")
            offset = int(offset_text)
            if (
                delivery != DELIVERY_ID
                or offset < 0
                or offset >= len(SHIPMENTS)
                or offset % PAGE_SIZE != 0
            ):
                raise ValueError
        except ValueError:
            raise HTTPException(400, "Invalid cursor")

    page_number = offset // PAGE_SIZE + 1
    failure_id = (DELIVERY_ID, page_number)

    if fail_once_page == page_number and failure_id not in failed_once:
        failed_once.add(failure_id)
        return JSONResponse(
            status_code=503,
            content={"detail": "Temporary source failure; retry this page"},
            headers={"Retry-After": "2"},
        )

    records = SHIPMENTS[offset : offset + PAGE_SIZE]
    next_offset = offset + len(records)
    next_cursor = (
        f"{DELIVERY_ID}:{next_offset}"
        if next_offset < len(SHIPMENTS)
        else None
    )

    return {
        "delivery_id": DELIVERY_ID,
        "page_number": page_number,
        "page_id": hashlib.sha256(
            f"{DELIVERY_ID}:{offset}".encode()
        ).hexdigest(),
        "total_records": len(SHIPMENTS),
        "record_count": len(records),
        "next_cursor": next_cursor,
        "records": records,
    }

