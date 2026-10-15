"""Restore ONLY the complete ten-page API delivery. Never overwrite a conflict."""
import hashlib
import json
import subprocess
from pathlib import Path
from azure.core.exceptions import ResourceExistsError
from azure.identity import AzureCliCredential
from azure.storage.blob import BlobServiceClient, ContentSettings

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "data/raw/shipments/shipments-2026-10-01-v1-checkpoint-success"
DELIVERY = "shipments-2026-10-01-v1"

def main():
    pages = sorted(SOURCE.glob("page_*.json"))
    if [p.name for p in pages] != [f"page_{i:03d}.json" for i in range(1, 11)]:
        raise RuntimeError("Restore requires the complete ten-page copy.")
    payloads = [(p.name, p.read_bytes()) for p in pages]
    records = []
    for name, payload in payloads:
        page = json.loads(payload)
        if page["delivery_id"] != DELIVERY:
            raise RuntimeError(f"Wrong delivery: {name}")
        records.extend(page["records"])
    if len(records) != 1000 or len({r["shipment_id"] for r in records}) != 1000:
        raise RuntimeError("Completed delivery does not reconcile.")
    result = subprocess.run(
        ["terraform", f"-chdir={Path(__file__).parent / 'azure'}", "output", "-json"],
        check=True, capture_output=True, text=True)
    account_name = json.loads(result.stdout)["storage_account_name"]["value"]
    prefix = f"fulfillment/shipments/delivery_id={DELIVERY}"
    with AzureCliCredential() as credential:
        with BlobServiceClient(f"https://{account_name}.blob.core.windows.net", credential=credential) as service:
            for name, payload in payloads:
                blob = service.get_blob_client("landing", f"{prefix}/{name}")
                try:
                    blob.upload_blob(payload, overwrite=False,
                                     content_settings=ContentSettings(content_type="application/json"))
                    print(f"UPLOADED {name}")
                except ResourceExistsError:
                    remote = blob.download_blob().readall()
                    if remote != payload:
                        raise RuntimeError(f"Conflicting existing blob: {name}. Not overwritten.")
                    print(f"IDENTICAL {name}")
                if hashlib.sha256(blob.download_blob().readall()).digest() != hashlib.sha256(payload).digest():
                    raise RuntimeError(f"Read-back mismatch: {name}")
    print("Restore PASS: 10 files verified; 1000 distinct local shipments.")

if __name__ == "__main__":
    main()
