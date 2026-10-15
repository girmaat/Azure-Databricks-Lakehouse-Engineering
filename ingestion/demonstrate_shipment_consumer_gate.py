import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4


PROJECT_ROOT = Path(__file__).resolve().parents[1]
VALIDATOR = PROJECT_ROOT / "ingestion/validate_shipment_contract.py"
CONTRACT = PROJECT_ROOT / "contracts/postgres/shipments_snapshot_v1.json"
BASELINE = PROJECT_ROOT / "data/raw/postgres/shipments/snapshot_baseline"


def project_path(value):
    """Resolve relative paths from the project root."""
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path.resolve()


def write_json(path, value):
    """Create a new evidence file without overwriting earlier evidence."""
    with path.open("x", encoding="utf-8") as handle:
        json.dump(value, handle, indent=2)


def consumer_action(receipt_path, input_path, report):
    """Demonstration consumer: record that the downstream action ran."""
    receipt = {
        "action": "demonstration_consumer_executed",
        "executed_at_utc": datetime.now(timezone.utc).isoformat(),
        "input_path": str(input_path),
        "contract_id": report["contract_id"],
        "contract_version": report["contract_version"],
        "validation_run_id": report["run_id"],
        "validated_rows": report["row_count"],
    }
    write_json(receipt_path, receipt)


def run_gate(input_path, run_dir):
    """Validate input and execute the consumer only after confirmed PASS."""
    run_dir.mkdir(parents=True, exist_ok=False)

    report_dir = run_dir / "validation"
    report_dir.mkdir()
    receipt_path = run_dir / "consumer_receipt.json"

    command = [
        sys.executable,
        str(VALIDATOR),
        "--contract", str(CONTRACT),
        "--input", str(input_path),
        "--report-dir", str(report_dir),
    ]

    # Keep validator output available for investigation without printing
    # all Spark startup messages during the demonstration.
    with (run_dir / "validator_console.log").open(
        "w", encoding="utf-8"
    ) as log:
        completed = subprocess.run(
            command,
            cwd=str(PROJECT_ROOT),
            stdout=log,
            stderr=subprocess.STDOUT,
            check=False,
        )

    outcome = {
        "input_path": str(input_path),
        "validator_exit_code": completed.returncode,
        "validation_status": "UNAVAILABLE",
        "gate_status": "BLOCKED",
        "consumer_executed": False,
    }

    # A nonzero exit always blocks the consumer. Missing, malformed,
    # or inconsistent reports also block it.
    report = None
    try:
        reports = list(report_dir.glob("validation_*.json"))
        if len(reports) != 1:
            raise ValueError("Expected exactly one validation report.")

        with reports[0].open("r", encoding="utf-8") as handle:
            report = json.load(handle)

        outcome["validation_status"] = report["status"]
        outcome["validation_report"] = str(reports[0])

        if project_path(report["input_path"]) != input_path:
            raise ValueError("Report input does not match gate input.")

        with CONTRACT.open("r", encoding="utf-8-sig") as handle:
            contract = json.load(handle)

        if (
            report["contract_id"] != contract["contract_id"]
            or report["contract_version"] != contract["contract_version"]
        ):
            raise ValueError("Report contract identity does not match.")

        # Require agreement between process status and report status.
        if completed.returncode == 0 and report["status"] == "PASS":
            if any(
                rule["status"] == "FAIL"
                for rule in report["rules"]
            ):
                raise ValueError("PASS report contains a failed rule.")

            consumer_action(receipt_path, input_path, report)
            outcome["gate_status"] = "ALLOWED"
            outcome["consumer_executed"] = True

    except Exception as error:
        outcome["gate_error"] = str(error)

    outcome["receipt_exists"] = receipt_path.is_file()
    write_json(run_dir / "gate_result.json", outcome)

    return outcome


def main():
    parser = argparse.ArgumentParser(
        description="Demonstrate allowed and blocked consumer execution."
    )
    parser.add_argument(
        "--invalid-input",
        required=True,
        help="Path to the previously confirmed invalid-status delivery.",
    )
    args = parser.parse_args()

    invalid_input = project_path(args.invalid_input)

    for path in (VALIDATOR, CONTRACT):
        if not path.is_file():
            raise FileNotFoundError(f"Missing file: {path}")

    for path in (BASELINE, invalid_input):
        if not path.is_dir():
            raise FileNotFoundError(f"Missing input folder: {path}")

    if invalid_input == BASELINE.resolve():
        raise ValueError("Valid and invalid inputs must be different.")

    # Separate execution directories prevent stale receipts or reports
    # from making a blocked run appear successful.
    suite_dir = (
        PROJECT_ROOT
        / "data/contract_tests/postgres/shipments"
        / f"gate_{uuid4().hex[:8]}"
    )
    suite_dir.mkdir(parents=True, exist_ok=False)

    print("Running gate against the valid baseline...", flush=True)
    valid = run_gate(BASELINE.resolve(), suite_dir / "valid")

    print("Running gate against invalid input...", flush=True)
    invalid = run_gate(invalid_input, suite_dir / "invalid")

    # Demonstration success requires both the positive and negative path.
    valid_ok = (
        valid["validator_exit_code"] == 0
        and valid["validation_status"] == "PASS"
        and valid["gate_status"] == "ALLOWED"
        and valid["consumer_executed"]
        and valid["receipt_exists"]
    )
    invalid_ok = (
        invalid["validator_exit_code"] == 1
        and invalid["validation_status"] == "FAIL"
        and invalid["gate_status"] == "BLOCKED"
        and not invalid["consumer_executed"]
        and not invalid["receipt_exists"]
        and "gate_error" not in invalid
    )

    summary = {
        "valid_input": valid,
        "invalid_input": invalid,
        "demonstration_passed": valid_ok and invalid_ok,
    }
    write_json(suite_dir / "gate_summary.json", summary)

    for label, result in (("VALID", valid), ("INVALID", invalid)):
        print(
            f"{label}: validation={result['validation_status']} | "
            f"gate={result['gate_status']} | "
            f"receipt_exists={result['receipt_exists']}"
        )
        if "gate_error" in result:
            print(f"Gate error: {result['gate_error']}")

    passed = summary["demonstration_passed"]
    print(f"\nConsumer gate demonstration: {'PASS' if passed else 'FAIL'}")
    print(f"Evidence folder: {suite_dir}")

    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())