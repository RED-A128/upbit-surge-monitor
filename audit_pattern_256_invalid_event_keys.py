
# -*- coding: utf-8 -*-
"""
Pattern 256 Invalid Event Key Root Cause Audit
Clean V001

Purpose:
- Investigate INVALID_EVENT_KEY rows from population inventory.
- Verify original checkpoint file SHA256.
- Inspect the exact source CSV rows.
- Classify missing event-key fields.
- Never modify source data.
- Produce SHA256-verified reports.
"""

import argparse
import csv
import hashlib
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path


VERSION = "Clean V001"
AUDIT = "pattern_256_invalid_event_keys"

SOURCE_DIR = (
    "data/reports/"
    "pattern_256_accumulation_30pct/checkpoints"
)

INVENTORY_DIR = (
    "data/reports/pattern_256_population_inventory"
)

OUTPUT_DIR = (
    "data/reports/pattern_256_invalid_event_keys"
)

DETAIL_FIELDS = [
    "source_file",
    "row_number",
    "timeframe",
    "market",
    "signal_timestamp",
    "classification",
    "horizon",
    "status",
    "hit_30pct",
    "source_issue",
    "classification_missing",
    "other_key_fields_missing",
    "row_found",
    "source_hash_verified",
    "diagnosis",
]


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def sha256(path):
    digest = hashlib.sha256()

    with open(path, "rb") as handle:
        for block in iter(
            lambda: handle.read(1024 * 1024),
            b"",
        ):
            digest.update(block)

    return digest.hexdigest()


def read_csv(path):
    with open(
        path,
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as handle:
        reader = csv.DictReader(handle)

        if not reader.fieldnames:
            raise ValueError(
                f"CSV header missing: {path}"
            )

        return list(reader)


def write_csv(path, rows, fields):
    with open(
        path,
        "w",
        encoding="utf-8",
        newline="",
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=fields,
            extrasaction="ignore",
        )

        writer.writeheader()
        writer.writerows(rows)


def write_json(path, data):
    with open(
        path,
        "w",
        encoding="utf-8",
    ) as handle:
        json.dump(
            data,
            handle,
            ensure_ascii=False,
            indent=2,
        )


def normalized(value):
    if value is None:
        return ""

    return str(value).strip()


def classify_row(row, source_hash_verified):
    required = [
        "timeframe",
        "market",
        "signal_timestamp",
        "classification",
        "horizon",
    ]

    missing = [
        field
        for field in required
        if not normalized(row.get(field))
    ]

    if not source_hash_verified:
        return "SOURCE_HASH_MISMATCH", missing

    if not missing:
        return "KEY_PRESENT_RECHECK_REQUIRED", missing

    if missing == ["classification"]:
        return "MISSING_CLASSIFICATION_ONLY", missing

    return "MULTIPLE_OR_OTHER_KEY_FIELDS_MISSING", missing


def resolve_source(root, relative):
    root = root.resolve()
    path = (root / relative).resolve()

    try:
        path.relative_to(root)
    except ValueError:
        raise ValueError(
            f"Source path escapes project root: {relative}"
        )

    return path


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--root",
        default=".",
    )

    args = parser.parse_args()

    root = Path(args.root).resolve()

    inventory = root / INVENTORY_DIR
    output = root / OUTPUT_DIR

    issues_path = (
        inventory / "population_inventory_issues.csv"
    )

    detail_path = (
        inventory / "population_inventory_detail.csv"
    )

    result_path = (
        inventory / "population_inventory_result.json"
    )

    required_files = [
        issues_path,
        detail_path,
        result_path,
    ]

    for path in required_files:
        if not path.is_file():
            raise FileNotFoundError(
                f"Required inventory report missing: {path}"
            )

    with open(
        result_path,
        "r",
        encoding="utf-8",
    ) as handle:
        inventory_result = json.load(handle)

    if inventory_result.get("audit") != (
        "pattern_256_population_inventory"
    ):
        raise ValueError("Unexpected inventory audit")

    issues = read_csv(issues_path)
    inventory_details = read_csv(detail_path)

    expected_count = inventory_result.get(
        "issues_total"
    )

    if len(issues) != expected_count:
        raise ValueError(
            "Inventory issue count mismatch"
        )

    source_hashes = {}

    for item in inventory_details:
        source = normalized(
            item.get("source_file")
        ).replace("\\", "/")

        expected_hash = normalized(
            item.get("source_sha256")
        ).lower()

        if source in source_hashes:
            raise ValueError(
                f"Duplicate source entry: {source}"
            )

        source_hashes[source] = expected_hash

    grouped = defaultdict(list)

    for issue in issues:
        if issue.get("issue_type") != (
            "INVALID_EVENT_KEY"
        ):
            raise ValueError(
                "Unexpected issue type; "
                "manual review required"
            )

        source = normalized(
            issue.get("source_file")
        ).replace("\\", "/")

        if source not in source_hashes:
            raise ValueError(
                f"Source not in inventory: {source}"
            )

        try:
            row_number = int(
                issue["row_number"]
            )
        except (ValueError, TypeError):
            raise ValueError(
                f"Invalid row number: {issue}"
            )

        if row_number < 2:
            raise ValueError(
                "CSV row number must be >= 2"
            )

        grouped[source].append(
            (row_number, issue)
        )

    output.mkdir(
        parents=True,
        exist_ok=True,
    )

    results = []
    diagnosis_counts = Counter()
    timeframe_counts = Counter()
    errors = []
    verified_files = 0

    for index, source in enumerate(
        sorted(grouped),
        start=1,
    ):
        source_path = resolve_source(
            root,
            source,
        )

        print(
            f"[{index}/{len(grouped)}] "
            f"{source}",
            flush=True,
        )

        if not source_path.is_file():
            errors.append(
                f"Missing source file: {source}"
            )
            continue

        actual_hash = sha256(
            source_path
        ).lower()

        expected_hash = source_hashes[source]

        hash_verified = (
            actual_hash == expected_hash
        )

        if not hash_verified:
            errors.append(
                f"Source SHA256 mismatch: {source}"
            )
            continue

        verified_files += 1

        target_rows = defaultdict(list)

        for row_number, issue in grouped[source]:
            target_rows[row_number].append(issue)

        found = set()

        with open(
            source_path,
            "r",
            encoding="utf-8-sig",
            newline="",
        ) as handle:
            reader = csv.DictReader(handle)

            if not reader.fieldnames:
                errors.append(
                    f"Missing header: {source}"
                )
                continue

            for row_number, row in enumerate(
                reader,
                start=2,
            ):
                if row_number not in target_rows:
                    continue

                found.add(row_number)

                diagnosis, missing = classify_row(
                    row,
                    hash_verified,
                )

                for issue in target_rows[row_number]:
                    record = {
                        "source_file": source,
                        "row_number": row_number,
                        "timeframe": normalized(
                            row.get("timeframe")
                        ),
                        "market": normalized(
                            row.get("market")
                        ),
                        "signal_timestamp": normalized(
                            row.get("signal_timestamp")
                        ),
                        "classification": normalized(
                            row.get("classification")
                        ),
                        "horizon": normalized(
                            row.get("horizon")
                        ),
                        "status": normalized(
                            row.get("status")
                        ),
                        "hit_30pct": normalized(
                            row.get("hit_30pct")
                        ),
                        "source_issue": normalized(
                            issue.get("issue_type")
                        ),
                        "classification_missing": (
                            "classification" in missing
                        ),
                        "other_key_fields_missing": (
                            ",".join(
                                field
                                for field in missing
                                if field != "classification"
                            )
                        ),
                        "row_found": True,
                        "source_hash_verified": True,
                        "diagnosis": diagnosis,
                    }

                    results.append(record)
                    diagnosis_counts[diagnosis] += 1

                    timeframe_counts[
                        record["timeframe"]
                    ] += 1

        for row_number in target_rows:
            if row_number not in found:
                errors.append(
                    f"Row not found: "
                    f"{source}:{row_number}"
                )

    results.sort(
        key=lambda item: (
            item["source_file"],
            item["row_number"],
        )
    )

    complete = (
        not errors
        and len(results) == len(issues)
        and verified_files == len(grouped)
    )

    if not complete:
        execution = "FAIL"
    elif diagnosis_counts.get(
        "MISSING_CLASSIFICATION_ONLY",
        0,
    ) == len(issues):
        execution = "REVIEW"
    else:
        execution = "REVIEW"

    summary = {
        "audit": AUDIT,
        "version": VERSION,
        "created_utc": utc_now(),
        "execution": execution,
        "research_decision": "HOLD",
        "source_inventory_execution": (
            inventory_result.get("execution")
        ),
        "source_inventory_issues": len(issues),
        "source_files_with_issues": len(grouped),
        "source_files_hash_verified": verified_files,
        "rows_inspected": len(results),
        "diagnosis_counts": dict(
            sorted(diagnosis_counts.items())
        ),
        "timeframe_counts": dict(
            sorted(timeframe_counts.items())
        ),
        "errors": errors,
        "source_inventory_sha256": {
            path.name: sha256(path)
            for path in required_files
        },
        "limitations": [
            "No source CSV is modified",
            "Event key convention is provisional",
            "Missing classification cause is not proven",
            "No independent OHLCV outcome replay",
            "No unbiased signal denominator proven",
            "No independent OOS proven",
            "No trading profitability proven",
        ],
        "train_approved": False,
        "live_trading_approved": False,
    }

    report_csv = (
        output / "invalid_event_key_diagnosis.csv"
    )

    report_json = (
        output / "invalid_event_key_result.json"
    )

    manifest_path = (
        output / "invalid_event_key_manifest.json"
    )

    write_csv(
        report_csv,
        results,
        DETAIL_FIELDS,
    )

    write_json(
        report_json,
        summary,
    )

    manifest = {
        "audit": AUDIT,
        "version": VERSION,
        "execution": execution,
        "research_decision": "HOLD",
        "output_sha256": {
            report_csv.name: sha256(report_csv),
            report_json.name: sha256(report_json),
        },
    }

    write_json(
        manifest_path,
        manifest,
    )

    print("")
    print("=== INVALID EVENT KEY AUDIT ===")
    print(f"Execution: {execution}")
    print(f"Issue rows: {len(issues)}")
    print(f"Inspected rows: {len(results)}")
    print(f"Verified files: {verified_files}")
    print(f"Errors: {len(errors)}")
    print(f"Diagnosis: {dict(diagnosis_counts)}")
    print("Research decision: HOLD")

    return 1 if execution == "FAIL" else 0


if __name__ == "__main__":
    sys.exit(main())
