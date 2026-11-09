
# -*- coding: utf-8 -*-
"""
Pattern 256 Classification Mapping Audit
Clean V001

Read-only diagnostic.
Never modifies source CSV or Python code.
"""

import argparse
import csv
import hashlib
import json
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path


VERSION = "Clean V001"
AUDIT = "pattern_256_classification_mapping"

INPUT_REL = (
    "data/reports/"
    "pattern_256_event_key_reconciliation/"
    "event_key_reconciliation_detail.csv"
)

OUTPUT_REL = (
    "data/reports/"
    "pattern_256_classification_mapping"
)

SOURCE_CODE_CANDIDATES = [
    "audit_pattern_256_population_inventory.py",
    "pattern_256_population_inventory.py",
    "audit_population_inventory.py",
]

CODE_TERMS = re.compile(
    r"classification|event_key|key_fields|"
    r"signal_timestamp|horizon|DictReader|"
    r"read_csv|row\.get|record\.get",
    re.IGNORECASE,
)


def sha256(path):
    h = hashlib.sha256()

    with path.open("rb") as f:
        for chunk in iter(
            lambda: f.read(1024 * 1024),
            b"",
        ):
            h.update(chunk)

    return h.hexdigest()


def read_csv(path):
    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as f:
        return list(csv.DictReader(f))


def write_csv(path, columns, rows):
    with path.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as f:
        writer = csv.DictWriter(
            f,
            fieldnames=columns,
        )
        writer.writeheader()
        writer.writerows(rows)


def write_json(path, obj):
    with path.open(
        "w",
        encoding="utf-8",
    ) as f:
        json.dump(
            obj,
            f,
            ensure_ascii=False,
            indent=2,
        )


def normalize(value):
    return str(value or "").strip()


def inspect_source_code(root):
    matches = []
    inspected = []

    candidates = []

    for name in SOURCE_CODE_CANDIDATES:
        candidates.extend(root.rglob(name))

    unique = sorted(set(
        p.resolve()
        for p in candidates
        if p.is_file()
        and ".git" not in p.parts
    ))

    for path in unique:
        relative = path.relative_to(root).as_posix()

        inspected.append({
            "path": relative,
            "sha256": sha256(path),
        })

        lines = path.read_text(
            encoding="utf-8-sig",
            errors="replace",
        ).splitlines()

        for index, line in enumerate(lines):
            if not CODE_TERMS.search(line):
                continue

            start = max(0, index - 2)
            end = min(len(lines), index + 3)

            context = "\n".join(
                f"{i + 1}: {lines[i]}"
                for i in range(start, end)
            )

            matches.append({
                "file": relative,
                "line": index + 1,
                "code": line.strip(),
                "context": context,
            })

    return inspected, matches


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--root",
        default=".",
    )

    parser.add_argument(
        "--code-root",
        default=None,
    )

    args = parser.parse_args()

    root = Path(args.root).resolve()

    code_root = (
        Path(args.code_root).resolve()
        if args.code_root
        else root
    )

    input_path = root / INPUT_REL
    output_dir = root / OUTPUT_REL

    if not input_path.is_file():
        raise FileNotFoundError(
            f"Reconciliation input missing: {input_path}"
        )

    records = read_csv(input_path)

    if not records:
        raise ValueError(
            "Reconciliation detail is empty"
        )

    required = {
        "source_file",
        "row_number",
        "timeframe",
        "market",
        "signal_timestamp",
        "inventory_classification",
        "source_classification",
        "horizon",
        "reconciliation",
    }

    if not required.issubset(records[0]):
        raise ValueError(
            "Unexpected reconciliation schema"
        )

    grouped = defaultdict(list)

    for record in records:
        grouped[record["source_file"]].append(
            record
        )

    detail = []
    source_files = []
    counts = Counter()
    errors = []

    for relative, events in sorted(grouped.items()):
        path = (root / relative).resolve()

        try:
            path.relative_to(root)
        except ValueError:
            errors.append(
                f"Unsafe source path: {relative}"
            )
            continue

        if not path.is_file():
            errors.append(
                f"Missing source: {relative}"
            )
            continue

        file_hash = sha256(path)

        with path.open(
            "r",
            encoding="utf-8-sig",
            newline="",
        ) as f:
            reader = csv.DictReader(f)
            columns = reader.fieldnames or []

            needed_rows = {
                int(event["row_number"])
                for event in events
            }

            selected = {}

            for line_number, row in enumerate(
                reader,
                start=2,
            ):
                if line_number in needed_rows:
                    selected[line_number] = row

        source_files.append({
            "path": relative,
            "sha256": file_hash,
            "classification_column_present": (
                "classification" in columns
            ),
            "columns": columns,
            "checked_events": len(events),
        })

        for event in events:
            line_number = int(
                event["row_number"]
            )

            row = selected.get(line_number)

            if row is None:
                status = "SOURCE_ROW_MISSING"
                actual = ""
            else:
                actual = normalize(
                    row.get("classification")
                )

                expected = normalize(
                    event["source_classification"]
                )

                if "classification" not in columns:
                    status = (
                        "CLASSIFICATION_COLUMN_MISSING"
                    )
                elif actual != expected:
                    status = (
                        "SOURCE_CLASSIFICATION_CHANGED"
                    )
                elif normalize(
                    event["inventory_classification"]
                ) == "":
                    status = (
                        "INVENTORY_EMPTY_SOURCE_PRESENT"
                    )
                else:
                    status = "OTHER"

            counts[status] += 1

            detail.append({
                "source_file": relative,
                "row_number": line_number,
                "market": event["market"],
                "timeframe": event["timeframe"],
                "signal_timestamp": (
                    event["signal_timestamp"]
                ),
                "horizon": event["horizon"],
                "inventory_classification": (
                    event["inventory_classification"]
                ),
                "expected_source_classification": (
                    event["source_classification"]
                ),
                "actual_source_classification": actual,
                "status": status,
                "source_sha256": file_hash,
            })

    code_files, code_matches = (
        inspect_source_code(code_root)
    )

    if not code_files:
        errors.append(
            "Inventory Python source not located"
        )

    if len(detail) != len(records):
        errors.append(
            "Not all reconciliation events inspected"
        )

    expected_status = (
        "INVENTORY_EMPTY_SOURCE_PRESENT"
    )

    verified = (
        len(records) == len(detail)
        and counts[expected_status] == len(records)
        and not errors
    )

    execution = (
        "PASS" if verified else "REVIEW"
    )

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    detail_path = (
        output_dir
        / "classification_mapping_detail.csv"
    )

    result_path = (
        output_dir
        / "classification_mapping_result.json"
    )

    code_path = (
        output_dir
        / "classification_mapping_code_locations.json"
    )

    manifest_path = (
        output_dir
        / "classification_mapping_manifest.json"
    )

    columns = [
        "source_file",
        "row_number",
        "market",
        "timeframe",
        "signal_timestamp",
        "horizon",
        "inventory_classification",
        "expected_source_classification",
        "actual_source_classification",
        "status",
        "source_sha256",
    ]

    write_csv(
        detail_path,
        columns,
        detail,
    )

    code_report = {
        "audit": AUDIT,
        "version": VERSION,
        "inspected_python_files": code_files,
        "matching_code_locations": code_matches,
        "note": (
            "Code matches are candidates only. "
            "They do not independently prove "
            "the defect location."
        ),
    }

    write_json(
        code_path,
        code_report,
    )

    result = {
        "audit": AUDIT,
        "version": VERSION,
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "execution": execution,
        "research_decision": "HOLD",
        "input_events": len(records),
        "checked_events": len(detail),
        "source_files": len(source_files),
        "python_files_inspected": len(code_files),
        "code_locations_found": len(code_matches),
        "classification_mapping_consistent": verified,
        "status_counts": dict(counts),
        "errors": errors,
        "input_sha256": sha256(input_path),
        "source_file_inventory": source_files,
        "limitations": [
            "Does not locate a defect conclusively",
            "Does not modify source data",
            "Does not repair inventory code",
            "Does not independently prove OOS",
            "Does not prove trading profitability",
        ],
        "train_approved": False,
        "live_trading_approved": False,
    }

    write_json(
        result_path,
        result,
    )

    manifest = {
        "audit": AUDIT,
        "version": VERSION,
        "execution": execution,
        "research_decision": "HOLD",
        "output_sha256": {
            path.name: sha256(path)
            for path in [
                detail_path,
                result_path,
                code_path,
            ]
        },
    }

    write_json(
        manifest_path,
        manifest,
    )

    print("\n=== CLASSIFICATION MAPPING AUDIT ===")
    print("Execution:", execution)
    print("Input events:", len(records))
    print("Checked events:", len(detail))
    print("Source files:", len(source_files))
    print("Python files:", len(code_files))
    print("Code locations:", len(code_matches))
    print("Status counts:", dict(counts))
    print("Errors:", errors)
    print("Research decision: HOLD")

    return 0 if verified else 1


if __name__ == "__main__":
    sys.exit(main())
