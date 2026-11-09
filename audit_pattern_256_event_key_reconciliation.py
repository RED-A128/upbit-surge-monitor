
# -*- coding: utf-8 -*-
"""
Pattern 256 Event Key Reconciliation
Clean V001

Compare inventory issue keys with independently
re-read checkpoint keys.

No source modification.
No automatic correction.
No training or trading approval.
"""

import argparse
import ast
import csv
import hashlib
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path


VERSION = "Clean V001"
AUDIT = "pattern_256_event_key_reconciliation"

INVENTORY = (
    "data/reports/pattern_256_population_inventory"
)

DIAGNOSIS = (
    "data/reports/pattern_256_invalid_event_keys"
)

OUTPUT = (
    "data/reports/pattern_256_event_key_reconciliation"
)

FIELDS = [
    "source_file",
    "row_number",
    "timeframe",
    "market",
    "signal_timestamp",
    "inventory_classification",
    "source_classification",
    "horizon",
    "mismatched_fields",
    "reconciliation",
]

KEY_FIELDS = [
    "timeframe",
    "market",
    "signal_timestamp",
    "classification",
    "horizon",
]


def digest(path):
    h = hashlib.sha256()

    with path.open("rb") as f:
        for block in iter(
            lambda: f.read(1024 * 1024),
            b"",
        ):
            h.update(block)

    return h.hexdigest()


def read_csv(path):
    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as f:
        return list(csv.DictReader(f))


def read_json(path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path, obj):
    with path.open("w", encoding="utf-8") as f:
        json.dump(
            obj,
            f,
            ensure_ascii=False,
            indent=2,
        )


def write_csv(path, rows):
    with path.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as f:
        writer = csv.DictWriter(
            f,
            fieldnames=FIELDS,
        )
        writer.writeheader()
        writer.writerows(rows)


def source_key(row):
    return tuple(
        str(row.get(field, "")).strip()
        for field in KEY_FIELDS
    )


def inventory_key(issue):
    value = ast.literal_eval(issue["detail"])

    if not isinstance(value, (tuple, list)):
        raise ValueError(
            "Inventory issue detail is not a tuple"
        )

    if len(value) != len(KEY_FIELDS):
        raise ValueError(
            "Inventory issue key has unexpected length"
        )

    return tuple(str(v).strip() for v in value)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=".")
    args = parser.parse_args()

    root = Path(args.root).resolve()
    inventory_dir = root / INVENTORY
    diagnosis_dir = root / DIAGNOSIS
    output_dir = root / OUTPUT

    issue_file = (
        inventory_dir / "population_inventory_issues.csv"
    )
    inventory_result_file = (
        inventory_dir / "population_inventory_result.json"
    )
    diagnosis_file = (
        diagnosis_dir / "invalid_event_key_diagnosis.csv"
    )
    diagnosis_result_file = (
        diagnosis_dir / "invalid_event_key_result.json"
    )

    required = [
        issue_file,
        inventory_result_file,
        diagnosis_file,
        diagnosis_result_file,
    ]

    for path in required:
        if not path.is_file():
            raise FileNotFoundError(
                f"Required input missing: {path}"
            )

    issues = read_csv(issue_file)
    diagnosis = read_csv(diagnosis_file)
    inventory_result = read_json(inventory_result_file)
    diagnosis_result = read_json(diagnosis_result_file)

    if inventory_result.get("audit") != (
        "pattern_256_population_inventory"
    ):
        raise ValueError("Wrong inventory result")

    if diagnosis_result.get("audit") != (
        "pattern_256_invalid_event_keys"
    ):
        raise ValueError("Wrong diagnosis result")

    if len(issues) != inventory_result.get("issues_total"):
        raise ValueError("Inventory issue count mismatch")

    if len(diagnosis) != diagnosis_result.get(
        "rows_inspected"
    ):
        raise ValueError("Diagnosis row count mismatch")

    if diagnosis_result.get("errors"):
        raise ValueError(
            "Previous diagnosis contains errors"
        )

    lookup = {}

    for row in diagnosis:
        key = (
            row["source_file"].replace("\\", "/"),
            str(row["row_number"]),
        )

        if key in lookup:
            raise ValueError(
                f"Duplicate diagnosis row: {key}"
            )

        if row["row_found"] != "True":
            raise ValueError(
                f"Unverified source row: {key}"
            )

        if row["source_hash_verified"] != "True":
            raise ValueError(
                f"Unverified source hash: {key}"
            )

        lookup[key] = row

    output_rows = []
    counts = Counter()
    classification_pairs = Counter()
    errors = []
    seen = set()

    for issue in issues:
        if issue["issue_type"] != "INVALID_EVENT_KEY":
            errors.append(
                "Unexpected issue type: "
                + issue["issue_type"]
            )
            continue

        key = (
            issue["source_file"].replace("\\", "/"),
            str(issue["row_number"]),
        )

        if key in seen:
            errors.append(
                f"Duplicate inventory issue: {key}"
            )
            continue

        seen.add(key)

        row = lookup.get(key)

        if row is None:
            errors.append(
                f"Diagnosis row missing: {key}"
            )
            continue

        try:
            recorded = inventory_key(issue)
        except (ValueError, SyntaxError) as exc:
            errors.append(
                f"Invalid issue detail {key}: {exc}"
            )
            continue

        actual = source_key(row)

        differences = [
            field
            for field, old, new in zip(
                KEY_FIELDS,
                recorded,
                actual,
            )
            if old != new
        ]

        if not differences:
            category = "KEYS_IDENTICAL"
        elif differences == ["classification"]:
            if recorded[3] == "" and actual[3]:
                category = (
                    "INVENTORY_CLASSIFICATION_EMPTY"
                )
            else:
                category = (
                    "CLASSIFICATION_VALUE_DIFFERENT"
                )
        else:
            category = "OTHER_KEY_MISMATCH"

        counts[category] += 1

        classification_pairs[
            (recorded[3], actual[3])
        ] += 1

        output_rows.append({
            "source_file": key[0],
            "row_number": key[1],
            "timeframe": actual[0],
            "market": actual[1],
            "signal_timestamp": actual[2],
            "inventory_classification": recorded[3],
            "source_classification": actual[3],
            "horizon": actual[4],
            "mismatched_fields": ",".join(differences),
            "reconciliation": category,
        })

    if len(output_rows) != len(issues):
        errors.append(
            "Not all inventory issues were reconciled"
        )

    if len(lookup) != len(output_rows):
        errors.append(
            "Diagnosis and inventory populations differ"
        )

    classification_only = counts.get(
        "INVENTORY_CLASSIFICATION_EMPTY",
        0,
    )

    all_classification_only = (
        len(issues) > 0
        and classification_only == len(issues)
        and not errors
    )

    if errors:
        execution = "FAIL"
    elif all_classification_only:
        execution = "PASS"
    else:
        execution = "REVIEW"

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_rows.sort(
        key=lambda x: (
            x["source_file"],
            int(x["row_number"]),
        )
    )

    csv_path = (
        output_dir / "event_key_reconciliation_detail.csv"
    )
    result_path = (
        output_dir / "event_key_reconciliation_result.json"
    )
    manifest_path = (
        output_dir / "event_key_reconciliation_manifest.json"
    )

    write_csv(csv_path, output_rows)

    result = {
        "audit": AUDIT,
        "version": VERSION,
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "execution": execution,
        "research_decision": "HOLD",
        "inventory_issues": len(issues),
        "reconciled_rows": len(output_rows),
        "classification_only_mismatch_proven": (
            all_classification_only
        ),
        "reconciliation_counts": dict(counts),
        "classification_pairs": [
            {
                "inventory": old,
                "source": new,
                "count": count,
            }
            for (old, new), count in sorted(
                classification_pairs.items()
            )
        ],
        "errors": errors,
        "input_sha256": {
            path.name: digest(path)
            for path in required
        },
        "limitations": [
            "Identifies key discrepancy, not its code location",
            "Does not modify historical checkpoints",
            "Does not prove the source generation logic",
            "Does not independently replay OHLCV",
            "Does not establish unbiased event population",
            "Does not establish independent OOS",
            "Does not establish profitability",
        ],
        "train_approved": False,
        "live_trading_approved": False,
    }

    write_json(result_path, result)

    manifest = {
        "audit": AUDIT,
        "version": VERSION,
        "execution": execution,
        "research_decision": "HOLD",
        "output_sha256": {
            csv_path.name: digest(csv_path),
            result_path.name: digest(result_path),
        },
    }

    write_json(manifest_path, manifest)

    print("\n=== EVENT KEY RECONCILIATION ===")
    print("Execution:", execution)
    print("Inventory issues:", len(issues))
    print("Reconciled rows:", len(output_rows))
    print("Classification-only:", classification_only)
    print("Other discrepancies:",
          len(output_rows) - classification_only)
    print("Errors:", len(errors))
    print("Research decision: HOLD")

    return 1 if execution == "FAIL" else 0


if __name__ == "__main__":
    sys.exit(main())
