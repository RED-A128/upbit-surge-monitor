
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Gap Review Finalization
Clean V001

Read-only review of existing reconciliation reports.
Does not modify OHLCV, signals, outcomes or checkpoints.
Does not prove OOS performance or approve trading.
"""

import argparse
import csv
import hashlib
import json
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

VERSION = "Clean V001"
AUDIT = "pattern_256_gap_review_finalization"

INPUT_DIR = Path(
    "data/reports/pattern_256_window_gap_reconciliation"
)
OUTPUT_DIR = Path(
    "data/reports/pattern_256_gap_review_finalization"
)

INPUTS = (
    "window_gap_reconciliation_result.json",
    "window_gap_reconciliation_samples.csv",
    "window_gap_reconciliation_by_category.csv",
    "window_gap_reconciliation_by_timeframe.csv",
)

OUTPUTS = (
    "gap_review_result.json",
    "gap_review_matrix.csv",
    "gap_review_attention_cases.csv",
)

EXPECTED_CATEGORIES = {
    "WINDOW_GAP",
    "INCOMPLETE_COMPARISON",
    "EXCLUDE_MATCH",
    "INCLUDE_MATCH",
    "BOTH_MATCH",
    "ROW_ERROR:TIMESTAMP_EMPTY",
}

EXPECTED_DIAGNOSES = {
    "BOTH_WINDOWS_COMPLETE",
    "BOTH_WINDOWS_HAVE_MISSING",
    "EXCLUDE_WINDOW_MISSING",
    "INCLUDE_WINDOW_MISSING",
    "ROW_ERROR:EMPTY_TIMESTAMP",
}


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(
            lambda: stream.read(1024 * 1024), b""
        ):
            digest.update(chunk)
    return digest.hexdigest()


def load_json(path):
    with path.open("r", encoding="utf-8-sig") as stream:
        return json.load(stream)


def load_csv(path):
    with path.open(
        "r", encoding="utf-8-sig", newline=""
    ) as stream:
        return list(csv.DictReader(stream))


def write_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8") as stream:
        json.dump(
            data, stream, indent=2,
            ensure_ascii=False, sort_keys=True
        )
        stream.write("\n")
    os.replace(temporary, path)


def write_csv(path, columns, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open(
        "w", encoding="utf-8-sig", newline=""
    ) as stream:
        writer = csv.DictWriter(
            stream,
            fieldnames=columns,
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(rows)
    os.replace(temporary, path)


def check_input_integrity(folder):
    manifest_path = (
        folder / "window_gap_reconciliation_manifest.json"
    )
    if not manifest_path.is_file():
        raise RuntimeError("Input manifest missing")

    manifest = load_json(manifest_path)
    if manifest.get("audit") != (
        "pattern_256_window_gap_reconciliation"
    ):
        raise RuntimeError("Input audit mismatch")

    if manifest.get("version") != "Clean V001":
        raise RuntimeError("Input version mismatch")

    hashes = manifest.get("outputs", {})
    for name in INPUTS:
        path = folder / name
        if not path.is_file():
            raise RuntimeError("Missing input: " + name)
        if sha256(path) != hashes.get(name):
            raise RuntimeError(
                "Input SHA256 mismatch: " + name
            )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True)
    args = parser.parse_args()

    root = Path(args.root).expanduser().resolve()
    source = root / INPUT_DIR
    output = root / OUTPUT_DIR

    check_input_integrity(source)

    prior = load_json(
        source / "window_gap_reconciliation_result.json"
    )
    samples = load_csv(
        source / "window_gap_reconciliation_samples.csv"
    )
    category_report = load_csv(
        source / "window_gap_reconciliation_by_category.csv"
    )
    timeframe_report = load_csv(
        source / "window_gap_reconciliation_by_timeframe.csv"
    )

    if prior.get("execution") != "PASS":
        raise RuntimeError("Prior audit did not PASS")

    if prior.get("error_count") != 0:
        raise RuntimeError("Prior audit contains errors")

    if len(samples) != prior.get("sample_rows_processed"):
        raise RuntimeError("Sample count mismatch")

    if len(samples) != prior.get("sample_rows_selected"):
        raise RuntimeError("Sample selection mismatch")

    required = {
        "timeframe",
        "market",
        "detail_line",
        "classification",
        "recorded_status",
        "horizon",
        "signal_timestamp",
        "entry_timestamp",
        "include_result",
        "exclude_result",
        "include_missing_slots",
        "exclude_missing_slots",
        "diagnosis",
    }

    if samples and not required.issubset(samples[0]):
        raise RuntimeError("Sample columns missing")

    matrix = Counter()
    by_timeframe = Counter()
    attention = []
    invalid = []
    keys = set()

    for row in samples:
        tf = row["timeframe"]
        category = row["classification"]
        diagnosis = row["diagnosis"]

        if tf not in {"h1", "h4", "d1"}:
            invalid.append("Invalid timeframe")
            continue

        if category not in EXPECTED_CATEGORIES:
            invalid.append("Unknown classification")
            continue

        if diagnosis not in EXPECTED_DIAGNOSES:
            invalid.append("Unknown diagnosis")
            continue

        key = (
            tf, row["market"],
            row["detail_line"]
        )

        if key in keys:
            invalid.append("Duplicate sample key")
            continue

        keys.add(key)
        matrix[(tf, category, diagnosis)] += 1
        by_timeframe[tf] += 1

        if category == "EXCLUDE_MATCH":
            reason = "EXCLUDE_POLICY_SEMANTICS_REVIEW"
        elif (
            category == "INCLUDE_MATCH"
            and diagnosis != "BOTH_WINDOWS_COMPLETE"
        ):
            reason = "ENTRY_WINDOW_BOUNDARY_REVIEW"
        elif category == "ROW_ERROR:TIMESTAMP_EMPTY":
            reason = "CENSORED_ENTRY_FIELDS_REVIEW"
        elif (
            category == "WINDOW_GAP"
            and diagnosis == "BOTH_WINDOWS_COMPLETE"
        ):
            reason = "POSSIBLE_GAP_CLASSIFICATION_MISMATCH"
        elif (
            category == "BOTH_MATCH"
            and diagnosis != "BOTH_WINDOWS_COMPLETE"
        ):
            reason = "POSSIBLE_COMPLETE_CLASSIFICATION_MISMATCH"
        else:
            reason = ""

        if reason:
            attention.append({
                **row,
                "review_reason": reason,
            })

    # Independently cross-check existing summary CSVs.
    expected_category = Counter()
    for (tf, category, diagnosis), count in matrix.items():
        expected_category[(tf, category)] += count

    reported_category = Counter()
    for row in category_report:
        key = (
            row["timeframe"],
            row["classification"]
        )
        reported_category[key] += int(row["total"])

    if expected_category != reported_category:
        invalid.append("Category summary mismatch")

    reported_timeframe = Counter()
    for row in timeframe_report:
        reported_timeframe[row["timeframe"]] += int(
            row["total"]
        )

    if by_timeframe != reported_timeframe:
        invalid.append("Timeframe summary mismatch")

    if len(keys) != len(samples):
        invalid.append("Unique sample count mismatch")

    matrix_rows = [
        {
            "timeframe": tf,
            "classification": category,
            "diagnosis": diagnosis,
            "count": count,
        }
        for (tf, category, diagnosis), count
        in sorted(matrix.items())
    ]

    attention_columns = (
        "review_reason",
        "timeframe",
        "market",
        "detail_line",
        "classification",
        "recorded_status",
        "horizon",
        "signal_timestamp",
        "entry_timestamp",
        "include_result",
        "exclude_result",
        "include_missing_slots",
        "exclude_missing_slots",
        "diagnosis",
    )

    write_csv(
        output / "gap_review_matrix.csv",
        (
            "timeframe",
            "classification",
            "diagnosis",
            "count",
        ),
        matrix_rows,
    )

    write_csv(
        output / "gap_review_attention_cases.csv",
        attention_columns,
        attention,
    )

    reasons = Counter(
        row["review_reason"] for row in attention
    )

    # A completed diagnostic is not an approval gate.
    execution = "PASS" if not invalid else "FAIL"

    result = {
        "audit": AUDIT,
        "version": VERSION,
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "execution": execution,
        "classification": "REVIEW",
        "research_decision": "HOLD",
        "input_samples": len(samples),
        "validated_samples": len(keys),
        "attention_cases": len(attention),
        "attention_counts": dict(reasons),
        "timeframe_counts": dict(by_timeframe),
        "errors": invalid,
        "error_count": len(invalid),
        "next_research_gate": (
            "REVIEW_EVALUATOR_POLICY_AND_ENTRY_BOUNDARY"
        ),
        "limitations": [
            "Uses an existing deterministic first-N sample",
            "Does not independently rerun the evaluator",
            "Classification names do not establish policy semantics",
            "Missing candles do not prove collection failure",
            "Censored events may legitimately lack entry fields",
            "Does not prove independent OOS performance",
            "Does not prove profitability",
        ],
        "train_approved": False,
        "live_trading_approved": False,
    }

    write_json(output / "gap_review_result.json", result)

    manifest = {
        "audit": AUDIT,
        "version": VERSION,
        "outputs": {
            name: sha256(output / name)
            for name in OUTPUTS
        },
    }

    write_json(
        output / "gap_review_manifest.json",
        manifest,
    )

    print(
        json.dumps(
            {
                "execution": execution,
                "samples": len(samples),
                "attention_cases": len(attention),
                "attention_counts": dict(reasons),
                "error_count": len(invalid),
                "research_decision": "HOLD",
            },
            ensure_ascii=False,
            indent=2,
        ),
        flush=True,
    )

    return 0 if execution == "PASS" else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(
            "[FATAL] {}: {}".format(
                type(exc).__name__, exc
            ),
            file=sys.stderr,
            flush=True,
        )
        sys.exit(1)
