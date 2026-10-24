#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Research Redesign Gate
Clean V001

Purpose:
- Diagnose the nine original research groups.
- Preserve all negative and uncertain findings.
- Produce a research redesign plan.
- Verify input and output SHA256.
- Support safe resume after output integrity checks.

Research only.
No automated trading.
"""

import argparse
import csv
import hashlib
import io
import json
import os
import sys
import tempfile

from collections import Counter
from datetime import datetime, timezone
from pathlib import Path


VERSION = "Clean V001"

SOURCE_DIR = (
    "data/reports/"
    "pattern_256_reproducibility_sensitivity"
)

OUTPUT_DIR = (
    "data/reports/"
    "pattern_256_research_redesign"
)

SOURCE_CSV = "sensitivity_comparison.csv"
SOURCE_RESULT = "sensitivity_result.json"
SOURCE_MANIFEST = "sensitivity_manifest.json"

OUTPUT_CSV = "research_redesign_comparison.csv"
OUTPUT_RESULT = "research_redesign_result.json"
OUTPUT_MANIFEST = "research_redesign_manifest.json"

TIMEFRAMES = ("h1", "h4", "d1")
HORIZONS = (24, 72, 168)

EXPECTED_SOURCE_ROWS = 540
EXPECTED_BASELINE_ROWS = 9

COLUMNS = [
    "timeframe",
    "horizon",
    "train_base_events",
    "train_both_events",
    "test_base_events",
    "test_both_events",
    "minimum_group_events",
    "train_difference_pp",
    "test_difference_pp",
    "train_lower_bound_pp",
    "test_lower_bound_pp",
    "baseline_classification",
    "redesign_classification",
    "recommended_research_action",
    "eligible_for_live_scanner",
    "research_decision",
]


def require(condition, message):
    if not condition:
        raise ValueError(message)


def file_hash(path):
    digest = hashlib.sha256()

    with path.open("rb") as stream:
        while True:
            block = stream.read(1024 * 1024)

            if not block:
                break

            digest.update(block)

    return digest.hexdigest()


def read_json(path):
    with path.open("r", encoding="utf-8-sig") as stream:
        return json.load(stream)


def read_csv(path):
    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as stream:
        reader = csv.DictReader(stream)
        return reader.fieldnames, list(reader)


def atomic_write(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)

    descriptor, temporary = tempfile.mkstemp(
        prefix=".tmp_",
        dir=str(path.parent),
    )

    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())

        os.replace(temporary, path)

    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def write_json(path, payload):
    content = (
        json.dumps(
            payload,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")

    atomic_write(path, content)


def write_csv(path, rows):
    buffer = io.StringIO(newline="")

    writer = csv.DictWriter(
        buffer,
        fieldnames=COLUMNS,
        lineterminator="\n",
    )

    writer.writeheader()
    writer.writerows(rows)

    atomic_write(
        path,
        buffer.getvalue().encode("utf-8"),
    )


def verify_safety(result):
    fields = (
        "statistical_independence_proven",
        "independent_oos_proven",
        "profitability_proven",
        "approved_for_live_trading",
    )

    for field in fields:
        require(
            result.get(field) is False,
            f"Invalid source safety field: {field}",
        )


def verify_sources(source_dir):
    paths = {
        SOURCE_CSV: source_dir / SOURCE_CSV,
        SOURCE_RESULT: source_dir / SOURCE_RESULT,
        SOURCE_MANIFEST: source_dir / SOURCE_MANIFEST,
    }

    for name, path in paths.items():
        require(
            path.is_file(),
            f"Missing source file: {name}",
        )

    result = read_json(paths[SOURCE_RESULT])
    manifest = read_json(paths[SOURCE_MANIFEST])

    require(
        result.get("version") == "Clean V001",
        "Unexpected source result version",
    )

    require(
        manifest.get("version") == "Clean V001",
        "Unexpected source manifest version",
    )

    require(
        result.get("execution_status") == "PASS",
        "Source execution is not PASS",
    )

    require(
        result.get("research_decision") == "HOLD",
        "Unexpected source decision",
    )

    require(
        result.get("comparison_rows") == EXPECTED_SOURCE_ROWS,
        "Unexpected source comparison rows",
    )

    require(
        result.get("baseline_exploratory_reproducible") == 0,
        "Unexpected baseline reproducibility",
    )

    require(
        result.get("sensitivity_exploratory_positive_cells") == 0,
        "Unexpected positive sensitivity cells",
    )

    verify_safety(result)

    expected_hashes = manifest.get(
        "output_sha256",
        {},
    )

    for name in (SOURCE_CSV, SOURCE_RESULT):
        require(
            expected_hashes.get(name) == file_hash(paths[name]),
            f"Source SHA256 mismatch: {name}",
        )

    return paths, result


def classify(row):
    train_difference = float(
        row["train_difference_pp"]
    )

    test_difference = float(
        row["test_difference_pp"]
    )

    train_lower = float(
        row["train_lower_bound_pp"]
    )

    test_lower = float(
        row["test_lower_bound_pp"]
    )

    event_counts = [
        int(row["train_base_events"]),
        int(row["train_both_events"]),
        int(row["test_base_events"]),
        int(row["test_both_events"]),
    ]

    minimum_events = min(event_counts)

    if minimum_events < 30:
        return (
            "INSUFFICIENT_SAMPLE",
            "COLLECT_MORE_INDEPENDENT_EVENTS",
        )

    if (
        train_difference <= 0
        and test_difference <= 0
    ):
        return (
            "CONSISTENT_NONPOSITIVE",
            "REJECT_CURRENT_COMBINATION",
        )

    if (
        train_difference * test_difference < 0
        or train_difference == 0
        or test_difference == 0
    ):
        return (
            "DIRECTION_UNSTABLE",
            "INVESTIGATE_REGIME_AND_TIME_SPLIT",
        )

    if (
        train_difference > 0
        and test_difference > 0
        and (
            train_lower <= 0
            or test_lower <= 0
        )
    ):
        return (
            "POSITIVE_BUT_UNCERTAIN",
            "REDESIGN_FEATURES_AND_VALIDATE_PROSPECTIVELY",
        )

    if (
        train_difference > 0
        and test_difference > 0
        and train_lower > 0
        and test_lower > 0
    ):
        return (
            "EXPLORATORY_POSITIVE",
            "REQUIRE_INDEPENDENT_PROSPECTIVE_VALIDATION",
        )

    return (
        "UNCLASSIFIED",
        "MANUAL_RESEARCH_REVIEW",
    )


def extract_baseline(path):
    fieldnames, source_rows = read_csv(path)

    required = {
        "timeframe",
        "horizon",
        "min_events",
        "confidence",
        "min_effect_pp",
        "train_base_events",
        "train_both_events",
        "test_base_events",
        "test_both_events",
        "train_difference_pp",
        "test_difference_pp",
        "train_lower_bound_pp",
        "test_lower_bound_pp",
        "classification",
        "research_decision",
    }

    require(
        required.issubset(set(fieldnames or [])),
        "Missing source CSV columns",
    )

    require(
        len(source_rows) == EXPECTED_SOURCE_ROWS,
        "Source CSV row count mismatch",
    )

    baseline = {}

    for row in source_rows:
        if (
            int(row["min_events"]) != 30
            or float(row["confidence"]) != 0.95
            or float(row["min_effect_pp"]) != 0.0
        ):
            continue

        key = (
            row["timeframe"],
            int(row["horizon"]),
        )

        require(
            key not in baseline,
            f"Duplicate baseline group: {key}",
        )

        require(
            row["research_decision"] == "HOLD",
            f"Unexpected decision: {key}",
        )

        baseline[key] = row

    expected = {
        (timeframe, horizon)
        for timeframe in TIMEFRAMES
        for horizon in HORIZONS
    }

    require(
        set(baseline) == expected,
        "Baseline groups missing or unexpected",
    )

    return baseline


def build_rows(baseline):
    rows = []

    for timeframe in TIMEFRAMES:
        for horizon in HORIZONS:
            source = baseline[
                (timeframe, horizon)
            ]

            classification, action = classify(
                source
            )

            event_counts = [
                int(source["train_base_events"]),
                int(source["train_both_events"]),
                int(source["test_base_events"]),
                int(source["test_both_events"]),
            ]

            rows.append({
                "timeframe": timeframe,
                "horizon": horizon,

                "train_base_events": (
                    source["train_base_events"]
                ),
                "train_both_events": (
                    source["train_both_events"]
                ),
                "test_base_events": (
                    source["test_base_events"]
                ),
                "test_both_events": (
                    source["test_both_events"]
                ),

                "minimum_group_events": min(
                    event_counts
                ),

                "train_difference_pp": (
                    source["train_difference_pp"]
                ),
                "test_difference_pp": (
                    source["test_difference_pp"]
                ),
                "train_lower_bound_pp": (
                    source["train_lower_bound_pp"]
                ),
                "test_lower_bound_pp": (
                    source["test_lower_bound_pp"]
                ),

                "baseline_classification": (
                    source["classification"]
                ),

                "redesign_classification": (
                    classification
                ),

                "recommended_research_action": (
                    action
                ),

                "eligible_for_live_scanner": "false",
                "research_decision": "HOLD",
            })

    require(
        len(rows) == EXPECTED_BASELINE_ROWS,
        "Redesign comparison count mismatch",
    )

    return rows


def verify_existing(
    output_dir,
    input_hashes,
    config,
):
    csv_path = output_dir / OUTPUT_CSV
    result_path = output_dir / OUTPUT_RESULT
    manifest_path = output_dir / OUTPUT_MANIFEST

    if not all(
        path.is_file()
        for path in (
            csv_path,
            result_path,
            manifest_path,
        )
    ):
        return False

    try:
        result = read_json(result_path)
        manifest = read_json(manifest_path)

        if result.get("version") != VERSION:
            return False

        if result.get("execution_status") != "PASS":
            return False

        if result.get("research_decision") != "HOLD":
            return False

        if result.get("comparison_rows") != 9:
            return False

        verify_safety(result)

        if manifest.get("version") != VERSION:
            return False

        if manifest.get("config") != config:
            return False

        if manifest.get("input_sha256") != input_hashes:
            return False

        hashes = manifest.get(
            "output_sha256",
            {},
        )

        if hashes.get(OUTPUT_CSV) != file_hash(csv_path):
            return False

        if hashes.get(OUTPUT_RESULT) != file_hash(result_path):
            return False

        fieldnames, rows = read_csv(csv_path)

        if fieldnames != COLUMNS:
            return False

        if len(rows) != 9:
            return False

        if any(
            row["research_decision"] != "HOLD"
            or row["eligible_for_live_scanner"] != "false"
            for row in rows
        ):
            return False

        counts = dict(
            Counter(
                row["redesign_classification"]
                for row in rows
            )
        )

        if counts != result.get("classification_counts"):
            return False

        return True

    except (
        OSError,
        ValueError,
        TypeError,
        KeyError,
        json.JSONDecodeError,
    ):
        return False


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--root",
        default=".",
    )

    parser.add_argument(
        "--force",
        action="store_true",
    )

    args = parser.parse_args()

    root = Path(
        args.root
    ).expanduser().resolve()

    source_dir = root / SOURCE_DIR
    output_dir = root / OUTPUT_DIR

    require(
        source_dir.is_dir(),
        f"Missing source directory: {source_dir}",
    )

    source_paths, source_result = verify_sources(
        source_dir
    )

    input_hashes = {
        name: file_hash(path)
        for name, path in source_paths.items()
    }

    config = {
        "version": VERSION,
        "source_version": "Clean V001",
        "baseline_min_events": 30,
        "baseline_confidence": 0.95,
        "baseline_min_effect_pp": 0.0,
    }

    if (
        not args.force
        and verify_existing(
            output_dir,
            input_hashes,
            config,
        )
    ):
        print("PASS: existing results verified")
        print("SKIP: completed redesign gate")
        return 0

    baseline = extract_baseline(
        source_paths[SOURCE_CSV]
    )

    rows = build_rows(baseline)

    counts = dict(
        Counter(
            row["redesign_classification"]
            for row in rows
        )
    )

    result = {
        "version": VERSION,
        "execution_status": "PASS",

        "source_rows": source_result["source_rows"],
        "selected_events": source_result["selected_events"],

        "source_sensitivity_rows": (
            EXPECTED_SOURCE_ROWS
        ),

        "comparison_rows": len(rows),

        "classification_counts": counts,

        "research_decision": "HOLD",

        "recommended_next_stage": (
            "FEATURE_REDESIGN_RESEARCH"
        ),

        "statistical_independence_proven": False,
        "independent_oos_proven": False,
        "profitability_proven": False,
        "approved_for_live_trading": False,

        "config": config,

        "limitations": [
            "This gate diagnoses previous results only.",
            "No new market evidence is introduced.",
            "Historical groups are not independent proof.",
            "Different markets may be correlated.",
            "Different horizons may be correlated.",
            "Confidence intervals are not cluster-robust.",
            "No prospective independent OOS proof.",
            "No profitability proof.",
            "No automated trading.",
        ],

        "generated_at_utc": (
            datetime.now(timezone.utc).isoformat()
        ),
    }

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    csv_path = output_dir / OUTPUT_CSV
    result_path = output_dir / OUTPUT_RESULT
    manifest_path = output_dir / OUTPUT_MANIFEST

    write_csv(csv_path, rows)
    write_json(result_path, result)

    manifest = {
        "version": VERSION,
        "config": config,

        "input_sha256": input_hashes,

        "output_sha256": {
            OUTPUT_CSV: file_hash(csv_path),
            OUTPUT_RESULT: file_hash(result_path),
        },
    }

    write_json(manifest_path, manifest)

    require(
        verify_existing(
            output_dir,
            input_hashes,
            config,
        ),
        "Final integrity verification failed",
    )

    print("=" * 60)
    print("PATTERN 256 RESEARCH REDESIGN GATE")
    print("=" * 60)

    print("Execution: PASS")
    print("Version:", VERSION)
    print("Source rows:", result["source_rows"])
    print("Selected events:", result["selected_events"])
    print("Comparison rows:", len(rows))

    for name, count in sorted(counts.items()):
        print(f"{name}: {count}")

    print("Research decision: HOLD")
    print("Next stage: FEATURE_REDESIGN_RESEARCH")
    print("Live trading approved: False")
    print("Input SHA256: VERIFIED")
    print("Output SHA256: VERIFIED")

    print("=" * 60)

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())

    except Exception as exc:
        print(
            f"FATAL: {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        sys.exit(1)
