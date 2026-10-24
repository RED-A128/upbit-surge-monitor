#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Reproducibility Sensitivity Audit
Clean V001

Research only. No automated trading.
"""

import argparse
import csv
import hashlib
import io
import json
import math
import os
import sys
import tempfile

from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from statistics import NormalDist


VERSION = "Clean V001"

SOURCE_DIR = (
    "data/reports/"
    "pattern_256_nonoverlap_reproducibility"
)

OUTPUT_DIR = (
    "data/reports/"
    "pattern_256_reproducibility_sensitivity"
)

SOURCE_CSV = "reproducibility_comparison.csv"
SOURCE_RESULT = "reproducibility_result.json"
SOURCE_MANIFEST = "reproducibility_manifest.json"

OUTPUT_CSV = "sensitivity_comparison.csv"
OUTPUT_RESULT = "sensitivity_result.json"
OUTPUT_MANIFEST = "sensitivity_manifest.json"

TIMEFRAMES = ("h1", "h4", "d1")
HORIZONS = (24, 72, 168)

SAMPLE_THRESHOLDS = (10, 30, 50, 100, 500)
CONFIDENCE_LEVELS = (0.90, 0.95, 0.99)
EFFECT_THRESHOLDS = (0.0, 0.1, 0.5, 1.0)

OUTPUT_COLUMNS = [
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
    "sample_pass",
    "direction_consistent",
    "train_interval_positive",
    "test_interval_positive",
    "classification",
    "research_decision",
]


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha256(path):
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
        fieldnames=OUTPUT_COLUMNS,
        lineterminator="\n",
    )

    writer.writeheader()
    writer.writerows(rows)

    atomic_write(
        path,
        buffer.getvalue().encode("utf-8"),
    )


def check_safety(payload):
    for field in (
        "statistical_independence_proven",
        "independent_oos_proven",
        "profitability_proven",
        "approved_for_live_trading",
    ):
        require(
            payload.get(field) is False,
            f"Invalid safety field: {field}",
        )


def verify_source(source_dir):
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
        result.get("comparison_rows") == 9,
        "Unexpected source comparison count",
    )

    require(
        result.get("exploratory_reproducible") == 0,
        "Source candidate count changed",
    )

    check_safety(result)

    expected = manifest.get("output_sha256", {})

    for name in (SOURCE_CSV, SOURCE_RESULT):
        require(
            expected.get(name) == sha256(paths[name]),
            f"Source SHA256 mismatch: {name}",
        )

    return paths, result


def read_groups(path):
    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as stream:
        reader = csv.DictReader(stream)

        required = {
            "timeframe",
            "horizon",
            "train_base_events",
            "train_both_events",
            "test_base_events",
            "test_both_events",
            "train_base_hits",
            "train_both_hits",
            "test_base_hits",
            "test_both_hits",
            "research_decision",
        }

        require(
            required.issubset(set(reader.fieldnames or [])),
            "Missing source CSV columns",
        )

        rows = list(reader)

    require(
        len(rows) == 9,
        "Expected nine source groups",
    )

    groups = {}

    for row in rows:
        timeframe = row["timeframe"]
        horizon = int(row["horizon"])

        key = (timeframe, horizon)

        require(
            timeframe in TIMEFRAMES
            and horizon in HORIZONS,
            f"Invalid group: {key}",
        )

        require(
            key not in groups,
            f"Duplicate group: {key}",
        )

        require(
            row["research_decision"] == "HOLD",
            f"Invalid research decision: {key}",
        )

        counts = {}

        for period in ("train", "test"):
            for category in ("base", "both"):
                event_key = f"{period}_{category}_events"
                hit_key = f"{period}_{category}_hits"

                events = int(row[event_key])
                hits = int(row[hit_key])

                require(
                    0 <= hits <= events,
                    f"Invalid event counts: {key}",
                )

                counts[event_key] = events
                counts[hit_key] = hits

        groups[key] = counts

    expected_groups = {
        (timeframe, horizon)
        for timeframe in TIMEFRAMES
        for horizon in HORIZONS
    }

    require(
        set(groups) == expected_groups,
        "Missing comparison groups",
    )

    return groups


def wilson(hits, events, confidence):
    if events == 0:
        return None

    z = NormalDist().inv_cdf(
        (1.0 + confidence) / 2.0
    )

    proportion = hits / events
    z2 = z * z

    denominator = 1.0 + z2 / events

    center = (
        proportion + z2 / (2.0 * events)
    ) / denominator

    radius = (
        z
        * math.sqrt(
            proportion * (1.0 - proportion) / events
            + z2 / (4.0 * events * events)
        )
        / denominator
    )

    return (
        max(0.0, center - radius),
        min(1.0, center + radius),
    )


def evaluate_period(counts, period, confidence):
    base_events = counts[f"{period}_base_events"]
    both_events = counts[f"{period}_both_events"]

    base_hits = counts[f"{period}_base_hits"]
    both_hits = counts[f"{period}_both_hits"]

    if not base_events or not both_events:
        return None

    difference = 100.0 * (
        both_hits / both_events
        - base_hits / base_events
    )

    # Bonferroni marginal intervals:
    # simultaneous coverage at least 'confidence'
    # without assuming independence between proportions.
    marginal = (1.0 + confidence) / 2.0

    base_lower, base_upper = wilson(
        base_hits,
        base_events,
        marginal,
    )

    both_lower, both_upper = wilson(
        both_hits,
        both_events,
        marginal,
    )

    lower = 100.0 * (both_lower - base_upper)

    return {
        "difference": difference,
        "lower": lower,
        "positive": lower > 0,
    }


def format_number(value):
    if value is None:
        return ""

    return f"{value:.8f}"


def build_rows(groups):
    rows = []
    counts = Counter()

    for timeframe in TIMEFRAMES:
        for horizon in HORIZONS:
            group = groups[(timeframe, horizon)]

            for min_events in SAMPLE_THRESHOLDS:
                for confidence in CONFIDENCE_LEVELS:
                    for min_effect in EFFECT_THRESHOLDS:
                        train = evaluate_period(
                            group,
                            "train",
                            confidence,
                        )

                        test = evaluate_period(
                            group,
                            "test",
                            confidence,
                        )

                        sample_pass = all(
                            value >= min_events
                            for key, value in group.items()
                            if key.endswith("_events")
                        )

                        direction_consistent = (
                            train is not None
                            and test is not None
                            and train["difference"] > 0
                            and test["difference"] > 0
                        )

                        train_positive = (
                            train is not None
                            and train["positive"]
                        )

                        test_positive = (
                            test is not None
                            and test["positive"]
                        )

                        if not sample_pass:
                            classification = (
                                "INSUFFICIENT_SAMPLE"
                            )

                        elif not direction_consistent:
                            classification = (
                                "DIRECTION_NOT_REPRODUCED"
                            )

                        elif (
                            train["difference"] < min_effect
                            or test["difference"] < min_effect
                        ):
                            classification = (
                                "EFFECT_BELOW_THRESHOLD"
                            )

                        elif (
                            train_positive
                            and test_positive
                        ):
                            classification = (
                                "EXPLORATORY_REPRODUCIBLE"
                            )

                        else:
                            classification = (
                                "UNCERTAINTY_REMAINS"
                            )

                        counts[classification] += 1

                        rows.append({
                            "timeframe": timeframe,
                            "horizon": horizon,
                            "min_events": min_events,
                            "confidence": confidence,
                            "min_effect_pp": min_effect,

                            "train_base_events": (
                                group["train_base_events"]
                            ),
                            "train_both_events": (
                                group["train_both_events"]
                            ),
                            "test_base_events": (
                                group["test_base_events"]
                            ),
                            "test_both_events": (
                                group["test_both_events"]
                            ),

                            "train_difference_pp": (
                                format_number(
                                    train["difference"]
                                    if train else None
                                )
                            ),
                            "test_difference_pp": (
                                format_number(
                                    test["difference"]
                                    if test else None
                                )
                            ),

                            "train_lower_bound_pp": (
                                format_number(
                                    train["lower"]
                                    if train else None
                                )
                            ),
                            "test_lower_bound_pp": (
                                format_number(
                                    test["lower"]
                                    if test else None
                                )
                            ),

                            "sample_pass": str(
                                sample_pass
                            ).lower(),

                            "direction_consistent": str(
                                direction_consistent
                            ).lower(),

                            "train_interval_positive": str(
                                train_positive
                            ).lower(),

                            "test_interval_positive": str(
                                test_positive
                            ).lower(),

                            "classification": classification,
                            "research_decision": "HOLD",
                        })

    expected = (
        len(TIMEFRAMES)
        * len(HORIZONS)
        * len(SAMPLE_THRESHOLDS)
        * len(CONFIDENCE_LEVELS)
        * len(EFFECT_THRESHOLDS)
    )

    require(
        len(rows) == expected,
        "Sensitivity row count mismatch",
    )

    return rows, dict(counts)


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

        check_safety(result)

        if manifest.get("version") != VERSION:
            return False

        if manifest.get("config") != config:
            return False

        if manifest.get("input_sha256") != input_hashes:
            return False

        hashes = manifest.get("output_sha256", {})

        if hashes.get(OUTPUT_CSV) != sha256(csv_path):
            return False

        if hashes.get(OUTPUT_RESULT) != sha256(result_path):
            return False

        with csv_path.open(
            "r",
            encoding="utf-8-sig",
            newline="",
        ) as stream:
            reader = csv.DictReader(stream)

            if reader.fieldnames != OUTPUT_COLUMNS:
                return False

            rows = list(reader)

        if len(rows) != result.get("comparison_rows"):
            return False

        expected = (
            len(TIMEFRAMES)
            * len(HORIZONS)
            * len(SAMPLE_THRESHOLDS)
            * len(CONFIDENCE_LEVELS)
            * len(EFFECT_THRESHOLDS)
        )

        if len(rows) != expected:
            return False

        actual_counts = dict(
            Counter(
                row["classification"]
                for row in rows
            )
        )

        if actual_counts != result.get(
            "classification_counts"
        ):
            return False

        if any(
            row["research_decision"] != "HOLD"
            for row in rows
        ):
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

    root = Path(args.root).expanduser().resolve()

    source_dir = root / SOURCE_DIR
    output_dir = root / OUTPUT_DIR

    require(
        source_dir.is_dir(),
        f"Missing source directory: {source_dir}",
    )

    source_paths, source_result = verify_source(
        source_dir
    )

    input_hashes = {
        name: sha256(path)
        for name, path in source_paths.items()
    }

    config = {
        "version": VERSION,
        "source_version": "Clean V001",
        "sample_thresholds": list(SAMPLE_THRESHOLDS),
        "confidence_levels": list(CONFIDENCE_LEVELS),
        "effect_thresholds_pp": list(EFFECT_THRESHOLDS),
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
        print("SKIP: completed sensitivity audit")
        return 0

    groups = read_groups(source_paths[SOURCE_CSV])

    rows, classification_counts = build_rows(groups)

    baseline_rows = [
        row
        for row in rows
        if (
            row["min_events"] == 30
            and row["confidence"] == 0.95
            and row["min_effect_pp"] == 0.0
        )
    ]

    require(
        len(baseline_rows) == 9,
        "Baseline comparison mismatch",
    )

    baseline_candidates = sum(
        row["classification"]
        == "EXPLORATORY_REPRODUCIBLE"
        for row in baseline_rows
    )

    require(
        baseline_candidates
        == source_result["exploratory_reproducible"],
        "Baseline reproducibility mismatch",
    )

    result = {
        "version": VERSION,
        "execution_status": "PASS",

        "source_rows": source_result["source_rows"],
        "selected_events": source_result["selected_events"],

        "source_comparison_rows": 9,
        "comparison_rows": len(rows),

        "baseline_exploratory_reproducible": (
            baseline_candidates
        ),

        "sensitivity_exploratory_positive_cells": (
            classification_counts.get(
                "EXPLORATORY_REPRODUCIBLE",
                0,
            )
        ),

        "classification_counts": classification_counts,

        "research_decision": "HOLD",

        "statistical_independence_proven": False,
        "independent_oos_proven": False,
        "profitability_proven": False,
        "approved_for_live_trading": False,

        "config": config,

        "limitations": [
            "Sensitivity scenarios reuse the same observations.",
            "Scenario rows are not independent experiments.",
            "Multiple testing is not globally corrected.",
            "Wilson bounds are not cluster-robust.",
            "Different markets may share common shocks.",
            "Different horizons may remain correlated.",
            "No prospective independent OOS validation.",
            "No matched causal comparison.",
            "No trading cost simulation.",
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
            OUTPUT_CSV: sha256(csv_path),
            OUTPUT_RESULT: sha256(result_path),
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
    print("PATTERN 256 REPRODUCIBILITY SENSITIVITY")
    print("=" * 60)

    print("Execution: PASS")
    print("Version:", VERSION)

    print(
        "Source rows:",
        result["source_rows"],
    )

    print(
        "Selected events:",
        result["selected_events"],
    )

    print(
        "Sensitivity comparison rows:",
        len(rows),
    )

    print(
        "Baseline reproducible groups:",
        baseline_candidates,
    )

    print(
        "Sensitivity exploratory positive cells:",
        result["sensitivity_exploratory_positive_cells"],
    )

    for name, count in sorted(
        classification_counts.items()
    ):
        print(f"{name}: {count}")

    print("Research decision: HOLD")
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
