#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Non-Overlap Reproducibility Gate
Clean V001

Purpose:
    Evaluate whether the observed benefit of BOTH
    over PATTERN_256_ONLY is reproducible between
    TRAIN and TEST after within-class de-overlap.

Checks:
    1. Source input/output SHA256 integrity
    2. Source row accounting
    3. TRAIN/TEST directional consistency
    4. Minimum sample requirements
    5. Wilson confidence intervals
    6. Conservative difference interval
    7. Research candidate classification

Important:
    - Diagnostic research only.
    - No automated trading.
    - No live trading approval.
    - No statistical independence claim.
    - No independent prospective OOS claim.
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
    "pattern_256_nonoverlap_retest"
)

OUTPUT_DIR = (
    "data/reports/"
    "pattern_256_nonoverlap_reproducibility"
)

SOURCE_CSV = "nonoverlap_retest_comparison.csv"
SOURCE_RESULT = "nonoverlap_retest_result.json"
SOURCE_MANIFEST = "nonoverlap_retest_manifest.json"

OUTPUT_CSV = "reproducibility_comparison.csv"
OUTPUT_RESULT = "reproducibility_result.json"
OUTPUT_MANIFEST = "reproducibility_manifest.json"

TIMEFRAMES = ("h1", "h4", "d1")
HORIZONS = (24, 72, 168)
SPLITS = ("TRAIN", "TEST")

CLASSES = (
    "PATTERN_256_ONLY",
    "BOTH",
)

SOURCE_COLUMNS = [
    "split",
    "timeframe",
    "horizon",
    "classification",
    "source_events",
    "selected_events",
    "excluded_overlap",
    "hits",
    "hit_rate_pct",
    "unique_markets",
    "research_decision",
]

OUTPUT_COLUMNS = [
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
    "train_difference_pp",
    "test_difference_pp",
    "train_lower_bound_pp",
    "train_upper_bound_pp",
    "test_lower_bound_pp",
    "test_upper_bound_pp",
    "train_direction",
    "test_direction",
    "minimum_sample_pass",
    "direction_consistent",
    "train_conservative_positive",
    "test_conservative_positive",
    "diagnostic_classification",
    "research_decision",
]


def file_sha256(path):
    digest = hashlib.sha256()

    with path.open("rb") as stream:
        while True:
            block = stream.read(1024 * 1024)

            if not block:
                break

            digest.update(block)

    return digest.hexdigest()


def atomic_write(path, content):
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

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
            sort_keys=True,
            indent=2,
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


def read_json(path):
    with path.open(
        "r",
        encoding="utf-8-sig",
    ) as stream:
        return json.load(stream)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def require_false_fields(payload):
    fields = (
        "statistical_independence_proven",
        "independent_oos_proven",
        "profitability_proven",
        "approved_for_live_trading",
    )

    for field in fields:
        require(
            payload.get(field) is False,
            f"Invalid source safety field: {field}",
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
        "Unexpected source research decision",
    )

    require_false_fields(result)

    expected_outputs = manifest.get(
        "output_sha256",
        {},
    )

    for name in (SOURCE_CSV, SOURCE_RESULT):
        expected = expected_outputs.get(name)

        require(
            isinstance(expected, str)
            and len(expected) == 64,
            f"Missing SHA256: {name}",
        )

        actual = file_sha256(paths[name])

        require(
            actual == expected.lower(),
            f"Source SHA256 mismatch: {name}",
        )

    input_hash = manifest.get(
        "input_sha256",
        {},
    ).get("backtest_trades.csv")

    require(
        isinstance(input_hash, str)
        and len(input_hash) == 64,
        "Missing original input hash",
    )

    return paths, result, manifest


def read_source_rows(csv_path, result):
    with csv_path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as stream:
        reader = csv.DictReader(stream)

        require(
            reader.fieldnames == SOURCE_COLUMNS,
            "Unexpected source CSV columns",
        )

        rows = list(reader)

    require(
        len(rows) == 36,
        "Expected 36 source comparison rows",
    )

    require(
        result.get("comparison_rows") == 36,
        "Source comparison count mismatch",
    )

    lookup = {}

    totals = Counter()

    for row in rows:
        split = row["split"]
        timeframe = row["timeframe"]
        classification = row["classification"]

        horizon = int(row["horizon"])

        require(
            split in SPLITS,
            f"Unexpected split: {split}",
        )

        require(
            timeframe in TIMEFRAMES,
            f"Unexpected timeframe: {timeframe}",
        )

        require(
            horizon in HORIZONS,
            f"Unexpected horizon: {horizon}",
        )

        require(
            classification in CLASSES,
            f"Unexpected classification: {classification}",
        )

        require(
            row["research_decision"] == "HOLD",
            "Unexpected CSV research decision",
        )

        key = (
            split,
            timeframe,
            horizon,
            classification,
        )

        require(
            key not in lookup,
            f"Duplicate comparison group: {key}",
        )

        source_events = int(row["source_events"])
        selected = int(row["selected_events"])
        excluded = int(row["excluded_overlap"])
        hits = int(row["hits"])
        markets = int(row["unique_markets"])

        require(
            min(
                source_events,
                selected,
                excluded,
                hits,
                markets,
            ) >= 0,
            f"Negative count: {key}",
        )

        require(
            source_events == selected + excluded,
            f"Accounting mismatch: {key}",
        )

        require(
            hits <= selected,
            f"Hits exceed events: {key}",
        )

        require(
            markets <= selected,
            f"Market count exceeds events: {key}",
        )

        reported_rate = float(row["hit_rate_pct"])

        require(
            math.isfinite(reported_rate),
            f"Nonfinite rate: {key}",
        )

        actual_rate = (
            100.0 * hits / selected
            if selected
            else 0.0
        )

        require(
            abs(reported_rate - actual_rate) <= 0.000001,
            f"Hit rate mismatch: {key}",
        )

        lookup[key] = {
            "source_events": source_events,
            "selected": selected,
            "excluded": excluded,
            "hits": hits,
            "markets": markets,
        }

        totals["source"] += source_events
        totals["selected"] += selected
        totals["excluded"] += excluded
        totals["hits"] += hits

    require(
        len(lookup) == 36,
        "Missing source comparison groups",
    )

    checks = (
        ("eligible_rows", "source"),
        ("selected_events", "selected"),
        ("excluded_overlap", "excluded"),
        ("selected_hits", "hits"),
    )

    for result_field, counter_field in checks:
        require(
            int(result[result_field])
            == totals[counter_field],
            f"Source total mismatch: {result_field}",
        )

    return lookup, totals


def wilson_interval(hits, total, confidence):
    require(
        0 <= hits <= total,
        "Invalid binomial counts",
    )

    if total == 0:
        return None, None

    alpha = 1.0 - confidence

    z = NormalDist().inv_cdf(
        1.0 - alpha / 2.0
    )

    p = hits / total
    z2 = z * z

    denominator = 1.0 + z2 / total

    center = (
        p + z2 / (2.0 * total)
    ) / denominator

    radius = (
        z
        * math.sqrt(
            p * (1.0 - p) / total
            + z2 / (4.0 * total * total)
        )
        / denominator
    )

    return (
        max(0.0, center - radius),
        min(1.0, center + radius),
    )


def compare_group(base, both, confidence):
    base_n = base["selected"]
    both_n = both["selected"]

    if base_n == 0 or both_n == 0:
        return {
            "difference": None,
            "lower": None,
            "upper": None,
            "direction": "MISSING",
            "positive_interval": False,
        }

    base_rate = base["hits"] / base_n
    both_rate = both["hits"] / both_n

    difference = 100.0 * (
        both_rate - base_rate
    )

    # Bonferroni-adjusted marginal Wilson intervals.
    # Each marginal interval uses (1 + confidence) / 2.
    # The resulting difference bounds are conservative
    # under a union-bound argument and do not assume
    # independence between the two proportions.
    marginal_confidence = (
        1.0 + confidence
    ) / 2.0

    base_lower, base_upper = wilson_interval(
        base["hits"],
        base_n,
        marginal_confidence,
    )

    both_lower, both_upper = wilson_interval(
        both["hits"],
        both_n,
        marginal_confidence,
    )

    lower = 100.0 * (
        both_lower - base_upper
    )

    upper = 100.0 * (
        both_upper - base_lower
    )

    if difference > 0:
        direction = "BOTH_HIGHER"
    elif difference < 0:
        direction = "BOTH_LOWER"
    else:
        direction = "EQUAL"

    return {
        "difference": difference,
        "lower": lower,
        "upper": upper,
        "direction": direction,
        "positive_interval": lower > 0,
    }


def display_number(value):
    if value is None:
        return ""

    return f"{value:.8f}"


def build_comparisons(
    lookup,
    min_events,
    confidence,
    min_effect_pp,
):
    rows = []

    counts = Counter()

    for timeframe in TIMEFRAMES:
        for horizon in HORIZONS:

            train_base = lookup[
                (
                    "TRAIN",
                    timeframe,
                    horizon,
                    "PATTERN_256_ONLY",
                )
            ]

            train_both = lookup[
                (
                    "TRAIN",
                    timeframe,
                    horizon,
                    "BOTH",
                )
            ]

            test_base = lookup[
                (
                    "TEST",
                    timeframe,
                    horizon,
                    "PATTERN_256_ONLY",
                )
            ]

            test_both = lookup[
                (
                    "TEST",
                    timeframe,
                    horizon,
                    "BOTH",
                )
            ]

            train = compare_group(
                train_base,
                train_both,
                confidence,
            )

            test = compare_group(
                test_base,
                test_both,
                confidence,
            )

            sample_pass = all(
                item["selected"] >= min_events
                for item in (
                    train_base,
                    train_both,
                    test_base,
                    test_both,
                )
            )

            direction_consistent = (
                train["direction"]
                == test["direction"]
                and train["direction"]
                in {
                    "BOTH_HIGHER",
                    "BOTH_LOWER",
                }
            )

            train_positive = train[
                "positive_interval"
            ]

            test_positive = test[
                "positive_interval"
            ]

            if not sample_pass:
                classification = (
                    "INSUFFICIENT_SAMPLE"
                )

            elif not direction_consistent:
                classification = (
                    "DIRECTION_NOT_REPRODUCED"
                )

            elif (
                train["direction"] == "BOTH_LOWER"
            ):
                classification = (
                    "CONSISTENT_NONPOSITIVE"
                )

            elif (
                train["difference"] < min_effect_pp
                or test["difference"] < min_effect_pp
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

                "train_base_events": (
                    train_base["selected"]
                ),
                "train_both_events": (
                    train_both["selected"]
                ),
                "test_base_events": (
                    test_base["selected"]
                ),
                "test_both_events": (
                    test_both["selected"]
                ),

                "train_base_hits": (
                    train_base["hits"]
                ),
                "train_both_hits": (
                    train_both["hits"]
                ),
                "test_base_hits": (
                    test_base["hits"]
                ),
                "test_both_hits": (
                    test_both["hits"]
                ),

                "train_difference_pp": (
                    display_number(
                        train["difference"]
                    )
                ),
                "test_difference_pp": (
                    display_number(
                        test["difference"]
                    )
                ),

                "train_lower_bound_pp": (
                    display_number(
                        train["lower"]
                    )
                ),
                "train_upper_bound_pp": (
                    display_number(
                        train["upper"]
                    )
                ),

                "test_lower_bound_pp": (
                    display_number(
                        test["lower"]
                    )
                ),
                "test_upper_bound_pp": (
                    display_number(
                        test["upper"]
                    )
                ),

                "train_direction": (
                    train["direction"]
                ),
                "test_direction": (
                    test["direction"]
                ),

                "minimum_sample_pass": (
                    str(sample_pass).lower()
                ),
                "direction_consistent": (
                    str(direction_consistent).lower()
                ),

                "train_conservative_positive": (
                    str(train_positive).lower()
                ),
                "test_conservative_positive": (
                    str(test_positive).lower()
                ),

                "diagnostic_classification": (
                    classification
                ),
                "research_decision": "HOLD",
            })

    require(
        len(rows) == 9,
        "Expected nine reproducibility comparisons",
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

        if result.get("comparison_rows") != 9:
            return False

        if manifest.get("version") != VERSION:
            return False

        if manifest.get("config") != config:
            return False

        if manifest.get("input_sha256") != input_hashes:
            return False

        require_false_fields(result)

        output_hashes = manifest.get(
            "output_sha256",
            {},
        )

        if output_hashes.get(
            OUTPUT_CSV
        ) != file_sha256(csv_path):
            return False

        if output_hashes.get(
            OUTPUT_RESULT
        ) != file_sha256(result_path):
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

        if len(rows) != 9:
            return False

        if any(
            row["research_decision"] != "HOLD"
            for row in rows
        ):
            return False

        expected_groups = {
            (timeframe, str(horizon))
            for timeframe in TIMEFRAMES
            for horizon in HORIZONS
        }

        actual_groups = {
            (row["timeframe"], row["horizon"])
            for row in rows
        }

        if actual_groups != expected_groups:
            return False

        actual_counts = dict(
            Counter(
                row["diagnostic_classification"]
                for row in rows
            )
        )

        if actual_counts != result.get(
            "classification_counts"
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
        "--min-events",
        type=int,
        default=30,
    )

    parser.add_argument(
        "--confidence",
        type=float,
        default=0.95,
    )

    parser.add_argument(
        "--min-effect-pp",
        type=float,
        default=0.0,
    )

    parser.add_argument(
        "--force",
        action="store_true",
    )

    args = parser.parse_args()

    require(
        args.min_events >= 1,
        "min-events must be >= 1",
    )

    require(
        math.isfinite(args.confidence)
        and 0 < args.confidence < 1,
        "confidence must be between 0 and 1",
    )

    require(
        math.isfinite(args.min_effect_pp)
        and args.min_effect_pp >= 0,
        "min-effect-pp must be nonnegative",
    )

    root = Path(
        args.root
    ).expanduser().resolve()

    source_dir = root / SOURCE_DIR
    output_dir = root / OUTPUT_DIR

    require(
        source_dir.is_dir(),
        f"Missing source directory: {source_dir}",
    )

    source_paths, source_result, _ = verify_source(
        source_dir
    )

    input_hashes = {
        name: file_sha256(path)
        for name, path in source_paths.items()
    }

    config = {
        "min_events": args.min_events,
        "confidence": args.confidence,
        "min_effect_pp": args.min_effect_pp,
        "source_version": "Clean V001",
        "version": VERSION,
    }

    if (
        not args.force
        and verify_existing(
            output_dir,
            input_hashes,
            config,
        )
    ):
        print(
            "PASS: existing results verified"
        )

        print(
            "SKIP: completed reproducibility gate"
        )

        return 0

    lookup, totals = read_source_rows(
        source_paths[SOURCE_CSV],
        source_result,
    )

    rows, classification_counts = build_comparisons(
        lookup,
        args.min_events,
        args.confidence,
        args.min_effect_pp,
    )

    result = {
        "version": VERSION,
        "execution_status": "PASS",

        "source_rows": (
            source_result["source_rows"]
        ),

        "eligible_rows": totals["source"],
        "selected_events": totals["selected"],
        "selected_hits": totals["hits"],
        "excluded_overlap": totals["excluded"],

        "comparison_rows": len(rows),

        "classification_counts": (
            classification_counts
        ),

        "exploratory_reproducible": (
            classification_counts.get(
                "EXPLORATORY_REPRODUCIBLE",
                0,
            )
        ),

        "research_decision": "HOLD",

        "statistical_independence_proven": False,
        "independent_oos_proven": False,
        "profitability_proven": False,
        "approved_for_live_trading": False,

        "config": config,

        "limitations": [
            "This audit uses aggregated non-overlap results.",
            "Within-class de-overlap does not establish full independence.",
            "Different markets may remain correlated.",
            "Different horizons may remain correlated.",
            "Conservative Wilson bounds are diagnostic.",
            "Confidence intervals are not cluster-robust.",
            "Repeated comparison multiplicity is not corrected globally.",
            "The minimum event threshold is a screening rule.",
            "TRAIN and TEST are historical partitions.",
            "No prospective independent OOS proof.",
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
            OUTPUT_CSV: file_sha256(csv_path),
            OUTPUT_RESULT: file_sha256(result_path),
        },
    }

    write_json(
        manifest_path,
        manifest,
    )

    require(
        verify_existing(
            output_dir,
            input_hashes,
            config,
        ),
        "Final integrity verification failed",
    )

    print("=" * 60)
    print("PATTERN 256 NON-OVERLAP REPRODUCIBILITY")
    print("=" * 60)

    print("Execution: PASS")
    print(f"Version: {VERSION}")

    print(
        "Source rows:",
        f"{result['source_rows']:,}",
    )

    print(
        "Selected events:",
        f"{result['selected_events']:,}",
    )

    print(
        "Comparison rows:",
        result["comparison_rows"],
    )

    print("Classification counts:")

    for name, count in sorted(
        classification_counts.items()
    ):
        print(f"  {name}: {count}")

    print("Research decision: HOLD")
    print("Statistical independence proven: False")
    print("Independent OOS proven: False")
    print("Profitability proven: False")
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
