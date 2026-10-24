#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Regime Stability Gate
Clean V001

Research-only validation.

Input:
    data/reports/pattern_256_regime_robustness/
        regime_robustness_comparison.csv
        regime_robustness_result.json
        regime_robustness_manifest.json

Output:
    data/reports/pattern_256_regime_stability_gate/
        regime_stability_comparison.csv
        regime_stability_result.json
        regime_stability_manifest.json

No trading.
No modification of source files.
No future information used for signal generation.
"""

import argparse
import csv
import hashlib
import json
import math
import os
import sys
import tempfile

from collections import Counter
from datetime import datetime, timezone
from pathlib import Path


VERSION = "Clean V001"

SOURCE_DIR = (
    "data/reports/"
    "pattern_256_regime_robustness"
)

OUTPUT_DIR = (
    "data/reports/"
    "pattern_256_regime_stability_gate"
)

SOURCE_FILES = (
    "regime_robustness_comparison.csv",
    "regime_robustness_result.json",
    "regime_robustness_manifest.json",
)

OUTPUT_CSV = "regime_stability_comparison.csv"
OUTPUT_RESULT = "regime_stability_result.json"
OUTPUT_MANIFEST = "regime_stability_manifest.json"

REQUIRED_COLUMNS = (
    "split",
    "timeframe",
    "horizon",
    "regime",
    "only_events",
    "only_hits",
    "both_events",
    "both_hits",
)

TIMEFRAMES = ("h1", "h4", "d1")
HORIZONS = (24, 72, 168)
REGIMES = ("UP", "DOWN", "SIDEWAYS")

CSV_COLUMNS = (
    "timeframe",
    "horizon",
    "regime",
    "train_only_events",
    "train_only_hits",
    "train_both_events",
    "train_both_hits",
    "train_uplift_pp",
    "test_only_events",
    "test_only_hits",
    "test_both_events",
    "test_both_hits",
    "test_uplift_pp",
    "train_sample_ok",
    "test_sample_ok",
    "train_direction",
    "test_direction",
    "stability_status",
)


def sha256_file(path):
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        while True:
            block = handle.read(1024 * 1024)

            if not block:
                break

            digest.update(block)

    return digest.hexdigest()


def load_json(path):
    with path.open("r", encoding="utf-8-sig") as handle:
        return json.load(handle)


def atomic_write(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)

    fd, temp_name = tempfile.mkstemp(
        prefix=".tmp_",
        dir=str(path.parent),
    )

    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())

        os.replace(temp_name, path)

    finally:
        if os.path.exists(temp_name):
            os.unlink(temp_name)


def write_json(path, payload):
    data = (
        json.dumps(
            payload,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")

    atomic_write(path, data)


def write_csv(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)

    fd, temp_name = tempfile.mkstemp(
        prefix=".tmp_",
        dir=str(path.parent),
        text=True,
    )

    try:
        with os.fdopen(
            fd,
            "w",
            encoding="utf-8",
            newline="",
        ) as handle:

            writer = csv.DictWriter(
                handle,
                fieldnames=CSV_COLUMNS,
            )

            writer.writeheader()
            writer.writerows(rows)

            handle.flush()
            os.fsync(handle.fileno())

        os.replace(temp_name, path)

    finally:
        if os.path.exists(temp_name):
            os.unlink(temp_name)


def require_sha256(value, label):
    if not isinstance(value, str):
        raise ValueError(
            f"Invalid SHA256 type: {label}"
        )

    value = value.lower().strip()

    if (
        len(value) != 64
        or any(ch not in "0123456789abcdef" for ch in value)
    ):
        raise ValueError(
            f"Invalid SHA256: {label}"
        )

    return value


def verify_source(source_dir):
    paths = {}

    for name in SOURCE_FILES:
        path = source_dir / name

        if not path.is_file():
            raise FileNotFoundError(
                f"Missing source file: {path}"
            )

        paths[name] = path

    result = load_json(
        paths["regime_robustness_result.json"]
    )

    manifest = load_json(
        paths["regime_robustness_manifest.json"]
    )

    if result.get("version") != "Clean V002":
        raise ValueError(
            "Unexpected source result version"
        )

    if manifest.get("version") != "Clean V002":
        raise ValueError(
            "Unexpected source manifest version"
        )

    if result.get("execution_status") != "PASS":
        raise ValueError(
            "Source execution is not PASS"
        )

    if result.get("research_decision") != "HOLD":
        raise ValueError(
            "Unexpected source research decision"
        )

    if result.get("approved_for_live_trading") is not False:
        raise ValueError(
            "Source trading safety check failed"
        )

    output_hashes = manifest.get("output_sha256")

    if not isinstance(output_hashes, dict):
        raise ValueError(
            "Source output_sha256 missing"
        )

    for name in (
        "regime_robustness_comparison.csv",
        "regime_robustness_result.json",
    ):
        expected = require_sha256(
            output_hashes.get(name),
            name,
        )

        actual = sha256_file(paths[name])

        if actual != expected:
            raise ValueError(
                f"Source SHA256 mismatch: {name}"
            )

    return paths, result, manifest


def parse_nonnegative_int(value, label):
    if value is None:
        raise ValueError(
            f"Missing integer: {label}"
        )

    text = str(value).strip()

    if not text.isdigit():
        raise ValueError(
            f"Invalid integer: {label}={value}"
        )

    result = int(text)

    if result < 0:
        raise ValueError(
            f"Negative integer: {label}"
        )

    return result


def parse_optional_float(value, label):
    if value is None:
        return None

    text = str(value).strip()

    if not text:
        return None

    number = float(text)

    if not math.isfinite(number):
        raise ValueError(
            f"Non-finite value: {label}"
        )

    return number


def rate_pct(hits, events):
    if events == 0:
        return None

    return 100.0 * hits / events


def uplift_pp(row):
    only_rate = rate_pct(
        row["only_hits"],
        row["only_events"],
    )

    both_rate = rate_pct(
        row["both_hits"],
        row["both_events"],
    )

    if only_rate is None or both_rate is None:
        return None

    return both_rate - only_rate


def direction(value):
    if value is None:
        return "MISSING"

    if abs(value) < 1e-12:
        return "EQUAL"

    if value > 0:
        return "BOTH_HIGHER"

    return "ONLY_HIGHER"


def format_number(value):
    if value is None:
        return ""

    return f"{value:.8f}"


def read_comparisons(path):
    records = {}

    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as handle:

        reader = csv.DictReader(handle)

        if not reader.fieldnames:
            raise ValueError(
                "Source CSV header missing"
            )

        missing = [
            column
            for column in REQUIRED_COLUMNS
            if column not in reader.fieldnames
        ]

        if missing:
            raise ValueError(
                f"Missing CSV columns: {missing}"
            )

        for line_number, raw in enumerate(
            reader,
            start=2,
        ):
            split = raw["split"].strip().upper()
            timeframe = raw["timeframe"].strip()
            regime = raw["regime"].strip().upper()

            horizon = parse_nonnegative_int(
                raw["horizon"],
                f"line {line_number} horizon",
            )

            if split not in ("TRAIN", "TEST"):
                raise ValueError(
                    f"Invalid split at line {line_number}"
                )

            if timeframe not in TIMEFRAMES:
                raise ValueError(
                    f"Invalid timeframe at line {line_number}"
                )

            if horizon not in HORIZONS:
                raise ValueError(
                    f"Invalid horizon at line {line_number}"
                )

            if regime not in REGIMES:
                raise ValueError(
                    f"Invalid regime at line {line_number}"
                )

            key = (
                split,
                timeframe,
                horizon,
                regime,
            )

            if key in records:
                raise ValueError(
                    f"Duplicate comparison cell: {key}"
                )

            item = {
                "split": split,
                "timeframe": timeframe,
                "horizon": horizon,
                "regime": regime,
            }

            for field in (
                "only_events",
                "only_hits",
                "both_events",
                "both_hits",
            ):
                item[field] = parse_nonnegative_int(
                    raw[field],
                    f"line {line_number} {field}",
                )

            if item["only_hits"] > item["only_events"]:
                raise ValueError(
                    f"Invalid ONLY hits at line {line_number}"
                )

            if item["both_hits"] > item["both_events"]:
                raise ValueError(
                    f"Invalid BOTH hits at line {line_number}"
                )

            calculated = uplift_pp(item)

            if "uplift_pp" in raw:
                supplied = parse_optional_float(
                    raw.get("uplift_pp"),
                    f"line {line_number} uplift_pp",
                )

                if (
                    calculated is not None
                    and supplied is not None
                    and abs(calculated - supplied) > 0.00001
                ):
                    raise ValueError(
                        f"Uplift mismatch at line {line_number}"
                    )

            records[key] = item

    if not records:
        raise ValueError(
            "Source comparison CSV is empty"
        )

    return records


def classify_stability(
    train,
    test,
    minimum_events,
):
    if train is None or test is None:
        return "MISSING_PERIOD"

    train_uplift = uplift_pp(train)
    test_uplift = uplift_pp(test)

    if train_uplift is None or test_uplift is None:
        return "MISSING_COMPARISON_GROUP"

    train_sample_ok = (
        train["only_events"] >= minimum_events
        and train["both_events"] >= minimum_events
    )

    test_sample_ok = (
        test["only_events"] >= minimum_events
        and test["both_events"] >= minimum_events
    )

    if not train_sample_ok or not test_sample_ok:
        return "INSUFFICIENT_SAMPLE"

    train_direction = direction(train_uplift)
    test_direction = direction(test_uplift)

    if (
        train_direction == "BOTH_HIGHER"
        and test_direction == "BOTH_HIGHER"
    ):
        return "POSITIVE_BOTH_PERIODS"

    if (
        train_direction == "ONLY_HIGHER"
        and test_direction == "ONLY_HIGHER"
    ):
        return "NEGATIVE_BOTH_PERIODS"

    if (
        train_direction == "EQUAL"
        or test_direction == "EQUAL"
    ):
        return "NEUTRAL_OR_CHANGED"

    return "DIRECTION_CHANGED"


def build_output_rows(records, minimum_events):
    output = []

    for timeframe in TIMEFRAMES:
        for horizon in HORIZONS:
            for regime in REGIMES:

                train = records.get(
                    ("TRAIN", timeframe, horizon, regime)
                )

                test = records.get(
                    ("TEST", timeframe, horizon, regime)
                )

                train_uplift = (
                    uplift_pp(train)
                    if train is not None
                    else None
                )

                test_uplift = (
                    uplift_pp(test)
                    if test is not None
                    else None
                )

                train_ok = (
                    train is not None
                    and train["only_events"] >= minimum_events
                    and train["both_events"] >= minimum_events
                )

                test_ok = (
                    test is not None
                    and test["only_events"] >= minimum_events
                    and test["both_events"] >= minimum_events
                )

                def count(item, field):
                    if item is None:
                        return ""

                    return item[field]

                output.append({
                    "timeframe": timeframe,
                    "horizon": horizon,
                    "regime": regime,

                    "train_only_events": count(
                        train, "only_events"
                    ),
                    "train_only_hits": count(
                        train, "only_hits"
                    ),
                    "train_both_events": count(
                        train, "both_events"
                    ),
                    "train_both_hits": count(
                        train, "both_hits"
                    ),

                    "train_uplift_pp": format_number(
                        train_uplift
                    ),

                    "test_only_events": count(
                        test, "only_events"
                    ),
                    "test_only_hits": count(
                        test, "only_hits"
                    ),
                    "test_both_events": count(
                        test, "both_events"
                    ),
                    "test_both_hits": count(
                        test, "both_hits"
                    ),

                    "test_uplift_pp": format_number(
                        test_uplift
                    ),

                    "train_sample_ok": train_ok,
                    "test_sample_ok": test_ok,

                    "train_direction": direction(
                        train_uplift
                    ),
                    "test_direction": direction(
                        test_uplift
                    ),

                    "stability_status": classify_stability(
                        train,
                        test,
                        minimum_events,
                    ),
                })

    return output


def validate_existing_outputs(
    output_dir,
    source_hashes,
    minimum_events,
):
    manifest_path = output_dir / OUTPUT_MANIFEST
    result_path = output_dir / OUTPUT_RESULT
    csv_path = output_dir / OUTPUT_CSV

    if not all(
        path.is_file()
        for path in (
            manifest_path,
            result_path,
            csv_path,
        )
    ):
        return False

    try:
        manifest = load_json(manifest_path)
        result = load_json(result_path)

        if manifest.get("version") != VERSION:
            return False

        if result.get("version") != VERSION:
            return False

        if result.get("execution_status") != "PASS":
            return False

        if result.get("research_decision") != "HOLD":
            return False

        if result.get("approved_for_live_trading") is not False:
            return False

        if manifest.get("input_sha256") != source_hashes:
            return False

        if manifest.get("config", {}).get(
            "min_events"
        ) != minimum_events:
            return False

        output_hashes = manifest.get(
            "output_sha256", {}
        )

        for name in (
            OUTPUT_CSV,
            OUTPUT_RESULT,
        ):
            expected = require_sha256(
                output_hashes.get(name),
                name,
            )

            actual = sha256_file(output_dir / name)

            if expected != actual:
                return False

        with csv_path.open(
            "r",
            encoding="utf-8-sig",
            newline="",
        ) as handle:
            reader = csv.DictReader(handle)

            if reader.fieldnames != list(CSV_COLUMNS):
                return False

            rows = list(reader)

        if len(rows) != result.get("comparison_rows"):
            return False

        if len(rows) != 27:
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
        "--force",
        action="store_true",
    )

    args = parser.parse_args()

    if args.min_events < 1:
        raise ValueError(
            "--min-events must be >= 1"
        )

    root = Path(args.root).expanduser().resolve()

    source_dir = root / SOURCE_DIR
    output_dir = root / OUTPUT_DIR

    if not source_dir.is_dir():
        raise FileNotFoundError(
            f"Missing source directory: {source_dir}"
        )

    source_paths, source_result, _ = verify_source(
        source_dir
    )

    source_hashes = {
        name: sha256_file(path)
        for name, path in source_paths.items()
    }

    if (
        not args.force
        and validate_existing_outputs(
            output_dir,
            source_hashes,
            args.min_events,
        )
    ):
        print("PASS: verified existing result")
        print("SKIP: recomputation not required")
        return 0

    records = read_comparisons(
        source_paths[
            "regime_robustness_comparison.csv"
        ]
    )

    source_cells = source_result.get(
        "comparison_cells"
    )

    if source_cells != len(records):
        raise ValueError(
            "Source comparison cell count mismatch"
        )

    output_rows = build_output_rows(
        records,
        args.min_events,
    )

    if len(output_rows) != 27:
        raise ValueError(
            "Unexpected stability cell count"
        )

    counts = Counter(
        row["stability_status"]
        for row in output_rows
    )

    positive = counts["POSITIVE_BOTH_PERIODS"]
    negative = counts["NEGATIVE_BOTH_PERIODS"]
    changed = counts["DIRECTION_CHANGED"]
    insufficient = counts["INSUFFICIENT_SAMPLE"]

    result = {
        "version": VERSION,
        "execution_status": "PASS",
        "research_decision": "HOLD",
        "approved_for_live_trading": False,
        "statistical_independence_proven": False,
        "independent_oos_proven": False,
        "profitability_proven": False,

        "source_version": source_result["version"],
        "source_rows": source_result["source_rows"],
        "source_comparison_cells": len(records),

        "comparison_rows": len(output_rows),

        "min_events": args.min_events,

        "summary": {
            "positive_both_periods": positive,
            "negative_both_periods": negative,
            "direction_changed": changed,
            "insufficient_sample": insufficient,
            "missing_period": counts["MISSING_PERIOD"],
            "missing_comparison_group": counts[
                "MISSING_COMPARISON_GROUP"
            ],
            "neutral_or_changed": counts[
                "NEUTRAL_OR_CHANGED"
            ],
            "status_counts": dict(
                sorted(counts.items())
            ),
        },

        "limitations": [
            "This stage uses aggregated regime results.",
            "Events are not independently deduplicated here.",
            "TRAIN and TEST share the original research design.",
            "A minimum event count is not statistical proof.",
            "Direction stability does not prove profitability.",
            "Trading costs are not included.",
            "No live trading approval is granted.",
        ],

        "generated_at_utc": (
            datetime.now(timezone.utc)
            .isoformat()
        ),
    }

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    csv_path = output_dir / OUTPUT_CSV
    result_path = output_dir / OUTPUT_RESULT
    manifest_path = output_dir / OUTPUT_MANIFEST

    write_csv(
        csv_path,
        output_rows,
    )

    write_json(
        result_path,
        result,
    )

    manifest = {
        "version": VERSION,

        "config": {
            "min_events": args.min_events,
            "source_version": source_result["version"],
        },

        "input_sha256": source_hashes,

        "output_sha256": {
            OUTPUT_CSV: sha256_file(csv_path),
            OUTPUT_RESULT: sha256_file(result_path),
        },
    }

    write_json(
        manifest_path,
        manifest,
    )

    if not validate_existing_outputs(
        output_dir,
        source_hashes,
        args.min_events,
    ):
        raise RuntimeError(
            "Final output integrity validation failed"
        )

    print("=" * 55)
    print("PATTERN 256 REGIME STABILITY GATE")
    print("=" * 55)

    print("Execution: PASS")
    print(f"Version: {VERSION}")
    print(f"Source cells: {len(records)}")
    print(f"Comparison rows: {len(output_rows)}")

    print(
        "Positive both periods:",
        positive,
    )

    print(
        "Negative both periods:",
        negative,
    )

    print(
        "Direction changed:",
        changed,
    )

    print(
        "Insufficient sample:",
        insufficient,
    )

    print("Research decision: HOLD")
    print("Statistical independence proven: False")
    print("Live trading approved: False")

    print("Input SHA256: VERIFIED")
    print("Output SHA256: VERIFIED")

    print("=" * 55)

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
