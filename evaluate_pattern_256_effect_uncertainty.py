#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Effect Size and Uncertainty Audit
Clean V001

Research-only diagnostic.

Source:
    data/reports/pattern_256_regime_stability_gate/
        regime_stability_comparison.csv
        regime_stability_result.json
        regime_stability_manifest.json

Output:
    data/reports/pattern_256_effect_uncertainty/
        effect_uncertainty_comparison.csv
        effect_uncertainty_result.json
        effect_uncertainty_manifest.json

Important:
    - Uses aggregated counts.
    - Wilson/Newcombe intervals are exploratory only.
    - Overlapping events may violate independence assumptions.
    - No statistical independence proof.
    - No profitability proof.
    - No automated trading.
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

SOURCE_RELATIVE = (
    "data/reports/"
    "pattern_256_regime_stability_gate"
)

OUTPUT_RELATIVE = (
    "data/reports/"
    "pattern_256_effect_uncertainty"
)

SOURCE_CSV = "regime_stability_comparison.csv"
SOURCE_RESULT = "regime_stability_result.json"
SOURCE_MANIFEST = "regime_stability_manifest.json"

OUTPUT_CSV = "effect_uncertainty_comparison.csv"
OUTPUT_RESULT = "effect_uncertainty_result.json"
OUTPUT_MANIFEST = "effect_uncertainty_manifest.json"

TIMEFRAMES = ("h1", "h4", "d1")
HORIZONS = (24, 72, 168)
REGIMES = ("UP", "DOWN", "SIDEWAYS")

SOURCE_COLUMNS = (
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

OUTPUT_COLUMNS = (
    "timeframe",
    "horizon",
    "regime",
    "stability_status",
    "train_only_events",
    "train_both_events",
    "train_uplift_pp",
    "test_only_events",
    "test_only_hits",
    "test_only_hit_rate_pct",
    "test_both_events",
    "test_both_hits",
    "test_both_hit_rate_pct",
    "test_uplift_pp",
    "test_uplift_lower_pp",
    "test_uplift_upper_pp",
    "sample_status",
    "interval_status",
    "research_priority",
    "limitations",
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


def read_json(path):
    with path.open("r", encoding="utf-8-sig") as handle:
        return json.load(handle)


def atomic_write_bytes(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)

    descriptor, temporary = tempfile.mkstemp(
        prefix=".tmp_",
        dir=str(path.parent),
    )

    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())

        os.replace(temporary, path)

    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def write_json(path, payload):
    encoded = (
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
        )
        + "\n"
    ).encode("utf-8")

    atomic_write_bytes(path, encoded)


def write_csv(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)

    descriptor, temporary = tempfile.mkstemp(
        prefix=".tmp_",
        dir=str(path.parent),
    )

    try:
        with os.fdopen(
            descriptor,
            "w",
            encoding="utf-8",
            newline="",
        ) as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=OUTPUT_COLUMNS,
            )

            writer.writeheader()
            writer.writerows(rows)

            handle.flush()
            os.fsync(handle.fileno())

        os.replace(temporary, path)

    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def parse_integer(value, label):
    text = str(value).strip()

    if not text.isdigit():
        raise ValueError(
            f"Invalid nonnegative integer: {label}={value}"
        )

    return int(text)


def parse_optional_float(value, label):
    if value is None:
        return None

    text = str(value).strip()

    if not text:
        return None

    number = float(text)

    if not math.isfinite(number):
        raise ValueError(
            f"Non-finite number: {label}"
        )

    return number


def parse_bool(value, label):
    text = str(value).strip().lower()

    if text == "true":
        return True

    if text == "false":
        return False

    raise ValueError(
        f"Invalid boolean: {label}={value}"
    )


def validate_hash(value, label):
    if not isinstance(value, str):
        raise ValueError(
            f"Missing SHA256: {label}"
        )

    value = value.strip().lower()

    if (
        len(value) != 64
        or any(
            character not in "0123456789abcdef"
            for character in value
        )
    ):
        raise ValueError(
            f"Invalid SHA256: {label}"
        )

    return value


def validate_source(source_dir):
    paths = {
        name: source_dir / name
        for name in (
            SOURCE_CSV,
            SOURCE_RESULT,
            SOURCE_MANIFEST,
        )
    }

    for name, path in paths.items():
        if not path.is_file():
            raise FileNotFoundError(
                f"Missing source: {name}: {path}"
            )

    result = read_json(paths[SOURCE_RESULT])
    manifest = read_json(paths[SOURCE_MANIFEST])

    if result.get("version") != "Clean V001":
        raise ValueError(
            "Unexpected source result version"
        )

    if manifest.get("version") != "Clean V001":
        raise ValueError(
            "Unexpected source manifest version"
        )

    if result.get("execution_status") != "PASS":
        raise ValueError(
            "Source execution is not PASS"
        )

    if result.get("research_decision") != "HOLD":
        raise ValueError(
            "Source research decision is not HOLD"
        )

    if result.get("approved_for_live_trading") is not False:
        raise ValueError(
            "Source trading safety check failed"
        )

    if result.get("statistical_independence_proven") is not False:
        raise ValueError(
            "Unexpected source independence proof"
        )

    hashes = manifest.get("output_sha256")

    if not isinstance(hashes, dict):
        raise ValueError(
            "Source output SHA256 manifest missing"
        )

    for name in (SOURCE_CSV, SOURCE_RESULT):
        expected = validate_hash(
            hashes.get(name),
            name,
        )

        actual = sha256_file(paths[name])

        if actual != expected:
            raise ValueError(
                f"Source SHA256 mismatch: {name}"
            )

    source_hashes = {
        name: sha256_file(path)
        for name, path in paths.items()
    }

    return paths, result, source_hashes


def hit_rate(hits, events):
    if events == 0:
        return None

    return hits / events


def wilson_interval(hits, events, z):
    """
    Wilson score interval for a single proportion.

    WARNING:
    Assumes independent Bernoulli observations.
    Used here only as a descriptive sensitivity diagnostic.
    """

    if events == 0:
        return None

    proportion = hits / events
    z_squared = z * z

    denominator = 1.0 + z_squared / events

    center = (
        proportion
        + z_squared / (2.0 * events)
    ) / denominator

    radius = (
        z
        * math.sqrt(
            (
                proportion * (1.0 - proportion)
                + z_squared / (4.0 * events)
            )
            / events
        )
        / denominator
    )

    lower = max(0.0, center - radius)
    upper = min(1.0, center + radius)

    return lower, upper


def difference_interval(
    only_hits,
    only_events,
    both_hits,
    both_events,
    z,
):
    """
    Conservative difference interval constructed from
    separate Wilson intervals.

    Not a cluster-robust confidence interval.
    Not a causal effect estimate.
    """

    only_interval = wilson_interval(
        only_hits,
        only_events,
        z,
    )

    both_interval = wilson_interval(
        both_hits,
        both_events,
        z,
    )

    if only_interval is None or both_interval is None:
        return None

    lower = (
        both_interval[0]
        - only_interval[1]
    )

    upper = (
        both_interval[1]
        - only_interval[0]
    )

    return lower * 100.0, upper * 100.0


def format_float(value):
    if value is None:
        return ""

    return f"{value:.8f}"


def direction(value):
    if value is None:
        return "MISSING"

    if abs(value) < 1e-10:
        return "EQUAL"

    if value > 0:
        return "BOTH_HIGHER"

    return "ONLY_HIGHER"


def read_source_rows(path):
    records = {}
    seen_count = 0

    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as handle:
        reader = csv.DictReader(handle)

        if reader.fieldnames != list(SOURCE_COLUMNS):
            raise ValueError(
                "Unexpected source CSV schema"
            )

        for line_number, raw in enumerate(
            reader,
            start=2,
        ):
            seen_count += 1

            timeframe = raw["timeframe"].strip()
            regime = raw["regime"].strip()
            horizon = parse_integer(
                raw["horizon"],
                f"line {line_number} horizon",
            )

            if timeframe not in TIMEFRAMES:
                raise ValueError(
                    f"Invalid timeframe at line {line_number}"
                )

            if regime not in REGIMES:
                raise ValueError(
                    f"Invalid regime at line {line_number}"
                )

            if horizon not in HORIZONS:
                raise ValueError(
                    f"Invalid horizon at line {line_number}"
                )

            key = (timeframe, horizon, regime)

            if key in records:
                raise ValueError(
                    f"Duplicate source cell: {key}"
                )

            record = {
                "timeframe": timeframe,
                "horizon": horizon,
                "regime": regime,
                "stability_status": (
                    raw["stability_status"].strip()
                ),
            }

            for split in ("train", "test"):
                for group in ("only", "both"):
                    for metric in ("events", "hits"):
                        field = (
                            f"{split}_{group}_{metric}"
                        )

                        value = raw[field].strip()

                        record[field] = (
                            None
                            if value == ""
                            else parse_integer(
                                value,
                                f"line {line_number} {field}",
                            )
                        )

                    events = record[
                        f"{split}_{group}_events"
                    ]

                    hits = record[
                        f"{split}_{group}_hits"
                    ]

                    if (events is None) != (hits is None):
                        raise ValueError(
                            f"Incomplete counts: line {line_number}"
                        )

                    if (
                        events is not None
                        and hits > events
                    ):
                        raise ValueError(
                            f"Hits exceed events: line {line_number}"
                        )

                supplied_uplift = parse_optional_float(
                    raw[f"{split}_uplift_pp"],
                    f"line {line_number} {split}_uplift_pp",
                )

                only_events = record[
                    f"{split}_only_events"
                ]

                both_events = record[
                    f"{split}_both_events"
                ]

                if (
                    only_events is None
                    or both_events is None
                    or only_events == 0
                    or both_events == 0
                ):
                    calculated = None

                else:
                    calculated = 100.0 * (
                        record[f"{split}_both_hits"]
                        / both_events
                        - record[f"{split}_only_hits"]
                        / only_events
                    )

                if (
                    supplied_uplift is None
                ) != (
                    calculated is None
                ):
                    raise ValueError(
                        f"Uplift missingness mismatch: "
                        f"line {line_number}"
                    )

                if (
                    supplied_uplift is not None
                    and abs(
                        supplied_uplift - calculated
                    ) > 0.00001
                ):
                    raise ValueError(
                        f"Uplift mismatch: line {line_number}"
                    )

                record[f"{split}_uplift_pp"] = calculated

                supplied_direction = (
                    raw[f"{split}_direction"].strip()
                )

                if supplied_direction != direction(calculated):
                    raise ValueError(
                        f"Direction mismatch: line {line_number}"
                    )

                supplied_sample = parse_bool(
                    raw[f"{split}_sample_ok"],
                    f"line {line_number} sample",
                )

                record[f"{split}_sample_ok"] = supplied_sample

            records[key] = record

    if seen_count != 27:
        raise ValueError(
            f"Expected 27 source cells, got {seen_count}"
        )

    expected_keys = {
        (timeframe, horizon, regime)
        for timeframe in TIMEFRAMES
        for horizon in HORIZONS
        for regime in REGIMES
    }

    if set(records) != expected_keys:
        raise ValueError(
            "Missing or unexpected source comparison cells"
        )

    return records


def classify_row(record, minimum_events, z):
    test_only_events = record["test_only_events"]
    test_only_hits = record["test_only_hits"]

    test_both_events = record["test_both_events"]
    test_both_hits = record["test_both_hits"]

    train_only_events = record["train_only_events"]
    train_both_events = record["train_both_events"]

    test_uplift = record["test_uplift_pp"]

    only_rate = None
    both_rate = None
    interval = None

    if (
        test_only_events is not None
        and test_both_events is not None
    ):
        only_rate = hit_rate(
            test_only_hits,
            test_only_events,
        )

        both_rate = hit_rate(
            test_both_hits,
            test_both_events,
        )

        interval = difference_interval(
            test_only_hits,
            test_only_events,
            test_both_hits,
            test_both_events,
            z,
        )

    if (
        test_only_events is None
        or test_both_events is None
        or test_only_events == 0
        or test_both_events == 0
    ):
        sample_status = "MISSING_GROUP"

    elif (
        test_only_events < minimum_events
        or test_both_events < minimum_events
    ):
        sample_status = "SMALL_SAMPLE"

    else:
        sample_status = "MINIMUM_MET"

    if interval is None:
        interval_status = "NOT_AVAILABLE"

    elif interval[0] > 0:
        interval_status = "EXPLORATORY_POSITIVE"

    elif interval[1] < 0:
        interval_status = "EXPLORATORY_NEGATIVE"

    else:
        interval_status = "CROSSES_ZERO"

    stability = record["stability_status"]

    if sample_status != "MINIMUM_MET":
        priority = "INSUFFICIENT_DATA"

    elif stability == "DIRECTION_CHANGED":
        priority = "INVESTIGATE_REVERSAL"

    elif stability != "POSITIVE_BOTH_PERIODS":
        priority = "REVIEW_OTHER"

    elif interval_status == "EXPLORATORY_POSITIVE":
        priority = "EVENT_LEVEL_VALIDATION_CANDIDATE"

    else:
        priority = "UNCERTAINTY_REMAINS"

    limitations = (
        "AGGREGATED_COUNTS;"
        "POTENTIAL_EVENT_OVERLAP;"
        "NON_INDEPENDENT_OBSERVATIONS;"
        "INTERVAL_EXPLORATORY_ONLY"
    )

    return {
        "timeframe": record["timeframe"],
        "horizon": record["horizon"],
        "regime": record["regime"],
        "stability_status": stability,

        "train_only_events": (
            ""
            if train_only_events is None
            else train_only_events
        ),

        "train_both_events": (
            ""
            if train_both_events is None
            else train_both_events
        ),

        "train_uplift_pp": format_float(
            record["train_uplift_pp"]
        ),

        "test_only_events": (
            ""
            if test_only_events is None
            else test_only_events
        ),

        "test_only_hits": (
            ""
            if test_only_hits is None
            else test_only_hits
        ),

        "test_only_hit_rate_pct": format_float(
            None
            if only_rate is None
            else only_rate * 100.0
        ),

        "test_both_events": (
            ""
            if test_both_events is None
            else test_both_events
        ),

        "test_both_hits": (
            ""
            if test_both_hits is None
            else test_both_hits
        ),

        "test_both_hit_rate_pct": format_float(
            None
            if both_rate is None
            else both_rate * 100.0
        ),

        "test_uplift_pp": format_float(
            test_uplift
        ),

        "test_uplift_lower_pp": format_float(
            None
            if interval is None
            else interval[0]
        ),

        "test_uplift_upper_pp": format_float(
            None
            if interval is None
            else interval[1]
        ),

        "sample_status": sample_status,
        "interval_status": interval_status,
        "research_priority": priority,
        "limitations": limitations,
    }


def validate_existing(
    output_dir,
    input_hashes,
    minimum_events,
    z,
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

        if manifest.get("version") != VERSION:
            return False

        if result.get("execution_status") != "PASS":
            return False

        if result.get("research_decision") != "HOLD":
            return False

        if result.get("approved_for_live_trading") is not False:
            return False

        if result.get("statistical_independence_proven") is not False:
            return False

        if result.get("independent_oos_proven") is not False:
            return False

        if result.get("profitability_proven") is not False:
            return False

        config = manifest.get("config", {})

        if config.get("min_events") != minimum_events:
            return False

        if config.get("z") != z:
            return False

        if manifest.get("input_sha256") != input_hashes:
            return False

        output_hashes = manifest.get(
            "output_sha256",
            {},
        )

        for name in (OUTPUT_CSV, OUTPUT_RESULT):
            expected = validate_hash(
                output_hashes.get(name),
                name,
            )

            if sha256_file(output_dir / name) != expected:
                return False

        with csv_path.open(
            "r",
            encoding="utf-8-sig",
            newline="",
        ) as handle:
            reader = csv.DictReader(handle)

            if reader.fieldnames != list(OUTPUT_COLUMNS):
                return False

            rows = list(reader)

        if len(rows) != 27:
            return False

        if result.get("comparison_rows") != len(rows):
            return False

        statuses = Counter(
            row["research_priority"]
            for row in rows
        )

        if result.get("priority_counts") != dict(
            sorted(statuses.items())
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
        "--z",
        type=float,
        default=1.959963984540054,
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

    if (
        not math.isfinite(args.z)
        or args.z <= 0
    ):
        raise ValueError(
            "--z must be a finite positive number"
        )

    root = Path(args.root).expanduser().resolve()

    source_dir = root / SOURCE_RELATIVE
    output_dir = root / OUTPUT_RELATIVE

    if not source_dir.is_dir():
        raise FileNotFoundError(
            f"Missing source directory: {source_dir}"
        )

    paths, source_result, input_hashes = validate_source(
        source_dir
    )

    if (
        not args.force
        and validate_existing(
            output_dir,
            input_hashes,
            args.min_events,
            args.z,
        )
    ):
        print("PASS: existing result integrity verified")
        print("SKIP: recomputation not required")
        return 0

    records = read_source_rows(
        paths[SOURCE_CSV]
    )

    if source_result.get("comparison_rows") != len(records):
        raise ValueError(
            "Source comparison count mismatch"
        )

    output_rows = []

    for timeframe in TIMEFRAMES:
        for horizon in HORIZONS:
            for regime in REGIMES:
                key = (
                    timeframe,
                    horizon,
                    regime,
                )

                record = records[key]

                output_rows.append(
                    classify_row(
                        record,
                        args.min_events,
                        args.z,
                    )
                )

    if len(output_rows) != 27:
        raise ValueError(
            "Unexpected output row count"
        )

    priorities = Counter(
        row["research_priority"]
        for row in output_rows
    )

    intervals = Counter(
        row["interval_status"]
        for row in output_rows
    )

    samples = Counter(
        row["sample_status"]
        for row in output_rows
    )

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
        "comparison_rows": len(output_rows),

        "config": {
            "min_events": args.min_events,
            "z": args.z,
            "interval_method": (
                "Wilson marginal intervals / "
                "conservative difference bounds"
            ),
        },

        "priority_counts": dict(
            sorted(priorities.items())
        ),

        "interval_status_counts": dict(
            sorted(intervals.items())
        ),

        "sample_status_counts": dict(
            sorted(samples.items())
        ),

        "limitations": [
            "Source data are aggregated observations.",
            "Overlapping events can invalidate independent "
            "Bernoulli uncertainty assumptions.",
            "Intervals are exploratory diagnostics only.",
            "These intervals are not cluster-robust.",
            "No multiple-testing-adjusted statistical claim.",
            "TRAIN and TEST were defined in prior research.",
            "No independent prospective OOS proof.",
            "No causal effect proof.",
            "No trading costs or execution simulation.",
            "No profitability proof.",
            "No live trading approval.",
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
            "z": args.z,
        },

        "input_sha256": input_hashes,

        "output_sha256": {
            OUTPUT_CSV: sha256_file(csv_path),
            OUTPUT_RESULT: sha256_file(result_path),
        },
    }

    write_json(
        manifest_path,
        manifest,
    )

    if not validate_existing(
        output_dir,
        input_hashes,
        args.min_events,
        args.z,
    ):
        raise RuntimeError(
            "Final output integrity validation failed"
        )

    print("=" * 60)
    print("PATTERN 256 EFFECT SIZE AND UNCERTAINTY AUDIT")
    print("=" * 60)

    print(f"Execution: PASS")
    print(f"Version: {VERSION}")
    print(f"Source rows: {source_result['source_rows']}")
    print(f"Comparison rows: {len(output_rows)}")

    print()
    print("RESEARCH PRIORITIES")

    for name, count in sorted(priorities.items()):
        print(f"  {name}: {count}")

    print()
    print("EXPLORATORY INTERVALS")

    for name, count in sorted(intervals.items()):
        print(f"  {name}: {count}")

    print()
    print("SAMPLE STATUS")

    for name, count in sorted(samples.items()):
        print(f"  {name}: {count}")

    print()
    print("Input SHA256: VERIFIED")
    print("Output SHA256: VERIFIED")
    print("Research decision: HOLD")
    print("Statistical independence proven: False")
    print("Profitability proven: False")
    print("Live trading approved: False")
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
