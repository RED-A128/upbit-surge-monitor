#!/usr/bin/env python3
"""
Pattern 256 Regime Robustness Evaluation

Version: Clean V001

Research only.
No automated trading.

Purpose:
- Compare PATTERN_256_ONLY against BOTH.
- Compare TRAIN and TEST performance.
- Evaluate UP, DOWN, SIDEWAYS regimes.
- Calculate Wilson 95% confidence intervals.
- Identify small samples and missing comparison groups.
- Preserve source data.
- Verify SHA256 and support safe resume.

Important:
- This is descriptive robustness research.
- Overlapping events may not be independent.
- Confidence intervals do not prove trading profitability.
"""

import argparse
import csv
import hashlib
import json
import math
import os
import sys

from collections import defaultdict
from pathlib import Path


VERSION = "Clean V001"

SOURCE_DIR = Path(
    "data/reports/pattern_256_market_regime"
)

OUTPUT_DIR = Path(
    "data/reports/pattern_256_regime_robustness"
)

SOURCE_CSV = "market_regime_comparison.csv"
SOURCE_RESULT = "market_regime_result.json"
SOURCE_MANIFEST = "market_regime_manifest.json"

OUTPUT_CSV = "regime_robustness_comparison.csv"
OUTPUT_JSON = "regime_robustness_result.json"
OUTPUT_MANIFEST = "regime_robustness_manifest.json"

SPLITS = ("TRAIN", "TEST")
TIMEFRAMES = ("h1", "h4", "d1")
HORIZONS = (24, 72, 168)
REGIMES = ("UP", "DOWN", "SIDEWAYS")

GROUPS = (
    "PATTERN_256_ONLY",
    "BOTH",
)

FIELDS = [
    "split",
    "timeframe",
    "horizon",
    "regime",
    "only_events",
    "only_hits",
    "only_hit_rate_pct",
    "only_ci_low_pct",
    "only_ci_high_pct",
    "both_events",
    "both_hits",
    "both_hit_rate_pct",
    "both_ci_low_pct",
    "both_ci_high_pct",
    "uplift_pp",
    "sample_status",
    "comparison_status",
]


def sha256(path):
    h = hashlib.sha256()

    with path.open("rb") as f:
        for block in iter(
            lambda: f.read(4 * 1024 * 1024),
            b"",
        ):
            h.update(block)

    return h.hexdigest()


def atomic_json(path, data):
    temp = path.with_suffix(
        path.suffix + ".tmp"
    )

    try:
        with temp.open(
            "w",
            encoding="utf-8",
            newline="\n",
        ) as f:
            json.dump(
                data,
                f,
                indent=2,
                ensure_ascii=False,
                sort_keys=True,
            )

            f.write("\n")

        os.replace(temp, path)

    finally:
        if temp.exists():
            temp.unlink()


def atomic_csv(path, rows):
    temp = path.with_suffix(
        path.suffix + ".tmp"
    )

    try:
        with temp.open(
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

        os.replace(temp, path)

    finally:
        if temp.exists():
            temp.unlink()


def wilson_interval(hits, total):
    """
    Wilson score interval, nominal 95%.

    Assumes independent Bernoulli observations.
    This assumption may not hold for overlapping
    market signals.

    Therefore this interval is descriptive only.
    """

    if total <= 0:
        return None, None

    z = 1.959963984540054

    p = hits / total

    z2 = z * z

    denominator = 1 + z2 / total

    center = (
        p + z2 / (2 * total)
    ) / denominator

    margin = (
        z
        * math.sqrt(
            p * (1 - p) / total
            + z2 / (4 * total * total)
        )
        / denominator
    )

    return (
        max(0.0, center - margin),
        min(1.0, center + margin),
    )


def percent(value):
    if value is None:
        return ""

    return round(
        value * 100,
        6,
    )


def validate_source(
    source_csv,
    source_result,
    source_manifest,
):
    """
    Verify the previous research stage.

    The source manifest must identify the exact
    CSV and result JSON hashes.
    """

    required = (
        source_csv,
        source_result,
        source_manifest,
    )

    for path in required:
        if not path.is_file():
            raise FileNotFoundError(
                "Missing source file: "
                + str(path)
            )

    result = json.loads(
        source_result.read_text(
            encoding="utf-8"
        )
    )

    manifest = json.loads(
        source_manifest.read_text(
            encoding="utf-8"
        )
    )

    if result.get("execution_status") != "PASS":
        raise ValueError(
            "Source analysis is not PASS"
        )

    if result.get("version") != "Clean V002":
        raise ValueError(
            "Unexpected source version: "
            + str(result.get("version"))
        )

    if (
        result.get("approved_for_live_trading")
        is not False
    ):
        raise ValueError(
            "Unsafe source approval state"
        )

    if (
        manifest.get("version")
        != result.get("version")
    ):
        raise ValueError(
            "Source version mismatch"
        )

    if (
        manifest.get("source_sha256")
        != result.get("source_sha256")
    ):
        raise ValueError(
            "Source backtest hash mismatch"
        )

    if (
        manifest.get("config")
        != result.get("config")
    ):
        raise ValueError(
            "Source configuration mismatch"
        )

    output_hashes = manifest.get(
        "output_sha256"
    )

    if not isinstance(
        output_hashes,
        dict,
    ):
        raise ValueError(
            "Missing source output hashes"
        )

    expected = {
        source_csv.name,
        source_result.name,
    }

    if set(output_hashes) != expected:
        raise ValueError(
            "Unexpected source output manifest"
        )

    for path in (
        source_csv,
        source_result,
    ):
        actual = sha256(path)

        expected_hash = output_hashes.get(
            path.name
        )

        if actual != expected_hash:
            raise ValueError(
                "Source SHA256 mismatch: "
                + str(path)
            )

    return result, manifest


def load_rows(path):
    """
    Load and validate all aggregate groups.

    Duplicate aggregate keys are rejected.
    """

    records = {}

    total_events = 0

    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as f:
        reader = csv.DictReader(f)

        required = {
            "split",
            "timeframe",
            "horizon",
            "regime",
            "classification",
            "events",
            "hits",
            "hit_rate_pct",
        }

        existing = set(
            reader.fieldnames or []
        )

        if not required.issubset(existing):
            raise ValueError(
                "Missing CSV columns: "
                + str(
                    sorted(
                        required - existing
                    )
                )
            )

        for line, row in enumerate(
            reader,
            2,
        ):
            split = row["split"].strip()
            tf = row["timeframe"].strip()
            regime = row["regime"].strip()
            group = row["classification"].strip()

            try:
                horizon = int(
                    row["horizon"]
                )

                events = int(
                    row["events"]
                )

                hits = int(
                    row["hits"]
                )

                reported_rate = float(
                    row["hit_rate_pct"]
                )

            except (
                ValueError,
                TypeError,
            ) as e:
                raise ValueError(
                    f"Invalid numeric value at line {line}"
                ) from e

            if split not in SPLITS:
                raise ValueError(
                    f"Invalid split at line {line}"
                )

            if tf not in TIMEFRAMES:
                raise ValueError(
                    f"Invalid timeframe at line {line}"
                )

            if horizon not in HORIZONS:
                raise ValueError(
                    f"Invalid horizon at line {line}"
                )

            if regime not in REGIMES:
                raise ValueError(
                    f"Invalid regime at line {line}"
                )

            if group not in GROUPS:
                raise ValueError(
                    f"Invalid group at line {line}"
                )

            if (
                events <= 0
                or hits < 0
                or hits > events
            ):
                raise ValueError(
                    f"Invalid event counts at line {line}"
                )

            if not math.isfinite(reported_rate):
                raise ValueError(
                    f"Non-finite hit rate at line {line}"
                )

            actual_rate = (
                100 * hits / events
            )

            if abs(
                actual_rate - reported_rate
            ) > 0.0000011:
                raise ValueError(
                    f"Hit rate mismatch at line {line}"
                )

            key = (
                split,
                tf,
                horizon,
                regime,
                group,
            )

            if key in records:
                raise ValueError(
                    "Duplicate aggregate group: "
                    + str(key)
                )

            records[key] = {
                "events": events,
                "hits": hits,
            }

            total_events += events

    if not records:
        raise ValueError(
            "No source comparison rows"
        )

    return records, total_events


def make_group_values(record):
    if record is None:
        return {
            "events": 0,
            "hits": 0,
            "rate": None,
            "ci_low": None,
            "ci_high": None,
        }

    n = record["events"]
    hits = record["hits"]

    low, high = wilson_interval(
        hits,
        n,
    )

    return {
        "events": n,
        "hits": hits,
        "rate": hits / n,
        "ci_low": low,
        "ci_high": high,
    }


def build_comparison(
    records,
    min_events,
):
    rows = []

    status_counts = defaultdict(int)

    paired_count = 0

    missing_count = 0

    small_count = 0

    positive_count = 0
    negative_count = 0
    equal_count = 0

    for split in SPLITS:
        for tf in TIMEFRAMES:
            for horizon in HORIZONS:
                for regime in REGIMES:

                    base_key = (
                        split,
                        tf,
                        horizon,
                        regime,
                    )

                    only_record = records.get(
                        base_key
                        + ("PATTERN_256_ONLY",)
                    )

                    both_record = records.get(
                        base_key
                        + ("BOTH",)
                    )

                    only = make_group_values(
                        only_record
                    )

                    both = make_group_values(
                        both_record
                    )

                    if (
                        only_record is None
                        and both_record is None
                    ):
                        # No observed events in either
                        # classification for this cell.
                        continue

                    if (
                        only_record is None
                        or both_record is None
                    ):
                        sample_status = (
                            "MISSING_COMPARISON_GROUP"
                        )

                        comparison_status = (
                            "NOT_COMPARABLE"
                        )

                        uplift = None

                        missing_count += 1

                    else:
                        paired_count += 1

                        uplift = (
                            both["rate"]
                            - only["rate"]
                        ) * 100

                        if (
                            only["events"] < min_events
                            or both["events"] < min_events
                        ):
                            sample_status = (
                                "SMALL_SAMPLE"
                            )

                            small_count += 1

                        else:
                            sample_status = (
                                "MINIMUM_SAMPLE_MET"
                            )

                        if uplift > 0:
                            comparison_status = (
                                "BOTH_HIGHER"
                            )

                            positive_count += 1

                        elif uplift < 0:
                            comparison_status = (
                                "ONLY_HIGHER"
                            )

                            negative_count += 1

                        else:
                            comparison_status = (
                                "EQUAL"
                            )

                            equal_count += 1

                    status_counts[
                        sample_status
                    ] += 1

                    rows.append(
                        {
                            "split": split,
                            "timeframe": tf,
                            "horizon": horizon,
                            "regime": regime,

                            "only_events": (
                                only["events"]
                            ),

                            "only_hits": (
                                only["hits"]
                            ),

                            "only_hit_rate_pct": percent(
                                only["rate"]
                            ),

                            "only_ci_low_pct": percent(
                                only["ci_low"]
                            ),

                            "only_ci_high_pct": percent(
                                only["ci_high"]
                            ),

                            "both_events": (
                                both["events"]
                            ),

                            "both_hits": (
                                both["hits"]
                            ),

                            "both_hit_rate_pct": percent(
                                both["rate"]
                            ),

                            "both_ci_low_pct": percent(
                                both["ci_low"]
                            ),

                            "both_ci_high_pct": percent(
                                both["ci_high"]
                            ),

                            "uplift_pp": (
                                round(uplift, 6)
                                if uplift is not None
                                else ""
                            ),

                            "sample_status": (
                                sample_status
                            ),

                            "comparison_status": (
                                comparison_status
                            ),
                        }
                    )

    summary = {
        "comparison_cells": len(rows),
        "paired_cells": paired_count,
        "missing_group_cells": missing_count,
        "small_sample_cells": small_count,
        "both_higher_cells": positive_count,
        "only_higher_cells": negative_count,
        "equal_cells": equal_count,
        "sample_status_counts": dict(
            status_counts
        ),
    }

    return rows, summary


def build_stability(rows, min_events):
    """
    Compare directional uplift between TRAIN and TEST.

    A stable direction is not proof of statistical
    independence or profitability.
    """

    indexed = {}

    for row in rows:
        key = (
            row["split"],
            row["timeframe"],
            row["horizon"],
            row["regime"],
        )

        indexed[key] = row

    results = []

    counts = defaultdict(int)

    for tf in TIMEFRAMES:
        for horizon in HORIZONS:
            for regime in REGIMES:

                train = indexed.get(
                    (
                        "TRAIN",
                        tf,
                        horizon,
                        regime,
                    )
                )

                test = indexed.get(
                    (
                        "TEST",
                        tf,
                        horizon,
                        regime,
                    )
                )

                if train is None or test is None:
                    status = (
                        "MISSING_TRAIN_OR_TEST"
                    )

                elif (
                    train["sample_status"]
                    != "MINIMUM_SAMPLE_MET"
                    or test["sample_status"]
                    != "MINIMUM_SAMPLE_MET"
                ):
                    status = (
                        "INSUFFICIENT_SAMPLE"
                    )

                else:
                    train_uplift = float(
                        train["uplift_pp"]
                    )

                    test_uplift = float(
                        test["uplift_pp"]
                    )

                    if (
                        train_uplift > 0
                        and test_uplift > 0
                    ):
                        status = (
                            "POSITIVE_BOTH_PERIODS"
                        )

                    elif (
                        train_uplift < 0
                        and test_uplift < 0
                    ):
                        status = (
                            "NEGATIVE_BOTH_PERIODS"
                        )

                    elif (
                        train_uplift == 0
                        and test_uplift == 0
                    ):
                        status = (
                            "EQUAL_BOTH_PERIODS"
                        )

                    else:
                        status = (
                            "DIRECTION_CHANGED"
                        )

                counts[status] += 1

                results.append(
                    {
                        "timeframe": tf,
                        "horizon": horizon,
                        "regime": regime,
                        "status": status,
                    }
                )

    return {
        "cells": results,
        "counts": dict(counts),
    }


def verify_existing(
    result_path,
    csv_path,
    manifest_path,
    input_hashes,
    config,
):
    paths = (
        result_path,
        csv_path,
        manifest_path,
    )

    if not all(
        path.is_file()
        for path in paths
    ):
        return False

    try:
        manifest = json.loads(
            manifest_path.read_text(
                encoding="utf-8"
            )
        )

        result = json.loads(
            result_path.read_text(
                encoding="utf-8"
            )
        )

        if manifest.get("version") != VERSION:
            return False

        if result.get("version") != VERSION:
            return False

        if (
            result.get("execution_status")
            != "PASS"
        ):
            return False

        if (
            result.get("approved_for_live_trading")
            is not False
        ):
            return False

        if (
            manifest.get("input_sha256")
            != input_hashes
        ):
            return False

        if (
            manifest.get("config")
            != config
        ):
            return False

        if result.get("config") != config:
            return False

        hashes = manifest.get(
            "output_sha256"
        )

        if not isinstance(
            hashes,
            dict,
        ):
            return False

        if set(hashes) != {
            OUTPUT_CSV,
            OUTPUT_JSON,
        }:
            return False

        for name, digest in hashes.items():
            path = result_path.parent / name

            if not path.is_file():
                return False

            if sha256(path) != digest:
                return False

        with csv_path.open(
            "r",
            encoding="utf-8-sig",
            newline="",
        ) as f:
            reader = csv.DictReader(f)

            if reader.fieldnames != FIELDS:
                return False

            row_count = sum(
                1 for _ in reader
            )

        if (
            row_count
            != result.get("comparison_cells")
        ):
            return False

        return True

    except (
        ValueError,
        KeyError,
        TypeError,
        OSError,
    ):
        return False


def run(args):
    root = args.root.resolve()

    source_dir = (
        root / SOURCE_DIR
    ).resolve()

    output_dir = (
        root / OUTPUT_DIR
    ).resolve()

    source_csv = (
        source_dir / SOURCE_CSV
    )

    source_result = (
        source_dir / SOURCE_RESULT
    )

    source_manifest = (
        source_dir / SOURCE_MANIFEST
    )

    if args.min_events < 1:
        raise ValueError(
            "min-events must be >= 1"
        )

    if (
        output_dir == source_dir
        or output_dir in source_dir.parents
        or source_dir in output_dir.parents
    ):
        raise ValueError(
            "Unsafe output directory"
        )

    print(
        "Pattern 256 Regime Robustness "
        + VERSION,
        flush=True,
    )

    source_info, _ = validate_source(
        source_csv,
        source_result,
        source_manifest,
    )

    input_paths = {
        SOURCE_CSV: source_csv,
        SOURCE_RESULT: source_result,
        SOURCE_MANIFEST: source_manifest,
    }

    input_hashes = {
        name: sha256(path)
        for name, path in input_paths.items()
    }

    config = {
        "version": VERSION,
        "min_events": args.min_events,
        "source_version": (
            source_info["version"]
        ),
        "source_config": (
            source_info["config"]
        ),
    }

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    result_path = (
        output_dir / OUTPUT_JSON
    )

    csv_path = (
        output_dir / OUTPUT_CSV
    )

    manifest_path = (
        output_dir / OUTPUT_MANIFEST
    )

    if not args.force:
        if verify_existing(
            result_path,
            csv_path,
            manifest_path,
            input_hashes,
            config,
        ):
            print(
                "Verified existing output: SKIP",
                flush=True,
            )

            return

    records, total_events = load_rows(
        source_csv
    )

    if (
        len(records)
        != source_info["comparison_rows"]
    ):
        raise ValueError(
            "Source comparison row count mismatch"
        )

    rows, summary = build_comparison(
        records,
        args.min_events,
    )

    if not rows:
        raise ValueError(
            "No comparable regime cells"
        )

    stability = build_stability(
        rows,
        args.min_events,
    )

    # Confirm all inputs remained unchanged.
    final_hashes = {
        name: sha256(path)
        for name, path in input_paths.items()
    }

    if final_hashes != input_hashes:
        raise RuntimeError(
            "Input files changed during analysis"
        )

    result = {
        "version": VERSION,
        "execution_status": "PASS",
        "research_decision": "HOLD",
        "approved_for_live_trading": False,
        "independent_oos_proven": False,
        "statistical_independence_proven": False,
        "profitability_proven": False,
        "source_version": (
            source_info["version"]
        ),
        "source_rows": (
            source_info["source_rows"]
        ),
        "source_comparison_rows": (
            source_info["comparison_rows"]
        ),
        "source_excluded": (
            source_info["excluded"]
        ),
        "included_aggregate_events": (
            total_events
        ),
        "comparison_cells": (
            summary["comparison_cells"]
        ),
        "summary": summary,
        "train_test_stability": stability,
        "config": config,
        "limitations": [
            "Market regime is per-market trailing trend.",
            "Aggregated data cannot remove overlapping events.",
            "Wilson intervals assume independent observations.",
            "No event-level statistical independence proof.",
            "No causal proof of accumulation improvement.",
            "No trading costs or execution profitability proof.",
            "Missing and small groups cannot establish robustness.",
            "No approval for live trading.",
        ],
    }

    atomic_csv(
        csv_path,
        rows,
    )

    atomic_json(
        result_path,
        result,
    )

    manifest = {
        "version": VERSION,
        "config": config,
        "input_sha256": input_hashes,
        "output_sha256": {
            OUTPUT_CSV: sha256(
                csv_path
            ),
            OUTPUT_JSON: sha256(
                result_path
            ),
        },
    }

    atomic_json(
        manifest_path,
        manifest,
    )

    if not verify_existing(
        result_path,
        csv_path,
        manifest_path,
        input_hashes,
        config,
    ):
        raise RuntimeError(
            "Final integrity verification failed"
        )

    print(
        json.dumps(
            {
                "execution_status": "PASS",
                "research_decision": "HOLD",
                "source_rows": (
                    source_info["source_rows"]
                ),
                "included_aggregate_events": (
                    total_events
                ),
                "comparison_cells": (
                    summary["comparison_cells"]
                ),
                "paired_cells": (
                    summary["paired_cells"]
                ),
                "small_sample_cells": (
                    summary["small_sample_cells"]
                ),
                "missing_group_cells": (
                    summary["missing_group_cells"]
                ),
                "both_higher_cells": (
                    summary["both_higher_cells"]
                ),
                "only_higher_cells": (
                    summary["only_higher_cells"]
                ),
                "train_test_stability": (
                    stability["counts"]
                ),
                "sha256_verified": True,
                "approved_for_live_trading": False,
            },
            indent=2,
        ),
        flush=True,
    )


def main():
    parser = argparse.ArgumentParser(
        description=__doc__
    )

    parser.add_argument(
        "--root",
        type=Path,
        default=Path("."),
    )

    parser.add_argument(
        "--min-events",
        type=int,
        default=100,
    )

    parser.add_argument(
        "--force",
        action="store_true",
    )

    try:
        run(
            parser.parse_args()
        )

    except Exception as e:
        print(
            "FATAL:",
            type(e).__name__,
            str(e),
            file=sys.stderr,
        )

        sys.exit(1)


if __name__ == "__main__":
    main()
