#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Regime Robustness Audit
Version: Clean V002

Research only.
No automated trading.

Purpose:
- Validate the previous market-regime research outputs.
- Compare PATTERN_256_ONLY against BOTH.
- Compare TRAIN and TEST performance.
- Evaluate UP, DOWN and SIDEWAYS market regimes.
- Calculate descriptive Wilson 95% confidence intervals.
- Identify small samples and missing comparison groups.
- Evaluate TRAIN/TEST directional stability.
- Preserve all original source data.
- Verify SHA256 hashes.
- Support safe resume using verified existing outputs.

Important:
- This is descriptive robustness research.
- Overlapping events may not be statistically independent.
- Wilson intervals do not prove trading profitability.
- No live trading approval is granted.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import sys
import tempfile

from collections import defaultdict
from pathlib import Path


VERSION = "Clean V002"

SOURCE_VERSION = "Clean V002"

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

SPLITS = (
    "TRAIN",
    "TEST",
)

TIMEFRAMES = (
    "h1",
    "h4",
    "d1",
)

HORIZONS = (
    24,
    72,
    168,
)

REGIMES = (
    "UP",
    "DOWN",
    "SIDEWAYS",
)

GROUPS = (
    "PATTERN_256_ONLY",
    "BOTH",
)

SOURCE_FIELDS = [
    "split",
    "timeframe",
    "horizon",
    "regime",
    "classification",
    "events",
    "hits",
    "hit_rate_pct",
]

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


# ============================================================
# File utilities
# ============================================================

def sha256(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        for block in iter(
            lambda: handle.read(4 * 1024 * 1024),
            b"",
        ):
            digest.update(block)

    return digest.hexdigest()


def atomic_write(
    path: Path,
    content: str,
) -> None:

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    fd, temp_name = tempfile.mkstemp(
        prefix="." + path.name + ".",
        suffix=".tmp",
        dir=str(path.parent),
    )

    try:
        with os.fdopen(
            fd,
            "w",
            encoding="utf-8",
            newline="",
        ) as handle:

            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())

        os.replace(
            temp_name,
            path,
        )

    finally:
        if os.path.exists(temp_name):
            os.unlink(temp_name)


def atomic_json(
    path: Path,
    data: dict,
) -> None:

    content = json.dumps(
        data,
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    ) + "\n"

    atomic_write(
        path,
        content,
    )


def atomic_csv(
    path: Path,
    rows: list,
) -> None:

    import io

    stream = io.StringIO(
        newline="",
    )

    writer = csv.DictWriter(
        stream,
        fieldnames=FIELDS,
        lineterminator="\n",
    )

    writer.writeheader()
    writer.writerows(rows)

    atomic_write(
        path,
        stream.getvalue(),
    )


def load_json(path: Path) -> dict:

    with path.open(
        "r",
        encoding="utf-8-sig",
    ) as handle:

        result = json.load(handle)

    if not isinstance(result, dict):
        raise ValueError(
            f"Expected JSON object: {path}"
        )

    return result


# ============================================================
# Statistical utilities
# ============================================================

def wilson_interval(
    hits: int,
    total: int,
):

    """
    Wilson score interval, nominal 95%.

    Assumes independent Bernoulli observations.

    Since market signals may overlap,
    this interval is descriptive only.
    """

    if total <= 0:
        return None, None

    z = 1.959963984540054

    p = hits / total

    z2 = z * z

    denominator = (
        1 + z2 / total
    )

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


# ============================================================
# Source validation
# ============================================================

def validate_source(
    source_csv: Path,
    source_result: Path,
    source_manifest: Path,
):

    required_files = (
        source_csv,
        source_result,
        source_manifest,
    )

    for path in required_files:

        if not path.is_file():
            raise FileNotFoundError(
                f"Missing source file: {path}"
            )

    result = load_json(
        source_result
    )

    manifest = load_json(
        source_manifest
    )

    if result.get("execution_status") != "PASS":
        raise ValueError(
            "Source analysis execution_status is not PASS"
        )

    if result.get("version") != SOURCE_VERSION:
        raise ValueError(
            "Unexpected source version: "
            + str(result.get("version"))
        )

    if result.get(
        "approved_for_live_trading"
    ) is not False:

        raise ValueError(
            "Unsafe source approval state"
        )

    if manifest.get("version") != SOURCE_VERSION:
        raise ValueError(
            "Source manifest version mismatch"
        )

    if (
        manifest.get("source_sha256")
        != result.get("source_sha256")
    ):
        raise ValueError(
            "Source backtest SHA256 mismatch"
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
            "Missing source output SHA256 dictionary"
        )

    expected_files = {
        SOURCE_CSV,
        SOURCE_RESULT,
    }

    if set(output_hashes) != expected_files:
        raise ValueError(
            "Unexpected source output manifest entries"
        )

    for path in (
        source_csv,
        source_result,
    ):

        actual_hash = sha256(
            path
        )

        expected_hash = output_hashes.get(
            path.name
        )

        if actual_hash != expected_hash:
            raise ValueError(
                f"Source SHA256 mismatch: {path}"
            )

    return result, manifest


# ============================================================
# CSV validation
# ============================================================

def load_rows(path: Path):

    records = {}

    total_events = 0

    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as handle:

        reader = csv.DictReader(
            handle
        )

        if reader.fieldnames is None:
            raise ValueError(
                "Source CSV has no header"
            )

        if set(reader.fieldnames) != set(SOURCE_FIELDS):
            raise ValueError(
                "Source CSV schema mismatch. "
                f"Expected: {SOURCE_FIELDS}, "
                f"Actual: {reader.fieldnames}"
            )

        for line, row in enumerate(
            reader,
            start=2,
        ):

            if (
                None in row
                or any(
                    row.get(field) is None
                    for field in SOURCE_FIELDS
                )
            ):
                raise ValueError(
                    f"Malformed CSV row at line {line}"
                )

            split = (
                row["split"]
                .strip()
                .upper()
            )

            timeframe = (
                row["timeframe"]
                .strip()
                .lower()
            )

            regime = (
                row["regime"]
                .strip()
                .upper()
            )

            classification = (
                row["classification"]
                .strip()
                .upper()
            )

            try:

                horizon = int(
                    row["horizon"].strip()
                )

                events = int(
                    row["events"].strip()
                )

                hits = int(
                    row["hits"].strip()
                )

                reported_rate = float(
                    row["hit_rate_pct"].strip()
                )

            except (
                ValueError,
                TypeError,
            ) as error:

                raise ValueError(
                    f"Invalid numeric value at line {line}"
                ) from error

            if split not in SPLITS:
                raise ValueError(
                    f"Invalid split at line {line}"
                )

            if timeframe not in TIMEFRAMES:
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

            if classification not in GROUPS:
                raise ValueError(
                    f"Invalid classification at line {line}"
                )

            if (
                events <= 0
                or hits < 0
                or hits > events
            ):
                raise ValueError(
                    f"Invalid event counts at line {line}"
                )

            if not math.isfinite(
                reported_rate
            ):
                raise ValueError(
                    f"Non-finite hit rate at line {line}"
                )

            if not (
                0 <= reported_rate <= 100
            ):
                raise ValueError(
                    f"Hit rate out of range at line {line}"
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
                timeframe,
                horizon,
                regime,
                classification,
            )

            if key in records:
                raise ValueError(
                    f"Duplicate aggregate group: {key}"
                )

            records[key] = {
                "events": events,
                "hits": hits,
            }

            total_events += events

    if not records:
        raise ValueError(
            "Source CSV contains no records"
        )

    return records, total_events


# ============================================================
# Group statistics
# ============================================================

def make_group_values(record):

    if record is None:

        return {
            "events": 0,
            "hits": 0,
            "rate": None,
            "ci_low": None,
            "ci_high": None,
        }

    events = record["events"]
    hits = record["hits"]

    low, high = wilson_interval(
        hits,
        events,
    )

    return {
        "events": events,
        "hits": hits,
        "rate": hits / events,
        "ci_low": low,
        "ci_high": high,
    }


# ============================================================
# Regime comparison
# ============================================================

def build_comparison(
    records: dict,
    min_events: int,
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

        for timeframe in TIMEFRAMES:

            for horizon in HORIZONS:

                for regime in REGIMES:

                    base_key = (
                        split,
                        timeframe,
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

                    if (
                        only_record is None
                        and both_record is None
                    ):
                        continue

                    only = make_group_values(
                        only_record
                    )

                    both = make_group_values(
                        both_record
                    )

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

                        if uplift > 1e-10:

                            comparison_status = (
                                "BOTH_HIGHER"
                            )

                            positive_count += 1

                        elif uplift < -1e-10:

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
                            "timeframe": timeframe,
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
            sorted(status_counts.items())
        ),
    }

    return rows, summary


# ============================================================
# TRAIN / TEST stability
# ============================================================

def build_stability(rows: list):

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

    for timeframe in TIMEFRAMES:

        for horizon in HORIZONS:

            for regime in REGIMES:

                train = indexed.get(
                    (
                        "TRAIN",
                        timeframe,
                        horizon,
                        regime,
                    )
                )

                test = indexed.get(
                    (
                        "TEST",
                        timeframe,
                        horizon,
                        regime,
                    )
                )

                if (
                    train is None
                    or test is None
                ):

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
                        "timeframe": timeframe,
                        "horizon": horizon,
                        "regime": regime,
                        "status": status,
                    }
                )

    return {
        "cells": results,
        "counts": dict(
            sorted(counts.items())
        ),
    }


# ============================================================
# Resume / checkpoint validation
# ============================================================

def verify_existing(
    result_path: Path,
    csv_path: Path,
    manifest_path: Path,
    input_hashes: dict,
    config: dict,
) -> bool:

    required = (
        result_path,
        csv_path,
        manifest_path,
    )

    if not all(
        path.is_file()
        for path in required
    ):
        return False

    try:

        manifest = load_json(
            manifest_path
        )

        result = load_json(
            result_path
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
            result.get("research_decision")
            != "HOLD"
        ):
            return False

        if (
            result.get("approved_for_live_trading")
            is not False
        ):
            return False

        if (
            result.get("statistical_independence_proven")
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

        output_hashes = manifest.get(
            "output_sha256"
        )

        if not isinstance(
            output_hashes,
            dict,
        ):
            return False

        if set(output_hashes) != {
            OUTPUT_CSV,
            OUTPUT_JSON,
        }:
            return False

        for name, expected_hash in output_hashes.items():

            path = (
                result_path.parent
                / name
            )

            if not path.is_file():
                return False

            if sha256(path) != expected_hash:
                return False

        with csv_path.open(
            "r",
            encoding="utf-8-sig",
            newline="",
        ) as handle:

            reader = csv.DictReader(
                handle
            )

            if reader.fieldnames != FIELDS:
                return False

            row_count = 0

            for row in reader:

                if (
                    None in row
                    or any(
                        row.get(field) is None
                        for field in FIELDS
                    )
                ):
                    return False

                row_count += 1

        if (
            row_count
            != result.get("comparison_cells")
        ):
            return False

        summary = result.get(
            "summary"
        )

        if not isinstance(
            summary,
            dict,
        ):
            return False

        if (
            summary.get("comparison_cells")
            != row_count
        ):
            return False

        return True

    except (
        ValueError,
        KeyError,
        TypeError,
        OSError,
        json.JSONDecodeError,
    ):
        return False


# ============================================================
# Main research execution
# ============================================================

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
            "--min-events must be >= 1"
        )

    if not root.is_dir():
        raise FileNotFoundError(
            f"Project root not found: {root}"
        )

    if (
        output_dir == root
        or output_dir == source_dir
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

    print(
        f"Project root: {root}",
        flush=True,
    )

    print(
        f"Source directory: {source_dir}",
        flush=True,
    )

    print(
        f"Output directory: {output_dir}",
        flush=True,
    )

    # --------------------------------------------------------
    # Validate previous research stage
    # --------------------------------------------------------

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
        "source_version": source_info["version"],
        "source_config": source_info["config"],
    }

    # --------------------------------------------------------
    # Output paths
    # --------------------------------------------------------

    result_path = (
        output_dir / OUTPUT_JSON
    )

    csv_path = (
        output_dir / OUTPUT_CSV
    )

    manifest_path = (
        output_dir / OUTPUT_MANIFEST
    )

    # --------------------------------------------------------
    # Safe resume
    # --------------------------------------------------------

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

            print(
                "execution_status=PASS",
                flush=True,
            )

            print(
                "research_decision=HOLD",
                flush=True,
            )

            return

    # --------------------------------------------------------
    # Read and validate source CSV
    # --------------------------------------------------------

    records, total_events = load_rows(
        source_csv
    )

    expected_rows = source_info.get(
        "comparison_rows"
    )

    if len(records) != expected_rows:
        raise ValueError(
            "Source comparison row count mismatch. "
            f"Expected={expected_rows}, "
            f"Actual={len(records)}"
        )

    print(
        f"Validated source rows: {len(records)}",
        flush=True,
    )

    # --------------------------------------------------------
    # Build regime comparison
    # --------------------------------------------------------

    rows, summary = build_comparison(
        records,
        args.min_events,
    )

    if not rows:
        raise ValueError(
            "No regime comparison cells generated"
        )

    # --------------------------------------------------------
    # TRAIN / TEST stability
    # --------------------------------------------------------

    stability = build_stability(
        rows
    )

    # --------------------------------------------------------
    # Confirm original inputs did not change
    # --------------------------------------------------------

    final_hashes = {
        name: sha256(path)
        for name, path in input_paths.items()
    }

    if final_hashes != input_hashes:
        raise RuntimeError(
            "Input files changed during analysis"
        )

    # --------------------------------------------------------
    # Build result
    # --------------------------------------------------------

    result = {
        "version": VERSION,

        "execution_status": "PASS",

        "research_decision": "HOLD",

        "approved_for_live_trading": False,

        "independent_oos_proven": False,

        "statistical_independence_proven": False,

        "profitability_proven": False,

        "source_version": source_info["version"],

        "source_rows": source_info["source_rows"],

        "source_comparison_rows": (
            source_info["comparison_rows"]
        ),

        "source_excluded": source_info.get(
            "excluded",
            {},
        ),

        "included_aggregate_events": total_events,

        "comparison_cells": (
            summary["comparison_cells"]
        ),

        "summary": summary,

        "train_test_stability": stability,

        "config": config,

        "limitations": [
            (
                "Market regime is based on "
                "the previous stage's classification."
            ),
            (
                "Aggregated data cannot remove "
                "overlapping signal events."
            ),
            (
                "Wilson confidence intervals assume "
                "independent observations."
            ),
            (
                "No event-level statistical "
                "independence proof."
            ),
            (
                "No causal proof of accumulation "
                "pattern improvement."
            ),
            (
                "No trading costs or execution "
                "profitability proof."
            ),
            (
                "Small and missing groups cannot "
                "establish statistical robustness."
            ),
            (
                "No approval for live trading."
            ),
        ],
    }

    # --------------------------------------------------------
    # Write output files
    # --------------------------------------------------------

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    atomic_csv(
        csv_path,
        rows,
    )

    atomic_json(
        result_path,
        result,
    )

    # --------------------------------------------------------
    # Output manifest
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # Final verification
    # --------------------------------------------------------

    if not verify_existing(
        result_path,
        csv_path,
        manifest_path,
        input_hashes,
        config,
    ):
        raise RuntimeError(
            "Final output integrity verification failed"
        )

    # --------------------------------------------------------
    # Confirm inputs are still unchanged
    # --------------------------------------------------------

    after_output_hashes = {
        name: sha256(path)
        for name, path in input_paths.items()
    }

    if after_output_hashes != input_hashes:
        raise RuntimeError(
            "Input files changed while writing reports"
        )

    # --------------------------------------------------------
    # Console summary
    # --------------------------------------------------------

    console_result = {
        "version": VERSION,

        "execution_status": "PASS",

        "research_decision": "HOLD",

        "source_rows": source_info["source_rows"],

        "included_aggregate_events": total_events,

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
    }

    print(
        json.dumps(
            console_result,
            ensure_ascii=False,
            indent=2,
        ),
        flush=True,
    )


# ============================================================
# CLI
# ============================================================

def main():

    parser = argparse.ArgumentParser(
        description=__doc__
    )

    parser.add_argument(
        "--root",
        type=Path,
        default=Path("."),
        help="Project root directory",
    )

    parser.add_argument(
        "--min-events",
        type=int,
        default=100,
        help=(
            "Minimum events per classification "
            "for each comparison group"
        ),
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help=(
            "Recalculate results even if verified "
            "existing outputs are available"
        ),
    )

    args = parser.parse_args()

    try:

        run(args)

    except (
        OSError,
        ValueError,
        RuntimeError,
        KeyError,
        TypeError,
    ) as error:

        print(
            "FATAL:",
            type(error).__name__,
            str(error),
            file=sys.stderr,
            flush=True,
        )

        sys.exit(1)


if __name__ == "__main__":
    main()
