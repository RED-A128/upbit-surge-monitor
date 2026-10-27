"""
Upbit Surge Monitor
Pattern 256 Feature + Signal Prefix Replay Audit
Clean V002

Purpose:
    Recalculate historical features and Pattern 256 signals
    using only OHLCV candles available up to each cutoff.

    Compare replay results against:
        data/features/{timeframe}/{market}.csv
        data/signals/256/{timeframe}/{market}.csv

Clean V002:
    - Check OHLCV length before selecting replay targets.
    - Require sufficient history at every requested cutoff.
    - Exclude insufficient-history markets before sampling.
    - Record excluded markets and reasons.
    - Require the requested number of markets per timeframe.
    - Preserve existing replay calculations and comparisons.
    - Preserve existing GitHub Actions result fields.
    - Verify input SHA256 before and after replay.

Safety:
    - Original OHLCV is read-only.
    - Existing features and signals are read-only.
    - No trading.
    - No model training.
    - No modification of other research reports.
    - Sample-based audit, not full-market proof.

Outputs:
    data/reports/pattern_256_prefix_replay/
        prefix_replay_checks.csv
        prefix_replay_issues.csv
        prefix_replay_exclusions.csv
        prefix_replay_result.json
        prefix_replay_manifest.json

Run:
    py -u audit_pattern_256_prefix_replay.py --root .

Expanded run:
    py -u audit_pattern_256_prefix_replay.py ^
        --root . ^
        --markets-per-timeframe 30 ^
        --cutoffs "0.40,0.65,0.85"
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import importlib.util
import json
import math
import os
import sys
import traceback

from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd


# ============================================================
# CONSTANTS
# ============================================================

VERSION = "Clean V002"

TIMEFRAMES = (
    "h1",
    "h4",
    "d1",
)

DEFAULT_MARKETS_PER_TIMEFRAME = 3

DEFAULT_CUTOFFS = (
    0.40,
    0.65,
    0.85,
)

MINIMUM_CANDLES = 300

RTOL = 1e-8
ATOL = 1e-9

OUTPUT_DIRECTORY = (
    "data/reports/pattern_256_prefix_replay"
)

CHECK_COLUMNS = [
    "timeframe",
    "market",
    "cutoff_fraction",
    "cutoff_row",
    "cutoff_timestamp",
    "source_rows",
    "feature_columns_checked",
    "signal_columns_checked",
    "feature_mismatches",
    "signal_mismatches",
    "status",
    "message",
]

ISSUE_COLUMNS = [
    "timeframe",
    "market",
    "cutoff_fraction",
    "cutoff_timestamp",
    "category",
    "column",
    "expected",
    "actual",
    "message",
]

EXCLUSION_COLUMNS = [
    "timeframe",
    "market",
    "source_rows",
    "minimum_required_rows",
    "reason",
    "message",
]


# ============================================================
# GENERAL UTILITIES
# ============================================================

def utc_now():
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path):
    path = Path(path)

    digest = hashlib.sha256()

    with path.open("rb") as handle:
        while True:
            block = handle.read(1024 * 1024)

            if not block:
                break

            digest.update(block)

    return digest.hexdigest()


def load_module(path, name):
    path = Path(path)

    if not path.is_file():
        raise FileNotFoundError(
            f"Required Python file missing: {path}"
        )

    spec = importlib.util.spec_from_file_location(
        name,
        str(path),
    )

    if spec is None or spec.loader is None:
        raise RuntimeError(
            f"Cannot load Python module: {path}"
        )

    module = importlib.util.module_from_spec(spec)

    spec.loader.exec_module(module)

    return module


def atomic_json(path, payload):
    path = Path(path)

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temporary = path.with_name(
        path.name + ".tmp"
    )

    try:
        with temporary.open(
            "w",
            encoding="utf-8",
        ) as handle:
            json.dump(
                payload,
                handle,
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )

            handle.write("\n")

        os.replace(
            temporary,
            path,
        )

    finally:
        if temporary.exists():
            temporary.unlink()


def atomic_csv(path, rows, columns):
    path = Path(path)

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temporary = path.with_name(
        path.name + ".tmp"
    )

    try:
        with temporary.open(
            "w",
            encoding="utf-8-sig",
            newline="",
        ) as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=columns,
                extrasaction="ignore",
            )

            writer.writeheader()

            writer.writerows(rows)

        os.replace(
            temporary,
            path,
        )

    finally:
        if temporary.exists():
            temporary.unlink()


def read_csv(path):
    path = Path(path)

    if not path.is_file():
        raise FileNotFoundError(
            f"Required CSV missing: {path}"
        )

    frame = pd.read_csv(
        path,
        encoding="utf-8-sig",
        low_memory=False,
    )

    if frame.empty:
        raise RuntimeError(
            f"CSV contains no rows: {path}"
        )

    return frame


def verify_timestamps(frame, label):
    column = "candle_date_time_utc"

    if column not in frame.columns:
        raise RuntimeError(
            f"{label}: missing {column}"
        )

    times = pd.to_datetime(
        frame[column],
        utc=True,
        errors="coerce",
    )

    if times.isna().any():
        raise RuntimeError(
            f"{label}: invalid UTC timestamps"
        )

    if times.duplicated().any():
        raise RuntimeError(
            f"{label}: duplicate UTC timestamps"
        )

    if not times.is_monotonic_increasing:
        raise RuntimeError(
            f"{label}: timestamps are not sorted"
        )

    return times


def verify_input_unchanged(paths, hashes):
    for path in paths:
        path = Path(path)

        before = hashes[str(path)]

        after = sha256_file(path)

        if before != after:
            raise RuntimeError(
                f"Input changed during audit: {path}"
            )


# ============================================================
# CUTOFF VALIDATION
# ============================================================

def required_source_rows(fractions):
    """
    Every cutoff must contain at least MINIMUM_CANDLES.

    Example:
        minimum = 300
        smallest fraction = 0.40

        required rows = 750

    We use the actual integer cutoff calculation to avoid
    floating-point boundary errors.
    """

    if not fractions:
        raise ValueError(
            "At least one cutoff is required"
        )

    smallest = min(fractions)

    estimate = max(
        MINIMUM_CANDLES + 1,
        int(math.ceil(
            MINIMUM_CANDLES / smallest
        )),
    )

    while int(estimate * smallest) < MINIMUM_CANDLES:
        estimate += 1

    return estimate


def calculate_cutoff_rows(total_rows, fraction):
    """
    Unlike Clean V001, this function does not silently
    increase a short prefix to 300 candles.

    The requested fraction must independently provide
    sufficient historical candles.
    """

    cutoff_rows = int(
        total_rows * fraction
    )

    if cutoff_rows < MINIMUM_CANDLES:
        raise RuntimeError(
            "Insufficient candles at requested cutoff: "
            f"total={total_rows}, "
            f"fraction={fraction}, "
            f"prefix={cutoff_rows}, "
            f"required={MINIMUM_CANDLES}"
        )

    if cutoff_rows >= total_rows:
        raise RuntimeError(
            "Replay cutoff must exclude future candles"
        )

    return cutoff_rows


# ============================================================
# SOURCE DISCOVERY
# ============================================================

def count_csv_data_rows(path):
    """
    Count physical CSV records without loading the complete
    OHLCV dataset into memory.

    This is only a preselection check.

    The selected files are subsequently parsed and validated
    by build_features.load_source_ohlcv().
    """

    path = Path(path)

    if not path.is_file():
        raise FileNotFoundError(
            f"OHLCV file missing: {path}"
        )

    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as handle:
        reader = csv.reader(handle)

        header = next(reader, None)

        if not header:
            raise RuntimeError(
                f"OHLCV header missing: {path}"
            )

        if "candle_date_time_utc" not in header:
            raise RuntimeError(
                f"OHLCV timestamp column missing: {path}"
            )

        count = 0

        for row in reader:
            if not row:
                continue

            count += 1

    return count


def spread_selection(available, limit):
    """
    Deterministic sampling across sorted eligible markets.

    This preserves the original V001 selection principle:
    do not simply select the first N markets.
    """

    if len(available) < limit:
        raise RuntimeError(
            "Insufficient eligible markets: "
            f"required={limit}, "
            f"available={len(available)}"
        )

    indices = np.linspace(
        0,
        len(available) - 1,
        num=limit,
        dtype=int,
    )

    selected = [
        available[int(index)]
        for index in indices
    ]

    if len(selected) != limit:
        raise RuntimeError(
            "Market selection count mismatch"
        )

    selected_keys = [
        (item[0], item[1])
        for item in selected
    ]

    if len(set(selected_keys)) != len(selected_keys):
        raise RuntimeError(
            "Duplicate market selected"
        )

    return selected


def discover_jobs(
    root,
    limit,
    fractions,
    exclusions,
):
    """
    Clean V002 discovery.

    1. Find OHLCV files.
    2. Require matching feature and signal files.
    3. Count OHLCV candles.
    4. Check every requested cutoff.
    5. Exclude insufficient-history markets.
    6. Sample only from eligible markets.
    7. Require the full requested sample size.
    """

    jobs = []

    discovery_stats = {}

    minimum_rows = required_source_rows(
        fractions
    )

    print(
        f"[DISCOVERY] Minimum OHLCV rows: "
        f"{minimum_rows:,}",
        flush=True,
    )

    for timeframe in TIMEFRAMES:
        ohlcv_directory = (
            root / "data" / "ohlcv" / timeframe
        )

        feature_directory = (
            root / "data" / "features" / timeframe
        )

        signal_directory = (
            root / "data" / "signals" / "256" / timeframe
        )

        if not ohlcv_directory.is_dir():
            raise RuntimeError(
                f"OHLCV directory missing: "
                f"{ohlcv_directory}"
            )

        if not feature_directory.is_dir():
            raise RuntimeError(
                f"Feature directory missing: "
                f"{feature_directory}"
            )

        if not signal_directory.is_dir():
            raise RuntimeError(
                f"Signal directory missing: "
                f"{signal_directory}"
            )

        available = []

        total_sources = 0
        missing_outputs = 0
        insufficient_history = 0
        invalid_sources = 0

        source_files = sorted(
            ohlcv_directory.glob("KRW-*.csv")
        )

        for source in source_files:
            total_sources += 1

            market = source.stem

            feature = (
                feature_directory / source.name
            )

            signal = (
                signal_directory / source.name
            )

            if not feature.is_file():
                missing_outputs += 1

                exclusions.append({
                    "timeframe": timeframe,
                    "market": market,
                    "source_rows": "",
                    "minimum_required_rows": minimum_rows,
                    "reason": "FEATURE_MISSING",
                    "message": (
                        "Matching feature CSV does not exist"
                    ),
                })

                continue

            if not signal.is_file():
                missing_outputs += 1

                exclusions.append({
                    "timeframe": timeframe,
                    "market": market,
                    "source_rows": "",
                    "minimum_required_rows": minimum_rows,
                    "reason": "SIGNAL_MISSING",
                    "message": (
                        "Matching Pattern 256 signal CSV "
                        "does not exist"
                    ),
                })

                continue

            try:
                source_rows = count_csv_data_rows(
                    source
                )

            except Exception as exc:
                invalid_sources += 1

                exclusions.append({
                    "timeframe": timeframe,
                    "market": market,
                    "source_rows": "",
                    "minimum_required_rows": minimum_rows,
                    "reason": "SOURCE_READ_ERROR",
                    "message": str(exc),
                })

                continue

            if source_rows < minimum_rows:
                insufficient_history += 1

                exclusions.append({
                    "timeframe": timeframe,
                    "market": market,
                    "source_rows": source_rows,
                    "minimum_required_rows": minimum_rows,
                    "reason": "INSUFFICIENT_HISTORY",
                    "message": (
                        f"OHLCV rows={source_rows}; "
                        f"required={minimum_rows}"
                    ),
                })

                continue

            valid_cutoffs = True

            for fraction in fractions:
                try:
                    calculate_cutoff_rows(
                        source_rows,
                        fraction,
                    )

                except RuntimeError:
                    valid_cutoffs = False
                    break

            if not valid_cutoffs:
                insufficient_history += 1

                exclusions.append({
                    "timeframe": timeframe,
                    "market": market,
                    "source_rows": source_rows,
                    "minimum_required_rows": minimum_rows,
                    "reason": "INVALID_CUTOFF_HISTORY",
                    "message": (
                        "At least one requested cutoff "
                        "does not contain sufficient candles"
                    ),
                })

                continue

            available.append((
                timeframe,
                market,
                source,
                feature,
                signal,
            ))

        stats = {
            "source_files": total_sources,
            "eligible_markets": len(available),
            "missing_outputs": missing_outputs,
            "insufficient_history": insufficient_history,
            "invalid_sources": invalid_sources,
            "requested_markets": limit,
            "selected_markets": 0,
        }

        discovery_stats[timeframe] = stats

        print(
            f"[DISCOVERY] {timeframe}: "
            f"sources={total_sources}, "
            f"eligible={len(available)}, "
            f"short={insufficient_history}, "
            f"missing={missing_outputs}, "
            f"invalid={invalid_sources}",
            flush=True,
        )

        if len(available) < limit:
            raise RuntimeError(
                f"{timeframe}: insufficient eligible "
                f"markets after history filtering: "
                f"required={limit}, "
                f"available={len(available)}"
            )

        selected = spread_selection(
            available,
            limit,
        )

        stats["selected_markets"] = len(
            selected
        )

        jobs.extend(selected)

        print(
            f"[SELECTED] {timeframe}: "
            f"{len(selected)} markets",
            flush=True,
        )

        for job in selected:
            print(
                f"  {job[1]}",
                flush=True,
            )

    expected_jobs = (
        len(TIMEFRAMES) * limit
    )

    if len(jobs) != expected_jobs:
        raise RuntimeError(
            "Selected job count mismatch: "
            f"expected={expected_jobs}, "
            f"actual={len(jobs)}"
        )

    return jobs, discovery_stats


# ============================================================
# VALUE COMPARISON
# ============================================================

def value_equal(expected, actual):
    if pd.isna(expected) and pd.isna(actual):
        return True

    if pd.isna(expected) or pd.isna(actual):
        return False

    if isinstance(
        expected,
        (int, float, np.number),
    ) and isinstance(
        actual,
        (int, float, np.number),
    ):
        left = float(expected)
        right = float(actual)

        if not math.isfinite(left):
            return left == right

        if not math.isfinite(right):
            return False

        return math.isclose(
            left,
            right,
            rel_tol=RTOL,
            abs_tol=ATOL,
        )

    return str(expected) == str(actual)


def compare_row(
    expected_frame,
    actual_frame,
    index,
    columns,
    category,
    context,
    issues,
):
    mismatches = 0

    for column in columns:
        if column not in expected_frame.columns:
            mismatches += 1

            issues.append({
                **context,
                "category": category,
                "column": column,
                "expected": "MISSING_COLUMN",
                "actual": "",
                "message": (
                    "Stored output is missing column"
                ),
            })

            continue

        if column not in actual_frame.columns:
            mismatches += 1

            issues.append({
                **context,
                "category": category,
                "column": column,
                "expected": "",
                "actual": "MISSING_COLUMN",
                "message": (
                    "Replay output is missing column"
                ),
            })

            continue

        expected = (
            expected_frame.iloc[index][column]
        )

        actual = (
            actual_frame.iloc[index][column]
        )

        if not value_equal(
            expected,
            actual,
        ):
            mismatches += 1

            issues.append({
                **context,
                "category": category,
                "column": column,
                "expected": str(expected),
                "actual": str(actual),
                "message": (
                    "Prefix replay value mismatch"
                ),
            })

    return mismatches


def compare_timestamps(
    source,
    stored,
    label,
):
    if len(source) != len(stored):
        raise RuntimeError(
            f"{label}: row count mismatch: "
            f"OHLCV={len(source)}, "
            f"stored={len(stored)}"
        )

    source_times = verify_timestamps(
        source,
        "OHLCV",
    )

    stored_times = verify_timestamps(
        stored,
        label,
    )

    if not source_times.equals(stored_times):
        raise RuntimeError(
            f"{label}: OHLCV timestamps do not match"
        )


# ============================================================
# REPLAY EXECUTION
# ============================================================

def run_job(
    root,
    job,
    fractions,
    feature_module,
    detector_module,
    checks,
    issues,
    input_hashes,
):
    (
        timeframe,
        market,
        source_path,
        feature_path,
        signal_path,
    ) = job

    paths = (
        source_path,
        feature_path,
        signal_path,
    )

    for path in paths:
        input_hashes[str(path)] = sha256_file(
            path
        )

    print(
        f"[LOAD] {timeframe} {market}",
        flush=True,
    )

    source = feature_module.load_source_ohlcv(
        source_path,
        expected_market=market,
    )

    stored_features = read_csv(
        feature_path
    )

    stored_signals = read_csv(
        signal_path
    )

    compare_timestamps(
        source,
        stored_features,
        "Stored features",
    )

    compare_timestamps(
        source,
        stored_signals,
        "Stored signals",
    )

    feature_columns = (
        feature_module.get_required_feature_columns()
    )

    signal_columns = list(
        detector_module.DETECTOR_COLUMNS
    )

    signal_columns = list(
        dict.fromkeys(signal_columns)
    )

    if not feature_columns:
        raise RuntimeError(
            "Feature column list is empty"
        )

    if not signal_columns:
        raise RuntimeError(
            "Signal column list is empty"
        )

    total_rows = len(source)

    minimum_rows = required_source_rows(
        fractions
    )

    if total_rows < minimum_rows:
        raise RuntimeError(
            "Selected source became ineligible: "
            f"rows={total_rows}, "
            f"required={minimum_rows}"
        )

    for fraction in fractions:
        cutoff_rows = calculate_cutoff_rows(
            total_rows,
            fraction,
        )

        cutoff_index = cutoff_rows - 1

        cutoff_timestamp = (
            source.iloc[cutoff_index][
                "candle_date_time_utc"
            ].isoformat()
        )

        context = {
            "timeframe": timeframe,
            "market": market,
            "cutoff_fraction": fraction,
            "cutoff_timestamp": cutoff_timestamp,
        }

        print(
            f"[REPLAY] {timeframe} {market} "
            f"fraction={fraction:.2f} "
            f"rows={cutoff_rows:,}",
            flush=True,
        )

        # Only candles available up to this cutoff
        # are passed into the feature builder.

        prefix = source.iloc[
            :cutoff_rows
        ].copy()

        replay_features = (
            feature_module.build_features(
                prefix
            )
        )

        feature_module.validate_feature_dataframe(
            source_df=prefix,
            feature_df=replay_features,
            expected_market=market,
        )

        replay_signals = (
            detector_module.build_detector_dataframe(
                replay_features
            )
        )

        detector_module.validate_detector_dataframe(
            feature_df=replay_features,
            detector_df=replay_signals,
            expected_market=market,
        )

        if len(replay_features) != cutoff_rows:
            raise RuntimeError(
                "Replay feature row count mismatch"
            )

        if len(replay_signals) != cutoff_rows:
            raise RuntimeError(
                "Replay signal row count mismatch"
            )

        feature_mismatches = compare_row(
            expected_frame=stored_features,
            actual_frame=replay_features,
            index=cutoff_index,
            columns=feature_columns,
            category="FEATURE",
            context=context,
            issues=issues,
        )

        signal_mismatches = compare_row(
            expected_frame=stored_signals,
            actual_frame=replay_signals,
            index=cutoff_index,
            columns=signal_columns,
            category="SIGNAL",
            context=context,
            issues=issues,
        )

        passed = (
            feature_mismatches == 0
            and signal_mismatches == 0
        )

        checks.append({
            **context,
            "cutoff_row": cutoff_rows,
            "source_rows": total_rows,
            "feature_columns_checked": len(
                feature_columns
            ),
            "signal_columns_checked": len(
                signal_columns
            ),
            "feature_mismatches": (
                feature_mismatches
            ),
            "signal_mismatches": (
                signal_mismatches
            ),
            "status": (
                "PASS" if passed else "FAIL"
            ),
            "message": (
                "Prefix replay matches stored values"
                if passed
                else "Replay mismatch detected"
            ),
        })

        print(
            f"[{'PASS' if passed else 'FAIL'}] "
            f"feature={feature_mismatches} "
            f"signal={signal_mismatches}",
            flush=True,
        )

    verify_input_unchanged(
        paths,
        input_hashes,
    )


# ============================================================
# ARGUMENTS
# ============================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Pattern 256 Feature + Signal "
            "Prefix Replay Audit Clean V002"
        )
    )

    parser.add_argument(
        "--root",
        default=".",
        help="Project root directory",
    )

    parser.add_argument(
        "--markets-per-timeframe",
        type=int,
        default=DEFAULT_MARKETS_PER_TIMEFRAME,
    )

    parser.add_argument(
        "--cutoffs",
        default="0.40,0.65,0.85",
        help="Comma-separated cutoff fractions",
    )

    return parser.parse_args()


def parse_fractions(value):
    parts = [
        item.strip()
        for item in value.split(",")
    ]

    if not parts or any(
        not item for item in parts
    ):
        raise ValueError(
            "Invalid cutoff list"
        )

    fractions = tuple(
        float(item)
        for item in parts
    )

    if len(set(fractions)) != len(fractions):
        raise ValueError(
            "Duplicate cutoff fractions"
        )

    for fraction in fractions:
        if not math.isfinite(fraction):
            raise ValueError(
                "Cutoff fraction must be finite"
            )

        if not 0.1 <= fraction < 1.0:
            raise ValueError(
                f"Invalid cutoff fraction: {fraction}"
            )

    return fractions


# ============================================================
# MAIN
# ============================================================

def main():
    args = parse_args()

    root = Path(
        args.root
    ).resolve()

    if args.markets_per_timeframe < 1:
        raise ValueError(
            "markets-per-timeframe must be >= 1"
        )

    fractions = parse_fractions(
        args.cutoffs
    )

    output = (
        root / OUTPUT_DIRECTORY
    )

    output.mkdir(
        parents=True,
        exist_ok=True,
    )

    result_path = (
        output / "prefix_replay_result.json"
    )

    manifest_path = (
        output / "prefix_replay_manifest.json"
    )

    checks_path = (
        output / "prefix_replay_checks.csv"
    )

    issues_path = (
        output / "prefix_replay_issues.csv"
    )

    exclusions_path = (
        output / "prefix_replay_exclusions.csv"
    )

    print("=" * 72)
    print(
        "PATTERN 256 FEATURE + SIGNAL PREFIX REPLAY AUDIT"
    )
    print(f"Version: {VERSION}")
    print(f"Root: {root}")
    print(
        "Markets per timeframe: "
        f"{args.markets_per_timeframe}"
    )
    print(f"Cutoffs: {fractions}")
    print("=" * 72)

    feature_script = (
        root / "build_features.py"
    )

    detector_script = (
        root / "detect_256.py"
    )

    feature_module = load_module(
        feature_script,
        "prefix_replay_feature_module",
    )

    detector_module = load_module(
        detector_script,
        "prefix_replay_detector_module",
    )

    input_hashes = {
        str(feature_script): sha256_file(
            feature_script
        ),
        str(detector_script): sha256_file(
            detector_script
        ),
    }

    checks = []
    issues = []
    exclusions = []
    execution_errors = []

    jobs = []
    discovery_stats = {}

    discovery_succeeded = False

    try:
        jobs, discovery_stats = discover_jobs(
            root=root,
            limit=args.markets_per_timeframe,
            fractions=fractions,
            exclusions=exclusions,
        )

        discovery_succeeded = True

    except Exception as exc:
        message = str(exc)

        print(
            f"[DISCOVERY ERROR] {message}",
            flush=True,
        )

        execution_errors.append({
            "timeframe": "",
            "market": "",
            "message": message,
        })

        issues.append({
            "timeframe": "",
            "market": "",
            "cutoff_fraction": "",
            "cutoff_timestamp": "",
            "category": "DISCOVERY",
            "column": "",
            "expected": "",
            "actual": "",
            "message": message,
        })

    if discovery_succeeded:
        for job in jobs:
            timeframe, market, *_ = job

            try:
                run_job(
                    root=root,
                    job=job,
                    fractions=fractions,
                    feature_module=feature_module,
                    detector_module=detector_module,
                    checks=checks,
                    issues=issues,
                    input_hashes=input_hashes,
                )

            except Exception as exc:
                message = str(exc)

                print(
                    f"[ERROR] {timeframe} {market}: "
                    f"{message}",
                    flush=True,
                )

                execution_errors.append({
                    "timeframe": timeframe,
                    "market": market,
                    "message": message,
                })

                issues.append({
                    "timeframe": timeframe,
                    "market": market,
                    "cutoff_fraction": "",
                    "cutoff_timestamp": "",
                    "category": "EXECUTION",
                    "column": "",
                    "expected": "",
                    "actual": "",
                    "message": message,
                })

    # Verify that every hashed input remained unchanged.

    for filename, expected_hash in input_hashes.items():
        try:
            path = Path(filename)

            actual_hash = sha256_file(
                path
            )

            if actual_hash != expected_hash:
                raise RuntimeError(
                    f"Input SHA256 changed: {filename}"
                )

        except Exception as exc:
            message = str(exc)

            execution_errors.append({
                "timeframe": "",
                "market": "",
                "message": message,
            })

            issues.append({
                "timeframe": "",
                "market": "",
                "cutoff_fraction": "",
                "cutoff_timestamp": "",
                "category": "INPUT_INTEGRITY",
                "column": "",
                "expected": expected_hash,
                "actual": "",
                "message": message,
            })

    # Write reports.

    atomic_csv(
        checks_path,
        checks,
        CHECK_COLUMNS,
    )

    atomic_csv(
        issues_path,
        issues,
        ISSUE_COLUMNS,
    )

    atomic_csv(
        exclusions_path,
        exclusions,
        EXCLUSION_COLUMNS,
    )

    passed = sum(
        item["status"] == "PASS"
        for item in checks
    )

    failed = sum(
        item["status"] != "PASS"
        for item in checks
    )

    expected_checks = (
        len(TIMEFRAMES)
        * args.markets_per_timeframe
        * len(fractions)
    )

    completed_checks = len(
        checks
    )

    execution_pass = (
        discovery_succeeded
        and not execution_errors
        and not issues
        and failed == 0
        and passed == expected_checks
        and completed_checks == expected_checks
    )

    exclusion_counts = {}

    for item in exclusions:
        reason = item["reason"]

        exclusion_counts[reason] = (
            exclusion_counts.get(reason, 0) + 1
        )

    result = {
        "version": VERSION,
        "audit": "pattern_256_prefix_replay",
        "execution": (
            "PASS" if execution_pass else "FAIL"
        ),
        "created_utc": utc_now(),

        "sampled_market_timeframe_jobs": (
            len(jobs)
        ),

        "requested_markets_per_timeframe": (
            args.markets_per_timeframe
        ),

        "cutoff_fractions": list(
            fractions
        ),

        "minimum_candles_per_cutoff": (
            MINIMUM_CANDLES
        ),

        "minimum_source_rows": (
            required_source_rows(fractions)
        ),

        "expected_checks": expected_checks,
        "completed_checks": completed_checks,
        "passed_checks": passed,
        "failed_checks": failed,

        "issue_count": len(issues),
        "execution_errors": execution_errors,

        "excluded_market_timeframe_count": (
            len(exclusions)
        ),

        "exclusion_counts": exclusion_counts,

        "discovery_stats": discovery_stats,

        "feature_prefix_replay_verified": (
            execution_pass
        ),

        "signal_prefix_replay_verified": (
            execution_pass
        ),

        "scope": (
            "SAMPLED_ELIGIBLE_MARKETS_AND_CUTOFFS_ONLY"
        ),

        "full_market_leakage_free_proven": False,

        "upstream_signal_formula_leakage_free_proven": (
            False
        ),

        "point_in_time_proven": False,
        "independent_oos_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",

        "limitations": [
            (
                "Only sampled eligible markets and "
                "cutoff rows are replayed."
            ),
            (
                "Markets with insufficient historical "
                "candles are excluded before sampling."
            ),
            (
                "Excluded markets are not treated "
                "as successful replay checks."
            ),
            (
                "Matching stored outputs does not "
                "independently prove their original "
                "generation was leakage-free."
            ),
            (
                "This audit does not prove historical "
                "data availability at actual decision time."
            ),
            (
                "No independent OOS performance "
                "validation is performed."
            ),
        ],
    }

    atomic_json(
        result_path,
        result,
    )

    output_hashes = {
        "prefix_replay_checks.csv": sha256_file(
            checks_path
        ),
        "prefix_replay_issues.csv": sha256_file(
            issues_path
        ),
        "prefix_replay_exclusions.csv": sha256_file(
            exclusions_path
        ),
        "prefix_replay_result.json": sha256_file(
            result_path
        ),
    }

    manifest = {
        "version": VERSION,
        "created_utc": utc_now(),
        "input_sha256": input_hashes,
        "output_sha256": output_hashes,
        "expected_checks": expected_checks,
        "completed_checks": completed_checks,
        "passed_checks": passed,
        "execution": result["execution"],
        "minimum_source_rows": (
            required_source_rows(fractions)
        ),
        "excluded_market_timeframe_count": (
            len(exclusions)
        ),
    }

    atomic_json(
        manifest_path,
        manifest,
    )

    print()
    print("=" * 72)
    print("PREFIX REPLAY AUDIT SUMMARY")
    print("=" * 72)
    print(f"Version         : {VERSION}")
    print(f"Execution       : {result['execution']}")
    print(f"Expected checks : {expected_checks}")
    print(f"Completed checks: {completed_checks}")
    print(f"Passed checks   : {passed}")
    print(f"Failed checks   : {failed}")
    print(f"Issues          : {len(issues)}")
    print(f"Excluded markets: {len(exclusions)}")
    print(
        "Minimum rows    : "
        f"{required_source_rows(fractions)}"
    )
    print("Research        : HOLD")
    print("TRAIN approved  : False")
    print("Trading approved: False")
    print(f"Reports         : {output}")
    print("=" * 72)

    return 0 if execution_pass else 1


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    try:
        sys.exit(
            main()
        )

    except KeyboardInterrupt:
        print(
            "[STOP] Interrupted by user",
            flush=True,
        )

        sys.exit(130)

    except Exception:
        traceback.print_exc()

        sys.exit(1)
