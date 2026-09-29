#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
===============================================================================
Upbit Surge Monitor
validate_ohlcv_timestamp.py
OHLCV Timestamp Validator - Clean V001
===============================================================================

Purpose
-------
Validate canonical timestamp integrity for all OHLCV CSV files.

This validator is READ ONLY.

It does NOT:
- modify OHLCV CSV files
- repair timestamp values
- delete rows
- reorder rows
- create feature data
- create signals
- create predictions
- trade

Validation policy
-----------------
Canonical time source:
    candle_date_time_utc

Canonical timestamp:
    Unix epoch milliseconds derived from candle_date_time_utc

Validation checks:
    1. CSV readable
    2. Required columns exist
    3. File is not empty
    4. timestamp contains no NaN
    5. candle_date_time_utc contains no NaT
    6. candle_date_time_utc contains no duplicates
    7. candle_date_time_utc is monotonically increasing
    8. timestamp is monotonically increasing
    9. timestamp exactly matches candle_date_time_utc epoch milliseconds

Default scope:
    data/ohlcv/h1/*.csv
    data/ohlcv/h4/*.csv
    data/ohlcv/d1/*.csv

Expected project:
    Upbit Surge Monitor

Version:
    OHLCV Timestamp Validator Clean V001
===============================================================================
"""

from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import pandas as pd


# =============================================================================
# CONSTANTS
# =============================================================================

PROJECT_NAME = "Upbit Surge Monitor"
VERSION = "OHLCV Timestamp Validator Clean V001"

TIMEFRAMES: Tuple[str, ...] = ("h1", "h4", "d1")

REQUIRED_COLUMNS: Tuple[str, ...] = (
    "timestamp",
    "candle_date_time_utc",
)

SEPARATOR = "=" * 88
SUB_SEPARATOR = "-" * 88


# =============================================================================
# DATA CLASSES
# =============================================================================


@dataclass
class ValidationResult:
    timeframe: str
    market: str
    path: Path

    rows: int = 0

    csv_readable: bool = False
    required_columns_ok: bool = False
    non_empty: bool = False

    timestamp_nan: int = 0
    utc_nat: int = 0
    utc_duplicates: int = 0

    utc_chronological: bool = False
    timestamp_chronological: bool = False

    timestamp_mismatch: int = 0

    passed: bool = False
    error: str = ""


# =============================================================================
# ARGUMENTS
# =============================================================================


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Validate OHLCV timestamp integrity for Upbit Surge Monitor. "
            "READ ONLY."
        )
    )

    parser.add_argument(
        "--market",
        type=str,
        default=None,
        help=(
            "Validate one market only. "
            "Example: --market KRW-BTC"
        ),
    )

    parser.add_argument(
        "--timeframe",
        type=str,
        choices=TIMEFRAMES,
        default=None,
        help=(
            "Validate one timeframe only. "
            "Choices: h1, h4, d1"
        ),
    )

    parser.add_argument(
        "--base-dir",
        type=str,
        default=None,
        help=(
            "Project base directory. "
            "Default: directory containing this script"
        ),
    )

    return parser.parse_args()


# =============================================================================
# UTILITY
# =============================================================================


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_market(market: Optional[str]) -> Optional[str]:
    if market is None:
        return None

    market = market.strip().upper()

    if not market:
        return None

    return market


def get_base_dir(base_dir_arg: Optional[str]) -> Path:
    if base_dir_arg:
        return Path(base_dir_arg).expanduser().resolve()

    return Path(__file__).resolve().parent


def get_selected_timeframes(
    timeframe: Optional[str],
) -> Tuple[str, ...]:
    if timeframe:
        return (timeframe,)

    return TIMEFRAMES


def safe_int(value: object) -> int:
    try:
        return int(value)
    except Exception:
        return 0


# =============================================================================
# FILE DISCOVERY
# =============================================================================


def discover_csv_files(
    base_dir: Path,
    timeframes: Sequence[str],
    market: Optional[str],
) -> Tuple[List[Tuple[str, str, Path]], List[str]]:
    """
    Discover OHLCV CSV files.

    Returns:
        jobs:
            [
                (timeframe, market, path),
                ...
            ]

        discovery_errors:
            [
                "...",
                ...
            ]
    """

    jobs: List[Tuple[str, str, Path]] = []
    discovery_errors: List[str] = []

    ohlcv_root = base_dir / "data" / "ohlcv"

    if not ohlcv_root.exists():
        discovery_errors.append(
            f"OHLCV root directory does not exist: {ohlcv_root}"
        )
        return jobs, discovery_errors

    for timeframe in timeframes:
        tf_dir = ohlcv_root / timeframe

        if not tf_dir.exists():
            discovery_errors.append(
                f"Timeframe directory does not exist: {tf_dir}"
            )
            continue

        if market:
            path = tf_dir / f"{market}.csv"

            if not path.exists():
                discovery_errors.append(
                    f"CSV file does not exist: {path}"
                )
                continue

            jobs.append(
                (
                    timeframe,
                    market,
                    path,
                )
            )

            continue

        paths = sorted(
            tf_dir.glob("KRW-*.csv"),
            key=lambda p: p.name.upper(),
        )

        if not paths:
            discovery_errors.append(
                f"No KRW CSV files found: {tf_dir}"
            )
            continue

        for path in paths:
            jobs.append(
                (
                    timeframe,
                    path.stem.upper(),
                    path,
                )
            )

    jobs.sort(
        key=lambda item: (
            item[1],
            TIMEFRAMES.index(item[0])
            if item[0] in TIMEFRAMES
            else 999,
        )
    )

    return jobs, discovery_errors


# =============================================================================
# VALIDATION CORE
# =============================================================================


def validate_csv(
    timeframe: str,
    market: str,
    path: Path,
) -> ValidationResult:
    result = ValidationResult(
        timeframe=timeframe,
        market=market,
        path=path,
    )

    # -------------------------------------------------------------------------
    # READ CSV
    # -------------------------------------------------------------------------

    try:
        df = pd.read_csv(
            path,
            low_memory=False,
        )

        result.csv_readable = True

    except Exception as exc:
        result.error = (
            f"CSV read failed: "
            f"{type(exc).__name__}: {exc}"
        )
        return result

    result.rows = len(df)

    # -------------------------------------------------------------------------
    # EMPTY CHECK
    # -------------------------------------------------------------------------

    if result.rows <= 0:
        result.error = "CSV contains zero rows."
        return result

    result.non_empty = True

    # -------------------------------------------------------------------------
    # REQUIRED COLUMN CHECK
    # -------------------------------------------------------------------------

    missing_columns = [
        column
        for column in REQUIRED_COLUMNS
        if column not in df.columns
    ]

    if missing_columns:
        result.error = (
            "Missing required columns: "
            + ", ".join(missing_columns)
        )
        return result

    result.required_columns_ok = True

    # -------------------------------------------------------------------------
    # PARSE candle_date_time_utc
    # -------------------------------------------------------------------------

    try:
        candle_time = pd.to_datetime(
            df["candle_date_time_utc"],
            utc=True,
            errors="coerce",
        )

    except Exception as exc:
        result.error = (
            "Failed to parse candle_date_time_utc: "
            f"{type(exc).__name__}: {exc}"
        )
        return result

    # -------------------------------------------------------------------------
    # PARSE timestamp
    # -------------------------------------------------------------------------

    try:
        timestamp = pd.to_numeric(
            df["timestamp"],
            errors="coerce",
        )

    except Exception as exc:
        result.error = (
            "Failed to parse timestamp: "
            f"{type(exc).__name__}: {exc}"
        )
        return result

    # -------------------------------------------------------------------------
    # BASIC COUNTS
    # -------------------------------------------------------------------------

    result.timestamp_nan = safe_int(
        timestamp.isna().sum()
    )

    result.utc_nat = safe_int(
        candle_time.isna().sum()
    )

    result.utc_duplicates = safe_int(
        candle_time.duplicated().sum()
    )

    # -------------------------------------------------------------------------
    # CHRONOLOGICAL CHECK
    # -------------------------------------------------------------------------

    if result.utc_nat == 0:
        result.utc_chronological = bool(
            candle_time.is_monotonic_increasing
        )
    else:
        result.utc_chronological = False

    if result.timestamp_nan == 0:
        result.timestamp_chronological = bool(
            timestamp.is_monotonic_increasing
        )
    else:
        result.timestamp_chronological = False

    # -------------------------------------------------------------------------
    # CANONICAL TIMESTAMP CHECK
    #
    # candle_date_time_utc:
    #     UTC datetime
    #
    # expected_timestamp:
    #     Unix epoch milliseconds
    #
    # IMPORTANT:
    #     We only compare rows where both values are valid.
    #     Invalid rows are already counted separately as NaT / NaN.
    # -------------------------------------------------------------------------

    valid_mask = (
        candle_time.notna()
        & timestamp.notna()
    )

    if bool(valid_mask.any()):
        try:
            valid_candle_time = candle_time.loc[valid_mask]

            expected_timestamp = (
                valid_candle_time.astype("int64")
                // 1_000_000
            )

            actual_timestamp = timestamp.loc[valid_mask]

            # Convert both sides to pandas nullable Int64.
            #
            # timestamp may have been loaded as float because old CSV files
            # historically contained values such as:
            #
            #     1506311839886.0
            #
            # After canonical repair the numeric value must still exactly
            # equal the epoch milliseconds derived from candle_date_time_utc.
            expected_int = pd.Series(
                expected_timestamp,
                index=valid_candle_time.index,
            ).astype("Int64")

            actual_int = (
                actual_timestamp
                .round()
                .astype("Int64")
            )

            mismatch_mask = (
                actual_int != expected_int
            ).fillna(True)

            result.timestamp_mismatch = safe_int(
                mismatch_mask.sum()
            )

        except Exception as exc:
            result.error = (
                "Canonical timestamp comparison failed: "
                f"{type(exc).__name__}: {exc}"
            )
            return result

    else:
        # No valid pair exists.
        #
        # NaN / NaT checks will already fail this file, but assigning all rows
        # as mismatches makes the diagnostic output unambiguous.
        result.timestamp_mismatch = result.rows

    # -------------------------------------------------------------------------
    # FINAL RESULT
    # -------------------------------------------------------------------------

    result.passed = bool(
        result.csv_readable
        and result.required_columns_ok
        and result.non_empty
        and result.timestamp_nan == 0
        and result.utc_nat == 0
        and result.utc_duplicates == 0
        and result.utc_chronological
        and result.timestamp_chronological
        and result.timestamp_mismatch == 0
        and not result.error
    )

    return result


# =============================================================================
# OUTPUT
# =============================================================================


def print_header(
    base_dir: Path,
    market: Optional[str],
    timeframes: Sequence[str],
    total_jobs: int,
) -> None:
    print(SEPARATOR)
    print(f"{PROJECT_NAME} - {VERSION}")
    print("STEP 2B - OHLCV CANONICAL TIMESTAMP VALIDATION")
    print(SEPARATOR)

    print(f"Started UTC : {utc_now_iso()}")
    print(f"Base dir    : {base_dir}")

    print()
    print("Mode:")
    print("  READ ONLY")
    print("  OHLCV modification DISABLED")
    print("  Timestamp repair DISABLED")
    print("  Feature generation DISABLED")
    print("  Signal generation DISABLED")
    print("  Prediction DISABLED")
    print("  Trading DISABLED")

    print()
    print("Timestamp policy:")
    print("  Source        : candle_date_time_utc")
    print("  Unit          : Unix epoch milliseconds")
    print("  API timestamp : NOT USED for validation truth")

    print()
    print("Validation checks:")
    print("  CSV readable")
    print("  Required columns")
    print("  Non-empty CSV")
    print("  timestamp NaN")
    print("  UTC NaT")
    print("  UTC duplicates")
    print("  UTC chronological")
    print("  timestamp chronological")
    print("  UTC/timestamp exact match")

    print()
    print("Selection:")

    if market:
        print(f"  Market        : {market}")
    else:
        print("  Market        : ALL KRW CSV FILES")

    print(
        "  Timeframes    : "
        + " / ".join(timeframes)
    )

    print(f"  Total jobs    : {total_jobs:,}")

    print(SEPARATOR)


def print_result(
    index: int,
    total: int,
    result: ValidationResult,
) -> None:
    status = "PASS" if result.passed else "FAIL"

    print(
        f"[{index:03d}/{total:03d}] "
        f"[{status}] "
        f"{result.market} "
        f"{result.timeframe.upper()} "
        f"rows={result.rows:,} "
        f"nan={result.timestamp_nan:,} "
        f"nat={result.utc_nat:,} "
        f"dup={result.utc_duplicates:,} "
        f"utc_order={result.utc_chronological} "
        f"ts_order={result.timestamp_chronological} "
        f"mismatch={result.timestamp_mismatch:,}"
    )

    if result.error:
        print(
            f"          ERROR: {result.error}"
        )


def print_failed_detail(
    failed_results: Sequence[ValidationResult],
) -> None:
    if not failed_results:
        return

    print()
    print(SEPARATOR)
    print("FAILED JOB DETAILS")
    print(SEPARATOR)

    for result in failed_results:
        print(
            f"{result.market} / "
            f"{result.timeframe.upper()}"
        )

        print(f"  Path                   : {result.path}")
        print(f"  Rows                   : {result.rows:,}")
        print(f"  CSV readable           : {result.csv_readable}")
        print(
            f"  Required columns       : "
            f"{result.required_columns_ok}"
        )
        print(f"  Non-empty              : {result.non_empty}")
        print(
            f"  timestamp NaN          : "
            f"{result.timestamp_nan:,}"
        )
        print(
            f"  UTC NaT                : "
            f"{result.utc_nat:,}"
        )
        print(
            f"  UTC duplicates         : "
            f"{result.utc_duplicates:,}"
        )
        print(
            f"  UTC chronological      : "
            f"{result.utc_chronological}"
        )
        print(
            f"  timestamp chronological: "
            f"{result.timestamp_chronological}"
        )
        print(
            f"  UTC/timestamp mismatch : "
            f"{result.timestamp_mismatch:,}"
        )

        if result.error:
            print(f"  Error                  : {result.error}")

        print(SUB_SEPARATOR)


def print_summary(
    results: Sequence[ValidationResult],
    discovery_errors: Sequence[str],
    elapsed_seconds: float,
    market: Optional[str],
    timeframes: Sequence[str],
) -> None:
    total_jobs = len(results)

    passed_jobs = sum(
        1
        for result in results
        if result.passed
    )

    failed_jobs = total_jobs - passed_jobs

    total_rows = sum(
        result.rows
        for result in results
    )

    total_timestamp_nan = sum(
        result.timestamp_nan
        for result in results
    )

    total_utc_nat = sum(
        result.utc_nat
        for result in results
    )

    total_utc_duplicates = sum(
        result.utc_duplicates
        for result in results
    )

    total_timestamp_mismatch = sum(
        result.timestamp_mismatch
        for result in results
    )

    utc_order_failures = sum(
        1
        for result in results
        if result.csv_readable
        and result.required_columns_ok
        and result.non_empty
        and not result.utc_chronological
    )

    timestamp_order_failures = sum(
        1
        for result in results
        if result.csv_readable
        and result.required_columns_ok
        and result.non_empty
        and not result.timestamp_chronological
    )

    markets = sorted(
        {
            result.market
            for result in results
        }
    )

    print()
    print(SEPARATOR)
    print("OHLCV TIMESTAMP VALIDATION SUMMARY")
    print(SEPARATOR)

    print(f"Project                 : {PROJECT_NAME}")
    print(f"Version                 : {VERSION}")

    if market:
        print("Execution mode          : SINGLE MARKET")
        print(f"Selected market         : {market}")
    else:
        print("Execution mode          : ALL KRW CSV FILES")

    print(
        "Timeframes              : "
        + " / ".join(timeframes)
    )

    print(f"Markets discovered      : {len(markets):,}")
    print(f"Total jobs              : {total_jobs:,}")
    print(f"Passed jobs             : {passed_jobs:,}")
    print(f"Failed jobs             : {failed_jobs:,}")
    print(
        f"Discovery errors        : "
        f"{len(discovery_errors):,}"
    )
    print(f"Total rows              : {total_rows:,}")
    print(f"timestamp NaN           : {total_timestamp_nan:,}")
    print(f"UTC NaT                 : {total_utc_nat:,}")
    print(f"UTC duplicates          : {total_utc_duplicates:,}")
    print(
        f"UTC order failures      : "
        f"{utc_order_failures:,}"
    )
    print(
        f"Timestamp order failures: "
        f"{timestamp_order_failures:,}"
    )
    print(
        f"UTC/timestamp mismatch  : "
        f"{total_timestamp_mismatch:,}"
    )
    print(f"Elapsed seconds         : {elapsed_seconds:.2f}")

    print()
    print("Timestamp policy:")
    print("  Source                : candle_date_time_utc")
    print("  Unit                  : Unix epoch milliseconds")
    print("  API timestamp         : NOT USED")

    print()
    print("Safety:")
    print("  OHLCV write           : DISABLED")
    print("  Timestamp repair      : DISABLED")
    print("  Row deletion          : DISABLED")
    print("  Row reorder           : DISABLED")
    print("  Feature generation    : DISABLED")
    print("  Signal generation     : DISABLED")
    print("  Prediction            : DISABLED")
    print("  Trading               : DISABLED")

    if discovery_errors:
        print()
        print("Discovery errors:")

        for error in discovery_errors:
            print(f"  - {error}")

    print(SEPARATOR)


# =============================================================================
# MAIN
# =============================================================================


def main() -> int:
    args = parse_args()

    started = time.perf_counter()

    base_dir = get_base_dir(args.base_dir)

    market = normalize_market(args.market)

    timeframes = get_selected_timeframes(
        args.timeframe
    )

    # -------------------------------------------------------------------------
    # MARKET ARGUMENT SAFETY
    # -------------------------------------------------------------------------

    if market is not None:
        if not market.startswith("KRW-"):
            print(SEPARATOR)
            print("[ERROR] Invalid market.")
            print(
                "Market must use KRW-* format. "
                "Example: KRW-BTC"
            )
            print(SEPARATOR)
            return 2

    # -------------------------------------------------------------------------
    # DISCOVERY
    # -------------------------------------------------------------------------

    jobs, discovery_errors = discover_csv_files(
        base_dir=base_dir,
        timeframes=timeframes,
        market=market,
    )

    print_header(
        base_dir=base_dir,
        market=market,
        timeframes=timeframes,
        total_jobs=len(jobs),
    )

    if discovery_errors:
        print()
        print("Discovery diagnostics:")

        for error in discovery_errors:
            print(f"  [ERROR] {error}")

    if not jobs:
        print()
        print(SEPARATOR)
        print("[RESULT] OHLCV TIMESTAMP VALIDATION FAILED")
        print("[ERROR] No validation jobs were discovered.")
        print(SEPARATOR)
        return 1

    # -------------------------------------------------------------------------
    # VALIDATE
    # -------------------------------------------------------------------------

    results: List[ValidationResult] = []

    print()
    print("Validating OHLCV CSV files...")
    print(SUB_SEPARATOR)

    total_jobs = len(jobs)

    for index, (
        timeframe,
        job_market,
        path,
    ) in enumerate(
        jobs,
        start=1,
    ):
        result = validate_csv(
            timeframe=timeframe,
            market=job_market,
            path=path,
        )

        results.append(result)

        print_result(
            index=index,
            total=total_jobs,
            result=result,
        )

    # -------------------------------------------------------------------------
    # RESULT
    # -------------------------------------------------------------------------

    elapsed = (
        time.perf_counter()
        - started
    )

    failed_results = [
        result
        for result in results
        if not result.passed
    ]

    print_failed_detail(
        failed_results
    )

    print_summary(
        results=results,
        discovery_errors=discovery_errors,
        elapsed_seconds=elapsed,
        market=market,
        timeframes=timeframes,
    )

    # -------------------------------------------------------------------------
    # FINAL PASS / FAIL
    # -------------------------------------------------------------------------

    if discovery_errors:
        print(
            "[RESULT] OHLCV TIMESTAMP VALIDATION FAILED"
        )
        print(
            "[FAIL] One or more required OHLCV files/directories "
            "were not discovered."
        )
        return 1

    if failed_results:
        print(
            "[RESULT] OHLCV TIMESTAMP VALIDATION FAILED"
        )
        print(
            f"[FAIL] {len(failed_results):,} "
            f"of {len(results):,} jobs failed."
        )
        print(
            "[STOP] Do not regenerate Feature data until "
            "OHLCV timestamp failures are resolved."
        )
        return 1

    print(
        "[RESULT] ALL OHLCV TIMESTAMPS PASSED"
    )
    print(
        f"[PASS] {len(results):,} / "
        f"{len(results):,} validation jobs passed."
    )
    print(
        "[PASS] Canonical timestamp integrity confirmed."
    )
    print(
        "[PASS] OHLCV CSV files remained unchanged."
    )
    print(
        "[NEXT] Full Feature regeneration."
    )

    return 0


# =============================================================================
# ENTRY POINT
# =============================================================================


if __name__ == "__main__":
    try:
        sys.exit(main())

    except KeyboardInterrupt:
        print()
        print(SEPARATOR)
        print("[INTERRUPTED] Validation stopped by user.")
        print("[INFO] OHLCV files were not modified.")
        print(SEPARATOR)
        sys.exit(130)

    except Exception as exc:
        print()
        print(SEPARATOR)
        print("[FATAL] Unexpected validator error.")
        print(
            f"{type(exc).__name__}: {exc}"
        )
        print("[INFO] OHLCV files were not modified.")
        print(SEPARATOR)
        sys.exit(1)
