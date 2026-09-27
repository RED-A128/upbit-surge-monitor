"""
Upbit Surge Monitor
OHLCV Data History Audit

Version:
    Clean V001

File:
    data_history_audit.py

Purpose:
    Measure how much OHLCV history has actually accumulated
    for research use.

Main responsibilities:
    - Read existing h1 / h4 / d1 OHLCV CSV files.
    - Measure row count for every market.
    - Measure first and last candle timestamps.
    - Measure actual history span.
    - Measure latest-candle age.
    - Summarize shortest / median / average / longest history.
    - Identify markets with relatively short histories.
    - Identify stale datasets.
    - Compare history availability across timeframes.

Important:
    - READ ONLY.
    - NEVER modifies OHLCV source files.
    - NEVER deletes historical data.
    - Does NOT perform pattern analysis.
    - Does NOT generate trading signals.
    - Does NOT perform prediction.
    - Does NOT perform machine learning.
    - A short history is NOT automatically an error.
    - Newly listed markets naturally may have shorter history.

Windows execution:

    py data_history_audit.py

Expected structure:

    data/
        ohlcv/
            h1/
            h4/
            d1/
"""

from __future__ import annotations

import math
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Iterable

import pandas as pd


# ============================================================
# PROJECT CONFIGURATION
# ============================================================

PROJECT_NAME = "Upbit Surge Monitor"
VERSION = "Clean V001"

BASE_DIR = Path(__file__).resolve().parent

DATA_DIR = BASE_DIR / "data"
OHLCV_DIR = DATA_DIR / "ohlcv"

TIMEFRAME_DIRS = {
    "h1": OHLCV_DIR / "h1",
    "h4": OHLCV_DIR / "h4",
    "d1": OHLCV_DIR / "d1",
}

TIMEFRAME_EXPECTED_INTERVAL_HOURS = {
    "h1": 1.0,
    "h4": 4.0,
    "d1": 24.0,
}

TIMESTAMP_CANDIDATES = (
    "candle_date_time_utc",
    "timestamp",
    "datetime",
    "candle_date_time_kst",
)

# This audit intentionally does NOT define a hard
# "research-ready" history requirement yet.
#
# We first measure the real dataset distribution.
# A minimum-history rule can be introduced later only after
# reviewing the actual accumulated data.

SHORT_HISTORY_RATIO = 0.50

# A dataset is considered stale for audit reporting when the
# newest candle is much older than the normal timeframe.
#
# This is informational only.
#
# It does NOT automatically make the entire audit fail because
# the purpose of this script is history measurement rather than
# Collector integrity validation.

STALE_INTERVAL_MULTIPLIER = 3.0


# ============================================================
# RESULT MODELS
# ============================================================

@dataclass
class MarketHistoryResult:
    timeframe: str
    market: str
    path: Path

    readable: bool = True

    row_count: int = 0

    timestamp_column: str = ""

    first_timestamp: pd.Timestamp | None = None
    last_timestamp: pd.Timestamp | None = None

    history_hours: float = 0.0
    history_days: float = 0.0

    latest_age_hours: float = 0.0
    latest_age_days: float = 0.0

    duplicate_timestamp_count: int = 0
    invalid_timestamp_count: int = 0

    is_short_history: bool = False
    is_stale: bool = False

    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


@dataclass
class TimeframeAuditResult:
    timeframe: str
    directory: Path

    directory_exists: bool = False

    file_count: int = 0
    readable_file_count: int = 0
    unreadable_file_count: int = 0

    total_rows: int = 0

    markets: list[MarketHistoryResult] = field(
        default_factory=list
    )

    short_history_markets: list[str] = field(
        default_factory=list
    )

    stale_markets: list[str] = field(
        default_factory=list
    )

    errors: list[str] = field(
        default_factory=list
    )

    warnings: list[str] = field(
        default_factory=list
    )


# ============================================================
# DISPLAY HELPERS
# ============================================================

def print_separator(
    char: str = "=",
    width: int = 88,
) -> None:
    print(char * width)


def print_header(
    title: str,
) -> None:
    print()
    print_separator("=")
    print(title)
    print_separator("=")


def format_integer(
    value: int,
) -> str:
    return f"{value:,}"


def format_float(
    value: float,
    decimals: int = 2,
) -> str:
    if not math.isfinite(value):
        return "N/A"

    return f"{value:,.{decimals}f}"


def format_timestamp(
    value: pd.Timestamp | None,
) -> str:
    if value is None:
        return "N/A"

    return value.strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )


def format_duration_days(
    days: float,
) -> str:
    if not math.isfinite(days):
        return "N/A"

    if days < 1.0:
        hours = days * 24.0

        return (
            f"{hours:,.2f} hours "
            f"({days:,.3f} days)"
        )

    if days < 365.0:
        return f"{days:,.2f} days"

    years = days / 365.2425

    return (
        f"{days:,.2f} days "
        f"({years:,.2f} years)"
    )


# ============================================================
# COLUMN HELPERS
# ============================================================

def normalize_column_name(
    column: str,
) -> str:
    return str(column).strip().lower()


def build_column_lookup(
    columns: Iterable[str],
) -> dict[str, str]:

    lookup: dict[str, str] = {}

    for column in columns:

        normalized = normalize_column_name(
            column
        )

        if normalized not in lookup:
            lookup[normalized] = str(column)

    return lookup


def find_timestamp_column(
    columns: Iterable[str],
) -> str | None:

    lookup = build_column_lookup(
        columns
    )

    for candidate in TIMESTAMP_CANDIDATES:

        if candidate in lookup:
            return lookup[candidate]

    return None


# ============================================================
# TIME HELPERS
# ============================================================

def get_now_utc() -> pd.Timestamp:
    return pd.Timestamp(
        datetime.now(timezone.utc)
    )


def calculate_history_hours(
    first_timestamp: pd.Timestamp,
    last_timestamp: pd.Timestamp,
) -> float:

    delta = (
        last_timestamp
        - first_timestamp
    )

    return max(
        0.0,
        delta.total_seconds() / 3600.0,
    )


def calculate_latest_age_hours(
    last_timestamp: pd.Timestamp,
    now_utc: pd.Timestamp,
) -> float:

    delta = (
        now_utc
        - last_timestamp
    )

    return max(
        0.0,
        delta.total_seconds() / 3600.0,
    )


# ============================================================
# CSV HISTORY READER
# ============================================================

def audit_market_file(
    timeframe: str,
    path: Path,
    now_utc: pd.Timestamp,
) -> MarketHistoryResult:

    result = MarketHistoryResult(
        timeframe=timeframe,
        market=path.stem.upper(),
        path=path,
    )

    try:

        if not path.exists():

            result.readable = False

            result.errors.append(
                "File does not exist."
            )

            return result

        if path.stat().st_size == 0:

            result.readable = False

            result.errors.append(
                "File is empty."
            )

            return result

        # ----------------------------------------------------
        # Read only the header first.
        # ----------------------------------------------------

        try:

            header_df = pd.read_csv(
                path,
                nrows=0,
            )

        except Exception as exc:

            result.readable = False

            result.errors.append(
                "CSV header read failed: "
                f"{type(exc).__name__}: {exc}"
            )

            return result

        timestamp_column = find_timestamp_column(
            header_df.columns
        )

        if timestamp_column is None:

            result.readable = False

            result.errors.append(
                "Timestamp column not found. "
                f"Expected one of: "
                f"{', '.join(TIMESTAMP_CANDIDATES)}"
            )

            return result

        result.timestamp_column = (
            timestamp_column
        )

        # ----------------------------------------------------
        # For the history audit we only need the timestamp
        # column.
        #
        # This avoids loading every OHLCV column again.
        # ----------------------------------------------------

        try:

            df = pd.read_csv(
                path,
                usecols=[
                    timestamp_column,
                ],
                low_memory=False,
            )

        except Exception as exc:

            result.readable = False

            result.errors.append(
                "Timestamp data read failed: "
                f"{type(exc).__name__}: {exc}"
            )

            return result

        result.row_count = len(df)

        if result.row_count == 0:

            result.readable = False

            result.errors.append(
                "CSV contains zero rows."
            )

            return result

        timestamps = pd.to_datetime(
            df[timestamp_column],
            errors="coerce",
            utc=True,
        )

        result.invalid_timestamp_count = int(
            timestamps.isna().sum()
        )

        valid_timestamps = (
            timestamps
            .dropna()
            .sort_values()
            .reset_index(drop=True)
        )

        if valid_timestamps.empty:

            result.readable = False

            result.errors.append(
                "No valid timestamps found."
            )

            return result

        result.duplicate_timestamp_count = int(
            valid_timestamps.duplicated(
                keep=False
            ).sum()
        )

        result.first_timestamp = (
            valid_timestamps.iloc[0]
        )

        result.last_timestamp = (
            valid_timestamps.iloc[-1]
        )

        result.history_hours = (
            calculate_history_hours(
                first_timestamp=(
                    result.first_timestamp
                ),
                last_timestamp=(
                    result.last_timestamp
                ),
            )
        )

        result.history_days = (
            result.history_hours / 24.0
        )

        result.latest_age_hours = (
            calculate_latest_age_hours(
                last_timestamp=(
                    result.last_timestamp
                ),
                now_utc=now_utc,
            )
        )

        result.latest_age_days = (
            result.latest_age_hours / 24.0
        )

        if result.invalid_timestamp_count > 0:

            result.warnings.append(
                "Invalid timestamps: "
                f"{result.invalid_timestamp_count:,}"
            )

        if result.duplicate_timestamp_count > 0:

            result.warnings.append(
                "Duplicate timestamps: "
                f"{result.duplicate_timestamp_count:,}"
            )

    except Exception as exc:

        result.readable = False

        result.errors.append(
            "Unexpected audit error: "
            f"{type(exc).__name__}: {exc}"
        )

    return result


# ============================================================
# TIMEFRAME AUDIT
# ============================================================

def audit_timeframe(
    timeframe: str,
    directory: Path,
    now_utc: pd.Timestamp,
) -> TimeframeAuditResult:

    result = TimeframeAuditResult(
        timeframe=timeframe,
        directory=directory,
    )

    print()
    print_separator("-")
    print(
        f"AUDITING TIMEFRAME: "
        f"{timeframe.upper()}"
    )
    print_separator("-")

    print(
        f"Directory : {directory}"
    )

    if not directory.exists():

        result.errors.append(
            f"Directory not found: {directory}"
        )

        print(
            "[FAIL] Directory not found."
        )

        return result

    if not directory.is_dir():

        result.errors.append(
            f"Path is not a directory: {directory}"
        )

        print(
            "[FAIL] Path is not a directory."
        )

        return result

    result.directory_exists = True

    files = sorted(
        directory.glob("*.csv"),
        key=lambda item: item.name.upper(),
    )

    result.file_count = len(files)

    print(
        f"CSV files : {result.file_count:,}"
    )

    if not files:

        result.errors.append(
            "No CSV files found."
        )

        print(
            "[FAIL] No CSV files found."
        )

        return result

    total_files = len(files)

    print()
    print(
        "Reading market history..."
    )
    print()

    for index, path in enumerate(
        files,
        start=1,
    ):

        market_result = audit_market_file(
            timeframe=timeframe,
            path=path,
            now_utc=now_utc,
        )

        result.markets.append(
            market_result
        )

        result.total_rows += (
            market_result.row_count
        )

        if market_result.readable:

            result.readable_file_count += 1
            status = "OK"

        else:

            result.unreadable_file_count += 1
            status = "FAIL"

        print(
            f"[{index:03d}/{total_files:03d}] "
            f"{market_result.market:<18} "
            f"{status:<4} "
            f"rows={market_result.row_count:>7,} "
            f"history={market_result.history_days:>9.2f}d "
            f"age={market_result.latest_age_hours:>8.2f}h"
        )

        for error in market_result.errors:

            print(
                f"    [ERROR] {error}"
            )

        for warning in market_result.warnings:

            print(
                f"    [WARN ] {warning}"
            )

    return result


# ============================================================
# RELATIVE HISTORY CLASSIFICATION
# ============================================================

def classify_relative_history(
    result: TimeframeAuditResult,
) -> None:
    """
    Mark markets whose history is less than a percentage of the
    median history span for that timeframe.

    This is NOT a pass/fail research requirement.

    It is only a diagnostic flag used to identify relatively
    short datasets.
    """

    readable = [
        item
        for item in result.markets
        if item.readable
    ]

    if not readable:
        return

    history_days = [
        item.history_days
        for item in readable
    ]

    median_days = median(
        history_days
    )

    if median_days <= 0:
        return

    threshold_days = (
        median_days
        * SHORT_HISTORY_RATIO
    )

    for item in readable:

        if (
            item.history_days
            < threshold_days
        ):

            item.is_short_history = True

            result.short_history_markets.append(
                item.market
            )


# ============================================================
# STALE CLASSIFICATION
# ============================================================

def classify_stale_data(
    result: TimeframeAuditResult,
) -> None:
    """
    Identify datasets whose newest candle is substantially older
    than the normal interval for that timeframe.

    This is informational only.

    Collector integrity is validated separately by
    validate_collector.py.
    """

    expected_interval_hours = (
        TIMEFRAME_EXPECTED_INTERVAL_HOURS[
            result.timeframe
        ]
    )

    threshold_hours = (
        expected_interval_hours
        * STALE_INTERVAL_MULTIPLIER
    )

    for item in result.markets:

        if not item.readable:
            continue

        if (
            item.latest_age_hours
            > threshold_hours
        ):

            item.is_stale = True

            result.stale_markets.append(
                item.market
            )


# ============================================================
# STATISTICS HELPERS
# ============================================================

def get_readable_markets(
    result: TimeframeAuditResult,
) -> list[MarketHistoryResult]:

    return [
        item
        for item in result.markets
        if item.readable
    ]


def get_history_day_values(
    result: TimeframeAuditResult,
) -> list[float]:

    return [
        item.history_days
        for item in get_readable_markets(
            result
        )
    ]


def get_row_count_values(
    result: TimeframeAuditResult,
) -> list[int]:

    return [
        item.row_count
        for item in get_readable_markets(
            result
        )
    ]


def calculate_float_stats(
    values: list[float],
) -> tuple[
    float,
    float,
    float,
    float,
]:

    if not values:

        return (
            math.nan,
            math.nan,
            math.nan,
            math.nan,
        )

    return (
        min(values),
        median(values),
        mean(values),
        max(values),
    )


def calculate_int_stats(
    values: list[int],
) -> tuple[
    int,
    float,
    float,
    int,
]:

    if not values:

        return (
            0,
            0.0,
            0.0,
            0,
        )

    return (
        min(values),
        median(values),
        mean(values),
        max(values),
    )


# ============================================================
# MARKET EXTREME HELPERS
# ============================================================

def get_shortest_history_market(
    result: TimeframeAuditResult,
) -> MarketHistoryResult | None:

    readable = get_readable_markets(
        result
    )

    if not readable:
        return None

    return min(
        readable,
        key=lambda item: item.history_days,
    )


def get_longest_history_market(
    result: TimeframeAuditResult,
) -> MarketHistoryResult | None:

    readable = get_readable_markets(
        result
    )

    if not readable:
        return None

    return max(
        readable,
        key=lambda item: item.history_days,
    )


def get_oldest_latest_candle(
    result: TimeframeAuditResult,
) -> MarketHistoryResult | None:

    readable = get_readable_markets(
        result
    )

    if not readable:
        return None

    return max(
        readable,
        key=lambda item: item.latest_age_hours,
    )


# ============================================================
# TIMEFRAME SUMMARY
# ============================================================

def print_timeframe_statistics(
    result: TimeframeAuditResult,
) -> None:

    print()
    print_separator("=")
    print(
        f"{result.timeframe.upper()} "
        f"HISTORY STATISTICS"
    )
    print_separator("=")

    readable = get_readable_markets(
        result
    )

    history_days = get_history_day_values(
        result
    )

    row_counts = get_row_count_values(
        result
    )

    (
        min_days,
        median_days,
        average_days,
        max_days,
    ) = calculate_float_stats(
        history_days
    )

    (
        min_rows,
        median_rows,
        average_rows,
        max_rows,
    ) = calculate_int_stats(
        row_counts
    )

    print(
        f"CSV files            : "
        f"{result.file_count:,}"
    )

    print(
        f"Readable files       : "
        f"{result.readable_file_count:,}"
    )

    print(
        f"Unreadable files     : "
        f"{result.unreadable_file_count:,}"
    )

    print(
        f"Total rows           : "
        f"{result.total_rows:,}"
    )

    print()

    print(
        "History span:"
    )

    print(
        f"  Shortest           : "
        f"{format_duration_days(min_days)}"
    )

    print(
        f"  Median             : "
        f"{format_duration_days(median_days)}"
    )

    print(
        f"  Average            : "
        f"{format_duration_days(average_days)}"
    )

    print(
        f"  Longest            : "
        f"{format_duration_days(max_days)}"
    )

    print()

    print(
        "Rows per market:"
    )

    print(
        f"  Minimum            : "
        f"{min_rows:,}"
    )

    print(
        f"  Median             : "
        f"{median_rows:,.2f}"
    )

    print(
        f"  Average            : "
        f"{average_rows:,.2f}"
    )

    print(
        f"  Maximum            : "
        f"{max_rows:,}"
    )

    shortest = get_shortest_history_market(
        result
    )

    longest = get_longest_history_market(
        result
    )

    oldest_latest = get_oldest_latest_candle(
        result
    )

    if shortest is not None:

        print()
        print(
            "Shortest-history market:"
        )

        print(
            f"  Market             : "
            f"{shortest.market}"
        )

        print(
            f"  Rows               : "
            f"{shortest.row_count:,}"
        )

        print(
            f"  First candle       : "
            f"{format_timestamp(shortest.first_timestamp)}"
        )

        print(
            f"  Last candle        : "
            f"{format_timestamp(shortest.last_timestamp)}"
        )

        print(
            f"  History            : "
            f"{format_duration_days(shortest.history_days)}"
        )

    if longest is not None:

        print()
        print(
            "Longest-history market:"
        )

        print(
            f"  Market             : "
            f"{longest.market}"
        )

        print(
            f"  Rows               : "
            f"{longest.row_count:,}"
        )

        print(
            f"  First candle       : "
            f"{format_timestamp(longest.first_timestamp)}"
        )

        print(
            f"  Last candle        : "
            f"{format_timestamp(longest.last_timestamp)}"
        )

        print(
            f"  History            : "
            f"{format_duration_days(longest.history_days)}"
        )

    if oldest_latest is not None:

        print()
        print(
            "Oldest latest-candle:"
        )

        print(
            f"  Market             : "
            f"{oldest_latest.market}"
        )

        print(
            f"  Last candle        : "
            f"{format_timestamp(oldest_latest.last_timestamp)}"
        )

        print(
            f"  Age                : "
            f"{oldest_latest.latest_age_hours:,.2f} hours"
        )

    print()

    print(
        f"Relatively short histories "
        f"(< {SHORT_HISTORY_RATIO:.0%} of median): "
        f"{len(result.short_history_markets):,}"
    )

    print(
        f"Stale-data flags     : "
        f"{len(result.stale_markets):,}"
    )

    if result.unreadable_file_count > 0:

        print()
        print(
            "[WARN] One or more CSV files "
            "could not be audited."
        )

    if not readable:

        print()
        print(
            "[WARN] No readable market history "
            "was available."
        )


# ============================================================
# SHORT HISTORY REPORT
# ============================================================

def print_short_history_report(
    result: TimeframeAuditResult,
) -> None:

    short_items = [
        item
        for item in result.markets
        if item.is_short_history
    ]

    if not short_items:
        return

    short_items = sorted(
        short_items,
        key=lambda item: (
            item.history_days,
            item.market,
        ),
    )

    print()
    print_separator("-")
    print(
        f"{result.timeframe.upper()} "
        f"RELATIVELY SHORT HISTORIES"
    )
    print_separator("-")

    print(
        "This is diagnostic only."
    )

    print(
        "Short history does NOT mean invalid data."
    )

    print()

    for item in short_items:

        print(
            f"{item.market:<18} "
            f"rows={item.row_count:>7,} "
            f"history={item.history_days:>10.2f}d "
            f"first={format_timestamp(item.first_timestamp)}"
        )


# ============================================================
# STALE DATA REPORT
# ============================================================

def print_stale_report(
    result: TimeframeAuditResult,
) -> None:

    stale_items = [
        item
        for item in result.markets
        if item.is_stale
    ]

    if not stale_items:
        return

    stale_items = sorted(
        stale_items,
        key=lambda item: (
            -item.latest_age_hours,
            item.market,
        ),
    )

    print()
    print_separator("-")
    print(
        f"{result.timeframe.upper()} "
        f"STALE-DATA FLAGS"
    )
    print_separator("-")

    expected_interval_hours = (
        TIMEFRAME_EXPECTED_INTERVAL_HOURS[
            result.timeframe
        ]
    )

    threshold_hours = (
        expected_interval_hours
        * STALE_INTERVAL_MULTIPLIER
    )

    print(
        f"Expected interval     : "
        f"{expected_interval_hours:,.2f} hours"
    )

    print(
        f"Audit stale threshold : "
        f"{threshold_hours:,.2f} hours"
    )

    print()

    print(
        "This is diagnostic only."
    )

    print(
        "A stale flag does NOT automatically "
        "make the audit fail."
    )

    print()

    for item in stale_items:

        print(
            f"{item.market:<18} "
            f"age={item.latest_age_hours:>10.2f}h "
            f"last={format_timestamp(item.last_timestamp)}"
        )


# ============================================================
# UNREADABLE FILE REPORT
# ============================================================

def print_unreadable_report(
    result: TimeframeAuditResult,
) -> None:

    unreadable = [
        item
        for item in result.markets
        if not item.readable
    ]

    if not unreadable:
        return

    print()
    print_separator("-")
    print(
        f"{result.timeframe.upper()} "
        f"UNREADABLE FILES"
    )
    print_separator("-")

    for item in unreadable:

        print(
            f"{item.market}"
        )

        print(
            f"  Path: {item.path}"
        )

        for error in item.errors:

            print(
                f"  ERROR: {error}"
            )


# ============================================================
# CROSS-TIMEFRAME MARKET COVERAGE
# ============================================================

def get_market_set(
    result: TimeframeAuditResult,
) -> set[str]:

    return {
        item.market
        for item in result.markets
    }


def print_cross_timeframe_coverage(
    results: dict[
        str,
        TimeframeAuditResult,
    ],
) -> int:

    print()
    print_separator("=")
    print(
        "CROSS-TIMEFRAME HISTORY COVERAGE"
    )
    print_separator("=")

    h1_markets = get_market_set(
        results["h1"]
    )

    h4_markets = get_market_set(
        results["h4"]
    )

    d1_markets = get_market_set(
        results["d1"]
    )

    all_markets = (
        h1_markets
        | h4_markets
        | d1_markets
    )

    mismatch_count = 0

    for market in sorted(
        all_markets
    ):

        missing: list[str] = []

        if market not in h1_markets:
            missing.append("h1")

        if market not in h4_markets:
            missing.append("h4")

        if market not in d1_markets:
            missing.append("d1")

        if missing:

            mismatch_count += 1

            print(
                f"[MISMATCH] {market}: "
                f"missing from "
                f"{', '.join(missing)}"
            )

    if mismatch_count == 0:

        print(
            "[PASS] h1 / h4 / d1 contain "
            "the same stored market set."
        )

    else:

        print(
            f"[WARN] Markets with timeframe "
            f"coverage differences: "
            f"{mismatch_count:,}"
        )

    return mismatch_count


# ============================================================
# OVERALL SUMMARY
# ============================================================

def print_overall_summary(
    results: dict[
        str,
        TimeframeAuditResult,
    ],
    mismatch_count: int,
) -> None:

    total_files = sum(
        result.file_count
        for result in results.values()
    )

    readable_files = sum(
        result.readable_file_count
        for result in results.values()
    )

    unreadable_files = sum(
        result.unreadable_file_count
        for result in results.values()
    )

    total_rows = sum(
        result.total_rows
        for result in results.values()
    )

    short_history_count = sum(
        len(
            result.short_history_markets
        )
        for result in results.values()
    )

    stale_count = sum(
        len(
            result.stale_markets
        )
        for result in results.values()
    )

    print()
    print_separator("=")
    print(
        "DATA HISTORY AUDIT SUMMARY"
    )
    print_separator("=")

    print(
        f"Project                 : "
        f"{PROJECT_NAME}"
    )

    print(
        f"Version                 : "
        f"{VERSION}"
    )

    print(
        f"Mode                    : "
        f"READ ONLY"
    )

    print(
        f"Timeframes              : "
        f"h1 / h4 / d1"
    )

    print(
        f"Total CSV files         : "
        f"{total_files:,}"
    )

    print(
        f"Readable CSV files      : "
        f"{readable_files:,}"
    )

    print(
        f"Unreadable CSV files    : "
        f"{unreadable_files:,}"
    )

    print(
        f"Total OHLCV rows        : "
        f"{total_rows:,}"
    )

    print(
        f"Relative short flags    : "
        f"{short_history_count:,}"
    )

    print(
        f"Stale-data flags        : "
        f"{stale_count:,}"
    )

    print(
        f"Coverage mismatches     : "
        f"{mismatch_count:,}"
    )

    print()
    print(
        "Research-ready minimum history:"
    )

    print(
        "  NOT DEFINED YET"
    )

    print()

    print(
        "Reason:"
    )

    print(
        "  STEP 2 first measures the actual "
        "history distribution."
    )

    print(
        "  A minimum research-history requirement "
        "will be decided only after reviewing "
        "these results."
    )

    print_separator("=")


# ============================================================
# FATAL ERROR CHECK
# ============================================================

def count_fatal_audit_errors(
    results: dict[
        str,
        TimeframeAuditResult,
    ],
) -> int:
    """
    Only structural/readability failures are fatal here.

    Short history and stale data are intentionally NOT fatal.
    """

    fatal_count = 0

    for result in results.values():

        if not result.directory_exists:
            fatal_count += 1

        fatal_count += (
            result.unreadable_file_count
        )

    return fatal_count


# ============================================================
# MAIN
# ============================================================

def main() -> int:

    print_header(
        "UPBIT SURGE MONITOR - "
        "OHLCV DATA HISTORY AUDIT"
    )

    print(
        f"Project root       : {BASE_DIR}"
    )

    print(
        f"Data directory     : {DATA_DIR}"
    )

    print(
        f"OHLCV directory    : {OHLCV_DIR}"
    )

    print(
        "Timeframes         : h1 / h4 / d1"
    )

    print()

    print(
        "Mode               : READ ONLY"
    )

    print(
        "Source modification: DISABLED"
    )

    print(
        "Source deletion    : DISABLED"
    )

    print(
        "Pattern analysis   : DISABLED"
    )

    print(
        "Prediction         : DISABLED"
    )

    print()

    now_utc = get_now_utc()

    print(
        f"Audit UTC time     : "
        f"{format_timestamp(now_utc)}"
    )

    # --------------------------------------------------------
    # VERIFY BASE DIRECTORIES
    # --------------------------------------------------------

    if not DATA_DIR.exists():

        print()
        print(
            f"[FATAL] Data directory "
            f"not found: {DATA_DIR}"
        )

        return 1

    if not OHLCV_DIR.exists():

        print()
        print(
            f"[FATAL] OHLCV directory "
            f"not found: {OHLCV_DIR}"
        )

        return 1

    # --------------------------------------------------------
    # AUDIT TIMEFRAMES
    # --------------------------------------------------------

    results: dict[
        str,
        TimeframeAuditResult,
    ] = {}

    for timeframe in (
        "h1",
        "h4",
        "d1",
    ):

        result = audit_timeframe(
            timeframe=timeframe,
            directory=TIMEFRAME_DIRS[
                timeframe
            ],
            now_utc=now_utc,
        )

        classify_relative_history(
            result
        )

        classify_stale_data(
            result
        )

        results[timeframe] = result

    # --------------------------------------------------------
    # PRINT TIMEFRAME STATISTICS
    # --------------------------------------------------------

    for timeframe in (
        "h1",
        "h4",
        "d1",
    ):

        result = results[
            timeframe
        ]

        print_timeframe_statistics(
            result
        )

        print_short_history_report(
            result
        )

        print_stale_report(
            result
        )

        print_unreadable_report(
            result
        )

    # --------------------------------------------------------
    # CROSS-TIMEFRAME COVERAGE
    # --------------------------------------------------------

    mismatch_count = (
        print_cross_timeframe_coverage(
            results
        )
    )

    # --------------------------------------------------------
    # OVERALL SUMMARY
    # --------------------------------------------------------

    print_overall_summary(
        results=results,
        mismatch_count=mismatch_count,
    )

    # --------------------------------------------------------
    # FINAL RESULT
    # --------------------------------------------------------

    fatal_error_count = (
        count_fatal_audit_errors(
            results
        )
    )

    print()
    print_separator("=")

    if fatal_error_count > 0:

        print(
            "[RESULT] DATA HISTORY AUDIT FAILED"
        )

        print(
            f"[ERROR] Fatal audit problems: "
            f"{fatal_error_count:,}"
        )

        print(
            "[INFO] Short-history and stale-data "
            "flags are NOT included in this count."
        )

        print_separator("=")

        return 1

    print(
        "[RESULT] DATA HISTORY AUDIT COMPLETED"
    )

    print(
        "[PASS] All OHLCV CSV files were "
        "read successfully."
    )

    print(
        "[INFO] This result does NOT yet mean "
        "that STEP 2 is complete."
    )

    print(
        "[INFO] Review the measured history "
        "distribution before defining the "
        "minimum research-history requirement."
    )

    print_separator("=")

    return 0


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    try:

        exit_code = main()

    except KeyboardInterrupt:

        print()
        print(
            "[STOP] Data history audit "
            "interrupted by user."
        )

        exit_code = 130

    except Exception as exc:

        print()
        print_separator("=")
        print(
            "DATA HISTORY AUDIT FATAL ERROR"
        )
        print_separator("=")

        print(
            f"{type(exc).__name__}: {exc}"
        )

        print_separator("=")

        exit_code = 1

    sys.exit(exit_code)
