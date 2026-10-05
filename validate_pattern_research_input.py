#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
========================================================================
UPBIT SURGE MONITOR
PATTERN RESEARCH INPUT VALIDATOR
Clean V001
========================================================================

File:
    validate_pattern_research_input.py

Purpose:
    Validate production OHLCV data before beginning pattern research.

Research sequence:
    1. 256
    2. 밥그릇 3번자리
    3. 역매공파
    4. 공구리
    5. 돌파
    6. 눌림목
    7. 기준봉
    8. 하이힐
    9. 오돌이

This program DOES NOT detect patterns.

This program validates that the production OHLCV dataset is suitable
for future pattern quantification and backtesting.

========================================================================
SAFETY CONTRACT
========================================================================

READ-ONLY VALIDATION ONLY.

FORBIDDEN:

    - OHLCV deletion
    - OHLCV modification
    - OHLCV overwrite
    - OHLCV repair
    - API candle download
    - recovery execution
    - feature generation
    - pattern detection
    - future label generation
    - prediction
    - trading
    - Git reset
    - Git clean
    - Git commit
    - Git push

ALLOWED:

    - Read production OHLCV CSV files
    - Validate structure
    - Validate timestamp ordering
    - Validate duplicate timestamps
    - Validate OHLC logic
    - Validate numeric values
    - Validate interval consistency
    - Generate validation reports
    - Generate checkpoint/report metadata

========================================================================
IMPORTANT
========================================================================

The validator intentionally does NOT assume one fixed CSV schema.

Supported timestamp candidates include:

    timestamp
    datetime
    date
    time
    candle_date_time_utc
    candle_date_time_kst
    trade_timestamp
    opening_time
    open_time

Supported OHLCV aliases are detected automatically.

========================================================================
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
import os
import re
import sys
import traceback

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd


# ======================================================================
# VERSION
# ======================================================================

PROGRAM_NAME = "validate_pattern_research_input.py"
PROGRAM_TITLE = "PATTERN RESEARCH INPUT VALIDATOR"
PROGRAM_VERSION = "Clean V001"


# ======================================================================
# RESEARCH CONTRACT
# ======================================================================

RESEARCH_PATTERNS = [
    "256",
    "밥그릇 3번자리",
    "역매공파",
    "공구리",
    "돌파",
    "눌림목",
    "기준봉",
    "하이힐",
    "오돌이",
]

REQUIRED_TIMEFRAMES = [
    "h1",
    "h4",
    "d1",
]

TIMEFRAME_EXPECTED_SECONDS = {
    "h1": 60 * 60,
    "h4": 4 * 60 * 60,
    "d1": 24 * 60 * 60,
}


# ======================================================================
# VALIDATION SETTINGS
# ======================================================================

CSV_ENCODING_CANDIDATES = [
    "utf-8-sig",
    "utf-8",
    "cp949",
]

TIMESTAMP_COLUMN_CANDIDATES = [
    "timestamp",
    "datetime",
    "date",
    "time",
    "candle_date_time_utc",
    "candle_date_time_kst",
    "trade_timestamp",
    "opening_time",
    "open_time",
]

MARKET_COLUMN_CANDIDATES = [
    "market",
    "symbol",
    "ticker",
    "code",
]

COLUMN_ALIASES = {
    "open": [
        "open",
        "opening_price",
        "trade_price_open",
    ],
    "high": [
        "high",
        "high_price",
    ],
    "low": [
        "low",
        "low_price",
    ],
    "close": [
        "close",
        "trade_price",
        "closing_price",
        "close_price",
    ],
    "volume": [
        "volume",
        "candle_acc_trade_volume",
        "acc_trade_volume",
        "trade_volume",
    ],
}

MIN_ROWS_WARNING = {
    "h1": 200,
    "h4": 200,
    "d1": 200,
}

# A gap is not automatically treated as fatal.
#
# Crypto markets trade 24/7, so interval gaps are important research
# information. However, historical listing boundaries and source history
# availability must not be confused with corruption.
#
# Exact duplicate timestamps and non-monotonic ordering are stricter.
GAP_FATAL = False

# Missing required OHLCV columns is fatal.
REQUIRED_SCHEMA_FATAL = True

# Duplicate timestamps inside a file are fatal.
DUPLICATE_TIMESTAMP_FATAL = True

# Timestamp parse failures are fatal.
TIMESTAMP_PARSE_FATAL = True

# Non-monotonic timestamps are fatal.
TIMESTAMP_ORDER_FATAL = True

# Invalid OHLC relationships are fatal.
OHLC_LOGIC_FATAL = True

# Negative volume is fatal.
NEGATIVE_VOLUME_FATAL = True

# NaN/Inf in required OHLCV fields is fatal.
NONFINITE_VALUE_FATAL = True


# ======================================================================
# DATA CLASSES
# ======================================================================

@dataclass
class Issue:
    severity: str
    code: str
    timeframe: str
    file: str
    market: str
    message: str
    row_count: Optional[int] = None
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class FileValidationResult:
    timeframe: str
    file: str
    market: str

    passed: bool = True

    row_count: int = 0

    timestamp_column: str = ""
    market_column: str = ""

    open_column: str = ""
    high_column: str = ""
    low_column: str = ""
    close_column: str = ""
    volume_column: str = ""

    first_timestamp: str = ""
    last_timestamp: str = ""

    duplicate_timestamps: int = 0
    timestamp_parse_failures: int = 0
    non_monotonic_count: int = 0

    invalid_open_count: int = 0
    invalid_high_count: int = 0
    invalid_low_count: int = 0
    invalid_close_count: int = 0
    invalid_volume_count: int = 0

    invalid_ohlc_logic_count: int = 0
    negative_volume_count: int = 0

    interval_gap_count: int = 0
    largest_gap_seconds: float = 0.0

    file_size_bytes: int = 0
    sha256: str = ""

    issues: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class TimeframeSummary:
    timeframe: str
    directory: str

    file_count: int = 0
    passed_files: int = 0
    failed_files: int = 0

    total_rows: int = 0

    unique_markets: int = 0

    duplicate_timestamp_files: int = 0
    non_monotonic_files: int = 0
    gap_files: int = 0
    invalid_ohlc_files: int = 0

    passed: bool = True


@dataclass
class ValidationSummary:
    program: str
    version: str

    started_at_utc: str
    completed_at_utc: str = ""

    project_root: str = ""
    root_source: str = ""

    production_data_root: str = ""
    report_directory: str = ""

    research_patterns: List[str] = field(default_factory=list)
    required_timeframes: List[str] = field(default_factory=list)

    total_files: int = 0
    passed_files: int = 0
    failed_files: int = 0

    total_rows: int = 0

    warning_count: int = 0
    fatal_count: int = 0

    result: str = "UNKNOWN"

    timeframe_summaries: List[Dict[str, Any]] = field(default_factory=list)


# ======================================================================
# OUTPUT HELPERS
# ======================================================================

def line(char: str = "=", width: int = 72) -> None:
    print(char * width)


def section(title: str) -> None:
    print()
    line("=")
    print(title)
    line("=")
    print()


def info(message: str) -> None:
    print(f"[INFO] {message}")


def passed(message: str) -> None:
    print(f"[PASS] {message}")


def warning(message: str) -> None:
    print(f"[WARN] {message}")


def failure(message: str) -> None:
    print(f"[FAIL] {message}")


def safety(message: str) -> None:
    print(f"[SAFETY] {message}")


# ======================================================================
# GENERAL HELPERS
# ======================================================================

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_name(value: str) -> str:
    value = str(value).strip().lower()
    value = value.replace("-", "_")
    value = value.replace(" ", "_")

    while "__" in value:
        value = value.replace("__", "_")

    return value


def safe_relative_path(path: Path, root: Path) -> str:
    try:
        return str(path.resolve().relative_to(root.resolve()))
    except Exception:
        return str(path.resolve())


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        while True:
            block = handle.read(1024 * 1024)

            if not block:
                break

            digest.update(block)

    return digest.hexdigest()


def ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def write_json(path: Path, data: Any) -> None:
    ensure_directory(path.parent)

    temp_path = path.with_suffix(path.suffix + ".tmp")

    with temp_path.open(
        "w",
        encoding="utf-8",
        newline="\n",
    ) as handle:
        json.dump(
            data,
            handle,
            ensure_ascii=False,
            indent=2,
            default=str,
        )

    temp_path.replace(path)


def write_csv(
    path: Path,
    rows: Sequence[Dict[str, Any]],
    fieldnames: Sequence[str],
) -> None:

    ensure_directory(path.parent)

    temp_path = path.with_suffix(path.suffix + ".tmp")

    with temp_path.open(
        "w",
        encoding="utf-8-sig",
        newline="",
    ) as handle:

        writer = csv.DictWriter(
            handle,
            fieldnames=list(fieldnames),
            extrasaction="ignore",
        )

        writer.writeheader()

        for row in rows:
            writer.writerow(row)

    temp_path.replace(path)


# ======================================================================
# PROJECT ROOT DISCOVERY
# ======================================================================

def candidate_project_roots() -> List[Tuple[str, Path]]:

    candidates: List[Tuple[str, Path]] = []

    env_root = os.environ.get("UPBIT_SURGE_PROJECT_ROOT")

    if env_root:
        candidates.append(
            (
                "environment",
                Path(env_root).expanduser(),
            )
        )

    user_profile = os.environ.get("USERPROFILE")

    if user_profile:
        candidates.append(
            (
                "user-documents",
                Path(user_profile)
                / "Documents"
                / "upbit-surge-monitor",
            )
        )

    candidates.append(
        (
            "script-directory",
            Path(__file__).resolve().parent,
        )
    )

    candidates.append(
        (
            "current-directory",
            Path.cwd(),
        )
    )

    unique: List[Tuple[str, Path]] = []
    seen = set()

    for source, path in candidates:

        try:
            resolved = path.resolve()
        except Exception:
            resolved = path

        key = str(resolved).lower()

        if key in seen:
            continue

        seen.add(key)
        unique.append((source, resolved))

    return unique


def root_has_production_data(root: Path) -> bool:

    data_root = root / "data" / "ohlcv"

    if not data_root.is_dir():
        return False

    found = 0

    for timeframe in REQUIRED_TIMEFRAMES:

        directory = data_root / timeframe

        if directory.is_dir():
            found += 1

    return found > 0


def discover_project_root() -> Tuple[Path, str]:

    for source, root in candidate_project_roots():

        if root_has_production_data(root):
            return root, source

    attempted = "\n".join(
        f"  - {source}: {path}"
        for source, path in candidate_project_roots()
    )

    raise FileNotFoundError(
        "Could not locate production project root.\n"
        "Checked:\n"
        f"{attempted}"
    )


# ======================================================================
# PRODUCTION DIRECTORY DISCOVERY
# ======================================================================

def discover_timeframe_directory(
    data_root: Path,
    timeframe: str,
) -> Path:

    direct = data_root / timeframe

    if direct.is_dir():
        return direct

    aliases = {
        "h1": [
            "1h",
            "hour1",
            "hours1",
        ],
        "h4": [
            "4h",
            "hour4",
            "hours4",
        ],
        "d1": [
            "1d",
            "day1",
            "daily",
        ],
    }

    for alias in aliases.get(timeframe, []):

        candidate = data_root / alias

        if candidate.is_dir():
            return candidate

    raise FileNotFoundError(
        f"Required production timeframe directory missing: "
        f"{data_root / timeframe}"
    )


# ======================================================================
# CSV DISCOVERY
# ======================================================================

def discover_csv_files(directory: Path) -> List[Path]:

    files = [
        path
        for path in directory.rglob("*.csv")
        if path.is_file()
    ]

    files.sort(
        key=lambda p: str(p).lower()
    )

    return files


# ======================================================================
# CSV READING
# ======================================================================

def read_csv_with_encoding(
    path: Path,
) -> Tuple[pd.DataFrame, str]:

    last_exception: Optional[Exception] = None

    for encoding in CSV_ENCODING_CANDIDATES:

        try:

            df = pd.read_csv(
                path,
                encoding=encoding,
                low_memory=False,
            )

            return df, encoding

        except UnicodeDecodeError as exc:
            last_exception = exc

    if last_exception is not None:
        raise last_exception

    raise RuntimeError(
        f"Unable to read CSV: {path}"
    )


# ======================================================================
# COLUMN DISCOVERY
# ======================================================================

def build_normalized_column_map(
    columns: Iterable[str],
) -> Dict[str, str]:

    result: Dict[str, str] = {}

    for column in columns:

        normalized = normalize_name(column)

        if normalized not in result:
            result[normalized] = str(column)

    return result


def find_column(
    columns: Iterable[str],
    candidates: Sequence[str],
) -> Optional[str]:

    normalized_map = build_normalized_column_map(columns)

    for candidate in candidates:

        normalized = normalize_name(candidate)

        if normalized in normalized_map:
            return normalized_map[normalized]

    return None


def find_timestamp_column(
    columns: Iterable[str],
) -> Optional[str]:

    return find_column(
        columns,
        TIMESTAMP_COLUMN_CANDIDATES,
    )


def find_market_column(
    columns: Iterable[str],
) -> Optional[str]:

    return find_column(
        columns,
        MARKET_COLUMN_CANDIDATES,
    )


def find_ohlcv_columns(
    columns: Iterable[str],
) -> Dict[str, Optional[str]]:

    result: Dict[str, Optional[str]] = {}

    for logical_name, aliases in COLUMN_ALIASES.items():

        result[logical_name] = find_column(
            columns,
            aliases,
        )

    return result


# ======================================================================
# MARKET DISCOVERY
# ======================================================================

def normalize_market_name(value: str) -> str:

    value = str(value).strip().upper()

    value = value.replace("_", "-")

    return value


def market_from_filename(path: Path) -> str:

    stem = path.stem.upper()

    patterns = [
        r"(KRW-[A-Z0-9]+)",
        r"(BTC-[A-Z0-9]+)",
        r"(USDT-[A-Z0-9]+)",
    ]

    for pattern in patterns:

        match = re.search(pattern, stem)

        if match:
            return normalize_market_name(
                match.group(1)
            )

    transformed = stem.replace("_", "-")

    for pattern in patterns:

        match = re.search(pattern, transformed)

        if match:
            return normalize_market_name(
                match.group(1)
            )

    return stem


def determine_market(
    df: pd.DataFrame,
    market_column: Optional[str],
    path: Path,
) -> str:

    if market_column is not None:

        values = (
            df[market_column]
            .dropna()
            .astype(str)
            .str.strip()
        )

        values = values[
            values != ""
        ]

        unique_values = values.unique()

        if len(unique_values) == 1:
            return normalize_market_name(
                unique_values[0]
            )

    return market_from_filename(path)


# ======================================================================
# TIMESTAMP PARSING
# ======================================================================

def parse_timestamp_series(
    series: pd.Series,
) -> pd.Series:

    # --------------------------------------------------------------
    # Numeric timestamps
    # --------------------------------------------------------------

    numeric = pd.to_numeric(
        series,
        errors="coerce",
    )

    numeric_valid_ratio = float(
        numeric.notna().mean()
    )

    if numeric_valid_ratio >= 0.95:

        non_null = numeric.dropna()

        if not non_null.empty:

            median_value = float(
                non_null.abs().median()
            )

            # milliseconds
            if median_value >= 1e11:

                return pd.to_datetime(
                    numeric,
                    unit="ms",
                    errors="coerce",
                    utc=True,
                )

            # seconds
            if median_value >= 1e9:

                return pd.to_datetime(
                    numeric,
                    unit="s",
                    errors="coerce",
                    utc=True,
                )

    # --------------------------------------------------------------
    # String/date timestamps
    # --------------------------------------------------------------

    return pd.to_datetime(
        series,
        errors="coerce",
        utc=True,
        format="mixed",
    )


# ======================================================================
# NUMERIC VALIDATION
# ======================================================================

def numeric_series(
    df: pd.DataFrame,
    column: str,
) -> pd.Series:

    return pd.to_numeric(
        df[column],
        errors="coerce",
    )


def count_nonfinite(series: pd.Series) -> int:

    numeric = pd.to_numeric(
        series,
        errors="coerce",
    )

    values = numeric.to_numpy(
        dtype=float,
        na_value=float("nan"),
    )

    count = 0

    for value in values:

        if not math.isfinite(value):
            count += 1

    return count


# ======================================================================
# ISSUE MANAGEMENT
# ======================================================================

def add_issue(
    result: FileValidationResult,
    all_issues: List[Issue],
    severity: str,
    code: str,
    message: str,
    details: Optional[Dict[str, Any]] = None,
) -> None:

    issue = Issue(
        severity=severity,
        code=code,
        timeframe=result.timeframe,
        file=result.file,
        market=result.market,
        message=message,
        row_count=result.row_count,
        details=details or {},
    )

    result.issues.append(
        asdict(issue)
    )

    all_issues.append(issue)

    if severity.upper() == "FATAL":
        result.passed = False


# ======================================================================
# SINGLE FILE VALIDATION
# ======================================================================

def validate_file(
    path: Path,
    timeframe: str,
    project_root: Path,
    all_issues: List[Issue],
) -> FileValidationResult:

    relative_file = safe_relative_path(
        path,
        project_root,
    )

    result = FileValidationResult(
        timeframe=timeframe,
        file=relative_file,
        market=market_from_filename(path),
    )

    result.file_size_bytes = path.stat().st_size
    result.sha256 = sha256_file(path)

    # --------------------------------------------------------------
    # Read
    # --------------------------------------------------------------

    try:

        df, encoding = read_csv_with_encoding(path)

    except Exception as exc:

        add_issue(
            result,
            all_issues,
            "FATAL",
            "CSV_READ_FAILED",
            f"CSV read failed: {type(exc).__name__}: {exc}",
        )

        return result

    result.row_count = len(df)

    # --------------------------------------------------------------
    # Empty file
    # --------------------------------------------------------------

    if df.empty:

        add_issue(
            result,
            all_issues,
            "FATAL",
            "EMPTY_CSV",
            "CSV contains no rows.",
        )

        return result

    # --------------------------------------------------------------
    # Discover columns
    # --------------------------------------------------------------

    timestamp_column = find_timestamp_column(
        df.columns
    )

    market_column = find_market_column(
        df.columns
    )

    ohlcv = find_ohlcv_columns(
        df.columns
    )

    result.timestamp_column = (
        timestamp_column or ""
    )

    result.market_column = (
        market_column or ""
    )

    result.open_column = (
        ohlcv["open"] or ""
    )

    result.high_column = (
        ohlcv["high"] or ""
    )

    result.low_column = (
        ohlcv["low"] or ""
    )

    result.close_column = (
        ohlcv["close"] or ""
    )

    result.volume_column = (
        ohlcv["volume"] or ""
    )

    result.market = determine_market(
        df,
        market_column,
        path,
    )

    # --------------------------------------------------------------
    # Required schema
    # --------------------------------------------------------------

    if timestamp_column is None:

        severity = (
            "FATAL"
            if REQUIRED_SCHEMA_FATAL
            else "WARN"
        )

        add_issue(
            result,
            all_issues,
            severity,
            "TIMESTAMP_COLUMN_MISSING",
            "Timestamp column could not be detected.",
            {
                "columns": list(df.columns),
            },
        )

    missing_ohlcv = [
        logical_name
        for logical_name, column
        in ohlcv.items()
        if column is None
    ]

    if missing_ohlcv:

        severity = (
            "FATAL"
            if REQUIRED_SCHEMA_FATAL
            else "WARN"
        )

        add_issue(
            result,
            all_issues,
            severity,
            "OHLCV_COLUMNS_MISSING",
            "Required OHLCV columns are missing.",
            {
                "missing": missing_ohlcv,
                "columns": list(df.columns),
            },
        )

    if timestamp_column is None or missing_ohlcv:
        return result

    # --------------------------------------------------------------
    # Timestamp
    # --------------------------------------------------------------

    parsed_ts = parse_timestamp_series(
        df[timestamp_column]
    )

    parse_failures = int(
        parsed_ts.isna().sum()
    )

    result.timestamp_parse_failures = (
        parse_failures
    )

    if parse_failures > 0:

        severity = (
            "FATAL"
            if TIMESTAMP_PARSE_FATAL
            else "WARN"
        )

        add_issue(
            result,
            all_issues,
            severity,
            "TIMESTAMP_PARSE_FAILURE",
            (
                f"{parse_failures} timestamp value(s) "
                "could not be parsed."
            ),
            {
                "count": parse_failures,
            },
        )

    valid_ts = parsed_ts.dropna()

    if not valid_ts.empty:

        result.first_timestamp = (
            valid_ts.iloc[0].isoformat()
        )

        result.last_timestamp = (
            valid_ts.iloc[-1].isoformat()
        )

    # --------------------------------------------------------------
    # Duplicate timestamp
    # --------------------------------------------------------------

    duplicate_count = int(
        valid_ts.duplicated(
            keep=False
        ).sum()
    )

    result.duplicate_timestamps = (
        duplicate_count
    )

    if duplicate_count > 0:

        severity = (
            "FATAL"
            if DUPLICATE_TIMESTAMP_FATAL
            else "WARN"
        )

        add_issue(
            result,
            all_issues,
            severity,
            "DUPLICATE_TIMESTAMP",
            (
                f"{duplicate_count} row(s) participate "
                "in duplicate timestamps."
            ),
            {
                "count": duplicate_count,
            },
        )

    # --------------------------------------------------------------
    # Monotonic timestamp order
    # --------------------------------------------------------------

    non_monotonic_count = 0

    if len(valid_ts) >= 2:

        diffs = valid_ts.diff()

        non_monotonic_count = int(
            (diffs.dropna() <= pd.Timedelta(0)).sum()
        )

    result.non_monotonic_count = (
        non_monotonic_count
    )

    if non_monotonic_count > 0:

        severity = (
            "FATAL"
            if TIMESTAMP_ORDER_FATAL
            else "WARN"
        )

        add_issue(
            result,
            all_issues,
            severity,
            "NON_MONOTONIC_TIMESTAMP",
            (
                f"{non_monotonic_count} timestamp ordering "
                "violation(s) detected."
            ),
            {
                "count": non_monotonic_count,
            },
        )

    # --------------------------------------------------------------
    # Interval consistency
    # --------------------------------------------------------------

    expected_seconds = (
        TIMEFRAME_EXPECTED_SECONDS[timeframe]
    )

    gap_count = 0
    largest_gap_seconds = 0.0

    if len(valid_ts) >= 2:

        diffs = (
            valid_ts
            .sort_values()
            .drop_duplicates()
            .diff()
            .dropna()
            .dt.total_seconds()
        )

        if not diffs.empty:

            largest_gap_seconds = float(
                diffs.max()
            )

            gap_mask = (
                diffs
                > expected_seconds
            )

            gap_count = int(
                gap_mask.sum()
            )

    result.interval_gap_count = gap_count
    result.largest_gap_seconds = (
        largest_gap_seconds
    )

    if gap_count > 0:

        severity = (
            "FATAL"
            if GAP_FATAL
            else "WARN"
        )

        add_issue(
            result,
            all_issues,
            severity,
            "INTERVAL_GAP",
            (
                f"{gap_count} interval gap(s) detected. "
                f"Largest gap={largest_gap_seconds:.0f}s."
            ),
            {
                "expected_seconds": expected_seconds,
                "gap_count": gap_count,
                "largest_gap_seconds": largest_gap_seconds,
            },
        )

    # --------------------------------------------------------------
    # Numeric conversion
    # --------------------------------------------------------------

    open_values = numeric_series(
        df,
        ohlcv["open"],
    )

    high_values = numeric_series(
        df,
        ohlcv["high"],
    )

    low_values = numeric_series(
        df,
        ohlcv["low"],
    )

    close_values = numeric_series(
        df,
        ohlcv["close"],
    )

    volume_values = numeric_series(
        df,
        ohlcv["volume"],
    )

    result.invalid_open_count = (
        count_nonfinite(open_values)
    )

    result.invalid_high_count = (
        count_nonfinite(high_values)
    )

    result.invalid_low_count = (
        count_nonfinite(low_values)
    )

    result.invalid_close_count = (
        count_nonfinite(close_values)
    )

    result.invalid_volume_count = (
        count_nonfinite(volume_values)
    )

    invalid_numeric_total = (
        result.invalid_open_count
        + result.invalid_high_count
        + result.invalid_low_count
        + result.invalid_close_count
        + result.invalid_volume_count
    )

    if invalid_numeric_total > 0:

        severity = (
            "FATAL"
            if NONFINITE_VALUE_FATAL
            else "WARN"
        )

        add_issue(
            result,
            all_issues,
            severity,
            "NONFINITE_OHLCV",
            (
                f"{invalid_numeric_total} non-finite "
                "required OHLCV value(s) detected."
            ),
            {
                "open": result.invalid_open_count,
                "high": result.invalid_high_count,
                "low": result.invalid_low_count,
                "close": result.invalid_close_count,
                "volume": result.invalid_volume_count,
            },
        )

    # --------------------------------------------------------------
    # Price positivity
    # --------------------------------------------------------------

    invalid_price_mask = (
        (open_values <= 0)
        | (high_values <= 0)
        | (low_values <= 0)
        | (close_values <= 0)
    )

    invalid_price_count = int(
        invalid_price_mask.fillna(False).sum()
    )

    if invalid_price_count > 0:

        add_issue(
            result,
            all_issues,
            "FATAL",
            "NONPOSITIVE_PRICE",
            (
                f"{invalid_price_count} row(s) contain "
                "zero or negative price values."
            ),
            {
                "count": invalid_price_count,
            },
        )

    # --------------------------------------------------------------
    # OHLC logic
    # --------------------------------------------------------------

    invalid_ohlc_mask = (
        (high_values < open_values)
        | (high_values < close_values)
        | (high_values < low_values)
        | (low_values > open_values)
        | (low_values > close_values)
    )

    invalid_ohlc_count = int(
        invalid_ohlc_mask.fillna(False).sum()
    )

    result.invalid_ohlc_logic_count = (
        invalid_ohlc_count
    )

    if invalid_ohlc_count > 0:

        severity = (
            "FATAL"
            if OHLC_LOGIC_FATAL
            else "WARN"
        )

        add_issue(
            result,
            all_issues,
            severity,
            "INVALID_OHLC_LOGIC",
            (
                f"{invalid_ohlc_count} row(s) violate "
                "OHLC price relationships."
            ),
            {
                "count": invalid_ohlc_count,
            },
        )

    # --------------------------------------------------------------
    # Negative volume
    # --------------------------------------------------------------

    negative_volume_count = int(
        (volume_values < 0)
        .fillna(False)
        .sum()
    )

    result.negative_volume_count = (
        negative_volume_count
    )

    if negative_volume_count > 0:

        severity = (
            "FATAL"
            if NEGATIVE_VOLUME_FATAL
            else "WARN"
        )

        add_issue(
            result,
            all_issues,
            severity,
            "NEGATIVE_VOLUME",
            (
                f"{negative_volume_count} row(s) contain "
                "negative volume."
            ),
            {
                "count": negative_volume_count,
            },
        )

    # --------------------------------------------------------------
    # Research history length warning
    # --------------------------------------------------------------

    minimum_rows = MIN_ROWS_WARNING[
        timeframe
    ]

    if result.row_count < minimum_rows:

        add_issue(
            result,
            all_issues,
            "WARN",
            "SHORT_HISTORY",
            (
                f"Only {result.row_count} rows available. "
                f"Recommended minimum for initial pattern "
                f"research validation is {minimum_rows}."
            ),
            {
                "rows": result.row_count,
                "recommended_minimum": minimum_rows,
            },
        )

    return result


# ======================================================================
# TIMEFRAME VALIDATION
# ======================================================================

def validate_timeframe(
    timeframe: str,
    directory: Path,
    project_root: Path,
    all_issues: List[Issue],
) -> Tuple[
    TimeframeSummary,
    List[FileValidationResult],
]:

    section(
        f"VALIDATE TIMEFRAME: {timeframe.upper()}"
    )

    print(f"Directory : {directory}")

    csv_files = discover_csv_files(
        directory
    )

    print(f"CSV files : {len(csv_files)}")
    print()

    summary = TimeframeSummary(
        timeframe=timeframe,
        directory=safe_relative_path(
            directory,
            project_root,
        ),
        file_count=len(csv_files),
    )

    results: List[FileValidationResult] = []

    if not csv_files:

        summary.passed = False

        issue = Issue(
            severity="FATAL",
            code="NO_CSV_FILES",
            timeframe=timeframe,
            file=safe_relative_path(
                directory,
                project_root,
            ),
            market="",
            message=(
                f"No CSV files found for {timeframe}."
            ),
        )

        all_issues.append(issue)

        failure(
            f"{timeframe.upper()} contains no CSV files."
        )

        return summary, results

    markets = set()

    for index, path in enumerate(
        csv_files,
        start=1,
    ):

        result = validate_file(
            path=path,
            timeframe=timeframe,
            project_root=project_root,
            all_issues=all_issues,
        )

        results.append(result)

        summary.total_rows += (
            result.row_count
        )

        if result.market:
            markets.add(result.market)

        if result.passed:
            summary.passed_files += 1
        else:
            summary.failed_files += 1
            summary.passed = False

        if result.duplicate_timestamps > 0:
            summary.duplicate_timestamp_files += 1

        if result.non_monotonic_count > 0:
            summary.non_monotonic_files += 1

        if result.interval_gap_count > 0:
            summary.gap_files += 1

        if result.invalid_ohlc_logic_count > 0:
            summary.invalid_ohlc_files += 1

        status = (
            "PASS"
            if result.passed
            else "FAIL"
        )

        print(
            f"[{status}] "
            f"{index:03d}/{len(csv_files):03d} "
            f"{result.market:<14} "
            f"rows={result.row_count:<8} "
            f"gaps={result.interval_gap_count:<5} "
            f"{result.file}"
        )

    summary.unique_markets = len(markets)

    print()

    if summary.passed:

        passed(
            f"{timeframe.upper()} validation completed."
        )

    else:

        failure(
            f"{timeframe.upper()} validation failed."
        )

    print(
        f"Files          : {summary.file_count}"
    )

    print(
        f"Passed files   : {summary.passed_files}"
    )

    print(
        f"Failed files   : {summary.failed_files}"
    )

    print(
        f"Markets        : {summary.unique_markets}"
    )

    print(
        f"Rows           : {summary.total_rows}"
    )

    print(
        f"Files with gaps: {summary.gap_files}"
    )

    return summary, results


# ======================================================================
# CROSS-TIMEFRAME VALIDATION
# ======================================================================

def validate_market_coverage(
    results_by_timeframe: Dict[
        str,
        List[FileValidationResult],
    ],
    all_issues: List[Issue],
) -> None:

    section(
        "CROSS-TIMEFRAME MARKET COVERAGE"
    )

    market_sets: Dict[str, set] = {}

    for timeframe, results in (
        results_by_timeframe.items()
    ):

        markets = {
            result.market
            for result in results
            if result.market
        }

        market_sets[timeframe] = markets

        print(
            f"{timeframe.upper():>3} markets : "
            f"{len(markets)}"
        )

    all_markets = set()

    for markets in market_sets.values():
        all_markets.update(markets)

    print()
    print(
        f"Union markets : {len(all_markets)}"
    )

    for market in sorted(all_markets):

        missing = [
            timeframe
            for timeframe
            in REQUIRED_TIMEFRAMES
            if market not in market_sets.get(
                timeframe,
                set(),
            )
        ]

        if missing:

            issue = Issue(
                severity="WARN",
                code="MARKET_TIMEFRAME_MISSING",
                timeframe=",".join(missing),
                file="",
                market=market,
                message=(
                    f"{market} missing timeframe(s): "
                    f"{', '.join(missing)}"
                ),
                details={
                    "missing_timeframes": missing,
                },
            )

            all_issues.append(issue)

    missing_count = sum(
        1
        for issue in all_issues
        if issue.code
        == "MARKET_TIMEFRAME_MISSING"
    )

    if missing_count == 0:

        passed(
            "Cross-timeframe market coverage is consistent."
        )

    else:

        warning(
            f"{missing_count} market/timeframe coverage "
            "difference(s) detected."
        )


# ======================================================================
# REPORT GENERATION
# ======================================================================

def flatten_file_result(
    result: FileValidationResult,
) -> Dict[str, Any]:

    return {
        "timeframe": result.timeframe,
        "file": result.file,
        "market": result.market,
        "passed": result.passed,
        "row_count": result.row_count,
        "timestamp_column": result.timestamp_column,
        "market_column": result.market_column,
        "open_column": result.open_column,
        "high_column": result.high_column,
        "low_column": result.low_column,
        "close_column": result.close_column,
        "volume_column": result.volume_column,
        "first_timestamp": result.first_timestamp,
        "last_timestamp": result.last_timestamp,
        "duplicate_timestamps": (
            result.duplicate_timestamps
        ),
        "timestamp_parse_failures": (
            result.timestamp_parse_failures
        ),
        "non_monotonic_count": (
            result.non_monotonic_count
        ),
        "invalid_open_count": (
            result.invalid_open_count
        ),
        "invalid_high_count": (
            result.invalid_high_count
        ),
        "invalid_low_count": (
            result.invalid_low_count
        ),
        "invalid_close_count": (
            result.invalid_close_count
        ),
        "invalid_volume_count": (
            result.invalid_volume_count
        ),
        "invalid_ohlc_logic_count": (
            result.invalid_ohlc_logic_count
        ),
        "negative_volume_count": (
            result.negative_volume_count
        ),
        "interval_gap_count": (
            result.interval_gap_count
        ),
        "largest_gap_seconds": (
            result.largest_gap_seconds
        ),
        "file_size_bytes": (
            result.file_size_bytes
        ),
        "sha256": result.sha256,
        "issue_count": len(result.issues),
    }


def issue_to_csv_row(
    issue: Issue,
) -> Dict[str, Any]:

    return {
        "severity": issue.severity,
        "code": issue.code,
        "timeframe": issue.timeframe,
        "file": issue.file,
        "market": issue.market,
        "row_count": (
            ""
            if issue.row_count is None
            else issue.row_count
        ),
        "message": issue.message,
        "details_json": json.dumps(
            issue.details,
            ensure_ascii=False,
            sort_keys=True,
        ),
    }


def write_reports(
    report_directory: Path,
    summary: ValidationSummary,
    file_results: List[FileValidationResult],
    all_issues: List[Issue],
) -> Dict[str, Path]:

    ensure_directory(
        report_directory
    )

    result_json = (
        report_directory
        / "pattern_research_input_validation.json"
    )

    detail_csv = (
        report_directory
        / "pattern_research_input_validation_detail.csv"
    )

    issues_csv = (
        report_directory
        / "pattern_research_input_validation_issues.csv"
    )

    checkpoint_json = (
        report_directory
        / "pattern_research_input_validation_checkpoint.json"
    )

    manifest_json = (
        report_directory
        / "pattern_research_input_validation_manifest.json"
    )

    # --------------------------------------------------------------
    # Result JSON
    # --------------------------------------------------------------

    result_payload = {
        "summary": asdict(summary),
        "files": [
            asdict(result)
            for result in file_results
        ],
        "issues": [
            asdict(issue)
            for issue in all_issues
        ],
    }

    write_json(
        result_json,
        result_payload,
    )

    # --------------------------------------------------------------
    # Detail CSV
    # --------------------------------------------------------------

    detail_rows = [
        flatten_file_result(result)
        for result in file_results
    ]

    detail_fields = [
        "timeframe",
        "file",
        "market",
        "passed",
        "row_count",
        "timestamp_column",
        "market_column",
        "open_column",
        "high_column",
        "low_column",
        "close_column",
        "volume_column",
        "first_timestamp",
        "last_timestamp",
        "duplicate_timestamps",
        "timestamp_parse_failures",
        "non_monotonic_count",
        "invalid_open_count",
        "invalid_high_count",
        "invalid_low_count",
        "invalid_close_count",
        "invalid_volume_count",
        "invalid_ohlc_logic_count",
        "negative_volume_count",
        "interval_gap_count",
        "largest_gap_seconds",
        "file_size_bytes",
        "sha256",
        "issue_count",
    ]

    write_csv(
        detail_csv,
        detail_rows,
        detail_fields,
    )

    # --------------------------------------------------------------
    # Issues CSV
    # --------------------------------------------------------------

    issue_rows = [
        issue_to_csv_row(issue)
        for issue in all_issues
    ]

    issue_fields = [
        "severity",
        "code",
        "timeframe",
        "file",
        "market",
        "row_count",
        "message",
        "details_json",
    ]

    write_csv(
        issues_csv,
        issue_rows,
        issue_fields,
    )

    # --------------------------------------------------------------
    # Checkpoint
    # --------------------------------------------------------------

    checkpoint = {
        "program": PROGRAM_NAME,
        "version": PROGRAM_VERSION,
        "completed_at_utc": (
            summary.completed_at_utc
        ),
        "result": summary.result,
        "fatal_count": summary.fatal_count,
        "warning_count": (
            summary.warning_count
        ),
        "total_files": summary.total_files,
        "passed_files": summary.passed_files,
        "failed_files": summary.failed_files,
        "total_rows": summary.total_rows,
        "research_stage": (
            "PATTERN_RESEARCH_INPUT_VALIDATION"
        ),
        "next_stage_allowed": (
            summary.result == "PASS"
        ),
        "next_stage": (
            "PATTERN_QUANTIFICATION"
            if summary.result == "PASS"
            else "BLOCKED"
        ),
        "patterns": RESEARCH_PATTERNS,
    }

    write_json(
        checkpoint_json,
        checkpoint,
    )

    # --------------------------------------------------------------
    # Manifest
    # --------------------------------------------------------------

    manifest_files = [
        result_json,
        detail_csv,
        issues_csv,
        checkpoint_json,
    ]

    manifest = {
        "program": PROGRAM_NAME,
        "version": PROGRAM_VERSION,
        "created_at_utc": utc_now_iso(),
        "files": [],
    }

    for path in manifest_files:

        manifest["files"].append(
            {
                "name": path.name,
                "size_bytes": (
                    path.stat().st_size
                ),
                "sha256": sha256_file(path),
            }
        )

    write_json(
        manifest_json,
        manifest,
    )

    return {
        "result_json": result_json,
        "detail_csv": detail_csv,
        "issues_csv": issues_csv,
        "checkpoint_json": checkpoint_json,
        "manifest_json": manifest_json,
    }


# ======================================================================
# FINAL SUMMARY
# ======================================================================

def print_final_summary(
    summary: ValidationSummary,
    report_paths: Dict[str, Path],
    project_root: Path,
) -> None:

    section(
        "FINAL PATTERN RESEARCH INPUT VALIDATION"
    )

    print(
        f"Program          : {summary.program}"
    )

    print(
        f"Version          : {summary.version}"
    )

    print(
        f"Project root     : {summary.project_root}"
    )

    print(
        f"Root source      : {summary.root_source}"
    )

    print()

    print(
        f"Total files      : {summary.total_files}"
    )

    print(
        f"Passed files     : {summary.passed_files}"
    )

    print(
        f"Failed files     : {summary.failed_files}"
    )

    print(
        f"Total rows       : {summary.total_rows}"
    )

    print(
        f"Warnings         : {summary.warning_count}"
    )

    print(
        f"Fatal issues     : {summary.fatal_count}"
    )

    print()

    print("Research patterns:")

    for index, pattern in enumerate(
        summary.research_patterns,
        start=1,
    ):
        print(
            f"  {index}. {pattern}"
        )

    print()

    print("Reports:")

    for name, path in report_paths.items():

        print(
            f"  {name:<16}: "
            f"{safe_relative_path(path, project_root)}"
        )

    print()

    line("=")

    if summary.result == "PASS":

        print(
            "RESULT: PASS"
        )

        print()

        print(
            "[PASS] Production OHLCV input contract "
            "is valid for the next research stage."
        )

        print(
            "[PASS] Pattern research input validation "
            "completed."
        )

        print(
            "[NEXT] Pattern quantification may begin."
        )

    else:

        print(
            "RESULT: FAIL"
        )

        print()

        print(
            "[BLOCK] Pattern quantification must NOT begin."
        )

        print(
            "[BLOCK] Resolve fatal input validation "
            "issues first."
        )

    line("=")


# ======================================================================
# SAFETY SUMMARY
# ======================================================================

def print_safety_summary() -> None:

    print()

    safety(
        "No OHLCV deletion was executed."
    )

    safety(
        "No OHLCV write was executed."
    )

    safety(
        "No existing candle overwrite was executed."
    )

    safety(
        "No historical gap repair was executed."
    )

    safety(
        "No API candle download was executed."
    )

    safety(
        "No feature build was executed."
    )

    safety(
        "No 256 detector was executed."
    )

    safety(
        "No pattern detector was executed."
    )

    safety(
        "No future labels were generated."
    )

    safety(
        "No prediction/trading was executed."
    )

    safety(
        "No Git reset/clean/commit/push was executed."
    )


# ======================================================================
# MAIN VALIDATION
# ======================================================================

def run_validation() -> int:

    started_at = utc_now_iso()

    section(
        "UPBIT SURGE MONITOR\n"
        "PATTERN RESEARCH INPUT VALIDATION\n"
        f"{PROGRAM_VERSION}"
    )

    safety(
        "READ-ONLY VALIDATION"
    )

    safety(
        "Production OHLCV modification : FORBIDDEN"
    )

    safety(
        "Recovery execution            : DISABLED"
    )

    safety(
        "Pattern detection             : DISABLED"
    )

    safety(
        "Future information usage      : DISABLED"
    )

    # --------------------------------------------------------------
    # 1. Project root
    # --------------------------------------------------------------

    section(
        "1. DISCOVER PRODUCTION PROJECT ROOT"
    )

    project_root, root_source = (
        discover_project_root()
    )

    data_root = (
        project_root
        / "data"
        / "ohlcv"
    )

    report_directory = (
        project_root
        / "data"
        / "reports"
        / "pattern_research_input_validation"
    )

    print(
        f"[PATH] Production project root: "
        f"{project_root}"
    )

    print(
        f"[PATH] Root source: {root_source}"
    )

    print(
        f"[PATH] Production OHLCV root: "
        f"{data_root}"
    )

    print(
        f"[PATH] Validation report directory: "
        f"{report_directory}"
    )

    # --------------------------------------------------------------
    # 2. Research contract
    # --------------------------------------------------------------

    section(
        "2. VALIDATE RESEARCH CONTRACT"
    )

    print("Required timeframes:")

    for timeframe in REQUIRED_TIMEFRAMES:
        print(f"  - {timeframe}")

    print()
    print("Research patterns:")

    for index, pattern in enumerate(
        RESEARCH_PATTERNS,
        start=1,
    ):
        print(
            f"  {index}. {pattern}"
        )

    passed(
        "Research contract loaded."
    )

    # --------------------------------------------------------------
    # 3. Discover timeframe directories
    # --------------------------------------------------------------

    section(
        "3. DISCOVER PRODUCTION TIMEFRAME DIRECTORIES"
    )

    timeframe_directories: Dict[
        str,
        Path,
    ] = {}

    for timeframe in REQUIRED_TIMEFRAMES:

        directory = (
            discover_timeframe_directory(
                data_root,
                timeframe,
            )
        )

        timeframe_directories[
            timeframe
        ] = directory

        passed(
            f"{timeframe.upper()} -> {directory}"
        )

    # --------------------------------------------------------------
    # 4. Validate production OHLCV
    # --------------------------------------------------------------

    all_issues: List[Issue] = []

    all_results: List[
        FileValidationResult
    ] = []

    results_by_timeframe: Dict[
        str,
        List[FileValidationResult],
    ] = {}

    timeframe_summaries: List[
        TimeframeSummary
    ] = []

    for timeframe in REQUIRED_TIMEFRAMES:

        summary, results = (
            validate_timeframe(
                timeframe=timeframe,
                directory=(
                    timeframe_directories[
                        timeframe
                    ]
                ),
                project_root=project_root,
                all_issues=all_issues,
            )
        )

        timeframe_summaries.append(
            summary
        )

        results_by_timeframe[
            timeframe
        ] = results

        all_results.extend(
            results
        )

    # --------------------------------------------------------------
    # 5. Cross-timeframe market coverage
    # --------------------------------------------------------------

    validate_market_coverage(
        results_by_timeframe,
        all_issues,
    )

    # --------------------------------------------------------------
    # 6. Aggregate
    # --------------------------------------------------------------

    section(
        "6. AGGREGATE VALIDATION RESULT"
    )

    total_files = len(
        all_results
    )

    passed_files = sum(
        1
        for result in all_results
        if result.passed
    )

    failed_files = (
        total_files - passed_files
    )

    total_rows = sum(
        result.row_count
        for result in all_results
    )

    fatal_count = sum(
        1
        for issue in all_issues
        if issue.severity.upper()
        == "FATAL"
    )

    warning_count = sum(
        1
        for issue in all_issues
        if issue.severity.upper()
        == "WARN"
    )

    final_result = (
        "PASS"
        if fatal_count == 0
        and failed_files == 0
        and total_files > 0
        else "FAIL"
    )

    print(
        f"Total files  : {total_files}"
    )

    print(
        f"Passed files : {passed_files}"
    )

    print(
        f"Failed files : {failed_files}"
    )

    print(
        f"Total rows   : {total_rows}"
    )

    print(
        f"Warnings     : {warning_count}"
    )

    print(
        f"Fatal issues : {fatal_count}"
    )

    # --------------------------------------------------------------
    # 7. Summary
    # --------------------------------------------------------------

    completed_at = utc_now_iso()

    validation_summary = ValidationSummary(
        program=PROGRAM_NAME,
        version=PROGRAM_VERSION,
        started_at_utc=started_at,
        completed_at_utc=completed_at,
        project_root=str(project_root),
        root_source=root_source,
        production_data_root=str(data_root),
        report_directory=str(
            report_directory
        ),
        research_patterns=(
            RESEARCH_PATTERNS.copy()
        ),
        required_timeframes=(
            REQUIRED_TIMEFRAMES.copy()
        ),
        total_files=total_files,
        passed_files=passed_files,
        failed_files=failed_files,
        total_rows=total_rows,
        warning_count=warning_count,
        fatal_count=fatal_count,
        result=final_result,
        timeframe_summaries=[
            asdict(item)
            for item in timeframe_summaries
        ],
    )

    # --------------------------------------------------------------
    # 8. Reports
    # --------------------------------------------------------------

    section(
        "7. WRITE VALIDATION REPORTS"
    )

    report_paths = write_reports(
        report_directory=report_directory,
        summary=validation_summary,
        file_results=all_results,
        all_issues=all_issues,
    )

    for name, path in (
        report_paths.items()
    ):

        passed(
            f"{name}: "
            f"{safe_relative_path(path, project_root)}"
        )

    # --------------------------------------------------------------
    # 9. Final
    # --------------------------------------------------------------

    print_final_summary(
        validation_summary,
        report_paths,
        project_root,
    )

    print_safety_summary()

    if final_result == "PASS":
        return 0

    return 1


# ======================================================================
# MAIN
# ======================================================================

def main() -> int:

    try:

        return run_validation()

    except KeyboardInterrupt:

        print()
        print()

        line("=")
        print(
            "PATTERN RESEARCH INPUT VALIDATION INTERRUPTED"
        )
        line("=")

        print_safety_summary()

        print()
        print(
            "[BLOCK] Validation did not complete."
        )

        return 130

    except Exception as exc:

        print()
        print()

        line("=")
        print(
            "PATTERN RESEARCH INPUT VALIDATION "
            "FATAL ERROR"
        )
        line("=")

        print(
            f"{type(exc).__name__}: {exc}"
        )

        print()

        traceback.print_exc()

        print_safety_summary()

        print()

        print(
            "[BLOCK] Do not begin pattern "
            "quantification until this validator passes."
        )

        line("=")

        return 1


if __name__ == "__main__":
    sys.exit(main())
