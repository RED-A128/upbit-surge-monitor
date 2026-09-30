"""
Upbit Surge Monitor
OHLCV Data History + Gap Audit

Version:
    Clean V003

File:
    data_history_audit.py

Purpose:
    Validate the accumulated OHLCV history for all stored KRW markets
    across h1 / h4 / d1 without modifying source OHLCV data.

Clean V003 responsibilities:
    - Audit all stored KRW market CSV files.
    - Verify h1 / h4 / d1 market coverage.
    - Measure row counts.
    - Measure first / last candle timestamps.
    - Measure actual history span.
    - Measure latest-candle age.
    - Detect invalid timestamps.
    - Detect duplicate timestamps.
    - Detect internal timestamp gaps.
    - Estimate missing candle counts.
    - Record first / last detected gap.
    - Verify source SHA256 before and after audit.
    - Support persistent checkpoint / resume.
    - Produce persistent status CSV.
    - Never modify OHLCV source files.

Important:
    - OHLCV source is READ ONLY.
    - NEVER rewrites source OHLCV CSV files.
    - NEVER deletes historical data.
    - NEVER fills gaps automatically.
    - NEVER runs Feature generation.
    - NEVER runs 256 Detector.
    - NEVER creates future labels.
    - NEVER performs prediction.
    - NEVER performs trading.
    - NEVER executes git reset / git clean / git commit / git push.

Gap interpretation:
    A timestamp gap is a research/audit finding.

    It does NOT automatically prove Collector failure.

    Some markets may legitimately have missing candles because an
    exchange can omit intervals in which no candle was published.

    Therefore Clean V003 records gaps accurately but does NOT
    automatically repair them.

Resume:
    Runtime checkpoint:
        data/validation/data_history_audit_checkpoint.json

    Runtime status:
        data/data_history_audit_status.csv

    A completed file is reused only when its current SHA256 matches
    the SHA256 stored in the checkpoint.

    If the source CSV changed after the previous audit, that file is
    audited again automatically.

Windows execution:
    py data_history_audit.py
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
import os
import sys
import tempfile
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Iterable

import pandas as pd


# ============================================================
# PROJECT CONFIGURATION
# ============================================================

PROJECT_NAME = "Upbit Surge Monitor"
VERSION = "Clean V003"

BASE_DIR = Path(__file__).resolve().parent

DATA_DIR = BASE_DIR / "data"
OHLCV_DIR = DATA_DIR / "ohlcv"
VALIDATION_DIR = DATA_DIR / "validation"

CHECKPOINT_PATH = (
    VALIDATION_DIR
    / "data_history_audit_checkpoint.json"
)

STATUS_PATH = (
    DATA_DIR
    / "data_history_audit_status.csv"
)

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

TIMEFRAME_EXPECTED_INTERVAL_SECONDS = {
    timeframe: int(hours * 3600)
    for timeframe, hours
    in TIMEFRAME_EXPECTED_INTERVAL_HOURS.items()
}

TIMESTAMP_CANDIDATES = (
    "candle_date_time_utc",
    "timestamp",
    "datetime",
    "candle_date_time_kst",
)

SHORT_HISTORY_RATIO = 0.50

STALE_INTERVAL_MULTIPLIER = 3.0

CHECKPOINT_SCHEMA_VERSION = 1

HASH_CHUNK_SIZE = 1024 * 1024

STATUS_FIELDNAMES = [
    "timeframe",
    "market",
    "source_path",
    "source_sha256",
    "row_count",
    "timestamp_column",
    "first_timestamp",
    "last_timestamp",
    "history_hours",
    "history_days",
    "latest_age_hours",
    "latest_age_days",
    "invalid_timestamp_count",
    "duplicate_timestamp_count",
    "gap_count",
    "estimated_missing_candles",
    "largest_gap_seconds",
    "largest_gap_intervals",
    "first_gap_start",
    "first_gap_end",
    "last_gap_start",
    "last_gap_end",
    "is_short_history",
    "is_stale",
    "readable",
    "errors",
    "warnings",
]


# ============================================================
# RESULT MODELS
# ============================================================

@dataclass
class MarketHistoryResult:
    timeframe: str
    market: str
    path: Path

    readable: bool = True

    source_sha256: str = ""

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

    gap_count: int = 0
    estimated_missing_candles: int = 0

    largest_gap_seconds: float = 0.0
    largest_gap_intervals: int = 0

    first_gap_start: pd.Timestamp | None = None
    first_gap_end: pd.Timestamp | None = None

    last_gap_start: pd.Timestamp | None = None
    last_gap_end: pd.Timestamp | None = None

    is_short_history: bool = False
    is_stale: bool = False

    resumed: bool = False

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

    gap_markets: list[str] = field(
        default_factory=list
    )

    total_gap_count: int = 0
    total_estimated_missing_candles: int = 0

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


def timestamp_to_json(
    value: pd.Timestamp | None,
) -> str | None:
    if value is None:
        return None

    return value.isoformat()


def timestamp_from_json(
    value: str | None,
) -> pd.Timestamp | None:
    if not value:
        return None

    parsed = pd.Timestamp(value)

    if parsed.tzinfo is None:
        parsed = parsed.tz_localize("UTC")
    else:
        parsed = parsed.tz_convert("UTC")

    return parsed


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
# FILE / HASH HELPERS
# ============================================================

def calculate_sha256(
    path: Path,
) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        while True:
            chunk = handle.read(
                HASH_CHUNK_SIZE
            )

            if not chunk:
                break

            digest.update(chunk)

    return digest.hexdigest()


def atomic_write_json(
    path: Path,
    payload: dict,
) -> None:
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temp_fd, temp_name = tempfile.mkstemp(
        prefix=f"{path.name}.",
        suffix=".tmp",
        dir=str(path.parent),
    )

    try:
        with os.fdopen(
            temp_fd,
            "w",
            encoding="utf-8",
            newline="\n",
        ) as handle:
            json.dump(
                payload,
                handle,
                ensure_ascii=False,
                indent=2,
            )

            handle.flush()
            os.fsync(
                handle.fileno()
            )

        os.replace(
            temp_name,
            path,
        )

    except Exception:
        try:
            os.unlink(
                temp_name
            )
        except OSError:
            pass

        raise


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
# CHECKPOINT SERIALIZATION
# ============================================================

def result_to_checkpoint_dict(
    result: MarketHistoryResult,
) -> dict:

    return {
        "timeframe": result.timeframe,
        "market": result.market,
        "path": str(result.path),
        "readable": result.readable,
        "source_sha256": result.source_sha256,
        "row_count": result.row_count,
        "timestamp_column": result.timestamp_column,
        "first_timestamp": timestamp_to_json(
            result.first_timestamp
        ),
        "last_timestamp": timestamp_to_json(
            result.last_timestamp
        ),
        "history_hours": result.history_hours,
        "history_days": result.history_days,
        "latest_age_hours": result.latest_age_hours,
        "latest_age_days": result.latest_age_days,
        "duplicate_timestamp_count": (
            result.duplicate_timestamp_count
        ),
        "invalid_timestamp_count": (
            result.invalid_timestamp_count
        ),
        "gap_count": result.gap_count,
        "estimated_missing_candles": (
            result.estimated_missing_candles
        ),
        "largest_gap_seconds": (
            result.largest_gap_seconds
        ),
        "largest_gap_intervals": (
            result.largest_gap_intervals
        ),
        "first_gap_start": timestamp_to_json(
            result.first_gap_start
        ),
        "first_gap_end": timestamp_to_json(
            result.first_gap_end
        ),
        "last_gap_start": timestamp_to_json(
            result.last_gap_start
        ),
        "last_gap_end": timestamp_to_json(
            result.last_gap_end
        ),
        "errors": list(result.errors),
        "warnings": list(result.warnings),
    }


def result_from_checkpoint_dict(
    data: dict,
    path: Path,
) -> MarketHistoryResult:

    return MarketHistoryResult(
        timeframe=str(
            data.get(
                "timeframe",
                "",
            )
        ),
        market=str(
            data.get(
                "market",
                path.stem.upper(),
            )
        ),
        path=path,
        readable=bool(
            data.get(
                "readable",
                True,
            )
        ),
        source_sha256=str(
            data.get(
                "source_sha256",
                "",
            )
        ),
        row_count=int(
            data.get(
                "row_count",
                0,
            )
        ),
        timestamp_column=str(
            data.get(
                "timestamp_column",
                "",
            )
        ),
        first_timestamp=timestamp_from_json(
            data.get(
                "first_timestamp"
            )
        ),
        last_timestamp=timestamp_from_json(
            data.get(
                "last_timestamp"
            )
        ),
        history_hours=float(
            data.get(
                "history_hours",
                0.0,
            )
        ),
        history_days=float(
            data.get(
                "history_days",
                0.0,
            )
        ),
        latest_age_hours=float(
            data.get(
                "latest_age_hours",
                0.0,
            )
        ),
        latest_age_days=float(
            data.get(
                "latest_age_days",
                0.0,
            )
        ),
        duplicate_timestamp_count=int(
            data.get(
                "duplicate_timestamp_count",
                0,
            )
        ),
        invalid_timestamp_count=int(
            data.get(
                "invalid_timestamp_count",
                0,
            )
        ),
        gap_count=int(
            data.get(
                "gap_count",
                0,
            )
        ),
        estimated_missing_candles=int(
            data.get(
                "estimated_missing_candles",
                0,
            )
        ),
        largest_gap_seconds=float(
            data.get(
                "largest_gap_seconds",
                0.0,
            )
        ),
        largest_gap_intervals=int(
            data.get(
                "largest_gap_intervals",
                0,
            )
        ),
        first_gap_start=timestamp_from_json(
            data.get(
                "first_gap_start"
            )
        ),
        first_gap_end=timestamp_from_json(
            data.get(
                "first_gap_end"
            )
        ),
        last_gap_start=timestamp_from_json(
            data.get(
                "last_gap_start"
            )
        ),
        last_gap_end=timestamp_from_json(
            data.get(
                "last_gap_end"
            )
        ),
        errors=list(
            data.get(
                "errors",
                [],
            )
        ),
        warnings=list(
            data.get(
                "warnings",
                [],
            )
        ),
        resumed=True,
    )


# ============================================================
# CHECKPOINT MANAGEMENT
# ============================================================

def new_checkpoint() -> dict:
    return {
        "schema_version": (
            CHECKPOINT_SCHEMA_VERSION
        ),
        "project": PROJECT_NAME,
        "version": VERSION,
        "updated_utc": (
            datetime.now(
                timezone.utc
            ).isoformat()
        ),
        "completed": {},
    }


def load_checkpoint() -> dict:
    if not CHECKPOINT_PATH.exists():
        return new_checkpoint()

    try:
        with CHECKPOINT_PATH.open(
            "r",
            encoding="utf-8",
        ) as handle:
            payload = json.load(
                handle
            )

    except Exception as exc:
        print(
            "[WARN] Existing checkpoint could "
            "not be read."
        )
        print(
            f"[WARN] {type(exc).__name__}: {exc}"
        )
        print(
            "[WARN] Audit will rebuild the "
            "checkpoint safely."
        )

        return new_checkpoint()

    if not isinstance(
        payload,
        dict,
    ):
        return new_checkpoint()

    if (
        payload.get(
            "schema_version"
        )
        != CHECKPOINT_SCHEMA_VERSION
    ):
        print(
            "[INFO] Checkpoint schema changed. "
            "A new audit checkpoint will be used."
        )

        return new_checkpoint()

    if not isinstance(
        payload.get(
            "completed"
        ),
        dict,
    ):
        payload["completed"] = {}

    return payload


def save_checkpoint(
    checkpoint: dict,
) -> None:
    checkpoint["updated_utc"] = (
        datetime.now(
            timezone.utc
        ).isoformat()
    )

    atomic_write_json(
        CHECKPOINT_PATH,
        checkpoint,
    )


def checkpoint_key(
    timeframe: str,
    market: str,
) -> str:
    return (
        f"{timeframe.lower()}|"
        f"{market.upper()}"
    )


def get_resumable_result(
    checkpoint: dict,
    timeframe: str,
    path: Path,
    current_sha256: str,
    now_utc: pd.Timestamp,
) -> MarketHistoryResult | None:

    key = checkpoint_key(
        timeframe,
        path.stem.upper(),
    )

    stored = (
        checkpoint
        .get(
            "completed",
            {},
        )
        .get(key)
    )

    if not isinstance(
        stored,
        dict,
    ):
        return None

    stored_sha256 = str(
        stored.get(
            "source_sha256",
            "",
        )
    )

    if not stored_sha256:
        return None

    if stored_sha256 != current_sha256:
        return None

    result = result_from_checkpoint_dict(
        stored,
        path,
    )

    result.source_sha256 = (
        current_sha256
    )

    # Latest age is time-dependent.
    # Recalculate it even when the expensive file scan is resumed.
    if result.last_timestamp is not None:
        result.latest_age_hours = (
            calculate_latest_age_hours(
                result.last_timestamp,
                now_utc,
            )
        )

        result.latest_age_days = (
            result.latest_age_hours
            / 24.0
        )

    return result


def store_checkpoint_result(
    checkpoint: dict,
    result: MarketHistoryResult,
) -> None:

    key = checkpoint_key(
        result.timeframe,
        result.market,
    )

    checkpoint.setdefault(
        "completed",
        {},
    )[key] = (
        result_to_checkpoint_dict(
            result
        )
    )

    save_checkpoint(
        checkpoint
    )


# ============================================================
# GAP ANALYSIS
# ============================================================

def analyze_timestamp_gaps(
    timeframe: str,
    valid_unique_timestamps: pd.Series,
    result: MarketHistoryResult,
) -> None:

    if len(
        valid_unique_timestamps
    ) < 2:
        return

    expected_seconds = (
        TIMEFRAME_EXPECTED_INTERVAL_SECONDS[
            timeframe
        ]
    )

    timestamps = (
        valid_unique_timestamps
        .sort_values()
        .reset_index(drop=True)
    )

    differences = (
        timestamps
        .diff()
        .dt.total_seconds()
    )

    gap_mask = (
        differences
        > expected_seconds
    )

    gap_indices = list(
        differences[
            gap_mask
        ].index
    )

    if not gap_indices:
        return

    result.gap_count = len(
        gap_indices
    )

    largest_gap_seconds = 0.0
    largest_gap_intervals = 0

    total_missing = 0

    first_gap_start = None
    first_gap_end = None

    last_gap_start = None
    last_gap_end = None

    for index in gap_indices:
        previous_timestamp = (
            timestamps.iloc[
                index - 1
            ]
        )

        current_timestamp = (
            timestamps.iloc[
                index
            ]
        )

        gap_seconds = float(
            (
                current_timestamp
                - previous_timestamp
            ).total_seconds()
        )

        # Number of complete expected intervals between
        # the two observed candles.
        #
        # Example:
        # expected = 1h
        # 10:00 -> 14:00
        #
        # 4 intervals exist between endpoints.
        # Missing candles = 3.
        interval_count = int(
            round(
                gap_seconds
                / expected_seconds
            )
        )

        missing_candles = max(
            0,
            interval_count - 1,
        )

        total_missing += (
            missing_candles
        )

        if (
            gap_seconds
            > largest_gap_seconds
        ):
            largest_gap_seconds = (
                gap_seconds
            )

            largest_gap_intervals = (
                interval_count
            )

        if first_gap_start is None:
            first_gap_start = (
                previous_timestamp
            )

            first_gap_end = (
                current_timestamp
            )

        last_gap_start = (
            previous_timestamp
        )

        last_gap_end = (
            current_timestamp
        )

    result.estimated_missing_candles = (
        total_missing
    )

    result.largest_gap_seconds = (
        largest_gap_seconds
    )

    result.largest_gap_intervals = (
        largest_gap_intervals
    )

    result.first_gap_start = (
        first_gap_start
    )

    result.first_gap_end = (
        first_gap_end
    )

    result.last_gap_start = (
        last_gap_start
    )

    result.last_gap_end = (
        last_gap_end
    )

    result.warnings.append(
        "Timestamp gaps detected: "
        f"{result.gap_count:,} gaps / "
        f"estimated "
        f"{result.estimated_missing_candles:,} "
        "missing candles."
    )


# ============================================================
# CSV HISTORY READER
# ============================================================

def audit_market_file(
    timeframe: str,
    path: Path,
    now_utc: pd.Timestamp,
    source_sha256_before: str,
) -> MarketHistoryResult:

    result = MarketHistoryResult(
        timeframe=timeframe,
        market=path.stem.upper(),
        path=path,
        source_sha256=(
            source_sha256_before
        ),
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
        # Read header only.
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

        timestamp_column = (
            find_timestamp_column(
                header_df.columns
            )
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
        # Only timestamp data is required for this audit.
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

        # Count duplicated ROWS, matching the historical
        # Clean V001 behavior.
        result.duplicate_timestamp_count = int(
            valid_timestamps
            .duplicated(
                keep=False
            )
            .sum()
        )

        unique_timestamps = (
            valid_timestamps
            .drop_duplicates()
            .sort_values()
            .reset_index(drop=True)
        )

        result.first_timestamp = (
            unique_timestamps.iloc[0]
        )

        result.last_timestamp = (
            unique_timestamps.iloc[-1]
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
            result.history_hours
            / 24.0
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
            result.latest_age_hours
            / 24.0
        )

        if (
            result.invalid_timestamp_count
            > 0
        ):
            result.warnings.append(
                "Invalid timestamps: "
                f"{result.invalid_timestamp_count:,}"
            )

        if (
            result.duplicate_timestamp_count
            > 0
        ):
            result.warnings.append(
                "Duplicate timestamp rows: "
                f"{result.duplicate_timestamp_count:,}"
            )

        analyze_timestamp_gaps(
            timeframe=timeframe,
            valid_unique_timestamps=(
                unique_timestamps
            ),
            result=result,
        )

        # ----------------------------------------------------
        # SOURCE INTEGRITY
        #
        # Verify that the source file did not change while
        # this individual file was being audited.
        # ----------------------------------------------------

        source_sha256_after = (
            calculate_sha256(
                path
            )
        )

        if (
            source_sha256_after
            != source_sha256_before
        ):
            result.readable = False

            result.errors.append(
                "Source SHA256 changed during audit."
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
    checkpoint: dict,
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
            f"Directory not found: "
            f"{directory}"
        )

        print(
            "[FAIL] Directory not found."
        )

        return result

    if not directory.is_dir():
        result.errors.append(
            f"Path is not a directory: "
            f"{directory}"
        )

        print(
            "[FAIL] Path is not a directory."
        )

        return result

    result.directory_exists = True

    files = sorted(
        directory.glob(
            "*.csv"
        ),
        key=lambda item: (
            item.name.upper()
        ),
    )

    result.file_count = len(
        files
    )

    print(
        f"CSV files : "
        f"{result.file_count:,}"
    )

    if not files:
        result.errors.append(
            "No CSV files found."
        )

        print(
            "[FAIL] No CSV files found."
        )

        return result

    total_files = len(
        files
    )

    print()
    print(
        "Reading market history + gaps..."
    )
    print()

    for index, path in enumerate(
        files,
        start=1,
    ):
        market = (
            path.stem.upper()
        )

        try:
            current_sha256 = (
                calculate_sha256(
                    path
                )
            )

        except Exception as exc:
            market_result = (
                MarketHistoryResult(
                    timeframe=timeframe,
                    market=market,
                    path=path,
                    readable=False,
                )
            )

            market_result.errors.append(
                "SHA256 read failed: "
                f"{type(exc).__name__}: {exc}"
            )

        else:
            resumed_result = (
                get_resumable_result(
                    checkpoint=checkpoint,
                    timeframe=timeframe,
                    path=path,
                    current_sha256=(
                        current_sha256
                    ),
                    now_utc=now_utc,
                )
            )

            if resumed_result is not None:
                market_result = (
                    resumed_result
                )

            else:
                market_result = (
                    audit_market_file(
                        timeframe=timeframe,
                        path=path,
                        now_utc=now_utc,
                        source_sha256_before=(
                            current_sha256
                        ),
                    )
                )

                # Only successfully audited source states are
                # resumable.
                #
                # Unreadable/error files are intentionally
                # checked again on the next run.
                if market_result.readable:
                    store_checkpoint_result(
                        checkpoint,
                        market_result,
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

        if market_result.gap_count > 0:
            result.gap_markets.append(
                market_result.market
            )

            result.total_gap_count += (
                market_result.gap_count
            )

            result.total_estimated_missing_candles += (
                market_result
                .estimated_missing_candles
            )

        resume_text = (
            "RESUME"
            if market_result.resumed
            else "SCAN"
        )

        print(
            f"[{index:03d}/{total_files:03d}] "
            f"{market_result.market:<18} "
            f"{status:<4} "
            f"{resume_text:<6} "
            f"rows={market_result.row_count:>8,} "
            f"gaps={market_result.gap_count:>6,} "
            f"missing≈"
            f"{market_result.estimated_missing_candles:>7,} "
            f"age="
            f"{market_result.latest_age_hours:>8.2f}h"
        )

        for error in (
            market_result.errors
        ):
            print(
                f"    [ERROR] {error}"
            )

        for warning in (
            market_result.warnings
        ):
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
        key=lambda item: (
            item.history_days
        ),
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
        key=lambda item: (
            item.history_days
        ),
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
        key=lambda item: (
            item.latest_age_hours
        ),
    )


def get_largest_gap_market(
    result: TimeframeAuditResult,
) -> MarketHistoryResult | None:

    candidates = [
        item
        for item in result.markets
        if (
            item.readable
            and item.gap_count > 0
        )
    ]

    if not candidates:
        return None

    return max(
        candidates,
        key=lambda item: (
            item.largest_gap_seconds
        ),
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
        f"HISTORY + GAP STATISTICS"
    )

    print_separator("=")

    history_days = (
        get_history_day_values(
            result
        )
    )

    row_counts = (
        get_row_count_values(
            result
        )
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

    print(
        f"Files with gaps      : "
        f"{len(result.gap_markets):,}"
    )

    print(
        f"Detected gap events  : "
        f"{result.total_gap_count:,}"
    )

    print(
        f"Estimated missing    : "
        f"{result.total_estimated_missing_candles:,}"
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

    shortest = (
        get_shortest_history_market(
            result
        )
    )

    longest = (
        get_longest_history_market(
            result
        )
    )

    oldest_latest = (
        get_oldest_latest_candle(
            result
        )
    )

    largest_gap = (
        get_largest_gap_market(
            result
        )
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

    if largest_gap is not None:
        print()
        print(
            "Largest detected gap:"
        )

        print(
            f"  Market             : "
            f"{largest_gap.market}"
        )

        print(
            f"  Gap seconds        : "
            f"{largest_gap.largest_gap_seconds:,.0f}"
        )

        print(
            f"  Gap intervals      : "
            f"{largest_gap.largest_gap_intervals:,}"
        )

        print(
            f"  First gap start    : "
            f"{format_timestamp(largest_gap.first_gap_start)}"
        )

        print(
            f"  First gap end      : "
            f"{format_timestamp(largest_gap.first_gap_end)}"
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

    print(
        f"Gap-market flags     : "
        f"{len(result.gap_markets):,}"
    )


# ============================================================
# GAP REPORT
# ============================================================

def print_gap_report(
    result: TimeframeAuditResult,
) -> None:

    gap_items = [
        item
        for item in result.markets
        if (
            item.readable
            and item.gap_count > 0
        )
    ]

    if not gap_items:
        print()
        print_separator("-")

        print(
            f"{result.timeframe.upper()} "
            f"GAP REPORT"
        )

        print_separator("-")

        print(
            "[PASS] No timestamp gaps detected."
        )

        return

    gap_items = sorted(
        gap_items,
        key=lambda item: (
            -item.estimated_missing_candles,
            item.market,
        ),
    )

    print()
    print_separator("-")

    print(
        f"{result.timeframe.upper()} "
        f"GAP REPORT"
    )

    print_separator("-")

    print(
        "Gap findings are diagnostic."
    )

    print(
        "A gap does NOT automatically prove "
        "Collector failure."
    )

    print()

    for item in gap_items:
        largest_hours = (
            item.largest_gap_seconds
            / 3600.0
        )

        print(
            f"{item.market:<18} "
            f"gaps={item.gap_count:>6,} "
            f"missing≈"
            f"{item.estimated_missing_candles:>8,} "
            f"largest={largest_hours:>10.2f}h"
        )

        print(
            f"    first gap : "
            f"{format_timestamp(item.first_gap_start)}"
            f" -> "
            f"{format_timestamp(item.first_gap_end)}"
        )

        print(
            f"    last gap  : "
            f"{format_timestamp(item.last_gap_start)}"
            f" -> "
            f"{format_timestamp(item.last_gap_end)}"
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
            f"rows={item.row_count:>8,} "
            f"history={item.history_days:>10.2f}d "
            f"first="
            f"{format_timestamp(item.first_timestamp)}"
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

    for item in stale_items:
        print(
            f"{item.market:<18} "
            f"age={item.latest_age_hours:>10.2f}h "
            f"last="
            f"{format_timestamp(item.last_timestamp)}"
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
            missing.append(
                "h1"
            )

        if market not in h4_markets:
            missing.append(
                "h4"
            )

        if market not in d1_markets:
            missing.append(
                "d1"
            )

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
# STATUS CSV
# ============================================================

def write_status_csv(
    results: dict[
        str,
        TimeframeAuditResult,
    ],
) -> None:

    STATUS_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temp_path = STATUS_PATH.with_suffix(
        ".csv.tmp"
    )

    with temp_path.open(
        "w",
        encoding="utf-8-sig",
        newline="",
    ) as handle:

        writer = csv.DictWriter(
            handle,
            fieldnames=(
                STATUS_FIELDNAMES
            ),
        )

        writer.writeheader()

        for timeframe in (
            "h1",
            "h4",
            "d1",
        ):
            result = results[
                timeframe
            ]

            for item in result.markets:
                writer.writerow(
                    {
                        "timeframe": (
                            item.timeframe
                        ),
                        "market": (
                            item.market
                        ),
                        "source_path": (
                            str(item.path)
                        ),
                        "source_sha256": (
                            item.source_sha256
                        ),
                        "row_count": (
                            item.row_count
                        ),
                        "timestamp_column": (
                            item.timestamp_column
                        ),
                        "first_timestamp": (
                            timestamp_to_json(
                                item.first_timestamp
                            )
                            or ""
                        ),
                        "last_timestamp": (
                            timestamp_to_json(
                                item.last_timestamp
                            )
                            or ""
                        ),
                        "history_hours": (
                            item.history_hours
                        ),
                        "history_days": (
                            item.history_days
                        ),
                        "latest_age_hours": (
                            item.latest_age_hours
                        ),
                        "latest_age_days": (
                            item.latest_age_days
                        ),
                        "invalid_timestamp_count": (
                            item.invalid_timestamp_count
                        ),
                        "duplicate_timestamp_count": (
                            item.duplicate_timestamp_count
                        ),
                        "gap_count": (
                            item.gap_count
                        ),
                        "estimated_missing_candles": (
                            item.estimated_missing_candles
                        ),
                        "largest_gap_seconds": (
                            item.largest_gap_seconds
                        ),
                        "largest_gap_intervals": (
                            item.largest_gap_intervals
                        ),
                        "first_gap_start": (
                            timestamp_to_json(
                                item.first_gap_start
                            )
                            or ""
                        ),
                        "first_gap_end": (
                            timestamp_to_json(
                                item.first_gap_end
                            )
                            or ""
                        ),
                        "last_gap_start": (
                            timestamp_to_json(
                                item.last_gap_start
                            )
                            or ""
                        ),
                        "last_gap_end": (
                            timestamp_to_json(
                                item.last_gap_end
                            )
                            or ""
                        ),
                        "is_short_history": (
                            item.is_short_history
                        ),
                        "is_stale": (
                            item.is_stale
                        ),
                        "readable": (
                            item.readable
                        ),
                        "errors": (
                            " | ".join(
                                item.errors
                            )
                        ),
                        "warnings": (
                            " | ".join(
                                item.warnings
                            )
                        ),
                    }
                )

        handle.flush()
        os.fsync(
            handle.fileno()
        )

    os.replace(
        temp_path,
        STATUS_PATH,
    )


# ============================================================
# FINAL SOURCE SHA256 VERIFICATION
# ============================================================

def verify_all_source_hashes(
    results: dict[
        str,
        TimeframeAuditResult,
    ],
) -> tuple[int, list[str]]:

    mismatch_count = 0
    messages: list[str] = []

    print()
    print_separator("=")

    print(
        "FINAL OHLCV SHA256 VERIFICATION"
    )

    print_separator("=")

    all_items: list[
        MarketHistoryResult
    ] = []

    for timeframe in (
        "h1",
        "h4",
        "d1",
    ):
        all_items.extend(
            results[
                timeframe
            ].markets
        )

    total = len(
        all_items
    )

    for index, item in enumerate(
        all_items,
        start=1,
    ):
        if not item.source_sha256:
            mismatch_count += 1

            message = (
                f"{item.timeframe.upper()} "
                f"{item.market}: "
                "original SHA256 unavailable."
            )

            messages.append(
                message
            )

            print(
                f"[{index:03d}/{total:03d}] "
                f"[FAIL] {message}"
            )

            continue

        try:
            current_sha256 = (
                calculate_sha256(
                    item.path
                )
            )

        except Exception as exc:
            mismatch_count += 1

            message = (
                f"{item.timeframe.upper()} "
                f"{item.market}: "
                "final SHA256 read failed: "
                f"{type(exc).__name__}: {exc}"
            )

            messages.append(
                message
            )

            print(
                f"[{index:03d}/{total:03d}] "
                f"[FAIL] {message}"
            )

            continue

        if (
            current_sha256
            != item.source_sha256
        ):
            mismatch_count += 1

            message = (
                f"{item.timeframe.upper()} "
                f"{item.market}: "
                "SHA256 changed."
            )

            messages.append(
                message
            )

            print(
                f"[{index:03d}/{total:03d}] "
                f"[FAIL] {message}"
            )

        elif (
            index == 1
            or index == total
            or index % 50 == 0
        ):
            print(
                f"[{index:03d}/{total:03d}] "
                "[PASS] SHA256 unchanged"
            )

    if mismatch_count == 0:
        print()
        print(
            "[PASS] All OHLCV source SHA256 "
            "values remained unchanged."
        )

    else:
        print()
        print(
            f"[FAIL] SHA256 mismatches/errors: "
            f"{mismatch_count:,}"
        )

    return (
        mismatch_count,
        messages,
    )


# ============================================================
# OVERALL SUMMARY
# ============================================================

def print_overall_summary(
    results: dict[
        str,
        TimeframeAuditResult,
    ],
    mismatch_count: int,
    hash_mismatch_count: int,
    elapsed_seconds: float,
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

    gap_file_count = sum(
        len(
            result.gap_markets
        )
        for result in results.values()
    )

    total_gap_count = sum(
        result.total_gap_count
        for result in results.values()
    )

    estimated_missing = sum(
        result.total_estimated_missing_candles
        for result in results.values()
    )

    resumed_count = sum(
        1
        for result in results.values()
        for item in result.markets
        if item.resumed
    )

    scanned_count = (
        total_files
        - resumed_count
    )

    print()
    print_separator("=")

    print(
        "DATA HISTORY + GAP AUDIT SUMMARY"
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
        f"Freshly scanned files   : "
        f"{scanned_count:,}"
    )

    print(
        f"Resumed files           : "
        f"{resumed_count:,}"
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
        f"Files containing gaps   : "
        f"{gap_file_count:,}"
    )

    print(
        f"Timestamp gap events    : "
        f"{total_gap_count:,}"
    )

    print(
        f"Estimated missing       : "
        f"{estimated_missing:,}"
    )

    print(
        f"Coverage mismatches     : "
        f"{mismatch_count:,}"
    )

    print(
        f"SHA256 mismatches       : "
        f"{hash_mismatch_count:,}"
    )

    print(
        f"Elapsed total           : "
        f"{elapsed_seconds:,.2f}s"
    )

    print()

    print(
        "Safety:"
    )

    print(
        "  OHLCV write           : DISABLED"
    )

    print(
        "  OHLCV delete          : DISABLED"
    )

    print(
        "  Automatic gap repair  : DISABLED"
    )

    print(
        "  Feature generation    : DISABLED"
    )

    print(
        "  256 Detector          : DISABLED"
    )

    print(
        "  Future labels         : DISABLED"
    )

    print(
        "  Prediction            : DISABLED"
    )

    print(
        "  Trading               : DISABLED"
    )

    print(
        "  Git reset             : DISABLED"
    )

    print(
        "  Git clean             : DISABLED"
    )

    print(
        "  Git commit            : DISABLED"
    )

    print(
        "  Git push              : DISABLED"
    )

    print()

    print(
        "Runtime:"
    )

    print(
        f"  Checkpoint            : "
        f"{CHECKPOINT_PATH}"
    )

    print(
        f"  Status CSV            : "
        f"{STATUS_PATH}"
    )

    print()

    print(
        "Important:"
    )

    print(
        "  Gap counts are diagnostic findings."
    )

    print(
        "  They do not automatically prove "
        "Collector failure."
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
    coverage_mismatch_count: int,
    hash_mismatch_count: int,
) -> int:

    fatal_count = 0

    for result in results.values():
        if not result.directory_exists:
            fatal_count += 1

        fatal_count += (
            result.unreadable_file_count
        )

    fatal_count += (
        hash_mismatch_count
    )

    # Cross-timeframe missing files are structural problems
    # for the current full-market research dataset.
    fatal_count += (
        coverage_mismatch_count
    )

    return fatal_count


# ============================================================
# MAIN
# ============================================================

def main() -> int:

    started_monotonic = (
        time.monotonic()
    )

    print_header(
        "UPBIT SURGE MONITOR - "
        "OHLCV DATA HISTORY + GAP AUDIT "
        "CLEAN V003"
    )

    print(
        f"Project root        : "
        f"{BASE_DIR}"
    )

    print(
        f"Data directory      : "
        f"{DATA_DIR}"
    )

    print(
        f"OHLCV directory     : "
        f"{OHLCV_DIR}"
    )

    print(
        "Timeframes          : "
        "h1 / h4 / d1"
    )

    print()

    print(
        "Mode                : READ ONLY"
    )

    print(
        "Source modification : DISABLED"
    )

    print(
        "Source deletion     : DISABLED"
    )

    print(
        "Automatic repair    : DISABLED"
    )

    print(
        "Feature generation  : DISABLED"
    )

    print(
        "256 Detector        : DISABLED"
    )

    print(
        "Prediction          : DISABLED"
    )

    print(
        "Trading             : DISABLED"
    )

    print()

    now_utc = get_now_utc()

    print(
        f"Audit UTC time      : "
        f"{format_timestamp(now_utc)}"
    )

    print(
        f"Checkpoint          : "
        f"{CHECKPOINT_PATH}"
    )

    print(
        f"Status CSV          : "
        f"{STATUS_PATH}"
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

    VALIDATION_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    # --------------------------------------------------------
    # LOAD CHECKPOINT
    # --------------------------------------------------------

    checkpoint = (
        load_checkpoint()
    )

    completed_before = len(
        checkpoint.get(
            "completed",
            {},
        )
    )

    print()
    print(
        f"Checkpoint entries  : "
        f"{completed_before:,}"
    )

    if completed_before > 0:
        print(
            "[RESUME] Existing completed audit "
            "entries found."
        )

        print(
            "[RESUME] Unchanged source files "
            "will reuse their audit results."
        )

        print(
            "[RESUME] Changed source files "
            "will be scanned again."
        )

    else:
        print(
            "[RESUME] No completed checkpoint "
            "entries found."
        )

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
            checkpoint=checkpoint,
        )

        classify_relative_history(
            result
        )

        classify_stale_data(
            result
        )

        results[timeframe] = (
            result
        )

    # --------------------------------------------------------
    # WRITE STATUS CSV
    # --------------------------------------------------------

    try:
        write_status_csv(
            results
        )

        print()
        print(
            "[PASS] Audit status CSV written:"
        )

        print(
            f"       {STATUS_PATH}"
        )

    except Exception as exc:
        print()
        print(
            "[FATAL] Failed to write audit "
            "status CSV."
        )

        print(
            f"{type(exc).__name__}: {exc}"
        )

        return 1

    # --------------------------------------------------------
    # PRINT TIMEFRAME REPORTS
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

        print_gap_report(
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

    coverage_mismatch_count = (
        print_cross_timeframe_coverage(
            results
        )
    )

    # --------------------------------------------------------
    # FINAL SOURCE HASH VERIFICATION
    # --------------------------------------------------------

    (
        hash_mismatch_count,
        hash_messages,
    ) = verify_all_source_hashes(
        results
    )

    if hash_messages:
        print()

        for message in hash_messages:
            print(
                f"[SHA256] {message}"
            )

    # --------------------------------------------------------
    # OVERALL SUMMARY
    # --------------------------------------------------------

    elapsed_seconds = (
        time.monotonic()
        - started_monotonic
    )

    print_overall_summary(
        results=results,
        mismatch_count=(
            coverage_mismatch_count
        ),
        hash_mismatch_count=(
            hash_mismatch_count
        ),
        elapsed_seconds=(
            elapsed_seconds
        ),
    )

    # --------------------------------------------------------
    # FINAL RESULT
    # --------------------------------------------------------

    fatal_error_count = (
        count_fatal_audit_errors(
            results=results,
            coverage_mismatch_count=(
                coverage_mismatch_count
            ),
            hash_mismatch_count=(
                hash_mismatch_count
            ),
        )
    )

    total_gap_files = sum(
        len(
            result.gap_markets
        )
        for result in results.values()
    )

    total_gap_events = sum(
        result.total_gap_count
        for result in results.values()
    )

    total_missing_estimate = sum(
        result.total_estimated_missing_candles
        for result in results.values()
    )

    print()
    print_separator("=")

    if fatal_error_count > 0:
        print(
            "[RESULT] DATA HISTORY + GAP "
            "AUDIT FAILED"
        )

        print(
            f"[ERROR] Fatal structural/integrity "
            f"problems: "
            f"{fatal_error_count:,}"
        )

        print(
            "[SAFETY] No OHLCV source file "
            "was intentionally modified."
        )

        print_separator("=")

        return 1

    print(
        "[RESULT] DATA HISTORY + GAP "
        "AUDIT PASSED"
    )

    print(
        "[PASS] All OHLCV CSV files were "
        "read successfully."
    )

    print(
        "[PASS] h1 / h4 / d1 market coverage "
        "is structurally consistent."
    )

    print(
        "[PASS] All OHLCV source SHA256 "
        "values remained unchanged."
    )

    if total_gap_files == 0:
        print(
            "[PASS] No internal timestamp "
            "gaps were detected."
        )

        print(
            "[NEXT] OHLCV history integrity "
            "is ready for the next stage."
        )

    else:
        print(
            f"[INFO] Files containing gaps : "
            f"{total_gap_files:,}"
        )

        print(
            f"[INFO] Gap events           : "
            f"{total_gap_events:,}"
        )

        print(
            f"[INFO] Estimated missing    : "
            f"{total_missing_estimate:,}"
        )

        print(
            "[NEXT] Review gap findings "
            "before deciding whether "
            "historical recovery is required."
        )

    print(
        "[PASS] Resume/checkpoint support "
        "is enabled."
    )

    print(
        "[PASS] Original OHLCV data remained "
        "READ ONLY."
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
        print_separator("=")

        print(
            "[STOP] Data history audit "
            "interrupted by user."
        )

        print(
            "[RESUME] Completed file audits "
            "remain stored in the checkpoint."
        )

        print(
            "[RESUME] Run the same command "
            "again to continue."
        )

        print_separator("=")

        exit_code = 130

    except Exception as exc:
        print()
        print_separator("=")

        print(
            "DATA HISTORY + GAP AUDIT "
            "FATAL ERROR"
        )

        print_separator("=")

        print(
            f"{type(exc).__name__}: {exc}"
        )

        print()

        print(
            "[SAFETY] No automatic cleanup "
            "or source deletion was executed."
        )

        print(
            "[RESUME] Successfully completed "
            "checkpoint entries remain available."
        )

        print_separator("=")

        exit_code = 1

    sys.exit(
        exit_code
    )
