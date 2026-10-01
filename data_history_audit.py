"""
Upbit Surge Monitor - Data History Audit Clean V003
===================================================

File:
    data_history_audit.py

Purpose:
    Existing OHLCV history files are audited in READ ONLY mode.

    This program checks:

    1. Full KRW market OHLCV files
    2. h1 / h4 / d1 datasets
    3. CSV readability
    4. Required OHLCV columns
    5. Timestamp validity
    6. Timestamp ordering
    7. Duplicate timestamps
    8. Internal timestamp gaps
    9. Estimated missing candles
    10. Persistent checkpoint / Resume
    11. Status CSV generation
    12. Source SHA256 tracking

Important:
    This script DOES NOT repair historical data.

    This script DOES NOT:
        - download candles
        - rewrite OHLCV
        - delete OHLCV
        - fill gaps
        - build features
        - run detector
        - create future labels
        - predict prices
        - trade
        - run git reset
        - run git clean
        - run git commit
        - run git push

Resume:
    Successfully audited jobs are stored in:

        data/validation/data_history_audit_checkpoint.json

    If the same source file is seen again with the same SHA256,
    the completed result is reused instead of auditing the file again.

Windows:
    stdout / stderr are configured for UTF-8 where supported.

    This prevents Windows CP949 console encoding errors from stopping
    a long-running audit because of Unicode output characters.

Safety:
    OHLCV source CSV files are READ ONLY.
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
import sys
import time
import traceback
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


# ============================================================
# WINDOWS / CONSOLE UTF-8 SAFETY
# ============================================================

def configure_console_encoding() -> None:
    """
    Prevent Windows CP949 console encoding failures.

    GitHub self-hosted Windows runners can inherit CP949 as the console
    encoding. Python output containing Unicode characters can then raise
    UnicodeEncodeError and terminate the audit.

    errors="replace" is intentionally used as a final safety layer.
    Console formatting must never terminate data auditing.
    """

    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(
                encoding="utf-8",
                errors="replace",
            )
    except Exception:
        pass

    try:
        if hasattr(sys.stderr, "reconfigure"):
            sys.stderr.reconfigure(
                encoding="utf-8",
                errors="replace",
            )
    except Exception:
        pass


configure_console_encoding()


# ============================================================
# PROJECT CONFIGURATION
# ============================================================

PROJECT_NAME = "Upbit Surge Monitor"
VERSION = "Data History Audit Clean V003"

BASE_DIR = Path(__file__).resolve().parent

DATA_DIR = BASE_DIR / "data"
OHLCV_DIR = DATA_DIR / "ohlcv"

TIMEFRAME_DIRS: Dict[str, Path] = {
    "h1": OHLCV_DIR / "h1",
    "h4": OHLCV_DIR / "h4",
    "d1": OHLCV_DIR / "d1",
}

VALIDATION_DIR = DATA_DIR / "validation"

CHECKPOINT_FILE = (
    VALIDATION_DIR /
    "data_history_audit_checkpoint.json"
)

STATUS_FILE = (
    DATA_DIR /
    "data_history_audit_status.csv"
)

TIMEFRAME_SECONDS: Dict[str, int] = {
    "h1": 60 * 60,
    "h4": 4 * 60 * 60,
    "d1": 24 * 60 * 60,
}

REQUIRED_OHLCV_COLUMNS = {
    "timestamp",
    "open",
    "high",
    "low",
    "close",
    "volume",
}

TIME_COLUMN_CANDIDATES = (
    "timestamp",
    "datetime",
    "date",
    "time",
    "candle_date_time_utc",
    "candle_date_time_kst",
)

STATUS_COLUMNS = [
    "timeframe",
    "market",
    "file",
    "readable",
    "rows",
    "first_timestamp",
    "last_timestamp",
    "duplicate_timestamps",
    "gap_count",
    "estimated_missing_candles",
    "max_gap_seconds",
    "source_sha256",
    "resumed",
    "errors",
]

CHECKPOINT_VERSION = 1


# ============================================================
# DATA CLASS
# ============================================================

@dataclass
class AuditResult:
    timeframe: str
    market: str
    file: str

    readable: bool

    rows: int

    first_timestamp: str
    last_timestamp: str

    duplicate_timestamps: int

    gap_count: int
    estimated_missing_candles: int
    max_gap_seconds: int

    source_sha256: str

    resumed: bool

    errors: str


# ============================================================
# GENERAL HELPERS
# ============================================================

def utc_now_iso() -> str:
    return datetime.now(
        timezone.utc
    ).replace(
        microsecond=0
    ).isoformat()


def safe_print(*args: Any, **kwargs: Any) -> None:
    """
    Last-resort console safety wrapper.

    Normal print() should already work after UTF-8 reconfiguration.
    This wrapper additionally prevents a console encoding problem from
    terminating the audit.
    """

    try:
        print(*args, **kwargs)

    except UnicodeEncodeError:

        separator = kwargs.get("sep", " ")
        ending = kwargs.get("end", "\n")

        text = separator.join(
            str(value)
            for value in args
        )

        encoding = getattr(
            sys.stdout,
            "encoding",
            None,
        ) or "utf-8"

        safe_text = (
            text
            .encode(
                encoding,
                errors="replace",
            )
            .decode(
                encoding,
                errors="replace",
            )
        )

        try:
            sys.stdout.write(
                safe_text + ending
            )
            sys.stdout.flush()

        except Exception:
            pass


def separator(
    char: str = "=",
    length: int = 76,
) -> None:

    safe_print(
        char * length
    )


def ensure_runtime_directories() -> None:

    VALIDATION_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )


def normalize_market_name(
    file_path: Path,
) -> str:

    return file_path.stem.upper()


# ============================================================
# SHA256
# ============================================================

def calculate_sha256(
    file_path: Path,
    chunk_size: int = 1024 * 1024,
) -> str:

    digest = hashlib.sha256()

    with file_path.open("rb") as file_handle:

        while True:

            chunk = file_handle.read(
                chunk_size
            )

            if not chunk:
                break

            digest.update(chunk)

    return digest.hexdigest()


# ============================================================
# CHECKPOINT
# ============================================================

def empty_checkpoint() -> Dict[str, Any]:

    return {
        "version": CHECKPOINT_VERSION,
        "project": PROJECT_NAME,
        "audit_version": VERSION,
        "updated_at_utc": utc_now_iso(),
        "completed": {},
    }


def load_checkpoint() -> Dict[str, Any]:

    if not CHECKPOINT_FILE.exists():

        return empty_checkpoint()

    try:

        with CHECKPOINT_FILE.open(
            "r",
            encoding="utf-8",
        ) as file_handle:

            data = json.load(
                file_handle
            )

        if not isinstance(
            data,
            dict,
        ):
            raise ValueError(
                "Checkpoint root is not an object."
            )

        completed = data.get(
            "completed"
        )

        if not isinstance(
            completed,
            dict,
        ):

            data["completed"] = {}

        return data

    except Exception as exc:

        safe_print(
            "[WARNING] Existing checkpoint could not be read."
        )

        safe_print(
            f"[WARNING] {type(exc).__name__}: {exc}"
        )

        safe_print(
            "[SAFETY] Existing checkpoint was NOT deleted."
        )

        safe_print(
            "[INFO] Audit will continue with an empty in-memory checkpoint."
        )

        return empty_checkpoint()


def save_checkpoint(
    checkpoint: Dict[str, Any],
) -> None:

    checkpoint["version"] = (
        CHECKPOINT_VERSION
    )

    checkpoint["project"] = (
        PROJECT_NAME
    )

    checkpoint["audit_version"] = (
        VERSION
    )

    checkpoint["updated_at_utc"] = (
        utc_now_iso()
    )

    temp_file = CHECKPOINT_FILE.with_suffix(
        CHECKPOINT_FILE.suffix + ".tmp"
    )

    with temp_file.open(
        "w",
        encoding="utf-8",
        newline="\n",
    ) as file_handle:

        json.dump(
            checkpoint,
            file_handle,
            ensure_ascii=False,
            indent=2,
        )

        file_handle.flush()

        try:
            os.fsync(
                file_handle.fileno()
            )
        except OSError:
            pass

    os.replace(
        temp_file,
        CHECKPOINT_FILE,
    )


def checkpoint_key(
    timeframe: str,
    market: str,
) -> str:

    return (
        f"{timeframe}:{market}"
    )


def checkpoint_result_is_reusable(
    checkpoint_entry: Any,
    source_sha256: str,
) -> bool:

    if not isinstance(
        checkpoint_entry,
        dict,
    ):
        return False

    saved_sha256 = checkpoint_entry.get(
        "source_sha256"
    )

    if saved_sha256 != source_sha256:
        return False

    result = checkpoint_entry.get(
        "result"
    )

    if not isinstance(
        result,
        dict,
    ):
        return False

    if result.get(
        "readable"
    ) is not True:
        return False

    return True


def result_from_checkpoint(
    checkpoint_entry: Dict[str, Any],
) -> AuditResult:

    raw = dict(
        checkpoint_entry["result"]
    )

    raw["resumed"] = True

    return AuditResult(
        timeframe=str(
            raw.get(
                "timeframe",
                "",
            )
        ),
        market=str(
            raw.get(
                "market",
                "",
            )
        ),
        file=str(
            raw.get(
                "file",
                "",
            )
        ),
        readable=bool(
            raw.get(
                "readable",
                False,
            )
        ),
        rows=int(
            raw.get(
                "rows",
                0,
            )
            or 0
        ),
        first_timestamp=str(
            raw.get(
                "first_timestamp",
                "",
            )
        ),
        last_timestamp=str(
            raw.get(
                "last_timestamp",
                "",
            )
        ),
        duplicate_timestamps=int(
            raw.get(
                "duplicate_timestamps",
                0,
            )
            or 0
        ),
        gap_count=int(
            raw.get(
                "gap_count",
                0,
            )
            or 0
        ),
        estimated_missing_candles=int(
            raw.get(
                "estimated_missing_candles",
                0,
            )
            or 0
        ),
        max_gap_seconds=int(
            raw.get(
                "max_gap_seconds",
                0,
            )
            or 0
        ),
        source_sha256=str(
            raw.get(
                "source_sha256",
                "",
            )
        ),
        resumed=True,
        errors=str(
            raw.get(
                "errors",
                "",
            )
        ),
    )


# ============================================================
# STATUS CSV
# ============================================================

def write_status_csv(
    results: List[AuditResult],
) -> None:

    temp_file = STATUS_FILE.with_suffix(
        STATUS_FILE.suffix + ".tmp"
    )

    with temp_file.open(
        "w",
        encoding="utf-8-sig",
        newline="",
    ) as file_handle:

        writer = csv.DictWriter(
            file_handle,
            fieldnames=STATUS_COLUMNS,
        )

        writer.writeheader()

        for result in results:

            row = asdict(
                result
            )

            writer.writerow(
                {
                    column: row.get(
                        column,
                        "",
                    )
                    for column
                    in STATUS_COLUMNS
                }
            )

        file_handle.flush()

        try:
            os.fsync(
                file_handle.fileno()
            )
        except OSError:
            pass

    os.replace(
        temp_file,
        STATUS_FILE,
    )


# ============================================================
# CSV READING
# ============================================================

def read_csv_safely(
    file_path: Path,
) -> pd.DataFrame:

    encodings = (
        "utf-8-sig",
        "utf-8",
        "cp949",
    )

    last_error: Optional[
        Exception
    ] = None

    for encoding in encodings:

        try:

            return pd.read_csv(
                file_path,
                encoding=encoding,
                low_memory=False,
            )

        except UnicodeDecodeError as exc:

            last_error = exc

    if last_error is not None:
        raise last_error

    return pd.read_csv(
        file_path,
        low_memory=False,
    )


# ============================================================
# COLUMN NORMALIZATION
# ============================================================

def normalize_columns(
    dataframe: pd.DataFrame,
) -> pd.DataFrame:

    dataframe = dataframe.copy()

    dataframe.columns = [
        str(column)
        .strip()
        .lower()
        for column
        in dataframe.columns
    ]

    return dataframe


def detect_timestamp_column(
    dataframe: pd.DataFrame,
) -> Optional[str]:

    for candidate in TIME_COLUMN_CANDIDATES:

        if candidate in dataframe.columns:
            return candidate

    return None


def validate_required_columns(
    dataframe: pd.DataFrame,
    timestamp_column: str,
) -> List[str]:

    errors: List[str] = []

    required_numeric = (
        "open",
        "high",
        "low",
        "close",
        "volume",
    )

    for column in required_numeric:

        if column not in dataframe.columns:

            errors.append(
                f"Missing required column: {column}"
            )

    if timestamp_column not in dataframe.columns:

        errors.append(
            "Timestamp column is missing."
        )

    return errors


# ============================================================
# TIMESTAMP PARSING
# ============================================================

def parse_timestamp_series(
    series: pd.Series,
) -> pd.Series:

    if pd.api.types.is_numeric_dtype(
        series
    ):

        numeric = pd.to_numeric(
            series,
            errors="coerce",
        )

        valid = numeric.dropna()

        if valid.empty:

            return pd.to_datetime(
                series,
                errors="coerce",
                utc=True,
            )

        median_value = float(
            valid.abs().median()
        )

        if median_value >= 1e17:
            unit = "ns"

        elif median_value >= 1e14:
            unit = "us"

        elif median_value >= 1e11:
            unit = "ms"

        else:
            unit = "s"

        return pd.to_datetime(
            numeric,
            unit=unit,
            errors="coerce",
            utc=True,
        )

    return pd.to_datetime(
        series,
        errors="coerce",
        utc=True,
    )


# ============================================================
# GAP ANALYSIS
# ============================================================

def analyze_timestamp_gaps(
    timestamps: pd.Series,
    expected_seconds: int,
) -> Tuple[
    int,
    int,
    int,
]:

    if timestamps.empty:

        return (
            0,
            0,
            0,
        )

    unique_sorted = (
        timestamps
        .dropna()
        .drop_duplicates()
        .sort_values()
        .reset_index(drop=True)
    )

    if len(
        unique_sorted
    ) < 2:

        return (
            0,
            0,
            0,
        )

    differences = (
        unique_sorted
        .diff()
        .dt.total_seconds()
        .dropna()
    )

    gaps = differences[
        differences > expected_seconds
    ]

    gap_count = int(
        len(gaps)
    )

    estimated_missing = 0

    for gap_seconds in gaps:

        if pd.isna(
            gap_seconds
        ):
            continue

        intervals = int(
            gap_seconds // expected_seconds
        )

        missing = max(
            intervals - 1,
            0,
        )

        estimated_missing += (
            missing
        )

    if gap_count > 0:

        max_gap_seconds = int(
            gaps.max()
        )

    else:

        max_gap_seconds = 0

    return (
        gap_count,
        estimated_missing,
        max_gap_seconds,
    )


# ============================================================
# SINGLE FILE AUDIT
# ============================================================

def audit_file(
    timeframe: str,
    file_path: Path,
    source_sha256: str,
) -> AuditResult:

    market = normalize_market_name(
        file_path
    )

    errors: List[str] = []

    rows = 0

    first_timestamp = ""
    last_timestamp = ""

    duplicate_timestamps = 0

    gap_count = 0
    estimated_missing = 0
    max_gap_seconds = 0

    try:

        dataframe = read_csv_safely(
            file_path
        )

        dataframe = normalize_columns(
            dataframe
        )

        rows = int(
            len(dataframe)
        )

        if rows == 0:

            errors.append(
                "CSV contains zero rows."
            )

            return AuditResult(
                timeframe=timeframe,
                market=market,
                file=str(file_path),
                readable=False,
                rows=rows,
                first_timestamp="",
                last_timestamp="",
                duplicate_timestamps=0,
                gap_count=0,
                estimated_missing_candles=0,
                max_gap_seconds=0,
                source_sha256=source_sha256,
                resumed=False,
                errors=" | ".join(
                    errors
                ),
            )

        timestamp_column = (
            detect_timestamp_column(
                dataframe
            )
        )

        if timestamp_column is None:

            errors.append(
                "No supported timestamp column found."
            )

            return AuditResult(
                timeframe=timeframe,
                market=market,
                file=str(file_path),
                readable=False,
                rows=rows,
                first_timestamp="",
                last_timestamp="",
                duplicate_timestamps=0,
                gap_count=0,
                estimated_missing_candles=0,
                max_gap_seconds=0,
                source_sha256=source_sha256,
                resumed=False,
                errors=" | ".join(
                    errors
                ),
            )

        column_errors = (
            validate_required_columns(
                dataframe,
                timestamp_column,
            )
        )

        errors.extend(
            column_errors
        )

        if column_errors:

            return AuditResult(
                timeframe=timeframe,
                market=market,
                file=str(file_path),
                readable=False,
                rows=rows,
                first_timestamp="",
                last_timestamp="",
                duplicate_timestamps=0,
                gap_count=0,
                estimated_missing_candles=0,
                max_gap_seconds=0,
                source_sha256=source_sha256,
                resumed=False,
                errors=" | ".join(
                    errors
                ),
            )

        timestamps = (
            parse_timestamp_series(
                dataframe[
                    timestamp_column
                ]
            )
        )

        invalid_timestamp_count = int(
            timestamps.isna().sum()
        )

        if invalid_timestamp_count > 0:

            errors.append(
                "Invalid timestamps: "
                f"{invalid_timestamp_count}"
            )

        valid_timestamps = (
            timestamps.dropna()
        )

        if valid_timestamps.empty:

            errors.append(
                "No usable timestamps."
            )

            return AuditResult(
                timeframe=timeframe,
                market=market,
                file=str(file_path),
                readable=False,
                rows=rows,
                first_timestamp="",
                last_timestamp="",
                duplicate_timestamps=0,
                gap_count=0,
                estimated_missing_candles=0,
                max_gap_seconds=0,
                source_sha256=source_sha256,
                resumed=False,
                errors=" | ".join(
                    errors
                ),
            )

        first_timestamp = (
            valid_timestamps.min().isoformat()
        )

        last_timestamp = (
            valid_timestamps.max().isoformat()
        )

        duplicate_timestamps = int(
            valid_timestamps.duplicated().sum()
        )

        if duplicate_timestamps > 0:

            errors.append(
                "Duplicate timestamps: "
                f"{duplicate_timestamps}"
            )

        original_order = (
            valid_timestamps
            .reset_index(drop=True)
        )

        sorted_order = (
            valid_timestamps
            .sort_values()
            .reset_index(drop=True)
        )

        if not original_order.equals(
            sorted_order
        ):

            errors.append(
                "Timestamps are not sorted ascending."
            )

        expected_seconds = (
            TIMEFRAME_SECONDS[
                timeframe
            ]
        )

        (
            gap_count,
            estimated_missing,
            max_gap_seconds,
        ) = analyze_timestamp_gaps(
            valid_timestamps,
            expected_seconds,
        )

        return AuditResult(
            timeframe=timeframe,
            market=market,
            file=str(file_path),
            readable=True,
            rows=rows,
            first_timestamp=first_timestamp,
            last_timestamp=last_timestamp,
            duplicate_timestamps=duplicate_timestamps,
            gap_count=gap_count,
            estimated_missing_candles=estimated_missing,
            max_gap_seconds=max_gap_seconds,
            source_sha256=source_sha256,
            resumed=False,
            errors=" | ".join(
                errors
            ),
        )

    except Exception as exc:

        errors.append(
            f"{type(exc).__name__}: {exc}"
        )

        return AuditResult(
            timeframe=timeframe,
            market=market,
            file=str(file_path),
            readable=False,
            rows=rows,
            first_timestamp=first_timestamp,
            last_timestamp=last_timestamp,
            duplicate_timestamps=duplicate_timestamps,
            gap_count=gap_count,
            estimated_missing_candles=estimated_missing,
            max_gap_seconds=max_gap_seconds,
            source_sha256=source_sha256,
            resumed=False,
            errors=" | ".join(
                errors
            ),
        )


# ============================================================
# DIRECTORY / MARKET DISCOVERY
# ============================================================

def get_csv_files(
    directory: Path,
) -> List[Path]:

    if not directory.exists():

        raise FileNotFoundError(
            f"Required directory not found: {directory}"
        )

    files = sorted(
        (
            path
            for path
            in directory.glob("*.csv")
            if path.is_file()
        ),
        key=lambda path: path.name.upper(),
    )

    if not files:

        raise RuntimeError(
            f"No CSV files found: {directory}"
        )

    return files


def verify_market_sets(
    files_by_timeframe: Dict[
        str,
        List[Path],
    ],
) -> List[str]:

    market_sets: Dict[
        str,
        set[str],
    ] = {}

    for timeframe, files in (
        files_by_timeframe.items()
    ):

        market_sets[
            timeframe
        ] = {
            normalize_market_name(
                file_path
            )
            for file_path
            in files
        }

    h1 = market_sets["h1"]
    h4 = market_sets["h4"]
    d1 = market_sets["d1"]

    if h1 != h4:

        missing_h4 = sorted(
            h1 - h4
        )

        extra_h4 = sorted(
            h4 - h1
        )

        raise RuntimeError(
            "h1 / h4 market set mismatch. "
            f"Missing h4={missing_h4[:20]}, "
            f"Extra h4={extra_h4[:20]}"
        )

    if h1 != d1:

        missing_d1 = sorted(
            h1 - d1
        )

        extra_d1 = sorted(
            d1 - h1
        )

        raise RuntimeError(
            "h1 / d1 market set mismatch. "
            f"Missing d1={missing_d1[:20]}, "
            f"Extra d1={extra_d1[:20]}"
        )

    return sorted(
        h1
    )


# ============================================================
# PROGRESS OUTPUT
# ============================================================

def print_job_result(
    job_number: int,
    total_jobs: int,
    result: AuditResult,
) -> None:

    prefix = (
        "[RESUME]"
        if result.resumed
        else "[AUDIT]"
    )

    safe_print(
        f"{prefix} "
        f"[{job_number}/{total_jobs}] "
        f"{result.timeframe.upper()} "
        f"{result.market} "
        f"rows={result.rows:,} "
        f"gaps={result.gap_count:,} "
        f"missing_est={result.estimated_missing_candles:,} "
        f"duplicates={result.duplicate_timestamps:,}"
    )

    if result.errors:

        safe_print(
            f"         notes={result.errors}"
        )


# ============================================================
# SUMMARY
# ============================================================

def print_summary(
    results: List[AuditResult],
    markets: List[str],
    elapsed_seconds: float,
) -> None:

    readable_count = sum(
        1
        for result
        in results
        if result.readable
    )

    failed_count = (
        len(results)
        - readable_count
    )

    resumed_count = sum(
        1
        for result
        in results
        if result.resumed
    )

    audited_count = (
        len(results)
        - resumed_count
    )

    total_rows = sum(
        result.rows
        for result
        in results
    )

    files_with_gaps = sum(
        1
        for result
        in results
        if result.gap_count > 0
    )

    total_gap_events = sum(
        result.gap_count
        for result
        in results
    )

    estimated_missing = sum(
        result.estimated_missing_candles
        for result
        in results
    )

    duplicate_count = sum(
        result.duplicate_timestamps
        for result
        in results
    )

    separator()

    safe_print(
        "FULL KRW DATA HISTORY AUDIT SUMMARY"
    )

    separator()

    safe_print(
        f"Project          : {PROJECT_NAME}"
    )

    safe_print(
        f"Version          : {VERSION}"
    )

    safe_print(
        "Execution mode   : FULL KRW MARKET"
    )

    safe_print(
        f"Markets          : {len(markets):,}"
    )

    safe_print(
        f"Timeframes       : {len(TIMEFRAME_DIRS):,}"
    )

    safe_print(
        f"Audit jobs       : {len(results):,}"
    )

    safe_print(
        f"Passed jobs      : {readable_count:,}"
    )

    safe_print(
        f"Failed jobs      : {failed_count:,}"
    )

    safe_print(
        f"Fresh audits     : {audited_count:,}"
    )

    safe_print(
        f"Resumed jobs     : {resumed_count:,}"
    )

    safe_print(
        f"Total rows       : {total_rows:,}"
    )

    safe_print("")

    safe_print(
        "Gap audit:"
    )

    safe_print(
        f"  Files w/gaps   : {files_with_gaps:,}"
    )

    safe_print(
        f"  Gap events     : {total_gap_events:,}"
    )

    safe_print(
        f"  Missing est.   : {estimated_missing:,}"
    )

    safe_print(
        f"  Duplicates     : {duplicate_count:,}"
    )

    safe_print("")

    safe_print(
        "Resume:"
    )

    safe_print(
        f"  Checkpoint     : {CHECKPOINT_FILE}"
    )

    safe_print(
        f"  Status CSV     : {STATUS_FILE}"
    )

    safe_print("")

    safe_print(
        "Safety:"
    )

    safe_print(
        "  OHLCV write    : DISABLED"
    )

    safe_print(
        "  OHLCV delete   : DISABLED"
    )

    safe_print(
        "  Gap repair     : DISABLED"
    )

    safe_print(
        "  Feature build  : DISABLED"
    )

    safe_print(
        "  256 Detector   : DISABLED"
    )

    safe_print(
        "  Future labels  : DISABLED"
    )

    safe_print(
        "  Prediction     : DISABLED"
    )

    safe_print(
        "  Trading        : DISABLED"
    )

    safe_print(
        "  Git reset      : DISABLED"
    )

    safe_print(
        "  Git clean      : DISABLED"
    )

    safe_print(
        "  Git commit     : DISABLED"
    )

    safe_print(
        "  Git push       : DISABLED"
    )

    safe_print("")

    safe_print(
        f"Elapsed total    : {elapsed_seconds:.2f}s"
    )

    separator()

    if failed_count > 0:

        safe_print(
            "[RESULT] DATA HISTORY AUDIT FAILED"
        )

        safe_print(
            f"[FAIL] {failed_count:,} unreadable or invalid jobs detected."
        )

    else:

        safe_print(
            "[RESULT] FULL KRW DATA HISTORY AUDIT PASSED"
        )

        safe_print(
            f"[PASS] {len(markets):,} markets verified."
        )

        safe_print(
            f"[PASS] {len(results):,} audit jobs verified."
        )

        safe_print(
            "[PASS] OHLCV source files were read only."
        )

        safe_print(
            "[PASS] Persistent Resume checkpoint is available."
        )

        if files_with_gaps > 0:

            safe_print(
                "[INFO] Historical timestamp gaps were detected."
            )

            safe_print(
                "[NEXT] Review missing history before recovery."
            )

        else:

            safe_print(
                "[PASS] No internal timestamp gaps were detected."
            )

            safe_print(
                "[NEXT] Historical dataset is ready for the next stage."
            )

    separator()


# ============================================================
# MAIN AUDIT
# ============================================================

def main() -> int:

    started_at = time.perf_counter()

    ensure_runtime_directories()

    separator()

    safe_print(
        "UPBIT SURGE MONITOR"
    )

    safe_print(
        "FULL KRW DATA HISTORY AUDIT CLEAN V003"
    )

    separator()

    safe_print(
        f"Project          : {PROJECT_NAME}"
    )

    safe_print(
        f"Version          : {VERSION}"
    )

    safe_print(
        f"Base directory   : {BASE_DIR}"
    )

    safe_print(
        f"OHLCV directory  : {OHLCV_DIR}"
    )

    safe_print("")

    safe_print(
        "Mode:"
    )

    safe_print(
        "  Full KRW       : ENABLED"
    )

    safe_print(
        "  Read only      : ENABLED"
    )

    safe_print(
        "  Resume         : ENABLED"
    )

    safe_print(
        "  Gap detection  : ENABLED"
    )

    safe_print(
        "  Gap repair     : DISABLED"
    )

    safe_print(
        "  Feature build  : DISABLED"
    )

    safe_print(
        "  Detector       : DISABLED"
    )

    safe_print(
        "  Prediction     : DISABLED"
    )

    safe_print(
        "  Trading        : DISABLED"
    )

    safe_print("")

    safe_print(
        f"Audit UTC time   : {utc_now_iso()}"
    )

    safe_print(
        f"Checkpoint       : {CHECKPOINT_FILE}"
    )

    safe_print(
        f"Status CSV       : {STATUS_FILE}"
    )

    safe_print("")

    checkpoint = load_checkpoint()

    completed_entries = checkpoint.get(
        "completed",
        {}
    )

    safe_print(
        "Checkpoint entries : "
        f"{len(completed_entries):,}"
    )

    if completed_entries:

        safe_print(
            "[RESUME] Existing completed checkpoint entries found."
        )

    else:

        safe_print(
            "[RESUME] No completed checkpoint entries found."
        )

    safe_print("")

    files_by_timeframe: Dict[
        str,
        List[Path],
    ] = {}

    for timeframe, directory in (
        TIMEFRAME_DIRS.items()
    ):

        files_by_timeframe[
            timeframe
        ] = get_csv_files(
            directory
        )

    markets = verify_market_sets(
        files_by_timeframe
    )

    total_jobs = sum(
        len(files)
        for files
        in files_by_timeframe.values()
    )

    safe_print(
        f"Markets          : {len(markets):,}"
    )

    safe_print(
        f"Timeframes       : {len(TIMEFRAME_DIRS):,}"
    )

    safe_print(
        f"Total jobs       : {total_jobs:,}"
    )

    safe_print("")

    results: List[
        AuditResult
    ] = []

    job_number = 0

    for timeframe in (
        "h1",
        "h4",
        "d1",
    ):

        directory = TIMEFRAME_DIRS[
            timeframe
        ]

        files = files_by_timeframe[
            timeframe
        ]

        separator(
            "-",
            76,
        )

        safe_print(
            f"AUDITING TIMEFRAME: {timeframe.upper()}"
        )

        separator(
            "-",
            76,
        )

        safe_print(
            f"Directory : {directory}"
        )

        safe_print(
            f"CSV files : {len(files):,}"
        )

        safe_print("")

        safe_print(
            "Reading market history + gaps..."
        )

        safe_print("")

        for file_path in files:

            job_number += 1

            market = normalize_market_name(
                file_path
            )

            key = checkpoint_key(
                timeframe,
                market,
            )

            try:

                source_sha256 = (
                    calculate_sha256(
                        file_path
                    )
                )

            except Exception as exc:

                result = AuditResult(
                    timeframe=timeframe,
                    market=market,
                    file=str(file_path),
                    readable=False,
                    rows=0,
                    first_timestamp="",
                    last_timestamp="",
                    duplicate_timestamps=0,
                    gap_count=0,
                    estimated_missing_candles=0,
                    max_gap_seconds=0,
                    source_sha256="",
                    resumed=False,
                    errors=(
                        f"SHA256 failure: "
                        f"{type(exc).__name__}: {exc}"
                    ),
                )

                results.append(
                    result
                )

                print_job_result(
                    job_number,
                    total_jobs,
                    result,
                )

                write_status_csv(
                    results
                )

                continue

            checkpoint_entry = (
                checkpoint[
                    "completed"
                ].get(
                    key
                )
            )

            if checkpoint_result_is_reusable(
                checkpoint_entry,
                source_sha256,
            ):

                result = (
                    result_from_checkpoint(
                        checkpoint_entry
                    )
                )

                # Ensure current location metadata is correct.
                result.timeframe = timeframe
                result.market = market
                result.file = str(
                    file_path
                )
                result.source_sha256 = (
                    source_sha256
                )
                result.resumed = True

            else:

                result = audit_file(
                    timeframe=timeframe,
                    file_path=file_path,
                    source_sha256=source_sha256,
                )

                if result.readable:

                    stored_result = asdict(
                        result
                    )

                    stored_result[
                        "resumed"
                    ] = False

                    checkpoint[
                        "completed"
                    ][
                        key
                    ] = {
                        "source_sha256": source_sha256,
                        "completed_at_utc": utc_now_iso(),
                        "result": stored_result,
                    }

                    # Save after every successful job.
                    #
                    # This is intentionally frequent.
                    # If the PC, runner or workflow stops,
                    # already completed jobs remain reusable.
                    save_checkpoint(
                        checkpoint
                    )

            results.append(
                result
            )

            print_job_result(
                job_number,
                total_jobs,
                result,
            )

            # Keep a progressively updated status CSV.
            #
            # This file is runtime output only and should be
            # ignored by Git.
            write_status_csv(
                results
            )

    # ========================================================
    # FINAL STATUS WRITE
    # ========================================================

    write_status_csv(
        results
    )

    save_checkpoint(
        checkpoint
    )

    elapsed_seconds = (
        time.perf_counter()
        - started_at
    )

    print_summary(
        results=results,
        markets=markets,
        elapsed_seconds=elapsed_seconds,
    )

    failed_jobs = [
        result
        for result
        in results
        if not result.readable
    ]

    if failed_jobs:

        safe_print("")

        separator(
            "-",
            76,
        )

        safe_print(
            "FAILED AUDIT JOBS"
        )

        separator(
            "-",
            76,
        )

        max_display = 50

        for result in (
            failed_jobs[
                :max_display
            ]
        ):

            safe_print(
                f"[FAILED] "
                f"{result.timeframe.upper()} "
                f"{result.market}"
            )

            safe_print(
                f"         {result.errors}"
            )

        remaining = (
            len(failed_jobs)
            - max_display
        )

        if remaining > 0:

            safe_print(
                f"... additional failed jobs omitted: "
                f"{remaining:,}"
            )

        return 1

    return 0


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    try:

        exit_code = main()

        raise SystemExit(
            exit_code
        )

    except KeyboardInterrupt:

        safe_print("")

        separator()

        safe_print(
            "DATA HISTORY AUDIT INTERRUPTED"
        )

        separator()

        safe_print(
            "[STOP] User or runner interrupted the audit."
        )

        safe_print(
            "[RESUME] Successfully completed checkpoint entries remain available."
        )

        safe_print(
            "[SAFETY] No automatic cleanup or source deletion was executed."
        )

        separator()

        raise SystemExit(
            130
        )

    except SystemExit:

        raise

    except Exception as exc:

        safe_print("")

        separator()

        safe_print(
            "DATA HISTORY + GAP AUDIT FATAL ERROR"
        )

        separator()

        safe_print(
            f"{type(exc).__name__}: {exc}"
        )

        safe_print("")

        safe_print(
            "[TRACEBACK]"
        )

        try:

            traceback.print_exc()

        except UnicodeEncodeError:

            safe_print(
                "Traceback could not be printed because of console encoding."
            )

        safe_print("")

        safe_print(
            "[SAFETY] No automatic cleanup or source deletion was executed."
        )

        safe_print(
            "[RESUME] Successfully completed checkpoint entries remain available."
        )

        separator()

        raise SystemExit(
            1
        )
