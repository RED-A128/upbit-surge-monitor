"""
Upbit Surge Monitor - Data History Gap Report Clean V001
=========================================================

File:
    data_history_gap_report.py

Purpose:
    Scan existing OHLCV history in READ ONLY mode and create a detailed
    timestamp-gap report before any historical recovery is attempted.

Scope:
    - Full KRW market
    - h1 / h4 / d1
    - READ ONLY source inspection
    - Persistent checkpoint / Resume
    - Detailed gap CSV
    - Per-market summary CSV
    - Source SHA256 protection

Important:
    This script DOES NOT repair gaps.

    It DOES NOT:
        - download candles
        - modify OHLCV
        - delete OHLCV
        - rebuild features
        - run detector
        - create future labels
        - predict
        - trade
        - git reset
        - git clean
        - git commit
        - git push
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


# ============================================================
# CONSOLE UTF-8 SAFETY
# ============================================================

def configure_console_encoding() -> None:
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


def safe_print(*args: Any, **kwargs: Any) -> None:
    try:
        print(*args, **kwargs)
    except UnicodeEncodeError:
        text = " ".join(str(value) for value in args)
        try:
            sys.stdout.write(
                text.encode(
                    "ascii",
                    errors="replace",
                ).decode("ascii")
                + "\n"
            )
            sys.stdout.flush()
        except Exception:
            pass


# ============================================================
# PROJECT
# ============================================================

PROJECT_NAME = "Upbit Surge Monitor"
VERSION = "Data History Gap Report Clean V001"

BASE_DIR = Path(__file__).resolve().parent

DATA_DIR = BASE_DIR / "data"
OHLCV_DIR = DATA_DIR / "ohlcv"

VALIDATION_DIR = DATA_DIR / "validation"

REPORT_DIR = DATA_DIR / "reports" / "history_gap"

CHECKPOINT_FILE = (
    VALIDATION_DIR /
    "data_history_gap_report_checkpoint.json"
)

GAP_REPORT_FILE = (
    REPORT_DIR /
    "data_history_gap_report.csv"
)

SUMMARY_FILE = (
    REPORT_DIR /
    "data_history_gap_summary.csv"
)

TIMEFRAME_DIRS: Dict[str, Path] = {
    "h1": OHLCV_DIR / "h1",
    "h4": OHLCV_DIR / "h4",
    "d1": OHLCV_DIR / "d1",
}

TIMEFRAME_SECONDS: Dict[str, int] = {
    "h1": 3600,
    "h4": 14400,
    "d1": 86400,
}

TIME_COLUMN_CANDIDATES = (
    "timestamp",
    "datetime",
    "date",
    "time",
    "candle_date_time_utc",
    "candle_date_time_kst",
)

CHECKPOINT_VERSION = 1


# ============================================================
# DIRECTORIES
# ============================================================

def ensure_runtime_directories() -> None:
    VALIDATION_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    REPORT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )


# ============================================================
# TIME
# ============================================================

def utc_now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
    )


# ============================================================
# SHA256
# ============================================================

def calculate_sha256(
    file_path: Path,
    chunk_size: int = 1024 * 1024,
) -> str:

    digest = hashlib.sha256()

    with file_path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)

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
        "report_version": VERSION,
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
        ) as handle:
            data = json.load(handle)

        if not isinstance(data, dict):
            return empty_checkpoint()

        if not isinstance(
            data.get("completed"),
            dict,
        ):
            data["completed"] = {}

        return data

    except Exception as exc:
        safe_print(
            "[WARNING] Checkpoint could not be loaded:"
        )
        safe_print(
            f"{type(exc).__name__}: {exc}"
        )
        safe_print(
            "[SAFETY] Existing checkpoint was not deleted."
        )

        return empty_checkpoint()


def save_checkpoint(
    checkpoint: Dict[str, Any],
) -> None:

    checkpoint["updated_at_utc"] = utc_now_iso()

    temp_path = CHECKPOINT_FILE.with_suffix(
        CHECKPOINT_FILE.suffix + ".tmp"
    )

    with temp_path.open(
        "w",
        encoding="utf-8",
        newline="\n",
    ) as handle:

        json.dump(
            checkpoint,
            handle,
            ensure_ascii=False,
            indent=2,
        )

        handle.flush()

        try:
            os.fsync(handle.fileno())
        except OSError:
            pass

    os.replace(
        temp_path,
        CHECKPOINT_FILE,
    )


def checkpoint_key(
    timeframe: str,
    market: str,
) -> str:
    return f"{timeframe}:{market}"


# ============================================================
# CSV READER
# ============================================================

def read_csv_safely(
    file_path: Path,
) -> pd.DataFrame:

    encodings = (
        "utf-8-sig",
        "utf-8",
        "cp949",
    )

    last_error: Optional[Exception] = None

    for encoding in encodings:
        try:
            return pd.read_csv(
                file_path,
                encoding=encoding,
                low_memory=False,
            )
        except UnicodeDecodeError as exc:
            last_error = exc

    if last_error:
        raise last_error

    return pd.read_csv(
        file_path,
        low_memory=False,
    )


# ============================================================
# TIMESTAMP
# ============================================================

def detect_timestamp_column(
    dataframe: pd.DataFrame,
) -> Optional[str]:

    columns = {
        str(column).strip().lower(): column
        for column in dataframe.columns
    }

    for candidate in TIME_COLUMN_CANDIDATES:
        if candidate in columns:
            return columns[candidate]

    return None


def parse_timestamp_series(
    series: pd.Series,
) -> pd.Series:

    if pd.api.types.is_numeric_dtype(series):
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

        median = float(
            valid.abs().median()
        )

        if median >= 1e17:
            unit = "ns"
        elif median >= 1e14:
            unit = "us"
        elif median >= 1e11:
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
# GAP DETECTION
# ============================================================

def detect_gaps(
    timestamps: pd.Series,
    timeframe: str,
    market: str,
    source_file: Path,
) -> List[Dict[str, Any]]:

    expected_seconds = TIMEFRAME_SECONDS[
        timeframe
    ]

    timestamps = (
        timestamps
        .dropna()
        .drop_duplicates()
        .sort_values()
        .reset_index(drop=True)
    )

    if len(timestamps) < 2:
        return []

    gaps: List[Dict[str, Any]] = []

    for index in range(
        1,
        len(timestamps),
    ):

        previous_timestamp = timestamps.iloc[
            index - 1
        ]

        current_timestamp = timestamps.iloc[
            index
        ]

        difference_seconds = int(
            (
                current_timestamp
                - previous_timestamp
            ).total_seconds()
        )

        if difference_seconds <= expected_seconds:
            continue

        interval_count = (
            difference_seconds
            // expected_seconds
        )

        estimated_missing = max(
            int(interval_count - 1),
            0,
        )

        first_missing = (
            previous_timestamp
            + pd.Timedelta(
                seconds=expected_seconds
            )
        )

        last_missing = (
            current_timestamp
            - pd.Timedelta(
                seconds=expected_seconds
            )
        )

        gaps.append(
            {
                "timeframe": timeframe,
                "market": market,
                "source_file": str(source_file),
                "previous_timestamp": (
                    previous_timestamp.isoformat()
                ),
                "next_timestamp": (
                    current_timestamp.isoformat()
                ),
                "first_missing_timestamp": (
                    first_missing.isoformat()
                ),
                "last_missing_timestamp": (
                    last_missing.isoformat()
                ),
                "gap_seconds": (
                    difference_seconds
                ),
                "expected_seconds": (
                    expected_seconds
                ),
                "estimated_missing_candles": (
                    estimated_missing
                ),
            }
        )

    return gaps


# ============================================================
# SINGLE JOB
# ============================================================

def inspect_market_file(
    timeframe: str,
    file_path: Path,
) -> Tuple[
    str,
    int,
    List[Dict[str, Any]],
]:

    market = file_path.stem.upper()

    dataframe = read_csv_safely(
        file_path
    )

    timestamp_column = (
        detect_timestamp_column(
            dataframe
        )
    )

    if timestamp_column is None:
        raise RuntimeError(
            "No supported timestamp column: "
            f"{file_path}"
        )

    timestamps = parse_timestamp_series(
        dataframe[timestamp_column]
    )

    invalid_count = int(
        timestamps.isna().sum()
    )

    if invalid_count > 0:
        raise RuntimeError(
            f"{market} {timeframe}: "
            f"{invalid_count:,} invalid timestamps."
        )

    gaps = detect_gaps(
        timestamps=timestamps,
        timeframe=timeframe,
        market=market,
        source_file=file_path,
    )

    return (
        market,
        len(dataframe),
        gaps,
    )


# ============================================================
# DISCOVERY
# ============================================================

def get_csv_files(
    directory: Path,
) -> List[Path]:

    if not directory.exists():
        raise FileNotFoundError(
            f"Directory not found: {directory}"
        )

    files = sorted(
        [
            path
            for path
            in directory.glob("*.csv")
            if path.is_file()
        ],
        key=lambda path: path.name.upper(),
    )

    if not files:
        raise RuntimeError(
            f"No CSV files: {directory}"
        )

    return files


def verify_market_sets(
    files_by_timeframe: Dict[
        str,
        List[Path],
    ],
) -> List[str]:

    sets: Dict[str, set[str]] = {}

    for timeframe, files in (
        files_by_timeframe.items()
    ):
        sets[timeframe] = {
            path.stem.upper()
            for path in files
        }

    if sets["h1"] != sets["h4"]:
        raise RuntimeError(
            "H1/H4 market set mismatch."
        )

    if sets["h1"] != sets["d1"]:
        raise RuntimeError(
            "H1/D1 market set mismatch."
        )

    return sorted(
        sets["h1"]
    )


# ============================================================
# OUTPUT
# ============================================================

GAP_COLUMNS = [
    "timeframe",
    "market",
    "source_file",
    "previous_timestamp",
    "next_timestamp",
    "first_missing_timestamp",
    "last_missing_timestamp",
    "gap_seconds",
    "expected_seconds",
    "estimated_missing_candles",
]


SUMMARY_COLUMNS = [
    "timeframe",
    "market",
    "rows",
    "gap_events",
    "estimated_missing_candles",
    "largest_gap_seconds",
    "source_sha256",
    "resumed",
]


def write_gap_report(
    gaps: List[Dict[str, Any]],
) -> None:

    temp_path = GAP_REPORT_FILE.with_suffix(
        ".csv.tmp"
    )

    with temp_path.open(
        "w",
        encoding="utf-8-sig",
        newline="",
    ) as handle:

        writer = csv.DictWriter(
            handle,
            fieldnames=GAP_COLUMNS,
        )

        writer.writeheader()

        for gap in gaps:
            writer.writerow(
                {
                    column: gap.get(
                        column,
                        "",
                    )
                    for column in GAP_COLUMNS
                }
            )

        handle.flush()

        try:
            os.fsync(handle.fileno())
        except OSError:
            pass

    os.replace(
        temp_path,
        GAP_REPORT_FILE,
    )


def write_summary(
    summaries: List[Dict[str, Any]],
) -> None:

    temp_path = SUMMARY_FILE.with_suffix(
        ".csv.tmp"
    )

    with temp_path.open(
        "w",
        encoding="utf-8-sig",
        newline="",
    ) as handle:

        writer = csv.DictWriter(
            handle,
            fieldnames=SUMMARY_COLUMNS,
        )

        writer.writeheader()

        for summary in summaries:
            writer.writerow(
                {
                    column: summary.get(
                        column,
                        "",
                    )
                    for column in SUMMARY_COLUMNS
                }
            )

        handle.flush()

        try:
            os.fsync(handle.fileno())
        except OSError:
            pass

    os.replace(
        temp_path,
        SUMMARY_FILE,
    )


# ============================================================
# MAIN
# ============================================================

def main() -> int:

    started = time.perf_counter()

    ensure_runtime_directories()

    safe_print(
        "=" * 72
    )
    safe_print(
        "FULL KRW DATA HISTORY GAP REPORT"
    )
    safe_print(
        "=" * 72
    )

    safe_print(
        f"Project       : {PROJECT_NAME}"
    )
    safe_print(
        f"Version       : {VERSION}"
    )
    safe_print(
        "Source mode   : READ ONLY"
    )
    safe_print(
        "Resume        : ENABLED"
    )
    safe_print(
        "Gap repair    : DISABLED"
    )
    safe_print(
        "Trading       : DISABLED"
    )
    safe_print()

    checkpoint = load_checkpoint()

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
        f"Markets       : {len(markets):,}"
    )
    safe_print(
        f"Timeframes    : {len(TIMEFRAME_DIRS):,}"
    )
    safe_print(
        f"Total jobs    : {total_jobs:,}"
    )
    safe_print()

    all_gaps: List[
        Dict[str, Any]
    ] = []

    summaries: List[
        Dict[str, Any]
    ] = []

    job_number = 0
    resumed_jobs = 0
    fresh_jobs = 0

    for timeframe in (
        "h1",
        "h4",
        "d1",
    ):

        safe_print(
            "-" * 72
        )
        safe_print(
            f"TIMEFRAME: {timeframe.upper()}"
        )
        safe_print(
            "-" * 72
        )

        for file_path in (
            files_by_timeframe[
                timeframe
            ]
        ):

            job_number += 1

            market = (
                file_path.stem.upper()
            )

            key = checkpoint_key(
                timeframe,
                market,
            )

            source_sha256 = (
                calculate_sha256(
                    file_path
                )
            )

            entry = (
                checkpoint[
                    "completed"
                ].get(key)
            )

            if (
                isinstance(entry, dict)
                and entry.get(
                    "source_sha256"
                ) == source_sha256
                and isinstance(
                    entry.get("gaps"),
                    list,
                )
                and isinstance(
                    entry.get("summary"),
                    dict,
                )
            ):
                gaps = entry["gaps"]

                summary = dict(
                    entry["summary"]
                )

                summary["resumed"] = True

                resumed_jobs += 1

                mode = "RESUME"

            else:
                (
                    detected_market,
                    rows,
                    gaps,
                ) = inspect_market_file(
                    timeframe,
                    file_path,
                )

                largest_gap = max(
                    (
                        int(
                            gap[
                                "gap_seconds"
                            ]
                        )
                        for gap in gaps
                    ),
                    default=0,
                )

                summary = {
                    "timeframe": timeframe,
                    "market": detected_market,
                    "rows": rows,
                    "gap_events": len(gaps),
                    "estimated_missing_candles": sum(
                        int(
                            gap[
                                "estimated_missing_candles"
                            ]
                        )
                        for gap in gaps
                    ),
                    "largest_gap_seconds": (
                        largest_gap
                    ),
                    "source_sha256": (
                        source_sha256
                    ),
                    "resumed": False,
                }

                checkpoint[
                    "completed"
                ][key] = {
                    "source_sha256": (
                        source_sha256
                    ),
                    "completed_at_utc": (
                        utc_now_iso()
                    ),
                    "summary": summary,
                    "gaps": gaps,
                }

                save_checkpoint(
                    checkpoint
                )

                fresh_jobs += 1

                mode = "SCAN"

            all_gaps.extend(
                gaps
            )

            summaries.append(
                summary
            )

            safe_print(
                f"[{mode}] "
                f"[{job_number}/{total_jobs}] "
                f"{timeframe.upper()} "
                f"{market} "
                f"gaps={len(gaps):,} "
                f"missing={sum(int(g['estimated_missing_candles']) for g in gaps):,}"
            )

            # Progressive runtime output.
            # If execution stops, completed work remains visible.
            write_gap_report(
                all_gaps
            )

            write_summary(
                summaries
            )

    write_gap_report(
        all_gaps
    )

    write_summary(
        summaries
    )

    save_checkpoint(
        checkpoint
    )

    elapsed = (
        time.perf_counter()
        - started
    )

    files_with_gaps = sum(
        1
        for summary in summaries
        if int(
            summary[
                "gap_events"
            ]
        ) > 0
    )

    total_missing = sum(
        int(
            summary[
                "estimated_missing_candles"
            ]
        )
        for summary in summaries
    )

    largest_gap = max(
        (
            int(
                summary[
                    "largest_gap_seconds"
                ]
            )
            for summary in summaries
        ),
        default=0,
    )

    safe_print()
    safe_print(
        "=" * 72
    )
    safe_print(
        "DATA HISTORY GAP REPORT SUMMARY"
    )
    safe_print(
        "=" * 72
    )

    safe_print(
        f"Project        : {PROJECT_NAME}"
    )
    safe_print(
        f"Version        : {VERSION}"
    )
    safe_print(
        f"Markets        : {len(markets):,}"
    )
    safe_print(
        f"Timeframes     : {len(TIMEFRAME_DIRS):,}"
    )
    safe_print(
        f"Total jobs     : {total_jobs:,}"
    )
    safe_print(
        f"Fresh jobs     : {fresh_jobs:,}"
    )
    safe_print(
        f"Resumed jobs   : {resumed_jobs:,}"
    )

    safe_print()
    safe_print(
        "Gap findings:"
    )
    safe_print(
        f"  Files w/gaps : {files_with_gaps:,}"
    )
    safe_print(
        f"  Gap events   : {len(all_gaps):,}"
    )
    safe_print(
        f"  Missing est. : {total_missing:,}"
    )
    safe_print(
        f"  Largest gap  : {largest_gap:,} seconds"
    )

    safe_print()
    safe_print(
        "Output:"
    )
    safe_print(
        f"  Gap report   : {GAP_REPORT_FILE}"
    )
    safe_print(
        f"  Summary      : {SUMMARY_FILE}"
    )
    safe_print(
        f"  Checkpoint   : {CHECKPOINT_FILE}"
    )

    safe_print()
    safe_print(
        "Safety:"
    )
    safe_print(
        "  OHLCV write  : DISABLED"
    )
    safe_print(
        "  OHLCV delete : DISABLED"
    )
    safe_print(
        "  Gap repair   : DISABLED"
    )
    safe_print(
        "  Trading      : DISABLED"
    )

    safe_print()
    safe_print(
        f"Elapsed total  : {elapsed:.2f}s"
    )

    safe_print(
        "=" * 72
    )

    safe_print(
        "[RESULT] FULL KRW DATA HISTORY GAP REPORT PASSED"
    )

    if all_gaps:
        safe_print(
            "[INFO] Timestamp gaps require classification before recovery."
        )
        safe_print(
            "[NEXT] Validate gap report and classify recoverable history gaps."
        )
    else:
        safe_print(
            "[PASS] No timestamp gaps detected."
        )
        safe_print(
            "[NEXT] Historical recovery is not required."
        )

    safe_print(
        "=" * 72
    )

    return 0


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    try:
        raise SystemExit(
            main()
        )

    except KeyboardInterrupt:

        safe_print()
        safe_print(
            "=" * 72
        )
        safe_print(
            "DATA HISTORY GAP REPORT INTERRUPTED"
        )
        safe_print(
            "=" * 72
        )

        safe_print(
            "[STOP] Execution was interrupted."
        )
        safe_print(
            "[RESUME] Completed jobs remain in the persistent checkpoint."
        )
        safe_print(
            "[SAFETY] OHLCV source files were not modified."
        )

        raise SystemExit(130)

    except SystemExit:
        raise

    except Exception as exc:

        safe_print()
        safe_print(
            "=" * 72
        )
        safe_print(
            "DATA HISTORY GAP REPORT FATAL ERROR"
        )
        safe_print(
            "=" * 72
        )

        safe_print(
            f"{type(exc).__name__}: {exc}"
        )

        try:
            traceback.print_exc()
        except Exception:
            pass

        safe_print()
        safe_print(
            "[SAFETY] No OHLCV repair or deletion was executed."
        )
        safe_print(
            "[RESUME] Successfully completed checkpoint jobs remain available."
        )

        raise SystemExit(1)
