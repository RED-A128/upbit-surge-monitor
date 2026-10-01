"""
Upbit Surge Monitor - History Gap Classifier Clean V001
=======================================================

File:
    classify_history_gaps.py

Purpose:
    data_history_gap_report.py Clean V001이 생성한
    timestamp-gap report를 READ ONLY로 읽고,
    실제 historical recovery 전에 각 gap을 분류한다.

Input:
    data/reports/history_gap/data_history_gap_report.csv

Reference:
    data/reports/history_gap/data_history_gap_summary.csv

Output:
    data/reports/history_gap_classification/
        history_gap_classification.csv
        history_gap_recovery_targets.csv
        history_gap_classification_summary.csv

Checkpoint:
    data/validation/
        classify_history_gaps_checkpoint.json

Scope:
    - Full KRW market
    - h1 / h4 / d1
    - Gap report READ ONLY
    - OHLCV READ ONLY
    - Persistent checkpoint / Resume
    - Source SHA256 protection
    - 290 markets x 3 timeframes = 870 jobs

Classification:
    RECOVERABLE
        Gap report의 구조가 정상이며,
        missing candle 범위가 명확하고,
        실제 복구 대상으로 넘길 수 있는 gap.

    STRUCTURAL
        candle 간격이 timeframe grid와 맞지 않거나
        정상적인 historical recovery 대상으로
        자동 처리하기 위험한 gap.

    REVIEW
        입력값 오류, timestamp 파싱 오류,
        비정상적인 범위, 과도하게 큰 gap 등
        자동 복구 전에 사람이 확인해야 하는 gap.

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
VERSION = "History Gap Classifier Clean V001"

BASE_DIR = Path(__file__).resolve().parent

DATA_DIR = BASE_DIR / "data"

OHLCV_DIR = DATA_DIR / "ohlcv"

VALIDATION_DIR = DATA_DIR / "validation"

SOURCE_REPORT_DIR = (
    DATA_DIR
    / "reports"
    / "history_gap"
)

SOURCE_GAP_REPORT_FILE = (
    SOURCE_REPORT_DIR
    / "data_history_gap_report.csv"
)

SOURCE_SUMMARY_FILE = (
    SOURCE_REPORT_DIR
    / "data_history_gap_summary.csv"
)

OUTPUT_DIR = (
    DATA_DIR
    / "reports"
    / "history_gap_classification"
)

CLASSIFICATION_FILE = (
    OUTPUT_DIR
    / "history_gap_classification.csv"
)

RECOVERY_TARGET_FILE = (
    OUTPUT_DIR
    / "history_gap_recovery_targets.csv"
)

CLASSIFICATION_SUMMARY_FILE = (
    OUTPUT_DIR
    / "history_gap_classification_summary.csv"
)

CHECKPOINT_FILE = (
    VALIDATION_DIR
    / "classify_history_gaps_checkpoint.json"
)

TIMEFRAMES = (
    "h1",
    "h4",
    "d1",
)

TIMEFRAME_SECONDS: Dict[str, int] = {
    "h1": 3600,
    "h4": 14400,
    "d1": 86400,
}

TIMEFRAME_DIRS: Dict[str, Path] = {
    "h1": OHLCV_DIR / "h1",
    "h4": OHLCV_DIR / "h4",
    "d1": OHLCV_DIR / "d1",
}

EXPECTED_MARKETS = 290
EXPECTED_JOBS = EXPECTED_MARKETS * len(TIMEFRAMES)

CHECKPOINT_VERSION = 1


# ============================================================
# CLASSIFICATION SAFETY LIMITS
# ============================================================

#
# 이 값은 "삭제" 또는 "복구 불가"를 의미하지 않는다.
#
# 자동 복구 대상으로 넘기기 전에 REVIEW로 보내는
# 보수적인 안전 한계다.
#
# 실제 recover_history_gaps.py 단계에서 API 범위,
# 상장 시점, 거래지원 상태 등을 다시 확인하게 된다.
#

MAX_AUTO_RECOVERY_MISSING: Dict[str, int] = {
    "h1": 24 * 31,
    "h4": 6 * 31,
    "d1": 366,
}


# ============================================================
# OUTPUT COLUMNS
# ============================================================

CLASSIFICATION_COLUMNS = [
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
    "classification",
    "classification_reason",
    "recoverable",
    "recovery_start_timestamp",
    "recovery_end_timestamp",
    "source_report_sha256",
    "classified_at_utc",
]


RECOVERY_TARGET_COLUMNS = [
    "timeframe",
    "market",
    "first_missing_timestamp",
    "last_missing_timestamp",
    "estimated_missing_candles",
    "previous_timestamp",
    "next_timestamp",
    "source_file",
    "classification",
    "classification_reason",
    "source_report_sha256",
]


SUMMARY_COLUMNS = [
    "timeframe",
    "market",
    "gap_events",
    "recoverable_gaps",
    "structural_gaps",
    "review_gaps",
    "recoverable_missing_candles",
    "total_missing_candles",
    "source_report_sha256",
    "resumed",
]


# ============================================================
# DIRECTORIES
# ============================================================

def ensure_runtime_directories() -> None:
    VALIDATION_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    OUTPUT_DIR.mkdir(
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
# CHECKPOINT
# ============================================================

def empty_checkpoint(
    source_report_sha256: str,
) -> Dict[str, Any]:

    return {
        "version": CHECKPOINT_VERSION,
        "project": PROJECT_NAME,
        "classifier_version": VERSION,
        "source_report_sha256": source_report_sha256,
        "updated_at_utc": utc_now_iso(),
        "completed": {},
    }


def load_checkpoint(
    source_report_sha256: str,
) -> Dict[str, Any]:

    if not CHECKPOINT_FILE.exists():
        return empty_checkpoint(
            source_report_sha256
        )

    try:
        with CHECKPOINT_FILE.open(
            "r",
            encoding="utf-8",
        ) as handle:

            data = json.load(handle)

        if not isinstance(data, dict):
            return empty_checkpoint(
                source_report_sha256
            )

        checkpoint_source_hash = str(
            data.get(
                "source_report_sha256",
                "",
            )
        )

        if (
            checkpoint_source_hash
            != source_report_sha256
        ):
            safe_print(
                "[INFO] Source gap report changed."
            )
            safe_print(
                "[INFO] Existing checkpoint will not "
                "be used for the changed report."
            )
            safe_print(
                "[SAFETY] Existing checkpoint file "
                "was not deleted."
            )

            return empty_checkpoint(
                source_report_sha256
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
            "[WARNING] Checkpoint could not be loaded:"
        )
        safe_print(
            f"{type(exc).__name__}: {exc}"
        )
        safe_print(
            "[SAFETY] Existing checkpoint was not deleted."
        )

        return empty_checkpoint(
            source_report_sha256
        )


def save_checkpoint(
    checkpoint: Dict[str, Any],
) -> None:

    checkpoint[
        "updated_at_utc"
    ] = utc_now_iso()

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
            os.fsync(
                handle.fileno()
            )
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

    return (
        f"{timeframe}:{market}"
    )


# ============================================================
# INPUT VALIDATION
# ============================================================

def verify_required_files() -> None:
    if not SOURCE_GAP_REPORT_FILE.exists():
        raise FileNotFoundError(
            "Gap report not found: "
            f"{SOURCE_GAP_REPORT_FILE}"
        )

    if not SOURCE_SUMMARY_FILE.exists():
        raise FileNotFoundError(
            "Gap summary not found: "
            f"{SOURCE_SUMMARY_FILE}"
        )

    for timeframe, directory in (
        TIMEFRAME_DIRS.items()
    ):
        if not directory.exists():
            raise FileNotFoundError(
                f"OHLCV directory not found "
                f"for {timeframe}: "
                f"{directory}"
            )


def verify_gap_report_columns(
    dataframe: pd.DataFrame,
) -> None:

    required = {
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
    }

    missing = (
        required
        - set(
            str(column)
            for column
            in dataframe.columns
        )
    )

    if missing:
        raise RuntimeError(
            "Gap report required columns missing: "
            + ", ".join(
                sorted(missing)
            )
        )


def verify_summary_columns(
    dataframe: pd.DataFrame,
) -> None:

    required = {
        "timeframe",
        "market",
    }

    missing = (
        required
        - set(
            str(column)
            for column
            in dataframe.columns
        )
    )

    if missing:
        raise RuntimeError(
            "Gap summary required columns missing: "
            + ", ".join(
                sorted(missing)
            )
        )


# ============================================================
# NORMALIZATION
# ============================================================

def normalize_timeframe(
    value: Any,
) -> str:

    return str(
        value
    ).strip().lower()


def normalize_market(
    value: Any,
) -> str:

    return str(
        value
    ).strip().upper()


def parse_int(
    value: Any,
    default: int = 0,
) -> int:

    try:
        if pd.isna(value):
            return default

        return int(
            float(value)
        )

    except Exception:
        return default


def parse_timestamp(
    value: Any,
) -> Optional[pd.Timestamp]:

    try:
        timestamp = pd.to_datetime(
            value,
            errors="coerce",
            utc=True,
        )

        if pd.isna(timestamp):
            return None

        return timestamp

    except Exception:
        return None


# ============================================================
# MARKET / JOB DISCOVERY
# ============================================================

def discover_jobs_from_summary(
    summary_df: pd.DataFrame,
) -> List[Tuple[str, str]]:

    jobs: set[Tuple[str, str]] = set()

    for _, row in summary_df.iterrows():
        timeframe = normalize_timeframe(
            row["timeframe"]
        )

        market = normalize_market(
            row["market"]
        )

        if timeframe not in TIMEFRAMES:
            raise RuntimeError(
                "Unsupported timeframe in summary: "
                f"{timeframe}"
            )

        if not market:
            raise RuntimeError(
                "Empty market in summary."
            )

        jobs.add(
            (
                timeframe,
                market,
            )
        )

    ordered: List[
        Tuple[str, str]
    ] = []

    for timeframe in TIMEFRAMES:
        markets = sorted(
            market
            for tf, market in jobs
            if tf == timeframe
        )

        for market in markets:
            ordered.append(
                (
                    timeframe,
                    market,
                )
            )

    return ordered


def verify_job_structure(
    jobs: List[Tuple[str, str]],
) -> List[str]:

    market_sets: Dict[
        str,
        set[str],
    ] = {
        timeframe: set()
        for timeframe in TIMEFRAMES
    }

    for timeframe, market in jobs:
        market_sets[
            timeframe
        ].add(
            market
        )

    if (
        market_sets["h1"]
        != market_sets["h4"]
    ):
        raise RuntimeError(
            "H1/H4 market set mismatch."
        )

    if (
        market_sets["h1"]
        != market_sets["d1"]
    ):
        raise RuntimeError(
            "H1/D1 market set mismatch."
        )

    markets = sorted(
        market_sets["h1"]
    )

    if len(markets) != EXPECTED_MARKETS:
        raise RuntimeError(
            "Expected "
            f"{EXPECTED_MARKETS} markets. "
            f"Found: {len(markets)}"
        )

    if len(jobs) != EXPECTED_JOBS:
        raise RuntimeError(
            "Expected "
            f"{EXPECTED_JOBS} jobs. "
            f"Found: {len(jobs)}"
        )

    return markets


# ============================================================
# OHLCV SOURCE FILE VERIFICATION
# ============================================================

def verify_market_source_file(
    timeframe: str,
    market: str,
) -> Path:

    file_path = (
        TIMEFRAME_DIRS[
            timeframe
        ]
        / f"{market}.csv"
    )

    if not file_path.exists():
        raise FileNotFoundError(
            "OHLCV source file not found: "
            f"{file_path}"
        )

    return file_path


# ============================================================
# GAP CLASSIFICATION
# ============================================================

def classify_gap(
    row: Dict[str, Any],
    source_report_sha256: str,
) -> Dict[str, Any]:

    timeframe = normalize_timeframe(
        row.get(
            "timeframe",
            "",
        )
    )

    market = normalize_market(
        row.get(
            "market",
            "",
        )
    )

    source_file = str(
        row.get(
            "source_file",
            "",
        )
    )

    previous_raw = row.get(
        "previous_timestamp"
    )

    next_raw = row.get(
        "next_timestamp"
    )

    first_missing_raw = row.get(
        "first_missing_timestamp"
    )

    last_missing_raw = row.get(
        "last_missing_timestamp"
    )

    gap_seconds = parse_int(
        row.get(
            "gap_seconds"
        ),
        default=-1,
    )

    expected_seconds = parse_int(
        row.get(
            "expected_seconds"
        ),
        default=-1,
    )

    estimated_missing = parse_int(
        row.get(
            "estimated_missing_candles"
        ),
        default=-1,
    )

    classification = "REVIEW"
    reason = ""
    recoverable = False

    previous_ts = parse_timestamp(
        previous_raw
    )

    next_ts = parse_timestamp(
        next_raw
    )

    first_missing_ts = parse_timestamp(
        first_missing_raw
    )

    last_missing_ts = parse_timestamp(
        last_missing_raw
    )

    expected_for_timeframe = (
        TIMEFRAME_SECONDS.get(
            timeframe
        )
    )

    #
    # 1. Basic structure
    #

    if timeframe not in TIMEFRAMES:
        classification = "REVIEW"
        reason = "UNSUPPORTED_TIMEFRAME"

    elif not market:
        classification = "REVIEW"
        reason = "EMPTY_MARKET"

    elif expected_for_timeframe is None:
        classification = "REVIEW"
        reason = "NO_EXPECTED_INTERVAL"

    elif expected_seconds <= 0:
        classification = "REVIEW"
        reason = "INVALID_EXPECTED_SECONDS"

    elif (
        expected_seconds
        != expected_for_timeframe
    ):
        classification = "STRUCTURAL"
        reason = (
            "EXPECTED_INTERVAL_MISMATCH"
        )

    elif gap_seconds <= 0:
        classification = "REVIEW"
        reason = "INVALID_GAP_SECONDS"

    elif estimated_missing <= 0:
        classification = "REVIEW"
        reason = (
            "INVALID_ESTIMATED_MISSING"
        )

    elif (
        previous_ts is None
        or next_ts is None
        or first_missing_ts is None
        or last_missing_ts is None
    ):
        classification = "REVIEW"
        reason = "TIMESTAMP_PARSE_ERROR"

    #
    # 2. Timestamp ordering
    #

    elif not (
        previous_ts
        < first_missing_ts
        <= last_missing_ts
        < next_ts
    ):
        classification = "REVIEW"
        reason = (
            "INVALID_TIMESTAMP_ORDER"
        )

    #
    # 3. Gap interval must be aligned
    #

    elif (
        gap_seconds
        % expected_seconds
        != 0
    ):
        classification = "STRUCTURAL"
        reason = (
            "GAP_NOT_ALIGNED_TO_TIMEFRAME"
        )

    else:
        calculated_gap_seconds = int(
            (
                next_ts
                - previous_ts
            ).total_seconds()
        )

        calculated_missing = max(
            (
                calculated_gap_seconds
                // expected_seconds
            )
            - 1,
            0,
        )

        expected_first_missing = (
            previous_ts
            + pd.Timedelta(
                seconds=expected_seconds
            )
        )

        expected_last_missing = (
            next_ts
            - pd.Timedelta(
                seconds=expected_seconds
            )
        )

        if (
            calculated_gap_seconds
            != gap_seconds
        ):
            classification = "REVIEW"
            reason = (
                "GAP_SECONDS_MISMATCH"
            )

        elif (
            calculated_missing
            != estimated_missing
        ):
            classification = "REVIEW"
            reason = (
                "MISSING_COUNT_MISMATCH"
            )

        elif (
            first_missing_ts
            != expected_first_missing
        ):
            classification = "REVIEW"
            reason = (
                "FIRST_MISSING_MISMATCH"
            )

        elif (
            last_missing_ts
            != expected_last_missing
        ):
            classification = "REVIEW"
            reason = (
                "LAST_MISSING_MISMATCH"
            )

        elif (
            estimated_missing
            > MAX_AUTO_RECOVERY_MISSING[
                timeframe
            ]
        ):
            classification = "REVIEW"
            reason = (
                "LARGE_GAP_REQUIRES_REVIEW"
            )

        else:
            classification = "RECOVERABLE"
            reason = (
                "VALID_ALIGNED_TIMESTAMP_GAP"
            )
            recoverable = True

    return {
        "timeframe": timeframe,
        "market": market,
        "source_file": source_file,
        "previous_timestamp": (
            str(previous_raw)
        ),
        "next_timestamp": (
            str(next_raw)
        ),
        "first_missing_timestamp": (
            str(first_missing_raw)
        ),
        "last_missing_timestamp": (
            str(last_missing_raw)
        ),
        "gap_seconds": gap_seconds,
        "expected_seconds": expected_seconds,
        "estimated_missing_candles": (
            estimated_missing
        ),
        "classification": classification,
        "classification_reason": reason,
        "recoverable": recoverable,
        "recovery_start_timestamp": (
            str(first_missing_raw)
            if recoverable
            else ""
        ),
        "recovery_end_timestamp": (
            str(last_missing_raw)
            if recoverable
            else ""
        ),
        "source_report_sha256": (
            source_report_sha256
        ),
        "classified_at_utc": (
            utc_now_iso()
        ),
    }


# ============================================================
# SINGLE JOB
# ============================================================

def classify_job(
    timeframe: str,
    market: str,
    gap_df: pd.DataFrame,
    source_report_sha256: str,
) -> Tuple[
    List[Dict[str, Any]],
    Dict[str, Any],
]:

    #
    # OHLCV 존재 여부만 확인한다.
    # 파일 내용을 수정하거나 다시 저장하지 않는다.
    #

    verify_market_source_file(
        timeframe,
        market,
    )

    job_rows = gap_df[
        (
            gap_df[
                "_normalized_timeframe"
            ]
            == timeframe
        )
        &
        (
            gap_df[
                "_normalized_market"
            ]
            == market
        )
    ]

    classifications: List[
        Dict[str, Any]
    ] = []

    for _, row in job_rows.iterrows():
        result = classify_gap(
            row.to_dict(),
            source_report_sha256,
        )

        classifications.append(
            result
        )

    recoverable_gaps = sum(
        1
        for item in classifications
        if (
            item["classification"]
            == "RECOVERABLE"
        )
    )

    structural_gaps = sum(
        1
        for item in classifications
        if (
            item["classification"]
            == "STRUCTURAL"
        )
    )

    review_gaps = sum(
        1
        for item in classifications
        if (
            item["classification"]
            == "REVIEW"
        )
    )

    recoverable_missing = sum(
        int(
            item[
                "estimated_missing_candles"
            ]
        )
        for item in classifications
        if (
            item["classification"]
            == "RECOVERABLE"
        )
    )

    total_missing = sum(
        max(
            int(
                item[
                    "estimated_missing_candles"
                ]
            ),
            0,
        )
        for item in classifications
    )

    summary = {
        "timeframe": timeframe,
        "market": market,
        "gap_events": len(
            classifications
        ),
        "recoverable_gaps": (
            recoverable_gaps
        ),
        "structural_gaps": (
            structural_gaps
        ),
        "review_gaps": (
            review_gaps
        ),
        "recoverable_missing_candles": (
            recoverable_missing
        ),
        "total_missing_candles": (
            total_missing
        ),
        "source_report_sha256": (
            source_report_sha256
        ),
        "resumed": False,
    }

    return (
        classifications,
        summary,
    )


# ============================================================
# ATOMIC CSV WRITER
# ============================================================

def write_dict_rows(
    file_path: Path,
    columns: List[str],
    rows: List[Dict[str, Any]],
) -> None:

    temp_path = file_path.with_suffix(
        file_path.suffix + ".tmp"
    )

    with temp_path.open(
        "w",
        encoding="utf-8-sig",
        newline="",
    ) as handle:

        writer = csv.DictWriter(
            handle,
            fieldnames=columns,
        )

        writer.writeheader()

        for row in rows:
            writer.writerow(
                {
                    column: row.get(
                        column,
                        "",
                    )
                    for column
                    in columns
                }
            )

        handle.flush()

        try:
            os.fsync(
                handle.fileno()
            )
        except OSError:
            pass

    os.replace(
        temp_path,
        file_path,
    )


# ============================================================
# OUTPUT
# ============================================================

def write_outputs(
    classifications: List[
        Dict[str, Any]
    ],
    summaries: List[
        Dict[str, Any]
    ],
) -> None:

    ordered_classifications = sorted(
        classifications,
        key=lambda item: (
            TIMEFRAMES.index(
                str(
                    item.get(
                        "timeframe",
                        "",
                    )
                )
            )
            if str(
                item.get(
                    "timeframe",
                    "",
                )
            ) in TIMEFRAMES
            else 999,
            str(
                item.get(
                    "market",
                    "",
                )
            ),
            str(
                item.get(
                    "first_missing_timestamp",
                    "",
                )
            ),
        ),
    )

    recovery_targets = [
        {
            column: item.get(
                column,
                "",
            )
            for column
            in RECOVERY_TARGET_COLUMNS
        }
        for item
        in ordered_classifications
        if (
            item.get(
                "classification"
            )
            == "RECOVERABLE"
        )
    ]

    ordered_summaries = sorted(
        summaries,
        key=lambda item: (
            TIMEFRAMES.index(
                str(
                    item.get(
                        "timeframe",
                        "",
                    )
                )
            )
            if str(
                item.get(
                    "timeframe",
                    "",
                )
            ) in TIMEFRAMES
            else 999,
            str(
                item.get(
                    "market",
                    "",
                )
            ),
        ),
    )

    write_dict_rows(
        CLASSIFICATION_FILE,
        CLASSIFICATION_COLUMNS,
        ordered_classifications,
    )

    write_dict_rows(
        RECOVERY_TARGET_FILE,
        RECOVERY_TARGET_COLUMNS,
        recovery_targets,
    )

    write_dict_rows(
        CLASSIFICATION_SUMMARY_FILE,
        SUMMARY_COLUMNS,
        ordered_summaries,
    )


# ============================================================
# SOURCE HASH VERIFICATION
# ============================================================

def verify_source_report_unchanged(
    expected_sha256: str,
) -> None:

    current_sha256 = calculate_sha256(
        SOURCE_GAP_REPORT_FILE
    )

    if current_sha256 != expected_sha256:
        raise RuntimeError(
            "Source gap report changed during "
            "classification. "
            "Execution stopped for safety."
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
        "FULL KRW HISTORY GAP CLASSIFIER"
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
        "OHLCV mode    : READ ONLY"
    )
    safe_print(
        "Resume        : ENABLED"
    )
    safe_print(
        "Gap repair    : DISABLED"
    )
    safe_print(
        "Prediction    : DISABLED"
    )
    safe_print(
        "Trading       : DISABLED"
    )
    safe_print()

    verify_required_files()

    source_report_sha256 = (
        calculate_sha256(
            SOURCE_GAP_REPORT_FILE
        )
    )

    safe_print(
        "Source report : "
        f"{SOURCE_GAP_REPORT_FILE}"
    )
    safe_print(
        "Source SHA256 : "
        f"{source_report_sha256}"
    )
    safe_print()

    gap_df = read_csv_safely(
        SOURCE_GAP_REPORT_FILE
    )

    summary_df = read_csv_safely(
        SOURCE_SUMMARY_FILE
    )

    verify_gap_report_columns(
        gap_df
    )

    verify_summary_columns(
        summary_df
    )

    gap_df[
        "_normalized_timeframe"
    ] = (
        gap_df["timeframe"]
        .map(
            normalize_timeframe
        )
    )

    gap_df[
        "_normalized_market"
    ] = (
        gap_df["market"]
        .map(
            normalize_market
        )
    )

    jobs = discover_jobs_from_summary(
        summary_df
    )

    markets = verify_job_structure(
        jobs
    )

    total_jobs = len(jobs)

    safe_print(
        f"Markets       : {len(markets):,}"
    )
    safe_print(
        f"Timeframes    : {len(TIMEFRAMES):,}"
    )
    safe_print(
        f"Jobs          : {total_jobs:,}"
    )
    safe_print(
        f"Gap rows      : {len(gap_df):,}"
    )
    safe_print()

    checkpoint = load_checkpoint(
        source_report_sha256
    )

    all_classifications: List[
        Dict[str, Any]
    ] = []

    all_summaries: List[
        Dict[str, Any]
    ] = []

    fresh_jobs = 0
    resumed_jobs = 0

    for job_number, (
        timeframe,
        market,
    ) in enumerate(
        jobs,
        start=1,
    ):

        key = checkpoint_key(
            timeframe,
            market,
        )

        entry = (
            checkpoint[
                "completed"
            ].get(key)
        )

        if (
            isinstance(entry, dict)
            and entry.get(
                "source_report_sha256"
            )
            == source_report_sha256
            and isinstance(
                entry.get(
                    "classifications"
                ),
                list,
            )
            and isinstance(
                entry.get(
                    "summary"
                ),
                dict,
            )
        ):
            classifications = list(
                entry[
                    "classifications"
                ]
            )

            summary = dict(
                entry[
                    "summary"
                ]
            )

            summary[
                "resumed"
            ] = True

            resumed_jobs += 1
            mode = "RESUME"

        else:
            (
                classifications,
                summary,
            ) = classify_job(
                timeframe=timeframe,
                market=market,
                gap_df=gap_df,
                source_report_sha256=(
                    source_report_sha256
                ),
            )

            checkpoint[
                "completed"
            ][key] = {
                "source_report_sha256": (
                    source_report_sha256
                ),
                "completed_at_utc": (
                    utc_now_iso()
                ),
                "classifications": (
                    classifications
                ),
                "summary": summary,
            }

            #
            # 매 job 완료 즉시 checkpoint 저장.
            #
            # PC 종료 / Runner 중단 / Ctrl+C가 발생해도
            # 완료된 job은 다음 실행에서 다시 하지 않는다.
            #

            save_checkpoint(
                checkpoint
            )

            fresh_jobs += 1
            mode = "CLASSIFY"

        all_classifications.extend(
            classifications
        )

        all_summaries.append(
            summary
        )

        recoverable_count = sum(
            1
            for item
            in classifications
            if (
                item[
                    "classification"
                ]
                == "RECOVERABLE"
            )
        )

        structural_count = sum(
            1
            for item
            in classifications
            if (
                item[
                    "classification"
                ]
                == "STRUCTURAL"
            )
        )

        review_count = sum(
            1
            for item
            in classifications
            if (
                item[
                    "classification"
                ]
                == "REVIEW"
            )
        )

        safe_print(
            f"[{mode}] "
            f"[{job_number}/{total_jobs}] "
            f"{timeframe.upper()} "
            f"{market} "
            f"gaps={len(classifications):,} "
            f"recoverable={recoverable_count:,} "
            f"structural={structural_count:,} "
            f"review={review_count:,}"
        )

        #
        # 진행 중에도 결과 CSV를 계속 갱신한다.
        #
        # 중간에 중단되어도 지금까지 분류된 결과를
        # 확인할 수 있다.
        #

        write_outputs(
            all_classifications,
            all_summaries,
        )

    #
    # Final output
    #

    write_outputs(
        all_classifications,
        all_summaries,
    )

    save_checkpoint(
        checkpoint
    )

    verify_source_report_unchanged(
        source_report_sha256
    )

    elapsed = (
        time.perf_counter()
        - started
    )

    total_gap_events = len(
        all_classifications
    )

    recoverable_gaps = sum(
        1
        for item
        in all_classifications
        if (
            item["classification"]
            == "RECOVERABLE"
        )
    )

    structural_gaps = sum(
        1
        for item
        in all_classifications
        if (
            item["classification"]
            == "STRUCTURAL"
        )
    )

    review_gaps = sum(
        1
        for item
        in all_classifications
        if (
            item["classification"]
            == "REVIEW"
        )
    )

    recoverable_missing = sum(
        max(
            int(
                item[
                    "estimated_missing_candles"
                ]
            ),
            0,
        )
        for item
        in all_classifications
        if (
            item["classification"]
            == "RECOVERABLE"
        )
    )

    total_missing = sum(
        max(
            int(
                item[
                    "estimated_missing_candles"
                ]
            ),
            0,
        )
        for item
        in all_classifications
    )

    checkpoint_completed = len(
        checkpoint[
            "completed"
        ]
    )

    safe_print()
    safe_print(
        "=" * 72
    )
    safe_print(
        "HISTORY GAP CLASSIFICATION SUMMARY"
    )
    safe_print(
        "=" * 72
    )

    safe_print(
        f"Project         : {PROJECT_NAME}"
    )
    safe_print(
        f"Version         : {VERSION}"
    )
    safe_print(
        f"Markets         : {len(markets):,}"
    )
    safe_print(
        f"Timeframes      : {len(TIMEFRAMES):,}"
    )
    safe_print(
        f"Jobs            : {total_jobs:,}"
    )
    safe_print(
        f"Fresh jobs      : {fresh_jobs:,}"
    )
    safe_print(
        f"Resumed jobs    : {resumed_jobs:,}"
    )
    safe_print(
        f"Checkpoint jobs : {checkpoint_completed:,}"
    )

    safe_print()
    safe_print(
        "Classification:"
    )
    safe_print(
        f"  Gap events    : {total_gap_events:,}"
    )
    safe_print(
        f"  RECOVERABLE   : {recoverable_gaps:,}"
    )
    safe_print(
        f"  STRUCTURAL    : {structural_gaps:,}"
    )
    safe_print(
        f"  REVIEW        : {review_gaps:,}"
    )

    safe_print()
    safe_print(
        "Missing candles:"
    )
    safe_print(
        f"  Total est.    : {total_missing:,}"
    )
    safe_print(
        f"  Recoverable   : {recoverable_missing:,}"
    )

    safe_print()
    safe_print(
        "Output:"
    )
    safe_print(
        "  Classification : "
        f"{CLASSIFICATION_FILE}"
    )
    safe_print(
        "  Recovery target: "
        f"{RECOVERY_TARGET_FILE}"
    )
    safe_print(
        "  Summary        : "
        f"{CLASSIFICATION_SUMMARY_FILE}"
    )
    safe_print(
        "  Checkpoint     : "
        f"{CHECKPOINT_FILE}"
    )

    safe_print()
    safe_print(
        "Integrity:"
    )
    safe_print(
        "  Gap report SHA256 : VERIFIED"
    )
    safe_print(
        "  Gap report write  : DISABLED"
    )
    safe_print(
        "  OHLCV write       : DISABLED"
    )
    safe_print(
        "  OHLCV delete      : DISABLED"
    )

    safe_print()
    safe_print(
        "Safety:"
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

    safe_print()
    safe_print(
        f"Elapsed total   : {elapsed:.2f}s"
    )

    safe_print(
        "=" * 72
    )

    if (
        checkpoint_completed
        != EXPECTED_JOBS
    ):
        raise RuntimeError(
            "Checkpoint job count mismatch. "
            f"Expected {EXPECTED_JOBS}, "
            f"found {checkpoint_completed}."
        )

    safe_print(
        "[RESULT] FULL KRW HISTORY GAP CLASSIFIER PASSED"
    )

    safe_print(
        f"[PASS] {len(markets):,} markets verified."
    )

    safe_print(
        f"[PASS] {total_jobs:,} classification jobs verified."
    )

    safe_print(
        "[PASS] Persistent Resume checkpoint verified."
    )

    safe_print(
        "[PASS] Source gap report remained unchanged."
    )

    if review_gaps > 0:
        safe_print(
            f"[INFO] {review_gaps:,} gap(s) require REVIEW."
        )

    if structural_gaps > 0:
        safe_print(
            f"[INFO] {structural_gaps:,} structural gap(s) "
            "were excluded from automatic recovery."
        )

    if recoverable_gaps > 0:
        safe_print(
            f"[PASS] {recoverable_gaps:,} recoverable "
            "gap(s) prepared."
        )
        safe_print(
            f"[PASS] {recoverable_missing:,} estimated "
            "missing candle(s) prepared for recovery."
        )
        safe_print(
            "[NEXT] Validate classification output before "
            "historical recovery."
        )

    else:
        safe_print(
            "[INFO] No automatic recovery targets were produced."
        )
        safe_print(
            "[NEXT] Review STRUCTURAL / REVIEW gaps."
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
            "HISTORY GAP CLASSIFIER INTERRUPTED"
        )
        safe_print(
            "=" * 72
        )

        safe_print(
            "[STOP] Execution was interrupted."
        )

        safe_print(
            "[RESUME] Completed jobs remain in the "
            "persistent checkpoint."
        )

        safe_print(
            "[SAFETY] Gap report and OHLCV source files "
            "were not modified."
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
            "HISTORY GAP CLASSIFIER FATAL ERROR"
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
            "[SAFETY] No OHLCV repair or deletion "
            "was executed."
        )

        safe_print(
            "[RESUME] Successfully completed jobs "
            "remain in the persistent checkpoint."
        )

        raise SystemExit(1)
