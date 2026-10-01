"""
Upbit Surge Monitor - Historical Gap Recovery Clean V002
=========================================================

File:
    recover_history_gaps.py

Purpose:
    Historical OHLCV Gap Report에서 확인된 누락 구간을
    Upbit Candle API를 이용하여 실제로 복구한다.

Clean V002:
    - 실제 Upbit Candle API 복구 지원
    - --repair 명시 시에만 OHLCV 수정
    - 기존 정상 OHLCV candle 최우선 보존
    - API에 실제 존재하는 candle만 추가
    - 가짜 candle 생성 금지
    - candle_date_time_utc를 canonical candle time으로 사용
    - timestamp는 candle_date_time_utc에서 재생성
    - Gap Event 단위 Resume / Checkpoint
    - Job 단위 완료 상태 저장
    - 원자적 CSV 저장
    - 저장 전/후 무결성 검증
    - 복구 후 실제 remaining gap 재검사
    - PARTIAL / UNRESOLVED / NO_API_DATA 구분
    - Feature / Signal / Prediction / Trading 실행 금지
    - git reset / clean / commit / push 금지

IMPORTANT:
    기존 Clean V001 DRY-RUN checkpoint는 절대로 사용하거나
    삭제하지 않는다.

    Clean V002 실제 복구 checkpoint:
        data/recovery/checkpoint/
        recover_history_gaps_repair_checkpoint.json

Safety:
    py recover_history_gaps.py

        -> 실제 복구하지 않음
        -> Gap Report와 복구 대상만 확인
        -> OHLCV 수정하지 않음

    py recover_history_gaps.py --repair

        -> 실제 historical gap recovery 수행

Windows:
    py recover_history_gaps.py
    py recover_history_gaps.py --repair
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import sys
import time
import traceback

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
)

import pandas as pd
import requests


# ============================================================
# 1. PROJECT / VERSION
# ============================================================

PROJECT_NAME = "Upbit Surge Monitor"
VERSION = "Historical Gap Recovery Clean V002"

TIMEFRAMES: Tuple[str, ...] = (
    "h1",
    "h4",
    "d1",
)

EXPECTED_MARKET_COUNT = 290
EXPECTED_JOB_COUNT = (
    EXPECTED_MARKET_COUNT
    * len(TIMEFRAMES)
)

HIGH_GAP_THRESHOLD = 100
EXTREME_GAP_THRESHOLD = 1000

UPBIT_API_BASE = "https://api.upbit.com/v1"

REQUEST_TIMEOUT = 15
MAX_RETRIES = 5
API_MAX_COUNT = 200

REQUEST_SLEEP_SECONDS = 0.15
GAP_SLEEP_SECONDS = 0.05

# API에서 gap 경계 주변 candle을 조금 더 확보한다.
FETCH_MARGIN_CANDLES = 2

# 하나의 gap에 대해 비정상적인 무한 API pagination을 막는다.
MAX_REQUESTS_PER_GAP = 10000


# ============================================================
# 2. PATHS
# ============================================================

ROOT_DIR = Path(__file__).resolve().parent

DATA_DIR = ROOT_DIR / "data"
OHLCV_DIR = DATA_DIR / "ohlcv"

RUNTIME_DIR = DATA_DIR / "recovery"
CHECKPOINT_DIR = RUNTIME_DIR / "checkpoint"
REPORT_DIR = RUNTIME_DIR / "reports"

# ------------------------------------------------------------
# Clean V001 DRY-RUN checkpoint
#
# IMPORTANT:
#     V002에서는 절대 수정하거나 삭제하지 않는다.
# ------------------------------------------------------------

V001_CHECKPOINT_FILE = (
    CHECKPOINT_DIR
    / "recover_history_gaps_checkpoint.json"
)

# ------------------------------------------------------------
# Clean V002 실제 복구 checkpoint
# ------------------------------------------------------------

REPAIR_CHECKPOINT_FILE = (
    CHECKPOINT_DIR
    / "recover_history_gaps_repair_checkpoint.json"
)

STATUS_FILE = (
    DATA_DIR
    / "recover_history_gaps_repair_status.csv"
)

REPAIR_DETAIL_FILE = (
    REPORT_DIR
    / "historical_gap_repair_detail.csv"
)

REPAIR_SUMMARY_FILE = (
    REPORT_DIR
    / "historical_gap_repair_summary.csv"
)

REPAIR_META_FILE = (
    REPORT_DIR
    / "historical_gap_repair_meta.json"
)

GAP_REPORT_SEARCH_DIRS: Tuple[Path, ...] = (
    DATA_DIR / "gap_report",
    DATA_DIR / "gap_reports",
    DATA_DIR / "history_gap_report",
    DATA_DIR / "history_gap_reports",
    DATA_DIR / "audit",
    DATA_DIR / "reports",
    DATA_DIR,
)


# ============================================================
# 3. TIMEFRAME CONFIG
# ============================================================

TIMEFRAME_CONFIG: Dict[str, Dict[str, Any]] = {

    "h1": {
        "url": (
            f"{UPBIT_API_BASE}"
            f"/candles/minutes/60"
        ),
        "interval_seconds": 60 * 60,
    },

    "h4": {
        "url": (
            f"{UPBIT_API_BASE}"
            f"/candles/minutes/240"
        ),
        "interval_seconds": 4 * 60 * 60,
    },

    "d1": {
        "url": (
            f"{UPBIT_API_BASE}"
            f"/candles/days"
        ),
        "interval_seconds": 24 * 60 * 60,
    },
}


# ============================================================
# 4. OHLCV FORMAT
# ============================================================

OHLCV_COLUMNS = [
    "market",
    "candle_date_time_utc",
    "candle_date_time_kst",
    "timestamp",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "trade_value",
]

NUMERIC_COLUMNS = [
    "open",
    "high",
    "low",
    "close",
    "volume",
    "trade_value",
]


# ============================================================
# 5. POSSIBLE GAP REPORT COLUMN NAMES
# ============================================================

MARKET_COLUMN_CANDIDATES: Tuple[str, ...] = (
    "market",
    "ticker",
    "symbol",
    "code",
)

TIMEFRAME_COLUMN_CANDIDATES: Tuple[str, ...] = (
    "timeframe",
    "interval",
    "tf",
)

GAP_COUNT_COLUMN_CANDIDATES: Tuple[str, ...] = (
    "missing_count",
    "gap_count",
    "missing_est",
    "missing_estimate",
    "missing",
    "count",
)

GAP_START_COLUMN_CANDIDATES: Tuple[str, ...] = (
    "gap_start",
    "start",
    "start_time",
    "start_timestamp",
    "missing_start",
    "from_time",
)

GAP_END_COLUMN_CANDIDATES: Tuple[str, ...] = (
    "gap_end",
    "end",
    "end_time",
    "end_timestamp",
    "missing_end",
    "to_time",
)

STATUS_COLUMN_CANDIDATES: Tuple[str, ...] = (
    "status",
    "gap_status",
    "classification",
    "result",
)

FILE_COLUMN_CANDIDATES: Tuple[str, ...] = (
    "file",
    "file_path",
    "path",
    "source_file",
)


# ============================================================
# 6. DATA CLASSES
# ============================================================

@dataclass(frozen=True)
class JobKey:

    timeframe: str
    market: str

    @property
    def key(self) -> str:

        return (
            f"{self.timeframe}|"
            f"{self.market}"
        )


@dataclass
class GapRecord:

    market: str
    timeframe: str
    gap_count: int

    gap_start: str
    gap_end: str

    source_report: str
    source_row: int

    original_status: str = ""
    source_file: str = ""

    @property
    def event_key(self) -> str:

        return "|".join(
            [
                self.timeframe,
                self.market,
                self.gap_start,
                self.gap_end,
                str(self.gap_count),
            ]
        )


# ============================================================
# 7. BASIC HELPERS
# ============================================================

def utc_now_iso() -> str:

    return datetime.now(
        timezone.utc
    ).isoformat(
        timespec="seconds"
    )


def safe_print(
    text: str = "",
) -> None:

    value = str(text)

    try:
        print(
            value,
            flush=True,
        )
        return

    except UnicodeEncodeError:
        pass

    encoding = (
        getattr(
            sys.stdout,
            "encoding",
            None,
        )
        or "utf-8"
    )

    try:

        safe_value = (
            value
            .encode(
                encoding,
                errors="replace",
            )
            .decode(
                encoding,
                errors="replace",
            )
        )

        print(
            safe_value,
            flush=True,
        )

    except Exception:

        fallback = (
            value
            .encode(
                "ascii",
                errors="replace",
            )
            .decode("ascii")
        )

        print(
            fallback,
            flush=True,
        )


def separator(
    char: str = "=",
    length: int = 72,
) -> str:

    return char * length


def clean_text(
    value: Any,
) -> str:

    if value is None:
        return ""

    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass

    return str(value).strip()


def normalize_column_name(
    value: Any,
) -> str:

    text = str(
        value
    ).strip().lower()

    text = re.sub(
        r"[\s\-]+",
        "_",
        text,
    )

    text = re.sub(
        r"_+",
        "_",
        text,
    )

    return text


def normalize_market(
    value: Any,
) -> str:

    text = str(
        value
    ).strip().upper()

    if not text:
        return ""

    text = text.replace(
        "_",
        "-",
    )

    if text.startswith(
        "KRW-"
    ):
        return text

    if (
        text.startswith("KRW")
        and "-" not in text
    ):

        remainder = (
            text[3:]
            .lstrip("-")
        )

        if remainder:
            return (
                f"KRW-{remainder}"
            )

    return text


def normalize_timeframe(
    value: Any,
) -> str:

    text = str(
        value
    ).strip().lower()

    mapping = {

        "1h": "h1",
        "h1": "h1",
        "60m": "h1",
        "60min": "h1",
        "minute60": "h1",

        "4h": "h4",
        "h4": "h4",
        "240m": "h4",
        "240min": "h4",
        "minute240": "h4",

        "1d": "d1",
        "d1": "d1",
        "day": "d1",
        "daily": "d1",
    }

    return mapping.get(
        text,
        text,
    )


def parse_int(
    value: Any,
    default: int = 0,
) -> int:

    if value is None:
        return default

    if isinstance(
        value,
        bool,
    ):
        return int(value)

    if isinstance(
        value,
        int,
    ):
        return value

    if isinstance(
        value,
        float,
    ):

        if pd.isna(value):
            return default

        return int(value)

    text = str(
        value
    ).strip()

    if not text:
        return default

    text = text.replace(
        ",",
        "",
    )

    try:

        return int(
            float(text)
        )

    except Exception:

        return default


def ensure_runtime_directories() -> None:

    CHECKPOINT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    REPORT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )


# ============================================================
# 8. DATETIME HELPERS
# ============================================================

def parse_datetime_utc(
    value: Any,
) -> Optional[pd.Timestamp]:

    text = clean_text(
        value
    )

    if not text:
        return None

    try:

        parsed = pd.to_datetime(
            text,
            utc=True,
            errors="coerce",
        )

    except Exception:

        return None

    if pd.isna(parsed):
        return None

    return parsed


def datetime_to_upbit_to(
    value: pd.Timestamp,
) -> str:

    value = pd.Timestamp(
        value
    )

    if value.tzinfo is None:

        value = value.tz_localize(
            "UTC"
        )

    else:

        value = value.tz_convert(
            "UTC"
        )

    # Upbit "to" parameter에 UTC ISO time 사용
    return value.strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


def timestamp_to_text(
    value: pd.Timestamp,
) -> str:

    value = pd.Timestamp(
        value
    )

    if value.tzinfo is None:

        value = value.tz_localize(
            "UTC"
        )

    else:

        value = value.tz_convert(
            "UTC"
        )

    return value.strftime(
        "%Y-%m-%dT%H:%M:%S"
    )


def interval_timedelta(
    timeframe: str,
) -> pd.Timedelta:

    seconds = int(
        TIMEFRAME_CONFIG[
            timeframe
        ][
            "interval_seconds"
        ]
    )

    return pd.Timedelta(
        seconds=seconds
    )


# ============================================================
# 9. HASH
# ============================================================

def sha256_file(
    path: Path,
    chunk_size: int = 1024 * 1024,
) -> str:

    digest = hashlib.sha256()

    with path.open(
        "rb"
    ) as handle:

        while True:

            chunk = handle.read(
                chunk_size
            )

            if not chunk:
                break

            digest.update(
                chunk
            )

    return digest.hexdigest()


# ============================================================
# 10. ROBUST CSV READER
# ============================================================

def read_csv_robust(
    path: Path,
) -> pd.DataFrame:

    attempts = (
        {
            "encoding":
            "utf-8-sig"
        },
        {
            "encoding":
            "utf-8"
        },
        {
            "encoding":
            "cp949"
        },
    )

    last_error: Optional[
        Exception
    ] = None

    for kwargs in attempts:

        try:

            return pd.read_csv(
                path,
                low_memory=False,
                **kwargs,
            )

        except Exception as exc:

            last_error = exc

    raise RuntimeError(
        f"Unable to read CSV: {path}\n"
        f"Last error: {last_error}"
    )


# ============================================================
# 11. COLUMN FINDER
# ============================================================

def find_column(
    columns: Sequence[str],
    candidates: Sequence[str],
) -> Optional[str]:

    normalized_map = {

        normalize_column_name(
            column
        ):
        column

        for column
        in columns
    }

    for candidate in candidates:

        normalized_candidate = (
            normalize_column_name(
                candidate
            )
        )

        if (
            normalized_candidate
            in normalized_map
        ):

            return normalized_map[
                normalized_candidate
            ]

    return None


# ============================================================
# 12. MARKET / JOB DISCOVERY
# ============================================================

def discover_markets() -> List[str]:

    market_sets: Dict[
        str,
        Set[str],
    ] = {}

    for timeframe in TIMEFRAMES:

        directory = (
            OHLCV_DIR
            / timeframe
        )

        if not directory.is_dir():

            raise FileNotFoundError(
                "OHLCV directory missing: "
                f"{directory}"
            )

        markets: Set[str] = set()

        for path in directory.glob(
            "KRW-*.csv"
        ):

            if path.is_file():

                markets.add(
                    path.stem.upper()
                )

        market_sets[
            timeframe
        ] = markets

    if not market_sets:

        raise RuntimeError(
            "No OHLCV markets discovered."
        )

    base = market_sets[
        TIMEFRAMES[0]
    ]

    for timeframe in TIMEFRAMES[1:]:

        current = market_sets[
            timeframe
        ]

        if current != base:

            missing = sorted(
                base - current
            )

            extra = sorted(
                current - base
            )

            message = [

                (
                    "Market file set mismatch: "
                    f"{timeframe}"
                ),

                (
                    "Missing count: "
                    f"{len(missing)}"
                ),

                (
                    "Extra count  : "
                    f"{len(extra)}"
                ),
            ]

            if missing:

                message.append(
                    "Missing sample: "
                    + ", ".join(
                        missing[:10]
                    )
                )

            if extra:

                message.append(
                    "Extra sample: "
                    + ", ".join(
                        extra[:10]
                    )
                )

            raise RuntimeError(
                "\n".join(
                    message
                )
            )

    markets = sorted(
        base
    )

    if not markets:

        raise RuntimeError(
            "No KRW market CSV files found."
        )

    return markets


def build_jobs(
    markets: Sequence[str],
) -> List[JobKey]:

    jobs: List[
        JobKey
    ] = []

    for timeframe in TIMEFRAMES:

        for market in markets:

            jobs.append(
                JobKey(
                    timeframe=timeframe,
                    market=market,
                )
            )

    return jobs


# ============================================================
# 13. GAP REPORT DISCOVERY
# ============================================================

def is_generated_recovery_file(
    path: Path,
) -> bool:

    generated_names = {

        STATUS_FILE.name.lower(),

        REPAIR_DETAIL_FILE
        .name
        .lower(),

        REPAIR_SUMMARY_FILE
        .name
        .lower(),
    }

    if (
        path.name.lower()
        in generated_names
    ):

        return True

    try:

        path.resolve().relative_to(
            RUNTIME_DIR.resolve()
        )

        return True

    except Exception:

        return False


def score_gap_report_candidate(
    path: Path,
) -> int:

    name = (
        path.name.lower()
    )

    score = 0

    if "gap" in name:
        score += 100

    if "history" in name:
        score += 40

    if "report" in name:
        score += 30

    if "audit" in name:
        score += 10

    if "summary" in name:
        score -= 30

    if "status" in name:
        score -= 40

    if "checkpoint" in name:
        score -= 100

    if "repair" in name:
        score -= 50

    return score


def discover_gap_report_files() -> List[Path]:

    candidates: Dict[
        str,
        Path,
    ] = {}

    for directory in (
        GAP_REPORT_SEARCH_DIRS
    ):

        if not directory.exists():
            continue

        if directory == DATA_DIR:

            iterator: Iterable[
                Path
            ] = directory.glob(
                "*.csv"
            )

        else:

            iterator = directory.rglob(
                "*.csv"
            )

        for path in iterator:

            if not path.is_file():
                continue

            if is_generated_recovery_file(
                path
            ):
                continue

            name = (
                path.name.lower()
            )

            if not (
                "gap" in name
                or "history" in name
                or "audit" in name
            ):
                continue

            candidates[
                str(path.resolve())
            ] = path

    return sorted(
        candidates.values(),
        key=lambda item: (
            -score_gap_report_candidate(
                item
            ),
            str(item).lower(),
        ),
    )


# ============================================================
# 14. GAP REPORT PARSER
# ============================================================

def infer_market_timeframe_from_path(
    path: Path,
) -> Tuple[str, str]:

    market = ""
    timeframe = ""

    stem_upper = (
        path.stem.upper()
    )

    match = re.search(
        r"(KRW-[A-Z0-9]+)",
        stem_upper,
    )

    if match:

        market = match.group(
            1
        )

    parts = [
        part.lower()
        for part
        in path.parts
    ]

    for part in reversed(
        parts
    ):

        normalized = (
            normalize_timeframe(
                part
            )
        )

        if normalized in TIMEFRAMES:

            timeframe = normalized
            break

    return (
        market,
        timeframe,
    )


def parse_gap_report(
    path: Path,
) -> List[GapRecord]:

    df = read_csv_robust(
        path
    )

    if df.empty:
        return []

    columns = list(
        df.columns
    )

    market_col = find_column(
        columns,
        MARKET_COLUMN_CANDIDATES,
    )

    timeframe_col = find_column(
        columns,
        TIMEFRAME_COLUMN_CANDIDATES,
    )

    gap_count_col = find_column(
        columns,
        GAP_COUNT_COLUMN_CANDIDATES,
    )

    gap_start_col = find_column(
        columns,
        GAP_START_COLUMN_CANDIDATES,
    )

    gap_end_col = find_column(
        columns,
        GAP_END_COLUMN_CANDIDATES,
    )

    status_col = find_column(
        columns,
        STATUS_COLUMN_CANDIDATES,
    )

    file_col = find_column(
        columns,
        FILE_COLUMN_CANDIDATES,
    )

    (
        inferred_market,
        inferred_timeframe,
    ) = (
        infer_market_timeframe_from_path(
            path
        )
    )

    if (
        market_col is None
        and not inferred_market
    ):
        return []

    if (
        timeframe_col is None
        and not inferred_timeframe
    ):
        return []

    if (
        gap_count_col is None
        and gap_start_col is None
        and gap_end_col is None
    ):
        return []

    records: List[
        GapRecord
    ] = []

    for row_number, (
        _,
        row,
    ) in enumerate(
        df.iterrows(),
        start=2,
    ):

        if market_col is not None:

            market = normalize_market(
                row.get(
                    market_col
                )
            )

        else:

            market = (
                inferred_market
            )

        if timeframe_col is not None:

            timeframe = (
                normalize_timeframe(
                    row.get(
                        timeframe_col
                    )
                )
            )

        else:

            timeframe = (
                inferred_timeframe
            )

        if not market.startswith(
            "KRW-"
        ):
            continue

        if timeframe not in TIMEFRAMES:
            continue

        gap_count = 0

        if gap_count_col is not None:

            gap_count = parse_int(
                row.get(
                    gap_count_col
                ),
                default=0,
            )

        gap_start = (

            clean_text(
                row.get(
                    gap_start_col
                )
            )

            if gap_start_col
            is not None

            else ""
        )

        gap_end = (

            clean_text(
                row.get(
                    gap_end_col
                )
            )

            if gap_end_col
            is not None

            else ""
        )

        original_status = (

            clean_text(
                row.get(
                    status_col
                )
            )

            if status_col
            is not None

            else ""
        )

        source_file = (

            clean_text(
                row.get(
                    file_col
                )
            )

            if file_col
            is not None

            else ""
        )

        if (
            gap_count <= 0
            and (
                gap_start
                or gap_end
            )
        ):

            gap_count = 1

        if gap_count <= 0:
            continue

        records.append(
            GapRecord(
                market=market,
                timeframe=timeframe,
                gap_count=gap_count,
                gap_start=gap_start,
                gap_end=gap_end,
                source_report=str(
                    path
                ),
                source_row=row_number,
                original_status=(
                    original_status
                ),
                source_file=(
                    source_file
                ),
            )
        )

    return records


def load_all_gap_records(
    report_files: Sequence[Path],
) -> Tuple[
    List[GapRecord],
    List[Path],
]:

    all_records: List[
        GapRecord
    ] = []

    accepted_files: List[
        Path
    ] = []

    safe_print()
    safe_print(
        separator()
    )
    safe_print(
        "DISCOVER GAP REPORT DATA"
    )
    safe_print(
        separator()
    )

    if not report_files:

        safe_print(
            "[WARN] No candidate Gap Report CSV files found."
        )

        return (
            [],
            [],
        )

    for path in report_files:

        try:

            records = (
                parse_gap_report(
                    path
                )
            )

        except Exception as exc:

            safe_print(
                f"[SKIP] {path} : {exc}"
            )

            continue

        if not records:
            continue

        accepted_files.append(
            path
        )

        all_records.extend(
            records
        )

        safe_print(
            f"[READ] {path}"
        )

        safe_print(
            "       Gap rows : "
            f"{len(records):,}"
        )

    return (
        all_records,
        accepted_files,
    )


# ============================================================
# 15. GAP DEDUPLICATION
# ============================================================

def gap_identity(
    record: GapRecord,
) -> Tuple[Any, ...]:

    return (
        record.market,
        record.timeframe,
        record.gap_start,
        record.gap_end,
        record.gap_count,
    )


def deduplicate_gap_records(
    records: Sequence[GapRecord],
) -> List[GapRecord]:

    seen: Set[
        Tuple[Any, ...]
    ] = set()

    result: List[
        GapRecord
    ] = []

    for record in records:

        identity = (
            gap_identity(
                record
            )
        )

        if identity in seen:
            continue

        seen.add(
            identity
        )

        result.append(
            record
        )

    result.sort(
        key=lambda item: (
            item.timeframe,
            item.market,
            item.gap_start,
            item.gap_end,
            item.gap_count,
        )
    )

    return result


# ============================================================
# 16. GAP INDEX
# ============================================================

def build_gap_index(
    records: Sequence[GapRecord],
) -> Dict[
    str,
    List[GapRecord],
]:

    result: Dict[
        str,
        List[GapRecord],
    ] = {}

    for record in records:

        key = (
            f"{record.timeframe}|"
            f"{record.market}"
        )

        result.setdefault(
            key,
            [],
        ).append(
            record
        )

    return result


# ============================================================
# 17. OHLCV NORMALIZATION
# ============================================================

def build_canonical_timestamp(
    candle_time: pd.Series,
) -> pd.Series:

    parsed = pd.to_datetime(
        candle_time,
        utc=True,
        errors="coerce",
    )

    result = pd.Series(
        pd.NA,
        index=parsed.index,
        dtype="Int64",
    )

    valid = (
        parsed.notna()
    )

    if valid.any():

        result.loc[
            valid
        ] = (
            parsed.loc[
                valid
            ]
            .astype("int64")
            // 1_000_000
        ).astype(
            "int64"
        )

    return result


def normalize_ohlcv_dataframe(
    df: pd.DataFrame,
    *,
    require_market: Optional[str] = None,
) -> pd.DataFrame:

    if df.empty:

        return pd.DataFrame(
            columns=OHLCV_COLUMNS
        )

    work = df.copy()

    for column in OHLCV_COLUMNS:

        if column not in work.columns:

            work[
                column
            ] = pd.NA

    work = work[
        OHLCV_COLUMNS
    ].copy()

    work[
        "candle_date_time_utc"
    ] = pd.to_datetime(
        work[
            "candle_date_time_utc"
        ],
        utc=True,
        errors="coerce",
    )

    invalid_time_count = int(
        work[
            "candle_date_time_utc"
        ]
        .isna()
        .sum()
    )

    if invalid_time_count > 0:

        raise RuntimeError(
            "Invalid candle_date_time_utc found: "
            f"{invalid_time_count:,}"
        )

    if require_market is not None:

        market_values = (
            work[
                "market"
            ]
            .astype(str)
            .str.strip()
            .str.upper()
        )

        invalid_market = (
            market_values
            != require_market
        )

        if invalid_market.any():

            raise RuntimeError(
                "Unexpected market value found "
                "in OHLCV CSV."
            )

        work[
            "market"
        ] = require_market

    work[
        "timestamp"
    ] = build_canonical_timestamp(
        work[
            "candle_date_time_utc"
        ]
    )

    if (
        work[
            "timestamp"
        ]
        .isna()
        .any()
    ):

        raise RuntimeError(
            "Canonical timestamp generation failed."
        )

    for column in NUMERIC_COLUMNS:

        work[
            column
        ] = pd.to_numeric(
            work[
                column
            ],
            errors="coerce",
        )

    work = work.drop_duplicates(
        subset=[
            "candle_date_time_utc"
        ],
        keep="first",
    )

    work = work.sort_values(
        "candle_date_time_utc"
    )

    work = work.reset_index(
        drop=True
    )

    return work


def load_ohlcv(
    market: str,
    timeframe: str,
) -> Tuple[
    Path,
    pd.DataFrame,
]:

    path = (
        OHLCV_DIR
        / timeframe
        / f"{market}.csv"
    )

    if not path.is_file():

        raise FileNotFoundError(
            f"OHLCV file missing: {path}"
        )

    df = read_csv_robust(
        path
    )

    df = normalize_ohlcv_dataframe(
        df,
        require_market=market,
    )

    return (
        path,
        df,
    )


# ============================================================
# 18. API REQUEST
# ============================================================

def request_json(
    url: str,
    *,
    params: Optional[
        Dict[str, Any]
    ] = None,
) -> Any:

    last_error: Optional[
        Exception
    ] = None

    headers = {
        "Accept": "application/json",
        "User-Agent": (
            "Upbit-Surge-Monitor/"
            "Historical-Gap-Recovery"
        ),
    }

    for attempt in range(
        1,
        MAX_RETRIES + 1,
    ):

        try:

            response = requests.get(
                url,
                params=params,
                headers=headers,
                timeout=REQUEST_TIMEOUT,
            )

            if response.status_code == 429:

                wait_seconds = min(
                    float(attempt),
                    5.0,
                )

                safe_print(
                    "[WARN] Upbit rate limit. "
                    f"retry={attempt}/{MAX_RETRIES} "
                    f"wait={wait_seconds:.1f}s"
                )

                time.sleep(
                    wait_seconds
                )

                continue

            response.raise_for_status()

            return response.json()

        except (
            requests.Timeout,
            requests.ConnectionError,
            requests.HTTPError,
            ValueError,
        ) as exc:

            last_error = exc

            if attempt >= MAX_RETRIES:
                break

            wait_seconds = min(
                float(attempt),
                5.0,
            )

            safe_print(
                "[WARN] API request failed: "
                f"{exc} "
                f"retry={attempt}/{MAX_RETRIES}"
            )

            time.sleep(
                wait_seconds
            )

    raise RuntimeError(
        "Upbit API request failed after "
        f"{MAX_RETRIES} attempts: "
        f"{last_error}"
    )


# ============================================================
# 19. CANDLE CONVERSION
# ============================================================

def candle_records_to_dataframe(
    market: str,
    records: Sequence[
        Dict[str, Any]
    ],
) -> pd.DataFrame:

    rows: List[
        Dict[str, Any]
    ] = []

    for item in records:

        if not isinstance(
            item,
            dict,
        ):
            continue

        rows.append(
            {
                "market": market,

                "candle_date_time_utc":
                item.get(
                    "candle_date_time_utc"
                ),

                "candle_date_time_kst":
                item.get(
                    "candle_date_time_kst"
                ),

                # API timestamp를 신뢰하지 않는다.
                "timestamp": pd.NA,

                "open":
                item.get(
                    "opening_price"
                ),

                "high":
                item.get(
                    "high_price"
                ),

                "low":
                item.get(
                    "low_price"
                ),

                "close":
                item.get(
                    "trade_price"
                ),

                "volume":
                item.get(
                    "candle_acc_trade_volume"
                ),

                "trade_value":
                item.get(
                    "candle_acc_trade_price"
                ),
            }
        )

    if not rows:

        return pd.DataFrame(
            columns=OHLCV_COLUMNS
        )

    df = pd.DataFrame(
        rows
    )

    return normalize_ohlcv_dataframe(
        df,
        require_market=market,
    )


# ============================================================
# 20. CANDLE API BATCH
# ============================================================

def fetch_candle_batch(
    market: str,
    timeframe: str,
    *,
    count: int,
    to_value: Optional[str] = None,
) -> pd.DataFrame:

    config = (
        TIMEFRAME_CONFIG[
            timeframe
        ]
    )

    params: Dict[
        str,
        Any,
    ] = {
        "market": market,
        "count": min(
            max(
                int(count),
                1,
            ),
            API_MAX_COUNT,
        ),
    }

    if to_value:

        params[
            "to"
        ] = to_value

    data = request_json(
        config[
            "url"
        ],
        params=params,
    )

    if not isinstance(
        data,
        list,
    ):

        raise RuntimeError(
            "Unexpected candle API response: "
            f"{market} {timeframe}"
        )

    return candle_records_to_dataframe(
        market=market,
        records=data,
    )


# ============================================================
# 21. EXPECTED GAP TIMES
# ============================================================

def infer_gap_bounds(
    record: GapRecord,
    existing_df: pd.DataFrame,
) -> Tuple[
    Optional[pd.Timestamp],
    Optional[pd.Timestamp],
]:

    start = parse_datetime_utc(
        record.gap_start
    )

    end = parse_datetime_utc(
        record.gap_end
    )

    interval = interval_timedelta(
        record.timeframe
    )

    count = max(
        int(record.gap_count),
        1,
    )

    if (
        start is not None
        and end is None
    ):

        end = (
            start
            + interval
            * (count - 1)
        )

    elif (
        end is not None
        and start is None
    ):

        start = (
            end
            - interval
            * (count - 1)
        )

    elif (
        start is None
        and end is None
    ):

        return (
            None,
            None,
        )

    if (
        start is not None
        and end is not None
        and end < start
    ):

        start, end = (
            end,
            start,
        )

    return (
        start,
        end,
    )


def expected_times_for_gap(
    record: GapRecord,
    existing_df: pd.DataFrame,
) -> List[pd.Timestamp]:

    start, end = infer_gap_bounds(
        record,
        existing_df,
    )

    if (
        start is None
        or end is None
    ):

        return []

    interval = interval_timedelta(
        record.timeframe
    )

    expected: List[
        pd.Timestamp
    ] = []

    current = start

    safety_count = 0

    # Gap Report의 count와 시간 범위가 약간 다르더라도
    # 실제 start~end 범위를 우선한다.
    while current <= end:

        expected.append(
            current
        )

        current = (
            current
            + interval
        )

        safety_count += 1

        if safety_count > 2_000_000:

            raise RuntimeError(
                "Gap expected time generation "
                "exceeded safety limit."
            )

    return expected


def missing_times_in_dataframe(
    expected_times: Sequence[
        pd.Timestamp
    ],
    df: pd.DataFrame,
) -> List[pd.Timestamp]:

    if not expected_times:
        return []

    existing_times = set(
        pd.to_datetime(
            df[
                "candle_date_time_utc"
            ],
            utc=True,
            errors="coerce",
        ).dropna()
    )

    return [
        value
        for value
        in expected_times
        if value not in existing_times
    ]


# ============================================================
# 22. FETCH GAP RANGE
# ============================================================

def fetch_gap_range(
    record: GapRecord,
    expected_times: Sequence[
        pd.Timestamp
    ],
) -> pd.DataFrame:

    if not expected_times:

        return pd.DataFrame(
            columns=OHLCV_COLUMNS
        )

    timeframe = (
        record.timeframe
    )

    market = (
        record.market
    )

    interval = interval_timedelta(
        timeframe
    )

    target_start = min(
        expected_times
    )

    target_end = max(
        expected_times
    )

    # Upbit "to"는 해당 시각 이전 데이터를 가져오는 구조를
    # 고려하여 gap 마지막 candle보다 여유 있게 뒤 시각을 사용한다.
    cursor = (
        target_end
        + interval
        * (
            FETCH_MARGIN_CANDLES
            + 1
        )
    )

    frames: List[
        pd.DataFrame
    ] = []

    request_count = 0

    while True:

        request_count += 1

        if (
            request_count
            > MAX_REQUESTS_PER_GAP
        ):

            raise RuntimeError(
                "Gap API pagination exceeded "
                "safety request limit."
            )

        # 현재 cursor와 target_start 사이에 필요한
        # candle 수를 계산한다.
        distance = (
            cursor
            - target_start
        )

        interval_seconds = (
            interval.total_seconds()
        )

        distance_seconds = max(
            distance.total_seconds(),
            0.0,
        )

        estimated_needed = (
            int(
                distance_seconds
                // interval_seconds
            )
            + FETCH_MARGIN_CANDLES
            + 2
        )

        request_size = min(
            API_MAX_COUNT,
            max(
                1,
                estimated_needed,
            ),
        )

        batch = fetch_candle_batch(
            market,
            timeframe,
            count=request_size,
            to_value=(
                datetime_to_upbit_to(
                    cursor
                )
            ),
        )

        time.sleep(
            REQUEST_SLEEP_SECONDS
        )

        if batch.empty:
            break

        frames.append(
            batch
        )

        batch_times = pd.to_datetime(
            batch[
                "candle_date_time_utc"
            ],
            utc=True,
            errors="coerce",
        ).dropna()

        if batch_times.empty:
            break

        oldest = batch_times.min()

        if oldest <= target_start:
            break

        next_cursor = (
            oldest
            - pd.Timedelta(
                seconds=1
            )
        )

        if next_cursor >= cursor:

            raise RuntimeError(
                "API pagination cursor "
                "did not move backward."
            )

        cursor = next_cursor

        # API가 200개 미만 반환하면
        # 해당 시장의 더 과거 데이터가 없을 가능성이 높다.
        if len(batch) < request_size:
            break

    if not frames:

        return pd.DataFrame(
            columns=OHLCV_COLUMNS
        )

    combined = pd.concat(
        frames,
        ignore_index=True,
    )

    combined = (
        normalize_ohlcv_dataframe(
            combined,
            require_market=market,
        )
    )

    # 필요한 gap 범위만 남긴다.
    combined = combined[
        (
            combined[
                "candle_date_time_utc"
            ]
            >= target_start
        )
        &
        (
            combined[
                "candle_date_time_utc"
            ]
            <= target_end
        )
    ].copy()

    combined = combined.reset_index(
        drop=True
    )

    return combined


# ============================================================
# 23. MERGE
# ============================================================

def merge_recovery_candles(
    existing_df: pd.DataFrame,
    recovery_df: pd.DataFrame,
    *,
    market: str,
) -> Tuple[
    pd.DataFrame,
    int,
]:

    existing = (
        normalize_ohlcv_dataframe(
            existing_df,
            require_market=market,
        )
    )

    if recovery_df.empty:

        return (
            existing.copy(),
            0,
        )

    recovery = (
        normalize_ohlcv_dataframe(
            recovery_df,
            require_market=market,
        )
    )

    existing_times = set(
        existing[
            "candle_date_time_utc"
        ]
    )

    new_rows = recovery[
        ~recovery[
            "candle_date_time_utc"
        ].isin(
            existing_times
        )
    ].copy()

    inserted = len(
        new_rows
    )

    if new_rows.empty:

        return (
            existing.copy(),
            0,
        )

    # IMPORTANT:
    # 기존 정상 candle을 항상 먼저 배치한다.
    #
    # drop_duplicates(... keep="first")
    # -> 기존 candle 우선
    combined = pd.concat(
        [
            existing,
            new_rows,
        ],
        ignore_index=True,
    )

    combined = combined.drop_duplicates(
        subset=[
            "candle_date_time_utc"
        ],
        keep="first",
    )

    combined = (
        normalize_ohlcv_dataframe(
            combined,
            require_market=market,
        )
    )

    return (
        combined,
        inserted,
    )


# ============================================================
# 24. DATA INTEGRITY
# ============================================================

def row_fingerprint(
    row: pd.Series,
) -> Tuple[str, ...]:

    result: List[
        str
    ] = []

    for column in OHLCV_COLUMNS:

        value = row.get(
            column
        )

        if column == (
            "candle_date_time_utc"
        ):

            parsed = pd.to_datetime(
                value,
                utc=True,
                errors="coerce",
            )

            if pd.isna(parsed):

                result.append(
                    ""
                )

            else:

                result.append(
                    parsed.isoformat()
                )

            continue

        if pd.isna(value):

            result.append(
                ""
            )

        else:

            result.append(
                str(value)
            )

    return tuple(
        result
    )


def validate_existing_rows_preserved(
    before_df: pd.DataFrame,
    after_df: pd.DataFrame,
) -> None:

    before = (
        before_df
        .set_index(
            "candle_date_time_utc",
            drop=False,
        )
    )

    after = (
        after_df
        .set_index(
            "candle_date_time_utc",
            drop=False,
        )
    )

    missing_existing = (
        before.index
        .difference(
            after.index
        )
    )

    if len(
        missing_existing
    ) > 0:

        raise RuntimeError(
            "Existing OHLCV candle loss detected: "
            f"{len(missing_existing):,}"
        )

    changed_count = 0

    for candle_time in before.index:

        before_row = before.loc[
            candle_time
        ]

        after_row = after.loc[
            candle_time
        ]

        # 중복 index 방어
        if isinstance(
            before_row,
            pd.DataFrame,
        ):

            before_row = (
                before_row.iloc[0]
            )

        if isinstance(
            after_row,
            pd.DataFrame,
        ):

            after_row = (
                after_row.iloc[0]
            )

        if (
            row_fingerprint(
                before_row
            )
            !=
            row_fingerprint(
                after_row
            )
        ):

            changed_count += 1

            if changed_count >= 10:
                break

    if changed_count > 0:

        raise RuntimeError(
            "Existing OHLCV candle values changed "
            "during recovery."
        )


def validate_ohlcv_integrity(
    df: pd.DataFrame,
    *,
    market: str,
) -> None:

    if df.empty:

        raise RuntimeError(
            "OHLCV became empty."
        )

    market_values = (
        df[
            "market"
        ]
        .astype(str)
        .str.strip()
        .str.upper()
    )

    if (
        market_values
        != market
    ).any():

        raise RuntimeError(
            "Unexpected market found "
            "after recovery."
        )

    candle_times = pd.to_datetime(
        df[
            "candle_date_time_utc"
        ],
        utc=True,
        errors="coerce",
    )

    if candle_times.isna().any():

        raise RuntimeError(
            "Invalid candle time "
            "after recovery."
        )

    duplicate_count = int(
        candle_times
        .duplicated()
        .sum()
    )

    if duplicate_count > 0:

        raise RuntimeError(
            "Duplicate candle time "
            "after recovery: "
            f"{duplicate_count:,}"
        )

    if not candle_times.is_monotonic_increasing:

        raise RuntimeError(
            "OHLCV candle order "
            "is not increasing."
        )

    canonical = (
        build_canonical_timestamp(
            candle_times
        )
    )

    actual = pd.to_numeric(
        df[
            "timestamp"
        ],
        errors="coerce",
    ).astype(
        "Int64"
    )

    mismatch = (
        canonical
        != actual
    )

    mismatch = mismatch.fillna(
        True
    )

    if mismatch.any():

        raise RuntimeError(
            "Canonical timestamp mismatch "
            "after recovery."
        )


# ============================================================
# 25. ATOMIC SAVE
# ============================================================

def dataframe_for_csv(
    df: pd.DataFrame,
) -> pd.DataFrame:

    output = df.copy()

    parsed = pd.to_datetime(
        output[
            "candle_date_time_utc"
        ],
        utc=True,
        errors="coerce",
    )

    output[
        "candle_date_time_utc"
    ] = parsed.dt.strftime(
        "%Y-%m-%dT%H:%M:%S"
    )

    # KST 값은 API에서 받은 원래 문자열과 기존 파일 값을
    # 가능한 그대로 유지한다.
    #
    # timestamp는 canonical Int64.
    output[
        "timestamp"
    ] = pd.to_numeric(
        output[
            "timestamp"
        ],
        errors="coerce",
    ).astype(
        "Int64"
    )

    return output[
        OHLCV_COLUMNS
    ]


def atomic_save_ohlcv(
    path: Path,
    df: pd.DataFrame,
    *,
    market: str,
    before_df: pd.DataFrame,
) -> None:

    validate_ohlcv_integrity(
        df,
        market=market,
    )

    validate_existing_rows_preserved(
        before_df,
        df,
    )

    temp_path = Path(
        str(path)
        + ".recovery.tmp"
    )

    if temp_path.exists():

        temp_path.unlink()

    output = dataframe_for_csv(
        df
    )

    try:

        output.to_csv(
            temp_path,
            index=False,
            encoding="utf-8-sig",
        )

        # ----------------------------------------------------
        # 디스크에 실제 저장된 temp CSV를 다시 읽어서 검증한다.
        # ----------------------------------------------------

        verify_df = read_csv_robust(
            temp_path
        )

        verify_df = (
            normalize_ohlcv_dataframe(
                verify_df,
                require_market=market,
            )
        )

        validate_ohlcv_integrity(
            verify_df,
            market=market,
        )

        validate_existing_rows_preserved(
            before_df,
            verify_df,
        )

        if len(
            verify_df
        ) != len(
            df
        ):

            raise RuntimeError(
                "Temporary recovery CSV "
                "row count mismatch."
            )

        # 모든 검증을 통과한 후에만 원본 교체
        os.replace(
            temp_path,
            path,
        )

    except Exception:

        if temp_path.exists():

            try:
                temp_path.unlink()
            except Exception:
                pass

        raise


# ============================================================
# 26. CHECKPOINT
# ============================================================

def new_checkpoint() -> Dict[
    str,
    Any,
]:

    return {
        "project": PROJECT_NAME,
        "version": VERSION,
        "mode": "REPAIR",
        "completed_events": [],
        "completed_jobs": [],
        "failed_events": {},
        "event_results": {},
        "recovered_candles": 0,
        "updated_at_utc": "",
    }


def load_repair_checkpoint() -> Dict[
    str,
    Any,
]:

    if not REPAIR_CHECKPOINT_FILE.is_file():

        return new_checkpoint()

    try:

        with REPAIR_CHECKPOINT_FILE.open(
            "r",
            encoding="utf-8",
        ) as handle:

            data = json.load(
                handle
            )

        if not isinstance(
            data,
            dict,
        ):

            raise ValueError(
                "Checkpoint root must "
                "be a JSON object."
            )

        result = new_checkpoint()

        completed_events = data.get(
            "completed_events",
            [],
        )

        if isinstance(
            completed_events,
            list,
        ):

            result[
                "completed_events"
            ] = [
                clean_text(
                    value
                )
                for value
                in completed_events
                if clean_text(
                    value
                )
            ]

        completed_jobs = data.get(
            "completed_jobs",
            [],
        )

        if isinstance(
            completed_jobs,
            list,
        ):

            result[
                "completed_jobs"
            ] = [
                clean_text(
                    value
                )
                for value
                in completed_jobs
                if clean_text(
                    value
                )
            ]

        failed_events = data.get(
            "failed_events",
            {},
        )

        if isinstance(
            failed_events,
            dict,
        ):

            result[
                "failed_events"
            ] = failed_events

        event_results = data.get(
            "event_results",
            {},
        )

        if isinstance(
            event_results,
            dict,
        ):

            result[
                "event_results"
            ] = event_results

        result[
            "recovered_candles"
        ] = parse_int(
            data.get(
                "recovered_candles",
                0,
            ),
            default=0,
        )

        result[
            "updated_at_utc"
        ] = clean_text(
            data.get(
                "updated_at_utc"
            )
        )

        return result

    except Exception as exc:

        raise RuntimeError(
            "Repair checkpoint read failed: "
            f"{REPAIR_CHECKPOINT_FILE}\n"
            f"{exc}"
        )


def save_repair_checkpoint(
    checkpoint: Dict[str, Any],
) -> None:

    payload = dict(
        checkpoint
    )

    payload[
        "project"
    ] = PROJECT_NAME

    payload[
        "version"
    ] = VERSION

    payload[
        "mode"
    ] = "REPAIR"

    payload[
        "updated_at_utc"
    ] = utc_now_iso()

    completed_events = sorted(
        set(
            clean_text(
                value
            )
            for value
            in payload.get(
                "completed_events",
                [],
            )
            if clean_text(
                value
            )
        )
    )

    completed_jobs = sorted(
        set(
            clean_text(
                value
            )
            for value
            in payload.get(
                "completed_jobs",
                [],
            )
            if clean_text(
                value
            )
        )
    )

    payload[
        "completed_events"
    ] = completed_events

    payload[
        "completed_jobs"
    ] = completed_jobs

    payload[
        "completed_event_count"
    ] = len(
        completed_events
    )

    payload[
        "completed_job_count"
    ] = len(
        completed_jobs
    )

    temp_file = Path(
        str(
            REPAIR_CHECKPOINT_FILE
        )
        + ".tmp"
    )

    with temp_file.open(
        "w",
        encoding="utf-8",
    ) as handle:

        json.dump(
            payload,
            handle,
            ensure_ascii=False,
            indent=2,
        )

    os.replace(
        temp_file,
        REPAIR_CHECKPOINT_FILE,
    )


# ============================================================
# 27. STATUS CSV
# ============================================================

STATUS_FIELDS = (
    "utc_time",
    "version",
    "mode",
    "job_index",
    "job_total",
    "event_index",
    "event_total",
    "timeframe",
    "market",
    "gap_start",
    "gap_end",
    "expected_missing",
    "api_received",
    "inserted",
    "remaining",
    "before_rows",
    "after_rows",
    "status",
    "message",
)


def append_status(
    row: Dict[str, Any],
) -> None:

    STATUS_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    exists = (
        STATUS_FILE.is_file()
    )

    with STATUS_FILE.open(
        "a",
        encoding="utf-8-sig",
        newline="",
    ) as handle:

        writer = csv.DictWriter(
            handle,
            fieldnames=STATUS_FIELDS,
        )

        if not exists:

            writer.writeheader()

        writer.writerow(
            {
                field:
                row.get(
                    field,
                    ""
                )

                for field
                in STATUS_FIELDS
            }
        )


# ============================================================
# 28. REPAIR DETAIL
# ============================================================

DETAIL_FIELDS = (
    "utc_time",
    "version",
    "timeframe",
    "market",
    "gap_start",
    "gap_end",
    "gap_report_count",
    "expected_missing_before",
    "api_received",
    "inserted",
    "remaining_after",
    "before_rows",
    "after_rows",
    "status",
    "message",
    "event_key",
    "source_report",
    "source_row",
)


def write_repair_detail(
    rows: Sequence[
        Dict[str, Any]
    ],
) -> None:

    temp_file = Path(
        str(
            REPAIR_DETAIL_FILE
        )
        + ".tmp"
    )

    with temp_file.open(
        "w",
        encoding="utf-8-sig",
        newline="",
    ) as handle:

        writer = csv.DictWriter(
            handle,
            fieldnames=DETAIL_FIELDS,
        )

        writer.writeheader()

        for row in rows:

            writer.writerow(
                {
                    field:
                    row.get(
                        field,
                        ""
                    )

                    for field
                    in DETAIL_FIELDS
                }
            )

    os.replace(
        temp_file,
        REPAIR_DETAIL_FILE,
    )


# ============================================================
# 29. SUMMARY
# ============================================================

SUMMARY_FIELDS = (
    "timeframe",
    "market",
    "gap_events",
    "recovered_events",
    "partial_events",
    "unresolved_events",
    "no_api_data_events",
    "failed_events",
    "inserted_candles",
    "remaining_candles",
)


def build_summary_rows(
    jobs: Sequence[JobKey],
    detail_rows: Sequence[
        Dict[str, Any]
    ],
) -> List[
    Dict[str, Any]
]:

    index: Dict[
        str,
        List[Dict[str, Any]],
    ] = {}

    for row in detail_rows:

        key = (
            f"{row.get('timeframe', '')}|"
            f"{row.get('market', '')}"
        )

        index.setdefault(
            key,
            [],
        ).append(
            row
        )

    summary: List[
        Dict[str, Any]
    ] = []

    for job in jobs:

        rows = index.get(
            job.key,
            [],
        )

        result = {
            "timeframe":
            job.timeframe,

            "market":
            job.market,

            "gap_events":
            len(rows),

            "recovered_events":
            0,

            "partial_events":
            0,

            "unresolved_events":
            0,

            "no_api_data_events":
            0,

            "failed_events":
            0,

            "inserted_candles":
            0,

            "remaining_candles":
            0,
        }

        for row in rows:

            status = clean_text(
                row.get(
                    "status"
                )
            )

            if status == "RECOVERED":

                result[
                    "recovered_events"
                ] += 1

            elif status == "PARTIAL":

                result[
                    "partial_events"
                ] += 1

            elif status == "UNRESOLVED":

                result[
                    "unresolved_events"
                ] += 1

            elif status == "NO_API_DATA":

                result[
                    "no_api_data_events"
                ] += 1

            elif status == "FAILED":

                result[
                    "failed_events"
                ] += 1

            result[
                "inserted_candles"
            ] += parse_int(
                row.get(
                    "inserted"
                ),
                0,
            )

            result[
                "remaining_candles"
            ] += parse_int(
                row.get(
                    "remaining_after"
                ),
                0,
            )

        summary.append(
            result
        )

    return summary


def write_repair_summary(
    rows: Sequence[
        Dict[str, Any]
    ],
) -> None:

    temp_file = Path(
        str(
            REPAIR_SUMMARY_FILE
        )
        + ".tmp"
    )

    with temp_file.open(
        "w",
        encoding="utf-8-sig",
        newline="",
    ) as handle:

        writer = csv.DictWriter(
            handle,
            fieldnames=SUMMARY_FIELDS,
        )

        writer.writeheader()

        for row in rows:

            writer.writerow(
                {
                    field:
                    row.get(
                        field,
                        ""
                    )

                    for field
                    in SUMMARY_FIELDS
                }
            )

    os.replace(
        temp_file,
        REPAIR_SUMMARY_FILE,
    )


# ============================================================
# 30. META
# ============================================================

def write_repair_meta(
    *,
    mode: str,
    markets: Sequence[str],
    jobs: Sequence[JobKey],
    report_files: Sequence[Path],
    records: Sequence[GapRecord],
    detail_rows: Sequence[
        Dict[str, Any]
    ],
    checkpoint: Dict[str, Any],
    elapsed: float,
) -> None:

    status_counts: Dict[
        str,
        int,
    ] = {}

    total_inserted = 0
    total_remaining = 0

    for row in detail_rows:

        status = clean_text(
            row.get(
                "status"
            )
        )

        status_counts[
            status
        ] = (
            status_counts.get(
                status,
                0,
            )
            + 1
        )

        total_inserted += parse_int(
            row.get(
                "inserted"
            ),
            0,
        )

        total_remaining += parse_int(
            row.get(
                "remaining_after"
            ),
            0,
        )

    payload = {

        "project":
        PROJECT_NAME,

        "version":
        VERSION,

        "mode":
        mode,

        "generated_at_utc":
        utc_now_iso(),

        "markets":
        len(markets),

        "timeframes":
        list(
            TIMEFRAMES
        ),

        "jobs":
        len(jobs),

        "gap_report_files":
        [
            str(path)
            for path
            in report_files
        ],

        "gap_events":
        len(records),

        "detail_rows":
        len(detail_rows),

        "status_counts":
        status_counts,

        "inserted_candles":
        total_inserted,

        "remaining_candles":
        total_remaining,

        "checkpoint": {
            "completed_events":
            len(
                checkpoint.get(
                    "completed_events",
                    [],
                )
            ),

            "completed_jobs":
            len(
                checkpoint.get(
                    "completed_jobs",
                    [],
                )
            ),

            "failed_events":
            len(
                checkpoint.get(
                    "failed_events",
                    {},
                )
            ),
        },

        "elapsed_seconds":
        round(
            elapsed,
            3,
        ),

        "safety": {
            "existing_candle_priority":
            "ENABLED",

            "fake_candle_generation":
            "DISABLED",

            "atomic_save":
            "ENABLED",

            "temp_file_validation":
            "ENABLED",

            "feature_build":
            "DISABLED",

            "256_detector":
            "DISABLED",

            "future_labels":
            "DISABLED",

            "prediction":
            "DISABLED",

            "trading":
            "DISABLED",

            "git_reset":
            "DISABLED",

            "git_clean":
            "DISABLED",

            "git_commit":
            "DISABLED",

            "git_push":
            "DISABLED",
        },
    }

    temp_file = Path(
        str(
            REPAIR_META_FILE
        )
        + ".tmp"
    )

    with temp_file.open(
        "w",
        encoding="utf-8",
    ) as handle:

        json.dump(
            payload,
            handle,
            ensure_ascii=False,
            indent=2,
        )

    os.replace(
        temp_file,
        REPAIR_META_FILE,
    )


# ============================================================
# 31. EVENT RESULT
# ============================================================

def make_detail_row(
    *,
    record: GapRecord,
    expected_missing_before: int,
    api_received: int,
    inserted: int,
    remaining_after: int,
    before_rows: int,
    after_rows: int,
    status: str,
    message: str,
) -> Dict[str, Any]:

    return {
        "utc_time":
        utc_now_iso(),

        "version":
        VERSION,

        "timeframe":
        record.timeframe,

        "market":
        record.market,

        "gap_start":
        record.gap_start,

        "gap_end":
        record.gap_end,

        "gap_report_count":
        record.gap_count,

        "expected_missing_before":
        expected_missing_before,

        "api_received":
        api_received,

        "inserted":
        inserted,

        "remaining_after":
        remaining_after,

        "before_rows":
        before_rows,

        "after_rows":
        after_rows,

        "status":
        status,

        "message":
        message,

        "event_key":
        record.event_key,

        "source_report":
        record.source_report,

        "source_row":
        record.source_row,
    }


# ============================================================
# 32. PROCESS ONE GAP
# ============================================================

def process_gap_event(
    record: GapRecord,
    *,
    repair_enabled: bool,
) -> Dict[str, Any]:

    path, existing_df = load_ohlcv(
        record.market,
        record.timeframe,
    )

    before_rows = len(
        existing_df
    )

    expected_times = (
        expected_times_for_gap(
            record,
            existing_df,
        )
    )

    if not expected_times:

        return make_detail_row(
            record=record,
            expected_missing_before=0,
            api_received=0,
            inserted=0,
            remaining_after=0,
            before_rows=before_rows,
            after_rows=before_rows,
            status="UNRESOLVED",
            message=(
                "Gap start/end could not be "
                "resolved safely."
            ),
        )

    missing_before = (
        missing_times_in_dataframe(
            expected_times,
            existing_df,
        )
    )

    expected_missing_before = len(
        missing_before
    )

    # --------------------------------------------------------
    # 이미 복구되어 있는 경우
    # --------------------------------------------------------

    if expected_missing_before == 0:

        return make_detail_row(
            record=record,
            expected_missing_before=0,
            api_received=0,
            inserted=0,
            remaining_after=0,
            before_rows=before_rows,
            after_rows=before_rows,
            status="RECOVERED",
            message=(
                "Gap is already filled in "
                "current OHLCV."
            ),
        )

    # --------------------------------------------------------
    # Preview / safety mode
    # --------------------------------------------------------

    if not repair_enabled:

        return make_detail_row(
            record=record,
            expected_missing_before=(
                expected_missing_before
            ),
            api_received=0,
            inserted=0,
            remaining_after=(
                expected_missing_before
            ),
            before_rows=before_rows,
            after_rows=before_rows,
            status="PREVIEW",
            message=(
                "Repair disabled. "
                "Use --repair to execute."
            ),
        )

    # --------------------------------------------------------
    # API fetch
    # --------------------------------------------------------

    api_df = fetch_gap_range(
        record,
        missing_before,
    )

    api_received = len(
        api_df
    )

    if api_df.empty:

        return make_detail_row(
            record=record,
            expected_missing_before=(
                expected_missing_before
            ),
            api_received=0,
            inserted=0,
            remaining_after=(
                expected_missing_before
            ),
            before_rows=before_rows,
            after_rows=before_rows,
            status="NO_API_DATA",
            message=(
                "Upbit API returned no candle "
                "for the requested gap range."
            ),
        )

    # API에서 받은 것 중 실제 missing time만 사용
    missing_set = set(
        missing_before
    )

    api_missing_df = api_df[
        api_df[
            "candle_date_time_utc"
        ].isin(
            missing_set
        )
    ].copy()

    if api_missing_df.empty:

        return make_detail_row(
            record=record,
            expected_missing_before=(
                expected_missing_before
            ),
            api_received=api_received,
            inserted=0,
            remaining_after=(
                expected_missing_before
            ),
            before_rows=before_rows,
            after_rows=before_rows,
            status="NO_API_DATA",
            message=(
                "API returned candles, but none "
                "matched actual missing times."
            ),
        )

    # --------------------------------------------------------
    # Merge
    # --------------------------------------------------------

    merged_df, inserted = (
        merge_recovery_candles(
            existing_df,
            api_missing_df,
            market=record.market,
        )
    )

    # --------------------------------------------------------
    # 기존 candle 완전 보존 검증
    # --------------------------------------------------------

    validate_existing_rows_preserved(
        existing_df,
        merged_df,
    )

    validate_ohlcv_integrity(
        merged_df,
        market=record.market,
    )

    # --------------------------------------------------------
    # 실제 추가 candle이 있을 때만 저장
    # --------------------------------------------------------

    if inserted > 0:

        atomic_save_ohlcv(
            path,
            merged_df,
            market=record.market,
            before_df=existing_df,
        )

        # 저장된 원본을 다시 읽는다.
        _, final_df = load_ohlcv(
            record.market,
            record.timeframe,
        )

    else:

        final_df = existing_df

    # --------------------------------------------------------
    # 최종 gap 재검사
    # --------------------------------------------------------

    remaining_times = (
        missing_times_in_dataframe(
            expected_times,
            final_df,
        )
    )

    remaining_after = len(
        remaining_times
    )

    after_rows = len(
        final_df
    )

    # --------------------------------------------------------
    # Row count sanity
    # --------------------------------------------------------

    expected_after_rows = (
        before_rows
        + inserted
    )

    if (
        after_rows
        != expected_after_rows
    ):

        raise RuntimeError(
            "Unexpected OHLCV row count after recovery. "
            f"before={before_rows:,} "
            f"inserted={inserted:,} "
            f"after={after_rows:,}"
        )

    # --------------------------------------------------------
    # Final status
    # --------------------------------------------------------

    if remaining_after == 0:

        status = "RECOVERED"

        message = (
            "Historical gap fully recovered."
        )

    elif inserted > 0:

        status = "PARTIAL"

        message = (
            "Historical gap partially recovered. "
            "Some candles are not available "
            "from Upbit API."
        )

    else:

        status = "UNRESOLVED"

        message = (
            "No missing candle was inserted."
        )

    return make_detail_row(
        record=record,
        expected_missing_before=(
            expected_missing_before
        ),
        api_received=api_received,
        inserted=inserted,
        remaining_after=(
            remaining_after
        ),
        before_rows=before_rows,
        after_rows=after_rows,
        status=status,
        message=message,
    )


# ============================================================
# 33. CHECKPOINT EVENT COMPLETION RULE
# ============================================================

def event_is_terminal(
    status: str,
) -> bool:

    # --------------------------------------------------------
    # RECOVERED
    #     완전 복구 완료
    #
    # PARTIAL
    #     API에 존재하는 데이터는 복구했지만 일부 없음
    #
    # NO_API_DATA
    #     현재 API에 해당 candle 없음
    #
    # UNRESOLVED
    #     자동 복구 불가
    #
    # 이 네 상태는 동일 실행에서 무한 재시도하지 않는다.
    #
    # FAILED는 completed_events에 넣지 않는다.
    # 다음 실행에서 다시 시도 가능.
    # --------------------------------------------------------

    return status in {
        "RECOVERED",
        "PARTIAL",
        "NO_API_DATA",
        "UNRESOLVED",
    }


# ============================================================
# 34. LOAD PREVIOUS EVENT RESULTS
# ============================================================

def checkpoint_detail_rows(
    checkpoint: Dict[str, Any],
) -> List[
    Dict[str, Any]
]:

    event_results = checkpoint.get(
        "event_results",
        {},
    )

    if not isinstance(
        event_results,
        dict,
    ):

        return []

    rows: List[
        Dict[str, Any]
    ] = []

    for value in (
        event_results.values()
    ):

        if isinstance(
            value,
            dict,
        ):

            rows.append(
                dict(value)
            )

    rows.sort(
        key=lambda row: (
            clean_text(
                row.get(
                    "timeframe"
                )
            ),
            clean_text(
                row.get(
                    "market"
                )
            ),
            clean_text(
                row.get(
                    "gap_start"
                )
            ),
            clean_text(
                row.get(
                    "gap_end"
                )
            ),
        )
    )

    return rows


# ============================================================
# 35. RUN
# ============================================================

def run(
    *,
    repair_enabled: bool,
    reset_repair_checkpoint: bool,
) -> int:

    start_time = time.time()

    ensure_runtime_directories()

    mode = (
        "REPAIR"
        if repair_enabled
        else "PREVIEW"
    )

    safe_print(
        separator()
    )

    safe_print(
        "HISTORICAL GAP RECOVERY - CLEAN V002"
    )

    safe_print(
        separator()
    )

    safe_print(
        f"Project        : {PROJECT_NAME}"
    )

    safe_print(
        f"Version        : {VERSION}"
    )

    safe_print(
        f"Execution mode : {mode}"
    )

    safe_print(
        "Existing OHLCV : PRESERVE"
    )

    safe_print(
        "Fake candles   : DISABLED"
    )

    safe_print(
        "Feature build  : DISABLED"
    )

    safe_print(
        "Prediction     : DISABLED"
    )

    safe_print(
        "Trading        : DISABLED"
    )

    safe_print(
        separator()
    )

    if not repair_enabled:

        safe_print()
        safe_print(
            "[SAFETY] Actual OHLCV repair is DISABLED."
        )
        safe_print(
            "[SAFETY] Run with --repair only after "
            "reviewing the recovery plan."
        )

    # --------------------------------------------------------
    # Market discovery
    # --------------------------------------------------------

    markets = discover_markets()

    jobs = build_jobs(
        markets
    )

    safe_print()
    safe_print(
        separator()
    )
    safe_print(
        "MARKET DISCOVERY"
    )
    safe_print(
        separator()
    )

    safe_print(
        f"Markets        : {len(markets):,}"
    )

    safe_print(
        f"Timeframes     : {len(TIMEFRAMES):,}"
    )

    safe_print(
        f"Total jobs     : {len(jobs):,}"
    )

    if (
        len(markets)
        != EXPECTED_MARKET_COUNT
    ):

        safe_print(
            "[WARN] Expected "
            f"{EXPECTED_MARKET_COUNT:,} markets, "
            f"found {len(markets):,}."
        )

    if (
        len(jobs)
        != EXPECTED_JOB_COUNT
    ):

        safe_print(
            "[WARN] Expected "
            f"{EXPECTED_JOB_COUNT:,} jobs, "
            f"found {len(jobs):,}."
        )

    # --------------------------------------------------------
    # Gap reports
    # --------------------------------------------------------

    candidates = (
        discover_gap_report_files()
    )

    (
        records,
        accepted_report_files,
    ) = load_all_gap_records(
        candidates
    )

    records = (
        deduplicate_gap_records(
            records
        )
    )

    valid_markets = set(
        markets
    )

    # 현재 실제 OHLCV 시장에 존재하는 record만 처리
    records = [
        record
        for record
        in records
        if (
            record.market
            in valid_markets
            and record.timeframe
            in TIMEFRAMES
        )
    ]

    safe_print()
    safe_print(
        separator()
    )
    safe_print(
        "GAP REPORT RESULT"
    )
    safe_print(
        separator()
    )

    safe_print(
        "Accepted reports : "
        f"{len(accepted_report_files):,}"
    )

    safe_print(
        "Gap events       : "
        f"{len(records):,}"
    )

    safe_print(
        "Missing estimate : "
        f"{sum(r.gap_count for r in records):,}"
    )

    if not accepted_report_files:

        safe_print()
        safe_print(
            "[WARN] No compatible Gap Report found."
        )

        safe_print(
            "[SAFETY] No OHLCV will be modified."
        )

        return 0

    if not records:

        safe_print()
        safe_print(
            "[INFO] No valid gap events to process."
        )

        return 0

    gap_index = build_gap_index(
        records
    )

    # --------------------------------------------------------
    # Checkpoint reset
    # --------------------------------------------------------

    if reset_repair_checkpoint:

        if (
            REPAIR_CHECKPOINT_FILE
            .exists()
        ):

            REPAIR_CHECKPOINT_FILE.unlink()

            safe_print()
            safe_print(
                "[INFO] Clean V002 repair checkpoint reset."
            )

        else:

            safe_print()
            safe_print(
                "[INFO] No Clean V002 repair checkpoint "
                "exists to reset."
            )

    # --------------------------------------------------------
    # V001 checkpoint is intentionally untouched
    # --------------------------------------------------------

    if V001_CHECKPOINT_FILE.exists():

        safe_print()
        safe_print(
            "[INFO] Clean V001 DRY-RUN checkpoint detected."
        )

        safe_print(
            "[INFO] It will NOT be modified or reused."
        )

    # --------------------------------------------------------
    # Preview mode does NOT consume the repair checkpoint.
    #
    # 실제 --repair 실행에서만 Resume state를 사용한다.
    # --------------------------------------------------------

    if repair_enabled:

        checkpoint = (
            load_repair_checkpoint()
        )

    else:

        checkpoint = (
            new_checkpoint()
        )

    completed_events: Set[
        str
    ] = set(
        checkpoint.get(
            "completed_events",
            [],
        )
    )

    completed_jobs: Set[
        str
    ] = set(
        checkpoint.get(
            "completed_jobs",
            [],
        )
    )

    failed_events = checkpoint.get(
        "failed_events",
        {},
    )

    if not isinstance(
        failed_events,
        dict,
    ):

        failed_events = {}

    event_results = checkpoint.get(
        "event_results",
        {},
    )

    if not isinstance(
        event_results,
        dict,
    ):

        event_results = {}

    # --------------------------------------------------------
    # 현재 존재하는 event/job만 checkpoint에서 유지
    # --------------------------------------------------------

    valid_event_keys = {
        record.event_key
        for record
        in records
    }

    valid_job_keys = {
        job.key
        for job
        in jobs
    }

    completed_events.intersection_update(
        valid_event_keys
    )

    completed_jobs.intersection_update(
        valid_job_keys
    )

    event_results = {
        key: value
        for key, value
        in event_results.items()
        if key in valid_event_keys
    }

    failed_events = {
        key: value
        for key, value
        in failed_events.items()
        if key in valid_event_keys
    }

    checkpoint[
        "completed_events"
    ] = sorted(
        completed_events
    )

    checkpoint[
        "completed_jobs"
    ] = sorted(
        completed_jobs
    )

    checkpoint[
        "failed_events"
    ] = failed_events

    checkpoint[
        "event_results"
    ] = event_results

    safe_print()
    safe_print(
        separator()
    )
    safe_print(
        "RESUME / CHECKPOINT"
    )
    safe_print(
        separator()
    )

    safe_print(
        f"Checkpoint : {REPAIR_CHECKPOINT_FILE}"
    )

    safe_print(
        "Completed events : "
        f"{len(completed_events):,}"
    )

    safe_print(
        "Completed jobs   : "
        f"{len(completed_jobs):,}"
    )

    safe_print(
        "Failed events    : "
        f"{len(failed_events):,}"
    )

    # --------------------------------------------------------
    # Process jobs
    # --------------------------------------------------------

    total_jobs = len(
        jobs
    )

    for job_index, job in enumerate(
        jobs,
        start=1,
    ):

        job_records = gap_index.get(
            job.key,
            [],
        )

        if not job_records:
            continue

        safe_print()
        safe_print(
            separator("-")
        )

        safe_print(
            f"[JOB {job_index:,}/{total_jobs:,}] "
            f"{job.timeframe.upper()} "
            f"{job.market}"
        )

        safe_print(
            f"Gap events : {len(job_records):,}"
        )

        safe_print(
            separator("-")
        )

        event_total = len(
            job_records
        )

        all_job_events_terminal = True

        for event_index, record in enumerate(
            job_records,
            start=1,
        ):

            event_key = (
                record.event_key
            )

            if (
                repair_enabled
                and event_key
                in completed_events
            ):

                safe_print(
                    f"[RESUME {event_index:,}/{event_total:,}] "
                    "already completed"
                )

                continue

            if (
                record.gap_count
                >= EXTREME_GAP_THRESHOLD
            ):

                gap_level = (
                    "EXTREME GAP"
                )

            elif (
                record.gap_count
                >= HIGH_GAP_THRESHOLD
            ):

                gap_level = (
                    "HIGH GAP"
                )

            else:

                gap_level = (
                    "NORMAL GAP"
                )

            safe_print()
            safe_print(
                f"[EVENT {event_index:,}/{event_total:,}] "
                f"{gap_level}"
            )

            safe_print(
                f"  Market      : {record.market}"
            )

            safe_print(
                f"  Timeframe   : {record.timeframe}"
            )

            safe_print(
                f"  Gap count   : {record.gap_count:,}"
            )

            safe_print(
                f"  Gap start   : {record.gap_start}"
            )

            safe_print(
                f"  Gap end     : {record.gap_end}"
            )

            try:

                detail = process_gap_event(
                    record,
                    repair_enabled=(
                        repair_enabled
                    ),
                )

                status = clean_text(
                    detail.get(
                        "status"
                    )
                )

                safe_print(
                    f"  Expected    : "
                    f"{detail['expected_missing_before']:,}"
                )

                safe_print(
                    f"  API received: "
                    f"{detail['api_received']:,}"
                )

                safe_print(
                    f"  Inserted    : "
                    f"{detail['inserted']:,}"
                )

                safe_print(
                    f"  Remaining   : "
                    f"{detail['remaining_after']:,}"
                )

                safe_print(
                    f"  Status      : {status}"
                )

                append_status(
                    {
                        "utc_time":
                        utc_now_iso(),

                        "version":
                        VERSION,

                        "mode":
                        mode,

                        "job_index":
                        job_index,

                        "job_total":
                        total_jobs,

                        "event_index":
                        event_index,

                        "event_total":
                        event_total,

                        "timeframe":
                        record.timeframe,

                        "market":
                        record.market,

                        "gap_start":
                        record.gap_start,

                        "gap_end":
                        record.gap_end,

                        "expected_missing":
                        detail[
                            "expected_missing_before"
                        ],

                        "api_received":
                        detail[
                            "api_received"
                        ],

                        "inserted":
                        detail[
                            "inserted"
                        ],

                        "remaining":
                        detail[
                            "remaining_after"
                        ],

                        "before_rows":
                        detail[
                            "before_rows"
                        ],

                        "after_rows":
                        detail[
                            "after_rows"
                        ],

                        "status":
                        status,

                        "message":
                        detail[
                            "message"
                        ],
                    }
                )

                # --------------------------------------------
                # Preview에서는 실제 repair checkpoint를
                # 절대 변경하지 않는다.
                # --------------------------------------------

                if repair_enabled:

                    event_results[
                        event_key
                    ] = detail

                    inserted_count = (
                        parse_int(
                            detail.get(
                                "inserted"
                            ),
                            0,
                        )
                    )

                    checkpoint[
                        "recovered_candles"
                    ] = (
                        parse_int(
                            checkpoint.get(
                                "recovered_candles",
                                0,
                            ),
                            0,
                        )
                        + inserted_count
                    )

                    if event_is_terminal(
                        status
                    ):

                        completed_events.add(
                            event_key
                        )

                        failed_events.pop(
                            event_key,
                            None,
                        )

                    else:

                        all_job_events_terminal = False

                    checkpoint[
                        "completed_events"
                    ] = sorted(
                        completed_events
                    )

                    checkpoint[
                        "failed_events"
                    ] = failed_events

                    checkpoint[
                        "event_results"
                    ] = event_results

                    # 매 Gap Event 직후 저장
                    save_repair_checkpoint(
                        checkpoint
                    )

            except Exception as exc:

                all_job_events_terminal = False

                error_message = (
                    f"{type(exc).__name__}: "
                    f"{exc}"
                )

                safe_print(
                    f"  Status      : FAILED"
                )

                safe_print(
                    f"  Error       : {error_message}"
                )

                detail = make_detail_row(
                    record=record,
                    expected_missing_before=0,
                    api_received=0,
                    inserted=0,
                    remaining_after=(
                        record.gap_count
                    ),
                    before_rows=0,
                    after_rows=0,
                    status="FAILED",
                    message=error_message,
                )

                append_status(
                    {
                        "utc_time":
                        utc_now_iso(),

                        "version":
                        VERSION,

                        "mode":
                        mode,

                        "job_index":
                        job_index,

                        "job_total":
                        total_jobs,

                        "event_index":
                        event_index,

                        "event_total":
                        event_total,

                        "timeframe":
                        record.timeframe,

                        "market":
                        record.market,

                        "gap_start":
                        record.gap_start,

                        "gap_end":
                        record.gap_end,

                        "expected_missing":
                        "",

                        "api_received":
                        0,

                        "inserted":
                        0,

                        "remaining":
                        record.gap_count,

                        "before_rows":
                        "",

                        "after_rows":
                        "",

                        "status":
                        "FAILED",

                        "message":
                        error_message,
                    }
                )

                if repair_enabled:

                    event_results[
                        event_key
                    ] = detail

                    failed_events[
                        event_key
                    ] = {
                        "utc_time":
                        utc_now_iso(),

                        "error":
                        error_message,

                        "market":
                        record.market,

                        "timeframe":
                        record.timeframe,

                        "gap_start":
                        record.gap_start,

                        "gap_end":
                        record.gap_end,
                    }

                    checkpoint[
                        "failed_events"
                    ] = failed_events

                    checkpoint[
                        "event_results"
                    ] = event_results

                    save_repair_checkpoint(
                        checkpoint
                    )

            time.sleep(
                GAP_SLEEP_SECONDS
            )

        # ----------------------------------------------------
        # Job completion
        # ----------------------------------------------------

        if repair_enabled:

            job_event_keys = {
                record.event_key
                for record
                in job_records
            }

            if (
                all_job_events_terminal
                and
                job_event_keys.issubset(
                    completed_events
                )
            ):

                completed_jobs.add(
                    job.key
                )

            else:

                completed_jobs.discard(
                    job.key
                )

            checkpoint[
                "completed_jobs"
            ] = sorted(
                completed_jobs
            )

            save_repair_checkpoint(
                checkpoint
            )

    # --------------------------------------------------------
    # Reports
    # --------------------------------------------------------

    if repair_enabled:

        detail_rows = (
            checkpoint_detail_rows(
                checkpoint
            )
        )

    else:

        # Preview 결과는 STATUS CSV에만 남기고,
        # actual repair detail checkpoint와 섞지 않는다.
        detail_rows = []

        for record in records:

            try:

                preview_detail = (
                    process_gap_event(
                        record,
                        repair_enabled=False,
                    )
                )

                detail_rows.append(
                    preview_detail
                )

            except Exception as exc:

                detail_rows.append(
                    make_detail_row(
                        record=record,
                        expected_missing_before=0,
                        api_received=0,
                        inserted=0,
                        remaining_after=(
                            record.gap_count
                        ),
                        before_rows=0,
                        after_rows=0,
                        status="FAILED",
                        message=(
                            f"{type(exc).__name__}: "
                            f"{exc}"
                        ),
                    )
                )

    summary_rows = (
        build_summary_rows(
            jobs,
            detail_rows,
        )
    )

    write_repair_detail(
        detail_rows
    )

    write_repair_summary(
        summary_rows
    )

    elapsed = (
        time.time()
        - start_time
    )

    write_repair_meta(
        mode=mode,
        markets=markets,
        jobs=jobs,
        report_files=(
            accepted_report_files
        ),
        records=records,
        detail_rows=detail_rows,
        checkpoint=checkpoint,
        elapsed=elapsed,
    )

    # --------------------------------------------------------
    # Final statistics
    # --------------------------------------------------------

    status_counts: Dict[
        str,
        int,
    ] = {}

    total_inserted = 0
    total_remaining = 0

    for row in detail_rows:

        status = clean_text(
            row.get(
                "status"
            )
        )

        status_counts[
            status
        ] = (
            status_counts.get(
                status,
                0,
            )
            + 1
        )

        total_inserted += (
            parse_int(
                row.get(
                    "inserted"
                ),
                0,
            )
        )

        total_remaining += (
            parse_int(
                row.get(
                    "remaining_after"
                ),
                0,
            )
        )

    # --------------------------------------------------------
    # Final output
    # --------------------------------------------------------

    safe_print()
    safe_print(
        separator()
    )

    safe_print(
        "HISTORICAL GAP RECOVERY SUMMARY"
    )

    safe_print(
        separator()
    )

    safe_print(
        f"Project          : {PROJECT_NAME}"
    )

    safe_print(
        f"Version          : {VERSION}"
    )

    safe_print(
        f"Execution mode   : {mode}"
    )

    safe_print(
        f"Markets          : {len(markets):,}"
    )

    safe_print(
        f"Total jobs       : {len(jobs):,}"
    )

    safe_print(
        f"Gap events       : {len(records):,}"
    )

    safe_print()

    safe_print(
        "Results:"
    )

    for status in (
        "RECOVERED",
        "PARTIAL",
        "NO_API_DATA",
        "UNRESOLVED",
        "PREVIEW",
        "FAILED",
    ):

        safe_print(
            f"  {status:<12}: "
            f"{status_counts.get(status, 0):,}"
        )

    safe_print()

    safe_print(
        f"Inserted candles : {total_inserted:,}"
    )

    safe_print(
        f"Remaining candles: {total_remaining:,}"
    )

    if repair_enabled:

        safe_print(
            "Completed events : "
            f"{len(completed_events):,}"
        )

        safe_print(
            "Completed jobs   : "
            f"{len(completed_jobs):,}"
        )

        safe_print(
            "Failed events    : "
            f"{len(failed_events):,}"
        )

    safe_print()

    safe_print(
        "Output:"
    )

    safe_print(
        f"  Detail     : {REPAIR_DETAIL_FILE}"
    )

    safe_print(
        f"  Summary    : {REPAIR_SUMMARY_FILE}"
    )

    safe_print(
        f"  Meta       : {REPAIR_META_FILE}"
    )

    safe_print(
        f"  Status     : {STATUS_FILE}"
    )

    safe_print(
        f"  Checkpoint : {REPAIR_CHECKPOINT_FILE}"
    )

    safe_print()

    safe_print(
        "Safety:"
    )

    safe_print(
        "  Existing candle priority : ENABLED"
    )

    safe_print(
        "  Fake candle generation   : DISABLED"
    )

    safe_print(
        "  Atomic save              : ENABLED"
    )

    safe_print(
        "  Temp CSV verification    : ENABLED"
    )

    safe_print(
        "  Feature build            : DISABLED"
    )

    safe_print(
        "  Prediction               : DISABLED"
    )

    safe_print(
        "  Trading                  : DISABLED"
    )

    safe_print(
        "  Git reset/clean          : DISABLED"
    )

    safe_print(
        "  Git commit/push          : DISABLED"
    )

    safe_print()

    safe_print(
        f"Elapsed total    : {elapsed:.2f}s"
    )

    safe_print(
        separator()
    )

    if repair_enabled:

        if status_counts.get(
            "FAILED",
            0,
        ) > 0:

            safe_print(
                "[RESULT] HISTORICAL GAP RECOVERY "
                "COMPLETED WITH FAILED EVENTS"
            )

            safe_print(
                "[RESUME] Failed events were NOT marked "
                "completed and can be retried."
            )

        elif total_remaining > 0:

            safe_print(
                "[RESULT] HISTORICAL GAP RECOVERY "
                "COMPLETED WITH UNRESOLVED GAPS"
            )

            safe_print(
                "[INFO] Existing/API-available candles "
                "were preserved and recovered."
            )

        else:

            safe_print(
                "[RESULT] HISTORICAL GAP RECOVERY PASSED"
            )

            safe_print(
                "[PASS] All detected recoverable gaps "
                "were filled."
            )

    else:

        safe_print(
            "[RESULT] HISTORICAL GAP RECOVERY "
            "PREVIEW PASSED"
        )

        safe_print(
            "[SAFETY] Original OHLCV data was not modified."
        )

        safe_print(
            "[NEXT] Use --repair only after reviewing "
            "the preview result."
        )

    safe_print(
        separator()
    )

    # FAILED event가 있으면 CI에서도 실패를 알 수 있도록
    # repair mode에서 exit code 1.
    if (
        repair_enabled
        and status_counts.get(
            "FAILED",
            0,
        ) > 0
    ):

        return 1

    return 0


# ============================================================
# 36. ARGUMENTS
# ============================================================

def parse_args() -> argparse.Namespace:

    parser = argparse.ArgumentParser(
        description=(
            "Upbit Surge Monitor "
            "Historical Gap Recovery "
            "Clean V002"
        )
    )

    parser.add_argument(
        "--repair",
        action="store_true",
        help=(
            "Enable actual historical OHLCV "
            "gap recovery. Without this option "
            "the program runs in PREVIEW mode."
        ),
    )

    parser.add_argument(
        "--reset-repair-checkpoint",
        action="store_true",
        help=(
            "Reset only the Clean V002 actual repair "
            "checkpoint. Clean V001 DRY-RUN checkpoint "
            "is never modified."
        ),
    )

    return parser.parse_args()


# ============================================================
# 37. ENTRY POINT
# ============================================================

def main() -> int:

    args = parse_args()

    try:

        return run(
            repair_enabled=(
                args.repair
            ),
            reset_repair_checkpoint=(
                args.reset_repair_checkpoint
            ),
        )

    except KeyboardInterrupt:

        safe_print()
        safe_print(
            separator()
        )

        safe_print(
            "HISTORICAL GAP RECOVERY INTERRUPTED"
        )

        safe_print(
            separator()
        )

        safe_print(
            "[RESUME] Successfully completed "
            "Clean V002 events remain checkpointed."
        )

        safe_print(
            "[SAFETY] Current uncommitted event "
            "will be checked again next run."
        )

        safe_print(
            "[SAFETY] No automatic cleanup executed."
        )

        safe_print(
            separator()
        )

        return 130

    except Exception as exc:

        safe_print()
        safe_print(
            separator()
        )

        safe_print(
            "HISTORICAL GAP RECOVERY FATAL ERROR"
        )

        safe_print(
            separator()
        )

        safe_print(
            f"{type(exc).__name__}: {exc}"
        )

        safe_print()

        safe_print(
            traceback.format_exc()
        )

        safe_print(
            "[SAFETY] Existing OHLCV preservation "
            "rules remain active."
        )

        safe_print(
            "[SAFETY] No automatic cleanup executed."
        )

        safe_print(
            "[RESUME] Successfully completed "
            "repair events remain checkpointed."
        )

        safe_print(
            separator()
        )

        return 1


if __name__ == "__main__":
    raise SystemExit(
        main()
    )
