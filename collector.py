"""
Upbit Surge Monitor - Collector Clean V003
==========================================

File:
    collector.py

Purpose:
    Upbit KRW 전체 마켓의 OHLCV 데이터를 수집하고
    로컬 CSV 파일에 안전하게 누적 저장한다.

Timeframes:
    - 1 hour
    - 4 hour
    - 1 day

Clean V003 goals:
    1. Clean V002 canonical timestamp 정책 유지
    2. 기존 OHLCV 데이터 절대 삭제 금지
    3. 마지막 저장 candle 이후 최신 데이터 증분 수집
    4. 전체 KRW 시장 x h1/h4/d1 작업
    5. 실행 단위 checkpoint / Resume 지원
    6. PC / Runner 중단 후 동일 run 재개
    7. 완료 job 즉시 checkpoint 저장
    8. 안전한 임시파일 저장 후 원본 교체
    9. source OHLCV 누적 보존
    10. Collector는 Feature / Signal / Prediction을 생성하지 않음

Important Resume policy:
    Collector의 checkpoint는 "현재 실행(run)"의 중단 복구용이다.

    Validator와 달리 Collector 데이터는 시간이 지나면 새로운 candle이
    계속 생기므로, 이전에 성공한 job을 영구적으로 SKIP하면 안 된다.

    따라서:

        - 실행 중 중단:
            같은 active run checkpoint를 이용해 완료 job SKIP

        - 전체 실행 정상 완료:
            active checkpoint를 completed 상태로 기록

        - 다음 Collector 실행:
            새로운 run 생성
            모든 market/timeframe의 최신 candle을 다시 확인

Canonical timestamp:
    Source:
        candle_date_time_utc

    Conversion:
        candle_date_time_utc
        -> UTC datetime
        -> Unix epoch milliseconds

    Upbit API의 timestamp 값은 사용하지 않는다.

Safety:
    - 기존 OHLCV 데이터 삭제 금지
    - 기존 candle 보존
    - candle_date_time_utc 기준 중복 제거
    - candle_date_time_utc 기준 시간순 정렬
    - 기존 CSV timestamp 자동 정규화
    - 저장 전 timestamp 재정규화
    - 임시 파일 저장 후 원본 교체
    - empty dataframe 저장 금지
    - Feature generation 없음
    - 256 Detector 없음
    - Prediction 없음
    - Trading 없음
    - Git reset 없음
    - Git clean 없음
    - Git commit 없음
    - Git push 없음

Windows:
    py collector.py
"""

from __future__ import annotations

import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests


# ============================================================
# PROJECT CONFIG
# ============================================================

PROJECT_NAME = "Upbit Surge Monitor"
VERSION = "Collector Clean V003"

BASE_DIR = Path(__file__).resolve().parent

DATA_DIR = BASE_DIR / "data"
OHLCV_DIR = DATA_DIR / "ohlcv"

H1_DIR = OHLCV_DIR / "h1"
H4_DIR = OHLCV_DIR / "h4"
D1_DIR = OHLCV_DIR / "d1"

STATUS_FILE = DATA_DIR / "collector_status.csv"

COLLECTOR_STATE_DIR = DATA_DIR / "collector"
CHECKPOINT_FILE = (
    COLLECTOR_STATE_DIR
    / "collector_checkpoint.json"
)

UPBIT_API_BASE = "https://api.upbit.com/v1"

REQUEST_TIMEOUT = 15
API_MAX_COUNT = 200

INITIAL_CANDLE_LIMITS = {
    "h1": 2000,
    "h4": 1500,
    "d1": 1000,
}

REQUEST_SLEEP_SECONDS = 0.12
MARKET_SLEEP_SECONDS = 0.05
MAX_RETRIES = 5

UPDATE_SAFETY_MAX_CANDLES = 10_000


# ============================================================
# TIMEFRAME CONFIG
# ============================================================

TIMEFRAMES = {
    "h1": {
        "url": (
            f"{UPBIT_API_BASE}"
            "/candles/minutes/60"
        ),
        "directory": H1_DIR,
        "interval_seconds": 60 * 60,
    },
    "h4": {
        "url": (
            f"{UPBIT_API_BASE}"
            "/candles/minutes/240"
        ),
        "directory": H4_DIR,
        "interval_seconds": 4 * 60 * 60,
    },
    "d1": {
        "url": (
            f"{UPBIT_API_BASE}"
            "/candles/days"
        ),
        "directory": D1_DIR,
        "interval_seconds": 24 * 60 * 60,
    },
}


# ============================================================
# OUTPUT COLUMNS
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

STATUS_COLUMNS = [
    "run_id",
    "run_time_utc",
    "version",
    "market",
    "timeframe",
    "status",
    "rows_before",
    "rows_after",
    "new_rows",
    "latest_before_utc",
    "latest_after_utc",
    "message",
]


# ============================================================
# SESSION
# ============================================================

SESSION = requests.Session()

SESSION.headers.update(
    {
        "Accept": "application/json",
        "User-Agent": (
            "upbit-surge-monitor-"
            "collector-clean-v003"
        ),
    }
)


# ============================================================
# BASIC UTILITY
# ============================================================

def print_line(
    char: str = "=",
    length: int = 72,
) -> None:
    print(char * length)


def utc_now_iso() -> str:
    return datetime.now(
        timezone.utc
    ).isoformat()


def ensure_directories() -> None:
    directories = [
        DATA_DIR,
        OHLCV_DIR,
        H1_DIR,
        H4_DIR,
        D1_DIR,
        COLLECTOR_STATE_DIR,
    ]

    for directory in directories:
        directory.mkdir(
            parents=True,
            exist_ok=True,
        )


def safe_int(
    value: Any,
    default: int = 0,
) -> int:
    try:
        return int(value)

    except (
        TypeError,
        ValueError,
    ):
        return default


def timestamp_to_iso(
    value: Any,
) -> str:
    if value is None:
        return ""

    try:
        parsed = pd.Timestamp(value)

        if pd.isna(parsed):
            return ""

        if parsed.tzinfo is None:
            parsed = parsed.tz_localize("UTC")
        else:
            parsed = parsed.tz_convert("UTC")

        return parsed.isoformat()

    except Exception:
        return ""


def atomic_write_json(
    data: dict[str, Any],
    file_path: Path,
) -> None:
    file_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temp_path = file_path.with_suffix(
        file_path.suffix + ".tmp"
    )

    try:
        with temp_path.open(
            "w",
            encoding="utf-8",
        ) as file:
            json.dump(
                data,
                file,
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )

            file.flush()

            try:
                os.fsync(
                    file.fileno()
                )
            except OSError:
                pass

        if not temp_path.exists():
            raise RuntimeError(
                "Checkpoint temporary file "
                "was not created."
            )

        if temp_path.stat().st_size == 0:
            raise RuntimeError(
                "Checkpoint temporary file "
                "is empty."
            )

        temp_path.replace(
            file_path
        )

    except Exception:
        if temp_path.exists():
            try:
                temp_path.unlink()
            except OSError:
                pass

        raise


# ============================================================
# CANONICAL TIMESTAMP
# ============================================================

def normalize_candle_time(
    series: pd.Series,
) -> pd.Series:
    """
    candle_date_time_utc를
    UTC datetime으로 변환한다.
    """

    return pd.to_datetime(
        series,
        utc=True,
        errors="coerce",
    )


def build_canonical_timestamp(
    candle_time: pd.Series,
) -> pd.Series:
    """
    UTC candle 시작 시각을
    Unix epoch milliseconds로 변환한다.

    API timestamp는 사용하지 않는다.
    """

    parsed = normalize_candle_time(
        candle_time
    )

    result = pd.Series(
        pd.NA,
        index=parsed.index,
        dtype="Int64",
    )

    valid = parsed.notna()

    if valid.any():
        result.loc[valid] = (
            parsed.loc[valid]
            .astype("int64")
            // 1_000_000
        ).astype("int64")

    return result


def normalize_ohlcv_dataframe(
    df: pd.DataFrame,
    *,
    require_market: str | None = None,
) -> pd.DataFrame:
    """
    프로젝트 OHLCV 공통 정규화.

    핵심:
        timestamp는 기존 값이나
        Upbit API timestamp를 신뢰하지 않는다.

        candle_date_time_utc에서
        항상 다시 생성한다.
    """

    if df.empty:
        return pd.DataFrame(
            columns=OHLCV_COLUMNS
        )

    work = df.copy()

    for column in OHLCV_COLUMNS:
        if column not in work.columns:
            work[column] = pd.NA

    work = work[
        OHLCV_COLUMNS
    ].copy()

    work[
        "candle_date_time_utc"
    ] = normalize_candle_time(
        work[
            "candle_date_time_utc"
        ]
    )

    invalid_time_count = int(
        work[
            "candle_date_time_utc"
        ].isna().sum()
    )

    if invalid_time_count > 0:
        raise RuntimeError(
            "Invalid candle_date_time_utc "
            f"found: {invalid_time_count:,}"
        )

    if require_market is not None:
        market_values = (
            work["market"]
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
                "Unexpected market value "
                "found in OHLCV CSV."
            )

        work["market"] = (
            require_market
        )

    # --------------------------------------------------------
    # Canonical timestamp
    # --------------------------------------------------------

    work["timestamp"] = (
        build_canonical_timestamp(
            work[
                "candle_date_time_utc"
            ]
        )
    )

    if work[
        "timestamp"
    ].isna().any():
        raise RuntimeError(
            "Canonical timestamp "
            "generation failed."
        )

    numeric_columns = [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "trade_value",
    ]

    for column in numeric_columns:
        work[column] = pd.to_numeric(
            work[column],
            errors="coerce",
        )

    # --------------------------------------------------------
    # Duplicate candle protection
    # --------------------------------------------------------

    work = work.drop_duplicates(
        subset=[
            "candle_date_time_utc"
        ],
        keep="last",
    )

    work = work.sort_values(
        "candle_date_time_utc"
    )

    work = work.reset_index(
        drop=True
    )

    if not work[
        "candle_date_time_utc"
    ].is_monotonic_increasing:
        raise RuntimeError(
            "candle_date_time_utc "
            "is not chronological."
        )

    timestamp_numeric = pd.to_numeric(
        work["timestamp"],
        errors="coerce",
    )

    if timestamp_numeric.isna().any():
        raise RuntimeError(
            "timestamp contains invalid "
            "values after normalization."
        )

    if not (
        timestamp_numeric
        .is_monotonic_increasing
    ):
        raise RuntimeError(
            "Canonical timestamp "
            "is not chronological."
        )

    return work[
        OHLCV_COLUMNS
    ]


# ============================================================
# HTTP
# ============================================================

def request_json(
    url: str,
    params: dict[str, Any] | None = None,
) -> Any:

    last_error: Exception | None = None

    for attempt in range(
        1,
        MAX_RETRIES + 1,
    ):
        try:
            response = SESSION.get(
                url,
                params=params,
                timeout=REQUEST_TIMEOUT,
            )

            if (
                response.status_code
                == 429
            ):
                wait_seconds = min(
                    float(attempt),
                    5.0,
                )

                print(
                    "[WARN] HTTP 429. "
                    f"retry={attempt}/"
                    f"{MAX_RETRIES}"
                )

                time.sleep(
                    wait_seconds
                )

                continue

            if (
                500
                <= response.status_code
                <= 599
            ):
                wait_seconds = min(
                    float(attempt),
                    5.0,
                )

                print(
                    "[WARN] Upbit server "
                    f"error "
                    f"{response.status_code}. "
                    f"retry={attempt}/"
                    f"{MAX_RETRIES}"
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

            if (
                attempt
                >= MAX_RETRIES
            ):
                break

            wait_seconds = min(
                float(attempt),
                5.0,
            )

            print(
                "[WARN] Request failed: "
                f"{exc} "
                f"retry={attempt}/"
                f"{MAX_RETRIES}"
            )

            time.sleep(
                wait_seconds
            )

    if last_error is None:
        raise RuntimeError(
            "Unknown API request failure."
        )

    raise RuntimeError(
        "Upbit API request failed after "
        f"{MAX_RETRIES} attempts: "
        f"{last_error}"
    )


# ============================================================
# MARKET LIST
# ============================================================

def get_krw_markets() -> list[str]:

    url = (
        f"{UPBIT_API_BASE}"
        "/market/all"
    )

    data = request_json(
        url,
        params={
            "is_details": "false",
        },
    )

    if not isinstance(
        data,
        list,
    ):
        raise RuntimeError(
            "Unexpected market "
            "API response."
        )

    markets: list[str] = []

    for item in data:
        if not isinstance(
            item,
            dict,
        ):
            continue

        market = str(
            item.get(
                "market",
                "",
            )
        ).strip().upper()

        if market.startswith(
            "KRW-"
        ):
            markets.append(
                market
            )

    markets = sorted(
        set(markets)
    )

    if not markets:
        raise RuntimeError(
            "No KRW markets were "
            "returned from Upbit."
        )

    return markets


# ============================================================
# CANDLE CONVERSION
# ============================================================

def candle_records_to_dataframe(
    market: str,
    records: list[
        dict[str, Any]
    ],
) -> pd.DataFrame:

    rows: list[
        dict[str, Any]
    ] = []

    for item in records:
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

                # API timestamp는
                # 사용하지 않는다.
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

    return (
        normalize_ohlcv_dataframe(
            df,
            require_market=market,
        )
    )


# ============================================================
# CANDLE API
# ============================================================

def fetch_candle_batch(
    market: str,
    timeframe: str,
    count: int,
    to: str | None = None,
) -> pd.DataFrame:

    config = TIMEFRAMES[
        timeframe
    ]

    params: dict[
        str,
        Any,
    ] = {
        "market": market,
        "count": min(
            count,
            API_MAX_COUNT,
        ),
    }

    if to:
        params["to"] = to

    data = request_json(
        config["url"],
        params=params,
    )

    if not isinstance(
        data,
        list,
    ):
        raise RuntimeError(
            "Unexpected candle "
            f"response: "
            f"{market} "
            f"{timeframe}"
        )

    return (
        candle_records_to_dataframe(
            market=market,
            records=data,
        )
    )


# ============================================================
# INITIAL HISTORY
# ============================================================

def fetch_initial_history(
    market: str,
    timeframe: str,
    target_count: int,
) -> pd.DataFrame:

    frames: list[
        pd.DataFrame
    ] = []

    collected = 0

    to_value: (
        str
        | None
    ) = None

    while (
        collected
        < target_count
    ):
        remaining = (
            target_count
            - collected
        )

        request_count = min(
            API_MAX_COUNT,
            remaining,
        )

        batch = (
            fetch_candle_batch(
                market=market,
                timeframe=timeframe,
                count=request_count,
                to=to_value,
            )
        )

        if batch.empty:
            break

        frames.append(
            batch
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

        collected = len(
            combined
        )

        oldest_time = batch[
            "candle_date_time_utc"
        ].min()

        if pd.isna(
            oldest_time
        ):
            break

        next_to = (
            oldest_time
            - pd.Timedelta(
                seconds=1
            )
        )

        to_value = (
            next_to.strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
        )

        if (
            len(batch)
            < request_count
        ):
            break

        time.sleep(
            REQUEST_SLEEP_SECONDS
        )

    if not frames:
        return pd.DataFrame(
            columns=OHLCV_COLUMNS
        )

    result = pd.concat(
        frames,
        ignore_index=True,
    )

    result = (
        normalize_ohlcv_dataframe(
            result,
            require_market=market,
        )
    )

    if (
        len(result)
        > target_count
    ):
        result = (
            result.tail(
                target_count
            )
            .reset_index(
                drop=True
            )
        )

    return result


# ============================================================
# EXISTING CSV
# ============================================================

def load_existing_csv(
    file_path: Path,
    market: str,
) -> pd.DataFrame:

    if not file_path.exists():
        return pd.DataFrame(
            columns=OHLCV_COLUMNS
        )

    if (
        file_path.stat().st_size
        == 0
    ):
        return pd.DataFrame(
            columns=OHLCV_COLUMNS
        )

    try:
        df = pd.read_csv(
            file_path,
            low_memory=False,
        )

    except pd.errors.EmptyDataError:
        return pd.DataFrame(
            columns=OHLCV_COLUMNS
        )

    except Exception as exc:
        raise RuntimeError(
            "Existing CSV read failed: "
            f"{file_path} | {exc}"
        ) from exc

    if df.empty:
        return pd.DataFrame(
            columns=OHLCV_COLUMNS
        )

    missing_columns = [
        column
        for column
        in OHLCV_COLUMNS
        if column
        not in df.columns
    ]

    if missing_columns:
        raise RuntimeError(
            "Existing CSV missing "
            "columns: "
            + ", ".join(
                missing_columns
            )
        )

    rows_before = len(
        df
    )

    normalized = (
        normalize_ohlcv_dataframe(
            df,
            require_market=market,
        )
    )

    # 기존 데이터에서 중복 제거가 발생했다면
    # Collector가 조용히 행을 삭제하면 안 된다.
    if (
        len(normalized)
        != rows_before
    ):
        raise RuntimeError(
            "Existing CSV row count "
            "changed during normalization. "
            f"{rows_before:,} -> "
            f"{len(normalized):,}"
        )

    return normalized


# ============================================================
# LATEST UPDATE
# ============================================================

def fetch_latest_update(
    market: str,
    timeframe: str,
    existing_df: pd.DataFrame,
) -> pd.DataFrame:

    if existing_df.empty:
        return fetch_initial_history(
            market=market,
            timeframe=timeframe,
            target_count=(
                INITIAL_CANDLE_LIMITS[
                    timeframe
                ]
            ),
        )

    latest_time = existing_df[
        "candle_date_time_utc"
    ].max()

    if pd.isna(
        latest_time
    ):
        raise RuntimeError(
            "Existing OHLCV has no "
            "valid latest candle time."
        )

    now = pd.Timestamp.now(
        tz="UTC"
    )

    interval_seconds = safe_int(
        TIMEFRAMES[
            timeframe
        ]["interval_seconds"],
        3600,
    )

    elapsed_seconds = max(
        0.0,
        (
            now
            - latest_time
        ).total_seconds(),
    )

    missing_estimate = int(
        elapsed_seconds
        // interval_seconds
    )

    # 최소 20개를 다시 읽어서
    # 경계 candle / 최근 candle을
    # 안전하게 겹쳐 받는다.
    target_count = max(
        20,
        missing_estimate + 10,
    )

    # --------------------------------------------------------
    # 200개 이하
    # --------------------------------------------------------

    if (
        target_count
        <= API_MAX_COUNT
    ):
        return fetch_candle_batch(
            market=market,
            timeframe=timeframe,
            count=target_count,
        )

    # --------------------------------------------------------
    # 200개 초과
    # --------------------------------------------------------

    frames: list[
        pd.DataFrame
    ] = []

    collected = 0

    to_value: (
        str
        | None
    ) = None

    stop_time = (
        latest_time
        - pd.Timedelta(
            seconds=(
                interval_seconds
                * 5
            )
        )
    )

    while True:
        batch = (
            fetch_candle_batch(
                market=market,
                timeframe=timeframe,
                count=API_MAX_COUNT,
                to=to_value,
            )
        )

        if batch.empty:
            break

        frames.append(
            batch
        )

        collected += len(
            batch
        )

        oldest_time = batch[
            "candle_date_time_utc"
        ].min()

        if pd.isna(
            oldest_time
        ):
            break

        if (
            oldest_time
            <= stop_time
        ):
            break

        next_to = (
            oldest_time
            - pd.Timedelta(
                seconds=1
            )
        )

        to_value = (
            next_to.strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
        )

        if (
            len(batch)
            < API_MAX_COUNT
        ):
            break

        if (
            collected
            >= UPDATE_SAFETY_MAX_CANDLES
        ):
            raise RuntimeError(
                "Update safety limit "
                "reached before existing "
                "history boundary. "
                f"{market} {timeframe} "
                f"collected={collected:,}"
            )

        time.sleep(
            REQUEST_SLEEP_SECONDS
        )

    if not frames:
        return pd.DataFrame(
            columns=OHLCV_COLUMNS
        )

    result = pd.concat(
        frames,
        ignore_index=True,
    )

    return (
        normalize_ohlcv_dataframe(
            result,
            require_market=market,
        )
    )


# ============================================================
# MERGE
# ============================================================

def merge_ohlcv(
    existing_df: pd.DataFrame,
    new_df: pd.DataFrame,
    market: str,
) -> pd.DataFrame:

    if (
        existing_df.empty
        and new_df.empty
    ):
        return pd.DataFrame(
            columns=OHLCV_COLUMNS
        )

    if existing_df.empty:
        combined = (
            new_df.copy()
        )

    elif new_df.empty:
        combined = (
            existing_df.copy()
        )

    else:
        combined = pd.concat(
            [
                existing_df,
                new_df,
            ],
            ignore_index=True,
        )

    merged = (
        normalize_ohlcv_dataframe(
            combined,
            require_market=market,
        )
    )

    # 기존 행보다 줄어드는 것은
    # 절대 허용하지 않는다.
    if (
        not existing_df.empty
        and len(merged)
        < len(existing_df)
    ):
        raise RuntimeError(
            "Merged OHLCV became "
            "smaller than existing data. "
            f"{len(existing_df):,} -> "
            f"{len(merged):,}"
        )

    return merged


# ============================================================
# SAFE CSV SAVE
# ============================================================

def save_dataframe_safely(
    df: pd.DataFrame,
    file_path: Path,
    market: str,
) -> None:

    if df.empty:
        raise RuntimeError(
            "Refusing to save empty "
            "OHLCV dataframe."
        )

    output_df = (
        normalize_ohlcv_dataframe(
            df,
            require_market=market,
        )
    )

    if output_df.empty:
        raise RuntimeError(
            "Normalized OHLCV became "
            "empty before save."
        )

    file_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temp_path = (
        file_path.with_suffix(
            file_path.suffix
            + ".tmp"
        )
    )

    output_df = (
        output_df.copy()
    )

    output_df[
        "candle_date_time_utc"
    ] = output_df[
        "candle_date_time_utc"
    ].apply(
        lambda value: (
            value.isoformat()
            if pd.notna(value)
            else ""
        )
    )

    try:
        output_df.to_csv(
            temp_path,
            index=False,
            encoding="utf-8-sig",
        )

        if not temp_path.exists():
            raise RuntimeError(
                "Temporary CSV "
                "was not created."
            )

        if (
            temp_path.stat().st_size
            == 0
        ):
            raise RuntimeError(
                "Temporary CSV is empty."
            )

        # 저장된 임시 CSV를 다시 읽어서
        # 최소 구조 검증 후 교체한다.
        verify_df = pd.read_csv(
            temp_path,
            low_memory=False,
        )

        if (
            len(verify_df)
            != len(output_df)
        ):
            raise RuntimeError(
                "Temporary CSV row count "
                "verification failed. "
                f"{len(output_df):,} -> "
                f"{len(verify_df):,}"
            )

        missing_columns = [
            column
            for column
            in OHLCV_COLUMNS
            if column
            not in verify_df.columns
        ]

        if missing_columns:
            raise RuntimeError(
                "Temporary CSV missing "
                "columns: "
                + ", ".join(
                    missing_columns
                )
            )

        temp_path.replace(
            file_path
        )

    except Exception:
        if temp_path.exists():
            try:
                temp_path.unlink()
            except OSError:
                pass

        raise


# ============================================================
# STATUS
# ============================================================

def append_status(
    run_id: str,
    market: str,
    timeframe: str,
    status: str,
    rows_before: int,
    rows_after: int,
    new_rows: int,
    latest_before_utc: str = "",
    latest_after_utc: str = "",
    message: str = "",
) -> None:

    row = pd.DataFrame(
        [
            {
                "run_id":
                    run_id,

                "run_time_utc":
                    utc_now_iso(),

                "version":
                    VERSION,

                "market":
                    market,

                "timeframe":
                    timeframe,

                "status":
                    status,

                "rows_before":
                    rows_before,

                "rows_after":
                    rows_after,

                "new_rows":
                    new_rows,

                "latest_before_utc":
                    latest_before_utc,

                "latest_after_utc":
                    latest_after_utc,

                "message":
                    message,
            }
        ],
        columns=STATUS_COLUMNS,
    )

    STATUS_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    # 기존 Clean V002 status 파일과
    # 컬럼 구조가 다를 수 있으므로
    # 기존 헤더를 검사한다.
    if (
        STATUS_FILE.exists()
        and STATUS_FILE.stat().st_size > 0
    ):
        try:
            existing_header = (
                pd.read_csv(
                    STATUS_FILE,
                    nrows=0,
                )
                .columns
                .tolist()
            )

        except Exception:
            existing_header = []

        if (
            existing_header
            == STATUS_COLUMNS
        ):
            row.to_csv(
                STATUS_FILE,
                mode="a",
                header=False,
                index=False,
                encoding="utf-8-sig",
            )

            return

        # V002 -> V003 status schema 변경.
        # 기존 status를 삭제하지 않고
        # 별도 backup으로 보존한다.
        backup_path = (
            STATUS_FILE.with_name(
                "collector_status_clean_v002.csv"
            )
        )

        if not backup_path.exists():
            STATUS_FILE.replace(
                backup_path
            )

        else:
            # 이미 backup이 있다면
            # 기존 status를 덮어쓰지 않고
            # 새 V003 status 이름으로 시작한다.
            STATUS_FILE.unlink()

    row.to_csv(
        STATUS_FILE,
        mode="w",
        header=True,
        index=False,
        encoding="utf-8-sig",
    )


# ============================================================
# CHECKPOINT
# ============================================================

def new_run_id() -> str:
    timestamp = datetime.now(
        timezone.utc
    ).strftime(
        "%Y%m%dT%H%M%SZ"
    )

    random_part = (
        uuid.uuid4()
        .hex[:8]
    )

    return (
        f"{timestamp}-"
        f"{random_part}"
    )


def job_key(
    market: str,
    timeframe: str,
) -> str:
    return (
        f"{market}|"
        f"{timeframe}"
    )


def create_checkpoint(
    markets: list[str],
) -> dict[str, Any]:

    run_id = new_run_id()

    return {
        "project":
            PROJECT_NAME,

        "version":
            VERSION,

        "run_id":
            run_id,

        "run_status":
            "RUNNING",

        "started_utc":
            utc_now_iso(),

        "updated_utc":
            utc_now_iso(),

        "completed_utc":
            None,

        "market_count":
            len(markets),

        "timeframes":
            [
                "h1",
                "h4",
                "d1",
            ],

        "total_jobs":
            (
                len(markets)
                * 3
            ),

        "completed_jobs":
            {},

        "failed_jobs":
            {},
    }


def load_checkpoint() -> (
    dict[str, Any]
    | None
):

    if not CHECKPOINT_FILE.exists():
        return None

    if (
        CHECKPOINT_FILE.stat().st_size
        == 0
    ):
        return None

    try:
        with CHECKPOINT_FILE.open(
            "r",
            encoding="utf-8",
        ) as file:
            data = json.load(
                file
            )

    except Exception as exc:
        raise RuntimeError(
            "Collector checkpoint "
            f"read failed: {exc}"
        ) from exc

    if not isinstance(
        data,
        dict,
    ):
        raise RuntimeError(
            "Collector checkpoint "
            "root must be an object."
        )

    return data


def save_checkpoint(
    checkpoint: dict[str, Any],
) -> None:

    checkpoint[
        "updated_utc"
    ] = utc_now_iso()

    atomic_write_json(
        checkpoint,
        CHECKPOINT_FILE,
    )


def checkpoint_is_compatible(
    checkpoint: dict[str, Any],
    markets: list[str],
) -> bool:

    if (
        checkpoint.get(
            "project"
        )
        != PROJECT_NAME
    ):
        return False

    if (
        checkpoint.get(
            "version"
        )
        != VERSION
    ):
        return False

    if (
        checkpoint.get(
            "run_status"
        )
        != "RUNNING"
    ):
        return False

    expected_jobs = (
        len(markets)
        * 3
    )

    if safe_int(
        checkpoint.get(
            "total_jobs"
        ),
        -1,
    ) != expected_jobs:
        return False

    if safe_int(
        checkpoint.get(
            "market_count"
        ),
        -1,
    ) != len(markets):
        return False

    completed_jobs = (
        checkpoint.get(
            "completed_jobs"
        )
    )

    failed_jobs = (
        checkpoint.get(
            "failed_jobs"
        )
    )

    if not isinstance(
        completed_jobs,
        dict,
    ):
        return False

    if not isinstance(
        failed_jobs,
        dict,
    ):
        return False

    return True


def prepare_checkpoint(
    markets: list[str],
) -> tuple[
    dict[str, Any],
    bool,
]:

    existing = load_checkpoint()

    if (
        existing is not None
        and checkpoint_is_compatible(
            existing,
            markets,
        )
    ):
        return (
            existing,
            True,
        )

    checkpoint = (
        create_checkpoint(
            markets
        )
    )

    save_checkpoint(
        checkpoint
    )

    return (
        checkpoint,
        False,
    )


def mark_job_success(
    checkpoint: dict[str, Any],
    market: str,
    timeframe: str,
    rows_before: int,
    rows_after: int,
    new_rows: int,
    latest_after_utc: str,
) -> None:

    key = job_key(
        market,
        timeframe,
    )

    completed_jobs = (
        checkpoint.setdefault(
            "completed_jobs",
            {},
        )
    )

    failed_jobs = (
        checkpoint.setdefault(
            "failed_jobs",
            {},
        )
    )

    completed_jobs[key] = {
        "market":
            market,

        "timeframe":
            timeframe,

        "completed_utc":
            utc_now_iso(),

        "rows_before":
            rows_before,

        "rows_after":
            rows_after,

        "new_rows":
            new_rows,

        "latest_after_utc":
            latest_after_utc,
    }

    failed_jobs.pop(
        key,
        None,
    )

    save_checkpoint(
        checkpoint
    )


def mark_job_failed(
    checkpoint: dict[str, Any],
    market: str,
    timeframe: str,
    message: str,
) -> None:

    key = job_key(
        market,
        timeframe,
    )

    failed_jobs = (
        checkpoint.setdefault(
            "failed_jobs",
            {},
        )
    )

    failed_jobs[key] = {
        "market":
            market,

        "timeframe":
            timeframe,

        "failed_utc":
            utc_now_iso(),

        "message":
            message,
    }

    save_checkpoint(
        checkpoint
    )


def checkpoint_job_completed(
    checkpoint: dict[str, Any],
    market: str,
    timeframe: str,
) -> bool:

    completed_jobs = (
        checkpoint.get(
            "completed_jobs",
            {}
        )
    )

    if not isinstance(
        completed_jobs,
        dict,
    ):
        return False

    return (
        job_key(
            market,
            timeframe,
        )
        in completed_jobs
    )


def finalize_checkpoint(
    checkpoint: dict[str, Any],
    *,
    success: bool,
) -> None:

    checkpoint[
        "run_status"
    ] = (
        "COMPLETED"
        if success
        else "FAILED"
    )

    checkpoint[
        "completed_utc"
    ] = utc_now_iso()

    save_checkpoint(
        checkpoint
    )


# ============================================================
# MARKET + TIMEFRAME
# ============================================================

def collect_market_timeframe(
    run_id: str,
    market: str,
    timeframe: str,
) -> tuple[
    bool,
    int,
    int,
    int,
    str,
]:

    directory: Path = (
        TIMEFRAMES[
            timeframe
        ]["directory"]
    )

    file_path = (
        directory
        / f"{market}.csv"
    )

    rows_before = 0
    rows_after = 0

    latest_before_utc = ""
    latest_after_utc = ""

    try:
        existing_df = (
            load_existing_csv(
                file_path,
                market,
            )
        )

        rows_before = len(
            existing_df
        )

        if not existing_df.empty:
            latest_before = (
                existing_df[
                    "candle_date_time_utc"
                ].max()
            )

            latest_before_utc = (
                timestamp_to_iso(
                    latest_before
                )
            )

        if existing_df.empty:
            print(
                f"    [{timeframe}] "
                "initial history..."
            )

            new_df = (
                fetch_initial_history(
                    market=market,
                    timeframe=timeframe,
                    target_count=(
                        INITIAL_CANDLE_LIMITS[
                            timeframe
                        ]
                    ),
                )
            )

        else:
            print(
                f"    [{timeframe}] "
                "latest update..."
            )

            new_df = (
                fetch_latest_update(
                    market=market,
                    timeframe=timeframe,
                    existing_df=(
                        existing_df
                    ),
                )
            )

        merged_df = merge_ohlcv(
            existing_df=existing_df,
            new_df=new_df,
            market=market,
        )

        rows_after = len(
            merged_df
        )

        added_rows = max(
            0,
            (
                rows_after
                - rows_before
            ),
        )

        if merged_df.empty:
            raise RuntimeError(
                "No OHLCV data returned."
            )

        if (
            rows_after
            < rows_before
        ):
            raise RuntimeError(
                "OHLCV row count decreased. "
                f"{rows_before:,} -> "
                f"{rows_after:,}"
            )

        latest_after = (
            merged_df[
                "candle_date_time_utc"
            ].max()
        )

        latest_after_utc = (
            timestamp_to_iso(
                latest_after
            )
        )

        save_dataframe_safely(
            merged_df,
            file_path,
            market,
        )

        # 저장된 실제 파일을 다시 읽어서
        # 최종 row count를 검증한다.
        saved_df = (
            load_existing_csv(
                file_path,
                market,
            )
        )

        if (
            len(saved_df)
            != rows_after
        ):
            raise RuntimeError(
                "Saved OHLCV row count "
                "verification failed. "
                f"expected={rows_after:,} "
                f"actual={len(saved_df):,}"
            )

        saved_latest = (
            saved_df[
                "candle_date_time_utc"
            ].max()
        )

        saved_latest_utc = (
            timestamp_to_iso(
                saved_latest
            )
        )

        if (
            saved_latest_utc
            != latest_after_utc
        ):
            raise RuntimeError(
                "Saved OHLCV latest candle "
                "verification failed."
            )

        append_status(
            run_id=run_id,
            market=market,
            timeframe=timeframe,
            status="SUCCESS",
            rows_before=rows_before,
            rows_after=rows_after,
            new_rows=added_rows,
            latest_before_utc=(
                latest_before_utc
            ),
            latest_after_utc=(
                latest_after_utc
            ),
            message="",
        )

        print(
            f"    [{timeframe}] OK "
            f"{rows_before:,} -> "
            f"{rows_after:,} "
            f"(+{added_rows:,})"
        )

        print(
            f"    [{timeframe}] "
            f"latest: "
            f"{latest_after_utc}"
        )

        return (
            True,
            added_rows,
            rows_before,
            rows_after,
            latest_after_utc,
        )

    except Exception as exc:
        message = str(
            exc
        )

        append_status(
            run_id=run_id,
            market=market,
            timeframe=timeframe,
            status="FAILED",
            rows_before=rows_before,
            rows_after=rows_before,
            new_rows=0,
            latest_before_utc=(
                latest_before_utc
            ),
            latest_after_utc="",
            message=message,
        )

        print(
            f"    [{timeframe}] "
            f"FAILED: {message}"
        )

        return (
            False,
            0,
            rows_before,
            rows_before,
            "",
        )


# ============================================================
# SUMMARY
# ============================================================

def show_final_summary(
    *,
    run_id: str,
    market_count: int,
    total_jobs: int,
    success_jobs: int,
    failed_jobs: int,
    resumed_jobs: int,
    executed_jobs: int,
    total_added_rows: int,
    elapsed_seconds: float,
) -> None:

    print()
    print_line()

    print(
        "COLLECTOR SUMMARY"
    )

    print_line()

    print(
        f"Project            : "
        f"{PROJECT_NAME}"
    )

    print(
        f"Version            : "
        f"{VERSION}"
    )

    print(
        f"Run ID             : "
        f"{run_id}"
    )

    print(
        f"KRW markets        : "
        f"{market_count:,}"
    )

    print(
        "Timeframes         : "
        "h1 / h4 / d1"
    )

    print(
        f"Total jobs         : "
        f"{total_jobs:,}"
    )

    print(
        f"Successful jobs    : "
        f"{success_jobs:,}"
    )

    print(
        f"Failed jobs        : "
        f"{failed_jobs:,}"
    )

    print(
        f"Resumed / skipped  : "
        f"{resumed_jobs:,}"
    )

    print(
        f"Executed this run  : "
        f"{executed_jobs:,}"
    )

    print(
        f"New OHLCV rows     : "
        f"{total_added_rows:,}"
    )

    print(
        f"Elapsed seconds    : "
        f"{elapsed_seconds:,.2f}"
    )

    print()

    print(
        "Timestamp policy:"
    )

    print(
        "  Source            : "
        "candle_date_time_utc"
    )

    print(
        "  Unit              : "
        "Unix epoch milliseconds"
    )

    print(
        "  API timestamp     : "
        "NOT USED"
    )

    print()

    print(
        "Resume:"
    )

    print(
        "  Checkpoint        : "
        f"{CHECKPOINT_FILE}"
    )

    print(
        "  Scope             : "
        "CURRENT ACTIVE RUN ONLY"
    )

    print(
        "  Next normal run   : "
        "NEW RUN / latest candles checked again"
    )

    print()

    print(
        "Safety:"
    )

    print(
        "  Existing OHLCV    : "
        "PRESERVED"
    )

    print(
        "  Row deletion      : "
        "DISABLED"
    )

    print(
        "  Feature generation: "
        "DISABLED"
    )

    print(
        "  Signal generation : "
        "DISABLED"
    )

    print(
        "  Prediction        : "
        "DISABLED"
    )

    print(
        "  Trading           : "
        "DISABLED"
    )

    print(
        "  Git reset         : "
        "DISABLED"
    )

    print(
        "  Git clean         : "
        "DISABLED"
    )

    print(
        "  Git commit        : "
        "DISABLED"
    )

    print(
        "  Git push          : "
        "DISABLED"
    )

    print()

    print(
        "Output directories:"
    )

    print(
        f"  H1     : {H1_DIR}"
    )

    print(
        f"  H4     : {H4_DIR}"
    )

    print(
        f"  D1     : {D1_DIR}"
    )

    print(
        f"  STATUS : {STATUS_FILE}"
    )

    print(
        f"  STATE  : "
        f"{COLLECTOR_STATE_DIR}"
    )

    print_line()


# ============================================================
# MAIN
# ============================================================

def main() -> int:

    start_time = time.time()

    print_line()

    print(
        f"{PROJECT_NAME} - "
        f"{VERSION}"
    )

    print_line()

    print(
        f"Started UTC : "
        f"{utc_now_iso()}"
    )

    print(
        f"Base dir    : "
        f"{BASE_DIR}"
    )

    print()

    print(
        "Mode:"
    )

    print(
        "  Incremental OHLCV collection"
    )

    print(
        "  Full KRW market"
    )

    print(
        "  h1 / h4 / d1"
    )

    print(
        "  Current-run Resume enabled"
    )

    print(
        "  Existing OHLCV preserved"
    )

    print(
        "  No Feature generation"
    )

    print(
        "  No signal generation"
    )

    print(
        "  No prediction"
    )

    print(
        "  No trading"
    )

    print()

    print(
        "Timestamp policy:"
    )

    print(
        "  candle_date_time_utc "
        "-> Unix epoch milliseconds"
    )

    print(
        "  Existing timestamp values "
        "are normalized on save."
    )

    print(
        "  Upbit API timestamp "
        "is NOT USED."
    )

    print()

    ensure_directories()

    # --------------------------------------------------------
    # 1. Market discovery
    # --------------------------------------------------------

    print(
        "[1/3] Loading Upbit "
        "KRW markets..."
    )

    try:
        markets = (
            get_krw_markets()
        )

    except Exception as exc:
        print()

        print(
            "[FATAL] Failed to load "
            f"KRW markets: {exc}"
        )

        return 1

    total_markets = len(
        markets
    )

    total_jobs = (
        total_markets
        * 3
    )

    print(
        "      KRW markets found: "
        f"{total_markets:,}"
    )

    print(
        "      Total jobs       : "
        f"{total_jobs:,}"
    )

    print()

    # --------------------------------------------------------
    # 2. Checkpoint
    # --------------------------------------------------------

    print(
        "[2/3] Preparing "
        "Collector checkpoint..."
    )

    try:
        (
            checkpoint,
            resumed,
        ) = prepare_checkpoint(
            markets
        )

    except Exception as exc:
        print(
            "[FATAL] Checkpoint "
            f"initialization failed: {exc}"
        )

        return 1

    run_id = str(
        checkpoint.get(
            "run_id",
            "",
        )
    )

    completed_jobs = (
        checkpoint.get(
            "completed_jobs",
            {}
        )
    )

    completed_count = (
        len(completed_jobs)
        if isinstance(
            completed_jobs,
            dict,
        )
        else 0
    )

    if resumed:
        print(
            "      Mode             : "
            "RESUME ACTIVE RUN"
        )

        print(
            "      Run ID           : "
            f"{run_id}"
        )

        print(
            "      Completed jobs   : "
            f"{completed_count:,}/"
            f"{total_jobs:,}"
        )

        print(
            "      Remaining jobs   : "
            f"{max(0, total_jobs - completed_count):,}"
        )

    else:
        print(
            "      Mode             : "
            "NEW RUN"
        )

        print(
            "      Run ID           : "
            f"{run_id}"
        )

        print(
            "      Completed jobs   : "
            f"0/{total_jobs:,}"
        )

    print()

    # --------------------------------------------------------
    # 3. Collection
    # --------------------------------------------------------

    print(
        "[3/3] Collecting OHLCV..."
    )

    print()

    success_jobs = 0
    failed_jobs = 0
    resumed_jobs = 0
    executed_jobs = 0
    total_added_rows = 0

    # 이미 완료된 checkpoint job은
    # 현재 실행의 success로 계산한다.
    if isinstance(
        completed_jobs,
        dict,
    ):
        success_jobs = len(
            completed_jobs
        )

        resumed_jobs = len(
            completed_jobs
        )

    job_number = 0

    try:
        for (
            market_index,
            market,
        ) in enumerate(
            markets,
            start=1,
        ):
            print(
                f"[MARKET "
                f"{market_index:03d}/"
                f"{total_markets:03d}] "
                f"{market}"
            )

            for timeframe in (
                "h1",
                "h4",
                "d1",
            ):
                job_number += 1

                key = job_key(
                    market,
                    timeframe,
                )

                print(
                    f"  [JOB "
                    f"{job_number:03d}/"
                    f"{total_jobs:03d}] "
                    f"{market} "
                    f"{timeframe}"
                )

                # ------------------------------------------------
                # Resume
                # ------------------------------------------------

                if (
                    checkpoint_job_completed(
                        checkpoint,
                        market,
                        timeframe,
                    )
                ):
                    print(
                        "    [RESUME] "
                        "Already completed "
                        "in current run. SKIP."
                    )

                    continue

                executed_jobs += 1

                (
                    success,
                    added_rows,
                    rows_before,
                    rows_after,
                    latest_after_utc,
                ) = collect_market_timeframe(
                    run_id=run_id,
                    market=market,
                    timeframe=timeframe,
                )

                if success:
                    success_jobs += 1

                    total_added_rows += (
                        added_rows
                    )

                    try:
                        mark_job_success(
                            checkpoint=checkpoint,
                            market=market,
                            timeframe=timeframe,
                            rows_before=rows_before,
                            rows_after=rows_after,
                            new_rows=added_rows,
                            latest_after_utc=(
                                latest_after_utc
                            ),
                        )

                    except Exception as exc:
                        raise RuntimeError(
                            "OHLCV was saved but "
                            "checkpoint update failed "
                            f"for {key}: {exc}"
                        ) from exc

                else:
                    failed_jobs += 1

                    try:
                        mark_job_failed(
                            checkpoint=checkpoint,
                            market=market,
                            timeframe=timeframe,
                            message=(
                                "Collector job failed. "
                                "See collector_status.csv."
                            ),
                        )

                    except Exception as exc:
                        raise RuntimeError(
                            "Failed job checkpoint "
                            f"write failed for "
                            f"{key}: {exc}"
                        ) from exc

                current_completed = (
                    checkpoint.get(
                        "completed_jobs",
                        {}
                    )
                )

                current_completed_count = (
                    len(current_completed)
                    if isinstance(
                        current_completed,
                        dict,
                    )
                    else 0
                )

                print(
                    "    Progress: "
                    f"{current_completed_count:,}/"
                    f"{total_jobs:,} "
                    "completed"
                )

                time.sleep(
                    REQUEST_SLEEP_SECONDS
                )

            time.sleep(
                MARKET_SLEEP_SECONDS
            )

    except KeyboardInterrupt:
        elapsed_seconds = (
            time.time()
            - start_time
        )

        print()
        print_line("!")

        print(
            "[INTERRUPTED] Collector "
            "stopped by user."
        )

        print(
            "[RESUME] Completed jobs "
            "are preserved in checkpoint."
        )

        print(
            "[RESUME] Run the same "
            "collector.py again."
        )

        print(
            f"[RESUME] Checkpoint: "
            f"{CHECKPOINT_FILE}"
        )

        print_line("!")

        show_final_summary(
            run_id=run_id,
            market_count=total_markets,
            total_jobs=total_jobs,
            success_jobs=success_jobs,
            failed_jobs=failed_jobs,
            resumed_jobs=resumed_jobs,
            executed_jobs=executed_jobs,
            total_added_rows=(
                total_added_rows
            ),
            elapsed_seconds=(
                elapsed_seconds
            ),
        )

        return 130

    except Exception as exc:
        elapsed_seconds = (
            time.time()
            - start_time
        )

        print()
        print_line("!")

        print(
            "[FATAL] Collector stopped "
            f"unexpectedly: {exc}"
        )

        print(
            "[RESUME] Existing completed "
            "jobs remain checkpointed."
        )

        print(
            "[RESUME] Fix the cause and "
            "run collector.py again."
        )

        print_line("!")

        show_final_summary(
            run_id=run_id,
            market_count=total_markets,
            total_jobs=total_jobs,
            success_jobs=success_jobs,
            failed_jobs=failed_jobs,
            resumed_jobs=resumed_jobs,
            executed_jobs=executed_jobs,
            total_added_rows=(
                total_added_rows
            ),
            elapsed_seconds=(
                elapsed_seconds
            ),
        )

        return 1

    # --------------------------------------------------------
    # Final checkpoint verification
    # --------------------------------------------------------

    completed_jobs_final = (
        checkpoint.get(
            "completed_jobs",
            {}
        )
    )

    completed_count_final = (
        len(completed_jobs_final)
        if isinstance(
            completed_jobs_final,
            dict,
        )
        else 0
    )

    failed_jobs_final = (
        checkpoint.get(
            "failed_jobs",
            {}
        )
    )

    failed_count_final = (
        len(failed_jobs_final)
        if isinstance(
            failed_jobs_final,
            dict,
        )
        else 0
    )

    elapsed_seconds = (
        time.time()
        - start_time
    )

    final_success = (
        completed_count_final
        == total_jobs
        and failed_count_final
        == 0
    )

    try:
        finalize_checkpoint(
            checkpoint,
            success=final_success,
        )

    except Exception as exc:
        print(
            "[FATAL] Final checkpoint "
            f"write failed: {exc}"
        )

        return 1

    show_final_summary(
        run_id=run_id,
        market_count=total_markets,
        total_jobs=total_jobs,
        success_jobs=(
            completed_count_final
        ),
        failed_jobs=(
            failed_count_final
        ),
        resumed_jobs=(
            resumed_jobs
        ),
        executed_jobs=(
            executed_jobs
        ),
        total_added_rows=(
            total_added_rows
        ),
        elapsed_seconds=(
            elapsed_seconds
        ),
    )

    if not final_success:
        print(
            "[RESULT] COLLECTOR FAILED"
        )

        print(
            "[FAIL] Completed jobs: "
            f"{completed_count_final:,}/"
            f"{total_jobs:,}"
        )

        print(
            "[FAIL] Failed jobs: "
            f"{failed_count_final:,}"
        )

        print(
            "[IMPORTANT] A failed completed "
            "run will start a NEW run on the "
            "next execution."
        )

        print(
            "[NEXT] Inspect "
            "collector_status.csv."
        )

        return 1

    print(
        "[RESULT] FULL KRW "
        "COLLECTOR PASSED"
    )

    print(
        f"[PASS] {total_markets:,} "
        "markets processed."
    )

    print(
        f"[PASS] {total_jobs:,} / "
        f"{total_jobs:,} "
        "collector jobs completed."
    )

    print(
        "[PASS] Existing OHLCV "
        "data preserved."
    )

    print(
        "[PASS] Canonical timestamp "
        "policy preserved."
    )

    print(
        "[PASS] Current run "
        "checkpoint completed."
    )

    print(
        "[NEXT] Full OHLCV "
        "freshness / gap validation."
    )

    return 0


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    sys.exit(
        main()
    )
