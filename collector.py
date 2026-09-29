"""
Upbit Surge Monitor - Collector Clean V002
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

Clean V002 change:
    timestamp 컬럼을 Upbit API의 timestamp 값에 의존하지 않는다.

    프로젝트의 canonical candle timestamp는
    candle_date_time_utc 로부터 직접 계산한다.

    timestamp =
        candle_date_time_utc Unix epoch milliseconds

Why:
    일부 과거 Upbit candle 응답에서 API timestamp가
    비어 있거나 candle 시작 시각과 일치하지 않을 수 있다.

    candle_date_time_utc는 실제 candle 시간축이므로
    프로젝트 전체에서 이것을 기준 시간으로 사용한다.

Safety:
    - 기존 OHLCV 데이터 삭제 금지
    - 기존 candle 보존
    - candle_date_time_utc 기준 중복 제거
    - candle_date_time_utc 기준 시간순 정렬
    - 기존 CSV를 읽을 때 timestamp 자동 정규화
    - 저장 전 timestamp 재정규화
    - 임시 파일 저장 후 원본 교체
    - 패턴 분석 없음
    - 예측 없음
    - 머신러닝 없음
    - 자동매매 없음

Windows:
    py collector.py
"""

from __future__ import annotations

import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests


# ============================================================
# PROJECT CONFIG
# ============================================================

PROJECT_NAME = "Upbit Surge Monitor"
VERSION = "Collector Clean V002"

BASE_DIR = Path(__file__).resolve().parent

DATA_DIR = BASE_DIR / "data"
OHLCV_DIR = DATA_DIR / "ohlcv"

H1_DIR = OHLCV_DIR / "h1"
H4_DIR = OHLCV_DIR / "h4"
D1_DIR = OHLCV_DIR / "d1"

STATUS_FILE = DATA_DIR / "collector_status.csv"

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


# ============================================================
# TIMEFRAME CONFIG
# ============================================================

TIMEFRAMES = {
    "h1": {
        "url": f"{UPBIT_API_BASE}/candles/minutes/60",
        "directory": H1_DIR,
        "interval_seconds": 60 * 60,
    },
    "h4": {
        "url": f"{UPBIT_API_BASE}/candles/minutes/240",
        "directory": H4_DIR,
        "interval_seconds": 4 * 60 * 60,
    },
    "d1": {
        "url": f"{UPBIT_API_BASE}/candles/days",
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
    "run_time_utc",
    "version",
    "market",
    "timeframe",
    "status",
    "rows_before",
    "rows_after",
    "new_rows",
    "message",
]


# ============================================================
# SESSION
# ============================================================

SESSION = requests.Session()

SESSION.headers.update(
    {
        "Accept": "application/json",
        "User-Agent": "upbit-surge-monitor-collector-clean-v002",
    }
)


# ============================================================
# UTILITY
# ============================================================

def print_line(char: str = "=", length: int = 72) -> None:
    print(char * length)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_directories() -> None:
    directories = [
        DATA_DIR,
        OHLCV_DIR,
        H1_DIR,
        H4_DIR,
        D1_DIR,
    ]

    for directory in directories:
        directory.mkdir(
            parents=True,
            exist_ok=True,
        )


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


# ============================================================
# CANONICAL TIMESTAMP
# ============================================================

def normalize_candle_time(
    series: pd.Series,
) -> pd.Series:
    """
    candle_date_time_utc를 UTC datetime으로 변환한다.
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
    UTC candle 시작 시각을 Unix epoch milliseconds로 변환한다.

    pandas datetime64[ns, UTC]
        -> nanoseconds
        -> milliseconds

    반환 dtype:
        Int64

    NaT는 <NA>가 된다.
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
            parsed.loc[valid].astype("int64")
            // 1_000_000
        ).astype("int64")

    return result


def normalize_ohlcv_dataframe(
    df: pd.DataFrame,
    *,
    require_market: str | None = None,
) -> pd.DataFrame:
    """
    프로젝트 OHLCV 데이터의 공통 정규화 함수.

    핵심:
        timestamp는 기존 값/API 값을 신뢰하지 않고
        candle_date_time_utc에서 항상 재생성한다.
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

    work["candle_date_time_utc"] = (
        normalize_candle_time(
            work["candle_date_time_utc"]
        )
    )

    invalid_time_count = int(
        work[
            "candle_date_time_utc"
        ].isna().sum()
    )

    if invalid_time_count > 0:
        raise RuntimeError(
            "Invalid candle_date_time_utc found: "
            f"{invalid_time_count:,}"
        )

    if require_market is not None:
        market_values = (
            work["market"]
            .astype(str)
            .str.strip()
            .str.upper()
        )

        invalid_market = (
            market_values != require_market
        )

        if invalid_market.any():
            raise RuntimeError(
                "Unexpected market value found in OHLCV CSV."
            )

        work["market"] = require_market

    # --------------------------------------------------------
    # Clean V002 핵심
    # --------------------------------------------------------

    work["timestamp"] = (
        build_canonical_timestamp(
            work["candle_date_time_utc"]
        )
    )

    if work["timestamp"].isna().any():
        raise RuntimeError(
            "Canonical timestamp generation failed."
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

    duplicate_count = int(
        work[
            "candle_date_time_utc"
        ].duplicated(
            keep=False
        ).sum()
    )

    if duplicate_count > 0:
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
            "candle_date_time_utc is not chronological."
        )

    timestamp_numeric = pd.to_numeric(
        work["timestamp"],
        errors="coerce",
    )

    if timestamp_numeric.isna().any():
        raise RuntimeError(
            "timestamp contains invalid values "
            "after normalization."
        )

    if not timestamp_numeric.is_monotonic_increasing:
        raise RuntimeError(
            "Canonical timestamp is not chronological."
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

            if response.status_code == 429:
                wait_seconds = min(
                    float(attempt),
                    5.0,
                )

                print(
                    f"[WARN] HTTP 429. "
                    f"retry={attempt}/{MAX_RETRIES}"
                )

                time.sleep(wait_seconds)
                continue

            if 500 <= response.status_code <= 599:
                wait_seconds = min(
                    float(attempt),
                    5.0,
                )

                print(
                    f"[WARN] Upbit server error "
                    f"{response.status_code}. "
                    f"retry={attempt}/{MAX_RETRIES}"
                )

                time.sleep(wait_seconds)
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

            print(
                f"[WARN] Request failed: {exc} "
                f"retry={attempt}/{MAX_RETRIES}"
            )

            time.sleep(wait_seconds)

    if last_error is None:
        raise RuntimeError(
            "Unknown API request failure."
        )

    raise RuntimeError(
        "Upbit API request failed after "
        f"{MAX_RETRIES} attempts: {last_error}"
    )


# ============================================================
# MARKET LIST
# ============================================================

def get_krw_markets() -> list[str]:

    url = f"{UPBIT_API_BASE}/market/all"

    data = request_json(
        url,
        params={
            "is_details": "false",
        },
    )

    if not isinstance(data, list):
        raise RuntimeError(
            "Unexpected market API response."
        )

    markets: list[str] = []

    for item in data:

        if not isinstance(item, dict):
            continue

        market = str(
            item.get(
                "market",
                "",
            )
        ).strip().upper()

        if market.startswith("KRW-"):
            markets.append(market)

    markets = sorted(
        set(markets)
    )

    if not markets:
        raise RuntimeError(
            "No KRW markets were returned from Upbit."
        )

    return markets


# ============================================================
# CANDLE CONVERSION
# ============================================================

def candle_records_to_dataframe(
    market: str,
    records: list[dict[str, Any]],
) -> pd.DataFrame:

    rows: list[dict[str, Any]] = []

    for item in records:

        rows.append(
            {
                "market": market,
                "candle_date_time_utc": item.get(
                    "candle_date_time_utc"
                ),
                "candle_date_time_kst": item.get(
                    "candle_date_time_kst"
                ),

                # Clean V002:
                # API timestamp는 사용하지 않는다.
                "timestamp": pd.NA,

                "open": item.get(
                    "opening_price"
                ),
                "high": item.get(
                    "high_price"
                ),
                "low": item.get(
                    "low_price"
                ),
                "close": item.get(
                    "trade_price"
                ),
                "volume": item.get(
                    "candle_acc_trade_volume"
                ),
                "trade_value": item.get(
                    "candle_acc_trade_price"
                ),
            }
        )

    if not rows:
        return pd.DataFrame(
            columns=OHLCV_COLUMNS
        )

    df = pd.DataFrame(rows)

    return normalize_ohlcv_dataframe(
        df,
        require_market=market,
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

    params: dict[str, Any] = {
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

    if not isinstance(data, list):
        raise RuntimeError(
            f"Unexpected candle response: "
            f"{market} {timeframe}"
        )

    return candle_records_to_dataframe(
        market=market,
        records=data,
    )


# ============================================================
# INITIAL HISTORY
# ============================================================

def fetch_initial_history(
    market: str,
    timeframe: str,
    target_count: int,
) -> pd.DataFrame:

    frames: list[pd.DataFrame] = []

    collected = 0
    to_value: str | None = None

    while collected < target_count:

        remaining = (
            target_count - collected
        )

        request_count = min(
            API_MAX_COUNT,
            remaining,
        )

        batch = fetch_candle_batch(
            market=market,
            timeframe=timeframe,
            count=request_count,
            to=to_value,
        )

        if batch.empty:
            break

        frames.append(batch)

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

        collected = len(combined)

        oldest_time = batch[
            "candle_date_time_utc"
        ].min()

        if pd.isna(oldest_time):
            break

        next_to = (
            oldest_time
            - pd.Timedelta(
                seconds=1
            )
        )

        to_value = next_to.strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )

        if len(batch) < request_count:
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

    result = normalize_ohlcv_dataframe(
        result,
        require_market=market,
    )

    if len(result) > target_count:
        result = result.tail(
            target_count
        ).reset_index(
            drop=True
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

    if file_path.stat().st_size == 0:
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
            f"Existing CSV read failed: "
            f"{file_path} | {exc}"
        ) from exc

    if df.empty:
        return pd.DataFrame(
            columns=OHLCV_COLUMNS
        )

    missing_columns = [
        column
        for column in OHLCV_COLUMNS
        if column not in df.columns
    ]

    if missing_columns:
        raise RuntimeError(
            "Existing CSV missing columns: "
            + ", ".join(
                missing_columns
            )
        )

    rows_before = len(df)

    normalized = (
        normalize_ohlcv_dataframe(
            df,
            require_market=market,
        )
    )

    if len(normalized) != rows_before:
        raise RuntimeError(
            "Existing CSV row count changed during "
            "normalization. "
            f"{rows_before:,} -> "
            f"{len(normalized):,}"
        )

    return normalized


# ============================================================
# UPDATE HISTORY
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
            now - latest_time
        ).total_seconds(),
    )

    missing_estimate = int(
        elapsed_seconds
        // interval_seconds
    )

    target_count = max(
        20,
        missing_estimate + 10,
    )

    if target_count <= API_MAX_COUNT:

        return fetch_candle_batch(
            market=market,
            timeframe=timeframe,
            count=target_count,
        )

    frames: list[pd.DataFrame] = []

    collected = 0
    to_value: str | None = None

    stop_time = (
        latest_time
        - pd.Timedelta(
            seconds=(
                interval_seconds * 5
            )
        )
    )

    while True:

        batch = fetch_candle_batch(
            market=market,
            timeframe=timeframe,
            count=API_MAX_COUNT,
            to=to_value,
        )

        if batch.empty:
            break

        frames.append(batch)

        collected += len(batch)

        oldest_time = batch[
            "candle_date_time_utc"
        ].min()

        if pd.isna(oldest_time):
            break

        if oldest_time <= stop_time:
            break

        next_to = (
            oldest_time
            - pd.Timedelta(
                seconds=1
            )
        )

        to_value = next_to.strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )

        if len(batch) < API_MAX_COUNT:
            break

        if collected >= 10_000:
            print(
                f"[WARN] Update safety limit "
                f"reached: "
                f"{market} {timeframe}"
            )
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

    return normalize_ohlcv_dataframe(
        result,
        require_market=market,
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
        combined = new_df.copy()

    elif new_df.empty:
        combined = existing_df.copy()

    else:
        combined = pd.concat(
            [
                existing_df,
                new_df,
            ],
            ignore_index=True,
        )

    return normalize_ohlcv_dataframe(
        combined,
        require_market=market,
    )


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
            "Refusing to save empty OHLCV dataframe."
        )

    output_df = (
        normalize_ohlcv_dataframe(
            df,
            require_market=market,
        )
    )

    file_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temp_path = (
        file_path.with_suffix(
            file_path.suffix + ".tmp"
        )
    )

    output_df = output_df.copy()

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
                "Temporary CSV was not created."
            )

        if temp_path.stat().st_size == 0:
            raise RuntimeError(
                "Temporary CSV is empty."
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
    market: str,
    timeframe: str,
    status: str,
    rows_before: int,
    rows_after: int,
    new_rows: int,
    message: str = "",
) -> None:

    row = pd.DataFrame(
        [
            {
                "run_time_utc": utc_now_iso(),
                "version": VERSION,
                "market": market,
                "timeframe": timeframe,
                "status": status,
                "rows_before": rows_before,
                "rows_after": rows_after,
                "new_rows": new_rows,
                "message": message,
            }
        ],
        columns=STATUS_COLUMNS,
    )

    STATUS_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    if STATUS_FILE.exists():

        row.to_csv(
            STATUS_FILE,
            mode="a",
            header=False,
            index=False,
            encoding="utf-8-sig",
        )

    else:

        row.to_csv(
            STATUS_FILE,
            mode="w",
            header=True,
            index=False,
            encoding="utf-8-sig",
        )


# ============================================================
# MARKET + TIMEFRAME
# ============================================================

def collect_market_timeframe(
    market: str,
    timeframe: str,
) -> tuple[bool, int]:

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

        if existing_df.empty:

            print(
                f"    [{timeframe}] "
                f"initial history..."
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
            rows_after - rows_before,
        )

        if merged_df.empty:
            raise RuntimeError(
                "No OHLCV data returned."
            )

        save_dataframe_safely(
            merged_df,
            file_path,
            market,
        )

        append_status(
            market=market,
            timeframe=timeframe,
            status="SUCCESS",
            rows_before=rows_before,
            rows_after=rows_after,
            new_rows=added_rows,
            message="",
        )

        print(
            f"    [{timeframe}] OK "
            f"{rows_before:,} -> "
            f"{rows_after:,} "
            f"(+{added_rows:,})"
        )

        return (
            True,
            added_rows,
        )

    except Exception as exc:

        message = str(exc)

        append_status(
            market=market,
            timeframe=timeframe,
            status="FAILED",
            rows_before=rows_before,
            rows_after=rows_before,
            new_rows=0,
            message=message,
        )

        print(
            f"    [{timeframe}] FAILED: "
            f"{message}"
        )

        return (
            False,
            0,
        )


# ============================================================
# SUMMARY
# ============================================================

def show_final_summary(
    market_count: int,
    success_jobs: int,
    failed_jobs: int,
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
        f"KRW markets        : "
        f"{market_count:,}"
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
        "Output directories:"
    )

    print(
        f"  H1 : {H1_DIR}"
    )

    print(
        f"  H4 : {H4_DIR}"
    )

    print(
        f"  D1 : {D1_DIR}"
    )

    print(
        f"  STATUS : "
        f"{STATUS_FILE}"
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
        "Timestamp policy:"
    )

    print(
        "  candle_date_time_utc "
        "-> Unix epoch milliseconds"
    )

    print(
        "  Existing timestamp "
        "values are normalized on save."
    )

    print()

    ensure_directories()

    print(
        "[1/2] Loading Upbit KRW markets..."
    )

    try:
        markets = get_krw_markets()

    except Exception as exc:

        print()

        print(
            f"[FATAL] Failed to load "
            f"KRW markets: {exc}"
        )

        return 1

    print(
        f"      KRW markets found: "
        f"{len(markets):,}"
    )

    print()

    print(
        "[2/2] Collecting OHLCV..."
    )

    print()

    success_jobs = 0
    failed_jobs = 0
    total_added_rows = 0

    total_markets = len(
        markets
    )

    for (
        market_index,
        market,
    ) in enumerate(
        markets,
        start=1,
    ):

        print(
            f"[{market_index:03d}/"
            f"{total_markets:03d}] "
            f"{market}"
        )

        for timeframe in (
            "h1",
            "h4",
            "d1",
        ):

            (
                success,
                added_rows,
            ) = collect_market_timeframe(
                market=market,
                timeframe=timeframe,
            )

            if success:
                success_jobs += 1
                total_added_rows += (
                    added_rows
                )
            else:
                failed_jobs += 1

            time.sleep(
                REQUEST_SLEEP_SECONDS
            )

        time.sleep(
            MARKET_SLEEP_SECONDS
        )

    elapsed_seconds = (
        time.time()
        - start_time
    )

    show_final_summary(
        market_count=total_markets,
        success_jobs=success_jobs,
        failed_jobs=failed_jobs,
        total_added_rows=(
            total_added_rows
        ),
        elapsed_seconds=(
            elapsed_seconds
        ),
    )

    if failed_jobs > 0:

        print(
            "[RESULT] Collector completed "
            "with one or more failed jobs."
        )

        return 1

    print(
        "[RESULT] Collector completed "
        "successfully."
    )

    return 0


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    sys.exit(
        main()
    )
