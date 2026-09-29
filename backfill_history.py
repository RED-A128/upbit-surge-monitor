"""
Upbit Surge Monitor - Backfill History Clean V002
=================================================

File:
    backfill_history.py

Purpose:
    기존 OHLCV CSV의 과거 데이터를 뒤쪽이 아닌
    과거 방향으로 계속 확장한다.

Clean V002:
    timestamp는 Upbit API timestamp를 사용하지 않는다.

    timestamp =
        candle_date_time_utc Unix epoch milliseconds

    기존 누적 CSV 역시 로드 시 canonical timestamp로
    정규화한다.

Safety:
    - 기존 OHLCV 삭제 금지
    - 기존 행 보존
    - 과거 candle 병합
    - candle UTC 기준 중복 제거
    - candle UTC 기준 정렬
    - timestamp 자동 복구
    - batch별 checkpoint 저장
    - 중단 후 재개 가능
    - prediction 없음
    - trading 없음

Windows:
    py backfill_history.py --market KRW-BTC

    전체:
    py backfill_history.py
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests


# ============================================================
# CONFIG
# ============================================================

PROJECT_NAME = "Upbit Surge Monitor"
VERSION = "Backfill Clean V002"

BASE_DIR = Path(__file__).resolve().parent

DATA_DIR = BASE_DIR / "data"
OHLCV_DIR = DATA_DIR / "ohlcv"

H1_DIR = OHLCV_DIR / "h1"
H4_DIR = OHLCV_DIR / "h4"
D1_DIR = OHLCV_DIR / "d1"

BACKFILL_STATUS_FILE = (
    DATA_DIR
    / "backfill_status.csv"
)

UPBIT_API_BASE = "https://api.upbit.com/v1"

REQUEST_TIMEOUT = 15
API_MAX_COUNT = 200

REQUEST_SLEEP_SECONDS = 0.15
MARKET_SLEEP_SECONDS = 0.05

MAX_RETRIES = 5

SAVE_EVERY_BATCHES = 1

MAX_CANDLES_PER_JOB_PER_RUN = (
    50_000
)


# ============================================================
# TIMEFRAMES
# ============================================================

TIMEFRAMES = {
    "h1": {
        "url": (
            f"{UPBIT_API_BASE}"
            "/candles/minutes/60"
        ),
        "directory": H1_DIR,
    },
    "h4": {
        "url": (
            f"{UPBIT_API_BASE}"
            "/candles/minutes/240"
        ),
        "directory": H4_DIR,
    },
    "d1": {
        "url": (
            f"{UPBIT_API_BASE}"
            "/candles/days"
        ),
        "directory": D1_DIR,
    },
}


# ============================================================
# COLUMNS
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

BACKFILL_STATUS_COLUMNS = [
    "run_time_utc",
    "version",
    "market",
    "timeframe",
    "status",
    "rows_before",
    "rows_after",
    "added_rows",
    "oldest_before",
    "oldest_after",
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
            "backfill-clean-v002"
        ),
    }
)


# ============================================================
# ARGUMENTS
# ============================================================

def parse_arguments() -> argparse.Namespace:

    parser = argparse.ArgumentParser(
        description=(
            "Extend existing Upbit OHLCV "
            "CSV files backward in time."
        )
    )

    parser.add_argument(
        "--market",
        type=str,
        default=None,
        help=(
            "Single KRW market. "
            "Example: KRW-BTC"
        ),
    )

    return parser.parse_args()


# ============================================================
# UTILITY
# ============================================================

def print_line(
    char: str = "=",
    length: int = 78,
) -> None:
    print(
        char * length
    )


def utc_now_iso() -> str:
    return datetime.now(
        timezone.utc
    ).isoformat()


def ensure_required_directories() -> None:

    for directory in (
        DATA_DIR,
        OHLCV_DIR,
        H1_DIR,
        H4_DIR,
        D1_DIR,
    ):
        directory.mkdir(
            parents=True,
            exist_ok=True,
        )


def normalize_market_argument(
    market: str,
) -> str:

    return (
        market
        .strip()
        .upper()
    )


def format_timestamp(
    value: pd.Timestamp | None,
) -> str:

    if value is None:
        return ""

    if pd.isna(value):
        return ""

    return value.isoformat()


# ============================================================
# CANONICAL TIMESTAMP
# ============================================================

def normalize_candle_time(
    series: pd.Series,
) -> pd.Series:

    return pd.to_datetime(
        series,
        utc=True,
        errors="coerce",
    )


def build_canonical_timestamp(
    candle_time: pd.Series,
) -> pd.Series:

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
    market: str,
) -> pd.DataFrame:

    if df.empty:

        return pd.DataFrame(
            columns=OHLCV_COLUMNS
        )

    work = df.copy()

    missing_columns = [
        column
        for column in OHLCV_COLUMNS
        if column not in work.columns
    ]

    if missing_columns:

        raise RuntimeError(
            "OHLCV missing required columns: "
            + ", ".join(
                missing_columns
            )
        )

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

    if invalid_time_count:

        raise RuntimeError(
            "Invalid candle_date_time_utc: "
            f"{invalid_time_count:,}"
        )

    market_values = (
        work["market"]
        .astype(str)
        .str.strip()
        .str.upper()
    )

    if (
        market_values
        != market
    ).any():

        raise RuntimeError(
            "Unexpected market value "
            "inside OHLCV CSV."
        )

    work["market"] = market

    # --------------------------------------------------------
    # Clean V002 canonical timestamp
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
            "Canonical timestamp generation "
            "failed."
        )

    for column in (
        "open",
        "high",
        "low",
        "close",
        "volume",
        "trade_value",
    ):

        work[column] = pd.to_numeric(
            work[column],
            errors="coerce",
        )

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
            "Canonical timestamp "
            "contains invalid values."
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

            if response.status_code == 429:

                wait_seconds = min(
                    float(attempt),
                    5.0,
                )

                print(
                    "      [WARN] HTTP 429 "
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
                    "      [WARN] Upbit "
                    "server error "
                    f"{response.status_code}"
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

            print(
                "      [WARN] Request "
                f"failed: {exc}"
            )

            time.sleep(
                wait_seconds
            )

    if last_error is None:

        raise RuntimeError(
            "Unknown API request failure."
        )

    raise RuntimeError(
        "Upbit API request failed "
        f"after {MAX_RETRIES} attempts: "
        f"{last_error}"
    )


# ============================================================
# MARKET LIST
# ============================================================

def get_krw_markets() -> list[str]:

    data = request_json(
        f"{UPBIT_API_BASE}/market/all",
        params={
            "is_details": "false",
        },
    )

    if not isinstance(
        data,
        list,
    ):

        raise RuntimeError(
            "Unexpected market API response."
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
            "No KRW markets returned."
        )

    return markets


def select_markets(
    available_markets: list[str],
    requested_market: str | None,
) -> list[str]:

    if requested_market is None:
        return available_markets

    market = (
        normalize_market_argument(
            requested_market
        )
    )

    if not market.startswith(
        "KRW-"
    ):

        raise RuntimeError(
            "--market must be KRW market."
        )

    if market not in available_markets:

        raise RuntimeError(
            "Requested market does not "
            f"exist: {market}"
        )

    return [
        market
    ]


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

                "candle_date_time_utc": (
                    item.get(
                        "candle_date_time_utc"
                    )
                ),

                "candle_date_time_kst": (
                    item.get(
                        "candle_date_time_kst"
                    )
                ),

                # Clean V002
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

    df = pd.DataFrame(
        rows
    )

    return normalize_ohlcv_dataframe(
        df,
        market,
    )


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

        raise RuntimeError(
            "Existing CSV is empty: "
            f"{file_path}"
        )

    try:

        df = pd.read_csv(
            file_path,
            low_memory=False,
        )

    except Exception as exc:

        raise RuntimeError(
            "Existing CSV read failed: "
            f"{file_path} | {exc}"
        ) from exc

    if df.empty:

        raise RuntimeError(
            "Existing CSV contains zero rows."
        )

    original_rows = len(
        df
    )

    normalized = (
        normalize_ohlcv_dataframe(
            df,
            market,
        )
    )

    if len(
        normalized
    ) != original_rows:

        raise RuntimeError(
            "Existing CSV row count "
            "changed during normalization: "
            f"{original_rows:,} -> "
            f"{len(normalized):,}"
        )

    return normalized


# ============================================================
# BACKFILL API
# ============================================================

def fetch_older_candle_batch(
    market: str,
    timeframe: str,
    oldest_timestamp: pd.Timestamp,
) -> pd.DataFrame:

    config = TIMEFRAMES[
        timeframe
    ]

    to_timestamp = (
        oldest_timestamp
        - pd.Timedelta(
            seconds=1
        )
    )

    to_value = (
        to_timestamp.strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
    )

    params: dict[str, Any] = {
        "market": market,
        "count": API_MAX_COUNT,
        "to": to_value,
    }

    data = request_json(
        config["url"],
        params=params,
    )

    if not isinstance(
        data,
        list,
    ):

        raise RuntimeError(
            "Unexpected candle response: "
            f"{market} {timeframe}"
        )

    return candle_records_to_dataframe(
        market=market,
        records=data,
    )


# ============================================================
# MERGE
# ============================================================

def merge_older_history(
    existing_df: pd.DataFrame,
    older_df: pd.DataFrame,
    market: str,
) -> tuple[pd.DataFrame, int]:

    if existing_df.empty:

        raise RuntimeError(
            "Backfill requires existing "
            "OHLCV dataset."
        )

    if older_df.empty:

        return (
            existing_df.copy(),
            0,
        )

    current_oldest = (
        existing_df[
            "candle_date_time_utc"
        ].min()
    )

    strictly_older = older_df[
        older_df[
            "candle_date_time_utc"
        ] < current_oldest
    ].copy()

    if strictly_older.empty:

        return (
            existing_df.copy(),
            0,
        )

    before_count = len(
        existing_df
    )

    combined = pd.concat(
        [
            strictly_older,
            existing_df,
        ],
        ignore_index=True,
    )

    combined = (
        normalize_ohlcv_dataframe(
            combined,
            market,
        )
    )

    added_rows = max(
        0,
        len(combined)
        - before_count,
    )

    return (
        combined,
        added_rows,
    )


# ============================================================
# SAFE SAVE
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
            market,
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
                "Temporary CSV was "
                "not created."
            )

        if (
            temp_path.stat().st_size
            == 0
        ):

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

def append_backfill_status(
    market: str,
    timeframe: str,
    status: str,
    rows_before: int,
    rows_after: int,
    added_rows: int,
    oldest_before: pd.Timestamp | None,
    oldest_after: pd.Timestamp | None,
    message: str = "",
) -> None:

    row = pd.DataFrame(
        [
            {
                "run_time_utc": (
                    utc_now_iso()
                ),
                "version": VERSION,
                "market": market,
                "timeframe": timeframe,
                "status": status,
                "rows_before": rows_before,
                "rows_after": rows_after,
                "added_rows": added_rows,
                "oldest_before": (
                    format_timestamp(
                        oldest_before
                    )
                ),
                "oldest_after": (
                    format_timestamp(
                        oldest_after
                    )
                ),
                "message": message,
            }
        ],
        columns=(
            BACKFILL_STATUS_COLUMNS
        ),
    )

    BACKFILL_STATUS_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    if BACKFILL_STATUS_FILE.exists():

        row.to_csv(
            BACKFILL_STATUS_FILE,
            mode="a",
            header=False,
            index=False,
            encoding="utf-8-sig",
        )

    else:

        row.to_csv(
            BACKFILL_STATUS_FILE,
            mode="w",
            header=True,
            index=False,
            encoding="utf-8-sig",
        )


# ============================================================
# SINGLE JOB
# ============================================================

def backfill_market_timeframe(
    market: str,
    timeframe: str,
) -> tuple[bool, int, bool]:

    directory: Path = (
        TIMEFRAMES[
            timeframe
        ]["directory"]
    )

    file_path = (
        directory
        / f"{market}.csv"
    )

    if not file_path.exists():

        message = (
            "Existing OHLCV CSV "
            "not found. Backfill skipped."
        )

        print(
            f"    [{timeframe}] "
            f"SKIP - {message}"
        )

        append_backfill_status(
            market=market,
            timeframe=timeframe,
            status="SKIPPED",
            rows_before=0,
            rows_after=0,
            added_rows=0,
            oldest_before=None,
            oldest_after=None,
            message=message,
        )

        return (
            True,
            0,
            False,
        )

    try:

        existing_df = (
            load_existing_csv(
                file_path,
                market,
            )
        )

    except Exception as exc:

        message = str(
            exc
        )

        print(
            f"    [{timeframe}] "
            f"FAILED - {message}"
        )

        append_backfill_status(
            market=market,
            timeframe=timeframe,
            status="FAILED",
            rows_before=0,
            rows_after=0,
            added_rows=0,
            oldest_before=None,
            oldest_after=None,
            message=message,
        )

        return (
            False,
            0,
            False,
        )

    rows_before_job = len(
        existing_df
    )

    oldest_before_job = (
        existing_df[
            "candle_date_time_utc"
        ].min()
    )

    print(
        f"    [{timeframe}] "
        f"start rows="
        f"{rows_before_job:,} "
        f"oldest="
        f"{format_timestamp(oldest_before_job)}"
    )

    total_added_rows = 0
    fetched_this_run = 0
    batch_number = 0
    unsaved_batches = 0

    reached_history_start = False

    working_df = (
        existing_df.copy()
    )

    try:

        while True:

            current_oldest = (
                working_df[
                    "candle_date_time_utc"
                ].min()
            )

            batch_number += 1

            older_df = (
                fetch_older_candle_batch(
                    market=market,
                    timeframe=timeframe,
                    oldest_timestamp=(
                        current_oldest
                    ),
                )
            )

            if older_df.empty:

                reached_history_start = True

                print(
                    f"      batch "
                    f"{batch_number:,}: "
                    "API returned 0 candles "
                    "-> history start reached"
                )

                break

            response_rows = len(
                older_df
            )

            response_oldest = (
                older_df[
                    "candle_date_time_utc"
                ].min()
            )

            response_newest = (
                older_df[
                    "candle_date_time_utc"
                ].max()
            )

            (
                merged_df,
                added_rows,
            ) = merge_older_history(
                existing_df=working_df,
                older_df=older_df,
                market=market,
            )

            if added_rows <= 0:

                reached_history_start = True

                print(
                    f"      batch "
                    f"{batch_number:,}: "
                    f"received="
                    f"{response_rows:,}, "
                    "added=0 "
                    "-> no older usable candles"
                )

                break

            new_oldest = (
                merged_df[
                    "candle_date_time_utc"
                ].min()
            )

            if (
                new_oldest
                >= current_oldest
            ):

                raise RuntimeError(
                    "Backfill safety check "
                    "failed: oldest timestamp "
                    "did not move backward."
                )

            working_df = (
                merged_df
            )

            total_added_rows += (
                added_rows
            )

            fetched_this_run += (
                added_rows
            )

            unsaved_batches += 1

            print(
                f"      batch "
                f"{batch_number:,}: "
                f"received="
                f"{response_rows:,}, "
                f"added="
                f"{added_rows:,}, "
                f"total_added="
                f"{total_added_rows:,}"
            )

            print(
                "        API range : "
                f"{format_timestamp(response_oldest)} "
                "-> "
                f"{format_timestamp(response_newest)}"
            )

            print(
                "        Oldest    : "
                f"{format_timestamp(current_oldest)} "
                "-> "
                f"{format_timestamp(new_oldest)}"
            )

            if (
                unsaved_batches
                >= SAVE_EVERY_BATCHES
            ):

                save_dataframe_safely(
                    working_df,
                    file_path,
                    market,
                )

                unsaved_batches = 0

                print(
                    "        [SAVE] "
                    f"{len(working_df):,} rows"
                )

            if (
                response_rows
                < API_MAX_COUNT
            ):

                print(
                    "        [INFO] "
                    "Short API batch "
                    f"({response_rows:,}/"
                    f"{API_MAX_COUNT:,}). "
                    "Checking further back..."
                )

            if (
                fetched_this_run
                >= MAX_CANDLES_PER_JOB_PER_RUN
            ):

                print(
                    "      [PAUSE] "
                    "Per-job safety limit "
                    f"reached: "
                    f"{fetched_this_run:,}"
                )

                break

            time.sleep(
                REQUEST_SLEEP_SECONDS
            )

        if unsaved_batches > 0:

            save_dataframe_safely(
                working_df,
                file_path,
                market,
            )

        # ----------------------------------------------------
        # IMPORTANT
        #
        # Backfill이 추가할 과거 candle이 하나도 없는 경우에도
        # 기존 파일의 잘못된 timestamp를 V002 canonical
        # timestamp로 복구하기 위해 반드시 한 번 저장한다.
        # ----------------------------------------------------

        if total_added_rows == 0:

            save_dataframe_safely(
                working_df,
                file_path,
                market,
            )

            print(
                "        [REPAIR] Existing "
                "timestamp column normalized."
            )

        rows_after_job = len(
            working_df
        )

        oldest_after_job = (
            working_df[
                "candle_date_time_utc"
            ].min()
        )

        if (
            oldest_after_job
            > oldest_before_job
        ):

            raise RuntimeError(
                "Final oldest timestamp "
                "became newer."
            )

        if (
            rows_after_job
            < rows_before_job
        ):

            raise RuntimeError(
                "OHLCV row count decreased."
            )

        if reached_history_start:

            status = (
                "HISTORY_START_REACHED"
            )

            message = (
                "Historical boundary reached. "
                "Canonical timestamps normalized."
            )

        else:

            status = (
                "PARTIAL_SUCCESS"
            )

            message = (
                "Backfill progressed. "
                "Canonical timestamps normalized."
            )

        append_backfill_status(
            market=market,
            timeframe=timeframe,
            status=status,
            rows_before=rows_before_job,
            rows_after=rows_after_job,
            added_rows=total_added_rows,
            oldest_before=(
                oldest_before_job
            ),
            oldest_after=(
                oldest_after_job
            ),
            message=message,
        )

        print(
            f"    [{timeframe}] OK "
            f"{rows_before_job:,} "
            f"-> "
            f"{rows_after_job:,} "
            f"(+{total_added_rows:,})"
        )

        return (
            True,
            total_added_rows,
            reached_history_start,
        )

    except Exception as exc:

        message = str(
            exc
        )

        print(
            f"    [{timeframe}] "
            f"FAILED: {message}"
        )

        append_backfill_status(
            market=market,
            timeframe=timeframe,
            status="FAILED",
            rows_before=(
                rows_before_job
            ),
            rows_after=(
                rows_before_job
            ),
            added_rows=0,
            oldest_before=(
                oldest_before_job
            ),
            oldest_after=(
                oldest_before_job
            ),
            message=message,
        )

        return (
            False,
            0,
            False,
        )


# ============================================================
# SUMMARY
# ============================================================

def show_final_summary(
    execution_mode: str,
    selected_market: str | None,
    market_count: int,
    total_jobs: int,
    success_jobs: int,
    failed_jobs: int,
    history_start_jobs: int,
    total_added_rows: int,
    elapsed_seconds: float,
) -> None:

    print()
    print_line()

    print(
        "BACKFILL HISTORY SUMMARY"
    )

    print_line()

    print(
        f"Project                 : "
        f"{PROJECT_NAME}"
    )

    print(
        f"Version                 : "
        f"{VERSION}"
    )

    print(
        f"Execution mode          : "
        f"{execution_mode}"
    )

    if selected_market:

        print(
            f"Selected market         : "
            f"{selected_market}"
        )

    print(
        f"Markets processed       : "
        f"{market_count:,}"
    )

    print(
        f"Total jobs              : "
        f"{total_jobs:,}"
    )

    print(
        f"Successful jobs         : "
        f"{success_jobs:,}"
    )

    print(
        f"Failed jobs             : "
        f"{failed_jobs:,}"
    )

    print(
        f"History-start reached   : "
        f"{history_start_jobs:,}"
    )

    print(
        f"New historical rows     : "
        f"{total_added_rows:,}"
    )

    print(
        f"Elapsed seconds         : "
        f"{elapsed_seconds:,.2f}"
    )

    print()

    print(
        "Timestamp policy:"
    )

    print(
        "  Source                : "
        "candle_date_time_utc"
    )

    print(
        "  Unit                  : "
        "Unix epoch milliseconds"
    )

    print(
        "  API timestamp         : "
        "NOT USED"
    )

    print()

    print(
        "Safety:"
    )

    print(
        "  Existing deletion     : "
        "DISABLED"
    )

    print(
        "  Existing history      : "
        "PRESERVED"
    )

    print(
        "  Timestamp repair      : "
        "ENABLED"
    )

    print(
        "  Checkpoint save       : "
        "ENABLED"
    )

    print_line()


# ============================================================
# MAIN
# ============================================================

def main() -> int:

    start_time = time.time()

    args = parse_arguments()

    requested_market = (
        args.market
    )

    if requested_market:

        requested_market = (
            normalize_market_argument(
                requested_market
            )
        )

    print_line()

    print(
        f"{PROJECT_NAME} - "
        f"{VERSION}"
    )

    print(
        "STEP 2 OHLCV HISTORY BACKFILL"
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
        "Clean V002:"
    )

    print(
        "  Canonical timestamp repair ENABLED"
    )

    print(
        "  API timestamp ignored"
    )

    print(
        "  Existing history preserved"
    )

    print()

    ensure_required_directories()

    print(
        "[1/2] Loading Upbit "
        "KRW markets..."
    )

    try:

        available_markets = (
            get_krw_markets()
        )

        markets = select_markets(
            available_markets,
            requested_market,
        )

    except Exception as exc:

        print(
            f"[FATAL] {exc}"
        )

        return 1

    print(
        f"      Markets selected: "
        f"{len(markets):,}"
    )

    print()

    print(
        "[2/2] Backfilling "
        "historical OHLCV..."
    )

    total_jobs = (
        len(markets)
        * len(TIMEFRAMES)
    )

    success_jobs = 0
    failed_jobs = 0
    history_start_jobs = 0
    total_added_rows = 0

    for (
        market_index,
        market,
    ) in enumerate(
        markets,
        start=1,
    ):

        print_line(
            "-",
            78,
        )

        print(
            f"[{market_index:03d}/"
            f"{len(markets):03d}] "
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
                reached_history_start,
            ) = backfill_market_timeframe(
                market,
                timeframe,
            )

            if success:

                success_jobs += 1

                total_added_rows += (
                    added_rows
                )

                if reached_history_start:
                    history_start_jobs += 1

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

    execution_mode = (
        "SINGLE MARKET"
        if requested_market
        else "ALL KRW MARKETS"
    )

    show_final_summary(
        execution_mode=(
            execution_mode
        ),
        selected_market=(
            requested_market
        ),
        market_count=len(markets),
        total_jobs=total_jobs,
        success_jobs=success_jobs,
        failed_jobs=failed_jobs,
        history_start_jobs=(
            history_start_jobs
        ),
        total_added_rows=(
            total_added_rows
        ),
        elapsed_seconds=(
            elapsed_seconds
        ),
    )

    if failed_jobs > 0:

        print(
            "[RESULT] BACKFILL COMPLETED "
            "WITH FAILED JOBS"
        )

        return 1

    print(
        "[RESULT] BACKFILL COMPLETED "
        "SUCCESSFULLY"
    )

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
            "[STOP] Backfill interrupted "
            "by user."
        )

        print(
            "[INFO] Previously saved "
            "data remains preserved."
        )

        exit_code = 130

    except Exception as exc:

        print()

        print_line()

        print(
            "BACKFILL FATAL ERROR"
        )

        print_line()

        print(
            f"{type(exc).__name__}: "
            f"{exc}"
        )

        print_line()

        exit_code = 1

    sys.exit(
        exit_code
    )
