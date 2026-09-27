"""
Upbit Surge Monitor - Clean V001
================================

File:
    backfill_history.py

Purpose:
    Existing OHLCV CSV files are extended backward in time.

    This script is dedicated to STEP 2 historical OHLCV backfill.

    It does NOT replace collector.py.

Roles:
    collector.py
        -> Keeps current/latest OHLCV data updated.

    backfill_history.py
        -> Extends existing OHLCV history backward.

Important:
    - Existing OHLCV data is preserved.
    - Existing CSV files are never intentionally deleted.
    - Historical candles are merged with existing candles.
    - Duplicate candle timestamps are removed.
    - Candles are sorted in chronological order.
    - Each successful batch is saved immediately.
    - The script can be stopped and restarted.
    - On restart, it continues from the oldest stored candle.
    - No pattern analysis.
    - No prediction.
    - No machine learning.
    - No trading.
    - No automatic orders.

Windows:
    py backfill_history.py

Expected existing directories:
    data/ohlcv/h1/
    data/ohlcv/h4/
    data/ohlcv/d1/
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
# PROJECT CONFIGURATION
# ============================================================

PROJECT_NAME = "Upbit Surge Monitor"
VERSION = "Clean V001"

BASE_DIR = Path(__file__).resolve().parent

DATA_DIR = BASE_DIR / "data"
OHLCV_DIR = DATA_DIR / "ohlcv"

H1_DIR = OHLCV_DIR / "h1"
H4_DIR = OHLCV_DIR / "h4"
D1_DIR = OHLCV_DIR / "d1"

BACKFILL_STATUS_FILE = (
    DATA_DIR / "backfill_status.csv"
)

UPBIT_API_BASE = "https://api.upbit.com/v1"

REQUEST_TIMEOUT = 15

# Upbit candle API request count per call.
API_MAX_COUNT = 200

# Delay between API requests.
#
# This intentionally stays conservative.
REQUEST_SLEEP_SECONDS = 0.15

# Delay between market/timeframe jobs.
MARKET_SLEEP_SECONDS = 0.05

# HTTP retry count.
MAX_RETRIES = 5

# Save after every successful API batch.
#
# This is intentionally 1 so that a stopped GitHub Action,
# PC shutdown, network interruption, or manual cancellation
# loses as little completed work as possible.
SAVE_EVERY_BATCHES = 1

# Safety limit for one market + timeframe during one execution.
#
# This is NOT the desired history depth.
#
# It prevents one abnormal API response sequence from creating
# an infinite loop.
#
# 50,000 candles is far beyond the current Clean V001 history
# and can be continued on the next run if ever reached.
MAX_CANDLES_PER_JOB_PER_RUN = 50_000


# ============================================================
# TIMEFRAME CONFIGURATION
# ============================================================

TIMEFRAMES = {
    "h1": {
        "url": f"{UPBIT_API_BASE}/candles/minutes/60",
        "directory": H1_DIR,
    },
    "h4": {
        "url": f"{UPBIT_API_BASE}/candles/minutes/240",
        "directory": H4_DIR,
    },
    "d1": {
        "url": f"{UPBIT_API_BASE}/candles/days",
        "directory": D1_DIR,
    },
}


# ============================================================
# OHLCV FORMAT
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
# HTTP SESSION
# ============================================================

SESSION = requests.Session()

SESSION.headers.update(
    {
        "Accept": "application/json",
        "User-Agent": (
            "upbit-surge-monitor-clean-v001-backfill"
        ),
    }
)


# ============================================================
# DISPLAY / UTILITY
# ============================================================

def print_line(
    char: str = "=",
    length: int = 78,
) -> None:
    print(char * length)


def utc_now_iso() -> str:
    return datetime.now(
        timezone.utc
    ).isoformat()


def ensure_required_directories() -> None:
    """
    Create only the project data directories if necessary.

    Existing OHLCV files are never removed.
    """

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


def format_timestamp(
    value: pd.Timestamp | None,
) -> str:
    if value is None:
        return ""

    if pd.isna(value):
        return ""

    return value.isoformat()


# ============================================================
# HTTP
# ============================================================

def request_json(
    url: str,
    params: dict[str, Any] | None = None,
) -> Any:
    """
    Request JSON from Upbit.

    Temporary network errors, HTTP 429, and server errors
    are retried.
    """

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
                    1.0 * attempt,
                    5.0,
                )

                print(
                    f"      [WARN] HTTP 429 rate limit. "
                    f"retry={attempt}/{MAX_RETRIES}, "
                    f"wait={wait_seconds:.1f}s"
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
                    1.0 * attempt,
                    5.0,
                )

                print(
                    f"      [WARN] Upbit server error "
                    f"{response.status_code}. "
                    f"retry={attempt}/{MAX_RETRIES}"
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
                1.0 * attempt,
                5.0,
            )

            print(
                f"      [WARN] Request failed: "
                f"{exc} "
                f"retry={attempt}/{MAX_RETRIES}"
            )

            time.sleep(
                wait_seconds
            )

    if last_error is None:

        raise RuntimeError(
            "Unknown API request failure."
        )

    raise RuntimeError(
        f"Upbit API request failed after "
        f"{MAX_RETRIES} attempts: "
        f"{last_error}"
    )


# ============================================================
# MARKET LIST
# ============================================================

def get_krw_markets() -> list[str]:
    """
    Load the current Upbit KRW market list.
    """

    url = (
        f"{UPBIT_API_BASE}/market/all"
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
        ).strip()

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
            "No KRW markets were returned "
            "from Upbit."
        )

    return markets


# ============================================================
# CANDLE CONVERSION
# ============================================================

def candle_records_to_dataframe(
    market: str,
    records: list[dict[str, Any]],
) -> pd.DataFrame:
    """
    Convert Upbit candle records into the exact OHLCV format
    used by Clean V001 collector.py.
    """

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
                "timestamp": (
                    item.get(
                        "timestamp"
                    )
                ),
                "open": (
                    item.get(
                        "opening_price"
                    )
                ),
                "high": (
                    item.get(
                        "high_price"
                    )
                ),
                "low": (
                    item.get(
                        "low_price"
                    )
                ),
                "close": (
                    item.get(
                        "trade_price"
                    )
                ),
                "volume": (
                    item.get(
                        "candle_acc_trade_volume"
                    )
                ),
                "trade_value": (
                    item.get(
                        "candle_acc_trade_price"
                    )
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

    for column in OHLCV_COLUMNS:

        if column not in df.columns:
            df[column] = pd.NA

    df = df[
        OHLCV_COLUMNS
    ]

    df[
        "candle_date_time_utc"
    ] = pd.to_datetime(
        df[
            "candle_date_time_utc"
        ],
        utc=True,
        errors="coerce",
    )

    df = df.dropna(
        subset=[
            "candle_date_time_utc"
        ]
    )

    df = df.drop_duplicates(
        subset=[
            "candle_date_time_utc"
        ],
        keep="last",
    )

    df = df.sort_values(
        "candle_date_time_utc"
    )

    df = df.reset_index(
        drop=True
    )

    return df


# ============================================================
# EXISTING CSV
# ============================================================

def load_existing_csv(
    file_path: Path,
) -> pd.DataFrame:
    """
    Load an existing Clean V001 OHLCV CSV.

    This function never creates replacement history when the
    existing file cannot be safely read.
    """

    if not file_path.exists():

        return pd.DataFrame(
            columns=OHLCV_COLUMNS
        )

    if file_path.stat().st_size == 0:

        raise RuntimeError(
            f"Existing CSV is empty: "
            f"{file_path}"
        )

    try:

        df = pd.read_csv(
            file_path,
            low_memory=False,
        )

    except Exception as exc:

        raise RuntimeError(
            f"Existing CSV read failed: "
            f"{file_path} | "
            f"{type(exc).__name__}: {exc}"
        ) from exc

    if df.empty:

        raise RuntimeError(
            f"Existing CSV contains zero rows: "
            f"{file_path}"
        )

    missing_columns = [
        column
        for column in OHLCV_COLUMNS
        if column not in df.columns
    ]

    if missing_columns:

        raise RuntimeError(
            "Existing CSV is missing required columns: "
            f"{file_path} | "
            f"{', '.join(missing_columns)}"
        )

    df = df[
        OHLCV_COLUMNS
    ].copy()

    original_row_count = len(
        df
    )

    df[
        "candle_date_time_utc"
    ] = pd.to_datetime(
        df[
            "candle_date_time_utc"
        ],
        utc=True,
        errors="coerce",
    )

    invalid_timestamp_count = int(
        df[
            "candle_date_time_utc"
        ].isna().sum()
    )

    if invalid_timestamp_count > 0:

        raise RuntimeError(
            "Existing CSV contains invalid "
            "candle timestamps: "
            f"{file_path} | "
            f"count={invalid_timestamp_count:,}"
        )

    duplicate_count = int(
        df[
            "candle_date_time_utc"
        ].duplicated(
            keep=False
        ).sum()
    )

    if duplicate_count > 0:

        raise RuntimeError(
            "Existing CSV contains duplicate "
            "candle timestamps: "
            f"{file_path} | "
            f"count={duplicate_count:,}"
        )

    df = df.sort_values(
        "candle_date_time_utc"
    )

    df = df.reset_index(
        drop=True
    )

    if len(df) != original_row_count:

        raise RuntimeError(
            "Existing CSV row count changed "
            "during safety loading."
        )

    return df


# ============================================================
# BACKFILL API
# ============================================================

def fetch_older_candle_batch(
    market: str,
    timeframe: str,
    oldest_timestamp: pd.Timestamp,
) -> pd.DataFrame:
    """
    Fetch candles strictly before the oldest currently stored
    candle.

    Upbit's 'to' parameter is moved one second before the
    current oldest candle to reduce overlap.
    """

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
            f"Unexpected candle response: "
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
) -> tuple[
    pd.DataFrame,
    int,
]:
    """
    Merge historical candles into the existing dataset.

    Only candles strictly older than the current oldest stored
    candle are accepted.

    Returns:
        (merged_dataframe, number_of_added_rows)
    """

    if existing_df.empty:

        raise RuntimeError(
            "Backfill requires an existing "
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

    strictly_older = (
        older_df[
            older_df[
                "candle_date_time_utc"
            ] < current_oldest
        ].copy()
    )

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

    combined[
        "candle_date_time_utc"
    ] = pd.to_datetime(
        combined[
            "candle_date_time_utc"
        ],
        utc=True,
        errors="coerce",
    )

    combined = combined.dropna(
        subset=[
            "candle_date_time_utc"
        ]
    )

    combined = combined.drop_duplicates(
        subset=[
            "candle_date_time_utc"
        ],
        keep="last",
    )

    combined = combined.sort_values(
        "candle_date_time_utc"
    )

    combined = combined.reset_index(
        drop=True
    )

    combined = combined[
        OHLCV_COLUMNS
    ]

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
) -> None:
    """
    Safely replace an existing OHLCV CSV.

    Process:
        1. Write complete merged data to .tmp.
        2. Confirm the temporary file exists and is non-empty.
        3. Atomically replace the existing CSV.

    Existing source CSV is not touched until the temporary
    write has completed successfully.
    """

    if df.empty:

        raise RuntimeError(
            "Refusing to save an empty "
            "OHLCV dataframe."
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

    output_df = df.copy()

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
    """
    Append one backfill job result.

    This status file is separate from collector_status.csv.
    """

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
# SINGLE MARKET + TIMEFRAME BACKFILL
# ============================================================

def backfill_market_timeframe(
    market: str,
    timeframe: str,
) -> tuple[
    bool,
    int,
    bool,
]:
    """
    Backfill one market + timeframe.

    Returns:
        success
        total_added_rows
        reached_history_start

    reached_history_start=True means the API returned no older
    usable candles and this dataset appears to have reached the
    available historical boundary.
    """

    directory: Path = (
        TIMEFRAMES[
            timeframe
        ]["directory"]
    )

    file_path = (
        directory
        / f"{market}.csv"
    )

    # --------------------------------------------------------
    # BACKFILL DOES NOT CREATE A NEW MARKET DATASET
    # --------------------------------------------------------
    #
    # collector.py remains responsible for initial/current
    # dataset creation.
    #
    # This prevents Backfill from silently replacing the role
    # of Collector.
    # --------------------------------------------------------

    if not file_path.exists():

        message = (
            "Existing OHLCV CSV not found. "
            "Backfill skipped."
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
                file_path
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

    previous_oldest = (
        oldest_before_job
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
                    f"API returned 0 candles "
                    f"-> history start reached"
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
            )

            if added_rows <= 0:

                reached_history_start = True

                print(
                    f"      batch "
                    f"{batch_number:,}: "
                    f"received="
                    f"{response_rows:,}, "
                    f"added=0 "
                    f"-> no older usable "
                    f"candles"
                )

                break

            new_oldest = (
                merged_df[
                    "candle_date_time_utc"
                ].min()
            )

            # ------------------------------------------------
            # SAFETY:
            # The oldest timestamp MUST move backward.
            # ------------------------------------------------

            if (
                new_oldest
                >= current_oldest
            ):

                raise RuntimeError(
                    "Backfill safety check failed: "
                    "oldest timestamp did not "
                    "move backward."
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
                f"        API range : "
                f"{format_timestamp(response_oldest)} "
                f"-> "
                f"{format_timestamp(response_newest)}"
            )

            print(
                f"        Oldest    : "
                f"{format_timestamp(current_oldest)} "
                f"-> "
                f"{format_timestamp(new_oldest)}"
            )

            # ------------------------------------------------
            # SAVE CHECKPOINT
            # ------------------------------------------------

            if (
                unsaved_batches
                >= SAVE_EVERY_BATCHES
            ):

                save_dataframe_safely(
                    working_df,
                    file_path,
                )

                unsaved_batches = 0

                print(
                    f"        [SAVE] "
                    f"{len(working_df):,} rows"
                )

            # ------------------------------------------------
            # API HISTORICAL BOUNDARY
            # ------------------------------------------------
            #
            # If fewer than API_MAX_COUNT candles were returned,
            # Upbit may have reached the beginning of available
            # candle history.
            #
            # We do not immediately assume completion solely
            # from this condition.
            #
            # One additional request will naturally test
            # whether older candles exist.
            # ------------------------------------------------

            if response_rows < API_MAX_COUNT:

                print(
                    f"        [INFO] "
                    f"Short API batch "
                    f"({response_rows:,}/"
                    f"{API_MAX_COUNT:,}). "
                    f"Checking one level "
                    f"further back..."
                )

            # ------------------------------------------------
            # PER-JOB SAFETY LIMIT
            # ------------------------------------------------

            if (
                fetched_this_run
                >= MAX_CANDLES_PER_JOB_PER_RUN
            ):

                print(
                    f"      [PAUSE] "
                    f"Per-job safety limit "
                    f"reached: "
                    f"{fetched_this_run:,} "
                    f"candles"
                )

                print(
                    "      [INFO] Run "
                    "backfill_history.py again "
                    "to continue."
                )

                break

            previous_oldest = (
                new_oldest
            )

            time.sleep(
                REQUEST_SLEEP_SECONDS
            )

        # ----------------------------------------------------
        # FINAL CHECKPOINT
        # ----------------------------------------------------

        if unsaved_batches > 0:

            save_dataframe_safely(
                working_df,
                file_path,
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
                "Backfill safety check failed: "
                "final oldest timestamp became newer."
            )

        if (
            rows_after_job
            < rows_before_job
        ):

            raise RuntimeError(
                "Backfill safety check failed: "
                "row count decreased."
            )

        if reached_history_start:

            status = (
                "HISTORY_START_REACHED"
            )

            message = (
                "No additional older candles "
                "were available."
            )

        else:

            status = (
                "PARTIAL_SUCCESS"
            )

            message = (
                "Backfill progressed backward "
                "and can continue on a later run."
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
            f"    [{timeframe}] "
            f"OK "
            f"{rows_before_job:,} "
            f"-> "
            f"{rows_after_job:,} "
            f"(+{total_added_rows:,})"
        )

        print(
            f"    [{timeframe}] "
            f"oldest "
            f"{format_timestamp(oldest_before_job)} "
            f"-> "
            f"{format_timestamp(oldest_after_job)}"
        )

        if reached_history_start:

            print(
                f"    [{timeframe}] "
                f"HISTORY START REACHED"
            )

        else:

            print(
                f"    [{timeframe}] "
                f"BACKFILL CAN CONTINUE"
            )

        return (
            True,
            total_added_rows,
            reached_history_start,
        )

    except Exception as exc:

        # ----------------------------------------------------
        # IMPORTANT:
        #
        # Every completed batch has already been checkpointed.
        #
        # Therefore previously saved progress is preserved.
        #
        # Reload the actual on-disk CSV for accurate status.
        # ----------------------------------------------------

        message = str(
            exc
        )

        try:

            disk_df = (
                load_existing_csv(
                    file_path
                )
            )

            rows_after_error = len(
                disk_df
            )

            oldest_after_error = (
                disk_df[
                    "candle_date_time_utc"
                ].min()
            )

            actual_added = max(
                0,
                rows_after_error
                - rows_before_job,
            )

        except Exception:

            rows_after_error = (
                rows_before_job
            )

            oldest_after_error = (
                oldest_before_job
            )

            actual_added = 0

        append_backfill_status(
            market=market,
            timeframe=timeframe,
            status="FAILED",
            rows_before=rows_before_job,
            rows_after=rows_after_error,
            added_rows=actual_added,
            oldest_before=(
                oldest_before_job
            ),
            oldest_after=(
                oldest_after_error
            ),
            message=message,
        )

        print(
            f"    [{timeframe}] "
            f"FAILED: {message}"
        )

        print(
            f"    [{timeframe}] "
            f"Saved progress before failure "
            f"is preserved."
        )

        return (
            False,
            actual_added,
            False,
        )


# ============================================================
# SUMMARY
# ============================================================

def show_final_summary(
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
        f"KRW markets             : "
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

    print()

    print(
        "Backfill status:"
    )

    print(
        f"  {BACKFILL_STATUS_FILE}"
    )

    print()

    print(
        "Safety:"
    )

    print(
        "  Existing OHLCV deletion : DISABLED"
    )

    print(
        "  Existing history        : PRESERVED"
    )

    print(
        "  Batch checkpoint save   : ENABLED"
    )

    print(
        "  Resume on next run      : ENABLED"
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
        "Mode:"
    )

    print(
        "  Historical backfill only"
    )

    print(
        "  Existing CSV required"
    )

    print(
        "  Existing data preserved"
    )

    print(
        "  Automatic resume enabled"
    )

    print(
        "  Pattern analysis disabled"
    )

    print(
        "  Prediction disabled"
    )

    print(
        "  Trading disabled"
    )

    print()

    ensure_required_directories()

    # --------------------------------------------------------
    # LOAD CURRENT KRW MARKETS
    # --------------------------------------------------------

    print(
        "[1/2] Loading Upbit "
        "KRW markets..."
    )

    try:

        markets = (
            get_krw_markets()
        )

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

    # --------------------------------------------------------
    # BACKFILL
    # --------------------------------------------------------

    print(
        "[2/2] Backfilling historical OHLCV..."
    )

    print()

    total_markets = len(
        markets
    )

    total_jobs = (
        total_markets
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
                reached_history_start,
            ) = backfill_market_timeframe(
                market=market,
                timeframe=timeframe,
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

    # --------------------------------------------------------
    # SUMMARY
    # --------------------------------------------------------

    elapsed_seconds = (
        time.time()
        - start_time
    )

    show_final_summary(
        market_count=total_markets,
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
            "WITH ONE OR MORE FAILED JOBS"
        )

        print(
            "[INFO] Successfully checkpointed "
            "historical data remains preserved."
        )

        return 1

    print(
        "[RESULT] BACKFILL COMPLETED "
        "SUCCESSFULLY"
    )

    if (
        history_start_jobs
        == total_jobs
    ):

        print(
            "[PASS] All market/timeframe jobs "
            "reached their available "
            "historical boundary."
        )

    else:

        remaining_jobs = (
            total_jobs
            - history_start_jobs
        )

        print(
            f"[INFO] Jobs that may still "
            f"continue backward: "
            f"{remaining_jobs:,}"
        )

        print(
            "[INFO] Run this script again "
            "to continue those datasets."
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
            "[INFO] Previously checkpointed "
            "batches remain saved."
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
