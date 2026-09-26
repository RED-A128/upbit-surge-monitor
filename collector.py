"""
Upbit Surge Monitor - Clean V001
================================

File:
    collector.py

Purpose:
    Upbit KRW 전체 마켓의 OHLCV 데이터를 수집하고
    로컬 CSV 파일에 안전하게 누적 저장한다.

Initial timeframes:
    - 1 hour
    - 4 hour
    - 1 day

Important:
    - 이 파일은 급등 여부를 판단하지 않는다.
    - 매수/매도 신호를 만들지 않는다.
    - 머신러닝을 수행하지 않는다.
    - 자동매매를 수행하지 않는다.
    - 원본 OHLCV 확보와 보존에 집중한다.

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
VERSION = "Clean V001"

BASE_DIR = Path(__file__).resolve().parent

DATA_DIR = BASE_DIR / "data"
OHLCV_DIR = DATA_DIR / "ohlcv"

H1_DIR = OHLCV_DIR / "h1"
H4_DIR = OHLCV_DIR / "h4"
D1_DIR = OHLCV_DIR / "d1"

STATUS_FILE = DATA_DIR / "collector_status.csv"

UPBIT_API_BASE = "https://api.upbit.com/v1"

REQUEST_TIMEOUT = 15

# Upbit candle API의 1회 최대 요청 개수
API_MAX_COUNT = 200

# 신규 저장소에서 처음 실행할 때 확보할 목표 candle 수
#
# h1 : 2000시간 ≈ 83일
# h4 : 1500개   ≈ 250일
# d1 : 1000일   ≈ 2.7년
#
# 이후 실행부터는 기존 CSV의 마지막 시점 이후 데이터만 갱신한다.
INITIAL_CANDLE_LIMITS = {
    "h1": 2000,
    "h4": 1500,
    "d1": 1000,
}

# API 요청 간 기본 대기시간
REQUEST_SLEEP_SECONDS = 0.12

# 마켓 하나 처리 후 짧은 대기
MARKET_SLEEP_SECONDS = 0.05

# HTTP 재시도
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
        "User-Agent": "upbit-surge-monitor-clean-v001",
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
    """
    프로젝트에서 필요한 데이터 디렉터리를 생성한다.
    """

    directories = [
        DATA_DIR,
        OHLCV_DIR,
        H1_DIR,
        H4_DIR,
        D1_DIR,
    ]

    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


# ============================================================
# HTTP
# ============================================================

def request_json(
    url: str,
    params: dict[str, Any] | None = None,
) -> Any:
    """
    Upbit API를 호출하고 JSON을 반환한다.

    429 / 일시적 네트워크 오류 / 서버 오류는 재시도한다.
    """

    last_error: Exception | None = None

    for attempt in range(1, MAX_RETRIES + 1):

        try:
            response = SESSION.get(
                url,
                params=params,
                timeout=REQUEST_TIMEOUT,
            )

            if response.status_code == 429:
                wait_seconds = min(1.0 * attempt, 5.0)

                print(
                    f"[WARN] HTTP 429 rate limit. "
                    f"retry={attempt}/{MAX_RETRIES}, "
                    f"wait={wait_seconds:.1f}s"
                )

                time.sleep(wait_seconds)
                continue

            if 500 <= response.status_code <= 599:
                wait_seconds = min(1.0 * attempt, 5.0)

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

            wait_seconds = min(1.0 * attempt, 5.0)

            print(
                f"[WARN] Request failed: {exc} "
                f"retry={attempt}/{MAX_RETRIES}"
            )

            time.sleep(wait_seconds)

    if last_error is None:
        raise RuntimeError("Unknown API request failure.")

    raise RuntimeError(
        f"Upbit API request failed after {MAX_RETRIES} attempts: "
        f"{last_error}"
    )


# ============================================================
# MARKET LIST
# ============================================================

def get_krw_markets() -> list[str]:
    """
    업비트 전체 마켓 중 KRW 마켓만 가져온다.
    """

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

        market = str(item.get("market", "")).strip()

        if market.startswith("KRW-"):
            markets.append(market)

    markets = sorted(set(markets))

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
    """
    Upbit candle 응답을 프로젝트 공통 OHLCV 형식으로 변환한다.
    """

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
                "timestamp": item.get("timestamp"),
                "open": item.get("opening_price"),
                "high": item.get("high_price"),
                "low": item.get("low_price"),
                "close": item.get("trade_price"),
                "volume": item.get(
                    "candle_acc_trade_volume"
                ),
                "trade_value": item.get(
                    "candle_acc_trade_price"
                ),
            }
        )

    if not rows:
        return pd.DataFrame(columns=OHLCV_COLUMNS)

    df = pd.DataFrame(rows)

    for column in OHLCV_COLUMNS:

        if column not in df.columns:
            df[column] = pd.NA

    df = df[OHLCV_COLUMNS]

    df["candle_date_time_utc"] = pd.to_datetime(
        df["candle_date_time_utc"],
        utc=True,
        errors="coerce",
    )

    df = df.dropna(
        subset=["candle_date_time_utc"]
    )

    df = df.sort_values(
        "candle_date_time_utc"
    )

    df = df.drop_duplicates(
        subset=["candle_date_time_utc"],
        keep="last",
    )

    df = df.reset_index(drop=True)

    return df


# ============================================================
# CANDLE API
# ============================================================

def fetch_candle_batch(
    market: str,
    timeframe: str,
    count: int,
    to: str | None = None,
) -> pd.DataFrame:
    """
    특정 마켓/시간봉의 candle 한 batch를 가져온다.
    """

    config = TIMEFRAMES[timeframe]

    params: dict[str, Any] = {
        "market": market,
        "count": min(count, API_MAX_COUNT),
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
    """
    해당 마켓의 CSV가 존재하지 않을 때
    과거 데이터를 여러 페이지에 걸쳐 확보한다.
    """

    frames: list[pd.DataFrame] = []

    collected = 0
    to_value: str | None = None

    while collected < target_count:

        remaining = target_count - collected

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

        combined = combined.drop_duplicates(
            subset=["candle_date_time_utc"],
            keep="last",
        )

        combined = combined.sort_values(
            "candle_date_time_utc"
        )

        collected = len(combined)

        oldest_time = batch[
            "candle_date_time_utc"
        ].min()

        if pd.isna(oldest_time):
            break

        # Upbit의 'to'는 해당 시각 이전 candle을 요청하기 위한 값이다.
        # 1초 이전으로 이동하여 동일 candle 재수신 가능성을 줄인다.
        next_to = (
            oldest_time
            - pd.Timedelta(seconds=1)
        )

        to_value = next_to.strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )

        if len(batch) < request_count:
            break

        time.sleep(REQUEST_SLEEP_SECONDS)

    if not frames:
        return pd.DataFrame(
            columns=OHLCV_COLUMNS
        )

    result = pd.concat(
        frames,
        ignore_index=True,
    )

    result = result.drop_duplicates(
        subset=["candle_date_time_utc"],
        keep="last",
    )

    result = result.sort_values(
        "candle_date_time_utc"
    )

    if len(result) > target_count:
        result = result.tail(target_count)

    result = result.reset_index(drop=True)

    return result


# ============================================================
# EXISTING CSV
# ============================================================

def load_existing_csv(
    file_path: Path,
) -> pd.DataFrame:
    """
    기존 OHLCV CSV를 읽는다.

    파일이 없거나 비어 있으면 빈 DataFrame을 반환한다.
    """

    if not file_path.exists():
        return pd.DataFrame(
            columns=OHLCV_COLUMNS
        )

    try:
        df = pd.read_csv(file_path)

    except pd.errors.EmptyDataError:
        return pd.DataFrame(
            columns=OHLCV_COLUMNS
        )

    if df.empty:
        return pd.DataFrame(
            columns=OHLCV_COLUMNS
        )

    for column in OHLCV_COLUMNS:

        if column not in df.columns:
            df[column] = pd.NA

    df = df[OHLCV_COLUMNS]

    df["candle_date_time_utc"] = pd.to_datetime(
        df["candle_date_time_utc"],
        utc=True,
        errors="coerce",
    )

    df = df.dropna(
        subset=["candle_date_time_utc"]
    )

    df = df.sort_values(
        "candle_date_time_utc"
    )

    df = df.drop_duplicates(
        subset=["candle_date_time_utc"],
        keep="last",
    )

    df = df.reset_index(drop=True)

    return df


# ============================================================
# UPDATE HISTORY
# ============================================================

def fetch_latest_update(
    market: str,
    timeframe: str,
    existing_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    기존 CSV가 있는 경우 최신 candle들을 다시 받아
    기존 데이터와 병합한다.

    마지막 몇 개 candle을 다시 받는 이유:
    가장 최근 candle은 아직 진행 중일 수 있으므로
    다음 실행에서 최신 값으로 갱신하기 위함이다.
    """

    if existing_df.empty:
        return fetch_initial_history(
            market=market,
            timeframe=timeframe,
            target_count=INITIAL_CANDLE_LIMITS[
                timeframe
            ],
        )

    # 일반적인 매시간 실행에서는 200개면 충분하다.
    #
    # PC가 장기간 꺼져 있었던 경우를 대비해
    # 마지막 저장 시점과 현재 시점 차이를 계산해서
    # 필요한 candle 수를 자동으로 늘린다.

    latest_time = existing_df[
        "candle_date_time_utc"
    ].max()

    now = pd.Timestamp.now(tz="UTC")

    interval_seconds = safe_int(
        TIMEFRAMES[timeframe][
            "interval_seconds"
        ],
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

    # 최신 candle 재확인을 위해 여유분 추가
    target_count = max(
        20,
        missing_estimate + 10,
    )

    # 200개보다 많으면 pagination으로 가져온다.
    if target_count <= API_MAX_COUNT:

        return fetch_candle_batch(
            market=market,
            timeframe=timeframe,
            count=target_count,
        )

    frames: list[pd.DataFrame] = []

    collected = 0
    to_value: str | None = None

    # 기존 마지막 시점보다 충분히 이전 candle까지
    # 확보하면 중단한다.
    stop_time = (
        latest_time
        - pd.Timedelta(
            seconds=interval_seconds * 5
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
            - pd.Timedelta(seconds=1)
        )

        to_value = next_to.strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )

        if len(batch) < API_MAX_COUNT:
            break

        # 비정상적으로 많은 API 요청 방지
        if collected >= 10000:
            print(
                f"[WARN] Update safety limit reached: "
                f"{market} {timeframe}"
            )
            break

        time.sleep(REQUEST_SLEEP_SECONDS)

    if not frames:
        return pd.DataFrame(
            columns=OHLCV_COLUMNS
        )

    result = pd.concat(
        frames,
        ignore_index=True,
    )

    result = result.drop_duplicates(
        subset=["candle_date_time_utc"],
        keep="last",
    )

    result = result.sort_values(
        "candle_date_time_utc"
    )

    result = result.reset_index(drop=True)

    return result


# ============================================================
# MERGE
# ============================================================

def merge_ohlcv(
    existing_df: pd.DataFrame,
    new_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    기존 데이터와 신규 데이터를 병합하고
    timestamp 중복을 제거한다.
    """

    if existing_df.empty and new_df.empty:
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

    combined["candle_date_time_utc"] = (
        pd.to_datetime(
            combined["candle_date_time_utc"],
            utc=True,
            errors="coerce",
        )
    )

    combined = combined.dropna(
        subset=["candle_date_time_utc"]
    )

    combined = combined.drop_duplicates(
        subset=["candle_date_time_utc"],
        keep="last",
    )

    combined = combined.sort_values(
        "candle_date_time_utc"
    )

    combined = combined.reset_index(
        drop=True
    )

    return combined[OHLCV_COLUMNS]


# ============================================================
# SAFE CSV SAVE
# ============================================================

def save_dataframe_safely(
    df: pd.DataFrame,
    file_path: Path,
) -> None:
    """
    임시 파일에 먼저 저장한 뒤 원본 파일을 교체한다.

    저장 중 오류가 발생했을 때 기존 정상 CSV가
    손상될 가능성을 줄인다.
    """

    file_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temp_path = file_path.with_suffix(
        file_path.suffix + ".tmp"
    )

    output_df = df.copy()

    if (
        "candle_date_time_utc"
        in output_df.columns
    ):
        output_df[
            "candle_date_time_utc"
        ] = output_df[
            "candle_date_time_utc"
        ].apply(
            lambda x: (
                x.isoformat()
                if pd.notna(x)
                else ""
            )
        )

    output_df.to_csv(
        temp_path,
        index=False,
        encoding="utf-8-sig",
    )

    temp_path.replace(file_path)


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
    """
    collector_status.csv에 처리 결과를 누적한다.
    """

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
    """
    마켓 하나 + 시간봉 하나를 수집한다.

    Returns:
        (success, added_rows)
    """

    directory: Path = TIMEFRAMES[
        timeframe
    ]["directory"]

    file_path = directory / f"{market}.csv"

    existing_df = load_existing_csv(
        file_path
    )

    rows_before = len(existing_df)

    try:

        if existing_df.empty:

            print(
                f"    [{timeframe}] "
                f"initial history..."
            )

            new_df = fetch_initial_history(
                market=market,
                timeframe=timeframe,
                target_count=(
                    INITIAL_CANDLE_LIMITS[
                        timeframe
                    ]
                ),
            )

        else:

            new_df = fetch_latest_update(
                market=market,
                timeframe=timeframe,
                existing_df=existing_df,
            )

        merged_df = merge_ohlcv(
            existing_df=existing_df,
            new_df=new_df,
        )

        rows_after = len(merged_df)

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

        return True, added_rows

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

        return False, 0


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
    print("COLLECTOR SUMMARY")
    print_line()

    print(
        f"Project            : {PROJECT_NAME}"
    )

    print(
        f"Version            : {VERSION}"
    )

    print(
        f"KRW markets        : {market_count:,}"
    )

    print(
        f"Successful jobs    : {success_jobs:,}"
    )

    print(
        f"Failed jobs        : {failed_jobs:,}"
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
    print("Output directories:")

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
        f"  STATUS : {STATUS_FILE}"
    )

    print_line()


# ============================================================
# MAIN
# ============================================================

def main() -> int:

    start_time = time.time()

    print_line()
    print(
        f"{PROJECT_NAME} - {VERSION}"
    )
    print_line()

    print(
        f"Started UTC : {utc_now_iso()}"
    )

    print(
        f"Base dir    : {BASE_DIR}"
    )

    print()

    ensure_directories()

    # --------------------------------------------------------
    # KRW MARKET LIST
    # --------------------------------------------------------

    print("[1/2] Loading Upbit KRW markets...")

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

    # --------------------------------------------------------
    # COLLECTION
    # --------------------------------------------------------

    print("[2/2] Collecting OHLCV...")
    print()

    success_jobs = 0
    failed_jobs = 0
    total_added_rows = 0

    total_markets = len(markets)

    for market_index, market in enumerate(
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

            success, added_rows = (
                collect_market_timeframe(
                    market=market,
                    timeframe=timeframe,
                )
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

    # --------------------------------------------------------
    # SUMMARY
    # --------------------------------------------------------

    elapsed_seconds = (
        time.time() - start_time
    )

    show_final_summary(
        market_count=total_markets,
        success_jobs=success_jobs,
        failed_jobs=failed_jobs,
        total_added_rows=(
            total_added_rows
        ),
        elapsed_seconds=elapsed_seconds,
    )

    # 일부 종목 실패가 있어도 나머지 데이터는 보존된다.
    # 실패가 하나라도 있으면 exit code 1을 반환하여
    # GitHub Actions에서 문제를 확인할 수 있도록 한다.

    if failed_jobs > 0:

        print(
            "[RESULT] Completed with "
            "one or more failed jobs."
        )

        return 1

    print(
        "[RESULT] Collector completed "
        "successfully."
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())
