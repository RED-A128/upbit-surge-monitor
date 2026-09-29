"""
Upbit Surge Monitor - Clean V002
================================

File:
    build_features.py

Purpose:
    검증 완료된 원본 OHLCV CSV를 읽어
    분석 / 스크리닝 / 백테스트 / 머신러닝에 사용할
    Feature CSV를 별도 생성한다.

Clean V002 scope:
    - data/ohlcv 기준 전체 KRW 마켓 자동 발견 및 처리
    - h1 / h4 / d1 처리
    - 원본 OHLCV는 READ ONLY
    - Feature 데이터는 data/features/ 아래에 별도 저장
    - 미래 데이터를 사용하는 label은 생성하지 않음
    - 자동매매 기능 없음

Clean V002 changes:
    - 256 기법 연구 기반 Feature 확장
    - SMA 112 / SMA 224 추가
    - Close-to-SMA Feature 확장
    - SMA 간 상대 이격도 Feature 추가
    - SMA 1봉 / 3봉 정규화 slope Feature 추가
    - 전체 KRW 마켓 자동 발견
    - h1 / h4 / d1 마켓 집합 사전 일치 검증
    - 256 신호 판정은 수행하지 않음
    - 미래 급등 결과 Label은 생성하지 않음

Input:
    data/ohlcv/h1/KRW-*.csv
    data/ohlcv/h4/KRW-*.csv
    data/ohlcv/d1/KRW-*.csv

Output:
    data/features/h1/KRW-*.csv
    data/features/h4/KRW-*.csv
    data/features/d1/KRW-*.csv

Windows:
    py build_features.py
"""

from __future__ import annotations

import hashlib
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


# ============================================================
# PROJECT CONFIG
# ============================================================

PROJECT_NAME = "Upbit Surge Monitor"
VERSION = "Clean V002"

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"

OHLCV_DIR = DATA_DIR / "ohlcv"
FEATURE_DIR = DATA_DIR / "features"

H1_SOURCE_DIR = OHLCV_DIR / "h1"
H4_SOURCE_DIR = OHLCV_DIR / "h4"
D1_SOURCE_DIR = OHLCV_DIR / "d1"

H1_FEATURE_DIR = FEATURE_DIR / "h1"
H4_FEATURE_DIR = FEATURE_DIR / "h4"
D1_FEATURE_DIR = FEATURE_DIR / "d1"

STATUS_FILE = DATA_DIR / "feature_build_status.csv"

TARGET_MARKETS: list[str] = []

TARGET_TIMEFRAMES = (
    "h1",
    "h4",
    "d1",
)

TIMEFRAMES = {
    "h1": {
        "source_directory": H1_SOURCE_DIR,
        "feature_directory": H1_FEATURE_DIR,
    },
    "h4": {
        "source_directory": H4_SOURCE_DIR,
        "feature_directory": H4_FEATURE_DIR,
    },
    "d1": {
        "source_directory": D1_SOURCE_DIR,
        "feature_directory": D1_FEATURE_DIR,
    },
}


# ============================================================
# SOURCE COLUMNS
# ============================================================

SOURCE_COLUMNS = [
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

NUMERIC_SOURCE_COLUMNS = [
    "timestamp",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "trade_value",
]


# ============================================================
# FEATURE CONFIG
# ============================================================

RETURN_PERIODS = (
    1,
    3,
    6,
    12,
    24,
)

SMA_PERIODS = (
    5,
    10,
    20,
    60,
    112,
    120,
    224,
)

EMA_PERIODS = (
    5,
    10,
    20,
    60,
)

VOLUME_SMA_PERIODS = (
    5,
    20,
)

RSI_PERIOD = 14

MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9

ATR_PERIOD = 14

BB_PERIOD = 20
BB_STD_MULTIPLIER = 2.0

PRICE_POSITION_SMA_PERIODS = (
    5,
    20,
    60,
    112,
    120,
    224,
)

MA_DISTANCE_PAIRS = (
    (5, 20),
    (5, 60),
    (20, 60),
    (5, 112),
    (5, 224),
    (112, 224),
)

MA_SLOPE_PERIODS = (
    5,
    20,
    60,
    112,
    224,
)

MA_SLOPE_LOOKBACKS = (
    1,
    3,
)


# ============================================================
# STATUS
# ============================================================

STATUS_COLUMNS = [
    "run_time_utc",
    "version",
    "market",
    "timeframe",
    "status",
    "source_rows",
    "feature_rows",
    "source_sha256_before",
    "source_sha256_after",
    "message",
]


# ============================================================
# UTILITY
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
    """
    Feature 출력용 디렉터리만 생성한다.

    OHLCV 원본 디렉터리는 생성하거나 수정하지 않는다.
    """
    directories = [
        DATA_DIR,
        FEATURE_DIR,
        H1_FEATURE_DIR,
        H4_FEATURE_DIR,
        D1_FEATURE_DIR,
    ]

    for directory in directories:
        directory.mkdir(
            parents=True,
            exist_ok=True,
        )


def safe_divide(
    numerator: pd.Series,
    denominator: pd.Series,
) -> pd.Series:
    denominator_safe = denominator.replace(
        0,
        np.nan,
    )

    result = numerator / denominator_safe

    return result.replace(
        [
            np.inf,
            -np.inf,
        ],
        np.nan,
    )


def calculate_file_sha256(
    file_path: Path,
) -> str:
    sha256 = hashlib.sha256()

    with file_path.open("rb") as file:
        while True:
            chunk = file.read(
                1024 * 1024
            )

            if not chunk:
                break

            sha256.update(
                chunk
            )

    return sha256.hexdigest()


# ============================================================
# MARKET DISCOVERY
# ============================================================

def discover_target_markets() -> list[str]:
    """
    h1 / h4 / d1 OHLCV 폴더에서 전체 KRW 마켓을 발견한다.

    세 타임프레임의 마켓 집합이 완전히 같지 않으면
    Feature 생성을 시작하지 않는다.
    """

    market_sets: dict[
        str,
        set[str],
    ] = {}

    for timeframe in TARGET_TIMEFRAMES:

        source_directory = TIMEFRAMES[
            timeframe
        ]["source_directory"]

        if not source_directory.exists():
            raise RuntimeError(
                "Source OHLCV directory not found: "
                f"{source_directory}"
            )

        markets = {
            file_path.stem
            for file_path
            in source_directory.glob(
                "KRW-*.csv"
            )
            if file_path.is_file()
        }

        if not markets:
            raise RuntimeError(
                "No KRW OHLCV CSV files found: "
                f"{source_directory}"
            )

        market_sets[
            timeframe
        ] = markets

    reference_timeframe = TARGET_TIMEFRAMES[0]

    reference_markets = market_sets[
        reference_timeframe
    ]

    mismatch_messages: list[str] = []

    for timeframe in TARGET_TIMEFRAMES[1:]:

        current_markets = market_sets[
            timeframe
        ]

        missing = sorted(
            reference_markets
            - current_markets
        )

        extra = sorted(
            current_markets
            - reference_markets
        )

        if missing or extra:

            mismatch_messages.append(
                f"{timeframe}: "
                f"missing={len(missing):,}, "
                f"extra={len(extra):,}"
            )

            if missing:
                mismatch_messages.append(
                    "  missing sample: "
                    + ", ".join(
                        missing[:10]
                    )
                )

            if extra:
                mismatch_messages.append(
                    "  extra sample: "
                    + ", ".join(
                        extra[:10]
                    )
                )

    if mismatch_messages:
        raise RuntimeError(
            "KRW market sets differ between "
            "h1 / h4 / d1.\n"
            + "\n".join(
                mismatch_messages
            )
        )

    return sorted(
        reference_markets
    )


# ============================================================
# SOURCE DATA LOAD
# ============================================================

def load_source_ohlcv(
    file_path: Path,
    expected_market: str,
) -> pd.DataFrame:

    if not file_path.exists():
        raise FileNotFoundError(
            f"Source OHLCV CSV not found: "
            f"{file_path}"
        )

    try:
        df = pd.read_csv(
            file_path,
            encoding="utf-8-sig",
        )

    except pd.errors.EmptyDataError as exc:
        raise RuntimeError(
            f"Source OHLCV CSV is empty: "
            f"{file_path}"
        ) from exc

    if df.empty:
        raise RuntimeError(
            f"Source OHLCV contains no rows: "
            f"{file_path}"
        )

    missing_columns = [
        column
        for column in SOURCE_COLUMNS
        if column not in df.columns
    ]

    if missing_columns:
        raise RuntimeError(
            "Source OHLCV is missing required columns: "
            + ", ".join(
                missing_columns
            )
        )

    df = df[
        SOURCE_COLUMNS
    ].copy()

    market_values = (
        df["market"]
        .dropna()
        .astype(str)
        .str.strip()
        .unique()
        .tolist()
    )

    if not market_values:
        raise RuntimeError(
            "Source OHLCV contains no market value."
        )

    unexpected_markets = [
        market
        for market in market_values
        if market != expected_market
    ]

    if unexpected_markets:
        raise RuntimeError(
            f"Unexpected market values in "
            f"{file_path.name}: "
            f"{unexpected_markets}"
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

    invalid_time_rows = int(
        df[
            "candle_date_time_utc"
        ].isna().sum()
    )

    if invalid_time_rows > 0:
        raise RuntimeError(
            "Invalid candle_date_time_utc rows: "
            f"{invalid_time_rows:,}"
        )

    for column in NUMERIC_SOURCE_COLUMNS:

        df[column] = pd.to_numeric(
            df[column],
            errors="coerce",
        )

    critical_numeric_columns = [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "trade_value",
    ]

    for column in critical_numeric_columns:

        invalid_count = int(
            df[column]
            .isna()
            .sum()
        )

        if invalid_count > 0:
            raise RuntimeError(
                f"Invalid numeric values in "
                f"{column}: "
                f"{invalid_count:,}"
            )

    for column in (
        "open",
        "high",
        "low",
        "close",
    ):
        if (
            df[column] <= 0
        ).any():
            raise RuntimeError(
                f"Source OHLCV contains "
                f"{column} <= 0."
            )

    if (
        df["volume"] < 0
    ).any():
        raise RuntimeError(
            "Source OHLCV contains volume < 0."
        )

    if (
        df["trade_value"] < 0
    ).any():
        raise RuntimeError(
            "Source OHLCV contains trade_value < 0."
        )

    price_max = df[
        [
            "open",
            "close",
            "low",
        ]
    ].max(
        axis=1
    )

    if (
        df["high"] < price_max
    ).any():
        raise RuntimeError(
            "Source OHLCV contains invalid "
            "high price relationship."
        )

    price_min = df[
        [
            "open",
            "close",
            "high",
        ]
    ].min(
        axis=1
    )

    if (
        df["low"] > price_min
    ).any():
        raise RuntimeError(
            "Source OHLCV contains invalid "
            "low price relationship."
        )

    duplicate_count = int(
        df.duplicated(
            subset=[
                "candle_date_time_utc"
            ],
            keep=False,
        ).sum()
    )

    if duplicate_count > 0:
        raise RuntimeError(
            "Duplicate UTC candle timestamps detected: "
            f"{duplicate_count:,}"
        )

    df = (
        df.sort_values(
            "candle_date_time_utc"
        )
        .reset_index(
            drop=True
        )
    )

    if not df[
        "candle_date_time_utc"
    ].is_monotonic_increasing:
        raise RuntimeError(
            "Source OHLCV time order is invalid."
        )

    return df


# ============================================================
# RETURN
# ============================================================

def add_return_features(
    df: pd.DataFrame,
) -> pd.DataFrame:

    for period in RETURN_PERIODS:

        df[
            f"return_{period}"
        ] = df[
            "close"
        ].pct_change(
            periods=period,
            fill_method=None,
        )

    return df


# ============================================================
# MOVING AVERAGE
# ============================================================

def add_moving_average_features(
    df: pd.DataFrame,
) -> pd.DataFrame:

    close = df["close"]

    for period in SMA_PERIODS:

        df[
            f"sma_{period}"
        ] = close.rolling(
            window=period,
            min_periods=period,
        ).mean()

    for period in EMA_PERIODS:

        df[
            f"ema_{period}"
        ] = close.ewm(
            span=period,
            adjust=False,
            min_periods=period,
        ).mean()

    return df


# ============================================================
# RSI
# ============================================================

def add_rsi_feature(
    df: pd.DataFrame,
) -> pd.DataFrame:

    delta = df[
        "close"
    ].diff()

    gain = delta.clip(
        lower=0.0
    )

    loss = -delta.clip(
        upper=0.0
    )

    alpha = (
        1.0
        / RSI_PERIOD
    )

    average_gain = gain.ewm(
        alpha=alpha,
        adjust=False,
        min_periods=RSI_PERIOD,
    ).mean()

    average_loss = loss.ewm(
        alpha=alpha,
        adjust=False,
        min_periods=RSI_PERIOD,
    ).mean()

    relative_strength = safe_divide(
        average_gain,
        average_loss,
    )

    rsi = (
        100.0
        - (
            100.0
            / (
                1.0
                + relative_strength
            )
        )
    )

    only_gain_mask = (
        (average_loss == 0)
        & (average_gain > 0)
    )

    rsi.loc[
        only_gain_mask
    ] = 100.0

    flat_mask = (
        (average_loss == 0)
        & (average_gain == 0)
    )

    rsi.loc[
        flat_mask
    ] = 50.0

    df[
        f"rsi_{RSI_PERIOD}"
    ] = rsi

    return df


# ============================================================
# MACD
# ============================================================

def add_macd_features(
    df: pd.DataFrame,
) -> pd.DataFrame:

    close = df["close"]

    ema_fast = close.ewm(
        span=MACD_FAST,
        adjust=False,
        min_periods=MACD_FAST,
    ).mean()

    ema_slow = close.ewm(
        span=MACD_SLOW,
        adjust=False,
        min_periods=MACD_SLOW,
    ).mean()

    macd = (
        ema_fast
        - ema_slow
    )

    signal = macd.ewm(
        span=MACD_SIGNAL,
        adjust=False,
        min_periods=MACD_SIGNAL,
    ).mean()

    histogram = (
        macd
        - signal
    )

    df["macd"] = macd
    df["macd_signal"] = signal
    df["macd_hist"] = histogram

    return df


# ============================================================
# ATR
# ============================================================

def add_atr_feature(
    df: pd.DataFrame,
) -> pd.DataFrame:

    previous_close = (
        df["close"].shift(1)
    )

    true_range_1 = (
        df["high"]
        - df["low"]
    )

    true_range_2 = (
        df["high"]
        - previous_close
    ).abs()

    true_range_3 = (
        df["low"]
        - previous_close
    ).abs()

    true_range = pd.concat(
        [
            true_range_1,
            true_range_2,
            true_range_3,
        ],
        axis=1,
    ).max(
        axis=1
    )

    atr = true_range.ewm(
        alpha=(
            1.0
            / ATR_PERIOD
        ),
        adjust=False,
        min_periods=ATR_PERIOD,
    ).mean()

    df[
        f"atr_{ATR_PERIOD}"
    ] = atr

    df[
        f"atr_pct_{ATR_PERIOD}"
    ] = safe_divide(
        atr,
        df["close"],
    )

    return df


# ============================================================
# BOLLINGER BANDS
# ============================================================

def add_bollinger_features(
    df: pd.DataFrame,
) -> pd.DataFrame:

    close = df["close"]

    middle = close.rolling(
        window=BB_PERIOD,
        min_periods=BB_PERIOD,
    ).mean()

    std = close.rolling(
        window=BB_PERIOD,
        min_periods=BB_PERIOD,
    ).std(
        ddof=0
    )

    upper = (
        middle
        + (
            BB_STD_MULTIPLIER
            * std
        )
    )

    lower = (
        middle
        - (
            BB_STD_MULTIPLIER
            * std
        )
    )

    width = safe_divide(
        upper - lower,
        middle,
    )

    position = safe_divide(
        close - lower,
        upper - lower,
    )

    df["bb_mid"] = middle
    df["bb_upper"] = upper
    df["bb_lower"] = lower
    df["bb_width"] = width
    df["bb_position"] = position

    return df


# ============================================================
# VOLUME
# ============================================================

def add_volume_features(
    df: pd.DataFrame,
) -> pd.DataFrame:

    volume = df[
        "volume"
    ]

    for period in VOLUME_SMA_PERIODS:

        volume_sma = volume.rolling(
            window=period,
            min_periods=period,
        ).mean()

        df[
            f"volume_sma_{period}"
        ] = volume_sma

        df[
            f"volume_ratio_{period}"
        ] = safe_divide(
            volume,
            volume_sma,
        )

    return df


# ============================================================
# CANDLE
# ============================================================

def add_candle_features(
    df: pd.DataFrame,
) -> pd.DataFrame:

    open_price = df["open"]
    high_price = df["high"]
    low_price = df["low"]
    close_price = df["close"]

    candle_range = (
        high_price
        - low_price
    )

    candle_body = (
        close_price
        - open_price
    )

    absolute_body = (
        candle_body.abs()
    )

    upper_body_price = pd.concat(
        [
            open_price,
            close_price,
        ],
        axis=1,
    ).max(
        axis=1
    )

    lower_body_price = pd.concat(
        [
            open_price,
            close_price,
        ],
        axis=1,
    ).min(
        axis=1
    )

    upper_wick = (
        high_price
        - upper_body_price
    )

    lower_wick = (
        lower_body_price
        - low_price
    )

    df[
        "candle_return"
    ] = safe_divide(
        close_price - open_price,
        open_price,
    )

    df[
        "candle_range_pct"
    ] = safe_divide(
        candle_range,
        open_price,
    )

    df[
        "candle_body_pct"
    ] = safe_divide(
        absolute_body,
        open_price,
    )

    df[
        "upper_wick_pct"
    ] = safe_divide(
        upper_wick,
        open_price,
    )

    df[
        "lower_wick_pct"
    ] = safe_divide(
        lower_wick,
        open_price,
    )

    df[
        "body_to_range"
    ] = safe_divide(
        absolute_body,
        candle_range,
    )

    df[
        "close_position_in_range"
    ] = safe_divide(
        close_price - low_price,
        candle_range,
    )

    df[
        "is_bullish"
    ] = (
        close_price
        > open_price
    ).astype(
        "int8"
    )

    df[
        "is_bearish"
    ] = (
        close_price
        < open_price
    ).astype(
        "int8"
    )

    return df


# ============================================================
# TRADE VALUE
# ============================================================

def add_trade_value_features(
    df: pd.DataFrame,
) -> pd.DataFrame:

    trade_value = df[
        "trade_value"
    ]

    trade_value_sma_5 = (
        trade_value.rolling(
            window=5,
            min_periods=5,
        ).mean()
    )

    trade_value_sma_20 = (
        trade_value.rolling(
            window=20,
            min_periods=20,
        ).mean()
    )

    df[
        "trade_value_sma_5"
    ] = trade_value_sma_5

    df[
        "trade_value_sma_20"
    ] = trade_value_sma_20

    df[
        "trade_value_ratio_5"
    ] = safe_divide(
        trade_value,
        trade_value_sma_5,
    )

    df[
        "trade_value_ratio_20"
    ] = safe_divide(
        trade_value,
        trade_value_sma_20,
    )

    return df


# ============================================================
# PRICE POSITION
# ============================================================

def add_price_position_features(
    df: pd.DataFrame,
) -> pd.DataFrame:

    close = df[
        "close"
    ]

    for period in PRICE_POSITION_SMA_PERIODS:

        column_name = (
            f"sma_{period}"
        )

        if column_name not in df.columns:
            raise RuntimeError(
                "Required SMA column does not exist "
                "for price-position feature: "
                f"{column_name}"
            )

        df[
            f"close_to_sma_{period}"
        ] = (
            safe_divide(
                close,
                df[column_name],
            )
            - 1.0
        )

    return df


# ============================================================
# 256 RESEARCH - MA DISTANCE
# ============================================================

def add_ma_distance_features(
    df: pd.DataFrame,
) -> pd.DataFrame:

    for (
        short_period,
        long_period,
    ) in MA_DISTANCE_PAIRS:

        short_column = (
            f"sma_{short_period}"
        )

        long_column = (
            f"sma_{long_period}"
        )

        if short_column not in df.columns:
            raise RuntimeError(
                "Required SMA column does not exist: "
                f"{short_column}"
            )

        if long_column not in df.columns:
            raise RuntimeError(
                "Required SMA column does not exist: "
                f"{long_column}"
            )

        feature_name = (
            f"sma_{short_period}"
            f"_to_{long_period}"
        )

        df[
            feature_name
        ] = (
            safe_divide(
                df[short_column],
                df[long_column],
            )
            - 1.0
        )

    return df


# ============================================================
# 256 RESEARCH - MA SLOPE
# ============================================================

def add_ma_slope_features(
    df: pd.DataFrame,
) -> pd.DataFrame:

    for period in MA_SLOPE_PERIODS:

        sma_column = (
            f"sma_{period}"
        )

        if sma_column not in df.columns:
            raise RuntimeError(
                "Required SMA column does not exist "
                "for slope feature: "
                f"{sma_column}"
            )

        current_sma = df[
            sma_column
        ]

        for lookback in MA_SLOPE_LOOKBACKS:

            previous_sma = (
                current_sma.shift(
                    lookback
                )
            )

            feature_name = (
                f"sma_{period}"
                f"_slope_{lookback}"
            )

            df[
                feature_name
            ] = (
                safe_divide(
                    current_sma,
                    previous_sma,
                )
                - 1.0
            )

    return df


# ============================================================
# RECENT HIGH / LOW
# ============================================================

def add_recent_range_features(
    df: pd.DataFrame,
) -> pd.DataFrame:

    close = df[
        "close"
    ]

    for period in (
        20,
        60,
    ):

        rolling_high = (
            df["high"]
            .rolling(
                window=period,
                min_periods=period,
            )
            .max()
        )

        rolling_low = (
            df["low"]
            .rolling(
                window=period,
                min_periods=period,
            )
            .min()
        )

        df[
            f"high_{period}"
        ] = rolling_high

        df[
            f"low_{period}"
        ] = rolling_low

        df[
            f"close_to_high_{period}"
        ] = (
            safe_divide(
                close,
                rolling_high,
            )
            - 1.0
        )

        df[
            f"close_to_low_{period}"
        ] = (
            safe_divide(
                close,
                rolling_low,
            )
            - 1.0
        )

        df[
            f"range_position_{period}"
        ] = safe_divide(
            close - rolling_low,
            rolling_high - rolling_low,
        )

    return df


# ============================================================
# FEATURE BUILD
# ============================================================

def build_features(
    source_df: pd.DataFrame,
) -> pd.DataFrame:

    df = source_df.copy()

    df = add_return_features(
        df
    )

    df = add_moving_average_features(
        df
    )

    df = add_rsi_feature(
        df
    )

    df = add_macd_features(
        df
    )

    df = add_atr_feature(
        df
    )

    df = add_bollinger_features(
        df
    )

    df = add_volume_features(
        df
    )

    df = add_candle_features(
        df
    )

    df = add_trade_value_features(
        df
    )

    df = add_price_position_features(
        df
    )

    df = add_ma_distance_features(
        df
    )

    df = add_ma_slope_features(
        df
    )

    df = add_recent_range_features(
        df
    )

    df = df.replace(
        [
            np.inf,
            -np.inf,
        ],
        np.nan,
    )

    return df


# ============================================================
# REQUIRED FEATURE COLUMNS
# ============================================================

def get_required_feature_columns() -> list[str]:

    return [
        "return_1",
        "return_3",
        "return_6",
        "return_12",
        "return_24",

        "sma_5",
        "sma_10",
        "sma_20",
        "sma_60",
        "sma_112",
        "sma_120",
        "sma_224",

        "ema_5",
        "ema_10",
        "ema_20",
        "ema_60",

        "rsi_14",

        "macd",
        "macd_signal",
        "macd_hist",

        "atr_14",
        "atr_pct_14",

        "bb_mid",
        "bb_upper",
        "bb_lower",
        "bb_width",
        "bb_position",

        "volume_sma_5",
        "volume_sma_20",
        "volume_ratio_5",
        "volume_ratio_20",

        "candle_return",
        "candle_range_pct",
        "candle_body_pct",
        "upper_wick_pct",
        "lower_wick_pct",
        "body_to_range",
        "close_position_in_range",
        "is_bullish",
        "is_bearish",

        "trade_value_sma_5",
        "trade_value_sma_20",
        "trade_value_ratio_5",
        "trade_value_ratio_20",

        "close_to_sma_5",
        "close_to_sma_20",
        "close_to_sma_60",
        "close_to_sma_112",
        "close_to_sma_120",
        "close_to_sma_224",

        "sma_5_to_20",
        "sma_5_to_60",
        "sma_20_to_60",
        "sma_5_to_112",
        "sma_5_to_224",
        "sma_112_to_224",

        "sma_5_slope_1",
        "sma_5_slope_3",
        "sma_20_slope_1",
        "sma_20_slope_3",
        "sma_60_slope_1",
        "sma_60_slope_3",
        "sma_112_slope_1",
        "sma_112_slope_3",
        "sma_224_slope_1",
        "sma_224_slope_3",

        "high_20",
        "low_20",
        "close_to_high_20",
        "close_to_low_20",
        "range_position_20",

        "high_60",
        "low_60",
        "close_to_high_60",
        "close_to_low_60",
        "range_position_60",
    ]


# ============================================================
# FEATURE VALIDATION
# ============================================================

def validate_feature_dataframe(
    source_df: pd.DataFrame,
    feature_df: pd.DataFrame,
    expected_market: str,
) -> None:

    if feature_df.empty:
        raise RuntimeError(
            "Generated feature DataFrame is empty."
        )

    if len(
        feature_df
    ) != len(
        source_df
    ):
        raise RuntimeError(
            "Feature row count changed unexpectedly: "
            f"source={len(source_df):,}, "
            f"feature={len(feature_df):,}"
        )

    missing_source_columns = [
        column
        for column in SOURCE_COLUMNS
        if column not in feature_df.columns
    ]

    if missing_source_columns:
        raise RuntimeError(
            "Generated feature data is missing "
            "source columns: "
            + ", ".join(
                missing_source_columns
            )
        )

    required_features = (
        get_required_feature_columns()
    )

    missing_features = [
        column
        for column in required_features
        if column not in feature_df.columns
    ]

    if missing_features:
        raise RuntimeError(
            "Generated feature data is missing "
            "required features: "
            + ", ".join(
                missing_features
            )
        )

    market_values = (
        feature_df["market"]
        .dropna()
        .astype(str)
        .str.strip()
        .unique()
        .tolist()
    )

    if market_values != [
        expected_market
    ]:
        raise RuntimeError(
            "Generated feature data contains "
            "unexpected market values: "
            f"{market_values}"
        )

    duplicate_count = int(
        feature_df.duplicated(
            subset=[
                "candle_date_time_utc"
            ],
            keep=False,
        ).sum()
    )

    if duplicate_count > 0:
        raise RuntimeError(
            "Generated feature data contains "
            "duplicate UTC timestamps: "
            f"{duplicate_count:,}"
        )

    if not feature_df[
        "candle_date_time_utc"
    ].is_monotonic_increasing:
        raise RuntimeError(
            "Generated feature data is not "
            "sorted by UTC timestamp."
        )

    for column in SOURCE_COLUMNS:

        source_series = (
            source_df[column]
            .reset_index(
                drop=True
            )
        )

        feature_series = (
            feature_df[column]
            .reset_index(
                drop=True
            )
        )

        if column == "candle_date_time_utc":

            source_values = pd.to_datetime(
                source_series,
                utc=True,
                errors="coerce",
            )

            feature_values = pd.to_datetime(
                feature_series,
                utc=True,
                errors="coerce",
            )

            if not source_values.equals(
                feature_values
            ):
                raise RuntimeError(
                    "Source UTC timestamps changed "
                    "during feature generation."
                )

        elif column in NUMERIC_SOURCE_COLUMNS:

            source_values = pd.to_numeric(
                source_series,
                errors="coerce",
            ).to_numpy(
                dtype=float
            )

            feature_values = pd.to_numeric(
                feature_series,
                errors="coerce",
            ).to_numpy(
                dtype=float
            )

            if not np.allclose(
                source_values,
                feature_values,
                equal_nan=True,
            ):
                raise RuntimeError(
                    "Source numeric column changed "
                    "during feature generation: "
                    f"{column}"
                )

        else:

            source_values = (
                source_series
                .astype(str)
                .tolist()
            )

            feature_values = (
                feature_series
                .astype(str)
                .tolist()
            )

            if (
                source_values
                != feature_values
            ):
                raise RuntimeError(
                    "Source column changed during "
                    "feature generation: "
                    f"{column}"
                )

    numeric_feature_df = (
        feature_df.select_dtypes(
            include=[
                np.number
            ]
        )
    )

    numeric_values = (
        numeric_feature_df.to_numpy(
            dtype=float
        )
    )

    if np.isinf(
        numeric_values
    ).any():
        raise RuntimeError(
            "Generated feature data contains "
            "infinite numeric values."
        )


# ============================================================
# SAVE FEATURE DATA
# ============================================================

def save_feature_dataframe(
    feature_df: pd.DataFrame,
    output_file: Path,
) -> None:

    output_file.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temporary_file = (
        output_file.parent
        / (
            output_file.name
            + ".tmp"
        )
    )

    save_df = (
        feature_df.copy()
    )

    save_df[
        "candle_date_time_utc"
    ] = save_df[
        "candle_date_time_utc"
    ].apply(
        lambda value: (
            value.isoformat()
            if pd.notna(value)
            else ""
        )
    )

    try:

        save_df.to_csv(
            temporary_file,
            index=False,
            encoding="utf-8-sig",
        )

        temporary_file.replace(
            output_file
        )

    finally:

        if temporary_file.exists():
            temporary_file.unlink(
                missing_ok=True
            )


# ============================================================
# STATUS LOG
# ============================================================

def append_feature_status(
    *,
    market: str,
    timeframe: str,
    status: str,
    source_rows: int,
    feature_rows: int,
    source_sha256_before: str,
    source_sha256_after: str,
    message: str,
) -> None:

    DATA_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    row = {
        "run_time_utc": utc_now_iso(),
        "version": VERSION,
        "market": market,
        "timeframe": timeframe,
        "status": status,
        "source_rows": source_rows,
        "feature_rows": feature_rows,
        "source_sha256_before": source_sha256_before,
        "source_sha256_after": source_sha256_after,
        "message": message,
    }

    status_df = pd.DataFrame(
        [
            row
        ],
        columns=STATUS_COLUMNS,
    )

    write_header = (
        not STATUS_FILE.exists()
    )

    status_df.to_csv(
        STATUS_FILE,
        mode="a",
        header=write_header,
        index=False,
        encoding="utf-8-sig",
    )


# ============================================================
# SINGLE FEATURE JOB
# ============================================================

def build_single_feature_file(
    market: str,
    timeframe: str,
) -> dict[str, Any]:

    if timeframe not in TIMEFRAMES:
        raise ValueError(
            f"Unsupported timeframe: "
            f"{timeframe}"
        )

    timeframe_config = TIMEFRAMES[
        timeframe
    ]

    source_directory = timeframe_config[
        "source_directory"
    ]

    feature_directory = timeframe_config[
        "feature_directory"
    ]

    source_file = (
        source_directory
        / f"{market}.csv"
    )

    output_file = (
        feature_directory
        / f"{market}.csv"
    )

    print_line(
        "-",
        72,
    )

    print(
        f"[{timeframe.upper()}] "
        f"{market}"
    )

    print(
        f"Source : "
        f"{source_file}"
    )

    print(
        f"Output : "
        f"{output_file}"
    )

    source_rows = 0
    feature_rows = 0

    source_sha256_before = ""
    source_sha256_after = ""

    started = (
        time.perf_counter()
    )

    try:

        source_sha256_before = (
            calculate_file_sha256(
                source_file
            )
        )

        source_df = (
            load_source_ohlcv(
                source_file,
                expected_market=market,
            )
        )

        source_rows = len(
            source_df
        )

        print(
            f"Rows   : "
            f"{source_rows:,}"
        )

        feature_df = (
            build_features(
                source_df
            )
        )

        feature_rows = len(
            feature_df
        )

        validate_feature_dataframe(
            source_df=source_df,
            feature_df=feature_df,
            expected_market=market,
        )

        source_sha256_mid = (
            calculate_file_sha256(
                source_file
            )
        )

        if (
            source_sha256_mid
            != source_sha256_before
        ):
            raise RuntimeError(
                "Source OHLCV changed while "
                "features were being built."
            )

        save_feature_dataframe(
            feature_df,
            output_file,
        )

        saved_df = pd.read_csv(
            output_file,
            encoding="utf-8-sig",
        )

        if len(
            saved_df
        ) != feature_rows:
            raise RuntimeError(
                "Saved feature row count mismatch: "
                f"expected={feature_rows:,}, "
                f"actual={len(saved_df):,}"
            )

        source_sha256_after = (
            calculate_file_sha256(
                source_file
            )
        )

        if (
            source_sha256_after
            != source_sha256_before
        ):
            raise RuntimeError(
                "Source OHLCV file was modified "
                "during feature generation."
            )

        elapsed = (
            time.perf_counter()
            - started
        )

        message = (
            "Feature build completed successfully. "
            "Source OHLCV remained unchanged."
        )

        append_feature_status(
            market=market,
            timeframe=timeframe,
            status="PASSED",
            source_rows=source_rows,
            feature_rows=feature_rows,
            source_sha256_before=source_sha256_before,
            source_sha256_after=source_sha256_after,
            message=message,
        )

        print(
            f"[PASS] "
            f"{feature_rows:,} rows"
        )

        print(
            "[PASS] Source SHA256 unchanged"
        )

        print(
            f"Elapsed: "
            f"{elapsed:.2f}s"
        )

        return {
            "market": market,
            "timeframe": timeframe,
            "status": "PASSED",
            "source_rows": source_rows,
            "feature_rows": feature_rows,
            "output_file": str(
                output_file
            ),
            "message": message,
        }

    except Exception as exc:

        try:

            if source_file.exists():
                source_sha256_after = (
                    calculate_file_sha256(
                        source_file
                    )
                )

        except Exception:
            source_sha256_after = (
                "HASH_CHECK_FAILED"
            )

        message = str(
            exc
        )

        append_feature_status(
            market=market,
            timeframe=timeframe,
            status="FAILED",
            source_rows=source_rows,
            feature_rows=feature_rows,
            source_sha256_before=source_sha256_before,
            source_sha256_after=source_sha256_after,
            message=message,
        )

        print(
            f"[FAIL] "
            f"{message}"
        )

        return {
            "market": market,
            "timeframe": timeframe,
            "status": "FAILED",
            "source_rows": source_rows,
            "feature_rows": feature_rows,
            "output_file": str(
                output_file
            ),
            "message": message,
        }


# ============================================================
# SUMMARY
# ============================================================

def print_summary(
    results: list[
        dict[str, Any]
    ],
) -> None:

    print()

    print_line()

    print(
        "FEATURE BUILD SUMMARY"
    )

    print_line()

    total_jobs = len(
        results
    )

    passed_jobs = sum(
        1
        for result in results
        if result[
            "status"
        ] == "PASSED"
    )

    failed_jobs = (
        total_jobs
        - passed_jobs
    )

    total_source_rows = sum(
        int(
            result.get(
                "source_rows",
                0,
            )
        )
        for result in results
    )

    total_feature_rows = sum(
        int(
            result.get(
                "feature_rows",
                0,
            )
        )
        for result in results
    )

    print(
        f"Project       : "
        f"{PROJECT_NAME}"
    )

    print(
        f"Version       : "
        f"{VERSION}"
    )

    print(
        f"Markets       : "
        f"{len(TARGET_MARKETS):,}"
    )

    print(
        f"Timeframes    : "
        f"{len(TARGET_TIMEFRAMES):,}"
    )

    print(
        f"Total jobs    : "
        f"{total_jobs:,}"
    )

    print(
        f"Passed jobs   : "
        f"{passed_jobs:,}"
    )

    print(
        f"Failed jobs   : "
        f"{failed_jobs:,}"
    )

    print(
        f"Source rows   : "
        f"{total_source_rows:,}"
    )

    print(
        f"Feature rows  : "
        f"{total_feature_rows:,}"
    )

    print()

    print(
        "256 Research Foundation:"
    )

    print(
        "  SMA          : "
        "5 / 10 / 20 / 60 / "
        "112 / 120 / 224"
    )

    print(
        "  Price vs SMA : "
        "5 / 20 / 60 / "
        "112 / 120 / 224"
    )

    print(
        "  MA distance  : "
        "6 pairs"
    )

    print(
        "  MA slope     : "
        "5 / 20 / 60 / 112 / 224 "
        "(1, 3 candle)"
    )

    print(
        "  256 detector : "
        "NOT IMPLEMENTED YET"
    )

    print(
        "  Future label : "
        "NOT IMPLEMENTED"
    )

    print(
        "  Trading      : "
        "DISABLED"
    )

    print()

    for result in results:

        print(
            f"[{result['status']}] "
            f"{result['market']} "
            f"{result['timeframe']} "
            f"source="
            f"{result['source_rows']:,} "
            f"feature="
            f"{result['feature_rows']:,}"
        )

    print_line()


# ============================================================
# MAIN
# ============================================================

def main() -> int:

    total_started = (
        time.perf_counter()
    )

    print_line()

    print(
        f"{PROJECT_NAME} - "
        f"{VERSION}"
    )

    print(
        "STEP 3 FEATURE FOUNDATION"
    )

    print(
        "256 RESEARCH FEATURE EXPANSION"
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
        "  Feature generation only"
    )

    print(
        "  Source OHLCV READ ONLY"
    )

    print(
        "  ALL KRW markets auto-discovery mode"
    )

    print(
        "  h1 / h4 / d1"
    )

    print(
        "  No future labels"
    )

    print(
        "  No 256 signal decision yet"
    )

    print(
        "  No prediction"
    )

    print(
        "  No trading"
    )

    print()

    print(
        "Clean V002 additions:"
    )

    print(
        "  SMA112 / SMA224"
    )

    print(
        "  Extended close-to-SMA"
    )

    print(
        "  Moving-average distance"
    )

    print(
        "  Normalized MA slope"
    )

    print()

    ensure_directories()

    global TARGET_MARKETS

    TARGET_MARKETS = (
        discover_target_markets()
    )

    planned_jobs = (
        len(TARGET_MARKETS)
        * len(TARGET_TIMEFRAMES)
    )

    print(
        f"Discovered KRW markets : "
        f"{len(TARGET_MARKETS):,}"
    )

    print(
        f"Planned jobs           : "
        f"{planned_jobs:,}"
    )

    print()

    if len(
        TARGET_MARKETS
    ) == 0:
        raise RuntimeError(
            "No target markets discovered."
        )

    results: list[
        dict[str, Any]
    ] = []

    total_market_count = len(
        TARGET_MARKETS
    )

    current_job = 0

    for market_index, market in enumerate(
        TARGET_MARKETS,
        start=1,
    ):

        print()
        print_line(
            "=",
            72,
        )

        print(
            f"MARKET "
            f"{market_index:,}"
            f"/"
            f"{total_market_count:,} "
            f"{market}"
        )

        print_line(
            "=",
            72,
        )

        for timeframe in TARGET_TIMEFRAMES:

            current_job += 1

            print(
                f"[JOB "
                f"{current_job:,}"
                f"/"
                f"{planned_jobs:,}]"
            )

            result = (
                build_single_feature_file(
                    market=market,
                    timeframe=timeframe,
                )
            )

            results.append(
                result
            )

    print_summary(
        results
    )

    failed_jobs = [
        result
        for result in results
        if result[
            "status"
        ] != "PASSED"
    ]

    total_elapsed = (
        time.perf_counter()
        - total_started
    )

    print(
        f"Total elapsed : "
        f"{total_elapsed:.2f}s"
    )

    if failed_jobs:

        print()

        print(
            "[FAILED JOBS]"
        )

        for result in failed_jobs:

            print(
                f"  {result['market']} "
                f"{result['timeframe']} : "
                f"{result['message']}"
            )

        print()

        print(
            "[RESULT] FEATURE BUILD FAILED"
        )

        print(
            f"[FAIL] "
            f"{len(failed_jobs):,} / "
            f"{len(results):,} jobs failed."
        )

        return 1

    print()

    print(
        "[RESULT] FEATURE BUILD PASSED"
    )

    print(
        "[PASS] Original OHLCV data "
        "remained unchanged."
    )

    print(
        "[PASS] Clean V002 256 research "
        "foundation features were generated."
    )

    print(
        f"[PASS] "
        f"{len(results):,} / "
        f"{len(results):,} jobs passed."
    )

    print(
        "[NEXT] Full Feature validation."
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
            "[STOP] Interrupted by user."
        )

        exit_code = 130

    except Exception as exc:

        print()

        print_line()

        print(
            "[FATAL] "
            f"{exc}"
        )

        print_line()

        exit_code = 1

    sys.exit(
        exit_code
    )


# ============================================================
# END
# ============================================================
