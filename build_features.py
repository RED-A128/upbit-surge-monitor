"""
Upbit Surge Monitor - Clean V001
================================

File:
    build_features.py

Purpose:
    검증 완료된 원본 OHLCV CSV를 읽어
    분석 / 스크리닝 / 백테스트 / 머신러닝에 사용할
    Feature CSV를 별도 생성한다.

Clean V001 scope:
    - KRW-BTC 단일 마켓 검증
    - h1 / h4 / d1 처리
    - 원본 OHLCV는 READ ONLY
    - Feature 데이터는 data/features/ 아래에 별도 저장
    - 미래 데이터를 사용하는 label은 생성하지 않음
    - 자동매매 기능 없음

Input:
    data/ohlcv/h1/KRW-BTC.csv
    data/ohlcv/h4/KRW-BTC.csv
    data/ohlcv/d1/KRW-BTC.csv

Output:
    data/features/h1/KRW-BTC.csv
    data/features/h4/KRW-BTC.csv
    data/features/d1/KRW-BTC.csv

Windows:
    py build_features.py
"""

from __future__ import annotations

import hashlib
import math
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
VERSION = "Clean V001"

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

# ------------------------------------------------------------
# Clean V001
#
# 첫 단계에서는 KRW-BTC 하나만 처리한다.
#
# 전체 290개 마켓 확장은 V001 검증 완료 후 진행한다.
# ------------------------------------------------------------

TARGET_MARKETS = [
    "KRW-BTC",
]

TARGET_TIMEFRAMES = (
    "h1",
    "h4",
    "d1",
)


# ============================================================
# TIMEFRAME CONFIG
# ============================================================

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
# SOURCE OHLCV COLUMNS
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


# ============================================================
# NUMERIC SOURCE COLUMNS
# ============================================================

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
    120,
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


# ============================================================
# STATUS COLUMNS
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
    Feature 출력 디렉터리만 생성한다.

    OHLCV 원본 디렉터리를 생성하거나 수정하지 않는다.
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
    """
    0으로 나누는 상황에서 inf가 발생하지 않도록
    안전하게 나눈다.
    """

    denominator_safe = (
        denominator.replace(
            0,
            np.nan,
        )
    )

    result = (
        numerator
        / denominator_safe
    )

    result = result.replace(
        [
            np.inf,
            -np.inf,
        ],
        np.nan,
    )

    return result


def calculate_file_sha256(
    file_path: Path,
) -> str:
    """
    파일 SHA256을 계산한다.

    Feature 생성 전/후 원본 OHLCV 파일이
    변경되지 않았는지 확인하기 위해 사용한다.
    """

    sha256 = hashlib.sha256()

    with file_path.open("rb") as file:

        while True:

            chunk = file.read(
                1024 * 1024
            )

            if not chunk:
                break

            sha256.update(chunk)

    return sha256.hexdigest()


# ============================================================
# SOURCE DATA LOAD
# ============================================================

def load_source_ohlcv(
    file_path: Path,
    expected_market: str,
) -> pd.DataFrame:
    """
    원본 OHLCV CSV를 읽고 기본 무결성을 검사한다.

    중요:
        이 함수는 원본 파일을 수정하지 않는다.
    """

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

    # --------------------------------------------------------
    # REQUIRED COLUMNS
    # --------------------------------------------------------

    missing_columns = [
        column
        for column in SOURCE_COLUMNS
        if column not in df.columns
    ]

    if missing_columns:

        raise RuntimeError(
            "Source OHLCV is missing required columns: "
            + ", ".join(missing_columns)
        )

    # --------------------------------------------------------
    # Keep collector.py common column order
    # --------------------------------------------------------

    df = df[
        SOURCE_COLUMNS
    ].copy()

    # --------------------------------------------------------
    # MARKET VALIDATION
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # UTC TIME
    # --------------------------------------------------------

    df[
        "candle_date_time_utc"
    ] = pd.to_datetime(
        df["candle_date_time_utc"],
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
            f"Invalid candle_date_time_utc rows: "
            f"{invalid_time_rows:,}"
        )

    # --------------------------------------------------------
    # NUMERIC CONVERSION
    # --------------------------------------------------------

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
            df[column].isna().sum()
        )

        if invalid_count > 0:

            raise RuntimeError(
                f"Invalid numeric values in "
                f"{column}: "
                f"{invalid_count:,}"
            )

    # --------------------------------------------------------
    # PRICE / VOLUME SANITY
    # --------------------------------------------------------

    if (
        df["open"] <= 0
    ).any():

        raise RuntimeError(
            "Source OHLCV contains "
            "open <= 0."
        )

    if (
        df["high"] <= 0
    ).any():

        raise RuntimeError(
            "Source OHLCV contains "
            "high <= 0."
        )

    if (
        df["low"] <= 0
    ).any():

        raise RuntimeError(
            "Source OHLCV contains "
            "low <= 0."
        )

    if (
        df["close"] <= 0
    ).any():

        raise RuntimeError(
            "Source OHLCV contains "
            "close <= 0."
        )

    if (
        df["volume"] < 0
    ).any():

        raise RuntimeError(
            "Source OHLCV contains "
            "volume < 0."
        )

    if (
        df["trade_value"] < 0
    ).any():

        raise RuntimeError(
            "Source OHLCV contains "
            "trade_value < 0."
        )

    # --------------------------------------------------------
    # OHLC RELATIONSHIP
    # --------------------------------------------------------

    price_max = df[
        [
            "open",
            "close",
            "low",
        ]
    ].max(axis=1)

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
    ].min(axis=1)

    if (
        df["low"] > price_min
    ).any():

        raise RuntimeError(
            "Source OHLCV contains invalid "
            "low price relationship."
        )

    # --------------------------------------------------------
    # DUPLICATE TIMESTAMP
    # --------------------------------------------------------

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
            f"Duplicate UTC candle timestamps "
            f"detected: "
            f"{duplicate_count:,}"
        )

    # --------------------------------------------------------
    # SORT
    # --------------------------------------------------------

    df = df.sort_values(
        "candle_date_time_utc"
    )

    df = df.reset_index(
        drop=True
    )

    if not df[
        "candle_date_time_utc"
    ].is_monotonic_increasing:

        raise RuntimeError(
            "Source OHLCV time order is invalid."
        )

    return df


# ============================================================
# RETURN FEATURES
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
# MOVING AVERAGE FEATURES
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
    """
    Wilder-style RSI 14.

    현재 및 과거 close만 사용한다.
    미래 데이터는 사용하지 않는다.
    """

    delta = df[
        "close"
    ].diff()

    gain = delta.clip(
        lower=0.0
    )

    loss = (
        -delta.clip(
            upper=0.0
        )
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

    # 상승만 존재하여 average_loss == 0인 경우
    # RSI는 100으로 처리한다.
    only_gain_mask = (
        (average_loss == 0)
        & (average_gain > 0)
    )

    rsi.loc[
        only_gain_mask
    ] = 100.0

    # 상승/하락이 모두 0인 완전 평탄 구간은
    # 중립값 50으로 처리한다.
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
    ).max(axis=1)

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
# VOLUME FEATURES
# ============================================================

def add_volume_features(
    df: pd.DataFrame,
) -> pd.DataFrame:

    volume = df["volume"]

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

    df[
        "volume_change_1"
    ] = volume.pct_change(
        periods=1,
        fill_method=None,
    )

    df[
        "trade_value_change_1"
    ] = df[
        "trade_value"
    ].pct_change(
        periods=1,
        fill_method=None,
    )

    return df


# ============================================================
# CANDLE STRUCTURE FEATURES
# ============================================================

def add_candle_features(
    df: pd.DataFrame,
) -> pd.DataFrame:

    candle_range = (
        df["high"]
        - df["low"]
    )

    candle_body = (
        df["close"]
        - df["open"]
    )

    absolute_body = (
        candle_body.abs()
    )

    candle_top = df[
        [
            "open",
            "close",
        ]
    ].max(axis=1)

    candle_bottom = df[
        [
            "open",
            "close",
        ]
    ].min(axis=1)

    upper_wick = (
        df["high"]
        - candle_top
    )

    lower_wick = (
        candle_bottom
        - df["low"]
    )

    df["body"] = candle_body

    df["body_abs"] = (
        absolute_body
    )

    df["body_pct"] = safe_divide(
        candle_body,
        df["open"],
    )

    df["range"] = candle_range

    df["range_pct"] = safe_divide(
        candle_range,
        df["open"],
    )

    df["upper_wick"] = upper_wick

    df["lower_wick"] = lower_wick

    df[
        "upper_wick_ratio"
    ] = safe_divide(
        upper_wick,
        candle_range,
    )

    df[
        "lower_wick_ratio"
    ] = safe_divide(
        lower_wick,
        candle_range,
    )

    df[
        "body_ratio"
    ] = safe_divide(
        absolute_body,
        candle_range,
    )

    df[
        "candle_direction"
    ] = np.select(
        [
            candle_body > 0,
            candle_body < 0,
        ],
        [
            1,
            -1,
        ],
        default=0,
    )

    return df


# ============================================================
# PRICE POSITION FEATURES
# ============================================================

def add_price_position_features(
    df: pd.DataFrame,
) -> pd.DataFrame:

    for period in (
        20,
        60,
        120,
    ):

        sma_column = (
            f"sma_{period}"
        )

        if sma_column not in df.columns:
            continue

        df[
            f"close_to_sma_{period}"
        ] = (
            safe_divide(
                df["close"],
                df[sma_column],
            )
            - 1.0
        )

    return df


# ============================================================
# ROLLING HIGH / LOW FEATURES
# ============================================================

def add_rolling_range_features(
    df: pd.DataFrame,
) -> pd.DataFrame:

    for period in (
        20,
        60,
    ):

        rolling_high = df[
            "high"
        ].rolling(
            window=period,
            min_periods=period,
        ).max()

        rolling_low = df[
            "low"
        ].rolling(
            window=period,
            min_periods=period,
        ).min()

        df[
            f"rolling_high_{period}"
        ] = rolling_high

        df[
            f"rolling_low_{period}"
        ] = rolling_low

        df[
            f"close_to_high_{period}"
        ] = (
            safe_divide(
                df["close"],
                rolling_high,
            )
            - 1.0
        )

        df[
            f"close_to_low_{period}"
        ] = (
            safe_divide(
                df["close"],
                rolling_low,
            )
            - 1.0
        )

        df[
            f"range_position_{period}"
        ] = safe_divide(
            (
                df["close"]
                - rolling_low
            ),
            (
                rolling_high
                - rolling_low
            ),
        )

    return df


# ============================================================
# FEATURE BUILD
# ============================================================

def build_features(
    source_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    OHLCV에서 Feature를 생성한다.

    모든 Feature는 현재 row 또는 과거 row만 사용한다.

    미래 shift(-N)는 사용하지 않는다.
    """

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

    df = add_price_position_features(
        df
    )

    df = add_rolling_range_features(
        df
    )

    # --------------------------------------------------------
    # Normalize infinities
    # --------------------------------------------------------

    numeric_columns = df.select_dtypes(
        include=[
            np.number,
        ]
    ).columns

    df[
        numeric_columns
    ] = df[
        numeric_columns
    ].replace(
        [
            np.inf,
            -np.inf,
        ],
        np.nan,
    )

    return df


# ============================================================
# FEATURE VALIDATION
# ============================================================

def validate_feature_dataframe(
    source_df: pd.DataFrame,
    feature_df: pd.DataFrame,
    market: str,
    timeframe: str,
) -> None:
    """
    Feature 저장 전 기본 무결성을 검사한다.

    정밀 Look-ahead 검증은 이후
    validate_features.py에서 별도로 수행한다.
    """

    if feature_df.empty:

        raise RuntimeError(
            "Feature DataFrame is empty."
        )

    # --------------------------------------------------------
    # ROW COUNT
    # --------------------------------------------------------

    if len(feature_df) != len(
        source_df
    ):

        raise RuntimeError(
            "Feature row count changed: "
            f"source={len(source_df):,}, "
            f"feature={len(feature_df):,}"
        )

    # --------------------------------------------------------
    # MARKET
    # --------------------------------------------------------

    feature_markets = (
        feature_df["market"]
        .dropna()
        .astype(str)
        .unique()
        .tolist()
    )

    if feature_markets != [
        market
    ]:

        raise RuntimeError(
            f"Unexpected feature market values: "
            f"{feature_markets}"
        )

    # --------------------------------------------------------
    # TIMESTAMP ALIGNMENT
    # --------------------------------------------------------

    source_time = (
        source_df[
            "candle_date_time_utc"
        ]
        .reset_index(drop=True)
    )

    feature_time = (
        feature_df[
            "candle_date_time_utc"
        ]
        .reset_index(drop=True)
    )

    if not source_time.equals(
        feature_time
    ):

        raise RuntimeError(
            "Feature timestamps do not match "
            "source timestamps."
        )

    # --------------------------------------------------------
    # DUPLICATE
    # --------------------------------------------------------

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
            f"Duplicate feature timestamps: "
            f"{duplicate_count:,}"
        )

    # --------------------------------------------------------
    # SORT
    # --------------------------------------------------------

    if not feature_df[
        "candle_date_time_utc"
    ].is_monotonic_increasing:

        raise RuntimeError(
            "Feature timestamps are not "
            "monotonic increasing."
        )

    # --------------------------------------------------------
    # SOURCE OHLCV VALUES MUST REMAIN IDENTICAL
    # --------------------------------------------------------

    source_compare_columns = [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "trade_value",
    ]

    for column in source_compare_columns:

        source_values = (
            source_df[column]
            .to_numpy(
                dtype=float
            )
        )

        feature_values = (
            feature_df[column]
            .to_numpy(
                dtype=float
            )
        )

        if not np.allclose(
            source_values,
            feature_values,
            equal_nan=True,
            rtol=0.0,
            atol=0.0,
        ):

            raise RuntimeError(
                f"Source column changed during "
                f"feature build: {column}"
            )

    # --------------------------------------------------------
    # INF CHECK
    # --------------------------------------------------------

    numeric_df = feature_df.select_dtypes(
        include=[
            np.number,
        ]
    )

    numeric_values = numeric_df.to_numpy(
        dtype=float,
        copy=False,
    )

    infinity_count = int(
        np.isinf(
            numeric_values
        ).sum()
    )

    if infinity_count > 0:

        raise RuntimeError(
            f"Infinite feature values detected: "
            f"{infinity_count:,}"
        )

    # --------------------------------------------------------
    # REQUIRED FEATURE COLUMNS
    # --------------------------------------------------------

    required_features = [
        "return_1",
        "return_3",
        "return_6",
        "return_12",
        "return_24",
        "sma_5",
        "sma_10",
        "sma_20",
        "sma_60",
        "sma_120",
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
        "volume_change_1",
        "trade_value_change_1",
        "body",
        "body_abs",
        "body_pct",
        "range",
        "range_pct",
        "upper_wick",
        "lower_wick",
        "upper_wick_ratio",
        "lower_wick_ratio",
        "body_ratio",
        "candle_direction",
        "close_to_sma_20",
        "close_to_sma_60",
        "close_to_sma_120",
        "rolling_high_20",
        "rolling_low_20",
        "close_to_high_20",
        "close_to_low_20",
        "range_position_20",
        "rolling_high_60",
        "rolling_low_60",
        "close_to_high_60",
        "close_to_low_60",
        "range_position_60",
    ]

    missing_features = [
        column
        for column in required_features
        if column not in feature_df.columns
    ]

    if missing_features:

        raise RuntimeError(
            "Missing generated features: "
            + ", ".join(
                missing_features
            )
        )

    print(
        f"        Validation : PASS "
        f"({market} {timeframe})"
    )


# ============================================================
# SAFE FEATURE SAVE
# ============================================================

def save_feature_dataframe_safely(
    df: pd.DataFrame,
    file_path: Path,
) -> None:
    """
    Feature CSV를 임시 파일에 먼저 저장한 뒤 교체한다.

    원본 OHLCV에는 접근하지 않는다.
    """

    file_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temp_path = file_path.with_suffix(
        file_path.suffix
        + ".tmp"
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
            lambda value: (
                value.isoformat()
                if pd.notna(value)
                else ""
            )
        )

    output_df.to_csv(
        temp_path,
        index=False,
        encoding="utf-8-sig",
    )

    temp_path.replace(
        file_path
    )


# ============================================================
# STATUS
# ============================================================

def append_status(
    market: str,
    timeframe: str,
    status: str,
    source_rows: int,
    feature_rows: int,
    source_sha256_before: str,
    source_sha256_after: str,
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
                "source_rows": (
                    source_rows
                ),
                "feature_rows": (
                    feature_rows
                ),
                "source_sha256_before": (
                    source_sha256_before
                ),
                "source_sha256_after": (
                    source_sha256_after
                ),
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

def process_market_timeframe(
    market: str,
    timeframe: str,
) -> tuple[
    bool,
    int,
]:
    """
    마켓 하나 + 시간봉 하나의 Feature를 생성한다.

    Returns:
        (success, feature_rows)
    """

    config = TIMEFRAMES[
        timeframe
    ]

    source_directory: Path = (
        config[
            "source_directory"
        ]
    )

    feature_directory: Path = (
        config[
            "feature_directory"
        ]
    )

    source_path = (
        source_directory
        / f"{market}.csv"
    )

    feature_path = (
        feature_directory
        / f"{market}.csv"
    )

    source_rows = 0
    feature_rows = 0

    source_sha256_before = ""
    source_sha256_after = ""

    try:

        print(
            f"    [{timeframe}] "
            f"loading source..."
        )

        # ----------------------------------------------------
        # SOURCE HASH BEFORE
        # ----------------------------------------------------

        source_sha256_before = (
            calculate_file_sha256(
                source_path
            )
        )

        # ----------------------------------------------------
        # LOAD
        # ----------------------------------------------------

        source_df = (
            load_source_ohlcv(
                file_path=source_path,
                expected_market=market,
            )
        )

        source_rows = len(
            source_df
        )

        print(
            f"        Source rows : "
            f"{source_rows:,}"
        )

        print(
            f"        Oldest      : "
            f"{source_df['candle_date_time_utc'].min()}"
        )

        print(
            f"        Newest      : "
            f"{source_df['candle_date_time_utc'].max()}"
        )

        # ----------------------------------------------------
        # BUILD
        # ----------------------------------------------------

        print(
            "        Building features..."
        )

        feature_df = (
            build_features(
                source_df
            )
        )

        feature_rows = len(
            feature_df
        )

        # ----------------------------------------------------
        # VALIDATE BEFORE SAVE
        # ----------------------------------------------------

        validate_feature_dataframe(
            source_df=source_df,
            feature_df=feature_df,
            market=market,
            timeframe=timeframe,
        )

        # ----------------------------------------------------
        # SOURCE HASH BEFORE SAVE
        # ----------------------------------------------------

        source_hash_pre_save = (
            calculate_file_sha256(
                source_path
            )
        )

        if (
            source_hash_pre_save
            != source_sha256_before
        ):

            raise RuntimeError(
                "Source OHLCV changed while "
                "features were being built."
            )

        # ----------------------------------------------------
        # SAVE FEATURE
        # ----------------------------------------------------

        save_feature_dataframe_safely(
            feature_df,
            feature_path,
        )

        # ----------------------------------------------------
        # SOURCE HASH AFTER
        # ----------------------------------------------------

        source_sha256_after = (
            calculate_file_sha256(
                source_path
            )
        )

        if (
            source_sha256_after
            != source_sha256_before
        ):

            raise RuntimeError(
                "CRITICAL SAFETY FAILURE: "
                "Source OHLCV file changed "
                "during feature generation."
            )

        # ----------------------------------------------------
        # SAVED FILE VERIFICATION
        # ----------------------------------------------------

        saved_df = pd.read_csv(
            feature_path,
            encoding="utf-8-sig",
        )

        if len(
            saved_df
        ) != feature_rows:

            raise RuntimeError(
                "Saved feature row count mismatch: "
                f"memory={feature_rows:,}, "
                f"saved={len(saved_df):,}"
            )

        append_status(
            market=market,
            timeframe=timeframe,
            status="SUCCESS",
            source_rows=source_rows,
            feature_rows=feature_rows,
            source_sha256_before=(
                source_sha256_before
            ),
            source_sha256_after=(
                source_sha256_after
            ),
            message="",
        )

        feature_count = (
            len(feature_df.columns)
            - len(SOURCE_COLUMNS)
        )

        print(
            f"        Feature rows: "
            f"{feature_rows:,}"
        )

        print(
            f"        New features: "
            f"{feature_count:,}"
        )

        print(
            f"        Output      : "
            f"{feature_path}"
        )

        print(
            "        Source hash : PASS"
        )

        print(
            f"    [{timeframe}] OK"
        )

        return (
            True,
            feature_rows,
        )

    except Exception as exc:

        message = str(
            exc
        )

        # ----------------------------------------------------
        # Best effort source hash after failure
        # ----------------------------------------------------

        try:

            if source_path.exists():

                source_sha256_after = (
                    calculate_file_sha256(
                        source_path
                    )
                )

        except Exception:

            source_sha256_after = (
                "HASH_CHECK_FAILED"
            )

        append_status(
            market=market,
            timeframe=timeframe,
            status="FAILED",
            source_rows=source_rows,
            feature_rows=feature_rows,
            source_sha256_before=(
                source_sha256_before
            ),
            source_sha256_after=(
                source_sha256_after
            ),
            message=message,
        )

        print(
            f"    [{timeframe}] "
            f"FAILED: "
            f"{message}"
        )

        return (
            False,
            0,
        )


# ============================================================
# FINAL SUMMARY
# ============================================================

def show_final_summary(
    market_count: int,
    success_jobs: int,
    failed_jobs: int,
    total_feature_rows: int,
    elapsed_seconds: float,
) -> None:

    print()

    print_line()

    print(
        "FEATURE BUILD SUMMARY"
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
        f"Target markets     : "
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
        f"Feature rows       : "
        f"{total_feature_rows:,}"
    )

    print(
        f"Elapsed seconds    : "
        f"{elapsed_seconds:,.2f}"
    )

    print()

    print(
        "Input directories:"
    )

    print(
        f"  H1 : "
        f"{H1_SOURCE_DIR}"
    )

    print(
        f"  H4 : "
        f"{H4_SOURCE_DIR}"
    )

    print(
        f"  D1 : "
        f"{D1_SOURCE_DIR}"
    )

    print()

    print(
        "Output directories:"
    )

    print(
        f"  H1 : "
        f"{H1_FEATURE_DIR}"
    )

    print(
        f"  H4 : "
        f"{H4_FEATURE_DIR}"
    )

    print(
        f"  D1 : "
        f"{D1_FEATURE_DIR}"
    )

    print()

    print(
        f"Status:"
    )

    print(
        f"  {STATUS_FILE}"
    )

    print()

    print(
        "Safety:"
    )

    print(
        "  Source OHLCV write      : DISABLED"
    )

    print(
        "  Source OHLCV delete     : DISABLED"
    )

    print(
        "  Trading                 : DISABLED"
    )

    print(
        "  Future label generation : DISABLED"
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
        f"Feature Builder"
    )

    print(
        f"Version: {VERSION}"
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
        "Clean V001 scope:"
    )

    print(
        "  Market     : KRW-BTC"
    )

    print(
        "  Timeframes : h1 / h4 / d1"
    )

    print(
        "  Source     : READ ONLY"
    )

    print(
        "  Labels     : NOT GENERATED"
    )

    print(
        "  Trading    : DISABLED"
    )

    print()

    # --------------------------------------------------------
    # OUTPUT DIRECTORIES
    # --------------------------------------------------------

    ensure_directories()

    # --------------------------------------------------------
    # JOBS
    # --------------------------------------------------------

    success_jobs = 0
    failed_jobs = 0
    total_feature_rows = 0

    total_markets = len(
        TARGET_MARKETS
    )

    for market_index, market in enumerate(
        TARGET_MARKETS,
        start=1,
    ):

        print_line(
            "-",
            72,
        )

        print(
            f"[{market_index:03d}/"
            f"{total_markets:03d}] "
            f"{market}"
        )

        print_line(
            "-",
            72,
        )

        for timeframe in (
            TARGET_TIMEFRAMES
        ):

            (
                success,
                feature_rows,
            ) = process_market_timeframe(
                market=market,
                timeframe=timeframe,
            )

            if success:

                success_jobs += 1

                total_feature_rows += (
                    feature_rows
                )

            else:

                failed_jobs += 1

        print()

    # --------------------------------------------------------
    # SUMMARY
    # --------------------------------------------------------

    elapsed_seconds = (
        time.time()
        - start_time
    )

    show_final_summary(
        market_count=total_markets,
        success_jobs=success_jobs,
        failed_jobs=failed_jobs,
        total_feature_rows=(
            total_feature_rows
        ),
        elapsed_seconds=(
            elapsed_seconds
        ),
    )

    # --------------------------------------------------------
    # RESULT
    # --------------------------------------------------------

    if failed_jobs > 0:

        print(
            "[RESULT] FEATURE BUILD FAILED"
        )

        print(
            "One or more feature jobs failed."
        )

        return 1

    expected_jobs = (
        len(TARGET_MARKETS)
        * len(TARGET_TIMEFRAMES)
    )

    if (
        success_jobs
        != expected_jobs
    ):

        print(
            "[RESULT] FEATURE BUILD FAILED"
        )

        print(
            "Unexpected successful job count."
        )

        return 1

    print(
        "[RESULT] FEATURE BUILD PASSED"
    )

    print(
        "KRW-BTC H1/H4/D1 features "
        "were generated successfully."
    )

    print(
        "Source OHLCV files remained unchanged."
    )

    return 0


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    sys.exit(
        main()
    )
