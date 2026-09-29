"""
Upbit Surge Monitor - 256 Detector Clean V002
==============================================

File:
    detect_256.py

Purpose:
    build_features.py Clean V002가 생성한 전체 KRW Feature CSV를
    READ ONLY로 읽어 256 연구용 상태를 판정한다.

    256 패턴을 단순 True / False 하나로 만들지 않고
    다음 3단계 상태로 분리한다.

        SETUP
        READY
        TRIGGER

    이후 실제 급등 결과와 비교하여
    어떤 조건이 유효했는지 연구 / 백테스트하기 위한
    Research Detector이다.

Clean V002 scope:
    - 전체 KRW 마켓 자동 발견
    - h1 / h4 / d1 공통 Feature 파일만 실행
    - Feature CSV READ ONLY
    - 원본 OHLCV READ ONLY
    - Signal CSV 별도 저장
    - 미래 데이터 사용 금지
    - 미래 급등 Label 생성 금지
    - 예측 기능 없음
    - 자동매매 기능 없음

Input:
    data/features/h1/*.csv
    data/features/h4/*.csv
    data/features/d1/*.csv

Output:
    data/signals/256/h1/*.csv
    data/signals/256/h4/*.csv
    data/signals/256/d1/*.csv

Status:
    data/detect_256_status.csv

Windows:
    py detect_256.py

Important:
    이 파일은 연구용 Detector이다.

    "256"이라는 이름이 특정 매매법의 수익성을 보장하거나
    미래 상승을 예측한다는 의미가 아니다.

    현재/과거 Feature만 사용하여
    시장 구조를 단계별로 분류한다.
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
VERSION = "256 Detector Clean V002"

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
FEATURE_DIR = DATA_DIR / "features"
SIGNAL_DIR = DATA_DIR / "signals" / "256"
STATUS_FILE = DATA_DIR / "detect_256_status.csv"


# ============================================================
# TARGET CONFIG
# ============================================================

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
        "feature_directory": FEATURE_DIR / "h1",
        "signal_directory": SIGNAL_DIR / "h1",
    },
    "h4": {
        "feature_directory": FEATURE_DIR / "h4",
        "signal_directory": SIGNAL_DIR / "h4",
    },
    "d1": {
        "feature_directory": FEATURE_DIR / "d1",
        "signal_directory": SIGNAL_DIR / "d1",
    },
}


# ============================================================
# REQUIRED BASE COLUMNS
# ============================================================

BASE_COLUMNS = [
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
# REQUIRED FEATURE COLUMNS
# ============================================================

REQUIRED_FEATURE_COLUMNS = [
    # MOVING AVERAGE
    "sma_5",
    "sma_20",
    "sma_60",
    "sma_112",
    "sma_224",

    # PRICE POSITION
    "close_to_sma_5",
    "close_to_sma_20",
    "close_to_sma_60",
    "close_to_sma_112",
    "close_to_sma_224",

    # MA DISTANCE
    "sma_5_to_20",
    "sma_5_to_60",
    "sma_20_to_60",
    "sma_5_to_112",
    "sma_5_to_224",
    "sma_112_to_224",

    # MA SLOPE
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

    # VOLUME
    "volume_ratio_5",
    "volume_ratio_20",

    # TRADE VALUE
    "trade_value_ratio_5",
    "trade_value_ratio_20",

    # MOMENTUM
    "return_1",
    "return_3",
    "return_6",
    "rsi_14",
    "macd",
    "macd_signal",
    "macd_hist",

    # VOLATILITY
    "atr_pct_14",
    "bb_width",
    "bb_position",

    # CANDLE
    "candle_return",
    "candle_range_pct",
    "candle_body_pct",
    "upper_wick_pct",
    "lower_wick_pct",
    "body_to_range",
    "close_position_in_range",
    "is_bullish",
    "is_bearish",

    # RECENT RANGE
    "close_to_high_20",
    "close_to_low_20",
    "range_position_20",
    "close_to_high_60",
    "close_to_low_60",
    "range_position_60",
]


# ============================================================
# DETECTOR OUTPUT COLUMNS
# ============================================================

DETECTOR_COLUMNS = [
    # CONDITION FLAGS
    "cond_ma_compression",
    "cond_long_ma_compression",
    "cond_short_ma_turn",
    "cond_mid_ma_turn",
    "cond_long_ma_stable",
    "cond_price_above_sma5",
    "cond_price_above_sma20",
    "cond_momentum_positive",
    "cond_macd_positive",
    "cond_volume_expansion",
    "cond_trade_value_expansion",
    "cond_candle_strength",
    "cond_not_overextended",

    # SCORES
    "setup_score",
    "ready_score",
    "trigger_score",

    # STATES
    "signal_setup",
    "signal_ready",
    "signal_trigger",
    "signal_stage",
]


# ============================================================
# STATUS COLUMNS
# ============================================================

STATUS_COLUMNS = [
    "run_time_utc",
    "version",
    "market",
    "timeframe",
    "status",
    "feature_rows",
    "signal_rows",
    "feature_sha256_before",
    "feature_sha256_after",
    "message",
]


# ============================================================
# DETECTOR THRESHOLDS
# ============================================================

# 기존 Clean V001 연구 시작값을 그대로 유지한다.
# 전체시장 실행 수정에서 Detector 판정값은 변경하지 않는다.

SHORT_MA_COMPRESSION_LIMIT = 0.015
MID_MA_COMPRESSION_LIMIT = 0.030
LONG_MA_COMPRESSION_LIMIT = 0.080

MAX_CLOSE_TO_SMA20 = 0.080

VOLUME_RATIO_5_MIN = 1.10
VOLUME_RATIO_20_MIN = 1.05

TRADE_VALUE_RATIO_5_MIN = 1.10
TRADE_VALUE_RATIO_20_MIN = 1.05

RETURN_1_MIN = 0.0
RETURN_3_MIN = 0.0

RSI_READY_MIN = 45.0
RSI_TRIGGER_MIN = 50.0
RSI_TRIGGER_MAX = 75.0

SETUP_SCORE_MIN = 4
READY_SCORE_MIN = 5
TRIGGER_SCORE_MIN = 5


# ============================================================
# UTILITY
# ============================================================

def print_line(
    char: str = "=",
    length: int = 72,
) -> None:
    print(char * length)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_directories() -> None:
    """
    Signal 출력 디렉터리만 생성한다.

    Feature 입력 디렉터리는 생성하거나 수정하지 않는다.
    """
    directories = [
        DATA_DIR,
        SIGNAL_DIR,
    ]

    for timeframe in TARGET_TIMEFRAMES:
        directories.append(
            TIMEFRAMES[timeframe]["signal_directory"]
        )

    for directory in directories:
        directory.mkdir(
            parents=True,
            exist_ok=True,
        )


def calculate_file_sha256(
    file_path: Path,
) -> str:
    """
    파일 SHA256 계산.

    Detector 실행 전/후 Feature CSV가
    변경되지 않았는지 검증한다.
    """
    sha256 = hashlib.sha256()

    with file_path.open("rb") as file:
        while True:
            chunk = file.read(1024 * 1024)

            if not chunk:
                break

            sha256.update(chunk)

    return sha256.hexdigest()


def numeric_flag(
    condition: pd.Series,
) -> pd.Series:
    """
    bool 조건을 0 / 1 int8로 변환한다.
    """
    return (
        condition
        .fillna(False)
        .astype("int8")
    )


# ============================================================
# MARKET DISCOVERY
# ============================================================

def discover_target_markets() -> list[str]:
    """
    h1 / h4 / d1 Feature 디렉터리의 KRW CSV를 조사한다.

    안전 원칙:
        1. Feature 파일은 읽기만 한다.
        2. 세 timeframe의 파일 집합이 정확히 동일해야 한다.
        3. KRW-*.csv만 대상으로 한다.
        4. 하나라도 불일치하면 전체 Detector 실행을 중단한다.

    현재 정상 상태라면:
        290 markets
        x 3 timeframes
        = 870 jobs
    """

    timeframe_markets: dict[str, set[str]] = {}

    for timeframe in TARGET_TIMEFRAMES:
        feature_directory = (
            TIMEFRAMES[timeframe]["feature_directory"]
        )

        if not feature_directory.exists():
            raise RuntimeError(
                "Feature directory not found: "
                f"{feature_directory}"
            )

        if not feature_directory.is_dir():
            raise RuntimeError(
                "Feature path is not a directory: "
                f"{feature_directory}"
            )

        markets: set[str] = set()

        for file_path in feature_directory.glob("KRW-*.csv"):
            if not file_path.is_file():
                continue

            market = file_path.stem.strip()

            if not market.startswith("KRW-"):
                continue

            markets.add(market)

        if not markets:
            raise RuntimeError(
                "No KRW Feature CSV files found: "
                f"{feature_directory}"
            )

        timeframe_markets[timeframe] = markets

    reference_timeframe = TARGET_TIMEFRAMES[0]
    reference_markets = timeframe_markets[reference_timeframe]

    for timeframe in TARGET_TIMEFRAMES[1:]:
        current_markets = timeframe_markets[timeframe]

        if current_markets != reference_markets:
            missing = sorted(
                reference_markets - current_markets
            )

            extra = sorted(
                current_markets - reference_markets
            )

            message_parts = [
                "Feature market sets differ between timeframes.",
                f"Reference={reference_timeframe}",
                f"Current={timeframe}",
                f"Reference count={len(reference_markets):,}",
                f"Current count={len(current_markets):,}",
            ]

            if missing:
                message_parts.append(
                    "Missing="
                    + ", ".join(missing[:20])
                )

            if extra:
                message_parts.append(
                    "Extra="
                    + ", ".join(extra[:20])
                )

            raise RuntimeError(
                " | ".join(message_parts)
            )

    target_markets = sorted(reference_markets)

    if not target_markets:
        raise RuntimeError(
            "No common KRW markets discovered."
        )

    return target_markets


# ============================================================
# FEATURE LOAD
# ============================================================

def load_feature_dataframe(
    file_path: Path,
    expected_market: str,
) -> pd.DataFrame:
    """
    build_features.py가 생성한 Feature CSV를 읽는다.

    입력 Feature CSV는 READ ONLY이다.
    """

    if not file_path.exists():
        raise FileNotFoundError(
            "Feature CSV not found: "
            f"{file_path}"
        )

    try:
        df = pd.read_csv(
            file_path,
            encoding="utf-8-sig",
        )

    except pd.errors.EmptyDataError as exc:
        raise RuntimeError(
            "Feature CSV is empty: "
            f"{file_path}"
        ) from exc

    if df.empty:
        raise RuntimeError(
            "Feature CSV contains no rows: "
            f"{file_path}"
        )

    required_columns = (
        BASE_COLUMNS
        + REQUIRED_FEATURE_COLUMNS
    )

    missing_columns = [
        column
        for column in required_columns
        if column not in df.columns
    ]

    if missing_columns:
        raise RuntimeError(
            "Feature CSV is missing required columns: "
            + ", ".join(missing_columns)
        )

    market_values = (
        df["market"]
        .dropna()
        .astype(str)
        .str.strip()
        .unique()
        .tolist()
    )

    if market_values != [expected_market]:
        raise RuntimeError(
            "Unexpected market values: "
            f"{market_values}"
        )

    df["candle_date_time_utc"] = pd.to_datetime(
        df["candle_date_time_utc"],
        utc=True,
        errors="coerce",
    )

    invalid_time_count = int(
        df["candle_date_time_utc"]
        .isna()
        .sum()
    )

    if invalid_time_count > 0:
        raise RuntimeError(
            "Invalid UTC timestamps: "
            f"{invalid_time_count:,}"
        )

    duplicate_count = int(
        df.duplicated(
            subset=["candle_date_time_utc"],
            keep=False,
        ).sum()
    )

    if duplicate_count > 0:
        raise RuntimeError(
            "Duplicate UTC timestamps detected: "
            f"{duplicate_count:,}"
        )

    df = (
        df.sort_values("candle_date_time_utc")
        .reset_index(drop=True)
    )

    if not df[
        "candle_date_time_utc"
    ].is_monotonic_increasing:
        raise RuntimeError(
            "Feature CSV time order is invalid."
        )

    return df


# ============================================================
# 256 CONDITION - MA COMPRESSION
# ============================================================

def add_ma_compression_conditions(
    df: pd.DataFrame,
) -> pd.DataFrame:

    short_compression = (
        df["sma_5_to_20"].abs()
        <= SHORT_MA_COMPRESSION_LIMIT
    )

    mid_compression = (
        df["sma_20_to_60"].abs()
        <= MID_MA_COMPRESSION_LIMIT
    )

    long_compression = (
        df["sma_112_to_224"].abs()
        <= LONG_MA_COMPRESSION_LIMIT
    )

    df["cond_ma_compression"] = numeric_flag(
        short_compression
        & mid_compression
    )

    df["cond_long_ma_compression"] = numeric_flag(
        long_compression
    )

    return df


# ============================================================
# 256 CONDITION - MA DIRECTION
# ============================================================

def add_ma_direction_conditions(
    df: pd.DataFrame,
) -> pd.DataFrame:

    short_turn = (
        (df["sma_5_slope_1"] > 0)
        & (df["sma_5_slope_3"] >= 0)
    )

    mid_turn = (
        (df["sma_20_slope_1"] >= 0)
        & (df["sma_20_slope_3"] >= 0)
    )

    long_stable = (
        (df["sma_112_slope_3"] >= 0)
        | (df["sma_224_slope_3"] >= 0)
    )

    df["cond_short_ma_turn"] = numeric_flag(
        short_turn
    )

    df["cond_mid_ma_turn"] = numeric_flag(
        mid_turn
    )

    df["cond_long_ma_stable"] = numeric_flag(
        long_stable
    )

    return df


# ============================================================
# 256 CONDITION - PRICE POSITION
# ============================================================

def add_price_position_conditions(
    df: pd.DataFrame,
) -> pd.DataFrame:

    df["cond_price_above_sma5"] = numeric_flag(
        df["close_to_sma_5"] >= 0
    )

    df["cond_price_above_sma20"] = numeric_flag(
        df["close_to_sma_20"] >= 0
    )

    df["cond_not_overextended"] = numeric_flag(
        df["close_to_sma_20"]
        <= MAX_CLOSE_TO_SMA20
    )

    return df


# ============================================================
# 256 CONDITION - MOMENTUM
# ============================================================

def add_momentum_conditions(
    df: pd.DataFrame,
) -> pd.DataFrame:

    momentum_positive = (
        (df["return_1"] > RETURN_1_MIN)
        & (df["return_3"] > RETURN_3_MIN)
        & (df["rsi_14"] >= RSI_READY_MIN)
    )

    macd_positive = (
        (df["macd_hist"] > 0)
        & (
            df["macd"]
            >= df["macd_signal"]
        )
    )

    df["cond_momentum_positive"] = numeric_flag(
        momentum_positive
    )

    df["cond_macd_positive"] = numeric_flag(
        macd_positive
    )

    return df


# ============================================================
# 256 CONDITION - VOLUME
# ============================================================

def add_volume_conditions(
    df: pd.DataFrame,
) -> pd.DataFrame:

    volume_expansion = (
        (df["volume_ratio_5"] >= VOLUME_RATIO_5_MIN)
        | (
            df["volume_ratio_20"]
            >= VOLUME_RATIO_20_MIN
        )
    )

    trade_value_expansion = (
        (
            df["trade_value_ratio_5"]
            >= TRADE_VALUE_RATIO_5_MIN
        )
        | (
            df["trade_value_ratio_20"]
            >= TRADE_VALUE_RATIO_20_MIN
        )
    )

    df["cond_volume_expansion"] = numeric_flag(
        volume_expansion
    )

    df["cond_trade_value_expansion"] = numeric_flag(
        trade_value_expansion
    )

    return df


# ============================================================
# 256 CONDITION - CANDLE
# ============================================================

def add_candle_conditions(
    df: pd.DataFrame,
) -> pd.DataFrame:

    candle_strength = (
        (df["is_bullish"] == 1)
        & (df["candle_return"] > 0)
        & (
            df["close_position_in_range"]
            >= 0.60
        )
    )

    df["cond_candle_strength"] = numeric_flag(
        candle_strength
    )

    return df


# ============================================================
# SETUP SCORE
# ============================================================

def add_setup_score(
    df: pd.DataFrame,
) -> pd.DataFrame:

    columns = [
        "cond_ma_compression",
        "cond_long_ma_compression",
        "cond_mid_ma_turn",
        "cond_long_ma_stable",
        "cond_not_overextended",
    ]

    df["setup_score"] = (
        df[columns]
        .sum(axis=1)
        .astype("int16")
    )

    return df


# ============================================================
# READY SCORE
# ============================================================

def add_ready_score(
    df: pd.DataFrame,
) -> pd.DataFrame:

    columns = [
        "cond_short_ma_turn",
        "cond_mid_ma_turn",
        "cond_price_above_sma5",
        "cond_momentum_positive",
        "cond_macd_positive",
        "cond_not_overextended",
    ]

    df["ready_score"] = (
        df[columns]
        .sum(axis=1)
        .astype("int16")
    )

    return df


# ============================================================
# TRIGGER SCORE
# ============================================================

def add_trigger_score(
    df: pd.DataFrame,
) -> pd.DataFrame:

    columns = [
        "cond_price_above_sma5",
        "cond_price_above_sma20",
        "cond_momentum_positive",
        "cond_macd_positive",
        "cond_volume_expansion",
        "cond_trade_value_expansion",
        "cond_candle_strength",
        "cond_not_overextended",
    ]

    df["trigger_score"] = (
        df[columns]
        .sum(axis=1)
        .astype("int16")
    )

    return df


# ============================================================
# SIGNAL STATES
# ============================================================

def add_signal_states(
    df: pd.DataFrame,
) -> pd.DataFrame:

    setup = (
        (df["setup_score"] >= SETUP_SCORE_MIN)
        & (df["cond_ma_compression"] == 1)
    )

    df["signal_setup"] = numeric_flag(
        setup
    )

    ready = (
        (df["ready_score"] >= READY_SCORE_MIN)
        & (df["cond_short_ma_turn"] == 1)
        & (df["cond_price_above_sma5"] == 1)
        & (df["rsi_14"] >= RSI_READY_MIN)
    )

    df["signal_ready"] = numeric_flag(
        ready
    )

    trigger = (
        (
            df["trigger_score"]
            >= TRIGGER_SCORE_MIN
        )
        & (df["signal_ready"] == 1)
        & (
            df["cond_price_above_sma20"]
            == 1
        )
        & (
            df["rsi_14"]
            >= RSI_TRIGGER_MIN
        )
        & (
            df["rsi_14"]
            <= RSI_TRIGGER_MAX
        )
        & (
            (df["cond_volume_expansion"] == 1)
            | (
                df["cond_trade_value_expansion"]
                == 1
            )
        )
    )

    df["signal_trigger"] = numeric_flag(
        trigger
    )

    stage = np.full(
        len(df),
        "NONE",
        dtype=object,
    )

    stage[
        df["signal_setup"].to_numpy(
            dtype=bool
        )
    ] = "SETUP"

    stage[
        df["signal_ready"].to_numpy(
            dtype=bool
        )
    ] = "READY"

    stage[
        df["signal_trigger"].to_numpy(
            dtype=bool
        )
    ] = "TRIGGER"

    df["signal_stage"] = stage

    return df


# ============================================================
# DETECTOR BUILD
# ============================================================

def build_detector_dataframe(
    feature_df: pd.DataFrame,
) -> pd.DataFrame:

    df = feature_df.copy()

    df = add_ma_compression_conditions(df)
    df = add_ma_direction_conditions(df)
    df = add_price_position_conditions(df)
    df = add_momentum_conditions(df)
    df = add_volume_conditions(df)
    df = add_candle_conditions(df)

    df = add_setup_score(df)
    df = add_ready_score(df)
    df = add_trigger_score(df)

    df = add_signal_states(df)

    df = df.replace(
        [
            np.inf,
            -np.inf,
        ],
        np.nan,
    )

    return df


# ============================================================
# DETECTOR VALIDATION
# ============================================================

def validate_detector_dataframe(
    feature_df: pd.DataFrame,
    detector_df: pd.DataFrame,
    expected_market: str,
) -> None:

    if detector_df.empty:
        raise RuntimeError(
            "Detector DataFrame is empty."
        )

    if len(detector_df) != len(feature_df):
        raise RuntimeError(
            "Detector row count changed unexpectedly: "
            f"feature={len(feature_df):,}, "
            f"detector={len(detector_df):,}"
        )

    missing_detector_columns = [
        column
        for column in DETECTOR_COLUMNS
        if column not in detector_df.columns
    ]

    if missing_detector_columns:
        raise RuntimeError(
            "Detector output is missing columns: "
            + ", ".join(
                missing_detector_columns
            )
        )

    market_values = (
        detector_df["market"]
        .dropna()
        .astype(str)
        .str.strip()
        .unique()
        .tolist()
    )

    if market_values != [expected_market]:
        raise RuntimeError(
            "Detector output contains "
            "unexpected market values: "
            f"{market_values}"
        )

    duplicate_count = int(
        detector_df.duplicated(
            subset=["candle_date_time_utc"],
            keep=False,
        ).sum()
    )

    if duplicate_count > 0:
        raise RuntimeError(
            "Detector output contains duplicate "
            "UTC timestamps: "
            f"{duplicate_count:,}"
        )

    if not detector_df[
        "candle_date_time_utc"
    ].is_monotonic_increasing:
        raise RuntimeError(
            "Detector output is not sorted "
            "by UTC timestamp."
        )

    # Feature 입력값 보존 검증
    for column in feature_df.columns:

        source_series = (
            feature_df[column]
            .reset_index(drop=True)
        )

        detector_series = (
            detector_df[column]
            .reset_index(drop=True)
        )

        if column == "candle_date_time_utc":

            source_values = pd.to_datetime(
                source_series,
                utc=True,
                errors="coerce",
            )

            detector_values = pd.to_datetime(
                detector_series,
                utc=True,
                errors="coerce",
            )

            if not source_values.equals(
                detector_values
            ):
                raise RuntimeError(
                    "Feature UTC timestamp changed "
                    "during detection."
                )

        elif pd.api.types.is_numeric_dtype(
            source_series
        ):

            source_values = pd.to_numeric(
                source_series,
                errors="coerce",
            ).to_numpy(dtype=float)

            detector_values = pd.to_numeric(
                detector_series,
                errors="coerce",
            ).to_numpy(dtype=float)

            if not np.allclose(
                source_values,
                detector_values,
                equal_nan=True,
            ):
                raise RuntimeError(
                    "Feature numeric column changed "
                    "during detection: "
                    f"{column}"
                )

        else:

            source_values = (
                source_series
                .astype(str)
                .tolist()
            )

            detector_values = (
                detector_series
                .astype(str)
                .tolist()
            )

            if source_values != detector_values:
                raise RuntimeError(
                    "Feature column changed "
                    "during detection: "
                    f"{column}"
                )

    # Flag 검증
    flag_columns = [
        column
        for column in DETECTOR_COLUMNS
        if (
            column.startswith("cond_")
            or (
                column.startswith("signal_")
                and column != "signal_stage"
            )
        )
    ]

    for column in flag_columns:

        values = set(
            detector_df[column]
            .dropna()
            .unique()
            .tolist()
        )

        if not values.issubset({0, 1}):
            raise RuntimeError(
                "Invalid detector flag values "
                f"in {column}: "
                f"{values}"
            )

    allowed_stages = {
        "NONE",
        "SETUP",
        "READY",
        "TRIGGER",
    }

    stage_values = set(
        detector_df["signal_stage"]
        .dropna()
        .astype(str)
        .unique()
        .tolist()
    )

    if not stage_values.issubset(
        allowed_stages
    ):
        raise RuntimeError(
            "Invalid signal_stage values: "
            f"{stage_values}"
        )

    trigger_without_ready = (
        (detector_df["signal_trigger"] == 1)
        & (detector_df["signal_ready"] != 1)
    )

    if trigger_without_ready.any():
        raise RuntimeError(
            "TRIGGER exists without READY."
        )

    numeric_df = detector_df.select_dtypes(
        include=[np.number]
    )

    numeric_values = numeric_df.to_numpy(
        dtype=float
    )

    if np.isinf(numeric_values).any():
        raise RuntimeError(
            "Detector output contains "
            "infinite numeric values."
        )


# ============================================================
# SAVE SIGNAL DATA
# ============================================================

def save_detector_dataframe(
    detector_df: pd.DataFrame,
    output_file: Path,
) -> None:

    output_file.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temporary_file = (
        output_file.parent
        / (output_file.name + ".tmp")
    )

    save_df = detector_df.copy()

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

def append_detector_status(
    *,
    market: str,
    timeframe: str,
    status: str,
    feature_rows: int,
    signal_rows: int,
    feature_sha256_before: str,
    feature_sha256_after: str,
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
        "feature_rows": feature_rows,
        "signal_rows": signal_rows,
        "feature_sha256_before": (
            feature_sha256_before
        ),
        "feature_sha256_after": (
            feature_sha256_after
        ),
        "message": message,
    }

    status_df = pd.DataFrame(
        [row],
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
# SINGLE JOB
# ============================================================

def detect_single_file(
    market: str,
    timeframe: str,
) -> dict[str, Any]:

    if timeframe not in TIMEFRAMES:
        raise ValueError(
            "Unsupported timeframe: "
            f"{timeframe}"
        )

    config = TIMEFRAMES[timeframe]

    feature_directory = (
        config["feature_directory"]
    )

    signal_directory = (
        config["signal_directory"]
    )

    feature_file = (
        feature_directory
        / f"{market}.csv"
    )

    output_file = (
        signal_directory
        / f"{market}.csv"
    )

    print_line("-", 72)

    print(
        f"[{timeframe.upper()}] "
        f"{market}"
    )

    print(
        "Feature : "
        f"{feature_file}"
    )

    print(
        "Signal  : "
        f"{output_file}"
    )

    feature_rows = 0
    signal_rows = 0

    feature_sha256_before = ""
    feature_sha256_after = ""

    setup_count = 0
    ready_count = 0
    trigger_count = 0

    started = time.perf_counter()

    try:
        feature_sha256_before = (
            calculate_file_sha256(
                feature_file
            )
        )

        feature_df = (
            load_feature_dataframe(
                feature_file,
                expected_market=market,
            )
        )

        feature_rows = len(feature_df)

        print(
            "Rows    : "
            f"{feature_rows:,}"
        )

        detector_df = (
            build_detector_dataframe(
                feature_df
            )
        )

        signal_rows = len(detector_df)

        validate_detector_dataframe(
            feature_df=feature_df,
            detector_df=detector_df,
            expected_market=market,
        )

        setup_count = int(
            detector_df[
                "signal_setup"
            ].sum()
        )

        ready_count = int(
            detector_df[
                "signal_ready"
            ].sum()
        )

        trigger_count = int(
            detector_df[
                "signal_trigger"
            ].sum()
        )

        feature_sha256_mid = (
            calculate_file_sha256(
                feature_file
            )
        )

        if (
            feature_sha256_mid
            != feature_sha256_before
        ):
            raise RuntimeError(
                "Feature CSV changed while "
                "detector was running."
            )

        save_detector_dataframe(
            detector_df,
            output_file,
        )

        saved_df = pd.read_csv(
            output_file,
            encoding="utf-8-sig",
        )

        if len(saved_df) != signal_rows:
            raise RuntimeError(
                "Saved detector row count mismatch: "
                f"expected={signal_rows:,}, "
                f"actual={len(saved_df):,}"
            )

        missing_saved_columns = [
            column
            for column in DETECTOR_COLUMNS
            if column not in saved_df.columns
        ]

        if missing_saved_columns:
            raise RuntimeError(
                "Saved detector CSV is missing columns: "
                + ", ".join(
                    missing_saved_columns
                )
            )

        feature_sha256_after = (
            calculate_file_sha256(
                feature_file
            )
        )

        if (
            feature_sha256_after
            != feature_sha256_before
        ):
            raise RuntimeError(
                "Feature CSV was modified "
                "during detector execution."
            )

        elapsed = (
            time.perf_counter()
            - started
        )

        message = (
            "256 detector completed successfully. "
            "Feature CSV remained unchanged."
        )

        append_detector_status(
            market=market,
            timeframe=timeframe,
            status="PASSED",
            feature_rows=feature_rows,
            signal_rows=signal_rows,
            feature_sha256_before=(
                feature_sha256_before
            ),
            feature_sha256_after=(
                feature_sha256_after
            ),
            message=message,
        )

        print(
            "[PASS] "
            f"{signal_rows:,} rows"
        )

        print(
            "[PASS] Feature SHA256 unchanged"
        )

        print(
            "SETUP   : "
            f"{setup_count:,}"
        )

        print(
            "READY   : "
            f"{ready_count:,}"
        )

        print(
            "TRIGGER : "
            f"{trigger_count:,}"
        )

        print(
            "Elapsed : "
            f"{elapsed:.2f}s"
        )

        return {
            "market": market,
            "timeframe": timeframe,
            "status": "PASSED",
            "feature_rows": feature_rows,
            "signal_rows": signal_rows,
            "setup_count": setup_count,
            "ready_count": ready_count,
            "trigger_count": trigger_count,
            "output_file": str(output_file),
            "message": message,
        }

    except Exception as exc:

        try:
            if feature_file.exists():
                feature_sha256_after = (
                    calculate_file_sha256(
                        feature_file
                    )
                )

        except Exception:
            feature_sha256_after = (
                "HASH_CHECK_FAILED"
            )

        message = str(exc)

        append_detector_status(
            market=market,
            timeframe=timeframe,
            status="FAILED",
            feature_rows=feature_rows,
            signal_rows=signal_rows,
            feature_sha256_before=(
                feature_sha256_before
            ),
            feature_sha256_after=(
                feature_sha256_after
            ),
            message=message,
        )

        print(
            "[FAIL] "
            f"{message}"
        )

        return {
            "market": market,
            "timeframe": timeframe,
            "status": "FAILED",
            "feature_rows": feature_rows,
            "signal_rows": signal_rows,
            "setup_count": setup_count,
            "ready_count": ready_count,
            "trigger_count": trigger_count,
            "output_file": str(output_file),
            "message": message,
        }


# ============================================================
# SUMMARY
# ============================================================

def print_summary(
    results: list[dict[str, Any]],
    target_markets: list[str],
    elapsed_total: float,
) -> None:

    print()
    print_line()
    print("256 DETECTOR SUMMARY")
    print_line()

    total_jobs = len(results)

    passed_jobs = sum(
        1
        for result in results
        if result["status"] == "PASSED"
    )

    failed_jobs = (
        total_jobs - passed_jobs
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

    total_signal_rows = sum(
        int(
            result.get(
                "signal_rows",
                0,
            )
        )
        for result in results
    )

    total_setup = sum(
        int(
            result.get(
                "setup_count",
                0,
            )
        )
        for result in results
    )

    total_ready = sum(
        int(
            result.get(
                "ready_count",
                0,
            )
        )
        for result in results
    )

    total_trigger = sum(
        int(
            result.get(
                "trigger_count",
                0,
            )
        )
        for result in results
    )

    print(
        "Project       : "
        f"{PROJECT_NAME}"
    )

    print(
        "Version       : "
        f"{VERSION}"
    )

    print(
        "Markets       : "
        f"{len(target_markets):,}"
    )

    print(
        "Timeframes    : "
        f"{len(TARGET_TIMEFRAMES):,}"
    )

    print(
        "Total jobs    : "
        f"{total_jobs:,}"
    )

    print(
        "Passed jobs   : "
        f"{passed_jobs:,}"
    )

    print(
        "Failed jobs   : "
        f"{failed_jobs:,}"
    )

    print(
        "Feature rows  : "
        f"{total_feature_rows:,}"
    )

    print(
        "Signal rows   : "
        f"{total_signal_rows:,}"
    )

    print()

    print("256 Research States:")

    print(
        "  SETUP       : "
        f"{total_setup:,}"
    )

    print(
        "  READY       : "
        f"{total_ready:,}"
    )

    print(
        "  TRIGGER     : "
        f"{total_trigger:,}"
    )

    print()

    print("Detector design:")
    print(
        "  SETUP   = MA structure / compression"
    )
    print(
        "  READY   = short-term turn / momentum"
    )
    print(
        "  TRIGGER = price + momentum + activity"
    )

    print()

    print("Safety:")
    print("  Feature CSV : READ ONLY")
    print("  OHLCV       : UNTOUCHED")
    print("  Future data : NOT USED")
    print("  Future label: NOT IMPLEMENTED")
    print("  Prediction  : DISABLED")
    print("  Trading     : DISABLED")

    print()
    print(
        "Elapsed total : "
        f"{elapsed_total:.2f}s"
    )

    if failed_jobs > 0:
        print()
        print("Failed jobs:")

        for result in results:
            if result["status"] == "FAILED":
                print(
                    f"[FAILED] "
                    f"{result['market']} "
                    f"{result['timeframe']} "
                    f"- {result['message']}"
                )

    print_line()


# ============================================================
# MAIN
# ============================================================

def main() -> int:

    started_total = time.perf_counter()

    print_line()

    print(
        f"{PROJECT_NAME} - "
        f"{VERSION}"
    )

    print(
        "STEP 4 - 256 DETECTOR FULL KRW MARKET"
    )

    print_line()

    print(
        "Started UTC : "
        f"{utc_now_iso()}"
    )

    print(
        "Base dir    : "
        f"{BASE_DIR}"
    )

    print()

    print("Mode:")
    print("  Feature CSV READ ONLY")
    print("  Original OHLCV untouched")
    print("  ALL KRW MARKET mode")
    print("  h1 / h4 / d1")
    print("  SETUP / READY / TRIGGER research")
    print("  No future labels")
    print("  No prediction")
    print("  No trading")

    print()

    print("Clean V002 detector:")
    print("  Full KRW market auto discovery")
    print("  h1 / h4 / d1 market-set consistency")
    print("  MA compression")
    print("  MA direction")
    print("  Price position")
    print("  Momentum")
    print("  Volume / trade-value expansion")
    print("  Candle strength")
    print("  SETUP / READY / TRIGGER scores")

    print()

    ensure_directories()

    # --------------------------------------------------------
    # DISCOVER ALL KRW MARKETS
    # --------------------------------------------------------

    target_markets = discover_target_markets()

    expected_jobs = (
        len(target_markets)
        * len(TARGET_TIMEFRAMES)
    )

    print_line("-", 72)

    print(
        "[DISCOVERY] KRW markets : "
        f"{len(target_markets):,}"
    )

    print(
        "[DISCOVERY] Timeframes  : "
        f"{len(TARGET_TIMEFRAMES):,}"
    )

    print(
        "[DISCOVERY] Total jobs  : "
        f"{expected_jobs:,}"
    )

    print(
        "[PASS] h1 / h4 / d1 "
        "market file sets are identical."
    )

    print_line("-", 72)

    results: list[
        dict[str, Any]
    ] = []

    # --------------------------------------------------------
    # RUN ALL MARKETS
    # --------------------------------------------------------

    current_job = 0

    for market in target_markets:

        for timeframe in TARGET_TIMEFRAMES:

            current_job += 1

            print()
            print(
                f"[JOB {current_job:,}"
                f"/{expected_jobs:,}]"
            )

            result = detect_single_file(
                market=market,
                timeframe=timeframe,
            )

            results.append(result)

    # --------------------------------------------------------
    # FINAL SUMMARY
    # --------------------------------------------------------

    elapsed_total = (
        time.perf_counter()
        - started_total
    )

    print_summary(
        results=results,
        target_markets=target_markets,
        elapsed_total=elapsed_total,
    )

    failed_jobs = [
        result
        for result in results
        if result["status"] != "PASSED"
    ]

    if failed_jobs:

        print(
            "[RESULT] 256 DETECTOR FAILED"
        )

        print(
            "[FAIL] "
            f"{len(failed_jobs):,} / "
            f"{expected_jobs:,} jobs failed."
        )

        print(
            "[STOP] Do not proceed to "
            "256 Validator."
        )

        return 1

    if len(results) != expected_jobs:

        print(
            "[RESULT] 256 DETECTOR FAILED"
        )

        print(
            "[FAIL] Job count mismatch: "
            f"expected={expected_jobs:,}, "
            f"actual={len(results):,}"
        )

        print(
            "[STOP] Do not proceed to "
            "256 Validator."
        )

        return 1

    print(
        "[RESULT] 256 DETECTOR PASSED"
    )

    print(
        "[PASS] "
        f"{len(target_markets):,} markets."
    )

    print(
        "[PASS] "
        f"{len(results):,} / "
        f"{expected_jobs:,} jobs passed."
    )

    print(
        "[PASS] Feature CSV files "
        "remained unchanged."
    )

    print(
        "[PASS] Original OHLCV data "
        "remained untouched."
    )

    print(
        "[PASS] SETUP / READY / TRIGGER "
        "research states were generated."
    )

    print(
        "[NEXT] Full 256 Detector validation."
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
        print("[STOP] Interrupted by user.")

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

    sys.exit(exit_code)


# ============================================================
# END
# ============================================================
