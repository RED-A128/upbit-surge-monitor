#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
========================================================================
UPBIT SURGE MONITOR
quantify_patterns.py
Clean V001
========================================================================

PURPOSE
-------
Pattern Quantification Framework for the following nine independent
research patterns:

    1. 256
    2. 밥그릇 3번자리
    3. 역매공파
    4. 공구리
    5. 돌파
    6. 눌림목
    7. 기준봉
    8. 하이힐
    9. 오돌이

IMPORTANT
---------
Clean V001 intentionally DOES NOT invent final trading-pattern formulas.

This version establishes:

    - production OHLCV read-only contract
    - deterministic OHLCV loading
    - schema validation
    - timestamp validation
    - duplicate validation
    - candle integrity validation
    - timeframe isolation
    - common quantitative research features
    - strict no-future-information rule
    - nine independent pattern research contracts
    - pattern registry
    - research readiness assessment
    - checkpoint/resume
    - deterministic reports
    - SHA256 source manifest
    - source immutability verification
    - idempotent execution support

NO future candle is used to calculate a pattern signal.

Future performance labels are NOT generated here.

This program DOES NOT:

    - modify OHLCV
    - delete OHLCV
    - overwrite OHLCV
    - repair historical gaps
    - call Upbit API
    - download candles
    - execute recovery
    - generate future labels
    - backtest
    - predict
    - trade
    - place orders
    - run Git mutation commands

========================================================================
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import re
import sys
import traceback

from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd


# ======================================================================
# VERSION
# ======================================================================

PROGRAM_NAME = "quantify_patterns.py"
PROGRAM_VERSION = "Clean V001"

PROJECT_NAME = "UPBIT SURGE MONITOR"
STAGE_NAME = "PATTERN QUANTIFICATION"


# ======================================================================
# SAFETY CONTRACT
# ======================================================================

READ_ONLY_OHLCV = True
ALLOW_OHLCV_WRITE = False
ALLOW_OHLCV_DELETE = False
ALLOW_RECOVERY = False
ALLOW_API_DOWNLOAD = False
ALLOW_FUTURE_LABELS = False
ALLOW_BACKTEST = False
ALLOW_PREDICTION = False
ALLOW_TRADING = False
ALLOW_GIT_MUTATION = False


# ======================================================================
# SUPPORTED TIMEFRAMES
# ======================================================================

TIMEFRAME_SECONDS: Dict[str, int] = {
    "h1": 60 * 60,
    "h4": 4 * 60 * 60,
    "d1": 24 * 60 * 60,
}

SUPPORTED_TIMEFRAMES: Tuple[str, ...] = tuple(TIMEFRAME_SECONDS.keys())


# ======================================================================
# REQUIRED OHLCV COLUMNS
# ======================================================================

REQUIRED_PRICE_COLUMNS: Tuple[str, ...] = (
    "open",
    "high",
    "low",
    "close",
)

TIMESTAMP_CANDIDATES: Tuple[str, ...] = (
    "timestamp",
    "candle_date_time_utc",
    "datetime",
    "date",
    "time",
)

VOLUME_CANDIDATES: Tuple[str, ...] = (
    "volume",
    "candle_acc_trade_volume",
)

TRADE_VALUE_CANDIDATES: Tuple[str, ...] = (
    "trade_value",
    "candle_acc_trade_price",
)


# ======================================================================
# OUTPUT PATH CONTRACT
# ======================================================================

REPORT_NAMESPACE = "pattern_quantification"

RESULT_FILENAME = "pattern_quantification_result.json"
SUMMARY_FILENAME = "pattern_quantification_summary.csv"
DETAIL_FILENAME = "pattern_quantification_detail.csv"
MANIFEST_FILENAME = "pattern_quantification_source_manifest.csv"
PATTERN_REGISTRY_FILENAME = "pattern_registry.json"
FEATURE_CONTRACT_FILENAME = "pattern_feature_contract.json"
README_FILENAME = "README.txt"

CHECKPOINT_FILENAME = "quantify_patterns_checkpoint.json"


# ======================================================================
# PATTERN IDS
# ======================================================================

PATTERN_256 = "256"
PATTERN_RICE_BOWL_3 = "rice_bowl_position_3"
PATTERN_REVERSE_MA_WAVE = "reverse_ma_wave"
PATTERN_CONCRETE = "concrete"
PATTERN_BREAKOUT = "breakout"
PATTERN_PULLBACK = "pullback"
PATTERN_REFERENCE_CANDLE = "reference_candle"
PATTERN_HIGH_HEEL = "high_heel"
PATTERN_ODORI = "odori"


# ======================================================================
# PATTERN DEFINITIONS
# ======================================================================

@dataclass(frozen=True)
class PatternDefinition:
    pattern_id: str
    korean_name: str
    english_name: str
    enabled: bool
    formula_status: str
    detector_status: str
    future_data_allowed: bool
    description: str


PATTERN_DEFINITIONS: Tuple[PatternDefinition, ...] = (
    PatternDefinition(
        pattern_id=PATTERN_256,
        korean_name="256",
        english_name="256",
        enabled=True,
        formula_status="PENDING_RESEARCH_DEFINITION",
        detector_status="NOT_IMPLEMENTED_IN_CLEAN_V001",
        future_data_allowed=False,
        description=(
            "256 패턴 독립 연구 슬롯. "
            "Clean V001에서는 최종 수치 조건을 임의 정의하지 않는다."
        ),
    ),
    PatternDefinition(
        pattern_id=PATTERN_RICE_BOWL_3,
        korean_name="밥그릇 3번자리",
        english_name="Rice Bowl Position 3",
        enabled=True,
        formula_status="PENDING_RESEARCH_DEFINITION",
        detector_status="NOT_IMPLEMENTED_IN_CLEAN_V001",
        future_data_allowed=False,
        description=(
            "밥그릇 3번자리 독립 연구 슬롯. "
            "최종 조건은 별도 검증 후 확정한다."
        ),
    ),
    PatternDefinition(
        pattern_id=PATTERN_REVERSE_MA_WAVE,
        korean_name="역매공파",
        english_name="Reverse MA Wave",
        enabled=True,
        formula_status="PENDING_RESEARCH_DEFINITION",
        detector_status="NOT_IMPLEMENTED_IN_CLEAN_V001",
        future_data_allowed=False,
        description=(
            "역매공파 독립 연구 슬롯. "
            "최종 조건은 별도 검증 후 확정한다."
        ),
    ),
    PatternDefinition(
        pattern_id=PATTERN_CONCRETE,
        korean_name="공구리",
        english_name="Concrete",
        enabled=True,
        formula_status="PENDING_RESEARCH_DEFINITION",
        detector_status="NOT_IMPLEMENTED_IN_CLEAN_V001",
        future_data_allowed=False,
        description=(
            "공구리 독립 연구 슬롯. "
            "횡보/압축/거래량 등 최종 정의는 별도 연구에서 확정한다."
        ),
    ),
    PatternDefinition(
        pattern_id=PATTERN_BREAKOUT,
        korean_name="돌파",
        english_name="Breakout",
        enabled=True,
        formula_status="PENDING_RESEARCH_DEFINITION",
        detector_status="NOT_IMPLEMENTED_IN_CLEAN_V001",
        future_data_allowed=False,
        description=(
            "돌파 독립 연구 슬롯. "
            "저항·거래량·종가 확인 조건은 별도 연구에서 확정한다."
        ),
    ),
    PatternDefinition(
        pattern_id=PATTERN_PULLBACK,
        korean_name="눌림목",
        english_name="Pullback",
        enabled=True,
        formula_status="PENDING_RESEARCH_DEFINITION",
        detector_status="NOT_IMPLEMENTED_IN_CLEAN_V001",
        future_data_allowed=False,
        description=(
            "눌림목 독립 연구 슬롯. "
            "추세·조정폭·지지 조건은 별도 연구에서 확정한다."
        ),
    ),
    PatternDefinition(
        pattern_id=PATTERN_REFERENCE_CANDLE,
        korean_name="기준봉",
        english_name="Reference Candle",
        enabled=True,
        formula_status="PENDING_RESEARCH_DEFINITION",
        detector_status="NOT_IMPLEMENTED_IN_CLEAN_V001",
        future_data_allowed=False,
        description=(
            "기준봉 독립 연구 슬롯. "
            "몸통·거래량·변동성 기준은 별도 연구에서 확정한다."
        ),
    ),
    PatternDefinition(
        pattern_id=PATTERN_HIGH_HEEL,
        korean_name="하이힐",
        english_name="High Heel",
        enabled=True,
        formula_status="PENDING_RESEARCH_DEFINITION",
        detector_status="NOT_IMPLEMENTED_IN_CLEAN_V001",
        future_data_allowed=False,
        description=(
            "하이힐 독립 연구 슬롯. "
            "캔들 구조 및 위치 조건은 별도 연구에서 확정한다."
        ),
    ),
    PatternDefinition(
        pattern_id=PATTERN_ODORI,
        korean_name="오돌이",
        english_name="Odori",
        enabled=True,
        formula_status="PENDING_RESEARCH_DEFINITION",
        detector_status="NOT_IMPLEMENTED_IN_CLEAN_V001",
        future_data_allowed=False,
        description=(
            "오돌이 독립 연구 슬롯. "
            "최종 패턴 조건은 별도 연구에서 확정한다."
        ),
    ),
)


# ======================================================================
# COMMON FEATURE CONTRACT
# ======================================================================

COMMON_FEATURE_COLUMNS: Tuple[str, ...] = (
    "return_1",
    "return_3",
    "return_5",
    "return_10",
    "range_pct",
    "body_pct",
    "upper_wick_pct",
    "lower_wick_pct",
    "close_position",
    "ma_5",
    "ma_10",
    "ma_20",
    "ma_60",
    "ma_120",
    "ma_200",
    "ma5_slope_3",
    "ma20_slope_5",
    "ma60_slope_5",
    "close_vs_ma5_pct",
    "close_vs_ma20_pct",
    "close_vs_ma60_pct",
    "close_vs_ma120_pct",
    "close_vs_ma200_pct",
    "ma5_vs_ma20_pct",
    "ma20_vs_ma60_pct",
    "ma60_vs_ma120_pct",
    "rolling_high_5",
    "rolling_high_10",
    "rolling_high_20",
    "rolling_high_60",
    "rolling_low_5",
    "rolling_low_10",
    "rolling_low_20",
    "rolling_low_60",
    "distance_high_20_pct",
    "distance_low_20_pct",
    "distance_high_60_pct",
    "distance_low_60_pct",
    "volume_ma_5",
    "volume_ma_20",
    "volume_ma_60",
    "volume_ratio_5",
    "volume_ratio_20",
    "volume_ratio_60",
    "trade_value_ma_5",
    "trade_value_ma_20",
    "trade_value_ratio_20",
    "volatility_5",
    "volatility_20",
    "volatility_60",
    "atr_14",
    "atr_pct_14",
    "rsi_14",
)


# ======================================================================
# DATACLASSES
# ======================================================================

@dataclass
class FileAuditResult:
    timeframe: str
    market: str
    relative_path: str
    sha256_before: str
    sha256_after: str
    source_unchanged: bool
    rows: int
    valid_rows: int
    first_timestamp: Optional[str]
    last_timestamp: Optional[str]
    duplicate_timestamp_count: int
    invalid_ohlc_count: int
    non_positive_price_count: int
    timestamp_parse_failure_count: int
    non_monotonic_count: int
    gap_count: int
    largest_gap_seconds: float
    expected_seconds: int
    minimum_research_rows: int
    research_ready: bool
    feature_rows: int
    feature_ready_rows: int
    status: str
    warning_count: int
    error_count: int


@dataclass
class RunStatistics:
    files_discovered: int = 0
    files_processed: int = 0
    files_skipped_checkpoint: int = 0
    files_ready: int = 0
    files_warning: int = 0
    files_error: int = 0
    total_rows: int = 0
    total_valid_rows: int = 0
    total_gaps: int = 0
    source_mutation_detected: int = 0


# ======================================================================
# UTILITY
# ======================================================================

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def print_rule(char: str = "=", width: int = 72) -> None:
    print(char * width)


def print_header(title: str) -> None:
    print()
    print_rule("=")
    print(title)
    print_rule("=")
    print()


def safe_float(value: Any) -> Optional[float]:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None

    if not math.isfinite(result):
        return None

    return result


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    temp_path = path.with_name(path.name + ".tmp")

    with temp_path.open(
        "w",
        encoding="utf-8",
        newline="\n",
    ) as handle:
        handle.write(text)
        handle.flush()
        os.fsync(handle.fileno())

    os.replace(temp_path, path)


def atomic_write_json(path: Path, payload: Any) -> None:
    text = json.dumps(
        payload,
        ensure_ascii=False,
        indent=2,
        sort_keys=False,
    )

    atomic_write_text(
        path,
        text + "\n",
    )


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        while True:
            block = handle.read(1024 * 1024)

            if not block:
                break

            digest.update(block)

    return digest.hexdigest()


def normalize_market_name(path: Path) -> str:
    return path.stem.upper()


def relative_path_string(path: Path, root: Path) -> str:
    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)


def sanitize_json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            str(key): sanitize_json_value(item)
            for key, item in value.items()
        }

    if isinstance(value, (list, tuple)):
        return [
            sanitize_json_value(item)
            for item in value
        ]

    if isinstance(value, np.generic):
        value = value.item()

    if isinstance(value, float):
        if not math.isfinite(value):
            return None

    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None
        return value.isoformat()

    if value is pd.NaT:
        return None

    return value


# ======================================================================
# PROJECT ROOT DISCOVERY
# ======================================================================

def looks_like_project_root(path: Path) -> bool:
    if not path.exists():
        return False

    if not path.is_dir():
        return False

    if (path / "data" / "ohlcv").is_dir():
        return True

    return False


def discover_project_root(
    explicit_root: Optional[str],
) -> Tuple[Path, str]:

    candidates: List[Tuple[Path, str]] = []

    if explicit_root:
        candidates.append(
            (
                Path(explicit_root).expanduser(),
                "command-line",
            )
        )

    env_root = os.environ.get(
        "UPBIT_SURGE_MONITOR_ROOT",
        "",
    ).strip()

    if env_root:
        candidates.append(
            (
                Path(env_root).expanduser(),
                "environment",
            )
        )

    user_profile = os.environ.get(
        "USERPROFILE",
        "",
    ).strip()

    if user_profile:
        candidates.append(
            (
                Path(user_profile)
                / "Documents"
                / "upbit-surge-monitor",
                "user-documents",
            )
        )

    candidates.append(
        (
            Path.cwd(),
            "current-working-directory",
        )
    )

    script_root = Path(__file__).resolve().parent

    candidates.append(
        (
            script_root,
            "script-directory",
        )
    )

    checked: List[str] = []

    for candidate, source in candidates:
        try:
            resolved = candidate.resolve()
        except OSError:
            resolved = candidate.absolute()

        key = str(resolved).lower()

        if key in checked:
            continue

        checked.append(key)

        if looks_like_project_root(resolved):
            return resolved, source

    message_lines = [
        "Unable to locate production project root.",
        "",
        "Checked:",
    ]

    for candidate, source in candidates:
        message_lines.append(
            f"  - {source}: {candidate}"
        )

    raise FileNotFoundError(
        "\n".join(message_lines)
    )


# ======================================================================
# OUTPUT DIRECTORY
# ======================================================================

def get_output_paths(
    project_root: Path,
) -> Dict[str, Path]:

    report_dir = (
        project_root
        / "data"
        / "reports"
        / REPORT_NAMESPACE
    )

    validation_dir = (
        project_root
        / "data"
        / "validation"
    )

    return {
        "report_dir": report_dir,
        "validation_dir": validation_dir,
        "result": report_dir / RESULT_FILENAME,
        "summary": report_dir / SUMMARY_FILENAME,
        "detail": report_dir / DETAIL_FILENAME,
        "manifest": report_dir / MANIFEST_FILENAME,
        "registry": report_dir / PATTERN_REGISTRY_FILENAME,
        "feature_contract": report_dir / FEATURE_CONTRACT_FILENAME,
        "readme": report_dir / README_FILENAME,
        "checkpoint": validation_dir / CHECKPOINT_FILENAME,
    }


# ======================================================================
# OHLCV DISCOVERY
# ======================================================================

def discover_ohlcv_files(
    project_root: Path,
    requested_timeframes: Sequence[str],
) -> List[Tuple[str, Path]]:

    base_dir = (
        project_root
        / "data"
        / "ohlcv"
    )

    if not base_dir.is_dir():
        raise FileNotFoundError(
            f"OHLCV directory not found: {base_dir}"
        )

    discovered: List[Tuple[str, Path]] = []

    for timeframe in requested_timeframes:
        if timeframe not in TIMEFRAME_SECONDS:
            raise ValueError(
                f"Unsupported timeframe: {timeframe}"
            )

        timeframe_dir = base_dir / timeframe

        if not timeframe_dir.is_dir():
            raise FileNotFoundError(
                f"Required OHLCV timeframe directory missing: "
                f"{timeframe_dir}"
            )

        files = sorted(
            path
            for path in timeframe_dir.glob("*.csv")
            if path.is_file()
        )

        for path in files:
            discovered.append(
                (
                    timeframe,
                    path,
                )
            )

    if not discovered:
        raise FileNotFoundError(
            "No production OHLCV CSV files discovered."
        )

    return discovered


# ======================================================================
# COLUMN DISCOVERY
# ======================================================================

def find_column(
    columns: Iterable[str],
    candidates: Sequence[str],
) -> Optional[str]:

    normalized: Dict[str, str] = {
        str(column).strip().lower(): str(column)
        for column in columns
    }

    for candidate in candidates:
        found = normalized.get(
            candidate.lower()
        )

        if found is not None:
            return found

    return None


def validate_required_price_columns(
    frame: pd.DataFrame,
    path: Path,
) -> Dict[str, str]:

    normalized: Dict[str, str] = {
        str(column).strip().lower(): str(column)
        for column in frame.columns
    }

    mapping: Dict[str, str] = {}

    missing: List[str] = []

    for required in REQUIRED_PRICE_COLUMNS:
        actual = normalized.get(required)

        if actual is None:
            missing.append(required)
        else:
            mapping[required] = actual

    if missing:
        raise ValueError(
            f"Missing required OHLC columns in {path}: "
            f"{', '.join(missing)}"
        )

    return mapping


# ======================================================================
# TIMESTAMP NORMALIZATION
# ======================================================================

def parse_timestamp_series(
    series: pd.Series,
) -> pd.Series:

    numeric = pd.to_numeric(
        series,
        errors="coerce",
    )

    numeric_ratio = float(
        numeric.notna().mean()
    ) if len(series) else 0.0

    if numeric_ratio >= 0.95:
        finite_numeric = numeric.dropna()

        if not finite_numeric.empty:
            median_abs = float(
                finite_numeric.abs().median()
            )

            if median_abs >= 1e14:
                unit = "us"
            elif median_abs >= 1e11:
                unit = "ms"
            else:
                unit = "s"

            parsed = pd.to_datetime(
                numeric,
                unit=unit,
                utc=True,
                errors="coerce",
            )

            return parsed

    parsed = pd.to_datetime(
        series,
        utc=True,
        errors="coerce",
    )

    return parsed


# ======================================================================
# OHLCV LOAD
# ======================================================================

def load_ohlcv(
    path: Path,
) -> Tuple[pd.DataFrame, Dict[str, Optional[str]]]:

    frame = pd.read_csv(
        path,
        low_memory=False,
    )

    if frame.empty:
        raise ValueError(
            f"OHLCV file is empty: {path}"
        )

    price_mapping = validate_required_price_columns(
        frame,
        path,
    )

    timestamp_column = find_column(
        frame.columns,
        TIMESTAMP_CANDIDATES,
    )

    if timestamp_column is None:
        raise ValueError(
            f"No supported timestamp column found: {path}"
        )

    volume_column = find_column(
        frame.columns,
        VOLUME_CANDIDATES,
    )

    trade_value_column = find_column(
        frame.columns,
        TRADE_VALUE_CANDIDATES,
    )

    normalized = pd.DataFrame(
        index=frame.index
    )

    normalized["timestamp"] = parse_timestamp_series(
        frame[timestamp_column]
    )

    for canonical in REQUIRED_PRICE_COLUMNS:
        normalized[canonical] = pd.to_numeric(
            frame[price_mapping[canonical]],
            errors="coerce",
        )

    if volume_column is not None:
        normalized["volume"] = pd.to_numeric(
            frame[volume_column],
            errors="coerce",
        )
    else:
        normalized["volume"] = np.nan

    if trade_value_column is not None:
        normalized["trade_value"] = pd.to_numeric(
            frame[trade_value_column],
            errors="coerce",
        )
    else:
        normalized["trade_value"] = np.nan

    metadata = {
        "timestamp_column": timestamp_column,
        "volume_column": volume_column,
        "trade_value_column": trade_value_column,
    }

    return normalized, metadata


# ======================================================================
# SOURCE VALIDATION
# ======================================================================

def validate_source_frame(
    frame: pd.DataFrame,
    timeframe: str,
) -> Dict[str, Any]:

    expected_seconds = TIMEFRAME_SECONDS[timeframe]

    timestamp_parse_failure_count = int(
        frame["timestamp"].isna().sum()
    )

    duplicate_timestamp_count = int(
        frame["timestamp"]
        .dropna()
        .duplicated()
        .sum()
    )

    valid_timestamp_frame = (
        frame
        .dropna(subset=["timestamp"])
        .copy()
    )

    original_timestamps = (
        valid_timestamp_frame["timestamp"]
        .reset_index(drop=True)
    )

    if len(original_timestamps) >= 2:
        original_diffs = (
            original_timestamps.diff()
        )

        non_monotonic_count = int(
            (
                original_diffs
                <= pd.Timedelta(0)
            )
            .fillna(False)
            .sum()
        )
    else:
        non_monotonic_count = 0

    numeric_required = (
        frame[
            [
                "open",
                "high",
                "low",
                "close",
            ]
        ]
        .apply(
            pd.to_numeric,
            errors="coerce",
        )
    )

    non_positive_mask = (
        numeric_required
        <= 0
    ).any(axis=1)

    non_positive_price_count = int(
        non_positive_mask.sum()
    )

    invalid_ohlc_mask = (
        (
            numeric_required["high"]
            < numeric_required[
                [
                    "open",
                    "close",
                    "low",
                ]
            ].max(axis=1)
        )
        |
        (
            numeric_required["low"]
            > numeric_required[
                [
                    "open",
                    "close",
                    "high",
                ]
            ].min(axis=1)
        )
    )

    invalid_ohlc_count = int(
        invalid_ohlc_mask.sum()
    )

    sorted_unique = (
        valid_timestamp_frame
        .drop_duplicates(
            subset=["timestamp"],
            keep="last",
        )
        .sort_values("timestamp")
        .reset_index(drop=True)
    )

    if len(sorted_unique) >= 2:
        diff_seconds = (
            sorted_unique["timestamp"]
            .diff()
            .dt.total_seconds()
        )

        gap_mask = (
            diff_seconds
            > expected_seconds
        )

        gap_count = int(
            gap_mask.fillna(False).sum()
        )

        if diff_seconds.dropna().empty:
            largest_gap_seconds = 0.0
        else:
            largest_gap_seconds = float(
                diff_seconds.dropna().max()
            )
    else:
        gap_count = 0
        largest_gap_seconds = 0.0

    first_timestamp: Optional[str] = None
    last_timestamp: Optional[str] = None

    if not sorted_unique.empty:
        first_timestamp = (
            sorted_unique["timestamp"]
            .iloc[0]
            .isoformat()
        )

        last_timestamp = (
            sorted_unique["timestamp"]
            .iloc[-1]
            .isoformat()
        )

    return {
        "timestamp_parse_failure_count":
            timestamp_parse_failure_count,
        "duplicate_timestamp_count":
            duplicate_timestamp_count,
        "non_monotonic_count":
            non_monotonic_count,
        "non_positive_price_count":
            non_positive_price_count,
        "invalid_ohlc_count":
            invalid_ohlc_count,
        "gap_count":
            gap_count,
        "largest_gap_seconds":
            largest_gap_seconds,
        "first_timestamp":
            first_timestamp,
        "last_timestamp":
            last_timestamp,
        "sorted_unique_frame":
            sorted_unique,
    }


# ======================================================================
# RESEARCH MINIMUMS
# ======================================================================

def minimum_research_rows(
    timeframe: str,
) -> int:

    if timeframe == "h1":
        return 500

    if timeframe == "h4":
        return 300

    if timeframe == "d1":
        return 200

    raise ValueError(
        f"Unsupported timeframe: {timeframe}"
    )


# ======================================================================
# TECHNICAL FEATURE HELPERS
# ======================================================================

def safe_divide(
    numerator: pd.Series,
    denominator: pd.Series,
) -> pd.Series:

    denominator_safe = denominator.replace(
        0,
        np.nan,
    )

    result = numerator / denominator_safe

    result = result.replace(
        [np.inf, -np.inf],
        np.nan,
    )

    return result


def pct_distance(
    value: pd.Series,
    reference: pd.Series,
) -> pd.Series:

    return (
        safe_divide(
            value - reference,
            reference,
        )
        * 100.0
    )


def calculate_rsi(
    close: pd.Series,
    period: int = 14,
) -> pd.Series:

    delta = close.diff()

    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)

    avg_gain = gain.ewm(
        alpha=1.0 / period,
        adjust=False,
        min_periods=period,
    ).mean()

    avg_loss = loss.ewm(
        alpha=1.0 / period,
        adjust=False,
        min_periods=period,
    ).mean()

    rs = safe_divide(
        avg_gain,
        avg_loss,
    )

    rsi = 100.0 - (
        100.0 / (1.0 + rs)
    )

    zero_loss = (
        avg_loss == 0
    ) & (
        avg_gain > 0
    )

    zero_gain = (
        avg_gain == 0
    ) & (
        avg_loss > 0
    )

    both_zero = (
        avg_gain == 0
    ) & (
        avg_loss == 0
    )

    rsi = rsi.mask(
        zero_loss,
        100.0,
    )

    rsi = rsi.mask(
        zero_gain,
        0.0,
    )

    rsi = rsi.mask(
        both_zero,
        50.0,
    )

    return rsi


def calculate_true_range(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
) -> pd.Series:

    previous_close = close.shift(1)

    components = pd.concat(
        [
            high - low,
            (high - previous_close).abs(),
            (low - previous_close).abs(),
        ],
        axis=1,
    )

    return components.max(axis=1)


# ======================================================================
# COMMON QUANTITATIVE FEATURES
# ======================================================================

def build_common_features(
    frame: pd.DataFrame,
) -> pd.DataFrame:
    """
    IMPORTANT:
    Every feature in this function uses ONLY the current candle and
    candles at or before the current candle.

    No negative shift is allowed.
    No centered rolling window is allowed.
    """

    source = frame.copy()

    source = (
        source
        .sort_values("timestamp")
        .reset_index(drop=True)
    )

    result = pd.DataFrame(
        index=source.index
    )

    result["timestamp"] = source["timestamp"]

    for column in (
        "open",
        "high",
        "low",
        "close",
        "volume",
        "trade_value",
    ):
        result[column] = source[column]

    close = source["close"].astype(float)
    open_price = source["open"].astype(float)
    high = source["high"].astype(float)
    low = source["low"].astype(float)
    volume = source["volume"].astype(float)
    trade_value = source["trade_value"].astype(float)

    # ------------------------------------------------------------------
    # Historical returns
    # ------------------------------------------------------------------

    result["return_1"] = (
        close.pct_change(
            periods=1,
            fill_method=None,
        )
        * 100.0
    )

    result["return_3"] = (
        close.pct_change(
            periods=3,
            fill_method=None,
        )
        * 100.0
    )

    result["return_5"] = (
        close.pct_change(
            periods=5,
            fill_method=None,
        )
        * 100.0
    )

    result["return_10"] = (
        close.pct_change(
            periods=10,
            fill_method=None,
        )
        * 100.0
    )

    # ------------------------------------------------------------------
    # Candle geometry
    # ------------------------------------------------------------------

    candle_range = high - low
    candle_body = (close - open_price).abs()

    upper_body = pd.concat(
        [
            open_price,
            close,
        ],
        axis=1,
    ).max(axis=1)

    lower_body = pd.concat(
        [
            open_price,
            close,
        ],
        axis=1,
    ).min(axis=1)

    upper_wick = high - upper_body
    lower_wick = lower_body - low

    result["range_pct"] = (
        safe_divide(
            candle_range,
            open_price,
        )
        * 100.0
    )

    result["body_pct"] = (
        safe_divide(
            candle_body,
            open_price,
        )
        * 100.0
    )

    result["upper_wick_pct"] = (
        safe_divide(
            upper_wick,
            open_price,
        )
        * 100.0
    )

    result["lower_wick_pct"] = (
        safe_divide(
            lower_wick,
            open_price,
        )
        * 100.0
    )

    result["close_position"] = safe_divide(
        close - low,
        candle_range,
    )

    # ------------------------------------------------------------------
    # Moving averages
    # ------------------------------------------------------------------

    moving_average_periods = (
        5,
        10,
        20,
        60,
        120,
        200,
    )

    for period in moving_average_periods:
        result[f"ma_{period}"] = (
            close
            .rolling(
                window=period,
                min_periods=period,
            )
            .mean()
        )

    # ------------------------------------------------------------------
    # MA slopes
    # ------------------------------------------------------------------

    result["ma5_slope_3"] = pct_distance(
        result["ma_5"],
        result["ma_5"].shift(3),
    )

    result["ma20_slope_5"] = pct_distance(
        result["ma_20"],
        result["ma_20"].shift(5),
    )

    result["ma60_slope_5"] = pct_distance(
        result["ma_60"],
        result["ma_60"].shift(5),
    )

    # ------------------------------------------------------------------
    # Price / MA relationships
    # ------------------------------------------------------------------

    result["close_vs_ma5_pct"] = pct_distance(
        close,
        result["ma_5"],
    )

    result["close_vs_ma20_pct"] = pct_distance(
        close,
        result["ma_20"],
    )

    result["close_vs_ma60_pct"] = pct_distance(
        close,
        result["ma_60"],
    )

    result["close_vs_ma120_pct"] = pct_distance(
        close,
        result["ma_120"],
    )

    result["close_vs_ma200_pct"] = pct_distance(
        close,
        result["ma_200"],
    )

    result["ma5_vs_ma20_pct"] = pct_distance(
        result["ma_5"],
        result["ma_20"],
    )

    result["ma20_vs_ma60_pct"] = pct_distance(
        result["ma_20"],
        result["ma_60"],
    )

    result["ma60_vs_ma120_pct"] = pct_distance(
        result["ma_60"],
        result["ma_120"],
    )

    # ------------------------------------------------------------------
    # Historical highs/lows
    # ------------------------------------------------------------------

    for period in (
        5,
        10,
        20,
        60,
    ):
        result[f"rolling_high_{period}"] = (
            high
            .rolling(
                window=period,
                min_periods=period,
            )
            .max()
        )

        result[f"rolling_low_{period}"] = (
            low
            .rolling(
                window=period,
                min_periods=period,
            )
            .min()
        )

    result["distance_high_20_pct"] = pct_distance(
        close,
        result["rolling_high_20"],
    )

    result["distance_low_20_pct"] = pct_distance(
        close,
        result["rolling_low_20"],
    )

    result["distance_high_60_pct"] = pct_distance(
        close,
        result["rolling_high_60"],
    )

    result["distance_low_60_pct"] = pct_distance(
        close,
        result["rolling_low_60"],
    )

    # ------------------------------------------------------------------
    # Volume
    # ------------------------------------------------------------------

    for period in (
        5,
        20,
        60,
    ):
        result[f"volume_ma_{period}"] = (
            volume
            .rolling(
                window=period,
                min_periods=period,
            )
            .mean()
        )

        result[f"volume_ratio_{period}"] = safe_divide(
            volume,
            result[f"volume_ma_{period}"],
        )

    # ------------------------------------------------------------------
    # Trade value
    # ------------------------------------------------------------------

    result["trade_value_ma_5"] = (
        trade_value
        .rolling(
            window=5,
            min_periods=5,
        )
        .mean()
    )

    result["trade_value_ma_20"] = (
        trade_value
        .rolling(
            window=20,
            min_periods=20,
        )
        .mean()
    )

    result["trade_value_ratio_20"] = safe_divide(
        trade_value,
        result["trade_value_ma_20"],
    )

    # ------------------------------------------------------------------
    # Historical volatility
    # ------------------------------------------------------------------

    returns = close.pct_change(
        fill_method=None,
    )

    result["volatility_5"] = (
        returns
        .rolling(
            window=5,
            min_periods=5,
        )
        .std()
        * 100.0
    )

    result["volatility_20"] = (
        returns
        .rolling(
            window=20,
            min_periods=20,
        )
        .std()
        * 100.0
    )

    result["volatility_60"] = (
        returns
        .rolling(
            window=60,
            min_periods=60,
        )
        .std()
        * 100.0
    )

    # ------------------------------------------------------------------
    # ATR
    # ------------------------------------------------------------------

    true_range = calculate_true_range(
        high,
        low,
        close,
    )

    result["atr_14"] = (
        true_range
        .rolling(
            window=14,
            min_periods=14,
        )
        .mean()
    )

    result["atr_pct_14"] = (
        safe_divide(
            result["atr_14"],
            close,
        )
        * 100.0
    )

    # ------------------------------------------------------------------
    # RSI
    # ------------------------------------------------------------------

    result["rsi_14"] = calculate_rsi(
        close,
        period=14,
    )

    return result


# ======================================================================
# FUTURE LEAKAGE STATIC CONTRACT
# ======================================================================

FORBIDDEN_FEATURE_NAME_PATTERNS: Tuple[str, ...] = (
    "future",
    "forward",
    "next_",
    "target",
    "label",
    "outcome",
    "profit_after",
    "return_after",
    "max_future",
    "min_future",
)


def validate_feature_contract_no_future_leakage() -> None:

    violations: List[str] = []

    for column in COMMON_FEATURE_COLUMNS:
        lowered = column.lower()

        for forbidden in FORBIDDEN_FEATURE_NAME_PATTERNS:
            if forbidden in lowered:
                violations.append(column)

    if violations:
        raise RuntimeError(
            "Future-information-like feature names detected: "
            + ", ".join(sorted(set(violations)))
        )


# ======================================================================
# PATTERN REGISTRY VALIDATION
# ======================================================================

def validate_pattern_registry() -> None:

    if len(PATTERN_DEFINITIONS) != 9:
        raise RuntimeError(
            "Pattern registry must contain exactly 9 patterns."
        )

    pattern_ids = [
        item.pattern_id
        for item in PATTERN_DEFINITIONS
    ]

    korean_names = [
        item.korean_name
        for item in PATTERN_DEFINITIONS
    ]

    if len(pattern_ids) != len(set(pattern_ids)):
        raise RuntimeError(
            "Duplicate pattern_id detected."
        )

    if len(korean_names) != len(set(korean_names)):
        raise RuntimeError(
            "Duplicate Korean pattern name detected."
        )

    for item in PATTERN_DEFINITIONS:
        if item.future_data_allowed:
            raise RuntimeError(
                f"Future data unexpectedly allowed for "
                f"{item.korean_name}"
            )


# ======================================================================
# CHECKPOINT
# ======================================================================

def empty_checkpoint() -> Dict[str, Any]:
    return {
        "program": PROGRAM_NAME,
        "version": PROGRAM_VERSION,
        "stage": STAGE_NAME,
        "created_at_utc": utc_now_iso(),
        "updated_at_utc": utc_now_iso(),
        "completed": {},
    }


def load_checkpoint(
    path: Path,
) -> Dict[str, Any]:

    if not path.exists():
        return empty_checkpoint()

    try:
        with path.open(
            "r",
            encoding="utf-8",
        ) as handle:
            payload = json.load(handle)
    except Exception:
        return empty_checkpoint()

    if not isinstance(payload, dict):
        return empty_checkpoint()

    if payload.get("program") != PROGRAM_NAME:
        return empty_checkpoint()

    if payload.get("version") != PROGRAM_VERSION:
        return empty_checkpoint()

    completed = payload.get("completed")

    if not isinstance(completed, dict):
        payload["completed"] = {}

    return payload


def save_checkpoint(
    path: Path,
    checkpoint: Dict[str, Any],
) -> None:

    checkpoint["updated_at_utc"] = utc_now_iso()

    atomic_write_json(
        path,
        sanitize_json_value(checkpoint),
    )


def checkpoint_key(
    timeframe: str,
    market: str,
) -> str:
    return f"{timeframe}|{market}"


def checkpoint_entry_valid(
    entry: Any,
    current_sha256: str,
    relative_path: str,
) -> bool:

    if not isinstance(entry, dict):
        return False

    if entry.get("sha256") != current_sha256:
        return False

    if entry.get("relative_path") != relative_path:
        return False

    if entry.get("status") not in {
        "READY",
        "WARN",
    }:
        return False

    return True


# ======================================================================
# CSV OUTPUT
# ======================================================================

DETAIL_COLUMNS: Tuple[str, ...] = (
    "timeframe",
    "market",
    "relative_path",
    "sha256_before",
    "sha256_after",
    "source_unchanged",
    "rows",
    "valid_rows",
    "first_timestamp",
    "last_timestamp",
    "duplicate_timestamp_count",
    "invalid_ohlc_count",
    "non_positive_price_count",
    "timestamp_parse_failure_count",
    "non_monotonic_count",
    "gap_count",
    "largest_gap_seconds",
    "expected_seconds",
    "minimum_research_rows",
    "research_ready",
    "feature_rows",
    "feature_ready_rows",
    "status",
    "warning_count",
    "error_count",
)


def write_detail_csv(
    path: Path,
    rows: Sequence[FileAuditResult],
) -> None:

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temp_path = path.with_name(
        path.name + ".tmp"
    )

    with temp_path.open(
        "w",
        encoding="utf-8-sig",
        newline="",
    ) as handle:

        writer = csv.DictWriter(
            handle,
            fieldnames=list(DETAIL_COLUMNS),
        )

        writer.writeheader()

        for item in rows:
            payload = asdict(item)

            writer.writerow(
                {
                    column: payload.get(column)
                    for column in DETAIL_COLUMNS
                }
            )

        handle.flush()
        os.fsync(handle.fileno())

    os.replace(
        temp_path,
        path,
    )


SUMMARY_COLUMNS: Tuple[str, ...] = (
    "timeframe",
    "file_count",
    "ready_count",
    "warning_count",
    "error_count",
    "row_count",
    "valid_row_count",
    "gap_count",
)


def build_summary_rows(
    details: Sequence[FileAuditResult],
) -> List[Dict[str, Any]]:

    rows: List[Dict[str, Any]] = []

    grouped: Dict[str, List[FileAuditResult]] = {
        timeframe: []
        for timeframe in SUPPORTED_TIMEFRAMES
    }

    for item in details:
        grouped.setdefault(
            item.timeframe,
            [],
        ).append(item)

    for timeframe in SUPPORTED_TIMEFRAMES:
        items = grouped.get(
            timeframe,
            [],
        )

        if not items:
            continue

        rows.append(
            {
                "timeframe": timeframe,
                "file_count": len(items),
                "ready_count": sum(
                    1
                    for item in items
                    if item.status == "READY"
                ),
                "warning_count": sum(
                    1
                    for item in items
                    if item.status == "WARN"
                ),
                "error_count": sum(
                    1
                    for item in items
                    if item.status == "ERROR"
                ),
                "row_count": sum(
                    item.rows
                    for item in items
                ),
                "valid_row_count": sum(
                    item.valid_rows
                    for item in items
                ),
                "gap_count": sum(
                    item.gap_count
                    for item in items
                ),
            }
        )

    rows.append(
        {
            "timeframe": "ALL",
            "file_count": len(details),
            "ready_count": sum(
                1
                for item in details
                if item.status == "READY"
            ),
            "warning_count": sum(
                1
                for item in details
                if item.status == "WARN"
            ),
            "error_count": sum(
                1
                for item in details
                if item.status == "ERROR"
            ),
            "row_count": sum(
                item.rows
                for item in details
            ),
            "valid_row_count": sum(
                item.valid_rows
                for item in details
            ),
            "gap_count": sum(
                item.gap_count
                for item in details
            ),
        }
    )

    return rows


def write_summary_csv(
    path: Path,
    rows: Sequence[Dict[str, Any]],
) -> None:

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temp_path = path.with_name(
        path.name + ".tmp"
    )

    with temp_path.open(
        "w",
        encoding="utf-8-sig",
        newline="",
    ) as handle:

        writer = csv.DictWriter(
            handle,
            fieldnames=list(SUMMARY_COLUMNS),
        )

        writer.writeheader()

        for row in rows:
            writer.writerow(
                {
                    column: row.get(column)
                    for column in SUMMARY_COLUMNS
                }
            )

        handle.flush()
        os.fsync(handle.fileno())

    os.replace(
        temp_path,
        path,
    )


# ======================================================================
# MANIFEST
# ======================================================================

MANIFEST_COLUMNS: Tuple[str, ...] = (
    "timeframe",
    "market",
    "relative_path",
    "sha256",
    "size_bytes",
)


def write_source_manifest(
    path: Path,
    rows: Sequence[Dict[str, Any]],
) -> None:

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temp_path = path.with_name(
        path.name + ".tmp"
    )

    with temp_path.open(
        "w",
        encoding="utf-8-sig",
        newline="",
    ) as handle:

        writer = csv.DictWriter(
            handle,
            fieldnames=list(MANIFEST_COLUMNS),
        )

        writer.writeheader()

        for row in rows:
            writer.writerow(
                {
                    column: row.get(column)
                    for column in MANIFEST_COLUMNS
                }
            )

        handle.flush()
        os.fsync(handle.fileno())

    os.replace(
        temp_path,
        path,
    )


# ======================================================================
# PATTERN REGISTRY OUTPUT
# ======================================================================

def write_pattern_registry(
    path: Path,
) -> None:

    payload = {
        "program": PROGRAM_NAME,
        "version": PROGRAM_VERSION,
        "stage": STAGE_NAME,
        "generated_at_utc": utc_now_iso(),
        "pattern_count": len(PATTERN_DEFINITIONS),
        "future_information_allowed": False,
        "patterns": [
            asdict(item)
            for item in PATTERN_DEFINITIONS
        ],
    }

    atomic_write_json(
        path,
        payload,
    )


# ======================================================================
# FEATURE CONTRACT OUTPUT
# ======================================================================

def write_feature_contract(
    path: Path,
) -> None:

    payload = {
        "program": PROGRAM_NAME,
        "version": PROGRAM_VERSION,
        "generated_at_utc": utc_now_iso(),
        "contract": {
            "current_candle_allowed": True,
            "past_candles_allowed": True,
            "future_candles_allowed": False,
            "negative_shift_allowed": False,
            "centered_rolling_allowed": False,
            "future_labels_generated": False,
            "backtest_generated": False,
        },
        "common_features": list(
            COMMON_FEATURE_COLUMNS
        ),
    }

    atomic_write_json(
        path,
        payload,
    )


# ======================================================================
# README OUTPUT
# ======================================================================

def write_report_readme(
    path: Path,
) -> None:

    content = f"""\
{PROJECT_NAME}
{STAGE_NAME}
{PROGRAM_VERSION}

This directory was generated by:

    {PROGRAM_NAME}

PURPOSE
-------
Prepare and validate the quantitative research foundation for:

    1. 256
    2. 밥그릇 3번자리
    3. 역매공파
    4. 공구리
    5. 돌파
    6. 눌림목
    7. 기준봉
    8. 하이힐
    9. 오돌이

IMPORTANT
---------
Clean V001 does NOT invent or finalize pattern formulas.

It creates and validates the common quantitative research contract.

SOURCE SAFETY
-------------
Production OHLCV is READ ONLY.

This stage does not:

    - repair candles
    - download candles
    - overwrite candles
    - delete candles
    - generate future labels
    - backtest
    - predict
    - trade

FUTURE INFORMATION
------------------
Pattern features may use only:

    - current candle
    - candles before current candle

No future candle is permitted.

NEXT
----
After this framework passes its GitHub test and production workflow,
the nine pattern definitions can be quantified independently and
validated against historical outcomes in later stages.
"""

    atomic_write_text(
        path,
        content,
    )


# ======================================================================
# PROCESS SINGLE FILE
# ======================================================================

def process_file(
    project_root: Path,
    timeframe: str,
    path: Path,
    source_sha256: str,
) -> FileAuditResult:

    market = normalize_market_name(path)

    relative_path = relative_path_string(
        path,
        project_root,
    )

    frame, _metadata = load_ohlcv(
        path
    )

    source_validation = validate_source_frame(
        frame,
        timeframe,
    )

    normalized = source_validation[
        "sorted_unique_frame"
    ]

    rows = int(len(frame))
    valid_rows = int(len(normalized))

    minimum_rows = minimum_research_rows(
        timeframe
    )

    warnings: List[str] = []
    errors: List[str] = []

    if (
        source_validation[
            "timestamp_parse_failure_count"
        ]
        > 0
    ):
        errors.append(
            "TIMESTAMP_PARSE_FAILURE"
        )

    if (
        source_validation[
            "duplicate_timestamp_count"
        ]
        > 0
    ):
        errors.append(
            "DUPLICATE_TIMESTAMP"
        )

    if (
        source_validation[
            "invalid_ohlc_count"
        ]
        > 0
    ):
        errors.append(
            "INVALID_OHLC"
        )

    if (
        source_validation[
            "non_positive_price_count"
        ]
        > 0
    ):
        errors.append(
            "NON_POSITIVE_PRICE"
        )

    if (
        source_validation[
            "non_monotonic_count"
        ]
        > 0
    ):
        errors.append(
            "NON_MONOTONIC_TIMESTAMP"
        )

    if (
        source_validation[
            "gap_count"
        ]
        > 0
    ):
        warnings.append(
            "HISTORICAL_GAP_PRESENT"
        )

    if valid_rows < minimum_rows:
        warnings.append(
            "SHORT_HISTORY"
        )

    feature_frame = build_common_features(
        normalized
    )

    feature_rows = int(
        len(feature_frame)
    )

    feature_ready_mask = (
        feature_frame[
            list(COMMON_FEATURE_COLUMNS)
        ]
        .notna()
        .all(axis=1)
    )

    feature_ready_rows = int(
        feature_ready_mask.sum()
    )

    if feature_ready_rows <= 0:
        warnings.append(
            "NO_FULL_COMMON_FEATURE_ROWS"
        )

    research_ready = (
        len(errors) == 0
        and valid_rows >= minimum_rows
        and feature_ready_rows > 0
    )

    if errors:
        status = "ERROR"
    elif warnings:
        status = "WARN"
    else:
        status = "READY"

    source_sha256_after = sha256_file(
        path
    )

    source_unchanged = (
        source_sha256_after
        == source_sha256
    )

    if not source_unchanged:
        status = "ERROR"
        errors.append(
            "SOURCE_MUTATION_DETECTED"
        )
        research_ready = False

    return FileAuditResult(
        timeframe=timeframe,
        market=market,
        relative_path=relative_path,
        sha256_before=source_sha256,
        sha256_after=source_sha256_after,
        source_unchanged=source_unchanged,
        rows=rows,
        valid_rows=valid_rows,
        first_timestamp=source_validation[
            "first_timestamp"
        ],
        last_timestamp=source_validation[
            "last_timestamp"
        ],
        duplicate_timestamp_count=int(
            source_validation[
                "duplicate_timestamp_count"
            ]
        ),
        invalid_ohlc_count=int(
            source_validation[
                "invalid_ohlc_count"
            ]
        ),
        non_positive_price_count=int(
            source_validation[
                "non_positive_price_count"
            ]
        ),
        timestamp_parse_failure_count=int(
            source_validation[
                "timestamp_parse_failure_count"
            ]
        ),
        non_monotonic_count=int(
            source_validation[
                "non_monotonic_count"
            ]
        ),
        gap_count=int(
            source_validation[
                "gap_count"
            ]
        ),
        largest_gap_seconds=float(
            source_validation[
                "largest_gap_seconds"
            ]
        ),
        expected_seconds=int(
            TIMEFRAME_SECONDS[timeframe]
        ),
        minimum_research_rows=int(
            minimum_rows
        ),
        research_ready=bool(
            research_ready
        ),
        feature_rows=feature_rows,
        feature_ready_rows=feature_ready_rows,
        status=status,
        warning_count=len(warnings),
        error_count=len(errors),
    )


# ======================================================================
# CHECKPOINT RESULT RESTORE
# ======================================================================

def result_from_checkpoint(
    entry: Dict[str, Any],
) -> Optional[FileAuditResult]:

    result_payload = entry.get(
        "result"
    )

    if not isinstance(
        result_payload,
        dict,
    ):
        return None

    try:
        return FileAuditResult(
            timeframe=str(
                result_payload["timeframe"]
            ),
            market=str(
                result_payload["market"]
            ),
            relative_path=str(
                result_payload["relative_path"]
            ),
            sha256_before=str(
                result_payload["sha256_before"]
            ),
            sha256_after=str(
                result_payload["sha256_after"]
            ),
            source_unchanged=bool(
                result_payload["source_unchanged"]
            ),
            rows=int(
                result_payload["rows"]
            ),
            valid_rows=int(
                result_payload["valid_rows"]
            ),
            first_timestamp=(
                result_payload.get(
                    "first_timestamp"
                )
            ),
            last_timestamp=(
                result_payload.get(
                    "last_timestamp"
                )
            ),
            duplicate_timestamp_count=int(
                result_payload[
                    "duplicate_timestamp_count"
                ]
            ),
            invalid_ohlc_count=int(
                result_payload[
                    "invalid_ohlc_count"
                ]
            ),
            non_positive_price_count=int(
                result_payload[
                    "non_positive_price_count"
                ]
            ),
            timestamp_parse_failure_count=int(
                result_payload[
                    "timestamp_parse_failure_count"
                ]
            ),
            non_monotonic_count=int(
                result_payload[
                    "non_monotonic_count"
                ]
            ),
            gap_count=int(
                result_payload[
                    "gap_count"
                ]
            ),
            largest_gap_seconds=float(
                result_payload[
                    "largest_gap_seconds"
                ]
            ),
            expected_seconds=int(
                result_payload[
                    "expected_seconds"
                ]
            ),
            minimum_research_rows=int(
                result_payload[
                    "minimum_research_rows"
                ]
            ),
            research_ready=bool(
                result_payload[
                    "research_ready"
                ]
            ),
            feature_rows=int(
                result_payload[
                    "feature_rows"
                ]
            ),
            feature_ready_rows=int(
                result_payload[
                    "feature_ready_rows"
                ]
            ),
            status=str(
                result_payload["status"]
            ),
            warning_count=int(
                result_payload[
                    "warning_count"
                ]
            ),
            error_count=int(
                result_payload[
                    "error_count"
                ]
            ),
        )

    except (
        KeyError,
        TypeError,
        ValueError,
    ):
        return None


# ======================================================================
# ARGUMENTS
# ======================================================================

def parse_args() -> argparse.Namespace:

    parser = argparse.ArgumentParser(
        description=(
            "UPBIT SURGE MONITOR "
            "Pattern Quantification Framework "
            "Clean V001"
        )
    )

    parser.add_argument(
        "--project-root",
        default=None,
        help=(
            "Production project root. "
            "If omitted, auto-discovery is used."
        ),
    )

    parser.add_argument(
        "--timeframes",
        nargs="+",
        default=list(
            SUPPORTED_TIMEFRAMES
        ),
        choices=list(
            SUPPORTED_TIMEFRAMES
        ),
        help=(
            "OHLCV timeframes to validate."
        ),
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help=(
            "Ignore valid checkpoint entries and "
            "reprocess all discovered files."
        ),
    )

    parser.add_argument(
        "--fail-on-warning",
        action="store_true",
        help=(
            "Return non-zero when any WARN result exists."
        ),
    )

    return parser.parse_args()


# ======================================================================
# STARTUP SAFETY
# ======================================================================

def validate_safety_contract() -> None:

    if not READ_ONLY_OHLCV:
        raise RuntimeError(
            "READ_ONLY_OHLCV must remain True."
        )

    forbidden_flags = {
        "ALLOW_OHLCV_WRITE":
            ALLOW_OHLCV_WRITE,
        "ALLOW_OHLCV_DELETE":
            ALLOW_OHLCV_DELETE,
        "ALLOW_RECOVERY":
            ALLOW_RECOVERY,
        "ALLOW_API_DOWNLOAD":
            ALLOW_API_DOWNLOAD,
        "ALLOW_FUTURE_LABELS":
            ALLOW_FUTURE_LABELS,
        "ALLOW_BACKTEST":
            ALLOW_BACKTEST,
        "ALLOW_PREDICTION":
            ALLOW_PREDICTION,
        "ALLOW_TRADING":
            ALLOW_TRADING,
        "ALLOW_GIT_MUTATION":
            ALLOW_GIT_MUTATION,
    }

    enabled = [
        name
        for name, value
        in forbidden_flags.items()
        if value
    ]

    if enabled:
        raise RuntimeError(
            "Forbidden safety flags enabled: "
            + ", ".join(enabled)
        )


# ======================================================================
# SOURCE MANIFEST BASELINE
# ======================================================================

def build_source_baseline(
    project_root: Path,
    discovered: Sequence[
        Tuple[str, Path]
    ],
) -> Tuple[
    Dict[str, str],
    List[Dict[str, Any]],
]:

    hashes: Dict[str, str] = {}

    manifest_rows: List[
        Dict[str, Any]
    ] = []

    for timeframe, path in discovered:
        market = normalize_market_name(
            path
        )

        relative_path = (
            relative_path_string(
                path,
                project_root,
            )
        )

        digest = sha256_file(
            path
        )

        hashes[relative_path] = digest

        manifest_rows.append(
            {
                "timeframe":
                    timeframe,
                "market":
                    market,
                "relative_path":
                    relative_path,
                "sha256":
                    digest,
                "size_bytes":
                    path.stat().st_size,
            }
        )

    return hashes, manifest_rows


# ======================================================================
# FINAL SOURCE HASH VERIFICATION
# ======================================================================

def verify_all_source_hashes(
    project_root: Path,
    discovered: Sequence[
        Tuple[str, Path]
    ],
    baseline_hashes: Dict[str, str],
) -> List[str]:

    changed: List[str] = []

    for _timeframe, path in discovered:
        relative_path = (
            relative_path_string(
                path,
                project_root,
            )
        )

        before = baseline_hashes.get(
            relative_path
        )

        after = sha256_file(
            path
        )

        if before != after:
            changed.append(
                relative_path
            )

    return changed


# ======================================================================
# RESULT PAYLOAD
# ======================================================================

def build_result_payload(
    project_root: Path,
    root_source: str,
    args: argparse.Namespace,
    details: Sequence[FileAuditResult],
    stats: RunStatistics,
    changed_sources: Sequence[str],
) -> Dict[str, Any]:

    status_counts = Counter(
        item.status
        for item in details
    )

    ready_count = sum(
        1
        for item in details
        if item.research_ready
    )

    warning_count = sum(
        item.warning_count
        for item in details
    )

    error_count = sum(
        item.error_count
        for item in details
    )

    all_sources_unchanged = (
        len(changed_sources) == 0
    )

    fatal_data_errors = (
        status_counts.get(
            "ERROR",
            0,
        )
        > 0
    )

    final_status = (
        "PASS"
        if (
            all_sources_unchanged
            and not fatal_data_errors
        )
        else "FAIL"
    )

    payload = {
        "program": PROGRAM_NAME,
        "version": PROGRAM_VERSION,
        "project": PROJECT_NAME,
        "stage": STAGE_NAME,
        "generated_at_utc": utc_now_iso(),
        "status": final_status,
        "project_root": str(
            project_root
        ),
        "project_root_source":
            root_source,
        "timeframes": list(
            args.timeframes
        ),
        "safety": {
            "ohlcv_read_only": True,
            "ohlcv_write_allowed": False,
            "ohlcv_delete_allowed": False,
            "recovery_allowed": False,
            "api_download_allowed": False,
            "future_labels_allowed": False,
            "backtest_allowed": False,
            "prediction_allowed": False,
            "trading_allowed": False,
            "git_mutation_allowed": False,
            "source_sha256_unchanged":
                all_sources_unchanged,
        },
        "patterns": {
            "count":
                len(PATTERN_DEFINITIONS),
            "final_formulas_defined":
                False,
            "future_information_allowed":
                False,
            "registry": [
                asdict(item)
                for item in PATTERN_DEFINITIONS
            ],
        },
        "common_feature_contract": {
            "feature_count":
                len(COMMON_FEATURE_COLUMNS),
            "features":
                list(
                    COMMON_FEATURE_COLUMNS
                ),
            "future_candles_used":
                False,
        },
        "statistics": {
            **asdict(stats),
            "detail_count":
                len(details),
            "research_ready_count":
                ready_count,
            "warning_event_count":
                warning_count,
            "error_event_count":
                error_count,
            "status_counts":
                dict(status_counts),
        },
        "changed_source_files":
            list(changed_sources),
    }

    return sanitize_json_value(
        payload
    )


# ======================================================================
# RUN
# ======================================================================

def run_quantification() -> int:

    args = parse_args()

    print_header(
        f"{PROJECT_NAME}\n"
        f"{STAGE_NAME}\n"
        f"{PROGRAM_VERSION}"
    )

    print(
        "[SAFETY] Production OHLCV: READ ONLY"
    )
    print(
        "[SAFETY] OHLCV write       : FORBIDDEN"
    )
    print(
        "[SAFETY] OHLCV delete      : FORBIDDEN"
    )
    print(
        "[SAFETY] Recovery           : DISABLED"
    )
    print(
        "[SAFETY] API download       : DISABLED"
    )
    print(
        "[SAFETY] Future labels      : DISABLED"
    )
    print(
        "[SAFETY] Backtest           : DISABLED"
    )
    print(
        "[SAFETY] Prediction         : DISABLED"
    )
    print(
        "[SAFETY] Trading            : DISABLED"
    )
    print(
        "[SAFETY] Git mutation       : DISABLED"
    )

    # ------------------------------------------------------------------
    # 1. Safety contract
    # ------------------------------------------------------------------

    print_header(
        "1. VALIDATE SAFETY CONTRACT"
    )

    validate_safety_contract()
    validate_pattern_registry()
    validate_feature_contract_no_future_leakage()

    print(
        "[PASS] Safety contract"
    )
    print(
        "[PASS] Pattern registry"
    )
    print(
        "[PASS] No-future-information feature contract"
    )

    # ------------------------------------------------------------------
    # 2. Project root
    # ------------------------------------------------------------------

    print_header(
        "2. DISCOVER PRODUCTION PROJECT ROOT"
    )

    project_root, root_source = (
        discover_project_root(
            args.project_root
        )
    )

    print(
        f"[PATH] Production project root: "
        f"{project_root}"
    )

    print(
        f"[PATH] Root source: "
        f"{root_source}"
    )

    # ------------------------------------------------------------------
    # 3. Output paths
    # ------------------------------------------------------------------

    paths = get_output_paths(
        project_root
    )

    paths["report_dir"].mkdir(
        parents=True,
        exist_ok=True,
    )

    paths["validation_dir"].mkdir(
        parents=True,
        exist_ok=True,
    )

    # ------------------------------------------------------------------
    # 4. Pattern registry
    # ------------------------------------------------------------------

    print_header(
        "3. REGISTER NINE RESEARCH PATTERNS"
    )

    for index, pattern in enumerate(
        PATTERN_DEFINITIONS,
        start=1,
    ):
        print(
            f"[{index:02d}] "
            f"{pattern.korean_name} "
            f"-> "
            f"{pattern.formula_status}"
        )

    write_pattern_registry(
        paths["registry"]
    )

    write_feature_contract(
        paths["feature_contract"]
    )

    write_report_readme(
        paths["readme"]
    )

    print()
    print(
        "[PASS] Pattern registry written"
    )
    print(
        "[PASS] Feature contract written"
    )

    # ------------------------------------------------------------------
    # 5. Discover OHLCV
    # ------------------------------------------------------------------

    print_header(
        "4. DISCOVER PRODUCTION OHLCV"
    )

    discovered = discover_ohlcv_files(
        project_root,
        args.timeframes,
    )

    print(
        f"[INFO] OHLCV files discovered: "
        f"{len(discovered)}"
    )

    timeframe_counts = Counter(
        timeframe
        for timeframe, _path
        in discovered
    )

    for timeframe in args.timeframes:
        print(
            f"[INFO] {timeframe}: "
            f"{timeframe_counts.get(timeframe, 0)}"
        )

    # ------------------------------------------------------------------
    # 6. Source baseline
    # ------------------------------------------------------------------

    print_header(
        "5. RECORD SOURCE SHA256 BASELINE"
    )

    baseline_hashes, manifest_rows = (
        build_source_baseline(
            project_root,
            discovered,
        )
    )

    write_source_manifest(
        paths["manifest"],
        manifest_rows,
    )

    print(
        f"[PASS] SHA256 baseline recorded: "
        f"{len(baseline_hashes)} files"
    )

    # ------------------------------------------------------------------
    # 7. Checkpoint
    # ------------------------------------------------------------------

    print_header(
        "6. LOAD RESUME CHECKPOINT"
    )

    checkpoint = load_checkpoint(
        paths["checkpoint"]
    )

    completed = checkpoint[
        "completed"
    ]

    print(
        f"[INFO] Existing checkpoint entries: "
        f"{len(completed)}"
    )

    if args.force:
        print(
            "[INFO] --force enabled. "
            "Valid checkpoint entries will not be skipped."
        )

    # ------------------------------------------------------------------
    # 8. Process files
    # ------------------------------------------------------------------

    print_header(
        "7. BUILD COMMON QUANTITATIVE RESEARCH CONTRACT"
    )

    stats = RunStatistics(
        files_discovered=len(discovered)
    )

    details: List[
        FileAuditResult
    ] = []

    total_files = len(discovered)

    for index, (
        timeframe,
        path,
    ) in enumerate(
        discovered,
        start=1,
    ):

        market = normalize_market_name(
            path
        )

        relative_path = (
            relative_path_string(
                path,
                project_root,
            )
        )

        source_sha256 = (
            baseline_hashes[
                relative_path
            ]
        )

        key = checkpoint_key(
            timeframe,
            market,
        )

        entry = completed.get(
            key
        )

        if (
            not args.force
            and checkpoint_entry_valid(
                entry,
                source_sha256,
                relative_path,
            )
        ):
            restored = result_from_checkpoint(
                entry
            )

            if restored is not None:
                current_sha = sha256_file(
                    path
                )

                if (
                    current_sha
                    == source_sha256
                    == restored.sha256_after
                ):
                    details.append(
                        restored
                    )

                    stats.files_skipped_checkpoint += 1

                    print(
                        f"[{index:04d}/{total_files:04d}] "
                        f"[SKIP] "
                        f"{timeframe} "
                        f"{market} "
                        f"checkpoint verified"
                    )

                    continue

        print(
            f"[{index:04d}/{total_files:04d}] "
            f"[RUN ] "
            f"{timeframe} "
            f"{market}"
        )

        try:
            result = process_file(
                project_root=project_root,
                timeframe=timeframe,
                path=path,
                source_sha256=source_sha256,
            )

        except Exception as exc:
            current_sha = sha256_file(
                path
            )

            result = FileAuditResult(
                timeframe=timeframe,
                market=market,
                relative_path=relative_path,
                sha256_before=source_sha256,
                sha256_after=current_sha,
                source_unchanged=(
                    current_sha
                    == source_sha256
                ),
                rows=0,
                valid_rows=0,
                first_timestamp=None,
                last_timestamp=None,
                duplicate_timestamp_count=0,
                invalid_ohlc_count=0,
                non_positive_price_count=0,
                timestamp_parse_failure_count=0,
                non_monotonic_count=0,
                gap_count=0,
                largest_gap_seconds=0.0,
                expected_seconds=int(
                    TIMEFRAME_SECONDS[
                        timeframe
                    ]
                ),
                minimum_research_rows=int(
                    minimum_research_rows(
                        timeframe
                    )
                ),
                research_ready=False,
                feature_rows=0,
                feature_ready_rows=0,
                status="ERROR",
                warning_count=0,
                error_count=1,
            )

            print(
                f"        [ERROR] "
                f"{type(exc).__name__}: "
                f"{exc}"
            )

        details.append(
            result
        )

        stats.files_processed += 1

        completed[key] = {
            "timeframe":
                timeframe,
            "market":
                market,
            "relative_path":
                relative_path,
            "sha256":
                source_sha256,
            "status":
                result.status,
            "completed_at_utc":
                utc_now_iso(),
            "result":
                sanitize_json_value(
                    asdict(result)
                ),
        }

        save_checkpoint(
            paths["checkpoint"],
            checkpoint,
        )

        print(
            f"        status={result.status} "
            f"rows={result.rows} "
            f"valid={result.valid_rows} "
            f"feature_ready={result.feature_ready_rows} "
            f"gaps={result.gap_count}"
        )

    # ------------------------------------------------------------------
    # 9. Sort deterministic output
    # ------------------------------------------------------------------

    timeframe_order = {
        timeframe: index
        for index, timeframe
        in enumerate(
            SUPPORTED_TIMEFRAMES
        )
    }

    details.sort(
        key=lambda item: (
            timeframe_order.get(
                item.timeframe,
                999,
            ),
            item.market,
            item.relative_path,
        )
    )

    # ------------------------------------------------------------------
    # 10. Statistics
    # ------------------------------------------------------------------

    for item in details:
        stats.total_rows += (
            item.rows
        )

        stats.total_valid_rows += (
            item.valid_rows
        )

        stats.total_gaps += (
            item.gap_count
        )

        if item.status == "READY":
            stats.files_ready += 1

        elif item.status == "WARN":
            stats.files_warning += 1

        elif item.status == "ERROR":
            stats.files_error += 1

        if not item.source_unchanged:
            stats.source_mutation_detected += 1

    # ------------------------------------------------------------------
    # 11. Write detail
    # ------------------------------------------------------------------

    print_header(
        "8. WRITE QUANTIFICATION REPORTS"
    )

    write_detail_csv(
        paths["detail"],
        details,
    )

    summary_rows = build_summary_rows(
        details
    )

    write_summary_csv(
        paths["summary"],
        summary_rows,
    )

    print(
        f"[PASS] Detail report : "
        f"{paths['detail']}"
    )

    print(
        f"[PASS] Summary report: "
        f"{paths['summary']}"
    )

    # ------------------------------------------------------------------
    # 12. Final source immutability
    # ------------------------------------------------------------------

    print_header(
        "9. VERIFY PRODUCTION OHLCV UNCHANGED"
    )

    changed_sources = (
        verify_all_source_hashes(
            project_root,
            discovered,
            baseline_hashes,
        )
    )

    if changed_sources:
        print(
            "[FAIL] Production OHLCV mutation detected."
        )

        for changed in changed_sources:
            print(
                f"       {changed}"
            )
    else:
        print(
            "[PASS] All production OHLCV SHA256 values unchanged."
        )

    # ------------------------------------------------------------------
    # 13. Final result
    # ------------------------------------------------------------------

    print_header(
        "10. FINAL PATTERN QUANTIFICATION FOUNDATION RESULT"
    )

    result_payload = build_result_payload(
        project_root=project_root,
        root_source=root_source,
        args=args,
        details=details,
        stats=stats,
        changed_sources=changed_sources,
    )

    atomic_write_json(
        paths["result"],
        result_payload,
    )

    checkpoint["final_status"] = (
        result_payload["status"]
    )

    checkpoint["last_result_path"] = (
        str(paths["result"])
    )

    checkpoint["source_file_count"] = (
        len(discovered)
    )

    checkpoint["pattern_count"] = (
        len(PATTERN_DEFINITIONS)
    )

    checkpoint[
        "future_information_used"
    ] = False

    save_checkpoint(
        paths["checkpoint"],
        checkpoint,
    )

    print(
        f"Status                 : "
        f"{result_payload['status']}"
    )

    print(
        f"OHLCV files            : "
        f"{len(details)}"
    )

    print(
        f"READY                   : "
        f"{stats.files_ready}"
    )

    print(
        f"WARN                    : "
        f"{stats.files_warning}"
    )

    print(
        f"ERROR                   : "
        f"{stats.files_error}"
    )

    print(
        f"Total rows              : "
        f"{stats.total_rows}"
    )

    print(
        f"Valid rows              : "
        f"{stats.total_valid_rows}"
    )

    print(
        f"Historical gaps         : "
        f"{stats.total_gaps}"
    )

    print(
        f"Patterns registered     : "
        f"{len(PATTERN_DEFINITIONS)}"
    )

    print(
        f"Final formulas defined  : NO"
    )

    print(
        f"Future information used : NO"
    )

    print(
        f"OHLCV modified          : "
        f"{'YES' if changed_sources else 'NO'}"
    )

    print()
    print(
        f"Result JSON:"
    )
    print(
        f"  {paths['result']}"
    )

    print()
    print(
        f"Checkpoint:"
    )
    print(
        f"  {paths['checkpoint']}"
    )

    print()
    print_rule("=")

    if result_payload["status"] != "PASS":
        print(
            "[BLOCK] Pattern quantification foundation failed."
        )
        print(
            "[BLOCK] Do not proceed to pattern formula research."
        )
        print_rule("=")
        return 1

    if (
        args.fail_on_warning
        and stats.files_warning > 0
    ):
        print(
            "[BLOCK] Warnings detected and "
            "--fail-on-warning is enabled."
        )
        print_rule("=")
        return 2

    print(
        "[PASS] PATTERN QUANTIFICATION FOUNDATION READY"
    )
    print()
    print(
        "[NEXT] Quantify each pattern independently."
    )
    print(
        "[NEXT] Do not use future candles for signal generation."
    )
    print(
        "[NEXT] Future +30% outcome validation belongs to a later stage."
    )
    print_rule("=")

    return 0


# ======================================================================
# MAIN
# ======================================================================

def main() -> int:

    try:
        return run_quantification()

    except KeyboardInterrupt:
        print()
        print_header(
            "PATTERN QUANTIFICATION INTERRUPTED"
        )

        print(
            "[INFO] Execution interrupted by user."
        )

        print(
            "[INFO] Existing checkpoint may be used for resume."
        )

        print(
            "[SAFETY] No OHLCV write operation is implemented."
        )

        return 130

    except Exception as exc:
        print()
        print_header(
            "PATTERN QUANTIFICATION FATAL ERROR"
        )

        print(
            f"{type(exc).__name__}: {exc}"
        )

        print()
        traceback.print_exc()

        print()
        print(
            "[SAFETY] No OHLCV deletion was executed."
        )
        print(
            "[SAFETY] No OHLCV write was executed."
        )
        print(
            "[SAFETY] No historical gap repair was executed."
        )
        print(
            "[SAFETY] No API candle download was executed."
        )
        print(
            "[SAFETY] No future labels were generated."
        )
        print(
            "[SAFETY] No backtest was executed."
        )
        print(
            "[SAFETY] No prediction/trading was executed."
        )
        print(
            "[SAFETY] No Git mutation was executed."
        )

        return 1


if __name__ == "__main__":
    sys.exit(main())
