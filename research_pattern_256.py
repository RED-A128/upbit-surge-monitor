# ============================================================
# Upbit Surge Monitor
# research_pattern_256.py
# Clean V001
# ============================================================
#
# PURPOSE
# ------------------------------------------------------------
# Independent quantitative research for the "256" pattern.
#
# This is NOT:
#
#   - a final 256 formula
#   - a trading signal
#   - a prediction model
#   - a backtest
#   - a future-label generator
#   - a historical-gap repair program
#
# This program DOES:
#
#   1. Read production OHLCV in READ ONLY mode
#   2. Research h1 / h4 / d1 independently
#   3. Calculate past/current-candle-only 256 candidate features
#   4. Build reproducible candidate observations
#   5. Summarize candidate distributions
#   6. Preserve source SHA256 integrity
#   7. Maintain checkpoint / resume state
#   8. Produce research evidence for later validation
#
# IMPORTANT RESEARCH PRINCIPLE
# ------------------------------------------------------------
# Clean V001 intentionally DOES NOT define a final 256 formula.
#
# Candidate rules in this file are RESEARCH WINDOWS only.
# They are used to collect measurable examples and distributions.
#
# No claim is made that these thresholds are the final or optimal
# definition of the 256 pattern.
#
# FUTURE INFORMATION POLICY
# ------------------------------------------------------------
# Signal/candidate generation may use:
#
#   - current candle
#   - past candles
#
# It may NOT use:
#
#   - future candles
#   - negative shift
#   - centered rolling windows
#   - future returns
#   - future max/min
#   - future 30% labels
#
# Future outcome validation belongs to a later backtest stage.
#
# SOURCE SAFETY
# ------------------------------------------------------------
# Production OHLCV is READ ONLY.
#
# This program does NOT:
#
#   - write OHLCV
#   - delete OHLCV
#   - overwrite candles
#   - download candles
#   - repair gaps
#   - rebuild production OHLCV
#   - trade
#   - git reset
#   - git clean
#   - git commit
#   - git push
#
# ============================================================


from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import sys
import traceback

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd


# ============================================================
# 1. VERSION / PROGRAM
# ============================================================

PROGRAM_NAME = "research_pattern_256.py"
VERSION = "Clean V001"
STAGE = "PATTERN 256 INDEPENDENT RESEARCH"

PROJECT_NAME = "Upbit Surge Monitor"

PATTERN_ID = "256"
PATTERN_KOREAN_NAME = "256"

CHECKPOINT_VERSION = 1


# ============================================================
# 2. CONSOLE UTF-8 SAFETY
# ============================================================

def configure_console_encoding() -> None:
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(
                encoding="utf-8",
                errors="replace",
            )
    except Exception:
        pass

    try:
        if hasattr(sys.stderr, "reconfigure"):
            sys.stderr.reconfigure(
                encoding="utf-8",
                errors="replace",
            )
    except Exception:
        pass


configure_console_encoding()


def safe_print(*args: Any, **kwargs: Any) -> None:
    try:
        print(*args, **kwargs)
    except UnicodeEncodeError:
        text = " ".join(str(value) for value in args)
        try:
            sys.stdout.write(
                text.encode(
                    "ascii",
                    errors="replace",
                ).decode("ascii")
                + "\n"
            )
            sys.stdout.flush()
        except Exception:
            pass


# ============================================================
# 3. CONSTANTS
# ============================================================

DEFAULT_TIMEFRAMES: Tuple[str, ...] = (
    "h1",
    "h4",
    "d1",
)

TIMEFRAME_SECONDS: Dict[str, int] = {
    "h1": 3600,
    "h4": 14400,
    "d1": 86400,
}

TIME_COLUMN_CANDIDATES: Tuple[str, ...] = (
    "timestamp",
    "datetime",
    "date",
    "time",
    "candle_date_time_utc",
    "candle_date_time_kst",
)

OPEN_COLUMN_CANDIDATES: Tuple[str, ...] = (
    "open",
    "opening_price",
    "trade_price_open",
)

HIGH_COLUMN_CANDIDATES: Tuple[str, ...] = (
    "high",
    "high_price",
)

LOW_COLUMN_CANDIDATES: Tuple[str, ...] = (
    "low",
    "low_price",
)

CLOSE_COLUMN_CANDIDATES: Tuple[str, ...] = (
    "close",
    "trade_price",
    "closing_price",
)

VOLUME_COLUMN_CANDIDATES: Tuple[str, ...] = (
    "volume",
    "candle_acc_trade_volume",
    "acc_trade_volume",
)

VALUE_COLUMN_CANDIDATES: Tuple[str, ...] = (
    "value",
    "candle_acc_trade_price",
    "acc_trade_price",
)

MIN_REQUIRED_ROWS = 30

EPSILON = 1e-12


# ============================================================
# 4. 256 RESEARCH CONTRACT
# ============================================================
#
# IMPORTANT:
#
# These are NOT final formulas.
#
# The 256 concept is decomposed into measurable dimensions:
#
#   A. Trend compression / moving-average convergence
#   B. Price position around medium/long moving averages
#   C. Volatility contraction
#   D. Volume contraction / stabilization
#   E. Price-base stability
#   F. Current-candle breakout pressure
#
# Clean V001 creates a research score from these dimensions so
# that later stages can test which combinations actually precede
# strong moves.
#
# No future candle is used here.
# ============================================================


@dataclass(frozen=True)
class ResearchThresholds:
    # MA convergence
    ma_spread_strict_pct: float = 3.0
    ma_spread_relaxed_pct: float = 6.0

    # Distance from long MA
    long_ma_distance_strict_pct: float = 5.0
    long_ma_distance_relaxed_pct: float = 10.0

    # Base range
    base_range_strict_pct: float = 12.0
    base_range_relaxed_pct: float = 20.0

    # Volatility contraction ratio
    volatility_strict_ratio: float = 0.75
    volatility_relaxed_ratio: float = 1.00

    # Volume ratio
    volume_strict_ratio: float = 0.80
    volume_relaxed_ratio: float = 1.10

    # Breakout pressure
    breakout_near_high_ratio: float = 0.97

    # Research score gates
    candidate_score_min: float = 60.0
    strong_candidate_score_min: float = 75.0


THRESHOLDS = ResearchThresholds()


# ============================================================
# 5. FEATURE WINDOWS
# ============================================================
#
# These are research windows only.
#
# We intentionally include several MA horizons rather than
# asserting one final "256" interpretation.
#
# ============================================================

MA_WINDOWS: Tuple[int, ...] = (
    5,
    10,
    20,
    60,
    120,
    256,
)

BASE_WINDOWS: Tuple[int, ...] = (
    10,
    20,
    60,
)

VOLATILITY_WINDOWS: Tuple[int, ...] = (
    5,
    20,
    60,
)

VOLUME_WINDOWS: Tuple[int, ...] = (
    5,
    20,
    60,
)


# ============================================================
# 6. GENERIC HELPERS
# ============================================================

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def separator(char: str = "=", width: int = 72) -> str:
    return char * width


def print_section(title: str) -> None:
    safe_print("")
    safe_print(separator())
    safe_print(title)
    safe_print(separator())
    safe_print("")


def normalize_path(path: Path) -> str:
    try:
        return str(path.resolve())
    except Exception:
        return str(path)


def relative_path(path: Path, root: Path) -> str:
    try:
        return str(path.resolve().relative_to(root.resolve()))
    except Exception:
        return str(path)


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )


def ensure_directory(path: Path) -> None:
    path.mkdir(
        parents=True,
        exist_ok=True,
    )


def json_safe(value: Any) -> Any:
    if value is None:
        return None

    if isinstance(value, Path):
        return str(value)

    if isinstance(value, (np.integer,)):
        return int(value)

    if isinstance(value, (np.floating,)):
        if np.isnan(value) or np.isinf(value):
            return None
        return float(value)

    if isinstance(value, np.bool_):
        return bool(value)

    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None
        return value.isoformat()

    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None

    return value


def clean_record(record: Dict[str, Any]) -> Dict[str, Any]:
    return {
        key: json_safe(value)
        for key, value in record.items()
    }


def write_json(
    path: Path,
    data: Dict[str, Any],
) -> None:
    ensure_parent(path)

    with path.open(
        "w",
        encoding="utf-8",
        newline="\n",
    ) as file:
        json.dump(
            data,
            file,
            ensure_ascii=False,
            indent=2,
            default=json_safe,
        )
        file.write("\n")


def read_json(path: Path) -> Dict[str, Any]:
    with path.open(
        "r",
        encoding="utf-8-sig",
    ) as file:
        data = json.load(file)

    if not isinstance(data, dict):
        raise ValueError(
            f"JSON root must be an object: {path}"
        )

    return data


def write_csv(
    path: Path,
    rows: Sequence[Dict[str, Any]],
    fieldnames: Optional[Sequence[str]] = None,
) -> None:
    ensure_parent(path)

    if fieldnames is None:
        discovered: List[str] = []
        seen = set()

        for row in rows:
            for key in row.keys():
                if key not in seen:
                    seen.add(key)
                    discovered.append(key)

        fieldnames = discovered

    fieldnames = list(fieldnames)

    with path.open(
        "w",
        encoding="utf-8-sig",
        newline="",
    ) as file:
        writer = csv.DictWriter(
            file,
            fieldnames=fieldnames,
            extrasaction="ignore",
        )

        writer.writeheader()

        for row in rows:
            writer.writerow(
                clean_record(dict(row))
            )


def sha256_file(
    path: Path,
    chunk_size: int = 1024 * 1024,
) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as file:
        while True:
            chunk = file.read(chunk_size)

            if not chunk:
                break

            digest.update(chunk)

    return digest.hexdigest()


def file_size(path: Path) -> int:
    return int(path.stat().st_size)


def finite_or_none(value: Any) -> Optional[float]:
    try:
        number = float(value)
    except Exception:
        return None

    if not math.isfinite(number):
        return None

    return number


# ============================================================
# 7. PROJECT PATHS
# ============================================================

@dataclass(frozen=True)
class ProjectPaths:
    root: Path
    data: Path
    ohlcv: Path
    reports: Path
    validation: Path
    report_dir: Path
    checkpoint: Path
    result_json: Path
    summary_csv: Path
    detail_csv: Path
    candidate_csv: Path
    source_manifest_csv: Path
    research_contract_json: Path
    readme_txt: Path


def build_project_paths(
    project_root: Path,
) -> ProjectPaths:
    root = project_root.resolve()

    data = root / "data"
    ohlcv = data / "ohlcv"
    reports = data / "reports"
    validation = data / "validation"

    report_dir = (
        reports
        / "pattern_research_256"
    )

    return ProjectPaths(
        root=root,
        data=data,
        ohlcv=ohlcv,
        reports=reports,
        validation=validation,
        report_dir=report_dir,
        checkpoint=(
            validation
            / "research_pattern_256_checkpoint.json"
        ),
        result_json=(
            report_dir
            / "pattern_256_research_result.json"
        ),
        summary_csv=(
            report_dir
            / "pattern_256_research_summary.csv"
        ),
        detail_csv=(
            report_dir
            / "pattern_256_research_detail.csv"
        ),
        candidate_csv=(
            report_dir
            / "pattern_256_candidates.csv"
        ),
        source_manifest_csv=(
            report_dir
            / "pattern_256_source_manifest.csv"
        ),
        research_contract_json=(
            report_dir
            / "pattern_256_research_contract.json"
        ),
        readme_txt=(
            report_dir
            / "README.txt"
        ),
    )


# ============================================================
# 8. COMMAND LINE
# ============================================================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Upbit Surge Monitor - "
            "256 independent pattern research"
        )
    )

    parser.add_argument(
        "--project-root",
        type=str,
        default=str(
            Path(__file__).resolve().parent
        ),
        help="Production project root.",
    )

    parser.add_argument(
        "--timeframes",
        nargs="+",
        default=list(DEFAULT_TIMEFRAMES),
        help="Timeframes to research.",
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help=(
            "Reprocess all source files even when "
            "checkpoint entries are already complete."
        ),
    )

    parser.add_argument(
        "--market",
        type=str,
        default="",
        help=(
            "Optional single market for diagnostic runs. "
            "Example: KRW-BTC"
        ),
    )

    parser.add_argument(
        "--max-files",
        type=int,
        default=0,
        help=(
            "Optional maximum source files for diagnostics. "
            "0 means no limit."
        ),
    )

    return parser.parse_args()


# ============================================================
# 9. SOURCE DISCOVERY
# ============================================================

@dataclass(frozen=True)
class SourceFile:
    timeframe: str
    market: str
    path: Path


def validate_timeframes(
    values: Iterable[str],
) -> List[str]:
    result: List[str] = []

    for raw in values:
        value = str(raw).strip().lower()

        if value not in TIMEFRAME_SECONDS:
            raise ValueError(
                f"Unsupported timeframe: {raw}"
            )

        if value not in result:
            result.append(value)

    if not result:
        raise ValueError(
            "At least one timeframe is required."
        )

    return result


def discover_source_files(
    paths: ProjectPaths,
    timeframes: Sequence[str],
    market_filter: str = "",
    max_files: int = 0,
) -> List[SourceFile]:
    sources: List[SourceFile] = []

    normalized_market_filter = (
        market_filter.strip().upper()
    )

    for timeframe in timeframes:
        timeframe_dir = (
            paths.ohlcv
            / timeframe
        )

        if not timeframe_dir.exists():
            raise FileNotFoundError(
                f"OHLCV timeframe directory missing: "
                f"{timeframe_dir}"
            )

        files = sorted(
            timeframe_dir.glob("*.csv"),
            key=lambda item: item.name.upper(),
        )

        for path in files:
            market = path.stem.upper()

            if normalized_market_filter:
                if market != normalized_market_filter:
                    continue

            sources.append(
                SourceFile(
                    timeframe=timeframe,
                    market=market,
                    path=path,
                )
            )

    if max_files > 0:
        sources = sources[:max_files]

    if not sources:
        raise FileNotFoundError(
            "No OHLCV CSV files discovered for requested scope."
        )

    return sources


# ============================================================
# 10. COLUMN DETECTION
# ============================================================

def normalize_column_name(value: Any) -> str:
    return str(value).strip().lower()


def build_column_lookup(
    columns: Iterable[Any],
) -> Dict[str, str]:
    result: Dict[str, str] = {}

    for column in columns:
        normalized = normalize_column_name(column)

        if normalized not in result:
            result[normalized] = str(column)

    return result


def find_column(
    frame: pd.DataFrame,
    candidates: Sequence[str],
    required: bool = True,
) -> Optional[str]:
    lookup = build_column_lookup(
        frame.columns
    )

    for candidate in candidates:
        normalized = normalize_column_name(
            candidate
        )

        if normalized in lookup:
            return lookup[normalized]

    if required:
        raise ValueError(
            "Required column not found. "
            f"Candidates={list(candidates)} "
            f"Columns={list(frame.columns)}"
        )

    return None


# ============================================================
# 11. TIMESTAMP PARSING
# ============================================================

def parse_numeric_timestamp_series(
    series: pd.Series,
) -> pd.Series:
    numeric = pd.to_numeric(
        series,
        errors="coerce",
    )

    finite = numeric[
        numeric.notna()
    ]

    if finite.empty:
        return pd.Series(
            pd.NaT,
            index=series.index,
            dtype="datetime64[ns, UTC]",
        )

    median_abs = float(
        finite.abs().median()
    )

    if median_abs >= 1e17:
        unit = "ns"
    elif median_abs >= 1e14:
        unit = "us"
    elif median_abs >= 1e11:
        unit = "ms"
    else:
        unit = "s"

    return pd.to_datetime(
        numeric,
        unit=unit,
        errors="coerce",
        utc=True,
    )


def parse_timestamp_series(
    series: pd.Series,
) -> pd.Series:
    numeric = pd.to_numeric(
        series,
        errors="coerce",
    )

    numeric_ratio = (
        float(numeric.notna().mean())
        if len(series) > 0
        else 0.0
    )

    if numeric_ratio >= 0.90:
        parsed = parse_numeric_timestamp_series(
            series
        )
    else:
        parsed = pd.to_datetime(
            series,
            errors="coerce",
            utc=True,
        )

    return parsed


# ============================================================
# 12. OHLCV LOAD / NORMALIZATION
# ============================================================

@dataclass
class LoadedOHLCV:
    frame: pd.DataFrame
    timestamp_column: str
    open_column: str
    high_column: str
    low_column: str
    close_column: str
    volume_column: str
    value_column: Optional[str]
    original_rows: int
    valid_rows: int
    timestamp_parse_failures: int
    duplicate_timestamps_removed: int


def load_ohlcv(
    source: SourceFile,
) -> LoadedOHLCV:
    frame = pd.read_csv(
        source.path,
        low_memory=False,
    )

    original_rows = len(frame)

    if original_rows == 0:
        raise ValueError(
            f"Empty OHLCV file: {source.path}"
        )

    if frame.columns.duplicated().any():
        duplicated = [
            str(column)
            for column in frame.columns[
                frame.columns.duplicated()
            ]
        ]

        raise ValueError(
            f"Duplicate OHLCV columns: "
            f"{source.path} {duplicated}"
        )

    timestamp_column = find_column(
        frame,
        TIME_COLUMN_CANDIDATES,
        required=True,
    )

    open_column = find_column(
        frame,
        OPEN_COLUMN_CANDIDATES,
        required=True,
    )

    high_column = find_column(
        frame,
        HIGH_COLUMN_CANDIDATES,
        required=True,
    )

    low_column = find_column(
        frame,
        LOW_COLUMN_CANDIDATES,
        required=True,
    )

    close_column = find_column(
        frame,
        CLOSE_COLUMN_CANDIDATES,
        required=True,
    )

    volume_column = find_column(
        frame,
        VOLUME_COLUMN_CANDIDATES,
        required=True,
    )

    value_column = find_column(
        frame,
        VALUE_COLUMN_CANDIDATES,
        required=False,
    )

    working = pd.DataFrame(
        index=frame.index
    )

    working["timestamp"] = parse_timestamp_series(
        frame[timestamp_column]
    )

    timestamp_parse_failures = int(
        working["timestamp"].isna().sum()
    )

    working["open"] = pd.to_numeric(
        frame[open_column],
        errors="coerce",
    )

    working["high"] = pd.to_numeric(
        frame[high_column],
        errors="coerce",
    )

    working["low"] = pd.to_numeric(
        frame[low_column],
        errors="coerce",
    )

    working["close"] = pd.to_numeric(
        frame[close_column],
        errors="coerce",
    )

    working["volume"] = pd.to_numeric(
        frame[volume_column],
        errors="coerce",
    )

    if value_column is not None:
        working["value"] = pd.to_numeric(
            frame[value_column],
            errors="coerce",
        )
    else:
        working["value"] = (
            working["close"]
            * working["volume"]
        )

    working = working.dropna(
        subset=[
            "timestamp",
            "open",
            "high",
            "low",
            "close",
            "volume",
        ]
    ).copy()

    working = working[
        (working["open"] > 0)
        & (working["high"] > 0)
        & (working["low"] > 0)
        & (working["close"] > 0)
        & (working["volume"] >= 0)
    ].copy()

    working = working[
        working["high"]
        >= working[["open", "close", "low"]].max(axis=1)
    ].copy()

    working = working[
        working["low"]
        <= working[["open", "close", "high"]].min(axis=1)
    ].copy()

    working = working.sort_values(
        "timestamp",
        kind="stable",
    )

    before_dedup = len(working)

    working = working.drop_duplicates(
        subset=["timestamp"],
        keep="last",
    )

    duplicate_timestamps_removed = (
        before_dedup
        - len(working)
    )

    working = working.reset_index(
        drop=True
    )

    valid_rows = len(working)

    if valid_rows < MIN_REQUIRED_ROWS:
        safe_print(
            "[SHORT_HISTORY] "
            f"{source.timeframe} {source.market} "
            f"valid_rows={valid_rows} "
            f"recommended_minimum={MIN_REQUIRED_ROWS}"
        )

    return LoadedOHLCV(
        frame=working,
        timestamp_column=timestamp_column,
        open_column=open_column,
        high_column=high_column,
        low_column=low_column,
        close_column=close_column,
        volume_column=volume_column,
        value_column=value_column,
        original_rows=original_rows,
        valid_rows=valid_rows,
        timestamp_parse_failures=(
            timestamp_parse_failures
        ),
        duplicate_timestamps_removed=(
            duplicate_timestamps_removed
        ),
    )


# ============================================================
# 13. SAFE FEATURE ENGINEERING
# ============================================================
#
# All rolling calculations below use:
#
#   center=False
#
# and never use:
#
#   shift(-N)
#
# ============================================================

def safe_divide(
    numerator: pd.Series,
    denominator: pd.Series,
) -> pd.Series:
    denominator_safe = denominator.replace(
        0,
        np.nan,
    )

    result = (
        numerator
        / denominator_safe
    )

    result = result.replace(
        [np.inf, -np.inf],
        np.nan,
    )

    return result


def rolling_mean(
    series: pd.Series,
    window: int,
) -> pd.Series:
    return series.rolling(
        window=window,
        min_periods=window,
        center=False,
    ).mean()


def rolling_std(
    series: pd.Series,
    window: int,
) -> pd.Series:
    return series.rolling(
        window=window,
        min_periods=window,
        center=False,
    ).std(ddof=0)


def rolling_max(
    series: pd.Series,
    window: int,
) -> pd.Series:
    return series.rolling(
        window=window,
        min_periods=window,
        center=False,
    ).max()


def rolling_min(
    series: pd.Series,
    window: int,
) -> pd.Series:
    return series.rolling(
        window=window,
        min_periods=window,
        center=False,
    ).min()


def add_common_features(
    frame: pd.DataFrame,
) -> pd.DataFrame:
    data = frame.copy()

    close = data["close"]
    high = data["high"]
    low = data["low"]
    volume = data["volume"]

    # --------------------------------------------------------
    # Candle geometry
    # --------------------------------------------------------

    data["candle_range_pct"] = (
        safe_divide(
            high - low,
            close,
        )
        * 100.0
    )

    data["body_pct"] = (
        safe_divide(
            (data["close"] - data["open"]).abs(),
            close,
        )
        * 100.0
    )

    data["upper_wick_pct"] = (
        safe_divide(
            high
            - data[["open", "close"]].max(axis=1),
            close,
        )
        * 100.0
    )

    data["lower_wick_pct"] = (
        safe_divide(
            data[["open", "close"]].min(axis=1)
            - low,
            close,
        )
        * 100.0
    )

    data["close_position_in_candle"] = (
        safe_divide(
            close - low,
            high - low,
        )
    )

    # --------------------------------------------------------
    # Historical return features
    # Positive shift only.
    # --------------------------------------------------------

    for period in (
        1,
        3,
        5,
        10,
        20,
        60,
    ):
        previous = close.shift(period)

        data[
            f"return_{period}_pct"
        ] = (
            safe_divide(
                close - previous,
                previous,
            )
            * 100.0
        )

    # --------------------------------------------------------
    # Moving averages
    # --------------------------------------------------------

    for window in MA_WINDOWS:
        ma = rolling_mean(
            close,
            window,
        )

        data[f"ma_{window}"] = ma

        data[
            f"close_to_ma_{window}_pct"
        ] = (
            safe_divide(
                close - ma,
                ma,
            )
            * 100.0
        )

        data[
            f"ma_{window}_slope_1_pct"
        ] = (
            safe_divide(
                ma - ma.shift(1),
                ma.shift(1),
            )
            * 100.0
        )

        data[
            f"ma_{window}_slope_5_pct"
        ] = (
            safe_divide(
                ma - ma.shift(5),
                ma.shift(5),
            )
            * 100.0
        )

    # --------------------------------------------------------
    # MA convergence
    # --------------------------------------------------------

    ma_columns = [
        "ma_20",
        "ma_60",
        "ma_120",
        "ma_256",
    ]

    ma_matrix = data[ma_columns]

    data["ma_cluster_max"] = (
        ma_matrix.max(axis=1)
    )

    data["ma_cluster_min"] = (
        ma_matrix.min(axis=1)
    )

    data["ma_cluster_mean"] = (
        ma_matrix.mean(axis=1)
    )

    data["ma_cluster_spread_pct"] = (
        safe_divide(
            data["ma_cluster_max"]
            - data["ma_cluster_min"],
            data["ma_cluster_mean"],
        )
        * 100.0
    )

    # --------------------------------------------------------
    # Base ranges
    # --------------------------------------------------------

    for window in BASE_WINDOWS:
        highest = rolling_max(
            high,
            window,
        )

        lowest = rolling_min(
            low,
            window,
        )

        data[
            f"range_high_{window}"
        ] = highest

        data[
            f"range_low_{window}"
        ] = lowest

        data[
            f"base_range_{window}_pct"
        ] = (
            safe_divide(
                highest - lowest,
                lowest,
            )
            * 100.0
        )

        data[
            f"close_position_{window}"
        ] = safe_divide(
            close - lowest,
            highest - lowest,
        )

    # --------------------------------------------------------
    # Volatility
    # --------------------------------------------------------

    returns = close.pct_change(
        periods=1,
        fill_method=None,
    )

    for window in VOLATILITY_WINDOWS:
        data[
            f"volatility_{window}"
        ] = rolling_std(
            returns,
            window,
        )

    data[
        "volatility_5_to_20_ratio"
    ] = safe_divide(
        data["volatility_5"],
        data["volatility_20"],
    )

    data[
        "volatility_20_to_60_ratio"
    ] = safe_divide(
        data["volatility_20"],
        data["volatility_60"],
    )

    # --------------------------------------------------------
    # Volume
    # --------------------------------------------------------

    for window in VOLUME_WINDOWS:
        data[
            f"volume_ma_{window}"
        ] = rolling_mean(
            volume,
            window,
        )

    data[
        "volume_5_to_20_ratio"
    ] = safe_divide(
        data["volume_ma_5"],
        data["volume_ma_20"],
    )

    data[
        "volume_20_to_60_ratio"
    ] = safe_divide(
        data["volume_ma_20"],
        data["volume_ma_60"],
    )

    data[
        "current_volume_to_20_ratio"
    ] = safe_divide(
        volume,
        data["volume_ma_20"],
    )

    # --------------------------------------------------------
    # Long-MA relationship
    # --------------------------------------------------------

    data[
        "close_to_ma256_abs_pct"
    ] = (
        data[
            "close_to_ma_256_pct"
        ].abs()
    )

    data[
        "ma120_to_ma256_pct"
    ] = (
        safe_divide(
            data["ma_120"]
            - data["ma_256"],
            data["ma_256"],
        )
        * 100.0
    )

    data[
        "ma60_to_ma120_pct"
    ] = (
        safe_divide(
            data["ma_60"]
            - data["ma_120"],
            data["ma_120"],
        )
        * 100.0
    )

    data[
        "ma20_to_ma60_pct"
    ] = (
        safe_divide(
            data["ma_20"]
            - data["ma_60"],
            data["ma_60"],
        )
        * 100.0
    )

    # --------------------------------------------------------
    # Current breakout pressure
    #
    # Uses current/past rolling high only.
    # This is NOT a future breakout label.
    # --------------------------------------------------------

    data[
        "prior_20_high"
    ] = rolling_max(
        high.shift(1),
        20,
    )

    data[
        "prior_60_high"
    ] = rolling_max(
        high.shift(1),
        60,
    )

    data[
        "close_to_prior20_high_ratio"
    ] = safe_divide(
        close,
        data["prior_20_high"],
    )

    data[
        "close_to_prior60_high_ratio"
    ] = safe_divide(
        close,
        data["prior_60_high"],
    )

    data[
        "break_above_prior20_high"
    ] = (
        close
        > data["prior_20_high"]
    )

    data[
        "break_above_prior60_high"
    ] = (
        close
        > data["prior_60_high"]
    )

    return data


# ============================================================
# 14. 256 RESEARCH DIMENSIONS
# ============================================================

def score_binary(
    strict_condition: pd.Series,
    relaxed_condition: pd.Series,
    strict_score: float,
    relaxed_score: float,
) -> pd.Series:
    result = pd.Series(
        0.0,
        index=strict_condition.index,
        dtype="float64",
    )

    relaxed_mask = (
        relaxed_condition.fillna(False)
    )

    strict_mask = (
        strict_condition.fillna(False)
    )

    result.loc[relaxed_mask] = (
        relaxed_score
    )

    result.loc[strict_mask] = (
        strict_score
    )

    return result


def add_256_research_dimensions(
    frame: pd.DataFrame,
) -> pd.DataFrame:
    data = frame.copy()

    t = THRESHOLDS

    # --------------------------------------------------------
    # Dimension A
    # MA convergence
    # Max 25
    # --------------------------------------------------------

    ma_spread = (
        data["ma_cluster_spread_pct"]
    )

    data[
        "cond_ma_convergence_strict"
    ] = (
        ma_spread
        <= t.ma_spread_strict_pct
    )

    data[
        "cond_ma_convergence_relaxed"
    ] = (
        ma_spread
        <= t.ma_spread_relaxed_pct
    )

    data[
        "score_ma_convergence"
    ] = score_binary(
        data[
            "cond_ma_convergence_strict"
        ],
        data[
            "cond_ma_convergence_relaxed"
        ],
        strict_score=25.0,
        relaxed_score=15.0,
    )

    # --------------------------------------------------------
    # Dimension B
    # Close near MA256
    # Max 20
    # --------------------------------------------------------

    long_distance = (
        data["close_to_ma256_abs_pct"]
    )

    data[
        "cond_long_ma_near_strict"
    ] = (
        long_distance
        <= t.long_ma_distance_strict_pct
    )

    data[
        "cond_long_ma_near_relaxed"
    ] = (
        long_distance
        <= t.long_ma_distance_relaxed_pct
    )

    data[
        "score_long_ma_position"
    ] = score_binary(
        data[
            "cond_long_ma_near_strict"
        ],
        data[
            "cond_long_ma_near_relaxed"
        ],
        strict_score=20.0,
        relaxed_score=12.0,
    )

    # --------------------------------------------------------
    # Dimension C
    # Base compression
    # Max 20
    # --------------------------------------------------------

    base_range = (
        data["base_range_20_pct"]
    )

    data[
        "cond_base_strict"
    ] = (
        base_range
        <= t.base_range_strict_pct
    )

    data[
        "cond_base_relaxed"
    ] = (
        base_range
        <= t.base_range_relaxed_pct
    )

    data[
        "score_base_compression"
    ] = score_binary(
        data[
            "cond_base_strict"
        ],
        data[
            "cond_base_relaxed"
        ],
        strict_score=20.0,
        relaxed_score=12.0,
    )

    # --------------------------------------------------------
    # Dimension D
    # Volatility contraction
    # Max 15
    # --------------------------------------------------------

    volatility_ratio = (
        data[
            "volatility_5_to_20_ratio"
        ]
    )

    data[
        "cond_volatility_strict"
    ] = (
        volatility_ratio
        <= t.volatility_strict_ratio
    )

    data[
        "cond_volatility_relaxed"
    ] = (
        volatility_ratio
        <= t.volatility_relaxed_ratio
    )

    data[
        "score_volatility"
    ] = score_binary(
        data[
            "cond_volatility_strict"
        ],
        data[
            "cond_volatility_relaxed"
        ],
        strict_score=15.0,
        relaxed_score=9.0,
    )

    # --------------------------------------------------------
    # Dimension E
    # Volume contraction / stabilization
    # Max 10
    # --------------------------------------------------------

    volume_ratio = (
        data["volume_5_to_20_ratio"]
    )

    data[
        "cond_volume_strict"
    ] = (
        volume_ratio
        <= t.volume_strict_ratio
    )

    data[
        "cond_volume_relaxed"
    ] = (
        volume_ratio
        <= t.volume_relaxed_ratio
    )

    data[
        "score_volume_structure"
    ] = score_binary(
        data[
            "cond_volume_strict"
        ],
        data[
            "cond_volume_relaxed"
        ],
        strict_score=10.0,
        relaxed_score=6.0,
    )

    # --------------------------------------------------------
    # Dimension F
    # Current breakout pressure
    # Max 10
    #
    # Current/past information only.
    # --------------------------------------------------------

    near_prior_high = (
        data[
            "close_to_prior20_high_ratio"
        ]
        >= t.breakout_near_high_ratio
    )

    actual_current_break = (
        data[
            "break_above_prior20_high"
        ]
    )

    data[
        "cond_breakout_pressure"
    ] = near_prior_high

    data[
        "cond_current_breakout"
    ] = actual_current_break

    data[
        "score_breakout_pressure"
    ] = score_binary(
        actual_current_break,
        near_prior_high,
        strict_score=10.0,
        relaxed_score=5.0,
    )

    # --------------------------------------------------------
    # Composite research score
    # --------------------------------------------------------

    score_columns = [
        "score_ma_convergence",
        "score_long_ma_position",
        "score_base_compression",
        "score_volatility",
        "score_volume_structure",
        "score_breakout_pressure",
    ]

    data[
        "pattern_256_research_score"
    ] = (
        data[score_columns]
        .fillna(0.0)
        .sum(axis=1)
    )

    # --------------------------------------------------------
    # Research candidate classification
    # --------------------------------------------------------

    data[
        "pattern_256_candidate"
    ] = (
        data[
            "pattern_256_research_score"
        ]
        >= t.candidate_score_min
    )

    data[
        "pattern_256_strong_candidate"
    ] = (
        data[
            "pattern_256_research_score"
        ]
        >= t.strong_candidate_score_min
    )

    # --------------------------------------------------------
    # Research stage
    #
    # This is descriptive only.
    # It is NOT an official production signal.
    # --------------------------------------------------------

    data[
        "pattern_256_research_stage"
    ] = "NONE"

    setup_mask = (
        data[
            "pattern_256_candidate"
        ]
    )

    strong_mask = (
        data[
            "pattern_256_strong_candidate"
        ]
    )

    trigger_mask = (
        strong_mask
        & data[
            "cond_current_breakout"
        ].fillna(False)
    )

    data.loc[
        setup_mask,
        "pattern_256_research_stage",
    ] = "SETUP"

    data.loc[
        strong_mask,
        "pattern_256_research_stage",
    ] = "READY"

    data.loc[
        trigger_mask,
        "pattern_256_research_stage",
    ] = "TRIGGER"

    return data


# ============================================================
# 15. FUTURE INFORMATION STATIC CONTRACT
# ============================================================

def build_research_contract() -> Dict[str, Any]:
    return {
        "program": PROGRAM_NAME,
        "version": VERSION,
        "stage": STAGE,
        "pattern_id": PATTERN_ID,
        "pattern_korean_name": (
            PATTERN_KOREAN_NAME
        ),
        "formula_status": (
            "RESEARCH_CANDIDATE_DEFINITION"
        ),
        "final_formula_defined": False,
        "contract": {
            "current_candle_allowed": True,
            "past_candles_allowed": True,
            "future_candles_allowed": False,
            "negative_shift_allowed": False,
            "centered_rolling_allowed": False,
            "future_labels_generated": False,
            "future_returns_generated": False,
            "future_max_generated": False,
            "future_min_generated": False,
            "backtest_generated": False,
            "prediction_generated": False,
            "trading_signal_generated": False,
            "production_signal_generated": False,
        },
        "research_dimensions": [
            {
                "id": "A",
                "name": "MA_CONVERGENCE",
                "max_score": 25,
            },
            {
                "id": "B",
                "name": "LONG_MA_POSITION",
                "max_score": 20,
            },
            {
                "id": "C",
                "name": "BASE_COMPRESSION",
                "max_score": 20,
            },
            {
                "id": "D",
                "name": "VOLATILITY_CONTRACTION",
                "max_score": 15,
            },
            {
                "id": "E",
                "name": "VOLUME_STRUCTURE",
                "max_score": 10,
            },
            {
                "id": "F",
                "name": "BREAKOUT_PRESSURE",
                "max_score": 10,
            },
        ],
        "thresholds": {
            key: value
            for key, value
            in THRESHOLDS.__dict__.items()
        },
        "moving_average_windows": list(
            MA_WINDOWS
        ),
        "base_windows": list(
            BASE_WINDOWS
        ),
        "volatility_windows": list(
            VOLATILITY_WINDOWS
        ),
        "volume_windows": list(
            VOLUME_WINDOWS
        ),
    }


# ============================================================
# 16. SOURCE MANIFEST
# ============================================================

SOURCE_MANIFEST_FIELDS: Tuple[str, ...] = (
    "timeframe",
    "market",
    "relative_path",
    "size_bytes",
    "sha256_before",
    "sha256_after",
    "sha256_unchanged",
    "original_rows",
    "valid_rows",
    "timestamp_parse_failures",
    "duplicate_timestamps_removed",
    "status",
    "error",
)


def build_source_manifest_base(
    source: SourceFile,
    paths: ProjectPaths,
) -> Dict[str, Any]:
    return {
        "timeframe": source.timeframe,
        "market": source.market,
        "relative_path": relative_path(
            source.path,
            paths.root,
        ),
        "size_bytes": file_size(
            source.path
        ),
        "sha256_before": sha256_file(
            source.path
        ),
        "sha256_after": "",
        "sha256_unchanged": False,
        "original_rows": 0,
        "valid_rows": 0,
        "timestamp_parse_failures": 0,
        "duplicate_timestamps_removed": 0,
        "status": "PENDING",
        "error": "",
    }


# ============================================================
# 17. CHECKPOINT
# ============================================================

def default_checkpoint() -> Dict[str, Any]:
    return {
        "checkpoint_version": (
            CHECKPOINT_VERSION
        ),
        "program": PROGRAM_NAME,
        "version": VERSION,
        "pattern_id": PATTERN_ID,
        "stage": STAGE,
        "updated_utc": utc_now_iso(),
        "completed_sources": {},
    }


def load_checkpoint(
    path: Path,
) -> Dict[str, Any]:
    if not path.exists():
        return default_checkpoint()

    try:
        checkpoint = read_json(path)
    except Exception:
        return default_checkpoint()

    if (
        checkpoint.get("checkpoint_version")
        != CHECKPOINT_VERSION
    ):
        return default_checkpoint()

    if checkpoint.get("program") != PROGRAM_NAME:
        return default_checkpoint()

    if checkpoint.get("version") != VERSION:
        return default_checkpoint()

    if checkpoint.get("pattern_id") != PATTERN_ID:
        return default_checkpoint()

    completed = checkpoint.get(
        "completed_sources"
    )

    if not isinstance(completed, dict):
        checkpoint["completed_sources"] = {}

    return checkpoint


def checkpoint_source_key(
    source: SourceFile,
) -> str:
    return (
        f"{source.timeframe}/"
        f"{source.market}"
    )


def save_checkpoint(
    path: Path,
    checkpoint: Dict[str, Any],
) -> None:
    checkpoint["updated_utc"] = (
        utc_now_iso()
    )

    write_json(
        path,
        checkpoint,
    )


def can_resume_source(
    checkpoint: Dict[str, Any],
    source: SourceFile,
    source_sha256: str,
) -> bool:
    key = checkpoint_source_key(
        source
    )

    completed = checkpoint.get(
        "completed_sources",
        {},
    )

    entry = completed.get(key)

    if not isinstance(entry, dict):
        return False

    if entry.get("status") != "PASS":
        return False

    if (
        entry.get("source_sha256")
        != source_sha256
    ):
        return False

    return True


# ============================================================
# 18. DETAIL / CANDIDATE COLUMNS
# ============================================================

DETAIL_FIELDS: Tuple[str, ...] = (
    "timeframe",
    "market",
    "timestamp",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "value",

    "ma_5",
    "ma_10",
    "ma_20",
    "ma_60",
    "ma_120",
    "ma_256",

    "close_to_ma_20_pct",
    "close_to_ma_60_pct",
    "close_to_ma_120_pct",
    "close_to_ma_256_pct",
    "close_to_ma256_abs_pct",

    "ma_20_slope_1_pct",
    "ma_60_slope_1_pct",
    "ma_120_slope_1_pct",
    "ma_256_slope_1_pct",

    "ma_20_slope_5_pct",
    "ma_60_slope_5_pct",
    "ma_120_slope_5_pct",
    "ma_256_slope_5_pct",

    "ma_cluster_spread_pct",
    "ma20_to_ma60_pct",
    "ma60_to_ma120_pct",
    "ma120_to_ma256_pct",

    "base_range_10_pct",
    "base_range_20_pct",
    "base_range_60_pct",

    "close_position_10",
    "close_position_20",
    "close_position_60",

    "volatility_5",
    "volatility_20",
    "volatility_60",
    "volatility_5_to_20_ratio",
    "volatility_20_to_60_ratio",

    "volume_ma_5",
    "volume_ma_20",
    "volume_ma_60",
    "volume_5_to_20_ratio",
    "volume_20_to_60_ratio",
    "current_volume_to_20_ratio",

    "return_1_pct",
    "return_3_pct",
    "return_5_pct",
    "return_10_pct",
    "return_20_pct",
    "return_60_pct",

    "candle_range_pct",
    "body_pct",
    "upper_wick_pct",
    "lower_wick_pct",
    "close_position_in_candle",

    "prior_20_high",
    "prior_60_high",
    "close_to_prior20_high_ratio",
    "close_to_prior60_high_ratio",
    "break_above_prior20_high",
    "break_above_prior60_high",

    "cond_ma_convergence_strict",
    "cond_ma_convergence_relaxed",
    "cond_long_ma_near_strict",
    "cond_long_ma_near_relaxed",
    "cond_base_strict",
    "cond_base_relaxed",
    "cond_volatility_strict",
    "cond_volatility_relaxed",
    "cond_volume_strict",
    "cond_volume_relaxed",
    "cond_breakout_pressure",
    "cond_current_breakout",

    "score_ma_convergence",
    "score_long_ma_position",
    "score_base_compression",
    "score_volatility",
    "score_volume_structure",
    "score_breakout_pressure",

    "pattern_256_research_score",
    "pattern_256_candidate",
    "pattern_256_strong_candidate",
    "pattern_256_research_stage",
)


CANDIDATE_FIELDS: Tuple[str, ...] = (
    "timeframe",
    "market",
    "timestamp",
    "close",

    "pattern_256_research_score",
    "pattern_256_candidate",
    "pattern_256_strong_candidate",
    "pattern_256_research_stage",

    "ma_cluster_spread_pct",
    "close_to_ma_256_pct",
    "base_range_20_pct",
    "volatility_5_to_20_ratio",
    "volume_5_to_20_ratio",
    "current_volume_to_20_ratio",
    "close_to_prior20_high_ratio",

    "score_ma_convergence",
    "score_long_ma_position",
    "score_base_compression",
    "score_volatility",
    "score_volume_structure",
    "score_breakout_pressure",

    "cond_ma_convergence_strict",
    "cond_long_ma_near_strict",
    "cond_base_strict",
    "cond_volatility_strict",
    "cond_volume_strict",
    "cond_current_breakout",
)


# ============================================================
# 19. FRAME -> RECORDS
# ============================================================

def dataframe_records(
    frame: pd.DataFrame,
    timeframe: str,
    market: str,
    fields: Sequence[str],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    for _, item in frame.iterrows():
        record: Dict[str, Any] = {
            "timeframe": timeframe,
            "market": market,
        }

        for field in fields:
            if field in (
                "timeframe",
                "market",
            ):
                continue

            value = (
                item[field]
                if field in item.index
                else None
            )

            record[field] = json_safe(
                value
            )

        rows.append(record)

    return rows


# ============================================================
# 20. SUMMARY
# ============================================================

SUMMARY_FIELDS: Tuple[str, ...] = (
    "timeframe",
    "market",
    "rows",
    "eligible_rows",
    "candidate_rows",
    "strong_candidate_rows",
    "setup_rows",
    "ready_rows",
    "trigger_rows",
    "candidate_rate_pct",
    "strong_candidate_rate_pct",
    "score_mean",
    "score_median",
    "score_p90",
    "score_p95",
    "score_max",
    "ma_cluster_spread_median",
    "base_range_20_median",
    "volatility_ratio_median",
    "volume_ratio_median",
    "status",
)


def summarize_frame(
    frame: pd.DataFrame,
    timeframe: str,
    market: str,
) -> Dict[str, Any]:
    score = (
        frame[
            "pattern_256_research_score"
        ]
    )

    eligible = score.notna()

    candidates = (
        frame[
            "pattern_256_candidate"
        ]
        .fillna(False)
        .astype(bool)
    )

    strong = (
        frame[
            "pattern_256_strong_candidate"
        ]
        .fillna(False)
        .astype(bool)
    )

    stage = (
        frame[
            "pattern_256_research_stage"
        ]
        .fillna("NONE")
        .astype(str)
    )

    eligible_rows = int(
        eligible.sum()
    )

    candidate_rows = int(
        candidates.sum()
    )

    strong_rows = int(
        strong.sum()
    )

    valid_score = score[
        score.notna()
    ]

    if len(valid_score) > 0:
        score_mean = float(
            valid_score.mean()
        )
        score_median = float(
            valid_score.median()
        )
        score_p90 = float(
            valid_score.quantile(0.90)
        )
        score_p95 = float(
            valid_score.quantile(0.95)
        )
        score_max = float(
            valid_score.max()
        )
    else:
        score_mean = 0.0
        score_median = 0.0
        score_p90 = 0.0
        score_p95 = 0.0
        score_max = 0.0

    candidate_rate = (
        candidate_rows
        / eligible_rows
        * 100.0
        if eligible_rows > 0
        else 0.0
    )

    strong_rate = (
        strong_rows
        / eligible_rows
        * 100.0
        if eligible_rows > 0
        else 0.0
    )

    return {
        "timeframe": timeframe,
        "market": market,
        "rows": int(len(frame)),
        "eligible_rows": eligible_rows,
        "candidate_rows": candidate_rows,
        "strong_candidate_rows": (
            strong_rows
        ),
        "setup_rows": int(
            (stage == "SETUP").sum()
        ),
        "ready_rows": int(
            (stage == "READY").sum()
        ),
        "trigger_rows": int(
            (stage == "TRIGGER").sum()
        ),
        "candidate_rate_pct": (
            candidate_rate
        ),
        "strong_candidate_rate_pct": (
            strong_rate
        ),
        "score_mean": score_mean,
        "score_median": score_median,
        "score_p90": score_p90,
        "score_p95": score_p95,
        "score_max": score_max,
        "ma_cluster_spread_median": (
            finite_or_none(
                frame[
                    "ma_cluster_spread_pct"
                ].median()
            )
        ),
        "base_range_20_median": (
            finite_or_none(
                frame[
                    "base_range_20_pct"
                ].median()
            )
        ),
        "volatility_ratio_median": (
            finite_or_none(
                frame[
                    "volatility_5_to_20_ratio"
                ].median()
            )
        ),
        "volume_ratio_median": (
            finite_or_none(
                frame[
                    "volume_5_to_20_ratio"
                ].median()
            )
        ),
        "status": "PASS",
    }


# ============================================================
# 21. PROCESS ONE SOURCE
# ============================================================

@dataclass
class SourceResearchResult:
    summary: Dict[str, Any]
    detail_rows: List[Dict[str, Any]]
    candidate_rows: List[Dict[str, Any]]
    manifest: Dict[str, Any]


def process_source(
    source: SourceFile,
    paths: ProjectPaths,
) -> SourceResearchResult:
    manifest = build_source_manifest_base(
        source,
        paths,
    )

    sha_before = manifest[
        "sha256_before"
    ]

    try:
        loaded = load_ohlcv(
            source
        )

        manifest[
            "original_rows"
        ] = loaded.original_rows

        manifest[
            "valid_rows"
        ] = loaded.valid_rows

        manifest[
            "timestamp_parse_failures"
        ] = (
            loaded.timestamp_parse_failures
        )

        manifest[
            "duplicate_timestamps_removed"
        ] = (
            loaded.duplicate_timestamps_removed
        )

        features = add_common_features(
            loaded.frame
        )

        research = (
            add_256_research_dimensions(
                features
            )
        )

        summary = summarize_frame(
            research,
            source.timeframe,
            source.market,
        )

        detail_rows = dataframe_records(
            research,
            source.timeframe,
            source.market,
            DETAIL_FIELDS,
        )

        candidate_frame = research[
            research[
                "pattern_256_candidate"
            ]
            .fillna(False)
            .astype(bool)
        ].copy()

        candidate_rows = dataframe_records(
            candidate_frame,
            source.timeframe,
            source.market,
            CANDIDATE_FIELDS,
        )

        sha_after = sha256_file(
            source.path
        )

        manifest[
            "sha256_after"
        ] = sha_after

        manifest[
            "sha256_unchanged"
        ] = (
            sha_before
            == sha_after
        )

        if sha_before != sha_after:
            raise RuntimeError(
                "Production OHLCV SHA256 changed "
                f"during research: {source.path}"
            )

        manifest["status"] = "PASS"

        return SourceResearchResult(
            summary=summary,
            detail_rows=detail_rows,
            candidate_rows=candidate_rows,
            manifest=manifest,
        )

    except Exception as exc:
        try:
            sha_after = sha256_file(
                source.path
            )

            manifest[
                "sha256_after"
            ] = sha_after

            manifest[
                "sha256_unchanged"
            ] = (
                sha_before
                == sha_after
            )
        except Exception:
            manifest[
                "sha256_after"
            ] = ""

            manifest[
                "sha256_unchanged"
            ] = False

        manifest["status"] = "ERROR"
        manifest["error"] = (
            f"{type(exc).__name__}: {exc}"
        )

        raise


# ============================================================
# 22. AGGREGATE SUMMARY
# ============================================================

def aggregate_statistics(
    summaries: Sequence[Dict[str, Any]],
) -> Dict[str, Any]:
    total_rows = sum(
        int(row.get("rows", 0))
        for row in summaries
    )

    eligible_rows = sum(
        int(row.get("eligible_rows", 0))
        for row in summaries
    )

    candidate_rows = sum(
        int(row.get("candidate_rows", 0))
        for row in summaries
    )

    strong_candidate_rows = sum(
        int(
            row.get(
                "strong_candidate_rows",
                0,
            )
        )
        for row in summaries
    )

    setup_rows = sum(
        int(row.get("setup_rows", 0))
        for row in summaries
    )

    ready_rows = sum(
        int(row.get("ready_rows", 0))
        for row in summaries
    )

    trigger_rows = sum(
        int(row.get("trigger_rows", 0))
        for row in summaries
    )

    return {
        "files_processed": len(
            summaries
        ),
        "total_rows": total_rows,
        "eligible_rows": eligible_rows,
        "candidate_rows": candidate_rows,
        "strong_candidate_rows": (
            strong_candidate_rows
        ),
        "setup_rows": setup_rows,
        "ready_rows": ready_rows,
        "trigger_rows": trigger_rows,
        "candidate_rate_pct": (
            candidate_rows
            / eligible_rows
            * 100.0
            if eligible_rows > 0
            else 0.0
        ),
        "strong_candidate_rate_pct": (
            strong_candidate_rows
            / eligible_rows
            * 100.0
            if eligible_rows > 0
            else 0.0
        ),
    }


def aggregate_by_timeframe(
    summaries: Sequence[Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    result: Dict[str, Dict[str, Any]] = {}

    for timeframe in DEFAULT_TIMEFRAMES:
        rows = [
            row
            for row in summaries
            if row.get("timeframe")
            == timeframe
        ]

        if not rows:
            continue

        result[timeframe] = (
            aggregate_statistics(rows)
        )

    return result


# ============================================================
# 23. README
# ============================================================

def write_readme(
    paths: ProjectPaths,
) -> None:
    text = f"""\
{PROJECT_NAME}
{PROGRAM_NAME}
{VERSION}

STAGE
-----
{STAGE}

PATTERN
-------
{PATTERN_KOREAN_NAME}

PURPOSE
-------
Independent quantitative research for the 256 pattern.

IMPORTANT
---------
This stage does NOT define the final 256 formula.

The score and thresholds are research candidate definitions only.
They are intended to create measurable observations for later
historical outcome validation.

FUTURE INFORMATION
------------------
Future candles are NOT used.

No:
- negative shift
- centered rolling
- future return
- future max/min
- future 30 percent label
- backtest
- prediction
- trading

SOURCE SAFETY
-------------
Production OHLCV is READ ONLY.

The program verifies SHA256 before and after each source file.

OUTPUTS
-------
pattern_256_research_result.json
pattern_256_research_summary.csv
pattern_256_research_detail.csv
pattern_256_candidates.csv
pattern_256_source_manifest.csv
pattern_256_research_contract.json
README.txt

CHECKPOINT
----------
data/validation/research_pattern_256_checkpoint.json

NEXT STAGE
----------
After test and production workflows pass, validate the research
outputs before introducing any future-outcome / 30 percent labels.
"""

    ensure_parent(
        paths.readme_txt
    )

    paths.readme_txt.write_text(
        text,
        encoding="utf-8",
    )


# ============================================================
# 24. OUTPUT VALIDATION
# ============================================================

def validate_output_contract(
    paths: ProjectPaths,
) -> None:
    required = [
        paths.result_json,
        paths.summary_csv,
        paths.detail_csv,
        paths.candidate_csv,
        paths.source_manifest_csv,
        paths.research_contract_json,
        paths.readme_txt,
        paths.checkpoint,
    ]

    for path in required:
        if not path.exists():
            raise FileNotFoundError(
                f"Required output missing: {path}"
            )

        if path.stat().st_size <= 0:
            raise RuntimeError(
                f"Output is empty: {path}"
            )


# ============================================================
# 25. FINAL SOURCE IMMUTABILITY CHECK
# ============================================================

def verify_manifest_immutability(
    manifests: Sequence[Dict[str, Any]],
) -> None:
    failures = []

    for row in manifests:
        if (
            row.get("status")
            != "PASS"
        ):
            failures.append(
                row.get("relative_path")
            )
            continue

        if (
            row.get("sha256_unchanged")
            is not True
        ):
            failures.append(
                row.get("relative_path")
            )

    if failures:
        raise RuntimeError(
            "Source immutability verification failed: "
            + ", ".join(
                str(item)
                for item in failures[:20]
            )
        )


# ============================================================
# 26. MAIN RESEARCH
# ============================================================

def run_research(
    args: argparse.Namespace,
) -> int:
    started_utc = utc_now_iso()

    project_root = Path(
        args.project_root
    )

    paths = build_project_paths(
        project_root
    )

    timeframes = validate_timeframes(
        args.timeframes
    )

    print_section(
        f"{PROJECT_NAME}\n"
        f"{STAGE}\n"
        f"{VERSION}"
    )

    safe_print(
        f"Started UTC : {started_utc}"
    )
    safe_print(
        f"Project root: {paths.root}"
    )
    safe_print(
        f"Timeframes  : "
        f"{', '.join(timeframes)}"
    )

    if args.market:
        safe_print(
            f"Market filter: "
            f"{args.market.upper()}"
        )

    safe_print("")
    safe_print("[SAFETY] Production OHLCV READ ONLY")
    safe_print("[SAFETY] Future candles DISABLED")
    safe_print("[SAFETY] Future labels DISABLED")
    safe_print("[SAFETY] Prediction DISABLED")
    safe_print("[SAFETY] Trading DISABLED")
    safe_print(
        "[RESEARCH] Final 256 formula NOT defined"
    )

    # --------------------------------------------------------
    # Validate directories
    # --------------------------------------------------------

    print_section(
        "1. VALIDATE PROJECT STRUCTURE"
    )

    required_directories = [
        paths.data,
        paths.ohlcv,
    ]

    for timeframe in timeframes:
        required_directories.append(
            paths.ohlcv / timeframe
        )

    for path in required_directories:
        if not path.exists():
            raise FileNotFoundError(
                f"Required directory missing: "
                f"{path}"
            )

        safe_print(
            f"[PASS] {path}"
        )

    ensure_directory(
        paths.reports
    )

    ensure_directory(
        paths.validation
    )

    ensure_directory(
        paths.report_dir
    )

    # --------------------------------------------------------
    # Research contract
    # --------------------------------------------------------

    print_section(
        "2. WRITE 256 RESEARCH CONTRACT"
    )

    research_contract = (
        build_research_contract()
    )

    write_json(
        paths.research_contract_json,
        research_contract,
    )

    safe_print(
        "[PASS] Research contract written:"
    )
    safe_print(
        f"       {paths.research_contract_json}"
    )

    # --------------------------------------------------------
    # Discover sources
    # --------------------------------------------------------

    print_section(
        "3. DISCOVER PRODUCTION OHLCV"
    )

    sources = discover_source_files(
        paths=paths,
        timeframes=timeframes,
        market_filter=args.market,
        max_files=max(
            int(args.max_files),
            0,
        ),
    )

    safe_print(
        f"Source files: {len(sources)}"
    )

    timeframe_counts: Dict[str, int] = {}

    for source in sources:
        timeframe_counts[
            source.timeframe
        ] = (
            timeframe_counts.get(
                source.timeframe,
                0,
            )
            + 1
        )

    for timeframe in timeframes:
        safe_print(
            f"  {timeframe}: "
            f"{timeframe_counts.get(timeframe, 0)}"
        )

    # --------------------------------------------------------
    # Load checkpoint
    # --------------------------------------------------------

    print_section(
        "4. LOAD CHECKPOINT"
    )

    checkpoint = load_checkpoint(
        paths.checkpoint
    )

    completed_count = len(
        checkpoint.get(
            "completed_sources",
            {},
        )
    )

    safe_print(
        f"Checkpoint: {paths.checkpoint}"
    )
    safe_print(
        f"Completed entries: "
        f"{completed_count}"
    )
    safe_print(
        f"Force mode: {bool(args.force)}"
    )

    # --------------------------------------------------------
    # Process sources
    # --------------------------------------------------------

    print_section(
        "5. RUN 256 INDEPENDENT RESEARCH"
    )

    all_summaries: List[
        Dict[str, Any]
    ] = []

    all_detail_rows: List[
        Dict[str, Any]
    ] = []

    all_candidate_rows: List[
        Dict[str, Any]
    ] = []

    all_manifests: List[
        Dict[str, Any]
    ] = []

    files_pass = 0
    files_error = 0
    resume_verified = 0

    for index, source in enumerate(
        sources,
        start=1,
    ):
        safe_print(
            separator("-", 72)
        )
        safe_print(
            f"[{index}/{len(sources)}] "
            f"{source.timeframe.upper()} "
            f"{source.market}"
        )
        safe_print(
            f"Source: {source.path}"
        )

        source_sha = sha256_file(
            source.path
        )

        if (
            not args.force
            and can_resume_source(
                checkpoint,
                source,
                source_sha,
            )
        ):
            # ------------------------------------------------
            # IMPORTANT:
            #
            # We do NOT blindly skip because an entry exists.
            #
            # A checkpoint entry is only recognized after
            # verifying current source SHA256 matches the SHA256
            # recorded when the source was processed.
            #
            # However, because aggregate output files must remain
            # deterministic and complete, Clean V001 still reads
            # and recomputes the research observations.
            #
            # Therefore this is a verified-resume marker, not an
            # unsafe "file exists -> skip".
            # ------------------------------------------------

            resume_verified += 1

            safe_print(
                "[RESUME] Source checkpoint SHA256 verified."
            )
            safe_print(
                "[RESUME] Rebuilding deterministic report rows."
            )

        try:
            result = process_source(
                source,
                paths,
            )

            all_summaries.append(
                result.summary
            )

            all_detail_rows.extend(
                result.detail_rows
            )

            all_candidate_rows.extend(
                result.candidate_rows
            )

            all_manifests.append(
                result.manifest
            )

            files_pass += 1

            key = checkpoint_source_key(
                source
            )

            checkpoint[
                "completed_sources"
            ][key] = {
                "status": "PASS",
                "source_sha256": (
                    result.manifest[
                        "sha256_after"
                    ]
                ),
                "rows": (
                    result.summary[
                        "rows"
                    ]
                ),
                "candidate_rows": (
                    result.summary[
                        "candidate_rows"
                    ]
                ),
                "strong_candidate_rows": (
                    result.summary[
                        "strong_candidate_rows"
                    ]
                ),
                "completed_utc": (
                    utc_now_iso()
                ),
            }

            save_checkpoint(
                paths.checkpoint,
                checkpoint,
            )

            safe_print(
                "[PASS] "
                f"rows={result.summary['rows']} "
                f"candidates="
                f"{result.summary['candidate_rows']} "
                f"strong="
                f"{result.summary['strong_candidate_rows']} "
                f"max_score="
                f"{result.summary['score_max']:.2f}"
            )

        except Exception as exc:
            files_error += 1

            error_manifest = (
                build_source_manifest_base(
                    source,
                    paths,
                )
            )

            try:
                error_manifest[
                    "sha256_after"
                ] = sha256_file(
                    source.path
                )

                error_manifest[
                    "sha256_unchanged"
                ] = (
                    error_manifest[
                        "sha256_before"
                    ]
                    == error_manifest[
                        "sha256_after"
                    ]
                )
            except Exception:
                pass

            error_manifest[
                "status"
            ] = "ERROR"

            error_manifest[
                "error"
            ] = (
                f"{type(exc).__name__}: "
                f"{exc}"
            )

            all_manifests.append(
                error_manifest
            )

            safe_print(
                f"[ERROR] "
                f"{type(exc).__name__}: "
                f"{exc}"
            )

            # Clean V001 is fail-closed.
            raise

    # --------------------------------------------------------
    # Sort deterministic outputs
    # --------------------------------------------------------

    print_section(
        "6. BUILD RESEARCH OUTPUTS"
    )

    all_summaries.sort(
        key=lambda row: (
            str(row.get("timeframe", "")),
            str(row.get("market", "")),
        )
    )

    all_detail_rows.sort(
        key=lambda row: (
            str(row.get("timeframe", "")),
            str(row.get("market", "")),
            str(row.get("timestamp", "")),
        )
    )

    all_candidate_rows.sort(
        key=lambda row: (
            str(row.get("timeframe", "")),
            str(row.get("market", "")),
            str(row.get("timestamp", "")),
        )
    )

    all_manifests.sort(
        key=lambda row: (
            str(row.get("timeframe", "")),
            str(row.get("market", "")),
        )
    )

    write_csv(
        paths.summary_csv,
        all_summaries,
        SUMMARY_FIELDS,
    )

    write_csv(
        paths.detail_csv,
        all_detail_rows,
        DETAIL_FIELDS,
    )

    write_csv(
        paths.candidate_csv,
        all_candidate_rows,
        CANDIDATE_FIELDS,
    )

    write_csv(
        paths.source_manifest_csv,
        all_manifests,
        SOURCE_MANIFEST_FIELDS,
    )

    safe_print(
        f"[PASS] {paths.summary_csv}"
    )
    safe_print(
        f"[PASS] {paths.detail_csv}"
    )
    safe_print(
        f"[PASS] {paths.candidate_csv}"
    )
    safe_print(
        f"[PASS] {paths.source_manifest_csv}"
    )

    # --------------------------------------------------------
    # Source integrity
    # --------------------------------------------------------

    print_section(
        "7. VERIFY PRODUCTION OHLCV IMMUTABILITY"
    )

    verify_manifest_immutability(
        all_manifests
    )

    safe_print(
        "[PASS] All processed production "
        "OHLCV SHA256 values unchanged."
    )

    # --------------------------------------------------------
    # Aggregate statistics
    # --------------------------------------------------------

    print_section(
        "8. AGGREGATE 256 RESEARCH STATISTICS"
    )

    statistics = aggregate_statistics(
        all_summaries
    )

    by_timeframe = aggregate_by_timeframe(
        all_summaries
    )

    safe_print(
        f"Files processed       : "
        f"{statistics['files_processed']}"
    )
    safe_print(
        f"Rows                  : "
        f"{statistics['total_rows']}"
    )
    safe_print(
        f"Eligible rows         : "
        f"{statistics['eligible_rows']}"
    )
    safe_print(
        f"Candidate rows        : "
        f"{statistics['candidate_rows']}"
    )
    safe_print(
        f"Strong candidate rows : "
        f"{statistics['strong_candidate_rows']}"
    )
    safe_print(
        f"SETUP rows            : "
        f"{statistics['setup_rows']}"
    )
    safe_print(
        f"READY rows            : "
        f"{statistics['ready_rows']}"
    )
    safe_print(
        f"TRIGGER rows          : "
        f"{statistics['trigger_rows']}"
    )

    safe_print("")
    safe_print(
        "Candidate rate        : "
        f"{statistics['candidate_rate_pct']:.6f}%"
    )
    safe_print(
        "Strong candidate rate : "
        f"{statistics['strong_candidate_rate_pct']:.6f}%"
    )

    # --------------------------------------------------------
    # README
    # --------------------------------------------------------

    write_readme(
        paths
    )

    # --------------------------------------------------------
    # Result JSON
    # --------------------------------------------------------

    print_section(
        "9. WRITE FINAL RESULT"
    )

    completed_utc = utc_now_iso()

    result_json: Dict[str, Any] = {
        "program": PROGRAM_NAME,
        "version": VERSION,
        "project": PROJECT_NAME,
        "stage": STAGE,
        "status": (
            "PASS"
            if files_error == 0
            else "FAIL"
        ),
        "started_utc": started_utc,
        "completed_utc": completed_utc,
        "project_root": str(
            paths.root
        ),
        "pattern": {
            "id": PATTERN_ID,
            "korean_name": (
                PATTERN_KOREAN_NAME
            ),
            "formula_status": (
                "RESEARCH_CANDIDATE_DEFINITION"
            ),
            "final_formula_defined": False,
            "independent_research": True,
        },
        "scope": {
            "timeframes": list(
                timeframes
            ),
            "market_filter": (
                args.market.upper()
                if args.market
                else None
            ),
            "source_files": len(
                sources
            ),
            "force": bool(
                args.force
            ),
            "max_files": int(
                args.max_files
            ),
        },
        "statistics": statistics,
        "by_timeframe": by_timeframe,
        "processing": {
            "files_pass": files_pass,
            "files_error": files_error,
            "resume_sha256_verified": (
                resume_verified
            ),
        },
        "research_definition": {
            "thresholds": {
                key: value
                for key, value
                in THRESHOLDS.__dict__.items()
            },
            "score_maximum": 100.0,
            "candidate_score_min": (
                THRESHOLDS.candidate_score_min
            ),
            "strong_candidate_score_min": (
                THRESHOLDS.strong_candidate_score_min
            ),
            "final_formula_claimed": False,
        },
        "future_information": {
            "current_candle_used": True,
            "past_candles_used": True,
            "future_candles_used": False,
            "negative_shift_used": False,
            "centered_rolling_used": False,
            "future_labels_generated": False,
            "future_returns_generated": False,
            "future_max_generated": False,
            "future_min_generated": False,
            "thirty_percent_label_generated": False,
        },
        "safety": {
            "ohlcv_read_only": True,
            "ohlcv_write_executed": False,
            "ohlcv_delete_executed": False,
            "ohlcv_repair_executed": False,
            "api_download_executed": False,
            "feature_build_executed": False,
            "prediction_executed": False,
            "trading_executed": False,
            "git_reset_executed": False,
            "git_clean_executed": False,
            "git_commit_executed": False,
            "git_push_executed": False,
            "source_sha256_unchanged": True,
        },
        "outputs": {
            "summary_csv": relative_path(
                paths.summary_csv,
                paths.root,
            ),
            "detail_csv": relative_path(
                paths.detail_csv,
                paths.root,
            ),
            "candidate_csv": relative_path(
                paths.candidate_csv,
                paths.root,
            ),
            "source_manifest_csv": relative_path(
                paths.source_manifest_csv,
                paths.root,
            ),
            "research_contract_json": relative_path(
                paths.research_contract_json,
                paths.root,
            ),
            "checkpoint_json": relative_path(
                paths.checkpoint,
                paths.root,
            ),
            "readme_txt": relative_path(
                paths.readme_txt,
                paths.root,
            ),
        },
        "next_stage": {
            "allowed": True,
            "name": (
                "PATTERN 256 RESEARCH OUTPUT VALIDATION"
            ),
            "note": (
                "Do not introduce future 30% outcome labels "
                "until the 256 research output itself has "
                "passed test and production validation."
            ),
        },
    }

    write_json(
        paths.result_json,
        result_json,
    )

    safe_print(
        f"[PASS] {paths.result_json}"
    )

    # --------------------------------------------------------
    # Final output validation
    # --------------------------------------------------------

    print_section(
        "10. FINAL CLEAN V001 GATE"
    )

    validate_output_contract(
        paths
    )

    if files_error != 0:
        raise RuntimeError(
            f"Research files_error={files_error}"
        )

    if (
        result_json[
            "future_information"
        ][
            "future_candles_used"
        ]
        is not False
    ):
        raise RuntimeError(
            "Future-information gate failed."
        )

    if (
        result_json[
            "pattern"
        ][
            "final_formula_defined"
        ]
        is not False
    ):
        raise RuntimeError(
            "Clean V001 must not claim "
            "a final 256 formula."
        )

    if (
        result_json[
            "safety"
        ][
            "source_sha256_unchanged"
        ]
        is not True
    ):
        raise RuntimeError(
            "Production OHLCV integrity gate failed."
        )

    safe_print(
        "[PASS] Pattern ID = 256"
    )
    safe_print(
        "[PASS] Independent research completed"
    )
    safe_print(
        "[PASS] Final formula remains undefined"
    )
    safe_print(
        "[PASS] Future candles not used"
    )
    safe_print(
        "[PASS] Future labels not generated"
    )
    safe_print(
        "[PASS] Production OHLCV unchanged"
    )
    safe_print(
        "[PASS] Checkpoint maintained"
    )
    safe_print(
        "[PASS] Research outputs generated"
    )

    safe_print("")
    safe_print(separator())
    safe_print(
        "PATTERN 256 RESEARCH CLEAN V001 PASS"
    )
    safe_print(separator())
    safe_print("")
    safe_print("[NEXT]")
    safe_print(
        "Create research_pattern_256_test.yml"
    )
    safe_print(
        "and validate this program before "
        "Production execution."
    )

    return 0


# ============================================================
# 27. MAIN
# ============================================================

def main() -> int:
    try:
        args = parse_args()

        return run_research(
            args
        )

    except KeyboardInterrupt:
        safe_print("")
        safe_print(separator())
        safe_print(
            "PATTERN 256 RESEARCH INTERRUPTED"
        )
        safe_print(separator())
        safe_print("")
        safe_print(
            "[INFO] Existing production OHLCV "
            "was not intentionally modified."
        )
        safe_print(
            "[INFO] Re-run the program to continue."
        )

        return 130

    except Exception as exc:
        safe_print("")
        safe_print(separator())
        safe_print(
            "PATTERN 256 RESEARCH FATAL ERROR"
        )
        safe_print(separator())
        safe_print("")
        safe_print(
            f"{type(exc).__name__}: {exc}"
        )
        safe_print("")
        traceback.print_exc()

        safe_print("")
        safe_print(
            "[SAFETY] No intentional OHLCV "
            "write was executed."
        )
        safe_print(
            "[SAFETY] No intentional OHLCV "
            "deletion was executed."
        )
        safe_print(
            "[SAFETY] No historical gap "
            "repair was executed."
        )
        safe_print(
            "[SAFETY] No API candle "
            "download was executed."
        )
        safe_print(
            "[SAFETY] No future labels "
            "were generated."
        )
        safe_print(
            "[SAFETY] No prediction/trading "
            "was executed."
        )
        safe_print(
            "[SAFETY] No Git reset/clean/"
            "commit/push was executed."
        )
        safe_print("")
        safe_print(
            "[BLOCK] Do not proceed to 256 "
            "outcome validation until this "
            "research stage passes."
        )
        safe_print(separator())

        return 1


if __name__ == "__main__":
    raise SystemExit(
        main()
    )
