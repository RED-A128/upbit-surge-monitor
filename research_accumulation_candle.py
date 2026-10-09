# ============================================================
# Upbit Surge Monitor
# research_accumulation_candle.py
# Clean V001
# ============================================================
#
# PURPOSE
# ------------------------------------------------------------
# Independent quantitative research for "accumulation candle".
#
# This program studies observable candle / volume / moving
# average characteristics that may describe an accumulation
# candle candidate.
#
# IMPORTANT
# ------------------------------------------------------------
# This program DOES NOT claim that a candle is actual
# institutional / whale accumulation.
#
# It only creates reproducible quantitative research features
# from information available at the current candle and past
# candles.
#
# NO FUTURE INFORMATION is used for candidate generation.
#
# Production OHLCV is READ ONLY.
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
import tempfile
import traceback

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd


# ============================================================
# 1. PROGRAM IDENTITY
# ============================================================

PROGRAM_NAME = "research_accumulation_candle.py"
VERSION = "Clean V001"
STAGE = "ACCUMULATION CANDLE INDEPENDENT RESEARCH"

PATTERN_ID = "ACCUMULATION_CANDLE"
PATTERN_NAME_KO = "매집봉"

CHECKPOINT_VERSION = 1


# ============================================================
# 2. SAFETY CONTRACT
# ============================================================

OHLCV_READ_ONLY = True

CURRENT_CANDLE_ALLOWED = True
PAST_CANDLES_ALLOWED = True

FUTURE_CANDLES_ALLOWED = False
NEGATIVE_SHIFT_ALLOWED = False
CENTERED_ROLLING_ALLOWED = False

FUTURE_LABELS_GENERATED = False
FUTURE_RETURNS_GENERATED = False
FUTURE_MAX_GENERATED = False
FUTURE_MIN_GENERATED = False

THIRTY_PERCENT_LABEL_GENERATED = False

BACKTEST_GENERATED = False
PREDICTION_GENERATED = False
TRADING_SIGNAL_GENERATED = False

FINAL_FORMULA_DEFINED = False


# ============================================================
# 3. DEFAULT CONFIGURATION
# ============================================================

DEFAULT_TIMEFRAMES = (
    "h1",
    "h4",
    "d1",
)

REQUIRED_OHLCV_COLUMNS = (
    "timestamp",
    "open",
    "high",
    "low",
    "close",
    "volume",
)

MA_WINDOWS = (
    5,
    20,
    60,
    112,
    224,
    448,
)

VOLUME_WINDOWS = (
    5,
    10,
    20,
    60,
)

RANGE_WINDOWS = (
    5,
    20,
    60,
)

COMPRESSION_WINDOWS = (
    20,
    60,
)

MIN_REQUIRED_ROWS = 1

CANDIDATE_SCORE_THRESHOLD = 60.0
STRONG_CANDIDATE_SCORE_THRESHOLD = 75.0


# ============================================================
# 4. RESEARCH DIMENSIONS
# ============================================================

RESEARCH_DIMENSIONS = (
    {
        "name": "VOLUME_EXPANSION",
        "max_score": 25.0,
        "description": (
            "Current volume expansion relative to historical "
            "rolling volume baselines."
        ),
    },
    {
        "name": "CANDLE_BODY_STRENGTH",
        "max_score": 20.0,
        "description": (
            "Body size and bullish body strength relative to "
            "the candle range and recent price behavior."
        ),
    },
    {
        "name": "CLOSE_LOCATION",
        "max_score": 15.0,
        "description": (
            "Closing-price position inside the current candle."
        ),
    },
    {
        "name": "WICK_STRUCTURE",
        "max_score": 10.0,
        "description": (
            "Upper/lower wick structure of the current candle."
        ),
    },
    {
        "name": "RANGE_EXPANSION",
        "max_score": 15.0,
        "description": (
            "Current candle range relative to historical "
            "rolling range baselines."
        ),
    },
    {
        "name": "BASE_CONTEXT",
        "max_score": 15.0,
        "description": (
            "Past/current context including compression and "
            "112/224/448 moving-average structure."
        ),
    },
)

MAX_RESEARCH_SCORE = sum(
    float(item["max_score"])
    for item in RESEARCH_DIMENSIONS
)


# ============================================================
# 5. OUTPUT NAMESPACE
# ============================================================

REPORT_NAMESPACE = "accumulation_candle_research"

RESULT_FILENAME = "accumulation_candle_research_result.json"
SUMMARY_FILENAME = "accumulation_candle_research_summary.csv"
DETAIL_FILENAME = "accumulation_candle_research_detail.csv"
CANDIDATE_FILENAME = "accumulation_candle_candidates.csv"
MANIFEST_FILENAME = "accumulation_candle_source_manifest.csv"
CONTRACT_FILENAME = "accumulation_candle_research_contract.json"
README_FILENAME = "README.txt"

CHECKPOINT_FILENAME = (
    "research_accumulation_candle_checkpoint.json"
)


# ============================================================
# 6. DATA CLASSES
# ============================================================

@dataclass(frozen=True)
class SourceFile:
    timeframe: str
    market: str
    path: Path
    relative_path: str
    size_bytes: int
    sha256_before: str


@dataclass
class LoadedOHLCV:
    source: SourceFile
    frame: pd.DataFrame
    original_rows: int
    valid_rows: int


# ============================================================
# 7. CONSOLE
# ============================================================

def safe_print(*values: Any) -> None:
    text = " ".join(str(value) for value in values)

    try:
        print(text, flush=True)
    except UnicodeEncodeError:
        encoding = (
            getattr(sys.stdout, "encoding", None)
            or "utf-8"
        )

        safe = text.encode(
            encoding,
            errors="replace",
        ).decode(
            encoding,
            errors="replace",
        )

        print(safe, flush=True)


def print_header(title: str) -> None:
    safe_print("")
    safe_print("=" * 72)
    safe_print(title)
    safe_print("=" * 72)


# ============================================================
# 8. BASIC UTILITIES
# ============================================================

def utc_now_iso() -> str:
    return datetime.now(
        timezone.utc
    ).isoformat()


def ensure_directory(path: Path) -> None:
    path.mkdir(
        parents=True,
        exist_ok=True,
    )


def sha256_file(
    path: Path,
    chunk_size: int = 1024 * 1024,
) -> str:

    digest = hashlib.sha256()

    with path.open("rb") as handle:

        while True:
            chunk = handle.read(chunk_size)

            if not chunk:
                break

            digest.update(chunk)

    return digest.hexdigest()


def normalize_market_name(path: Path) -> str:
    return path.stem.strip().upper()


def finite_float(
    value: Any,
    default: float = 0.0,
) -> float:

    try:
        result = float(value)

        if math.isfinite(result):
            return result

    except (TypeError, ValueError):
        pass

    return default


def bool_to_text(value: Any) -> str:
    return "true" if bool(value) else "false"


# ============================================================
# 9. ATOMIC WRITE
# ============================================================

def atomic_write_text(
    path: Path,
    text: str,
) -> None:

    ensure_directory(path.parent)

    fd, temp_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=str(path.parent),
    )

    os.close(fd)

    temp_path = Path(temp_name)

    try:
        temp_path.write_text(
            text,
            encoding="utf-8",
        )

        os.replace(
            temp_path,
            path,
        )

    finally:
        if temp_path.exists():
            try:
                temp_path.unlink()
            except OSError:
                pass


def atomic_write_json(
    path: Path,
    payload: Dict[str, Any],
) -> None:

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


def atomic_write_csv(
    path: Path,
    frame: pd.DataFrame,
) -> None:

    ensure_directory(path.parent)

    fd, temp_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=str(path.parent),
    )

    os.close(fd)

    temp_path = Path(temp_name)

    try:
        frame.to_csv(
            temp_path,
            index=False,
            encoding="utf-8-sig",
        )

        os.replace(
            temp_path,
            path,
        )

    finally:
        if temp_path.exists():
            try:
                temp_path.unlink()
            except OSError:
                pass


# ============================================================
# 10. CHECKPOINT
# ============================================================

def new_checkpoint() -> Dict[str, Any]:

    return {
        "checkpoint_version": CHECKPOINT_VERSION,
        "program": PROGRAM_NAME,
        "version": VERSION,
        "pattern_id": PATTERN_ID,
        "updated_at_utc": utc_now_iso(),
        "completed_sources": {},
    }


def load_checkpoint(
    path: Path,
) -> Dict[str, Any]:

    if not path.exists():
        return new_checkpoint()

    try:
        with path.open(
            "r",
            encoding="utf-8-sig",
        ) as handle:
            payload = json.load(handle)

    except Exception as exc:
        raise ValueError(
            f"Checkpoint read failed: {path}: {exc}"
        ) from exc

    if (
        payload.get("checkpoint_version")
        != CHECKPOINT_VERSION
    ):
        raise ValueError(
            "Checkpoint version mismatch."
        )

    if payload.get("program") != PROGRAM_NAME:
        raise ValueError(
            "Checkpoint program mismatch."
        )

    if payload.get("version") != VERSION:
        raise ValueError(
            "Checkpoint Clean version mismatch."
        )

    if payload.get("pattern_id") != PATTERN_ID:
        raise ValueError(
            "Checkpoint pattern mismatch."
        )

    completed = payload.get(
        "completed_sources"
    )

    if not isinstance(completed, dict):
        raise ValueError(
            "Checkpoint completed_sources is invalid."
        )

    return payload


def save_checkpoint(
    path: Path,
    checkpoint: Dict[str, Any],
) -> None:

    checkpoint["updated_at_utc"] = (
        utc_now_iso()
    )

    atomic_write_json(
        path,
        checkpoint,
    )


def checkpoint_key(
    source: SourceFile,
) -> str:

    return (
        f"{source.timeframe}"
        f"|{source.market}"
        f"|{source.relative_path}"
    )


# ============================================================
# 11. SOURCE DISCOVERY
# ============================================================

def discover_sources(
    project_root: Path,
    timeframes: Sequence[str],
) -> List[SourceFile]:

    ohlcv_root = (
        project_root
        / "data"
        / "ohlcv"
    )

    if not ohlcv_root.exists():
        raise FileNotFoundError(
            f"OHLCV root missing: {ohlcv_root}"
        )

    sources: List[SourceFile] = []

    for timeframe in timeframes:

        directory = (
            ohlcv_root
            / timeframe
        )

        if not directory.exists():
            raise FileNotFoundError(
                f"OHLCV timeframe directory missing: "
                f"{directory}"
            )

        files = sorted(
            directory.glob("*.csv"),
            key=lambda p: p.name.upper(),
        )

        if not files:
            raise FileNotFoundError(
                f"No OHLCV CSV files: {directory}"
            )

        for path in files:

            resolved = path.resolve()

            relative_path = str(
                resolved.relative_to(
                    project_root.resolve()
                )
            )

            source = SourceFile(
                timeframe=timeframe,
                market=normalize_market_name(
                    resolved
                ),
                path=resolved,
                relative_path=relative_path,
                size_bytes=resolved.stat().st_size,
                sha256_before=sha256_file(
                    resolved
                ),
            )

            sources.append(source)

    if not sources:
        raise RuntimeError(
            "No OHLCV source files discovered."
        )

    return sources


# ============================================================
# 12. OHLCV LOADING
# ============================================================

def load_ohlcv(
    source: SourceFile,
) -> LoadedOHLCV:

    try:
        frame = pd.read_csv(
            source.path,
            encoding="utf-8-sig",
        )

    except UnicodeDecodeError:
        frame = pd.read_csv(
            source.path,
            encoding="utf-8",
        )

    except Exception as exc:
        raise ValueError(
            f"CSV read failed: "
            f"{source.path}: {exc}"
        ) from exc

    original_rows = len(frame)

    if original_rows <= 0:
        raise ValueError(
            f"Empty OHLCV file: {source.path}"
        )

    missing = [
        column
        for column in REQUIRED_OHLCV_COLUMNS
        if column not in frame.columns
    ]

    if missing:
        raise ValueError(
            f"Missing OHLCV columns: "
            f"{source.path}: {missing}"
        )

    working = frame.loc[
        :,
        list(REQUIRED_OHLCV_COLUMNS),
    ].copy()

    # Numeric Unix epochs must be interpreted using their actual
    # unit. Pandas may otherwise interpret millisecond values as
    # microseconds and silently move dates into January 1970.
    raw_timestamp = working["timestamp"].astype("string").str.strip()
    numeric_timestamp = pd.to_numeric(raw_timestamp, errors="coerce")
    numeric_mask = numeric_timestamp.notna() & raw_timestamp.ne("")
    parsed_timestamp = pd.Series(pd.NaT, index=working.index, dtype="datetime64[ns, UTC]")

    if numeric_mask.any():
        values = numeric_timestamp.loc[numeric_mask]
        magnitudes = values.abs()
        # Unix seconds / milliseconds / microseconds / nanoseconds.
        for unit, mask in (
            ("s", magnitudes < 1e11),
            ("ms", (magnitudes >= 1e11) & (magnitudes < 1e14)),
            ("us", (magnitudes >= 1e14) & (magnitudes < 1e17)),
            ("ns", magnitudes >= 1e17),
        ):
            if mask.any():
                parsed_timestamp.loc[values.index[mask]] = pd.to_datetime(
                    values.loc[mask], unit=unit, errors="coerce", utc=True,
                )

    if (~numeric_mask).any():
        parsed_timestamp.loc[~numeric_mask] = pd.to_datetime(
            raw_timestamp.loc[~numeric_mask], errors="coerce", utc=True,
            format="mixed",
        )

    working["timestamp"] = parsed_timestamp


    numeric_columns = (
        "open",
        "high",
        "low",
        "close",
        "volume",
    )

    for column in numeric_columns:
        working[column] = pd.to_numeric(
            working[column],
            errors="coerce",
        )

    working = working.dropna(
        subset=list(
            REQUIRED_OHLCV_COLUMNS
        )
    )

    working = working.loc[
        (working["open"] > 0)
        & (working["high"] > 0)
        & (working["low"] > 0)
        & (working["close"] > 0)
        & (working["volume"] >= 0)
    ].copy()

    working = working.loc[
        (working["high"] >= working["low"])
        & (
            working["high"]
            >= working[
                ["open", "close"]
            ].max(axis=1)
        )
        & (
            working["low"]
            <= working[
                ["open", "close"]
            ].min(axis=1)
        )
    ].copy()

    working = working.sort_values(
        "timestamp"
    )

    working = working.drop_duplicates(
        subset=["timestamp"],
        keep="last",
    )

    working = working.reset_index(
        drop=True
    )

    valid_rows = len(working)

    if valid_rows < MIN_REQUIRED_ROWS:
        raise ValueError(
            f"No valid OHLCV rows: "
            f"{source.path}"
        )

    #
    # SHORT HISTORY POLICY
    #
    # A newly listed market is not an error merely because
    # 112 / 224 / 448 candles do not yet exist.
    #
    # No missing history is fabricated.
    # Rolling features remain NaN naturally.
    #

    if valid_rows < 448:
        safe_print(
            "[SHORT_HISTORY]",
            source.timeframe,
            source.market,
            f"valid_rows={valid_rows}",
            "longest_research_window=448",
        )

    return LoadedOHLCV(
        source=source,
        frame=working,
        original_rows=original_rows,
        valid_rows=valid_rows,
    )


# ============================================================
# 13. SAFE ROLLING HELPERS
# ============================================================

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


# ============================================================
# 14. BASE FEATURES
# ============================================================

def build_base_features(    frame: pd.DataFrame,
) -> pd.DataFrame:

    df = frame.copy()

    open_ = df["open"]
    high = df["high"]
    low = df["low"]
    close = df["close"]
    volume = df["volume"]

    candle_range = (
        high - low
    ).clip(lower=0.0)

    body = close - open_

    body_abs = body.abs()

    upper_wick = (
        high
        - pd.concat(
            [open_, close],
            axis=1,
        ).max(axis=1)
    ).clip(lower=0.0)

    lower_wick = (
        pd.concat(
            [open_, close],
            axis=1,
        ).min(axis=1)
        - low
    ).clip(lower=0.0)

    safe_range = candle_range.replace(
        0.0,
        np.nan,
    )

    df["candle_range"] = (
        candle_range
    )

    df["body"] = body

    df["body_abs"] = body_abs

    df["upper_wick"] = (
        upper_wick
    )

    df["lower_wick"] = (
        lower_wick
    )

    df["is_bullish"] = (
        close > open_
    )

    df["is_bearish"] = (
        close < open_
    )

    df["is_doji"] = (
        close == open_
    )

    df["body_pct_of_range"] = (
        body_abs
        / safe_range
        * 100.0
    )

    df["upper_wick_pct_of_range"] = (
        upper_wick
        / safe_range
        * 100.0
    )

    df["lower_wick_pct_of_range"] = (
        lower_wick
        / safe_range
        * 100.0
    )

    df["close_location_pct"] = (
        (close - low)
        / safe_range
        * 100.0
    )

    df["open_location_pct"] = (
        (open_ - low)
        / safe_range
        * 100.0
    )

    df["range_pct_of_open"] = (
        candle_range
        / open_.replace(
            0.0,
            np.nan,
        )
        * 100.0
    )

    df["body_pct_of_open"] = (
        body
        / open_.replace(
            0.0,
            np.nan,
        )
        * 100.0
    )

    df["abs_body_pct_of_open"] = (
        body_abs
        / open_.replace(
            0.0,
            np.nan,
        )
        * 100.0
    )

    df["close_change_pct"] = (
        close.pct_change(
            periods=1,
            fill_method=None,
        )
        * 100.0
    )

    return df


# ============================================================
# 15. MOVING AVERAGE FEATURES
# ============================================================

def add_moving_average_features(
    frame: pd.DataFrame,
) -> pd.DataFrame:

    df = frame.copy()

    close = df["close"]

    for window in MA_WINDOWS:

        ma = rolling_mean(
            close,
            window,
        )

        df[f"ma_{window}"] = ma

        df[
            f"close_vs_ma_{window}_pct"
        ] = (
            (
                close
                / ma.replace(
                    0.0,
                    np.nan,
                )
            )
            - 1.0
        ) * 100.0

    df["ma_112_below_224"] = (
        df["ma_112"]
        < df["ma_224"]
    )

    df["ma_224_below_448"] = (
        df["ma_224"]
        < df["ma_448"]
    )

    df["ma_reverse_alignment_112_224_448"] = (
        df["ma_112_below_224"]
        & df["ma_224_below_448"]
    )

    long_ma_max = pd.concat(
        [
            df["ma_112"],
            df["ma_224"],
            df["ma_448"],
        ],
        axis=1,
    ).max(axis=1)

    long_ma_min = pd.concat(
        [
            df["ma_112"],
            df["ma_224"],
            df["ma_448"],
        ],
        axis=1,
    ).min(axis=1)

    long_ma_mid = (
        (
            df["ma_112"]
            + df["ma_224"]
            + df["ma_448"]
        )
        / 3.0
    )

    df["long_ma_spread_pct"] = (
        (
            long_ma_max
            - long_ma_min
        )
        / long_ma_mid.replace(
            0.0,
            np.nan,
        )
        * 100.0
    )

    return df


# ============================================================
# 16. VOLUME FEATURES
# ============================================================

def add_volume_features(
    frame: pd.DataFrame,
) -> pd.DataFrame:

    df = frame.copy()

    volume = df["volume"]

    #
    # Historical baseline excludes the current candle.
    #
    # shift(1) is allowed because it only accesses past data.
    #

    past_volume = volume.shift(1)

    for window in VOLUME_WINDOWS:

        mean = rolling_mean(
            past_volume,
            window,
        )

        std = rolling_std(
            past_volume,
            window,
        )

        df[
            f"volume_mean_prev_{window}"
        ] = mean

        df[
            f"volume_ratio_prev_{window}"
        ] = (
            volume
            / mean.replace(
                0.0,
                np.nan,
            )
        )

        df[
            f"volume_zscore_prev_{window}"
        ] = (
            volume - mean
        ) / std.replace(
            0.0,
            np.nan,
        )

    return df


# ============================================================
# 17. RANGE FEATURES
# ============================================================

def add_range_features(
    frame: pd.DataFrame,
) -> pd.DataFrame:

    df = frame.copy()

    candle_range = df[
        "candle_range"
    ]

    past_range = candle_range.shift(1)

    for window in RANGE_WINDOWS:

        mean = rolling_mean(
            past_range,
            window,
        )

        df[
            f"range_mean_prev_{window}"
        ] = mean

        df[
            f"range_ratio_prev_{window}"
        ] = (
            candle_range
            / mean.replace(
                0.0,
                np.nan,
            )
        )

    return df


# ============================================================
# 18. BASE / COMPRESSION CONTEXT
# ============================================================

def add_base_context_features(
    frame: pd.DataFrame,
) -> pd.DataFrame:

    df = frame.copy()

    close = df["close"]

    #
    # Previous candles only.
    #

    past_close = close.shift(1)

    for window in COMPRESSION_WINDOWS:

        rolling_high = rolling_max(
            past_close,
            window,
        )

        rolling_low = rolling_min(
            past_close,
            window,
        )

        rolling_mid = (
            rolling_high
            + rolling_low
        ) / 2.0

        df[
            f"base_high_prev_{window}"
        ] = rolling_high

        df[
            f"base_low_prev_{window}"
        ] = rolling_low

        df[
            f"base_width_prev_{window}_pct"
        ] = (
            (
                rolling_high
                - rolling_low
            )
            / rolling_mid.replace(
                0.0,
                np.nan,
            )
            * 100.0
        )

        mean = rolling_mean(
            past_close,
            window,
        )

        std = rolling_std(
            past_close,
            window,
        )

        df[
            f"close_cv_prev_{window}_pct"
        ] = (
            std
            / mean.replace(
                0.0,
                np.nan,
            )
            * 100.0
        )

    return df


# ============================================================
# 19. DIMENSION SCORE:
#     VOLUME EXPANSION
# ============================================================

def score_volume_expansion(
    df: pd.DataFrame,
) -> pd.Series:

    score = pd.Series(
        0.0,
        index=df.index,
        dtype=float,
    )

    ratio20 = df[
        "volume_ratio_prev_20"
    ]

    ratio60 = df[
        "volume_ratio_prev_60"
    ]

    score += np.where(
        ratio20 >= 1.5,
        5.0,
        0.0,
    )

    score += np.where(
        ratio20 >= 2.0,
        5.0,
        0.0,
    )

    score += np.where(
        ratio20 >= 3.0,
        5.0,
        0.0,
    )

    score += np.where(
        ratio60 >= 2.0,
        5.0,
        0.0,
    )

    score += np.where(
        ratio60 >= 3.0,
        5.0,
        0.0,
    )

    return score.clip(
        lower=0.0,
        upper=25.0,
    )


# ============================================================
# 20. DIMENSION SCORE:
#     BODY STRENGTH
# ============================================================

def score_body_strength(
    df: pd.DataFrame,
) -> pd.Series:

    score = pd.Series(
        0.0,
        index=df.index,
        dtype=float,
    )

    bullish = df[
        "is_bullish"
    ].fillna(False)

    body_ratio = df[
        "body_pct_of_range"
    ]

    body_open = df[
        "body_pct_of_open"
    ]

    score += np.where(
        bullish,
        5.0,
        0.0,
    )

    score += np.where(
        bullish
        & (body_ratio >= 40.0),
        5.0,
        0.0,
    )

    score += np.where(
        bullish
        & (body_ratio >= 60.0),
        5.0,
        0.0,
    )

    score += np.where(
        bullish
        & (body_open >= 2.0),
        5.0,
        0.0,
    )

    return score.clip(
        lower=0.0,
        upper=20.0,
    )


# ============================================================
# 21. DIMENSION SCORE:
#     CLOSE LOCATION
# ============================================================

def score_close_location(
    df: pd.DataFrame,
) -> pd.Series:

    score = pd.Series(
        0.0,
        index=df.index,
        dtype=float,
    )

    close_location = df[
        "close_location_pct"
    ]

    score += np.where(
        close_location >= 60.0,
        5.0,
        0.0,
    )

    score += np.where(
        close_location >= 75.0,
        5.0,
        0.0,
    )

    score += np.where(
        close_location >= 90.0,
        5.0,
        0.0,
    )

    return score.clip(
        lower=0.0,
        upper=15.0,
    )


# ============================================================
# 22. DIMENSION SCORE:
#     WICK STRUCTURE
# ============================================================

def score_wick_structure(
    df: pd.DataFrame,
) -> pd.Series:

    score = pd.Series(
        0.0,
        index=df.index,
        dtype=float,
    )

    upper = df[
        "upper_wick_pct_of_range"
    ]

    lower = df[
        "lower_wick_pct_of_range"
    ]

    body = df[
        "body_pct_of_range"
    ]

    score += np.where(
        upper <= 30.0,
        4.0,
        0.0,
    )

    score += np.where(
        upper <= 15.0,
        3.0,
        0.0,
    )

    score += np.where(
        (body >= 40.0)
        & (lower <= 35.0),
        3.0,
        0.0,
    )

    return score.clip(
        lower=0.0,
        upper=10.0,
    )


# ============================================================
# 23. DIMENSION SCORE:
#     RANGE EXPANSION
# ============================================================

def score_range_expansion(
    df: pd.DataFrame,
) -> pd.Series:

    score = pd.Series(
        0.0,
        index=df.index,
        dtype=float,
    )

    ratio20 = df[
        "range_ratio_prev_20"
    ]

    ratio60 = df[
        "range_ratio_prev_60"
    ]

    score += np.where(
        ratio20 >= 1.25,
        5.0,
        0.0,
    )

    score += np.where(
        ratio20 >= 1.75,
        5.0,
        0.0,
    )

    score += np.where(
        ratio60 >= 1.50,
        5.0,
        0.0,
    )

    return score.clip(
        lower=0.0,
        upper=15.0,
    )


# ============================================================
# 24. DIMENSION SCORE:
#     BASE CONTEXT
# ============================================================

def score_base_context(
    df: pd.DataFrame,
) -> pd.Series:

    score = pd.Series(
        0.0,
        index=df.index,
        dtype=float,
    )

    width20 = df[
        "base_width_prev_20_pct"
    ]

    cv20 = df[
        "close_cv_prev_20_pct"
    ]

    reverse = df[
        "ma_reverse_alignment_112_224_448"
    ].fillna(False)

    below448 = (
        df["close"]
        <= df["ma_448"]
    ).fillna(False)

    score += np.where(
        width20 <= 15.0,
        4.0,
        0.0,
    )

    score += np.where(
        cv20 <= 5.0,
        4.0,
        0.0,
    )

    score += np.where(
        reverse,
        4.0,
        0.0,
    )

    score += np.where(
        below448,
        3.0,
        0.0,
    )

    return score.clip(
        lower=0.0,
        upper=15.0,
    )


# ============================================================
# 25. BUILD RESEARCH FEATURES
# ============================================================

def build_research_features(
    loaded: LoadedOHLCV,
) -> pd.DataFrame:

    df = loaded.frame.copy()

    df = build_base_features(
        df
    )

    df = add_moving_average_features(
        df
    )

    df = add_volume_features(
        df
    )

    df = add_range_features(
        df
    )

    df = add_base_context_features(
        df
    )

    df[
        "score_volume_expansion"
    ] = score_volume_expansion(
        df
    )

    df[
        "score_candle_body_strength"
    ] = score_body_strength(
        df
    )

    df[
        "score_close_location"
    ] = score_close_location(
        df
    )

    df[
        "score_wick_structure"
    ] = score_wick_structure(
        df
    )

    df[
        "score_range_expansion"
    ] = score_range_expansion(
        df
    )

    df[
        "score_base_context"
    ] = score_base_context(
        df
    )

    score_columns = [
        "score_volume_expansion",
        "score_candle_body_strength",
        "score_close_location",
        "score_wick_structure",
        "score_range_expansion",
        "score_base_context",
    ]

    df[
        "accumulation_candle_research_score"
    ] = (
        df[
            score_columns
        ]
        .sum(axis=1)
        .clip(
            lower=0.0,
            upper=MAX_RESEARCH_SCORE,
        )
    )

    #
    # Eligibility:
    #
    # A candidate needs enough past volume history to make
    # the basic volume-expansion hypothesis meaningful.
    #
    # Long MA 112/224/448 history is NOT mandatory because
    # newly listed markets must remain researchable.
    #

    df[
        "accumulation_candle_eligible"
    ] = (
        df[
            "volume_mean_prev_20"
        ].notna()
        & df[
            "range_mean_prev_20"
        ].notna()
    )
        df[
        "accumulation_candle_candidate"
    ] = (
        df[
            "accumulation_candle_eligible"
        ]
        & (
            df[
                "accumulation_candle_research_score"
            ]
            >= CANDIDATE_SCORE_THRESHOLD
        )
    )

    df[
        "accumulation_candle_strong_candidate"
    ] = (
        df[
            "accumulation_candle_candidate"
        ]
        & (
            df[
                "accumulation_candle_research_score"
            ]
            >= STRONG_CANDIDATE_SCORE_THRESHOLD
        )
    )

    #
    # Research stage is descriptive only.
    #
    # It is NOT a trading signal.
    #

    stage = pd.Series(
        "NONE",
        index=df.index,
        dtype="object",
    )

    stage.loc[
        df[
            "accumulation_candle_eligible"
        ]
        & (
            df[
                "accumulation_candle_research_score"
            ]
            >= 40.0
        )
    ] = "OBSERVE"

    stage.loc[
        df[
            "accumulation_candle_candidate"
        ]
    ] = "CANDIDATE"

    stage.loc[
        df[
            "accumulation_candle_strong_candidate"
        ]
    ] = "STRONG"

    df[
        "accumulation_candle_research_stage"
    ] = stage

    df.insert(
        0,
        "market",
        loaded.source.market,
    )

    df.insert(
        0,
        "timeframe",
        loaded.source.timeframe,
    )

    return df


# ============================================================
# 26. DETAIL OUTPUT COLUMNS
# ============================================================

DETAIL_COLUMNS = (
    "timeframe",
    "market",
    "timestamp",

    "open",
    "high",
    "low",
    "close",
    "volume",

    "is_bullish",
    "is_bearish",
    "is_doji",

    "candle_range",
    "body",
    "body_abs",

    "upper_wick",
    "lower_wick",

    "body_pct_of_range",
    "upper_wick_pct_of_range",
    "lower_wick_pct_of_range",
    "close_location_pct",

    "range_pct_of_open",
    "body_pct_of_open",
    "close_change_pct",

    "volume_mean_prev_5",
    "volume_mean_prev_10",
    "volume_mean_prev_20",
    "volume_mean_prev_60",

    "volume_ratio_prev_5",
    "volume_ratio_prev_10",
    "volume_ratio_prev_20",
    "volume_ratio_prev_60",

    "volume_zscore_prev_20",
    "volume_zscore_prev_60",

    "range_mean_prev_20",
    "range_mean_prev_60",

    "range_ratio_prev_20",
    "range_ratio_prev_60",

    "ma_5",
    "ma_20",
    "ma_60",
    "ma_112",
    "ma_224",
    "ma_448",

    "close_vs_ma_112_pct",
    "close_vs_ma_224_pct",
    "close_vs_ma_448_pct",

    "ma_112_below_224",
    "ma_224_below_448",
    "ma_reverse_alignment_112_224_448",
    "long_ma_spread_pct",

    "base_width_prev_20_pct",
    "base_width_prev_60_pct",
    "close_cv_prev_20_pct",
    "close_cv_prev_60_pct",

    "score_volume_expansion",
    "score_candle_body_strength",
    "score_close_location",
    "score_wick_structure",
    "score_range_expansion",
    "score_base_context",

    "accumulation_candle_research_score",
    "accumulation_candle_eligible",
    "accumulation_candle_candidate",
    "accumulation_candle_strong_candidate",
    "accumulation_candle_research_stage",
)


# ============================================================
# 27. SUMMARY
# ============================================================

def build_summary_row(
    loaded: LoadedOHLCV,
    detail: pd.DataFrame,
) -> Dict[str, Any]:

    eligible = detail[
        "accumulation_candle_eligible"
    ].fillna(False)

    candidate = detail[
        "accumulation_candle_candidate"
    ].fillna(False)

    strong = detail[
        "accumulation_candle_strong_candidate"
    ].fillna(False)

    scores = pd.to_numeric(
        detail[
            "accumulation_candle_research_score"
        ],
        errors="coerce",
    )

    valid_scores = scores[
        np.isfinite(scores)
    ]

    rows = len(detail)

    eligible_rows = int(
        eligible.sum()
    )

    candidate_rows = int(
        candidate.sum()
    )

    strong_rows = int(
        strong.sum()
    )

    if eligible_rows > 0:
        candidate_rate = (
            candidate_rows
            / eligible_rows
            * 100.0
        )

        strong_rate = (
            strong_rows
            / eligible_rows
            * 100.0
        )

    else:
        candidate_rate = 0.0
        strong_rate = 0.0

    def quantile(
        q: float,
    ) -> float:

        if valid_scores.empty:
            return 0.0

        return finite_float(
            valid_scores.quantile(q)
        )

    return {
        "timeframe": loaded.source.timeframe,
        "market": loaded.source.market,

        "rows": rows,
        "eligible_rows": eligible_rows,
        "candidate_rows": candidate_rows,
        "strong_candidate_rows": strong_rows,

        "observe_rows": int(
            (
                detail[
                    "accumulation_candle_research_stage"
                ]
                == "OBSERVE"
            ).sum()
        ),

        "candidate_stage_rows": int(
            (
                detail[
                    "accumulation_candle_research_stage"
                ]
                == "CANDIDATE"
            ).sum()
        ),

        "strong_stage_rows": int(
            (
                detail[
                    "accumulation_candle_research_stage"
                ]
                == "STRONG"
            ).sum()
        ),

        "candidate_rate_pct": round(
            candidate_rate,
            6,
        ),

        "strong_candidate_rate_pct": round(
            strong_rate,
            6,
        ),

        "score_mean": round(
            finite_float(
                valid_scores.mean()
                if not valid_scores.empty
                else 0.0
            ),
            6,
        ),

        "score_median": round(
            finite_float(
                valid_scores.median()
                if not valid_scores.empty
                else 0.0
            ),
            6,
        ),

        "score_p90": round(
            quantile(0.90),
            6,
        ),

        "score_p95": round(
            quantile(0.95),
            6,
        ),

        "score_max": round(
            finite_float(
                valid_scores.max()
                if not valid_scores.empty
                else 0.0
            ),
            6,
        ),

        "status": "PASS",
    }


# ============================================================
# 28. MANIFEST
# ============================================================

def build_manifest_row(
    loaded: LoadedOHLCV,
    sha256_after: str,
) -> Dict[str, Any]:

    unchanged = (
        loaded.source.sha256_before
        == sha256_after
    )

    return {
        "timeframe": (
            loaded.source.timeframe
        ),
        "market": (
            loaded.source.market
        ),
        "relative_path": (
            loaded.source.relative_path
        ),
        "size_bytes": (
            loaded.source.size_bytes
        ),
        "sha256_before": (
            loaded.source.sha256_before
        ),
        "sha256_after": (
            sha256_after
        ),
        "sha256_unchanged": (
            unchanged
        ),
        "original_rows": (
            loaded.original_rows
        ),
        "valid_rows": (
            loaded.valid_rows
        ),
        "status": (
            "PASS"
            if unchanged
            else "FAIL"
        ),
        "error": (
            ""
            if unchanged
            else "SOURCE_SHA256_CHANGED"
        ),
    }


# ============================================================
# 29. CONTRACT
# ============================================================

def build_contract() -> Dict[str, Any]:

    return {
        "program": PROGRAM_NAME,
        "version": VERSION,
        "stage": STAGE,

        "pattern_id": PATTERN_ID,
        "pattern_name_ko": PATTERN_NAME_KO,

        "final_formula_defined": (
            FINAL_FORMULA_DEFINED
        ),

        "research_notice": (
            "Candidate scores are research hypotheses only. "
            "They do not prove actual accumulation."
        ),

        "contract": {
            "ohlcv_read_only": (
                OHLCV_READ_ONLY
            ),
            "current_candle_allowed": (
                CURRENT_CANDLE_ALLOWED
            ),
            "past_candles_allowed": (
                PAST_CANDLES_ALLOWED
            ),
            "future_candles_allowed": (
                FUTURE_CANDLES_ALLOWED
            ),
            "negative_shift_allowed": (
                NEGATIVE_SHIFT_ALLOWED
            ),
            "centered_rolling_allowed": (
                CENTERED_ROLLING_ALLOWED
            ),
            "future_labels_generated": (
                FUTURE_LABELS_GENERATED
            ),
            "future_returns_generated": (
                FUTURE_RETURNS_GENERATED
            ),
            "future_max_generated": (
                FUTURE_MAX_GENERATED
            ),
            "future_min_generated": (
                FUTURE_MIN_GENERATED
            ),
            "thirty_percent_label_generated": (
                THIRTY_PERCENT_LABEL_GENERATED
            ),
            "backtest_generated": (
                BACKTEST_GENERATED
            ),
            "prediction_generated": (
                PREDICTION_GENERATED
            ),
            "trading_signal_generated": (
                TRADING_SIGNAL_GENERATED
            ),
        },

        "research_dimensions": list(
            RESEARCH_DIMENSIONS
        ),

        "maximum_research_score": (
            MAX_RESEARCH_SCORE
        ),

        "candidate_score_threshold": (
            CANDIDATE_SCORE_THRESHOLD
        ),

        "strong_candidate_score_threshold": (
            STRONG_CANDIDATE_SCORE_THRESHOLD
        ),

        "moving_average_windows": list(
            MA_WINDOWS
        ),

        "important_long_moving_averages": [
            112,
            224,
            448,
        ],
    }


# ============================================================
# 30. README
# ============================================================

def build_readme() -> str:

    return f"""Upbit Surge Monitor
{PROGRAM_NAME}
{VERSION}

Stage:
{STAGE}

Pattern:
{PATTERN_NAME_KO}

Purpose
-------
This output is an independent quantitative research dataset
for accumulation-candle hypotheses.

It does NOT prove actual institutional accumulation.

Research dimensions
-------------------
1. VOLUME_EXPANSION
2. CANDLE_BODY_STRENGTH
3. CLOSE_LOCATION
4. WICK_STRUCTURE
5. RANGE_EXPANSION
6. BASE_CONTEXT

Long moving averages
--------------------
112
224
448

Safety
------
Production OHLCV: READ ONLY

Future candles used:
False

Negative shift used:
False

Centered rolling used:
False

Future labels generated:
False

Future returns generated:
False

30 percent future label generated:
False

Prediction executed:
False

Trading executed:
False

Final accumulation-candle formula defined:
False

Candidate threshold:
{CANDIDATE_SCORE_THRESHOLD}

Strong candidate threshold:
{STRONG_CANDIDATE_SCORE_THRESHOLD}

Important
---------
Candidate scores are research hypotheses.

The score must later be validated against historical outcomes
before any final accumulation-candle formula is accepted.
"""


# ============================================================
# 31. CHECKPOINT VALIDATION
# ============================================================

def checkpoint_entry_valid(
    source: SourceFile,
    entry: Any,
) -> bool:

    if not isinstance(
        entry,
        dict,
    ):
        return False

    if entry.get("status") != "PASS":
        return False

    if (
        entry.get("source_sha256")
        != source.sha256_before
    ):
        return False

    if (
        entry.get("timeframe")
        != source.timeframe
    ):
        return False

    if (
        entry.get("market")
        != source.market
    ):
        return False

    return True


# ============================================================
# 32. RESEARCH ONE SOURCE
# ============================================================

def research_source(
    source: SourceFile,
) -> Tuple[
    pd.DataFrame,
    Dict[str, Any],
    Dict[str, Any],
]:

    loaded = load_ohlcv(
        source
    )

    detail = build_research_features(
        loaded
    )

    detail = detail.loc[
        :,
        [
            column
            for column in DETAIL_COLUMNS
            if column in detail.columns
        ],
    ].copy()

    summary = build_summary_row(
        loaded,
        detail,
    )

    sha256_after = sha256_file(
        source.path
    )

    manifest = build_manifest_row(
        loaded,
        sha256_after,
    )

    if not manifest[
        "sha256_unchanged"
    ]:
        raise RuntimeError(
            "Production OHLCV changed during research: "
            f"{source.path}"
        )

    return (
        detail,
        summary,
        manifest,
    )


# ============================================================
# 33. RESULT BUILDING
# ============================================================

def build_result(
    *,
    timeframes: Sequence[str],
    source_count: int,
    summary_frame: pd.DataFrame,
    detail_frame: pd.DataFrame,
    manifest_frame: pd.DataFrame,
) -> Dict[str, Any]:

    files_error = int(
        (
            manifest_frame["status"]
            != "PASS"
        ).sum()
    )

    candidate_rows = int(
        detail_frame[
            "accumulation_candle_candidate"
        ]
        .fillna(False)
        .sum()
    )

    strong_rows = int(
        detail_frame[
            "accumulation_candle_strong_candidate"
        ]
        .fillna(False)
        .sum()
    )

    eligible_rows = int(
        detail_frame[
            "accumulation_candle_eligible"
        ]
        .fillna(False)
        .sum()
    )

    source_sha256_unchanged = bool(
        (
            manifest_frame[
                "sha256_unchanged"
            ]
            .fillna(False)
            .astype(bool)
        ).all()
    )

    status = (
        "PASS"
        if (
            files_error == 0
            and source_sha256_unchanged
            and len(detail_frame) > 0
        )
        else "FAIL"
    )

    return {
        "program": PROGRAM_NAME,
        "version": VERSION,
        "stage": STAGE,
        "status": status,

        "generated_at_utc": (
            utc_now_iso()
        ),

        "pattern": {
            "id": PATTERN_ID,
            "name_ko": PATTERN_NAME_KO,
            "independent_research": True,
            "final_formula_defined": (
                FINAL_FORMULA_DEFINED
            ),
        },

        "timeframes": list(
            timeframes
        ),

        "processing": {
            "files_discovered": (
                source_count
            ),
            "files_processed": (
                len(manifest_frame)
            ),
            "files_error": (
                files_error
            ),
        },

        "statistics": {
            "files_processed": (
                len(manifest_frame)
            ),
            "total_rows": (
                len(detail_frame)
            ),
            "eligible_rows": (
                eligible_rows
            ),
            "candidate_rows": (
                candidate_rows
            ),
            "strong_candidate_rows": (
                strong_rows
            ),
            "summary_rows": (
                len(summary_frame)
            ),
        },

        "future_information": {
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
            "source_sha256_unchanged": (
                source_sha256_unchanged
            ),
            "prediction_executed": False,
            "trading_executed": False,
        },

        "research": {
            "maximum_score": (
                MAX_RESEARCH_SCORE
            ),
            "candidate_threshold": (
                CANDIDATE_SCORE_THRESHOLD
            ),
            "strong_candidate_threshold": (
                STRONG_CANDIDATE_SCORE_THRESHOLD
            ),
            "long_moving_averages": [
                112,
                224,
                448,
            ],
        },
    }


# ============================================================
# 34. CLI
# ============================================================

def parse_args(
    argv: Optional[
        Sequence[str]
    ] = None,
) -> argparse.Namespace:

    parser = argparse.ArgumentParser(
        description=(
            "Independent accumulation-candle "            "quantitative research."
        )
    )

    parser.add_argument(
        "--project-root",
        required=True,
        help=(
            "Upbit Surge Monitor "
            "production project root."
        ),
    )

    parser.add_argument(
        "--timeframes",
        nargs="+",
        default=list(
            DEFAULT_TIMEFRAMES
        ),
        choices=[
            "h1",
            "h4",
            "d1",
        ],
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help=(
            "Reprocess all sources even when "
            "checkpoint entries are valid."
        ),
    )

    return parser.parse_args(
        argv
    )


# ============================================================
# 35. MAIN RESEARCH
# ============================================================

def run_research(
    args: argparse.Namespace,
) -> int:

    print_header(
        "ACCUMULATION CANDLE "
        "INDEPENDENT RESEARCH"
    )

    safe_print(
        f"Program : {PROGRAM_NAME}"
    )

    safe_print(
        f"Version : {VERSION}"
    )

    safe_print(
        f"Stage   : {STAGE}"
    )

    safe_print(
        ""
    )

    project_root = Path(
        args.project_root
    ).expanduser().resolve()

    if not project_root.exists():
        raise FileNotFoundError(
            f"Project root missing: "
            f"{project_root}"
        )

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

    ensure_directory(
        report_dir
    )

    ensure_directory(
        validation_dir
    )

    result_path = (
        report_dir
        / RESULT_FILENAME
    )

    summary_path = (
        report_dir
        / SUMMARY_FILENAME
    )

    detail_path = (
        report_dir
        / DETAIL_FILENAME
    )

    candidate_path = (
        report_dir
        / CANDIDATE_FILENAME
    )

    manifest_path = (
        report_dir
        / MANIFEST_FILENAME
    )

    contract_path = (
        report_dir
        / CONTRACT_FILENAME
    )

    readme_path = (
        report_dir
        / README_FILENAME
    )

    checkpoint_path = (
        validation_dir
        / CHECKPOINT_FILENAME
    )

    timeframes = tuple(
        args.timeframes
    )

    safe_print(
        f"Project root: {project_root}"
    )

    safe_print(
        "Timeframes:",
        ", ".join(timeframes),
    )

    safe_print(
        f"Force: {args.force}"
    )

    safe_print(
        ""
    )

    safe_print(
        "[SAFETY] Production OHLCV = READ ONLY"
    )

    safe_print(
        "[SAFETY] Future candles = BLOCKED"
    )

    safe_print(
        "[SAFETY] Negative shift = BLOCKED"
    )

    safe_print(
        "[SAFETY] Centered rolling = BLOCKED"
    )

    safe_print(
        "[SAFETY] Future labels = BLOCKED"
    )

    safe_print(
        "[SAFETY] Prediction = BLOCKED"
    )

    safe_print(
        "[SAFETY] Trading = BLOCKED"
    )

    safe_print(
        "[RESEARCH] Final formula = UNDEFINED"
    )

    print_header(
        "DISCOVER SOURCES"
    )

    sources = discover_sources(
        project_root,
        timeframes,
    )

    safe_print(
        f"Sources discovered: {len(sources)}"
    )

    checkpoint = load_checkpoint(
        checkpoint_path
    )

    #
    # Clean V001 writes deterministic complete outputs.
    #
    # Checkpoint is retained for audit and safe future resume
    # development, but --force reprocesses every source.
    #

    detail_frames: List[
        pd.DataFrame
    ] = []

    summary_rows: List[
        Dict[str, Any]
    ] = []

    manifest_rows: List[
        Dict[str, Any]
    ] = []

    total_sources = len(
        sources
    )

    for index, source in enumerate(
        sources,
        start=1,
    ):

        print_header(
            f"[{index}/{total_sources}] "
            f"{source.timeframe.upper()} "
            f"{source.market}"
        )

        safe_print(
            f"Source: {source.path}"
        )

        key = checkpoint_key(
            source
        )

        existing = (
            checkpoint[
                "completed_sources"
            ].get(key)
        )

        if (
            not args.force
            and checkpoint_entry_valid(
                source,
                existing,
            )
        ):
            safe_print(
                "[INFO] Valid checkpoint exists."
            )

            safe_print(
                "[INFO] Source is still reprocessed "
                "to build deterministic complete outputs."
            )

        try:

            (
                detail,
                summary,
                manifest,
            ) = research_source(
                source
            )

            detail_frames.append(
                detail
            )

            summary_rows.append(
                summary
            )

            manifest_rows.append(
                manifest
            )

            checkpoint[
                "completed_sources"
            ][key] = {
                "status": "PASS",
                "timeframe": (
                    source.timeframe
                ),
                "market": (
                    source.market
                ),
                "relative_path": (
                    source.relative_path
                ),
                "source_sha256": (
                    source.sha256_before
                ),
                "rows": int(
                    len(detail)
                ),
                "candidate_rows": int(
                    detail[
                        "accumulation_candle_candidate"
                    ]
                    .fillna(False)
                    .sum()
                ),
                "strong_candidate_rows": int(
                    detail[
                        "accumulation_candle_strong_candidate"
                    ]
                    .fillna(False)
                    .sum()
                ),
                "completed_at_utc": (
                    utc_now_iso()
                ),
            }

            save_checkpoint(
                checkpoint_path,
                checkpoint,
            )

            safe_print(
                "[PASS]",
                f"rows={len(detail)}",
                (
                    "candidates="
                    f"{summary['candidate_rows']}"
                ),
                (
                    "strong="
                    f"{summary['strong_candidate_rows']}"
                ),
                (
                    "max_score="
                    f"{summary['score_max']:.2f}"
                ),
            )

        except Exception as exc:

            checkpoint[
                "completed_sources"
            ][key] = {
                "status": "FAIL",
                "timeframe": (
                    source.timeframe
                ),
                "market": (
                    source.market
                ),
                "relative_path": (
                    source.relative_path
                ),
                "source_sha256": (
                    source.sha256_before
                ),
                "error": (
                    f"{type(exc).__name__}: "
                    f"{exc}"
                ),
                "completed_at_utc": (
                    utc_now_iso()
                ),
            }

            save_checkpoint(
                checkpoint_path,
                checkpoint,
            )

            safe_print(
                "[ERROR]",
                f"{type(exc).__name__}:",
                exc,
            )

            raise

    if not detail_frames:
        raise RuntimeError(
            "No research detail was generated."
        )

    print_header(
        "BUILD FINAL OUTPUTS"
    )

    detail_frame = pd.concat(
        detail_frames,
        ignore_index=True,
    )

    summary_frame = pd.DataFrame(
        summary_rows
    )

    manifest_frame = pd.DataFrame(
        manifest_rows
    )

    candidate_frame = detail_frame.loc[
        detail_frame[
            "accumulation_candle_candidate"
        ].fillna(False)
    ].copy()

    #
    # Stable ordering
    #

    detail_frame = (
        detail_frame
        .sort_values(
            [
                "timeframe",
                "market",
                "timestamp",
            ]
        )
        .reset_index(
            drop=True
        )
    )

    candidate_frame = (
        candidate_frame
        .sort_values(
            [
                "timeframe",
                "market",
                "timestamp",
            ]
        )
        .reset_index(
            drop=True
        )
    )

    summary_frame = (
        summary_frame
        .sort_values(
            [
                "timeframe",
                "market",
            ]
        )
        .reset_index(
            drop=True
        )
    )

    manifest_frame = (
        manifest_frame
        .sort_values(
            [
                "timeframe",
                "market",
            ]
        )
        .reset_index(
            drop=True
        )
    )

    #
    # Final source SHA256 verification
    #

    source_map = {
        (
            source.timeframe,
            source.market,
            source.relative_path,
        ): source
        for source in sources
    }

    for _, row in manifest_frame.iterrows():

        key = (
            row["timeframe"],
            row["market"],
            row["relative_path"],
        )

        source = source_map.get(
            key
        )

        if source is None:
            raise RuntimeError(
                f"Manifest source missing: {key}"
            )

        current_hash = sha256_file(
            source.path
        )

        if (
            current_hash
            != source.sha256_before
        ):
            raise RuntimeError(
                "Production OHLCV SHA256 changed: "
                f"{source.path}"
            )

    result = build_result(
        timeframes=timeframes,
        source_count=len(sources),
        summary_frame=summary_frame,
        detail_frame=detail_frame,
        manifest_frame=manifest_frame,
    )

    contract = build_contract()

    #
    # Output writes are ONLY under reports / validation.
    #
    # Source OHLCV is never written.
    #

    atomic_write_csv(
        summary_path,
        summary_frame,
    )

    atomic_write_csv(
        detail_path,
        detail_frame,
    )

    atomic_write_csv(
        candidate_path,
        candidate_frame,
    )

    atomic_write_csv(
        manifest_path,
        manifest_frame,
    )

    atomic_write_json(
        contract_path,
        contract,
    )

    atomic_write_json(
        result_path,
        result,
    )

    atomic_write_text(
        readme_path,
        build_readme(),
    )

    #
    # Final checkpoint verification
    #

    final_checkpoint = load_checkpoint(
        checkpoint_path
    )

    completed = final_checkpoint[
        "completed_sources"
    ]

    for source in sources:

        key = checkpoint_key(
            source
        )

        entry = completed.get(
            key
        )

        if not checkpoint_entry_valid(
            source,
            entry,
        ):
            raise RuntimeError(
                "Checkpoint integrity failed: "
                f"{key}"
            )

    #
    # Final gate
    #

    if result["status"] != "PASS":
        raise RuntimeError(
            "Final research result is not PASS."
        )

    if not result[
        "safety"
    ][
        "source_sha256_unchanged"
    ]:
        raise RuntimeError(
            "Source SHA256 safety gate failed."
        )

    print_header(
        "ACCUMULATION CANDLE RESEARCH PASS"
    )

    statistics = result[
        "statistics"
    ]

    safe_print(
        "[PASS]",
        PROGRAM_NAME,
        VERSION,
    )

    safe_print(
        "[PASS] Pattern:",
        PATTERN_NAME_KO,
    )

    safe_print(
        "[PASS] Production OHLCV unchanged"
    )

    safe_print(
        "[PASS] Source SHA256 unchanged"
    )

    safe_print(
        "[PASS] Future candles blocked"
    )

    safe_print(
        "[PASS] Negative shift blocked"
    )

    safe_print(
        "[PASS] Centered rolling blocked"
    )

    safe_print(
        "[PASS] Future labels blocked"
    )

    safe_print(
        "[PASS] Future returns blocked"
    )

    safe_print(
        "[PASS] Prediction blocked"
    )

    safe_print(
        "[PASS] Trading blocked"
    )

    safe_print(
        "[PASS] Final accumulation-candle "
        "formula remains undefined"
    )

    safe_print(
        ""
    )

    safe_print(
        "Files processed:",
        statistics[
            "files_processed"
        ],
    )

    safe_print(
        "Total rows:",
        statistics[
            "total_rows"
        ],
    )

    safe_print(
        "Eligible rows:",
        statistics[
            "eligible_rows"
        ],
    )

    safe_print(
        "Candidate rows:",
        statistics[
            "candidate_rows"
        ],
    )

    safe_print(
        "Strong candidate rows:",
        statistics[
            "strong_candidate_rows"
        ],
    )

    safe_print(
        ""
    )

    safe_print(
        "Result:",
        result_path,
    )

    safe_print(
        "Summary:",
        summary_path,
    )

    safe_print(
        "Detail:",
        detail_path,
    )

    safe_print(
        "Candidates:",
        candidate_path,
    )

    safe_print(
        "Manifest:",
        manifest_path,
    )

    safe_print(
        "Contract:",
        contract_path,
    )

    safe_print(
        "Checkpoint:",
        checkpoint_path,
    )

    safe_print(
        ""
    )

    safe_print(
        "[NEXT]"
    )

    safe_print(
        "Run research_accumulation_candle_test.yml."
    )

    safe_print(
        "Do NOT combine this with Pattern 256 "
        "until the independent research test passes."
    )

    return 0


# ============================================================
# 36. ENTRY POINT
# ============================================================

def main(
    argv: Optional[
        Sequence[str]
    ] = None,
) -> int:

    args = parse_args(
        argv
    )

    try:

        return run_research(
            args
        )

    except KeyboardInterrupt:

        safe_print("")
        safe_print(
            "ACCUMULATION CANDLE RESEARCH INTERRUPTED"
        )

        safe_print(
            "Checkpoint remains available."
        )

        return 130

    except Exception as exc:

        safe_print("")
        safe_print(
            "ACCUMULATION CANDLE RESEARCH FATAL ERROR"
        )

        safe_print(
            f"{type(exc).__name__}: {exc}"
        )

        safe_print("")
        safe_print(
            traceback.format_exc()
        )

        return 1


if __name__ == "__main__":
    raise SystemExit(
        main()
    )
