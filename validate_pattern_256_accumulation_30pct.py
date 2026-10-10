
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
validate_pattern_256_accumulation_30pct.py
Clean V001

Purpose
-------
Validate whether historical Pattern 256 / Accumulation Candle
signals were followed by a >=30% price increase.

Signal groups:
    PATTERN_256_ONLY
    ACCUMULATION_ONLY
    BOTH
    NEITHER

Timeframes:
    h1, h4, d1

Horizons:
    24, 72, 168 candles

Entry:
    Open of the next candle after signal candle.

Success:
    Future high >= next-candle open * 1.30.

Safety:
    - OHLCV source files are read-only.
    - Crosscheck source files are read-only.
    - No trading functionality.
    - No future information in signal construction.
    - Explicit Unix millisecond timestamp handling.
    - Checkpoint and resume with input SHA256 validation.
    - Atomic output writes.
    - No destructive repository operations.

Dependencies:
    Python 3.10+
    pandas >= 2.2
    numpy >= 2.0

Run:
    py validate_pattern_256_accumulation_30pct.py

Optional:
    py validate_pattern_256_accumulation_30pct.py --limit-markets 3
    py validate_pattern_256_accumulation_30pct.py --force
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import sys
import tempfile
import time
import traceback

from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd


PROGRAM_NAME = "validate_pattern_256_accumulation_30pct.py"
PROGRAM_VERSION = "Clean V001"

TIMEFRAMES = ("h1", "h4", "d1")
HORIZONS = (24, 72, 168)

TARGET_RETURN = 0.30

GROUPS = (
    "PATTERN_256_ONLY",
    "ACCUMULATION_ONLY",
    "BOTH",
    "NEITHER",
)

ROOT = Path(__file__).resolve().parent

OHLCV_ROOT = ROOT / "data" / "ohlcv"

CROSSCHECK_ROOT = (
    ROOT
    / "data"
    / "reports"
    / "pattern_256_accumulation_crosscheck"
)

CANDIDATES_FILE = (
    CROSSCHECK_ROOT
    / "crosscheck_candidates.csv"
)

OUTPUT_ROOT = (
    ROOT
    / "data"
    / "reports"
    / "pattern_256_accumulation_30pct"
)

DETAIL_FILE = OUTPUT_ROOT / "validation_detail.csv"
SUMMARY_FILE = OUTPUT_ROOT / "validation_summary.csv"
RESULT_FILE = OUTPUT_ROOT / "validation_result.json"
MANIFEST_FILE = OUTPUT_ROOT / "validation_manifest.json"

CHECKPOINT_ROOT = OUTPUT_ROOT / "checkpoints"

DETAIL_COLUMNS = [
    "timeframe",
    "market",
    "signal_timestamp",
    "classification",
    "pattern_256_score",
    "accumulation_score",
    "signal_open",
    "signal_close",
    "entry_timestamp",
    "entry_price",
    "horizon",
    "target_return_pct",
    "target_price",
    "status",
    "hit_30pct",
    "first_hit_bars",
    "first_hit_timestamp",
    "max_high",
    "max_return_pct",
    "min_low",
    "max_drawdown_pct",
    "observed_bars",
    "required_bars",
    "source_ohlcv_sha256",
]

SUMMARY_COLUMNS = [
    "timeframe",
    "classification",
    "horizon",
    "total_signals",
    "complete",
    "hit",
    "miss",
    "censored",
    "invalid",
    "hit_rate_pct",
    "average_max_return_pct",
    "median_max_return_pct",
    "average_first_hit_bars",
]

REQUIRED_OHLCV_COLUMNS = {
    "market",
    "timestamp",
    "open",
    "high",
    "low",
    "close",
}

REQUIRED_CANDIDATE_COLUMNS = {
    "timeframe",
    "market",
    "timestamp",
}

CSV_ENCODING = "utf-8-sig"


def log(message: str) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] {message}", flush=True)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)

            if not chunk:
                break

            digest.update(chunk)

    return digest.hexdigest()


def atomic_write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    fd, temporary_name = tempfile.mkstemp(
        prefix=path.name + ".",
        suffix=".tmp",
        dir=str(path.parent),
    )

    temporary_path = Path(temporary_name)

    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(
                data,
                handle,
                ensure_ascii=False,
                indent=2,
                allow_nan=False,
            )

        os.replace(temporary_path, path)

    finally:
        if temporary_path.exists():
            temporary_path.unlink()


def atomic_write_csv(
    path: Path,
    rows: list[dict],
    columns: list[str],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    fd, temporary_name = tempfile.mkstemp(
        prefix=path.name + ".",
        suffix=".tmp",
        dir=str(path.parent),
    )

    temporary_path = Path(temporary_name)

    try:
        with os.fdopen(
            fd,
            "w",
            encoding=CSV_ENCODING,
            newline="",
        ) as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=columns,
                extrasaction="ignore",
            )

            writer.writeheader()

            for row in rows:
                writer.writerow(row)

        os.replace(temporary_path, path)

    finally:
        if temporary_path.exists():
            temporary_path.unlink()


def read_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def normalize_timeframe(value: object) -> str:
    text = str(value).strip().lower()

    aliases = {
        "1h": "h1",
        "60m": "h1",
        "h1": "h1",
        "4h": "h4",
        "240m": "h4",
        "h4": "h4",
        "1d": "d1",
        "24h": "d1",
        "d1": "d1",
    }

    return aliases.get(text, text)


def normalize_market(value: object) -> str:
    return str(value).strip().upper()


def parse_timestamp(value: object) -> pd.Timestamp:
    """
    Parse ISO datetime or numeric Unix epoch.

    Current OHLCV collector stores milliseconds.
    Other supported numeric units are detected explicitly.
    """
    if value is None or pd.isna(value):
        return pd.NaT

    text = str(value).strip()

    if not text:
        return pd.NaT

    try:
        numeric = float(text)

        if not math.isfinite(numeric):
            return pd.NaT

        magnitude = abs(numeric)

        if magnitude >= 1e17:
            unit = "ns"
        elif magnitude >= 1e14:
            unit = "us"
        elif magnitude >= 1e11:
            unit = "ms"
        else:
            unit = "s"

        return pd.to_datetime(
            numeric,
            unit=unit,
            utc=True,
            errors="coerce",
        )

    except ValueError:
        return pd.to_datetime(
            text,
            utc=True,
            errors="coerce",
        )


def parse_bool(value: object) -> bool:
    if isinstance(value, (bool, np.bool_)):
        return bool(value)

    if value is None or pd.isna(value):
        return False

    text = str(value).strip().lower()

    return text in {
        "true",
        "1",
        "yes",
        "y",
        "t",
    }


def optional_float(value: object) -> float | None:
    try:
        number = float(value)

        if math.isfinite(number):
            return number

    except (TypeError, ValueError):
        pass

    return None


def read_csv_header(path: Path) -> list[str]:
    with path.open(
        "r",
        encoding=CSV_ENCODING,
        newline="",
    ) as handle:
        reader = csv.reader(handle)
        return next(reader)


def resolve_candidate_flags(
    row: dict,
    columns: set[str],
) -> tuple[bool, bool]:
    """
    Prefer explicit candidate flags.

    Classification strings are supported as a fallback.
    Scores alone are never treated as candidate signals.
    """
    pattern_names = (
        "pattern_256_candidate",
        "pattern256_candidate",
        "candidate_256",
        "is_pattern_256_candidate",
    )

    accumulation_names = (
        "accumulation_candle_candidate",
        "accumulation_candidate",
        "is_accumulation_candidate",
    )

    pattern_field = next(
        (name for name in pattern_names if name in columns),
        None,
    )

    accumulation_field = next(
        (name for name in accumulation_names if name in columns),
        None,
    )

    if pattern_field and accumulation_field:
        return (
            parse_bool(row.get(pattern_field)),
            parse_bool(row.get(accumulation_field)),
        )

    classification = str(
        row.get("classification", "")
    ).strip().upper()

    mapping = {
        "PATTERN_256_ONLY": (True, False),
        "256_ONLY": (True, False),
        "ACCUMULATION_ONLY": (False, True),
        "BOTH": (True, True),
        "BOTH_CANDIDATE": (True, True),
        "BOTH_STRONG": (True, True),
        "NEITHER": (False, False),
    }

    if classification in mapping:
        return mapping[classification]

    if "both_candidate" in columns:
        if parse_bool(row.get("both_candidate")):
            return True, True

    raise ValueError(
        "Cannot determine independent candidate flags. "
        "Required explicit candidate columns or a recognized "
        f"classification. Received: {classification!r}"
    )


def classify(
    pattern_candidate: bool,
    accumulation_candidate: bool,
) -> str:
    if pattern_candidate and accumulation_candidate:
        return "BOTH"

    if pattern_candidate:
        return "PATTERN_256_ONLY"

    if accumulation_candidate:
        return "ACCUMULATION_ONLY"

    return "NEITHER"


def find_ohlcv_files() -> dict[tuple[str, str], Path]:
    """
    Discover OHLCV files by reading CSV headers.

    This avoids assuming a particular collector filename.
    """
    if not OHLCV_ROOT.is_dir():
        raise FileNotFoundError(
            f"OHLCV directory not found: {OHLCV_ROOT}"
        )

    mapping = {}

    for timeframe in TIMEFRAMES:
        folder = OHLCV_ROOT / timeframe

        if not folder.is_dir():
            raise FileNotFoundError(
                f"Missing timeframe directory: {folder}"
            )

        paths = sorted(folder.rglob("*.csv"))

        log(
            f"[SCAN] {timeframe}: "
            f"{len(paths):,} OHLCV CSV files"
        )

        for path in paths:
            try:
                with path.open(
                    "r",
                    encoding=CSV_ENCODING,
                    newline="",
                ) as handle:
                    reader = csv.DictReader(handle)

                    if not reader.fieldnames:
                        continue

                    if not REQUIRED_OHLCV_COLUMNS.issubset(
                        set(reader.fieldnames)
                    ):
                        continue

                    first = next(reader, None)

                    if not first:
                        continue

                    market = normalize_market(
                        first.get("market")
                    )

                    if not market.startswith("KRW-"):
                        continue

                    key = (timeframe, market)

                    if key in mapping:
                        raise ValueError(
                            "Duplicate OHLCV source for "
                            f"{key}: {mapping[key]} and {path}"
                        )

                    mapping[key] = path

            except (OSError, UnicodeError, csv.Error) as exc:
                raise RuntimeError(
                    f"Failed to inspect OHLCV file: {path}"
                ) from exc

    return mapping


def load_ohlcv(
    path: Path,
    expected_market: str,
) -> pd.DataFrame:
    frame = pd.read_csv(
        path,
        encoding=CSV_ENCODING,
        low_memory=False,
    )

    missing = REQUIRED_OHLCV_COLUMNS - set(frame.columns)

    if missing:
        raise ValueError(
            f"OHLCV missing columns {sorted(missing)}: {path}"
        )

    frame["market"] = (
        frame["market"].astype(str).str.strip().str.upper()
    )

    if not frame["market"].eq(expected_market).all():
        raise ValueError(
            f"Market mismatch in OHLCV: {path}"
        )

    frame["parsed_timestamp"] = frame[
        "timestamp"
    ].map(parse_timestamp)

    if frame["parsed_timestamp"].isna().any():
        raise ValueError(
            f"Invalid timestamp in OHLCV: {path}"
        )

    for column in ("open", "high", "low", "close"):
        frame[column] = pd.to_numeric(
            frame[column],
            errors="coerce",
        )

    if frame[["open", "high", "low", "close"]].isna().any().any():
        raise ValueError(
            f"Invalid OHLC price in {path}"
        )

    if (frame[["open", "high", "low", "close"]] <= 0).any().any():
        raise ValueError(
            f"Non-positive OHLC price in {path}"
        )

    invalid_range = (
        (frame["high"] < frame["low"])
        | (frame["high"] < frame["open"])
        | (frame["high"] < frame["close"])
        | (frame["low"] > frame["open"])
        | (frame["low"] > frame["close"])
    )

    if invalid_range.any():
        raise ValueError(
            f"Invalid OHLC candle range in {path}"
        )

    frame = frame.sort_values(
        "parsed_timestamp",
        kind="stable",
    ).reset_index(drop=True)

    if frame["parsed_timestamp"].duplicated().any():
        raise ValueError(
            f"Duplicate OHLCV timestamps in {path}"
        )

    return frame


def load_candidates() -> dict[tuple[str, str], list[dict]]:
    if not CANDIDATES_FILE.is_file():
        raise FileNotFoundError(
            f"Crosscheck candidates not found: {CANDIDATES_FILE}"
        )

    columns = set(read_csv_header(CANDIDATES_FILE))

    missing = REQUIRED_CANDIDATE_COLUMNS - columns

    if missing:
        raise ValueError(
            "Crosscheck candidates missing columns: "
            f"{sorted(missing)}"
        )

    groups = defaultdict(list)

    total = 0
    seen = set()

    with CANDIDATES_FILE.open(
        "r",
        encoding=CSV_ENCODING,
        newline="",
    ) as handle:
        reader = csv.DictReader(handle)

        for row in reader:
            total += 1

            timeframe = normalize_timeframe(
                row["timeframe"]
            )

            market = normalize_market(
                row["market"]
            )

            if timeframe not in TIMEFRAMES:
                raise ValueError(
                    f"Unsupported timeframe: {timeframe}"
                )

            if not market.startswith("KRW-"):
                raise ValueError(
                    f"Unexpected market: {market}"
                )

            timestamp = parse_timestamp(
                row["timestamp"]
            )

            if pd.isna(timestamp):
                raise ValueError(
                    f"Invalid candidate timestamp at row {total}"
                )

            pattern_flag, accumulation_flag = (
                resolve_candidate_flags(row, columns)
            )

            classification = classify(
                pattern_flag,
                accumulation_flag,
            )

            key = (
                timeframe,
                market,
                timestamp.isoformat(),
            )

            if key in seen:
                raise ValueError(
                    f"Duplicate candidate signal: {key}"
                )

            seen.add(key)

            groups[(timeframe, market)].append({
                "timestamp": timestamp,
                "classification": classification,
                "pattern_256_score": row.get(
                    "pattern_256_score", ""
                ),
                "accumulation_score": row.get(
                    "accumulation_score", ""
                ),
            })

            if total % 100000 == 0:
                log(
                    f"[LOAD] candidates: {total:,}"
                )

    log(
        f"[PASS] candidates loaded: {total:,}, "
        f"market/timeframe groups: {len(groups):,}"
    )

    return dict(groups)


def validate_one_signal(
    signal: dict,
    frame: pd.DataFrame,
    timestamp_index: dict,
    timeframe: str,
    market: str,
    source_hash: str,
) -> list[dict]:
    timestamp = signal["timestamp"]

    if timestamp not in timestamp_index:
        raise ValueError(
            "Candidate timestamp not found in OHLCV: "
            f"{timeframe} {market} {timestamp}"
        )

    signal_index = timestamp_index[timestamp]

    signal_candle = frame.iloc[signal_index]

    entry_index = signal_index + 1

    base = {
        "timeframe": timeframe,
        "market": market,
        "signal_timestamp": timestamp.isoformat(),
        "classification": signal["classification"],
        "pattern_256_score": signal[
            "pattern_256_score"
        ],
        "accumulation_score": signal[
            "accumulation_score"
        ],
        "signal_open": float(signal_candle["open"]),
        "signal_close": float(signal_candle["close"]),
        "source_ohlcv_sha256": source_hash,
    }

    rows = []

    for horizon in HORIZONS:
        result = dict(base)

        result.update({
            "entry_timestamp": "",
            "entry_price": "",
            "horizon": horizon,
            "target_return_pct": TARGET_RETURN * 100,
            "target_price": "",
            "status": "",
            "hit_30pct": "",
            "first_hit_bars": "",
            "first_hit_timestamp": "",
            "max_high": "",
            "max_return_pct": "",
            "min_low": "",
            "max_drawdown_pct": "",
            "observed_bars": 0,
            "required_bars": horizon,
        })

        if entry_index >= len(frame):
            result["status"] = "CENSORED"
            rows.append(result)
            continue

        entry_candle = frame.iloc[entry_index]

        entry_price = float(entry_candle["open"])

        if not math.isfinite(entry_price) or entry_price <= 0:
            result["status"] = "INVALID"
            rows.append(result)
            continue

        target_price = entry_price * (
            1.0 + TARGET_RETURN
        )

        end_index = min(
            entry_index + horizon,
            len(frame),
        )

        future = frame.iloc[
            entry_index:end_index
        ]

        observed = len(future)

        result["entry_timestamp"] = (
            entry_candle["parsed_timestamp"].isoformat()
        )
        result["entry_price"] = entry_price
        result["target_price"] = target_price
        result["observed_bars"] = observed

        if observed == 0:
            result["status"] = "CENSORED"
            rows.append(result)
            continue

        highs = future["high"].to_numpy(
            dtype=np.float64
        )

        lows = future["low"].to_numpy(
            dtype=np.float64
        )

        max_high = float(np.max(highs))
        min_low = float(np.min(lows))

        max_return_pct = (
            (max_high / entry_price) - 1.0
        ) * 100.0

        max_drawdown_pct = (
            (min_low / entry_price) - 1.0
        ) * 100.0

        result["max_high"] = max_high
        result["max_return_pct"] = max_return_pct
        result["min_low"] = min_low
        result["max_drawdown_pct"] = max_drawdown_pct

        hit_positions = np.flatnonzero(
            highs >= target_price
        )

        if len(hit_positions) > 0:
            first_position = int(hit_positions[0])

            first_hit_candle = future.iloc[
                first_position
            ]

            result["status"] = "HIT"
            result["hit_30pct"] = 1
            result["first_hit_bars"] = (
                first_position + 1
            )
            result["first_hit_timestamp"] = (
                first_hit_candle[
                    "parsed_timestamp"
                ].isoformat()
            )

        elif observed < horizon:
            result["status"] = "CENSORED"

        else:
            result["status"] = "MISS"
            result["hit_30pct"] = 0

        rows.append(result)

    return rows


def checkpoint_path(
    timeframe: str,
    market: str,
) -> Path:
    safe_market = market.replace("/", "_")

    return (
        CHECKPOINT_ROOT
        / timeframe
        / f"{safe_market}.json"
    )


def chunk_path(
    timeframe: str,
    market: str,
) -> Path:
    safe_market = market.replace("/", "_")

    return (
        CHECKPOINT_ROOT
        / timeframe
        / f"{safe_market}.csv"
    )


def checkpoint_is_valid(
    checkpoint: Path,
    chunk: Path,
    candidate_hash: str,
    source_hash: str,
    expected_signal_count: int,
) -> bool:
    if not checkpoint.is_file():
        return False

    if not chunk.is_file():
        return False

    try:
        data = read_json(checkpoint)

        if data.get("version") != PROGRAM_VERSION:
            return False

        if data.get("candidate_sha256") != candidate_hash:
            return False

        if data.get("ohlcv_sha256") != source_hash:
            return False

        if data.get("signal_count") != expected_signal_count:
            return False

        if data.get("row_count") != (
            expected_signal_count * len(HORIZONS)
        ):
            return False

        if data.get("chunk_sha256") != sha256_file(chunk):
            return False

        with chunk.open(
            "r",
            encoding=CSV_ENCODING,
            newline="",
        ) as handle:
            reader = csv.DictReader(handle)

            if reader.fieldnames != DETAIL_COLUMNS:
                return False

            row_count = 0

            for row in reader:
                row_count += 1

                if row["status"] not in {
                    "HIT",
                    "MISS",
                    "CENSORED",
                    "INVALID",
                }:
                    return False

                if int(row["horizon"]) not in HORIZONS:
                    return False

            if row_count != data["row_count"]:
                return False

        return True

    except (
        OSError,
        ValueError,
        TypeError,
        KeyError,
        json.JSONDecodeError,
    ):
        return False


def process_market(
    timeframe: str,
    market: str,
    signals: list[dict],
    ohlcv_path: Path,
    candidate_hash: str,
    force: bool,
) -> tuple[int, bool]:
    checkpoint = checkpoint_path(
        timeframe,
        market,
    )

    chunk = chunk_path(
        timeframe,
        market,
    )

    source_hash_before = sha256_file(
        ohlcv_path
    )

    if not force and checkpoint_is_valid(
        checkpoint,
        chunk,
        candidate_hash,
        source_hash_before,
        len(signals),
    ):
        log(
            f"[SKIP] verified checkpoint: "
            f"{timeframe} {market}"
        )

        return len(signals) * len(HORIZONS), True

    frame = load_ohlcv(
        ohlcv_path,
        market,
    )

    timestamp_index = {
        timestamp: index
        for index, timestamp in enumerate(
            frame["parsed_timestamp"]
        )
    }

    rows = []

    for signal in signals:
        rows.extend(
            validate_one_signal(
                signal=signal,
                frame=frame,
                timestamp_index=timestamp_index,
                timeframe=timeframe,
                market=market,
                source_hash=source_hash_before,
            )
        )

    source_hash_after = sha256_file(
        ohlcv_path
    )

    if source_hash_after != source_hash_before:
        raise RuntimeError(
            "OHLCV source changed during validation: "
            f"{ohlcv_path}"
        )

    atomic_write_csv(
        chunk,
        rows,
        DETAIL_COLUMNS,
    )

    chunk_hash = sha256_file(
        chunk
    )

    checkpoint_data = {
        "program": PROGRAM_NAME,
        "version": PROGRAM_VERSION,
        "created_at_utc": utc_now(),
        "timeframe": timeframe,
        "market": market,
        "candidate_sha256": candidate_hash,
        "ohlcv_sha256": source_hash_before,
        "signal_count": len(signals),
        "row_count": len(rows),
        "chunk_sha256": chunk_hash,
        "chunk_path": str(
            chunk.relative_to(ROOT)
        ),
    }

    atomic_write_json(
        checkpoint,
        checkpoint_data,
    )

    log(
        f"[PASS] {timeframe} {market}: "
        f"{len(signals):,} signals, "
        f"{len(rows):,} validation rows"
    )

    return len(rows), False


def combine_chunks(
    keys: list[tuple[str, str]],
) -> int:
    DETAIL_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    fd, temporary_name = tempfile.mkstemp(
        prefix=DETAIL_FILE.name + ".",
        suffix=".tmp",
        dir=str(DETAIL_FILE.parent),
    )

    temporary_path = Path(temporary_name)

    total = 0

    try:
        with os.fdopen(
            fd,
            "w",
            encoding=CSV_ENCODING,
            newline="",
        ) as output:
            writer = csv.DictWriter(
                output,
                fieldnames=DETAIL_COLUMNS,
            )

            writer.writeheader()

            for timeframe, market in keys:
                path = chunk_path(
                    timeframe,
                    market,
                )

                with path.open(
                    "r",
                    encoding=CSV_ENCODING,
                    newline="",
                ) as handle:
                    reader = csv.DictReader(handle)

                    if reader.fieldnames != DETAIL_COLUMNS:
                        raise ValueError(
                            f"Invalid chunk schema: {path}"
                        )

                    for row in reader:
                        writer.writerow(row)
                        total += 1

        os.replace(
            temporary_path,
            DETAIL_FILE,
        )

    finally:
        if temporary_path.exists():
            temporary_path.unlink()

    return total


def build_summary() -> tuple[list[dict], dict]:
    """
    Aggregate complete HIT/MISS observations.

    CENSORED signals are excluded from the hit-rate
    denominator, preventing incomplete future windows
    from being counted as failures.
    """
    stats = defaultdict(
        lambda: {
            "total": 0,
            "complete": 0,
            "hit": 0,
            "miss": 0,
            "censored": 0,
            "invalid": 0,
            "max_returns": [],
            "hit_bars": [],
        }
    )

    with DETAIL_FILE.open(
        "r",
        encoding=CSV_ENCODING,
        newline="",
    ) as handle:
        reader = csv.DictReader(handle)

        for row in reader:
            key = (
                row["timeframe"],
                row["classification"],
                int(row["horizon"]),
            )

            item = stats[key]

            item["total"] += 1

            status = row["status"]

            if status == "HIT":
                item["hit"] += 1
                item["complete"] += 1

                bars = optional_float(
                    row["first_hit_bars"]
                )

                if bars is not None:
                    item["hit_bars"].append(bars)

            elif status == "MISS":
                item["miss"] += 1
                item["complete"] += 1

            elif status == "CENSORED":
                item["censored"] += 1

            elif status == "INVALID":
                item["invalid"] += 1

            else:
                raise ValueError(
                    f"Unexpected validation status: {status}"
                )

            if status in {"HIT", "MISS"}:
                max_return = optional_float(
                    row["max_return_pct"]
                )

                if max_return is not None:
                    item["max_returns"].append(
                        max_return
                    )

    summary_rows = []

    for timeframe in TIMEFRAMES:
        for classification in GROUPS:
            for horizon in HORIZONS:
                key = (
                    timeframe,
                    classification,
                    horizon,
                )

                item = stats[key]

                complete = item["complete"]

                if complete:
                    hit_rate = (
                        item["hit"] / complete
                    ) * 100.0
                else:
                    hit_rate = None

                returns = item["max_returns"]
                hit_bars = item["hit_bars"]

                summary_rows.append({
                    "timeframe": timeframe,
                    "classification": classification,
                    "horizon": horizon,
                    "total_signals": item["total"],
                    "complete": complete,
                    "hit": item["hit"],
                    "miss": item["miss"],
                    "censored": item["censored"],
                    "invalid": item["invalid"],
                    "hit_rate_pct": (
                        round(hit_rate, 6)
                        if hit_rate is not None
                        else ""
                    ),
                    "average_max_return_pct": (
                        round(float(np.mean(returns)), 6)
                        if returns
                        else ""
                    ),
                    "median_max_return_pct": (
                        round(float(np.median(returns)), 6)
                        if returns
                        else ""
                    ),
                    "average_first_hit_bars": (
                        round(float(np.mean(hit_bars)), 6)
                        if hit_bars
                        else ""
                    ),
                })

    total_status = defaultdict(int)

    for item in stats.values():
        total_status["hit"] += item["hit"]
        total_status["miss"] += item["miss"]
        total_status["censored"] += item["censored"]
        total_status["invalid"] += item["invalid"]

    return summary_rows, dict(total_status)


def verify_final_outputs(
    expected_rows: int,
) -> None:
    if not DETAIL_FILE.is_file():
        raise FileNotFoundError(
            DETAIL_FILE
        )

    if not SUMMARY_FILE.is_file():
        raise FileNotFoundError(
            SUMMARY_FILE
        )

    with DETAIL_FILE.open(
        "r",
        encoding=CSV_ENCODING,
        newline="",
    ) as handle:
        reader = csv.DictReader(handle)

        if reader.fieldnames != DETAIL_COLUMNS:
            raise ValueError(
                "Final detail schema mismatch"
            )

        count = sum(1 for _ in reader)

    if count != expected_rows:
        raise ValueError(
            "Final detail row count mismatch: "
            f"expected={expected_rows}, actual={count}"
        )

    with SUMMARY_FILE.open(
        "r",
        encoding=CSV_ENCODING,
        newline="",
    ) as handle:
        reader = csv.DictReader(handle)

        if reader.fieldnames != SUMMARY_COLUMNS:
            raise ValueError(
                "Final summary schema mismatch"
            )

        count = sum(1 for _ in reader)

    expected_summary_rows = (
        len(TIMEFRAMES)
        * len(GROUPS)
        * len(HORIZONS)
    )

    if count != expected_summary_rows:
        raise ValueError(
            "Final summary row count mismatch: "
            f"expected={expected_summary_rows}, actual={count}"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Validate Pattern 256 and accumulation "
            "candle signals against 30% future rises."
        )
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help="Recalculate even valid checkpoints.",
    )

    parser.add_argument(
        "--limit-markets",
        type=int,
        default=0,
        help=(
            "Limit number of timeframe/market groups "
            "for a development test. 0 means all."
        ),
    )

    return parser.parse_args()


def main() -> int:
    args = parse_args()

    start = time.monotonic()

    log("=" * 72)
    log(f"{PROGRAM_NAME} - {PROGRAM_VERSION}")
    log("=" * 72)

    if args.limit_markets < 0:
        raise ValueError(
            "--limit-markets must be >= 0"
        )

    if not CANDIDATES_FILE.is_file():
        raise FileNotFoundError(
            CANDIDATES_FILE
        )

    OUTPUT_ROOT.mkdir(
        parents=True,
        exist_ok=True,
    )

    CHECKPOINT_ROOT.mkdir(
        parents=True,
        exist_ok=True,
    )

    candidate_hash_before = sha256_file(
        CANDIDATES_FILE
    )

    log(
        "[SOURCE] Crosscheck SHA256: "
        f"{candidate_hash_before}"
    )

    candidates = load_candidates()

    source_files = find_ohlcv_files()

    keys = sorted(candidates.keys())

    if args.limit_markets:
        keys = keys[:args.limit_markets]

        log(
            f"[TEST] Limited to {len(keys)} "
            "timeframe/market groups"
        )

    if not keys:
        raise ValueError(
            "No candidate groups to validate"
        )

    expected_rows = sum(
        len(candidates[key]) * len(HORIZONS)
        for key in keys
    )

    log(
        f"[PLAN] Groups: {len(keys):,}, "
        f"expected rows: {expected_rows:,}"
    )

    missing_sources = [
        key
        for key in keys
        if key not in source_files
    ]

    if missing_sources:
        raise FileNotFoundError(
            "Missing OHLCV sources: "
            f"{missing_sources[:20]}"
        )

    processed_rows = 0
    skipped = 0

    source_manifest = []

    for number, key in enumerate(keys, start=1):
        timeframe, market = key

        path = source_files[key]

        log(
            f"[RUN] {number}/{len(keys)} "
            f"{timeframe} {market}"
        )

        row_count, was_skipped = process_market(
            timeframe=timeframe,
            market=market,
            signals=candidates[key],
            ohlcv_path=path,
            candidate_hash=candidate_hash_before,
            force=args.force,
        )

        processed_rows += row_count

        if was_skipped:
            skipped += 1

        source_manifest.append({
            "timeframe": timeframe,
            "market": market,
            "path": str(path.relative_to(ROOT)),
            "sha256": sha256_file(path),
            "signal_count": len(candidates[key]),
        })

    if processed_rows != expected_rows:
        raise ValueError(
            "Processed row count mismatch: "
            f"{processed_rows} != {expected_rows}"
        )

    candidate_hash_after = sha256_file(
        CANDIDATES_FILE
    )

    if candidate_hash_after != candidate_hash_before:
        raise RuntimeError(
            "Crosscheck source changed during validation"
        )

    log("[MERGE] Combining verified chunks")

    for timeframe, market in keys:
        path = source_files[
            (timeframe, market)
        ]

        current_hash = sha256_file(path)

        checkpoint = read_json(
            checkpoint_path(
                timeframe,
                market,
            )
        )

        if current_hash != checkpoint["ohlcv_sha256"]:
            raise RuntimeError(
                "OHLCV changed before final merge: "
                f"{timeframe} {market}"
            )

        if not checkpoint_is_valid(
            checkpoint_path(timeframe, market),
            chunk_path(timeframe, market),
            candidate_hash_before,
            current_hash,
            len(candidates[(timeframe, market)]),
        ):
            raise RuntimeError(
                "Checkpoint failed final validation: "
                f"{timeframe} {market}"
            )

    merged_rows = combine_chunks(keys)

    if merged_rows != expected_rows:
        raise ValueError(
            "Merged row count mismatch"
        )

    log("[SUMMARY] Calculating performance")

    summary_rows, total_status = build_summary()

    atomic_write_csv(
        SUMMARY_FILE,
        summary_rows,
        SUMMARY_COLUMNS,
    )

    verify_final_outputs(
        expected_rows
    )

    elapsed = round(
        time.monotonic() - start,
        3,
    )

    result = {
        "program": PROGRAM_NAME,
        "version": PROGRAM_VERSION,
        "status": "PASS",
        "created_at_utc": utc_now(),
        "target_return_pct": 30.0,
        "entry_rule": "NEXT_CANDLE_OPEN",
        "hit_rule": "FUTURE_HIGH_GTE_ENTRY_X_1_30",
        "horizons": list(HORIZONS),
        "timeframes": list(TIMEFRAMES),
        "groups": list(GROUPS),
        "future_data_policy": (
            "INCOMPLETE_WINDOWS_CENSORED"
        ),
        "candidate_source": str(
            CANDIDATES_FILE.relative_to(ROOT)
        ),
        "candidate_source_sha256": (
            candidate_hash_before
        ),
        "market_timeframe_groups": len(keys),
        "checkpoint_skipped": skipped,
        "detail_rows": merged_rows,
        "summary_rows": len(summary_rows),
        "status_counts": total_status,
        "elapsed_seconds": elapsed,
        "limited_test": bool(
            args.limit_markets
        ),
        "detail_sha256": sha256_file(
            DETAIL_FILE
        ),
        "summary_sha256": sha256_file(
            SUMMARY_FILE
        ),
    }

    manifest = {
        "program": PROGRAM_NAME,
        "version": PROGRAM_VERSION,
        "created_at_utc": utc_now(),
        "candidate_source": str(
            CANDIDATES_FILE.relative_to(ROOT)
        ),
        "candidate_sha256": candidate_hash_before,
        "ohlcv_sources": source_manifest,
        "detail_sha256": sha256_file(
            DETAIL_FILE
        ),
        "summary_sha256": sha256_file(
            SUMMARY_FILE
        ),
    }

    atomic_write_json(
        MANIFEST_FILE,
        manifest,
    )

    atomic_write_json(
        RESULT_FILE,
        result,
    )

    log("=" * 72)
    log("[PASS] 30% validation completed")
    log(f"[PASS] Detail rows: {merged_rows:,}")
    log(f"[PASS] Checkpoints skipped: {skipped:,}")
    log(f"[PASS] HIT: {total_status.get('hit', 0):,}")
    log(f"[PASS] MISS: {total_status.get('miss', 0):,}")
    log(
        "[PASS] CENSORED: "
        f"{total_status.get('censored', 0):,}"
    )
    log(f"[PASS] Elapsed: {elapsed:.1f}s")
    log(f"[OUTPUT] {OUTPUT_ROOT}")
    log("=" * 72)

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())

    except KeyboardInterrupt:
        log("[STOP] Interrupted safely.")
        sys.exit(130)

    except Exception:
        log("[FAIL] 30% validation fatal error")
        traceback.print_exc()
        sys.exit(1)
