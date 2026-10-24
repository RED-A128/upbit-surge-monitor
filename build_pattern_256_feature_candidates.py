#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Feature Candidate Engine
Clean V001

Inputs:
  data/ohlcv/{h1,h4,d1}/KRW-*.csv
  data/reports/pattern_256_feature_redesign/
      feature_redesign_plan.csv
      feature_redesign_protocol.json
      feature_redesign_result.json
      feature_redesign_manifest.json

Outputs:
  data/features/pattern_256_candidates/{timeframe}/{market}.csv
  data/reports/pattern_256_feature_candidates/
      feature_candidate_result.json
      feature_candidate_manifest.json

Properties:
  - No future candle usage
  - Features use current and preceding CLOSED candles only
  - No new signals or trading decisions
  - SHA256 input/output integrity
  - Per-market resume
  - Original OHLCV is never modified
"""

import argparse
import csv
import hashlib
import json
import math
import os
import sys
import tempfile

from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd


VERSION = "Clean V001"

TIMEFRAMES = ("h1", "h4", "d1")

PLAN_DIR = (
    "data/reports/pattern_256_feature_redesign"
)

OUTPUT_DIR = (
    "data/features/pattern_256_candidates"
)

REPORT_DIR = (
    "data/reports/pattern_256_feature_candidates"
)

PLAN_FILES = (
    "feature_redesign_plan.csv",
    "feature_redesign_protocol.json",
    "feature_redesign_result.json",
    "feature_redesign_manifest.json",
)

FEATURE_COLUMNS = [
    "market",
    "candle_date_time_utc",
    "timestamp",
    "close",
    "volume",
    "trade_value",
    "relative_volume_20",
    "relative_volume_40",
    "relative_volume_60",
    "relative_trade_value_20",
    "volume_persistence_2",
    "volume_persistence_3",
    "volume_persistence_5",
    "atr_14",
    "atr_ratio_10",
    "atr_ratio_20",
    "atr_ratio_30",
    "ma_20",
    "ma_60",
    "ma_120",
    "trend_alignment",
    "resistance_20",
    "resistance_40",
    "distance_to_resistance_20_pct",
    "distance_to_resistance_40_pct",
    "regime_return_30",
    "regime_return_60",
    "regime_return_90",
]


def require(condition, message):
    if not condition:
        raise ValueError(message)


def file_sha256(path):
    digest = hashlib.sha256()

    with path.open("rb") as stream:
        for chunk in iter(
            lambda: stream.read(1024 * 1024),
            b"",
        ):
            digest.update(chunk)

    return digest.hexdigest()


def read_json(path):
    with path.open(
        "r",
        encoding="utf-8-sig",
    ) as stream:
        return json.load(stream)


def atomic_bytes(path, data):
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    descriptor, temporary = tempfile.mkstemp(
        prefix=".p256_feature_",
        dir=str(path.parent),
    )

    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())

        os.replace(temporary, path)

    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def atomic_json(path, payload):
    content = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        indent=2,
        allow_nan=False,
    )

    atomic_bytes(
        path,
        (content + "\n").encode("utf-8"),
    )


def validate_plan(root):
    directory = root / PLAN_DIR

    paths = {
        name: directory / name
        for name in PLAN_FILES
    }

    for name, path in paths.items():
        require(
            path.is_file(),
            f"Missing plan file: {name}",
        )

    result = read_json(
        paths["feature_redesign_result.json"]
    )

    protocol = read_json(
        paths["feature_redesign_protocol.json"]
    )

    manifest = read_json(
        paths["feature_redesign_manifest.json"]
    )

    require(
        result.get("version") == "Clean V001",
        "Unexpected plan version",
    )

    require(
        result.get("execution_status") == "PASS",
        "Plan execution not PASS",
    )

    require(
        result.get("hypothesis_rows") == 15,
        "Unexpected hypothesis count",
    )

    require(
        result.get("research_decision") == "HOLD",
        "Unexpected plan decision",
    )

    require(
        protocol.get("feature_rules", {}).get(
            "no_future_information"
        ) is True,
        "No-future-information rule missing",
    )

    for document in (result, protocol):
        require(
            document.get("approved_for_live_trading")
            is False,
            "Unexpected live approval",
        )

    require(
        manifest.get("version") == "Clean V001",
        "Unexpected manifest version",
    )

    expected = manifest.get(
        "output_sha256",
        {},
    )

    for name in PLAN_FILES[:3]:
        require(
            expected.get(name) == file_sha256(paths[name]),
            f"Plan SHA256 mismatch: {name}",
        )

    with paths["feature_redesign_plan.csv"].open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as stream:
        rows = list(csv.DictReader(stream))

    require(
        len(rows) == 15,
        "Plan must contain 15 rows",
    )

    for row in rows:
        require(
            row["approved_for_live_scanner"] == "false",
            "Unexpected scanner approval",
        )

    return {
        name: file_sha256(path)
        for name, path in paths.items()
    }


def load_ohlcv(path, timeframe):
    frame = pd.read_csv(
        path,
        low_memory=False,
    )

    required = {
        "market",
        "candle_date_time_utc",
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "trade_value",
    }

    require(
        required.issubset(frame.columns),
        f"Missing OHLCV columns: {path}",
    )

    require(
        len(frame) > 0,
        f"Empty OHLCV file: {path}",
    )

    expected_market = path.stem

    require(
        expected_market.startswith("KRW-"),
        f"Unexpected market: {expected_market}",
    )

    require(
        frame["market"].notna().all(),
        f"Missing market values: {path}",
    )

    require(
        (
            frame["market"].astype(str)
            == expected_market
        ).all(),
        f"Market mismatch: {path}",
    )

    frame["parsed_time"] = pd.to_datetime(
        frame["candle_date_time_utc"],
        utc=True,
        errors="raise",
    )

    require(
        frame["parsed_time"].notna().all(),
        f"Invalid UTC timestamps: {path}",
    )

    for column in (
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "trade_value",
    ):
        frame[column] = pd.to_numeric(
            frame[column],
            errors="raise",
        )

    numeric = frame[
        [
            "timestamp",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "trade_value",
        ]
    ].to_numpy(dtype=float)

    require(
        np.isfinite(numeric).all(),
        f"Non-finite OHLCV values: {path}",
    )

    require(
        (frame["open"] > 0).all()
        and (frame["high"] > 0).all()
        and (frame["low"] > 0).all()
        and (frame["close"] > 0).all(),
        f"Nonpositive prices: {path}",
    )

    require(
        (
            frame["high"]
            >= frame[["open", "close", "low"]].max(axis=1)
        ).all(),
        f"Invalid high price: {path}",
    )

    require(
        (
            frame["low"]
            <= frame[["open", "close", "high"]].min(axis=1)
        ).all(),
        f"Invalid low price: {path}",
    )

    require(
        (frame["volume"] >= 0).all()
        and (frame["trade_value"] >= 0).all(),
        f"Negative volume: {path}",
    )

    require(
        frame["parsed_time"].is_unique,
        f"Duplicate candle times: {path}",
    )

    require(
        frame["timestamp"].is_unique,
        f"Duplicate timestamps: {path}",
    )

    frame = frame.sort_values(
        "parsed_time",
        kind="stable",
    ).reset_index(drop=True)

    milliseconds = (
        frame["parsed_time"].astype("int64") // 1_000_000
    )

    require(
        np.array_equal(
            milliseconds.to_numpy(dtype=np.int64),
            frame["timestamp"].to_numpy(dtype=np.int64),
        ),
        f"UTC timestamp mismatch: {path}",
    )

    interval_ms = {
        "h1": 3_600_000,
        "h4": 14_400_000,
        "d1": 86_400_000,
    }[timeframe]

    differences = frame["timestamp"].diff().dropna()

    require(
        (differences > 0).all(),
        f"Non-increasing timestamps: {path}",
    )

    require(
        (
            differences.astype(np.int64)
            % interval_ms
            == 0
        ).all(),
        f"Unexpected candle interval: {path}",
    )

    return frame


def rolling_mean(series, window):
    return series.rolling(
        window=window,
        min_periods=window,
    ).mean()


def calculate_features(frame):
    close = frame["close"]
    high = frame["high"]
    low = frame["low"]
    volume = frame["volume"]
    trade_value = frame["trade_value"]

    output = frame[
        [
            "market",
            "candle_date_time_utc",
            "timestamp",
            "close",
            "volume",
            "trade_value",
        ]
    ].copy()

    for window in (20, 40, 60):
        baseline = rolling_mean(
            volume.shift(1),
            window,
        )

        output[
            f"relative_volume_{window}"
        ] = volume.div(
            baseline.where(baseline > 0)
        )

    turnover_baseline = rolling_mean(
        trade_value.shift(1),
        20,
    )

    output["relative_trade_value_20"] = (
        trade_value.div(
            turnover_baseline.where(
                turnover_baseline > 0
            )
        )
    )

    # A bar counts as high-volume when its volume
    # exceeds 1.5x the preceding 20-bar average.
    high_volume = (
        output["relative_volume_20"] >= 1.5
    ).astype(float)

    high_volume = high_volume.where(
        output["relative_volume_20"].notna()
    )

    for window in (2, 3, 5):
        output[
            f"volume_persistence_{window}"
        ] = high_volume.rolling(
            window=window,
            min_periods=window,
        ).sum()

    previous_close = close.shift(1)

    true_range = pd.concat(
        [
            high - low,
            (high - previous_close).abs(),
            (low - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    atr = rolling_mean(
        true_range,
        14,
    )

    output["atr_14"] = atr

    # ATR compression compares current ATR against
    # preceding ATR history; the current candle is
    # not included in the baseline.
    for window in (10, 20, 30):
        baseline = rolling_mean(
            atr.shift(1),
            window,
        )

        output[
            f"atr_ratio_{window}"
        ] = atr.div(
            baseline.where(baseline > 0)
        )

    for window in (20, 60, 120):
        output[
            f"ma_{window}"
        ] = rolling_mean(
            close,
            window,
        )

    valid_trend = output[
        ["ma_20", "ma_60", "ma_120"]
    ].notna().all(axis=1)

    aligned = (
        (output["ma_20"] > output["ma_60"])
        & (output["ma_60"] > output["ma_120"])
    )

    output["trend_alignment"] = (
        aligned.astype(float).where(valid_trend)
    )

    for window in (20, 40):
        # Previous highs only. Current high is excluded.
        resistance = high.shift(1).rolling(
            window=window,
            min_periods=window,
        ).max()

        output[
            f"resistance_{window}"
        ] = resistance

        output[
            f"distance_to_resistance_{window}_pct"
        ] = (
            (close / resistance - 1.0) * 100.0
        )

    for window in (30, 60, 90):
        output[
            f"regime_return_{window}"
        ] = close.div(
            close.shift(window)
        ) - 1.0

    output = output[FEATURE_COLUMNS]

    numeric_columns = [
        column
        for column in FEATURE_COLUMNS
        if column not in (
            "market",
            "candle_date_time_utc",
        )
    ]

    output[numeric_columns] = output[
        numeric_columns
    ].replace(
        [np.inf, -np.inf],
        np.nan,
    )

    return output


def validate_feature_output(frame, output):
    require(
        len(frame) == len(output),
        "Feature row count mismatch",
    )

    require(
        list(output.columns) == FEATURE_COLUMNS,
        "Feature schema mismatch",
    )

    require(
        frame["timestamp"].equals(
            output["timestamp"]
        ),
        "Feature timestamp mismatch",
    )

    require(
        frame["close"].equals(
            output["close"]
        ),
        "Feature close mismatch",
    )

    require(
        frame["volume"].equals(
            output["volume"]
        ),
        "Feature volume mismatch",
    )

    for column in output.columns:
        if column in (
            "market",
            "candle_date_time_utc",
        ):
            continue

        values = output[column].dropna().to_numpy(
            dtype=float
        )

        require(
            np.isfinite(values).all(),
            f"Invalid feature values: {column}",
        )


def checkpoint_path(report_dir, timeframe, market):
    return (
        report_dir
        / "checkpoints"
        / timeframe
        / f"{market}.json"
    )


def checkpoint_valid(
    checkpoint,
    source_hash,
    output_path,
    timeframe,
    market,
):
    if not checkpoint.is_file():
        return False

    if not output_path.is_file():
        return False

    try:
        data = read_json(checkpoint)

        if data.get("version") != VERSION:
            return False

        if data.get("timeframe") != timeframe:
            return False

        if data.get("market") != market:
            return False

        if data.get("source_sha256") != source_hash:
            return False

        if data.get("output_sha256") != file_sha256(
            output_path
        ):
            return False

        if int(data.get("rows", 0)) < 1:
            return False

        with output_path.open(
            "r",
            encoding="utf-8",
            newline="",
        ) as stream:
            reader = csv.reader(stream)

            if next(reader, None) != FEATURE_COLUMNS:
                return False

            actual_rows = sum(1 for _ in reader)

        return actual_rows == int(data["rows"])

    except (
        OSError,
        ValueError,
        TypeError,
        KeyError,
        json.JSONDecodeError,
    ):
        return False


def process_market(
    source_path,
    output_path,
    checkpoint,
    timeframe,
    force,
):
    market = source_path.stem
    source_hash = file_sha256(source_path)

    if not force and checkpoint_valid(
        checkpoint,
        source_hash,
        output_path,
        timeframe,
        market,
    ):
        return "SKIP", read_json(checkpoint)

    frame = load_ohlcv(
        source_path,
        timeframe,
    )

    features = calculate_features(frame)

    validate_feature_output(
        frame,
        features,
    )

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        newline="",
        suffix=".csv",
        prefix=".p256_features_",
        dir=str(output_path.parent),
        delete=False,
    ) as temporary:
        temporary_path = Path(temporary.name)

    try:
        features.to_csv(
            temporary_path,
            index=False,
            float_format="%.15g",
        )

        # Re-read the persisted file to verify its
        # row count and exact output schema.
        with temporary_path.open(
            "r",
            encoding="utf-8",
            newline="",
        ) as stream:
            reader = csv.reader(stream)

            require(
                next(reader, None) == FEATURE_COLUMNS,
                "Persisted feature schema mismatch",
            )

            written_rows = sum(1 for _ in reader)

        require(
            written_rows == len(features),
            "Persisted feature row count mismatch",
        )

        os.replace(
            temporary_path,
            output_path,
        )

    finally:
        if temporary_path.exists():
            temporary_path.unlink()

    record = {
        "version": VERSION,
        "timeframe": timeframe,
        "market": market,
        "source_sha256": source_hash,
        "output_sha256": file_sha256(output_path),
        "rows": len(features),
        "first_timestamp": int(
            frame["timestamp"].iloc[0]
        ),
        "last_timestamp": int(
            frame["timestamp"].iloc[-1]
        ),
        "feature_schema": FEATURE_COLUMNS,
        "status": "PASS",
    }

    atomic_json(
        checkpoint,
        record,
    )

    return "BUILD", record


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--root",
        default=".",
    )

    parser.add_argument(
        "--force",
        action="store_true",
    )

    args = parser.parse_args()

    root = Path(args.root).expanduser().resolve()

    ohlcv_root = root / "data" / "ohlcv"
    output_root = root / OUTPUT_DIR
    report_root = root / REPORT_DIR

    require(
        ohlcv_root.is_dir(),
        f"Missing OHLCV root: {ohlcv_root}",
    )

    plan_hashes = validate_plan(root)

    require(
        output_root != ohlcv_root,
        "Output must not overwrite OHLCV",
    )

    records = []
    built = 0
    skipped = 0

    for timeframe in TIMEFRAMES:
        source_dir = ohlcv_root / timeframe

        require(
            source_dir.is_dir(),
            f"Missing timeframe directory: {source_dir}",
        )

        sources = sorted(
            source_dir.glob("KRW-*.csv")
        )

        require(
            len(sources) > 0,
            f"No OHLCV files: {timeframe}",
        )

        print(
            f"[{timeframe}] markets={len(sources)}",
            flush=True,
        )

        for index, source_path in enumerate(
            sources,
            start=1,
        ):
            market = source_path.stem

            output_path = (
                output_root
                / timeframe
                / f"{market}.csv"
            )

            checkpoint = checkpoint_path(
                report_root,
                timeframe,
                market,
            )

            action, record = process_market(
                source_path,
                output_path,
                checkpoint,
                timeframe,
                args.force,
            )

            if action == "BUILD":
                built += 1
            else:
                skipped += 1

            records.append(record)

            if (
                index == 1
                or index % 25 == 0
                or index == len(sources)
            ):
                print(
                    f"[{timeframe}] "
                    f"{index}/{len(sources)} "
                    f"BUILD={built} SKIP={skipped}",
                    flush=True,
                )

    require(
        len(records) > 0,
        "No feature outputs",
    )

    # Final integrity verification.
    for record in records:
        timeframe = record["timeframe"]
        market = record["market"]

        source_path = (
            ohlcv_root
            / timeframe
            / f"{market}.csv"
        )

        output_path = (
            output_root
            / timeframe
            / f"{market}.csv"
        )

        checkpoint = checkpoint_path(
            report_root,
            timeframe,
            market,
        )

        require(
            checkpoint_valid(
                checkpoint,
                file_sha256(source_path),
                output_path,
                timeframe,
                market,
            ),
            f"Final integrity failure: {timeframe}/{market}",
        )

    result = {
        "version": VERSION,
        "execution_status": "PASS",
        "market_files": len(records),
        "built_files": built,
        "resumed_files": skipped,
        "total_feature_rows": sum(
            int(item["rows"])
            for item in records
        ),
        "feature_columns": FEATURE_COLUMNS,
        "research_decision": "HOLD",
        "feature_generation_completed": True,
        "train_experiment_completed": False,
        "independent_oos_proven": False,
        "statistical_independence_proven": False,
        "profitability_proven": False,
        "approved_for_live_trading": False,
        "automated_trading_enabled": False,
        "generated_at_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "limitations": [
            "Feature generation does not prove predictive value.",
            "No feature optimization was performed.",
            "No independent OOS test was performed.",
            "All features use closed-candle historical values.",
            "Missing OHLCV intervals are not imputed.",
            "No automated trading is enabled.",
        ],
    }

    report_root.mkdir(
        parents=True,
        exist_ok=True,
    )

    result_path = (
        report_root
        / "feature_candidate_result.json"
    )

    manifest_path = (
        report_root
        / "feature_candidate_manifest.json"
    )

    atomic_json(
        result_path,
        result,
    )

    manifest = {
        "version": VERSION,
        "plan_input_sha256": plan_hashes,
        "market_files": len(records),
        "records": sorted(
            records,
            key=lambda item: (
                item["timeframe"],
                item["market"],
            ),
        ),
        "output_sha256": {
            "feature_candidate_result.json": (
                file_sha256(result_path)
            ),
        },
    }

    atomic_json(
        manifest_path,
        manifest,
    )

    require(
        manifest["output_sha256"][
            "feature_candidate_result.json"
        ] == file_sha256(result_path),
        "Result SHA256 mismatch",
    )

    print("=" * 65)
    print("PATTERN 256 FEATURE CANDIDATE ENGINE")
    print("=" * 65)
    print("Execution: PASS")
    print("Version:", VERSION)
    print("Market files:", len(records))
    print("Built:", built)
    print("Resumed:", skipped)
    print(
        "Feature rows:",
        result["total_feature_rows"],
    )
    print("Research decision: HOLD")
    print("Live trading approved: False")
    print("SHA256: VERIFIED")
    print("=" * 65)

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())

    except Exception as exc:
        print(
            f"FATAL: {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        sys.exit(1)
