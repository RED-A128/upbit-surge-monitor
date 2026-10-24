#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Feature Integrity & Gap Audit
Clean V001

Purpose:
    Audit previously generated Pattern 256 candidate features.

Inputs:
    data/ohlcv/{h1,h4,d1}/KRW-*.csv
    data/features/pattern_256_candidates/{h1,h4,d1}/KRW-*.csv
    data/reports/pattern_256_feature_candidates/
        feature_candidate_result.json
        feature_candidate_manifest.json

Outputs:
    data/reports/pattern_256_feature_integrity/
        feature_integrity_markets.csv
        feature_integrity_summary.json
        feature_integrity_manifest.json
        checkpoints/{timeframe}/{market}.json

Safety:
    - No modification of OHLCV
    - No modification of generated features
    - No future information used
    - SHA256 validation
    - Per-market resume
    - No trading
"""

import argparse
import csv
import hashlib
import json
import math
import os
import sys
import tempfile

from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd


VERSION = "Clean V001"

TIMEFRAME_MS = {
    "h1": 3_600_000,
    "h4": 14_400_000,
    "d1": 86_400_000,
}

SOURCE_REPORT_DIR = (
    "data/reports/pattern_256_feature_candidates"
)

AUDIT_REPORT_DIR = (
    "data/reports/pattern_256_feature_integrity"
)

FEATURE_ROOT = (
    "data/features/pattern_256_candidates"
)

OHLCV_ROOT = "data/ohlcv"

SOURCE_RESULT_NAME = "feature_candidate_result.json"
SOURCE_MANIFEST_NAME = "feature_candidate_manifest.json"

MARKET_REPORT_NAME = "feature_integrity_markets.csv"
SUMMARY_NAME = "feature_integrity_summary.json"
MANIFEST_NAME = "feature_integrity_manifest.json"

# Longest lookback is 120 candles.
# A feature at position i is considered eligible only if
# the previous 119 transitions are all contiguous.
REQUIRED_CONTIGUOUS_BARS = 120

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

NUMERIC_FEATURES = [
    name
    for name in FEATURE_COLUMNS
    if name not in (
        "market",
        "candle_date_time_utc",
    )
]

# These fields are needed for the full 120-bar feature
# eligibility check. Other features may be valid earlier.
REQUIRED_FEATURES = [
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

MARKET_COLUMNS = [
    "version",
    "timeframe",
    "market",
    "source_rows",
    "feature_rows",
    "missing_intervals",
    "gap_events",
    "maximum_gap_bars",
    "contiguous_eligible_rows",
    "feature_complete_rows",
    "train_eligible_rows",
    "first_timestamp",
    "last_timestamp",
    "source_sha256",
    "feature_sha256",
    "audit_status",
]


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha256_file(path):
    digest = hashlib.sha256()

    with path.open("rb") as stream:
        for block in iter(
            lambda: stream.read(1024 * 1024),
            b"",
        ):
            digest.update(block)

    return digest.hexdigest()


def load_json(path):
    with path.open(
        "r",
        encoding="utf-8-sig",
    ) as stream:
        return json.load(stream)


def atomic_write(path, content):
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    descriptor, temporary_name = tempfile.mkstemp(
        prefix=".p256_integrity_",
        dir=str(path.parent),
    )

    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())

        os.replace(temporary_name, path)

    finally:
        if os.path.exists(temporary_name):
            os.unlink(temporary_name)


def atomic_json(path, payload):
    content = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        indent=2,
        allow_nan=False,
    )

    atomic_write(
        path,
        (content + "\n").encode("utf-8"),
    )


def atomic_csv(path, rows, columns):
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    descriptor, temporary_name = tempfile.mkstemp(
        prefix=".p256_integrity_",
        suffix=".csv",
        dir=str(path.parent),
    )

    try:
        with os.fdopen(
            descriptor,
            "w",
            encoding="utf-8",
            newline="",
        ) as stream:
            writer = csv.DictWriter(
                stream,
                fieldnames=columns,
            )

            writer.writeheader()
            writer.writerows(rows)

            stream.flush()
            os.fsync(stream.fileno())

        os.replace(temporary_name, path)

    finally:
        if os.path.exists(temporary_name):
            os.unlink(temporary_name)


def validate_source_reports(root):
    report_dir = root / SOURCE_REPORT_DIR

    result_path = report_dir / SOURCE_RESULT_NAME
    manifest_path = report_dir / SOURCE_MANIFEST_NAME

    require(
        result_path.is_file(),
        f"Missing result: {result_path}",
    )

    require(
        manifest_path.is_file(),
        f"Missing manifest: {manifest_path}",
    )

    result = load_json(result_path)
    manifest = load_json(manifest_path)

    require(
        result.get("version") == "Clean V001",
        "Feature result version mismatch",
    )

    require(
        manifest.get("version") == "Clean V001",
        "Feature manifest version mismatch",
    )

    require(
        result.get("execution_status") == "PASS",
        "Feature generation did not PASS",
    )

    require(
        result.get("feature_generation_completed")
        is True,
        "Feature generation incomplete",
    )

    require(
        result.get("train_experiment_completed")
        is False,
        "Unexpected TRAIN experiment status",
    )

    require(
        result.get("approved_for_live_trading")
        is False,
        "Unexpected trading approval",
    )

    expected_result_hash = (
        manifest.get("output_sha256", {}).get(
            SOURCE_RESULT_NAME
        )
    )

    require(
        expected_result_hash == sha256_file(result_path),
        "Feature result SHA256 mismatch",
    )

    records = manifest.get("records")

    require(
        isinstance(records, list),
        "Invalid feature manifest records",
    )

    require(
        len(records) == int(result["market_files"]),
        "Manifest market count mismatch",
    )

    require(
        len(records) == int(manifest["market_files"]),
        "Manifest count mismatch",
    )

    total_rows = sum(
        int(record["rows"])
        for record in records
    )

    require(
        total_rows == int(result["total_feature_rows"]),
        "Feature row total mismatch",
    )

    return result, manifest, {
        SOURCE_RESULT_NAME: sha256_file(result_path),
        SOURCE_MANIFEST_NAME: sha256_file(manifest_path),
    }


def validate_records(records):
    seen = set()

    for record in records:
        timeframe = record["timeframe"]
        market = record["market"]

        require(
            timeframe in TIMEFRAME_MS,
            f"Unexpected timeframe: {timeframe}",
        )

        require(
            isinstance(market, str)
            and market.startswith("KRW-")
            and market.replace("-", "").replace(
                "_", ""
            ).isalnum(),
            f"Unexpected market: {market}",
        )

        key = (timeframe, market)

        require(
            key not in seen,
            f"Duplicate manifest record: {key}",
        )

        seen.add(key)

        require(
            record.get("version") == "Clean V001",
            f"Record version mismatch: {key}",
        )

        require(
            record.get("status") == "PASS",
            f"Record not PASS: {key}",
        )

        require(
            record.get("feature_schema")
            == FEATURE_COLUMNS,
            f"Feature schema mismatch: {key}",
        )

        require(
            int(record["rows"]) > 0,
            f"Empty feature record: {key}",
        )


def checkpoint_path(report_dir, timeframe, market):
    return (
        report_dir
        / "checkpoints"
        / timeframe
        / f"{market}.json"
    )


def valid_checkpoint(
    path,
    record,
    source_hash,
    feature_hash,
):
    if not path.is_file():
        return False

    try:
        data = load_json(path)

        if data.get("version") != VERSION:
            return False

        if data.get("timeframe") != record["timeframe"]:
            return False

        if data.get("market") != record["market"]:
            return False

        if data.get("source_sha256") != source_hash:
            return False

        if data.get("feature_sha256") != feature_hash:
            return False

        if data.get("audit_status") != "PASS":
            return False

        if int(data.get("source_rows", -1)) != int(
            record["rows"]
        ):
            return False

        for column in MARKET_COLUMNS:
            if column not in data:
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


def read_market_data(source_path, feature_path):
    source = pd.read_csv(
        source_path,
        low_memory=False,
    )

    features = pd.read_csv(
        feature_path,
        low_memory=False,
    )

    source_required = [
        "market",
        "candle_date_time_utc",
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "trade_value",
    ]

    require(
        set(source_required).issubset(source.columns),
        f"OHLCV schema mismatch: {source_path}",
    )

    require(
        list(features.columns) == FEATURE_COLUMNS,
        f"Feature schema mismatch: {feature_path}",
    )

    require(
        len(source) == len(features),
        f"Source/feature row mismatch: {feature_path}",
    )

    require(
        len(source) > 0,
        f"Empty source: {source_path}",
    )

    for frame in (source, features):
        frame["timestamp"] = pd.to_numeric(
            frame["timestamp"],
            errors="raise",
        )

        require(
            frame["timestamp"].notna().all(),
            "Missing timestamps",
        )

        require(
            np.isfinite(
                frame["timestamp"].to_numpy(
                    dtype=float
                )
            ).all(),
            "Invalid timestamps",
        )

        require(
            frame["timestamp"].is_unique,
            "Duplicate timestamps",
        )

        frame.sort_values(
            "timestamp",
            kind="stable",
            inplace=True,
        )

        frame.reset_index(
            drop=True,
            inplace=True,
        )

    require(
        np.array_equal(
            source["timestamp"].to_numpy(),
            features["timestamp"].to_numpy(),
        ),
        "Source/feature timestamp mismatch",
    )

    require(
        source["market"].astype(str).equals(
            features["market"].astype(str)
        ),
        "Source/feature market mismatch",
    )

    source_time = pd.to_datetime(
        source["candle_date_time_utc"],
        utc=True,
        errors="raise",
    )

    feature_time = pd.to_datetime(
        features["candle_date_time_utc"],
        utc=True,
        errors="raise",
    )

    require(
        source_time.equals(feature_time),
        "Source/feature UTC mismatch",
    )

    for column in (
        "close",
        "volume",
        "trade_value",
    ):
        left = pd.to_numeric(
            source[column],
            errors="raise",
        ).to_numpy(dtype=float)

        right = pd.to_numeric(
            features[column],
            errors="raise",
        ).to_numpy(dtype=float)

        require(
            np.isfinite(left).all()
            and np.isfinite(right).all(),
            f"Non-finite base values: {column}",
        )

        require(
            np.allclose(
                left,
                right,
                rtol=1e-12,
                atol=1e-12,
            ),
            f"Source/feature value mismatch: {column}",
        )

    for column in NUMERIC_FEATURES:
        values = pd.to_numeric(
            features[column],
            errors="coerce",
        )

        invalid = (
            features[column].notna()
            & values.isna()
        )

        require(
            not invalid.any(),
            f"Non-numeric feature: {column}",
        )

        require(
            np.isfinite(
                values.dropna().to_numpy(
                    dtype=float
                )
            ).all(),
            f"Non-finite feature: {column}",
        )

        features[column] = values

    return source, features


def audit_market(
    record,
    source_path,
    feature_path,
    source_hash,
    feature_hash,
):
    timeframe = record["timeframe"]
    market = record["market"]

    source, features = read_market_data(
        source_path,
        feature_path,
    )

    require(
        len(source) == int(record["rows"]),
        f"Manifest row mismatch: {market}",
    )

    require(
        (source["market"].astype(str) == market).all(),
        f"Market mismatch: {market}",
    )

    interval = TIMEFRAME_MS[timeframe]

    timestamps = source["timestamp"].to_numpy(
        dtype=np.int64
    )

    differences = np.diff(timestamps)

    require(
        (differences > 0).all(),
        f"Non-increasing timestamps: {market}",
    )

    require(
        (differences % interval == 0).all(),
        f"Unexpected candle spacing: {market}",
    )

    missing_per_transition = (
        differences // interval - 1
    )

    missing_intervals = int(
        missing_per_transition.sum()
    )

    gap_events = int(
        np.count_nonzero(missing_per_transition)
    )

    maximum_gap_bars = int(
        missing_per_transition.max()
    ) if len(missing_per_transition) else 0

    # Run length resets at every missing interval.
    contiguous_run = np.ones(
        len(timestamps),
        dtype=np.int32,
    )

    for index in range(1, len(timestamps)):
        if differences[index - 1] == interval:
            contiguous_run[index] = (
                contiguous_run[index - 1] + 1
            )

    contiguous_eligible = (
        contiguous_run >= REQUIRED_CONTIGUOUS_BARS
    )

    feature_complete = features[
        REQUIRED_FEATURES
    ].notna().all(axis=1).to_numpy()

    train_eligible = (
        contiguous_eligible & feature_complete
    )

    # Important:
    # These counts are a conservative data-quality
    # screening, not a label or training-set split.
    # No future price or target is used.
    result = {
        "timeframe": timeframe,
        "market": market,
        "source_rows": int(len(source)),
        "feature_rows": int(len(features)),
        "missing_intervals": missing_intervals,
        "gap_events": gap_events,
        "maximum_gap_bars": maximum_gap_bars,
        "contiguous_eligible_rows": int(
            contiguous_eligible.sum()
        ),
        "feature_complete_rows": int(
            feature_complete.sum()
        ),
        "train_eligible_rows": int(
            train_eligible.sum()
        ),
        "first_timestamp": int(timestamps[0]),
        "last_timestamp": int(timestamps[-1]),
        "source_sha256": source_hash,
        "feature_sha256": feature_hash,
        "audit_status": "PASS",
    }

    return result


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

    source_root = root / OHLCV_ROOT
    feature_root = root / FEATURE_ROOT
    report_root = root / AUDIT_REPORT_DIR

    require(
        source_root.is_dir(),
        f"Missing OHLCV root: {source_root}",
    )

    require(
        feature_root.is_dir(),
        f"Missing feature root: {feature_root}",
    )

    _, manifest, source_report_hashes = (
        validate_source_reports(root)
    )

    records = manifest["records"]

    validate_records(records)

    sorted_records = sorted(
        records,
        key=lambda item: (
            item["timeframe"],
            item["market"],
        ),
    )

    results = []
    built = 0
    resumed = 0

    for index, record in enumerate(
        sorted_records,
        start=1,
    ):
        timeframe = record["timeframe"]
        market = record["market"]

        source_path = (
            source_root
            / timeframe
            / f"{market}.csv"
        )

        feature_path = (
            feature_root
            / timeframe
            / f"{market}.csv"
        )

        checkpoint = checkpoint_path(
            report_root,
            timeframe,
            market,
        )

        require(
            source_path.is_file(),
            f"Missing OHLCV: {source_path}",
        )

        require(
            feature_path.is_file(),
            f"Missing features: {feature_path}",
        )

        source_hash = sha256_file(source_path)
        feature_hash = sha256_file(feature_path)

        require(
            source_hash == record["source_sha256"],
            f"OHLCV SHA256 mismatch: {timeframe}/{market}",
        )

        require(
            feature_hash == record["output_sha256"],
            f"Feature SHA256 mismatch: {timeframe}/{market}",
        )

        if not args.force and valid_checkpoint(
            checkpoint,
            record,
            source_hash,
            feature_hash,
        ):
            result = load_json(checkpoint)
            resumed += 1

        else:
            result = audit_market(
                record,
                source_path,
                feature_path,
                source_hash,
                feature_hash,
            )

            result["version"] = VERSION

            atomic_json(
                checkpoint,
                result,
            )

            built += 1

        results.append(result)

        if (
            index == 1
            or index % 25 == 0
            or index == len(sorted_records)
        ):
            print(
                f"[AUDIT] {index}/{len(sorted_records)} "
                f"BUILD={built} RESUME={resumed}",
                flush=True,
            )

    require(
        len(results) == len(records),
        "Incomplete market audit",
    )

    totals = {
        column: sum(
            int(item[column])
            for item in results
        )
        for column in (
            "source_rows",
            "feature_rows",
            "missing_intervals",
            "gap_events",
            "contiguous_eligible_rows",
            "feature_complete_rows",
            "train_eligible_rows",
        )
    }

    require(
        totals["source_rows"]
        == totals["feature_rows"],
        "Total row mismatch",
    )

    require(
        totals["source_rows"]
        == sum(int(r["rows"]) for r in records),
        "Manifest total mismatch",
    )

    market_report_path = (
        report_root / MARKET_REPORT_NAME
    )

    summary_path = (
        report_root / SUMMARY_NAME
    )

    output_manifest_path = (
        report_root / MANIFEST_NAME
    )

    atomic_csv(
        market_report_path,
        results,
        MARKET_COLUMNS,
    )

    summary = {
        "version": VERSION,
        "execution_status": "PASS",
        "market_files": len(results),
        "built_audits": built,
        "resumed_audits": resumed,
        "totals": totals,
        "markets_with_gaps": sum(
            int(item["gap_events"] > 0)
            for item in results
        ),
        "markets_with_zero_train_eligible_rows": sum(
            int(item["train_eligible_rows"] == 0)
            for item in results
        ),
        "feature_integrity_verified": True,
        "gap_audit_completed": True,
        "train_experiment_completed": False,
        "research_decision": "HOLD",
        "independent_oos_proven": False,
        "statistical_independence_proven": False,
        "profitability_proven": False,
        "approved_for_live_trading": False,
        "automated_trading_enabled": False,
        "generated_at_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "limitations": [
            "Gap screening does not repair missing candles.",
            "Eligibility is conservative and requires 120 contiguous bars.",
            "No event labels or future returns were analyzed.",
            "No train/test split was performed.",
            "No independent OOS validation was performed.",
            "No automated trading is enabled.",
        ],
    }

    atomic_json(
        summary_path,
        summary,
    )

    output_manifest = {
        "version": VERSION,
        "source_report_sha256": source_report_hashes,
        "input_market_files": len(records),
        "output_sha256": {
            MARKET_REPORT_NAME: sha256_file(
                market_report_path
            ),
            SUMMARY_NAME: sha256_file(
                summary_path
            ),
        },
    }

    atomic_json(
        output_manifest_path,
        output_manifest,
    )

    for filename, expected in (
        output_manifest["output_sha256"].items()
    ):
        actual = sha256_file(
            report_root / filename
        )

        require(
            expected == actual,
            f"Output SHA256 mismatch: {filename}",
        )

    print("=" * 65)
    print("PATTERN 256 FEATURE INTEGRITY & GAP AUDIT")
    print("=" * 65)
    print("Execution: PASS")
    print("Version:", VERSION)
    print("Markets:", len(results))
    print("Audited:", built)
    print("Resumed:", resumed)
    print("Feature rows:", totals["feature_rows"])
    print("Missing intervals:", totals["missing_intervals"])
    print("Gap events:", totals["gap_events"])
    print(
        "TRAIN-eligible rows:",
        totals["train_eligible_rows"],
    )
    print("Research decision: HOLD")
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
