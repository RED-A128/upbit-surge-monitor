#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Continuous Segment Rebuild
Clean V010

Research-only segment reconstruction.

- Verify Clean V009 source manifest
- Verify all original input hashes
- Rebuild features and signals independently
- Preserve original research files
- No synthetic candles
- No cross-gap calculations
- Atomic outputs
- SHA256 checkpoint validation
- Safe resume
- No evaluator approval
- No training or trading approval
"""

import argparse
import hashlib
import importlib.util
import json
import os
import re
import sys
import tempfile

from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd


VERSION = "Clean V010"
SOURCE_VERSION = "Clean V009"

SOURCE_DIR = (
    "data/reports/"
    "pattern_256_gap_exclusion_policy"
)

OUTPUT_DIR = (
    "data/reports/"
    "pattern_256_continuous_segments"
)

LAYER_DIRS = {
    "ohlcv": "data/ohlcv/d1",
    "features": "data/features/d1",
    "signals": "data/signals/256/d1",
}

TIME_COLUMN = "candle_date_time_utc"

EXPECTED_MARKETS = 290
EXPECTED_SEGMENTS = 307
EXPECTED_ROWS = 321481
EXPECTED_GAPS = 17
EXPECTED_BLOCKED_ROWS = 52175

# Conservative provisional warmup.
# This is not proof of the actual
# mathematical warmup requirement.
PROVISIONAL_WARMUP = 300

PRICE_COLUMNS = [
    "open",
    "high",
    "low",
    "close",
    "volume",
    "trade_value",
]

SIGNAL_COLUMNS = [
    "signal_setup",
    "signal_ready",
    "signal_trigger",
]


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def utc_now():
    return datetime.now(
        timezone.utc
    ).isoformat()


def sha256_file(path):
    digest = hashlib.sha256()

    with path.open("rb") as f:
        for block in iter(
            lambda: f.read(1024 * 1024),
            b"",
        ):
            digest.update(block)

    return digest.hexdigest()


def read_json(path):
    with path.open(
        "r",
        encoding="utf-8-sig",
    ) as f:
        return json.load(f)


def atomic_bytes(path, content):
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    fd, temp_name = tempfile.mkstemp(
        prefix=".tmp_",
        dir=str(path.parent),
    )

    try:
        with os.fdopen(fd, "wb") as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())

        os.replace(temp_name, path)

    finally:
        if os.path.exists(temp_name):
            os.unlink(temp_name)


def atomic_json(path, payload):
    content = json.dumps(
        payload,
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
        allow_nan=False,
    ) + "\n"

    atomic_bytes(
        path,
        content.encode("utf-8"),
    )


def atomic_csv(path, frame):
    content = frame.to_csv(
        index=False,
        lineterminator="\n",
    )

    atomic_bytes(
        path,
        content.encode("utf-8-sig"),
    )


def safe_path(base, relative):
    base = base.resolve()

    path = (
        base / relative
    ).resolve()

    require(
        path != base
        and base in path.parents,
        f"Unsafe path: {relative}",
    )

    return path


def valid_market(market):
    return bool(
        re.fullmatch(
            r"KRW-[A-Z0-9]+",
            market,
        )
    )


def load_module(path, module_name):
    require(
        path.is_file(),
        f"Missing implementation: {path}",
    )

    spec = (
        importlib.util.spec_from_file_location(
            module_name,
            path,
        )
    )

    require(
        spec is not None
        and spec.loader is not None,
        f"Module load failed: {path}",
    )

    module = (
        importlib.util.module_from_spec(spec)
    )

    sys.modules[module_name] = module

    spec.loader.exec_module(module)

    return module


def timestamps(frame):
    require(
        TIME_COLUMN in frame.columns,
        "Missing UTC timestamp column",
    )

    values = pd.to_datetime(
        frame[TIME_COLUMN],
        utc=True,
        errors="raise",
    ).reset_index(drop=True)

    require(
        not values.isna().any(),
        "Invalid timestamps",
    )

    require(
        not values.duplicated().any(),
        "Duplicate timestamps",
    )

    require(
        values.is_monotonic_increasing,
        "Unsorted timestamps",
    )

    require(
        (
            values
            == values.dt.normalize()
        ).all(),
        "Non-midnight daily timestamp",
    )

    return values


def verify_alignment(
    source,
    output,
    label,
):
    require(
        len(source) == len(output),
        f"{label}: row count mismatch",
    )

    require(
        timestamps(source).equals(
            timestamps(output)
        ),
        f"{label}: timestamp mismatch",
    )

    require(
        not output.columns.duplicated().any(),
        f"{label}: duplicate columns",
    )


def verify_continuity(frame):
    values = timestamps(frame)

    if len(values) > 1:
        require(
            (
                values.diff().iloc[1:]
                == pd.Timedelta(days=1)
            ).all(),
            "Non-continuous segment",
        )


def verify_market(frame, market):
    require(
        "market" in frame.columns,
        "Missing market column",
    )

    require(
        frame["market"].eq(market).all(),
        f"Market mismatch: {market}",
    )


def verify_ohlcv_unchanged(
    source,
    output,
    label,
):
    for column in PRICE_COLUMNS:
        require(
            column in source.columns,
            f"Missing source column: {column}",
        )

        require(
            column in output.columns,
            f"Missing output column: {column}",
        )

        left = pd.to_numeric(
            source[column],
            errors="raise",
        ).to_numpy(dtype=float)

        right = pd.to_numeric(
            output[column],
            errors="raise",
        ).to_numpy(dtype=float)

        require(
            np.isfinite(left).all()
            and np.isfinite(right).all(),
            f"{label}: invalid OHLCV",
        )

        require(
            np.array_equal(left, right),
            f"{label}: OHLCV changed: {column}",
        )


def verify_source(root):
    folder = root / SOURCE_DIR

    result_path = folder / "result.json"
    manifest_path = folder / "manifest.json"

    require(
        result_path.is_file(),
        "Missing V009 result.json",
    )

    require(
        manifest_path.is_file(),
        "Missing V009 manifest.json",
    )

    result = read_json(result_path)
    manifest = read_json(manifest_path)

    require(
        result["version"] == SOURCE_VERSION,
        "V009 result version mismatch",
    )

    require(
        manifest["version"] == SOURCE_VERSION,
        "V009 manifest version mismatch",
    )

    require(
        result["run_id"] == manifest["run_id"],
        "V009 run ID mismatch",
    )

    expected = {
        "execution": "PASS",
        "classification": (
            "POLICY_DEFINED_PENDING_INTEGRATION"
        ),
        "markets_checked": EXPECTED_MARKETS,
        "gap_free_markets": 274,
        "affected_markets": 16,
        "unique_gaps": EXPECTED_GAPS,
        "missing_days": 65,
        "continuous_segments": EXPECTED_SEGMENTS,
        "total_daily_rows": EXPECTED_ROWS,
        "existing_post_gap_rows_blocked": (
            EXPECTED_BLOCKED_ROWS
        ),
        "pre_gap_or_gap_free_rows": 269306,
        "raw_api_files_verified": 17,
        "layer_alignment_mismatches": 0,
        "policy_self_tests_passed": 13,
        "policy_self_tests_failed": 0,
        "data_readiness": "BLOCKED",
        "research_decision": "HOLD",
        "next_action": (
            "SEGMENT_REBUILD_AND_"
            "EVALUATOR_INTEGRATION_REQUIRED"
        ),
    }

    for field, value in expected.items():
        require(
            result.get(field) == value,
            f"V009 field mismatch: {field}",
        )

    for field in (
        "gap_policy_defined",
        "reference_window_policy_tested",
        "exclusion_plan_generated",
    ):
        require(
            result.get(field) is True,
            f"V009 policy not verified: {field}",
        )

    for field in (
        "source_csv_modified_by_audit",
        "segment_rebuild_completed",
        "gap_policy_implemented_in_evaluator",
        "actual_feature_warmup_verified",
        "actual_forward_horizon_verified",
        "remaining_rows_approved_for_backtest",
        "point_in_time_proven",
        "independent_oos_proven",
        "train_approved",
        "live_trading_approved",
    ):
        require(
            result.get(field) is False,
            f"Unexpected V009 approval: {field}",
        )

    required_files = {
        "result.json",
        "report.md",
        "policy.json",
        "policy_self_tests.json",
        "market_summary.csv",
        "gap_inventory.csv",
        "continuous_segments.csv",
        "exclusion_plan.csv",
    }

    require(
        set(manifest["sha256"])
        == required_files,
        "V009 manifest file set mismatch",
    )

    for relative, expected_hash in (
        manifest["sha256"].items()
    ):
        path = safe_path(folder, relative)

        require(
            path.is_file(),
            f"Missing V009 evidence: {relative}",
        )

        require(
            sha256_file(path) == expected_hash,
            f"V009 SHA256 mismatch: {relative}",
        )

    policy = read_json(
        folder / "policy.json"
    )

    require(
        policy == result["policy"],
        "V009 policy mismatch",
    )

    require(
        policy["synthetic_candles_allowed"]
        is False,
        "Synthetic candle policy mismatch",
    )

    require(
        policy["cross_gap_feature_history_allowed"]
        is False,
        "Cross-gap history policy mismatch",
    )

    require(
        policy["cross_gap_forward_window_allowed"]
        is False,
        "Cross-gap forward policy mismatch",
    )

    markets = {}

    for record in result["markets"]:
        market = record["market"]

        require(
            valid_market(market),
            f"Invalid market: {market}",
        )

        require(
            market not in markets,
            f"Duplicate market: {market}",
        )

        require(
            record["status"] == "PASS",
            f"Invalid market status: {market}",
        )

        require(
            record["alignment_verified"] is True,
            f"Invalid alignment: {market}",
        )

        markets[market] = record

    require(
        len(markets) == EXPECTED_MARKETS,
        "V009 market count mismatch",
    )

    require(
        sum(
            item["rows"]
            for item in markets.values()
        ) == EXPECTED_ROWS,
        "V009 row accounting mismatch",
    )

    require(
        sum(
            len(item["segments"])
            for item in markets.values()
        ) == EXPECTED_SEGMENTS,
        "V009 segment accounting mismatch",
    )

    print(
        "[PASS] V009 source integrity",
        flush=True,
    )

    return {
        "markets": markets,
        "result_sha256": sha256_file(
            result_path
        ),
        "manifest_sha256": sha256_file(
            manifest_path
        ),
    }


def input_paths(root, market):
    return {
        layer: root / folder / f"{market}.csv"
        for layer, folder in LAYER_DIRS.items()
    }


def verify_market_inputs(
    root,
    market,
    record,
):
    paths = input_paths(
        root,
        market,
    )

    hashes = {}

    for layer, path in paths.items():
        require(
            path.is_file(),
            f"Missing input: {path}",
        )

        actual = sha256_file(path)

        require(
            actual
            == record["input_sha256"][layer],
            f"Input SHA256 changed: "
            f"{market} {layer}",
        )

        hashes[layer] = actual

    return paths, hashes


def verify_market_sets(root, markets):
    expected = set(markets)

    for layer, relative in (
        LAYER_DIRS.items()
    ):
        folder = root / relative

        require(
            folder.is_dir(),
            f"Missing layer: {folder}",
        )

        actual = {
            path.stem
            for path in folder.glob("KRW-*.csv")
        }

        require(
            actual == expected,
            f"Market set changed: {layer}",
        )


def verify_segment_inventory(
    source,
    market,
    record,
):
    require(
        len(source) == record["rows"],
        f"Source row mismatch: {market}",
    )

    values = timestamps(source)

    expected_index = 0
    previous_end = None

    for number, segment in enumerate(
        record["segments"],
        start=1,
    ):
        require(
            segment["market"] == market,
            "Segment market mismatch",
        )

        require(
            segment["segment_id"] == number,
            "Segment ID mismatch",
        )

        start = int(segment["start_index"])
        end = int(segment["end_index"])

        require(
            start == expected_index,
            "Segment index discontinuity",
        )

        require(
            0 <= start <= end < len(source),
            "Segment index outside source",
        )

        require(
            end - start + 1 == segment["rows"],
            "Segment row count mismatch",
        )

        start_time = pd.Timestamp(
            segment["start_utc"]
        )

        end_time = pd.Timestamp(
            segment["end_utc"]
        )

        require(
            values.iloc[start] == start_time,
            "Segment start timestamp mismatch",
        )

        require(
            values.iloc[end] == end_time,
            "Segment end timestamp mismatch",
        )

        part = source.iloc[
            start:end + 1
        ].reset_index(drop=True)

        verify_continuity(part)

        if previous_end is not None:
            require(
                start_time - previous_end
                > pd.Timedelta(days=1),
                "Invalid segment separation",
            )

        previous_end = end_time
        expected_index = end + 1

    require(
        expected_index == len(source),
        "Segment coverage mismatch",
    )


def build_segment(
    source,
    market,
    feature_module,
    detector_module,
):
    source = source.copy(
        deep=True
    ).reset_index(drop=True)

    original = source.copy(deep=True)

    verify_continuity(source)
    verify_market(source, market)

    features = (
        feature_module.build_features(
            source.copy(deep=True)
        )
    )

    feature_module.validate_feature_dataframe(
        source_df=original,
        feature_df=features,
        expected_market=market,
    )

    verify_alignment(
        original,
        features,
        "Features",
    )

    verify_market(
        features,
        market,
    )

    verify_ohlcv_unchanged(
        original,
        features,
        "Features",
    )

    signals = (
        detector_module.build_detector_dataframe(
            features.copy(deep=True)
        )
    )

    detector_module.validate_detector_dataframe(
        feature_df=features,
        detector_df=signals,
        expected_market=market,
    )

    verify_alignment(
        original,
        signals,
        "Signals",
    )

    verify_market(
        signals,
        market,
    )

    required_features = list(
        feature_module.get_required_feature_columns()
    )

    required_signals = list(
        detector_module.DETECTOR_COLUMNS
    )

    require(
        bool(required_features),
        "Empty feature column definition",
    )

    require(
        bool(required_signals),
        "Empty signal column definition",
    )

    require(
        set(required_features).issubset(
            features.columns
        ),
        "Missing required feature columns",
    )

    require(
        set(required_signals).issubset(
            signals.columns
        ),
        "Missing required detector columns",
    )

    for column in SIGNAL_COLUMNS:
        require(
            column in signals.columns,
            f"Missing signal flag: {column}",
        )

        values = pd.to_numeric(
            signals[column],
            errors="raise",
        )

        require(
            values.isin([0, 1]).all(),
            f"Invalid signal flag: {column}",
        )

    return (
        features.reset_index(drop=True),
        signals.reset_index(drop=True),
    )


def compare_prefix(
    full_frame,
    prefix_frame,
    columns,
    label,
):
    expected = full_frame.iloc[
        :len(prefix_frame)
    ].reset_index(drop=True)

    actual = prefix_frame.reset_index(
        drop=True
    )

    verify_alignment(
        expected,
        actual,
        label,
    )

    for column in dict.fromkeys(columns):
        left = expected[column]
        right = actual[column]

        if column == TIME_COLUMN:
            continue

        if (
            pd.api.types.is_numeric_dtype(left)
            and pd.api.types.is_numeric_dtype(right)
        ):
            a = left.to_numpy(dtype=float)
            b = right.to_numpy(dtype=float)

            require(
                np.allclose(
                    a,
                    b,
                    rtol=1e-8,
                    atol=1e-10,
                    equal_nan=True,
                ),
                f"{label}: prefix mismatch: "
                f"{column}",
            )

        else:
            a = left.astype(
                "string"
            ).fillna("<NA>")

            b = right.astype(
                "string"
            ).fillna("<NA>")

            require(
                a.equals(b),
                f"{label}: prefix mismatch: "
                f"{column}",
            )


def verify_segment_prefixes(
    source,
    features,
    signals,
    market,
    feature_module,
    detector_module,
):
    rows = len(source)

    if rows < 2:
        return {
            "checks": 0,
            "status": "TOO_SHORT_FOR_PREFIX",
        }

    cutoffs = sorted({
        max(
            1,
            min(
                rows - 1,
                int(rows * fraction),
            ),
        )
        for fraction in (
            0.40,
            0.65,
            0.85,
        )
    })

    feature_columns = (
        feature_module.get_required_feature_columns()
    )

    signal_columns = (
        detector_module.DETECTOR_COLUMNS
    )

    for cutoff in cutoffs:
        prefix = source.iloc[
            :cutoff
        ].copy().reset_index(drop=True)

        replay_features, replay_signals = (
            build_segment(
                prefix,
                market,
                feature_module,
                detector_module,
            )
        )

        compare_prefix(
            features,
            replay_features,
            feature_columns,
            "Feature replay",
        )

        compare_prefix(
            signals,
            replay_signals,
            signal_columns,
            "Signal replay",
        )

    return {
        "checks": len(cutoffs),
        "status": "PASS",
    }


def build_policy_mask(
    source,
    features,
    signals,
    market,
    segment_id,
    feature_module,
):
    rows = len(source)

    values = timestamps(source)

    required = list(
        feature_module.get_required_feature_columns()
    )

    numeric_columns = [
        column
        for column in required
        if pd.api.types.is_numeric_dtype(
            features[column]
        )
    ]

    require(
        bool(numeric_columns),
        "No numeric features available",
    )

    matrix = features[
        numeric_columns
    ].to_numpy(dtype=float)

    finite = np.isfinite(
        matrix
    ).all(axis=1)

    positions = np.arange(
        1,
        rows + 1,
    )

    warmup_pass = (
        positions >= PROVISIONAL_WARMUP
    )

    # This cutoff is a current-time check,
    # not historical availability proof.
    now = pd.Timestamp.now(tz="UTC")

    candle_closed = (
        values + pd.Timedelta(days=1)
        <= now
    ).to_numpy()

    provisional_pass = (
        finite
        & warmup_pass
        & candle_closed
    )

    reasons = []

    for index in range(rows):
        row_reasons = []

        if not warmup_pass[index]:
            row_reasons.append(
                "PROVISIONAL_WARMUP"
            )

        if not finite[index]:
            row_reasons.append(
                "NONFINITE_REQUIRED_FEATURE"
            )

        if not candle_closed[index]:
            row_reasons.append(
                "CANDLE_NOT_CLOSED"
            )

        if not row_reasons:
            row_reasons.append(
                "PENDING_EVALUATOR_INTEGRATION"
            )

        reasons.append(
            "|".join(row_reasons)
        )

    mask = pd.DataFrame({
        "market": market,
        TIME_COLUMN: values,
        "segment_id": segment_id,
        "segment_row_number": positions,
        "segment_rows": rows,
        "segment_start_utc": (
            values.iloc[0].isoformat()
        ),
        "segment_end_utc": (
            values.iloc[-1].isoformat()
        ),
        "history_bars_available": positions,
        "forward_bars_available": (
            rows - positions
        ),
        "provisional_warmup_pass": (
            warmup_pass.astype(int)
        ),
        "required_numeric_features_finite": (
            finite.astype(int)
        ),
        "candle_closed_at_rebuild": (
            candle_closed.astype(int)
        ),
        "provisional_row_pass": (
            provisional_pass.astype(int)
        ),
        "backtest_approved": 0,
        "reason": reasons,
    })

    for column in SIGNAL_COLUMNS:
        mask[column] = pd.to_numeric(
            signals[column],
            errors="raise",
        ).astype(int).to_numpy()

    return mask


def output_paths(folder, market):
    return {
        "features": (
            folder / "rebuilt_features"
            / "d1" / f"{market}.csv"
        ),
        "signals": (
            folder / "rebuilt_signals"
            / "d1" / f"{market}.csv"
        ),
        "policy_mask": (
            folder / "policy_masks"
            / "d1" / f"{market}.csv"
        ),
    }


def checkpoint_paths(folder, market):
    return (
        folder / "checkpoints"
        / f"{market}.json",

        folder / "checkpoints"
        / f"{market}.sha256",
    )


def validate_saved_outputs(
    folder,
    market,
    source,
    source_record,
    record,
):
    paths = output_paths(
        folder,
        market,
    )

    frames = {}

    require(
        record["market"] == market,
        "Saved market mismatch",
    )

    require(
        record["status"] == "PASS",
        "Saved status mismatch",
    )

    require(
        record["rows"] == len(source),
        "Saved row count mismatch",
    )

    require(
        record["segment_count"]
        == len(source_record["segments"]),
        "Saved segment count mismatch",
    )

    require(
        set(record["output_sha256"])
        == set(paths),
        "Saved output hash set mismatch",
    )

    for layer, path in paths.items():
        require(
            path.is_file(),
            f"Missing saved output: {path}",
        )

        require(
            sha256_file(path)
            == record["output_sha256"][layer],
            f"Saved SHA256 mismatch: {path}",
        )

        frame = pd.read_csv(
            path,
            encoding="utf-8-sig",
            low_memory=False,
        )

        verify_alignment(
            source,
            frame,
            layer,
        )

        verify_market(
            frame,
            market,
        )

        frames[layer] = frame

    mask = frames["policy_mask"]

    require(
        mask["backtest_approved"].eq(0).all(),
        "Unexpected backtest approval",
    )

    require(
        mask["provisional_row_pass"]
        .isin([0, 1]).all(),
        "Invalid provisional mask",
    )

    require(
        int(
            mask["provisional_row_pass"].sum()
        ) == record["provisional_rows"],
        "Provisional row accounting mismatch",
    )

    require(
        record["provisional_rows"]
        + record["provisional_blocked_rows"]
        == len(source),
        "Saved row accounting mismatch",
    )

    for segment in source_record["segments"]:
        start = segment["start_index"]
        end = segment["end_index"]

        part = mask.iloc[
            start:end + 1
        ]

        require(
            part["segment_id"].eq(
                segment["segment_id"]
            ).all(),
            "Saved segment ID mismatch",
        )

        require(
            part["segment_row_number"].tolist()
            == list(
                range(
                    1,
                    len(part) + 1,
                )
            ),
            "Saved segment position mismatch",
        )

        require(
            part["forward_bars_available"].tolist()
            == list(
                range(
                    len(part) - 1,
                    -1,
                    -1,
                )
            ),
            "Saved forward capacity mismatch",
        )

        require(
            part.loc[
                part["segment_row_number"]
                < PROVISIONAL_WARMUP,
                "provisional_row_pass",
            ].eq(0).all(),
            "Warmup rows unexpectedly accepted",
        )

    for column in SIGNAL_COLUMNS:
        require(
            frames["signals"][column]
            .isin([0, 1]).all(),
            "Saved signal flag invalid",
        )

        require(
            np.array_equal(
                frames["signals"][column]
                .to_numpy(dtype=int),

                mask[column]
                .to_numpy(dtype=int),
            ),
            "Saved signal mask mismatch",
        )

    return frames


def load_checkpoint(
    folder,
    market,
    context,
    source,
    source_record,
):
    path, checksum = checkpoint_paths(
        folder,
        market,
    )

    try:
        if not (
            path.is_file()
            and checksum.is_file()
        ):
            return None

        expected_hash = checksum.read_text(
            encoding="utf-8"
        ).strip()

        if sha256_file(path) != expected_hash:
            return None

        payload = read_json(path)

        if payload["context"] != context:
            return None

        record = payload["record"]

        validate_saved_outputs(
            folder,
            market,
            source,
            source_record,
            record,
        )

        return record

    except Exception as exc:
        print(
            f"[REBUILD CHECKPOINT] "
            f"{market}: {exc}",
            flush=True,
        )

        return None


def save_checkpoint(
    folder,
    market,
    context,
    record,
):
    path, checksum = checkpoint_paths(
        folder,
        market,
    )

    atomic_json(
        path,
        {
            "context": context,
            "record": record,
        },
    )

    atomic_bytes(
        checksum,
        (
            sha256_file(path) + "\n"
        ).encode("utf-8"),
    )


def process_market(
    root,
    folder,
    market,
    source_record,
    source_info,
    implementation_hashes,
    feature_module,
    detector_module,
    refresh,
):
    paths, hashes = verify_market_inputs(
        root,
        market,
        source_record,
    )

    source = (
        feature_module.load_source_ohlcv(
            paths["ohlcv"],
            expected_market=market,
        )
    )

    source = source.reset_index(
        drop=True
    )

    verify_market(
        source,
        market,
    )

    verify_segment_inventory(
        source,
        market,
        source_record,
    )

    context = {
        "version": VERSION,
        "market": market,
        "source_result_sha256": (
            source_info["result_sha256"]
        ),
        "source_manifest_sha256": (
            source_info["manifest_sha256"]
        ),
        "implementation_sha256": (
            implementation_hashes
        ),
        "input_sha256": hashes,
        "provisional_warmup": (
            PROVISIONAL_WARMUP
        ),
        "rebuild_utc_date": (
            datetime.now(timezone.utc)
            .date().isoformat()
        ),
        "pandas_version": pd.__version__,
        "numpy_version": np.__version__,
        "python_version": sys.version,
    }

    if not refresh:
        cached = load_checkpoint(
            folder,
            market,
            context,
            source,
            source_record,
        )

        if cached is not None:
            verify_market_inputs(
                root,
                market,
                source_record,
            )

            print(
                f"[RESUME] {market}",
                flush=True,
            )

            return {
                **cached,
                "resumed": True,
            }

    feature_parts = []
    signal_parts = []
    mask_parts = []
    segment_reports = []

    for segment in source_record["segments"]:
        segment_id = segment["segment_id"]

        start = segment["start_index"]
        end = segment["end_index"]

        part = source.iloc[
            start:end + 1
        ].copy().reset_index(drop=True)

        print(
            f"[SEGMENT] {market} "
            f"id={segment_id} "
            f"rows={len(part)}",
            flush=True,
        )

        features, signals = build_segment(
            part,
            market,
            feature_module,
            detector_module,
        )

        replay = verify_segment_prefixes(
            part,
            features,
            signals,
            market,
            feature_module,
            detector_module,
        )

        mask = build_policy_mask(
            part,
            features,
            signals,
            market,
            segment_id,
            feature_module,
        )

        provisional_rows = int(
            mask["provisional_row_pass"].sum()
        )

        feature_parts.append(features)
        signal_parts.append(signals)
        mask_parts.append(mask)

        segment_reports.append({
            "market": market,
            "segment_id": segment_id,
            "start_utc": segment["start_utc"],
            "end_utc": segment["end_utc"],
            "rows": len(part),
            "post_gap": segment_id > 1,
            "provisional_rows": (
                provisional_rows
            ),
            "provisional_blocked_rows": (
                len(part) - provisional_rows
            ),
            "prefix_checks": replay["checks"],
            "prefix_status": replay["status"],
            "signal_setup": int(
                signals["signal_setup"].sum()
            ),
            "signal_ready": int(
                signals["signal_ready"].sum()
            ),
            "signal_trigger": int(
                signals["signal_trigger"].sum()
            ),
        })

    combined_features = pd.concat(
        feature_parts,
        ignore_index=True,
    )

    combined_signals = pd.concat(
        signal_parts,
        ignore_index=True,
    )

    combined_mask = pd.concat(
        mask_parts,
        ignore_index=True,
    )

    frames = {
        "features": combined_features,
        "signals": combined_signals,
        "policy_mask": combined_mask,
    }

    destinations = output_paths(
        folder,
        market,
    )

    for layer, frame in frames.items():
        verify_alignment(
            source,
            frame,
            layer,
        )

        atomic_csv(
            destinations[layer],
            frame,
        )

    output_hashes = {
        layer: sha256_file(path)
        for layer, path in destinations.items()
    }

    provisional_rows = int(
        combined_mask[
            "provisional_row_pass"
        ].sum()
    )

    record = {
        "market": market,
        "status": "PASS",
        "rows": len(source),
        "segment_count": len(segment_reports),
        "input_sha256": hashes,
        "output_sha256": output_hashes,
        "provisional_rows": provisional_rows,
        "provisional_blocked_rows": (
            len(source) - provisional_rows
        ),
        "post_gap_rows_rebuilt": sum(
            item["rows"]
            for item in segment_reports
            if item["post_gap"]
        ),
        "post_gap_provisional_rows": sum(
            item["provisional_rows"]
            for item in segment_reports
            if item["post_gap"]
        ),
        "prefix_checks": sum(
            item["prefix_checks"]
            for item in segment_reports
        ),
        "segments": segment_reports,
    }

    validate_saved_outputs(
        folder,
        market,
        source,
        source_record,
        record,
    )

    verify_market_inputs(
        root,
        market,
        source_record,
    )

    save_checkpoint(
        folder,
        market,
        context,
        record,
    )

    print(
        f"[PASS] {market} "
        f"rows={len(source)} "
        f"segments={len(segment_reports)} "
        f"provisional={provisional_rows}",
        flush=True,
    )

    return {
        **record,
        "resumed": False,
    }


def verify_final_inputs(
    root,
    source_info,
    implementation_paths,
    implementation_hashes,
):
    verify_market_sets(
        root,
        source_info["markets"],
    )

    checked = 0

    for market, record in (
        source_info["markets"].items()
    ):
        verify_market_inputs(
            root,
            market,
            record,
        )

        checked += 3

    require(
        checked == 870,
        "Final input file count mismatch",
    )

    for name, path in (
        implementation_paths.items()
    ):
        require(
            sha256_file(path)
            == implementation_hashes[name],
            f"Implementation changed: {name}",
        )

    final_source = verify_source(root)

    for key in (
        "result_sha256",
        "manifest_sha256",
    ):
        require(
            final_source[key]
            == source_info[key],
            f"V009 source changed: {key}",
        )

    print(
        "[PASS] 870 original CSV hashes",
        flush=True,
    )


def write_reports(
    folder,
    records,
    source_info,
    implementation_hashes,
    run_id,
):
    segments = [
        segment
        for record in records
        for segment in record["segments"]
    ]

    rows = sum(
        record["rows"]
        for record in records
    )

    provisional = sum(
        record["provisional_rows"]
        for record in records
    )

    blocked = sum(
        record["provisional_blocked_rows"]
        for record in records
    )

    post_gap_rebuilt = sum(
        record["post_gap_rows_rebuilt"]
        for record in records
    )

    post_gap_provisional = sum(
        record["post_gap_provisional_rows"]
        for record in records
    )

    prefix_checks = sum(
        record["prefix_checks"]
        for record in records
    )

    require(
        len(records) == EXPECTED_MARKETS,
        "Final market count mismatch",
    )

    require(
        len(segments) == EXPECTED_SEGMENTS,
        "Final segment count mismatch",
    )

    require(
        rows == EXPECTED_ROWS,
        "Final row count mismatch",
    )

    require(
        provisional + blocked == rows,
        "Final provisional accounting mismatch",
    )

    require(
        post_gap_rebuilt
        == EXPECTED_BLOCKED_ROWS,
        "Post-gap rebuild count mismatch",
    )

    require(
        sum(
            segment["rows"]
            for segment in segments
        ) == rows,
        "Segment row accounting mismatch",
    )

    require(
        all(
            segment["prefix_status"]
            in (
                "PASS",
                "TOO_SHORT_FOR_PREFIX",
            )
            for segment in segments
        ),
        "Unexpected prefix status",
    )

    market_summary = []

    for record in records:
        market_summary.append({
            key: value
            for key, value in record.items()
            if key not in (
                "segments",
                "input_sha256",
                "output_sha256",
            )
        })

    atomic_csv(
        folder / "market_summary.csv",
        pd.DataFrame(market_summary),
    )

    atomic_csv(
        folder / "segment_summary.csv",
        pd.DataFrame(segments),
    )

    recovery = pd.DataFrame([
        {
            "market": record["market"],
            "post_gap_rows_rebuilt": (
                record["post_gap_rows_rebuilt"]
            ),
            "post_gap_provisional_rows": (
                record["post_gap_provisional_rows"]
            ),
            "post_gap_provisional_blocked": (
                record["post_gap_rows_rebuilt"]
                - record["post_gap_provisional_rows"]
            ),
        }
        for record in records
        if record["post_gap_rows_rebuilt"] > 0
    ])

    require(
        len(recovery) == 16,
        "Recovery market count mismatch",
    )

    atomic_csv(
        folder / "post_gap_recovery.csv",
        recovery,
    )

    result = {
        "version": VERSION,
        "created_utc": utc_now(),
        "run_id": run_id,
        "execution": "PASS",
        "classification": (
            "SEGMENT_REBUILT_"
            "PENDING_EVALUATOR_INTEGRATION"
        ),
        "source_version": SOURCE_VERSION,
        "source_result_sha256": (
            source_info["result_sha256"]
        ),
        "source_manifest_sha256": (
            source_info["manifest_sha256"]
        ),
        "implementation_sha256": (
            implementation_hashes
        ),
        "candidate": "d1|DOWN|LOW",
        "markets_rebuilt": len(records),
        "segments_rebuilt": len(segments),
        "total_rows_rebuilt": rows,
        "original_input_files_verified": 870,
        "rebuilt_output_files": (
            len(records) * 3
        ),
        "post_gap_rows_rebuilt": (
            post_gap_rebuilt
        ),
        "post_gap_provisional_rows": (
            post_gap_provisional
        ),
        "post_gap_provisional_blocked": (
            post_gap_rebuilt
            - post_gap_provisional
        ),
        "provisional_warmup_bars": (
            PROVISIONAL_WARMUP
        ),
        "provisional_rows": provisional,
        "provisional_blocked_rows": blocked,
        "prefix_checks_passed": prefix_checks,
        "prefix_checks_failed": 0,
        "segments_without_prefix_check": sum(
            segment["prefix_status"]
            == "TOO_SHORT_FOR_PREFIX"
            for segment in segments
        ),
        "resumed_markets": sum(
            record["resumed"]
            for record in records
        ),
        "segment_rebuild_completed": True,
        "sampled_segment_prefix_replay_passed": (
            prefix_checks > 0
        ),
        "actual_feature_warmup_verified": False,
        "actual_forward_horizon_verified": False,
        "gap_policy_implemented_in_evaluator": False,
        "source_csv_modified_by_rebuild": False,
        "synthetic_candles_created": 0,
        "remaining_rows_approved_for_backtest": False,
        "historical_no_trade_proven": False,
        "point_in_time_proven": False,
        "independent_oos_proven": False,
        "profitability_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "data_readiness": "BLOCKED",
        "research_decision": "HOLD",
        "next_action": (
            "ACTUAL_WARMUP_HORIZON_AND_"
            "EVALUATOR_INTEGRATION_REQUIRED"
        ),
        "markets": records,
    }

    atomic_json(
        folder / "result.json",
        result,
    )

    lines = [
        "# Pattern 256 Continuous Segment Rebuild",
        "",
        f"Version: {VERSION}",
        f"Run ID: {run_id}",
        "",
        "| Metric | Result |",
        "|---|---|",
    ]

    summary_fields = [
        "execution",
        "classification",
        "markets_rebuilt",
        "segments_rebuilt",
        "total_rows_rebuilt",
        "original_input_files_verified",
        "rebuilt_output_files",
        "post_gap_rows_rebuilt",
        "post_gap_provisional_rows",
        "post_gap_provisional_blocked",
        "provisional_warmup_bars",
        "provisional_rows",
        "provisional_blocked_rows",
        "prefix_checks_passed",
        "prefix_checks_failed",
        "segments_without_prefix_check",
        "resumed_markets",
        "data_readiness",
        "research_decision",
        "next_action",
    ]

    for field in summary_fields:
        lines.append(
            f"| {field} | {result[field]} |"
        )

    lines.extend([
        "",
        "## Limitations",
        "",
        "- Original research CSV files are preserved.",
        "- Rebuilt files are research-only outputs.",
        "- All calculations restart per segment.",
        "- The 300-bar warmup is provisional.",
        "- Sampled prefix replay is not exhaustive PIT proof.",
        "- Actual evaluator integration remains pending.",
        "- No rows are approved for backtesting.",
        "- Training and live trading remain unapproved.",
        "",
    ])

    atomic_bytes(
        folder / "report.md",
        "\n".join(lines).encode("utf-8"),
    )

    report_names = [
        "result.json",
        "report.md",
        "market_summary.csv",
        "segment_summary.csv",
        "post_gap_recovery.csv",
    ]

    rebuilt_hashes = {}

    for record in records:
        paths = output_paths(
            folder,
            record["market"],
        )

        for layer, path in paths.items():
            expected = record[
                "output_sha256"
            ][layer]

            require(
                sha256_file(path) == expected,
                f"Final output hash mismatch: {path}",
            )

            relative = path.relative_to(
                folder
            ).as_posix()

            rebuilt_hashes[relative] = expected

    require(
        len(rebuilt_hashes) == 870,
        "Rebuilt manifest count mismatch",
    )

    manifest = {
        "version": VERSION,
        "created_utc": utc_now(),
        "run_id": run_id,
        "source_version": SOURCE_VERSION,
        "source_result_sha256": (
            source_info["result_sha256"]
        ),
        "source_manifest_sha256": (
            source_info["manifest_sha256"]
        ),
        "implementation_sha256": (
            implementation_hashes
        ),
        "sha256": {
            name: sha256_file(
                folder / name
            )
            for name in report_names
        },
        "rebuilt_sha256": rebuilt_hashes,
    }

    atomic_json(
        folder / "manifest.json",
        manifest,
    )

    return result


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--root",
        required=True,
    )

    parser.add_argument(
        "--run-id",
        default="manual",
    )

    parser.add_argument(
        "--refresh",
        action="store_true",
    )

    args = parser.parse_args()

    root = Path(args.root).resolve()

    require(
        root.is_dir(),
        f"Missing project root: {root}",
    )

    folder = root / OUTPUT_DIR

    folder.mkdir(
        parents=True,
        exist_ok=True,
    )

    print("=" * 72)
    print("Pattern 256 Continuous Segment Rebuild")
    print("Version:", VERSION)
    print("Root:", root)
    print("Run ID:", args.run_id)
    print("Refresh:", args.refresh)
    print("Provisional warmup:", PROVISIONAL_WARMUP)
    print("=" * 72)

    source_info = verify_source(root)

    verify_market_sets(
        root,
        source_info["markets"],
    )

    implementation_paths = {
        "rebuild_script": (
            Path(__file__).resolve()
        ),
        "build_features.py": (
            root / "build_features.py"
        ),
        "detect_256.py": (
            root / "detect_256.py"
        ),
    }

    implementation_hashes = {
        name: sha256_file(path)
        for name, path
        in implementation_paths.items()
    }

    sys.path.insert(
        0,
        str(root),
    )

    feature_module = load_module(
        implementation_paths[
            "build_features.py"
        ],
        "segment_rebuild_features",
    )

    detector_module = load_module(
        implementation_paths[
            "detect_256.py"
        ],
        "segment_rebuild_detector",
    )

    for name in (
        "load_source_ohlcv",
        "build_features",
        "validate_feature_dataframe",
        "get_required_feature_columns",
    ):
        require(
            callable(
                getattr(
                    feature_module,
                    name,
                    None,
                )
            ),
            f"Missing feature function: {name}",
        )

    for name in (
        "build_detector_dataframe",
        "validate_detector_dataframe",
    ):
        require(
            callable(
                getattr(
                    detector_module,
                    name,
                    None,
                )
            ),
            f"Missing detector function: {name}",
        )

    require(
        hasattr(
            detector_module,
            "DETECTOR_COLUMNS",
        ),
        "Missing DETECTOR_COLUMNS",
    )

    records = []

    markets = sorted(
        source_info["markets"]
    )

    for index, market in enumerate(
        markets,
        start=1,
    ):
        print(
            f"\n[{index}/{len(markets)}] "
            f"{market}",
            flush=True,
        )

        record = process_market(
            root=root,
            folder=folder,
            market=market,
            source_record=(
                source_info["markets"][market]
            ),
            source_info=source_info,
            implementation_hashes=(
                implementation_hashes
            ),
            feature_module=feature_module,
            detector_module=detector_module,
            refresh=args.refresh,
        )

        records.append(record)

    verify_final_inputs(
        root,
        source_info,
        implementation_paths,
        implementation_hashes,
    )

    result = write_reports(
        folder,
        records,
        source_info,
        implementation_hashes,
        args.run_id,
    )

    print()
    print("=" * 72)
    print("[PASS] Clean V010")
    print(
        "Markets rebuilt:",
        result["markets_rebuilt"],
    )
    print(
        "Segments rebuilt:",
        result["segments_rebuilt"],
    )
    print(
        "Rows rebuilt:",
        result["total_rows_rebuilt"],
    )
    print(
        "Post-gap rows rebuilt:",
        result["post_gap_rows_rebuilt"],
    )
    print(
        "Post-gap provisional rows:",
        result["post_gap_provisional_rows"],
    )
    print(
        "Prefix checks:",
        result["prefix_checks_passed"],
    )
    print(
        "Resumed markets:",
        result["resumed_markets"],
    )
    print("Data readiness: BLOCKED")
    print("Research decision: HOLD")
    print("Output:", folder)
    print("=" * 72)

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())

    except KeyboardInterrupt:
        print(
            "[STOP] Interrupted. "
            "Completed checkpoints retained.",
            file=sys.stderr,
            flush=True,
        )

        sys.exit(130)

    except Exception as exc:
        import traceback

        traceback.print_exc()

        print(
            "[FAIL]",
            type(exc).__name__,
            str(exc),
            file=sys.stderr,
            flush=True,
        )

        sys.exit(1)
