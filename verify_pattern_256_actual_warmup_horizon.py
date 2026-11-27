#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Actual Warmup and Horizon Verification
Clean V011

Research-only verification.

- Verify Clean V010 manifest and source lineage
- Preserve original input and rebuilt CSV files
- Compare warmup sensitivity using truncated history
- Verify continuous forward horizon capacity
- Separate complete and incomplete observation windows
- Validate SHA256 checkpoints
- Support safe resume
- No evaluator, training or trading approval
"""

import argparse
import hashlib
import json
import sys
import traceback

from pathlib import Path

import numpy as np
import pandas as pd

import rebuild_pattern_256_continuous_segments as v010


VERSION = "Clean V011"
SOURCE_VERSION = "Clean V010"

SOURCE_DIR = (
    "data/reports/"
    "pattern_256_continuous_segments"
)

OUTPUT_DIR = (
    "data/reports/"
    "pattern_256_actual_warmup_horizon"
)

TIME_COLUMN = "candle_date_time_utc"

EXPECTED_MARKETS = 290
EXPECTED_SEGMENTS = 307
EXPECTED_ROWS = 321481

WARMUP_CANDIDATES = (
    227,
    250,
    300,
    400,
    500,
)

HORIZONS = (
    24,
    72,
    168,
)

TARGET_RETURN = 0.30

RTOL = 1e-6
ATOL = 1e-8

SIGNAL_COLUMNS = (
    "signal_setup",
    "signal_ready",
    "signal_trigger",
)

REFERENCE_MIN_BARS = 600

REQUIRED_REPORTS = {
    "result.json",
    "report.md",
    "market_summary.csv",
    "segment_summary.csv",
    "post_gap_recovery.csv",
}


require = v010.require
sha256_file = v010.sha256_file
read_json = v010.read_json
atomic_json = v010.atomic_json
atomic_csv = v010.atomic_csv
atomic_bytes = v010.atomic_bytes
safe_path = v010.safe_path
utc_now = v010.utc_now


def read_csv(path):
    require(
        path.is_file(),
        f"Missing CSV: {path}",
    )

    return pd.read_csv(
        path,
        encoding="utf-8-sig",
        low_memory=False,
        float_precision="round_trip",
    )


def hash_payload(payload):
    raw = json.dumps(
        payload,
        sort_keys=True,
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")

    return hashlib.sha256(raw).hexdigest()


def verify_hash_map(folder, hashes):
    require(
        isinstance(hashes, dict),
        "Invalid hash inventory",
    )

    for relative, expected in hashes.items():
        path = safe_path(
            folder,
            relative,
        )

        require(
            path.is_file(),
            f"Missing evidence: {path}",
        )

        require(
            sha256_file(path) == expected,
            f"SHA256 mismatch: {path}",
        )


def verify_v010(root):
    folder = root / SOURCE_DIR

    result_path = folder / "result.json"
    manifest_path = folder / "manifest.json"

    result = read_json(result_path)
    manifest = read_json(manifest_path)

    require(
        result["version"] == SOURCE_VERSION,
        "Unexpected source result version",
    )

    require(
        manifest["version"] == SOURCE_VERSION,
        "Unexpected source manifest version",
    )

    require(
        result["run_id"] == manifest["run_id"],
        "V010 run ID mismatch",
    )

    expected = {
        "execution": "PASS",
        "markets_rebuilt": EXPECTED_MARKETS,
        "segments_rebuilt": EXPECTED_SEGMENTS,
        "total_rows_rebuilt": EXPECTED_ROWS,
        "original_input_files_verified": 870,
        "rebuilt_output_files": 870,
        "prefix_checks_failed": 0,
        "synthetic_candles_created": 0,
        "provisional_warmup_bars": 300,
        "research_decision": "HOLD",
        "data_readiness": "BLOCKED",
    }

    for field, value in expected.items():
        require(
            result.get(field) == value,
            f"V010 result mismatch: {field}",
        )

    require(
        result.get(
            "segment_rebuild_completed"
        ) is True,
        "V010 rebuild not completed",
    )

    for field in (
        "actual_feature_warmup_verified",
        "actual_forward_horizon_verified",
        "gap_policy_implemented_in_evaluator",
        "remaining_rows_approved_for_backtest",
        "train_approved",
        "live_trading_approved",
    ):
        require(
            result.get(field) is False,
            f"Unexpected V010 approval: {field}",
        )

    require(
        set(manifest["sha256"])
        == REQUIRED_REPORTS,
        "V010 report inventory mismatch",
    )

    verify_hash_map(
        folder,
        manifest["sha256"],
    )

    verify_hash_map(
        folder,
        manifest["rebuilt_sha256"],
    )

    require(
        len(manifest["rebuilt_sha256"]) == 870,
        "V010 rebuilt file count mismatch",
    )

    records = {}

    for record in result["markets"]:
        market = record["market"]

        require(
            v010.valid_market(market),
            f"Invalid market: {market}",
        )

        require(
            market not in records,
            f"Duplicate market: {market}",
        )

        require(
            record["status"] == "PASS",
            f"V010 market failed: {market}",
        )

        records[market] = record

    require(
        len(records) == EXPECTED_MARKETS,
        "V010 market inventory mismatch",
    )

    require(
        sum(
            item["rows"]
            for item in records.values()
        ) == EXPECTED_ROWS,
        "V010 row inventory mismatch",
    )

    require(
        sum(
            item["segment_count"]
            for item in records.values()
        ) == EXPECTED_SEGMENTS,
        "V010 segment inventory mismatch",
    )

    expected_rebuilt = {}

    for market, record in records.items():
        paths = v010.output_paths(
            folder,
            market,
        )

        require(
            set(paths)
            == set(record["output_sha256"]),
            f"V010 output inventory mismatch: {market}",
        )

        for layer, path in paths.items():
            relative = path.relative_to(
                folder
            ).as_posix()

            expected_rebuilt[relative] = (
                record["output_sha256"][layer]
            )

    require(
        expected_rebuilt
        == manifest["rebuilt_sha256"],
        "V010 rebuilt hash inventory disagreement",
    )

    source_v009 = v010.verify_source(root)

    require(
        set(source_v009["markets"])
        == set(records),
        "V009/V010 market mismatch",
    )

    for key in (
        "result_sha256",
        "manifest_sha256",
    ):
        field = f"source_{key}"

        require(
            result[field] == source_v009[key],
            f"V009 lineage mismatch: {field}",
        )

        require(
            manifest[field] == source_v009[key],
            f"V009 manifest lineage mismatch: {field}",
        )

    require(
        result["implementation_sha256"]
        == manifest["implementation_sha256"],
        "V010 implementation inventory mismatch",
    )

    v010.verify_market_sets(
        root,
        source_v009["markets"],
    )

    for market, record in records.items():
        _, hashes = v010.verify_market_inputs(
            root,
            market,
            source_v009["markets"][market],
        )

        require(
            hashes == record["input_sha256"],
            f"Original input lineage mismatch: {market}",
        )

    print(
        "[PASS] V010 reports and 870 rebuilt hashes",
        flush=True,
    )

    print(
        "[PASS] V009 lineage and 870 original hashes",
        flush=True,
    )

    return {
        "folder": folder,
        "result": result,
        "manifest": manifest,
        "markets": records,
        "v009": source_v009,
        "result_sha256": sha256_file(result_path),
        "manifest_sha256": sha256_file(
            manifest_path
        ),
    }


def implementation_paths(root):
    return {
        "v011_script": Path(__file__).resolve(),
        "rebuild_script": (
            root
            / "rebuild_pattern_256_continuous_segments.py"
        ),
        "build_features.py": (
            root / "build_features.py"
        ),
        "detect_256.py": (
            root / "detect_256.py"
        ),
        "backtest_generator": (
            root
            / (
                "backtest_pattern_256_"
                "accumulation_candidate_independent.py"
            )
        ),
        "backtest_evaluator": (
            root
            / (
                "evaluate_pattern_256_accumulation_"
                "candidate_independent_backtest.py"
            )
        ),
    }


def verify_implementations(root, source):
    paths = implementation_paths(root)

    hashes = {}

    for name, path in paths.items():
        require(
            path.is_file(),
            f"Missing implementation: {path}",
        )

        hashes[name] = sha256_file(path)

    old_hashes = source[
        "result"
    ]["implementation_sha256"]

    for name in (
        "rebuild_script",
        "build_features.py",
        "detect_256.py",
    ):
        require(
            hashes[name] == old_hashes[name],
            f"V010 implementation changed: {name}",
        )

    return paths, hashes


def load_modules(paths):
    feature_module = v010.load_module(
        paths["build_features.py"],
        "v011_features",
    )

    detector_module = v010.load_module(
        paths["detect_256.py"],
        "v011_detector",
    )

    return (
        feature_module,
        detector_module,
    )


def verify_binary(frame, column):
    require(
        column in frame.columns,
        f"Missing binary column: {column}",
    )

    values = pd.to_numeric(
        frame[column],
        errors="raise",
    )

    require(
        values.isin([0, 1]).all(),
        f"Invalid binary values: {column}",
    )

    return values.to_numpy(dtype=bool)


def numeric_matrix(frame, columns):
    return frame[
        list(columns)
    ].apply(
        pd.to_numeric,
        errors="raise",
    ).to_numpy(dtype=float)


def compare_feature_row(
    reference,
    replay,
    columns,
):
    left = numeric_matrix(
        reference,
        columns,
    )

    right = numeric_matrix(
        replay,
        columns,
    )

    finite = (
        np.isfinite(left)
        & np.isfinite(right)
    )

    matched = (
        finite
        & np.isclose(
            left,
            right,
            rtol=RTOL,
            atol=ATOL,
            equal_nan=False,
        )
    )

    return bool(matched.all())


def compare_signal_row(reference, replay):
    for column in SIGNAL_COLUMNS:
        left = verify_binary(
            reference,
            column,
        )

        right = verify_binary(
            replay,
            column,
        )

        if not np.array_equal(left, right):
            return False

    return True


def select_reference_positions(rows):
    first = REFERENCE_MIN_BARS - 1

    if rows <= first:
        return []

    last = rows - 1

    return sorted({
        first,
        first + (last - first) // 4,
        first + (last - first) // 2,
        first + 3 * (last - first) // 4,
        last,
    })


def verify_warmup_segment(
    source,
    features,
    signals,
    mask,
    market,
    segment_id,
    feature_module,
    detector_module,
):
    rows = len(source)

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
        "No numeric features to verify",
    )

    closed = verify_binary(
        mask,
        "candle_closed_at_rebuild",
    )

    reference_positions = [
        position
        for position in select_reference_positions(
            rows
        )
        if closed[position]
    ]

    output = []

    for position in reference_positions:
        reference_features = features.iloc[
            [position]
        ].reset_index(drop=True)

        reference_signals = signals.iloc[
            [position]
        ].reset_index(drop=True)

        for warmup in WARMUP_CANDIDATES:
            start = position - warmup + 1

            require(
                start >= 0,
                "Invalid warmup start",
            )

            history = source.iloc[
                start:position + 1
            ].copy().reset_index(drop=True)

            require(
                len(history) == warmup,
                "Warmup row count mismatch",
            )

            replay_features, replay_signals = (
                v010.build_segment(
                    history,
                    market,
                    feature_module,
                    detector_module,
                )
            )

            replay_features = replay_features.tail(
                1
            ).reset_index(drop=True)

            replay_signals = replay_signals.tail(
                1
            ).reset_index(drop=True)

            v010.verify_alignment(
                reference_features,
                replay_features,
                "Warmup feature alignment",
            )

            v010.verify_alignment(
                reference_signals,
                replay_signals,
                "Warmup signal alignment",
            )

            left = numeric_matrix(
                reference_features,
                numeric_columns,
            )

            right = numeric_matrix(
                replay_features,
                numeric_columns,
            )

            finite = (
                np.isfinite(left)
                & np.isfinite(right)
            )

            feature_match = compare_feature_row(
                reference_features,
                replay_features,
                numeric_columns,
            )

            signal_match = compare_signal_row(
                reference_signals,
                replay_signals,
            )

            if finite.any():
                errors = np.abs(
                    left[finite] - right[finite]
                )

                max_absolute_error = float(
                    errors.max()
                )

                denominator = np.maximum(
                    np.abs(left[finite]),
                    ATOL,
                )

                max_relative_error = float(
                    (errors / denominator).max()
                )
            else:
                max_absolute_error = None
                max_relative_error = None

            output.append({
                "market": market,
                "segment_id": segment_id,
                TIME_COLUMN: str(
                    reference_features[
                        TIME_COLUMN
                    ].iloc[0]
                ),
                "reference_history_bars": (
                    position + 1
                ),
                "warmup_bars": warmup,
                "numeric_features_checked": (
                    len(numeric_columns)
                ),
                "all_features_finite": bool(
                    finite.all()
                ),
                "feature_match": feature_match,
                "signal_match": signal_match,
                "combined_match": (
                    feature_match
                    and signal_match
                ),
                "max_absolute_error": (
                    max_absolute_error
                ),
                "max_relative_error": (
                    max_relative_error
                ),
            })

    return output


def verify_horizon_segment(
    source,
    mask,
    market,
    segment_id,
):
    v010.verify_continuity(source)

    v010.verify_alignment(
        source,
        mask,
        "Horizon source alignment",
    )

    rows = len(source)

    positions = np.arange(
        1,
        rows + 1,
        dtype=int,
    )

    expected_forward = rows - positions

    require(
        np.array_equal(
            mask[
                "segment_row_number"
            ].to_numpy(dtype=int),
            positions,
        ),
        "Segment row number mismatch",
    )

    require(
        np.array_equal(
            mask[
                "history_bars_available"
            ].to_numpy(dtype=int),
            positions,
        ),
        "History capacity mismatch",
    )

    require(
        np.array_equal(
            mask[
                "forward_bars_available"
            ].to_numpy(dtype=int),
            expected_forward,
        ),
        "Forward capacity mismatch",
    )

    closed = verify_binary(
        mask,
        "candle_closed_at_rebuild",
    )

    finite = verify_binary(
        mask,
        "required_numeric_features_finite",
    )

    provisional = verify_binary(
        mask,
        "provisional_row_pass",
    )

    approved = verify_binary(
        mask,
        "backtest_approved",
    )

    require(
        not approved.any(),
        "Unexpected V010 backtest approval",
    )

    require(
        np.array_equal(
            provisional,
            (
                (positions >= 300)
                & finite
                & closed
            ),
        ),
        "V010 provisional policy mismatch",
    )

    times = v010.timestamps(source)

    opens = pd.to_numeric(
        source["open"],
        errors="raise",
    ).to_numpy(dtype=float)

    highs = pd.to_numeric(
        source["high"],
        errors="raise",
    ).to_numpy(dtype=float)

    require(
        np.isfinite(opens).all()
        and np.isfinite(highs).all()
        and (opens > 0).all()
        and (highs > 0).all(),
        "Invalid price values",
    )

    result = pd.DataFrame({
        "market": market,
        TIME_COLUMN: times,
        "segment_id": segment_id,
        "segment_row_number": positions,
        "history_bars_available": positions,
        "forward_bars_available": (
            expected_forward
        ),
        "candle_closed_at_rebuild": (
            closed.astype(int)
        ),
        "required_numeric_features_finite": (
            finite.astype(int)
        ),
        "provisional_row_pass": (
            provisional.astype(int)
        ),
        "backtest_approved": 0,
    })

    for warmup in WARMUP_CANDIDATES:
        result[
            f"warmup_{warmup}_capacity"
        ] = (
            (positions >= warmup)
            & finite
            & closed
        ).astype(int)

    for column in SIGNAL_COLUMNS:
        result[column] = verify_binary(
            mask,
            column,
        ).astype(int)

    for horizon in HORIZONS:
        observed = np.zeros(
            rows,
            dtype=int,
        )

        complete = np.zeros(
            rows,
            dtype=int,
        )

        entry_valid = np.zeros(
            rows,
            dtype=int,
        )

        target_hit = np.zeros(
            rows,
            dtype=int,
        )

        entry_price = np.full(
            rows,
            np.nan,
        )

        maximum_high = np.full(
            rows,
            np.nan,
        )

        outcome = np.full(
            rows,
            "NO_ENTRY",
            dtype=object,
        )

        for index in range(rows):
            if not closed[index]:
                outcome[index] = (
                    "SIGNAL_CANDLE_NOT_CLOSED"
                )
                continue

            start = index + 1

            if start >= rows:
                continue

            require(
                times.iloc[start]
                - times.iloc[index]
                == pd.Timedelta(days=1),
                "Non-continuous entry",
            )

            stop = min(
                start + horizon,
                rows,
            )

            valid_stop = start

            for forward in range(start, stop):
                if not closed[forward]:
                    break

                valid_stop = forward + 1

            count = valid_stop - start

            if count == 0:
                outcome[index] = (
                    "ENTRY_CANDLE_NOT_CLOSED"
                )
                continue

            observed[index] = count
            entry_valid[index] = 1

            entry_price[index] = opens[start]

            maximum_high[index] = float(
                highs[start:valid_stop].max()
            )

            hit = (
                maximum_high[index]
                >= entry_price[index]
                * (1.0 + TARGET_RETURN)
            )

            target_hit[index] = int(hit)

            if count == horizon:
                complete[index] = 1

                require(
                    times.iloc[valid_stop - 1]
                    - times.iloc[index]
                    == pd.Timedelta(days=horizon),
                    "Horizon elapsed time mismatch",
                )

                outcome[index] = (
                    "COMPLETE_HIT"
                    if hit
                    else "COMPLETE_MISS"
                )

            else:
                outcome[index] = (
                    "INCOMPLETE_HIT"
                    if hit
                    else "CENSORED"
                )

        prefix = f"horizon_{horizon}"

        result[
            f"{prefix}_entry_valid"
        ] = entry_valid

        result[
            f"{prefix}_observed_bars"
        ] = observed

        result[
            f"{prefix}_complete"
        ] = complete

        result[
            f"{prefix}_target_hit"
        ] = target_hit

        result[
            f"{prefix}_entry_price"
        ] = entry_price

        result[
            f"{prefix}_maximum_high"
        ] = maximum_high

        result[
            f"{prefix}_outcome"
        ] = outcome

        result[
            f"{prefix}_provisional_eligible"
        ] = (
            provisional
            & complete.astype(bool)
        ).astype(int)

    return result


def market_output_paths(folder, market):
    return {
        "warmup": (
            folder
            / "warmup_checks"
            / "d1"
            / f"{market}.csv"
        ),
        "horizon": (
            folder
            / "horizon_masks"
            / "d1"
            / f"{market}.csv"
        ),
    }


WARMUP_COLUMNS = [
    "market",
    "segment_id",
    TIME_COLUMN,
    "reference_history_bars",
    "warmup_bars",
    "numeric_features_checked",
    "all_features_finite",
    "feature_match",
    "signal_match",
    "combined_match",
    "max_absolute_error",
    "max_relative_error",
]


def validate_market_outputs(
    folder,
    market,
    source,
    source_mask,
    record,
):
    paths = market_output_paths(
        folder,
        market,
    )

    require(
        record["market"] == market,
        "Checkpoint market mismatch",
    )

    require(
        record["status"] == "PASS",
        "Checkpoint status mismatch",
    )

    require(
        record["rows"] == len(source),
        "Checkpoint row count mismatch",
    )

    require(
        set(record["output_sha256"])
        == set(paths),
        "Checkpoint hash inventory mismatch",
    )

    for name, path in paths.items():
        require(
            sha256_file(path)
            == record["output_sha256"][name],
            f"Output hash mismatch: {path}",
        )

    warmup = read_csv(paths["warmup"])
    horizon = read_csv(paths["horizon"])

    require(
        list(warmup.columns) == WARMUP_COLUMNS,
        "Warmup schema mismatch",
    )

    require(
        len(warmup) == record["warmup_checks"],
        "Warmup check count mismatch",
    )

    require(
        warmup["market"].eq(market).all(),
        "Warmup market mismatch",
    )

    require(
        warmup["warmup_bars"].isin(
            WARMUP_CANDIDATES
        ).all(),
        "Unexpected warmup candidate",
    )

    require(
        not warmup.duplicated([
            "segment_id",
            TIME_COLUMN,
            "warmup_bars",
        ]).any(),
        "Duplicate warmup observation",
    )

    v010.verify_alignment(
        source,
        horizon,
        "Saved horizon alignment",
    )

    v010.verify_market(
        horizon,
        market,
    )

    for column in (
        "segment_id",
        "segment_row_number",
        "history_bars_available",
        "forward_bars_available",
        "provisional_row_pass",
        *SIGNAL_COLUMNS,
    ):
        require(
            np.array_equal(
                horizon[column].to_numpy(),
                source_mask[column].to_numpy(),
            ),
            f"Saved mask mismatch: {column}",
        )

    require(
        horizon["backtest_approved"].eq(0).all(),
        "Unexpected saved approval",
    )

    for h in HORIZONS:
        prefix = f"horizon_{h}"

        observed = horizon[
            f"{prefix}_observed_bars"
        ].to_numpy(dtype=int)

        complete = verify_binary(
            horizon,
            f"{prefix}_complete",
        )

        entry = verify_binary(
            horizon,
            f"{prefix}_entry_valid",
        )

        hit = verify_binary(
            horizon,
            f"{prefix}_target_hit",
        )

        require(
            (
                (observed >= 0)
                & (observed <= h)
                & (
                    observed
                    <= horizon[
                        "forward_bars_available"
                    ].to_numpy(dtype=int)
                )
            ).all(),
            "Invalid saved horizon capacity",
        )

        require(
            np.array_equal(
                complete,
                observed == h,
            ),
            "Saved horizon completeness mismatch",
        )

        require(
            np.array_equal(
                entry,
                observed > 0,
            ),
            "Saved entry validity mismatch",
        )

        require(
            not (hit & ~entry).any(),
            "Hit without valid entry",
        )

    return warmup, horizon


def checkpoint_paths(folder, market):
    return (
        folder
        / "checkpoints"
        / f"{market}.json",

        folder
        / "checkpoints"
        / f"{market}.sha256",
    )


def load_checkpoint(
    folder,
    market,
    context,
    source,
    mask,
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

        expected = checksum.read_text(
            encoding="utf-8"
        ).strip()

        if sha256_file(path) != expected:
            return None

        payload = read_json(path)

        if payload["context"] != context:
            return None

        record = payload["record"]

        validate_market_outputs(
            folder,
            market,
            source,
            mask,
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
    source_info,
    implementation_hashes,
    feature_module,
    detector_module,
    refresh,
):
    old_record = source_info[
        "markets"
    ][market]

    v009_record = source_info[
        "v009"
    ]["markets"][market]

    paths, input_hashes = (
        v010.verify_market_inputs(
            root,
            market,
            v009_record,
        )
    )

    source = feature_module.load_source_ohlcv(
        paths["ohlcv"],
        expected_market=market,
    ).reset_index(drop=True)

    v010.verify_segment_inventory(
        source,
        market,
        v009_record,
    )

    rebuilt = v010.validate_saved_outputs(
        source_info["folder"],
        market,
        source,
        v009_record,
        old_record,
    )

    features = rebuilt["features"]
    signals = rebuilt["signals"]
    mask = rebuilt["policy_mask"]

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
        "original_input_sha256": input_hashes,
        "rebuilt_input_sha256": (
            old_record["output_sha256"]
        ),
        "warmup_candidates": list(
            WARMUP_CANDIDATES
        ),
        "horizons": list(HORIZONS),
        "target_return": TARGET_RETURN,
        "rtol": RTOL,
        "atol": ATOL,
        "reference_min_bars": (
            REFERENCE_MIN_BARS
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
            mask,
        )

        if cached is not None:
            print(
                f"[RESUME] {market}",
                flush=True,
            )

            return {
                **cached,
                "resumed": True,
            }

    warmup_records = []
    horizon_parts = []

    for segment in v009_record["segments"]:
        segment_id = segment["segment_id"]

        start = int(
            segment["start_index"]
        )

        end = int(
            segment["end_index"]
        ) + 1

        part_source = source.iloc[
            start:end
        ].copy().reset_index(drop=True)

        part_features = features.iloc[
            start:end
        ].copy().reset_index(drop=True)

        part_signals = signals.iloc[
            start:end
        ].copy().reset_index(drop=True)

        part_mask = mask.iloc[
            start:end
        ].copy().reset_index(drop=True)

        print(
            f"[SEGMENT] {market} "
            f"id={segment_id} "
            f"rows={len(part_source)}",
            flush=True,
        )

        warmup_records.extend(
            verify_warmup_segment(
                part_source,
                part_features,
                part_signals,
                part_mask,
                market,
                segment_id,
                feature_module,
                detector_module,
            )
        )

        horizon_parts.append(
            verify_horizon_segment(
                part_source,
                part_mask,
                market,
                segment_id,
            )
        )

    warmup_frame = pd.DataFrame(
        warmup_records,
        columns=WARMUP_COLUMNS,
    )

    horizon_frame = pd.concat(
        horizon_parts,
        ignore_index=True,
    )

    destinations = market_output_paths(
        folder,
        market,
    )

    atomic_csv(
        destinations["warmup"],
        warmup_frame,
    )

    atomic_csv(
        destinations["horizon"],
        horizon_frame,
    )

    record = {
        "market": market,
        "status": "PASS",
        "rows": len(source),
        "segments": len(
            v009_record["segments"]
        ),
        "warmup_checks": len(warmup_frame),
        "output_sha256": {
            name: sha256_file(path)
            for name, path in destinations.items()
        },
    }

    validate_market_outputs(
        folder,
        market,
        source,
        mask,
        record,
    )

    v010.verify_market_inputs(
        root,
        market,
        v009_record,
    )

    verify_hash_map(
        source_info["folder"],
        {
            path.relative_to(
                source_info["folder"]
            ).as_posix(): (
                old_record[
                    "output_sha256"
                ][layer]
            )
            for layer, path
            in v010.output_paths(
                source_info["folder"],
                market,
            ).items()
        },
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
        f"warmup_checks={len(warmup_frame)}",
        flush=True,
    )

    return {
        **record,
        "resumed": False,
    }


def percentage(numerator, denominator):
    if denominator == 0:
        return None

    return float(
        numerator / denominator * 100.0
    )


def boolean_count(series):
    values = series.astype(
        str
    ).str.lower()

    require(
        values.isin(
            ["true", "false", "1", "0"]
        ).all(),
        "Invalid summary boolean",
    )

    return int(
        values.isin(
            ["true", "1"]
        ).sum()
    )


def summarize_warmup(frame):
    records = []

    for candidate in WARMUP_CANDIDATES:
        part = frame.loc[
            frame["warmup_bars"] == candidate
        ]

        checks = len(part)

        feature_matches = boolean_count(
            part["feature_match"]
        )

        signal_matches = boolean_count(
            part["signal_match"]
        )

        combined_matches = boolean_count(
            part["combined_match"]
        )

        records.append({
            "warmup_bars": candidate,
            "checks": checks,
            "markets_tested": int(
                part["market"].nunique()
            ),
            "segments_tested": int(
                part[
                    ["market", "segment_id"]
                ].drop_duplicates().shape[0]
            ),
            "feature_matches": feature_matches,
            "feature_mismatches": (
                checks - feature_matches
            ),
            "feature_match_pct": percentage(
                feature_matches,
                checks,
            ),
            "signal_matches": signal_matches,
            "signal_mismatches": (
                checks - signal_matches
            ),
            "signal_match_pct": percentage(
                signal_matches,
                checks,
            ),
            "combined_matches": combined_matches,
            "combined_mismatches": (
                checks - combined_matches
            ),
            "combined_match_pct": percentage(
                combined_matches,
                checks,
            ),
            "sampled_stability_pass": (
                checks > 0
                and combined_matches == checks
            ),
        })

    return records


def summarize_horizons(frame):
    records = []

    for horizon in HORIZONS:
        prefix = f"horizon_{horizon}"

        for scope in (
            "ALL_ROWS",
            "PROVISIONAL_300",
            "SETUP_300",
            "READY_300",
            "TRIGGER_300",
        ):
            if scope == "ALL_ROWS":
                part = frame

            else:
                part = frame.loc[
                    frame[
                        "provisional_row_pass"
                    ].eq(1)
                ]

                if scope != "PROVISIONAL_300":
                    signal = {
                        "SETUP_300": "signal_setup",
                        "READY_300": "signal_ready",
                        "TRIGGER_300": "signal_trigger",
                    }[scope]

                    part = part.loc[
                        part[signal].eq(1)
                    ]

            outcomes = part[
                f"{prefix}_outcome"
            ]

            complete_hit = int(
                outcomes.eq(
                    "COMPLETE_HIT"
                ).sum()
            )

            complete_miss = int(
                outcomes.eq(
                    "COMPLETE_MISS"
                ).sum()
            )

            incomplete_hit = int(
                outcomes.eq(
                    "INCOMPLETE_HIT"
                ).sum()
            )

            censored = int(
                outcomes.eq(
                    "CENSORED"
                ).sum()
            )

            no_entry = int(
                outcomes.isin([
                    "NO_ENTRY",
                    "ENTRY_CANDLE_NOT_CLOSED",
                    "SIGNAL_CANDLE_NOT_CLOSED",
                ]).sum()
            )

            complete = (
                complete_hit
                + complete_miss
            )

            require(
                complete
                + incomplete_hit
                + censored
                + no_entry
                == len(part),
                "Horizon outcome accounting mismatch",
            )

            records.append({
                "horizon_bars": horizon,
                "scope": scope,
                "rows": len(part),
                "complete_rows": complete,
                "complete_hit": complete_hit,
                "complete_miss": complete_miss,
                "incomplete_hit": incomplete_hit,
                "censored": censored,
                "no_entry": no_entry,
                "complete_hit_pct": percentage(
                    complete_hit,
                    complete,
                ),
                "hit_pct_excluding_censored": (
                    percentage(
                        complete_hit
                        + incomplete_hit,
                        complete
                        + incomplete_hit,
                    )
                ),
                "backtest_approved": False,
            })

    return records


def verify_final_inputs(
    root,
    source_info,
    paths,
    hashes,
):
    final = verify_v010(root)

    for key in (
        "result_sha256",
        "manifest_sha256",
    ):
        require(
            final[key] == source_info[key],
            f"Source changed during run: {key}",
        )

    for name, path in paths.items():
        require(
            sha256_file(path) == hashes[name],
            f"Implementation changed: {name}",
        )


def write_reports(
    folder,
    records,
    source_info,
    implementation_hashes,
    run_id,
):
    require(
        len(records) == EXPECTED_MARKETS,
        "Final market count mismatch",
    )

    require(
        sum(
            record["rows"]
            for record in records
        ) == EXPECTED_ROWS,
        "Final row count mismatch",
    )

    require(
        sum(
            record["segments"]
            for record in records
        ) == EXPECTED_SEGMENTS,
        "Final segment count mismatch",
    )

    warmup_parts = []
    horizon_parts = []

    output_hashes = {}

    for record in records:
        market = record["market"]

        paths = market_output_paths(
            folder,
            market,
        )

        for name, path in paths.items():
            actual = sha256_file(path)

            require(
                actual
                == record["output_sha256"][name],
                f"Final output changed: {path}",
            )

            output_hashes[
                path.relative_to(
                    folder
                ).as_posix()
            ] = actual

        warmup_parts.append(
            read_csv(paths["warmup"])
        )

        horizon_parts.append(
            read_csv(paths["horizon"])
        )

    warmup_frame = pd.concat(
        warmup_parts,
        ignore_index=True,
    )

    horizon_frame = pd.concat(
        horizon_parts,
        ignore_index=True,
    )

    require(
        len(horizon_frame) == EXPECTED_ROWS,
        "Combined horizon row mismatch",
    )

    require(
        not horizon_frame.duplicated([
            "market",
            TIME_COLUMN,
        ]).any(),
        "Duplicate combined horizon row",
    )

    require(
        len(warmup_frame) == sum(
            record["warmup_checks"]
            for record in records
        ),
        "Combined warmup count mismatch",
    )

    require(
        horizon_frame[
            "backtest_approved"
        ].eq(0).all(),
        "Unexpected final backtest approval",
    )

    warmup_summary = summarize_warmup(
        warmup_frame
    )

    horizon_summary = summarize_horizons(
        horizon_frame
    )

    tested_segments = int(
        warmup_frame[
            ["market", "segment_id"]
        ].drop_duplicates().shape[0]
    )

    tested_markets = int(
        warmup_frame["market"].nunique()
    )

    sampled_pass_candidates = [
        item["warmup_bars"]
        for item in warmup_summary
        if item["sampled_stability_pass"]
    ]

    result = {
        "version": VERSION,
        "created_utc": utc_now(),
        "run_id": run_id,
        "execution": "PASS",
        "classification": (
            "SAMPLED_WARMUP_AND_"
            "HORIZON_CAPACITY_AUDITED"
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
        "markets_checked": len(records),
        "segments_checked": EXPECTED_SEGMENTS,
        "rows_checked": EXPECTED_ROWS,
        "original_input_files_verified": 870,
        "rebuilt_input_files_verified": 870,
        "output_files_verified": len(
            output_hashes
        ),
        "warmup_candidates": list(
            WARMUP_CANDIDATES
        ),
        "warmup_checks": len(warmup_frame),
        "warmup_markets_tested": tested_markets,
        "warmup_segments_tested": tested_segments,
        "warmup_segments_untested": (
            EXPECTED_SEGMENTS - tested_segments
        ),
        "reference_min_bars": (
            REFERENCE_MIN_BARS
        ),
        "relative_tolerance": RTOL,
        "absolute_tolerance": ATOL,
        "sampled_pass_candidates": (
            sampled_pass_candidates
        ),
        "warmup_summary": warmup_summary,
        "horizons": list(HORIZONS),
        "horizon_summary": horizon_summary,
        "target_return": TARGET_RETURN,
        "resumed_markets": sum(
            record["resumed"]
            for record in records
        ),
        "source_csv_modified": False,
        "synthetic_candles_created": 0,
        "sampled_warmup_audit_completed": True,
        "horizon_capacity_audit_completed": True,
        "actual_feature_warmup_verified": False,
        "actual_forward_horizon_verified": False,
        "gap_policy_implemented_in_evaluator": False,
        "remaining_rows_approved_for_backtest": False,
        "point_in_time_proven": False,
        "independent_oos_proven": False,
        "profitability_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "data_readiness": "BLOCKED",
        "research_decision": "HOLD",
        "next_action": (
            "REVIEW_WARMUP_EVIDENCE_AND_"
            "INTEGRATE_GAP_SAFE_EVALUATOR"
        ),
        "markets": records,
    }

    atomic_csv(
        folder / "warmup_detail.csv",
        warmup_frame,
    )

    atomic_csv(
        folder / "warmup_summary.csv",
        pd.DataFrame(warmup_summary),
    )

    atomic_csv(
        folder / "horizon_summary.csv",
        pd.DataFrame(horizon_summary),
    )

    atomic_csv(
        folder / "market_summary.csv",
        pd.DataFrame([
            {
                key: value
                for key, value in record.items()
                if key != "output_sha256"
            }
            for record in records
        ]),
    )

    atomic_json(
        folder / "result.json",
        result,
    )

    lines = [
        "# Pattern 256 Warmup and Horizon Audit",
        "",
        f"Version: {VERSION}",
        f"Run ID: {run_id}",
        "",
        "## Execution",
        "",
        "| Metric | Result |",
        "|---|---|",
        f"| Execution | PASS |",
        f"| Markets | {EXPECTED_MARKETS} |",
        f"| Segments | {EXPECTED_SEGMENTS} |",
        f"| Rows | {EXPECTED_ROWS} |",
        f"| Warmup checks | {len(warmup_frame)} |",
        f"| Tested segments | {tested_segments} |",
        f"| Tested markets | {tested_markets} |",
        f"| Research decision | HOLD |",
        "",
        "## Warmup comparison",
        "",
        "| Bars | Checks | Feature mismatch | "
        "Signal mismatch | Combined mismatch |",
        "|---:|---:|---:|---:|---:|",
    ]

    for item in warmup_summary:
        lines.append(
            f"| {item['warmup_bars']} "
            f"| {item['checks']} "
            f"| {item['feature_mismatches']} "
            f"| {item['signal_mismatches']} "
            f"| {item['combined_mismatches']} |"
        )

    lines.extend([
        "",
        "## Limitations",
        "",
        "- Warmup checks use sampled reference rows.",
        "- Reference history is not infinite history.",
        "- Short segments may have no warmup tests.",
        "- Numeric tolerance is provisional.",
        "- Horizon checks are research-only diagnostics.",
        "- Horizon outcomes are not strategy returns.",
        "- No accumulation or regime filtering is applied.",
        "- Evaluator integration remains pending.",
        "- Historical point-in-time availability "
        "is not proven.",
        "- Independent OOS is not proven.",
        "- No backtest, training or trading approval.",
        "",
    ])

    atomic_bytes(
        folder / "report.md",
        "\n".join(lines).encode("utf-8"),
    )

    report_names = [
        "result.json",
        "report.md",
        "warmup_detail.csv",
        "warmup_summary.csv",
        "horizon_summary.csv",
        "market_summary.csv",
    ]

    require(
        len(output_hashes)
        == EXPECTED_MARKETS * 2,
        "Final output inventory mismatch",
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
            name: sha256_file(folder / name)
            for name in report_names
        },
        "output_sha256": output_hashes,
    }

    atomic_json(
        folder / "manifest.json",
        manifest,
    )

    verify_hash_map(
        folder,
        manifest["sha256"],
    )

    verify_hash_map(
        folder,
        manifest["output_sha256"],
    )

    return result


def run_self_tests():
    source = pd.DataFrame({
        "market": ["KRW-TEST"] * 26,
        TIME_COLUMN: pd.date_range(
            "2020-01-01",
            periods=26,
            freq="D",
            tz="UTC",
        ),
        "open": [100.0] * 26,
        "high": [100.0] * 26,
    })

    source.loc[24, "high"] = 131.0

    positions = np.arange(
        1,
        27,
    )

    mask = pd.DataFrame({
        "market": source["market"],
        TIME_COLUMN: source[TIME_COLUMN],
        "segment_id": 1,
        "segment_row_number": positions,
        "history_bars_available": positions,
        "forward_bars_available": (
            26 - positions
        ),
        "candle_closed_at_rebuild": 1,
        "required_numeric_features_finite": 1,
        "provisional_row_pass": 0,
        "backtest_approved": 0,
        "signal_setup": 0,
        "signal_ready": 0,
        "signal_trigger": 0,
    })

    result = verify_horizon_segment(
        source,
        mask,
        "KRW-TEST",
        1,
    )

    require(
        result.loc[
            0,
            "horizon_24_outcome",
        ] == "COMPLETE_HIT",
        "Self-test complete HIT failed",
    )

    require(
        result.loc[
            0,
            "horizon_72_outcome",
        ] == "INCOMPLETE_HIT",
        "Self-test incomplete HIT failed",
    )

    require(
        result.loc[
            25,
            "horizon_24_outcome",
        ] == "NO_ENTRY",
        "Self-test no entry failed",
    )

    require(
        result.loc[
            24,
            "horizon_24_outcome",
        ] == "CENSORED",
        "Self-test censored failed",
    )

    source.loc[24, "high"] = 100.0

    result = verify_horizon_segment(
        source,
        mask,
        "KRW-TEST",
        1,
    )

    require(
        result.loc[
            0,
            "horizon_24_outcome",
        ] == "COMPLETE_MISS",
        "Self-test complete MISS failed",
    )

    mask.loc[
        24:,
        "candle_closed_at_rebuild",
    ] = 0

    result = verify_horizon_segment(
        source,
        mask,
        "KRW-TEST",
        1,
    )

    require(
        result.loc[
            0,
            "horizon_24_observed_bars",
        ] == 23,
        "Self-test unclosed candle failed",
    )

    broken = source.copy()

    broken.loc[
        10:,
        TIME_COLUMN,
    ] += pd.Timedelta(days=1)

    broken_mask = mask.copy()

    broken_mask[TIME_COLUMN] = (
        broken[TIME_COLUMN]
    )

    rejected = False

    try:
        verify_horizon_segment(
            broken,
            broken_mask,
            "KRW-TEST",
            1,
        )

    except RuntimeError:
        rejected = True

    require(
        rejected,
        "Self-test gap rejection failed",
    )

    print(
        "[PASS] 7 horizon policy self-tests",
        flush=True,
    )


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

    parser.add_argument(
        "--self-test-only",
        action="store_true",
    )

    args = parser.parse_args()

    run_self_tests()

    if args.self_test_only:
        return 0

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

    # Remove the previous completion marker.
    # Completed market checkpoints remain intact.
    manifest_path = folder / "manifest.json"

    if manifest_path.exists():
        manifest_path.unlink()

    atomic_json(
        folder / "run_state.json",
        {
            "version": VERSION,
            "run_id": args.run_id,
            "status": "RUNNING",
            "started_utc": utc_now(),
        },
    )

    print("=" * 72)
    print("Pattern 256 Actual Warmup and Horizon")
    print("Version:", VERSION)
    print("Root:", root)
    print("Run ID:", args.run_id)
    print("Refresh:", args.refresh)
    print("Warmup:", WARMUP_CANDIDATES)
    print("Horizons:", HORIZONS)
    print("=" * 72)

    source_info = verify_v010(root)

    paths, implementation_hashes = (
        verify_implementations(
            root,
            source_info,
        )
    )

    sys.path.insert(
        0,
        str(root),
    )

    feature_module, detector_module = (
        load_modules(paths)
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
        paths,
        implementation_hashes,
    )

    result = write_reports(
        folder,
        records,
        source_info,
        implementation_hashes,
        args.run_id,
    )

    atomic_json(
        folder / "run_state.json",
        {
            "version": VERSION,
            "run_id": args.run_id,
            "status": "COMPLETED",
            "completed_utc": utc_now(),
            "manifest_sha256": sha256_file(
                folder / "manifest.json"
            ),
        },
    )

    print()
    print("=" * 72)
    print("[PASS] Clean V011 audit execution")
    print(
        "Markets:",
        result["markets_checked"],
    )
    print(
        "Segments:",
        result["segments_checked"],
    )
    print(
        "Rows:",
        result["rows_checked"],
    )
    print(
        "Warmup checks:",
        result["warmup_checks"],
    )
    print(
        "Sampled pass candidates:",
        result["sampled_pass_candidates"],
    )
    print(
        "Untested warmup segments:",
        result["warmup_segments_untested"],
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
        traceback.print_exc()

        print(
            "[FAIL]",
            type(exc).__name__,
            str(exc),
            file=sys.stderr,
            flush=True,
        )

        sys.exit(1)
