#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Extended Warmup Convergence Audit
Clean V011.1

Research-only verification.

- Verify V010 and V011 evidence.
- Preserve all original input files.
- Compare truncated history with full segment prefix.
- Record feature-level mismatches.
- Compare detector states and numeric detector outputs.
- Classify short and untested segments.
- Support validated market-level resume.
- Never approve backtest, training or live trading.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import platform
import re
import sys
import traceback

from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd


# ============================================================
# CONFIGURATION
# ============================================================

VERSION = "Clean V011.1"
SOURCE_VERSION = "Clean V011"
REBUILD_VERSION = "Clean V010"

TIMEFRAME = "d1"
TIMESTAMP = "candle_date_time_utc"

EXPECTED_MARKETS = 290
EXPECTED_SEGMENTS = 307
EXPECTED_ROWS = 321481

WARMUP_BARS = (
    300,
    400,
    500,
    600,
    700,
    800,
    1000,
    1200,
    1500,
)

RTOL = 1e-6
ATOL = 1e-8

# Each group uses identical reference rows for every
# warmup length inside that group.
COHORTS = {
    "COMMON_500": (300, 400, 500),
    "COMMON_800": (300, 400, 500, 600, 700, 800),
    "COMMON_1000": (
        300, 400, 500, 600, 700, 800, 1000
    ),
    "COMMON_1500": WARMUP_BARS,
}

SAMPLES_PER_COHORT = 7
REFERENCE_EXTRA_BARS = 100

SIGNAL_COLUMNS = (
    "signal_setup",
    "signal_ready",
    "signal_trigger",
)

IMPLEMENTATION_FILES = {
    "build_features.py": "build_features.py",
    "detect_256.py": "detect_256.py",
    "rebuild_script":
        "rebuild_pattern_256_continuous_segments.py",
    "backtest_generator":
        "backtest_pattern_256_accumulation_candidate_independent.py",
    "backtest_evaluator":
        "evaluate_pattern_256_accumulation_candidate_independent_backtest.py",
    "v011_script":
        "verify_pattern_256_actual_warmup_horizon.py",
}

DETAIL_COLUMNS = [
    "market",
    "segment_id",
    "cohort",
    "timestamp",
    "reference_history_bars",
    "warmup_bars",
    "features_checked",
    "detector_columns_checked",
    "feature_match",
    "signal_match",
    "detector_match",
    "combined_match",
    "max_absolute_error",
    "max_relative_error",
]

MISMATCH_COLUMNS = [
    "market",
    "segment_id",
    "cohort",
    "timestamp",
    "warmup_bars",
    "layer",
    "column",
    "reference_value",
    "truncated_value",
    "absolute_error",
    "relative_error",
    "reason",
]

COVERAGE_COLUMNS = [
    "market",
    "segment_id",
    "segment_rows",
    "cohort",
    "minimum_reference_rows",
    "sample_count",
    "status",
]


# ============================================================
# BASIC UTILITIES
# ============================================================

def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path):
    path = Path(path)

    require(
        path.is_file(),
        f"Missing file: {path}",
    )

    digest = hashlib.sha256()

    with path.open("rb") as handle:
        for block in iter(
            lambda: handle.read(1024 * 1024),
            b"",
        ):
            digest.update(block)

    return digest.hexdigest()


def json_digest(data):
    raw = json.dumps(
        data,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")

    return hashlib.sha256(raw).hexdigest()


def read_json(path):
    with Path(path).open(
        "r",
        encoding="utf-8-sig",
    ) as handle:
        return json.load(handle)


def atomic_json(path, data):
    path = Path(path)

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temp = path.with_name(
        path.name + ".tmp"
    )

    with temp.open(
        "w",
        encoding="utf-8",
    ) as handle:
        json.dump(
            data,
            handle,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            allow_nan=False,
        )

        handle.flush()
        os.fsync(handle.fileno())

    os.replace(temp, path)


def atomic_csv(path, frame):
    path = Path(path)

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temp = path.with_name(
        path.name + ".tmp"
    )

    frame.to_csv(
        temp,
        index=False,
        encoding="utf-8-sig",
    )

    os.replace(temp, path)


def atomic_text(path, content):
    path = Path(path)

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temp = path.with_name(
        path.name + ".tmp"
    )

    temp.write_text(
        content,
        encoding="utf-8",
    )

    os.replace(temp, path)


def safe_path(base, relative):
    base = Path(base).resolve()
    target = (base / relative).resolve()

    require(
        target.is_relative_to(base),
        f"Unsafe path: {relative}",
    )

    return target


def verify_hash(path, expected):
    require(
        isinstance(expected, str)
        and re.fullmatch(
            r"[a-fA-F0-9]{64}",
            expected,
        ) is not None,
        f"Invalid expected SHA256: {path}",
    )

    actual = sha256_file(path)

    require(
        actual == expected.lower(),
        (
            f"SHA256 mismatch: {path}\n"
            f"Expected: {expected}\n"
            f"Actual:   {actual}"
        ),
    )

    return actual


def verify_inventory(base, inventory):
    require(
        isinstance(inventory, dict)
        and bool(inventory),
        f"Empty hash inventory: {base}",
    )

    for relative, expected in sorted(
        inventory.items()
    ):
        verify_hash(
            safe_path(base, relative),
            expected,
        )

    return len(inventory)


def load_module(path, module_name):
    path = Path(path)

    spec = importlib.util.spec_from_file_location(
        module_name,
        path,
    )

    require(
        spec is not None
        and spec.loader is not None,
        f"Cannot load module: {path}",
    )

    module = importlib.util.module_from_spec(
        spec
    )

    sys.modules[module_name] = module
    spec.loader.exec_module(module)

    return module


# ============================================================
# SOURCE EVIDENCE VERIFICATION
# ============================================================

def verify_source_evidence(root):
    v010_dir = (
        root
        / "data"
        / "reports"
        / "pattern_256_continuous_segments"
    )

    v011_dir = (
        root
        / "data"
        / "reports"
        / "pattern_256_actual_warmup_horizon"
    )

    v010_result_path = (
        v010_dir / "result.json"
    )

    v010_manifest_path = (
        v010_dir / "manifest.json"
    )

    v011_result_path = (
        v011_dir / "result.json"
    )

    v011_manifest_path = (
        v011_dir / "manifest.json"
    )

    v011_state_path = (
        v011_dir / "run_state.json"
    )

    v010_result = read_json(
        v010_result_path
    )

    v010_manifest = read_json(
        v010_manifest_path
    )

    v011_result = read_json(
        v011_result_path
    )

    v011_manifest = read_json(
        v011_manifest_path
    )

    v011_state = read_json(
        v011_state_path
    )

    require(
        v010_result["version"]
        == REBUILD_VERSION,
        "Unexpected V010 result version",
    )

    require(
        v010_manifest["version"]
        == REBUILD_VERSION,
        "Unexpected V010 manifest version",
    )

    require(
        v011_result["version"]
        == SOURCE_VERSION,
        "Unexpected V011 result version",
    )

    require(
        v011_manifest["version"]
        == SOURCE_VERSION,
        "Unexpected V011 manifest version",
    )

    require(
        v011_state["version"]
        == SOURCE_VERSION,
        "Unexpected V011 state version",
    )

    require(
        v010_result["execution"] == "PASS",
        "V010 execution not PASS",
    )

    require(
        v011_result["execution"] == "PASS",
        "V011 execution not PASS",
    )

    require(
        v011_state["status"] == "COMPLETED",
        "V011 source run incomplete",
    )

    require(
        v010_result["run_id"]
        == v010_manifest["run_id"],
        "V010 run ID mismatch",
    )

    require(
        v011_result["run_id"]
        == v011_manifest["run_id"]
        == v011_state["run_id"],
        "V011 run ID mismatch",
    )

    verify_hash(
        v011_manifest_path,
        v011_state["manifest_sha256"],
    )

    verify_hash(
        v010_result_path,
        v011_manifest[
            "source_result_sha256"
        ],
    )

    verify_hash(
        v010_manifest_path,
        v011_manifest[
            "source_manifest_sha256"
        ],
    )

    require(
        v011_result["research_decision"]
        == "HOLD",
        "Unexpected V011 research decision",
    )

    require(
        v011_result["data_readiness"]
        == "BLOCKED",
        "Unexpected V011 data readiness",
    )

    require(
        v011_result[
            "sampled_warmup_audit_completed"
        ] is True,
        "V011 warmup audit incomplete",
    )

    require(
        v011_result[
            "horizon_capacity_audit_completed"
        ] is True,
        "V011 horizon audit incomplete",
    )

    require(
        v011_result[
            "actual_feature_warmup_verified"
        ] is False,
        "Unexpected final warmup approval",
    )

    require(
        v011_result["markets_checked"]
        == EXPECTED_MARKETS,
        "V011 market count mismatch",
    )

    require(
        v011_result["segments_checked"]
        == EXPECTED_SEGMENTS,
        "V011 segment count mismatch",
    )

    require(
        v011_result["rows_checked"]
        == EXPECTED_ROWS,
        "V011 row count mismatch",
    )

    v010_report_count = verify_inventory(
        v010_dir,
        v010_manifest["sha256"],
    )

    v010_output_count = verify_inventory(
        v010_dir,
        v010_manifest["rebuilt_sha256"],
    )

    v011_report_count = verify_inventory(
        v011_dir,
        v011_manifest["sha256"],
    )

    v011_output_count = verify_inventory(
        v011_dir,
        v011_manifest["output_sha256"],
    )

    require(
        v010_output_count == 870,
        "V010 rebuilt output count mismatch",
    )

    require(
        v011_output_count == 580,
        "V011 output count mismatch",
    )

    implementation_hashes = {}

    for key, filename in (
        IMPLEMENTATION_FILES.items()
    ):
        expected = v011_manifest[
            "implementation_sha256"
        ][key]

        require(
            expected
            == v011_result[
                "implementation_sha256"
            ][key],
            f"V011 implementation mismatch: {key}",
        )

        implementation_hashes[key] = (
            verify_hash(
                root / filename,
                expected,
            )
        )

    for key, expected in v010_manifest[
        "implementation_sha256"
    ].items():
        require(
            implementation_hashes[key]
            == expected,
            f"V010 implementation mismatch: {key}",
        )

    markets = {}

    for record in v010_result["markets"]:
        market = record["market"]

        require(
            re.fullmatch(
                r"KRW-[A-Z0-9]+",
                market,
            ) is not None,
            f"Invalid market: {market}",
        )

        require(
            market not in markets,
            f"Duplicate market: {market}",
        )

        require(
            record["status"] == "PASS",
            f"V010 market not PASS: {market}",
        )

        markets[market] = record

    require(
        len(markets) == EXPECTED_MARKETS,
        "V010 market inventory mismatch",
    )

    require(
        sum(
            int(record["rows"])
            for record in markets.values()
        ) == EXPECTED_ROWS,
        "V010 total row mismatch",
    )

    require(
        sum(
            int(record["segment_count"])
            for record in markets.values()
        ) == EXPECTED_SEGMENTS,
        "V010 total segment mismatch",
    )

    print(
        "[PASS] V010 reports:",
        v010_report_count,
        flush=True,
    )

    print(
        "[PASS] V010 outputs:",
        v010_output_count,
        flush=True,
    )

    print(
        "[PASS] V011 reports:",
        v011_report_count,
        flush=True,
    )

    print(
        "[PASS] V011 outputs:",
        v011_output_count,
        flush=True,
    )

    return {
        "v010_dir": v010_dir,
        "v011_dir": v011_dir,
        "v010_result": v010_result,
        "v010_manifest": v010_manifest,
        "v011_result": v011_result,
        "v011_manifest": v011_manifest,
        "markets": markets,
        "implementation_hashes":
            implementation_hashes,
        "source_hashes": {
            "v010_result":
                sha256_file(v010_result_path),
            "v010_manifest":
                sha256_file(v010_manifest_path),
            "v011_result":
                sha256_file(v011_result_path),
            "v011_manifest":
                sha256_file(v011_manifest_path),
            "v011_state":
                sha256_file(v011_state_path),
        },
    }


# ============================================================
# MARKET INPUT VALIDATION
# ============================================================

def read_market_csv(path, market):
    frame = pd.read_csv(
        path,
        low_memory=False,
    )

    require(
        len(frame) > 0,
        f"Empty market file: {path}",
    )

    require(
        TIMESTAMP in frame.columns,
        f"Missing timestamp: {path}",
    )

    require(
        "market" in frame.columns,
        f"Missing market column: {path}",
    )

    require(
        frame["market"].eq(market).all(),
        f"Market mismatch: {path}",
    )

    frame[TIMESTAMP] = pd.to_datetime(
        frame[TIMESTAMP],
        utc=True,
        errors="raise",
    )

    require(
        not frame[TIMESTAMP].isna().any(),
        f"Missing timestamps: {path}",
    )

    require(
        frame[TIMESTAMP].is_monotonic_increasing,
        f"Unsorted timestamps: {path}",
    )

    require(
        not frame[TIMESTAMP].duplicated().any(),
        f"Duplicate timestamps: {path}",
    )

    require(
        frame[TIMESTAMP].eq(
            frame[TIMESTAMP].dt.normalize()
        ).all(),
        f"Invalid daily candle boundary: {path}",
    )

    return frame.reset_index(drop=True)


def verify_alignment(left, right, label):
    require(
        len(left) == len(right),
        f"Row count mismatch: {label}",
    )

    require(
        left[TIMESTAMP].reset_index(
            drop=True
        ).equals(
            right[TIMESTAMP].reset_index(
                drop=True
            )
        ),
        f"Timestamp mismatch: {label}",
    )


def get_market_paths(root, evidence, market):
    v010 = evidence["v010_dir"]

    return {
        "ohlcv":
            root / "data" / "ohlcv"
            / TIMEFRAME / f"{market}.csv",

        "features":
            root / "data" / "features"
            / TIMEFRAME / f"{market}.csv",

        "signals":
            root / "data" / "signals"
            / "256" / TIMEFRAME
            / f"{market}.csv",

        "rebuilt_features":
            v010 / "rebuilt_features"
            / TIMEFRAME / f"{market}.csv",

        "rebuilt_signals":
            v010 / "rebuilt_signals"
            / TIMEFRAME / f"{market}.csv",

        "policy_mask":
            v010 / "policy_masks"
            / TIMEFRAME / f"{market}.csv",
    }


def verify_market_hashes(
    paths,
    evidence,
    market,
):
    record = evidence["markets"][market]

    original_hashes = record[
        "input_sha256"
    ]

    actual = {}

    for key in (
        "ohlcv",
        "features",
        "signals",
    ):
        actual[key] = verify_hash(
            paths[key],
            original_hashes[key],
        )

    rebuilt_keys = {
        "rebuilt_features":
            "rebuilt_features",
        "rebuilt_signals":
            "rebuilt_signals",
        "policy_mask":
            "policy_masks",
    }

    inventory = evidence[
        "v010_manifest"
    ]["rebuilt_sha256"]

    for key, folder in (
        rebuilt_keys.items()
    ):
        relative = (
            f"{folder}/{TIMEFRAME}/{market}.csv"
        )

        actual[key] = verify_hash(
            paths[key],
            inventory[relative],
        )

    return actual


def validate_segments(source, mask, record):
    verify_alignment(
        source,
        mask,
        "OHLCV / policy mask",
    )

    required = {
        "segment_id",
        "segment_row_number",
        "segment_rows",
        "history_bars_available",
        "candle_closed_at_rebuild",
    }

    require(
        required.issubset(mask.columns),
        "Missing segment mask columns",
    )

    require(
        len(source) == int(record["rows"]),
        "Market row count mismatch",
    )

    expected_ids = (
        source[TIMESTAMP]
        .diff()
        .ne(pd.Timedelta(days=1))
        .cumsum()
        .astype(int)
    )

    actual_ids = pd.to_numeric(
        mask["segment_id"],
        errors="raise",
    )

    require(
        np.array_equal(
            expected_ids.to_numpy(),
            actual_ids.to_numpy(),
        ),
        "Segment ID does not match actual gaps",
    )

    segments = []

    for segment_id, group in mask.groupby(
        "segment_id",
        sort=True,
    ):
        indices = group.index.to_numpy()
        rows = len(indices)

        require(
            np.array_equal(
                indices,
                np.arange(
                    indices[0],
                    indices[0] + rows,
                ),
            ),
            "Non-contiguous segment indices",
        )

        require(
            np.array_equal(
                pd.to_numeric(
                    group["segment_row_number"],
                    errors="raise",
                ).to_numpy(),
                np.arange(1, rows + 1),
            ),
            "Invalid segment row numbers",
        )

        require(
            pd.to_numeric(
                group["segment_rows"],
                errors="raise",
            ).eq(rows).all(),
            "Segment length mismatch",
        )

        require(
            np.array_equal(
                pd.to_numeric(
                    group["history_bars_available"],
                    errors="raise",
                ).to_numpy(),
                np.arange(1, rows + 1),
            ),
            "History capacity mismatch",
        )

        closed = pd.to_numeric(
            group["candle_closed_at_rebuild"],
            errors="raise",
        )

        require(
            closed.isin([0, 1]).all(),
            "Invalid closed candle flag",
        )

        closed_count = int(closed.sum())

        require(
            np.array_equal(
                closed.to_numpy(),
                np.concatenate([
                    np.ones(
                        closed_count,
                        dtype=int,
                    ),
                    np.zeros(
                        rows - closed_count,
                        dtype=int,
                    ),
                ]),
            ),
            "Closed candles are not a prefix",
        )

        segments.append({
            "segment_id": int(segment_id),
            "indices": indices,
            "rows": rows,
            "closed_rows": closed_count,
        })

    require(
        len(segments)
        == int(record["segment_count"]),
        "Segment inventory mismatch",
    )

    return segments


# ============================================================
# NUMERIC COMPARISON
# ============================================================

def scalar_compare(reference, candidate):
    if pd.isna(reference) or pd.isna(candidate):
        return (
            False,
            None,
            None,
            "NON_FINITE",
        )

    try:
        a = float(reference)
        b = float(candidate)

    except (
        TypeError,
        ValueError,
    ):
        matched = str(reference) == str(candidate)

        return (
            matched,
            None,
            None,
            "" if matched else "VALUE_MISMATCH",
        )

    if not (
        np.isfinite(a)
        and np.isfinite(b)
    ):
        return (
            False,
            None,
            None,
            "NON_FINITE",
        )

    absolute_error = abs(a - b)

    relative_error = (
        absolute_error
        / max(abs(a), ATOL)
    )

    matched = bool(
        np.isclose(
            b,
            a,
            rtol=RTOL,
            atol=ATOL,
        )
    )

    return (
        matched,
        absolute_error,
        relative_error,
        "" if matched else "NUMERIC_MISMATCH",
    )


def serial_value(value):
    if pd.isna(value):
        return ""

    if isinstance(
        value,
        (np.integer, int),
    ):
        return int(value)

    if isinstance(
        value,
        (np.floating, float),
    ):
        value = float(value)

        return (
            value
            if np.isfinite(value)
            else str(value)
        )

    return str(value)


def compare_columns(
    reference,
    candidate,
    columns,
    layer,
    context,
):
    mismatches = []

    maximum_absolute = 0.0
    maximum_relative = 0.0

    for column in columns:
        require(
            column in reference.index
            and column in candidate.index,
            f"Missing comparison column: {column}",
        )

        a = reference[column]
        b = candidate[column]

        if layer == "SIGNAL":
            matched = (
                not pd.isna(a)
                and not pd.isna(b)
                and a == b
            )

            absolute_error = None
            relative_error = None

            reason = (
                ""
                if matched
                else "SIGNAL_MISMATCH"
            )

        else:
            (
                matched,
                absolute_error,
                relative_error,
                reason,
            ) = scalar_compare(a, b)

        if absolute_error is not None:
            maximum_absolute = max(
                maximum_absolute,
                absolute_error,
            )

        if relative_error is not None:
            maximum_relative = max(
                maximum_relative,
                relative_error,
            )

        if not matched:
            mismatches.append({
                **context,
                "layer": layer,
                "column": column,
                "reference_value":
                    serial_value(a),
                "truncated_value":
                    serial_value(b),
                "absolute_error":
                    absolute_error,
                "relative_error":
                    relative_error,
                "reason": reason,
            })

    return (
        len(mismatches) == 0,
        maximum_absolute,
        maximum_relative,
        mismatches,
    )


# ============================================================
# FEATURE AND DETECTOR REBUILD
# ============================================================

def rebuild_prefix(
    source,
    feature_module,
    detector_module,
):
    source = source.copy(
        deep=True
    ).reset_index(drop=True)

    features = feature_module.build_features(
        source.copy(deep=True)
    )

    signals = (
        detector_module.build_detector_dataframe(
            features.copy(deep=True)
        )
    )

    require(
        len(features) == len(source),
        "Feature rebuild row count mismatch",
    )

    require(
        len(signals) == len(source),
        "Detector rebuild row count mismatch",
    )

    for frame, label in (
        (features, "features"),
        (signals, "signals"),
    ):
        frame[TIMESTAMP] = pd.to_datetime(
            frame[TIMESTAMP],
            utc=True,
            errors="raise",
        )

        verify_alignment(
            source,
            frame,
            label,
        )

    return features, signals


def sample_positions(closed_rows, warmups):
    minimum = (
        max(warmups)
        + REFERENCE_EXTRA_BARS
    )

    if closed_rows < minimum:
        return []

    positions = np.linspace(
        minimum - 1,
        closed_rows - 1,
        num=SAMPLES_PER_COHORT,
        dtype=int,
    )

    return sorted(
        set(
            int(position)
            for position in positions
        )
    )


# ============================================================
# EXTENDED SEGMENT AUDIT
# ============================================================

def audit_segment(
    market,
    segment,
    source,
    rebuilt_features,
    rebuilt_signals,
    feature_module,
    detector_module,
    feature_columns,
    detector_columns,
):
    segment_id = segment["segment_id"]
    indices = segment["indices"]

    local = source.iloc[
        indices
    ].reset_index(drop=True)

    saved_features = rebuilt_features.iloc[
        indices
    ].reset_index(drop=True)

    saved_signals = rebuilt_signals.iloc[
        indices
    ].reset_index(drop=True)

    details = []
    mismatches = []
    coverage = []

    reference_cache = {}
    truncated_cache = {}

    for cohort, warmups in COHORTS.items():
        positions = sample_positions(
            segment["closed_rows"],
            warmups,
        )

        coverage.append({
            "market": market,
            "segment_id": segment_id,
            "segment_rows": segment["rows"],
            "cohort": cohort,
            "minimum_reference_rows":
                max(warmups)
                + REFERENCE_EXTRA_BARS,
            "sample_count": len(positions),
            "status":
                "TESTED"
                if positions
                else "INSUFFICIENT_HISTORY",
        })

        for position in positions:
            timestamp = local.iloc[
                position
            ][TIMESTAMP].isoformat()

            if position not in reference_cache:
                prefix = local.iloc[
                    :position + 1
                ]

                ref_features, ref_signals = (
                    rebuild_prefix(
                        prefix,
                        feature_module,
                        detector_module,
                    )
                )

                reference_feature = (
                    ref_features.iloc[-1]
                )

                reference_signal = (
                    ref_signals.iloc[-1]
                )

                # Confirm the reference is consistent
                # with the V010 full-segment output.
                reference_context = {
                    "market": market,
                    "segment_id": segment_id,
                    "cohort": "REFERENCE",
                    "timestamp": timestamp,
                    "warmup_bars": position + 1,
                }

                feature_check = compare_columns(
                    reference_feature,
                    saved_features.iloc[position],
                    feature_columns,
                    "FEATURE",
                    reference_context,
                )

                detector_check = compare_columns(
                    reference_signal,
                    saved_signals.iloc[position],
                    detector_columns,
                    "DETECTOR",
                    reference_context,
                )

                signal_check = compare_columns(
                    reference_signal,
                    saved_signals.iloc[position],
                    SIGNAL_COLUMNS,
                    "SIGNAL",
                    reference_context,
                )

                require(
                    feature_check[0]
                    and detector_check[0]
                    and signal_check[0],
                    (
                        "Full-prefix reference mismatch: "
                        f"{market} / {segment_id} / "
                        f"{timestamp}"
                    ),
                )

                reference_cache[position] = (
                    reference_feature,
                    reference_signal,
                )

            (
                reference_feature,
                reference_signal,
            ) = reference_cache[position]

            for warmup in warmups:
                cache_key = (
                    position,
                    warmup,
                )

                if cache_key not in truncated_cache:
                    start = (
                        position - warmup + 1
                    )

                    require(
                        start >= 0,
                        "Warmup crosses segment boundary",
                    )

                    truncated = local.iloc[
                        start:position + 1
                    ]

                    require(
                        len(truncated) == warmup,
                        "Truncated history length mismatch",
                    )

                    candidate_features, candidate_signals = (
                        rebuild_prefix(
                            truncated,
                            feature_module,
                            detector_module,
                        )
                    )

                    truncated_cache[cache_key] = (
                        candidate_features.iloc[-1],
                        candidate_signals.iloc[-1],
                    )

                (
                    candidate_feature,
                    candidate_signal,
                ) = truncated_cache[cache_key]

                context = {
                    "market": market,
                    "segment_id": segment_id,
                    "cohort": cohort,
                    "timestamp": timestamp,
                    "warmup_bars": warmup,
                }

                feature_result = compare_columns(
                    reference_feature,
                    candidate_feature,
                    feature_columns,
                    "FEATURE",
                    context,
                )

                signal_result = compare_columns(
                    reference_signal,
                    candidate_signal,
                    SIGNAL_COLUMNS,
                    "SIGNAL",
                    context,
                )

                detector_result = compare_columns(
                    reference_signal,
                    candidate_signal,
                    detector_columns,
                    "DETECTOR",
                    context,
                )

                feature_match = feature_result[0]
                signal_match = signal_result[0]
                detector_match = detector_result[0]

                combined_match = (
                    feature_match
                    and signal_match
                    and detector_match
                )

                details.append({
                    **context,
                    "reference_history_bars":
                        position + 1,
                    "features_checked":
                        len(feature_columns),
                    "detector_columns_checked":
                        len(detector_columns),
                    "feature_match":
                        int(feature_match),
                    "signal_match":
                        int(signal_match),
                    "detector_match":
                        int(detector_match),
                    "combined_match":
                        int(combined_match),
                    "max_absolute_error": max(
                        feature_result[1],
                        detector_result[1],
                    ),
                    "max_relative_error": max(
                        feature_result[2],
                        detector_result[2],
                    ),
                })

                mismatches.extend(
                    feature_result[3]
                )

                mismatches.extend(
                    signal_result[3]
                )

                mismatches.extend(
                    detector_result[3]
                )

    return (
        details,
        mismatches,
        coverage,
    )


# ============================================================
# CHECKPOINT MANAGEMENT
# ============================================================

def market_output_paths(output, market):
    return {
        "detail":
            output / "market_detail"
            / f"{market}.csv",

        "mismatches":
            output / "market_mismatches"
            / f"{market}.csv",

        "coverage":
            output / "market_coverage"
            / f"{market}.csv",
    }


def checkpoint_path(output, market):
    return (
        output
        / "checkpoints"
        / f"{market}.json"
    )


def expected_sample_inventory(
    market,
    source,
    segments,
):
    expected = set()

    for segment in segments:
        local = source.iloc[
            segment["indices"]
        ].reset_index(drop=True)

        for cohort, warmups in COHORTS.items():
            positions = sample_positions(
                segment["closed_rows"],
                warmups,
            )

            for position in positions:
                timestamp = local.iloc[
                    position
                ][TIMESTAMP].isoformat()

                for warmup in warmups:
                    expected.add((
                        market,
                        segment["segment_id"],
                        cohort,
                        timestamp,
                        warmup,
                    ))

    return expected


def validate_market_outputs(
    paths,
    market,
    source,
    segments,
):
    detail = pd.read_csv(paths["detail"])
    mismatches = pd.read_csv(
        paths["mismatches"]
    )
    coverage = pd.read_csv(
        paths["coverage"]
    )

    require(
        list(detail.columns)
        == DETAIL_COLUMNS,
        "Detail schema mismatch",
    )

    require(
        list(mismatches.columns)
        == MISMATCH_COLUMNS,
        "Mismatch schema mismatch",
    )

    require(
        list(coverage.columns)
        == COVERAGE_COLUMNS,
        "Coverage schema mismatch",
    )

    key_columns = [
        "market",
        "segment_id",
        "cohort",
        "timestamp",
        "warmup_bars",
    ]

    require(
        not detail.duplicated(
            key_columns
        ).any(),
        "Duplicate warmup checks",
    )

    expected = expected_sample_inventory(
        market,
        source,
        segments,
    )

    actual = set(
        detail[key_columns].itertuples(
            index=False,
            name=None,
        )
    )

    require(
        actual == expected,
        "Checkpoint sample inventory mismatch",
    )

    flags = [
        "feature_match",
        "signal_match",
        "detector_match",
        "combined_match",
    ]

    for column in flags:
        require(
            detail[column].isin(
                [0, 1]
            ).all(),
            f"Invalid flag: {column}",
        )

    combined = (
        detail["feature_match"].eq(1)
        & detail["signal_match"].eq(1)
        & detail["detector_match"].eq(1)
    ).astype(int)

    require(
        combined.eq(
            detail["combined_match"]
        ).all(),
        "Combined match inconsistency",
    )

    mismatch_keys = set(
        mismatches[key_columns].itertuples(
            index=False,
            name=None,
        )
    )

    failed_keys = set(
        detail.loc[
            detail["combined_match"].eq(0),
            key_columns,
        ].itertuples(
            index=False,
            name=None,
        )
    )

    require(
        mismatch_keys == failed_keys,
        "Mismatch evidence inventory mismatch",
    )

    expected_coverage = {}

    for segment in segments:
        for cohort, warmups in COHORTS.items():
            count = len(
                sample_positions(
                    segment["closed_rows"],
                    warmups,
                )
            )

            expected_coverage[
                (
                    market,
                    segment["segment_id"],
                    cohort,
                )
            ] = count

    actual_coverage = {}

    for row in coverage.itertuples(
        index=False
    ):
        key = (
            row.market,
            row.segment_id,
            row.cohort,
        )

        require(
            key not in actual_coverage,
            "Duplicate coverage record",
        )

        actual_coverage[key] = (
            int(row.sample_count)
        )

        expected_status = (
            "TESTED"
            if row.sample_count > 0
            else "INSUFFICIENT_HISTORY"
        )

        require(
            row.status == expected_status,
            "Invalid coverage status",
        )

    require(
        actual_coverage
        == expected_coverage,
        "Coverage inventory mismatch",
    )

    return {
        "checks": len(detail),
        "mismatch_records": len(mismatches),
        "coverage_records": len(coverage),
    }


def load_checkpoint(
    output,
    market,
    context_hash,
    input_hashes,
    paths,
    source,
    segments,
):
    path = checkpoint_path(
        output,
        market,
    )

    sidecar = path.with_suffix(
        ".sha256.json"
    )

    if not (
        path.is_file()
        and sidecar.is_file()
    ):
        return None

    try:
        sidecar_data = read_json(sidecar)

        verify_hash(
            path,
            sidecar_data["sha256"],
        )

        checkpoint = read_json(path)

        require(
            checkpoint["version"] == VERSION,
            "Checkpoint version mismatch",
        )

        require(
            checkpoint["status"] == "COMPLETED",
            "Checkpoint incomplete",
        )

        require(
            checkpoint["market"] == market,
            "Checkpoint market mismatch",
        )

        require(
            checkpoint["context_sha256"]
            == context_hash,
            "Checkpoint context changed",
        )

        require(
            checkpoint["input_sha256"]
            == input_hashes,
            "Checkpoint inputs changed",
        )

        for key, path in paths.items():
            verify_hash(
                path,
                checkpoint[
                    "output_sha256"
                ][key],
            )

        actual_counts = (
            validate_market_outputs(
                paths,
                market,
                source,
                segments,
            )
        )

        require(
            actual_counts
            == checkpoint["counts"],
            "Checkpoint count mismatch",
        )

        return checkpoint

    except Exception as exc:
        print(
            f"[REBUILD] {market}: "
            f"invalid checkpoint: {exc}",
            flush=True,
        )

        return None


def save_checkpoint(
    output,
    market,
    context_hash,
    input_hashes,
    paths,
    counts,
):
    checkpoint = {
        "version": VERSION,
        "market": market,
        "status": "COMPLETED",
        "completed_utc": utc_now(),
        "context_sha256": context_hash,
        "input_sha256": input_hashes,
        "output_sha256": {
            key: sha256_file(path)
            for key, path in paths.items()
        },
        "counts": counts,
    }

    path = checkpoint_path(
        output,
        market,
    )

    atomic_json(path, checkpoint)

    atomic_json(
        path.with_suffix(".sha256.json"),
        {
            "sha256": sha256_file(path),
        },
    )

    return checkpoint


# ============================================================
# MARKET PROCESSING
# ============================================================

def process_market(
    root,
    output,
    evidence,
    market,
    context_hash,
    feature_module,
    detector_module,
    feature_columns,
    detector_columns,
    refresh,
):
    input_paths = get_market_paths(
        root,
        evidence,
        market,
    )

    input_hashes = verify_market_hashes(
        input_paths,
        evidence,
        market,
    )

    source = read_market_csv(
        input_paths["ohlcv"],
        market,
    )

    rebuilt_features = read_market_csv(
        input_paths["rebuilt_features"],
        market,
    )

    rebuilt_signals = read_market_csv(
        input_paths["rebuilt_signals"],
        market,
    )

    mask = read_market_csv(
        input_paths["policy_mask"],
        market,
    )

    verify_alignment(
        source,
        rebuilt_features,
        "OHLCV / rebuilt features",
    )

    verify_alignment(
        source,
        rebuilt_signals,
        "OHLCV / rebuilt signals",
    )

    segments = validate_segments(
        source,
        mask,
        evidence["markets"][market],
    )

    output_paths = market_output_paths(
        output,
        market,
    )

    checkpoint = None

    if not refresh:
        checkpoint = load_checkpoint(
            output,
            market,
            context_hash,
            input_hashes,
            output_paths,
            source,
            segments,
        )

    resumed = checkpoint is not None

    if checkpoint is None:
        details = []
        mismatches = []
        coverage = []

        for segment in segments:
            (
                segment_details,
                segment_mismatches,
                segment_coverage,
            ) = audit_segment(
                market,
                segment,
                source,
                rebuilt_features,
                rebuilt_signals,
                feature_module,
                detector_module,
                feature_columns,
                detector_columns,
            )

            details.extend(
                segment_details
            )

            mismatches.extend(
                segment_mismatches
            )

            coverage.extend(
                segment_coverage
            )

        atomic_csv(
            output_paths["detail"],
            pd.DataFrame(
                details,
                columns=DETAIL_COLUMNS,
            ),
        )

        atomic_csv(
            output_paths["mismatches"],
            pd.DataFrame(
                mismatches,
                columns=MISMATCH_COLUMNS,
            ),
        )

        atomic_csv(
            output_paths["coverage"],
            pd.DataFrame(
                coverage,
                columns=COVERAGE_COLUMNS,
            ),
        )

        counts = validate_market_outputs(
            output_paths,
            market,
            source,
            segments,
        )

        require(
            verify_market_hashes(
                input_paths,
                evidence,
                market,
            ) == input_hashes,
            f"Source changed: {market}",
        )

        checkpoint = save_checkpoint(
            output,
            market,
            context_hash,
            input_hashes,
            output_paths,
            counts,
        )

    return {
        "market": market,
        "status": "PASS",
        "rows": len(source),
        "segments": len(segments),
        "resumed": resumed,
        "input_sha256": input_hashes,
        "output_sha256":
            checkpoint["output_sha256"],
        **checkpoint["counts"],
    }


# ============================================================
# SUMMARY GENERATION
# ============================================================

def collect_market_outputs(
    output,
    market_records,
):
    frames = {
        "detail": [],
        "mismatches": [],
        "coverage": [],
    }

    inventory = {}

    for record in market_records:
        market = record["market"]

        paths = market_output_paths(
            output,
            market,
        )

        for key, path in paths.items():
            expected = record[
                "output_sha256"
            ][key]

            verify_hash(path, expected)

            relative = path.relative_to(
                output
            ).as_posix()

            inventory[relative] = expected

            frames[key].append(
                pd.read_csv(path)
            )

    combined = {
        key: pd.concat(
            values,
            ignore_index=True,
        )
        for key, values in frames.items()
    }

    return combined, inventory


def build_warmup_summary(detail):
    rows = []

    for cohort, warmups in COHORTS.items():
        cohort_frame = detail.loc[
            detail["cohort"].eq(cohort)
        ]

        for warmup in warmups:
            group = cohort_frame.loc[
                cohort_frame[
                    "warmup_bars"
                ].eq(warmup)
            ]

            count = len(group)

            feature_mismatch = int(
                group["feature_match"]
                .eq(0)
                .sum()
            )

            signal_mismatch = int(
                group["signal_match"]
                .eq(0)
                .sum()
            )

            detector_mismatch = int(
                group["detector_match"]
                .eq(0)
                .sum()
            )

            combined_mismatch = int(
                group["combined_match"]
                .eq(0)
                .sum()
            )

            if count == 0:
                status = "NO_SAMPLE"

            elif combined_mismatch == 0:
                status = "SAMPLED_MATCH"

            else:
                status = "MISMATCH_FOUND"

            rows.append({
                "cohort": cohort,
                "warmup_bars": warmup,
                "checks": count,
                "markets_tested":
                    int(group["market"].nunique()),
                "segments_tested": len(
                    group[
                        ["market", "segment_id"]
                    ].drop_duplicates()
                ),
                "feature_mismatch":
                    feature_mismatch,
                "signal_mismatch":
                    signal_mismatch,
                "detector_mismatch":
                    detector_mismatch,
                "combined_mismatch":
                    combined_mismatch,
                "combined_match_pct": (
                    100.0
                    * (count - combined_mismatch)
                    / count
                    if count
                    else None
                ),
                "status": status,
            })

    return pd.DataFrame(rows)


def build_feature_summary(mismatches):
    columns = [
        "cohort",
        "warmup_bars",
        "layer",
        "column",
        "reason",
        "mismatch_records",
        "affected_markets",
        "max_absolute_error",
        "max_relative_error",
    ]

    if mismatches.empty:
        return pd.DataFrame(
            columns=columns
        )

    result = (
        mismatches.groupby(
            [
                "cohort",
                "warmup_bars",
                "layer",
                "column",
                "reason",
            ],
            dropna=False,
        )
        .agg(
            mismatch_records=(
                "market",
                "size",
            ),
            affected_markets=(
                "market",
                "nunique",
            ),
            max_absolute_error=(
                "absolute_error",
                "max",
            ),
            max_relative_error=(
                "relative_error",
                "max",
            ),
        )
        .reset_index()
    )

    return result.sort_values(
        [
            "cohort",
            "warmup_bars",
            "mismatch_records",
        ],
        ascending=[
            True,
            True,
            False,
        ],
    )


def build_coverage_summary(coverage):
    rows = []

    for cohort in COHORTS:
        group = coverage.loc[
            coverage["cohort"].eq(cohort)
        ]

        tested = group.loc[
            group["status"].eq("TESTED")
        ]

        rows.append({
            "cohort": cohort,
            "segments_total": len(group),
            "segments_tested": len(tested),
            "segments_untested":
                len(group) - len(tested),
            "markets_tested":
                int(tested["market"].nunique()),
            "sample_rows":
                int(tested["sample_count"].sum()),
        })

    return pd.DataFrame(rows)


def find_sampled_candidates(summary):
    candidates = []

    for cohort, warmups in COHORTS.items():
        group = summary.loc[
            summary["cohort"].eq(cohort)
        ]

        for warmup in warmups:
            suffix = group.loc[
                group["warmup_bars"].ge(warmup)
            ]

            if suffix.empty:
                continue

            if (
                suffix["checks"].gt(0).all()
                and suffix[
                    "combined_mismatch"
                ].eq(0).all()
            ):
                candidates.append({
                    "cohort": cohort,
                    "sampled_candidate_bars":
                        warmup,
                    "checks_per_length":
                        int(
                            suffix[
                                "checks"
                            ].min()
                        ),
                    "approved": False,
                })

                break

    return candidates


# ============================================================
# SELF TESTS
# ============================================================

def run_self_tests():
    require(
        scalar_compare(1.0, 1.0)[0],
        "Exact comparison test failed",
    )

    require(
        scalar_compare(
            100.0,
            100.000001,
        )[0],
        "Tolerance test failed",
    )

    require(
        not scalar_compare(
            100.0,
            101.0,
        )[0],
        "Mismatch detection failed",
    )

    require(
        not scalar_compare(
            np.nan,
            np.nan,
        )[0],
        "Non-finite test failed",
    )

    require(
        not scalar_compare(
            np.inf,
            np.inf,
        )[0],
        "Infinity test failed",
    )

    require(
        sample_positions(
            599,
            (300, 500),
        ) == [],
        "Short segment test failed",
    )

    require(
        sample_positions(
            600,
            (300, 500),
        ) == [599],
        "Boundary sample test failed",
    )

    positions = sample_positions(
        2000,
        WARMUP_BARS,
    )

    require(
        len(positions)
        == SAMPLES_PER_COHORT,
        "Sample count test failed",
    )

    require(
        positions[0] == 1599
        and positions[-1] == 1999,
        "Sample boundary test failed",
    )

    for position in positions:
        for warmup in WARMUP_BARS:
            require(
                position - warmup + 1 >= 0,
                "Segment boundary test failed",
            )

    print(
        "[PASS] V011.1 policy self-tests",
        flush=True,
    )


# ============================================================
# FINAL REPORT
# ============================================================

def write_final_reports(
    root,
    output,
    evidence,
    context,
    market_records,
    run_id,
):
    combined, output_inventory = (
        collect_market_outputs(
            output,
            market_records,
        )
    )

    detail = combined["detail"]
    mismatches = combined["mismatches"]
    coverage = combined["coverage"]

    require(
        len(output_inventory)
        == EXPECTED_MARKETS * 3,
        "Output inventory count mismatch",
    )

    require(
        len(market_records)
        == EXPECTED_MARKETS,
        "Final market count mismatch",
    )

    require(
        sum(
            record["segments"]
            for record in market_records
        ) == EXPECTED_SEGMENTS,
        "Final segment count mismatch",
    )

    require(
        sum(
            record["rows"]
            for record in market_records
        ) == EXPECTED_ROWS,
        "Final row count mismatch",
    )

    require(
        len(coverage)
        == EXPECTED_SEGMENTS * len(COHORTS),
        "Final coverage count mismatch",
    )

    require(
        len(detail) > 0,
        "No warmup comparisons performed",
    )

    summary = build_warmup_summary(
        detail
    )

    feature_summary = build_feature_summary(
        mismatches
    )

    coverage_summary = (
        build_coverage_summary(
            coverage
        )
    )

    candidates = find_sampled_candidates(
        summary
    )

    market_summary = pd.DataFrame([
        {
            key: value
            for key, value in record.items()
            if key not in (
                "input_sha256",
                "output_sha256",
            )
        }
        for record in market_records
    ])

    reports = {
        "warmup_detail.csv": detail,
        "warmup_summary.csv": summary,
        "feature_mismatch_detail.csv":
            mismatches,
        "feature_mismatch_summary.csv":
            feature_summary,
        "segment_coverage.csv":
            coverage,
        "coverage_summary.csv":
            coverage_summary,
        "market_summary.csv":
            market_summary,
    }

    for filename, frame in reports.items():
        atomic_csv(
            output / filename,
            frame,
        )

    # Recheck all original and rebuilt market inputs.
    for record in market_records:
        market = record["market"]

        paths = get_market_paths(
            root,
            evidence,
            market,
        )

        require(
            verify_market_hashes(
                paths,
                evidence,
                market,
            ) == record["input_sha256"],
            f"Final source mismatch: {market}",
        )

    # Recheck source reports and implementations.
    final_evidence = verify_source_evidence(
        root
    )

    require(
        final_evidence["source_hashes"]
        == evidence["source_hashes"],
        "Source evidence changed during audit",
    )

    require(
        final_evidence[
            "implementation_hashes"
        ]
        == evidence[
            "implementation_hashes"
        ],
        "Source implementation changed",
    )

    require(
        sha256_file(Path(__file__))
        == context["script_sha256"],
        "Audit implementation changed",
    )

    result = {
        "version": VERSION,
        "run_id": run_id,
        "created_utc": utc_now(),
        "execution": "PASS",
        "classification":
            "EXTENDED_SAMPLED_CONVERGENCE_AUDITED",
        "source_version": SOURCE_VERSION,
        "source_sha256":
            evidence["source_hashes"],
        "context_sha256":
            json_digest(context),
        "markets_checked":
            EXPECTED_MARKETS,
        "segments_checked":
            EXPECTED_SEGMENTS,
        "rows_checked":
            EXPECTED_ROWS,
        "warmup_bars":
            list(WARMUP_BARS),
        "relative_tolerance": RTOL,
        "absolute_tolerance": ATOL,
        "samples_per_cohort":
            SAMPLES_PER_COHORT,
        "reference_extra_bars":
            REFERENCE_EXTRA_BARS,
        "warmup_checks":
            len(detail),
        "feature_mismatch_checks": int(
            detail["feature_match"].eq(0).sum()
        ),
        "signal_mismatch_checks": int(
            detail["signal_match"].eq(0).sum()
        ),
        "detector_mismatch_checks": int(
            detail["detector_match"].eq(0).sum()
        ),
        "combined_mismatch_checks": int(
            detail["combined_match"].eq(0).sum()
        ),
        "mismatch_records":
            len(mismatches),
        "original_input_files_verified":
            EXPECTED_MARKETS * 3,
        "rebuilt_input_files_verified":
            EXPECTED_MARKETS * 3,
        "v011_output_files_verified":
            580,
        "output_files_verified":
            len(output_inventory),
        "resumed_markets": sum(
            int(record["resumed"])
            for record in market_records
        ),
        "sampled_candidates":
            candidates,
        "extended_warmup_audit_completed":
            True,
        "actual_feature_warmup_verified":
            False,
        "actual_forward_horizon_verified":
            False,
        "gap_policy_implemented_in_evaluator":
            False,
        "remaining_rows_approved_for_backtest":
            False,
        "source_csv_modified":
            False,
        "synthetic_candles_created":
            0,
        "point_in_time_proven":
            False,
        "independent_oos_proven":
            False,
        "profitability_proven":
            False,
        "train_approved":
            False,
        "live_trading_approved":
            False,
        "data_readiness":
            "BLOCKED",
        "research_decision":
            "HOLD",
        "markets":
            market_records,
    }

    atomic_json(
        output / "result.json",
        result,
    )

    lines = [
        "# Extended Warmup Convergence Audit",
        "",
        f"Version: {VERSION}",
        f"Run ID: {run_id}",
        "",
        "## Execution",
        "",
        "- Execution: PASS",
        f"- Markets: {EXPECTED_MARKETS}",
        f"- Segments: {EXPECTED_SEGMENTS}",
        f"- Rows: {EXPECTED_ROWS}",
        f"- Warmup checks: {len(detail)}",
        "- Research decision: HOLD",
        "- Data readiness: BLOCKED",
        "",
        "## Warmup comparison",
        "",
        "| Cohort | Bars | Checks | Feature mismatch "
        "| Signal mismatch | Detector mismatch "
        "| Combined mismatch | Status |",
        "|---|---:|---:|---:|---:|---:|---:|---|",
    ]

    for row in summary.itertuples(
        index=False
    ):
        lines.append(
            f"| {row.cohort} "
            f"| {row.warmup_bars} "
            f"| {row.checks} "
            f"| {row.feature_mismatch} "
            f"| {row.signal_mismatch} "
            f"| {row.detector_mismatch} "
            f"| {row.combined_mismatch} "
            f"| {row.status} |"
        )

    lines.extend([
        "",
        "## Coverage",
        "",
        "| Cohort | Segments | Tested "
        "| Untested | Markets | Samples |",
        "|---|---:|---:|---:|---:|---:|",
    ])

    for row in coverage_summary.itertuples(
        index=False
    ):
        lines.append(
            f"| {row.cohort} "
            f"| {row.segments_total} "
            f"| {row.segments_tested} "
            f"| {row.segments_untested} "
            f"| {row.markets_tested} "
            f"| {row.sample_rows} |"
        )

    lines.extend([
        "",
        "## Sampled candidates",
        "",
    ])

    if candidates:
        for candidate in candidates:
            lines.append(
                "- "
                + candidate["cohort"]
                + ": "
                + str(
                    candidate[
                        "sampled_candidate_bars"
                    ]
                )
                + " bars; NOT APPROVED"
            )
    else:
        lines.append(
            "- No zero-mismatch candidate found."
        )

    lines.extend([
        "",
        "## Limitations",
        "",
        "- Comparisons are sampled, not exhaustive.",
        "- Cohorts have different eligible segments.",
        "- Compare warmup lengths within a cohort.",
        "- Short segments remain unverified.",
        "- Full-prefix reference starts at segment start.",
        "- Unknown pre-gap history is not recovered.",
        "- Recursive indicator convergence is not proven.",
        "- Horizon policy integration remains pending.",
        "- No backtest, training or trading approval.",
        "- No synthetic candles are created.",
        "",
    ])

    atomic_text(
        output / "report.md",
        "\n".join(lines),
    )

    report_names = [
        *reports.keys(),
        "result.json",
        "report.md",
    ]

    manifest = {
        "version": VERSION,
        "run_id": run_id,
        "created_utc": utc_now(),
        "source_version": SOURCE_VERSION,
        "source_sha256":
            evidence["source_hashes"],
        "context": context,
        "sha256": {
            name: sha256_file(
                output / name
            )
            for name in report_names
        },
        "output_sha256":
            output_inventory,
    }

    atomic_json(
        output / "manifest.json",
        manifest,
    )

    verify_inventory(
        output,
        manifest["sha256"],
    )

    verify_inventory(
        output,
        manifest["output_sha256"],
    )

    atomic_json(
        output / "run_state.json",
        {
            "version": VERSION,
            "run_id": run_id,
            "status": "COMPLETED",
            "completed_utc": utc_now(),
            "manifest_sha256": sha256_file(
                output / "manifest.json"
            ),
        },
    )

    print(
        "\n"
        + "\n".join(lines),
        flush=True,
    )

    return result


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--root",
        required=True,
    )

    parser.add_argument(
        "--run-id",
        default=None,
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

    root = Path(
        args.root
    ).resolve()

    require(
        root.is_dir(),
        f"Invalid project root: {root}",
    )

    sys.path.insert(
        0,
        str(root),
    )

    run_id = (
        args.run_id
        or datetime.now(
            timezone.utc
        ).strftime(
            "%Y%m%dT%H%M%SZ"
        )
    )

    output = (
        root
        / "data"
        / "reports"
        / "pattern_256_extended_warmup"
    )

    output.mkdir(
        parents=True,
        exist_ok=True,
    )

    atomic_json(
        output / "run_state.json",
        {
            "version": VERSION,
            "run_id": run_id,
            "status": "RUNNING",
            "started_utc": utc_now(),
        },
    )

    try:
        evidence = verify_source_evidence(
            root
        )

        feature_module = load_module(
            root / "build_features.py",
            "v0111_build_features",
        )

        detector_module = load_module(
            root / "detect_256.py",
            "v0111_detect_256",
        )

        feature_columns = list(
            dict.fromkeys(
                feature_module
                .get_required_feature_columns()
            )
        )

        # Identifiers are verified separately.
        feature_columns = [
            column
            for column in feature_columns
            if column not in (
                "market",
                TIMESTAMP,
                "candle_date_time_kst",
            )
        ]

        detector_columns = list(
            dict.fromkeys(
                detector_module.DETECTOR_COLUMNS
            )
        )

        detector_columns = [
            column
            for column in detector_columns
            if column not in (
                "market",
                TIMESTAMP,
                "candle_date_time_kst",
                *SIGNAL_COLUMNS,
            )
        ]

        require(
            bool(feature_columns),
            "Empty feature comparison definition",
        )

        require(
            bool(detector_columns),
            "Empty detector comparison definition",
        )

        context = {
            "version": VERSION,
            "script_sha256":
                sha256_file(Path(__file__)),
            "source_sha256":
                evidence["source_hashes"],
            "implementation_sha256":
                evidence["implementation_hashes"],
            "python":
                platform.python_version(),
            "pandas":
                pd.__version__,
            "numpy":
                np.__version__,
            "warmup_bars":
                list(WARMUP_BARS),
            "cohorts": {
                key: list(value)
                for key, value in COHORTS.items()
            },
            "samples_per_cohort":
                SAMPLES_PER_COHORT,
            "reference_extra_bars":
                REFERENCE_EXTRA_BARS,
            "rtol": RTOL,
            "atol": ATOL,
            "feature_columns":
                feature_columns,
            "detector_columns":
                detector_columns,
        }

        context_hash = json_digest(
            context
        )

        markets = sorted(
            evidence["markets"]
        )

        records = []

        print(
            f"Version: {VERSION}",
            flush=True,
        )

        print(
            f"Context SHA256: {context_hash}",
            flush=True,
        )

        print(
            f"Markets: {len(markets)}",
            flush=True,
        )

        print(
            f"Refresh: {args.refresh}",
            flush=True,
        )

        for index, market in enumerate(
            markets,
            start=1,
        ):
            print(
                f"[{index}/{len(markets)}] "
                f"{market}",
                flush=True,
            )

            record = process_market(
                root,
                output,
                evidence,
                market,
                context_hash,
                feature_module,
                detector_module,
                feature_columns,
                detector_columns,
                args.refresh,
            )

            records.append(record)

            mode = (
                "RESUME"
                if record["resumed"]
                else "PASS"
            )

            print(
                f"[{mode}] {market} | "
                f"checks={record['checks']} | "
                f"mismatches="
                f"{record['mismatch_records']}",
                flush=True,
            )

            atomic_json(
                output / "run_state.json",
                {
                    "version": VERSION,
                    "run_id": run_id,
                    "status": "RUNNING",
                    "markets_completed": index,
                    "markets_total": len(markets),
                    "last_market": market,
                    "updated_utc": utc_now(),
                },
            )

        write_final_reports(
            root,
            output,
            evidence,
            context,
            records,
            run_id,
        )

        print(
            "[PASS] Clean V011.1 completed",
            flush=True,
        )

        print(
            "Research decision: HOLD",
            flush=True,
        )

        return 0

    except BaseException as exc:
        atomic_json(
            output / "run_state.json",
            {
                "version": VERSION,
                "run_id": run_id,
                "status": "FAILED",
                "failed_utc": utc_now(),
                "error": str(exc),
                "research_decision": "HOLD",
            },
        )

        traceback.print_exc()

        return 1


if __name__ == "__main__":
    sys.exit(main())
