"""
Upbit Surge Monitor
Pattern 256 Full-Market Prefix Replay Audit
Clean V001

Purpose
-------
Run prefix replay verification across every eligible KRW market
for h1, h4, and d1.

Verify that historical feature and Pattern 256 signal values
can be reproduced using only candles available at each cutoff.

Default cutoffs:
    40%, 65%, 85%

Minimum candles per cutoff:
    300

Important
---------
- Existing OHLCV, feature, and signal files are read-only.
- Original feature and signal calculation functions are reused.
- Every eligible market is included; no sampling.
- Insufficient-history markets are recorded separately.
- Any execution error or mismatch results in FAIL.
- Resume uses verified per-market checkpoints.
- A checkpoint is reused only when its input hashes,
  configuration, schema, and recorded results are valid.
- No automated trading.
- No training approval.
- No live trading approval.

Output directory
----------------
data/reports/pattern_256_prefix_replay_full_market/

Outputs
-------
full_market_checks.csv
full_market_issues.csv
full_market_exclusions.csv
full_market_result.json
full_market_manifest.json

Internal checkpoints
--------------------
checkpoints/{timeframe}/{market}.json

Run
---
py -u audit_pattern_256_prefix_replay_full_market.py --root .

Force replay
------------
py -u audit_pattern_256_prefix_replay_full_market.py --root . --force
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import importlib.util
import json
import math
import os
import sys
import traceback

from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd


VERSION = "Clean V001"

AUDIT_NAME = "pattern_256_prefix_replay_full_market"

TIMEFRAMES = ("h1", "h4", "d1")

DEFAULT_CUTOFFS = (0.40, 0.65, 0.85)

MINIMUM_CANDLES = 300

RTOL = 1e-8
ATOL = 1e-9

OUTPUT_DIRECTORY = (
    "data/reports/pattern_256_prefix_replay_full_market"
)

CHECK_COLUMNS = [
    "timeframe",
    "market",
    "cutoff_fraction",
    "cutoff_row",
    "cutoff_timestamp",
    "source_rows",
    "feature_columns_checked",
    "signal_columns_checked",
    "feature_mismatches",
    "signal_mismatches",
    "status",
    "message",
]

ISSUE_COLUMNS = [
    "timeframe",
    "market",
    "cutoff_fraction",
    "cutoff_timestamp",
    "category",
    "column",
    "expected",
    "actual",
    "message",
]

EXCLUSION_COLUMNS = [
    "timeframe",
    "market",
    "source_rows",
    "minimum_required_rows",
    "reason",
    "message",
]


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path):
    digest = hashlib.sha256()

    with Path(path).open("rb") as handle:
        while True:
            block = handle.read(1024 * 1024)

            if not block:
                break

            digest.update(block)

    return digest.hexdigest()


def atomic_json(path, payload):
    path = Path(path)

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temporary = path.with_name(path.name + ".tmp")

    try:
        with temporary.open(
            "w",
            encoding="utf-8",
        ) as handle:
            json.dump(
                payload,
                handle,
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
                allow_nan=False,
            )

            handle.write("\n")

        os.replace(temporary, path)

    finally:
        if temporary.exists():
            temporary.unlink()


def atomic_csv(path, rows, columns):
    path = Path(path)

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temporary = path.with_name(path.name + ".tmp")

    try:
        with temporary.open(
            "w",
            encoding="utf-8-sig",
            newline="",
        ) as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=columns,
                extrasaction="ignore",
            )

            writer.writeheader()
            writer.writerows(rows)

        os.replace(temporary, path)

    finally:
        if temporary.exists():
            temporary.unlink()


def load_module(path, name):
    path = Path(path)

    if not path.is_file():
        raise FileNotFoundError(
            f"Required Python module missing: {path}"
        )

    spec = importlib.util.spec_from_file_location(
        name,
        str(path),
    )

    if spec is None or spec.loader is None:
        raise RuntimeError(
            f"Cannot load module: {path}"
        )

    module = importlib.util.module_from_spec(spec)

    spec.loader.exec_module(module)

    return module


def read_csv(path):
    path = Path(path)

    if not path.is_file():
        raise FileNotFoundError(
            f"Required CSV missing: {path}"
        )

    frame = pd.read_csv(
        path,
        encoding="utf-8-sig",
        low_memory=False,
    )

    if frame.empty:
        raise RuntimeError(
            f"CSV has no rows: {path}"
        )

    return frame


def verify_timestamps(frame, label):
    column = "candle_date_time_utc"

    if column not in frame.columns:
        raise RuntimeError(
            f"{label}: missing timestamp column"
        )

    times = pd.to_datetime(
        frame[column],
        utc=True,
        errors="coerce",
    )

    if times.isna().any():
        raise RuntimeError(
            f"{label}: invalid timestamps"
        )

    if times.duplicated().any():
        raise RuntimeError(
            f"{label}: duplicate timestamps"
        )

    if not times.is_monotonic_increasing:
        raise RuntimeError(
            f"{label}: unsorted timestamps"
        )

    return times


def verify_alignment(source, stored, label):
    if len(source) != len(stored):
        raise RuntimeError(
            f"{label}: row count mismatch: "
            f"source={len(source)}, "
            f"stored={len(stored)}"
        )

    source_times = verify_timestamps(
        source,
        "OHLCV",
    )

    stored_times = verify_timestamps(
        stored,
        label,
    )

    if not source_times.equals(stored_times):
        raise RuntimeError(
            f"{label}: timestamp alignment mismatch"
        )


def parse_fractions(value):
    fractions = tuple(
        float(item.strip())
        for item in value.split(",")
    )

    if not fractions:
        raise ValueError("Empty cutoff list")

    if len(set(fractions)) != len(fractions):
        raise ValueError("Duplicate cutoffs")

    for fraction in fractions:
        if not math.isfinite(fraction):
            raise ValueError("Non-finite cutoff")

        if not 0.1 <= fraction < 1.0:
            raise ValueError(
                f"Invalid cutoff: {fraction}"
            )

    return fractions


def calculate_cutoff_rows(total_rows, fraction):
    rows = int(total_rows * fraction)

    if rows < MINIMUM_CANDLES:
        raise RuntimeError(
            f"Insufficient prefix history: "
            f"rows={rows}"
        )

    if rows >= total_rows:
        raise RuntimeError(
            "Cutoff must exclude future rows"
        )

    return rows


def required_source_rows(fractions):
    smallest = min(fractions)

    required = max(
        MINIMUM_CANDLES + 1,
        math.ceil(MINIMUM_CANDLES / smallest),
    )

    while int(required * smallest) < MINIMUM_CANDLES:
        required += 1

    return required


def count_csv_rows(path):
    with Path(path).open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as handle:
        reader = csv.reader(handle)

        header = next(reader, None)

        if not header:
            raise RuntimeError(
                "Missing CSV header"
            )

        if "candle_date_time_utc" not in header:
            raise RuntimeError(
                "Missing timestamp column"
            )

        return sum(
            1 for row in reader if row
        )


def values_equal(expected, actual):
    if pd.isna(expected) and pd.isna(actual):
        return True

    if pd.isna(expected) or pd.isna(actual):
        return False

    if isinstance(
        expected,
        (int, float, np.number),
    ) and isinstance(
        actual,
        (int, float, np.number),
    ):
        left = float(expected)
        right = float(actual)

        if not math.isfinite(left):
            return left == right

        if not math.isfinite(right):
            return False

        return math.isclose(
            left,
            right,
            rel_tol=RTOL,
            abs_tol=ATOL,
        )

    return str(expected) == str(actual)


def compare_row(
    stored,
    replay,
    index,
    columns,
    category,
    context,
    issues,
):
    mismatches = 0

    for column in columns:
        if column not in stored.columns:
            mismatches += 1

            issues.append({
                **context,
                "category": category,
                "column": column,
                "expected": "MISSING_COLUMN",
                "actual": "",
                "message": "Stored column missing",
            })

            continue

        if column not in replay.columns:
            mismatches += 1

            issues.append({
                **context,
                "category": category,
                "column": column,
                "expected": "",
                "actual": "MISSING_COLUMN",
                "message": "Replay column missing",
            })

            continue

        expected = stored.iloc[index][column]
        actual = replay.iloc[index][column]

        if not values_equal(expected, actual):
            mismatches += 1

            issues.append({
                **context,
                "category": category,
                "column": column,
                "expected": str(expected),
                "actual": str(actual),
                "message": "Prefix replay mismatch",
            })

    return mismatches


def discover_jobs(root, fractions):
    minimum_rows = required_source_rows(
        fractions
    )

    jobs = []
    exclusions = []
    stats = {}

    for timeframe in TIMEFRAMES:
        source_dir = (
            root / "data" / "ohlcv" / timeframe
        )

        feature_dir = (
            root / "data" / "features" / timeframe
        )

        signal_dir = (
            root / "data" / "signals"
            / "256" / timeframe
        )

        for directory in (
            source_dir,
            feature_dir,
            signal_dir,
        ):
            if not directory.is_dir():
                raise RuntimeError(
                    f"Required directory missing: "
                    f"{directory}"
                )

        source_files = sorted(
            source_dir.glob("KRW-*.csv")
        )

        if not source_files:
            raise RuntimeError(
                f"No OHLCV files: {timeframe}"
            )

        eligible = 0
        excluded = 0

        for source_path in source_files:
            market = source_path.stem

            feature_path = (
                feature_dir / source_path.name
            )

            signal_path = (
                signal_dir / source_path.name
            )

            reason = None
            message = ""
            source_rows = ""

            if not feature_path.is_file():
                reason = "FEATURE_MISSING"
                message = "Feature file missing"

            elif not signal_path.is_file():
                reason = "SIGNAL_MISSING"
                message = "Signal file missing"

            else:
                try:
                    source_rows = count_csv_rows(
                        source_path
                    )

                except Exception as exc:
                    reason = "SOURCE_READ_ERROR"
                    message = str(exc)

                if reason is None:
                    if source_rows < minimum_rows:
                        reason = "INSUFFICIENT_HISTORY"

                        message = (
                            f"rows={source_rows}; "
                            f"required={minimum_rows}"
                        )

            if reason is not None:
                excluded += 1

                exclusions.append({
                    "timeframe": timeframe,
                    "market": market,
                    "source_rows": source_rows,
                    "minimum_required_rows": (
                        minimum_rows
                    ),
                    "reason": reason,
                    "message": message,
                })

                continue

            eligible += 1

            jobs.append({
                "timeframe": timeframe,
                "market": market,
                "source": source_path,
                "feature": feature_path,
                "signal": signal_path,
            })

        stats[timeframe] = {
            "source_files": len(source_files),
            "eligible": eligible,
            "excluded": excluded,
        }

        print(
            f"[DISCOVERY] {timeframe}: "
            f"sources={len(source_files)}, "
            f"eligible={eligible}, "
            f"excluded={excluded}",
            flush=True,
        )

        if eligible == 0:
            raise RuntimeError(
                f"No eligible markets: {timeframe}"
            )

    return jobs, exclusions, stats


def job_input_hashes(job):
    return {
        "source": sha256_file(job["source"]),
        "feature": sha256_file(job["feature"]),
        "signal": sha256_file(job["signal"]),
    }


def checkpoint_path(output, job):
    return (
        output
        / "checkpoints"
        / job["timeframe"]
        / f"{job['market']}.json"
    )


def checkpoint_is_valid(
    checkpoint,
    job,
    fractions,
    script_hashes,
    input_hashes,
):
    if not isinstance(checkpoint, dict):
        return False

    if checkpoint.get("version") != VERSION:
        return False

    if checkpoint.get("timeframe") != job["timeframe"]:
        return False

    if checkpoint.get("market") != job["market"]:
        return False

    if checkpoint.get("fractions") != list(fractions):
        return False

    if checkpoint.get("script_hashes") != script_hashes:
        return False

    if checkpoint.get("input_hashes") != input_hashes:
        return False

    checks = checkpoint.get("checks")
    issues = checkpoint.get("issues")

    if not isinstance(checks, list):
        return False

    if not isinstance(issues, list):
        return False

    if len(checks) != len(fractions):
        return False

    if issues:
        return False

    observed_fractions = []

    for check in checks:
        if not isinstance(check, dict):
            return False

        if set(check) != set(CHECK_COLUMNS):
            return False

        if check["timeframe"] != job["timeframe"]:
            return False

        if check["market"] != job["market"]:
            return False

        if check["status"] != "PASS":
            return False

        if check["feature_mismatches"] != 0:
            return False

        if check["signal_mismatches"] != 0:
            return False

        if check["cutoff_row"] < MINIMUM_CANDLES:
            return False

        if check["cutoff_row"] >= check["source_rows"]:
            return False

        observed_fractions.append(
            check["cutoff_fraction"]
        )

    if observed_fractions != list(fractions):
        return False

    return True


def load_checkpoint(
    path,
    job,
    fractions,
    script_hashes,
    input_hashes,
):
    if not path.is_file():
        return None

    try:
        with path.open(
            "r",
            encoding="utf-8",
        ) as handle:
            checkpoint = json.load(handle)

        if checkpoint_is_valid(
            checkpoint,
            job,
            fractions,
            script_hashes,
            input_hashes,
        ):
            return checkpoint

    except Exception:
        return None

    return None


def replay_job(
    job,
    fractions,
    feature_module,
    detector_module,
    input_hashes,
):
    timeframe = job["timeframe"]
    market = job["market"]

    source = feature_module.load_source_ohlcv(
        job["source"],
        expected_market=market,
    )

    stored_features = read_csv(
        job["feature"]
    )

    stored_signals = read_csv(
        job["signal"]
    )

    verify_alignment(
        source,
        stored_features,
        "Stored features",
    )

    verify_alignment(
        source,
        stored_signals,
        "Stored signals",
    )

    feature_columns = list(
        feature_module.get_required_feature_columns()
    )

    signal_columns = list(
        dict.fromkeys(
            detector_module.DETECTOR_COLUMNS
        )
    )

    if not feature_columns or not signal_columns:
        raise RuntimeError(
            "Required comparison columns are empty"
        )

    total_rows = len(source)

    if total_rows < required_source_rows(fractions):
        raise RuntimeError(
            "Loaded source has insufficient history"
        )

    checks = []
    issues = []

    for fraction in fractions:
        cutoff_rows = calculate_cutoff_rows(
            total_rows,
            fraction,
        )

        cutoff_index = cutoff_rows - 1

        prefix = source.iloc[
            :cutoff_rows
        ].copy()

        replay_features = (
            feature_module.build_features(prefix)
        )

        feature_module.validate_feature_dataframe(
            source_df=prefix,
            feature_df=replay_features,
            expected_market=market,
        )

        replay_signals = (
            detector_module.build_detector_dataframe(
                replay_features
            )
        )

        detector_module.validate_detector_dataframe(
            feature_df=replay_features,
            detector_df=replay_signals,
            expected_market=market,
        )

        if len(replay_features) != cutoff_rows:
            raise RuntimeError(
                "Replay feature length mismatch"
            )

        if len(replay_signals) != cutoff_rows:
            raise RuntimeError(
                "Replay signal length mismatch"
            )

        timestamp = (
            source.iloc[cutoff_index][
                "candle_date_time_utc"
            ].isoformat()
        )

        context = {
            "timeframe": timeframe,
            "market": market,
            "cutoff_fraction": fraction,
            "cutoff_timestamp": timestamp,
        }

        feature_mismatches = compare_row(
            stored_features,
            replay_features,
            cutoff_index,
            feature_columns,
            "FEATURE",
            context,
            issues,
        )

        signal_mismatches = compare_row(
            stored_signals,
            replay_signals,
            cutoff_index,
            signal_columns,
            "SIGNAL",
            context,
            issues,
        )

        passed = (
            feature_mismatches == 0
            and signal_mismatches == 0
        )

        checks.append({
            **context,
            "cutoff_row": cutoff_rows,
            "source_rows": total_rows,
            "feature_columns_checked": len(
                feature_columns
            ),
            "signal_columns_checked": len(
                signal_columns
            ),
            "feature_mismatches": (
                feature_mismatches
            ),
            "signal_mismatches": (
                signal_mismatches
            ),
            "status": (
                "PASS" if passed else "FAIL"
            ),
            "message": (
                "Prefix replay matches stored values"
                if passed
                else "Prefix replay mismatch detected"
            ),
        })

    after_hashes = job_input_hashes(job)

    if after_hashes != input_hashes:
        raise RuntimeError(
            "Input SHA256 changed during replay"
        )

    return checks, issues


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Pattern 256 full-market prefix replay"
        )
    )

    parser.add_argument(
        "--root",
        default=".",
    )

    parser.add_argument(
        "--cutoffs",
        default="0.40,0.65,0.85",
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help="Ignore valid checkpoints",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    root = Path(args.root).resolve()

    fractions = parse_fractions(
        args.cutoffs
    )

    output = (
        root / OUTPUT_DIRECTORY
    )

    output.mkdir(
        parents=True,
        exist_ok=True,
    )

    feature_script = (
        root / "build_features.py"
    )

    detector_script = (
        root / "detect_256.py"
    )

    current_script = Path(__file__).resolve()

    script_hashes = {
        "audit": sha256_file(current_script),
        "features": sha256_file(feature_script),
        "detector": sha256_file(detector_script),
    }

    feature_module = load_module(
        feature_script,
        "full_market_replay_features",
    )

    detector_module = load_module(
        detector_script,
        "full_market_replay_detector",
    )

    print("=" * 72)
    print("PATTERN 256 FULL-MARKET PREFIX REPLAY")
    print(f"Version: {VERSION}")
    print(f"Root: {root}")
    print(f"Cutoffs: {fractions}")
    print(
        "Minimum source rows: "
        f"{required_source_rows(fractions)}"
    )
    print(f"Force: {args.force}")
    print("=" * 72)

    jobs, exclusions, discovery_stats = (
        discover_jobs(root, fractions)
    )

    checks = []
    issues = []
    errors = []

    resumed_jobs = 0
    executed_jobs = 0

    total_jobs = len(jobs)

    input_manifest = {}

    for number, job in enumerate(jobs, start=1):
        timeframe = job["timeframe"]
        market = job["market"]

        label = f"{timeframe}/{market}"

        print(
            f"[{number}/{total_jobs}] {label}",
            flush=True,
        )

        checkpoint_file = checkpoint_path(
            output,
            job,
        )

        try:
            input_hashes = job_input_hashes(job)

            input_manifest[label] = input_hashes

            checkpoint = None

            if not args.force:
                checkpoint = load_checkpoint(
                    checkpoint_file,
                    job,
                    fractions,
                    script_hashes,
                    input_hashes,
                )

            if checkpoint is not None:
                checks.extend(
                    checkpoint["checks"]
                )

                resumed_jobs += 1

                print(
                    f"[RESUME PASS] {label}",
                    flush=True,
                )

                continue

            job_checks, job_issues = replay_job(
                job,
                fractions,
                feature_module,
                detector_module,
                input_hashes,
            )

            checks.extend(job_checks)
            issues.extend(job_issues)

            executed_jobs += 1

            job_pass = (
                len(job_checks) == len(fractions)
                and not job_issues
                and all(
                    item["status"] == "PASS"
                    for item in job_checks
                )
            )

            if job_pass:
                checkpoint_payload = {
                    "version": VERSION,
                    "created_utc": utc_now(),
                    "timeframe": timeframe,
                    "market": market,
                    "fractions": list(fractions),
                    "script_hashes": script_hashes,
                    "input_hashes": input_hashes,
                    "checks": job_checks,
                    "issues": [],
                }

                atomic_json(
                    checkpoint_file,
                    checkpoint_payload,
                )

                print(
                    f"[PASS] {label}",
                    flush=True,
                )

            else:
                print(
                    f"[FAIL] {label}",
                    flush=True,
                )

        except Exception as exc:
            message = str(exc)

            errors.append({
                "timeframe": timeframe,
                "market": market,
                "message": message,
            })

            issues.append({
                "timeframe": timeframe,
                "market": market,
                "cutoff_fraction": "",
                "cutoff_timestamp": "",
                "category": "EXECUTION",
                "column": "",
                "expected": "",
                "actual": "",
                "message": message,
            })

            print(
                f"[ERROR] {label}: {message}",
                flush=True,
            )

    # Check all selected inputs again before finalizing.
    # A changed file invalidates the complete audit.

    for job in jobs:
        label = (
            f"{job['timeframe']}/{job['market']}"
        )

        try:
            current_hashes = job_input_hashes(
                job
            )

            if current_hashes != input_manifest[label]:
                raise RuntimeError(
                    "Input SHA256 changed during audit"
                )

        except Exception as exc:
            message = str(exc)

            errors.append({
                "timeframe": job["timeframe"],
                "market": job["market"],
                "message": message,
            })

            issues.append({
                "timeframe": job["timeframe"],
                "market": job["market"],
                "cutoff_fraction": "",
                "cutoff_timestamp": "",
                "category": "INPUT_INTEGRITY",
                "column": "",
                "expected": "",
                "actual": "",
                "message": message,
            })

    for name, path in (
        ("audit", current_script),
        ("features", feature_script),
        ("detector", detector_script),
    ):
        if sha256_file(path) != script_hashes[name]:
            message = (
                f"Script changed during audit: {name}"
            )

            errors.append({
                "timeframe": "",
                "market": "",
                "message": message,
            })

            issues.append({
                "timeframe": "",
                "market": "",
                "cutoff_fraction": "",
                "cutoff_timestamp": "",
                "category": "SCRIPT_INTEGRITY",
                "column": name,
                "expected": script_hashes[name],
                "actual": sha256_file(path),
                "message": message,
            })

    checks_path = (
        output / "full_market_checks.csv"
    )

    issues_path = (
        output / "full_market_issues.csv"
    )

    exclusions_path = (
        output / "full_market_exclusions.csv"
    )

    result_path = (
        output / "full_market_result.json"
    )

    manifest_path = (
        output / "full_market_manifest.json"
    )

    atomic_csv(
        checks_path,
        checks,
        CHECK_COLUMNS,
    )

    atomic_csv(
        issues_path,
        issues,
        ISSUE_COLUMNS,
    )

    atomic_csv(
        exclusions_path,
        exclusions,
        EXCLUSION_COLUMNS,
    )

    expected_checks = (
        total_jobs * len(fractions)
    )

    completed_checks = len(checks)

    passed_checks = sum(
        item["status"] == "PASS"
        for item in checks
    )

    failed_checks = sum(
        item["status"] != "PASS"
        for item in checks
    )

    exclusion_counts = {}

    for item in exclusions:
        reason = item["reason"]

        exclusion_counts[reason] = (
            exclusion_counts.get(reason, 0) + 1
        )

    # Insufficient history is an explicitly documented
    # exclusion, not an execution failure.
    #
    # Missing feature/signal files and unreadable sources
    # are treated as coverage failures.

    invalid_exclusions = [
        item
        for item in exclusions
        if item["reason"] != "INSUFFICIENT_HISTORY"
    ]

    execution_pass = (
        total_jobs > 0
        and completed_checks == expected_checks
        and passed_checks == expected_checks
        and failed_checks == 0
        and len(issues) == 0
        and len(errors) == 0
        and len(invalid_exclusions) == 0
        and resumed_jobs + executed_jobs == total_jobs
    )

    result = {
        "version": VERSION,
        "audit": AUDIT_NAME,
        "created_utc": utc_now(),
        "execution": (
            "PASS" if execution_pass else "FAIL"
        ),
        "scope": (
            "ALL_ELIGIBLE_KRW_MARKETS_H1_H4_D1"
        ),
        "timeframes": list(TIMEFRAMES),
        "cutoff_fractions": list(fractions),
        "minimum_candles_per_cutoff": (
            MINIMUM_CANDLES
        ),
        "minimum_source_rows": (
            required_source_rows(fractions)
        ),
        "eligible_market_timeframe_jobs": (
            total_jobs
        ),
        "resumed_jobs": resumed_jobs,
        "executed_jobs": executed_jobs,
        "expected_checks": expected_checks,
        "completed_checks": completed_checks,
        "passed_checks": passed_checks,
        "failed_checks": failed_checks,
        "issue_count": len(issues),
        "execution_errors": errors,
        "excluded_market_timeframe_count": (
            len(exclusions)
        ),
        "invalid_exclusion_count": (
            len(invalid_exclusions)
        ),
        "exclusion_counts": exclusion_counts,
        "discovery_stats": discovery_stats,
        "feature_prefix_replay_verified": (
            execution_pass
        ),
        "signal_prefix_replay_verified": (
            execution_pass
        ),
        "full_eligible_market_prefix_replay_verified": (
            execution_pass
        ),
        "full_market_leakage_free_proven": False,
        "upstream_signal_formula_leakage_free_proven": (
            False
        ),
        "point_in_time_proven": False,
        "independent_oos_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
        "limitations": [
            (
                "All eligible markets are included, "
                "but only the requested cutoff rows "
                "are replayed."
            ),
            (
                "Markets with insufficient history "
                "are excluded and reported."
            ),
            (
                "This audit reuses existing feature "
                "and signal calculation functions."
            ),
            (
                "Matching outputs do not independently "
                "prove that the original formulas "
                "are free of future leakage."
            ),
            (
                "Historical point-in-time data "
                "availability is not established."
            ),
            (
                "No independent out-of-sample "
                "performance validation is performed."
            ),
        ],
    }

    atomic_json(
        result_path,
        result,
    )

    output_hashes = {
        "full_market_checks.csv": sha256_file(
            checks_path
        ),
        "full_market_issues.csv": sha256_file(
            issues_path
        ),
        "full_market_exclusions.csv": sha256_file(
            exclusions_path
        ),
        "full_market_result.json": sha256_file(
            result_path
        ),
    }

    manifest = {
        "version": VERSION,
        "audit": AUDIT_NAME,
        "created_utc": utc_now(),
        "execution": result["execution"],
        "script_sha256": script_hashes,
        "input_sha256": input_manifest,
        "output_sha256": output_hashes,
        "expected_checks": expected_checks,
        "completed_checks": completed_checks,
        "passed_checks": passed_checks,
        "failed_checks": failed_checks,
        "resumed_jobs": resumed_jobs,
        "executed_jobs": executed_jobs,
    }

    atomic_json(
        manifest_path,
        manifest,
    )

    print()
    print("=" * 72)
    print("FULL-MARKET PREFIX REPLAY SUMMARY")
    print("=" * 72)
    print(f"Version         : {VERSION}")
    print(f"Execution       : {result['execution']}")
    print(f"Eligible jobs   : {total_jobs}")
    print(f"Resumed jobs    : {resumed_jobs}")
    print(f"Executed jobs   : {executed_jobs}")
    print(f"Expected checks : {expected_checks}")
    print(f"Completed checks: {completed_checks}")
    print(f"Passed checks   : {passed_checks}")
    print(f"Failed checks   : {failed_checks}")
    print(f"Issues          : {len(issues)}")
    print(f"Excluded        : {len(exclusions)}")
    print(
        f"Coverage errors : {len(invalid_exclusions)}"
    )
    print("Research        : HOLD")
    print("TRAIN approved  : False")
    print("Trading approved: False")
    print(f"Reports         : {output}")
    print("=" * 72)

    return 0 if execution_pass else 1


if __name__ == "__main__":
    try:
        sys.exit(main())

    except KeyboardInterrupt:
        print(
            "[STOP] Interrupted by user",
            flush=True,
        )

        sys.exit(130)

    except Exception:
        traceback.print_exc()
        sys.exit(1)
