
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Signal-OHLCV Alignment Audit
Clean V001

READ ONLY:
    data/ohlcv/{h1,h4,d1}/KRW-*.csv
    data/signals/256/{h1,h4,d1}/KRW-*.csv

OUTPUT:
    data/reports/pattern_256_signal_ohlcv_alignment/

Checks:
    - Previous signal formula audit integrity
    - Exact market/timeframe coverage
    - Row counts and UTC timestamp alignment
    - OHLCV numeric validity
    - Signal/OHLCV price and volume equality
    - Timestamp ordering and duplicates
    - Time-gap diagnostics
    - Source SHA256 before and after
    - SHA256-based safe checkpoint resume

This does NOT prove:
    - Point-in-time data availability
    - Upstream feature causality
    - Independent OOS
    - +30% prediction performance
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

VERSION = "Clean V001"
TIMEFRAMES = {
    "h1": pd.Timedelta(hours=1),
    "h4": pd.Timedelta(hours=4),
    "d1": pd.Timedelta(days=1),
}

PRICE_COLUMNS = (
    "open", "high", "low", "close"
)
NUMERIC_COLUMNS = PRICE_COLUMNS + (
    "volume", "trade_value"
)
KEY_COLUMNS = (
    "market", "candle_date_time_utc"
)

REPORT_COLUMNS = (
    "timeframe", "market", "rows",
    "checked_values", "mismatches",
    "missing_intervals", "largest_gap_candles",
    "ohlcv_sha256", "signal_sha256",
    "status", "error"
)


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def sha256(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(
            lambda: f.read(1024 * 1024), b""
        ):
            h.update(chunk)
    return h.hexdigest()


def load_json(path):
    with path.open(
        "r", encoding="utf-8-sig"
    ) as f:
        return json.load(f)


def save_json(path, obj):
    path.parent.mkdir(
        parents=True, exist_ok=True
    )
    temp = path.with_name(path.name + ".tmp")
    with temp.open(
        "w", encoding="utf-8"
    ) as f:
        json.dump(
            obj, f, indent=2,
            ensure_ascii=False, sort_keys=True
        )
        f.write("\n")
    os.replace(temp, path)


def save_csv(path, rows):
    path.parent.mkdir(
        parents=True, exist_ok=True
    )
    temp = path.with_name(path.name + ".tmp")
    with temp.open(
        "w", encoding="utf-8-sig",
        newline=""
    ) as f:
        writer = csv.DictWriter(
            f, fieldnames=REPORT_COLUMNS
        )
        writer.writeheader()
        writer.writerows(rows)
    os.replace(temp, path)


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def verify_previous(root):
    folder = (
        root / "data/reports/"
        "pattern_256_signal_formula"
    )
    result_path = (
        folder / "signal_formula_result.json"
    )
    manifest_path = (
        folder / "signal_formula_manifest.json"
    )

    require(
        result_path.is_file(),
        "Previous result missing"
    )
    require(
        manifest_path.is_file(),
        "Previous manifest missing"
    )

    result = load_json(result_path)
    manifest = load_json(manifest_path)

    require(
        result.get("execution") == "PASS",
        "Previous audit not PASS"
    )
    require(
        result.get("jobs") == 870,
        "Previous job count invalid"
    )
    require(
        result.get("completed_jobs") == 870,
        "Previous completed count invalid"
    )
    require(
        result.get("failed_jobs") == 0,
        "Previous audit has failures"
    )
    require(
        result.get("mismatches") == 0,
        "Previous formula mismatches"
    )
    require(
        result.get(
            "formula_output_consistency_verified"
        ) is True,
        "Previous formula verification absent"
    )
    require(
        manifest.get("execution") == "PASS",
        "Previous manifest not PASS"
    )

    expected_names = {
        "signal_formula_result.json",
        "signal_formula_files.csv",
    }
    hashes = manifest.get(
        "output_sha256", {}
    )
    require(
        set(hashes) == expected_names,
        "Previous output list invalid"
    )

    for name, expected in hashes.items():
        path = folder / name
        require(
            path.is_file(),
            f"Previous file missing: {name}"
        )
        require(
            sha256(path) == expected,
            f"Previous hash mismatch: {name}"
        )

    detector_path = root / "detect_256.py"
    require(
        detector_path.is_file(),
        "Detector source missing"
    )
    require(
        sha256(detector_path)
        == result["detector_sha256"],
        "Detector source changed"
    )

    previous_rows = {}
    files_path = (
        folder / "signal_formula_files.csv"
    )
    with files_path.open(
        "r", encoding="utf-8-sig",
        newline=""
    ) as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = (
                row["timeframe"],
                row["market"]
            )
            require(
                key not in previous_rows,
                f"Duplicate previous job: {key}"
            )
            require(
                row["status"] == "PASS"
                and int(row["mismatches"]) == 0,
                f"Previous job failed: {key}"
            )
            previous_rows[key] = row

    require(
        len(previous_rows) == 870,
        "Previous file coverage mismatch"
    )

    return sha256(manifest_path), previous_rows


def discover(root, previous_rows):
    jobs = []
    for timeframe in TIMEFRAMES:
        source_dir = (
            root / "data/ohlcv" / timeframe
        )
        signal_dir = (
            root / "data/signals/256" / timeframe
        )

        require(
            source_dir.is_dir(),
            f"Missing OHLCV directory: {source_dir}"
        )
        require(
            signal_dir.is_dir(),
            f"Missing signal directory: {signal_dir}"
        )

        source_files = {
            p.stem: p
            for p in source_dir.glob("KRW-*.csv")
            if p.is_file()
        }
        signal_files = {
            p.stem: p
            for p in signal_dir.glob("KRW-*.csv")
            if p.is_file()
        }

        require(
            set(source_files) == set(signal_files),
            f"Market coverage differs: {timeframe}"
        )

        for market in sorted(source_files):
            key = (timeframe, market)
            require(
                key in previous_rows,
                f"Market missing in previous audit: {key}"
            )
            jobs.append((
                timeframe,
                market,
                source_files[market],
                signal_files[market],
            ))

    require(
        len(jobs) == 870,
        f"Expected 870 jobs, found {len(jobs)}"
    )
    require(
        set((t, m) for t, m, _, _ in jobs)
        == set(previous_rows),
        "Previous/current market coverage differs"
    )
    return jobs


def read_frame(path, market):
    df = pd.read_csv(
        path, encoding="utf-8-sig",
        low_memory=False
    )

    require(
        not df.empty,
        f"Empty CSV: {path}"
    )
    require(
        len(df.columns) == len(set(df.columns)),
        f"Duplicate columns: {path}"
    )

    needed = set(
        KEY_COLUMNS + NUMERIC_COLUMNS
    )
    missing = needed - set(df.columns)
    require(
        not missing,
        f"Missing columns {sorted(missing)}: {path}"
    )

    markets = (
        df["market"]
        .dropna()
        .astype(str)
        .str.strip()
        .unique()
        .tolist()
    )
    require(
        markets == [market]
        and df["market"].notna().all(),
        f"Invalid market values: {path}"
    )

    times = pd.to_datetime(
        df["candle_date_time_utc"],
        utc=True, errors="coerce"
    )
    require(
        times.notna().all(),
        f"Invalid UTC timestamps: {path}"
    )
    require(
        not times.duplicated().any(),
        f"Duplicate UTC timestamps: {path}"
    )
    require(
        times.is_monotonic_increasing,
        f"Unsorted UTC timestamps: {path}"
    )

    df["_audit_utc"] = times

    for col in NUMERIC_COLUMNS:
        df[col] = pd.to_numeric(
            df[col], errors="coerce"
        )

    return df


def inspect_job(
    timeframe, market, ohlcv_path,
    signal_path, previous
):
    before_source = sha256(ohlcv_path)
    before_signal = sha256(signal_path)

    require(
        before_signal
        == previous["source_sha256"],
        "Signal changed since formula audit"
    )

    source = read_frame(
        ohlcv_path, market
    )
    signal = read_frame(
        signal_path, market
    )

    require(
        len(source) == len(signal),
        "OHLCV/signal row count differs"
    )
    require(
        len(signal) == int(previous["rows"]),
        "Row count differs from previous audit"
    )

    rows = len(source)
    checked = 0
    mismatches = 0
    samples = []

    def record(column, invalid):
        nonlocal checked, mismatches
        checked += rows
        count = int(np.count_nonzero(invalid))
        mismatches += count

        if count and len(samples) < 30:
            indexes = np.flatnonzero(invalid)
            for idx in indexes[:5]:
                if len(samples) >= 30:
                    break
                samples.append({
                    "row": int(idx) + 2,
                    "column": column,
                    "utc": str(
                        source["_audit_utc"].iloc[idx]
                    ),
                })

    source_time = source[
        "_audit_utc"
    ].reset_index(drop=True)
    signal_time = signal[
        "_audit_utc"
    ].reset_index(drop=True)

    record(
        "utc_alignment",
        (
            source_time != signal_time
        ).to_numpy()
    )

    for col in NUMERIC_COLUMNS:
        a = source[col].to_numpy(
            dtype=np.float64
        )
        b = signal[col].to_numpy(
            dtype=np.float64
        )

        finite_a = np.isfinite(a)
        finite_b = np.isfinite(b)

        record(
            f"{col}_alignment",
            ~(
                finite_a & finite_b
                & np.isclose(
                    a, b,
                    rtol=1e-10,
                    atol=1e-12,
                    equal_nan=False
                )
            )
        )

        if col in PRICE_COLUMNS:
            record(
                f"{col}_positive",
                ~(finite_a & (a > 0))
            )
        else:
            record(
                f"{col}_nonnegative",
                ~(finite_a & (a >= 0))
            )

    o = source["open"].to_numpy(
        dtype=np.float64
    )
    h = source["high"].to_numpy(
        dtype=np.float64
    )
    l = source["low"].to_numpy(
        dtype=np.float64
    )
    c = source["close"].to_numpy(
        dtype=np.float64
    )

    record(
        "ohlc_price_structure",
        ~(
            (h >= np.maximum(o, c))
            & (l <= np.minimum(o, c))
            & (h >= l)
        )
    )

    deltas = source_time.diff().dropna()
    expected_delta = TIMEFRAMES[timeframe]

    # Missing intervals are diagnostics, not automatically
    # evidence of corruption. Upbit may omit no-trade candles.
    gaps = deltas[deltas > expected_delta]
    missing_intervals = int(
        sum(
            max(
                int(delta / expected_delta) - 1,
                0
            )
            for delta in gaps
        )
    )
    largest_gap = (
        max(
            int(delta / expected_delta)
            for delta in gaps
        )
        if len(gaps) else 0
    )

    irregular = int(
        (deltas != expected_delta).sum()
    )
    backwards_or_zero = int(
        (deltas <= pd.Timedelta(0)).sum()
    )

    # Nonstandard time differences are reported separately.
    # Only non-positive intervals are fatal here.
    if backwards_or_zero:
        mismatches += backwards_or_zero
        samples.append({
            "column": "nonpositive_time_delta",
            "count": backwards_or_zero
        })

    after_source = sha256(ohlcv_path)
    after_signal = sha256(signal_path)

    require(
        before_source == after_source,
        "OHLCV changed during audit"
    )
    require(
        before_signal == after_signal,
        "Signal changed during audit"
    )

    return {
        "timeframe": timeframe,
        "market": market,
        "rows": rows,
        "checked_values": checked,
        "mismatches": mismatches,
        "missing_intervals": missing_intervals,
        "largest_gap_candles": largest_gap,
        "irregular_time_deltas": irregular,
        "ohlcv_sha256": before_source,
        "signal_sha256": before_signal,
        "status": (
            "PASS" if mismatches == 0
            else "FAIL"
        ),
        "error": "",
        "samples": samples,
    }


def checkpoint_valid(
    data, source_hash, signal_hash,
    previous_hash, script_hash
):
    return (
        isinstance(data, dict)
        and data.get("version") == VERSION
        and data.get("ohlcv_sha256")
        == source_hash
        and data.get("signal_sha256")
        == signal_hash
        and data.get("previous_sha256")
        == previous_hash
        and data.get("script_sha256")
        == script_hash
        and data.get("status") == "PASS"
        and data.get("mismatches") == 0
        and isinstance(data.get("rows"), int)
        and data["rows"] > 0
        and data.get("checked_values")
        == data["rows"] * 14
        and isinstance(
            data.get("samples"), list
        )
        and not data["samples"]
    )


def run(root, force):
    root = root.resolve()
    previous_hash, previous_rows = (
        verify_previous(root)
    )
    script_hash = sha256(
        Path(__file__).resolve()
    )

    jobs = discover(root, previous_rows)

    folder = (
        root / "data/reports/"
        "pattern_256_signal_ohlcv_alignment"
    )
    checkpoint_dir = folder / "checkpoints"
    checkpoint_dir.mkdir(
        parents=True, exist_ok=True
    )

    report = []
    errors = []
    samples = []
    executed = 0
    resumed = 0

    for index, (
        tf, market, source_path, signal_path
    ) in enumerate(jobs, 1):
        try:
            source_hash = sha256(source_path)
            signal_hash = sha256(signal_path)
            cp = (
                checkpoint_dir
                / f"{tf}_{market}.json"
            )

            data = None

            if not force and cp.is_file():
                try:
                    cached = load_json(cp)
                    if checkpoint_valid(
                        cached,
                        source_hash,
                        signal_hash,
                        previous_hash,
                        script_hash
                    ):
                        data = cached
                except (
                    OSError, ValueError,
                    TypeError, KeyError
                ):
                    pass

            if data is None:
                data = inspect_job(
                    tf, market,
                    source_path,
                    signal_path,
                    previous_rows[(tf, market)]
                )
                data.update({
                    "version": VERSION,
                    "previous_sha256":
                        previous_hash,
                    "script_sha256":
                        script_hash,
                })
                executed += 1

                if data["status"] == "PASS":
                    save_json(cp, data)
            else:
                resumed += 1

            report.append({
                k: data[k]
                for k in REPORT_COLUMNS
            })

            if data["status"] != "PASS":
                errors.append({
                    "timeframe": tf,
                    "market": market,
                    "mismatches":
                        data["mismatches"]
                })

            for sample in data.get(
                "samples", []
            ):
                if len(samples) < 100:
                    samples.append({
                        "timeframe": tf,
                        "market": market,
                        **sample
                    })

        except Exception as exc:
            message = (
                f"{type(exc).__name__}: {exc}"
            )
            errors.append({
                "timeframe": tf,
                "market": market,
                "error": message
            })
            report.append({
                "timeframe": tf,
                "market": market,
                "rows": 0,
                "checked_values": 0,
                "mismatches": 0,
                "missing_intervals": 0,
                "largest_gap_candles": 0,
                "ohlcv_sha256": "",
                "signal_sha256": "",
                "status": "FAIL",
                "error": message,
            })
            print(
                f"[FAIL] {tf} {market}: "
                f"{message}",
                flush=True
            )

        if (
            index % 25 == 0
            or index == len(jobs)
        ):
            print(
                f"[PROGRESS] {index}/{len(jobs)} "
                f"executed={executed} "
                f"resumed={resumed} "
                f"errors={len(errors)}",
                flush=True
            )

    total_rows = sum(
        int(x["rows"]) for x in report
    )
    total_checks = sum(
        int(x["checked_values"])
        for x in report
    )
    total_mismatches = sum(
        int(x["mismatches"])
        for x in report
    )
    missing_intervals = sum(
        int(x["missing_intervals"])
        for x in report
    )
    completed = sum(
        x["status"] == "PASS"
        for x in report
    )

    passed = (
        completed == len(jobs)
        and len(errors) == 0
        and total_mismatches == 0
        and total_checks == total_rows * 14
    )

    result = {
        "audit":
            "pattern_256_signal_ohlcv_alignment",
        "version": VERSION,
        "created_utc": utc_now(),
        "execution":
            "PASS" if passed
            else "REVIEW_REQUIRED",
        "jobs": len(jobs),
        "completed_jobs": completed,
        "executed_jobs": executed,
        "resumed_jobs": resumed,
        "failed_jobs": len(jobs) - completed,
        "rows": total_rows,
        "checked_values": total_checks,
        "mismatches": total_mismatches,
        "missing_intervals":
            missing_intervals,
        "errors": errors[:100],
        "mismatch_samples": samples,
        "signal_ohlcv_alignment_verified":
            passed,
        "point_in_time_proven": False,
        "upstream_leakage_free_proven":
            False,
        "independent_oos_proven": False,
        "prediction_performance_verified":
            False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
        "limitations": [
            "Matching data does not prove "
            "historical availability.",
            "Time gaps are diagnostics.",
            "No +30% outcome is calculated.",
            "No independent OOS is proven."
        ],
    }

    report_path = (
        folder / "alignment_files.csv"
    )
    result_path = (
        folder / "alignment_result.json"
    )
    manifest_path = (
        folder / "alignment_manifest.json"
    )

    save_csv(report_path, report)
    save_json(result_path, result)

    save_json(manifest_path, {
        "audit":
            "pattern_256_signal_ohlcv_alignment",
        "version": VERSION,
        "execution": result["execution"],
        "script_sha256": script_hash,
        "previous_manifest_sha256":
            previous_hash,
        "output_sha256": {
            report_path.name:
                sha256(report_path),
            result_path.name:
                sha256(result_path),
        },
    })

    print(json.dumps({
        "execution": result["execution"],
        "jobs": result["jobs"],
        "completed": completed,
        "failed": result["failed_jobs"],
        "rows": total_rows,
        "checks": total_checks,
        "mismatches": total_mismatches,
        "missing_intervals":
            missing_intervals,
        "decision": "HOLD"
    }, indent=2, ensure_ascii=False))

    return 0 if passed else 1


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--root", default="."
    )
    parser.add_argument(
        "--force", action="store_true"
    )
    args = parser.parse_args()

    try:
        return run(
            Path(args.root), args.force
        )
    except KeyboardInterrupt:
        print(
            "[STOP] Interrupted",
            file=sys.stderr
        )
        return 130
    except Exception as exc:
        print(
            f"[FAIL] {type(exc).__name__}: "
            f"{exc}",
            file=sys.stderr
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
