
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
audit_pattern_256_signal_formula.py
Clean V001

Verify saved 256 detector conditions, scores and signal states.
Read-only input. SHA256 checkpoints. Research HOLD.

This audits the current Clean V002 detector formula.
It does not prove causal correctness, OOS or profitability.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

VERSION = "Clean V001"
TIMEFRAMES = ("h1", "h4", "d1")

CONDITIONS = (
    "cond_ma_compression",
    "cond_long_ma_compression",
    "cond_short_ma_turn",
    "cond_mid_ma_turn",
    "cond_long_ma_stable",
    "cond_price_above_sma5",
    "cond_price_above_sma20",
    "cond_momentum_positive",
    "cond_macd_positive",
    "cond_volume_expansion",
    "cond_trade_value_expansion",
    "cond_candle_strength",
    "cond_not_overextended",
)

SETUP = (
    "cond_ma_compression",
    "cond_long_ma_compression",
    "cond_mid_ma_turn",
    "cond_long_ma_stable",
    "cond_not_overextended",
)
READY = (
    "cond_short_ma_turn",
    "cond_mid_ma_turn",
    "cond_price_above_sma5",
    "cond_momentum_positive",
    "cond_macd_positive",
    "cond_not_overextended",
)
TRIGGER = (
    "cond_price_above_sma5",
    "cond_price_above_sma20",
    "cond_momentum_positive",
    "cond_macd_positive",
    "cond_volume_expansion",
    "cond_trade_value_expansion",
    "cond_candle_strength",
    "cond_not_overextended",
)

INPUTS = (
    "sma_5_to_20", "sma_20_to_60", "sma_112_to_224",
    "sma_5_slope_1", "sma_5_slope_3",
    "sma_20_slope_1", "sma_20_slope_3",
    "sma_112_slope_3", "sma_224_slope_3",
    "close_to_sma_5", "close_to_sma_20",
    "return_1", "return_3", "rsi_14",
    "macd", "macd_signal", "macd_hist",
    "volume_ratio_5", "volume_ratio_20",
    "trade_value_ratio_5", "trade_value_ratio_20",
    "is_bullish", "candle_return",
    "close_position_in_range",
)

OUTPUTS = CONDITIONS + (
    "setup_score", "ready_score", "trigger_score",
    "signal_setup", "signal_ready", "signal_trigger",
    "signal_stage",
)

REPORT_COLUMNS = (
    "timeframe", "market", "rows", "checked_values",
    "mismatches", "source_sha256", "status", "error",
)

MAX_SAMPLES = 30


def digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def read_json(path: Path):
    with path.open("r", encoding="utf-8-sig") as f:
        return json.load(f)


def write_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + ".tmp")
    with temp.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False,
                  sort_keys=True)
        f.write("\n")
    os.replace(temp, path)


def write_csv(path: Path, rows):
    temp = path.with_name(path.name + ".tmp")
    with temp.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=REPORT_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    os.replace(temp, path)


def check(condition, message):
    if not condition:
        raise RuntimeError(message)


def number(value):
    try:
        return float(value)
    except (ValueError, TypeError):
        return float("nan")


def flag(value):
    v = str(value).strip().lower()
    if v in ("1", "1.0", "true"):
        return 1
    if v in ("0", "0.0", "false"):
        return 0
    raise ValueError(f"Not binary: {value!r}")


def expected_values(row):
    x = {name: number(row[name]) for name in INPUTS}

    def g(name):
        return x[name]

    c = {}

    c["cond_ma_compression"] = int(
        abs(g("sma_5_to_20")) <= 0.015
        and abs(g("sma_20_to_60")) <= 0.030
    )
    c["cond_long_ma_compression"] = int(
        abs(g("sma_112_to_224")) <= 0.080
    )
    c["cond_short_ma_turn"] = int(
        g("sma_5_slope_1") > 0
        and g("sma_5_slope_3") >= 0
    )
    c["cond_mid_ma_turn"] = int(
        g("sma_20_slope_1") >= 0
        and g("sma_20_slope_3") >= 0
    )
    c["cond_long_ma_stable"] = int(
        g("sma_112_slope_3") >= 0
        or g("sma_224_slope_3") >= 0
    )
    c["cond_price_above_sma5"] = int(
        g("close_to_sma_5") >= 0
    )
    c["cond_price_above_sma20"] = int(
        g("close_to_sma_20") >= 0
    )
    c["cond_not_overextended"] = int(
        g("close_to_sma_20") <= 0.080
    )
    c["cond_momentum_positive"] = int(
        g("return_1") > 0
        and g("return_3") > 0
        and g("rsi_14") >= 45
    )
    c["cond_macd_positive"] = int(
        g("macd_hist") > 0
        and g("macd") >= g("macd_signal")
    )
    c["cond_volume_expansion"] = int(
        g("volume_ratio_5") >= 1.10
        or g("volume_ratio_20") >= 1.05
    )
    c["cond_trade_value_expansion"] = int(
        g("trade_value_ratio_5") >= 1.10
        or g("trade_value_ratio_20") >= 1.05
    )
    c["cond_candle_strength"] = int(
        g("is_bullish") == 1
        and g("candle_return") > 0
        and g("close_position_in_range") >= 0.60
    )

    setup_score = sum(c[k] for k in SETUP)
    ready_score = sum(c[k] for k in READY)
    trigger_score = sum(c[k] for k in TRIGGER)

    setup = int(
        setup_score >= 4
        and c["cond_ma_compression"] == 1
    )
    ready = int(
        ready_score >= 5
        and c["cond_short_ma_turn"] == 1
        and c["cond_price_above_sma5"] == 1
        and g("rsi_14") >= 45
    )
    trigger = int(
        trigger_score >= 5
        and ready == 1
        and c["cond_price_above_sma20"] == 1
        and 50 <= g("rsi_14") <= 75
        and (
            c["cond_volume_expansion"] == 1
            or c["cond_trade_value_expansion"] == 1
        )
    )

    stage = (
        "TRIGGER" if trigger
        else "READY" if ready
        else "SETUP" if setup
        else "NONE"
    )

    return {
        **c,
        "setup_score": setup_score,
        "ready_score": ready_score,
        "trigger_score": trigger_score,
        "signal_setup": setup,
        "signal_ready": ready,
        "signal_trigger": trigger,
        "signal_stage": stage,
    }


def verify_previous(root):
    folder = root / "data/reports/pattern_256_trigger_runs"
    result_path = folder / "trigger_run_result.json"
    manifest_path = folder / "trigger_run_manifest.json"

    check(result_path.is_file(), "Missing trigger run result")
    check(manifest_path.is_file(), "Missing trigger run manifest")

    result = read_json(result_path)
    manifest = read_json(manifest_path)

    check(result.get("execution") == "PASS",
          "Previous execution not PASS")
    check(result.get("failed_jobs") == 0,
          "Previous audit has failures")
    check(result.get("jobs") == 870,
          "Previous job count mismatch")
    check(result.get("research_decision") == "HOLD",
          "Previous decision not HOLD")
    check(manifest.get("execution") == "PASS",
          "Previous manifest not PASS")

    for name, expected in manifest.get(
        "output_sha256", {}
    ).items():
        path = folder / name
        check(path.is_file(), f"Missing previous output: {name}")
        check(digest(path) == expected.lower(),
              f"Previous output hash mismatch: {name}")

    check(
        set(manifest.get("output_sha256", {}))
        == {"trigger_run_result.json", "trigger_run_periods.csv"},
        "Previous manifest output list incomplete",
    )

    detector = root / "detect_256.py"
    check(detector.is_file(), "Missing detect_256.py")
    detector_hash = digest(detector)
    check(
        detector_hash == result["detector_source"]["sha256"],
        "Detector source changed since previous audit",
    )
    check(
        detector_hash == manifest["detector_sha256"],
        "Detector manifest SHA256 mismatch",
    )

    # The formula below is specifically for the reviewed
    # Clean V002 detector source, not arbitrary future versions.
    check(
        detector_hash
        == "5e57913dfa4ea40d0ea9706c6d8f00b3634dc47dea6af0b71e28ca036b27d238",
        "Unreviewed detector version; stop for formula review",
    )

    return digest(manifest_path), detector_hash


def inspect_file(path, timeframe, market):
    rows = 0
    checked = 0
    mismatches = 0
    samples = []

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []

        check(len(headers) == len(set(headers)),
              "Duplicate CSV columns")
        missing = sorted(set(INPUTS + OUTPUTS) - set(headers))
        check(not missing, f"Missing columns: {missing}")

        for line, row in enumerate(reader, 2):
            rows += 1
            expected = expected_values(row)

            for column in OUTPUTS:
                checked += 1
                try:
                    if column == "signal_stage":
                        actual = str(row[column]).strip()
                    elif column.startswith(("cond_", "signal_")):
                        actual = flag(row[column])
                    else:
                        raw = number(row[column])
                        if not math.isfinite(raw):
                            raise ValueError("Invalid score")
                        actual = raw

                    valid = actual == expected[column]
                except (ValueError, TypeError) as exc:
                    actual = f"INVALID: {exc}"
                    valid = False

                if not valid:
                    mismatches += 1
                    if len(samples) < MAX_SAMPLES:
                        samples.append({
                            "row": line,
                            "column": column,
                            "actual": actual,
                            "expected": expected[column],
                        })

    check(rows > 0, f"Empty signal CSV: {path}")
    return {
        "timeframe": timeframe,
        "market": market,
        "rows": rows,
        "checked_values": checked,
        "mismatches": mismatches,
        "source_sha256": digest(path),
        "status": "PASS" if mismatches == 0 else "FAIL",
        "error": "",
        "samples": samples,
    }


def checkpoint_valid(data, source_hash, previous_hash,
                     script_hash, detector_hash):
    return (
        isinstance(data, dict)
        and data.get("version") == VERSION
        and data.get("source_sha256") == source_hash
        and data.get("previous_sha256") == previous_hash
        and data.get("script_sha256") == script_hash
        and data.get("detector_sha256") == detector_hash
        and data.get("status") == "PASS"
        and data.get("mismatches") == 0
        and isinstance(data.get("rows"), int)
        and data["rows"] > 0
        and data.get("checked_values")
        == data["rows"] * len(OUTPUTS)
        and isinstance(data.get("samples"), list)
        and not data["samples"]
    )


def run(root, force):
    root = root.resolve()
    previous_hash, detector_hash = verify_previous(root)
    script_hash = digest(Path(__file__).resolve())

    output = root / "data/reports/pattern_256_signal_formula"
    checkpoint_dir = output / "checkpoints"
    checkpoint_dir.mkdir(parents=True, exist_ok=True)

    jobs = []
    for tf in TIMEFRAMES:
        folder = root / "data/signals/256" / tf
        check(folder.is_dir(), f"Missing directory: {folder}")
        jobs.extend(
            (tf, p.stem, p)
            for p in sorted(folder.glob("KRW-*.csv"))
        )

    check(len(jobs) == 870, "Expected 870 signal files")

    report_rows = []
    samples = []
    executed = 0
    resumed = 0
    errors = []

    for index, (tf, market, path) in enumerate(jobs, 1):
        try:
            source_hash = digest(path)
            cp = checkpoint_dir / f"{tf}_{market}.json"
            cached = None

            if not force and cp.is_file():
                try:
                    candidate = read_json(cp)
                    if checkpoint_valid(
                        candidate, source_hash, previous_hash,
                        script_hash, detector_hash
                    ):
                        cached = candidate
                except (OSError, ValueError, TypeError):
                    pass

            if cached is None:
                data = inspect_file(path, tf, market)
                data.update({
                    "version": VERSION,
                    "previous_sha256": previous_hash,
                    "script_sha256": script_hash,
                    "detector_sha256": detector_hash,
                })
                if data["status"] == "PASS":
                    write_json(cp, data)
                executed += 1
            else:
                data = cached
                resumed += 1

            report_rows.append({
                k: data[k] for k in REPORT_COLUMNS
            })

            if data["status"] != "PASS":
                errors.append({
                    "timeframe": tf,
                    "market": market,
                    "error": "Formula mismatch",
                    "mismatches": data["mismatches"],
                })

            for sample in data.get("samples", []):
                if len(samples) < 100:
                    samples.append({
                        "timeframe": tf,
                        "market": market,
                        **sample,
                    })

        except Exception as exc:
            message = f"{type(exc).__name__}: {exc}"
            errors.append({
                "timeframe": tf,
                "market": market,
                "error": message,
            })
            report_rows.append({
                "timeframe": tf,
                "market": market,
                "rows": 0,
                "checked_values": 0,
                "mismatches": 0,
                "source_sha256": "",
                "status": "FAIL",
                "error": message,
            })
            print(f"[FAIL] {tf} {market}: {message}", flush=True)

        if index % 25 == 0 or index == len(jobs):
            print(
                f"[PROGRESS] {index}/{len(jobs)} "
                f"executed={executed} resumed={resumed} "
                f"errors={len(errors)}",
                flush=True,
            )

    total_rows = sum(int(r["rows"]) for r in report_rows)
    checked = sum(int(r["checked_values"]) for r in report_rows)
    mismatches = sum(int(r["mismatches"]) for r in report_rows)
    completed = sum(r["status"] == "PASS" for r in report_rows)

    passed = (
        completed == len(jobs)
        and not errors
        and mismatches == 0
        and checked == total_rows * len(OUTPUTS)
    )

    result = {
        "audit": "pattern_256_signal_formula",
        "version": VERSION,
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "execution": "PASS" if passed else "REVIEW_REQUIRED",
        "jobs": len(jobs),
        "completed_jobs": completed,
        "executed_jobs": executed,
        "resumed_jobs": resumed,
        "failed_jobs": len(jobs) - completed,
        "signal_rows": total_rows,
        "checked_values": checked,
        "mismatches": mismatches,
        "errors": errors[:100],
        "mismatch_samples": samples,
        "formula_output_consistency_verified": passed,
        "detector_sha256": detector_hash,
        "detector_formula_reviewed": True,
        "upstream_feature_leakage_free_proven": False,
        "independent_trade_events_verified": False,
        "independent_oos_proven": False,
        "prediction_performance_verified": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
        "limitations": [
            "Formula consistency does not establish predictive value.",
            "Upstream feature causality is not proven by this audit.",
            "No independent OOS or +30% outcome is evaluated.",
            "This audit is pinned to detector Clean V002 SHA256.",
        ],
    }

    csv_path = output / "signal_formula_files.csv"
    result_path = output / "signal_formula_result.json"
    manifest_path = output / "signal_formula_manifest.json"

    write_csv(csv_path, report_rows)
    write_json(result_path, result)
    write_json(manifest_path, {
        "audit": "pattern_256_signal_formula",
        "version": VERSION,
        "execution": result["execution"],
        "script_sha256": script_hash,
        "detector_sha256": detector_hash,
        "previous_manifest_sha256": previous_hash,
        "output_sha256": {
            csv_path.name: digest(csv_path),
            result_path.name: digest(result_path),
        },
    })

    print(json.dumps({
        "execution": result["execution"],
        "jobs": len(jobs),
        "completed": completed,
        "failed": result["failed_jobs"],
        "rows": total_rows,
        "checked_values": checked,
        "mismatches": mismatches,
        "research_decision": "HOLD",
    }, indent=2, ensure_ascii=False))

    return 0 if passed else 1


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=".")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    try:
        return run(Path(args.root), args.force)
    except KeyboardInterrupt:
        print("[STOP] Interrupted", file=sys.stderr)
        return 130
    except Exception as exc:
        print(
            f"[FAIL] {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
