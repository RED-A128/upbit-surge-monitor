
"""256 signal independent recomputation audit. Clean V003-26.

Reads feature and signal CSVs without modifying them. Recomputes all detector
conditions, scores, states and stage using a separate vectorized implementation.
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

VERSION = "Clean V003-26"
TIMEFRAMES = ("h1", "h4", "d1")
CONDITIONS = (
    "cond_ma_compression", "cond_long_ma_compression", "cond_short_ma_turn",
    "cond_mid_ma_turn", "cond_long_ma_stable", "cond_price_above_sma5",
    "cond_price_above_sma20", "cond_momentum_positive", "cond_macd_positive",
    "cond_volume_expansion", "cond_trade_value_expansion", "cond_candle_strength",
    "cond_not_overextended",
)
SCORES = ("setup_score", "ready_score", "trigger_score")
STATES = ("signal_setup", "signal_ready", "signal_trigger")
OUTPUTS = CONDITIONS + SCORES + STATES + ("signal_stage",)
INPUTS = (
    "sma_5_to_20", "sma_20_to_60", "sma_112_to_224",
    "sma_5_slope_1", "sma_5_slope_3", "sma_20_slope_1",
    "sma_20_slope_3", "sma_112_slope_3", "sma_224_slope_3",
    "close_to_sma_5", "close_to_sma_20", "return_1", "return_3",
    "rsi_14", "macd", "macd_signal", "macd_hist",
    "volume_ratio_5", "volume_ratio_20", "trade_value_ratio_5",
    "trade_value_ratio_20", "is_bullish", "candle_return",
    "close_position_in_range",
)
KEY = "candle_date_time_utc"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def atomic_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2, ensure_ascii=False)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def read_csv(path: Path, required: tuple[str, ...]) -> pd.DataFrame:
    if not path.is_file():
        raise FileNotFoundError(str(path))
    frame = pd.read_csv(path, encoding="utf-8-sig", low_memory=False)
    if frame.empty:
        raise ValueError(f"Empty CSV: {path}")
    missing = sorted(set(required + (KEY,)) - set(frame.columns))
    if missing:
        raise ValueError(f"Missing columns in {path}: {missing}")
    timestamp = pd.to_datetime(frame[KEY], utc=True, errors="coerce")
    if timestamp.isna().any() or timestamp.duplicated().any():
        raise ValueError(f"Invalid/duplicate timestamps: {path}")
    frame[KEY] = timestamp
    return frame.sort_values(KEY).reset_index(drop=True)


def independently_recompute(df: pd.DataFrame) -> pd.DataFrame:
    # Deliberately does not import detect_256.py or call its functions.
    v = {name: pd.to_numeric(df[name], errors="coerce").to_numpy(dtype=float)
         for name in INPUTS}
    n = len(df)

    def flag(condition):
        return np.asarray(condition, dtype=np.int8)

    c = {}
    c["cond_ma_compression"] = flag(
        (abs(v["sma_5_to_20"]) <= .015)
        & (abs(v["sma_20_to_60"]) <= .030)
    )
    c["cond_long_ma_compression"] = flag(
        abs(v["sma_112_to_224"]) <= .080
    )
    c["cond_short_ma_turn"] = flag(
        (v["sma_5_slope_1"] > 0)
        & (v["sma_5_slope_3"] >= 0)
    )
    c["cond_mid_ma_turn"] = flag(
        (v["sma_20_slope_1"] >= 0)
        & (v["sma_20_slope_3"] >= 0)
    )
    c["cond_long_ma_stable"] = flag(
        (v["sma_112_slope_3"] >= 0)
        | (v["sma_224_slope_3"] >= 0)
    )
    c["cond_price_above_sma5"] = flag(
        v["close_to_sma_5"] >= 0
    )
    c["cond_price_above_sma20"] = flag(
        v["close_to_sma_20"] >= 0
    )
    c["cond_not_overextended"] = flag(
        v["close_to_sma_20"] <= .080
    )
    c["cond_momentum_positive"] = flag(
        (v["return_1"] > 0)
        & (v["return_3"] > 0)
        & (v["rsi_14"] >= 45)
    )
    c["cond_macd_positive"] = flag(
        (v["macd_hist"] > 0)
        & (v["macd"] >= v["macd_signal"])
    )
    c["cond_volume_expansion"] = flag(
        (v["volume_ratio_5"] >= 1.10)
        | (v["volume_ratio_20"] >= 1.05)
    )
    c["cond_trade_value_expansion"] = flag(
        (v["trade_value_ratio_5"] >= 1.10)
        | (v["trade_value_ratio_20"] >= 1.05)
    )
    c["cond_candle_strength"] = flag(
        (v["is_bullish"] == 1)
        & (v["candle_return"] > 0)
        & (v["close_position_in_range"] >= .60)
    )

    setup_parts = (
        "cond_ma_compression", "cond_long_ma_compression",
        "cond_mid_ma_turn", "cond_long_ma_stable",
        "cond_not_overextended",
    )
    ready_parts = (
        "cond_short_ma_turn", "cond_mid_ma_turn",
        "cond_price_above_sma5", "cond_momentum_positive",
        "cond_macd_positive", "cond_not_overextended",
    )
    trigger_parts = (
        "cond_price_above_sma5", "cond_price_above_sma20",
        "cond_momentum_positive", "cond_macd_positive",
        "cond_volume_expansion", "cond_trade_value_expansion",
        "cond_candle_strength", "cond_not_overextended",
    )

    def score(parts):
        return np.sum(
            np.stack([c[x] for x in parts]),
            axis=0,
            dtype=np.int16,
        )

    s = {
        "setup_score": score(setup_parts),
        "ready_score": score(ready_parts),
        "trigger_score": score(trigger_parts),
    }

    setup = flag(
        (s["setup_score"] >= 4)
        & (c["cond_ma_compression"] == 1)
    )
    ready = flag(
        (s["ready_score"] >= 5)
        & (c["cond_short_ma_turn"] == 1)
        & (c["cond_price_above_sma5"] == 1)
        & (v["rsi_14"] >= 45)
    )
    trigger = flag(
        (s["trigger_score"] >= 5)
        & (ready == 1)
        & (c["cond_price_above_sma20"] == 1)
        & (v["rsi_14"] >= 50)
        & (v["rsi_14"] <= 75)
        & (
            (c["cond_volume_expansion"] == 1)
            | (c["cond_trade_value_expansion"] == 1)
        )
    )

    stage = np.full(n, "NONE", dtype=object)
    stage[setup == 1] = "SETUP"
    stage[ready == 1] = "READY"
    stage[trigger == 1] = "TRIGGER"

    result = pd.DataFrame({KEY: df[KEY]})
    for name in CONDITIONS:
        result[name] = c[name]
    for name in SCORES:
        result[name] = s[name]
    result["signal_setup"] = setup
    result["signal_ready"] = ready
    result["signal_trigger"] = trigger
    result["signal_stage"] = stage
    return result


def compare(
    feature: pd.DataFrame,
    signal: pd.DataFrame,
    max_examples: int,
) -> tuple[int, dict, list]:
    if len(feature) != len(signal) or not feature[KEY].equals(signal[KEY]):
        raise ValueError("Feature/Signal timestamp alignment mismatch")

    recomputed = independently_recompute(feature)
    differences = {}
    examples = []
    total = 0

    for column in OUTPUTS:
        expected = recomputed[column].to_numpy()
        if column == "signal_stage":
            actual = (
                signal[column].fillna("").astype(str).str.strip().to_numpy()
            )
        else:
            numeric = pd.to_numeric(signal[column], errors="coerce")
            actual = numeric.to_numpy(dtype=float)

        mismatch = expected != actual
        count = int(np.count_nonzero(mismatch))
        differences[column] = count
        total += count

        for idx in np.flatnonzero(mismatch)[
            :max(0, max_examples - len(examples))
        ]:
            examples.append({
                "timestamp": str(feature[KEY].iat[idx]),
                "column": column,
                "expected": str(expected[idx]),
                "actual": str(actual[idx]),
            })

    return total, differences, examples


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=".")
    parser.add_argument("--max-examples", type=int, default=20)
    args = parser.parse_args()

    root = Path(args.root).resolve()
    features = root / "data" / "features"
    signals = root / "data" / "signals" / "256"
    out = (
        root / "data" / "reports"
        / "pattern_256_independent_recomputation"
    )
    out.mkdir(parents=True, exist_ok=True)
    checkpoint_path = out / "checkpoint.json"

    previous = {}
    if checkpoint_path.exists():
        try:
            old = json.loads(
                checkpoint_path.read_text(encoding="utf-8")
            )
            if (
                old.get("version") == VERSION
                and isinstance(old.get("jobs"), dict)
            ):
                previous = old["jobs"]
        except (ValueError, OSError):
            pass

    jobs = dict(previous)
    all_jobs = []

    for tf in TIMEFRAMES:
        fdir, sdir = features / tf, signals / tf

        if not fdir.is_dir() or not sdir.is_dir():
            raise ValueError(
                f"Missing directory: {fdir} or {sdir}"
            )

        feature_markets = {
            p.name for p in fdir.glob("KRW-*.csv")
        }
        signal_markets = {
            p.name for p in sdir.glob("KRW-*.csv")
        }

        if not feature_markets or feature_markets != signal_markets:
            raise ValueError(
                f"Market set mismatch in {tf}: "
                f"features={len(feature_markets)}, "
                f"signals={len(signal_markets)}"
            )

        all_jobs.extend(
            (tf, name) for name in sorted(feature_markets)
        )

    failed = 0
    total_rows = 0

    for index, (tf, name) in enumerate(all_jobs, 1):
        key = f"{tf}/{name}"
        fpath = features / tf / name
        spath = signals / tf / name

        fh, sh = sha256(fpath), sha256(spath)
        saved = jobs.get(key)

        if (
            isinstance(saved, dict)
            and saved.get("status") == "PASS"
            and saved.get("feature_sha256") == fh
            and saved.get("signal_sha256") == sh
            and saved.get("version") == VERSION
            and saved.get("rows", 0) > 0
        ):
            record = saved
            print(
                f"[{index}/{len(all_jobs)}] SKIP verified {key}",
                flush=True,
            )
        else:
            try:
                feature = read_csv(fpath, INPUTS)
                signal = read_csv(spath, OUTPUTS)

                count, by_column, examples = compare(
                    feature, signal, args.max_examples
                )

                if sha256(fpath) != fh or sha256(spath) != sh:
                    raise RuntimeError(
                        "Input file changed during audit"
                    )

                record = {
                    "version": VERSION,
                    "status": "PASS" if count == 0 else "FAIL",
                    "rows": len(feature),
                    "mismatches": count,
                    "by_column": by_column,
                    "examples": examples,
                    "feature_sha256": fh,
                    "signal_sha256": sh,
                }
            except Exception as exc:
                record = {
                    "version": VERSION,
                    "status": "ERROR",
                    "rows": 0,
                    "error": f"{type(exc).__name__}: {exc}",
                    "feature_sha256": fh,
                    "signal_sha256": sh,
                }

            jobs[key] = record
            atomic_json(
                checkpoint_path,
                {"version": VERSION, "jobs": jobs},
            )
            print(
                f"[{index}/{len(all_jobs)}] "
                f"{record['status']} {key} "
                f"mismatches={record.get('mismatches', 'N/A')}",
                flush=True,
            )

        total_rows += record.get("rows", 0)
        if record["status"] != "PASS":
            failed += 1

    relevant = {
        f"{tf}/{name}": jobs[f"{tf}/{name}"]
        for tf, name in all_jobs
    }

    report = {
        "version": VERSION,
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "status": "PASS" if failed == 0 else "FAIL",
        "jobs": len(all_jobs),
        "failed_jobs": failed,
        "audited_rows": total_rows,
        "independent_oos_proven": False,
        "approved_for_live_trading": False,
        "results": relevant,
    }
    atomic_json(out / "audit_result.json", report)

    with (out / "audit_summary.csv").open(
        "w", newline="", encoding="utf-8"
    ) as handle:
        writer = csv.writer(handle)
        writer.writerow([
            "job", "status", "rows", "mismatches",
            "feature_sha256", "signal_sha256", "error",
        ])

        for key, rec in relevant.items():
            writer.writerow([
                key,
                rec["status"],
                rec.get("rows", 0),
                rec.get("mismatches", ""),
                rec.get("feature_sha256", ""),
                rec.get("signal_sha256", ""),
                rec.get("error", ""),
            ])

    print(
        f"FINAL: {report['status']} "
        f"jobs={len(all_jobs)} "
        f"failed={failed} rows={total_rows}"
    )
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(
            f"FATAL: {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        sys.exit(1)
