#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Pattern 256 / accumulation 30% performance comparison - Clean V001.

Reads existing validated summary/result only; never writes input sources.
No OHLCV or detail CSV needed. This is descriptive comparison, not proof
of out-of-sample predictive performance.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

VERSION = "Clean V001"
GROUPS = ("PATTERN_256_ONLY", "ACCUMULATION_ONLY", "BOTH", "NEITHER")
TIMEFRAMES = ("h1", "h4", "d1")
HORIZONS = (24, 72, 168)
REQUIRED = ("timeframe", "classification", "horizon", "total_signals", "complete", "hit", "miss", "censored", "invalid", "hit_rate_pct", "average_max_return_pct", "median_max_return_pct", "average_first_hit_bars")
COUNT_FIELDS = ("total_signals", "complete", "hit", "miss", "censored", "invalid")
REPORT_REL = Path("data/reports/pattern_256_accumulation_30pct")
OUTPUT_REL = Path("data/reports/pattern_256_accumulation_performance")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def optional_float(value: str, name: str) -> float | None:
    if value is None or not str(value).strip():
        return None
    result = float(value)
    require(math.isfinite(result), f"Non-finite {name}")
    return result


def wilson_interval(hit: int, complete: int) -> tuple[float | None, float | None]:
    if complete == 0:
        return None, None
    z = 1.959963984540054
    p = hit / complete
    d = 1 + z * z / complete
    center = (p + z * z / (2 * complete)) / d
    half = z * math.sqrt(p * (1 - p) / complete + z * z / (4 * complete * complete)) / d
    return 100 * max(0, center - half), 100 * min(1, center + half)


def parse_summary(path: Path) -> dict[tuple[str, str, int], dict]:
    parsed = {}
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        require(reader.fieldnames is not None, "Summary header missing")
        require(set(REQUIRED).issubset(reader.fieldnames), "Summary columns missing: " + str(set(REQUIRED) - set(reader.fieldnames)))
        for index, raw in enumerate(reader, start=2):
            try:
                tf = raw["timeframe"].strip()
                group = raw["classification"].strip()
                horizon = int(raw["horizon"])
                require(tf in TIMEFRAMES and group in GROUPS and horizon in HORIZONS, f"Unknown category on line {index}")
                key = (tf, group, horizon)
                require(key not in parsed, f"Duplicate summary row {key}")
                row = {field: int(raw[field]) for field in COUNT_FIELDS}
                require(all(value >= 0 for value in row.values()), f"Negative count on line {index}")
                require(row["complete"] == row["hit"] + row["miss"], f"Complete count mismatch: {key}")
                require(row["total_signals"] == row["complete"] + row["censored"] + row["invalid"], f"Total count mismatch: {key}")
                for field in REQUIRED[9:]:
                    row[field] = optional_float(raw[field], field)
                if row["complete"]:
                    expected = 100 * row["hit"] / row["complete"]
                    require(row["hit_rate_pct"] is not None and abs(row["hit_rate_pct"] - expected) < 0.00001, f"Hit rate mismatch: {key}")
                else:
                    require(row["hit_rate_pct"] is None, f"Zero complete with hit rate: {key}")
                if row["total_signals"] == 0:
                    require(all(row[field] is None for field in REQUIRED[9:]), f"Zero-signal row contains metrics: {key}")
                row.update(timeframe=tf, classification=group, horizon=horizon)
                parsed[key] = row
            except (TypeError, ValueError) as exc:
                raise ValueError(f"Summary line {index}: {exc}") from exc
    expected_keys = {(tf, group, horizon) for tf in TIMEFRAMES for group in GROUPS for horizon in HORIZONS}
    require(set(parsed) == expected_keys, f"Expected 36 exact rows; missing={expected_keys - set(parsed)}, extra={set(parsed) - expected_keys}")
    for tf in TIMEFRAMES:
        for group in GROUPS:
            signals = {parsed[(tf, group, h)]["total_signals"] for h in HORIZONS}
            require(len(signals) == 1, f"Signal counts differ across horizons: {tf}/{group}")
    return parsed


def verify_result(path: Path, summary_path: Path, rows: dict) -> dict:
    result = json.loads(path.read_text(encoding="utf-8-sig"))
    require(isinstance(result, dict), "Result JSON must be an object")
    expected = {"status": "PASS", "version": VERSION, "entry_rule": "NEXT_CANDLE_OPEN", "hit_rule": "FUTURE_HIGH_GTE_ENTRY_X_1_30", "future_data_policy": "INCOMPLETE_WINDOWS_CENSORED"}
    for key, value in expected.items():
        require(result.get(key) == value, f"Result field mismatch: {key}")
    require(float(result.get("target_return_pct", -1)) == 30.0, "Target is not 30%")
    require(result.get("limited_test") is False, "Result must be full production, not limited test")
    require(result.get("timeframes") == list(TIMEFRAMES), "Unexpected timeframes")
    require(result.get("horizons") == list(HORIZONS), "Unexpected horizons")
    require(result.get("groups") == list(GROUPS), "Unexpected groups")
    require(result.get("summary_rows") == len(rows), "Summary row count mismatch")
    require(result.get("summary_sha256", "").lower() == sha256_file(summary_path), "Summary SHA256 differs from validated result")
    counts = result.get("status_counts", {})
    for source_key, column in (("hit", "hit"), ("miss", "miss"), ("censored", "censored"), ("invalid", "invalid")):
        actual = sum(row[column] for row in rows.values())
        require(int(counts.get(source_key, -1)) == actual, f"Status count mismatch: {source_key}")
    require(int(result.get("detail_rows", -1)) == sum(row["total_signals"] for row in rows.values()), "Detail row count mismatch")
    return result


def fmt(value: float | None, decimals: int = 6) -> str:
    return "" if value is None else f"{value:.{decimals}f}"


def atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=".pending_", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp_name, path)
    finally:
        if os.path.exists(temp_name):
            os.unlink(temp_name)


def write_csv(path: Path, fields: list[str], records: list[dict]) -> None:
    from io import StringIO
    stream = StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore", lineterminator="\n")
    writer.writeheader()
    writer.writerows(records)
    atomic_write(path, stream.getvalue())


def compare(rows: dict) -> tuple[list[dict], list[dict], list[str]]:
    metrics = []
    comparisons = []
    warnings = []
    for tf in TIMEFRAMES:
        for horizon in HORIZONS:
            for group in GROUPS:
                row = rows[(tf, group, horizon)]
                low, high = wilson_interval(row["hit"], row["complete"])
                metrics.append({
                    "timeframe": tf, "horizon": horizon, "classification": group,
                    "status": "OK" if row["complete"] else "NO_DATA",
                    **{field: row[field] for field in COUNT_FIELDS},
                    "hit_rate_pct": fmt(row["hit_rate_pct"]),
                    "wilson_95_low_pct": fmt(low), "wilson_95_high_pct": fmt(high),
                    "average_max_return_pct": fmt(row["average_max_return_pct"]),
                    "median_max_return_pct": fmt(row["median_max_return_pct"]),
                    "average_first_hit_bars": fmt(row["average_first_hit_bars"]),
                })
            base = rows[(tf, "PATTERN_256_ONLY", horizon)]
            both = rows[(tf, "BOTH", horizon)]
            if base["complete"] and both["complete"]:
                p_base = base["hit"] / base["complete"]
                p_both = both["hit"] / both["complete"]
                diff = 100 * (p_both - p_base)
                # Descriptive unadjusted standard error; repeated signals may be correlated.
                se = math.sqrt(p_base * (1-p_base)/base["complete"] + p_both*(1-p_both)/both["complete"])
                low = diff - 100 * 1.959963984540054 * se
                high = diff + 100 * 1.959963984540054 * se
                relative = 100 * (p_both / p_base - 1) if p_base > 0 else None
                status = "DESCRIPTIVE_ONLY" if both["complete"] >= 100 else "SMALL_SAMPLE"
            else:
                diff = low = high = relative = None
                status = "NO_DATA"
            comparisons.append({
                "timeframe": tf, "horizon": horizon, "status": status,
                "pattern_only_complete": base["complete"], "pattern_only_hit": base["hit"],
                "both_complete": both["complete"], "both_hit": both["hit"],
                "pattern_only_hit_rate_pct": fmt(base["hit_rate_pct"]),
                "both_hit_rate_pct": fmt(both["hit_rate_pct"]),
                "difference_percentage_points": fmt(diff),
                "relative_lift_pct": fmt(relative),
                "naive_difference_95_low_pp": fmt(low),
                "naive_difference_95_high_pp": fmt(high),
            })
            if status == "SMALL_SAMPLE":
                warnings.append(f"Small sample: {tf}/{horizon} BOTH complete={both['complete']}")
        for group in ("ACCUMULATION_ONLY", "NEITHER"):
            if all(rows[(tf, group, h)]["total_signals"] == 0 for h in HORIZONS):
                warnings.append(f"No observations: {tf}/{group}; cannot compare this group")
    warnings.append("Confidence intervals are descriptive, unadjusted and assume independent observations; repeated signals and market/time correlation violate that assumption.")
    warnings.append("This is in-sample historical hit-rate comparison, not an out-of-sample or live predictive guarantee.")
    warnings.append("A 30% future high is not guaranteed executable profit; fees, slippage and exit fills are not included.")
    warnings.append("Horizons are candle counts, not equal calendar periods across timeframes.")
    return metrics, comparisons, warnings


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parent, help="Project root")
    parser.add_argument("--summary", type=Path, default=None, help="Validated summary CSV")
    parser.add_argument("--result", type=Path, default=None, help="Validated result JSON")
    parser.add_argument("--output-dir", type=Path, default=None, help="Separate output directory")
    args = parser.parse_args()
    root = args.root.resolve()
    summary = (args.summary or root / REPORT_REL / "validation_summary.csv").resolve()
    result_path = (args.result or root / REPORT_REL / "validation_result.json").resolve()
    output = (args.output_dir or root / OUTPUT_REL).resolve()
    require(summary.is_file() and result_path.is_file(), "Validated summary/result input missing")
    require(output != summary.parent and output != result_path.parent, "Output must be separate from input folder")
    before = {str(p): sha256_file(p) for p in (summary, result_path)}
    rows = parse_summary(summary)
    result = verify_result(result_path, summary, rows)
    metrics, comparisons, warnings = compare(rows)
    metric_fields = ["timeframe", "horizon", "classification", "status", *COUNT_FIELDS, "hit_rate_pct", "wilson_95_low_pct", "wilson_95_high_pct", "average_max_return_pct", "median_max_return_pct", "average_first_hit_bars"]
    comparison_fields = ["timeframe", "horizon", "status", "pattern_only_complete", "pattern_only_hit", "both_complete", "both_hit", "pattern_only_hit_rate_pct", "both_hit_rate_pct", "difference_percentage_points", "relative_lift_pct", "naive_difference_95_low_pp", "naive_difference_95_high_pp"]
    write_csv(output / "performance_groups.csv", metric_fields, metrics)
    write_csv(output / "performance_comparison.csv", comparison_fields, comparisons)
    report = {
        "program": Path(__file__).name, "version": VERSION, "status": "PASS",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "validation_created_at_utc": result.get("created_at_utc"),
        "source_result_status": result["status"], "target_return_pct": 30.0,
        "entry_rule": result["entry_rule"], "horizons": list(HORIZONS),
        "timeframes": list(TIMEFRAMES), "groups": list(GROUPS),
        "source_summary_sha256": before[str(summary)],
        "source_result_sha256": before[str(result_path)],
        "group_rows": len(metrics), "comparison_rows": len(comparisons),
        "warnings": warnings,
    }
    atomic_write(output / "performance_result.json", json.dumps(report, indent=2, ensure_ascii=False) + "\n")
    after = {str(p): sha256_file(p) for p in (summary, result_path)}
    require(before == after, "Input source changed during analysis")
    manifest = {"version": VERSION, "source_sha256": before, "output_sha256": {p.name: sha256_file(p) for p in (output / "performance_groups.csv", output / "performance_comparison.csv", output / "performance_result.json")}}
    atomic_write(output / "performance_manifest.json", json.dumps(manifest, indent=2, ensure_ascii=False) + "\n")
    print("[PASS] Performance comparison completed")
    print(f"[PASS] Group rows: {len(metrics)}; comparison rows: {len(comparisons)}")
    print(f"[PASS] Source SHA256 unchanged; output: {output}")
    for row in comparisons:
        print(f"[{row['timeframe']} / {row['horizon']}] ONLY={row['pattern_only_hit_rate_pct']}% BOTH={row['both_hit_rate_pct']}% difference={row['difference_percentage_points']} pp [{row['status']}]")
    for warning in warnings:
        print("[WARN]", warning)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"[FAIL] {type(exc).__name__}: {exc}")
        raise
