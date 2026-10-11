
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
analyze_pattern_256_accumulation_entry_risk.py
Clean V002

Historical entry-risk research for Pattern 256 and
Pattern 256 + accumulation signals.

Read-only input:
 data/reports/pattern_256_accumulation_30pct/
   validation_detail.csv
   validation_result.json

Optional read-only context:
 data/reports/pattern_256_accumulation_fair/
   fair_comparison.csv
   fair_result.json

Outputs:
 data/reports/pattern_256_accumulation_entry_risk/
   entry_risk_summary.csv
   entry_risk_stop_scenarios.csv
   entry_risk_result.json
   entry_risk_manifest.json

IMPORTANT:
 - No trading.
 - No changes to upstream data.
 - Large CSV processed as a stream.
 - HIT may be confirmed before full horizon.
 - Partial observations are never treated as full-horizon
   drawdown observations.
 - Target/stop chronological order is unknown when
   both barriers are touched.
 - These are descriptive statistics, not executable
   trading backtests.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
import os
import sys
import tempfile
import time
import traceback

from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path


PROGRAM = "analyze_pattern_256_accumulation_entry_risk.py"
VERSION = "Clean V002"

ROOT = Path(__file__).resolve().parent
REPORTS = ROOT / "data" / "reports"

SOURCE_DIR = REPORTS / "pattern_256_accumulation_30pct"
SOURCE_CSV = SOURCE_DIR / "validation_detail.csv"
SOURCE_RESULT = SOURCE_DIR / "validation_result.json"

FAIR_DIR = REPORTS / "pattern_256_accumulation_fair"
FAIR_CSV = FAIR_DIR / "fair_comparison.csv"
FAIR_RESULT = FAIR_DIR / "fair_result.json"

OUTPUT_DIR = REPORTS / "pattern_256_accumulation_entry_risk"

SUMMARY_CSV = OUTPUT_DIR / "entry_risk_summary.csv"
STOP_CSV = OUTPUT_DIR / "entry_risk_stop_scenarios.csv"
RESULT_JSON = OUTPUT_DIR / "entry_risk_result.json"
MANIFEST_JSON = OUTPUT_DIR / "entry_risk_manifest.json"

TIMEFRAMES = ("h1", "h4", "d1")
HORIZONS = (24, 72, 168)
CLASSES = ("PATTERN_256_ONLY", "BOTH")
STOP_LEVELS = (5.0, 10.0, 15.0, 20.0)
TARGET_PCT = 30.0

REQUIRED_COLUMNS = (
    "timeframe",
    "market",
    "signal_timestamp",
    "classification",
    "entry_timestamp",
    "entry_price",
    "horizon",
    "target_return_pct",
    "status",
    "hit_30pct",
    "first_hit_bars",
    "max_high",
    "max_return_pct",
    "min_low",
    "max_drawdown_pct",
    "observed_bars",
    "required_bars",
)

SUMMARY_COLUMNS = [
    "timeframe",
    "horizon",
    "classification",
    "total_rows",
    "complete_rows",
    "censored_rows",
    "invalid_rows",
    "other_status_rows",
    "markets",
    "hit_count",
    "miss_count",
    "hit_rate_complete_pct",
    "hit_rate_all_pct",
    "mean_max_return_pct",
    "mean_max_drawdown_pct",
    "worst_max_drawdown_pct",
    "drawdown_below_5pct_count",
    "drawdown_below_10pct_count",
    "drawdown_below_15pct_count",
    "drawdown_below_20pct_count",
    "mean_first_hit_bars",
    "median_first_hit_bars",
    "hit_within_1_bar",
    "hit_within_3_bars",
    "hit_within_5_bars",
    "hit_within_10_bars",
    "hit_within_24_bars",
    "hit_within_72_bars",
    "assessment",
]

STOP_COLUMNS = [
    "timeframe",
    "horizon",
    "classification",
    "stop_loss_pct",
    "complete_rows",
    "target_hit_count",
    "stop_touched_count",
    "stop_touched_pct",
    "target_without_stop_touch",
    "target_and_stop_both_touched",
    "stop_touched_without_target",
    "neither_touched",
    "definite_target_first_count",
    "definite_stop_first_count",
    "ambiguous_order_count",
    "neither_count",
    "target_first_lower_bound_pct",
    "target_first_upper_bound_pct",
    "stop_first_lower_bound_pct",
    "stop_first_upper_bound_pct",
    "interpretation",
]


def log(message):
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{stamp}] {message}", flush=True)


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def file_hash(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def read_json(path):
    with path.open("r", encoding="utf-8-sig") as handle:
        return json.load(handle)


def atomic_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(
        prefix=path.name + ".",
        suffix=".tmp",
        dir=str(path.parent),
    )
    temp = Path(name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(
                value,
                handle,
                indent=2,
                ensure_ascii=False,
                allow_nan=False,
            )
        os.replace(temp, path)
    finally:
        if temp.exists():
            temp.unlink()


def atomic_csv(path, columns, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(
        prefix=path.name + ".",
        suffix=".tmp",
        dir=str(path.parent),
    )
    temp = Path(name)
    try:
        with os.fdopen(
            fd, "w", encoding="utf-8-sig", newline=""
        ) as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=columns,
                extrasaction="ignore",
            )
            writer.writeheader()
            writer.writerows(rows)
        os.replace(temp, path)
    finally:
        if temp.exists():
            temp.unlink()


def number(value, name, line):
    try:
        result = float(value)
    except (ValueError, TypeError):
        raise ValueError(
            f"Line {line}: invalid {name}: {value!r}"
        )
    if not math.isfinite(result):
        raise ValueError(
            f"Line {line}: non-finite {name}"
        )
    return result


def integer(value, name, line):
    try:
        return int(value)
    except (ValueError, TypeError):
        raise ValueError(
            f"Line {line}: invalid {name}: {value!r}"
        )


def percentage(count, total):
    if total == 0:
        return None
    return 100.0 * count / total


def fmt(value):
    if value is None:
        return ""
    return round(float(value), 6)


def median_from_counter(counts):
    size = sum(counts.values())
    if size == 0:
        return None

    positions = ((size - 1) // 2, size // 2)
    values = []
    cumulative = 0

    for item in sorted(counts):
        cumulative += counts[item]
        for position in positions[len(values):]:
            if position < cumulative:
                values.append(item)
            else:
                break
        if len(values) == 2:
            break

    return (values[0] + values[1]) / 2.0


def validate_inputs():
    log("[CHECK] Input files")

    for path in (SOURCE_CSV, SOURCE_RESULT):
        if not path.is_file():
            raise FileNotFoundError(path)

    source_result = read_json(SOURCE_RESULT)

    if source_result.get("status") != "PASS":
        raise ValueError("Source validation status is not PASS")

    detail_hash = file_hash(SOURCE_CSV)
    expected = source_result.get("detail_sha256")

    if expected is not None:
        if not isinstance(expected, str):
            raise ValueError("Invalid source detail SHA256")
        if detail_hash != expected.lower():
            raise ValueError("Source detail SHA256 mismatch")
    else:
        log("[WARN] Source result has no detail_sha256")

    hashes = {
        str(SOURCE_CSV.relative_to(ROOT)): detail_hash,
        str(SOURCE_RESULT.relative_to(ROOT)):
            file_hash(SOURCE_RESULT),
    }

    fair_exists = (
        FAIR_CSV.is_file(),
        FAIR_RESULT.is_file(),
    )

    if any(fair_exists) and not all(fair_exists):
        raise ValueError("Incomplete fair comparison context")

    if all(fair_exists):
        fair_result = read_json(FAIR_RESULT)
        if fair_result.get("status") != "PASS":
            raise ValueError("Fair comparison is not PASS")

        fair_hash = file_hash(FAIR_CSV)
        expected_fair = fair_result.get(
            "fair_comparison_sha256"
        )

        if (
            not isinstance(expected_fair, str)
            or fair_hash != expected_fair.lower()
        ):
            raise ValueError("Fair comparison hash mismatch")

        hashes[str(FAIR_CSV.relative_to(ROOT))] = fair_hash
        hashes[str(FAIR_RESULT.relative_to(ROOT))] = (
            file_hash(FAIR_RESULT)
        )

    log("[PASS] Input validation")
    return hashes


def new_group():
    return {
        "total": 0,
        "complete": 0,
        "censored": 0,
        "invalid": 0,
        "other": 0,
        "markets": set(),
        "hits": 0,
        "misses": 0,
        "early_hits": 0,
        "full_observation": 0,
        "return_sum": 0.0,
        "drawdown_sum": 0.0,
        "worst_drawdown": None,
        "drawdown_counts": Counter(),
        "first_hit_sum": 0,
        "first_hit_counts": Counter(),
        "stop": {
            level: Counter()
            for level in STOP_LEVELS
        },
    }


def update_group(group, row, line):
    group["total"] += 1
    status = row["status"].strip().upper()

    if status == "CENSORED":
        group["censored"] += 1
        return

    if status == "INVALID":
        group["invalid"] += 1
        return

    if status not in ("HIT", "MISS"):
        group["other"] += 1
        return

    horizon = integer(row["horizon"], "horizon", line)
    observed = integer(
        row["observed_bars"], "observed_bars", line
    )
    required = integer(
        row["required_bars"], "required_bars", line
    )

    if required != horizon:
        raise ValueError(
            f"Line {line}: required_bars != horizon"
        )

    if not (1 <= observed <= required):
        raise ValueError(
            f"Line {line}: invalid observed_bars"
        )

    hit = status == "HIT"

    # Clean V002:
    # A HIT may be confirmed before the full horizon.
    # A MISS requires the full horizon to be observed.
    if not hit and observed < required:
        raise ValueError(
            f"Line {line}: incomplete MISS evaluation"
        )

    hit_flag = row["hit_30pct"].strip()
    if hit_flag not in ("0", "1"):
        raise ValueError(
            f"Line {line}: invalid hit_30pct"
        )

    if (hit_flag == "1") != hit:
        raise ValueError(
            f"Line {line}: HIT flag mismatch"
        )

    target = number(
        row["target_return_pct"],
        "target_return_pct",
        line,
    )
    if abs(target - TARGET_PCT) > 1e-8:
        raise ValueError(
            f"Line {line}: unexpected target"
        )

    entry = number(row["entry_price"], "entry_price", line)
    high = number(row["max_high"], "max_high", line)
    low = number(row["min_low"], "min_low", line)
    max_return = number(
        row["max_return_pct"], "max_return_pct", line
    )
    max_drawdown = number(
        row["max_drawdown_pct"], "max_drawdown_pct", line
    )

    if (
        entry <= 0
        or high < 0
        or low < 0
        or low > high
    ):
        raise ValueError(
            f"Line {line}: invalid price values"
        )

    calculated_return = (high / entry - 1) * 100
    calculated_drawdown = (low / entry - 1) * 100

    if abs(calculated_return - max_return) > 0.001:
        raise ValueError(
            f"Line {line}: max_return mismatch"
        )

    if abs(calculated_drawdown - max_drawdown) > 0.001:
        raise ValueError(
            f"Line {line}: max_drawdown mismatch"
        )

    if hit and max_return < TARGET_PCT - 0.001:
        raise ValueError(
            f"Line {line}: HIT without target high"
        )

    if not hit and max_return >= TARGET_PCT + 0.001:
        raise ValueError(
            f"Line {line}: MISS despite target high"
        )

    first_hit = None

    if hit:
        first_hit = integer(
            row["first_hit_bars"],
            "first_hit_bars",
            line,
        )

        if not (1 <= first_hit <= observed):
            raise ValueError(
                f"Line {line}: first_hit_bars "
                "outside observed range"
            )

        group["hits"] += 1
        group["first_hit_sum"] += first_hit
        group["first_hit_counts"][first_hit] += 1

        if observed < required:
            group["early_hits"] += 1

    else:
        if row["first_hit_bars"].strip():
            raise ValueError(
                f"Line {line}: MISS has first_hit_bars"
            )
        group["misses"] += 1

    group["complete"] += 1

    if observed == required:
        group["full_observation"] += 1

    # Max high/low describe the actually observed
    # interval, which can be shorter than the horizon.
    group["return_sum"] += max_return
    group["drawdown_sum"] += max_drawdown

    if (
        group["worst_drawdown"] is None
        or max_drawdown < group["worst_drawdown"]
    ):
        group["worst_drawdown"] = max_drawdown

    for level in STOP_LEVELS:
        stop_touched = max_drawdown <= -level

        if stop_touched:
            group["drawdown_counts"][level] += 1

        counts = group["stop"][level]

        if hit and stop_touched:
            counts["both"] += 1
        elif hit:
            counts["target_only"] += 1
        elif stop_touched:
            counts["stop_only"] += 1
        else:
            counts["neither"] += 1


def scan_source():
    groups = defaultdict(new_group)
    total_rows = 0
    included_rows = 0
    excluded = Counter()

    log("[SCAN] Starting streamed CSV analysis")

    with SOURCE_CSV.open(
        "r", encoding="utf-8-sig", newline=""
    ) as handle:
        reader = csv.DictReader(handle)

        if not reader.fieldnames:
            raise ValueError("Source CSV header missing")

        missing = set(REQUIRED_COLUMNS) - set(
            reader.fieldnames
        )
        if missing:
            raise ValueError(
                "Missing source columns: "
                + ", ".join(sorted(missing))
            )

        for line, row in enumerate(reader, start=2):
            total_rows += 1

            classification = (
                row["classification"].strip().upper()
            )

            if classification not in CLASSES:
                excluded[classification] += 1
                continue

            timeframe = row["timeframe"].strip().lower()
            if timeframe not in TIMEFRAMES:
                raise ValueError(
                    f"Line {line}: unknown timeframe"
                )

            horizon = integer(
                row["horizon"], "horizon", line
            )
            if horizon not in HORIZONS:
                raise ValueError(
                    f"Line {line}: unknown horizon"
                )

            market = row["market"].strip()
            if not market:
                raise ValueError(
                    f"Line {line}: missing market"
                )

            key = (timeframe, horizon, classification)
            group = groups[key]
            group["markets"].add(market)

            update_group(group, row, line)
            included_rows += 1

            if total_rows % 500000 == 0:
                log(
                    f"[SCAN] {total_rows:,} rows processed"
                )

    log(f"[PASS] Source rows: {total_rows:,}")

    return groups, total_rows, included_rows, excluded


def build_summary(groups):
    rows = []

    for timeframe in TIMEFRAMES:
        for horizon in HORIZONS:
            for classification in CLASSES:
                group = groups[
                    (timeframe, horizon, classification)
                ]

                complete = group["complete"]
                hits = group["hits"]
                hit_counts = group["first_hit_counts"]

                if complete == 0:
                    assessment = "NO_COMPLETE_SAMPLES"
                elif complete < 100:
                    assessment = "SMALL_SAMPLE"
                elif group["early_hits"]:
                    assessment = (
                        "DESCRIPTIVE_MIXED_OBSERVATION"
                    )
                else:
                    assessment = "DESCRIPTIVE_RISK_ONLY"

                rows.append({
                    "timeframe": timeframe,
                    "horizon": horizon,
                    "classification": classification,
                    "total_rows": group["total"],
                    "complete_rows": complete,
                    "censored_rows": group["censored"],
                    "invalid_rows": group["invalid"],
                    "other_status_rows": group["other"],
                    "markets": len(group["markets"]),
                    "hit_count": hits,
                    "miss_count": group["misses"],
                    "hit_rate_complete_pct": fmt(
                        percentage(hits, complete)
                    ),
                    "hit_rate_all_pct": fmt(
                        percentage(hits, group["total"])
                    ),
                    "mean_max_return_pct": fmt(
                        group["return_sum"] / complete
                        if complete else None
                    ),
                    "mean_max_drawdown_pct": fmt(
                        group["drawdown_sum"] / complete
                        if complete else None
                    ),
                    "worst_max_drawdown_pct": fmt(
                        group["worst_drawdown"]
                    ),
                    "drawdown_below_5pct_count":
                        group["drawdown_counts"][5.0],
                    "drawdown_below_10pct_count":
                        group["drawdown_counts"][10.0],
                    "drawdown_below_15pct_count":
                        group["drawdown_counts"][15.0],
                    "drawdown_below_20pct_count":
                        group["drawdown_counts"][20.0],
                    "mean_first_hit_bars": fmt(
                        group["first_hit_sum"] / hits
                        if hits else None
                    ),
                    "median_first_hit_bars": fmt(
                        median_from_counter(hit_counts)
                    ),
                    "hit_within_1_bar": sum(
                        n for bar, n in hit_counts.items()
                        if bar <= 1
                    ),
                    "hit_within_3_bars": sum(
                        n for bar, n in hit_counts.items()
                        if bar <= 3
                    ),
                    "hit_within_5_bars": sum(
                        n for bar, n in hit_counts.items()
                        if bar <= 5
                    ),
                    "hit_within_10_bars": sum(
                        n for bar, n in hit_counts.items()
                        if bar <= 10
                    ),
                    "hit_within_24_bars": sum(
                        n for bar, n in hit_counts.items()
                        if bar <= 24
                    ),
                    "hit_within_72_bars": sum(
                        n for bar, n in hit_counts.items()
                        if bar <= 72
                    ),
                    "assessment": assessment,
                })

    return rows


def build_stop_scenarios(groups):
    rows = []

    for timeframe in TIMEFRAMES:
        for horizon in HORIZONS:
            for classification in CLASSES:
                group = groups[
                    (timeframe, horizon, classification)
                ]

                complete = group["complete"]

                for level in STOP_LEVELS:
                    counts = group["stop"][level]

                    target_only = counts["target_only"]
                    stop_only = counts["stop_only"]
                    both = counts["both"]
                    neither = counts["neither"]

                    if (
                        target_only + stop_only + both
                        + neither != complete
                    ):
                        raise ValueError(
                            "Barrier accounting mismatch"
                        )

                    stop_touched = stop_only + both

                    rows.append({
                        "timeframe": timeframe,
                        "horizon": horizon,
                        "classification": classification,
                        "stop_loss_pct": level,
                        "complete_rows": complete,
                        "target_hit_count": group["hits"],
                        "stop_touched_count": stop_touched,
                        "stop_touched_pct": fmt(
                            percentage(
                                stop_touched, complete
                            )
                        ),
                        "target_without_stop_touch":
                            target_only,
                        "target_and_stop_both_touched":
                            both,
                        "stop_touched_without_target":
                            stop_only,
                        "neither_touched": neither,
                        "definite_target_first_count":
                            target_only,
                        "definite_stop_first_count":
                            stop_only,
                        "ambiguous_order_count": both,
                        "neither_count": neither,
                        "target_first_lower_bound_pct":
                            fmt(percentage(
                                target_only, complete
                            )),
                        "target_first_upper_bound_pct":
                            fmt(percentage(
                                target_only + both,
                                complete,
                            )),
                        "stop_first_lower_bound_pct":
                            fmt(percentage(
                                stop_only, complete
                            )),
                        "stop_first_upper_bound_pct":
                            fmt(percentage(
                                stop_only + both,
                                complete,
                            )),
                        "interpretation": (
                            "BARRIER_ORDER_UNKNOWN"
                            if both else
                            "NO_DUAL_TOUCH_AMBIGUITY"
                        ),
                    })

    return rows


def verify_accounting(
    groups,
    summary,
    stops,
    total,
    included,
    excluded,
):
    if len(summary) != 18:
        raise ValueError("Expected 18 summary rows")

    if len(stops) != 72:
        raise ValueError("Expected 72 stop rows")

    if sum(g["total"] for g in groups.values()) != included:
        raise ValueError("Included row count mismatch")

    if included + sum(excluded.values()) != total:
        raise ValueError("Source row count mismatch")

    for group in groups.values():
        if (
            group["complete"]
            + group["censored"]
            + group["invalid"]
            + group["other"]
            != group["total"]
        ):
            raise ValueError("Status accounting mismatch")

        if (
            group["hits"] + group["misses"]
            != group["complete"]
        ):
            raise ValueError("HIT/MISS accounting mismatch")

        if (
            sum(group["first_hit_counts"].values())
            != group["hits"]
        ):
            raise ValueError("First-hit accounting mismatch")

        if group["early_hits"] > group["hits"]:
            raise ValueError("Early-hit accounting mismatch")

    log("[PASS] Accounting verification")


def verify_unchanged(hashes):
    for relative, expected in hashes.items():
        path = ROOT / relative
        if not path.is_file():
            raise FileNotFoundError(path)
        if file_hash(path) != expected:
            raise ValueError(
                f"Upstream input changed: {relative}"
            )
    log("[PASS] Upstream input hashes unchanged")


def main():
    started = time.monotonic()

    log("=" * 65)
    log(f"{PROGRAM} / {VERSION}")
    log("=" * 65)

    hashes = validate_inputs()

    groups, total, included, excluded = scan_source()

    summary = build_summary(groups)
    stops = build_stop_scenarios(groups)

    verify_accounting(
        groups, summary, stops, total, included, excluded
    )

    verify_unchanged(hashes)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    atomic_csv(SUMMARY_CSV, SUMMARY_COLUMNS, summary)
    atomic_csv(STOP_CSV, STOP_COLUMNS, stops)

    summary_hash = file_hash(SUMMARY_CSV)
    stop_hash = file_hash(STOP_CSV)

    complete = sum(
        g["complete"] for g in groups.values()
    )
    hits = sum(g["hits"] for g in groups.values())
    early_hits = sum(
        g["early_hits"] for g in groups.values()
    )

    result = {
        "program": PROGRAM,
        "version": VERSION,
        "status": "PASS",
        "created_at_utc": utc_now(),
        "target_return_pct": TARGET_PCT,
        "timeframes": list(TIMEFRAMES),
        "horizons": list(HORIZONS),
        "classifications": list(CLASSES),
        "stop_levels_pct": list(STOP_LEVELS),
        "source_rows": total,
        "included_rows": included,
        "excluded_classes": dict(excluded),
        "complete_rows": complete,
        "hit_rows": hits,
        "early_hit_rows": early_hits,
        "summary_rows": len(summary),
        "stop_scenario_rows": len(stops),
        "entry_risk_summary_sha256": summary_hash,
        "entry_risk_stop_scenarios_sha256": stop_hash,
        "elapsed_seconds": round(
            time.monotonic() - started, 3
        ),
        "limitations": [
            "Descriptive research, not an executable backtest.",
            "HIT may be confirmed before the full horizon.",
            "Some risk observations therefore have shorter windows.",
            "High/low are measured over observed windows only.",
            "Stop and target chronological order is unknown.",
            "Intrabar execution, gaps, fees and slippage are not modeled.",
            "Overlapping signals are not independent observations.",
            "TRAIN and TEST are not separated in this report.",
            "Future outcomes are used for evaluation only.",
        ],
    }

    manifest = {
        "program": PROGRAM,
        "version": VERSION,
        "created_at_utc": utc_now(),
        "inputs": hashes,
        "outputs": {
            "entry_risk_summary.csv": summary_hash,
            "entry_risk_stop_scenarios.csv": stop_hash,
        },
    }

    atomic_json(MANIFEST_JSON, manifest)
    atomic_json(RESULT_JSON, result)

    verify_unchanged(hashes)

    log("=" * 65)
    log("[PASS] Entry risk analysis completed")
    log(f"Source rows: {total:,}")
    log(f"Included rows: {included:,}")
    log(f"Complete HIT/MISS rows: {complete:,}")
    log(f"HIT rows: {hits:,}")
    log(f"Early HIT rows: {early_hits:,}")
    log(f"Summary rows: {len(summary)}")
    log(f"Stop scenario rows: {len(stops)}")
    log(f"Output: {OUTPUT_DIR}")
    log("=" * 65)

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        log("[STOP] Interrupted")
        sys.exit(130)
    except Exception:
        log("[FAIL] Entry risk analysis failed")
        traceback.print_exc()
        sys.exit(1)
