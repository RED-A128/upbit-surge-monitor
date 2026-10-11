
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
analyze_pattern_256_accumulation_entry_risk.py
Clean V001

Upbit Surge Monitor
Pattern 256 / Accumulation Entry Risk Research

PURPOSE
-------
Analyze historical entry risk separately for:
- PATTERN_256_ONLY
- BOTH (Pattern 256 + Accumulation)

INPUT (READ ONLY)
-----------------
data/reports/pattern_256_accumulation_30pct/
    validation_detail.csv
    validation_result.json

OPTIONAL CONTEXT (READ ONLY)
----------------------------
data/reports/pattern_256_accumulation_fair/
    fair_comparison.csv
    fair_result.json

OUTPUT
------
data/reports/pattern_256_accumulation_entry_risk/
    entry_risk_summary.csv
    entry_risk_stop_scenarios.csv
    entry_risk_result.json
    entry_risk_manifest.json

SAFETY
------
- Never change original OHLCV.
- Never change validation results.
- Never change independent validation.
- Never change fair comparison reports.
- Stream CSV; do not load entire source into memory.
- No trading orders.
- No API requests.
- No future information used to create signals.
- Stop/target ordering is NOT inferred from
  aggregate high/low values.
- No claim of executable trading profitability.

RUN
---
py analyze_pattern_256_accumulation_entry_risk.py

Python standard library only.
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


PROGRAM = (
    "analyze_pattern_256_accumulation_entry_risk.py"
)

VERSION = "Clean V001"

ROOT = Path(__file__).resolve().parent

REPORTS = ROOT / "data" / "reports"

SOURCE_DIR = (
    REPORTS / "pattern_256_accumulation_30pct"
)

SOURCE_CSV = SOURCE_DIR / "validation_detail.csv"
SOURCE_RESULT = SOURCE_DIR / "validation_result.json"

FAIR_DIR = (
    REPORTS / "pattern_256_accumulation_fair"
)

FAIR_CSV = FAIR_DIR / "fair_comparison.csv"
FAIR_RESULT = FAIR_DIR / "fair_result.json"

OUTPUT_DIR = (
    REPORTS / "pattern_256_accumulation_entry_risk"
)

SUMMARY_CSV = OUTPUT_DIR / "entry_risk_summary.csv"

STOP_CSV = (
    OUTPUT_DIR / "entry_risk_stop_scenarios.csv"
)

RESULT_JSON = OUTPUT_DIR / "entry_risk_result.json"

MANIFEST_JSON = (
    OUTPUT_DIR / "entry_risk_manifest.json"
)

TIMEFRAMES = ("h1", "h4", "d1")

HORIZONS = (24, 72, 168)

CLASSES = (
    "PATTERN_256_ONLY",
    "BOTH",
)

STOP_LEVELS = (5.0, 10.0, 15.0, 20.0)

TARGET_RETURN_PCT = 30.0

PROGRESS_INTERVAL = 500000

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
    timestamp = datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    print(f"[{timestamp}] {message}", flush=True)


def now_utc():
    return datetime.now(
        timezone.utc
    ).isoformat()


def sha256(path):
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        for block in iter(
            lambda: handle.read(1024 * 1024),
            b"",
        ):
            digest.update(block)

    return digest.hexdigest()


def read_json(path):
    with path.open(
        "r",
        encoding="utf-8-sig",
    ) as handle:
        return json.load(handle)


def write_json_atomic(path, data):
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    fd, temporary_name = tempfile.mkstemp(
        prefix=path.name + ".",
        suffix=".tmp",
        dir=str(path.parent),
    )

    temporary = Path(temporary_name)

    try:
        with os.fdopen(
            fd,
            "w",
            encoding="utf-8",
        ) as handle:
            json.dump(
                data,
                handle,
                indent=2,
                ensure_ascii=False,
                allow_nan=False,
            )

        os.replace(temporary, path)

    finally:
        if temporary.exists():
            temporary.unlink()


def write_csv_atomic(path, columns, rows):
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    fd, temporary_name = tempfile.mkstemp(
        prefix=path.name + ".",
        suffix=".tmp",
        dir=str(path.parent),
    )

    temporary = Path(temporary_name)

    try:
        with os.fdopen(
            fd,
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


def parse_float(value, field, line):
    try:
        number = float(value)
    except (TypeError, ValueError):
        raise ValueError(
            f"Line {line}: invalid {field}: {value!r}"
        )

    if not math.isfinite(number):
        raise ValueError(
            f"Line {line}: non-finite {field}"
        )

    return number


def parse_int(value, field, line):
    try:
        number = int(value)
    except (TypeError, ValueError):
        raise ValueError(
            f"Line {line}: invalid {field}: {value!r}"
        )

    return number


def rate(numerator, denominator):
    if denominator == 0:
        return None

    return 100.0 * numerator / denominator


def rounded(value):
    if value is None:
        return ""

    return round(float(value), 6)


def counter_median(counter):
    total = sum(counter.values())

    if total == 0:
        return None

    left_index = (total - 1) // 2
    right_index = total // 2

    left_value = None
    right_value = None
    seen = 0

    for value in sorted(counter):
        next_seen = seen + counter[value]

        if (
            left_value is None
            and left_index < next_seen
        ):
            left_value = value

        if right_index < next_seen:
            right_value = value
            break

        seen = next_seen

    return (
        left_value + right_value
    ) / 2.0


def validate_inputs():
    log("[CHECK] Source files")

    for path in (SOURCE_CSV, SOURCE_RESULT):
        if not path.is_file():
            raise FileNotFoundError(
                f"Required input missing: {path}"
            )

    result = read_json(SOURCE_RESULT)

    if result.get("status") != "PASS":
        raise ValueError(
            "Validation result status is not PASS"
        )

    expected_hash = result.get(
        "detail_sha256"
    )

    actual_hash = sha256(SOURCE_CSV)

    if expected_hash is not None:
        if (
            not isinstance(expected_hash, str)
            or len(expected_hash) != 64
        ):
            raise ValueError(
                "Invalid validation detail SHA256"
            )

        if (
            actual_hash.lower()
            != expected_hash.lower()
        ):
            raise ValueError(
                "Validation detail SHA256 mismatch"
            )

    else:
        log(
            "[WARN] Source result has no "
            "detail_sha256; recording actual hash"
        )

    hashes = {
        str(SOURCE_CSV.relative_to(ROOT)):
            actual_hash,
        str(SOURCE_RESULT.relative_to(ROOT)):
            sha256(SOURCE_RESULT),
    }

    optional_files = (
        FAIR_CSV,
        FAIR_RESULT,
    )

    if all(
        path.is_file()
        for path in optional_files
    ):
        fair_result = read_json(FAIR_RESULT)

        if fair_result.get("status") != "PASS":
            raise ValueError(
                "Existing fair comparison "
                "result is not PASS"
            )

        expected_fair_hash = fair_result.get(
            "fair_comparison_sha256"
        )

        actual_fair_hash = sha256(FAIR_CSV)

        if (
            not isinstance(expected_fair_hash, str)
            or actual_fair_hash.lower()
            != expected_fair_hash.lower()
        ):
            raise ValueError(
                "Fair comparison SHA256 mismatch"
            )

        for path in optional_files:
            hashes[
                str(path.relative_to(ROOT))
            ] = sha256(path)

        log("[PASS] Fair comparison context verified")

    elif any(
        path.is_file()
        for path in optional_files
    ):
        raise ValueError(
            "Fair comparison context is incomplete"
        )

    else:
        log(
            "[INFO] Optional fair comparison "
            "context not present"
        )

    log("[PASS] Source integrity")

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

    entry = parse_float(
        row["entry_price"],
        "entry_price",
        line,
    )

    high = parse_float(
        row["max_high"],
        "max_high",
        line,
    )

    low = parse_float(
        row["min_low"],
        "min_low",
        line,
    )

    max_return = parse_float(
        row["max_return_pct"],
        "max_return_pct",
        line,
    )

    max_drawdown = parse_float(
        row["max_drawdown_pct"],
        "max_drawdown_pct",
        line,
    )

    observed = parse_int(
        row["observed_bars"],
        "observed_bars",
        line,
    )

    required = parse_int(
        row["required_bars"],
        "required_bars",
        line,
    )

    horizon = int(row["horizon"])

    if (
        entry <= 0
        or high < 0
        or low < 0
        or low > high
    ):
        raise ValueError(
            f"Line {line}: invalid price values"
        )

    if (
        observed != required
        or required != horizon
    ):
        raise ValueError(
            f"Line {line}: incomplete evaluation "
            "marked HIT/MISS"
        )

    calculated_return = (
        (high / entry - 1.0) * 100.0
    )

    calculated_drawdown = (
        (low / entry - 1.0) * 100.0
    )

    if (
        abs(
            calculated_return - max_return
        ) > 0.001
    ):
        raise ValueError(
            f"Line {line}: max return mismatch"
        )

    if (
        abs(
            calculated_drawdown - max_drawdown
        ) > 0.001
    ):
        raise ValueError(
            f"Line {line}: max drawdown mismatch"
        )

    hit = status == "HIT"

    hit_field = row["hit_30pct"].strip()

    if hit_field not in ("0", "1"):
        raise ValueError(
            f"Line {line}: invalid hit_30pct"
        )

    if (hit_field == "1") != hit:
        raise ValueError(
            f"Line {line}: HIT status mismatch"
        )

    target = parse_float(
        row["target_return_pct"],
        "target_return_pct",
        line,
    )

    if abs(target - TARGET_RETURN_PCT) > 1e-8:
        raise ValueError(
            f"Line {line}: unexpected target"
        )

    if hit and max_return < (
        TARGET_RETURN_PCT - 0.001
    ):
        raise ValueError(
            f"Line {line}: HIT without target"
        )

    if not hit and max_return >= (
        TARGET_RETURN_PCT + 0.001
    ):
        raise ValueError(
            f"Line {line}: MISS despite target"
        )

    group["complete"] += 1

    if hit:
        group["hits"] += 1
    else:
        group["misses"] += 1

    group["return_sum"] += max_return
    group["drawdown_sum"] += max_drawdown

    worst = group["worst_drawdown"]

    if (
        worst is None
        or max_drawdown < worst
    ):
        group["worst_drawdown"] = max_drawdown

    for level in STOP_LEVELS:
        if max_drawdown <= -level:
            group["drawdown_counts"][level] += 1

    if hit:
        first_hit = parse_int(
            row["first_hit_bars"],
            "first_hit_bars",
            line,
        )

        if not (1 <= first_hit <= horizon):
            raise ValueError(
                f"Line {line}: invalid first_hit_bars"
            )

        group["first_hit_sum"] += first_hit
        group["first_hit_counts"][first_hit] += 1

    elif row["first_hit_bars"].strip():
        raise ValueError(
            f"Line {line}: MISS has first_hit_bars"
        )

    for level in STOP_LEVELS:
        stop_touched = (
            max_drawdown <= -level
        )

        counter = group["stop"][level]

        if hit and stop_touched:
            counter["both"] += 1

        elif hit and not stop_touched:
            counter["target_only"] += 1

        elif not hit and stop_touched:
            counter["stop_only"] += 1

        else:
            counter["neither"] += 1


def scan_source():
    groups = defaultdict(new_group)

    total_rows = 0
    included_rows = 0
    excluded_classes = Counter()

    log("[SCAN] Streaming validation_detail.csv")

    with SOURCE_CSV.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as handle:

        reader = csv.DictReader(handle)

        if not reader.fieldnames:
            raise ValueError(
                "Source CSV has no header"
            )

        missing = set(
            REQUIRED_COLUMNS
        ) - set(reader.fieldnames)

        if missing:
            raise ValueError(
                "Missing CSV columns: "
                + ", ".join(sorted(missing))
            )

        for line, row in enumerate(
            reader,
            start=2,
        ):
            total_rows += 1

            classification = (
                row["classification"]
                .strip()
                .upper()
            )

            if classification not in CLASSES:
                excluded_classes[
                    classification
                ] += 1
                continue

            timeframe = (
                row["timeframe"]
                .strip()
                .lower()
            )

            if timeframe not in TIMEFRAMES:
                raise ValueError(
                    f"Line {line}: unexpected timeframe"
                )

            horizon = parse_int(
                row["horizon"],
                "horizon",
                line,
            )

            if horizon not in HORIZONS:
                raise ValueError(
                    f"Line {line}: unexpected horizon"
                )

            market = row["market"].strip()

            if not market:
                raise ValueError(
                    f"Line {line}: empty market"
                )

            key = (
                timeframe,
                horizon,
                classification,
            )

            group = groups[key]

            group["markets"].add(market)

            update_group(
                group,
                row,
                line,
            )

            included_rows += 1

            if (
                total_rows % PROGRESS_INTERVAL
                == 0
            ):
                log(
                    "[SCAN] "
                    f"{total_rows:,} rows processed"
                )

    log(
        "[PASS] Source rows: "
        f"{total_rows:,}"
    )

    return (
        groups,
        total_rows,
        included_rows,
        excluded_classes,
    )


def build_summary(groups):
    results = []

    for timeframe in TIMEFRAMES:
        for horizon in HORIZONS:
            for classification in CLASSES:
                group = groups[
                    (
                        timeframe,
                        horizon,
                        classification,
                    )
                ]

                complete = group["complete"]
                hits = group["hits"]

                hit_counts = group[
                    "first_hit_counts"
                ]

                if complete == 0:
                    assessment = "NO_COMPLETE_SAMPLES"

                elif complete < 100:
                    assessment = "SMALL_SAMPLE"

                else:
                    assessment = (
                        "DESCRIPTIVE_RISK_ONLY"
                    )

                results.append({
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
                    "hit_rate_complete_pct": rounded(
                        rate(hits, complete)
                    ),
                    "hit_rate_all_pct": rounded(
                        rate(hits, group["total"])
                    ),
                    "mean_max_return_pct": rounded(
                        (
                            group["return_sum"] / complete
                            if complete else None
                        )
                    ),
                    "mean_max_drawdown_pct": rounded(
                        (
                            group["drawdown_sum"] / complete
                            if complete else None
                        )
                    ),
                    "worst_max_drawdown_pct": rounded(
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
                    "mean_first_hit_bars": rounded(
                        (
                            group["first_hit_sum"] / hits
                            if hits else None
                        )
                    ),
                    "median_first_hit_bars": rounded(
                        counter_median(hit_counts)
                    ),
                    "hit_within_1_bar": sum(
                        count
                        for bar, count
                        in hit_counts.items()
                        if bar <= 1
                    ),
                    "hit_within_3_bars": sum(
                        count
                        for bar, count
                        in hit_counts.items()
                        if bar <= 3
                    ),
                    "hit_within_5_bars": sum(
                        count
                        for bar, count
                        in hit_counts.items()
                        if bar <= 5
                    ),
                    "hit_within_10_bars": sum(
                        count
                        for bar, count
                        in hit_counts.items()
                        if bar <= 10
                    ),
                    "hit_within_24_bars": sum(
                        count
                        for bar, count
                        in hit_counts.items()
                        if bar <= 24
                    ),
                    "hit_within_72_bars": sum(
                        count
                        for bar, count
                        in hit_counts.items()
                        if bar <= 72
                    ),
                    "assessment": assessment,
                })

    return results


def build_stop_scenarios(groups):
    results = []

    for timeframe in TIMEFRAMES:
        for horizon in HORIZONS:
            for classification in CLASSES:
                group = groups[
                    (
                        timeframe,
                        horizon,
                        classification,
                    )
                ]

                complete = group["complete"]

                for level in STOP_LEVELS:
                    counts = group["stop"][level]

                    target_only = counts[
                        "target_only"
                    ]

                    stop_only = counts[
                        "stop_only"
                    ]

                    both = counts["both"]
                    neither = counts["neither"]

                    if (
                        target_only
                        + stop_only
                        + both
                        + neither
                        != complete
                    ):
                        raise ValueError(
                            "Stop scenario accounting mismatch"
                        )

                    stop_touched = (
                        stop_only + both
                    )

                    # With aggregate high/low, if both
                    # stop and target were touched,
                    # chronological ordering is unknown.
                    #
                    # Bounds assume only two barriers:
                    # stop and +30% target.
                    #
                    # These are counts of potential
                    # first-barrier outcomes, not actual
                    # executed trade results.

                    target_lower = target_only
                    target_upper = (
                        target_only + both
                    )

                    stop_lower = stop_only
                    stop_upper = (
                        stop_only + both
                    )

                    results.append({
                        "timeframe": timeframe,
                        "horizon": horizon,
                        "classification": classification,
                        "stop_loss_pct": level,
                        "complete_rows": complete,
                        "target_hit_count": group["hits"],
                        "stop_touched_count": stop_touched,
                        "stop_touched_pct": rounded(
                            rate(
                                stop_touched,
                                complete,
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
                            rounded(
                                rate(
                                    target_lower,
                                    complete,
                                )
                            ),
                        "target_first_upper_bound_pct":
                            rounded(
                                rate(
                                    target_upper,
                                    complete,
                                )
                            ),
                        "stop_first_lower_bound_pct":
                            rounded(
                                rate(
                                    stop_lower,
                                    complete,
                                )
                            ),
                        "stop_first_upper_bound_pct":
                            rounded(
                                rate(
                                    stop_upper,
                                    complete,
                                )
                            ),
                        "interpretation": (
                            "BARRIER_ORDER_UNKNOWN"
                            if both
                            else "NO_DUAL_TOUCH_AMBIGUITY"
                        ),
                    })

    return results


def verify_results(
    groups,
    summary_rows,
    stop_rows,
    total_rows,
    included_rows,
    excluded_classes,
):
    if len(summary_rows) != 18:
        raise ValueError(
            "Expected 18 summary rows"
        )

    if len(stop_rows) != 72:
        raise ValueError(
            "Expected 72 stop scenario rows"
        )

    group_total = sum(
        group["total"]
        for group in groups.values()
    )

    if group_total != included_rows:
        raise ValueError(
            "Included row accounting mismatch"
        )

    if (
        included_rows
        + sum(excluded_classes.values())
        != total_rows
    ):
        raise ValueError(
            "Source row accounting mismatch"
        )

    for group in groups.values():
        if (
            group["complete"]
            + group["censored"]
            + group["invalid"]
            + group["other"]
            != group["total"]
        ):
            raise ValueError(
                "Status accounting mismatch"
            )

        if (
            group["hits"] + group["misses"]
            != group["complete"]
        ):
            raise ValueError(
                "HIT/MISS accounting mismatch"
            )

        if (
            sum(
                group["first_hit_counts"].values()
            )
            != group["hits"]
        ):
            raise ValueError(
                "First-hit accounting mismatch"
            )

    log("[PASS] Result accounting")


def verify_inputs_unchanged(hashes):
    log("[CHECK] Input files unchanged")

    for relative, expected in hashes.items():
        path = ROOT / relative

        if not path.is_file():
            raise FileNotFoundError(
                f"Input disappeared: {path}"
            )

        if sha256(path) != expected:
            raise ValueError(
                f"Input SHA256 changed: {relative}"
            )

    log("[PASS] Input SHA256 unchanged")


def main():
    started = time.monotonic()

    log("=" * 72)
    log(f"{PROGRAM} - {VERSION}")
    log("=" * 72)

    hashes = validate_inputs()

    (
        groups,
        total_rows,
        included_rows,
        excluded_classes,
    ) = scan_source()

    summary_rows = build_summary(groups)

    stop_rows = build_stop_scenarios(groups)

    verify_results(
        groups,
        summary_rows,
        stop_rows,
        total_rows,
        included_rows,
        excluded_classes,
    )

    # Verify all upstream files before writing
    # new reports.
    verify_inputs_unchanged(hashes)

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    write_csv_atomic(
        SUMMARY_CSV,
        SUMMARY_COLUMNS,
        summary_rows,
    )

    write_csv_atomic(
        STOP_CSV,
        STOP_COLUMNS,
        stop_rows,
    )

    summary_hash = sha256(SUMMARY_CSV)
    stop_hash = sha256(STOP_CSV)

    complete_rows = sum(
        group["complete"]
        for group in groups.values()
    )

    hit_rows = sum(
        group["hits"]
        for group in groups.values()
    )

    result = {
        "program": PROGRAM,
        "version": VERSION,
        "status": "PASS",
        "created_at_utc": now_utc(),
        "method": (
            "DESCRIPTIVE_ENTRY_RISK_AND_"
            "TWO_BARRIER_OUTCOME_BOUNDS"
        ),
        "target_return_pct": TARGET_RETURN_PCT,
        "timeframes": list(TIMEFRAMES),
        "horizons": list(HORIZONS),
        "classifications": list(CLASSES),
        "stop_levels_pct": list(STOP_LEVELS),
        "source_rows": total_rows,
        "included_rows": included_rows,
        "excluded_classes": dict(
            excluded_classes
        ),
        "complete_rows": complete_rows,
        "hit_rows": hit_rows,
        "summary_rows": len(summary_rows),
        "stop_scenario_rows": len(stop_rows),
        "entry_risk_summary_sha256":
            summary_hash,
        "entry_risk_stop_scenarios_sha256":
            stop_hash,
        "elapsed_seconds": round(
            time.monotonic() - started,
            3,
        ),
        "limitations": [
            "This report is descriptive, not a trading backtest.",
            "High and low values describe the entire evaluation horizon.",
            "Maximum drawdown may occur after the +30% target was hit.",
            "When both target and stop are touched, their order is unknown.",
            "First-hit bars are available for target hits only.",
            "No intrabar execution order is inferred.",
            "No slippage, fees, liquidity or gap execution model is applied.",
            "Repeated signals and multiple horizons are correlated.",
            "This report does not reproduce the independent non-overlap selection.",
            "TRAIN and TEST are not separated in this source-level report.",
            "Future outcomes are used for historical evaluation only.",
            "Historical 30% target attainment is not guaranteed future profit.",
        ],
    }

    manifest = {
        "program": PROGRAM,
        "version": VERSION,
        "created_at_utc": now_utc(),
        "inputs": hashes,
        "outputs": {
            "entry_risk_summary.csv":
                summary_hash,
            "entry_risk_stop_scenarios.csv":
                stop_hash,
        },
    }

    write_json_atomic(
        MANIFEST_JSON,
        manifest,
    )

    write_json_atomic(
        RESULT_JSON,
        result,
    )

    # Recheck source files after all report writes.
    verify_inputs_unchanged(hashes)

    log("=" * 72)
    log("[PASS] Entry risk analysis completed")
    log(
        f"[PASS] Source rows: {total_rows:,}"
    )
    log(
        f"[PASS] Included rows: {included_rows:,}"
    )
    log(
        f"[PASS] Complete evaluations: "
        f"{complete_rows:,}"
    )
    log(
        f"[PASS] Target hits: {hit_rows:,}"
    )
    log(
        f"[PASS] Summary rows: "
        f"{len(summary_rows)}"
    )
    log(
        f"[PASS] Stop scenario rows: "
        f"{len(stop_rows)}"
    )
    log(
        f"[OUTPUT] {OUTPUT_DIR}"
    )
    log("=" * 72)

    for row in summary_rows:
        if row["horizon"] == 168:
            log(
                "[168 BARS] "
                f"{row['timeframe']} "
                f"{row['classification']} "
                f"complete={row['complete_rows']} "
                f"hits={row['hit_count']} "
                f"hit_rate={row['hit_rate_complete_pct']}% "
                f"mean_dd={row['mean_max_drawdown_pct']}%"
            )

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())

    except KeyboardInterrupt:
        log("[STOP] Interrupted safely")
        sys.exit(130)

    except Exception:
        log("[FAIL] Entry risk analysis failed")
        traceback.print_exc()
        sys.exit(1)
