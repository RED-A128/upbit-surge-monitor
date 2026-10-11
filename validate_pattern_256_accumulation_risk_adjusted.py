
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
validate_pattern_256_accumulation_risk_adjusted.py
Clean V002

Pattern 256 / Accumulation Risk Adjusted Validation

- Read upstream reports only.
- Validate source and output SHA256.
- Compare PATTERN_256_ONLY and BOTH.
- Separate h1/h4/d1 and 24/72/168 bars.
- Preserve uncertainty of target/stop ordering.
- Do not modify OHLCV or upstream reports.
- Do not execute trades.
- Standard library only.

V002:
Fix source_rows accounting. The upstream entry-risk report
counts rows across timeframe, horizon and classification.
It must not be compared with 24-bar groups alone.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import math
import os
import sys
import tempfile
import traceback

from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PROGRAM = "validate_pattern_256_accumulation_risk_adjusted.py"
VERSION = "Clean V002"

SOURCE_DIR = Path(
    "data/reports/pattern_256_accumulation_entry_risk"
)

OUTPUT_DIR = Path(
    "data/reports/pattern_256_accumulation_risk_adjusted"
)

SUMMARY_FILE = "entry_risk_summary.csv"
STOP_FILE = "entry_risk_stop_scenarios.csv"
SOURCE_RESULT_FILE = "entry_risk_result.json"
SOURCE_MANIFEST_FILE = "entry_risk_manifest.json"

GROUPS_FILE = "risk_adjusted_groups.csv"
COMPARISON_FILE = "risk_adjusted_comparison.csv"
RESULT_FILE = "risk_adjusted_result.json"
MANIFEST_FILE = "risk_adjusted_manifest.json"

TIMEFRAMES = ("h1", "h4", "d1")
HORIZONS = (24, 72, 168)
CLASSIFICATIONS = ("PATTERN_256_ONLY", "BOTH")
STOP_LEVELS = (5, 10, 15, 20)

MIN_COMPLETE_ROWS = 100
MIN_HIT_ROWS = 30
MAX_CENSORED_PCT = 10.0

SUMMARY_REQUIRED = (
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
)

STOP_REQUIRED = (
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
)

GROUP_FIELDS = [
    "timeframe",
    "horizon",
    "classification",
    "stop_loss_pct",
    "total_rows",
    "complete_rows",
    "censored_rows",
    "censored_pct",
    "markets",
    "hit_count",
    "hit_rate_pct",
    "stop_touched_count",
    "stop_touched_pct",
    "target_without_stop_touch",
    "target_and_stop_both_touched",
    "ambiguous_order_pct",
    "target_first_lower_bound_pct",
    "target_first_upper_bound_pct",
    "stop_first_lower_bound_pct",
    "stop_first_upper_bound_pct",
    "conservative_target_minus_stop_pp",
    "optimistic_target_minus_stop_pp",
    "sample_warning",
    "censoring_warning",
    "interpretation",
]

COMPARISON_FIELDS = [
    "timeframe",
    "horizon",
    "stop_loss_pct",
    "only_complete_rows",
    "both_complete_rows",
    "only_hit_rate_pct",
    "both_hit_rate_pct",
    "hit_rate_difference_pp",
    "only_stop_touched_pct",
    "both_stop_touched_pct",
    "stop_touch_difference_pp",
    "only_target_first_lower_pct",
    "both_target_first_lower_pct",
    "target_lower_difference_pp",
    "only_stop_first_upper_pct",
    "both_stop_first_upper_pct",
    "stop_upper_difference_pp",
    "only_conservative_margin_pp",
    "both_conservative_margin_pp",
    "conservative_margin_difference_pp",
    "only_ambiguous_count",
    "both_ambiguous_count",
    "only_censored_pct",
    "both_censored_pct",
    "sample_warning",
    "censoring_warning",
    "descriptive_direction",
    "interpretation",
]


class ValidationError(RuntimeError):
    pass


def log(message: str) -> None:
    timestamp = datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    print(f"[{timestamp}] {message}", flush=True)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        for block in iter(
            lambda: handle.read(1024 * 1024),
            b"",
        ):
            digest.update(block)

    return digest.hexdigest()


def require_file(path: Path) -> None:
    if not path.is_file():
        raise ValidationError(
            f"Required file missing: {path}"
        )


def to_int(
    value: Any,
    field: str,
    minimum: int = 0,
) -> int:
    try:
        text = str(value).strip()

        if not text:
            raise ValueError("empty")

        number = int(text)

    except (ValueError, TypeError) as exc:
        raise ValidationError(
            f"Invalid integer {field}: {value!r}"
        ) from exc

    if number < minimum:
        raise ValidationError(
            f"{field} below {minimum}: {number}"
        )

    return number


def to_float(value: Any, field: str) -> float:
    try:
        number = float(str(value).strip())

    except (ValueError, TypeError) as exc:
        raise ValidationError(
            f"Invalid float {field}: {value!r}"
        ) from exc

    if not math.isfinite(number):
        raise ValidationError(
            f"Non-finite {field}: {value!r}"
        )

    return number


def pct(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0

    return 100.0 * numerator / denominator


def rnd(value: float) -> float:
    return round(value, 6)


def rate_matches(
    actual: float,
    expected: float,
    tolerance: float = 0.0001,
) -> bool:
    return abs(actual - expected) <= tolerance


def read_csv(
    path: Path,
    required: tuple[str, ...],
) -> list[dict[str, str]]:
    require_file(path)

    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as handle:
        reader = csv.DictReader(handle)

        if not reader.fieldnames:
            raise ValidationError(
                f"CSV header missing: {path}"
            )

        missing = [
            field
            for field in required
            if field not in reader.fieldnames
        ]

        if missing:
            raise ValidationError(
                f"Missing CSV fields in {path.name}: "
                + ", ".join(missing)
            )

        rows = []

        for line_number, row in enumerate(
            reader,
            start=2,
        ):
            if None in row or any(
                value is None
                for value in row.values()
            ):
                raise ValidationError(
                    f"Malformed CSV line {line_number}: "
                    f"{path.name}"
                )

            rows.append(row)

    if not rows:
        raise ValidationError(
            f"Empty CSV: {path}"
        )

    return rows


def load_json(path: Path) -> dict[str, Any]:
    require_file(path)

    with path.open(
        "r",
        encoding="utf-8-sig",
    ) as handle:
        data = json.load(handle)

    if not isinstance(data, dict):
        raise ValidationError(
            f"Expected JSON object: {path}"
        )

    return data


def validate_dimensions(
    timeframe: str,
    horizon: int,
    classification: str,
) -> None:
    if timeframe not in TIMEFRAMES:
        raise ValidationError(
            f"Unexpected timeframe: {timeframe}"
        )

    if horizon not in HORIZONS:
        raise ValidationError(
            f"Unexpected horizon: {horizon}"
        )

    if classification not in CLASSIFICATIONS:
        raise ValidationError(
            f"Unexpected classification: {classification}"
        )


def validate_source_metadata(
    result: dict[str, Any],
    manifest: dict[str, Any],
    hashes: dict[str, str],
) -> None:
    if result.get("status") != "PASS":
        raise ValidationError(
            "Upstream result status is not PASS."
        )

    if result.get("program") != (
        "analyze_pattern_256_accumulation_entry_risk.py"
    ):
        raise ValidationError(
            "Unexpected upstream program."
        )

    if result.get("version") != "Clean V002":
        raise ValidationError(
            "Unexpected upstream version."
        )

    if manifest.get("program") != result["program"]:
        raise ValidationError(
            "Upstream manifest program mismatch."
        )

    if manifest.get("version") != result["version"]:
        raise ValidationError(
            "Upstream manifest version mismatch."
        )

    outputs = manifest.get("outputs")

    if not isinstance(outputs, dict):
        raise ValidationError(
            "Upstream manifest outputs missing."
        )

    checks = (
        (
            SUMMARY_FILE,
            "entry_risk_summary_sha256",
        ),
        (
            STOP_FILE,
            "entry_risk_stop_scenarios_sha256",
        ),
    )

    for filename, result_field in checks:
        actual = hashes[filename]

        expected_result = str(
            result.get(result_field, "")
        ).lower()

        expected_manifest = str(
            outputs.get(filename, "")
        ).lower()

        if actual != expected_result:
            raise ValidationError(
                f"Result SHA256 mismatch: {filename}"
            )

        if actual != expected_manifest:
            raise ValidationError(
                f"Manifest SHA256 mismatch: {filename}"
            )


def expected_summary_keys() -> set:
    return {
        (tf, horizon, classification)
        for tf in TIMEFRAMES
        for horizon in HORIZONS
        for classification in CLASSIFICATIONS
    }


def expected_stop_keys() -> set:
    return {
        (tf, horizon, classification, stop)
        for tf in TIMEFRAMES
        for horizon in HORIZONS
        for classification in CLASSIFICATIONS
        for stop in STOP_LEVELS
    }


def parse_summary(
    raw_rows: list[dict[str, str]],
) -> dict[tuple, dict[str, Any]]:
    parsed = {}

    count_fields = (
        "total_rows",
        "complete_rows",
        "censored_rows",
        "invalid_rows",
        "other_status_rows",
        "markets",
        "hit_count",
        "miss_count",
    )

    for row in raw_rows:
        tf = row["timeframe"].strip()
        horizon = to_int(
            row["horizon"],
            "horizon",
        )
        classification = row[
            "classification"
        ].strip()

        validate_dimensions(
            tf,
            horizon,
            classification,
        )

        key = (tf, horizon, classification)

        if key in parsed:
            raise ValidationError(
                f"Duplicate summary group: {key}"
            )

        item = {
            "timeframe": tf,
            "horizon": horizon,
            "classification": classification,
        }

        for field in count_fields:
            item[field] = to_int(
                row[field],
                field,
            )

        item["hit_rate_pct"] = to_float(
            row["hit_rate_complete_pct"],
            "hit_rate_complete_pct",
        )

        if item["total_rows"] != sum(
            item[field]
            for field in (
                "complete_rows",
                "censored_rows",
                "invalid_rows",
                "other_status_rows",
            )
        ):
            raise ValidationError(
                f"Summary status accounting mismatch: {key}"
            )

        if item["complete_rows"] != (
            item["hit_count"]
            + item["miss_count"]
        ):
            raise ValidationError(
                f"Summary HIT/MISS mismatch: {key}"
            )

        expected_rate = pct(
            item["hit_count"],
            item["complete_rows"],
        )

        if not rate_matches(
            item["hit_rate_pct"],
            expected_rate,
        ):
            raise ValidationError(
                f"Summary HIT rate mismatch: {key}"
            )

        parsed[key] = item

    if set(parsed) != expected_summary_keys():
        raise ValidationError(
            "Summary group keys incomplete or unexpected."
        )

    return parsed


def parse_stops(
    raw_rows: list[dict[str, str]],
    summaries: dict,
) -> dict[tuple, dict[str, Any]]:
    parsed = {}

    count_fields = (
        "complete_rows",
        "target_hit_count",
        "stop_touched_count",
        "target_without_stop_touch",
        "target_and_stop_both_touched",
        "stop_touched_without_target",
        "neither_touched",
        "definite_target_first_count",
        "definite_stop_first_count",
        "ambiguous_order_count",
        "neither_count",
    )

    rate_fields = (
        "stop_touched_pct",
        "target_first_lower_bound_pct",
        "target_first_upper_bound_pct",
        "stop_first_lower_bound_pct",
        "stop_first_upper_bound_pct",
    )

    for row in raw_rows:
        tf = row["timeframe"].strip()
        horizon = to_int(
            row["horizon"],
            "horizon",
        )
        classification = row[
            "classification"
        ].strip()

        validate_dimensions(
            tf,
            horizon,
            classification,
        )

        stop_float = to_float(
            row["stop_loss_pct"],
            "stop_loss_pct",
        )
        stop = int(stop_float)

        if (
            stop_float != stop
            or stop not in STOP_LEVELS
        ):
            raise ValidationError(
                f"Unexpected stop level: {stop_float}"
            )

        key = (tf, horizon, classification, stop)

        if key in parsed:
            raise ValidationError(
                f"Duplicate stop group: {key}"
            )

        summary_key = key[:3]

        if summary_key not in summaries:
            raise ValidationError(
                f"Missing summary group: {summary_key}"
            )

        item = {
            "timeframe": tf,
            "horizon": horizon,
            "classification": classification,
            "stop_loss_pct": stop,
        }

        for field in count_fields:
            item[field] = to_int(
                row[field],
                field,
            )

        for field in rate_fields:
            item[field] = to_float(
                row[field],
                field,
            )

        complete = item["complete_rows"]
        target = item["target_hit_count"]
        touched = item["stop_touched_count"]

        target_only = item[
            "target_without_stop_touch"
        ]
        both = item[
            "target_and_stop_both_touched"
        ]
        stop_only = item[
            "stop_touched_without_target"
        ]
        neither = item["neither_touched"]

        summary = summaries[summary_key]

        if complete != summary["complete_rows"]:
            raise ValidationError(
                f"Stop complete count mismatch: {key}"
            )

        if target != summary["hit_count"]:
            raise ValidationError(
                f"Stop HIT count mismatch: {key}"
            )

        if complete != (
            target_only
            + both
            + stop_only
            + neither
        ):
            raise ValidationError(
                f"Stop category accounting mismatch: {key}"
            )

        if target != target_only + both:
            raise ValidationError(
                f"Target accounting mismatch: {key}"
            )

        if touched != stop_only + both:
            raise ValidationError(
                f"Stop touch accounting mismatch: {key}"
            )

        if (
            item["definite_target_first_count"]
            != target_only
            or item["definite_stop_first_count"]
            != stop_only
            or item["ambiguous_order_count"]
            != both
            or item["neither_count"]
            != neither
        ):
            raise ValidationError(
                f"Barrier order accounting mismatch: {key}"
            )

        calculated = {
            "stop_touched_pct": pct(
                touched,
                complete,
            ),
            "target_first_lower_bound_pct": pct(
                target_only,
                complete,
            ),
            "target_first_upper_bound_pct": pct(
                target_only + both,
                complete,
            ),
            "stop_first_lower_bound_pct": pct(
                stop_only,
                complete,
            ),
            "stop_first_upper_bound_pct": pct(
                stop_only + both,
                complete,
            ),
        }

        for field, expected in calculated.items():
            if not rate_matches(
                item[field],
                expected,
            ):
                raise ValidationError(
                    f"Stop rate mismatch {field}: {key}"
                )

        parsed[key] = item

    if set(parsed) != expected_stop_keys():
        raise ValidationError(
            "Stop scenario group keys incomplete or unexpected."
        )

    return parsed


def verify_result_counts(
    result: dict[str, Any],
    summaries: dict,
    stops: dict,
) -> None:
    """
    V002 correction:

    The upstream entry-risk analysis reports source_rows
    across all evaluated timeframe/horizon/classification
    rows.

    Never compare source_rows against the 24-bar subset.
    """

    if len(summaries) != 18:
        raise ValidationError(
            "Expected 18 summary groups."
        )

    if len(stops) != 72:
        raise ValidationError(
            "Expected 72 stop scenario groups."
        )

    if to_int(
        result.get("summary_rows"),
        "summary_rows",
    ) != len(summaries):
        raise ValidationError(
            "Source summary row count mismatch."
        )

    if to_int(
        result.get("stop_scenario_rows"),
        "stop_scenario_rows",
    ) != len(stops):
        raise ValidationError(
            "Source stop scenario row count mismatch."
        )

    total = sum(
        item["total_rows"]
        for item in summaries.values()
    )

    complete = sum(
        item["complete_rows"]
        for item in summaries.values()
    )

    hits = sum(
        item["hit_count"]
        for item in summaries.values()
    )

    source_rows = to_int(
        result.get("source_rows"),
        "source_rows",
    )

    included_rows = to_int(
        result.get("included_rows"),
        "included_rows",
    )

    complete_rows = to_int(
        result.get("complete_rows"),
        "complete_rows",
    )

    hit_rows = to_int(
        result.get("hit_rows"),
        "hit_rows",
    )

    # Check that all reported rows were accounted for.
    if total != included_rows:
        raise ValidationError(
            "Included row accounting mismatch: "
            f"summary={total}, result={included_rows}"
        )

    # The source CSV has one record for each evaluated
    # horizon. The upstream report counts those records.
    if source_rows < included_rows:
        raise ValidationError(
            "Source rows cannot be less than included rows."
        )

    if complete != complete_rows:
        raise ValidationError(
            "Complete row accounting mismatch: "
            f"summary={complete}, result={complete_rows}"
        )

    if hits != hit_rows:
        raise ValidationError(
            "HIT row accounting mismatch: "
            f"summary={hits}, result={hit_rows}"
        )

    log(
        "[PASS] Source row accounting: "
        f"source={source_rows}, included={included_rows}"
    )

    log(
        "[PASS] Complete/HIT accounting: "
        f"complete={complete}, hits={hits}"
    )


def verify_horizon_consistency(summaries: dict) -> None:
    """
    Horizon checks are descriptive. Complete counts
    can decrease as longer horizons become censored.
    """

    for tf in TIMEFRAMES:
        for classification in CLASSIFICATIONS:
            previous_total = None
            previous_complete = None

            for horizon in HORIZONS:
                item = summaries[
                    (tf, horizon, classification)
                ]

                total = item["total_rows"]
                complete = item["complete_rows"]

                if previous_total is not None:
                    if total != previous_total:
                        raise ValidationError(
                            "Total count differs between horizons: "
                            f"{tf}/{classification}"
                        )

                    if complete > previous_complete:
                        raise ValidationError(
                            "Complete count increases with horizon: "
                            f"{tf}/{classification}"
                        )

                previous_total = total
                previous_complete = complete


def group_quality(summary: dict) -> tuple:
    complete = summary["complete_rows"]
    hits = summary["hit_count"]
    total = summary["total_rows"]
    censored = summary["censored_rows"]

    censored_pct = pct(censored, total)

    warnings = []

    if complete < MIN_COMPLETE_ROWS:
        warnings.append("SMALL_COMPLETE_SAMPLE")

    if hits < MIN_HIT_ROWS:
        warnings.append("FEW_TARGET_HITS")

    sample_warning = (
        "|".join(warnings)
        if warnings
        else "NONE"
    )

    censoring_warning = (
        "HIGH_CENSORING"
        if censored_pct > MAX_CENSORED_PCT
        else "NONE"
    )

    return (
        sample_warning,
        censoring_warning,
        censored_pct,
    )


def build_groups(
    summaries: dict,
    stops: dict,
) -> list[dict[str, Any]]:
    rows = []

    for tf in TIMEFRAMES:
        for horizon in HORIZONS:
            for classification in CLASSIFICATIONS:
                summary = summaries[
                    (tf, horizon, classification)
                ]

                (
                    sample_warning,
                    censoring_warning,
                    censored_pct,
                ) = group_quality(summary)

                for stop_level in STOP_LEVELS:
                    stop = stops[
                        (
                            tf,
                            horizon,
                            classification,
                            stop_level,
                        )
                    ]

                    target_lower = stop[
                        "target_first_lower_bound_pct"
                    ]

                    target_upper = stop[
                        "target_first_upper_bound_pct"
                    ]

                    stop_lower = stop[
                        "stop_first_lower_bound_pct"
                    ]

                    stop_upper = stop[
                        "stop_first_upper_bound_pct"
                    ]

                    conservative = (
                        target_lower - stop_upper
                    )

                    optimistic = (
                        target_upper - stop_lower
                    )

                    rows.append({
                        "timeframe": tf,
                        "horizon": horizon,
                        "classification": classification,
                        "stop_loss_pct": stop_level,
                        "total_rows": summary["total_rows"],
                        "complete_rows": summary["complete_rows"],
                        "censored_rows": summary["censored_rows"],
                        "censored_pct": rnd(censored_pct),
                        "markets": summary["markets"],
                        "hit_count": summary["hit_count"],
                        "hit_rate_pct": rnd(
                            summary["hit_rate_pct"]
                        ),
                        "stop_touched_count": stop[
                            "stop_touched_count"
                        ],
                        "stop_touched_pct": rnd(
                            stop["stop_touched_pct"]
                        ),
                        "target_without_stop_touch": stop[
                            "target_without_stop_touch"
                        ],
                        "target_and_stop_both_touched": stop[
                            "target_and_stop_both_touched"
                        ],
                        "ambiguous_order_pct": rnd(
                            pct(
                                stop["ambiguous_order_count"],
                                stop["complete_rows"],
                            )
                        ),
                        "target_first_lower_bound_pct": rnd(
                            target_lower
                        ),
                        "target_first_upper_bound_pct": rnd(
                            target_upper
                        ),
                        "stop_first_lower_bound_pct": rnd(
                            stop_lower
                        ),
                        "stop_first_upper_bound_pct": rnd(
                            stop_upper
                        ),
                        "conservative_target_minus_stop_pp": rnd(
                            conservative
                        ),
                        "optimistic_target_minus_stop_pp": rnd(
                            optimistic
                        ),
                        "sample_warning": sample_warning,
                        "censoring_warning": censoring_warning,
                        "interpretation": (
                            "DESCRIPTIVE_BARRIER_BOUNDS_ONLY"
                        ),
                    })

    return rows


def compare_groups(
    groups: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    indexed = {
        (
            row["timeframe"],
            row["horizon"],
            row["classification"],
            row["stop_loss_pct"],
        ): row
        for row in groups
    }

    comparisons = []

    for tf in TIMEFRAMES:
        for horizon in HORIZONS:
            for stop_level in STOP_LEVELS:
                only = indexed[
                    (
                        tf,
                        horizon,
                        "PATTERN_256_ONLY",
                        stop_level,
                    )
                ]

                both = indexed[
                    (
                        tf,
                        horizon,
                        "BOTH",
                        stop_level,
                    )
                ]

                hit_diff = (
                    both["hit_rate_pct"]
                    - only["hit_rate_pct"]
                )

                stop_diff = (
                    both["stop_touched_pct"]
                    - only["stop_touched_pct"]
                )

                target_lower_diff = (
                    both["target_first_lower_bound_pct"]
                    - only["target_first_lower_bound_pct"]
                )

                stop_upper_diff = (
                    both["stop_first_upper_bound_pct"]
                    - only["stop_first_upper_bound_pct"]
                )

                margin_diff = (
                    both["conservative_target_minus_stop_pp"]
                    - only["conservative_target_minus_stop_pp"]
                )

                sample_warnings = set()
                censor_warnings = set()

                for group in (only, both):
                    if group["sample_warning"] != "NONE":
                        sample_warnings.update(
                            group["sample_warning"].split("|")
                        )

                    if group["censoring_warning"] != "NONE":
                        censor_warnings.update(
                            group["censoring_warning"].split("|")
                        )

                if hit_diff > 0 and stop_diff < 0:
                    direction = (
                        "BOTH_HIGHER_HIT_LOWER_STOP_TOUCH"
                    )
                elif hit_diff > 0 and stop_diff > 0:
                    direction = (
                        "BOTH_HIGHER_HIT_HIGHER_STOP_TOUCH"
                    )
                elif hit_diff < 0 and stop_diff < 0:
                    direction = (
                        "BOTH_LOWER_HIT_LOWER_STOP_TOUCH"
                    )
                elif hit_diff < 0 and stop_diff > 0:
                    direction = (
                        "BOTH_LOWER_HIT_HIGHER_STOP_TOUCH"
                    )
                else:
                    direction = "MIXED_OR_EQUAL"

                comparisons.append({
                    "timeframe": tf,
                    "horizon": horizon,
                    "stop_loss_pct": stop_level,
                    "only_complete_rows": only[
                        "complete_rows"
                    ],
                    "both_complete_rows": both[
                        "complete_rows"
                    ],
                    "only_hit_rate_pct": only[
                        "hit_rate_pct"
                    ],
                    "both_hit_rate_pct": both[
                        "hit_rate_pct"
                    ],
                    "hit_rate_difference_pp": rnd(
                        hit_diff
                    ),
                    "only_stop_touched_pct": only[
                        "stop_touched_pct"
                    ],
                    "both_stop_touched_pct": both[
                        "stop_touched_pct"
                    ],
                    "stop_touch_difference_pp": rnd(
                        stop_diff
                    ),
                    "only_target_first_lower_pct": only[
                        "target_first_lower_bound_pct"
                    ],
                    "both_target_first_lower_pct": both[
                        "target_first_lower_bound_pct"
                    ],
                    "target_lower_difference_pp": rnd(
                        target_lower_diff
                    ),
                    "only_stop_first_upper_pct": only[
                        "stop_first_upper_bound_pct"
                    ],
                    "both_stop_first_upper_pct": both[
                        "stop_first_upper_bound_pct"
                    ],
                    "stop_upper_difference_pp": rnd(
                        stop_upper_diff
                    ),
                    "only_conservative_margin_pp": only[
                        "conservative_target_minus_stop_pp"
                    ],
                    "both_conservative_margin_pp": both[
                        "conservative_target_minus_stop_pp"
                    ],
                    "conservative_margin_difference_pp": rnd(
                        margin_diff
                    ),
                    "only_ambiguous_count": only[
                        "target_and_stop_both_touched"
                    ],
                    "both_ambiguous_count": both[
                        "target_and_stop_both_touched"
                    ],
                    "only_censored_pct": only[
                        "censored_pct"
                    ],
                    "both_censored_pct": both[
                        "censored_pct"
                    ],
                    "sample_warning": (
                        "|".join(sorted(sample_warnings))
                        if sample_warnings
                        else "NONE"
                    ),
                    "censoring_warning": (
                        "|".join(sorted(censor_warnings))
                        if censor_warnings
                        else "NONE"
                    ),
                    "descriptive_direction": direction,
                    "interpretation": (
                        "NOT_A_TRADE_PERFORMANCE_ESTIMATE"
                    ),
                })

    return comparisons


def atomic_write(path: Path, payload: bytes) -> None:
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    descriptor, temporary = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=str(path.parent),
    )

    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())

        os.replace(temporary, path)

    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def write_csv(
    path: Path,
    fields: list[str],
    rows: list[dict[str, Any]],
) -> None:
    buffer = io.StringIO(newline="")

    writer = csv.DictWriter(
        buffer,
        fieldnames=fields,
        extrasaction="raise",
        lineterminator="\n",
    )

    writer.writeheader()
    writer.writerows(rows)

    atomic_write(
        path,
        buffer.getvalue().encode("utf-8"),
    )


def write_json(
    path: Path,
    value: dict[str, Any],
) -> None:
    payload = (
        json.dumps(
            value,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")

    atomic_write(path, payload)


def verify_hashes(
    paths: dict[str, Path],
    expected: dict[str, str],
) -> None:
    for name, path in paths.items():
        if sha256_file(path) != expected[name]:
            raise ValidationError(
                f"Upstream input changed: {name}"
            )

    log("[PASS] Upstream input hashes unchanged")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Pattern 256 risk-adjusted research validation."
        )
    )

    parser.add_argument(
        "--root",
        default=".",
        help="Project root directory.",
    )

    parser.add_argument(
        "--output-dir",
        default=None,
        help="Optional isolated output directory.",
    )

    return parser.parse_args()


def main() -> int:
    args = parse_args()

    root = Path(args.root).resolve()
    source_dir = (root / SOURCE_DIR).resolve()

    output_dir = (
        Path(args.output_dir).resolve()
        if args.output_dir
        else (root / OUTPUT_DIR).resolve()
    )

    log("=" * 68)
    log(f"{PROGRAM} / {VERSION}")
    log("=" * 68)

    if not source_dir.is_dir():
        raise ValidationError(
            f"Source directory missing: {source_dir}"
        )

    if (
        output_dir == source_dir
        or source_dir in output_dir.parents
        or output_dir in source_dir.parents
    ):
        raise ValidationError(
            "Output directory overlaps source directory."
        )

    paths = {
        SUMMARY_FILE: source_dir / SUMMARY_FILE,
        STOP_FILE: source_dir / STOP_FILE,
        SOURCE_RESULT_FILE: (
            source_dir / SOURCE_RESULT_FILE
        ),
        SOURCE_MANIFEST_FILE: (
            source_dir / SOURCE_MANIFEST_FILE
        ),
    }

    for path in paths.values():
        require_file(path)

    log("[CHECK] Capturing input SHA256")

    input_hashes = {
        name: sha256_file(path)
        for name, path in paths.items()
    }

    log("[CHECK] Reading upstream metadata")

    source_result = load_json(
        paths[SOURCE_RESULT_FILE]
    )

    source_manifest = load_json(
        paths[SOURCE_MANIFEST_FILE]
    )

    validate_source_metadata(
        source_result,
        source_manifest,
        input_hashes,
    )

    log("[PASS] Upstream metadata and hashes")

    log("[CHECK] Reading entry risk summary")

    raw_summary = read_csv(
        paths[SUMMARY_FILE],
        SUMMARY_REQUIRED,
    )

    summaries = parse_summary(raw_summary)

    log(f"[PASS] Summary groups: {len(summaries)}")

    log("[CHECK] Reading stop scenarios")

    raw_stops = read_csv(
        paths[STOP_FILE],
        STOP_REQUIRED,
    )

    stops = parse_stops(
        raw_stops,
        summaries,
    )

    log(f"[PASS] Stop scenario groups: {len(stops)}")

    log("[CHECK] Cross-report accounting")

    verify_result_counts(
        source_result,
        summaries,
        stops,
    )

    verify_horizon_consistency(summaries)

    log("[PASS] Cross-report accounting")

    log("[ANALYZE] Building risk-adjusted groups")

    group_rows = build_groups(
        summaries,
        stops,
    )

    log("[ANALYZE] Comparing classifications")

    comparison_rows = compare_groups(
        group_rows
    )

    if len(group_rows) != 72:
        raise ValidationError(
            "Expected 72 group rows."
        )

    if len(comparison_rows) != 36:
        raise ValidationError(
            "Expected 36 comparison rows."
        )

    verify_hashes(paths, input_hashes)

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    groups_path = output_dir / GROUPS_FILE
    comparison_path = output_dir / COMPARISON_FILE
    result_path = output_dir / RESULT_FILE
    manifest_path = output_dir / MANIFEST_FILE

    log("[SAVE] Writing reports")

    write_csv(
        groups_path,
        GROUP_FIELDS,
        group_rows,
    )

    write_csv(
        comparison_path,
        COMPARISON_FIELDS,
        comparison_rows,
    )

    groups_hash = sha256_file(groups_path)
    comparison_hash = sha256_file(comparison_path)

    sample_warning_rows = sum(
        row["sample_warning"] != "NONE"
        for row in comparison_rows
    )

    censoring_warning_rows = sum(
        row["censoring_warning"] != "NONE"
        for row in comparison_rows
    )

    direction_counts = {
        direction: sum(
            row["descriptive_direction"] == direction
            for row in comparison_rows
        )
        for direction in sorted({
            row["descriptive_direction"]
            for row in comparison_rows
        })
    }

    result = {
        "status": "PASS",
        "program": PROGRAM,
        "version": VERSION,
        "generated_at_utc": utc_now(),
        "source_program": source_result["program"],
        "source_version": source_result["version"],
        "source_rows": source_result["source_rows"],
        "included_rows": source_result["included_rows"],
        "complete_rows": source_result["complete_rows"],
        "hit_rows": source_result["hit_rows"],
        "summary_source_rows": len(raw_summary),
        "stop_source_rows": len(raw_stops),
        "group_rows": len(group_rows),
        "comparison_rows": len(comparison_rows),
        "sample_warning_rows": sample_warning_rows,
        "censoring_warning_rows": censoring_warning_rows,
        "descriptive_direction_counts": direction_counts,
        "risk_adjusted_groups_sha256": groups_hash,
        "risk_adjusted_comparison_sha256": comparison_hash,
        "methodology": {
            "target_return_pct": 30,
            "timeframes": list(TIMEFRAMES),
            "horizons_bars": list(HORIZONS),
            "stop_levels_pct": list(STOP_LEVELS),
            "comparison": (
                "BOTH minus PATTERN_256_ONLY"
            ),
            "hit_rate_denominator": (
                "complete HIT/MISS observations"
            ),
            "stop_touch_denominator": (
                "complete HIT/MISS observations"
            ),
            "barrier_order": (
                "Unknown when target and stop both touch"
            ),
            "conservative_margin": (
                "target-first lower percentage minus "
                "stop-first upper percentage"
            ),
            "margin_is_trading_return": False,
            "statistical_significance_tested": False,
            "overlapping_signals_independent": False,
            "intrabar_order_resolved": False,
            "out_of_sample_verified": False,
        },
        "assessment": (
            "DESCRIPTIVE_RISK_COMPARISON_ONLY"
        ),
        "warnings": [
            "Overlapping signals are not independent.",
            "Multiple horizons reuse underlying signals.",
            "Multiple stop levels reuse observations.",
            "Target/stop touch ordering is unresolved.",
            "Stop touch is not confirmed stop execution.",
            "Small samples may distort comparisons.",
            "Censored observations affect estimates.",
            "Fees and slippage are not included.",
            "No realized trading PnL is calculated.",
            "No out-of-sample validation in this stage.",
        ],
    }

    write_json(result_path, result)

    manifest = {
        "status": "PASS",
        "program": PROGRAM,
        "version": VERSION,
        "generated_at_utc": utc_now(),
        "inputs": dict(sorted(input_hashes.items())),
        "outputs": {
            GROUPS_FILE: groups_hash,
            COMPARISON_FILE: comparison_hash,
            RESULT_FILE: sha256_file(result_path),
        },
    }

    write_json(manifest_path, manifest)

    if sha256_file(groups_path) != groups_hash:
        raise ValidationError(
            "Group output hash mismatch."
        )

    if sha256_file(
        comparison_path
    ) != comparison_hash:
        raise ValidationError(
            "Comparison output hash mismatch."
        )

    if (
        sha256_file(result_path)
        != manifest["outputs"][RESULT_FILE]
    ):
        raise ValidationError(
            "Result output hash mismatch."
        )

    verify_hashes(paths, input_hashes)

    log("=" * 68)
    log("[PASS] Risk-adjusted validation completed")
    log(f"Source rows: {source_result['source_rows']}")
    log(f"Included rows: {source_result['included_rows']}")
    log(f"Complete rows: {source_result['complete_rows']}")
    log(f"HIT rows: {source_result['hit_rows']}")
    log(f"Group rows: {len(group_rows)}")
    log(f"Comparison rows: {len(comparison_rows)}")
    log(f"Sample warnings: {sample_warning_rows}")
    log(f"Censoring warnings: {censoring_warning_rows}")
    log(f"Output: {output_dir}")
    log("=" * 68)

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())

    except (
        ValidationError,
        OSError,
        ValueError,
        KeyError,
        json.JSONDecodeError,
    ) as exc:
        log(f"[FAIL] {exc}")
        sys.exit(1)

    except Exception:
        log("[FAIL] Unexpected error")
        traceback.print_exc()
        sys.exit(1)
