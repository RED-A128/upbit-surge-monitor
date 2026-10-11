
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
validate_pattern_256_accumulation_risk_adjusted.py
Clean V001

Purpose
-------
Compare PATTERN_256_ONLY and BOTH using existing entry-risk
summary and stop-scenario reports.

Safety
------
- Read-only upstream inputs.
- No OHLCV modifications.
- No trading or exchange API calls.
- No future data used to generate new signals.
- No assumption that target was reached before stop.
- Separate output directory.
- SHA256 verification before and after analysis.
- Atomic output replacement.
- Standard library only.

Important
---------
This is a descriptive research validation, not a trading
backtest or a proof of profitability.

The source report rows may contain overlapping signals.
Consequently, the row counts must not be interpreted as
independent trade counts.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import sys
import tempfile
import traceback

from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PROGRAM = "validate_pattern_256_accumulation_risk_adjusted.py"
VERSION = "Clean V001"

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

COMPARISON_FILE = "risk_adjusted_comparison.csv"
GROUPS_FILE = "risk_adjusted_groups.csv"
RESULT_FILE = "risk_adjusted_result.json"
MANIFEST_FILE = "risk_adjusted_manifest.json"

TIMEFRAMES = ("h1", "h4", "d1")
HORIZONS = (24, 72, 168)

CLASSIFICATIONS = (
    "PATTERN_256_ONLY",
    "BOTH",
)

STOP_LEVELS = (5, 10, 15, 20)

MIN_COMPLETE_ROWS = 100
MIN_HIT_ROWS = 30

# These are research-quality flags, not statistical
# significance thresholds.
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
    """Invalid or inconsistent source report."""


def log(message: str) -> None:
    timestamp = datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    print(
        f"[{timestamp}] {message}",
        flush=True,
    )


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        while True:
            block = handle.read(1024 * 1024)

            if not block:
                break

            digest.update(block)

    return digest.hexdigest()


def utc_now() -> str:
    return datetime.now(
        timezone.utc
    ).isoformat()


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
            raise ValueError("empty value")

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


def to_float(
    value: Any,
    field: str,
) -> float:
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


def percentage(
    numerator: float,
    denominator: float,
) -> float:
    if denominator <= 0:
        return 0.0

    return numerator * 100.0 / denominator


def rounded(value: float) -> float:
    return round(value, 6)


def close_enough(
    actual: float,
    expected: float,
    tolerance: float = 0.0001,
) -> bool:
    return abs(actual - expected) <= tolerance


def validate_dimensions(
    timeframe: str,
    horizon: int,
    classification: str,
) -> None:
    if timeframe not in TIMEFRAMES:
        raise ValidationError(
            f"Unknown timeframe: {timeframe}"
        )

    if horizon not in HORIZONS:
        raise ValidationError(
            f"Unknown horizon: {horizon}"
        )

    if classification not in CLASSIFICATIONS:
        raise ValidationError(
            f"Unknown classification: {classification}"
        )


def read_csv(
    path: Path,
    required_columns: tuple[str, ...],
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
            column
            for column in required_columns
            if column not in reader.fieldnames
        ]

        if missing:
            raise ValidationError(
                f"Missing columns in {path.name}: "
                + ", ".join(missing)
            )

        rows = []

        for line_number, row in enumerate(
            reader,
            start=2,
        ):
            if None in row:
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
        value = json.load(handle)

    if not isinstance(value, dict):
        raise ValidationError(
            f"JSON object expected: {path}"
        )

    return value


def validate_source_metadata(
    result: dict[str, Any],
    manifest: dict[str, Any],
    input_hashes: dict[str, str],
) -> None:
    if result.get("status") != "PASS":
        raise ValidationError(
            "Upstream entry risk result is not PASS."
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

    if manifest.get("program") != result.get("program"):
        raise ValidationError(
            "Upstream manifest program mismatch."
        )

    if manifest.get("version") != result.get("version"):
        raise ValidationError(
            "Upstream manifest version mismatch."
        )

    outputs = manifest.get("outputs")

    if not isinstance(outputs, dict):
        raise ValidationError(
            "Upstream manifest outputs missing."
        )

    for filename, result_key in (
        (
            SUMMARY_FILE,
            "entry_risk_summary_sha256",
        ),
        (
            STOP_FILE,
            "entry_risk_stop_scenarios_sha256",
        ),
    ):
        actual = input_hashes[filename]

        expected_manifest = str(
            outputs.get(filename, "")
        ).lower()

        expected_result = str(
            result.get(result_key, "")
        ).lower()

        if actual != expected_manifest:
            raise ValidationError(
                f"Upstream manifest hash mismatch: "
                f"{filename}"
            )

        if actual != expected_result:
            raise ValidationError(
                f"Upstream result hash mismatch: "
                f"{filename}"
            )


def parse_summary(
    raw_rows: list[dict[str, str]],
) -> dict[tuple[str, int, str], dict[str, Any]]:

    parsed = {}

    for row in raw_rows:
        timeframe = row["timeframe"].strip()
        horizon = to_int(
            row["horizon"],
            "horizon",
        )
        classification = row[
            "classification"
        ].strip()

        validate_dimensions(
            timeframe,
            horizon,
            classification,
        )

        key = (
            timeframe,
            horizon,
            classification,
        )

        if key in parsed:
            raise ValidationError(
                f"Duplicate summary key: {key}"
            )

        item = {
            "timeframe": timeframe,
            "horizon": horizon,
            "classification": classification,
        }

        for field in (
            "total_rows",
            "complete_rows",
            "censored_rows",
            "invalid_rows",
            "other_status_rows",
            "markets",
            "hit_count",
            "miss_count",
        ):
            item[field] = to_int(
                row[field],
                field,
            )

        item["hit_rate_pct"] = to_float(
            row["hit_rate_complete_pct"],
            "hit_rate_complete_pct",
        )

        if (
            item["total_rows"]
            != item["complete_rows"]
            + item["censored_rows"]
            + item["invalid_rows"]
            + item["other_status_rows"]
        ):
            raise ValidationError(
                f"Summary accounting mismatch: {key}"
            )

        if (
            item["complete_rows"]
            != item["hit_count"]
            + item["miss_count"]
        ):
            raise ValidationError(
                f"HIT/MISS accounting mismatch: {key}"
            )

        calculated_rate = percentage(
            item["hit_count"],
            item["complete_rows"],
        )

        if not close_enough(
            item["hit_rate_pct"],
            calculated_rate,
        ):
            raise ValidationError(
                f"Summary HIT rate mismatch: {key}"
            )

        parsed[key] = item

    expected_keys = {
        (timeframe, horizon, classification)
        for timeframe in TIMEFRAMES
        for horizon in HORIZONS
        for classification in CLASSIFICATIONS
    }

    if set(parsed) != expected_keys:
        missing = expected_keys - set(parsed)

        raise ValidationError(
            f"Summary groups incomplete: {sorted(missing)}"
        )

    return parsed


def parse_stops(
    raw_rows: list[dict[str, str]],
    summaries: dict,
) -> dict[tuple[str, int, str, int], dict[str, Any]]:

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
        timeframe = row["timeframe"].strip()
        horizon = to_int(
            row["horizon"],
            "horizon",
        )
        classification = row[
            "classification"
        ].strip()

        validate_dimensions(
            timeframe,
            horizon,
            classification,
        )

        stop_float = to_float(
            row["stop_loss_pct"],
            "stop_loss_pct",
        )

        stop_level = int(stop_float)

        if (
            stop_float != stop_level
            or stop_level not in STOP_LEVELS
        ):
            raise ValidationError(
                f"Unexpected stop level: {stop_float}"
            )

        key = (
            timeframe,
            horizon,
            classification,
            stop_level,
        )

        if key in parsed:
            raise ValidationError(
                f"Duplicate stop key: {key}"
            )

        summary_key = key[:3]

        if summary_key not in summaries:
            raise ValidationError(
                f"Missing summary: {summary_key}"
            )

        item = {
            "timeframe": timeframe,
            "horizon": horizon,
            "classification": classification,
            "stop_loss_pct": stop_level,
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
        hit = item["target_hit_count"]
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

        if complete != summaries[
            summary_key
        ]["complete_rows"]:
            raise ValidationError(
                f"Stop/summary complete mismatch: {key}"
            )

        if hit != summaries[
            summary_key
        ]["hit_count"]:
            raise ValidationError(
                f"Stop/summary HIT mismatch: {key}"
            )

        if complete != (
            target_only
            + both
            + stop_only
            + neither
        ):
            raise ValidationError(
                f"Four-category accounting mismatch: {key}"
            )

        if hit != target_only + both:
            raise ValidationError(
                f"Target accounting mismatch: {key}"
            )

        if touched != stop_only + both:
            raise ValidationError(
                f"Stop accounting mismatch: {key}"
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
                f"Order classification mismatch: {key}"
            )

        calculated_rates = {
            "stop_touched_pct": percentage(
                touched,
                complete,
            ),
            "target_first_lower_bound_pct": percentage(
                target_only,
                complete,
            ),
            "target_first_upper_bound_pct": percentage(
                target_only + both,
                complete,
            ),
            "stop_first_lower_bound_pct": percentage(
                stop_only,
                complete,
            ),
            "stop_first_upper_bound_pct": percentage(
                stop_only + both,
                complete,
            ),
        }

        for field, expected in calculated_rates.items():
            if not close_enough(
                item[field],
                expected,
            ):
                raise ValidationError(
                    f"Rate mismatch {field}: {key}"
                )

        parsed[key] = item

    expected_keys = {
        (
            timeframe,
            horizon,
            classification,
            stop_level,
        )
        for timeframe in TIMEFRAMES
        for horizon in HORIZONS
        for classification in CLASSIFICATIONS
        for stop_level in STOP_LEVELS
    }

    if set(parsed) != expected_keys:
        missing = expected_keys - set(parsed)

        raise ValidationError(
            f"Stop groups incomplete: {sorted(missing)}"
        )

    return parsed


def group_quality(
    summary: dict[str, Any],
) -> tuple[str, str, float]:

    complete = summary["complete_rows"]
    hits = summary["hit_count"]
    total = summary["total_rows"]
    censored = summary["censored_rows"]

    censored_pct = percentage(
        censored,
        total,
    )

    sample_warnings = []

    if complete < MIN_COMPLETE_ROWS:
        sample_warnings.append(
            "SMALL_COMPLETE_SAMPLE"
        )

    if hits < MIN_HIT_ROWS:
        sample_warnings.append(
            "FEW_TARGET_HITS"
        )

    sample_warning = (
        "|".join(sample_warnings)
        if sample_warnings
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


def build_group_rows(
    summaries: dict,
    stops: dict,
) -> list[dict[str, Any]]:

    output = []

    for timeframe in TIMEFRAMES:
        for horizon in HORIZONS:
            for classification in CLASSIFICATIONS:
                summary_key = (
                    timeframe,
                    horizon,
                    classification,
                )

                summary = summaries[summary_key]

                (
                    sample_warning,
                    censoring_warning,
                    censored_pct,
                ) = group_quality(summary)

                for stop_level in STOP_LEVELS:
                    stop = stops[
                        (
                            timeframe,
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

                    # Descriptive percentage-point margins only.
                    # These are NOT expected trading returns.
                    conservative_margin = (
                        target_lower - stop_upper
                    )

                    optimistic_margin = (
                        target_upper - stop_lower
                    )

                    output.append({
                        "timeframe": timeframe,
                        "horizon": horizon,
                        "classification": classification,
                        "stop_loss_pct": stop_level,
                        "total_rows": summary["total_rows"],
                        "complete_rows": summary["complete_rows"],
                        "censored_rows": summary["censored_rows"],
                        "censored_pct": rounded(censored_pct),
                        "markets": summary["markets"],
                        "hit_count": summary["hit_count"],
                        "hit_rate_pct": rounded(
                            summary["hit_rate_pct"]
                        ),
                        "stop_touched_count": stop[
                            "stop_touched_count"
                        ],
                        "stop_touched_pct": rounded(
                            stop["stop_touched_pct"]
                        ),
                        "target_without_stop_touch": stop[
                            "target_without_stop_touch"
                        ],
                        "target_and_stop_both_touched": stop[
                            "target_and_stop_both_touched"
                        ],
                        "ambiguous_order_pct": rounded(
                            percentage(
                                stop["ambiguous_order_count"],
                                stop["complete_rows"],
                            )
                        ),
                        "target_first_lower_bound_pct": rounded(
                            target_lower
                        ),
                        "target_first_upper_bound_pct": rounded(
                            target_upper
                        ),
                        "stop_first_lower_bound_pct": rounded(
                            stop_lower
                        ),
                        "stop_first_upper_bound_pct": rounded(
                            stop_upper
                        ),
                        "conservative_target_minus_stop_pp": rounded(
                            conservative_margin
                        ),
                        "optimistic_target_minus_stop_pp": rounded(
                            optimistic_margin
                        ),
                        "sample_warning": sample_warning,
                        "censoring_warning": censoring_warning,
                        "interpretation": (
                            "DESCRIPTIVE_BARRIER_BOUNDS_ONLY"
                        ),
                    })

    return output


def compare_groups(
    group_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:

    indexed = {
        (
            row["timeframe"],
            row["horizon"],
            row["classification"],
            row["stop_loss_pct"],
        ): row
        for row in group_rows
    }

    output = []

    for timeframe in TIMEFRAMES:
        for horizon in HORIZONS:
            for stop_level in STOP_LEVELS:

                only = indexed[
                    (
                        timeframe,
                        horizon,
                        "PATTERN_256_ONLY",
                        stop_level,
                    )
                ]

                both = indexed[
                    (
                        timeframe,
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

                warnings = []

                for group in (only, both):
                    if group["sample_warning"] != "NONE":
                        warnings.append(
                            group["sample_warning"]
                        )

                censoring_warnings = []

                for group in (only, both):
                    if group["censoring_warning"] != "NONE":
                        censoring_warnings.append(
                            group["censoring_warning"]
                        )

                sample_warning = (
                    "|".join(
                        sorted(
                            set(
                                "|".join(warnings).split("|")
                            )
                        )
                    )
                    if warnings
                    else "NONE"
                )

                censoring_warning = (
                    "|".join(
                        sorted(set(censoring_warnings))
                    )
                    if censoring_warnings
                    else "NONE"
                )

                # This label is intentionally descriptive.
                # It is NOT a recommendation to trade.
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

                output.append({
                    "timeframe": timeframe,
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
                    "hit_rate_difference_pp": rounded(
                        hit_diff
                    ),
                    "only_stop_touched_pct": only[
                        "stop_touched_pct"
                    ],
                    "both_stop_touched_pct": both[
                        "stop_touched_pct"
                    ],
                    "stop_touch_difference_pp": rounded(
                        stop_diff
                    ),
                    "only_target_first_lower_pct": only[
                        "target_first_lower_bound_pct"
                    ],
                    "both_target_first_lower_pct": both[
                        "target_first_lower_bound_pct"
                    ],
                    "target_lower_difference_pp": rounded(
                        target_lower_diff
                    ),
                    "only_stop_first_upper_pct": only[
                        "stop_first_upper_bound_pct"
                    ],
                    "both_stop_first_upper_pct": both[
                        "stop_first_upper_bound_pct"
                    ],
                    "stop_upper_difference_pp": rounded(
                        stop_upper_diff
                    ),
                    "only_conservative_margin_pp": only[
                        "conservative_target_minus_stop_pp"
                    ],
                    "both_conservative_margin_pp": both[
                        "conservative_target_minus_stop_pp"
                    ],
                    "conservative_margin_difference_pp": rounded(
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
                    "sample_warning": sample_warning,
                    "censoring_warning": censoring_warning,
                    "descriptive_direction": direction,
                    "interpretation": (
                        "NOT_A_TRADE_PERFORMANCE_ESTIMATE"
                    ),
                })

    return output


def verify_cross_horizon_consistency(
    summaries: dict,
) -> None:
    for timeframe in TIMEFRAMES:
        for classification in CLASSIFICATIONS:

            previous_total = None
            previous_complete = None
            previous_hits = None

            for horizon in HORIZONS:
                item = summaries[
                    (
                        timeframe,
                        horizon,
                        classification,
                    )
                ]

                total = item["total_rows"]
                complete = item["complete_rows"]
                hits = item["hit_count"]

                if previous_total is not None:
                    if total != previous_total:
                        raise ValidationError(
                            "Total signal count changed "
                            "between horizons: "
                            f"{timeframe}/{classification}"
                        )

                    if complete > previous_complete:
                        raise ValidationError(
                            "Complete count unexpectedly "
                            "increased with horizon: "
                            f"{timeframe}/{classification}"
                        )

                    if hits < previous_hits:
                        raise ValidationError(
                            "HIT count decreased with horizon: "
                            f"{timeframe}/{classification}"
                        )

                previous_total = total
                previous_complete = complete
                previous_hits = hits


def verify_result_counts(
    source_result: dict[str, Any],
    summaries: dict,
    stops: dict,
) -> None:

    if len(summaries) != 18:
        raise ValidationError(
            "Expected 18 summary groups."
        )

    if len(stops) != 72:
        raise ValidationError(
            "Expected 72 stop groups."
        )

    if to_int(
        source_result.get("summary_rows"),
        "summary_rows",
    ) != len(summaries):
        raise ValidationError(
            "Source summary row count mismatch."
        )

    if to_int(
        source_result.get("stop_scenario_rows"),
        "stop_scenario_rows",
    ) != len(stops):
        raise ValidationError(
            "Source stop row count mismatch."
        )

    # Each timeframe/classification signal is represented
    # once per horizon. Therefore source_rows is checked
    # using only the 24-bar groups, not all 18 groups.
    total_source = sum(
        summaries[
            (timeframe, 24, classification)
        ]["total_rows"]
        for timeframe in TIMEFRAMES
        for classification in CLASSIFICATIONS
    )

    if total_source != to_int(
        source_result.get("source_rows"),
        "source_rows",
    ):
        raise ValidationError(
            "Source row accounting mismatch."
        )

    # The source program's included_rows is the sum of
    # all horizon-specific group rows.
    included = sum(
        item["total_rows"]
        for item in summaries.values()
    )

    if included != to_int(
        source_result.get("included_rows"),
        "included_rows",
    ):
        raise ValidationError(
            "Included row accounting mismatch."
        )

    complete = sum(
        item["complete_rows"]
        for item in summaries.values()
    )

    hits = sum(
        item["hit_count"]
        for item in summaries.values()
    )

    if complete != to_int(
        source_result.get("complete_rows"),
        "complete_rows",
    ):
        raise ValidationError(
            "Complete row accounting mismatch."
        )

    if hits != to_int(
        source_result.get("hit_rows"),
        "hit_rows",
    ):
        raise ValidationError(
            "HIT row accounting mismatch."
        )


def atomic_write_bytes(
    path: Path,
    payload: bytes,
) -> None:
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
        with os.fdopen(
            descriptor,
            "wb",
        ) as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())

        os.replace(
            temporary,
            path,
        )

    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def write_json(
    path: Path,
    data: dict[str, Any],
) -> None:
    payload = (
        json.dumps(
            data,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")

    atomic_write_bytes(
        path,
        payload,
    )


def write_csv(
    path: Path,
    fieldnames: list[str],
    rows: list[dict[str, Any]],
) -> None:
    import io

    buffer = io.StringIO(
        newline="",
    )

    writer = csv.DictWriter(
        buffer,
        fieldnames=fieldnames,
        extrasaction="raise",
        lineterminator="\n",
    )

    writer.writeheader()

    for row in rows:
        writer.writerow(row)

    atomic_write_bytes(
        path,
        buffer.getvalue().encode("utf-8"),
    )


def verify_input_hashes(
    paths: dict[str, Path],
    expected: dict[str, str],
) -> None:
    for filename, path in paths.items():
        actual = sha256_file(path)

        if actual != expected[filename]:
            raise ValidationError(
                f"Upstream input changed: {filename}"
            )

    log("[PASS] Upstream input hashes unchanged")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Validate risk-adjusted descriptive comparison "
            "for Pattern 256 and accumulation signals."
        )
    )

    parser.add_argument(
        "--root",
        default=".",
        help=(
            "Project root containing data/reports. "
            "Default: current working directory."
        ),
    )

    parser.add_argument(
        "--output-dir",
        default=None,
        help=(
            "Optional separate output directory. "
            "Must not equal an upstream input directory."
        ),
    )

    return parser.parse_args()


def main() -> int:
    args = parse_args()

    root = Path(args.root).resolve()

    source_dir = (
        root / SOURCE_DIR
    ).resolve()

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
            f"Source report directory missing: {source_dir}"
        )

    if (
        output_dir == source_dir
        or source_dir in output_dir.parents
        or output_dir in source_dir.parents
    ):
        raise ValidationError(
            "Output directory must be separate "
            "from upstream input directory."
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
        filename: sha256_file(path)
        for filename, path in paths.items()
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

    summaries = parse_summary(
        raw_summary
    )

    log(
        f"[PASS] Summary groups: {len(summaries)}"
    )

    log("[CHECK] Reading stop scenarios")

    raw_stops = read_csv(
        paths[STOP_FILE],
        STOP_REQUIRED,
    )

    stops = parse_stops(
        raw_stops,
        summaries,
    )

    log(
        f"[PASS] Stop scenario groups: {len(stops)}"
    )

    log("[CHECK] Cross-report accounting")

    verify_cross_horizon_consistency(
        summaries
    )

    verify_result_counts(
        source_result,
        summaries,
        stops,
    )

    log("[PASS] Cross-report accounting")

    log("[ANALYZE] Building group metrics")

    group_rows = build_group_rows(
        summaries,
        stops,
    )

    log("[ANALYZE] Comparing classifications")

    comparison_rows = compare_groups(
        group_rows
    )

    if len(group_rows) != 72:
        raise ValidationError(
            "Expected 72 output group rows."
        )

    if len(comparison_rows) != 36:
        raise ValidationError(
            "Expected 36 comparison rows."
        )

    verify_input_hashes(
        paths,
        input_hashes,
    )

    log("[SAVE] Writing risk-adjusted reports")

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    groups_path = (
        output_dir / GROUPS_FILE
    )

    comparison_path = (
        output_dir / COMPARISON_FILE
    )

    result_path = (
        output_dir / RESULT_FILE
    )

    manifest_path = (
        output_dir / MANIFEST_FILE
    )

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

    group_hash = sha256_file(
        groups_path
    )

    comparison_hash = sha256_file(
        comparison_path
    )

    sample_warning_rows = sum(
        row["sample_warning"] != "NONE"
        for row in comparison_rows
    )

    censoring_warning_rows = sum(
        row["censoring_warning"] != "NONE"
        for row in comparison_rows
    )

    direction_counts = dict(
        sorted(
            (
                direction,
                sum(
                    row["descriptive_direction"]
                    == direction
                    for row in comparison_rows
                ),
            )
            for direction in sorted({
                row["descriptive_direction"]
                for row in comparison_rows
            })
        )
    )

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
        "risk_adjusted_groups_sha256": group_hash,
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
                "Unknown when both target and stop "
                "are touched in the observation window"
            ),
            "conservative_margin": (
                "target-first lower bound percentage "
                "minus stop-first upper bound percentage"
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
            (
                "Overlapping signals and multiple horizons "
                "may cause strong observation dependence."
            ),
            (
                "The same source signals are evaluated "
                "under multiple stop thresholds."
            ),
            (
                "Target and stop touch ordering is not "
                "resolved from summary reports."
            ),
            (
                "A stop touch does not prove that a stop "
                "order executed before a target."
            ),
            (
                "Small samples and censored observations "
                "can distort comparisons."
            ),
            (
                "No fees, slippage, position sizing, "
                "or realized trading PnL are estimated."
            ),
            (
                "No statistically independent out-of-sample "
                "validation is performed in this stage."
            ),
        ],
    }

    write_json(
        result_path,
        result,
    )

    manifest = {
        "status": "PASS",
        "program": PROGRAM,
        "version": VERSION,
        "generated_at_utc": utc_now(),
        "inputs": dict(
            sorted(input_hashes.items())
        ),
        "outputs": {
            GROUPS_FILE: group_hash,
            COMPARISON_FILE: comparison_hash,
            RESULT_FILE: sha256_file(
                result_path
            ),
        },
    }

    write_json(
        manifest_path,
        manifest,
    )

    # Confirm that output hashes match recorded hashes.
    if sha256_file(groups_path) != group_hash:
        raise ValidationError(
            "Group output hash changed."
        )

    if sha256_file(
        comparison_path
    ) != comparison_hash:
        raise ValidationError(
            "Comparison output hash changed."
        )

    if (
        sha256_file(result_path)
        != manifest["outputs"][RESULT_FILE]
    ):
        raise ValidationError(
            "Result output hash changed."
        )

    verify_input_hashes(
        paths,
        input_hashes,
    )

    log("=" * 68)
    log("[PASS] Risk-adjusted comparison completed")
    log(f"Source summary groups: {len(raw_summary)}")
    log(f"Source stop scenarios: {len(raw_stops)}")
    log(f"Output group rows: {len(group_rows)}")
    log(f"Comparison rows: {len(comparison_rows)}")
    log(f"Sample warning rows: {sample_warning_rows}")
    log(
        "Censoring warning rows: "
        f"{censoring_warning_rows}"
    )
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
