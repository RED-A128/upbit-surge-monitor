
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
evaluate_pattern_256_statistical_robustness.py

Clean V003-3

Walk-Forward statistical robustness and market concentration.

Inputs:
    walk_forward_comparison.csv
    walk_forward_by_market.csv
    walk_forward_result.json
    walk_forward_manifest.json

Outputs:
    robustness_comparison.csv
    robustness_market_concentration.csv
    robustness_result.json
    robustness_manifest.json

Principles:
    - Read-only inputs
    - Verify input SHA256
    - Validate fold and market aggregation
    - Wilson 95% confidence intervals
    - Descriptive two-proportion z diagnostics
    - Market concentration diagnostics
    - No independence or causal claims
    - No automated trading
    - No live scanner approval

Statistical warning:
    Signals may remain dependent across markets, horizons,
    and time. Wilson intervals and z tests are descriptive
    diagnostics, not cluster-robust inferential evidence.
"""

import argparse
import csv
import hashlib
import json
import math
import os
import sys

from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import NormalDist


VERSION = "Clean V003-3"

INPUT_RELATIVE = (
    "data/reports/pattern_256_walk_forward"
)

OUTPUT_RELATIVE = (
    "data/reports/pattern_256_statistical_robustness"
)

EXPECTED_SOURCE_SHA256 = (
    "ed4b4e44fcb387ab70eaac465b3eaa87036466298fde344195e25d06ad7996e0"
)

EXPECTED_SOURCE_ROWS = 7_949_826

EXPECTED_INPUT_VERSION = "Clean V003-2"

INPUT_FILES = (
    "walk_forward_comparison.csv",
    "walk_forward_by_market.csv",
    "walk_forward_result.json",
)

FOLDS = (
    "2024_to_2025",
    "2025_to_2026",
)

ROLES = (
    "TRAIN",
    "TEST",
)

TIMEFRAMES = (
    "h1",
    "h4",
    "d1",
)

HORIZONS = (
    24,
    72,
    168,
)

CLASSIFICATIONS = (
    "BASELINE",
    "COMBINED",
)

MIN_CLASSIFICATION_SAMPLE = 100

MIN_MARKETS = 20

MAX_TOP_MARKET_SHARE_PCT = 20.0

MAX_TOP_FIVE_SHARE_PCT = 50.0

COMPARISON_COLUMNS = [
    "fold",
    "role",
    "timeframe",
    "horizon",
    "baseline_completed",
    "baseline_hits",
    "baseline_rate_pct",
    "baseline_ci_low_pct",
    "baseline_ci_high_pct",
    "combined_completed",
    "combined_hits",
    "combined_rate_pct",
    "combined_ci_low_pct",
    "combined_ci_high_pct",
    "difference_pp",
    "z_score_descriptive",
    "p_value_descriptive",
    "baseline_market_count",
    "combined_market_count",
    "baseline_top_market_share_pct",
    "combined_top_market_share_pct",
    "baseline_top_five_share_pct",
    "combined_top_five_share_pct",
    "sample_gate",
    "market_diversity_gate",
    "concentration_gate",
    "observed_improvement",
    "decision",
]

CONCENTRATION_COLUMNS = [
    "fold",
    "role",
    "timeframe",
    "horizon",
    "classification",
    "completed",
    "hits",
    "market_count",
    "top_market",
    "top_market_completed",
    "top_market_share_pct",
    "top_five_share_pct",
    "effective_market_count",
]


def sha256_file(path):
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        for chunk in iter(
            lambda: handle.read(4 * 1024 * 1024),
            b"",
        ):
            digest.update(chunk)

    return digest.hexdigest()


def read_json(path):
    with path.open(
        "r",
        encoding="utf-8",
    ) as handle:
        return json.load(handle)


def read_csv(path):
    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as handle:
        reader = csv.DictReader(handle)

        if not reader.fieldnames:
            raise ValueError(
                "CSV header missing: " + str(path)
            )

        return list(reader)


def atomic_json(path, payload):
    temporary = path.with_suffix(
        path.suffix + ".tmp"
    )

    with temporary.open(
        "w",
        encoding="utf-8",
        newline="\n",
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
        handle.flush()
        os.fsync(handle.fileno())

    os.replace(temporary, path)


def atomic_csv(path, columns, records):
    temporary = path.with_suffix(
        path.suffix + ".tmp"
    )

    with temporary.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=columns,
            extrasaction="raise",
        )

        writer.writeheader()
        writer.writerows(records)

        handle.flush()
        os.fsync(handle.fileno())

    os.replace(temporary, path)


def require_columns(rows, required, label):
    if not rows:
        raise ValueError(
            "Empty input: " + label
        )

    missing = set(required) - set(rows[0])

    if missing:
        raise ValueError(
            "Missing columns in "
            + label
            + ": "
            + ", ".join(sorted(missing))
        )


def integer(value, label):
    try:
        result = int(value)
    except (ValueError, TypeError) as exc:
        raise ValueError(
            "Invalid integer for " + label
        ) from exc

    return result


def finite_float(value, label):
    try:
        result = float(value)
    except (ValueError, TypeError) as exc:
        raise ValueError(
            "Invalid number for " + label
        ) from exc

    if not math.isfinite(result):
        raise ValueError(
            "Non-finite number for " + label
        )

    return result


def percentage(numerator, denominator):
    if denominator <= 0:
        return 0.0

    return round(
        100.0 * numerator / denominator,
        6,
    )


def wilson_interval(hits, completed):
    if completed <= 0:
        return None, None

    z = 1.959963984540054

    proportion = hits / completed

    denominator = 1.0 + z * z / completed

    center = (
        proportion
        + z * z / (2.0 * completed)
    ) / denominator

    margin = (
        z
        * math.sqrt(
            proportion
            * (1.0 - proportion)
            / completed
            + z * z
            / (4.0 * completed * completed)
        )
        / denominator
    )

    return (
        round(
            100.0 * max(0.0, center - margin),
            6,
        ),
        round(
            100.0 * min(1.0, center + margin),
            6,
        ),
    )


def two_proportion_diagnostic(
    baseline_hits,
    baseline_n,
    combined_hits,
    combined_n,
):
    if baseline_n == 0 or combined_n == 0:
        return None, None

    pooled = (
        baseline_hits + combined_hits
    ) / (
        baseline_n + combined_n
    )

    variance = (
        pooled
        * (1.0 - pooled)
        * (
            1.0 / baseline_n
            + 1.0 / combined_n
        )
    )

    if variance <= 0:
        return None, None

    difference = (
        combined_hits / combined_n
        - baseline_hits / baseline_n
    )

    z_score = difference / math.sqrt(
        variance
    )

    p_value = math.erfc(
        abs(z_score) / math.sqrt(2.0)
    )

    return (
        round(z_score, 6),
        round(p_value, 10),
    )


def validate_inputs(input_dir):
    manifest_path = (
        input_dir / "walk_forward_manifest.json"
    )

    if not manifest_path.is_file():
        raise FileNotFoundError(
            manifest_path
        )

    manifest = read_json(
        manifest_path
    )

    if (
        manifest.get("version")
        != EXPECTED_INPUT_VERSION
    ):
        raise ValueError(
            "Input manifest version mismatch"
        )

    if (
        manifest.get("source_sha256")
        != EXPECTED_SOURCE_SHA256
    ):
        raise ValueError(
            "Original source SHA256 mismatch"
        )

    if (
        manifest.get("source_rows")
        != EXPECTED_SOURCE_ROWS
    ):
        raise ValueError(
            "Original source row count mismatch"
        )

    hashes = manifest.get(
        "output_sha256"
    )

    if not isinstance(hashes, dict):
        raise ValueError(
            "Input output hashes missing"
        )

    if set(hashes) != set(INPUT_FILES):
        raise ValueError(
            "Unexpected input file manifest"
        )

    verified = {}

    for filename in INPUT_FILES:
        path = input_dir / filename

        if not path.is_file():
            raise FileNotFoundError(path)

        digest = sha256_file(path)

        if digest != hashes[filename]:
            raise ValueError(
                "Input SHA256 mismatch: "
                + filename
            )

        verified[filename] = digest

    result = read_json(
        input_dir / "walk_forward_result.json"
    )

    if (
        result.get("version")
        != EXPECTED_INPUT_VERSION
    ):
        raise ValueError(
            "Input result version mismatch"
        )

    if result.get("execution_status") != "PASS":
        raise ValueError(
            "Input evaluation not PASS"
        )

    if (
        result.get("source_rows")
        != EXPECTED_SOURCE_ROWS
    ):
        raise ValueError(
            "Input result source row mismatch"
        )

    if (
        result.get("source_sha256")
        != EXPECTED_SOURCE_SHA256
    ):
        raise ValueError(
            "Input result source hash mismatch"
        )

    if (
        result.get("matched_timestamp_pairs")
        != 0
    ):
        raise ValueError(
            "Matched pair assumption mismatch"
        )

    if (
        result.get("approved_for_live_trading")
        is not False
    ):
        raise ValueError(
            "Input safety flag mismatch"
        )

    if (
        result.get("comparison_rows")
        != 36
    ):
        raise ValueError(
            "Input comparison count mismatch"
        )

    comparison_rows = read_csv(
        input_dir / "walk_forward_comparison.csv"
    )

    market_rows = read_csv(
        input_dir / "walk_forward_by_market.csv"
    )

    require_columns(
        comparison_rows,
        (
            "fold",
            "role",
            "timeframe",
            "horizon",
            "baseline_completed",
            "baseline_hits",
            "combined_completed",
            "combined_hits",
            "baseline_rate_pct",
            "combined_rate_pct",
            "difference_pp",
        ),
        "comparison",
    )

    require_columns(
        market_rows,
        (
            "fold",
            "role",
            "market",
            "timeframe",
            "horizon",
            "classification",
            "completed",
            "hits",
        ),
        "by_market",
    )

    if len(comparison_rows) != 36:
        raise ValueError(
            "Expected 36 comparison rows"
        )

    if (
        len(market_rows)
        != result.get("market_rows")
    ):
        raise ValueError(
            "Market row count mismatch"
        )

    return (
        result,
        comparison_rows,
        market_rows,
        verified,
    )


def parse_comparison_rows(rows):
    parsed = {}

    for row in rows:
        fold = row["fold"]
        role = row["role"]
        timeframe = row["timeframe"]
        horizon = integer(
            row["horizon"],
            "horizon",
        )

        key = (
            fold,
            role,
            timeframe,
            horizon,
        )

        if (
            fold not in FOLDS
            or role not in ROLES
            or timeframe not in TIMEFRAMES
            or horizon not in HORIZONS
        ):
            raise ValueError(
                "Unexpected comparison group: "
                + str(key)
            )

        if key in parsed:
            raise ValueError(
                "Duplicate comparison group: "
                + str(key)
            )

        baseline_n = integer(
            row["baseline_completed"],
            "baseline_completed",
        )

        baseline_hits = integer(
            row["baseline_hits"],
            "baseline_hits",
        )

        combined_n = integer(
            row["combined_completed"],
            "combined_completed",
        )

        combined_hits = integer(
            row["combined_hits"],
            "combined_hits",
        )

        for completed, hits in (
            (baseline_n, baseline_hits),
            (combined_n, combined_hits),
        ):
            if (
                completed < 0
                or hits < 0
                or hits > completed
            ):
                raise ValueError(
                    "Invalid comparison counts: "
                    + str(key)
                )

        baseline_rate = percentage(
            baseline_hits,
            baseline_n,
        )

        combined_rate = percentage(
            combined_hits,
            combined_n,
        )

        difference = round(
            combined_rate - baseline_rate,
            6,
        )

        for field, expected in (
            ("baseline_rate_pct", baseline_rate),
            ("combined_rate_pct", combined_rate),
            ("difference_pp", difference),
        ):
            actual = finite_float(
                row[field],
                field,
            )

            if abs(actual - expected) > 0.000002:
                raise ValueError(
                    "Comparison arithmetic mismatch: "
                    + str(key)
                    + " "
                    + field
                )

        parsed[key] = {
            "baseline_n": baseline_n,
            "baseline_hits": baseline_hits,
            "combined_n": combined_n,
            "combined_hits": combined_hits,
        }

    expected = {
        (fold, role, timeframe, horizon)
        for fold in FOLDS
        for role in ROLES
        for timeframe in TIMEFRAMES
        for horizon in HORIZONS
    }

    if set(parsed) != expected:
        raise ValueError(
            "Comparison groups incomplete"
        )

    return parsed


def parse_market_rows(rows):
    grouped = defaultdict(list)
    unique = set()

    for row in rows:
        fold = row["fold"]
        role = row["role"]
        market = row["market"].strip().upper()
        timeframe = row["timeframe"]
        horizon = integer(
            row["horizon"],
            "market horizon",
        )
        classification = row["classification"]

        if (
            fold not in FOLDS
            or role not in ROLES
            or timeframe not in TIMEFRAMES
            or horizon not in HORIZONS
            or classification not in CLASSIFICATIONS
            or not market.startswith("KRW-")
        ):
            raise ValueError(
                "Invalid market row: "
                + str(row)
            )

        completed = integer(
            row["completed"],
            "market completed",
        )

        hits = integer(
            row["hits"],
            "market hits",
        )

        if (
            completed <= 0
            or hits < 0
            or hits > completed
        ):
            raise ValueError(
                "Invalid market counts"
            )

        key = (
            fold,
            role,
            timeframe,
            horizon,
            classification,
        )

        unique_key = key + (market,)

        if unique_key in unique:
            raise ValueError(
                "Duplicate market row: "
                + str(unique_key)
            )

        unique.add(unique_key)

        grouped[key].append({
            "market": market,
            "completed": completed,
            "hits": hits,
        })

    return grouped


def concentration_for(
    fold,
    role,
    timeframe,
    horizon,
    classification,
    records,
):
    sorted_records = sorted(
        records,
        key=lambda item: (
            -item["completed"],
            item["market"],
        ),
    )

    completed = sum(
        record["completed"]
        for record in sorted_records
    )

    hits = sum(
        record["hits"]
        for record in sorted_records
    )

    market_count = len(
        sorted_records
    )

    if completed:
        top_market = sorted_records[0]["market"]

        top_completed = (
            sorted_records[0]["completed"]
        )

        top_share = percentage(
            top_completed,
            completed,
        )

        top_five = percentage(
            sum(
                record["completed"]
                for record in sorted_records[:5]
            ),
            completed,
        )

        hhi = sum(
            (
                record["completed"]
                / completed
            ) ** 2
            for record in sorted_records
        )

        effective_count = (
            round(1.0 / hhi, 6)
            if hhi > 0
            else 0.0
        )

    else:
        top_market = ""
        top_completed = 0
        top_share = 0.0
        top_five = 0.0
        effective_count = 0.0

    return {
        "fold": fold,
        "role": role,
        "timeframe": timeframe,
        "horizon": horizon,
        "classification": classification,
        "completed": completed,
        "hits": hits,
        "market_count": market_count,
        "top_market": top_market,
        "top_market_completed": top_completed,
        "top_market_share_pct": top_share,
        "top_five_share_pct": top_five,
        "effective_market_count": effective_count,
    }


def analyze(comparisons, markets):
    comparison_output = []
    concentration_output = []

    concentration_index = {}

    for fold in FOLDS:
        for role in ROLES:
            for timeframe in TIMEFRAMES:
                for horizon in HORIZONS:
                    for classification in CLASSIFICATIONS:
                        key = (
                            fold,
                            role,
                            timeframe,
                            horizon,
                            classification,
                        )

                        concentration = concentration_for(
                            *key,
                            markets.get(key, []),
                        )

                        concentration_output.append(
                            concentration
                        )

                        concentration_index[key] = (
                            concentration
                        )

    for fold in FOLDS:
        for role in ROLES:
            for timeframe in TIMEFRAMES:
                for horizon in HORIZONS:
                    key = (
                        fold,
                        role,
                        timeframe,
                        horizon,
                    )

                    counts = comparisons[key]

                    baseline_n = counts["baseline_n"]
                    baseline_hits = counts["baseline_hits"]

                    combined_n = counts["combined_n"]
                    combined_hits = counts["combined_hits"]

                    baseline_concentration = (
                        concentration_index[
                            key + ("BASELINE",)
                        ]
                    )

                    combined_concentration = (
                        concentration_index[
                            key + ("COMBINED",)
                        ]
                    )

                    if (
                        baseline_concentration["completed"]
                        != baseline_n
                        or baseline_concentration["hits"]
                        != baseline_hits
                        or combined_concentration["completed"]
                        != combined_n
                        or combined_concentration["hits"]
                        != combined_hits
                    ):
                        raise ValueError(
                            "Market aggregation mismatch: "
                            + str(key)
                        )

                    baseline_low, baseline_high = (
                        wilson_interval(
                            baseline_hits,
                            baseline_n,
                        )
                    )

                    combined_low, combined_high = (
                        wilson_interval(
                            combined_hits,
                            combined_n,
                        )
                    )

                    z_score, p_value = (
                        two_proportion_diagnostic(
                            baseline_hits,
                            baseline_n,
                            combined_hits,
                            combined_n,
                        )
                    )

                    baseline_rate = percentage(
                        baseline_hits,
                        baseline_n,
                    )

                    combined_rate = percentage(
                        combined_hits,
                        combined_n,
                    )

                    difference = round(
                        combined_rate - baseline_rate,
                        6,
                    )

                    sample_gate = (
                        min(
                            baseline_n,
                            combined_n,
                        )
                        >= MIN_CLASSIFICATION_SAMPLE
                    )

                    diversity_gate = (
                        min(
                            baseline_concentration["market_count"],
                            combined_concentration["market_count"],
                        )
                        >= MIN_MARKETS
                    )

                    concentration_gate = all(
                        (
                            record["top_market_share_pct"]
                            <= MAX_TOP_MARKET_SHARE_PCT
                            and record["top_five_share_pct"]
                            <= MAX_TOP_FIVE_SHARE_PCT
                        )
                        for record in (
                            baseline_concentration,
                            combined_concentration,
                        )
                    )

                    improved = difference > 0

                    # No promotion based on unadjusted
                    # p-values or observational comparisons.
                    decision = "HOLD"

                    comparison_output.append({
                        "fold": fold,
                        "role": role,
                        "timeframe": timeframe,
                        "horizon": horizon,
                        "baseline_completed": baseline_n,
                        "baseline_hits": baseline_hits,
                        "baseline_rate_pct": baseline_rate,
                        "baseline_ci_low_pct": baseline_low,
                        "baseline_ci_high_pct": baseline_high,
                        "combined_completed": combined_n,
                        "combined_hits": combined_hits,
                        "combined_rate_pct": combined_rate,
                        "combined_ci_low_pct": combined_low,
                        "combined_ci_high_pct": combined_high,
                        "difference_pp": difference,
                        "z_score_descriptive": z_score,
                        "p_value_descriptive": p_value,
                        "baseline_market_count": baseline_concentration["market_count"],
                        "combined_market_count": combined_concentration["market_count"],
                        "baseline_top_market_share_pct": baseline_concentration["top_market_share_pct"],
                        "combined_top_market_share_pct": combined_concentration["top_market_share_pct"],
                        "baseline_top_five_share_pct": baseline_concentration["top_five_share_pct"],
                        "combined_top_five_share_pct": combined_concentration["top_five_share_pct"],
                        "sample_gate": sample_gate,
                        "market_diversity_gate": diversity_gate,
                        "concentration_gate": concentration_gate,
                        "observed_improvement": improved,
                        "decision": decision,
                    })

    return (
        comparison_output,
        concentration_output,
    )


def build_summary(comparison_rows):
    summary = []

    for timeframe in TIMEFRAMES:
        for horizon in HORIZONS:
            test_rows = [
                row
                for row in comparison_rows
                if row["role"] == "TEST"
                and row["timeframe"] == timeframe
                and row["horizon"] == horizon
            ]

            if len(test_rows) != 2:
                raise ValueError(
                    "Missing TEST fold"
                )

            improved_both = all(
                row["observed_improvement"]
                for row in test_rows
            )

            sample_both = all(
                row["sample_gate"]
                for row in test_rows
            )

            diversity_both = all(
                row["market_diversity_gate"]
                for row in test_rows
            )

            concentration_both = all(
                row["concentration_gate"]
                for row in test_rows
            )

            summary.append({
                "timeframe": timeframe,
                "horizon": horizon,
                "test_improvement_both_years": improved_both,
                "sample_gate_both_years": sample_both,
                "market_diversity_both_years": diversity_both,
                "concentration_gate_both_years": concentration_both,
                "research_candidate": (
                    improved_both
                    and sample_both
                    and diversity_both
                    and concentration_both
                ),
                "decision": "HOLD",
            })

    return summary


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--root",
        default=".",
    )

    args = parser.parse_args()

    root = Path(
        args.root
    ).resolve()

    input_dir = (
        root / INPUT_RELATIVE
    ).resolve()

    output_dir = (
        root / OUTPUT_RELATIVE
    ).resolve()

    if (
        input_dir == output_dir
        or input_dir in output_dir.parents
        or output_dir in input_dir.parents
    ):
        raise RuntimeError(
            "Unsafe input/output paths"
        )

    print(
        "[START] " + VERSION,
        flush=True,
    )

    (
        input_result,
        raw_comparisons,
        raw_markets,
        input_hashes,
    ) = validate_inputs(
        input_dir
    )

    print(
        "[PASS] Input SHA256 verified",
        flush=True,
    )

    comparisons = parse_comparison_rows(
        raw_comparisons
    )

    markets = parse_market_rows(
        raw_markets
    )

    (
        comparison_rows,
        concentration_rows,
    ) = analyze(
        comparisons,
        markets,
    )

    if len(comparison_rows) != 36:
        raise RuntimeError(
            "Expected 36 comparison rows"
        )

    if len(concentration_rows) != 72:
        raise RuntimeError(
            "Expected 72 concentration rows"
        )

    summary = build_summary(
        comparison_rows
    )

    if len(summary) != 9:
        raise RuntimeError(
            "Expected 9 research groups"
        )

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    comparison_path = (
        output_dir
        / "robustness_comparison.csv"
    )

    concentration_path = (
        output_dir
        / "robustness_market_concentration.csv"
    )

    result_path = (
        output_dir
        / "robustness_result.json"
    )

    manifest_path = (
        output_dir
        / "robustness_manifest.json"
    )

    atomic_csv(
        comparison_path,
        COMPARISON_COLUMNS,
        comparison_rows,
    )

    atomic_csv(
        concentration_path,
        CONCENTRATION_COLUMNS,
        concentration_rows,
    )

    result = {
        "version": VERSION,
        "execution_status": "PASS",
        "generated_at_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "input_version": EXPECTED_INPUT_VERSION,
        "source_rows": EXPECTED_SOURCE_ROWS,
        "source_sha256": EXPECTED_SOURCE_SHA256,
        "comparison_rows": len(comparison_rows),
        "concentration_rows": len(concentration_rows),
        "research_groups": len(summary),
        "summary": summary,
        "settings": {
            "minimum_classification_sample": MIN_CLASSIFICATION_SAMPLE,
            "minimum_markets": MIN_MARKETS,
            "maximum_top_market_share_pct": MAX_TOP_MARKET_SHARE_PCT,
            "maximum_top_five_share_pct": MAX_TOP_FIVE_SHARE_PCT,
            "confidence_interval": "Wilson 95%",
            "z_test": (
                "Unadjusted descriptive two-sided "
                "two-proportion diagnostic"
            ),
        },
        "limitations": [
            "Baseline and combined are different observational cohorts.",
            "A causal effect of accumulation is not established.",
            "Signals are not guaranteed statistically independent.",
            "Cross-market and cross-horizon dependence remains.",
            "Confidence intervals assume independent Bernoulli trials.",
            "Reported z-test p-values are not cluster robust.",
            "Multiple-comparison correction is not performed.",
            "No profitability or execution-cost validation.",
            "Market regimes are not directly measured.",
            "No live trading approval.",
        ],
        "independent_oos_proven": False,
        "statistical_independence_proven": False,
        "causal_improvement_proven": False,
        "profitability_proven": False,
        "approved_for_live_trading": False,
        "overall_decision": "HOLD",
    }

    atomic_json(
        result_path,
        result,
    )

    output_files = (
        comparison_path,
        concentration_path,
        result_path,
    )

    output_hashes = {
        path.name: sha256_file(path)
        for path in output_files
    }

    # Ensure inputs did not change during analysis.
    for filename, expected_hash in input_hashes.items():
        actual_hash = sha256_file(
            input_dir / filename
        )

        if actual_hash != expected_hash:
            raise RuntimeError(
                "Input changed during evaluation: "
                + filename
            )

    manifest = {
        "version": VERSION,
        "source_sha256": EXPECTED_SOURCE_SHA256,
        "input_version": EXPECTED_INPUT_VERSION,
        "input_sha256": input_hashes,
        "output_sha256": output_hashes,
        "comparison_rows": len(comparison_rows),
        "concentration_rows": len(concentration_rows),
    }

    atomic_json(
        manifest_path,
        manifest,
    )

    for path in output_files:
        if sha256_file(path) != output_hashes[path.name]:
            raise RuntimeError(
                "Output SHA256 mismatch: "
                + path.name
            )

    print(
        "[PASS] Comparison rows: "
        + str(len(comparison_rows)),
        flush=True,
    )

    print(
        "[PASS] Concentration rows: "
        + str(len(concentration_rows)),
        flush=True,
    )

    print(
        "[PASS] Research groups: "
        + str(len(summary)),
        flush=True,
    )

    print(
        "[PASS] SHA256 integrity verified",
        flush=True,
    )

    print(
        "[DECISION] HOLD",
        flush=True,
    )

    print(
        "[OUTPUT] " + str(output_dir),
        flush=True,
    )

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())

    except KeyboardInterrupt:
        print(
            "[INTERRUPTED]",
            file=sys.stderr,
            flush=True,
        )
        sys.exit(130)

    except Exception as exc:
        print(
            "[FAIL] " + str(exc),
            file=sys.stderr,
            flush=True,
        )
        sys.exit(1)
