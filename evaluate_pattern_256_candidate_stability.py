
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Clean V003-14
Pattern 256 candidate stability and sample bottleneck audit.

Input:
    data/reports/pattern_256_threshold_sensitivity/

Outputs:
    data/reports/pattern_256_candidate_stability/
        candidate_stability_markets.csv
        candidate_stability_summary.csv
        candidate_stability_result.json
        candidate_stability_manifest.json

Purpose:
    - Diagnose why historical candidates fail stricter thresholds.
    - Separate sample bottlenecks from weak effect sizes.
    - Examine year-to-year effect asymmetry.
    - Preserve strict research-only HOLD decision.
    - Verify source/output SHA256 and completed-run resume.

Important:
    This analysis uses already aggregated historical data.
    It cannot calculate independent effective sample sizes,
    causal effects, future predictive accuracy, or profitability.
"""

import argparse
import csv
import hashlib
import io
import json
import math
import os
import statistics
import sys

from collections import Counter
from pathlib import Path


VERSION = "Clean V003-14"
INPUT_VERSION = "Clean V003-13"

INPUT_REL = (
    "data/reports/pattern_256_threshold_sensitivity"
)

OUTPUT_REL = (
    "data/reports/pattern_256_candidate_stability"
)

SOURCE_FILES = (
    "threshold_sensitivity_grid.csv",
    "threshold_sensitivity_markets.csv",
    "threshold_sensitivity_result.json",
)

OUTPUT_FILES = (
    "candidate_stability_markets.csv",
    "candidate_stability_summary.csv",
    "candidate_stability_result.json",
)

SAMPLE_THRESHOLDS = (100, 200, 500)
GAIN_THRESHOLDS = (0.0, 0.25, 0.5, 1.0)

MARKET_FIELDS = [
    "market",
    "previous_classification",
    "minimum_observations",
    "year_2025_difference_pp",
    "year_2026_difference_pp",
    "mean_difference_pp",
    "minimum_yearly_difference_pp",
    "absolute_yearly_gap_pp",
    "original_candidate",
    "positive_both_years",
    "passes_sample_100",
    "passes_sample_200",
    "passes_sample_500",
    "passes_gain_0",
    "passes_gain_025",
    "passes_gain_05",
    "passes_gain_1",
    "passes_strict_200_05",
    "strict_failure_reason",
    "sample_bottleneck",
    "effect_bottleneck",
    "additional_observations_to_200",
    "additional_observations_to_500",
    "yearly_effect_asymmetry_ratio",
    "research_priority",
    "approved_for_live_trading",
]

SUMMARY_FIELDS = [
    "group",
    "market_count",
    "original_candidates",
    "positive_both_years",
    "strict_200_05_pass",
    "median_minimum_observations",
    "median_minimum_yearly_improvement_pp",
    "median_absolute_yearly_gap_pp",
    "decision",
]


def sha256(path):
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        for chunk in iter(
            lambda: handle.read(1024 * 1024),
            b"",
        ):
            digest.update(chunk)

    return digest.hexdigest()


def load_json(path):
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def load_csv(path):
    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as handle:
        return list(csv.DictReader(handle))


def atomic_write(path, content):
    temporary = path.with_name(path.name + ".tmp")

    try:
        with temporary.open(
            "w",
            encoding="utf-8",
            newline="",
        ) as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())

        os.replace(temporary, path)

    finally:
        if temporary.exists():
            temporary.unlink()


def save_json(path, value):
    content = json.dumps(
        value,
        indent=2,
        ensure_ascii=False,
        sort_keys=True,
        allow_nan=False,
    )

    atomic_write(path, content + "\n")


def save_csv(path, fields, rows):
    buffer = io.StringIO(newline="")

    writer = csv.DictWriter(
        buffer,
        fieldnames=fields,
        extrasaction="raise",
        lineterminator="\n",
    )

    writer.writeheader()
    writer.writerows(rows)

    atomic_write(path, buffer.getvalue())


def as_float(value, name):
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "Invalid float: " + name
        ) from exc

    if not math.isfinite(result):
        raise ValueError(
            "Nonfinite float: " + name
        )

    return result


def as_int(value, name):
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "Invalid integer: " + name
        ) from exc

    if result < 0:
        raise ValueError(
            "Negative integer: " + name
        )

    return result


def as_bool(value, name):
    if isinstance(value, bool):
        return value

    if isinstance(value, str):
        if value.lower() == "true":
            return True

        if value.lower() == "false":
            return False

    raise ValueError(
        "Invalid boolean: " + name
    )


def rounded(value):
    if value is None:
        return None

    return round(value, 8)


def verify_inputs(folder):
    manifest_path = (
        folder / "threshold_sensitivity_manifest.json"
    )

    if not manifest_path.is_file():
        raise FileNotFoundError(manifest_path)

    manifest = load_json(manifest_path)

    if manifest.get("version") != INPUT_VERSION:
        raise ValueError("Input manifest version mismatch")

    expected_hashes = manifest.get("output_sha256")

    if not isinstance(expected_hashes, dict):
        raise ValueError("Missing input SHA256 map")

    if set(expected_hashes) != set(SOURCE_FILES):
        raise ValueError("Unexpected source file set")

    for filename in SOURCE_FILES:
        path = folder / filename

        if not path.is_file():
            raise FileNotFoundError(path)

        if sha256(path) != expected_hashes[filename]:
            raise ValueError(
                "Source SHA256 mismatch: " + filename
            )

    result = load_json(
        folder / "threshold_sensitivity_result.json"
    )

    if result.get("version") != INPUT_VERSION:
        raise ValueError("Input result version mismatch")

    if result.get("execution_status") != "PASS":
        raise ValueError("Input result is not PASS")

    if result.get("overall_decision") != "HOLD":
        raise ValueError("Input decision is not HOLD")

    for field in (
        "approved_for_live_trading",
        "independent_oos_proven",
        "statistical_independence_proven",
        "causal_improvement_proven",
        "profitability_proven",
    ):
        if result.get(field) is not False:
            raise ValueError(
                "Unsafe input flag: " + field
            )

    if result.get("market_count") != 209:
        raise ValueError("Unexpected market count")

    if result.get("original_candidate_count") != 22:
        raise ValueError("Unexpected candidate count")

    if result.get("strict_200_05_count") != 1:
        raise ValueError("Unexpected strict candidate count")

    if result.get("sample_thresholds") != list(
        SAMPLE_THRESHOLDS
    ):
        raise ValueError("Sample threshold mismatch")

    if result.get("improvement_thresholds_pp") != list(
        GAIN_THRESHOLDS
    ):
        raise ValueError("Gain threshold mismatch")

    grid = load_csv(
        folder / "threshold_sensitivity_grid.csv"
    )

    markets = load_csv(
        folder / "threshold_sensitivity_markets.csv"
    )

    if len(grid) != 12:
        raise ValueError("Grid row count mismatch")

    if len(markets) != 209:
        raise ValueError("Market row count mismatch")

    expected_pairs = {
        (sample, gain)
        for sample in SAMPLE_THRESHOLDS
        for gain in GAIN_THRESHOLDS
    }

    actual_pairs = set()

    for row in grid:
        pair = (
            as_int(
                row["minimum_observations"],
                "grid.minimum_observations",
            ),
            as_float(
                row["minimum_each_year_improvement_pp"],
                "grid.minimum_each_year_improvement_pp",
            ),
        )

        if pair in actual_pairs:
            raise ValueError("Duplicate grid pair")

        actual_pairs.add(pair)

        if row["decision"] != "HOLD":
            raise ValueError("Unsafe grid decision")

    if actual_pairs != expected_pairs:
        raise ValueError("Grid threshold mismatch")

    return markets, grid, dict(expected_hashes)


def parse_markets(raw_rows):
    parsed = []
    seen = set()

    allowed_classes = {
        "RESEARCH_CANDIDATE",
        "INSUFFICIENT_SAMPLE",
        "MIXED_OR_UNSTABLE",
        "NO_REPLICATED_IMPROVEMENT",
    }

    for row in raw_rows:
        market = row["market"].strip()

        if not market.startswith("KRW-"):
            raise ValueError("Invalid market: " + market)

        if market in seen:
            raise ValueError("Duplicate market: " + market)

        seen.add(market)

        classification = row[
            "previous_classification"
        ].strip()

        if classification not in allowed_classes:
            raise ValueError(
                "Invalid classification: " + market
            )

        sample = as_int(
            row["minimum_observations"],
            "minimum_observations",
        )

        d25 = as_float(
            row["year_2025_difference_pp"],
            "year_2025_difference_pp",
        )

        d26 = as_float(
            row["year_2026_difference_pp"],
            "year_2026_difference_pp",
        )

        mean = as_float(
            row["mean_difference_pp"],
            "mean_difference_pp",
        )

        minimum = as_float(
            row["minimum_yearly_difference_pp"],
            "minimum_yearly_difference_pp",
        )

        gap = as_float(
            row["absolute_yearly_gap_pp"],
            "absolute_yearly_gap_pp",
        )

        tolerance = 0.00000002

        if abs(mean - (d25 + d26) / 2) > tolerance:
            raise ValueError(
                "Mean difference mismatch: " + market
            )

        if abs(minimum - min(d25, d26)) > tolerance:
            raise ValueError(
                "Minimum effect mismatch: " + market
            )

        if abs(gap - abs(d25 - d26)) > tolerance:
            raise ValueError(
                "Yearly gap mismatch: " + market
            )

        original = (
            classification == "RESEARCH_CANDIDATE"
        )

        expected_original = (
            sample >= 100
            and d25 > 0
            and d26 > 0
        )

        if original != expected_original:
            raise ValueError(
                "Original classification mismatch: "
                + market
            )

        expected_flags = {
            "passes_sample_100": sample >= 100,
            "passes_sample_200": sample >= 200,
            "passes_sample_500": sample >= 500,
            "passes_improvement_0": minimum > 0,
            "passes_improvement_025": minimum > 0.25,
            "passes_improvement_05": minimum > 0.5,
            "passes_improvement_1": minimum > 1,
            "passes_strict_200_05": (
                sample >= 200
                and minimum > 0.5
            ),
            "approved_for_live_trading": False,
        }

        for field, expected in expected_flags.items():
            actual = as_bool(row[field], field)

            if actual != expected:
                raise ValueError(
                    "Flag mismatch: "
                    + market + " / " + field
                )

        parsed.append({
            "market": market,
            "classification": classification,
            "sample": sample,
            "d25": d25,
            "d26": d26,
            "mean": mean,
            "minimum": minimum,
            "gap": gap,
            "original": original,
        })

    parsed.sort(key=lambda item: item["market"])

    if len(parsed) != 209:
        raise ValueError("Parsed market count mismatch")

    if sum(item["original"] for item in parsed) != 22:
        raise ValueError("Original candidate mismatch")

    if sum(
        item["sample"] >= 200
        and item["minimum"] > 0.5
        for item in parsed
    ) != 1:
        raise ValueError("Strict candidate mismatch")

    return parsed


def verify_grid(markets, grid):
    for row in grid:
        sample = as_int(
            row["minimum_observations"],
            "grid.sample",
        )

        gain = as_float(
            row["minimum_each_year_improvement_pp"],
            "grid.gain",
        )

        selected = [
            item
            for item in markets
            if item["sample"] >= sample
            and item["minimum"] > gain
        ]

        count = len(selected)

        if as_int(
            row["eligible_markets"],
            "grid.eligible_markets",
        ) != count:
            raise ValueError("Grid count mismatch")

        if as_int(
            row["retained_from_original_22"],
            "grid.retained",
        ) != count:
            raise ValueError("Grid retention mismatch")

        rate = as_float(
            row["retention_rate"],
            "grid.retention_rate",
        )

        if abs(rate - count / 22) > 0.00000002:
            raise ValueError("Grid retention rate mismatch")

        if selected:
            mean = statistics.mean(
                item["mean"] for item in selected
            )

            median = statistics.median(
                item["mean"] for item in selected
            )

            if abs(
                as_float(
                    row["mean_two_year_difference_pp"],
                    "grid.mean",
                ) - mean
            ) > 0.00000002:
                raise ValueError("Grid mean mismatch")

            if abs(
                as_float(
                    row["median_two_year_difference_pp"],
                    "grid.median",
                ) - median
            ) > 0.00000002:
                raise ValueError("Grid median mismatch")

        else:
            if row["mean_two_year_difference_pp"].strip():
                raise ValueError("Unexpected empty-group mean")

            if row["median_two_year_difference_pp"].strip():
                raise ValueError("Unexpected empty-group median")


def diagnose(item):
    sample_ok = item["sample"] >= 200
    effect_ok = item["minimum"] > 0.5
    positive = item["minimum"] > 0

    if sample_ok and effect_ok:
        reason = "PASS_STRICT"

    elif not sample_ok and not effect_ok:
        reason = "SAMPLE_AND_EFFECT"

    elif not sample_ok:
        reason = "SAMPLE_ONLY"

    else:
        reason = "EFFECT_ONLY"

    if sample_ok and effect_ok:
        priority = "STRICT_RESEARCH_CANDIDATE"

    elif effect_ok and not sample_ok:
        priority = "SAMPLE_EXTENSION_RESEARCH"

    elif positive and not effect_ok:
        priority = "WEAK_EFFECT_RESEARCH"

    elif item["d25"] * item["d26"] < 0:
        priority = "YEAR_DIRECTION_CONFLICT"

    else:
        priority = "NO_REPLICATED_POSITIVE_EFFECT"

    if positive:
        asymmetry = item["gap"] / item["minimum"]
    else:
        asymmetry = None

    return {
        "reason": reason,
        "priority": priority,
        "sample_bottleneck": not sample_ok,
        "effect_bottleneck": not effect_ok,
        "asymmetry": asymmetry,
    }


def build_market_rows(markets):
    rows = []

    for item in markets:
        diagnostic = diagnose(item)
        sample = item["sample"]
        minimum = item["minimum"]

        rows.append({
            "market": item["market"],
            "previous_classification": item["classification"],
            "minimum_observations": sample,
            "year_2025_difference_pp": rounded(item["d25"]),
            "year_2026_difference_pp": rounded(item["d26"]),
            "mean_difference_pp": rounded(item["mean"]),
            "minimum_yearly_difference_pp": rounded(minimum),
            "absolute_yearly_gap_pp": rounded(item["gap"]),
            "original_candidate": item["original"],
            "positive_both_years": minimum > 0,
            "passes_sample_100": sample >= 100,
            "passes_sample_200": sample >= 200,
            "passes_sample_500": sample >= 500,
            "passes_gain_0": minimum > 0,
            "passes_gain_025": minimum > 0.25,
            "passes_gain_05": minimum > 0.5,
            "passes_gain_1": minimum > 1,
            "passes_strict_200_05": (
                sample >= 200 and minimum > 0.5
            ),
            "strict_failure_reason": diagnostic["reason"],
            "sample_bottleneck": diagnostic["sample_bottleneck"],
            "effect_bottleneck": diagnostic["effect_bottleneck"],
            "additional_observations_to_200": max(
                0, 200 - sample
            ),
            "additional_observations_to_500": max(
                0, 500 - sample
            ),
            "yearly_effect_asymmetry_ratio": rounded(
                diagnostic["asymmetry"]
            ),
            "research_priority": diagnostic["priority"],
            "approved_for_live_trading": False,
        })

    return rows


def build_summary(markets):
    groups = {
        "ALL_MARKETS": markets,
        "ORIGINAL_CANDIDATES": [
            item for item in markets if item["original"]
        ],
        "POSITIVE_BOTH_YEARS": [
            item for item in markets
            if item["minimum"] > 0
        ],
        "SAMPLE_BELOW_200": [
            item for item in markets
            if item["sample"] < 200
        ],
        "SAMPLE_AT_LEAST_200": [
            item for item in markets
            if item["sample"] >= 200
        ],
        "EFFECT_AT_MOST_05": [
            item for item in markets
            if item["minimum"] <= 0.5
        ],
        "EFFECT_ABOVE_05": [
            item for item in markets
            if item["minimum"] > 0.5
        ],
        "STRICT_PASS": [
            item for item in markets
            if item["sample"] >= 200
            and item["minimum"] > 0.5
        ],
    }

    rows = []

    for name, selected in groups.items():
        rows.append({
            "group": name,
            "market_count": len(selected),
            "original_candidates": sum(
                item["original"] for item in selected
            ),
            "positive_both_years": sum(
                item["minimum"] > 0
                for item in selected
            ),
            "strict_200_05_pass": sum(
                item["sample"] >= 200
                and item["minimum"] > 0.5
                for item in selected
            ),
            "median_minimum_observations": rounded(
                statistics.median(
                    item["sample"] for item in selected
                ) if selected else None
            ),
            "median_minimum_yearly_improvement_pp": rounded(
                statistics.median(
                    item["minimum"] for item in selected
                ) if selected else None
            ),
            "median_absolute_yearly_gap_pp": rounded(
                statistics.median(
                    item["gap"] for item in selected
                ) if selected else None
            ),
            "decision": "HOLD",
        })

    return rows


def verify_completed(output_dir, input_hashes):
    manifest_path = (
        output_dir / "candidate_stability_manifest.json"
    )

    if not manifest_path.is_file():
        return False

    try:
        manifest = load_json(manifest_path)
        result = load_json(
            output_dir / "candidate_stability_result.json"
        )

        if manifest.get("version") != VERSION:
            return False

        if manifest.get("input_sha256") != input_hashes:
            return False

        if result.get("version") != VERSION:
            return False

        if result.get("execution_status") != "PASS":
            return False

        if result.get("overall_decision") != "HOLD":
            return False

        if result.get("approved_for_live_trading") is not False:
            return False

        if result.get("market_count") != 209:
            return False

        if result.get("original_candidate_count") != 22:
            return False

        if result.get("strict_candidate_count") != 1:
            return False

        hashes = manifest.get("output_sha256")

        if not isinstance(hashes, dict):
            return False

        if set(hashes) != set(OUTPUT_FILES):
            return False

        for filename in OUTPUT_FILES:
            path = output_dir / filename

            if not path.is_file():
                return False

            if sha256(path) != hashes[filename]:
                return False

        markets = load_csv(
            output_dir / "candidate_stability_markets.csv"
        )

        summary = load_csv(
            output_dir / "candidate_stability_summary.csv"
        )

        if len(markets) != 209:
            return False

        if len(summary) != 8:
            return False

        if len({
            row["market"] for row in markets
        }) != 209:
            return False

        if sum(
            as_bool(
                row["original_candidate"],
                "original_candidate",
            )
            for row in markets
        ) != 22:
            return False

        if sum(
            as_bool(
                row["passes_strict_200_05"],
                "passes_strict_200_05",
            )
            for row in markets
        ) != 1:
            return False

        if any(
            as_bool(
                row["approved_for_live_trading"],
                "approved_for_live_trading",
            )
            for row in markets
        ):
            return False

        if any(
            row["decision"] != "HOLD"
            for row in summary
        ):
            return False

        return True

    except (
        OSError,
        ValueError,
        TypeError,
        KeyError,
        json.JSONDecodeError,
    ):
        return False


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--root",
        default=".",
    )

    args = parser.parse_args()

    root = Path(args.root).resolve()
    input_dir = (root / INPUT_REL).resolve()
    output_dir = (root / OUTPUT_REL).resolve()

    if (
        input_dir == output_dir
        or input_dir in output_dir.parents
        or output_dir in input_dir.parents
        or output_dir == root
    ):
        raise ValueError("Unsafe output directory")

    print("[START]", VERSION, flush=True)

    raw_markets, grid, input_hashes = verify_inputs(
        input_dir
    )

    if verify_completed(output_dir, input_hashes):
        print(
            "[SKIP] Verified completed outputs",
            flush=True,
        )
        print("[DECISION] HOLD", flush=True)
        return 0

    markets = parse_markets(raw_markets)

    verify_grid(markets, grid)

    market_rows = build_market_rows(markets)
    summary_rows = build_summary(markets)

    original = [
        item for item in markets
        if item["original"]
    ]

    reasons = Counter(
        diagnose(item)["reason"]
        for item in original
    )

    priorities = Counter(
        diagnose(item)["priority"]
        for item in markets
    )

    strict = [
        item["market"]
        for item in markets
        if item["sample"] >= 200
        and item["minimum"] > 0.5
    ]

    result = {
        "version": VERSION,
        "input_version": INPUT_VERSION,
        "execution_status": "PASS",
        "market_count": 209,
        "original_candidate_count": 22,
        "strict_candidate_count": len(strict),
        "strict_candidate_markets": strict,
        "strict_sample_threshold": 200,
        "strict_minimum_each_year_improvement_pp": 0.5,
        "original_candidate_failure_reasons": dict(
            sorted(reasons.items())
        ),
        "all_market_research_priorities": dict(
            sorted(priorities.items())
        ),
        "market_rows": len(market_rows),
        "summary_rows": len(summary_rows),
        "method": (
            "Retrospective market-level threshold "
            "failure and stability diagnostics"
        ),
        "limitations": [
            "Source contains aggregated yearly effects, not raw independent events.",
            "Minimum observations is a source-defined descriptive count.",
            "Additional observations needed is arithmetic only, not a data collection guarantee.",
            "Market correlation and overlapping signals are not resolved.",
            "Thresholds were inspected on historical data.",
            "No independent future OOS validation.",
            "No executable trading model or transaction costs.",
            "Research priority is not a buy signal.",
        ],
        "independent_oos_proven": False,
        "statistical_independence_proven": False,
        "causal_improvement_proven": False,
        "profitability_proven": False,
        "approved_for_live_trading": False,
        "overall_decision": "HOLD",
    }

    # Prevent writing results based on modified sources.
    for filename, expected in input_hashes.items():
        actual = sha256(input_dir / filename)

        if actual != expected:
            raise ValueError(
                "Source changed during run: " + filename
            )

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    save_csv(
        output_dir / OUTPUT_FILES[0],
        MARKET_FIELDS,
        market_rows,
    )

    save_csv(
        output_dir / OUTPUT_FILES[1],
        SUMMARY_FIELDS,
        summary_rows,
    )

    save_json(
        output_dir / OUTPUT_FILES[2],
        result,
    )

    output_hashes = {
        filename: sha256(output_dir / filename)
        for filename in OUTPUT_FILES
    }

    manifest = {
        "version": VERSION,
        "input_version": INPUT_VERSION,
        "input_sha256": input_hashes,
        "output_sha256": output_hashes,
        "market_rows": len(market_rows),
        "summary_rows": len(summary_rows),
    }

    save_json(
        output_dir / "candidate_stability_manifest.json",
        manifest,
    )

    if not verify_completed(output_dir, input_hashes):
        raise ValueError(
            "Final output integrity verification failed"
        )

    print("[PASS]", VERSION, flush=True)
    print("[PASS] Markets:", len(markets), flush=True)
    print(
        "[PASS] Original candidates:",
        len(original),
        flush=True,
    )
    print(
        "[PASS] Strict candidates:",
        len(strict),
        flush=True,
    )
    print(
        "[PASS] Original candidate failure reasons:",
        dict(sorted(reasons.items())),
        flush=True,
    )
    print("[PASS] SHA256 integrity", flush=True)
    print("[DECISION] HOLD", flush=True)

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())

    except KeyboardInterrupt:
        print(
            "[INTERRUPTED]",
            file=sys.stderr,
            flush=True,
        )
        raise SystemExit(130)

    except Exception as exc:
        print(
            "[FAIL] " + str(exc),
            file=sys.stderr,
            flush=True,
        )
        raise SystemExit(1)
