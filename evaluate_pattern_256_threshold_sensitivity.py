
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Clean V003-13
Pattern 256 candidate threshold sensitivity audit.

Input:
  data/reports/pattern_256_candidate_reliability/

Output:
  data/reports/pattern_256_threshold_sensitivity/

Checks:
  - Minimum observations: 100, 200, 500
  - Minimum improvement in EACH year:
    0.00, 0.25, 0.50, 1.00 percentage points
  - Candidate retention against V003-12
  - Year-to-year improvement asymmetry
  - Input/output SHA256 integrity
  - Verified resume

This is descriptive retrospective research.
No new independent OOS evidence.
No trading approval.
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

from pathlib import Path


VERSION = "Clean V003-13"
INPUT_VERSION = "Clean V003-12"

INPUT_REL = (
    "data/reports/pattern_256_candidate_reliability"
)

OUTPUT_REL = (
    "data/reports/pattern_256_threshold_sensitivity"
)

INPUT_FILES = (
    "candidate_reliability_markets.csv",
    "candidate_reliability_summary.csv",
    "candidate_reliability_result.json",
)

OUTPUT_FILES = (
    "threshold_sensitivity_grid.csv",
    "threshold_sensitivity_markets.csv",
    "threshold_sensitivity_result.json",
)

SAMPLE_THRESHOLDS = (100, 200, 500)
IMPROVEMENT_THRESHOLDS = (0.0, 0.25, 0.5, 1.0)

GRID_FIELDS = [
    "minimum_observations",
    "minimum_each_year_improvement_pp",
    "eligible_markets",
    "retained_from_original_22",
    "retention_rate",
    "mean_two_year_difference_pp",
    "median_two_year_difference_pp",
    "decision",
]

MARKET_FIELDS = [
    "market",
    "previous_classification",
    "minimum_observations",
    "year_2025_difference_pp",
    "year_2026_difference_pp",
    "mean_difference_pp",
    "minimum_yearly_difference_pp",
    "absolute_yearly_gap_pp",
    "passes_sample_100",
    "passes_sample_200",
    "passes_sample_500",
    "passes_improvement_0",
    "passes_improvement_025",
    "passes_improvement_05",
    "passes_improvement_1",
    "passes_strict_200_05",
    "approved_for_live_trading",
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


def read_json(path):
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def read_csv(path):
    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as handle:
        return list(csv.DictReader(handle))


def atomic_write(path, content):
    temp = path.with_suffix(path.suffix + ".tmp")

    with temp.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as handle:
        handle.write(content)
        handle.flush()
        os.fsync(handle.fileno())

    os.replace(temp, path)


def write_json(path, data):
    content = json.dumps(
        data,
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
        allow_nan=False,
    )

    atomic_write(path, content + "\n")


def write_csv(path, fields, rows):
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


def number(value, field):
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "Invalid number: " + field
        ) from exc

    if not math.isfinite(result):
        raise ValueError(
            "Non-finite number: " + field
        )

    return result


def integer(value, field):
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "Invalid integer: " + field
        ) from exc

    return result


def rounded(value):
    if value is None:
        return None

    return round(value, 8)


def verify_source(folder):
    manifest_path = (
        folder / "candidate_reliability_manifest.json"
    )

    if not manifest_path.is_file():
        raise FileNotFoundError(manifest_path)

    manifest = read_json(manifest_path)

    if manifest.get("version") != INPUT_VERSION:
        raise ValueError(
            "Input manifest version mismatch"
        )

    hashes = manifest.get("output_sha256")

    if not isinstance(hashes, dict):
        raise ValueError("Missing input hashes")

    if set(hashes) != set(INPUT_FILES):
        raise ValueError("Unexpected input file set")

    for filename in INPUT_FILES:
        path = folder / filename

        if not path.is_file():
            raise FileNotFoundError(path)

        if sha256(path) != hashes[filename]:
            raise ValueError(
                "Input SHA256 mismatch: " + filename
            )

    result = read_json(
        folder / "candidate_reliability_result.json"
    )

    if result.get("version") != INPUT_VERSION:
        raise ValueError(
            "Input result version mismatch"
        )

    if result.get("execution_status") != "PASS":
        raise ValueError("Input not PASS")

    if result.get("overall_decision") != "HOLD":
        raise ValueError("Input not HOLD")

    if result.get("approved_for_live_trading") is not False:
        raise ValueError("Unsafe input approval")

    if result.get("market_count") != 209:
        raise ValueError("Unexpected market count")

    if result.get("positive_both_years") != 26:
        raise ValueError(
            "Unexpected positive-both count"
        )

    if result.get("research_candidate_count") != 22:
        raise ValueError(
            "Unexpected original candidate count"
        )

    if result.get("minimum_sample_threshold") != 100:
        raise ValueError(
            "Unexpected source sample threshold"
        )

    if result.get("minimum_yearly_improvement_pp") != 0:
        raise ValueError(
            "Unexpected source improvement threshold"
        )

    rows = read_csv(
        folder / "candidate_reliability_markets.csv"
    )

    summary = read_csv(
        folder / "candidate_reliability_summary.csv"
    )

    if len(rows) != 209:
        raise ValueError("Market row count mismatch")

    if len(summary) != 4:
        raise ValueError("Summary row count mismatch")

    if sum(
        integer(row["market_count"], "market_count")
        for row in summary
    ) != 209:
        raise ValueError("Summary total mismatch")

    if any(
        row["decision"] != "HOLD"
        for row in summary
    ):
        raise ValueError("Unsafe summary decision")

    return rows, hashes


def parse_markets(raw_rows):
    markets = []
    seen = set()

    valid_classes = {
        "RESEARCH_CANDIDATE",
        "INSUFFICIENT_SAMPLE",
        "MIXED_OR_UNSTABLE",
        "NO_REPLICATED_IMPROVEMENT",
    }

    for raw in raw_rows:
        market = raw["market"].strip()

        if not market.startswith("KRW-"):
            raise ValueError(
                "Invalid market: " + market
            )

        if market in seen:
            raise ValueError(
                "Duplicate market: " + market
            )

        seen.add(market)

        classification = raw[
            "classification"
        ].strip()

        if classification not in valid_classes:
            raise ValueError(
                "Invalid classification: " + market
            )

        d25 = number(
            raw["year_2025_difference_pp"],
            "year_2025_difference_pp",
        )

        d26 = number(
            raw["year_2026_difference_pp"],
            "year_2026_difference_pp",
        )

        mean = number(
            raw["two_year_mean_difference_pp"],
            "two_year_mean_difference_pp",
        )

        sample = integer(
            raw["minimum_observations"],
            "minimum_observations",
        )

        if sample < 10:
            raise ValueError(
                "Invalid sample count: " + market
            )

        if not math.isclose(
            mean,
            (d25 + d26) / 2,
            rel_tol=0,
            abs_tol=0.000001,
        ):
            raise ValueError(
                "Mean mismatch: " + market
            )

        expected_candidate = (
            sample >= 100
            and d25 > 0
            and d26 > 0
        )

        actual_candidate = (
            classification == "RESEARCH_CANDIDATE"
        )

        if expected_candidate != actual_candidate:
            raise ValueError(
                "Candidate mismatch: " + market
            )

        declared_positive = raw[
            "positive_both_years"
        ].strip().lower()

        if declared_positive not in ("true", "false"):
            raise ValueError(
                "Invalid positive-both flag"
            )

        if (
            declared_positive == "true"
        ) != (d25 > 0 and d26 > 0):
            raise ValueError(
                "Positive-both mismatch: " + market
            )

        if raw[
            "approved_for_live_trading"
        ].strip().lower() != "false":
            raise ValueError(
                "Unsafe trading flag: " + market
            )

        markets.append({
            "market": market,
            "classification": classification,
            "sample": sample,
            "d25": d25,
            "d26": d26,
            "mean": mean,
        })

    markets.sort(key=lambda item: item["market"])

    if len(markets) != 209:
        raise ValueError("Parsed market count mismatch")

    if sum(
        item["classification"] == "RESEARCH_CANDIDATE"
        for item in markets
    ) != 22:
        raise ValueError(
            "Original candidate count mismatch"
        )

    return markets


def qualifies(market, minimum_sample, minimum_gain):
    return (
        market["sample"] >= minimum_sample
        and market["d25"] > minimum_gain
        and market["d26"] > minimum_gain
    )


def build_grid(markets):
    rows = []

    for sample in SAMPLE_THRESHOLDS:
        for improvement in IMPROVEMENT_THRESHOLDS:
            selected = [
                market
                for market in markets
                if qualifies(
                    market,
                    sample,
                    improvement,
                )
            ]

            values = [
                item["mean"]
                for item in selected
            ]

            retained = sum(
                item["classification"]
                == "RESEARCH_CANDIDATE"
                for item in selected
            )

            rows.append({
                "minimum_observations": sample,
                "minimum_each_year_improvement_pp": improvement,
                "eligible_markets": len(selected),
                "retained_from_original_22": retained,
                "retention_rate": rounded(
                    retained / 22
                ),
                "mean_two_year_difference_pp": rounded(
                    statistics.mean(values)
                    if values else None
                ),
                "median_two_year_difference_pp": rounded(
                    statistics.median(values)
                    if values else None
                ),
                "decision": "HOLD",
            })

    baseline = next(
        row for row in rows
        if (
            row["minimum_observations"] == 100
            and row[
                "minimum_each_year_improvement_pp"
            ] == 0
        )
    )

    if baseline["eligible_markets"] != 22:
        raise ValueError(
            "Baseline candidate count mismatch"
        )

    if baseline["retained_from_original_22"] != 22:
        raise ValueError(
            "Baseline retention mismatch"
        )

    if len(rows) != 12:
        raise ValueError("Grid row count mismatch")

    return rows


def build_market_details(markets):
    rows = []

    for item in markets:
        sample = item["sample"]
        d25 = item["d25"]
        d26 = item["d26"]
        minimum_gain = min(d25, d26)

        rows.append({
            "market": item["market"],
            "previous_classification": item["classification"],
            "minimum_observations": sample,
            "year_2025_difference_pp": rounded(d25),
            "year_2026_difference_pp": rounded(d26),
            "mean_difference_pp": rounded(item["mean"]),
            "minimum_yearly_difference_pp": rounded(
                minimum_gain
            ),
            "absolute_yearly_gap_pp": rounded(
                abs(d25 - d26)
            ),
            "passes_sample_100": sample >= 100,
            "passes_sample_200": sample >= 200,
            "passes_sample_500": sample >= 500,
            "passes_improvement_0": minimum_gain > 0,
            "passes_improvement_025": minimum_gain > 0.25,
            "passes_improvement_05": minimum_gain > 0.5,
            "passes_improvement_1": minimum_gain > 1.0,
            "passes_strict_200_05": qualifies(
                item, 200, 0.5
            ),
            "approved_for_live_trading": False,
        })

    return rows


def verified_resume(folder, input_hashes):
    manifest_path = (
        folder / "threshold_sensitivity_manifest.json"
    )

    if not manifest_path.is_file():
        return False

    try:
        manifest = read_json(manifest_path)
        result = read_json(
            folder / "threshold_sensitivity_result.json"
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

        if result.get("sample_thresholds") != list(
            SAMPLE_THRESHOLDS
        ):
            return False

        if result.get("improvement_thresholds_pp") != list(
            IMPROVEMENT_THRESHOLDS
        ):
            return False

        hashes = manifest.get("output_sha256")

        if not isinstance(hashes, dict):
            return False

        if set(hashes) != set(OUTPUT_FILES):
            return False

        for filename in OUTPUT_FILES:
            path = folder / filename

            if not path.is_file():
                return False

            if sha256(path) != hashes[filename]:
                return False

        grid = read_csv(
            folder / "threshold_sensitivity_grid.csv"
        )

        markets = read_csv(
            folder / "threshold_sensitivity_markets.csv"
        )

        if len(grid) != 12 or len(markets) != 209:
            return False

        if len({
            row["market"] for row in markets
        }) != 209:
            return False

        baseline = [
            row for row in grid
            if (
                int(row["minimum_observations"]) == 100
                and float(
                    row["minimum_each_year_improvement_pp"]
                ) == 0
            )
        ]

        if len(baseline) != 1:
            return False

        if int(baseline[0]["eligible_markets"]) != 22:
            return False

        if any(
            row["decision"] != "HOLD"
            for row in grid
        ):
            return False

        if any(
            row["approved_for_live_trading"].lower()
            != "false"
            for row in markets
        ):
            return False

        return True

    except (
        OSError,
        ValueError,
        KeyError,
        TypeError,
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
        raise ValueError("Unsafe output path")

    print("[START]", VERSION, flush=True)

    raw_rows, input_hashes = verify_source(input_dir)

    if verified_resume(output_dir, input_hashes):
        print(
            "[SKIP] Verified completed outputs",
            flush=True,
        )
        print("[DECISION] HOLD", flush=True)
        return 0

    markets = parse_markets(raw_rows)

    grid_rows = build_grid(markets)
    detail_rows = build_market_details(markets)

    strict_count = sum(
        qualifies(item, 200, 0.5)
        for item in markets
    )

    original_candidates = [
        item
        for item in markets
        if item["classification"] == "RESEARCH_CANDIDATE"
    ]

    original_yearly_gaps = [
        abs(item["d25"] - item["d26"])
        for item in original_candidates
    ]

    result = {
        "version": VERSION,
        "execution_status": "PASS",
        "input_version": INPUT_VERSION,
        "market_count": 209,
        "original_candidate_count": 22,
        "sample_thresholds": list(SAMPLE_THRESHOLDS),
        "improvement_thresholds_pp": list(
            IMPROVEMENT_THRESHOLDS
        ),
        "strict_200_05_count": strict_count,
        "original_candidate_median_yearly_gap_pp": rounded(
            statistics.median(original_yearly_gaps)
        ),
        "grid_rows": 12,
        "market_rows": 209,
        "method": (
            "Retrospective sensitivity analysis of "
            "sample and two-year effect thresholds"
        ),
        "limitations": [
            "Thresholds are descriptive and not validated selection rules.",
            "The same historical data was used to select candidates.",
            "Observation counts are not independent effective sample sizes.",
            "Overlapping signals and market correlation remain.",
            "No fresh independent OOS evaluation was performed.",
            "No causal effect or net profitability was established.",
            "Stricter thresholds do not imply greater predictive accuracy.",
        ],
        "independent_oos_proven": False,
        "statistical_independence_proven": False,
        "causal_improvement_proven": False,
        "profitability_proven": False,
        "approved_for_live_trading": False,
        "overall_decision": "HOLD",
    }

    # Recheck inputs before writing any output.
    for filename, expected in input_hashes.items():
        if sha256(input_dir / filename) != expected:
            raise ValueError(
                "Input changed during evaluation: " + filename
            )

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    write_csv(
        output_dir / OUTPUT_FILES[0],
        GRID_FIELDS,
        grid_rows,
    )

    write_csv(
        output_dir / OUTPUT_FILES[1],
        MARKET_FIELDS,
        detail_rows,
    )

    write_json(
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
        "grid_rows": len(grid_rows),
        "market_rows": len(detail_rows),
    }

    write_json(
        output_dir / "threshold_sensitivity_manifest.json",
        manifest,
    )

    if not verified_resume(output_dir, input_hashes):
        raise ValueError(
            "Final integrity verification failed"
        )

    print("[PASS]", VERSION, flush=True)
    print("[PASS] Markets: 209", flush=True)
    print("[PASS] Original candidates: 22", flush=True)
    print(
        "[PASS] Strict 200 / 0.5pp:",
        strict_count,
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
