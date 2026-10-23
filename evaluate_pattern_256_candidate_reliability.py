
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Clean V003-12
Pattern 256 candidate reliability audit.

Input:
    data/reports/pattern_256_concentration_audit/

Output:
    data/reports/pattern_256_candidate_reliability/

Research-only classification:
    RESEARCH_CANDIDATE
    INSUFFICIENT_SAMPLE
    MIXED_OR_UNSTABLE
    NO_REPLICATED_IMPROVEMENT

These are descriptive labels, not trading signals.
No automated trading.
"""

import argparse
import csv
import hashlib
import json
import math
import os
import sys
from collections import Counter
from pathlib import Path

VERSION = "Clean V003-12"
INPUT_VERSION = "Clean V003-11"

INPUT_REL = (
    "data/reports/pattern_256_concentration_audit"
)
OUTPUT_REL = (
    "data/reports/pattern_256_candidate_reliability"
)

INPUT_FILES = (
    "concentration_exclusion.csv",
    "concentration_markets.csv",
    "concentration_result.json",
)

OUTPUT_FILES = (
    "candidate_reliability_markets.csv",
    "candidate_reliability_summary.csv",
    "candidate_reliability_result.json",
)

MINIMUM_SAMPLE = 100
MINIMUM_YEARLY_IMPROVEMENT_PP = 0.0

MARKET_FIELDS = [
    "market",
    "rank_from_previous_audit",
    "year_2025_difference_pp",
    "year_2026_difference_pp",
    "two_year_mean_difference_pp",
    "minimum_observations",
    "positive_both_years",
    "sample_threshold_met",
    "absolute_yearly_difference_pp",
    "classification",
    "approved_for_live_trading",
]

SUMMARY_FIELDS = [
    "classification",
    "market_count",
    "fraction_of_all_markets",
    "mean_difference_pp",
    "median_difference_pp",
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


def read_json(path):
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def read_csv(path):
    with path.open(
        "r", encoding="utf-8-sig", newline=""
    ) as handle:
        return list(csv.DictReader(handle))


def atomic_write(path, content):
    temporary = path.with_suffix(path.suffix + ".tmp")

    with temporary.open(
        "w", encoding="utf-8", newline=""
    ) as handle:
        handle.write(content)
        handle.flush()
        os.fsync(handle.fileno())

    os.replace(temporary, path)


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
    import io

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


def parse_float(value, name):
    try:
        result = float(value)
    except (ValueError, TypeError) as exc:
        raise ValueError(
            "Invalid float: " + name
        ) from exc

    if not math.isfinite(result):
        raise ValueError(
            "Non-finite float: " + name
        )

    return result


def parse_int(value, name):
    try:
        result = int(value)
    except (ValueError, TypeError) as exc:
        raise ValueError(
            "Invalid integer: " + name
        ) from exc

    return result


def median(values):
    ordered = sorted(values)
    size = len(ordered)

    if size == 0:
        return None

    middle = size // 2

    if size % 2:
        return ordered[middle]

    return (
        ordered[middle - 1] + ordered[middle]
    ) / 2


def rounded(value):
    if value is None:
        return None
    return round(value, 8)


def verify_inputs(folder):
    manifest_path = (
        folder / "concentration_manifest.json"
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
        raise ValueError("Input file set mismatch")

    for filename in INPUT_FILES:
        path = folder / filename

        if not path.is_file():
            raise FileNotFoundError(path)

        if sha256(path) != hashes[filename]:
            raise ValueError(
                "Input SHA256 mismatch: " + filename
            )

    result = read_json(
        folder / "concentration_result.json"
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

    if result.get("markets") != 209:
        raise ValueError("Unexpected market count")

    if result.get("positive_both_years") != 26:
        raise ValueError(
            "Unexpected replication count"
        )

    return (
        read_csv(folder / "concentration_markets.csv"),
        result,
        hashes,
    )


def classify(d25, d26, minimum_n):
    both_positive = (
        d25 > MINIMUM_YEARLY_IMPROVEMENT_PP
        and d26 > MINIMUM_YEARLY_IMPROVEMENT_PP
    )

    if both_positive:
        if minimum_n >= MINIMUM_SAMPLE:
            return "RESEARCH_CANDIDATE"
        return "INSUFFICIENT_SAMPLE"

    if (
        (d25 > 0 and d26 < 0)
        or (d25 < 0 and d26 > 0)
        or (d25 > 0 and d26 == 0)
        or (d25 == 0 and d26 > 0)
    ):
        return "MIXED_OR_UNSTABLE"

    return "NO_REPLICATED_IMPROVEMENT"


def analyze(raw_rows):
    if len(raw_rows) != 209:
        raise ValueError("Expected 209 market rows")

    seen_markets = set()
    seen_ranks = set()
    rows = []

    for raw in raw_rows:
        market = raw["market"].strip()
        rank = parse_int(raw["rank"], "rank")

        if not market.startswith("KRW-"):
            raise ValueError("Invalid market")

        if market in seen_markets:
            raise ValueError("Duplicate market")

        if rank in seen_ranks:
            raise ValueError("Duplicate rank")

        seen_markets.add(market)
        seen_ranks.add(rank)

        d25 = parse_float(
            raw["difference_2025_pp"],
            "difference_2025_pp",
        )
        d26 = parse_float(
            raw["difference_2026_pp"],
            "difference_2026_pp",
        )
        mean = parse_float(
            raw["mean_difference_pp"],
            "mean_difference_pp",
        )
        minimum_n = parse_int(
            raw["minimum_observations"],
            "minimum_observations",
        )

        if minimum_n < 10:
            raise ValueError("Sample count below 10")

        if not math.isclose(
            mean,
            (d25 + d26) / 2,
            rel_tol=0,
            abs_tol=0.000001,
        ):
            raise ValueError(
                "Mean mismatch: " + market
            )

        both_positive = d25 > 0 and d26 > 0
        declared = raw[
            "positive_both_years"
        ].strip().lower()

        if declared not in ("true", "false"):
            raise ValueError("Invalid boolean")

        if (declared == "true") != both_positive:
            raise ValueError(
                "Replication mismatch: " + market
            )

        classification = classify(
            d25, d26, minimum_n
        )

        rows.append({
            "market": market,
            "rank_from_previous_audit": rank,
            "year_2025_difference_pp": d25,
            "year_2026_difference_pp": d26,
            "two_year_mean_difference_pp": mean,
            "minimum_observations": minimum_n,
            "positive_both_years": both_positive,
            "sample_threshold_met": (
                minimum_n >= MINIMUM_SAMPLE
            ),
            "absolute_yearly_difference_pp": rounded(
                abs(d25 - d26)
            ),
            "classification": classification,
            "approved_for_live_trading": False,
        })

    if seen_ranks != set(range(1, 210)):
        raise ValueError("Rank sequence mismatch")

    rows.sort(
        key=lambda item: (
            item["classification"],
            -item["two_year_mean_difference_pp"],
            item["market"],
        )
    )

    if sum(
        item["positive_both_years"]
        for item in rows
    ) != 26:
        raise ValueError(
            "Positive-both count mismatch"
        )

    return rows


def build_summary(rows):
    classes = (
        "RESEARCH_CANDIDATE",
        "INSUFFICIENT_SAMPLE",
        "MIXED_OR_UNSTABLE",
        "NO_REPLICATED_IMPROVEMENT",
    )

    summary = []

    for classification in classes:
        subset = [
            row for row in rows
            if row["classification"] == classification
        ]

        values = [
            row["two_year_mean_difference_pp"]
            for row in subset
        ]

        summary.append({
            "classification": classification,
            "market_count": len(subset),
            "fraction_of_all_markets": rounded(
                len(subset) / len(rows)
            ),
            "mean_difference_pp": rounded(
                sum(values) / len(values)
                if values else None
            ),
            "median_difference_pp": rounded(
                median(values)
            ),
            "decision": "HOLD",
        })

    if sum(
        item["market_count"]
        for item in summary
    ) != 209:
        raise ValueError("Summary total mismatch")

    return summary


def check_existing_outputs(folder, input_hashes):
    manifest_path = (
        folder / "candidate_reliability_manifest.json"
    )

    if not manifest_path.is_file():
        return False

    try:
        manifest = read_json(manifest_path)
        result = read_json(
            folder / "candidate_reliability_result.json"
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

        market_rows = read_csv(
            folder / "candidate_reliability_markets.csv"
        )
        summary_rows = read_csv(
            folder / "candidate_reliability_summary.csv"
        )

        if len(market_rows) != 209:
            return False

        if len(summary_rows) != 4:
            return False

        if len({
            row["market"] for row in market_rows
        }) != 209:
            return False

        if sum(
            int(row["market_count"])
            for row in summary_rows
        ) != 209:
            return False

        if sum(
            row["positive_both_years"].lower() == "true"
            for row in market_rows
        ) != 26:
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
        "--root", default="."
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

    raw, source_result, input_hashes = (
        verify_inputs(input_dir)
    )

    if check_existing_outputs(
        output_dir, input_hashes
    ):
        print(
            "[SKIP] Verified completed outputs",
            flush=True,
        )
        print("[DECISION] HOLD", flush=True)
        return 0

    market_rows = analyze(raw)
    summary_rows = build_summary(market_rows)

    candidate_count = sum(
        row["classification"] == "RESEARCH_CANDIDATE"
        for row in market_rows
    )

    insufficient_count = sum(
        row["classification"] == "INSUFFICIENT_SAMPLE"
        for row in market_rows
    )

    if candidate_count + insufficient_count != 26:
        raise ValueError(
            "Replicated-positive partition mismatch"
        )

    result = {
        "version": VERSION,
        "execution_status": "PASS",
        "input_version": INPUT_VERSION,
        "market_count": 209,
        "positive_both_years": 26,
        "minimum_sample_threshold": MINIMUM_SAMPLE,
        "minimum_yearly_improvement_pp": (
            MINIMUM_YEARLY_IMPROVEMENT_PP
        ),
        "research_candidate_count": candidate_count,
        "insufficient_sample_count": insufficient_count,
        "classification_counts": dict(
            Counter(
                row["classification"]
                for row in market_rows
            )
        ),
        "method": (
            "Two-year positive sign and minimum "
            "sample-count descriptive filter"
        ),
        "limitations": [
            "Retrospective research, not independent OOS.",
            "Sample count is not effective independent sample size.",
            "Overlapping events may inflate apparent sample size.",
            "Markets are correlated through common shocks.",
            "No confidence-adjusted market-level effect test.",
            "No transaction costs or executable trade model.",
            "Candidate label is not a buy signal.",
            "No profitability or causality established.",
        ],
        "independent_oos_proven": False,
        "statistical_independence_proven": False,
        "causal_improvement_proven": False,
        "profitability_proven": False,
        "approved_for_live_trading": False,
        "overall_decision": "HOLD",
    }

    for filename, expected in input_hashes.items():
        if sha256(input_dir / filename) != expected:
            raise ValueError(
                "Input changed during analysis: " + filename
            )

    output_dir.mkdir(
        parents=True, exist_ok=True
    )

    market_path = (
        output_dir / "candidate_reliability_markets.csv"
    )
    summary_path = (
        output_dir / "candidate_reliability_summary.csv"
    )
    result_path = (
        output_dir / "candidate_reliability_result.json"
    )
    manifest_path = (
        output_dir / "candidate_reliability_manifest.json"
    )

    write_csv(
        market_path, MARKET_FIELDS, market_rows
    )
    write_csv(
        summary_path, SUMMARY_FIELDS, summary_rows
    )
    write_json(result_path, result)

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

    write_json(manifest_path, manifest)

    if not check_existing_outputs(
        output_dir, input_hashes
    ):
        raise ValueError(
            "Final output integrity check failed"
        )

    print("[PASS]", VERSION, flush=True)
    print(
        "[PASS] Research candidates:",
        candidate_count,
        flush=True,
    )
    print(
        "[PASS] Insufficient sample:",
        insufficient_count,
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
