
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Statistical Robustness Audit
Clean V001

Inputs:
  data/reports/pattern_256_matched_baseline/
  data/reports/pattern_256_conservative_audit/

Purpose:
  - Verify previous SHA256 manifests
  - Check paired hit-count consistency
  - Calculate paired difference confidence intervals
  - Calculate exact McNemar p-values
  - Apply Holm multiple-testing correction
  - Identify direction reversals
  - Report sample retention
  - Explicitly flag untestable groups

No source data modification.
No independent OOS claim.
No training or live trading approval.
"""

import argparse
import csv
import hashlib
import io
import json
import math
import os
import tempfile
from pathlib import Path

VERSION = "Clean V001"
TIMEFRAMES = ("h1", "h4", "d1")
SPLITS = ("DEV", "VALIDATION", "TEST")
HORIZONS = (24, 72, 168)

BASE_DIR = "pattern_256_matched_baseline"
AUDIT_DIR = "pattern_256_conservative_audit"
OUT_DIR = "pattern_256_statistical_robustness"

FIELDS = [
    "timeframe",
    "split",
    "horizon",
    "baseline_pairs",
    "conservative_pairs",
    "retention_pct",
    "baseline_difference_pp",
    "conservative_difference_pp",
    "direction_reversed",
    "signal_only_hits",
    "control_only_hits",
    "discordant_pairs",
    "paired_difference_ci95_lower_pp",
    "paired_difference_ci95_upper_pp",
    "mcnemar_exact_p",
    "holm_adjusted_p",
    "nominal_p_below_005",
    "holm_p_below_005",
    "classification",
]


def file_hash(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(
            lambda: f.read(1024 * 1024), b""
        ):
            h.update(chunk)
    return h.hexdigest()


def read_json(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def atomic_write(path, content):
    path.parent.mkdir(
        parents=True, exist_ok=True
    )

    fd, temporary = tempfile.mkstemp(
        prefix=".partial_",
        dir=str(path.parent),
    )

    try:
        with os.fdopen(
            fd, "w", encoding="utf-8",
            newline=""
        ) as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())

        os.replace(temporary, path)

    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def write_json(path, obj):
    atomic_write(
        path,
        json.dumps(
            obj,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            allow_nan=False,
        ) + "\n",
    )


def write_csv(path, rows):
    stream = io.StringIO()
    writer = csv.DictWriter(
        stream,
        fieldnames=FIELDS,
        extrasaction="ignore",
    )
    writer.writeheader()
    writer.writerows(rows)
    atomic_write(path, stream.getvalue())


def verify_manifest(folder, manifest_name):
    manifest_path = folder / manifest_name
    manifest = read_json(manifest_path)

    expected_files = manifest.get("files")

    if not isinstance(expected_files, dict):
        raise ValueError(
            f"Invalid manifest: {manifest_path}"
        )

    for filename, expected in expected_files.items():
        if Path(filename).name != filename:
            raise ValueError(
                f"Unsafe manifest filename: {filename}"
            )

        path = folder / filename

        if not path.is_file():
            raise FileNotFoundError(path)

        actual = file_hash(path)

        if actual.lower() != str(expected).lower():
            raise ValueError(
                f"SHA256 mismatch: {path}"
            )

    return {
        "manifest": str(manifest_path),
        "verified_files": len(expected_files),
        "manifest_sha256": file_hash(manifest_path),
    }


def result_index(result):
    index = {}

    for row in result["comparison"]:
        key = (
            row["timeframe"],
            row["split"],
            int(row["horizon"]),
        )

        if key in index:
            raise ValueError(
                f"Duplicate comparison: {key}"
            )

        index[key] = row

    expected = {
        (tf, split, horizon)
        for tf in TIMEFRAMES
        for split in SPLITS
        for horizon in HORIZONS
    }

    if set(index) != expected:
        raise ValueError(
            "Comparison groups do not match "
            "the expected 27 groups"
        )

    return index


def validate_counts(row):
    n = int(row["matched_pairs"])

    both = int(row["both_hits"])
    signal_only = int(row["signal_only_hits"])
    control_only = int(row["control_only_hits"])
    neither = int(row["neither_hits"])

    signal_hits = int(row["signal_hits"])
    control_hits = int(row["control_hits"])

    values = [
        n, both, signal_only,
        control_only, neither,
        signal_hits, control_hits,
    ]

    if any(x < 0 for x in values):
        raise ValueError(
            "Negative paired count"
        )

    if (
        both + signal_only
        + control_only + neither != n
    ):
        raise ValueError(
            "Paired contingency counts mismatch"
        )

    if signal_hits != both + signal_only:
        raise ValueError(
            "Signal hit counts mismatch"
        )

    if control_hits != both + control_only:
        raise ValueError(
            "Control hit counts mismatch"
        )

    if n == 0:
        if (
            row["signal_rate_pct"] is not None
            or row["control_rate_pct"] is not None
            or row["difference_pp"] is not None
        ):
            raise ValueError(
                "Zero sample has non-null rate"
            )
        return

    signal_pct = 100.0 * signal_hits / n
    control_pct = 100.0 * control_hits / n
    diff_pct = signal_pct - control_pct

    checks = (
        ("signal_rate_pct", signal_pct),
        ("control_rate_pct", control_pct),
        ("difference_pp", diff_pct),
    )

    for field, calculated in checks:
        reported = row[field]

        if reported is None:
            raise ValueError(
                f"Missing rate: {field}"
            )

        if not math.isclose(
            float(reported),
            calculated,
            abs_tol=0.000002,
            rel_tol=0,
        ):
            raise ValueError(
                f"Rate mismatch: {field}"
            )


def logsumexp(values):
    if not values:
        return float("-inf")

    maximum = max(values)

    return maximum + math.log(
        sum(math.exp(x - maximum) for x in values)
    )


def exact_mcnemar_p(signal_only, control_only):
    """
    Exact two-sided McNemar test:
    conditional Binomial(n, 0.5).

    Uses log probabilities to avoid overflow.
    """

    discordant = signal_only + control_only

    if discordant == 0:
        return 1.0

    k = min(signal_only, control_only)

    terms = []

    for i in range(k + 1):
        log_probability = (
            math.lgamma(discordant + 1)
            - math.lgamma(i + 1)
            - math.lgamma(discordant - i + 1)
            - discordant * math.log(2)
        )
        terms.append(log_probability)

    log_tail = logsumexp(terms)

    return min(
        1.0,
        2.0 * math.exp(log_tail)
    )


def paired_ci95(row):
    """
    Descriptive normal-approximation CI for
    the paired mean difference in percentage points.

    Does NOT account for market-level clustering.
    """

    n = int(row["matched_pairs"])

    if n < 2:
        return None, None

    positive = int(row["signal_only_hits"])
    negative = int(row["control_only_hits"])

    mean = (positive - negative) / n

    second_moment = (
        positive + negative
    ) / n

    sample_variance = (
        n / (n - 1)
    ) * (second_moment - mean * mean)

    standard_error = math.sqrt(
        max(0.0, sample_variance) / n
    )

    lower = 100.0 * (
        mean - 1.96 * standard_error
    )
    upper = 100.0 * (
        mean + 1.96 * standard_error
    )

    return round(lower, 6), round(upper, 6)


def sign(value):
    if value is None:
        return None

    value = float(value)

    if value > 0:
        return 1
    if value < 0:
        return -1

    return 0


def holm_adjust(rows):
    """
    Holm step-down adjustment across all
    evaluable conservative comparison groups.
    """

    candidates = [
        (i, row["mcnemar_exact_p"])
        for i, row in enumerate(rows)
        if row["mcnemar_exact_p"] is not None
    ]

    candidates.sort(key=lambda x: x[1])

    m = len(candidates)
    previous = 0.0

    for rank, (index, p_value) in enumerate(
        candidates
    ):
        adjusted = min(
            1.0,
            (m - rank) * p_value
        )

        previous = max(
            previous, adjusted
        )

        rows[index]["holm_adjusted_p"] = round(
            previous, 12
        )

        rows[index]["holm_p_below_005"] = (
            previous < 0.05
        )

    return m


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--root", required=True
    )
    args = parser.parse_args()

    root = Path(args.root).resolve()
    reports = root / "data" / "reports"

    baseline_dir = reports / BASE_DIR
    conservative_dir = reports / AUDIT_DIR
    output_dir = reports / OUT_DIR

    baseline_integrity = verify_manifest(
        baseline_dir,
        "matched_manifest.json",
    )

    conservative_integrity = verify_manifest(
        conservative_dir,
        "conservative_manifest.json",
    )

    baseline_path = (
        baseline_dir / "matched_result.json"
    )
    conservative_path = (
        conservative_dir / "conservative_result.json"
    )

    baseline = read_json(baseline_path)
    conservative = read_json(conservative_path)

    if baseline.get("execution") != "PASS":
        raise ValueError(
            "Baseline execution not PASS"
        )

    if conservative.get("execution") != "PASS":
        raise ValueError(
            "Conservative execution not PASS"
        )

    if baseline.get("version") != "Clean V001":
        raise ValueError(
            "Unexpected baseline version"
        )

    if conservative.get("version") != "Clean V002":
        raise ValueError(
            "Unexpected conservative version"
        )

    if (
        baseline.get("research_decision") != "HOLD"
        or conservative.get("research_decision") != "HOLD"
    ):
        raise ValueError(
            "Previous research gate changed"
        )

    if (
        baseline.get("input_pairs")
        != conservative.get("input_pairs")
    ):
        raise ValueError(
            "Input pair count changed"
        )

    if (
        baseline.get("source_rows")
        != conservative.get("source_rows")
    ):
        raise ValueError(
            "Source row count changed"
        )

    expected_previous_hash = conservative.get(
        "previous_matched_result_sha256"
    )

    if (
        expected_previous_hash
        != file_hash(baseline_path)
    ):
        raise ValueError(
            "Conservative audit baseline hash mismatch"
        )

    base_index = result_index(baseline)
    audit_index = result_index(conservative)

    rows = []

    for tf in TIMEFRAMES:
        for split in SPLITS:
            for horizon in HORIZONS:
                key = (tf, split, horizon)

                old = base_index[key]
                new = audit_index[key]

                validate_counts(old)
                validate_counts(new)

                old_n = int(old["matched_pairs"])
                new_n = int(new["matched_pairs"])

                if new_n > old_n:
                    raise ValueError(
                        f"Conservative pairs exceed "
                        f"baseline pairs: {key}"
                    )

                retention = (
                    round(
                        100.0 * new_n / old_n,
                        6
                    )
                    if old_n else None
                )

                old_diff = old["difference_pp"]
                new_diff = new["difference_pp"]

                reversed_direction = (
                    sign(old_diff) is not None
                    and sign(new_diff) is not None
                    and sign(old_diff) * sign(new_diff) == -1
                )

                positive = int(
                    new["signal_only_hits"]
                )
                negative = int(
                    new["control_only_hits"]
                )

                if new_n == 0:
                    p_value = None
                    ci_lower = None
                    ci_upper = None
                    classification = "NOT_EVALUABLE"
                else:
                    p_value = exact_mcnemar_p(
                        positive, negative
                    )

                    ci_lower, ci_upper = paired_ci95(
                        new
                    )

                    classification = (
                        "POSITIVE_DESCRIPTIVE"
                        if float(new_diff) > 0
                        else "NEGATIVE_DESCRIPTIVE"
                        if float(new_diff) < 0
                        else "ZERO_DIFFERENCE"
                    )

                rows.append({
                    "timeframe": tf,
                    "split": split,
                    "horizon": horizon,
                    "baseline_pairs": old_n,
                    "conservative_pairs": new_n,
                    "retention_pct": retention,
                    "baseline_difference_pp": old_diff,
                    "conservative_difference_pp": new_diff,
                    "direction_reversed": reversed_direction,
                    "signal_only_hits": positive,
                    "control_only_hits": negative,
                    "discordant_pairs": positive + negative,
                    "paired_difference_ci95_lower_pp":
                        ci_lower,
                    "paired_difference_ci95_upper_pp":
                        ci_upper,
                    "mcnemar_exact_p": (
                        round(p_value, 12)
                        if p_value is not None else None
                    ),
                    "holm_adjusted_p": None,
                    "nominal_p_below_005": (
                        p_value < 0.05
                        if p_value is not None else None
                    ),
                    "holm_p_below_005": None,
                    "classification": classification,
                })

    tested_groups = holm_adjust(rows)

    result = {
        "version": VERSION,
        "execution": "PASS",
        "classification":
            "EXPLORATORY_PAIRED_STATISTICAL_AUDIT",
        "input_pairs": baseline["input_pairs"],
        "source_rows": baseline["source_rows"],
        "baseline_result_sha256":
            file_hash(baseline_path),
        "conservative_result_sha256":
            file_hash(conservative_path),
        "baseline_integrity": baseline_integrity,
        "conservative_integrity":
            conservative_integrity,
        "comparison_groups": len(rows),
        "evaluable_groups": tested_groups,
        "not_evaluable_groups":
            len(rows) - tested_groups,
        "direction_reversal_groups": sum(
            1 for row in rows
            if row["direction_reversed"]
        ),
        "holm_family":
            "all_evaluable_conservative_groups",
        "comparison": rows,
        "limitations": [
            "TEST calendar period already exposed",
            "No independent out-of-sample proof",
            "McNemar test assumes independent pairs",
            "Market-level clustering not corrected",
            "Selection of matched pairs may bias results",
            "Previous-only controls are from earlier regimes",
            "No volatility or market-state matching",
            "Only 168-candle-complete windows included",
            "Daily timeframe has no conservative matches",
            "Intrabar high is not executable profit",
            "No fees, slippage or market impact",
            "Current OHLCV is not verified PIT data",
            "Statistical significance is not causal proof",
            "No profitability or trading approval",
        ],
        "independent_oos_proven": False,
        "point_in_time_proven": False,
        "profitability_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
    }

    output_dir.mkdir(
        parents=True, exist_ok=True
    )

    write_json(
        output_dir / "robustness_result.json",
        result,
    )

    write_csv(
        output_dir / "robustness_comparison.csv",
        rows,
    )

    lines = [
        "# Pattern 256 Statistical Robustness Audit",
        "",
        f"Version: {VERSION}",
        "Execution: PASS",
        "Research decision: HOLD",
        "",
        "## TEST results",
        "",
        "| TF | Horizon | Pairs | Retention % | "
        "Old diff pp | New diff pp | "
        "CI lower | CI upper | Holm p |",
        "|---|---:|---:|---:|---:|---:|"
        "---:|---:|---:|",
    ]

    for row in rows:
        if row["split"] != "TEST":
            continue

        lines.append(
            "| {timeframe} | {horizon} | "
            "{conservative_pairs} | {retention_pct} | "
            "{baseline_difference_pp} | "
            "{conservative_difference_pp} | "
            "{paired_difference_ci95_lower_pp} | "
            "{paired_difference_ci95_upper_pp} | "
            "{holm_adjusted_p} |".format(**row)
        )

    lines.extend([
        "",
        "## Interpretation limits",
        "",
        "These confidence intervals and p-values "
        "are exploratory only.",
        "",
        "They do not correct for market clustering, "
        "historical regime differences or "
        "previously exposed TEST data.",
        "",
        "A positive difference does not establish "
        "a profitable trading strategy.",
        "",
        "## Limitations",
        "",
    ])

    lines.extend(
        "- " + item
        for item in result["limitations"]
    )

    atomic_write(
        output_dir / "robustness_report.md",
        "\n".join(lines) + "\n",
    )

    filenames = [
        "robustness_result.json",
        "robustness_comparison.csv",
        "robustness_report.md",
    ]

    write_json(
        output_dir / "robustness_manifest.json",
        {
            "version": VERSION,
            "files": {
                name: file_hash(output_dir / name)
                for name in filenames
            },
        },
    )

    print(
        json.dumps(
            {
                "execution": "PASS",
                "evaluable_groups": tested_groups,
                "direction_reversals":
                    result["direction_reversal_groups"],
                "test_comparison": [
                    row for row in rows
                    if row["split"] == "TEST"
                ],
                "research_decision": "HOLD",
            },
            ensure_ascii=False,
            indent=2,
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
