
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Temporal Holdout Statistical Gate
Clean V001

Research only.
No trading, no orders, no notifications.

This script:
- Reads the temporal holdout comparison CSV.
- Validates all counts and reported percentages.
- Calculates Wilson confidence intervals.
- Calculates an exploratory two-proportion z statistic.
- Applies a Bonferroni correction to TEST comparisons.
- Flags small samples and deteriorating results.
- Does not claim statistical independence.
- Does not approve live trading.

Important:
The underlying signal observations may overlap.
Therefore the exploratory z-test cannot establish
independent statistical significance.
"""

import argparse
import csv
import hashlib
import json
import math
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path


VERSION = "Clean V001"

SOURCE_RELATIVE = (
    "data/reports/pattern_256_temporal_holdout/"
    "temporal_holdout_comparison.csv"
)

OUTPUT_RELATIVE = (
    "data/reports/pattern_256_holdout_statistical_gate"
)

OUTPUT_CSV = "statistical_gate_comparison.csv"
OUTPUT_JSON = "statistical_gate_result.json"
OUTPUT_MANIFEST = "statistical_gate_manifest.json"

EXPECTED_PARTITIONS = ("TRAIN", "TEST")
EXPECTED_TIMEFRAMES = ("h1", "h4", "d1")
EXPECTED_HORIZONS = (24, 72, 168)

REQUIRED_COLUMNS = (
    "partition",
    "timeframe",
    "horizon",
    "baseline_completed",
    "baseline_hits",
    "baseline_rate_pct",
    "combined_completed",
    "combined_hits",
    "combined_rate_pct",
    "difference_pp",
    "status",
)

MIN_COMBINED_SAMPLE = 100
MIN_COMBINED_HITS = 10

# The 9 TEST comparisons are treated as one exploratory family.
ALPHA = 0.05
BONFERRONI_TESTS = 9


def fail(message):
    raise ValueError(message)


def sha256_file(path):
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        while True:
            block = handle.read(1024 * 1024)

            if not block:
                break

            digest.update(block)

    return digest.hexdigest()


def atomic_write(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)

    descriptor, temporary_name = tempfile.mkstemp(
        prefix=".statistical_gate_",
        suffix=".tmp",
        dir=str(path.parent),
    )

    temporary_path = Path(temporary_name)

    try:
        with os.fdopen(
            descriptor,
            "w",
            encoding="utf-8",
            newline="",
        ) as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())

        os.replace(temporary_path, path)

    finally:
        if temporary_path.exists():
            temporary_path.unlink()


def atomic_json(path, payload):
    text = json.dumps(
        payload,
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
        allow_nan=False,
    )

    atomic_write(path, text + "\n")


def read_integer(value, field, row_number):
    try:
        number = int(value)
    except (ValueError, TypeError):
        fail(
            f"Row {row_number}: invalid integer "
            f"for {field}: {value}"
        )

    if number < 0:
        fail(
            f"Row {row_number}: negative value "
            f"for {field}"
        )

    return number


def read_float(value, field, row_number):
    try:
        number = float(value)
    except (ValueError, TypeError):
        fail(
            f"Row {row_number}: invalid float "
            f"for {field}: {value}"
        )

    if not math.isfinite(number):
        fail(
            f"Row {row_number}: non-finite "
            f"value for {field}"
        )

    return number


def rate_pct(hits, completed):
    if completed == 0:
        return None

    return 100.0 * hits / completed


def wilson_interval(hits, completed, z=1.959963984540054):
    if completed == 0:
        return None, None

    p = hits / completed
    z2 = z * z

    denominator = 1.0 + z2 / completed

    center = (
        p + z2 / (2.0 * completed)
    ) / denominator

    margin = (
        z
        * math.sqrt(
            p * (1.0 - p) / completed
            + z2 / (4.0 * completed * completed)
        )
        / denominator
    )

    lower = max(0.0, center - margin)
    upper = min(1.0, center + margin)

    return lower * 100.0, upper * 100.0


def exploratory_z_test(
    baseline_hits,
    baseline_total,
    combined_hits,
    combined_total,
):
    """
    Exploratory unpaired two-proportion z-test.

    The independent-observation assumption is NOT
    established by the supplied aggregate data.

    Results are diagnostic only.
    """

    if baseline_total == 0 or combined_total == 0:
        return None, None

    p1 = baseline_hits / baseline_total
    p2 = combined_hits / combined_total

    pooled = (
        baseline_hits + combined_hits
    ) / (
        baseline_total + combined_total
    )

    variance = (
        pooled
        * (1.0 - pooled)
        * (
            1.0 / baseline_total
            + 1.0 / combined_total
        )
    )

    if variance <= 0.0:
        return None, None

    z_value = (p2 - p1) / math.sqrt(variance)

    p_value = math.erfc(
        abs(z_value) / math.sqrt(2.0)
    )

    return z_value, p_value


def rounded(value):
    if value is None:
        return ""

    return round(value, 8)


def validate_reported_rate(
    reported,
    calculated,
    field,
    row_number,
):
    if calculated is None:
        fail(
            f"Row {row_number}: zero sample "
            f"cannot have a valid rate"
        )

    # Source percentages are rounded to six decimals.
    if abs(reported - calculated) > 0.000002:
        fail(
            f"Row {row_number}: {field} mismatch. "
            f"reported={reported}, "
            f"calculated={calculated}"
        )


def load_source(path):
    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as handle:
        reader = csv.DictReader(handle)

        if not reader.fieldnames:
            fail("CSV header missing")

        missing = set(REQUIRED_COLUMNS) - set(
            reader.fieldnames
        )

        if missing:
            fail(
                "Missing CSV columns: "
                + ", ".join(sorted(missing))
            )

        rows = list(reader)

    if len(rows) != 18:
        fail(
            f"Expected 18 comparison rows, "
            f"found {len(rows)}"
        )

    parsed = []
    observed_keys = set()

    for row_number, row in enumerate(rows, start=2):
        partition = row["partition"].strip()
        timeframe = row["timeframe"].strip()

        horizon = read_integer(
            row["horizon"],
            "horizon",
            row_number,
        )

        if partition not in EXPECTED_PARTITIONS:
            fail(
                f"Row {row_number}: invalid partition"
            )

        if timeframe not in EXPECTED_TIMEFRAMES:
            fail(
                f"Row {row_number}: invalid timeframe"
            )

        if horizon not in EXPECTED_HORIZONS:
            fail(
                f"Row {row_number}: invalid horizon"
            )

        key = (partition, timeframe, horizon)

        if key in observed_keys:
            fail(f"Duplicate comparison: {key}")

        observed_keys.add(key)

        baseline_total = read_integer(
            row["baseline_completed"],
            "baseline_completed",
            row_number,
        )

        baseline_hits = read_integer(
            row["baseline_hits"],
            "baseline_hits",
            row_number,
        )

        combined_total = read_integer(
            row["combined_completed"],
            "combined_completed",
            row_number,
        )

        combined_hits = read_integer(
            row["combined_hits"],
            "combined_hits",
            row_number,
        )

        if baseline_hits > baseline_total:
            fail(
                f"Row {row_number}: baseline hits "
                f"exceed completed"
            )

        if combined_hits > combined_total:
            fail(
                f"Row {row_number}: combined hits "
                f"exceed completed"
            )

        if baseline_total == 0 or combined_total == 0:
            fail(
                f"Row {row_number}: zero completed "
                f"observations"
            )

        baseline_rate = rate_pct(
            baseline_hits,
            baseline_total,
        )

        combined_rate = rate_pct(
            combined_hits,
            combined_total,
        )

        difference = combined_rate - baseline_rate

        validate_reported_rate(
            read_float(
                row["baseline_rate_pct"],
                "baseline_rate_pct",
                row_number,
            ),
            baseline_rate,
            "baseline_rate_pct",
            row_number,
        )

        validate_reported_rate(
            read_float(
                row["combined_rate_pct"],
                "combined_rate_pct",
                row_number,
            ),
            combined_rate,
            "combined_rate_pct",
            row_number,
        )

        validate_reported_rate(
            read_float(
                row["difference_pp"],
                "difference_pp",
                row_number,
            ),
            difference,
            "difference_pp",
            row_number,
        )

        parsed.append(
            {
                "partition": partition,
                "timeframe": timeframe,
                "horizon": horizon,
                "baseline_completed": baseline_total,
                "baseline_hits": baseline_hits,
                "combined_completed": combined_total,
                "combined_hits": combined_hits,
                "baseline_rate_pct": baseline_rate,
                "combined_rate_pct": combined_rate,
                "difference_pp": difference,
                "source_status": row["status"].strip(),
            }
        )

    expected_keys = {
        (partition, timeframe, horizon)
        for partition in EXPECTED_PARTITIONS
        for timeframe in EXPECTED_TIMEFRAMES
        for horizon in EXPECTED_HORIZONS
    }

    if observed_keys != expected_keys:
        fail("Comparison matrix is incomplete")

    return sorted(
        parsed,
        key=lambda row: (
            row["partition"] != "TRAIN",
            EXPECTED_TIMEFRAMES.index(
                row["timeframe"]
            ),
            row["horizon"],
        ),
    )


def analyze(rows):
    results = []

    for row in rows:
        baseline_low, baseline_high = wilson_interval(
            row["baseline_hits"],
            row["baseline_completed"],
        )

        combined_low, combined_high = wilson_interval(
            row["combined_hits"],
            row["combined_completed"],
        )

        z_value, p_value = exploratory_z_test(
            row["baseline_hits"],
            row["baseline_completed"],
            row["combined_hits"],
            row["combined_completed"],
        )

        corrected_p = None

        if p_value is not None:
            corrected_p = min(
                1.0,
                p_value * BONFERRONI_TESTS,
            )

        sample_ok = (
            row["combined_completed"]
            >= MIN_COMBINED_SAMPLE
            and row["combined_hits"]
            >= MIN_COMBINED_HITS
        )

        if not sample_ok:
            assessment = "SMALL_SAMPLE"

        elif row["difference_pp"] < 0:
            assessment = "OBSERVED_DETERIORATION"

        elif row["difference_pp"] == 0:
            assessment = "NO_OBSERVED_DIFFERENCE"

        else:
            assessment = "OBSERVED_IMPROVEMENT"

        exploratory_threshold_met = (
            row["partition"] == "TEST"
            and sample_ok
            and row["difference_pp"] > 0
            and corrected_p is not None
            and corrected_p < ALPHA
        )

        output = dict(row)

        output.update(
            {
                "baseline_wilson_low_pct": baseline_low,
                "baseline_wilson_high_pct": baseline_high,
                "combined_wilson_low_pct": combined_low,
                "combined_wilson_high_pct": combined_high,
                "exploratory_z": z_value,
                "exploratory_p": p_value,
                "exploratory_bonferroni_p": corrected_p,
                "minimum_sample_gate": sample_ok,
                "exploratory_threshold_met": (
                    exploratory_threshold_met
                ),
                "assessment": assessment,
                "statistical_independence_proven": False,
                "approved_for_live_trading": False,
            }
        )

        results.append(output)

    return results


def make_csv(rows):
    columns = [
        "partition",
        "timeframe",
        "horizon",
        "baseline_completed",
        "baseline_hits",
        "baseline_rate_pct",
        "baseline_wilson_low_pct",
        "baseline_wilson_high_pct",
        "combined_completed",
        "combined_hits",
        "combined_rate_pct",
        "combined_wilson_low_pct",
        "combined_wilson_high_pct",
        "difference_pp",
        "exploratory_z",
        "exploratory_p",
        "exploratory_bonferroni_p",
        "minimum_sample_gate",
        "exploratory_threshold_met",
        "assessment",
        "source_status",
        "statistical_independence_proven",
        "approved_for_live_trading",
    ]

    import io

    buffer = io.StringIO(newline="")

    writer = csv.DictWriter(
        buffer,
        fieldnames=columns,
        lineterminator="\n",
    )

    writer.writeheader()

    for row in rows:
        output = {}

        for column in columns:
            value = row.get(column)

            if isinstance(value, float):
                output[column] = rounded(value)

            elif value is None:
                output[column] = ""

            else:
                output[column] = value

        writer.writerow(output)

    return buffer.getvalue()


def make_result(rows, source_hash):
    test_rows = [
        row
        for row in rows
        if row["partition"] == "TEST"
    ]

    train_rows = [
        row
        for row in rows
        if row["partition"] == "TRAIN"
    ]

    improved = sum(
        row["difference_pp"] > 0
        for row in test_rows
    )

    deteriorated = sum(
        row["difference_pp"] < 0
        for row in test_rows
    )

    small_samples = sum(
        not row["minimum_sample_gate"]
        for row in test_rows
    )

    exploratory_positive = sum(
        row["exploratory_threshold_met"]
        for row in test_rows
    )

    by_timeframe = {}

    for timeframe in EXPECTED_TIMEFRAMES:
        selected = [
            row
            for row in test_rows
            if row["timeframe"] == timeframe
        ]

        by_timeframe[timeframe] = {
            "comparisons": len(selected),
            "improved": sum(
                row["difference_pp"] > 0
                for row in selected
            ),
            "deteriorated": sum(
                row["difference_pp"] < 0
                for row in selected
            ),
            "small_samples": sum(
                not row["minimum_sample_gate"]
                for row in selected
            ),
        }

    return {
        "version": VERSION,
        "execution_status": "PASS",
        "research_decision": "HOLD",
        "source_sha256": source_hash,
        "comparison_rows": len(rows),
        "train_comparisons": len(train_rows),
        "test_comparisons": len(test_rows),
        "test_observed_improvements": improved,
        "test_observed_deteriorations": deteriorated,
        "test_small_samples": small_samples,
        "test_exploratory_threshold_met": (
            exploratory_positive
        ),
        "test_by_timeframe": by_timeframe,
        "minimum_combined_sample": MIN_COMBINED_SAMPLE,
        "minimum_combined_hits": MIN_COMBINED_HITS,
        "exploratory_alpha": ALPHA,
        "bonferroni_test_count": BONFERRONI_TESTS,
        "statistical_independence_proven": False,
        "independent_oos_proven": False,
        "profitability_proven": False,
        "approved_for_live_trading": False,
        "next_required_validation": (
            "Event-level deduplication, "
            "signal-overlap analysis, "
            "market-cluster inference, "
            "and prospective validation"
        ),
        "caution": (
            "Aggregate comparison rows do not "
            "establish independent observations. "
            "Exploratory p-values and Wilson "
            "intervals assume independent trials "
            "and must not be treated as proof."
        ),
        "generated_at_utc": datetime.now(
            timezone.utc
        ).isoformat(),
    }


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--root",
        default=".",
        help="Project root",
    )

    args = parser.parse_args()

    root = Path(args.root).resolve()

    source = (root / SOURCE_RELATIVE).resolve()

    output_dir = (
        root / OUTPUT_RELATIVE
    ).resolve()

    if not source.is_file():
        fail(f"Source CSV not found: {source}")

    if output_dir == source.parent:
        fail("Output directory overlaps source")

    if output_dir == root:
        fail("Unsafe output directory")

    source_hash = sha256_file(source)

    rows = load_source(source)

    analyzed = analyze(rows)

    result = make_result(
        analyzed,
        source_hash,
    )

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    comparison_path = output_dir / OUTPUT_CSV
    result_path = output_dir / OUTPUT_JSON
    manifest_path = output_dir / OUTPUT_MANIFEST

    atomic_write(
        comparison_path,
        make_csv(analyzed),
    )

    atomic_json(
        result_path,
        result,
    )

    manifest = {
        "version": VERSION,
        "source_sha256": source_hash,
        "output_sha256": {
            OUTPUT_CSV: sha256_file(
                comparison_path
            ),
            OUTPUT_JSON: sha256_file(
                result_path
            ),
        },
    }

    atomic_json(
        manifest_path,
        manifest,
    )

    # Verify files after writing.
    for filename, expected in (
        manifest["output_sha256"].items()
    ):
        actual = sha256_file(
            output_dir / filename
        )

        if actual != expected:
            fail(
                f"Output SHA256 mismatch: "
                f"{filename}"
            )

    print("=" * 60)
    print("Pattern 256 Holdout Statistical Gate")
    print(f"Version: {VERSION}")
    print("Execution: PASS")
    print("Research decision: HOLD")
    print(
        "TEST improvements:",
        result["test_observed_improvements"],
    )
    print(
        "TEST deteriorations:",
        result["test_observed_deteriorations"],
    )
    print(
        "TEST small samples:",
        result["test_small_samples"],
    )
    print(
        "Independent OOS proven: False"
    )
    print(
        "Live trading approved: False"
    )
    print(f"Output: {output_dir}")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())

    except Exception as error:
        print(
            f"FATAL: {type(error).__name__}: "
            f"{error}",
            file=sys.stderr,
        )

        sys.exit(1)
