
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 H1 Stability Gate
Clean V001

Read-only audit of an existing period-stability report.

Does not modify OHLCV, features, signals, or prior reports.
Does not claim independent OOS or profitability.
"""

import argparse
import hashlib
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path


VERSION = "Clean V001"

INPUT_DIR = Path(
    "data/reports/pattern_256_h1_period_stability"
)

OUTPUT_DIR = Path(
    "data/reports/pattern_256_h1_stability_gate"
)

INPUT_FILES = (
    "period_stability_result.json",
    "period_stability_report.md",
    "period_stability_manifest.json",
)


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
    with path.open("r", encoding="utf-8-sig") as handle:
        return json.load(handle)


def save_json(path, value):
    payload = json.dumps(
        value,
        ensure_ascii=False,
        indent=2,
        allow_nan=False,
    )

    path.write_text(
        payload + "\n",
        encoding="utf-8",
    )


def require(condition, message):
    if not condition:
        raise ValueError(message)


def as_number(value, name):
    require(
        isinstance(value, (int, float))
        and not isinstance(value, bool),
        f"{name}: numeric value required",
    )

    number = float(value)

    require(
        math.isfinite(number),
        f"{name}: non-finite value",
    )

    return number


def as_integer(value, name):
    number = as_number(value, name)

    require(
        number.is_integer() and number >= 0,
        f"{name}: nonnegative integer required",
    )

    return int(number)


def verify_input(folder):
    paths = {
        name: folder / name
        for name in INPUT_FILES
    }

    for name, path in paths.items():
        require(
            path.is_file(),
            f"Missing input: {name}",
        )

    manifest = load_json(
        paths["period_stability_manifest.json"]
    )

    expected_hashes = manifest.get(
        "output_sha256", {}
    )

    for name in (
        "period_stability_result.json",
        "period_stability_report.md",
    ):
        expected = expected_hashes.get(name)

        require(
            isinstance(expected, str)
            and len(expected) == 64,
            f"Missing SHA256: {name}",
        )

        actual = sha256(paths[name])

        require(
            actual.lower() == expected.lower(),
            f"SHA256 mismatch: {name}",
        )

    return paths


def validate_group(group, kind):
    require(
        isinstance(group, dict),
        f"Invalid {kind} group",
    )

    label = group.get("group")

    require(
        isinstance(label, str) and label,
        f"Missing {kind} label",
    )

    pairs = as_integer(
        group.get("pairs"),
        f"{label}.pairs",
    )

    signal_hits = as_integer(
        group.get("signal_hits"),
        f"{label}.signal_hits",
    )

    control_hits = as_integer(
        group.get("control_hits"),
        f"{label}.control_hits",
    )

    require(
        pairs > 0,
        f"{label}: zero pairs",
    )

    require(
        signal_hits <= pairs
        and control_hits <= pairs,
        f"{label}: hits exceed pairs",
    )

    calculated = (
        (signal_hits - control_hits)
        / pairs
        * 100.0
    )

    reported = as_number(
        group.get("difference_pp"),
        f"{label}.difference_pp",
    )

    require(
        abs(calculated - reported) <= 0.000001,
        f"{label}: difference mismatch",
    )

    expected_direction = (
        "POSITIVE"
        if signal_hits > control_hits
        else "NEGATIVE"
        if signal_hits < control_hits
        else "NEUTRAL"
    )

    require(
        group.get("direction") == expected_direction,
        f"{label}: direction mismatch",
    )

    return {
        "group": label,
        "pairs": pairs,
        "signal_hits": signal_hits,
        "control_hits": control_hits,
        "difference_pp": round(calculated, 8),
        "direction": expected_direction,
    }


def evaluate(result):
    require(
        result.get("execution") == "PASS",
        "Upstream execution not PASS",
    )

    require(
        result.get("timeframe") == "h1",
        "Expected h1 report",
    )

    yearly_raw = result.get("yearly_groups")
    periods_raw = result.get("chronological_groups")

    require(
        isinstance(yearly_raw, list)
        and len(yearly_raw) >= 2,
        "Insufficient yearly groups",
    )

    require(
        isinstance(periods_raw, list)
        and len(periods_raw) == 3,
        "Expected three chronological periods",
    )

    yearly = [
        validate_group(group, "YEAR")
        for group in yearly_raw
    ]

    periods = [
        validate_group(group, "PERIOD")
        for group in periods_raw
    ]

    years = [row["group"] for row in yearly]

    require(
        len(set(years)) == len(years),
        "Duplicate years",
    )

    require(
        years == sorted(years),
        "Years not chronological",
    )

    require(
        len(set(row["group"] for row in periods)) == 3,
        "Duplicate periods",
    )

    for field in (
        "pairs",
        "signal_hits",
        "control_hits",
    ):
        yearly_total = sum(
            row[field] for row in yearly
        )

        period_total = sum(
            row[field] for row in periods
        )

        require(
            yearly_total == period_total,
            f"Year/period total mismatch: {field}",
        )

    positive_years = sum(
        row["direction"] == "POSITIVE"
        for row in yearly
    )

    negative_years = sum(
        row["direction"] == "NEGATIVE"
        for row in yearly
    )

    require(
        positive_years == result.get("positive_years"),
        "Positive year count mismatch",
    )

    require(
        negative_years == result.get("negative_years"),
        "Negative year count mismatch",
    )

    require(
        len(yearly) == result.get("year_count"),
        "Year count mismatch",
    )

    first = periods[0]["difference_pp"]
    latest = periods[-1]["difference_pp"]

    require(
        abs(
            first
            - as_number(
                result.get("first_period_difference_pp"),
                "first_period_difference_pp",
            )
        ) <= 0.000001,
        "First period mismatch",
    )

    require(
        abs(
            latest
            - as_number(
                result.get("latest_period_difference_pp"),
                "latest_period_difference_pp",
            )
        ) <= 0.000001,
        "Latest period mismatch",
    )

    reported_change = as_number(
        result.get("latest_minus_first_pp"),
        "latest_minus_first_pp",
    )

    require(
        abs((latest - first) - reported_change)
        <= 0.000001,
        "Period change mismatch",
    )

    lower = as_number(
        result.get("bootstrap_95_lower_pp"),
        "bootstrap_95_lower_pp",
    )

    upper = as_number(
        result.get("bootstrap_95_upper_pp"),
        "bootstrap_95_upper_pp",
    )

    require(
        lower <= upper,
        "Invalid bootstrap interval",
    )

    excludes_zero = lower > 0 or upper < 0

    require(
        result.get("bootstrap_excludes_zero")
        is excludes_zero,
        "Bootstrap zero-exclusion mismatch",
    )

    p_value = as_number(
        result.get("mcnemar_exact_p"),
        "mcnemar_exact_p",
    )

    require(
        0 <= p_value <= 1,
        "Invalid p-value",
    )

    for key in (
        "independent_oos_proven",
        "profitability_proven",
        "causality_proven",
        "train_approved",
        "live_trading_approved",
    ):
        require(
            result.get(key) is False,
            f"Unexpected approval: {key}",
        )

    require(
        result.get("research_decision") == "HOLD",
        "Unexpected upstream research decision",
    )

    positive_periods = sum(
        row["direction"] == "POSITIVE"
        for row in periods
    )

    latest_negative = latest < 0
    performance_declined = latest < first
    statistical_uncertainty = (
        not excludes_zero or p_value >= 0.05
    )

    # Conservative descriptive classification.
    # No claim of statistical significance is inferred
    # from differences between aggregate periods.
    if latest_negative and performance_declined:
        classification = (
            "RECENT_EDGE_DECAY_OBSERVED"
        )
    elif statistical_uncertainty:
        classification = (
            "EDGE_NOT_STATISTICALLY_ESTABLISHED"
        )
    else:
        classification = (
            "FURTHER_INDEPENDENT_VALIDATION_REQUIRED"
        )

    return {
        "version": VERSION,
        "execution": "PASS",
        "classification": classification,
        "timeframe": "h1",
        "total_pairs": sum(
            row["pairs"] for row in yearly
        ),
        "year_count": len(yearly),
        "positive_years": positive_years,
        "negative_years": negative_years,
        "positive_periods": positive_periods,
        "period_count": len(periods),
        "first_period_difference_pp": first,
        "latest_period_difference_pp": latest,
        "latest_minus_first_pp": round(
            latest - first, 8
        ),
        "latest_period_negative": latest_negative,
        "performance_declined": performance_declined,
        "bootstrap_95_lower_pp": lower,
        "bootstrap_95_upper_pp": upper,
        "bootstrap_excludes_zero": excludes_zero,
        "mcnemar_exact_p": p_value,
        "statistical_uncertainty": statistical_uncertainty,
        "yearly_groups": yearly,
        "chronological_groups": periods,
        "independent_oos_proven": False,
        "profitability_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
        "limitations": [
            "Aggregate report revalidation only",
            "No independent OOS evaluation",
            "No event-level bootstrap recomputation",
            "No proof of regime-shift significance",
            "No trading cost or slippage evaluation",
            "No strategy approval",
        ],
    }


def make_report(result):
    lines = [
        "# Pattern 256 H1 Stability Gate",
        "",
        f"Version: {VERSION}",
        f"Execution: {result['execution']}",
        f"Classification: {result['classification']}",
        "",
        "## Annual results",
        "",
        "| Year | Pairs | Difference pp | Direction |",
        "|---|---:|---:|---|",
    ]

    for row in result["yearly_groups"]:
        lines.append(
            f"| {row['group']} | {row['pairs']} "
            f"| {row['difference_pp']:.8f} "
            f"| {row['direction']} |"
        )

    lines.extend([
        "",
        "## Chronological periods",
        "",
        "| Period | Pairs | Difference pp | Direction |",
        "|---|---:|---:|---|",
    ])

    for row in result["chronological_groups"]:
        lines.append(
            f"| {row['group']} | {row['pairs']} "
            f"| {row['difference_pp']:.8f} "
            f"| {row['direction']} |"
        )

    lines.extend([
        "",
        "## Stability assessment",
        "",
        f"- First period difference pp: "
        f"{result['first_period_difference_pp']:.8f}",
        f"- Latest period difference pp: "
        f"{result['latest_period_difference_pp']:.8f}",
        f"- Change pp: "
        f"{result['latest_minus_first_pp']:.8f}",
        f"- Positive years: {result['positive_years']}",
        f"- Negative years: {result['negative_years']}",
        f"- McNemar p: {result['mcnemar_exact_p']}",
        f"- Bootstrap 95% interval: "
        f"[{result['bootstrap_95_lower_pp']}, "
        f"{result['bootstrap_95_upper_pp']}]",
        "",
        "## Decision",
        "",
        "- Research decision: HOLD",
        "- Independent OOS: NOT PROVEN",
        "- Profitability: NOT PROVEN",
        "- Training: NOT APPROVED",
        "- Live trading: NOT APPROVED",
        "",
        "This is an aggregate historical audit, "
        "not a new independent test.",
        "",
    ])

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--root",
        default=".",
        help="Persistent project root",
    )

    args = parser.parse_args()

    root = Path(args.root).expanduser().resolve()
    source = root / INPUT_DIR
    destination = root / OUTPUT_DIR

    paths = verify_input(source)

    upstream = load_json(
        paths["period_stability_result.json"]
    )

    result = evaluate(upstream)

    destination.mkdir(
        parents=True,
        exist_ok=True,
    )

    result_path = destination / "stability_gate_result.json"
    report_path = destination / "stability_gate_report.md"
    manifest_path = destination / "stability_gate_manifest.json"

    save_json(result_path, result)

    report_path.write_text(
        make_report(result),
        encoding="utf-8",
    )

    manifest = {
        "version": VERSION,
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "input_sha256": {
            name: sha256(path)
            for name, path in paths.items()
        },
        "output_sha256": {
            result_path.name: sha256(result_path),
            report_path.name: sha256(report_path),
        },
    }

    save_json(manifest_path, manifest)

    print("==================================")
    print("Pattern 256 H1 Stability Gate")
    print("==================================")
    print("Execution: PASS")
    print(
        "Classification:",
        result["classification"],
    )
    print(
        "Latest period difference pp:",
        result["latest_period_difference_pp"],
    )
    print(
        "Research decision:",
        result["research_decision"],
    )
    print("SHA256 manifest generated")
    print("Independent OOS: NOT PROVEN")
    print("Profitability: NOT PROVEN")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(
            f"[FAIL] {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        sys.exit(1)
