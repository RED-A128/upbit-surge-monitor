
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 H1 Period Stability Audit
Clean V001

Read-only audit of previously generated paired robustness
aggregate reports.

Does NOT establish independent OOS, causality or profitability.
Does NOT modify source OHLCV, features or signals.
"""

import argparse
import csv
import hashlib
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path

VERSION = "Clean V001"

INPUT_NAMES = (
    "paired_robustness_groups.csv",
    "paired_robustness_result.json",
    "paired_robustness_report.md",
    "paired_robustness_manifest.json",
)


def sha256(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def read_json(path):
    with path.open("r", encoding="utf-8-sig") as f:
        return json.load(f)


def read_groups(path):
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def as_int(value):
    return int(value)


def as_float(value):
    return float(value)


def validate_inputs(folder):
    paths = {name: folder / name for name in INPUT_NAMES}

    for name, path in paths.items():
        if not path.is_file():
            raise RuntimeError("Missing input: " + str(path))

    manifest = read_json(paths["paired_robustness_manifest.json"])
    recorded = manifest.get("files", {})

    for name in INPUT_NAMES:
        if name == "paired_robustness_manifest.json":
            continue

        expected = recorded.get(name)
        actual = sha256(paths[name])

        if not expected or expected.lower() != actual.lower():
            raise RuntimeError("SHA256 mismatch: " + name)

    result = read_json(paths["paired_robustness_result.json"])
    groups = read_groups(paths["paired_robustness_groups.csv"])

    if result.get("execution") != "PASS":
        raise RuntimeError("Upstream execution is not PASS")

    if result.get("timeframe") != "h1":
        raise RuntimeError("Expected H1 report")

    if result.get("research_decision") != "HOLD":
        raise RuntimeError("Unexpected upstream research decision")

    if not groups:
        raise RuntimeError("Empty group report")

    return result, groups, paths


def verify_group(row):
    pairs = as_int(row["pairs"])
    signal_hits = as_int(row["signal_hits"])
    control_hits = as_int(row["control_hits"])

    signal_only = as_int(row["signal_only"])
    control_only = as_int(row["control_only"])
    both = as_int(row["both_hit"])
    neither = as_int(row["neither_hit"])

    if min(
        pairs, signal_hits, control_hits,
        signal_only, control_only, both, neither
    ) < 0:
        raise RuntimeError("Negative counts: " + row["group"])

    if signal_only + control_only + both + neither != pairs:
        raise RuntimeError("Pair count mismatch: " + row["group"])

    if signal_only + both != signal_hits:
        raise RuntimeError("Signal count mismatch: " + row["group"])

    if control_only + both != control_hits:
        raise RuntimeError("Control count mismatch: " + row["group"])

    if pairs == 0:
        raise RuntimeError("Zero pairs: " + row["group"])

    calculated = 100.0 * (signal_hits - control_hits) / pairs
    reported = as_float(row["difference_pp"])

    if not math.isclose(calculated, reported, abs_tol=0.000001):
        raise RuntimeError("Difference mismatch: " + row["group"])

    return {
        "group_type": row["group_type"],
        "group": row["group"],
        "pairs": pairs,
        "signal_hits": signal_hits,
        "control_hits": control_hits,
        "difference_pp": round(calculated, 8),
        "direction": (
            "POSITIVE" if calculated > 0
            else "NEGATIVE" if calculated < 0
            else "ZERO"
        ),
    }


def audit(result, groups):
    verified = [verify_group(row) for row in groups]

    years = [
        row for row in verified
        if row["group_type"] == "YEAR"
    ]

    thirds = [
        row for row in verified
        if row["group_type"] == "CHRONOLOGICAL_THIRD"
    ]

    if not years or len(thirds) != 3:
        raise RuntimeError("Expected year and three chronological groups")

    years.sort(key=lambda row: row["group"])
    thirds.sort(key=lambda row: row["group"])

    overall = result["overall"]

    for field in ("pairs", "signal_hits", "control_hits"):
        total = sum(row[field] for row in years)
        if total != as_int(overall[field]):
            raise RuntimeError("Year totals mismatch: " + field)

        third_total = sum(row[field] for row in thirds)
        if third_total != as_int(overall[field]):
            raise RuntimeError("Chronological totals mismatch: " + field)

    positive_years = sum(
        row["direction"] == "POSITIVE" for row in years
    )
    negative_years = sum(
        row["direction"] == "NEGATIVE" for row in years
    )

    first = thirds[0]["difference_pp"]
    last = thirds[-1]["difference_pp"]

    bootstrap = result["monthly_cluster_bootstrap"]
    lower = as_float(bootstrap["lower_95_pp"])
    upper = as_float(bootstrap["upper_95_pp"])

    if lower > upper:
        raise RuntimeError("Invalid bootstrap interval")

    if (lower <= 0 <= upper) != bool(bootstrap["contains_zero"]):
        raise RuntimeError("Bootstrap zero inclusion mismatch")

    latest_positive = last > 0
    interval_excludes_zero = lower > 0 or upper < 0
    all_thirds_positive = all(
        row["difference_pp"] > 0 for row in thirds
    )

    stable_positive = (
        all_thirds_positive
        and latest_positive
        and lower > 0
    )

    return {
        "version": VERSION,
        "execution": "PASS",
        "classification": (
            "STABLE_POSITIVE_EXPLORATORY"
            if stable_positive
            else "POSITIVE_EDGE_NOT_STABLY_ESTABLISHED"
        ),
        "timeframe": "h1",
        "year_count": len(years),
        "positive_years": positive_years,
        "negative_years": negative_years,
        "chronological_groups": thirds,
        "yearly_groups": years,
        "first_period_difference_pp": first,
        "latest_period_difference_pp": last,
        "latest_minus_first_pp": round(last - first, 8),
        "all_periods_positive": all_thirds_positive,
        "bootstrap_95_lower_pp": lower,
        "bootstrap_95_upper_pp": upper,
        "bootstrap_excludes_zero": interval_excludes_zero,
        "mcnemar_exact_p": overall["mcnemar_exact_p"],
        "independent_oos_proven": False,
        "profitability_proven": False,
        "causality_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
        "limitations": [
            "Aggregate data only",
            "No independent OOS evaluation",
            "No liquidity or volatility matching",
            "Market clustering remains unresolved",
            "Historical market universe bias remains",
            "Chronological thirds are descriptive, not independent tests",
        ],
    }


def write_report(path, output):
    lines = [
        "# Pattern 256 H1 Period Stability Audit",
        "",
        "Version: " + VERSION,
        "Execution: PASS",
        "Classification: " + output["classification"],
        "",
        "## Annual stability",
        "",
        "| Year | Pairs | Difference (pp) | Direction |",
        "|---|---:|---:|---|",
    ]

    for row in output["yearly_groups"]:
        lines.append(
            f"| {row['group']} | {row['pairs']} | "
            f"{row['difference_pp']:.8f} | {row['direction']} |"
        )

    lines.extend([
        "",
        "## Chronological stability",
        "",
        "| Period | Pairs | Difference (pp) | Direction |",
        "|---|---:|---:|---|",
    ])

    for row in output["chronological_groups"]:
        lines.append(
            f"| {row['group']} | {row['pairs']} | "
            f"{row['difference_pp']:.8f} | {row['direction']} |"
        )

    lines.extend([
        "",
        "## Statistical context",
        "",
        f"- McNemar p: {output['mcnemar_exact_p']}",
        f"- Bootstrap lower: {output['bootstrap_95_lower_pp']}",
        f"- Bootstrap upper: {output['bootstrap_95_upper_pp']}",
        f"- Bootstrap excludes zero: {output['bootstrap_excludes_zero']}",
        "",
        "## Decision",
        "",
        "- Research decision: HOLD",
        "- Independent OOS: NOT PROVEN",
        "- Profitability: NOT PROVEN",
        "- Training: NOT APPROVED",
        "- Live trading: NOT APPROVED",
        "",
    ])

    path.write_text("\n".join(lines), encoding="utf-8")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True)
    args = parser.parse_args()

    root = Path(args.root).resolve()

    input_folder = (
        root / "data" / "reports"
        / "pattern_256_h1_paired_robustness"
    )

    output_folder = (
        root / "data" / "reports"
        / "pattern_256_h1_period_stability"
    )

    result, groups, paths = validate_inputs(input_folder)
    output = audit(result, groups)

    output_folder.mkdir(parents=True, exist_ok=True)

    result_path = output_folder / "period_stability_result.json"
    report_path = output_folder / "period_stability_report.md"
    manifest_path = output_folder / "period_stability_manifest.json"

    result_path.write_text(
        json.dumps(output, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    write_report(report_path, output)

    manifest = {
        "version": VERSION,
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "input_sha256": {
            name: sha256(path)
            for name, path in paths.items()
        },
        "output_sha256": {
            result_path.name: sha256(result_path),
            report_path.name: sha256(report_path),
        },
    }

    manifest_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    print(json.dumps({
        "execution": output["execution"],
        "classification": output["classification"],
        "positive_years": output["positive_years"],
        "negative_years": output["negative_years"],
        "latest_period_difference_pp":
            output["latest_period_difference_pp"],
        "research_decision": output["research_decision"],
    }, indent=2))

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print("[ERROR]", str(exc), file=sys.stderr)
        sys.exit(1)
