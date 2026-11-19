
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 H1 Paired Robustness Audit
Clean V001

Research only. No trading or model approval.

Inputs:
 data/reports/pattern_256_h1_matched_controls/
   matched_controls_result.json
   matched_controls_pairs.csv
   matched_controls_unmatched.csv
   matched_controls_report.md
   matched_controls_manifest.json

Outputs:
 data/reports/pattern_256_h1_paired_robustness/
   paired_robustness_result.json
   paired_robustness_groups.csv
   paired_robustness_report.md
   paired_robustness_manifest.json
"""

import argparse
import csv
import hashlib
import io
import json
import math
import os
import random
import tempfile
from collections import defaultdict
from datetime import datetime
from pathlib import Path

VERSION = "Clean V001"
SEED = 25620261118

INPUT_NAMES = (
    "matched_controls_result.json",
    "matched_controls_pairs.csv",
    "matched_controls_unmatched.csv",
    "matched_controls_report.md",
)

GROUP_FIELDS = [
    "group_type",
    "group",
    "pairs",
    "signal_hits",
    "control_hits",
    "signal_only",
    "control_only",
    "both_hit",
    "neither_hit",
    "signal_rate_pct",
    "control_rate_pct",
    "difference_pp",
    "mcnemar_exact_p",
]


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1048576), b""):
            h.update(block)
    return h.hexdigest()


def write_atomic(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp = tempfile.mkstemp(
        prefix=".audit_", dir=str(path.parent)
    )
    try:
        with os.fdopen(
            fd, "w", encoding="utf-8", newline=""
        ) as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp, path)
    finally:
        if os.path.exists(temp):
            os.unlink(temp)


def write_json(path, obj):
    write_atomic(
        path,
        json.dumps(
            obj,
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
            allow_nan=False,
        ) + "\n",
    )


def read_json(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def csv_output(rows, fields):
    s = io.StringIO(newline="")
    w = csv.DictWriter(
        s, fieldnames=fields, lineterminator="\n"
    )
    w.writeheader()
    w.writerows(rows)
    return s.getvalue()


def check_inputs(folder):
    manifest_path = (
        folder / "matched_controls_manifest.json"
    )
    manifest = read_json(manifest_path)

    if manifest.get("version") != "Clean V001":
        raise ValueError("Input manifest version mismatch")

    files = manifest.get("files", {})
    if set(files) != set(INPUT_NAMES):
        raise ValueError("Input inventory mismatch")

    for name in INPUT_NAMES:
        path = folder / name
        if not path.is_file():
            raise FileNotFoundError(path)
        if sha256(path) != files[name]:
            raise ValueError(
                f"Input SHA256 mismatch: {name}"
            )

    result = read_json(
        folder / "matched_controls_result.json"
    )

    if (
        result.get("execution") != "PASS"
        or result.get("version") != "Clean V001"
        or result.get("classification")
        != "MATCHED_CONTROL_COMPUTED"
        or result.get("research_decision") != "HOLD"
        or result.get("timeframe") != "h1"
        or result.get("horizon_hours") != 168
    ):
        raise ValueError("Unexpected input result")

    return result, sha256(manifest_path)


def load_pairs(path):
    rows = []
    seen_ids = set()

    required = {
        "event_id",
        "signal_time_utc",
        "signal_market",
        "control_market",
        "signal_hit_30pct",
        "control_hit_30pct",
    }

    with open(
        path, encoding="utf-8-sig", newline=""
    ) as f:
        reader = csv.DictReader(f)

        if not required.issubset(
            set(reader.fieldnames or [])
        ):
            raise ValueError("Missing paired CSV columns")

        for row in reader:
            event_id = int(row["event_id"])
            if event_id in seen_ids:
                raise ValueError("Duplicate event ID")
            seen_ids.add(event_id)

            a = row["signal_hit_30pct"]
            b = row["control_hit_30pct"]
            if a not in ("0", "1") or b not in ("0", "1"):
                raise ValueError("Invalid outcome flag")

            signal_market = row["signal_market"]
            control_market = row["control_market"]
            if (
                not signal_market.startswith("KRW-")
                or not control_market.startswith("KRW-")
                or signal_market == control_market
            ):
                raise ValueError("Invalid market pair")

            timestamp = datetime.fromisoformat(
                row["signal_time_utc"].replace(
                    "Z", "+00:00"
                )
            )
            if (
                timestamp.tzinfo is None
                or timestamp.utcoffset().total_seconds()
                != 0
            ):
                raise ValueError("Timestamp must be UTC")

            rows.append({
                "event_id": event_id,
                "time": timestamp,
                "month": timestamp.strftime("%Y-%m"),
                "year": timestamp.strftime("%Y"),
                "signal_market": signal_market,
                "control_market": control_market,
                "a": int(a),
                "b": int(b),
            })

    return rows


def verify_accounting(rows, source):
    outcomes = source["paired_outcomes"]

    counts = {
        "signal_only_hits": 0,
        "control_only_hits": 0,
        "both_hit": 0,
        "neither_hit": 0,
    }

    for r in rows:
        if r["a"] and not r["b"]:
            counts["signal_only_hits"] += 1
        elif r["b"] and not r["a"]:
            counts["control_only_hits"] += 1
        elif r["a"] and r["b"]:
            counts["both_hit"] += 1
        else:
            counts["neither_hit"] += 1

    if (
        len(rows) != source["matched_pairs"]
        or counts != outcomes
        or sum(r["a"] for r in rows)
        != source["signal_hits_in_pairs"]
        or sum(r["b"] for r in rows)
        != source["control_hits_in_pairs"]
    ):
        raise ValueError("Paired accounting mismatch")

    return counts


def exact_mcnemar_p(signal_only, control_only):
    """
    Two-sided exact binomial McNemar test.
    Stable log-space computation; no scipy required.
    """
    n = signal_only + control_only
    if n == 0:
        return 1.0

    k = min(signal_only, control_only)

    # Sum lower binomial tail using recurrence in log
    # space to avoid overflow/underflow.
    terms = []
    log_two = math.log(2.0)
    for i in range(k + 1):
        log_p = (
            math.lgamma(n + 1)
            - math.lgamma(i + 1)
            - math.lgamma(n - i + 1)
            - n * log_two
        )
        terms.append(log_p)

    maximum = max(terms)
    tail = math.exp(maximum) * sum(
        math.exp(x - maximum) for x in terms
    )
    return min(1.0, 2.0 * tail)


def aggregate(rows):
    n = len(rows)
    a = sum(r["a"] for r in rows)
    b = sum(r["b"] for r in rows)

    so = sum(
        r["a"] == 1 and r["b"] == 0
        for r in rows
    )
    co = sum(
        r["a"] == 0 and r["b"] == 1
        for r in rows
    )
    both = sum(
        r["a"] == 1 and r["b"] == 1
        for r in rows
    )
    neither = n - so - co - both

    return {
        "pairs": n,
        "signal_hits": a,
        "control_hits": b,
        "signal_only": so,
        "control_only": co,
        "both_hit": both,
        "neither_hit": neither,
        "signal_rate_pct": (
            round(100 * a / n, 8) if n else None
        ),
        "control_rate_pct": (
            round(100 * b / n, 8) if n else None
        ),
        "difference_pp": (
            round(100 * (a - b) / n, 8)
            if n else None
        ),
        "mcnemar_exact_p": (
            round(exact_mcnemar_p(so, co), 12)
            if n else None
        ),
    }


def cluster_bootstrap(rows, iterations):
    """
    Calendar-month cluster bootstrap.

    All events in a sampled month are retained
    together. This partially addresses within-month
    correlation, not cross-month dependence.
    """
    by_month = defaultdict(lambda: [0, 0])

    for r in rows:
        x = by_month[r["month"]]
        x[0] += r["a"] - r["b"]
        x[1] += 1

    months = sorted(by_month)
    if len(months) < 2:
        return {
            "status": "INSUFFICIENT_MONTHS",
            "months": len(months),
        }

    rng = random.Random(SEED)
    values = []

    for _ in range(iterations):
        difference = 0
        total = 0

        for _ in months:
            month = months[
                rng.randrange(len(months))
            ]
            diff, count = by_month[month]
            difference += diff
            total += count

        values.append(100.0 * difference / total)

    values.sort()

    def percentile(p):
        pos = (len(values) - 1) * p
        lo = math.floor(pos)
        hi = math.ceil(pos)
        weight = pos - lo
        return (
            values[lo] * (1 - weight)
            + values[hi] * weight
        )

    return {
        "status": "COMPUTED",
        "cluster": "CALENDAR_MONTH",
        "months": len(months),
        "iterations": iterations,
        "seed": SEED,
        "lower_95_pp": round(
            percentile(0.025), 8
        ),
        "upper_95_pp": round(
            percentile(0.975), 8
        ),
        "contains_zero": (
            percentile(0.025) <= 0
            <= percentile(0.975)
        ),
    }


def grouped_analysis(rows):
    groups = []

    def append(group_type, name, members):
        groups.append({
            "group_type": group_type,
            "group": name,
            **aggregate(members),
        })

    by_year = defaultdict(list)
    by_month = defaultdict(list)

    for r in rows:
        by_year[r["year"]].append(r)
        by_month[r["month"]].append(r)

    for year in sorted(by_year):
        append("YEAR", year, by_year[year])

    months = sorted(by_month)
    if months:
        # Chronological calendar-month thirds,
        # not randomly shuffled event rows.
        for i in range(3):
            start = i * len(months) // 3
            end = (i + 1) * len(months) // 3
            selected = months[start:end]
            members = [
                row
                for month in selected
                for row in by_month[month]
            ]
            if selected:
                label = (
                    f"T{i + 1}:"
                    f"{selected[0]}..{selected[-1]}"
                )
                append(
                    "CHRONOLOGICAL_THIRD",
                    label,
                    members,
                )

    return groups


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True)
    parser.add_argument(
        "--bootstrap-iterations",
        type=int,
        default=5000,
    )
    args = parser.parse_args()

    if not (
        1000 <= args.bootstrap_iterations <= 50000
    ):
        raise ValueError(
            "Bootstrap iterations must be 1000..50000"
        )

    root = Path(args.root).resolve()
    input_folder = (
        root / "data" / "reports"
        / "pattern_256_h1_matched_controls"
    )
    output_folder = (
        root / "data" / "reports"
        / "pattern_256_h1_paired_robustness"
    )

    source, input_manifest_hash = check_inputs(
        input_folder
    )
    rows = load_pairs(
        input_folder / "matched_controls_pairs.csv"
    )
    counts = verify_accounting(rows, source)

    overall = aggregate(rows)
    groups = grouped_analysis(rows)
    bootstrap = cluster_bootstrap(
        rows, args.bootstrap_iterations
    )

    positive_groups = sum(
        x["difference_pp"] is not None
        and x["difference_pp"] > 0
        for x in groups
        if x["group_type"] == "YEAR"
    )
    negative_groups = sum(
        x["difference_pp"] is not None
        and x["difference_pp"] < 0
        for x in groups
        if x["group_type"] == "YEAR"
    )

    result = {
        "version": VERSION,
        "execution": "PASS",
        "classification": (
            "PAIRED_ROBUSTNESS_COMPUTED"
        ),
        "timeframe": "h1",
        "horizon_hours": 168,
        "input_manifest_sha256": (
            input_manifest_hash
        ),
        "overall": overall,
        "monthly_cluster_bootstrap": bootstrap,
        "yearly_positive_groups": positive_groups,
        "yearly_negative_groups": negative_groups,
        "yearly_zero_groups": sum(
            x["difference_pp"] == 0
            for x in groups
            if x["group_type"] == "YEAR"
        ),
        "groups_count": len(groups),
        "paired_counts_verified": counts,
        "exploratory_tests_only": True,
        "independent_oos_proven": False,
        "causal_effect_proven": False,
        "profitability_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
    }

    report = [
        "# Pattern 256 H1 Paired Robustness",
        "",
        f"Version: {VERSION}",
        "Execution: PASS",
        "Classification: PAIRED_ROBUSTNESS_COMPUTED",
        "",
        f"Matched pairs: {overall['pairs']}",
        f"Signal hits: {overall['signal_hits']}",
        f"Control hits: {overall['control_hits']}",
        f"Signal rate (%): "
        f"{overall['signal_rate_pct']}",
        f"Control rate (%): "
        f"{overall['control_rate_pct']}",
        f"Difference (pp): "
        f"{overall['difference_pp']}",
        f"McNemar exact p: "
        f"{overall['mcnemar_exact_p']}",
        "",
        "## Monthly cluster bootstrap",
        "",
        f"Status: {bootstrap['status']}",
        f"Months: {bootstrap['months']}",
    ]

    if bootstrap["status"] == "COMPUTED":
        report += [
            f"Iterations: {bootstrap['iterations']}",
            f"95% lower (pp): "
            f"{bootstrap['lower_95_pp']}",
            f"95% upper (pp): "
            f"{bootstrap['upper_95_pp']}",
            f"Contains zero: "
            f"{bootstrap['contains_zero']}",
        ]

    report += [
        "",
        "## Annual direction",
        "",
        f"Positive years: {positive_groups}",
        f"Negative years: {negative_groups}",
        "",
        "## Limitations",
        "",
        "- Exact McNemar assumes independent pairs.",
        "- Market-wide co-movement can violate "
        "that assumption.",
        "- Month clustering is an approximation.",
        "- Calendar-month clusters may still be "
        "dependent across months.",
        "- Period comparisons are exploratory.",
        "- No liquidity or volatility matching.",
        "- Historical market universe bias remains.",
        "- Full future-window eligibility may "
        "introduce selection bias.",
        "- This does not establish independent OOS.",
        "- This does not establish profitability.",
        "- No training or live trading approval.",
        "- Research decision: HOLD.",
        "",
    ]

    output_folder.mkdir(
        parents=True, exist_ok=True
    )

    result_path = (
        output_folder
        / "paired_robustness_result.json"
    )
    groups_path = (
        output_folder
        / "paired_robustness_groups.csv"
    )
    report_path = (
        output_folder
        / "paired_robustness_report.md"
    )
    manifest_path = (
        output_folder
        / "paired_robustness_manifest.json"
    )

    write_json(result_path, result)
    write_atomic(
        groups_path,
        csv_output(groups, GROUP_FIELDS),
    )
    write_atomic(
        report_path,
        "\n".join(report),
    )

    outputs = [
        result_path,
        groups_path,
        report_path,
    ]

    write_json(manifest_path, {
        "version": VERSION,
        "files": {
            p.name: sha256(p) for p in outputs
        },
    })

    print(
        json.dumps({
            "classification": (
                result["classification"]
            ),
            "overall": overall,
            "monthly_cluster_bootstrap": bootstrap,
            "yearly_positive_groups": (
                positive_groups
            ),
            "yearly_negative_groups": (
                negative_groups
            ),
            "research_decision": "HOLD",
        }, ensure_ascii=False, indent=2),
        flush=True,
    )


if __name__ == "__main__":
    main()
