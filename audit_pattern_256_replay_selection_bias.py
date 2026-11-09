
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Pattern 256 replay selection and hypothesis audit.

Clean V001.
Audits existing replay output, not predictive performance.
No training or live trading approval.
"""

import argparse
import csv
import hashlib
import json
import math
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

VERSION = "Clean V001"
AUDIT = "pattern_256_replay_selection_bias"

HYPOTHESES = (
    "A_CLOSE_INCLUDE",
    "B_CLOSE_EXCLUDE",
    "C_NEXT_OPEN_INCLUDE",
    "D_NEXT_OPEN_EXCLUDE",
)

OUTPUT_FIELDS = (
    "event_id",
    "source",
    "market",
    "timeframe",
    "classification",
    "window_status",
    "horizon_bars",
    "complete",
    "statuses",
    "all_hypotheses_hit",
    "all_first_hit_times_equal",
    "first_hit_time_variants",
    "first_hit_bar_variants",
    "entry_price_variants",
    "audit_status",
    "issues",
)


def sha256(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def load_json(path):
    with path.open("r", encoding="utf-8-sig") as f:
        return json.load(f)


def save_json(path, data):
    with path.open("w", encoding="utf-8") as f:
        json.dump(
            data, f, ensure_ascii=False,
            indent=2, sort_keys=True
        )
        f.write("\n")


def as_float(value):
    try:
        n = float(value)
        return n if math.isfinite(n) else None
    except (TypeError, ValueError):
        return None


def read_input(folder):
    names = (
        "independent_outcome_detail.csv",
        "independent_outcome_result.json",
        "independent_outcome_manifest.json",
    )
    paths = {n: folder / n for n in names}

    for name, path in paths.items():
        if not path.is_file():
            raise FileNotFoundError(name)

    result = load_json(paths[names[1]])
    manifest = load_json(paths[names[2]])

    if (
        result.get("audit")
        != "pattern_256_outcome_replay_independent"
        or result.get("execution") != "PASS"
        or manifest.get("execution") != "PASS"
    ):
        raise ValueError("Previous replay not verified PASS")

    for name in names[:2]:
        expected = (
            manifest.get("output_sha256", {})
            .get(name, "")
        )
        if not expected or sha256(paths[name]) != expected:
            raise ValueError("Input SHA256 mismatch: " + name)

    with paths[names[0]].open(
        "r", encoding="utf-8-sig", newline=""
    ) as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    required = {
        "source_index", "source", "market", "timeframe",
        "classification", "window_status", "entry_epoch",
        "horizon_bars", "hypothesis", "entry_price",
        "first_hit_offset", "first_hit_bar_1based",
        "first_hit_utc", "status", "missing_bars"
    }

    if not rows or not required.issubset(rows[0]):
        raise ValueError("Missing required detail columns")

    expected_rows = result.get("hypothesis_rows")
    if len(rows) != expected_rows:
        raise ValueError("Input row count mismatch")

    return rows, result, paths


def inspect_event(key, rows):
    problems = []

    by_hypothesis = defaultdict(list)
    for row in rows:
        by_hypothesis[row["hypothesis"]].append(row)

    if set(by_hypothesis) != set(HYPOTHESES):
        problems.append("HYPOTHESIS_SET_MISMATCH")

    if any(
        len(by_hypothesis.get(h, [])) != 1
        for h in HYPOTHESES
    ):
        problems.append("DUPLICATE_OR_MISSING_HYPOTHESIS")

    first = rows[0]
    metadata = (
        "source", "market", "timeframe",
        "classification", "window_status",
        "entry_epoch", "horizon_bars"
    )

    for col in metadata:
        if len({r[col] for r in rows}) != 1:
            problems.append("INCONSISTENT_" + col.upper())

    complete = (
        first["window_status"] == "PRICE_WINDOW_COMPLETE"
    )

    statuses = {
        h: by_hypothesis[h][0]["status"]
        for h in HYPOTHESES
        if len(by_hypothesis.get(h, [])) == 1
    }

    if complete:
        if any(
            statuses.get(h) not in ("HIT", "NO_HIT")
            for h in HYPOTHESES
        ):
            problems.append("COMPLETE_EVENT_NOT_COMPARABLE")
    else:
        if any(
            statuses.get(h) in ("HIT", "NO_HIT")
            for h in HYPOTHESES
        ):
            problems.append("INCOMPLETE_EVENT_COUNTED_AS_OUTCOME")

    hit_rows = [
        r for r in rows if r["status"] == "HIT"
    ]

    for row in hit_rows:
        price = as_float(row["entry_price"])
        offset = as_float(row["first_hit_offset"])
        bar = as_float(row["first_hit_bar_1based"])

        if price is None or price <= 0:
            problems.append("INVALID_HIT_ENTRY_PRICE")

        if (
            offset is None or bar is None
            or offset < 0 or bar < 1
        ):
            problems.append("INVALID_FIRST_HIT_BAR")

        if not row["first_hit_utc"]:
            problems.append("MISSING_FIRST_HIT_TIME")

    first_hit_times = {
        r["first_hit_utc"]
        for r in hit_rows
        if r["first_hit_utc"]
    }

    first_hit_bars = {
        r["first_hit_bar_1based"]
        for r in hit_rows
        if r["first_hit_bar_1based"]
    }

    entry_prices = {
        r["entry_price"]
        for r in rows
        if r["entry_price"]
    }

    all_hit = (
        len(statuses) == len(HYPOTHESES)
        and all(s == "HIT" for s in statuses.values())
    )

    equal_time = (
        all_hit and len(first_hit_times) == 1
    )

    return {
        "event_id": "|".join(key),
        "source": first["source"],
        "market": first["market"],
        "timeframe": first["timeframe"],
        "classification": first["classification"],
        "window_status": first["window_status"],
        "horizon_bars": first["horizon_bars"],
        "complete": complete,
        "statuses": json.dumps(
            statuses, sort_keys=True
        ),
        "all_hypotheses_hit": all_hit,
        "all_first_hit_times_equal": equal_time,
        "first_hit_time_variants": len(first_hit_times),
        "first_hit_bar_variants": len(first_hit_bars),
        "entry_price_variants": len(entry_prices),
        "audit_status": "FAIL" if problems else "PASS",
        "issues": ";".join(sorted(set(problems))),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    source = Path(args.input).resolve()
    output = Path(args.output).resolve()

    rows, prior, input_paths = read_input(source)

    groups = defaultdict(list)

    for row in rows:
        key = (
            row["source_index"],
            row["source"],
            row["market"],
            row["timeframe"],
            row["entry_epoch"],
            row["horizon_bars"],
        )
        groups[key].append(row)

    detail = [
        inspect_event(key, items)
        for key, items in sorted(groups.items())
    ]

    source_counts = Counter(
        x["source"] for x in detail
    )
    classification_counts = Counter(
        x["classification"] for x in detail
    )
    complete_by_source = Counter(
        x["source"]
        for x in detail if x["complete"]
    )
    complete_by_classification = Counter(
        x["classification"]
        for x in detail if x["complete"]
    )

    complete = [
        x for x in detail if x["complete"]
    ]
    incomplete = [
        x for x in detail if not x["complete"]
    ]

    all_hit_count = sum(
        x["all_hypotheses_hit"] for x in complete
    )
    same_hit_time_count = sum(
        x["all_first_hit_times_equal"]
        for x in complete
    )

    failures = [
        {
            "event_id": x["event_id"],
            "issues": x["issues"]
        }
        for x in detail if x["audit_status"] != "PASS"
    ]

    expected_events = prior.get("source_events")
    expected_complete = prior.get("complete_events")
    expected_incomplete = prior.get("incomplete_events")

    count_ok = (
        len(detail) == expected_events
        and len(complete) == expected_complete
        and len(incomplete) == expected_incomplete
        and len(rows) == len(detail) * len(HYPOTHESES)
    )

    # PASS means internal consistency only.
    # It is not evidence of predictive accuracy.
    execution = (
        "PASS"
        if count_ok and not failures
        else "FAIL"
    )

    result = {
        "audit": AUDIT,
        "version": VERSION,
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "execution": execution,
        "source_events": len(detail),
        "hypothesis_rows": len(rows),
        "complete_events": len(complete),
        "incomplete_events": len(incomplete),
        "complete_all_four_hit": all_hit_count,
        "complete_same_first_hit_utc": same_hit_time_count,
        "complete_different_first_hit_utc": (
            len(complete) - same_hit_time_count
        ),
        "source_counts": dict(source_counts),
        "classification_counts": dict(
            classification_counts
        ),
        "complete_by_source": dict(
            complete_by_source
        ),
        "complete_by_classification": dict(
            complete_by_classification
        ),
        "selected_sample": True,
        "selection_bias_risk": "HIGH",
        "sample_hit_rate_generalizable": False,
        "original_entry_convention_proven": False,
        "horizon_unit_proven": False,
        "independent_oos_proven": False,
        "predictive_performance_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
        "errors": failures[:100],
        "limitations": [
            "Audits previously selected 74 events only",
            "Complete events were selected from review cases",
            "Does not independently replay OHLCV",
            "Does not reconstruct all historical signals",
            "Does not establish an unbiased denominator",
            "No OOS, fees, slippage or trading fills"
        ]
    }

    output.mkdir(parents=True, exist_ok=True)

    detail_path = output / "selection_bias_detail.csv"
    result_path = output / "selection_bias_result.json"
    manifest_path = output / "selection_bias_manifest.json"

    with detail_path.open(
        "w", encoding="utf-8", newline=""
    ) as f:
        writer = csv.DictWriter(
            f, fieldnames=OUTPUT_FIELDS
        )
        writer.writeheader()
        writer.writerows(detail)

    save_json(result_path, result)

    manifest = {
        "audit": AUDIT,
        "version": VERSION,
        "execution": execution,
        "research_decision": "HOLD",
        "script_sha256": sha256(
            Path(__file__).resolve()
        ),
        "input_sha256": {
            name: sha256(path)
            for name, path in input_paths.items()
        },
        "output_sha256": {
            detail_path.name: sha256(detail_path),
            result_path.name: sha256(result_path)
        }
    }
    save_json(manifest_path, manifest)

    print("Execution:", execution)
    print("Source events:", len(detail))
    print("Complete:", len(complete))
    print("Incomplete:", len(incomplete))
    print("All four HIT:", all_hit_count)
    print("Same first-hit UTC:", same_hit_time_count)
    print("Complete by source:", dict(complete_by_source))
    print(
        "Complete by classification:",
        dict(complete_by_classification)
    )
    print("Selection bias risk: HIGH")
    print("Research decision: HOLD")

    return 0 if execution == "PASS" else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print("[FATAL]", str(exc), file=sys.stderr)
        sys.exit(1)
