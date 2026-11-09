
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Pattern 256 Entry Boundary Audit - Clean V001."""

import argparse
import csv
import hashlib
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

VERSION = "Clean V001"
AUDIT = "pattern_256_entry_boundary"

FIELDS = [
    "index", "source", "timeframe", "market",
    "entry_epoch", "expected_bars", "original_status",
    "include_complete", "exclude_complete",
    "include_available", "exclude_available",
    "include_hit", "exclude_hit",
    "include_first_hit", "exclude_first_hit",
    "classification", "error",
]

STEP = {"h1": 3600, "h4": 14400, "d1": 86400}


def digest(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1048576), b""):
            h.update(block)
    return h.hexdigest()


def load_json(path):
    with path.open(encoding="utf-8-sig") as f:
        return json.load(f)


def save_json(path, data):
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=True,
                  ensure_ascii=False)
        f.write("\n")


def boolean(value):
    if value == "True":
        return True
    if value == "False":
        return False
    raise ValueError(f"Invalid boolean: {value!r}")


def optional_boolean(value):
    return None if value == "" else boolean(value)


def optional_integer(value):
    return None if value == "" else int(value)


def classify(row):
    source = row["source"]
    if source not in ("gap", "independent"):
        raise ValueError("Unknown source")

    tf = row["timeframe"]
    if tf not in STEP:
        raise ValueError("Unknown timeframe")

    if not row["market"].startswith("KRW-"):
        raise ValueError("Invalid market")

    entry = int(row["entry_epoch"])
    bars = int(row["expected_bars"])

    if entry <= 0 or bars <= 0:
        raise ValueError("Invalid entry or horizon")

    included = boolean(row["include_complete"])
    excluded = boolean(row["exclude_complete"])
    available_a = int(row["include_available"])
    available_b = int(row["exclude_available"])

    if not 0 <= available_a <= bars:
        raise ValueError("Invalid included availability")
    if not 0 <= available_b <= bars:
        raise ValueError("Invalid excluded availability")

    if included != (available_a == bars):
        raise ValueError("Included completeness inconsistent")
    if excluded != (available_b == bars):
        raise ValueError("Excluded completeness inconsistent")

    hit_a = optional_boolean(row["include_hit"])
    hit_b = optional_boolean(row["exclude_hit"])
    first_a = optional_integer(row["include_first_hit"])
    first_b = optional_integer(row["exclude_first_hit"])

    if source == "gap" and any(
        value is not None
        for value in (hit_a, hit_b, first_a, first_b)
    ):
        raise ValueError("Gap source has outcome values")

    if not included or not excluded:
        if any(
            value is not None
            for value in (hit_a, hit_b, first_a, first_b)
        ):
            raise ValueError("Incomplete window has outcome")
        return "INCOMPLETE_COMPARISON", "MISSING_CANDLE_SLOTS"

    if source == "gap":
        return "WINDOW_ONLY", "ENTRY_PRICE_UNAVAILABLE"

    if hit_a is None or hit_b is None:
        raise ValueError("Independent hit flag missing")

    for hit, first in ((hit_a, first_a), (hit_b, first_b)):
        if hit and (first is None or not 1 <= first <= bars):
            raise ValueError("Hit without valid first bar")
        if not hit and first is not None:
            raise ValueError("Non-hit has first bar")

    if hit_a != hit_b:
        return "HIT_CHANGED", "TARGET_CLASSIFICATION_CHANGED"

    if first_a != first_b:
        if first_a is not None and first_b is not None:
            delta = first_a - first_b
            if delta == 1:
                return (
                    "FIRST_HIT_BAR_CHANGED",
                    "ONE_BAR_INDEX_SHIFT",
                )
            return (
                "FIRST_HIT_BAR_CHANGED",
                "NONSTANDARD_INDEX_SHIFT",
            )
        return "FIRST_HIT_BAR_CHANGED", "UNEXPECTED_BAR_SHIFT"

    return "UNCHANGED", "SAME_OUTCOME_AND_BAR"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    src = Path(args.input).resolve()
    out = Path(args.output).resolve()

    paths = {
        "entry_sensitivity_detail.csv":
            src / "entry_sensitivity_detail.csv",
        "entry_sensitivity_result.json":
            src / "entry_sensitivity_result.json",
        "entry_sensitivity_manifest.json":
            src / "entry_sensitivity_manifest.json",
    }

    for name, path in paths.items():
        if not path.is_file():
            raise FileNotFoundError(name)

    prior = load_json(paths["entry_sensitivity_result.json"])
    manifest = load_json(paths["entry_sensitivity_manifest.json"])

    if prior.get("execution") != "PASS":
        raise ValueError("Prior audit not PASS")
    if manifest.get("execution") != "PASS":
        raise ValueError("Prior manifest not PASS")
    if prior.get("audit") != "pattern_256_entry_candle_sensitivity":
        raise ValueError("Unexpected prior audit")

    for name in (
        "entry_sensitivity_detail.csv",
        "entry_sensitivity_result.json",
    ):
        expected = manifest.get("output_sha256", {}).get(name)
        if not expected or digest(paths[name]) != expected:
            raise ValueError(f"SHA256 mismatch: {name}")

    with paths["entry_sensitivity_detail.csv"].open(
        encoding="utf-8-sig", newline=""
    ) as f:
        reader = csv.DictReader(f)
        if reader.fieldnames != FIELDS:
            raise ValueError("Unexpected input CSV schema")
        rows = list(reader)

    if len(rows) != prior.get("sampled_events"):
        raise ValueError("Sample count mismatch")

    counts = Counter()
    reasons = Counter()
    by_source = Counter()
    by_timeframe = Counter()
    reviewed = []
    failures = []
    seen = set()

    for position, row in enumerate(rows, start=1):
        try:
            if int(row["index"]) != position:
                raise ValueError("Nonsequential row index")

            identity = (
                row["source"], row["timeframe"],
                row["market"], row["entry_epoch"],
                row["expected_bars"],
            )
            if identity in seen:
                raise ValueError("Duplicate event identity")
            seen.add(identity)

            category, reason = classify(row)

            if row["classification"] != category:
                raise ValueError(
                    "Original classification mismatch: "
                    f"{row['classification']} != {category}"
                )

            counts[category] += 1
            reasons[reason] += 1
            by_source[(row["source"], category)] += 1
            by_timeframe[(row["timeframe"], category)] += 1

            if category not in ("UNCHANGED", "WINDOW_ONLY"):
                reviewed.append({
                    "index": row["index"],
                    "source": row["source"],
                    "timeframe": row["timeframe"],
                    "market": row["market"],
                    "entry_epoch": row["entry_epoch"],
                    "expected_bars": row["expected_bars"],
                    "include_available": row["include_available"],
                    "exclude_available": row["exclude_available"],
                    "include_hit": row["include_hit"],
                    "exclude_hit": row["exclude_hit"],
                    "include_first_hit": row["include_first_hit"],
                    "exclude_first_hit": row["exclude_first_hit"],
                    "classification": category,
                    "reason": reason,
                })

        except Exception as exc:
            failures.append({
                "index": position,
                "error": str(exc),
            })

    prior_counts = prior.get("classification_counts", {})
    if dict(counts) != prior_counts:
        failures.append({
            "index": 0,
            "error": "Aggregate classification counts mismatch",
        })

    if sum(counts.values()) != len(rows):
        failures.append({
            "index": 0,
            "error": "Not all input rows validated",
        })

    out.mkdir(parents=True, exist_ok=True)

    detail = out / "boundary_review.csv"
    columns = [
        "index", "source", "timeframe", "market",
        "entry_epoch", "expected_bars",
        "include_available", "exclude_available",
        "include_hit", "exclude_hit",
        "include_first_hit", "exclude_first_hit",
        "classification", "reason",
    ]

    with detail.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(reviewed)

    status = "FAIL" if failures else "PASS"

    output = {
        "audit": AUDIT,
        "version": VERSION,
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "execution": status,
        "input_events": len(rows),
        "review_rows": len(reviewed),
        "classification_counts": dict(sorted(counts.items())),
        "reason_counts": dict(sorted(reasons.items())),
        "source_counts": [
            {"source": a, "classification": b, "count": n}
            for (a, b), n in sorted(by_source.items())
        ],
        "timeframe_counts": [
            {"timeframe": a, "classification": b, "count": n}
            for (a, b), n in sorted(by_timeframe.items())
        ],
        "validation_errors": failures[:100],
        "hit_changed": counts["HIT_CHANGED"],
        "nonstandard_bar_shifts":
            reasons["NONSTANDARD_INDEX_SHIFT"]
            + reasons["UNEXPECTED_BAR_SHIFT"],
        "horizon_unit_verified": False,
        "original_entry_convention_proven": False,
        "full_population_replay_proven": False,
        "independent_oos_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
        "limitations": [
            "Revalidates prior sensitivity CSV only",
            "Does not independently replay OHLCV",
            "One-bar shifts do not prove original convention",
            "Incomplete windows remain unresolved",
            "Does not prove trading profitability",
        ],
    }

    result_path = out / "boundary_result.json"
    save_json(result_path, output)

    output_manifest = {
        "audit": AUDIT,
        "version": VERSION,
        "execution": status,
        "research_decision": "HOLD",
        "script_sha256": digest(Path(__file__).resolve()),
        "input_sha256": {
            name: digest(path) for name, path in paths.items()
        },
        "output_sha256": {
            detail.name: digest(detail),
            result_path.name: digest(result_path),
        },
    }
    save_json(out / "boundary_manifest.json", output_manifest)

    print("=== Pattern 256 Entry Boundary Audit ===")
    print(f"Execution: {status}")
    print(f"Input events: {len(rows)}")
    print(f"Review rows: {len(reviewed)}")

    for name, count in sorted(counts.items()):
        print(f"{name}: {count}")

    for name, count in sorted(reasons.items()):
        print(f"Reason {name}: {count}")

    if failures:
        for item in failures[:10]:
            print(f"[ERROR] {item}", file=sys.stderr)
        return 1

    print("Research decision: HOLD")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(f"[FATAL] {exc}", file=sys.stderr)
        sys.exit(1)
