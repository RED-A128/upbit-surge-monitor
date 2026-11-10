
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Evaluator Policy Inspection
Clean V001

Read-only inspection of evaluator source code and
previously reviewed attention cases.

Does not change any original data.
Does not approve training or live trading.
"""

import argparse
import csv
import hashlib
import json
import os
import re
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

VERSION = "Clean V001"
AUDIT = "pattern_256_evaluator_policy_inspection"

REVIEW_DIR = Path(
    "data/reports/pattern_256_gap_review_finalization"
)
OUTPUT_DIR = Path(
    "data/reports/pattern_256_evaluator_policy_inspection"
)

TERMS = (
    "entry_timestamp",
    "entry_price",
    "signal_timestamp",
    "first_hit",
    "window_gap",
    "censored",
    "horizon",
    "observed_bars",
    "required_bars",
    "target_price",
    "hit_30pct",
)

REVIEW_FILES = (
    "gap_review_result.json",
    "gap_review_matrix.csv",
    "gap_review_attention_cases.csv",
)

OUTPUT_FILES = (
    "policy_inspection_result.json",
    "policy_source_locations.csv",
    "policy_attention_summary.csv",
    "policy_boundary_cases.csv",
)


def digest(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(
            lambda: f.read(1024 * 1024), b""
        ):
            h.update(block)
    return h.hexdigest()


def read_json(path):
    with path.open("r", encoding="utf-8-sig") as f:
        return json.load(f)


def read_csv(path):
    with path.open(
        "r", encoding="utf-8-sig", newline=""
    ) as f:
        return list(csv.DictReader(f))


def write_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + ".tmp")
    with temp.open("w", encoding="utf-8") as f:
        json.dump(
            data, f, ensure_ascii=False,
            indent=2, sort_keys=True
        )
        f.write("\n")
    os.replace(temp, path)


def write_csv(path, columns, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + ".tmp")
    with temp.open(
        "w", encoding="utf-8-sig", newline=""
    ) as f:
        writer = csv.DictWriter(
            f, fieldnames=columns,
            extrasaction="ignore"
        )
        writer.writeheader()
        writer.writerows(rows)
    os.replace(temp, path)


def verify_review(folder):
    manifest_path = folder / "gap_review_manifest.json"

    if not manifest_path.is_file():
        raise RuntimeError("Review manifest missing")

    manifest = read_json(manifest_path)

    if manifest.get("audit") != (
        "pattern_256_gap_review_finalization"
    ):
        raise RuntimeError("Wrong review audit")

    for name in REVIEW_FILES:
        path = folder / name
        if not path.is_file():
            raise RuntimeError("Missing review file: " + name)

        expected = manifest.get("outputs", {}).get(name)

        if digest(path) != expected:
            raise RuntimeError("SHA256 mismatch: " + name)

    result = read_json(
        folder / "gap_review_result.json"
    )

    if result.get("execution") != "PASS":
        raise RuntimeError("Review did not PASS")

    if result.get("error_count") != 0:
        raise RuntimeError("Review contains errors")

    return result


def inspect_sources(root):
    locations = []
    files_inspected = 0
    files_matched = 0

    # Scan the project source tree, excluding generated,
    # dependency, runner, and version-control folders.
    excluded = {
        ".git", ".venv", "venv", "__pycache__",
        "node_modules", "data", "reports",
        "actions-runner", ".pytest_cache",
    }

    pattern = re.compile(
        r"\b(?:" +
        "|".join(re.escape(x) for x in TERMS) +
        r")\b",
        re.IGNORECASE
    )

    for current, dirs, files in os.walk(root):
        dirs[:] = sorted(
            d for d in dirs
            if d.lower() not in excluded
        )

        for name in sorted(files):
            if not name.endswith(".py"):
                continue

            path = Path(current) / name
            files_inspected += 1

            try:
                text = path.read_text(
                    encoding="utf-8-sig"
                )
            except (UnicodeError, OSError):
                continue

            lines = text.splitlines()
            matched = False

            for index, line in enumerate(lines):
                found = sorted({
                    m.group(0).lower()
                    for m in pattern.finditer(line)
                })

                if not found:
                    continue

                matched = True

                # Keep a short context window.
                start = max(0, index - 2)
                end = min(len(lines), index + 3)

                context = "\n".join(
                    "{}: {}".format(j + 1, lines[j])
                    for j in range(start, end)
                )

                locations.append({
                    "source_file": str(
                        path.relative_to(root)
                    ).replace("\\", "/"),
                    "source_sha256": digest(path),
                    "line_number": index + 1,
                    "terms": "|".join(found),
                    "context": context[:3000],
                })

            if matched:
                files_matched += 1

    return locations, files_inspected, files_matched


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True)
    args = parser.parse_args()

    root = Path(args.root).expanduser().resolve()

    review_folder = root / REVIEW_DIR
    output = root / OUTPUT_DIR

    prior = verify_review(review_folder)

    attention = read_csv(
        review_folder / "gap_review_attention_cases.csv"
    )
    matrix = read_csv(
        review_folder / "gap_review_matrix.csv"
    )

    if len(attention) != prior.get("attention_cases"):
        raise RuntimeError("Attention count mismatch")

    if sum(int(x["count"]) for x in matrix) != (
        prior.get("input_samples")
    ):
        raise RuntimeError("Matrix total mismatch")

    counts = Counter(
        (x["review_reason"], x["timeframe"])
        for x in attention
    )

    summary_rows = [
        {
            "review_reason": reason,
            "timeframe": tf,
            "count": count,
        }
        for (reason, tf), count in sorted(counts.items())
    ]

    boundary_rows = [
        x for x in attention
        if x["review_reason"] == (
            "ENTRY_WINDOW_BOUNDARY_REVIEW"
        )
    ]

    locations, inspected, matched = inspect_sources(root)

    write_csv(
        output / "policy_source_locations.csv",
        (
            "source_file", "source_sha256",
            "line_number", "terms", "context"
        ),
        locations,
    )

    write_csv(
        output / "policy_attention_summary.csv",
        ("review_reason", "timeframe", "count"),
        summary_rows,
    )

    write_csv(
        output / "policy_boundary_cases.csv",
        (
            "review_reason", "timeframe", "market",
            "detail_line", "classification",
            "recorded_status", "horizon",
            "signal_timestamp", "entry_timestamp",
            "include_result", "exclude_result",
            "include_missing_slots",
            "exclude_missing_slots", "diagnosis"
        ),
        boundary_rows,
    )

    result = {
        "audit": AUDIT,
        "version": VERSION,
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "execution": "PASS",
        "classification": "REVIEW",
        "research_decision": "HOLD",
        "attention_cases": len(attention),
        "attention_counts": dict(
            Counter(
                x["review_reason"]
                for x in attention
            )
        ),
        "boundary_cases": len(boundary_rows),
        "python_files_inspected": inspected,
        "python_files_matched": matched,
        "source_locations": len(locations),
        "source_policy_confirmed": False,
        "next_gate": "MANUAL_POLICY_REVIEW_THEN_OOS",
        "limitations": [
            "Source search is lexical, not semantic proof",
            "Policy meaning requires reading actual code",
            "Source snapshots may differ from evaluator run",
            "No original evaluator is rerun",
            "No independent OOS performance is proven",
            "No trading profitability is proven",
        ],
        "train_approved": False,
        "live_trading_approved": False,
    }

    write_json(
        output / "policy_inspection_result.json",
        result
    )

    write_json(
        output / "policy_inspection_manifest.json",
        {
            "audit": AUDIT,
            "version": VERSION,
            "outputs": {
                name: digest(output / name)
                for name in OUTPUT_FILES
            }
        }
    )

    print(
        json.dumps(
            result, ensure_ascii=False, indent=2
        ),
        flush=True
    )

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(
            "[FATAL] {}: {}".format(
                type(exc).__name__, exc
            ),
            file=sys.stderr
        )
        sys.exit(1)
