
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Evaluator Policy Review
Clean V001

Purpose:
- Verify the previous policy-inspection artifacts.
- Inspect the actual evaluator source code.
- Extract entry/window/censoring policy evidence.
- Check source SHA256 against the previous inspection.
- Produce a compact manual-review report.

Safety:
- Read-only access to all existing research files.
- Never rewrite OHLCV, signals, outcomes or checkpoints.
- Never approve training or trading automatically.
"""

import argparse
import csv
import hashlib
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path


VERSION = "Clean V001"

INPUT_NAMES = (
    "policy_inspection_result.json",
    "policy_inspection_manifest.json",
    "policy_source_locations.csv",
    "policy_boundary_cases.csv",
    "policy_attention_summary.csv",
)

OUTPUT_DIR = (
    Path("data")
    / "reports"
    / "pattern_256_evaluator_policy_review"
)

POLICY_TERMS = (
    "entry_timestamp",
    "entry_price",
    "signal_timestamp",
    "observed_bars",
    "required_bars",
    "first_hit",
    "hit_30pct",
    "window_gap",
    "censored",
    "horizon",
    "next_open",
    "target_price",
)

PRIMARY_FILES = (
    "evaluate_pattern_256_accumulation_30pct.py",
    "evaluate_pattern_256_accumulation_candidate_independent_backtest.py",
    "backtest_pattern_256_accumulation_candidate_independent.py",
    "verify_pattern_256_outcomes_full_market.py",
    "validate_pattern_256_accumulation_30pct.py",
)

MAX_EVIDENCE_PER_FILE = 35
MAX_SOURCE_FILES = 12


def sha256_file(path):
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        for chunk in iter(
            lambda: handle.read(1024 * 1024), b""
        ):
            digest.update(chunk)

    return digest.hexdigest()


def read_json(path):
    with path.open("r", encoding="utf-8-sig") as handle:
        return json.load(handle)


def read_csv(path):
    with path.open(
        "r",
        encoding="utf-8-sig",
        newline=""
    ) as handle:
        return list(csv.DictReader(handle))


def write_json(path, data):
    with path.open("w", encoding="utf-8") as handle:
        json.dump(
            data,
            handle,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        handle.write("\n")


def safe_relative_path(value):
    path = Path(value)

    if path.is_absolute() or ".." in path.parts:
        raise ValueError(
            "Unsafe source path: {}".format(value)
        )

    return path


def resolve_source(root, checkout, name):
    relative = safe_relative_path(name)

    candidates = [
        root / relative,
        checkout / relative,
    ]

    for candidate in candidates:
        if candidate.is_file():
            return candidate.resolve()

    return None


def inspect_source(path, locations, expected_hash):
    actual_hash = sha256_file(path)

    if expected_hash:
        hash_match = actual_hash == expected_hash
    else:
        hash_match = None

    with path.open(
        "r",
        encoding="utf-8-sig",
        errors="replace"
    ) as handle:
        lines = handle.readlines()

    selected = set()

    for location in locations:
        try:
            number = int(location["line_number"])
        except (ValueError, KeyError, TypeError):
            continue

        if 1 <= number <= len(lines):
            selected.add(number)

    # Add direct policy matches to catch relevant code
    # even when the lexical inspection missed a line.
    for number, line in enumerate(lines, start=1):
        lower = line.lower()

        if any(term in lower for term in POLICY_TERMS):
            selected.add(number)

    evidence = []

    for number in sorted(selected)[
        :MAX_EVIDENCE_PER_FILE
    ]:
        start = max(1, number - 3)
        end = min(len(lines), number + 3)

        excerpt = "".join(
            "{}: {}".format(i, lines[i - 1])
            for i in range(start, end + 1)
        ).rstrip()

        evidence.append({
            "line": number,
            "excerpt": excerpt,
        })

    return {
        "path": str(path),
        "expected_sha256": expected_hash,
        "actual_sha256": actual_hash,
        "hash_match": hash_match,
        "line_count": len(lines),
        "evidence_count": len(evidence),
        "evidence": evidence,
    }


def markdown_report(report):
    lines = [
        "# Pattern 256 Evaluator Policy Review",
        "",
        "- Version: {}".format(VERSION),
        "- Execution: {}".format(
            report["execution"]
        ),
        "- Classification: {}".format(
            report["classification"]
        ),
        "- Research decision: HOLD",
        "",
        "## Previous inspection",
        "",
        "- Previous classification: {}".format(
            report["previous_classification"]
        ),
        "- Attention cases: {}".format(
            report["attention_cases"]
        ),
        "- Boundary cases: {}".format(
            report["boundary_cases"]
        ),
        "",
        "## Source integrity",
        "",
        "- Sources inspected: {}".format(
            report["sources_inspected"]
        ),
        "- Hash mismatches: {}".format(
            report["hash_mismatches"]
        ),
        "- Missing primary sources: {}".format(
            len(report["missing_primary_sources"])
        ),
        "",
        "## Policy status",
        "",
        "The source evidence below is diagnostic.",
        "It does not automatically establish the",
        "correct entry convention, gap handling,",
        "causal availability, or trading profitability.",
        "",
    ]

    if report["missing_primary_sources"]:
        lines.extend([
            "### Missing primary sources",
            "",
        ])

        for name in report["missing_primary_sources"]:
            lines.append("- `{}`".format(name))

        lines.append("")

    lines.extend([
        "## Boundary cases",
        "",
    ])

    for case in report["boundary_details"]:
        lines.extend([
            "### {} / {}".format(
                case.get("timeframe", ""),
                case.get("market", ""),
            ),
            "",
            "- Horizon: {}".format(
                case.get("horizon", "")
            ),
            "- Include result: {}".format(
                case.get("include_result", "")
            ),
            "- Exclude result: {}".format(
                case.get("exclude_result", "")
            ),
            "- Diagnosis: {}".format(
                case.get("diagnosis", "")
            ),
            "",
        ])

    lines.extend([
        "## Source evidence",
        "",
    ])

    for name, source in report["sources"].items():
        lines.extend([
            "### {}".format(name),
            "",
            "- SHA256 match: {}".format(
                source["hash_match"]
            ),
            "- File: `{}`".format(
                source["path"]
            ),
            "",
        ])

        for item in source["evidence"]:
            lines.extend([
                "Line {}".format(item["line"]),
                "",
                "```python",
                item["excerpt"],
                "```",
                "",
            ])

    lines.extend([
        "## Required manual decisions",
        "",
        "1. Confirm signal candle close timing.",
        "2. Confirm the actual entry price convention.",
        "3. Confirm whether the entry candle is included.",
        "4. Confirm whether missing candles invalidate",
        "   the evaluation window.",
        "5. Confirm CENSORED handling.",
        "6. Confirm the original evaluator matches",
        "   the inspected source snapshot.",
        "7. Only then proceed to independent OOS.",
        "",
        "No source files were modified.",
        "Training and live trading remain unapproved.",
        "",
    ])

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--root",
        required=True,
        help="Persistent local research project root",
    )

    parser.add_argument(
        "--checkout",
        required=True,
        help="GitHub Actions checkout directory",
    )

    parser.add_argument(
        "--inspection-dir",
        default="",
        help="Optional previous inspection directory",
    )

    args = parser.parse_args()

    root = Path(args.root).resolve()
    checkout = Path(args.checkout).resolve()

    if not root.is_dir():
        raise FileNotFoundError(
            "Research root missing: {}".format(root)
        )

    if not checkout.is_dir():
        raise FileNotFoundError(
            "Checkout missing: {}".format(checkout)
        )

    if args.inspection_dir:
        inspection = Path(
            args.inspection_dir
        ).resolve()
    else:
        inspection = (
            root
            / "data"
            / "reports"
            / "pattern_256_evaluator_policy_inspection"
        )

    if not inspection.is_dir():
        raise FileNotFoundError(
            "Previous inspection folder missing: {}".format(
                inspection
            )
        )

    for name in INPUT_NAMES:
        if not (inspection / name).is_file():
            raise FileNotFoundError(
                "Required input missing: {}".format(name)
            )

    previous = read_json(
        inspection / "policy_inspection_result.json"
    )

    manifest = read_json(
        inspection / "policy_inspection_manifest.json"
    )

    expected_outputs = manifest.get("outputs", {})

    integrity_errors = []

    for name, expected_hash in expected_outputs.items():
        path = inspection / name

        if not path.is_file():
            integrity_errors.append(
                "Missing: {}".format(name)
            )
            continue

        actual_hash = sha256_file(path)

        if actual_hash.lower() != str(
            expected_hash
        ).lower():
            integrity_errors.append(
                "SHA256 mismatch: {}".format(name)
            )

    if integrity_errors:
        raise RuntimeError(
            "Previous inspection integrity failed:\n"
            + "\n".join(integrity_errors)
        )

    locations = read_csv(
        inspection / "policy_source_locations.csv"
    )

    boundary = read_csv(
        inspection / "policy_boundary_cases.csv"
    )

    attention = read_csv(
        inspection / "policy_attention_summary.csv"
    )

    grouped = defaultdict(list)
    expected_hashes = defaultdict(set)

    for row in locations:
        name = row.get("source_file", "").strip()

        if not name:
            continue

        grouped[name].append(row)

        source_hash = row.get(
            "source_sha256", ""
        ).strip()

        if source_hash:
            expected_hashes[name].add(
                source_hash.lower()
            )

    selected = []

    for name in PRIMARY_FILES:
        if name in grouped:
            selected.append(name)

    ranked = sorted(
        grouped,
        key=lambda name: (
            -len(grouped[name]),
            name,
        ),
    )

    for name in ranked:
        if name not in selected:
            selected.append(name)

        if len(selected) >= MAX_SOURCE_FILES:
            break

    sources = {}
    missing_primary = []
    hash_mismatches = 0
    missing_sources = []

    for name in selected:
        path = resolve_source(
            root, checkout, name
        )

        if path is None:
            missing_sources.append(name)

            if name in PRIMARY_FILES:
                missing_primary.append(name)

            continue

        hashes = expected_hashes[name]

        expected_hash = (
            next(iter(hashes))
            if len(hashes) == 1
            else None
        )

        inspected = inspect_source(
            path,
            grouped[name],
            expected_hash,
        )

        if len(hashes) > 1:
            inspected["hash_match"] = False
            inspected["hash_conflict"] = True

        if inspected["hash_match"] is False:
            hash_mismatches += 1

        sources[name] = inspected

    attention_counts = Counter()

    for row in attention:
        try:
            count = int(row.get("count", 0))
        except (TypeError, ValueError):
            count = 0

        attention_counts[
            row.get("review_reason", "UNKNOWN")
        ] += count

    if (
        not sources
        or hash_mismatches
        or missing_primary
        or missing_sources
    ):
        classification = "REVIEW"
    else:
        # Even with matching source hashes,
        # policy semantics need manual confirmation.
        classification = "MANUAL_REVIEW_REQUIRED"

    report = {
        "audit": "pattern_256_evaluator_policy_review",
        "version": VERSION,
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "execution": "PASS",
        "classification": classification,
        "previous_classification": previous.get(
            "classification", "UNKNOWN"
        ),
        "attention_cases": sum(
            attention_counts.values()
        ),
        "attention_counts": dict(
            attention_counts
        ),
        "boundary_cases": len(boundary),
        "boundary_details": boundary,
        "sources_inspected": len(sources),
        "hash_mismatches": hash_mismatches,
        "missing_primary_sources": missing_primary,
        "missing_sources": missing_sources,
        "sources": sources,
        "previous_inspection_integrity": True,
        "source_policy_confirmed": False,
        "independent_oos_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
        "next_gate": "MANUAL_POLICY_REVIEW",
    }

    output = root / OUTPUT_DIR
    output.mkdir(
        parents=True,
        exist_ok=True,
    )

    json_path = output / "policy_review_result.json"
    md_path = output / "policy_review_evidence.md"
    manifest_path = output / "policy_review_manifest.json"

    write_json(json_path, report)

    md_path.write_text(
        markdown_report(report),
        encoding="utf-8",
    )

    write_json(
        manifest_path,
        {
            "audit": report["audit"],
            "version": VERSION,
            "outputs": {
                json_path.name: sha256_file(
                    json_path
                ),
                md_path.name: sha256_file(
                    md_path
                ),
            },
        },
    )

    print("=" * 60)
    print("Pattern 256 Evaluator Policy Review")
    print("=" * 60)
    print("Execution: PASS")
    print("Classification:", classification)
    print("Attention cases:", report["attention_cases"])
    print("Boundary cases:", len(boundary))
    print("Sources inspected:", len(sources))
    print("Hash mismatches:", hash_mismatches)
    print("Missing sources:", len(missing_sources))
    print("Research decision: HOLD")
    print("Report directory:", output)
    print("=" * 60)

    # A review finding is not a Python execution error.
    # Workflow succeeds if the audit ran and wrote
    # integrity-verifiable reports.
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(
            "[ERROR] {}: {}".format(
                type(exc).__name__,
                exc,
            ),
            file=sys.stderr,
        )
        sys.exit(1)
