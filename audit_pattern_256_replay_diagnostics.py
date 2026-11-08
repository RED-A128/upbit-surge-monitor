
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Replay Diagnostics
Clean V001

Input:
  replay_details.csv
  replay_result.json
  replay_manifest.json

Outputs:
  diagnostics_result.json
  diagnostics_detail.csv
  diagnostics_manifest.json

Research decision: HOLD
No OHLCV source modifications.
"""

import argparse
import csv
import hashlib
import json
import math
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

VERSION = "Clean V001"
AUDIT = "pattern_256_replay_diagnostics"

EXPECTED_COLUMNS = {
    "source",
    "source_line",
    "timeframe",
    "market",
    "horizon",
    "entry_epoch",
    "entry_price_reported",
    "entry_open_ohlcv",
    "entry_price_difference_pct",
    "expected_bars",
    "available_bars",
    "contiguous_bars",
    "max_high",
    "replay_hit_30pct",
    "reported_hit_30pct",
    "hit_agreement",
    "first_hit_bar",
    "status",
    "reason",
}

ALLOWED_STATUSES = {
    "MATCH",
    "NOT_COMPARABLE",
    "WINDOW_VERIFIED",
}

ALLOWED_REASONS = {
    "REPLAY_AGREES_WITH_REPORTED",
    "INCOMPLETE_WINDOW",
    "NON_CONTIGUOUS_WINDOW",
    "GAP_ENTRY_PRICE_NOT_AVAILABLE",
}


def sha256(path):
    digest = hashlib.sha256()

    with open(path, "rb") as handle:
        for block in iter(
            lambda: handle.read(1024 * 1024),
            b"",
        ):
            digest.update(block)

    return digest.hexdigest()


def load_json(path):
    with open(path, "r", encoding="utf-8-sig") as handle:
        return json.load(handle)


def save_json(path, value):
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(
            value,
            handle,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        handle.write("\n")


def read_csv(path):
    with open(
        path,
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as handle:
        reader = csv.DictReader(handle)

        if not reader.fieldnames:
            raise ValueError("CSV header missing")

        missing = EXPECTED_COLUMNS - set(reader.fieldnames)

        if missing:
            raise ValueError(
                "Missing columns: " + ", ".join(sorted(missing))
            )

        return list(reader)


def parse_int(value, field):
    try:
        number = float(value)
    except (ValueError, TypeError):
        raise ValueError(f"Invalid {field}: {value}")

    if not math.isfinite(number) or not number.is_integer():
        raise ValueError(f"Invalid integer {field}: {value}")

    return int(number)


def parse_bool(value):
    value = str(value).strip().lower()

    if value in ("true", "1"):
        return True

    if value in ("false", "0"):
        return False

    return None


def validate_manifest(manifest, paths):
    if manifest.get("audit") != "pattern_256_ohlcv_outcome_replay":
        raise ValueError("Unexpected input audit")

    if manifest.get("execution") != "PASS":
        raise ValueError("Input execution is not PASS")

    hashes = manifest.get("output_sha256", {})

    for filename in (
        "replay_details.csv",
        "replay_result.json",
    ):
        expected = hashes.get(filename)

        if not expected:
            raise ValueError(f"Missing SHA256: {filename}")

        actual = sha256(paths[filename])

        if actual.lower() != str(expected).lower():
            raise ValueError(
                f"SHA256 mismatch: {filename}"
            )


def validate_row(row, index):
    problems = []

    source = row["source"].strip()
    timeframe = row["timeframe"].strip()
    status = row["status"].strip()
    reason = row["reason"].strip()

    if source not in ("gap", "independent"):
        problems.append("INVALID_SOURCE")

    if timeframe not in ("h1", "h4", "d1"):
        problems.append("INVALID_TIMEFRAME")

    if status not in ALLOWED_STATUSES:
        problems.append("INVALID_STATUS")

    if reason not in ALLOWED_REASONS:
        problems.append("UNKNOWN_REASON")

    try:
        expected = parse_int(
            row["expected_bars"],
            "expected_bars",
        )
        available = parse_int(
            row["available_bars"],
            "available_bars",
        )

        if expected <= 0:
            problems.append("INVALID_EXPECTED_BARS")

        if available < 0:
            problems.append("INVALID_AVAILABLE_BARS")

    except ValueError:
        expected = None
        available = None
        problems.append("INVALID_BAR_COUNT")

    contiguous = parse_bool(row["contiguous_bars"])
    agreement = parse_bool(row["hit_agreement"])
    replay_hit = parse_bool(row["replay_hit_30pct"])
    reported_hit = parse_bool(row["reported_hit_30pct"])

    if contiguous is None:
        problems.append("UNKNOWN_CONTIGUITY")

    if status == "MATCH":
        if source != "independent":
            problems.append("MATCH_SOURCE_INVALID")

        if reason != "REPLAY_AGREES_WITH_REPORTED":
            problems.append("MATCH_REASON_INVALID")

        if agreement is not True:
            problems.append("MATCH_AGREEMENT_INVALID")

        if replay_hit is None or reported_hit is None:
            problems.append("MATCH_HIT_MISSING")

        elif replay_hit != reported_hit:
            problems.append("MATCH_HIT_CONFLICT")

        if contiguous is not True:
            problems.append("MATCH_NONCONTIGUOUS")

        if (
            expected is not None
            and available is not None
            and available < expected
        ):
            problems.append("MATCH_INCOMPLETE")

    elif status == "WINDOW_VERIFIED":
        if source != "gap":
            problems.append("WINDOW_SOURCE_INVALID")

        if reason != "GAP_ENTRY_PRICE_NOT_AVAILABLE":
            problems.append("WINDOW_REASON_INVALID")

        if contiguous is not True:
            problems.append("WINDOW_NONCONTIGUOUS")

        if (
            expected is not None
            and available is not None
            and available < expected
        ):
            problems.append("WINDOW_INCOMPLETE")

    elif status == "NOT_COMPARABLE":
        if reason == "INCOMPLETE_WINDOW":
            if (
                expected is not None
                and available is not None
                and available >= expected
            ):
                problems.append("INCOMPLETE_REASON_CONFLICT")

        elif reason == "NON_CONTIGUOUS_WINDOW":
            if contiguous is not False:
                problems.append("GAP_REASON_CONFLICT")

    if source == "gap" and row["entry_price_reported"].strip():
        problems.append("UNEXPECTED_GAP_ENTRY_PRICE")

    if source == "independent":
        if not row["entry_price_reported"].strip():
            problems.append("MISSING_INDEPENDENT_ENTRY_PRICE")

    return {
        "row_number": index,
        "source": source,
        "timeframe": timeframe,
        "market": row["market"],
        "status": status,
        "reason": reason,
        "expected_bars": expected,
        "available_bars": available,
        "contiguous_bars": contiguous,
        "hit_agreement": agreement,
        "diagnostic": (
            "PASS" if not problems else "REVIEW"
        ),
        "problems": "|".join(problems),
    }


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--input",
        required=True,
        help="Directory containing replay reports",
    )

    parser.add_argument(
        "--output",
        required=True,
        help="New diagnostics output directory",
    )

    args = parser.parse_args()

    input_dir = Path(args.input).resolve()
    output_dir = Path(args.output).resolve()

    if input_dir == output_dir:
        raise ValueError(
            "Input and output directories must differ"
        )

    filenames = (
        "replay_details.csv",
        "replay_result.json",
        "replay_manifest.json",
    )

    paths = {
        name: input_dir / name
        for name in filenames
    }

    for name, path in paths.items():
        if not path.is_file():
            raise FileNotFoundError(
                f"Missing input: {name}"
            )

    manifest = load_json(paths["replay_manifest.json"])
    result = load_json(paths["replay_result.json"])

    validate_manifest(manifest, paths)

    if result.get("execution") != "PASS":
        raise ValueError("Replay result is not PASS")

    if result.get("research_decision") != "HOLD":
        raise ValueError("Unexpected research decision")

    rows = read_csv(paths["replay_details.csv"])

    if len(rows) != result.get("processed_events"):
        raise ValueError(
            "CSV row count differs from processed_events"
        )

    diagnostics = [
        validate_row(row, index)
        for index, row in enumerate(rows, start=2)
    ]

    status_counts = Counter(
        item["status"] for item in diagnostics
    )

    reason_counts = Counter(
        item["reason"] for item in diagnostics
    )

    source_timeframe = defaultdict(Counter)

    for item in diagnostics:
        key = (
            item["source"],
            item["timeframe"],
        )

        source_timeframe[key][item["status"]] += 1

    expected_statuses = result.get(
        "status_counts",
        {},
    )

    for status in ALLOWED_STATUSES:
        actual = status_counts[status]
        expected = expected_statuses.get(status, 0)

        if actual != expected:
            raise ValueError(
                f"Status count mismatch: {status}"
            )

    review_rows = [
        item for item in diagnostics
        if item["diagnostic"] == "REVIEW"
    ]

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    detail_path = (
        output_dir / "diagnostics_detail.csv"
    )

    columns = [
        "row_number",
        "source",
        "timeframe",
        "market",
        "status",
        "reason",
        "expected_bars",
        "available_bars",
        "contiguous_bars",
        "hit_agreement",
        "diagnostic",
        "problems",
    ]

    with open(
        detail_path,
        "w",
        encoding="utf-8",
        newline="",
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=columns,
        )

        writer.writeheader()
        writer.writerows(diagnostics)

    output_result = {
        "audit": AUDIT,
        "version": VERSION,
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "execution": (
            "PASS" if not review_rows else "REVIEW"
        ),
        "input_events": len(rows),
        "status_counts": dict(status_counts),
        "reason_counts": dict(reason_counts),
        "source_timeframe": [
            {
                "source": source,
                "timeframe": timeframe,
                "status_counts": dict(counts),
            }
            for (source, timeframe), counts
            in sorted(source_timeframe.items())
        ],
        "review_rows": len(review_rows),
        "review_examples": review_rows[:30],
        "full_population_replay_proven": False,
        "horizon_unit_verified": False,
        "independent_oos_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
        "limitations": [
            "Audits existing replay output only",
            "Does not independently reload OHLCV",
            "Does not establish entry-candle convention",
            "Does not prove trading profitability",
            "Does not repair missing candles",
        ],
    }

    result_path = (
        output_dir / "diagnostics_result.json"
    )

    save_json(result_path, output_result)

    script_path = Path(__file__).resolve()

    output_manifest = {
        "audit": AUDIT,
        "version": VERSION,
        "execution": output_result["execution"],
        "research_decision": "HOLD",
        "script_sha256": sha256(script_path),
        "input_sha256": {
            name: sha256(path)
            for name, path in paths.items()
        },
        "output_sha256": {
            "diagnostics_detail.csv": sha256(
                detail_path
            ),
            "diagnostics_result.json": sha256(
                result_path
            ),
        },
    }

    save_json(
        output_dir / "diagnostics_manifest.json",
        output_manifest,
    )

    print("=" * 60)
    print("Pattern 256 Replay Diagnostics")
    print("=" * 60)
    print(f"Version: {VERSION}")
    print(f"Execution: {output_result['execution']}")
    print(f"Events: {len(rows)}")
    print(f"Review rows: {len(review_rows)}")
    print(f"Statuses: {dict(status_counts)}")
    print(f"Reasons: {dict(reason_counts)}")
    print("Research decision: HOLD")
    print("=" * 60)

    if review_rows:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
