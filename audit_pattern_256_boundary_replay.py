
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Boundary Replay - Clean V001

Replays the two identified entry-window boundary cases
against original OHLCV CSV files.

Read-only inputs. No trading or training approval.
"""

import argparse
import csv
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

VERSION = "Clean V001"
AUDIT = "pattern_256_boundary_replay"

PERIOD_SECONDS = {
    "h1": 3600,
    "h4": 14400,
    "d1": 86400,
}

TIME_COLUMNS = (
    "timestamp",
    "candle_date_time_utc",
    "datetime",
    "time",
    "date",
)

OUTPUT_RELATIVE = Path(
    "data/reports/pattern_256_boundary_replay"
)

INPUT_RELATIVE = Path(
    "data/reports/pattern_256_evaluator_policy_review/"
    "policy_review_result.json"
)

CSV_FIELDS = [
    "timeframe",
    "market",
    "convention",
    "horizon",
    "start_timestamp",
    "last_timestamp",
    "observed_rows",
    "missing_slots",
    "duplicate_timestamps",
    "reverse_timestamps",
    "complete_by_rows",
    "continuous_by_time",
    "source_sha256",
]


def sha256(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1048576), b""):
            h.update(block)
    return h.hexdigest()


def parse_time(value):
    value = str(value).strip()

    if not value:
        raise ValueError("Empty timestamp")

    try:
        number = float(value)
        if number > 1e12:
            number /= 1000
        if number > 1e9:
            return int(number)
    except ValueError:
        pass

    value = value.replace("Z", "+00:00")

    dt = datetime.fromisoformat(value)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return int(dt.timestamp())


def iso(epoch):
    if epoch is None:
        return ""
    return datetime.fromtimestamp(
        epoch, timezone.utc
    ).isoformat()


def load_candles(path):
    candles = []
    duplicate_count = 0
    reverse_count = 0
    seen = set()
    previous = None

    with path.open(
        "r", encoding="utf-8-sig", newline=""
    ) as f:
        reader = csv.DictReader(f)

        if not reader.fieldnames:
            raise ValueError("Missing CSV header")

        column = next(
            (
                name for name in TIME_COLUMNS
                if name in reader.fieldnames
            ),
            None,
        )

        if column is None:
            raise ValueError(
                "Unsupported timestamp columns: "
                + ",".join(reader.fieldnames)
            )

        for line, row in enumerate(reader, start=2):
            try:
                epoch = parse_time(row[column])
            except Exception as exc:
                raise ValueError(
                    f"Invalid timestamp at line {line}: {exc}"
                ) from exc

            if epoch in seen:
                duplicate_count += 1

            if previous is not None and epoch < previous:
                reverse_count += 1

            seen.add(epoch)
            candles.append(epoch)
            previous = epoch

    return candles, duplicate_count, reverse_count


def locate_ohlcv(root, timeframe, market):
    path = (
        root / "data" / "ohlcv"
        / timeframe / f"{market}.csv"
    )

    if not path.is_file():
        raise FileNotFoundError(
            f"OHLCV missing: {path}"
        )

    return path


def window_result(
    candles,
    entry_epoch,
    horizon,
    period,
    convention,
    source_hash,
    duplicate_count,
    reverse_count,
    timeframe,
    market,
):
    if convention == "INCLUDE_ENTRY":
        eligible = [
            t for t in candles
            if t >= entry_epoch
        ]
        expected_start = entry_epoch
    else:
        eligible = [
            t for t in candles
            if t > entry_epoch
        ]
        expected_start = entry_epoch + period

    window = eligible[:horizon]
    observed = len(window)

    # Expected slots are measured on the regular
    # calendar grid from the entry boundary.
    expected_slots = {
        expected_start + i * period
        for i in range(horizon)
    }

    actual_slots = set(window)
    missing = len(expected_slots - actual_slots)

    complete = observed == horizon
    continuous = complete and missing == 0

    return {
        "timeframe": timeframe,
        "market": market,
        "convention": convention,
        "horizon": horizon,
        "start_timestamp": (
            iso(window[0]) if window else ""
        ),
        "last_timestamp": (
            iso(window[-1]) if window else ""
        ),
        "observed_rows": observed,
        "missing_slots": missing,
        "duplicate_timestamps": duplicate_count,
        "reverse_timestamps": reverse_count,
        "complete_by_rows": complete,
        "continuous_by_time": continuous,
        "source_sha256": source_hash,
    }


def write_json(path, data):
    with path.open("w", encoding="utf-8") as f:
        json.dump(
            data, f,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        f.write("\n")


def write_csv(path, rows):
    with path.open(
        "w", encoding="utf-8", newline=""
    ) as f:
        writer = csv.DictWriter(
            f, fieldnames=CSV_FIELDS
        )
        writer.writeheader()
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True)
    args = parser.parse_args()

    root = Path(args.root).resolve()

    input_path = root / INPUT_RELATIVE

    if not input_path.is_file():
        raise FileNotFoundError(
            f"Policy review missing: {input_path}"
        )

    with input_path.open(
        "r", encoding="utf-8"
    ) as f:
        previous = json.load(f)

    if previous.get("execution") != "PASS":
        raise ValueError(
            "Previous policy review not PASS"
        )

    if previous.get("research_decision") != "HOLD":
        raise ValueError(
            "Unexpected previous research decision"
        )

    cases = previous.get("boundary_details", [])

    if len(cases) != 2:
        raise ValueError(
            "Expected exactly two boundary cases; "
            f"found {len(cases)}"
        )

    rows = []
    case_reports = []

    for case in cases:
        timeframe = case["timeframe"]
        market = case["market"]
        horizon = int(case["horizon"])

        if timeframe not in PERIOD_SECONDS:
            raise ValueError(
                f"Unsupported timeframe: {timeframe}"
            )

        period = PERIOD_SECONDS[timeframe]
        signal_epoch = parse_time(
            case["signal_timestamp"]
        )
        entry_epoch = parse_time(
            case["entry_timestamp"]
        )

        if entry_epoch <= signal_epoch:
            raise ValueError(
                f"Invalid entry ordering: {market}"
            )

        source = locate_ohlcv(
            root, timeframe, market
        )

        source_hash = sha256(source)

        candles, duplicates, reversals = (
            load_candles(source)
        )

        if duplicates or reversals:
            raise ValueError(
                f"Invalid OHLCV chronology: {market}; "
                f"duplicates={duplicates}, "
                f"reversals={reversals}"
            )

        if entry_epoch not in candles:
            raise ValueError(
                f"Entry candle not found: {market}"
            )

        include = window_result(
            candles,
            entry_epoch,
            horizon,
            period,
            "INCLUDE_ENTRY",
            source_hash,
            duplicates,
            reversals,
            timeframe,
            market,
        )

        exclude = window_result(
            candles,
            entry_epoch,
            horizon,
            period,
            "EXCLUDE_ENTRY",
            source_hash,
            duplicates,
            reversals,
            timeframe,
            market,
        )

        rows.extend([include, exclude])

        case_reports.append({
            "timeframe": timeframe,
            "market": market,
            "signal_timestamp": case[
                "signal_timestamp"
            ],
            "entry_timestamp": case[
                "entry_timestamp"
            ],
            "recorded_status": case[
                "recorded_status"
            ],
            "horizon": horizon,
            "source_path": str(source),
            "source_sha256": source_hash,
            "previous_include_result": case[
                "include_result"
            ],
            "previous_exclude_result": case[
                "exclude_result"
            ],
            "previous_include_missing_slots": int(
                case["include_missing_slots"]
            ),
            "previous_exclude_missing_slots": int(
                case["exclude_missing_slots"]
            ),
            "include": include,
            "exclude": exclude,
            "include_gap_count_matches_previous": (
                include["missing_slots"]
                == int(
                    case["include_missing_slots"]
                )
            ),
            "exclude_gap_count_matches_previous": (
                exclude["missing_slots"]
                == int(
                    case["exclude_missing_slots"]
                )
            ),
        })

    gap_agreement = all(
        case["include_gap_count_matches_previous"]
        and case["exclude_gap_count_matches_previous"]
        for case in case_reports
    )

    result = {
        "audit": AUDIT,
        "version": VERSION,
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "execution": "PASS",
        "classification": (
            "BOUNDARY_REPLAY_MATCH"
            if gap_agreement
            else "BOUNDARY_REPLAY_DISAGREEMENT"
        ),
        "boundary_cases": len(case_reports),
        "window_checks": len(rows),
        "gap_counts_agree_with_previous": (
            gap_agreement
        ),
        "cases": case_reports,
        "limitations": [
            "This replays timestamps, not price outcomes.",
            "A matching window does not prove entry "
            "execution or signal causality.",
            "Missing slots are checked on a fixed "
            "UTC candle grid.",
            "Independent OOS performance is unproven.",
        ],
        "source_policy_confirmed": False,
        "independent_oos_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
        "next_gate": (
            "CONFIRM_ENTRY_AND_GAP_POLICY_THEN_OOS"
        ),
    }

    output_dir = root / OUTPUT_RELATIVE
    output_dir.mkdir(
        parents=True, exist_ok=True
    )

    result_path = (
        output_dir / "boundary_replay_result.json"
    )
    csv_path = (
        output_dir / "boundary_replay_windows.csv"
    )
    manifest_path = (
        output_dir / "boundary_replay_manifest.json"
    )

    write_json(result_path, result)
    write_csv(csv_path, rows)

    manifest = {
        "audit": AUDIT,
        "version": VERSION,
        "outputs": {
            result_path.name: sha256(result_path),
            csv_path.name: sha256(csv_path),
        },
    }

    write_json(manifest_path, manifest)

    print("=" * 60)
    print("Pattern 256 Boundary Replay")
    print("Execution: PASS")
    print("Cases:", len(case_reports))
    print("Window checks:", len(rows))
    print("Gap agreement:", gap_agreement)
    print("Research decision: HOLD")
    print("Output:", output_dir)
    print("=" * 60)

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(
            f"[ERROR] {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        sys.exit(1)
