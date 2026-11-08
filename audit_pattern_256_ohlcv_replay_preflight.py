
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 OHLCV Replay Preflight
Clean V001

Read-only diagnostics.
No outcome relabeling.
No training approval.
No trading approval.
"""

import argparse
import csv
import hashlib
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path


VERSION = "Clean V001"

SOURCES = {
    "gap": (
        "data/reports/pattern_256_event_gap_integrity/"
        "gap_integrity_events.csv"
    ),
    "independent": (
        "data/reports/pattern_256_accumulation_independent/"
        "independent_detail.csv"
    ),
}

TIMEFRAMES = {
    "h1": 3600,
    "h4": 14400,
    "d1": 86400,
}

TIMESTAMP_NAMES = [
    "timestamp",
    "timestamp_utc",
    "candle_date_time_utc",
    "datetime",
    "date",
    "time",
    "utc_time",
]

PRICE_NAMES = {
    "open": ["open", "opening_price"],
    "high": ["high", "high_price"],
    "low": ["low", "low_price"],
    "close": ["close", "trade_price"],
}

EVENT_NAMES = {
    "gap": {
        "timeframe": "timeframe",
        "market": "market",
        "signal": "signal_utc",
        "entry": "entry_utc",
        "horizon": "horizon_hours",
    },
    "independent": {
        "timeframe": "timeframe",
        "market": "market",
        "signal": "signal_timestamp",
        "entry": "entry_timestamp",
        "horizon": "horizon",
    },
}


def sha256(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(
            lambda: f.read(1024 * 1024), b""
        ):
            h.update(chunk)
    return h.hexdigest()


def get_header(path):
    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as f:
        reader = csv.reader(f)
        return next(reader, [])


def choose(columns, candidates):
    lookup = {
        str(c).strip().lower(): c
        for c in columns
    }

    for candidate in candidates:
        if candidate.lower() in lookup:
            return lookup[candidate.lower()]

    return None


def to_epoch(value):
    value = str(value).strip()

    if not value:
        raise ValueError("empty timestamp")

    try:
        number = float(value)

        if number > 100000000000:
            number /= 1000

        return number

    except ValueError:
        dt = datetime.fromisoformat(
            value.replace("Z", "+00:00")
        )

        if dt.tzinfo is None:
            dt = dt.replace(
                tzinfo=timezone.utc
            )

        return dt.timestamp()


def read_sample_events(path, kind, limit):
    mapping = EVENT_NAMES[kind]
    events = []
    errors = []

    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as f:
        reader = csv.DictReader(f)

        missing = [
            name
            for name in mapping.values()
            if name not in (reader.fieldnames or [])
        ]

        if missing:
            return [], [
                "Missing required columns: "
                + ", ".join(missing)
            ]

        for line, row in enumerate(reader, 2):
            if len(events) >= limit:
                break

            try:
                tf = row[
                    mapping["timeframe"]
                ].strip().lower()

                market = row[
                    mapping["market"]
                ].strip().upper()

                signal = to_epoch(
                    row[mapping["signal"]]
                )

                entry = to_epoch(
                    row[mapping["entry"]]
                )

                horizon = float(
                    row[mapping["horizon"]]
                )

                if tf not in TIMEFRAMES:
                    raise ValueError(
                        "unknown timeframe"
                    )

                if not market.startswith("KRW-"):
                    raise ValueError(
                        "invalid market"
                    )

                if horizon <= 0:
                    raise ValueError(
                        "invalid horizon"
                    )

                events.append({
                    "source": kind,
                    "timeframe": tf,
                    "market": market,
                    "signal_epoch": signal,
                    "entry_epoch": entry,
                    "horizon_raw": horizon,
                })

            except (ValueError, KeyError) as exc:
                errors.append(
                    f"line {line}: {exc}"
                )

    return events, errors


def inspect_ohlcv(path):
    if not path.is_file():
        return {
            "status": "MISSING",
            "columns": [],
        }

    columns = get_header(path)

    timestamp = choose(
        columns, TIMESTAMP_NAMES
    )

    prices = {
        key: choose(columns, candidates)
        for key, candidates in PRICE_NAMES.items()
    }

    missing = []

    if timestamp is None:
        missing.append("timestamp")

    for key, value in prices.items():
        if value is None:
            missing.append(key)

    result = {
        "status": (
            "SCHEMA_OK"
            if not missing
            else "SCHEMA_INCOMPLETE"
        ),
        "columns": columns,
        "timestamp_column": timestamp,
        "price_columns": prices,
        "missing_fields": missing,
        "file_bytes": path.stat().st_size,
    }

    if missing:
        return result

    rows = 0
    first = None
    last = None
    ordered = True
    duplicates = 0
    previous = None
    invalid = 0

    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as f:
        reader = csv.DictReader(f)

        for row in reader:
            rows += 1

            try:
                current = to_epoch(
                    row[timestamp]
                )

                for col in prices.values():
                    float(row[col])

                if first is None:
                    first = current

                if previous is not None:
                    if current < previous:
                        ordered = False

                    if current == previous:
                        duplicates += 1

                previous = current
                last = current

            except (
                ValueError,
                KeyError,
                TypeError,
            ):
                invalid += 1

    result.update({
        "rows": rows,
        "first_epoch": first,
        "last_epoch": last,
        "ordered": ordered,
        "adjacent_duplicate_timestamps": duplicates,
        "invalid_rows": invalid,
    })

    if invalid or not ordered or duplicates:
        result["status"] = "DATA_REVIEW"

    return result


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--root",
        required=True,
    )

    parser.add_argument(
        "--output",
        required=True,
    )

    parser.add_argument(
        "--sample-per-source",
        type=int,
        default=100,
    )

    args = parser.parse_args()

    root = Path(args.root).resolve()
    output = Path(args.output).resolve()

    if not root.is_dir():
        raise RuntimeError(
            "Project root not found"
        )

    if output == root or root in output.parents:
        raise RuntimeError(
            "Output must be outside project root"
        )

    if output.exists():
        raise RuntimeError(
            "Output directory already exists"
        )

    if args.sample_per_source < 1:
        raise RuntimeError(
            "Sample size must be positive"
        )

    output.mkdir(parents=True)

    source_reports = {}
    all_events = []
    issues = []

    for kind, relative in SOURCES.items():
        path = (root / relative).resolve()

        if not path.is_file():
            raise RuntimeError(
                f"Source missing: {path}"
            )

        columns = get_header(path)

        events, errors = read_sample_events(
            path,
            kind,
            args.sample_per_source,
        )

        source_reports[kind] = {
            "path": relative,
            "sha256": sha256(path),
            "columns": columns,
            "sampled_events": len(events),
            "sample_errors": len(errors),
            "price_candidates": [
                col for col in columns
                if any(
                    token in col.lower()
                    for token in (
                        "price",
                        "open",
                        "high",
                        "low",
                        "close",
                        "target",
                        "return",
                    )
                )
            ],
        }

        all_events.extend(events)

        for error in errors:
            issues.append({
                "scope": kind,
                "issue": error,
            })

    market_pairs = sorted({
        (
            event["timeframe"],
            event["market"],
        )
        for event in all_events
    })

    ohlcv_reports = []
    cache = {}

    for tf, market in market_pairs:
        relative = (
            f"data/ohlcv/{tf}/{market}.csv"
        )

        path = root / relative
        report = inspect_ohlcv(path)

        report["timeframe"] = tf
        report["market"] = market
        report["path"] = relative

        cache[(tf, market)] = report
        ohlcv_reports.append(report)

    event_reports = []

    for event in all_events:
        tf = event["timeframe"]
        market = event["market"]

        report = cache[(tf, market)]

        first = report.get("first_epoch")
        last = report.get("last_epoch")

        # This is a diagnostic hypothesis,
        # NOT a verified horizon-unit rule.
        assumed_end = (
            event["entry_epoch"]
            + event["horizon_raw"] * 3600
        )

        start_covered = (
            first is not None
            and last is not None
            and first <= event["entry_epoch"] <= last
        )

        end_covered = (
            first is not None
            and last is not None
            and first <= assumed_end <= last
        )

        event_reports.append({
            **event,
            "ohlcv_status": report["status"],
            "ohlcv_start_covered": start_covered,
            "assumed_hours_end_covered": end_covered,
            "assumed_hours_end_epoch": assumed_end,
            "horizon_unit_verified": False,
        })

    status_counts = Counter(
        row["status"]
        for row in ohlcv_reports
    )

    result = {
        "audit": "pattern_256_ohlcv_replay_preflight",
        "version": VERSION,
        "execution": "PASS",
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "source_reports": source_reports,
        "sampled_events": len(all_events),
        "ohlcv_market_pairs": len(market_pairs),
        "ohlcv_status_counts": dict(status_counts),
        "sampled_event_start_covered": sum(
            bool(row["ohlcv_start_covered"])
            for row in event_reports
        ),
        "sampled_assumed_end_covered": sum(
            bool(row["assumed_hours_end_covered"])
            for row in event_reports
        ),
        "issues": issues[:100],
        "horizon_unit_verified": False,
        "outcome_replay_performed": False,
        "event_level_attribution_proven": False,
        "independent_oos_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
    }

    def write_json(name, obj):
        path = output / name

        path.write_text(
            json.dumps(
                obj,
                ensure_ascii=False,
                indent=2,
            ) + "\n",
            encoding="utf-8",
        )

        return path

    files = [
        write_json(
            "preflight_result.json",
            result,
        ),
        write_json(
            "preflight_ohlcv.json",
            ohlcv_reports,
        ),
        write_json(
            "preflight_events.json",
            event_reports,
        ),
    ]

    manifest = {
        "version": VERSION,
        "execution": "PASS",
        "script_sha256": sha256(
            Path(__file__).resolve()
        ),
        "output_sha256": {
            path.name: sha256(path)
            for path in files
        },
        "research_decision": "HOLD",
    }

    write_json(
        "preflight_manifest.json",
        manifest,
    )

    print(
        json.dumps(
            result,
            ensure_ascii=False,
            indent=2,
        ),
        flush=True,
    )


if __name__ == "__main__":
    try:
        main()

    except Exception as exc:
        print(
            f"[FAIL] {type(exc).__name__}: {exc}",
            file=sys.stderr,
            flush=True,
        )
        sys.exit(1)
