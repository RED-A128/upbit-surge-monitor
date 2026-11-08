
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Horizon Semantics Audit
Clean V001

Purpose:
- Inspect all gap and independent event rows.
- Compare independent evaluation_end_epoch against
  hours-based and bars-based horizon hypotheses.
- Inspect gap expected_candles against both hypotheses.
- Produce balanced, deterministic samples across
  h1, h4 and d1.
- Never modify research source files.
- Never approve training or trading.
"""

import argparse
import csv
import hashlib
import heapq
import json
import math
import sys

from collections import Counter, defaultdict
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

SECONDS = {
    "h1": 3600,
    "h4": 14400,
    "d1": 86400,
}

REQUIRED = {
    "gap": [
        "timeframe",
        "market",
        "signal_utc",
        "entry_utc",
        "horizon_hours",
        "expected_candles",
        "observed_candles",
        "missing_candles",
        "original_outcome",
        "corrected_outcome",
    ],
    "independent": [
        "timeframe",
        "market",
        "signal_timestamp",
        "entry_timestamp",
        "horizon",
        "evaluation_end_epoch",
        "required_bars",
        "observed_bars",
        "status",
        "hit_30pct",
        "entry_price",
    ],
}

SAMPLE_COLUMNS = [
    "source",
    "source_line",
    "timeframe",
    "market",
    "signal_epoch",
    "entry_epoch",
    "horizon",
    "reported_end_epoch",
    "hours_end_epoch",
    "bars_end_epoch",
    "reported_end_matches_hours",
    "reported_end_matches_bars",
    "expected_candles",
    "required_bars",
    "observed_bars",
    "status",
    "hit_30pct",
    "entry_price",
    "diagnostic",
]


def digest(path):
    h = hashlib.sha256()

    with path.open("rb") as stream:
        for chunk in iter(
            lambda: stream.read(1024 * 1024),
            b"",
        ):
            h.update(chunk)

    return h.hexdigest()


def parse_time(value):
    value = str(value or "").strip()

    if not value:
        raise ValueError("empty timestamp")

    try:
        number = float(value)

        if not math.isfinite(number):
            raise ValueError("nonfinite timestamp")

        if abs(number) > 100000000000:
            number /= 1000

        return number

    except ValueError:
        dt = datetime.fromisoformat(
            value.replace("Z", "+00:00")
        )

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        return dt.timestamp()


def number(value):
    value = str(value or "").strip()

    if not value:
        return None

    result = float(value)

    if not math.isfinite(result):
        raise ValueError("nonfinite number")

    return result


def near(a, b):
    if a is None or b is None:
        return None

    return abs(a - b) <= 1.0


def stable_rank(values):
    payload = "|".join(
        str(value) for value in values
    )

    return int.from_bytes(
        hashlib.sha256(
            payload.encode("utf-8")
        ).digest()[:8],
        "big",
    )


def select_sample(heaps, group, item, limit):
    rank = stable_rank([
        item["source"],
        item["source_line"],
        item["market"],
        item["signal_epoch"],
        item["horizon"],
    ])

    # Max-heap behavior using negative rank.
    entry = (
        -rank,
        -int(item["source_line"]),
        item,
    )

    heap = heaps[group]

    if len(heap) < limit:
        heapq.heappush(heap, entry)
        return

    if entry[:2] > heap[0][:2]:
        heapq.heapreplace(heap, entry)


def process_row(kind, row, line):
    tf = str(
        row.get("timeframe", "")
    ).strip().lower()

    market = str(
        row.get("market", "")
    ).strip().upper()

    if tf not in SECONDS:
        raise ValueError("invalid timeframe")

    if not market.startswith("KRW-"):
        raise ValueError("invalid market")

    if kind == "gap":
        signal = parse_time(
            row["signal_utc"]
        )
        entry = parse_time(
            row["entry_utc"]
        )
        horizon = number(
            row["horizon_hours"]
        )
        reported_end = None

        expected = number(
            row["expected_candles"]
        )
        observed = number(
            row["observed_candles"]
        )
        required = None

        status = row.get(
            "corrected_outcome", ""
        )
        hit = ""
        entry_price = ""

    else:
        signal = parse_time(
            row["signal_timestamp"]
        )
        entry = parse_time(
            row["entry_timestamp"]
        )
        horizon = number(
            row["horizon"]
        )

        reported_end = parse_time(
            row["evaluation_end_epoch"]
        )

        expected = None
        required = number(
            row["required_bars"]
        )
        observed = number(
            row["observed_bars"]
        )

        status = row.get("status", "")
        hit = row.get("hit_30pct", "")
        entry_price = row.get(
            "entry_price", ""
        )

    if horizon is None or horizon <= 0:
        raise ValueError("invalid horizon")

    if entry < signal:
        raise ValueError(
            "entry earlier than signal"
        )

    hours_end = entry + horizon * 3600
    bars_end = (
        entry + horizon * SECONDS[tf]
    )

    matches_hours = near(
        reported_end, hours_end
    )
    matches_bars = near(
        reported_end, bars_end
    )

    if kind == "independent":
        if matches_hours and matches_bars:
            diagnostic = "BOTH_HYPOTHESES"

        elif matches_hours:
            diagnostic = "HOURS_HYPOTHESIS"

        elif matches_bars:
            diagnostic = "BARS_HYPOTHESIS"

        else:
            diagnostic = "NEITHER_HYPOTHESIS"

    else:
        # Gap has no explicit evaluation_end_epoch.
        # This is only a candle-count comparison.
        if expected is None:
            diagnostic = "NO_EXPECTED_CANDLES"

        else:
            hour_bars = math.ceil(
                horizon * 3600 / SECONDS[tf]
            )

            matches_hour_count = near(
                expected, hour_bars
            )
            matches_bar_count = near(
                expected, horizon
            )

            if (
                matches_hour_count
                and matches_bar_count
            ):
                diagnostic = "BOTH_COUNT_HYPOTHESES"

            elif matches_hour_count:
                diagnostic = "HOURS_COUNT_HYPOTHESIS"

            elif matches_bar_count:
                diagnostic = "BARS_COUNT_HYPOTHESIS"

            else:
                diagnostic = "NEITHER_COUNT_HYPOTHESIS"

    return {
        "source": kind,
        "source_line": line,
        "timeframe": tf,
        "market": market,
        "signal_epoch": signal,
        "entry_epoch": entry,
        "horizon": horizon,
        "reported_end_epoch": reported_end,
        "hours_end_epoch": hours_end,
        "bars_end_epoch": bars_end,
        "reported_end_matches_hours": matches_hours,
        "reported_end_matches_bars": matches_bars,
        "expected_candles": expected,
        "required_bars": required,
        "observed_bars": observed,
        "status": status,
        "hit_30pct": hit,
        "entry_price": entry_price,
        "diagnostic": diagnostic,
    }


def write_csv(path, rows):
    with path.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as stream:
        writer = csv.DictWriter(
            stream,
            fieldnames=SAMPLE_COLUMNS,
        )

        writer.writeheader()

        for row in rows:
            writer.writerow({
                name: row.get(name, "")
                for name in SAMPLE_COLUMNS
            })


def write_json(path, obj):
    path.write_text(
        json.dumps(
            obj,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        ) + "\n",
        encoding="utf-8",
    )


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
        "--sample-per-group",
        type=int,
        default=100,
    )

    args = parser.parse_args()

    root = Path(args.root).resolve()
    output = Path(args.output).resolve()

    if not root.is_dir():
        raise RuntimeError(
            "Project root missing"
        )

    if (
        output == root
        or root in output.parents
    ):
        raise RuntimeError(
            "Output must be outside project root"
        )

    if output.exists():
        raise RuntimeError(
            "Output already exists"
        )

    if args.sample_per_group < 1:
        raise RuntimeError(
            "Invalid sample size"
        )

    paths = {}

    for kind, relative in SOURCES.items():
        path = root / relative

        if not path.is_file():
            raise RuntimeError(
                f"Missing source: {path}"
            )

        paths[kind] = path

    output.mkdir(
        parents=True,
        exist_ok=False,
    )

    counts = Counter()
    diagnostics = Counter()
    errors = []
    samples = defaultdict(list)
    source_hashes = {}

    for kind, path in paths.items():
        source_hashes[kind] = digest(path)

        with path.open(
            "r",
            encoding="utf-8-sig",
            newline="",
        ) as stream:
            reader = csv.DictReader(stream)

            missing = [
                column
                for column in REQUIRED[kind]
                if column not in (
                    reader.fieldnames or []
                )
            ]

            if missing:
                raise RuntimeError(
                    f"{kind} missing columns: "
                    + ", ".join(missing)
                )

            for line, row in enumerate(
                reader, 2
            ):
                try:
                    event = process_row(
                        kind, row, line
                    )

                except (
                    ValueError,
                    TypeError,
                    KeyError,
                    OverflowError,
                ) as exc:
                    counts[
                        (kind, "invalid", "all")
                    ] += 1

                    if len(errors) < 100:
                        errors.append({
                            "source": kind,
                            "line": line,
                            "error": str(exc),
                        })

                    continue

                tf = event["timeframe"]
                diag = event["diagnostic"]

                counts[
                    (kind, tf, "valid")
                ] += 1

                diagnostics[
                    (kind, tf, diag)
                ] += 1

                select_sample(
                    samples,
                    (kind, tf),
                    event,
                    args.sample_per_group,
                )

    selected = []

    for group in sorted(samples):
        selected.extend(
            entry[2]
            for entry in samples[group]
        )

    selected.sort(
        key=lambda row: (
            row["source"],
            row["timeframe"],
            row["market"],
            row["signal_epoch"],
            row["horizon"],
            row["source_line"],
        )
    )

    count_rows = [
        {
            "source": source,
            "timeframe": timeframe,
            "category": category,
            "count": count,
        }
        for (
            source,
            timeframe,
            category,
        ), count in sorted(counts.items())
    ]

    diagnostic_rows = [
        {
            "source": source,
            "timeframe": timeframe,
            "diagnostic": diagnostic,
            "count": count,
        }
        for (
            source,
            timeframe,
            diagnostic,
        ), count in sorted(
            diagnostics.items()
        )
    ]

    valid_total = sum(
        count
        for (
            source,
            timeframe,
            category,
        ), count in counts.items()
        if category == "valid"
    )

    invalid_total = sum(
        count
        for (
            source,
            timeframe,
            category,
        ), count in counts.items()
        if category == "invalid"
    )

    result = {
        "audit": "pattern_256_horizon_semantics",
        "version": VERSION,
        "execution": "PASS",
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "source_sha256": source_hashes,
        "valid_events": valid_total,
        "invalid_events": invalid_total,
        "sampled_events": len(selected),
        "counts": count_rows,
        "diagnostics": diagnostic_rows,
        "errors": errors,
        "limitations": [
            "No independent OHLCV price replay",
            "Horizon hypotheses are not proof of original intent",
            "Gap source lacks explicit evaluation end",
            "Samples are deterministic, not full OHLCV audits",
            "No causal event attribution proof",
            "No independent OOS validation",
        ],
        "horizon_unit_verified": False,
        "outcome_replay_performed": False,
        "independent_oos_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
    }

    result_path = (
        output / "horizon_result.json"
    )
    sample_path = (
        output / "horizon_samples.csv"
    )
    manifest_path = (
        output / "horizon_manifest.json"
    )

    write_json(
        result_path, result
    )

    write_csv(
        sample_path, selected
    )

    manifest = {
        "audit": "pattern_256_horizon_semantics",
        "version": VERSION,
        "execution": "PASS",
        "script_sha256": digest(
            Path(__file__).resolve()
        ),
        "source_sha256": source_hashes,
        "output_sha256": {
            result_path.name: digest(
                result_path
            ),
            sample_path.name: digest(
                sample_path
            ),
        },
        "research_decision": "HOLD",
    }

    write_json(
        manifest_path, manifest
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
            "[FAIL] "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
            flush=True,
        )

        sys.exit(1)
