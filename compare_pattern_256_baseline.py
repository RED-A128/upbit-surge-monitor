
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Baseline Comparison - Clean V001

Compares previous trigger episode hit rates against
deterministically sampled non-trigger candle entries.

Research only.
No training or live trading approval.
Original OHLCV and signal files are read-only.
"""

import argparse
import csv
import hashlib
import json
import math
import os
import tempfile
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path

VERSION = "Clean V001"
HORIZONS = (24, 72, 168)
STEPS = {"h1": 3600, "h4": 14400, "d1": 86400}
SPLITS = ("DEV", "VALIDATION", "TEST")

COLUMNS = [
    "timeframe", "split", "horizon",
    "signal_hits", "signal_completed",
    "signal_hit_rate_pct",
    "baseline_hits", "baseline_completed",
    "baseline_hit_rate_pct",
    "difference_percentage_points",
    "baseline_candidates",
    "baseline_excluded",
]

POLICY = {
    "version": VERSION,
    "baseline": "signal_trigger=0 and signal_stage!=TRIGGER",
    "entry": "next consecutive candle open",
    "target": 0.30,
    "horizons": list(HORIZONS),
    "selection": "smallest SHA256 ranks per market and split",
    "max_candidates_per_market_split": 100,
    "comparison": "unmatched non-trigger baseline",
}


def sha_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for chunk in iter(
            lambda: stream.read(1024 * 1024), b""
        ):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_write(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp = tempfile.mkstemp(
        prefix=".partial_",
        dir=str(path.parent),
    )
    try:
        with os.fdopen(
            fd, "w", encoding="utf-8", newline=""
        ) as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, path)
    finally:
        if os.path.exists(temp):
            os.unlink(temp)


def save_json(path, data):
    atomic_write(
        path,
        json.dumps(
            data, ensure_ascii=False,
            indent=2, sort_keys=True,
        ) + "\n",
    )


def save_csv(path, rows, columns):
    import io
    buffer = io.StringIO()
    writer = csv.DictWriter(
        buffer, fieldnames=columns
    )
    writer.writeheader()
    writer.writerows(rows)
    atomic_write(path, buffer.getvalue())


def utc(value):
    dt = datetime.fromisoformat(
        str(value).replace("Z", "+00:00")
    )
    if dt.tzinfo is None:
        raise ValueError("UTC timezone missing")
    return dt.astimezone(timezone.utc)


def positive(value):
    result = float(value)
    if not math.isfinite(result) or result <= 0:
        raise ValueError("Invalid price")
    return result


def read_csv(path, required):
    with open(
        path, encoding="utf-8-sig", newline=""
    ) as stream:
        reader = csv.DictReader(stream)
        if not reader.fieldnames:
            raise ValueError("CSV header missing")
        missing = set(required) - set(reader.fieldnames)
        if missing:
            raise ValueError(
                "Missing columns: " + str(sorted(missing))
            )
        return list(reader)


def split_for(ts, bounds):
    start, dev_end, validation_end, end = bounds
    if ts < start or ts > end:
        return None
    if ts < dev_end:
        return "DEV"
    if ts < validation_end:
        return "VALIDATION"
    return "TEST"


def parse_signal(row):
    raw = str(row["signal_trigger"]).strip().lower()
    stage = str(row["signal_stage"]).strip().upper()

    if raw not in ("0", "1", "true", "false"):
        raise ValueError("Unexpected trigger value")

    return raw in ("1", "true"), stage


def candidate_rank(tf, market, timestamp):
    value = (
        tf + "|" + market + "|" + timestamp.isoformat()
    )
    return hashlib.sha256(
        value.encode("utf-8")
    ).hexdigest()


def evaluate_market(
    tf, market, source, signal, bounds,
    max_samples, aggregate,
):
    prices = read_csv(
        source,
        ["market", "candle_date_time_utc",
         "open", "high", "low", "close"],
    )
    signals = read_csv(
        signal,
        ["market", "candle_date_time_utc",
         "signal_trigger", "signal_stage"],
    )

    if len(prices) != len(signals):
        raise ValueError("Row count mismatch")

    n = len(prices)
    times = []
    opens = []
    highs = []
    triggers = []
    stages = []

    for i in range(n):
        p = prices[i]
        s = signals[i]

        if p["market"] != market or s["market"] != market:
            raise ValueError("Market mismatch")

        t = utc(p["candle_date_time_utc"])
        st = utc(s["candle_date_time_utc"])

        if t != st:
            raise ValueError("Timestamp mismatch")

        if times and t <= times[-1]:
            raise ValueError("Unsorted timestamps")

        op = positive(p["open"])
        hi = positive(p["high"])
        lo = positive(p["low"])
        cl = positive(p["close"])

        if lo > min(op, cl) or hi < max(op, cl):
            raise ValueError("OHLC relationship invalid")

        trigger, stage = parse_signal(s)

        times.append(t)
        opens.append(op)
        highs.append(hi)
        triggers.append(trigger)
        stages.append(stage)

    # Selection uses only timestamp and signal state.
    # No future OHLCV prices enter candidate ranking.
    selections = defaultdict(list)
    step = timedelta(seconds=STEPS[tf])

    for i in range(n - 1):
        if triggers[i] or stages[i] == "TRIGGER":
            continue

        split = split_for(times[i], bounds)

        if split is None:
            continue

        if times[i + 1] - times[i] != step:
            continue

        if split_for(times[i + 1], bounds) != split:
            continue

        rank = candidate_rank(tf, market, times[i])
        selections[split].append((rank, i))

    for split, ranked in selections.items():
        ranked.sort()
        selected = ranked[:max_samples]

        for _, i in selected:
            entry_index = i + 1
            entry_price = opens[entry_index]

            for horizon in HORIZONS:
                key = (tf, split, horizon)
                bucket = aggregate[key]
                bucket["candidates"] += 1

                end_index = entry_index + horizon - 1

                if end_index >= n:
                    bucket["excluded"] += 1
                    continue

                if split_for(
                    times[end_index], bounds
                ) != split:
                    bucket["excluded"] += 1
                    continue

                valid = True
                for j in range(
                    entry_index, end_index + 1
                ):
                    if times[j] - times[j - 1] != step:
                        valid = False
                        break

                if not valid:
                    bucket["excluded"] += 1
                    continue

                peak = max(
                    highs[entry_index:end_index + 1]
                )

                bucket["completed"] += 1

                if peak >= entry_price * 1.30:
                    bucket["hits"] += 1

    return n, sum(
        min(len(v), max_samples)
        for v in selections.values()
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True)
    parser.add_argument(
        "--max-samples-per-market-split",
        type=int,
        default=100,
    )
    args = parser.parse_args()

    if args.max_samples_per_market_split < 1:
        raise ValueError("Sample limit must be positive")

    root = Path(args.root).resolve()

    previous_dir = (
        root / "data" / "reports"
        / "pattern_256_chronological_oos"
    )
    result_file = previous_dir / "oos_result.json"
    manifest_file = previous_dir / "oos_manifest.json"

    with open(
        result_file, encoding="utf-8"
    ) as stream:
        previous = json.load(stream)

    with open(
        manifest_file, encoding="utf-8"
    ) as stream:
        manifest = json.load(stream)

    expected_hash = manifest["files"]["oos_result.json"]

    if sha_file(result_file) != expected_hash:
        raise RuntimeError(
            "Previous result SHA256 mismatch"
        )

    if previous["execution"] != "PASS":
        raise RuntimeError("Previous evaluation not PASS")

    if previous["version"] != "Clean V001":
        raise RuntimeError("Unexpected previous version")

    if previous["input_pairs"] != 870:
        raise RuntimeError(
            "Unexpected pair count: "
            + str(previous["input_pairs"])
        )

    bounds_data = previous["calendar_boundaries_utc"]
    bounds = (
        utc(bounds_data["start"]),
        utc(bounds_data["dev_end_exclusive"]),
        utc(bounds_data["validation_end_exclusive"]),
        utc(bounds_data["end"]),
    )

    aggregate = defaultdict(
        lambda: {
            "hits": 0,
            "completed": 0,
            "candidates": 0,
            "excluded": 0,
        }
    )

    pair_count = 0
    source_rows = 0
    selected_count = 0

    for tf in STEPS:
        ohlcv_dir = root / "data" / "ohlcv" / tf
        signal_dir = (
            root / "data" / "signals" / "256" / tf
        )

        sources = {
            p.name: p
            for p in ohlcv_dir.glob("KRW-*.csv")
        }
        signals = {
            p.name: p
            for p in signal_dir.glob("KRW-*.csv")
        }

        if set(sources) != set(signals):
            raise RuntimeError(
                "Source/signal mismatch: " + tf
            )

        for filename in sorted(sources):
            market = filename[:-4]

            rows, samples = evaluate_market(
                tf, market,
                sources[filename],
                signals[filename],
                bounds,
                args.max_samples_per_market_split,
                aggregate,
            )

            pair_count += 1
            source_rows += rows
            selected_count += samples

            if pair_count % 25 == 0:
                print(
                    "[PROGRESS]",
                    pair_count,
                    "pairs,",
                    selected_count,
                    "baseline candidates",
                    flush=True,
                )

    if pair_count != previous["input_pairs"]:
        raise RuntimeError(
            "Input pair count changed since prior audit"
        )

    if source_rows != previous["source_rows"]:
        raise RuntimeError(
            "Source row count changed since prior audit"
        )

    signal_summary = {
        (
            row["timeframe"],
            row["split"],
            int(row["horizon"]),
        ): row
        for row in previous["summary"]
    }

    output_rows = []

    for tf in STEPS:
        for split in SPLITS:
            for horizon in HORIZONS:
                key = (tf, split, horizon)
                baseline = aggregate[key]
                signal = signal_summary[key]

                count = baseline["completed"]
                rate = (
                    baseline["hits"] / count * 100.0
                    if count else None
                )

                signal_rate = (
                    float(signal["hit_rate_pct"])
                    if signal["completed"] else None
                )

                difference = (
                    signal_rate - rate
                    if rate is not None
                    and signal_rate is not None
                    else None
                )

                output_rows.append({
                    "timeframe": tf,
                    "split": split,
                    "horizon": horizon,
                    "signal_hits": signal["hits"],
                    "signal_completed": signal["completed"],
                    "signal_hit_rate_pct": signal_rate,
                    "baseline_hits": baseline["hits"],
                    "baseline_completed": count,
                    "baseline_hit_rate_pct": (
                        round(rate, 6)
                        if rate is not None else ""
                    ),
                    "difference_percentage_points": (
                        round(difference, 6)
                        if difference is not None else ""
                    ),
                    "baseline_candidates": baseline["candidates"],
                    "baseline_excluded": baseline["excluded"],
                })

    out = (
        root / "data" / "reports"
        / "pattern_256_baseline_comparison"
    )

    result = {
        "version": VERSION,
        "execution": "PASS",
        "classification": "EXPLORATORY_BASELINE_COMPARISON",
        "policy": POLICY,
        "previous_result_sha256": sha_file(result_file),
        "input_pairs": pair_count,
        "source_rows": source_rows,
        "selected_candidates": selected_count,
        "comparison": output_rows,
        "limitations": [
            "Baseline is sampled from non-trigger candles",
            "Market and volatility distribution are not matched",
            "Trigger events may overlap",
            "Baseline samples may overlap",
            "Calendar TEST was previously exposed",
            "No fees or slippage",
            "No independent OOS proof",
            "No point-in-time proof",
        ],
        "independent_oos_proven": False,
        "profitability_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
    }

    save_csv(
        out / "baseline_comparison.csv",
        output_rows, COLUMNS,
    )
    save_json(out / "baseline_result.json", result)

    lines = [
        "# Pattern 256 Baseline Comparison",
        "",
        "Version: " + VERSION,
        "Execution: PASS",
        "Research decision: HOLD",
        "",
        "| TF | Split | Horizon | Signal % | Baseline % | Difference pp |",
        "|---|---|---:|---:|---:|---:|",
    ]

    for row in output_rows:
        lines.append(
            "| {timeframe} | {split} | {horizon} | "
            "{signal_hit_rate_pct} | "
            "{baseline_hit_rate_pct} | "
            "{difference_percentage_points} |".format(
                **row
            )
        )

    lines.extend([
        "",
        "## Limitations",
        "",
    ])

    for item in result["limitations"]:
        lines.append("- " + item)

    save_text = "\n".join(lines) + "\n"
    atomic_write(out / "baseline_report.md", save_text)

    names = [
        "baseline_result.json",
        "baseline_comparison.csv",
        "baseline_report.md",
    ]

    hashes = {
        name: sha_file(out / name)
        for name in names
    }

    save_json(
        out / "baseline_manifest.json",
        {
            "version": VERSION,
            "files": hashes,
        },
    )

    print(
        json.dumps(
            {
                "execution": "PASS",
                "pairs": pair_count,
                "candidates": selected_count,
                "decision": "HOLD",
            },
            indent=2,
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
