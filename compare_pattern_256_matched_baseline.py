
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Matched Baseline - Clean V001

Same-market, nearby-calendar non-trigger controls.
Diagnostic research only.

Original data is read-only.
No automatic trading or model training.
"""

import argparse
import bisect
import csv
import hashlib
import json
import math
import os
import tempfile
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

VERSION = "Clean V001"
TIMEFRAMES = {"h1": 3600, "h4": 14400, "d1": 86400}
HORIZONS = (24, 72, 168)
SPLITS = ("DEV", "VALIDATION", "TEST")
TARGET = 1.30
MAX_DAYS = 30

FIELDS = [
    "timeframe", "split", "horizon",
    "matched_pairs", "signal_hits", "control_hits",
    "signal_rate_pct", "control_rate_pct",
    "difference_pp", "signal_only_hits",
    "control_only_hits", "both_hits", "neither_hits",
]


def digest(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1048576), b""):
            h.update(chunk)
    return h.hexdigest()


def write_atomic(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(
        dir=str(path.parent), prefix=".partial_"
    )
    try:
        with os.fdopen(
            fd, "w", encoding="utf-8", newline=""
        ) as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def write_json(path, value):
    write_atomic(
        path,
        json.dumps(
            value, ensure_ascii=False,
            indent=2, sort_keys=True
        ) + "\n"
    )


def write_csv(path, rows, columns):
    import io
    stream = io.StringIO()
    writer = csv.DictWriter(
        stream, fieldnames=columns
    )
    writer.writeheader()
    writer.writerows(rows)
    write_atomic(path, stream.getvalue())


def parse_time(value):
    dt = datetime.fromisoformat(
        str(value).replace("Z", "+00:00")
    )
    if dt.tzinfo is None:
        raise ValueError("Missing UTC timezone")
    return int(dt.astimezone(timezone.utc).timestamp())


def parse_price(value):
    v = float(value)
    if not math.isfinite(v) or v <= 0:
        raise ValueError("Invalid price")
    return v


def parse_trigger(value):
    v = str(value).strip().lower()
    if v not in ("0", "1", "false", "true"):
        raise ValueError("Invalid signal_trigger: " + v)
    return v in ("1", "true")


def load_csv(path, required):
    with open(
        path, encoding="utf-8-sig", newline=""
    ) as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError("Empty CSV header")
        missing = set(required) - set(reader.fieldnames)
        if missing:
            raise ValueError(
                f"{path}: missing {sorted(missing)}"
            )
        return list(reader)


def get_split(t, bounds):
    start, dev_end, validation_end, end = bounds
    if t < start or t > end:
        return None
    if t < dev_end:
        return "DEV"
    if t < validation_end:
        return "VALIDATION"
    return "TEST"


def stable_rank(tf, market, timestamp):
    data = f"{tf}|{market}|{timestamp}"
    return hashlib.sha256(
        data.encode("utf-8")
    ).hexdigest()


def load_pair(tf, market, price_path, signal_path):
    prices = load_csv(
        price_path,
        ["market", "candle_date_time_utc",
         "open", "high", "low", "close"]
    )
    signals = load_csv(
        signal_path,
        ["market", "candle_date_time_utc",
         "signal_trigger", "signal_stage"]
    )

    if len(prices) != len(signals):
        raise ValueError("OHLCV/signal length mismatch")

    times, opens, highs = [], [], []
    triggers, stages = [], []

    for p, s in zip(prices, signals):
        if p["market"] != market or s["market"] != market:
            raise ValueError("Market mismatch")

        t = parse_time(p["candle_date_time_utc"])
        st = parse_time(s["candle_date_time_utc"])

        if t != st:
            raise ValueError("Timestamp mismatch")

        if times and t <= times[-1]:
            raise ValueError("Non-increasing timestamps")

        op = parse_price(p["open"])
        hi = parse_price(p["high"])
        lo = parse_price(p["low"])
        cl = parse_price(p["close"])

        if lo > min(op, cl) or hi < max(op, cl):
            raise ValueError("Invalid OHLC relationships")

        times.append(t)
        opens.append(op)
        highs.append(hi)
        triggers.append(
            parse_trigger(s["signal_trigger"])
        )
        stages.append(
            str(s["signal_stage"]).strip().upper()
        )

    return times, opens, highs, triggers, stages


def contiguous_prefix(times, step):
    # prefix[i] = count of missing candle transitions
    # among transitions ending at or before index i.
    prefix = [0] * len(times)
    for i in range(1, len(times)):
        prefix[i] = prefix[i - 1] + int(
            times[i] - times[i - 1] != step
        )
    return prefix


def eligible_window(
    index, horizon, times, prefix, bounds
):
    entry = index + 1
    end = entry + horizon - 1

    if end >= len(times):
        return False

    split = get_split(times[index], bounds)
    if split is None:
        return False

    if get_split(times[entry], bounds) != split:
        return False

    if get_split(times[end], bounds) != split:
        return False

    # Includes transition from signal candle to entry.
    return prefix[end] == prefix[index]


def hit(index, horizon, opens, highs):
    entry = index + 1
    end = entry + horizon
    return max(highs[entry:end]) >= (
        opens[entry] * TARGET
    )


def process_pair(
    tf, market, source, signal, bounds,
    sample_limit, max_distance_seconds,
    stats, diagnostics
):
    (
        times, opens, highs,
        triggers, stages
    ) = load_pair(tf, market, source, signal)

    n = len(times)
    if n < 2:
        return n

    prefix = contiguous_prefix(
        times, TIMEFRAMES[tf]
    )

    # Only rising-edge triggers are signal episodes.
    # No future prices are used for selection.
    episodes = defaultdict(list)
    controls = defaultdict(list)

    for i in range(n - 1):
        split = get_split(times[i], bounds)
        if split is None:
            continue

        if triggers[i] and (
            i == 0 or not triggers[i - 1]
        ):
            episodes[split].append(i)

        if (
            not triggers[i]
            and stages[i] != "TRIGGER"
        ):
            controls[split].append(i)

    for split in SPLITS:
        episode_indices = episodes[split]
        control_indices = controls[split]

        diagnostics[(tf, split)]["episodes_found"] += (
            len(episode_indices)
        )

        # Stable deterministic episode sampling.
        # Sampling does not use future OHLCV.
        episode_indices = sorted(
            episode_indices,
            key=lambda i: stable_rank(
                tf, market, times[i]
            )
        )[:sample_limit]

        diagnostics[(tf, split)]["episodes_sampled"] += (
            len(episode_indices)
        )

        # Controls remain sorted by time.
        # Each control is used at most once per market.
        used_controls = set()

        for i in episode_indices:
            if not eligible_window(
                i, max(HORIZONS),
                times, prefix, bounds
            ):
                diagnostics[(tf, split)][
                    "signal_full_window_unavailable"
                ] += 1
                continue

            pos = bisect.bisect_left(
                control_indices, i
            )

            # Search nearby controls in time order.
            # Prefer smallest calendar distance;
            # tie-break by earlier timestamp.
            left = pos - 1
            right = pos
            selected = None

            while (
                left >= 0
                or right < len(control_indices)
            ):
                lidx = (
                    control_indices[left]
                    if left >= 0 else None
                )
                ridx = (
                    control_indices[right]
                    if right < len(control_indices)
                    else None
                )

                ld = (
                    abs(times[i] - times[lidx])
                    if lidx is not None
                    else float("inf")
                )
                rd = (
                    abs(times[i] - times[ridx])
                    if ridx is not None
                    else float("inf")
                )

                if min(ld, rd) > max_distance_seconds:
                    break

                if ld <= rd:
                    candidate = lidx
                    left -= 1
                else:
                    candidate = ridx
                    right += 1

                if candidate in used_controls:
                    continue

                if not eligible_window(
                    candidate, max(HORIZONS),
                    times, prefix, bounds
                ):
                    continue

                selected = candidate
                break

            if selected is None:
                diagnostics[(tf, split)][
                    "no_valid_control"
                ] += 1
                continue

            used_controls.add(selected)
            diagnostics[(tf, split)][
                "matched_pairs"
            ] += 1

            for horizon in HORIZONS:
                # Both windows are valid because the
                # longest horizon was checked above.
                a = hit(i, horizon, opens, highs)
                b = hit(
                    selected, horizon, opens, highs
                )

                bucket = stats[(tf, split, horizon)]
                bucket["pairs"] += 1
                bucket["signal_hits"] += int(a)
                bucket["control_hits"] += int(b)

                if a and b:
                    bucket["both"] += 1
                elif a:
                    bucket["signal_only"] += 1
                elif b:
                    bucket["control_only"] += 1
                else:
                    bucket["neither"] += 1

    return n


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True)
    parser.add_argument(
        "--max-episodes-per-market-split",
        type=int, default=100
    )
    parser.add_argument(
        "--max-match-days",
        type=int, default=MAX_DAYS
    )
    args = parser.parse_args()

    if args.max_episodes_per_market_split < 1:
        raise ValueError("Invalid episode limit")

    if args.max_match_days < 1:
        raise ValueError("Invalid match distance")

    root = Path(args.root).resolve()

    previous_dir = (
        root / "data" / "reports"
        / "pattern_256_baseline_comparison"
    )
    previous_path = (
        previous_dir / "baseline_result.json"
    )
    manifest_path = (
        previous_dir / "baseline_manifest.json"
    )

    with open(
        previous_path, encoding="utf-8"
    ) as f:
        previous = json.load(f)

    with open(
        manifest_path, encoding="utf-8"
    ) as f:
        manifest = json.load(f)

    expected = manifest["files"][
        "baseline_result.json"
    ]
    if digest(previous_path) != expected:
        raise RuntimeError(
            "Previous baseline SHA256 mismatch"
        )

    if previous["execution"] != "PASS":
        raise RuntimeError("Previous baseline not PASS")

    if previous["research_decision"] != "HOLD":
        raise RuntimeError(
            "Unexpected previous research decision"
        )

    if previous["version"] != "Clean V001":
        raise RuntimeError("Unexpected previous version")

    original_dir = (
        root / "data" / "reports"
        / "pattern_256_chronological_oos"
    )
    original_path = original_dir / "oos_result.json"

    if digest(original_path) != (
        previous["previous_result_sha256"]
    ):
        raise RuntimeError(
            "Chronological OOS SHA256 mismatch"
        )

    with open(
        original_path, encoding="utf-8"
    ) as f:
        original = json.load(f)

    b = original["calendar_boundaries_utc"]
    bounds = (
        parse_time(b["start"]),
        parse_time(b["dev_end_exclusive"]),
        parse_time(
            b["validation_end_exclusive"]
        ),
        parse_time(b["end"]),
    )

    stats = defaultdict(
        lambda: {
            "pairs": 0,
            "signal_hits": 0,
            "control_hits": 0,
            "signal_only": 0,
            "control_only": 0,
            "both": 0,
            "neither": 0,
        }
    )

    diagnostics = defaultdict(
        lambda: defaultdict(int)
    )

    pairs = 0
    rows_total = 0

    for tf in TIMEFRAMES:
        price_dir = root / "data" / "ohlcv" / tf
        signal_dir = (
            root / "data" / "signals" / "256" / tf
        )

        prices = {
            p.name: p
            for p in price_dir.glob("KRW-*.csv")
        }
        signals = {
            p.name: p
            for p in signal_dir.glob("KRW-*.csv")
        }

        if not prices or set(prices) != set(signals):
            raise RuntimeError(
                "Input pairs missing/mismatched: " + tf
            )

        for filename in sorted(prices):
            market = filename[:-4]

            count = process_pair(
                tf, market,
                prices[filename],
                signals[filename],
                bounds,
                args.max_episodes_per_market_split,
                args.max_match_days * 86400,
                stats, diagnostics
            )

            pairs += 1
            rows_total += count

            if pairs % 25 == 0:
                print(
                    f"[PROGRESS] {pairs} pairs",
                    flush=True
                )

    if pairs != previous["input_pairs"]:
        raise RuntimeError(
            "Input pair count mismatch"
        )

    if rows_total != previous["source_rows"]:
        raise RuntimeError(
            "Source row count mismatch"
        )

    comparison = []

    for tf in TIMEFRAMES:
        for split in SPLITS:
            for horizon in HORIZONS:
                s = stats[(tf, split, horizon)]
                n = s["pairs"]

                sr = (
                    100 * s["signal_hits"] / n
                    if n else None
                )
                cr = (
                    100 * s["control_hits"] / n
                    if n else None
                )

                comparison.append({
                    "timeframe": tf,
                    "split": split,
                    "horizon": horizon,
                    "matched_pairs": n,
                    "signal_hits": s["signal_hits"],
                    "control_hits": s["control_hits"],
                    "signal_rate_pct": (
                        round(sr, 6)
                        if sr is not None else ""
                    ),
                    "control_rate_pct": (
                        round(cr, 6)
                        if cr is not None else ""
                    ),
                    "difference_pp": (
                        round(sr - cr, 6)
                        if sr is not None else ""
                    ),
                    "signal_only_hits": s["signal_only"],
                    "control_only_hits": s["control_only"],
                    "both_hits": s["both"],
                    "neither_hits": s["neither"],
                })

    diagnostic_rows = []

    for tf in TIMEFRAMES:
        for split in SPLITS:
            item = diagnostics[(tf, split)]
            diagnostic_rows.append({
                "timeframe": tf,
                "split": split,
                "episodes_found": item["episodes_found"],
                "episodes_sampled": item["episodes_sampled"],
                "signal_full_window_unavailable":
                    item["signal_full_window_unavailable"],
                "no_valid_control":
                    item["no_valid_control"],
                "matched_pairs": item["matched_pairs"],
            })

    output_dir = (
        root / "data" / "reports"
        / "pattern_256_matched_baseline"
    )

    result = {
        "version": VERSION,
        "execution": "PASS",
        "classification":
            "EXPLORATORY_CALENDAR_MATCHED_COMPARISON",
        "input_pairs": pairs,
        "source_rows": rows_total,
        "max_match_days": args.max_match_days,
        "max_episodes_per_market_split":
            args.max_episodes_per_market_split,
        "signal_definition":
            "rising_edge_signal_trigger",
        "control_definition":
            "non_trigger_non_TRIGGER_stage",
        "selection":
            "deterministic_SHA256_episode_sampling",
        "matching":
            "same_market_same_split_nearest_time",
        "window_policy":
            "both_sides_require_full_168_candles",
        "target_return": 0.30,
        "entry": "next_consecutive_candle_open",
        "previous_baseline_sha256":
            digest(previous_path),
        "comparison": comparison,
        "diagnostics": diagnostic_rows,
        "limitations": [
            "Calendar TEST has already been exposed",
            "Matching does not control volatility",
            "Only full 168-candle windows are used",
            "Matched signal episodes can overlap",
            "Control windows can overlap",
            "Current OHLCV snapshots are not PIT",
            "No causal signal availability proof",
            "No fees, slippage or market impact",
            "Intrabar high does not prove fill",
            "Not an independent OOS test",
        ],
        "independent_oos_proven": False,
        "point_in_time_proven": False,
        "profitability_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
    }

    write_json(
        output_dir / "matched_result.json",
        result
    )
    write_csv(
        output_dir / "matched_comparison.csv",
        comparison, FIELDS
    )
    write_csv(
        output_dir / "matched_diagnostics.csv",
        diagnostic_rows,
        [
            "timeframe", "split", "episodes_found",
            "episodes_sampled",
            "signal_full_window_unavailable",
            "no_valid_control", "matched_pairs",
        ]
    )

    lines = [
        "# Pattern 256 Matched Baseline",
        "",
        "Version: " + VERSION,
        "Execution: PASS",
        "Research decision: HOLD",
        "",
        "## Comparison",
        "",
        "| TF | Split | Horizon | Pairs | Signal % | Control % | Difference pp |",
        "|---|---|---:|---:|---:|---:|---:|",
    ]

    for row in comparison:
        lines.append(
            "| {timeframe} | {split} | {horizon} | "
            "{matched_pairs} | {signal_rate_pct} | "
            "{control_rate_pct} | {difference_pp} |"
            .format(**row)
        )

    lines.extend([
        "",
        "## Limitations",
        "",
    ])
    lines.extend(
        "- " + x for x in result["limitations"]
    )

    write_atomic(
        output_dir / "matched_report.md",
        "\n".join(lines) + "\n"
    )

    names = [
        "matched_result.json",
        "matched_comparison.csv",
        "matched_diagnostics.csv",
        "matched_report.md",
    ]

    write_json(
        output_dir / "matched_manifest.json",
        {
            "version": VERSION,
            "files": {
                name: digest(output_dir / name)
                for name in names
            },
        }
    )

    print(json.dumps(
        {
            "execution": "PASS",
            "input_pairs": pairs,
            "source_rows": rows_total,
            "test_results": [
                r for r in comparison
                if r["split"] == "TEST"
            ],
            "research_decision": "HOLD",
        },
        ensure_ascii=False,
        indent=2
    ), flush=True)


if __name__ == "__main__":
    main()
