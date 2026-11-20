
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Dedup + Baseline + Temporal Audit
Clean V001

Read-only research audit.
No training, no orders, no automatic trading.

Requires:
 data/ohlcv/{h4,d1}/KRW-*.csv
 data/signals/256/{h4,d1}/KRW-*.csv

Python standard library only.
"""

import argparse
import csv
import hashlib
import json
import math
import random
import sys

from collections import Counter
from datetime import datetime, timezone
from pathlib import Path


VERSION = "Clean V001"
SEED = 2562026

CONFIG = {
    "h4": (14400, 42),
    "d1": (86400, 7),
}

TIME_NAMES = (
    "candle_date_time_utc",
    "timestamp",
    "datetime",
    "time",
)

OPEN_NAMES = (
    "opening_price",
    "open",
)

HIGH_NAMES = (
    "high_price",
    "high",
)


def digest(path):
    h = hashlib.sha256()

    with path.open("rb") as f:
        for block in iter(
            lambda: f.read(1048576),
            b"",
        ):
            h.update(block)

    return h.hexdigest()


def save_json(path, value):
    path.write_text(
        json.dumps(
            value,
            ensure_ascii=False,
            indent=2,
            allow_nan=False,
        ) + "\n",
        encoding="utf-8",
    )


def column(fields, names):
    for name in names:
        if name in fields:
            return name

    raise ValueError(
        f"Missing column {names}; got {fields}"
    )


def utc(value):
    s = str(value).strip()

    if s.endswith("Z"):
        s = s[:-1] + "+00:00"

    t = datetime.fromisoformat(s)

    if t.tzinfo is None:
        raise ValueError(
            "Timestamp must include UTC offset"
        )

    return t.astimezone(timezone.utc)


def positive(value):
    v = float(value)

    if not math.isfinite(v) or v <= 0:
        raise ValueError(
            f"Invalid price: {value}"
        )

    return v


def trigger(value):
    s = str(value or "").strip().lower()

    if s in ("1", "1.0", "true"):
        return True

    if s in ("0", "0.0", "false", ""):
        return False

    raise ValueError(
        f"Invalid signal_trigger: {value}"
    )


def load_market(price_path, signal_path):
    times = []
    opens = []
    highs = []
    signals = []

    with price_path.open(
        encoding="utf-8-sig",
        newline="",
    ) as pf, signal_path.open(
        encoding="utf-8-sig",
        newline="",
    ) as sf:

        pr = csv.DictReader(pf)
        sr = csv.DictReader(sf)

        pc = pr.fieldnames or []
        sc = sr.fieldnames or []

        pt = column(pc, TIME_NAMES)
        st = column(sc, TIME_NAMES)
        op = column(pc, OPEN_NAMES)
        hi = column(pc, HIGH_NAMES)

        if "signal_trigger" not in sc:
            raise ValueError(
                "Missing signal_trigger"
            )

        for number, p in enumerate(pr, 1):
            s = next(sr, None)

            if s is None:
                raise ValueError(
                    "Signal rows shorter than OHLCV"
                )

            t = utc(p[pt])

            if t != utc(s[st]):
                raise ValueError(
                    f"Time mismatch at row {number}"
                )

            if times and t <= times[-1]:
                raise ValueError(
                    "Non-increasing timestamps"
                )

            o = positive(p[op])
            h = positive(p[hi])

            if h < o * (1 - 1e-10):
                raise ValueError(
                    f"High below open at {number}"
                )

            times.append(t)
            opens.append(o)
            highs.append(h)
            signals.append(
                trigger(s["signal_trigger"])
            )

        if next(sr, None) is not None:
            raise ValueError(
                "Signal rows longer than OHLCV"
            )

    if not times:
        raise ValueError("Empty market")

    return times, opens, highs, signals


def gaps_prefix(times, seconds):
    prefix = [0] * len(times)

    for i in range(1, len(times)):
        elapsed = (
            times[i] - times[i - 1]
        ).total_seconds()

        ratio = elapsed / seconds
        rounded = round(ratio)

        if (
            rounded < 1
            or abs(ratio - rounded) > 1e-8
        ):
            raise ValueError(
                "Invalid candle spacing"
            )

        prefix[i] = (
            prefix[i - 1]
            + int(rounded != 1)
        )

    return prefix


def valid(prefix, i, horizon, warmup):
    if i < warmup:
        return False

    if i + horizon >= len(prefix):
        return False

    return (
        prefix[i] == prefix[i - warmup]
        and
        prefix[i + horizon] == prefix[i]
    )


def hit(opens, highs, i, horizon):
    target = opens[i + 1] * 1.30

    return (
        max(highs[i + 1:i + horizon + 1])
        >= target
    )


def choose_nonoverlap(indices, horizon):
    selected = []
    last = -horizon - 1

    for i in indices:
        if i > last + horizon:
            selected.append(i)
            last = i

    return selected


def random_nonoverlap(
    candidates,
    target_count,
    horizon,
    rng,
):
    """
    Random priority selection without
    overlapping 7-day event windows.

    Never select based on future returns.
    """

    shuffled = list(candidates)
    rng.shuffle(shuffled)

    selected = []
    occupied = set()

    for i in shuffled:
        if len(selected) >= target_count:
            break

        # Block all candidate signal indices
        # within horizon candles.
        if i in occupied:
            continue

        selected.append(i)

        for j in range(
            i - horizon,
            i + horizon + 1,
        ):
            occupied.add(j)

    return sorted(selected)


def period_of(times, i, horizon, cutoff):
    """
    TRAIN:
      Full outcome ends before cutoff.

    TEST:
      Signal is on or after cutoff.

    PURGED:
      Signal is before cutoff but its
      outcome reaches cutoff.
    """

    if times[i + horizon] < cutoff:
        return "train"

    if times[i] >= cutoff:
        return "test"

    return "purged"


def counts_for(
    indices,
    opens,
    highs,
    horizon,
):
    n = len(indices)

    hits = sum(
        hit(opens, highs, i, horizon)
        for i in indices
    )

    return {
        "events": n,
        "hits": hits,
        "misses": n - hits,
        "hit_rate_pct": (
            round(hits * 100 / n, 6)
            if n
            else None
        ),
    }


def summarize(records, tf, period, field):
    events = sum(
        r[period][field]["events"]
        for r in records
        if r["timeframe"] == tf
    )

    hits = sum(
        r[period][field]["hits"]
        for r in records
        if r["timeframe"] == tf
    )

    return {
        "events": events,
        "hits": hits,
        "misses": events - hits,
        "hit_rate_pct": (
            round(100 * hits / events, 6)
            if events
            else None
        ),
    }


def run(root, warmup, split, seed):
    records = []
    boundaries = {}

    # Establish one global calendar split
    # separately for each timeframe.
    for tf in CONFIG:
        folder = root / "data/ohlcv" / tf
        files = sorted(
            folder.glob("KRW-*.csv")
        )

        if len(files) != 290:
            raise ValueError(
                f"{tf}: expected 290 markets, "
                f"found {len(files)}"
            )

        first = None
        last = None

        for path in files:
            with path.open(
                encoding="utf-8-sig",
                newline="",
            ) as f:
                reader = csv.DictReader(f)
                tc = column(
                    reader.fieldnames or [],
                    TIME_NAMES,
                )

                for row in reader:
                    t = utc(row[tc])

                    if first is None or t < first:
                        first = t

                    if last is None or t > last:
                        last = t

        span = last - first

        cutoff = first + span * split

        boundaries[tf] = {
            "first_utc": first.isoformat(),
            "last_utc": last.isoformat(),
            "cutoff_utc": cutoff.isoformat(),
        }

    for tf, (seconds, horizon) in CONFIG.items():
        price_dir = root / "data/ohlcv" / tf
        signal_dir = root / "data/signals/256" / tf

        prices = {
            p.stem: p
            for p in price_dir.glob("KRW-*.csv")
        }

        signals = {
            p.stem: p
            for p in signal_dir.glob("KRW-*.csv")
        }

        if set(prices) != set(signals):
            raise ValueError(
                f"{tf}: market mismatch"
            )

        cutoff = utc(
            boundaries[tf]["cutoff_utc"]
        )

        for number, market in enumerate(
            sorted(prices),
            1,
        ):
            times, opens, highs, flags = (
                load_market(
                    prices[market],
                    signals[market],
                )
            )

            prefix = gaps_prefix(
                times,
                seconds,
            )

            eligible = [
                i
                for i in range(len(times))
                if valid(
                    prefix,
                    i,
                    horizon,
                    warmup,
                )
            ]

            signal_indices = [
                i
                for i in eligible
                if flags[i]
            ]

            original_signals = sum(flags)

            record = {
                "timeframe": tf,
                "market": market,
                "original_signals": (
                    original_signals
                ),
                "eligible_signals": (
                    len(signal_indices)
                ),
            }

            for period in ("train", "test"):
                signal_pool = [
                    i
                    for i in signal_indices
                    if period_of(
                        times,
                        i,
                        horizon,
                        cutoff,
                    ) == period
                ]

                baseline_pool = [
                    i
                    for i in eligible
                    if period_of(
                        times,
                        i,
                        horizon,
                        cutoff,
                    ) == period
                ]

                dedup = choose_nonoverlap(
                    signal_pool,
                    horizon,
                )

                # Per-market, per-period
                # deterministic random seed.
                material = (
                    f"{seed}|{tf}|"
                    f"{market}|{period}"
                )

                local_seed = int(
                    hashlib.sha256(
                        material.encode("utf-8")
                    ).hexdigest()[:16],
                    16,
                )

                rng = random.Random(
                    local_seed
                )

                baseline = random_nonoverlap(
                    baseline_pool,
                    len(dedup),
                    horizon,
                    rng,
                )

                # Equal-size comparison only.
                matched = min(
                    len(dedup),
                    len(baseline),
                )

                signal_selected = dedup[:matched]
                baseline_selected = baseline[:matched]

                record[period] = {
                    "raw_eligible_signals": (
                        len(signal_pool)
                    ),
                    "deduplicated_signals": (
                        len(dedup)
                    ),
                    "matched_events": matched,
                    "unmatched_signals": (
                        len(dedup) - matched
                    ),
                    "signal": counts_for(
                        signal_selected,
                        opens,
                        highs,
                        horizon,
                    ),
                    "baseline": counts_for(
                        baseline_selected,
                        opens,
                        highs,
                        horizon,
                    ),
                }

            records.append(record)

            if number % 25 == 0:
                print(
                    f"[{tf}] "
                    f"{number}/{len(prices)}",
                    flush=True,
                )

    summary = {}

    for tf in CONFIG:
        summary[tf] = {}

        for period in ("train", "test"):
            signal = summarize(
                records,
                tf,
                period,
                "signal",
            )

            baseline = summarize(
                records,
                tf,
                period,
                "baseline",
            )

            difference = None

            if (
                signal["hit_rate_pct"] is not None
                and baseline["hit_rate_pct"]
                is not None
            ):
                difference = round(
                    signal["hit_rate_pct"]
                    - baseline["hit_rate_pct"],
                    6,
                )

            summary[tf][period] = {
                "signal": signal,
                "baseline": baseline,
                "lift_percentage_points": (
                    difference
                ),
                "deduplicated_signals": sum(
                    r[period][
                        "deduplicated_signals"
                    ]
                    for r in records
                    if r["timeframe"] == tf
                ),
                "unmatched_signals": sum(
                    r[period][
                        "unmatched_signals"
                    ]
                    for r in records
                    if r["timeframe"] == tf
                ),
            }

    return {
        "version": VERSION,
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "execution": "PASS",
        "classification": (
            "DEDUP_BASELINE_TEMPORAL_DESCRIPTIVE"
        ),
        "threshold_pct": 30,
        "outcome_days": 7,
        "split_fraction": split,
        "random_seed": seed,
        "warmup_candles": warmup,
        "boundaries": boundaries,
        "summary": summary,
        "records": records,
        "research_limits": [
            "Temporal split is descriptive.",
            "No model is trained.",
            "No independent OOS proof.",
            "Random baseline is one seeded draw.",
            "Baseline does not match signal "
            "market regimes or volatility.",
            "Matched samples may be smaller "
            "than deduplicated signal counts.",
            "Signal selection is chronological.",
            "Overlapping outcomes across "
            "different markets remain possible.",
            "No fees or slippage.",
            "No realized trading profit.",
            "Point-in-time market universe "
            "is not independently proven.",
        ],
        "independent_oos_proven": False,
        "profitability_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
    }


def report(result):
    lines = [
        "# Pattern 256 Dedup/Baseline/Temporal Audit",
        "",
        f"Version: {result['version']}",
        f"Execution: {result['execution']}",
        "Research decision: HOLD",
        "",
        "## Results",
        "",
        (
            "| TF | Period | Signal N | "
            "Signal hit % | Baseline hit % | "
            "Lift (pp) |"
        ),
        "|---|---|---:|---:|---:|---:|",
    ]

    for tf, periods in result["summary"].items():
        for period, item in periods.items():
            s = item["signal"]
            b = item["baseline"]

            def fmt(v):
                return (
                    f"{v:.4f}"
                    if v is not None
                    else "N/A"
                )

            lines.append(
                f"| {tf} | {period} "
                f"| {s['events']} "
                f"| {fmt(s['hit_rate_pct'])} "
                f"| {fmt(b['hit_rate_pct'])} "
                f"| {fmt(item['lift_percentage_points'])} |"
            )

    lines.extend([
        "",
        "## Limitations",
        "",
        "- No independent OOS proof",
        "- No realized profit calculation",
        "- Random baseline is a single draw",
        "- No market-regime matching",
        "- Research decision remains HOLD",
        "",
    ])

    return "\n".join(lines)


def main():
    p = argparse.ArgumentParser()

    p.add_argument(
        "--root",
        default=".",
    )

    p.add_argument(
        "--warmup",
        type=int,
        default=3,
    )

    p.add_argument(
        "--split",
        type=float,
        default=0.70,
    )

    p.add_argument(
        "--seed",
        type=int,
        default=SEED,
    )

    args = p.parse_args()

    if not 0 < args.split < 1:
        raise ValueError("Invalid split")

    if not 1 <= args.warmup <= 100:
        raise ValueError("Invalid warmup")

    root = Path(args.root).resolve()

    result = run(
        root,
        args.warmup,
        args.split,
        args.seed,
    )

    output = (
        root
        / "data/reports/"
        "pattern_256_dedup_baseline_temporal"
    )

    output.mkdir(
        parents=True,
        exist_ok=True,
    )

    result_path = output / "result.json"
    report_path = output / "report.md"
    manifest_path = output / "manifest.json"

    save_json(result_path, result)

    report_path.write_text(
        report(result),
        encoding="utf-8",
    )

    save_json(
        manifest_path,
        {
            "version": VERSION,
            "created_utc": datetime.now(
                timezone.utc
            ).isoformat(),
            "sha256": {
                "result.json": digest(
                    result_path
                ),
                "report.md": digest(
                    report_path
                ),
            },
        },
    )

    for tf, periods in result["summary"].items():
        for period, item in periods.items():
            print(
                f"{tf}/{period}: "
                f"signal={item['signal']}, "
                f"baseline={item['baseline']}, "
                f"lift_pp="
                f"{item['lift_percentage_points']}",
                flush=True,
            )

    print("Execution: PASS")
    print("Research decision: HOLD")


if __name__ == "__main__":
    try:
        main()

    except Exception as exc:
        print(
            f"[FAIL] {type(exc).__name__}: "
            f"{exc}",
            file=sys.stderr,
        )
        sys.exit(1)
