
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Multi-Seed Baseline Stability Audit
Clean V001

- H4 and D1
- 30 deterministic random seeds by default
- Chronological signal deduplication
- Seven-day outcome window
- Gap-safe outcome validation
- Calendar-based temporal split
- Read-only source data
- No trading or model training

Standard library only.
"""

import argparse
import csv
import hashlib
import json
import math
import random
import statistics
import sys

from datetime import datetime, timezone
from pathlib import Path


VERSION = "Clean V001"

CONFIG = {
    "h4": (14400, 42),
    "d1": (86400, 7),
}

TIME_COLUMNS = (
    "candle_date_time_utc",
    "timestamp",
    "datetime",
    "time",
)

OPEN_COLUMNS = (
    "opening_price",
    "open",
)

HIGH_COLUMNS = (
    "high_price",
    "high",
)


def sha256(path):
    h = hashlib.sha256()

    with path.open("rb") as f:
        for block in iter(
            lambda: f.read(1048576),
            b"",
        ):
            h.update(block)

    return h.hexdigest()


def write_json(path, data):
    path.write_text(
        json.dumps(
            data,
            ensure_ascii=False,
            indent=2,
            allow_nan=False,
        ) + "\n",
        encoding="utf-8",
    )


def get_column(fields, candidates):
    for name in candidates:
        if name in fields:
            return name

    raise ValueError(
        f"Missing columns {candidates}: {fields}"
    )


def parse_time(value):
    s = str(value).strip()

    if s.endswith("Z"):
        s = s[:-1] + "+00:00"

    t = datetime.fromisoformat(s)

    if t.tzinfo is None:
        raise ValueError(
            "Timestamp lacks UTC offset"
        )

    return t.astimezone(timezone.utc)


def price(value):
    v = float(value)

    if not math.isfinite(v) or v <= 0:
        raise ValueError(
            f"Invalid price: {value}"
        )

    return v


def signal_flag(value):
    s = str(value or "").strip().lower()

    if s in ("1", "1.0", "true"):
        return True

    if s in ("0", "0.0", "false", ""):
        return False

    raise ValueError(
        f"Invalid signal_trigger: {value}"
    )


def load_pair(ohlcv_path, signal_path):
    times = []
    opens = []
    highs = []
    flags = []

    with ohlcv_path.open(
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

        pt = get_column(pc, TIME_COLUMNS)
        st = get_column(sc, TIME_COLUMNS)
        op = get_column(pc, OPEN_COLUMNS)
        hi = get_column(pc, HIGH_COLUMNS)

        if "signal_trigger" not in sc:
            raise ValueError(
                "Missing signal_trigger"
            )

        for n, row in enumerate(pr, 1):
            sig = next(sr, None)

            if sig is None:
                raise ValueError(
                    f"Short signal file at row {n}"
                )

            t = parse_time(row[pt])

            if t != parse_time(sig[st]):
                raise ValueError(
                    f"Timestamp mismatch at row {n}"
                )

            if times and t <= times[-1]:
                raise ValueError(
                    f"Non-increasing timestamp at {n}"
                )

            o = price(row[op])
            h = price(row[hi])

            if h < o * (1 - 1e-10):
                raise ValueError(
                    f"High below open at row {n}"
                )

            times.append(t)
            opens.append(o)
            highs.append(h)
            flags.append(
                signal_flag(sig["signal_trigger"])
            )

        if next(sr, None) is not None:
            raise ValueError(
                "Signal file has extra rows"
            )

    if not times:
        raise ValueError("Empty market")

    return times, opens, highs, flags


def gap_prefix(times, seconds):
    prefix = [0] * len(times)

    for i in range(1, len(times)):
        delta = (
            times[i] - times[i - 1]
        ).total_seconds()

        ratio = delta / seconds
        rounded = round(ratio)

        if (
            rounded < 1
            or abs(ratio - rounded) > 1e-8
        ):
            raise ValueError(
                f"Invalid candle spacing at {i}"
            )

        prefix[i] = (
            prefix[i - 1]
            + int(rounded != 1)
        )

    return prefix


def eligible_index(prefix, i, horizon, warmup):
    if i < warmup:
        return False

    if i + horizon >= len(prefix):
        return False

    return (
        prefix[i] == prefix[i - warmup]
        and
        prefix[i + horizon] == prefix[i]
    )


def event_hit(opens, highs, i, horizon):
    target = opens[i + 1] * 1.30

    return int(
        max(highs[i + 1:i + horizon + 1])
        >= target
    )


def deduplicate(indices, horizon):
    selected = []
    last = -horizon - 1

    for i in indices:
        if i > last + horizon:
            selected.append(i)
            last = i

    return selected


def random_nonoverlap(
    candidates,
    count,
    horizon,
    rng,
):
    shuffled = list(candidates)
    rng.shuffle(shuffled)

    selected = []
    occupied = set()

    for i in shuffled:
        if len(selected) >= count:
            break

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
    if times[i + horizon] < cutoff:
        return "train"

    if times[i] >= cutoff:
        return "test"

    return "purged"


def market_seed(base_seed, tf, market, period):
    material = (
        f"{base_seed}|{tf}|{market}|{period}"
    )

    return int(
        hashlib.sha256(
            material.encode("utf-8")
        ).hexdigest()[:16],
        16,
    )


def rate(hits, events):
    if not events:
        return None

    return round(
        hits * 100 / events,
        6,
    )


def percentile(values, fraction):
    ordered = sorted(values)

    if not ordered:
        return None

    pos = (len(ordered) - 1) * fraction
    lower = math.floor(pos)
    upper = math.ceil(pos)

    return (
        ordered[lower]
        + (ordered[upper] - ordered[lower])
        * (pos - lower)
    )


def build_boundaries(root, split):
    result = {}

    for tf in CONFIG:
        files = sorted(
            (root / "data/ohlcv" / tf).glob(
                "KRW-*.csv"
            )
        )

        if len(files) != 290:
            raise ValueError(
                f"{tf}: expected 290 OHLCV files, "
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

                tc = get_column(
                    reader.fieldnames or [],
                    TIME_COLUMNS,
                )

                for row in reader:
                    t = parse_time(row[tc])

                    if first is None or t < first:
                        first = t

                    if last is None or t > last:
                        last = t

        cutoff = first + (last - first) * split

        result[tf] = {
            "first_utc": first.isoformat(),
            "last_utc": last.isoformat(),
            "cutoff_utc": cutoff.isoformat(),
        }

    return result


def prepare_markets(root, boundaries, warmup):
    prepared = []

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
                f"{tf}: OHLCV/signal market mismatch"
            )

        if len(prices) != 290:
            raise ValueError(
                f"{tf}: expected 290 markets"
            )

        cutoff = parse_time(
            boundaries[tf]["cutoff_utc"]
        )

        for number, market in enumerate(
            sorted(prices),
            1,
        ):
            times, opens, highs, flags = load_pair(
                prices[market],
                signals[market],
            )

            prefix = gap_prefix(times, seconds)

            eligible = [
                i
                for i in range(len(times))
                if eligible_index(
                    prefix,
                    i,
                    horizon,
                    warmup,
                )
            ]

            outcome = {
                i: event_hit(
                    opens,
                    highs,
                    i,
                    horizon,
                )
                for i in eligible
            }

            for period in ("train", "test"):
                pool = [
                    i
                    for i in eligible
                    if period_of(
                        times,
                        i,
                        horizon,
                        cutoff,
                    ) == period
                ]

                signal_indices = [
                    i
                    for i in pool
                    if flags[i]
                ]

                selected = deduplicate(
                    signal_indices,
                    horizon,
                )

                prepared.append({
                    "tf": tf,
                    "market": market,
                    "period": period,
                    "horizon": horizon,
                    "pool": pool,
                    "selected": selected,
                    "outcome": outcome,
                })

            if number % 25 == 0:
                print(
                    f"[PREPARE] {tf}: "
                    f"{number}/{len(prices)}",
                    flush=True,
                )

    return prepared


def run_seed(prepared, seed):
    totals = {
        tf: {
            period: {
                "signal_events": 0,
                "signal_hits": 0,
                "baseline_events": 0,
                "baseline_hits": 0,
                "deduplicated": 0,
                "unmatched": 0,
            }
            for period in ("train", "test")
        }
        for tf in CONFIG
    }

    for item in prepared:
        tf = item["tf"]
        period = item["period"]
        selected = item["selected"]
        outcome = item["outcome"]

        rng = random.Random(
            market_seed(
                seed,
                tf,
                item["market"],
                period,
            )
        )

        baseline = random_nonoverlap(
            item["pool"],
            len(selected),
            item["horizon"],
            rng,
        )

        matched = min(
            len(selected),
            len(baseline),
        )

        # Keep identical sample sizes.
        signals = selected[:matched]
        baseline = baseline[:matched]

        total = totals[tf][period]

        total["deduplicated"] += len(selected)
        total["unmatched"] += (
            len(selected) - matched
        )

        total["signal_events"] += matched
        total["signal_hits"] += sum(
            outcome[i] for i in signals
        )

        total["baseline_events"] += matched
        total["baseline_hits"] += sum(
            outcome[i] for i in baseline
        )

    result = {}

    for tf in CONFIG:
        result[tf] = {}

        for period in ("train", "test"):
            item = totals[tf][period]

            n = item["signal_events"]
            bn = item["baseline_events"]

            if n != bn:
                raise ValueError(
                    "Signal/baseline size mismatch"
                )

            sr = rate(
                item["signal_hits"],
                n,
            )

            br = rate(
                item["baseline_hits"],
                bn,
            )

            lift = (
                round(sr - br, 6)
                if sr is not None and br is not None
                else None
            )

            result[tf][period] = {
                **item,
                "signal_hit_rate_pct": sr,
                "baseline_hit_rate_pct": br,
                "lift_pp": lift,
            }

    return result


def aggregate(runs):
    summary = {}

    for tf in CONFIG:
        summary[tf] = {}

        for period in ("train", "test"):
            items = [
                run["results"][tf][period]
                for run in runs
            ]

            baselines = [
                x["baseline_hit_rate_pct"]
                for x in items
            ]

            lifts = [
                x["lift_pp"]
                for x in items
            ]

            if any(
                x is None
                for x in baselines + lifts
            ):
                raise ValueError(
                    f"No valid samples: {tf}/{period}"
                )

            summary[tf][period] = {
                "seed_count": len(items),
                "signal_rate_first_seed_pct": (
                    items[0]["signal_hit_rate_pct"]
                ),
                "baseline_mean_pct": round(
                    statistics.mean(baselines),
                    6,
                ),
                "baseline_min_pct": min(baselines),
                "baseline_max_pct": max(baselines),
                "baseline_p025_pct": round(
                    percentile(baselines, 0.025),
                    6,
                ),
                "baseline_p975_pct": round(
                    percentile(baselines, 0.975),
                    6,
                ),
                "lift_mean_pp": round(
                    statistics.mean(lifts),
                    6,
                ),
                "lift_min_pp": min(lifts),
                "lift_max_pp": max(lifts),
                "positive_lift_seeds": sum(
                    x > 0 for x in lifts
                ),
                "negative_lift_seeds": sum(
                    x < 0 for x in lifts
                ),
                "zero_lift_seeds": sum(
                    x == 0 for x in lifts
                ),
                "signal_events_first_seed": (
                    items[0]["signal_events"]
                ),
                "unmatched_min": min(
                    x["unmatched"] for x in items
                ),
                "unmatched_max": max(
                    x["unmatched"] for x in items
                ),
            }

    return summary


def make_report(result):
    lines = [
        "# Pattern 256 Multi-Seed Baseline Audit",
        "",
        f"Version: {result['version']}",
        f"Execution: {result['execution']}",
        f"Research decision: {result['research_decision']}",
        f"Random seeds: {result['seed_count']}",
        "",
        "## Results",
        "",
        "| TF | Period | Signal % (first seed) | "
        "Baseline mean % | Mean lift pp | "
        "Positive seeds |",
        "|---|---|---:|---:|---:|---:|",
    ]

    for tf, periods in result["summary"].items():
        for period, s in periods.items():
            lines.append(
                f"| {tf} | {period} "
                f"| {s['signal_rate_first_seed_pct']:.4f} "
                f"| {s['baseline_mean_pct']:.4f} "
                f"| {s['lift_mean_pp']:.4f} "
                f"| {s['positive_lift_seeds']}"
                f"/{s['seed_count']} |"
            )

    lines.extend([
        "",
        "## Limitations",
        "",
        "- Baseline seeds are not independent "
        "market observations.",
        "- The same historical outcomes are reused.",
        "- This is not a statistical significance test.",
        "- Market regime and volatility are not matched.",
        "- Point-in-time universe is not proven.",
        "- No realized profitability calculation.",
        "- No independent OOS proof.",
        "- No training or trading approval.",
        "",
    ])

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--root",
        default=".",
    )

    parser.add_argument(
        "--seeds",
        type=int,
        default=30,
    )

    parser.add_argument(
        "--base-seed",
        type=int,
        default=2562026,
    )

    parser.add_argument(
        "--warmup",
        type=int,
        default=3,
    )

    parser.add_argument(
        "--split",
        type=float,
        default=0.70,
    )

    args = parser.parse_args()

    if not 1 <= args.seeds <= 100:
        raise ValueError(
            "Seeds must be between 1 and 100"
        )

    if not 1 <= args.warmup <= 100:
        raise ValueError("Invalid warmup")

    if not 0 < args.split < 1:
        raise ValueError("Invalid split")

    root = Path(args.root).resolve()

    boundaries = build_boundaries(
        root,
        args.split,
    )

    prepared = prepare_markets(
        root,
        boundaries,
        args.warmup,
    )

    runs = []

    for n in range(args.seeds):
        seed = args.base_seed + n

        results = run_seed(
            prepared,
            seed,
        )

        runs.append({
            "seed": seed,
            "results": results,
        })

        print(
            f"[SEED] {n + 1}/{args.seeds}: "
            f"{seed}",
            flush=True,
        )

    result = {
        "version": VERSION,
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "execution": "PASS",
        "classification": (
            "MULTISEED_BASELINE_STABILITY_DESCRIPTIVE"
        ),
        "seed_count": args.seeds,
        "base_seed": args.base_seed,
        "warmup_candles": args.warmup,
        "split_fraction": args.split,
        "threshold_pct": 30,
        "outcome_days": 7,
        "boundaries": boundaries,
        "summary": aggregate(runs),
        "runs": runs,
        "research_limitations": [
            "Repeated seeds do not create "
            "independent historical samples.",
            "Random baseline does not control "
            "for market regimes or volatility.",
            "Baseline sampling may not reach "
            "the requested sample count.",
            "Matched signal subsets may vary "
            "between random seeds.",
            "No independent OOS proof.",
            "No point-in-time universe proof.",
            "No realized profitability proof.",
        ],
        "independent_oos_proven": False,
        "profitability_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
    }

    folder = (
        root
        / "data/reports/"
        "pattern_256_multiseed_baseline"
    )

    folder.mkdir(
        parents=True,
        exist_ok=True,
    )

    result_path = folder / "result.json"
    report_path = folder / "report.md"
    manifest_path = folder / "manifest.json"

    write_json(result_path, result)

    report_path.write_text(
        make_report(result),
        encoding="utf-8",
    )

    write_json(
        manifest_path,
        {
            "version": VERSION,
            "created_utc": datetime.now(
                timezone.utc
            ).isoformat(),
            "sha256": {
                "result.json": sha256(
                    result_path
                ),
                "report.md": sha256(
                    report_path
                ),
            },
        },
    )

    print("\n=== SUMMARY ===")

    for tf, periods in result["summary"].items():
        for period, s in periods.items():
            print(
                f"{tf}/{period}: "
                f"baseline_mean="
                f"{s['baseline_mean_pct']}%, "
                f"lift_mean="
                f"{s['lift_mean_pp']}pp, "
                f"positive="
                f"{s['positive_lift_seeds']}"
                f"/{s['seed_count']}",
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
