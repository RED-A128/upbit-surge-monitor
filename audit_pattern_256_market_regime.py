
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Market Regime Audit
Clean V001

Research only. No trading.

Regime: trailing 30-candle return
  UP   >= +10%
  DOWN <= -10%
  SIDE otherwise

Volatility: trailing 30 log-return RMS
  LOW / HIGH split by market-period median

Baseline matching:
  same market, period, regime, volatility bucket
  exclude signal candles
  30 deterministic random seeds
  unmatched signals explicitly reported

Outcome:
  next candle open to maximum high over 7 days
  target >= +30%

The original signal rate is never truncated
to fit the random baseline.
"""

import argparse
import csv
import hashlib
import json
import math
import random
import statistics
import sys

from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


VERSION = "Clean V001"
CONFIG = {
    "h4": (14400, 42),
    "d1": (86400, 7),
}
REGIMES = ("UP", "SIDE", "DOWN")
VOLS = ("LOW", "HIGH")
PERIODS = ("train", "test")

TIME_NAMES = (
    "candle_date_time_utc",
    "timestamp",
    "datetime",
    "time",
)
OPEN_NAMES = ("opening_price", "open")
HIGH_NAMES = ("high_price", "high")
CLOSE_NAMES = ("trade_price", "close", "closing_price")


def digest(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1048576), b""):
            h.update(block)
    return h.hexdigest()


def save_json(path, obj):
    path.write_text(
        json.dumps(
            obj, ensure_ascii=False,
            indent=2, allow_nan=False,
        ) + "\n",
        encoding="utf-8",
    )


def column(names, choices):
    for name in choices:
        if name in names:
            return name
    raise ValueError(f"Missing column {choices}: {names}")


def utc(value):
    s = str(value).strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    t = datetime.fromisoformat(s)
    if t.tzinfo is None:
        raise ValueError(f"Timezone missing: {s}")
    return t.astimezone(timezone.utc)


def positive(value):
    x = float(value)
    if not math.isfinite(x) or x <= 0:
        raise ValueError(f"Invalid price: {value}")
    return x


def flag(value):
    s = str(value or "").strip().lower()
    if s in ("1", "1.0", "true"):
        return True
    if s in ("0", "0.0", "false", ""):
        return False
    raise ValueError(f"Invalid signal: {value}")


def read_market(price_path, signal_path):
    times, opens, highs, closes, signals = [], [], [], [], []

    with price_path.open(
        encoding="utf-8-sig", newline=""
    ) as pf, signal_path.open(
        encoding="utf-8-sig", newline=""
    ) as sf:
        pr, sr = csv.DictReader(pf), csv.DictReader(sf)
        pc, sc = pr.fieldnames or [], sr.fieldnames or []

        pt = column(pc, TIME_NAMES)
        st = column(sc, TIME_NAMES)
        op = column(pc, OPEN_NAMES)
        hi = column(pc, HIGH_NAMES)
        cl = column(pc, CLOSE_NAMES)

        if "signal_trigger" not in sc:
            raise ValueError("Missing signal_trigger")

        for n, row in enumerate(pr, 1):
            sig = next(sr, None)
            if sig is None:
                raise ValueError(f"Short signal file: {n}")

            t = utc(row[pt])
            if t != utc(sig[st]):
                raise ValueError(f"Timestamp mismatch: {n}")
            if times and t <= times[-1]:
                raise ValueError(f"Unsorted timestamp: {n}")

            o = positive(row[op])
            h = positive(row[hi])
            c = positive(row[cl])

            if h + 1e-10 < max(o, c):
                raise ValueError(f"Invalid OHLC high: {n}")

            times.append(t)
            opens.append(o)
            highs.append(h)
            closes.append(c)
            signals.append(flag(sig["signal_trigger"]))

        if next(sr, None) is not None:
            raise ValueError("Extra signal rows")

    if not times:
        raise ValueError("Empty CSV")

    return times, opens, highs, closes, signals


def gaps(times, seconds):
    prefix = [0] * len(times)

    for i in range(1, len(times)):
        ratio = (
            times[i] - times[i - 1]
        ).total_seconds() / seconds

        nearest = round(ratio)
        if nearest < 1 or abs(ratio - nearest) > 1e-8:
            raise ValueError(f"Invalid spacing: {i}")

        prefix[i] = (
            prefix[i - 1] + int(nearest != 1)
        )

    return prefix


def dedup(indices, horizon):
    chosen = []
    last = -horizon - 1

    for i in indices:
        if i > last + horizon:
            chosen.append(i)
            last = i

    return chosen


def period_at(times, i, horizon, cutoff):
    if times[i + horizon] < cutoff:
        return "train"
    if times[i] >= cutoff:
        return "test"
    return None


def compute_cutoffs(root, fraction):
    output = {}

    for tf in CONFIG:
        files = sorted(
            (root / "data/ohlcv" / tf).glob("KRW-*.csv")
        )

        if len(files) != 290:
            raise ValueError(
                f"{tf}: expected 290 markets, got {len(files)}"
            )

        first, last = None, None

        for path in files:
            with path.open(
                encoding="utf-8-sig", newline=""
            ) as f:
                reader = csv.DictReader(f)
                tc = column(reader.fieldnames or [], TIME_NAMES)

                for row in reader:
                    t = utc(row[tc])
                    if first is None or t < first:
                        first = t
                    if last is None or t > last:
                        last = t

        cutoff = first + (last - first) * fraction
        output[tf] = {
            "first": first.isoformat(),
            "last": last.isoformat(),
            "cutoff": cutoff.isoformat(),
        }

    return output


def market_data(root, tf, market, cutoff, lookback):
    seconds, horizon = CONFIG[tf]

    pp = root / "data/ohlcv" / tf / (market + ".csv")
    sp = root / "data/signals/256" / tf / (market + ".csv")

    times, opens, highs, closes, flags = read_market(pp, sp)
    prefix = gaps(times, seconds)

    # Rolling return and volatility using past/current closes.
    logret = [0.0] * len(times)
    sumsq = [0.0] * len(times)

    for i in range(1, len(times)):
        logret[i] = math.log(closes[i] / closes[i - 1])
        sumsq[i] = sumsq[i - 1] + logret[i] ** 2

    records = []

    for i in range(lookback, len(times) - horizon):
        if prefix[i] != prefix[i - lookback]:
            continue
        if prefix[i + horizon] != prefix[i]:
            continue

        period = period_at(times, i, horizon, cutoff)
        if period is None:
            continue

        trailing = closes[i] / closes[i - lookback] - 1

        if trailing >= 0.10:
            regime = "UP"
        elif trailing <= -0.10:
            regime = "DOWN"
        else:
            regime = "SIDE"

        vol = math.sqrt(
            max(0.0, (sumsq[i] - sumsq[i - lookback]))
            / lookback
        )

        target = opens[i + 1] * 1.30
        hit = int(
            max(highs[i + 1:i + horizon + 1]) >= target
        )

        records.append({
            "index": i,
            "period": period,
            "regime": regime,
            "vol": vol,
            "hit": hit,
            "signal": flags[i],
        })

    # Volatility median is computed separately for each
    # market and period. This is descriptive hindsight
    # stratification, not a live point-in-time feature.
    medians = {}

    for period in PERIODS:
        values = [
            r["vol"] for r in records
            if r["period"] == period
        ]
        medians[period] = (
            statistics.median(values) if values else None
        )

    for r in records:
        median = medians[r["period"]]
        r["bucket"] = (
            "HIGH" if r["vol"] >= median else "LOW"
        )

    return records, horizon


def seed_for(seed, *parts):
    s = "|".join(map(str, (seed,) + parts))
    return int(
        hashlib.sha256(s.encode()).hexdigest()[:16],
        16,
    )


def choose_baseline(pool, count, horizon, rng):
    shuffled = list(pool)
    rng.shuffle(shuffled)

    selected = []
    occupied = set()

    for item in shuffled:
        if len(selected) >= count:
            break

        i = item["index"]
        if i in occupied:
            continue

        selected.append(item)

        for j in range(i - horizon, i + horizon + 1):
            occupied.add(j)

    return selected


def empty_stats():
    return {
        "signal_events": 0,
        "signal_hits": 0,
        "baseline_events": 0,
        "baseline_hits": 0,
        "unmatched": 0,
        "strata_without_baseline": 0,
    }


def add_stats(target, source):
    for k in target:
        target[k] += source[k]


def analyze_market(records, horizon, tf, market, seed):
    output = defaultdict(empty_stats)

    for period in PERIODS:
        period_records = [
            r for r in records if r["period"] == period
        ]

        signals = dedup(
            [
                r["index"] for r in period_records
                if r["signal"]
            ],
            horizon,
        )
        selected_set = set(signals)

        strata = defaultdict(lambda: {
            "signals": [],
            "pool": [],
        })

        for r in period_records:
            key = (r["regime"], r["bucket"])

            if r["index"] in selected_set:
                strata[key]["signals"].append(r)

            # Exclude all signal-triggered candles from
            # the random control population.
            if not r["signal"]:
                strata[key]["pool"].append(r)

        for (regime, vol), item in strata.items():
            key = (tf, period, regime, vol)
            stats = output[key]

            signals_here = item["signals"]
            n = len(signals_here)

            stats["signal_events"] += n
            stats["signal_hits"] += sum(
                x["hit"] for x in signals_here
            )

            if n == 0:
                continue

            rng = random.Random(
                seed_for(
                    seed, tf, market,
                    period, regime, vol,
                )
            )

            baseline = choose_baseline(
                item["pool"], n, horizon, rng
            )

            stats["baseline_events"] += len(baseline)
            stats["baseline_hits"] += sum(
                x["hit"] for x in baseline
            )
            stats["unmatched"] += n - len(baseline)

            if not baseline:
                stats["strata_without_baseline"] += 1

    return output


def pct(hits, count):
    return round(100 * hits / count, 6) if count else None


def aggregate_runs(runs):
    result = {}

    for tf in CONFIG:
        result[tf] = {}

        for period in PERIODS:
            result[tf][period] = {}

            for regime in REGIMES:
                result[tf][period][regime] = {}

                for vol in VOLS:
                    key = f"{tf}|{period}|{regime}|{vol}"
                    values = [r[key] for r in runs]

                    first = values[0]
                    signal_count = first["signal_events"]
                    signal_hits = first["signal_hits"]

                    if any(
                        x["signal_events"] != signal_count
                        or x["signal_hits"] != signal_hits
                        for x in values
                    ):
                        raise ValueError(
                            f"Signal totals changed: {key}"
                        )

                    sr = pct(signal_hits, signal_count)

                    baseline_rates = [
                        pct(
                            x["baseline_hits"],
                            x["baseline_events"],
                        )
                        for x in values
                    ]
                    valid = [
                        x for x in baseline_rates
                        if x is not None
                    ]

                    mean = (
                        round(statistics.mean(valid), 6)
                        if valid else None
                    )

                    lift = (
                        round(sr - mean, 6)
                        if sr is not None and mean is not None
                        else None
                    )

                    result[tf][period][regime][vol] = {
                        "signal_events": signal_count,
                        "signal_hits": signal_hits,
                        "signal_rate_pct": sr,
                        "baseline_mean_pct": mean,
                        "mean_lift_pp": lift,
                        "positive_seed_count": (
                            sum(sr > x for x in valid)
                            if sr is not None else 0
                        ),
                        "valid_baseline_seeds": len(valid),
                        "baseline_events_min": min(
                            (x["baseline_events"] for x in values),
                            default=0,
                        ),
                        "baseline_events_max": max(
                            (x["baseline_events"] for x in values),
                            default=0,
                        ),
                        "unmatched_min": min(
                            x["unmatched"] for x in values
                        ),
                        "unmatched_max": max(
                            x["unmatched"] for x in values
                        ),
                    }

    return result


def report_text(result):
    lines = [
        "# Pattern 256 Market Regime Audit",
        "",
        f"Version: {VERSION}",
        "Execution: PASS",
        "Research decision: HOLD",
        "",
        "| TF | Period | Regime | Vol | Events | "
        "Signal % | Baseline % | Lift pp | Positive seeds |",
        "|---|---|---|---|---:|---:|---:|---:|---:|",
    ]

    for tf, periods in result["summary"].items():
        for period, regimes in periods.items():
            for regime, buckets in regimes.items():
                for vol, item in buckets.items():
                    def fmt(x):
                        return (
                            f"{x:.4f}" if x is not None
                            else "N/A"
                        )

                    lines.append(
                        f"| {tf} | {period} | {regime} | {vol} "
                        f"| {item['signal_events']} "
                        f"| {fmt(item['signal_rate_pct'])} "
                        f"| {fmt(item['baseline_mean_pct'])} "
                        f"| {fmt(item['mean_lift_pp'])} "
                        f"| {item['positive_seed_count']}"
                        f"/{item['valid_baseline_seeds']} |"
                    )

    lines.extend([
        "",
        "## Methodological limitations",
        "",
        "- Market regimes use trailing 30 candles.",
        "- Volatility buckets use market-period medians,",
        "  which include future observations relative to",
        "  individual signal dates.",
        "- Baseline is sampled within matched strata.",
        "- Baseline may have fewer observations.",
        "- Original signal rates are not truncated.",
        "- Results are descriptive, not causal.",
        "- The baseline is not a trading strategy.",
        "- No independent OOS or profit proof.",
        "- No training or live trading approval.",
        "",
    ])

    return "\n".join(lines)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--root", default=".")
    p.add_argument("--seeds", type=int, default=30)
    p.add_argument("--base-seed", type=int, default=2562026)
    p.add_argument("--split", type=float, default=0.70)
    p.add_argument("--lookback", type=int, default=30)
    args = p.parse_args()

    if not 1 <= args.seeds <= 100:
        raise ValueError("Invalid seeds")
    if not 10 <= args.lookback <= 365:
        raise ValueError("Invalid lookback")
    if not 0 < args.split < 1:
        raise ValueError("Invalid split")

    root = Path(args.root).resolve()
    cutoffs = compute_cutoffs(root, args.split)

    all_runs = [
        defaultdict(empty_stats)
        for _ in range(args.seeds)
    ]

    counts = {}

    for tf in CONFIG:
        prices = {
            x.stem: x
            for x in (
                root / "data/ohlcv" / tf
            ).glob("KRW-*.csv")
        }

        signals = {
            x.stem: x
            for x in (
                root / "data/signals/256" / tf
            ).glob("KRW-*.csv")
        }

        if len(prices) != 290 or set(prices) != set(signals):
            raise ValueError(
                f"{tf}: OHLCV and signal sets mismatch"
            )

        cutoff = utc(cutoffs[tf]["cutoff"])
        counts[tf] = len(prices)

        for number, market in enumerate(sorted(prices), 1):
            records, horizon = market_data(
                root, tf, market, cutoff, args.lookback
            )

            for k in range(args.seeds):
                stats = analyze_market(
                    records,
                    horizon,
                    tf,
                    market,
                    args.base_seed + k,
                )

                for key, value in stats.items():
                    add_stats(all_runs[k][key], value)

            if number % 25 == 0:
                print(
                    f"[PROGRESS] {tf} {number}/290",
                    flush=True,
                )

    keys = [
        f"{tf}|{period}|{regime}|{vol}"
        for tf in CONFIG
        for period in PERIODS
        for regime in REGIMES
        for vol in VOLS
    ]

    runs = []

    for k, data in enumerate(all_runs):
        runs.append({
            key: dict(data[key])
            for key in keys
        })

    summary = aggregate_runs(runs)

    output = {
        "version": VERSION,
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "execution": "PASS",
        "classification": "MARKET_REGIME_DESCRIPTIVE",
        "market_counts": counts,
        "seeds": args.seeds,
        "base_seed": args.base_seed,
        "split": args.split,
        "lookback": args.lookback,
        "target_pct": 30,
        "outcome_days": 7,
        "cutoffs": cutoffs,
        "summary": summary,
        "runs": runs,
        "independent_oos_proven": False,
        "profitability_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
    }

    folder = (
        root
        / "data/reports/pattern_256_market_regime"
    )
    folder.mkdir(parents=True, exist_ok=True)

    rp = folder / "result.json"
    md = folder / "report.md"
    mf = folder / "manifest.json"

    save_json(rp, output)
    md.write_text(report_text(output), encoding="utf-8")

    save_json(mf, {
        "version": VERSION,
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "sha256": {
            "result.json": digest(rp),
            "report.md": digest(md),
        },
    })

    print("\n[PASS] Market regime audit")
    print("Research decision: HOLD")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(
            f"[FAIL] {type(e).__name__}: {e}",
            file=sys.stderr,
        )
        sys.exit(1)
