
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 H4/D1 Gap-Safe +30% Outcome Audit
Clean V001

Research-only, read-only evaluation.

Entry:
    Next candle OPEN after a completed signal candle.

Outcome:
    Any HIGH >= entry OPEN * 1.30 within 7 days.

Strict rules:
    - No synthesized candles.
    - No gaps within entry/outcome window.
    - Three continuous preceding candles required.
    - Incomplete future windows are excluded.
    - Timestamp and OHLCV validity checked.
    - Signal files must align exactly.
    - No trading, training or profitability approval.
"""

import argparse
import csv
import hashlib
import json
import math
import sys

from bisect import bisect_left
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path


VERSION = "Clean V001"

TIMEFRAMES = {
    "h4": {
        "seconds": 14400,
        "horizon": 42,
    },
    "d1": {
        "seconds": 86400,
        "horizon": 7,
    },
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

LOW_COLUMNS = (
    "low_price",
    "low",
)

CLOSE_COLUMNS = (
    "trade_price",
    "close",
    "closing_price",
)

OUTPUT_RELATIVE = (
    "data/reports/"
    "pattern_256_h4_d1_gap_safe_outcome"
)


def sha256(path):
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        for chunk in iter(
            lambda: handle.read(1024 * 1024),
            b"",
        ):
            digest.update(chunk)

    return digest.hexdigest()


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


def choose_column(columns, candidates):
    for candidate in candidates:
        if candidate in columns:
            return candidate

    raise ValueError(
        "Required column missing. "
        f"Candidates={candidates}; "
        f"Available={columns}"
    )


def parse_utc(value):
    text = str(value or "").strip()

    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    if not text:
        raise ValueError("Empty UTC timestamp")

    result = datetime.fromisoformat(text)

    if result.tzinfo is None:
        raise ValueError(
            "Timezone missing in timestamp"
        )

    return result.astimezone(timezone.utc)


def parse_price(value, label):
    number = float(value)

    if not math.isfinite(number) or number <= 0:
        raise ValueError(
            f"Invalid {label}: {value}"
        )

    return number


def parse_trigger(value):
    text = str(value or "").strip().lower()

    if text in ("true", "1"):
        return True

    if text in ("false", "0", ""):
        return False

    try:
        number = float(text)

        if math.isfinite(number):
            if number == 1.0:
                return True

            if number == 0.0:
                return False

    except ValueError:
        pass

    raise ValueError(
        f"Invalid signal_trigger: {value}"
    )


def load_pair(ohlcv_path, signal_path):
    times = []
    opens = []
    highs = []
    lows = []
    closes = []
    triggers = []

    with ohlcv_path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as price_handle, signal_path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as signal_handle:

        price_reader = csv.DictReader(price_handle)
        signal_reader = csv.DictReader(signal_handle)

        price_fields = price_reader.fieldnames or []
        signal_fields = signal_reader.fieldnames or []

        pt = choose_column(
            price_fields,
            TIME_COLUMNS,
        )

        st = choose_column(
            signal_fields,
            TIME_COLUMNS,
        )

        op = choose_column(
            price_fields,
            OPEN_COLUMNS,
        )

        hi = choose_column(
            price_fields,
            HIGH_COLUMNS,
        )

        lo = choose_column(
            price_fields,
            LOW_COLUMNS,
        )

        cl = choose_column(
            price_fields,
            CLOSE_COLUMNS,
        )

        if "signal_trigger" not in signal_fields:
            raise ValueError(
                "Missing signal_trigger column"
            )

        for index, price in enumerate(
            price_reader,
            start=1,
        ):
            signal = next(signal_reader, None)

            if signal is None:
                raise ValueError(
                    "Signal file has fewer rows"
                )

            price_time = parse_utc(
                price[pt]
            )

            signal_time = parse_utc(
                signal[st]
            )

            if price_time != signal_time:
                raise ValueError(
                    f"Timestamp mismatch at {index}"
                )

            opening = parse_price(
                price[op],
                "open",
            )

            high = parse_price(
                price[hi],
                "high",
            )

            low = parse_price(
                price[lo],
                "low",
            )

            close = parse_price(
                price[cl],
                "close",
            )

            tolerance = max(
                abs(high),
                abs(low),
                1.0,
            ) * 1e-10

            if high + tolerance < max(
                opening,
                close,
                low,
            ):
                raise ValueError(
                    f"Invalid high at row {index}"
                )

            if low - tolerance > min(
                opening,
                close,
                high,
            ):
                raise ValueError(
                    f"Invalid low at row {index}"
                )

            if times and price_time <= times[-1]:
                raise ValueError(
                    f"Non-increasing time at {index}"
                )

            times.append(price_time)
            opens.append(opening)
            highs.append(high)
            lows.append(low)
            closes.append(close)

            triggers.append(
                parse_trigger(
                    signal["signal_trigger"]
                )
            )

        if next(signal_reader, None) is not None:
            raise ValueError(
                "Signal file has extra rows"
            )

    if not times:
        raise ValueError("Empty market data")

    return {
        "times": times,
        "opens": opens,
        "highs": highs,
        "lows": lows,
        "closes": closes,
        "triggers": triggers,
    }


def calculate_breaks(times, expected_seconds):
    """
    break_prefix[i] is the number of
    discontinuities in transitions ending
    at or before index i.

    A window [start, end] is continuous iff:
    break_prefix[end] == break_prefix[start].
    """

    prefix = [0] * len(times)
    missing_slots = 0
    gap_events = 0

    for i in range(1, len(times)):
        elapsed = (
            times[i] - times[i - 1]
        ).total_seconds()

        ratio = elapsed / expected_seconds
        rounded = round(ratio)

        if (
            rounded < 1
            or abs(ratio - rounded) > 1e-8
        ):
            raise ValueError(
                "Invalid candle interval "
                f"at row {i + 1}: {elapsed}"
            )

        broken = rounded != 1

        prefix[i] = (
            prefix[i - 1]
            + int(broken)
        )

        if broken:
            gap_events += 1
            missing_slots += rounded - 1

    return prefix, gap_events, missing_slots


def continuous(prefix, start, end):
    return (
        prefix[end] == prefix[start]
    )


def evaluate_market(
    timeframe,
    market,
    price_path,
    signal_path,
    threshold,
    warmup,
):
    config = TIMEFRAMES[timeframe]

    seconds = config["seconds"]
    horizon = config["horizon"]

    data = load_pair(
        price_path,
        signal_path,
    )

    times = data["times"]
    opens = data["opens"]
    highs = data["highs"]
    triggers = data["triggers"]

    prefix, gaps, missing = calculate_breaks(
        times,
        seconds,
    )

    counts = Counter()
    examples = []
    n = len(times)

    for i, triggered in enumerate(triggers):
        if not triggered:
            continue

        counts["total_signals"] += 1

        # Signal candle must have sufficient
        # preceding continuous history.
        if i < warmup:
            counts["excluded_warmup"] += 1
            continue

        if not continuous(
            prefix,
            i - warmup,
            i,
        ):
            counts["excluded_history_gap"] += 1
            continue

        entry_index = i + 1
        final_index = i + horizon

        if final_index >= n:
            counts["excluded_future_incomplete"] += 1
            continue

        # All candles from the signal to
        # the final outcome candle must
        # exist at the expected intervals.
        if not continuous(
            prefix,
            i,
            final_index,
        ):
            counts["excluded_future_gap"] += 1
            continue

        entry_price = opens[entry_index]
        target_price = entry_price * (
            1.0 + threshold
        )

        maximum_high = max(
            highs[
                entry_index:final_index + 1
            ]
        )

        reached = (
            maximum_high >= target_price
        )

        counts["eligible"] += 1

        if reached:
            counts["hit"] += 1
        else:
            counts["miss"] += 1

        # Descriptive examples only.
        # No individual trade orders.
        if len(examples) < 5:
            examples.append({
                "signal_utc": (
                    times[i].isoformat()
                ),
                "entry_utc": (
                    times[entry_index].isoformat()
                ),
                "entry_open": entry_price,
                "max_future_high": maximum_high,
                "max_high_return_pct": (
                    (
                        maximum_high / entry_price
                    ) - 1.0
                ) * 100.0,
                "hit": reached,
            })

    total = counts["total_signals"]
    eligible = counts["eligible"]

    excluded = (
        counts["excluded_warmup"]
        + counts["excluded_history_gap"]
        + counts["excluded_future_incomplete"]
        + counts["excluded_future_gap"]
    )

    if total != eligible + excluded:
        raise ValueError(
            "Signal classification count mismatch"
        )

    if eligible != (
        counts["hit"] + counts["miss"]
    ):
        raise ValueError(
            "Outcome classification mismatch"
        )

    return {
        "timeframe": timeframe,
        "market": market,
        "rows": n,
        "gap_events": gaps,
        "missing_slots": missing,
        "total_signals": total,
        "eligible": eligible,
        "hit": counts["hit"],
        "miss": counts["miss"],
        "excluded_warmup": (
            counts["excluded_warmup"]
        ),
        "excluded_history_gap": (
            counts["excluded_history_gap"]
        ),
        "excluded_future_incomplete": (
            counts["excluded_future_incomplete"]
        ),
        "excluded_future_gap": (
            counts["excluded_future_gap"]
        ),
        "excluded_total": excluded,
        "hit_rate_pct": (
            round(
                counts["hit"] / eligible * 100,
                6,
            )
            if eligible
            else None
        ),
        "examples": examples,
    }


def audit(root, threshold, warmup):
    records = []
    summaries = {}

    for timeframe in TIMEFRAMES:
        price_dir = (
            root / "data" / "ohlcv" / timeframe
        )

        signal_dir = (
            root / "data" / "signals"
            / "256" / timeframe
        )

        if not price_dir.is_dir():
            raise FileNotFoundError(
                str(price_dir)
            )

        if not signal_dir.is_dir():
            raise FileNotFoundError(
                str(signal_dir)
            )

        prices = {
            p.stem: p
            for p in price_dir.glob("KRW-*.csv")
        }

        signals = {
            p.stem: p
            for p in signal_dir.glob("KRW-*.csv")
        }

        if not prices:
            raise ValueError(
                f"No OHLCV files: {timeframe}"
            )

        if set(prices) != set(signals):
            raise ValueError(
                f"Market file mismatch: {timeframe}"
            )

        markets = sorted(prices)

        print(
            f"[{timeframe}] "
            f"markets={len(markets)}",
            flush=True,
        )

        for number, market in enumerate(
            markets,
            start=1,
        ):
            record = evaluate_market(
                timeframe,
                market,
                prices[market],
                signals[market],
                threshold,
                warmup,
            )

            records.append(record)

            if number % 25 == 0:
                print(
                    f"[{timeframe}] "
                    f"{number}/{len(markets)}",
                    flush=True,
                )

        subset = [
            record
            for record in records
            if record["timeframe"] == timeframe
        ]

        keys = (
            "rows",
            "gap_events",
            "missing_slots",
            "total_signals",
            "eligible",
            "hit",
            "miss",
            "excluded_warmup",
            "excluded_history_gap",
            "excluded_future_incomplete",
            "excluded_future_gap",
            "excluded_total",
        )

        summary = {
            key: sum(
                record[key]
                for record in subset
            )
            for key in keys
        }

        summary["markets"] = len(subset)

        summary["hit_rate_pct"] = (
            round(
                summary["hit"]
                / summary["eligible"]
                * 100,
                6,
            )
            if summary["eligible"]
            else None
        )

        summaries[timeframe] = summary

    return {
        "version": VERSION,
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "execution": "PASS",
        "classification": (
            "GAP_SAFE_OUTCOME_DESCRIPTIVE"
        ),
        "threshold_pct": threshold * 100.0,
        "outcome_days": 7,
        "history_warmup_candles": warmup,
        "entry_policy": (
            "Next candle opening price"
        ),
        "outcome_policy": (
            "Maximum future candle high "
            "reaches threshold"
        ),
        "gap_policy": (
            "Exclude any signal with a gap "
            "in history warmup or future "
            "outcome window"
        ),
        "timeframes": summaries,
        "records": records,
        "limitations": [
            "Historical signals may overlap.",
            "Results are not independent trades.",
            "Next-open execution is hypothetical.",
            "High-touch is not realized profit.",
            "Intrabar execution is not modeled.",
            "Fees and slippage are not modeled.",
            "Delisting and market-universe "
            "point-in-time validity not proven.",
            "No independent OOS validation.",
            "No causal proof of gap origins.",
            "No live trading approval.",
        ],
        "independent_oos_proven": False,
        "profitability_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
    }


def make_report(result):
    lines = [
        "# Pattern 256 H4/D1 Gap-Safe Outcome Audit",
        "",
        f"Version: {result['version']}",
        f"Execution: {result['execution']}",
        (
            "Classification: "
            + result["classification"]
        ),
        "",
        "## Evaluation",
        "",
        "- Threshold: +30%",
        "- Horizon: 7 calendar days",
        "- Entry: next candle open",
        "- Missing candles: never synthesized",
        "- Incomplete future: excluded",
        "",
        "## Results",
        "",
        (
            "| TF | Markets | Signals | Eligible | "
            "Hits | Misses | Hit rate | Excluded |"
        ),
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]

    for tf, s in result["timeframes"].items():
        rate = (
            f"{s['hit_rate_pct']:.4f}%"
            if s["hit_rate_pct"] is not None
            else "N/A"
        )

        lines.append(
            f"| {tf} "
            f"| {s['markets']} "
            f"| {s['total_signals']} "
            f"| {s['eligible']} "
            f"| {s['hit']} "
            f"| {s['miss']} "
            f"| {rate} "
            f"| {s['excluded_total']} |"
        )

    lines.extend([
        "",
        "## Exclusion breakdown",
        "",
        (
            "| TF | Warmup | History gap | "
            "Incomplete future | Future gap |"
        ),
        "|---|---:|---:|---:|---:|",
    ])

    for tf, s in result["timeframes"].items():
        lines.append(
            f"| {tf} "
            f"| {s['excluded_warmup']} "
            f"| {s['excluded_history_gap']} "
            f"| {s['excluded_future_incomplete']} "
            f"| {s['excluded_future_gap']} |"
        )

    lines.extend([
        "",
        "## Research limits",
        "",
        "- Decision: HOLD",
        "- Independent OOS: NOT PROVEN",
        "- Profitability: NOT PROVEN",
        "- Training: NOT APPROVED",
        "- Live trading: NOT APPROVED",
        "",
        (
            "Hit rate is the fraction of eligible "
            "historical signal events whose future "
            "high reaches the target. It is not "
            "a trading win rate."
        ),
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
        "--threshold",
        type=float,
        default=0.30,
    )

    parser.add_argument(
        "--warmup",
        type=int,
        default=3,
    )

    args = parser.parse_args()

    if not 0 < args.threshold <= 10:
        raise ValueError(
            "Invalid threshold"
        )

    if not 1 <= args.warmup <= 100:
        raise ValueError(
            "Invalid warmup"
        )

    root = Path(args.root).resolve()

    if not root.is_dir():
        raise FileNotFoundError(str(root))

    result = audit(
        root,
        args.threshold,
        args.warmup,
    )

    output = root / OUTPUT_RELATIVE

    output.mkdir(
        parents=True,
        exist_ok=True,
    )

    result_path = (
        output / "gap_safe_outcome_result.json"
    )

    report_path = (
        output / "gap_safe_outcome_report.md"
    )

    manifest_path = (
        output / "gap_safe_outcome_manifest.json"
    )

    write_json(
        result_path,
        result,
    )

    report_path.write_text(
        make_report(result),
        encoding="utf-8",
    )

    manifest = {
        "version": VERSION,
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "output_sha256": {
            result_path.name: sha256(
                result_path
            ),
            report_path.name: sha256(
                report_path
            ),
        },
    }

    write_json(
        manifest_path,
        manifest,
    )

    print(
        "======================================"
    )
    print(
        "Pattern 256 H4/D1 Gap-Safe Outcome"
    )
    print(
        "======================================"
    )

    for tf, summary in result[
        "timeframes"
    ].items():
        print(
            f"{tf}: "
            f"signals={summary['total_signals']}, "
            f"eligible={summary['eligible']}, "
            f"hits={summary['hit']}, "
            f"rate={summary['hit_rate_pct']}, "
            f"excluded={summary['excluded_total']}"
        )

    print("Execution: PASS")
    print("Research decision: HOLD")


if __name__ == "__main__":
    try:
        main()

    except Exception as exc:
        print(
            f"[FAIL] "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
        )

        sys.exit(1)
