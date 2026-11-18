
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 H1 Same-Time Matched Controls
Clean V001

Research only. No trading or training.

- Prior event list is immutable.
- Verify prior report manifest and input fingerprints.
- Match at the exact UTC signal timestamp.
- Different KRW market, signal_trigger == 0.
- Same next-candle-open entry and 168-hour horizon.
- Require uninterrupted hourly candles.
- One control per event, without overlapping control
  windows within the same market.
- Deterministic candidate rotation, no outcome-based
  choice among eligible controls.
- Unmatched events are excluded from paired rates.
- No causal, OOS, or profitability claims.
"""

import argparse
import csv
import hashlib
import io
import json
import math
import os
import tempfile
from array import array
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

VERSION = "Clean V001"
HORIZON = 168
HOUR_SECONDS = 3600
TARGET_MULTIPLIER = 1.30

EVENT_FIELDS = [
    "market",
    "signal_time_utc",
    "entry_time_utc",
    "entry_open",
    "target_price",
    "hit_30pct",
    "first_hit_hour",
    "max_high_return_pct",
    "max_adverse_low_pct",
    "end_close_return_pct",
    "horizon_hours",
]

PAIR_FIELDS = [
    "event_id",
    "signal_time_utc",
    "signal_market",
    "control_market",
    "signal_hit_30pct",
    "control_hit_30pct",
    "signal_entry_open",
    "control_entry_open",
    "control_target_price",
    "control_first_hit_hour",
    "control_max_high_return_pct",
    "control_end_close_return_pct",
]

REQUIRED_COLUMNS = {
    "market",
    "candle_date_time_utc",
    "open",
    "high",
    "low",
    "close",
}


def digest(path):
    h = hashlib.sha256()
    with open(path, "rb") as stream:
        for block in iter(
            lambda: stream.read(1024 * 1024), b""
        ):
            h.update(block)
    return h.hexdigest()


def atomic_text(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(
        prefix=".matched_",
        dir=str(path.parent),
    )
    try:
        with os.fdopen(
            fd, "w", encoding="utf-8", newline=""
        ) as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def write_json(path, data):
    atomic_text(
        path,
        json.dumps(
            data,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            allow_nan=False,
        ) + "\n",
    )


def csv_text(rows, fields):
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(
        stream,
        fieldnames=fields,
        lineterminator="\n",
        extrasaction="raise",
    )
    writer.writeheader()
    writer.writerows(rows)
    return stream.getvalue()


def utc_second(value):
    dt = datetime.fromisoformat(
        value.strip().replace("Z", "+00:00")
    )
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def utc_iso(seconds):
    return datetime.fromtimestamp(
        seconds, tz=timezone.utc
    ).isoformat()


def read_json(path):
    with open(path, "r", encoding="utf-8") as stream:
        return json.load(stream)


def check_prior_manifest(folder):
    manifest = read_json(
        folder / "event_outcomes_manifest.json"
    )
    if manifest.get("version") != "Clean V001":
        raise ValueError("Prior manifest version mismatch")

    expected_names = {
        "event_outcomes_result.json",
        "event_outcomes_events.csv",
        "event_outcomes_report.md",
    }
    entries = manifest.get("files", {})

    if set(entries) != expected_names:
        raise ValueError("Prior manifest inventory mismatch")

    for name, expected in entries.items():
        path = folder / name
        if not path.is_file():
            raise FileNotFoundError(path)
        if digest(path) != expected:
            raise ValueError(
                f"Prior report SHA256 mismatch: {name}"
            )


def load_events(path):
    events = []
    with open(
        path, "r", encoding="utf-8-sig", newline=""
    ) as stream:
        reader = csv.DictReader(stream)
        if reader.fieldnames != EVENT_FIELDS:
            raise ValueError("Unexpected event CSV schema")

        for number, row in enumerate(reader, start=1):
            market = row["market"]
            signal_second = utc_second(
                row["signal_time_utc"]
            )
            entry_second = utc_second(
                row["entry_time_utc"]
            )
            if entry_second - signal_second != HOUR_SECONDS:
                raise ValueError(
                    f"Event entry-time mismatch: {number}"
                )
            if int(row["horizon_hours"]) != HORIZON:
                raise ValueError(
                    f"Event horizon mismatch: {number}"
                )
            if row["hit_30pct"] not in ("0", "1"):
                raise ValueError(
                    f"Event hit flag invalid: {number}"
                )

            entry = float(row["entry_open"])
            target = float(row["target_price"])
            if (
                not math.isfinite(entry)
                or not math.isfinite(target)
                or entry <= 0
                or not math.isclose(
                    target,
                    entry * TARGET_MULTIPLIER,
                    rel_tol=1e-9,
                    abs_tol=1e-9,
                )
            ):
                raise ValueError(
                    f"Event entry/target invalid: {number}"
                )

            events.append({
                "event_id": number,
                "market": market,
                "second": signal_second,
                "entry": entry,
                "hit": int(row["hit_30pct"]),
            })

    events.sort(
        key=lambda x: (
            x["second"],
            x["market"],
            x["event_id"],
        )
    )
    return events


def load_market(ohlcv_path, signal_path, market):
    times = array("q")
    opens = array("d")
    highs = array("d")
    closes = array("d")
    flags = array("b")

    with open(
        ohlcv_path, "r",
        encoding="utf-8-sig", newline=""
    ) as left, open(
        signal_path, "r",
        encoding="utf-8-sig", newline=""
    ) as right:

        candles = csv.DictReader(left)
        signals = csv.DictReader(right)

        if not REQUIRED_COLUMNS.issubset(
            set(candles.fieldnames or [])
        ):
            raise ValueError(
                f"OHLCV columns missing: {market}"
            )

        if not (
            REQUIRED_COLUMNS | {"signal_trigger"}
        ).issubset(set(signals.fieldnames or [])):
            raise ValueError(
                f"Signal columns missing: {market}"
            )

        previous = None
        count = 0

        from itertools import zip_longest
        for candle, signal in zip_longest(
            candles, signals
        ):
            if candle is None or signal is None:
                raise ValueError(
                    f"Row count mismatch: {market}"
                )

            t = utc_second(
                candle["candle_date_time_utc"]
            )
            st = utc_second(
                signal["candle_date_time_utc"]
            )

            if (
                t != st
                or candle["market"] != market
                or signal["market"] != market
            ):
                raise ValueError(
                    f"Timestamp/market mismatch: {market}"
                )

            if previous is not None and t <= previous:
                raise ValueError(
                    f"Timestamp order invalid: {market}"
                )
            previous = t

            for field in ("open", "high", "low", "close"):
                a = float(candle[field])
                b = float(signal[field])
                if (
                    not math.isfinite(a)
                    or not math.isfinite(b)
                    or a <= 0
                    or b <= 0
                    or not math.isclose(
                        a, b,
                        rel_tol=1e-9,
                        abs_tol=1e-12,
                    )
                ):
                    raise ValueError(
                        f"Price mismatch: {market}:{field}"
                    )

            raw_flag = signal[
                "signal_trigger"
            ].strip().lower()
            if raw_flag not in (
                "0", "1", "true", "false"
            ):
                raise ValueError(
                    f"Invalid signal flag: {market}"
                )

            times.append(t)
            opens.append(float(candle["open"]))
            highs.append(float(candle["high"]))
            closes.append(float(candle["close"]))
            flags.append(
                int(raw_flag in ("1", "true"))
            )
            count += 1

    # Prefix count of broken one-hour transitions.
    breaks = array("I", [0])
    for i in range(1, count):
        broken = int(
            times[i] - times[i - 1] != HOUR_SECONDS
        )
        breaks.append(breaks[-1] + broken)

    return {
        "times": times,
        "opens": opens,
        "highs": highs,
        "closes": closes,
        "flags": flags,
        "breaks": breaks,
        "count": count,
        "busy_until": -1,
    }


def lookup_index(times, target):
    from bisect import bisect_left
    index = bisect_left(times, target)
    if index == len(times) or times[index] != target:
        return None
    return index


def candidate_eligible(data, index, second):
    if data["flags"][index] != 0:
        return False, "control_signal_present"

    if second <= data["busy_until"]:
        return False, "control_window_overlap"

    end = index + HORIZON
    if end >= data["count"]:
        return False, "control_right_censored"

    if (
        data["breaks"][end]
        - data["breaks"][index]
    ) != 0:
        return False, "control_time_gap"

    return True, "eligible"


def evaluate_control(data, index):
    entry = data["opens"][index + 1]
    target = entry * TARGET_MULTIPLIER
    first_hit = None
    max_high = -math.inf

    for offset in range(1, HORIZON + 1):
        high = data["highs"][index + offset]
        max_high = max(max_high, high)
        if first_hit is None and high >= target:
            first_hit = offset

    end_close = data["closes"][index + HORIZON]
    return {
        "entry": entry,
        "target": target,
        "hit": int(first_hit is not None),
        "first_hit": first_hit or "",
        "max_high_return_pct": round(
            (max_high / entry - 1) * 100, 8
        ),
        "end_close_return_pct": round(
            (end_close / entry - 1) * 100, 8
        ),
    }


def deterministic_start(event, market_count):
    value = (
        f"{event['market']}|"
        f"{event['second']}|"
        f"{event['event_id']}"
    )
    h = hashlib.sha256(value.encode("utf-8")).digest()
    return int.from_bytes(h[:8], "big") % market_count


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True)
    args = parser.parse_args()

    root = Path(args.root).resolve()
    prior_folder = (
        root / "data" / "reports"
        / "pattern_256_h1_event_outcomes"
    )
    output = (
        root / "data" / "reports"
        / "pattern_256_h1_matched_controls"
    )

    check_prior_manifest(prior_folder)

    prior = read_json(
        prior_folder / "event_outcomes_result.json"
    )

    if (
        prior.get("execution") != "PASS"
        or prior.get("version") != "Clean V001"
        or prior.get("timeframe") != "h1"
        or prior.get("horizon_hours") != HORIZON
        or prior.get("research_decision") != "HOLD"
    ):
        raise ValueError("Prior result not approved as input")

    events = load_events(
        prior_folder / "event_outcomes_events.csv"
    )
    totals = prior.get("totals", {})

    if (
        len(events) != totals.get("eligible_events")
        or sum(x["hit"] for x in events)
        != totals.get("target_hits")
    ):
        raise ValueError("Prior event totals mismatch")

    fingerprints = prior.get(
        "input_fingerprints", {}
    )
    markets = sorted(fingerprints)

    if len(markets) != totals.get("markets"):
        raise ValueError("Prior market inventory mismatch")

    ohlcv_folder = root / "data" / "ohlcv" / "h1"
    signal_folder = (
        root / "data" / "signals" / "256" / "h1"
    )

    if (
        not ohlcv_folder.is_dir()
        or not signal_folder.is_dir()
    ):
        raise FileNotFoundError("H1 source folders missing")

    data_by_market = {}

    print(
        f"Verifying and loading {len(markets)} markets",
        flush=True,
    )

    for number, market in enumerate(markets, start=1):
        left = ohlcv_folder / f"{market}.csv"
        right = signal_folder / f"{market}.csv"

        if not left.is_file() or not right.is_file():
            raise FileNotFoundError(
                f"Source file missing: {market}"
            )

        expected = fingerprints[market]
        if (
            digest(left) != expected["ohlcv_sha256"]
            or digest(right) != expected["signal_sha256"]
        ):
            raise ValueError(
                f"Source changed since prior audit: {market}"
            )

        data_by_market[market] = load_market(
            left, right, market
        )

        if number % 10 == 0:
            print(
                f"Loaded {number}/{len(markets)}",
                flush=True,
            )

    # Validate that every original event is still
    # attached to a real signal candle.
    for event in events:
        data = data_by_market[event["market"]]
        index = lookup_index(
            data["times"], event["second"]
        )
        if index is None or data["flags"][index] != 1:
            raise ValueError(
                f"Prior event signal not found: {event}"
            )
        if index + HORIZON >= data["count"]:
            raise ValueError(
                "Prior event horizon missing"
            )
        if (
            data["breaks"][index + HORIZON]
            - data["breaks"][index]
        ) != 0:
            raise ValueError(
                "Prior event contains a time gap"
            )
        if not math.isclose(
            data["opens"][index + 1],
            event["entry"],
            rel_tol=1e-9,
            abs_tol=1e-12,
        ):
            raise ValueError(
                "Prior event entry mismatch"
            )

        # Recheck original +30% label.
        outcome = evaluate_control(data, index)
        if outcome["hit"] != event["hit"]:
            raise ValueError(
                "Prior event outcome mismatch"
            )

    print(
        f"Validated {len(events)} original events",
        flush=True,
    )

    counters = Counter()
    pairs = []
    unmatched = []
    market_count = len(markets)

    for number, event in enumerate(events, start=1):
        start = deterministic_start(
            event, market_count
        )
        selected = None
        reasons = Counter()

        for offset in range(market_count):
            market = markets[
                (start + offset) % market_count
            ]

            if market == event["market"]:
                continue

            data = data_by_market[market]
            index = lookup_index(
                data["times"], event["second"]
            )
            if index is None:
                reasons["control_no_same_time_candle"] += 1
                continue

            eligible, reason = candidate_eligible(
                data, index, event["second"]
            )
            if not eligible:
                reasons[reason] += 1
                continue

            selected = (market, data, index)
            break

        if selected is None:
            counters["unmatched_events"] += 1
            unmatched.append({
                "event_id": event["event_id"],
                "signal_time_utc": utc_iso(
                    event["second"]
                ),
                "signal_market": event["market"],
                "reason": "NO_ELIGIBLE_SAME_TIME_CONTROL",
            })
            continue

        market, data, index = selected
        outcome = evaluate_control(data, index)

        # Reserve this market's evaluation interval.
        data["busy_until"] = (
            event["second"]
            + HORIZON * HOUR_SECONDS
        )

        signal_hit = event["hit"]
        control_hit = outcome["hit"]

        pairs.append({
            "event_id": event["event_id"],
            "signal_time_utc": utc_iso(
                event["second"]
            ),
            "signal_market": event["market"],
            "control_market": market,
            "signal_hit_30pct": signal_hit,
            "control_hit_30pct": control_hit,
            "signal_entry_open": event["entry"],
            "control_entry_open": outcome["entry"],
            "control_target_price": outcome["target"],
            "control_first_hit_hour": outcome["first_hit"],
            "control_max_high_return_pct": (
                outcome["max_high_return_pct"]
            ),
            "control_end_close_return_pct": (
                outcome["end_close_return_pct"]
            ),
        })

        counters["matched_pairs"] += 1
        counters["signal_hits_in_pairs"] += signal_hit
        counters["control_hits_in_pairs"] += control_hit

        if signal_hit and not control_hit:
            counters["signal_only_hits"] += 1
        elif control_hit and not signal_hit:
            counters["control_only_hits"] += 1
        elif signal_hit and control_hit:
            counters["both_hit"] += 1
        else:
            counters["neither_hit"] += 1

        if number % 1000 == 0:
            print(
                f"Matched progress {number}/{len(events)}",
                flush=True,
            )

    matched = counters["matched_pairs"]
    if matched == 0:
        raise ValueError("No matched control pairs")

    signal_rate = (
        100 * counters["signal_hits_in_pairs"] / matched
    )
    control_rate = (
        100 * counters["control_hits_in_pairs"] / matched
    )
    difference = signal_rate - control_rate

    result = {
        "version": VERSION,
        "execution": "PASS",
        "classification": "MATCHED_CONTROL_COMPUTED",
        "timeframe": "h1",
        "horizon_hours": HORIZON,
        "target_return_pct": 30,
        "matching": "EXACT_UTC_TIME_DIFFERENT_MARKET",
        "control_signal_requirement": "TRIGGER_ZERO",
        "entry_assumption": "NEXT_CANDLE_OPEN",
        "nonoverlap_control": "PER_MARKET_168H",
        "control_selection": (
            "DETERMINISTIC_HASH_ROTATION_"
            "FIRST_ELIGIBLE"
        ),
        "prior_event_count": len(events),
        "matched_pairs": matched,
        "unmatched_events": len(unmatched),
        "signal_hits_in_pairs": (
            counters["signal_hits_in_pairs"]
        ),
        "control_hits_in_pairs": (
            counters["control_hits_in_pairs"]
        ),
        "signal_hit_rate_pct": round(
            signal_rate, 8
        ),
        "control_hit_rate_pct": round(
            control_rate, 8
        ),
        "difference_percentage_points": round(
            difference, 8
        ),
        "paired_outcomes": {
            "signal_only_hits": (
                counters["signal_only_hits"]
            ),
            "control_only_hits": (
                counters["control_only_hits"]
            ),
            "both_hit": counters["both_hit"],
            "neither_hit": counters["neither_hit"],
        },
        "prior_manifest_sha256": digest(
            prior_folder / "event_outcomes_manifest.json"
        ),
        "prior_result_sha256": digest(
            prior_folder / "event_outcomes_result.json"
        ),
        "selection_uses_future_returns": False,
        "eligibility_requires_complete_future_data": True,
        "historical_market_universe_bias_possible": True,
        "independent_oos_proven": False,
        "causal_effect_proven": False,
        "profitability_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
    }

    report_lines = [
        "# Pattern 256 H1 Matched Controls",
        "",
        f"Version: {VERSION}",
        "Execution: PASS",
        "Classification: MATCHED_CONTROL_COMPUTED",
        "",
        f"Original events: {len(events)}",
        f"Matched pairs: {matched}",
        f"Unmatched events: {len(unmatched)}",
        f"Signal hits (matched): "
        f"{counters['signal_hits_in_pairs']}",
        f"Control hits (matched): "
        f"{counters['control_hits_in_pairs']}",
        f"Signal hit rate (%): {signal_rate:.8f}",
        f"Control hit rate (%): {control_rate:.8f}",
        f"Difference (percentage points): "
        f"{difference:.8f}",
        "",
        "## Paired outcomes",
        "",
        f"Signal only: {counters['signal_only_hits']}",
        f"Control only: {counters['control_only_hits']}",
        f"Both: {counters['both_hit']}",
        f"Neither: {counters['neither_hit']}",
        "",
        "## Limitations",
        "",
        "- This is an observational matched comparison.",
        "- Matching is on UTC time, not liquidity or "
        "volatility.",
        "- Controls require full future coverage; "
        "selection bias remains possible.",
        "- Market universe may contain historical "
        "survivorship or listing bias.",
        "- Original event selection also requires "
        "complete future coverage.",
        "- Exact-time control absence of signal does "
        "not imply no future signal.",
        "- Signal and control events across different "
        "markets can remain correlated.",
        "- A high touch does not guarantee execution.",
        "- Fees, spread, slippage and order book "
        "liquidity are not modeled.",
        "- Independent OOS is not proven.",
        "- No causal or profitability conclusion.",
        "- Training and trading remain blocked.",
        "- Research decision: HOLD.",
        "",
    ]

    output.mkdir(parents=True, exist_ok=True)

    result_path = (
        output / "matched_controls_result.json"
    )
    pairs_path = (
        output / "matched_controls_pairs.csv"
    )
    unmatched_path = (
        output / "matched_controls_unmatched.csv"
    )
    report_path = (
        output / "matched_controls_report.md"
    )
    manifest_path = (
        output / "matched_controls_manifest.json"
    )

    write_json(result_path, result)
    atomic_text(
        pairs_path,
        csv_text(pairs, PAIR_FIELDS),
    )
    atomic_text(
        unmatched_path,
        csv_text(
            unmatched,
            [
                "event_id",
                "signal_time_utc",
                "signal_market",
                "reason",
            ],
        ),
    )
    atomic_text(
        report_path,
        "\n".join(report_lines),
    )

    output_files = (
        result_path,
        pairs_path,
        unmatched_path,
        report_path,
    )

    write_json(manifest_path, {
        "version": VERSION,
        "files": {
            path.name: digest(path)
            for path in output_files
        },
    })

    print(
        json.dumps({
            "classification": (
                result["classification"]
            ),
            "matched_pairs": matched,
            "unmatched_events": len(unmatched),
            "signal_hit_rate_pct": (
                result["signal_hit_rate_pct"]
            ),
            "control_hit_rate_pct": (
                result["control_hit_rate_pct"]
            ),
            "difference_percentage_points": (
                result["difference_percentage_points"]
            ),
            "research_decision": "HOLD",
        }, ensure_ascii=False, indent=2),
        flush=True,
    )


if __name__ == "__main__":
    main()
