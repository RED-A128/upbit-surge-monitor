
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 H1 Independent Event Outcomes
Clean V001

Research only. No orders or training.

Signal: signal_trigger == 1
Entry: next candle open
Target: entry * 1.30
Horizon: 168 actual hourly intervals
Event isolation: no overlapping evaluation windows
Incomplete or gapped windows: excluded

Target touch is not a guaranteed executable fill.
"""

import argparse
import csv
import hashlib
import json
import math
import os
import tempfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

VERSION = "Clean V001"
HORIZON = 168
TARGET_RETURN = 0.30
EXPECTED_SECONDS = 3600


def sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for block in iter(
            lambda: stream.read(1024 * 1024), b""
        ):
            digest.update(block)
    return digest.hexdigest()


def atomic_write(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp = tempfile.mkstemp(
        prefix=".event_",
        dir=str(path.parent),
    )
    try:
        with os.fdopen(
            fd, "w", encoding="utf-8", newline=""
        ) as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, path)
    finally:
        if os.path.exists(temp):
            os.unlink(temp)


def write_json(path, value):
    atomic_write(
        path,
        json.dumps(
            value,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            allow_nan=False,
        ) + "\n",
    )


def timestamp(value):
    parsed = datetime.fromisoformat(
        value.strip().replace("Z", "+00:00")
    )
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def read_market(path, is_signal):
    required = {
        "market",
        "candle_date_time_utc",
        "open",
        "high",
        "low",
        "close",
    }
    if is_signal:
        required.add("signal_trigger")

    rows = []
    with open(
        path, "r", encoding="utf-8-sig", newline=""
    ) as stream:
        reader = csv.DictReader(stream)
        if not required.issubset(set(reader.fieldnames or [])):
            raise ValueError(
                f"Required columns missing: {path}"
            )

        previous = None
        for line, item in enumerate(reader, start=2):
            dt = timestamp(item["candle_date_time_utc"])
            if previous is not None and dt <= previous:
                raise ValueError(
                    f"Unsorted timestamp: {path}:{line}"
                )
            previous = dt

            values = {
                "time": dt,
                "market": item["market"],
                "open": float(item["open"]),
                "high": float(item["high"]),
                "low": float(item["low"]),
                "close": float(item["close"]),
            }
            for col in ("open", "high", "low", "close"):
                if (
                    not math.isfinite(values[col])
                    or values[col] <= 0
                ):
                    raise ValueError(
                        f"Invalid price: {path}:{line}"
                    )

            if is_signal:
                flag = item["signal_trigger"].strip().lower()
                if flag not in ("0", "1", "true", "false"):
                    raise ValueError(
                        f"Invalid trigger: {path}:{line}"
                    )
                values["trigger"] = flag in ("1", "true")

            rows.append(values)
    return rows


def analyze_market(market, candles, signals):
    if len(candles) != len(signals):
        raise ValueError(f"Row mismatch: {market}")

    for index, (candle, signal) in enumerate(
        zip(candles, signals)
    ):
        if (
            candle["time"] != signal["time"]
            or candle["market"] != market
            or signal["market"] != market
        ):
            raise ValueError(
                f"Identity mismatch: {market}:{index}"
            )
        for field in ("open", "high", "low", "close"):
            if not math.isclose(
                candle[field],
                signal[field],
                rel_tol=1e-9,
                abs_tol=1e-12,
            ):
                raise ValueError(
                    f"Price mismatch: {market}:{index}"
                )

    stats = Counter()
    events = []
    next_allowed = 0
    n = len(candles)

    for i, signal in enumerate(signals):
        if not signal["trigger"]:
            continue

        stats["raw_trigger_rows"] += 1

        if i < next_allowed:
            stats["overlap_skipped"] += 1
            continue

        # One signal candle, then 168 future candles.
        # Entry is at candle i+1 open.
        if i + HORIZON >= n:
            stats["right_censored"] += 1
            continue

        window = candles[i:i + HORIZON + 1]
        continuous = all(
            int(
                (
                    window[j + 1]["time"]
                    - window[j]["time"]
                ).total_seconds()
            ) == EXPECTED_SECONDS
            for j in range(HORIZON)
        )

        if not continuous:
            stats["time_gap_excluded"] += 1
            continue

        entry = candles[i + 1]["open"]
        target = entry * (1.0 + TARGET_RETURN)
        future = candles[i + 1:i + HORIZON + 1]

        first_hit = None
        max_high = -math.inf
        min_low = math.inf

        for offset, candle in enumerate(future, start=1):
            max_high = max(max_high, candle["high"])
            min_low = min(min_low, candle["low"])
            if first_hit is None and candle["high"] >= target:
                first_hit = offset

        end_close = future[-1]["close"]
        hit = first_hit is not None

        events.append({
            "market": market,
            "signal_time_utc": (
                candles[i]["time"].isoformat()
            ),
            "entry_time_utc": (
                future[0]["time"].isoformat()
            ),
            "entry_open": entry,
            "target_price": target,
            "hit_30pct": int(hit),
            "first_hit_hour": first_hit or "",
            "max_high_return_pct": round(
                (max_high / entry - 1) * 100, 8
            ),
            "max_adverse_low_pct": round(
                (min_low / entry - 1) * 100, 8
            ),
            "end_close_return_pct": round(
                (end_close / entry - 1) * 100, 8
            ),
            "horizon_hours": HORIZON,
        })

        stats["eligible_events"] += 1
        if hit:
            stats["target_hits"] += 1
        else:
            stats["target_misses"] += 1

        # The next accepted event must start after
        # the current event's last evaluated candle.
        next_allowed = i + HORIZON + 1

    return stats, events


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True)
    args = parser.parse_args()

    root = Path(args.root).resolve()
    source = root / "data" / "ohlcv" / "h1"
    signals = root / "data" / "signals" / "256" / "h1"
    output = (
        root / "data" / "reports"
        / "pattern_256_h1_event_outcomes"
    )

    if not source.is_dir() or not signals.is_dir():
        raise FileNotFoundError(
            "H1 OHLCV or signal directory missing"
        )

    source_files = {
        p.name: p for p in source.glob("KRW-*.csv")
    }
    signal_files = {
        p.name: p for p in signals.glob("KRW-*.csv")
    }

    if (
        not source_files
        or set(source_files) != set(signal_files)
    ):
        raise ValueError(
            "H1 file inventory mismatch"
        )

    totals = Counter()
    market_results = []
    all_events = []
    fingerprints = {}

    for index, filename in enumerate(
        sorted(source_files), start=1
    ):
        market = Path(filename).stem
        left = source_files[filename]
        right = signal_files[filename]

        candles = read_market(left, False)
        triggers = read_market(right, True)

        stats, events = analyze_market(
            market, candles, triggers
        )

        totals.update(stats)
        totals["markets"] += 1
        all_events.extend(events)

        market_results.append({
            "market": market,
            "candle_rows": len(candles),
            **dict(stats),
        })

        fingerprints[market] = {
            "ohlcv_sha256": sha256(left),
            "signal_sha256": sha256(right),
        }

        if index % 10 == 0:
            print(
                f"Processed {index}/{len(source_files)}",
                flush=True,
            )

    eligible = totals["eligible_events"]
    hits = totals["target_hits"]

    result = {
        "version": VERSION,
        "execution": "PASS",
        "classification": "EVENT_OUTCOMES_COMPUTED",
        "timeframe": "h1",
        "horizon_hours": HORIZON,
        "target_return_pct": 30,
        "entry_assumption": "NEXT_CANDLE_OPEN",
        "target_test": "FUTURE_HIGH_TOUCH",
        "nonoverlap": "PER_MARKET_168H_WINDOW",
        "totals": dict(totals),
        "hit_rate_pct": (
            round(100 * hits / eligible, 8)
            if eligible else None
        ),
        "markets": market_results,
        "input_fingerprints": fingerprints,
        "point_in_time_proven": False,
        "independent_oos_proven": False,
        "matched_control_proven": False,
        "profitability_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
    }

    output.mkdir(parents=True, exist_ok=True)
    result_path = output / "event_outcomes_result.json"
    csv_path = output / "event_outcomes_events.csv"
    report_path = output / "event_outcomes_report.md"
    manifest_path = output / "event_outcomes_manifest.json"

    fields = [
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

    import io
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(
        buffer, fieldnames=fields, lineterminator="\n"
    )
    writer.writeheader()
    writer.writerows(all_events)

    report = "\n".join([
        "# Pattern 256 H1 Event Outcomes",
        "",
        f"Version: {VERSION}",
        "Execution: PASS",
        "Classification: EVENT_OUTCOMES_COMPUTED",
        "",
        f"Markets: {totals['markets']}",
        f"Raw trigger rows: {totals['raw_trigger_rows']}",
        f"Eligible independent events: {eligible}",
        f"Target hits: {hits}",
        f"Target misses: {totals['target_misses']}",
        f"Overlap skipped: {totals['overlap_skipped']}",
        f"Right censored: {totals['right_censored']}",
        f"Time-gap excluded: {totals['time_gap_excluded']}",
        f"Hit rate (%): {result['hit_rate_pct']}",
        "",
        "## Limitations",
        "",
        "- Next-candle open is a hypothetical entry.",
        "- High touch does not guarantee execution.",
        "- Signal availability at candle close is unproven.",
        "- Independent out-of-sample is unproven.",
        "- Matched controls have not been evaluated.",
        "- Fees, slippage and liquidity are excluded.",
        "- No live-trading or training approval.",
        "- Research decision: HOLD.",
        "",
    ]) + "\n"

    write_json(result_path, result)
    atomic_write(csv_path, buffer.getvalue())
    atomic_write(report_path, report)

    write_json(manifest_path, {
        "version": VERSION,
        "files": {
            p.name: sha256(p)
            for p in (
                result_path,
                csv_path,
                report_path,
            )
        },
    })

    print(json.dumps({
        "classification": result["classification"],
        "totals": result["totals"],
        "hit_rate_pct": result["hit_rate_pct"],
        "research_decision": "HOLD",
    }, ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    main()
