
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pattern 256 Chronological OOS Preliminary Evaluation
Clean V001

Research only. No trading or training approval.

Input:
  data/ohlcv/{h1,h4,d1}/KRW-*.csv
  data/signals/256/{h1,h4,d1}/KRW-*.csv

Output:
  data/reports/pattern_256_chronological_oos/

Policy:
  - Signal: signal_trigger == 1 AND signal_stage == TRIGGER
  - Consecutive triggers form one episode
  - Entry: next consecutive candle OPEN
  - Horizons: 24, 72, 168 candles
  - Target: entry price * 1.30
  - A hit uses subsequent HIGH prices
  - Missing candles invalidate the affected horizon
  - Chronological calendar split: 70% / 15% / 15%
  - No crossing a split boundary
  - No fabricated fills, fees, or profitability claims
"""

import argparse
import csv
import hashlib
import json
import math
import os
import sys
import tempfile
from collections import Counter, defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path

VERSION = "Clean V001"
POLICY_ID = "pattern_256_chronological_oos_v001"

TIMEFRAMES = {
    "h1": 3600,
    "h4": 14400,
    "d1": 86400,
}

HORIZONS = (24, 72, 168)
TARGET_RETURN = 0.30

EVENT_COLUMNS = [
    "timeframe",
    "market",
    "split",
    "signal_timestamp",
    "entry_timestamp",
    "entry_price",
    "horizon",
    "status",
    "hit_30pct",
    "first_hit_bars",
    "max_return_pct",
    "max_drawdown_pct",
    "observed_bars",
    "reason",
]

REQUIRED_OHLCV = {
    "market", "candle_date_time_utc",
    "open", "high", "low", "close",
}

REQUIRED_SIGNAL = {
    "market", "candle_date_time_utc",
    "signal_trigger", "signal_stage",
}

POLICY = {
    "policy_id": POLICY_ID,
    "version": VERSION,
    "trigger": "signal_trigger=1 AND signal_stage=TRIGGER",
    "episode": "first row of consecutive trigger run",
    "entry": "next consecutive candle open",
    "horizons": list(HORIZONS),
    "target_return": TARGET_RETURN,
    "split": [0.70, 0.15, 0.15],
    "boundary": "entry and full horizon within same split",
    "missing": "strict consecutive UTC candles",
    "profitability": "not evaluated",
}


def digest_bytes(value):
    return hashlib.sha256(value).hexdigest()


def digest_file(path):
    sha = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            sha.update(chunk)
    return sha.hexdigest()


def policy_hash():
    encoded = json.dumps(
        POLICY, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return digest_bytes(encoded)


def atomic_text(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(
        prefix=".partial_",
        dir=str(path.parent),
        suffix=".tmp",
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as out:
            out.write(content)
            out.flush()
            os.fsync(out.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def atomic_json(path, obj):
    atomic_text(
        path,
        json.dumps(obj, indent=2, ensure_ascii=False, sort_keys=True)
        + "\n",
    )


def atomic_csv(path, rows, columns):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(
        prefix=".partial_",
        dir=str(path.parent),
        suffix=".tmp",
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as out:
            writer = csv.DictWriter(
                out, fieldnames=columns, extrasaction="ignore"
            )
            writer.writeheader()
            writer.writerows(rows)
            out.flush()
            os.fsync(out.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def parse_time(value):
    value = str(value).strip()
    if not value:
        raise ValueError("Empty UTC timestamp")
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise ValueError("Timezone missing: " + value)
    return dt.astimezone(timezone.utc)


def parse_price(value):
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise ValueError("Invalid positive price")
    return number


def load_csv(path, required):
    with open(path, "r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError("Empty CSV header: " + str(path))
        missing = required - set(reader.fieldnames)
        if missing:
            raise ValueError(
                "Missing columns: " + ", ".join(sorted(missing))
            )
        return list(reader)


def scan_bounds(path):
    first = None
    last = None
    previous = None
    count = 0
    with open(path, "r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames or (
            "candle_date_time_utc" not in reader.fieldnames
        ):
            raise ValueError("Timestamp column missing: " + str(path))
        for row in reader:
            ts = parse_time(row["candle_date_time_utc"])
            if previous is not None and ts <= previous:
                raise ValueError(
                    "Unsorted or duplicate timestamps: " + str(path)
                )
            if first is None:
                first = ts
            last = ts
            previous = ts
            count += 1
    if not count:
        raise ValueError("Empty OHLCV file: " + str(path))
    return first, last, count


def get_split(ts, start, boundary1, boundary2, end):
    if ts < start or ts > end:
        return None
    if ts < boundary1:
        return "DEV"
    if ts < boundary2:
        return "VALIDATION"
    return "TEST"


def build_boundaries(start, end):
    duration = end - start
    return (
        start + duration * 0.70,
        start + duration * 0.85,
    )


def signal_active(row):
    raw = str(row["signal_trigger"]).strip().lower()
    stage = str(row["signal_stage"]).strip().upper()
    if raw not in {"0", "1", "true", "false"}:
        raise ValueError("Unexpected signal_trigger: " + raw)
    return raw in {"1", "true"} and stage == "TRIGGER"


def validate_ohlcv_row(row, expected_market):
    if row["market"].strip() != expected_market:
        raise ValueError("OHLCV market mismatch")
    o = parse_price(row["open"])
    h = parse_price(row["high"])
    l = parse_price(row["low"])
    c = parse_price(row["close"])
    if l > min(o, c) or h < max(o, c) or l > h:
        raise ValueError("Invalid OHLC price relationship")
    return o, h, l, c


def make_event(
    tf, market, split, signal_time, entry_time,
    entry_price, horizon, status, hit, hit_bars,
    max_return, max_drawdown, observed, reason,
):
    return {
        "timeframe": tf,
        "market": market,
        "split": split,
        "signal_timestamp": signal_time.isoformat(),
        "entry_timestamp": (
            entry_time.isoformat() if entry_time else ""
        ),
        "entry_price": entry_price if entry_price else "",
        "horizon": horizon,
        "status": status,
        "hit_30pct": hit if status in {"HIT", "MISS"} else "",
        "first_hit_bars": hit_bars if hit_bars else "",
        "max_return_pct": (
            round(max_return, 8) if max_return is not None else ""
        ),
        "max_drawdown_pct": (
            round(max_drawdown, 8) if max_drawdown is not None else ""
        ),
        "observed_bars": observed,
        "reason": reason,
    }


def evaluate_pair(
    tf, market, ohlcv_path, signal_path,
    start, boundary1, boundary2, end,
):
    ohlcv = load_csv(ohlcv_path, REQUIRED_OHLCV)
    signals = load_csv(signal_path, REQUIRED_SIGNAL)

    if len(ohlcv) != len(signals):
        raise ValueError("OHLCV / signal row count mismatch")

    n = len(ohlcv)
    seconds = TIMEFRAMES[tf]
    step = timedelta(seconds=seconds)

    times = []
    opens = []
    highs = []
    lows = []
    active = []

    previous = None

    for index in range(n):
        price_row = ohlcv[index]
        signal_row = signals[index]

        ts = parse_time(price_row["candle_date_time_utc"])
        signal_ts = parse_time(
            signal_row["candle_date_time_utc"]
        )

        if ts != signal_ts:
            raise ValueError(
                "OHLCV / signal timestamp mismatch at row "
                + str(index)
            )

        if previous is not None and ts <= previous:
            raise ValueError("Duplicate or reverse timestamp")

        if signal_row["market"].strip() != market:
            raise ValueError("Signal market mismatch")

        o, h, l, _ = validate_ohlcv_row(price_row, market)

        times.append(ts)
        opens.append(o)
        highs.append(h)
        lows.append(l)
        active.append(signal_active(signal_row))
        previous = ts

    events = []
    episodes = 0
    previous_active = False

    for i in range(n):
        current_active = active[i]

        if not current_active:
            previous_active = False
            continue

        # A new episode starts only after a non-trigger row,
        # or after a discontinuity in source timestamps.
        continuous_trigger = (
            previous_active
            and i > 0
            and times[i] - times[i - 1] == step
        )

        previous_active = True

        if continuous_trigger:
            continue

        episodes += 1
        signal_time = times[i]
        split = get_split(
            signal_time, start, boundary1, boundary2, end
        )

        # The signal is assumed available only after
        # its candle closes. Entry is at the next candle open.
        entry_i = i + 1
        valid_entry = (
            entry_i < n
            and times[entry_i] - times[i] == step
        )

        entry_time = times[entry_i] if valid_entry else None
        entry_price = opens[entry_i] if valid_entry else None

        for horizon in HORIZONS:
            if not valid_entry:
                events.append(make_event(
                    tf, market, split, signal_time,
                    None, None, horizon, "EXCLUDED",
                    "", None, None, None, 0,
                    "NEXT_CANDLE_MISSING",
                ))
                continue

            entry_split = get_split(
                entry_time, start, boundary1, boundary2, end
            )

            if split != entry_split:
                events.append(make_event(
                    tf, market, split, signal_time,
                    entry_time, entry_price, horizon,
                    "EXCLUDED", "", None, None, None,
                    0, "SPLIT_ENTRY_BOUNDARY",
                ))
                continue

            last_i = entry_i + horizon - 1

            if last_i >= n:
                events.append(make_event(
                    tf, market, split, signal_time,
                    entry_time, entry_price, horizon,
                    "CENSORED", "", None, None, None,
                    max(0, n - entry_i),
                    "INSUFFICIENT_FUTURE_CANDLES",
                ))
                continue

            last_split = get_split(
                times[last_i],
                start, boundary1, boundary2, end,
            )

            if last_split != split:
                events.append(make_event(
                    tf, market, split, signal_time,
                    entry_time, entry_price, horizon,
                    "EXCLUDED", "", None, None, None,
                    0, "SPLIT_HORIZON_BOUNDARY",
                ))
                continue

            gap = False
            for j in range(entry_i, last_i + 1):
                if j > 0 and times[j] - times[j - 1] != step:
                    gap = True
                    break

            if gap:
                events.append(make_event(
                    tf, market, split, signal_time,
                    entry_time, entry_price, horizon,
                    "EXCLUDED", "", None, None, None,
                    0, "WINDOW_GAP",
                ))
                continue

            target = entry_price * (1.0 + TARGET_RETURN)
            high_window = highs[entry_i:last_i + 1]
            low_window = lows[entry_i:last_i + 1]

            first_hit = None
            for offset, high in enumerate(high_window, start=1):
                if high >= target:
                    first_hit = offset
                    break

            max_return = (
                max(high_window) / entry_price - 1.0
            ) * 100.0

            max_drawdown = (
                min(low_window) / entry_price - 1.0
            ) * 100.0

            hit = int(first_hit is not None)

            events.append(make_event(
                tf, market, split, signal_time,
                entry_time, entry_price, horizon,
                "HIT" if hit else "MISS",
                hit, first_hit, max_return,
                max_drawdown, horizon, "",
            ))

    return events, episodes, n


def summarize(events):
    grouped = defaultdict(Counter)
    reasons = Counter()

    for event in events:
        key = (
            event["timeframe"],
            event["split"],
            int(event["horizon"]),
        )
        grouped[key][event["status"]] += 1
        if event["reason"]:
            reasons[event["reason"]] += 1

    rows = []

    for key in sorted(grouped):
        tf, split, horizon = key
        counts = grouped[key]
        hits = counts["HIT"]
        misses = counts["MISS"]
        completed = hits + misses

        rows.append({
            "timeframe": tf,
            "split": split,
            "horizon": horizon,
            "hits": hits,
            "misses": misses,
            "completed": completed,
            "hit_rate_pct": (
                round(hits / completed * 100.0, 6)
                if completed else ""
            ),
            "excluded": counts["EXCLUDED"],
            "censored": counts["CENSORED"],
        })

    return rows, dict(sorted(reasons.items()))


SUMMARY_COLUMNS = [
    "timeframe", "split", "horizon",
    "hits", "misses", "completed",
    "hit_rate_pct", "excluded", "censored",
]


def valid_checkpoint(
    checkpoint, event_file, ohlcv_sha,
    signal_sha, current_policy_hash,
):
    if not checkpoint.exists() or not event_file.exists():
        return False

    try:
        with open(checkpoint, encoding="utf-8") as handle:
            saved = json.load(handle)

        return all([
            saved.get("complete") is True,
            saved.get("version") == VERSION,
            saved.get("policy_sha256") == current_policy_hash,
            saved.get("ohlcv_sha256") == ohlcv_sha,
            saved.get("signal_sha256") == signal_sha,
            saved.get("event_sha256") == digest_file(event_file),
        ])
    except (OSError, ValueError, TypeError):
        return False


def discover(root):
    pairs = []

    for tf in TIMEFRAMES:
        ohlcv_dir = root / "data" / "ohlcv" / tf
        signal_dir = root / "data" / "signals" / "256" / tf

        if not ohlcv_dir.is_dir() or not signal_dir.is_dir():
            raise RuntimeError("Missing input directory for " + tf)

        ohlcv_files = {
            p.name: p for p in ohlcv_dir.glob("KRW-*.csv")
        }
        signal_files = {
            p.name: p for p in signal_dir.glob("KRW-*.csv")
        }

        if not ohlcv_files or not signal_files:
            raise RuntimeError("No input files for " + tf)

        if set(ohlcv_files) != set(signal_files):
            raise RuntimeError(
                "OHLCV / signal file mismatch for " + tf
            )

        for name in sorted(ohlcv_files):
            pairs.append((
                tf, name[:-4],
                ohlcv_files[name], signal_files[name],
            ))

    return pairs


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True)
    parser.add_argument(
        "--reset-checkpoints",
        action="store_true",
        help="Recompute without deleting existing source files",
    )
    args = parser.parse_args()

    root = Path(args.root).expanduser().resolve()
    out = (
        root / "data" / "reports"
        / "pattern_256_chronological_oos"
    )
    checkpoints = out / "checkpoints"
    checkpoints.mkdir(parents=True, exist_ok=True)

    pairs = discover(root)

    print("Pattern 256 Chronological OOS", VERSION, flush=True)
    print("Input pairs:", len(pairs), flush=True)

    # Determine global calendar boundaries using OHLCV
    # timestamps only. Signal labels are not used.
    starts = []
    ends = []
    total_source_rows = 0

    for index, (_, market, source, _) in enumerate(pairs, 1):
        first, last, count = scan_bounds(source)
        starts.append(first)
        ends.append(last)
        total_source_rows += count

        if index % 50 == 0:
            print(
                "[BOUNDS]", index, "/", len(pairs),
                flush=True,
            )

    global_start = min(starts)
    global_end = max(ends)
    boundary1, boundary2 = build_boundaries(
        global_start, global_end
    )

    print("DEV until:", boundary1.isoformat(), flush=True)
    print("VALIDATION until:", boundary2.isoformat(), flush=True)
    print("TEST until:", global_end.isoformat(), flush=True)

    current_policy_hash = policy_hash()
    all_events = []
    all_errors = []
    processed = 0
    resumed = 0
    episodes_total = 0

    for index, (tf, market, source, signal) in enumerate(
        pairs, start=1
    ):
        checkpoint = checkpoints / tf / (market + ".json")
        event_file = checkpoints / tf / (market + ".csv")
        checkpoint.parent.mkdir(parents=True, exist_ok=True)

        try:
            ohlcv_sha = digest_file(source)
            signal_sha = digest_file(signal)

            can_resume = (
                not args.reset_checkpoints
                and valid_checkpoint(
                    checkpoint, event_file,
                    ohlcv_sha, signal_sha,
                    current_policy_hash,
                )
            )

            if can_resume:
                with open(
                    checkpoint, encoding="utf-8"
                ) as handle:
                    saved = json.load(handle)
                resumed += 1
                episodes_total += int(saved["episodes"])
            else:
                events, episodes, rows = evaluate_pair(
                    tf, market, source, signal,
                    global_start, boundary1,
                    boundary2, global_end,
                )

                atomic_csv(
                    event_file, events, EVENT_COLUMNS
                )

                atomic_json(checkpoint, {
                    "complete": True,
                    "version": VERSION,
                    "policy_sha256": current_policy_hash,
                    "ohlcv_sha256": ohlcv_sha,
                    "signal_sha256": signal_sha,
                    "event_sha256": digest_file(event_file),
                    "episodes": episodes,
                    "source_rows": rows,
                })

                episodes_total += episodes
                processed += 1

            with open(
                event_file, "r",
                encoding="utf-8", newline=""
            ) as handle:
                reader = csv.DictReader(handle)
                all_events.extend(reader)

            if index % 10 == 0 or index == len(pairs):
                print(
                    "[PROGRESS]", index, "/", len(pairs),
                    "new=", processed,
                    "resumed=", resumed,
                    "events=", len(all_events),
                    flush=True,
                )

        except Exception as exc:
            message = (
                tf + "/" + market + ":"
                + type(exc).__name__ + ":" + str(exc)
            )
            all_errors.append(message)
            print("[ERROR]", message, flush=True)

    summary, reasons = summarize(all_events)

    # Every report is regenerated from verified checkpoint
    # events. Old summary files are never trusted.
    atomic_csv(
        out / "oos_events.csv",
        all_events, EVENT_COLUMNS,
    )
    atomic_csv(
        out / "oos_summary.csv",
        summary, SUMMARY_COLUMNS,
    )

    result = {
        "audit": "pattern_256_chronological_oos",
        "version": VERSION,
        "policy_id": POLICY_ID,
        "policy_sha256": current_policy_hash,
        "execution": "PASS" if not all_errors else "FAIL",
        "classification": "PRELIMINARY_CHRONOLOGICAL_EVALUATION",
        "input_pairs": len(pairs),
        "source_rows": total_source_rows,
        "new_pairs": processed,
        "resumed_pairs": resumed,
        "signal_episodes": episodes_total,
        "event_rows": len(all_events),
        "calendar_boundaries_utc": {
            "start": global_start.isoformat(),
            "dev_end_exclusive": boundary1.isoformat(),
            "validation_end_exclusive": boundary2.isoformat(),
            "end": global_end.isoformat(),
        },
        "summary": summary,
        "exclusion_reasons": reasons,
        "errors": all_errors,
        "independent_oos_proven": False,
        "point_in_time_proven": False,
        "profitability_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
        "limitations": [
            "Current OHLCV snapshots, not historical snapshots",
            "Signal causal availability not independently proven",
            "TEST is exposed by this evaluation",
            "Repeated nonconsecutive episodes can overlap",
            "Intrabar high does not prove executable fill",
            "No fees, slippage, or market impact",
            "Calendar splits are not a purged ML validation",
            "Hit rate is not profitability",
        ],
    }

    atomic_json(out / "oos_result.json", result)

    report_lines = [
        "# Pattern 256 Chronological OOS",
        "",
        "- Version: " + VERSION,
        "- Execution: " + result["execution"],
        "- Research decision: HOLD",
        "- Signal episodes: " + str(episodes_total),
        "- Event rows: " + str(len(all_events)),
        "- New pairs: " + str(processed),
        "- Resumed pairs: " + str(resumed),
        "- Errors: " + str(len(all_errors)),
        "",
        "## Chronological results",
        "",
        "| TF | Split | Horizon | HIT | MISS | Hit rate % | Excluded | Censored |",
        "|---|---|---:|---:|---:|---:|---:|---:|",
    ]

    for item in summary:
        report_lines.append(
            "| {timeframe} | {split} | {horizon} | "
            "{hits} | {misses} | {hit_rate_pct} | "
            "{excluded} | {censored} |".format(**item)
        )

    report_lines.extend([
        "",
        "## Limitations",
        "",
    ])

    for limitation in result["limitations"]:
        report_lines.append("- " + limitation)

    atomic_text(
        out / "oos_report.md",
        "\n".join(report_lines) + "\n",
    )

    manifest = {
        "version": VERSION,
        "policy_sha256": current_policy_hash,
        "files": {},
    }

    for name in [
        "oos_result.json",
        "oos_summary.csv",
        "oos_events.csv",
        "oos_report.md",
    ]:
        manifest["files"][name] = digest_file(out / name)

    atomic_json(out / "oos_manifest.json", manifest)

    print(
        json.dumps(
            {
                "execution": result["execution"],
                "pairs": len(pairs),
                "episodes": episodes_total,
                "events": len(all_events),
                "errors": len(all_errors),
                "research_decision": "HOLD",
            },
            indent=2,
        ),
        flush=True,
    )

    return 0 if not all_errors else 1


if __name__ == "__main__":
    sys.exit(main())
