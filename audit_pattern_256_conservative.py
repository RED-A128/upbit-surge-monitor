
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Conservative Audit
Clean V002

- Previous-only matched controls
- Same market, timeframe and calendar split
- No overlapping signal/control evaluation windows
  within each market
- No future OHLCV used for selection
- Read-only source data
- Research decision always HOLD

This is a sensitivity audit, NOT independent OOS.
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

VERSION = "Clean V002"
STEPS = {"h1": 3600, "h4": 14400, "d1": 86400}
SPLITS = ("DEV", "VALIDATION", "TEST")
HORIZONS = (24, 72, 168)
MAX_HORIZON = 168
TARGET = 1.30

COLUMNS = [
    "timeframe", "split", "horizon",
    "matched_pairs", "signal_hits", "control_hits",
    "signal_rate_pct", "control_rate_pct",
    "difference_pp", "signal_only_hits",
    "control_only_hits", "both_hits", "neither_hits",
]


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(
            lambda: f.read(1024 * 1024), b""
        ):
            h.update(block)
    return h.hexdigest()


def atomic_text(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(
        prefix=".partial_", dir=str(path.parent)
    )
    try:
        with os.fdopen(
            fd, "w", encoding="utf-8", newline=""
        ) as f:
            f.write(value)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def save_json(path, data):
    atomic_text(
        path,
        json.dumps(
            data, indent=2, sort_keys=True,
            ensure_ascii=False
        ) + "\n"
    )


def save_csv(path, rows, fields):
    import io
    output = io.StringIO()
    writer = csv.DictWriter(
        output, fieldnames=fields,
        extrasaction="ignore"
    )
    writer.writeheader()
    writer.writerows(rows)
    atomic_text(path, output.getvalue())


def read_json(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def read_rows(path, required):
    with open(
        path, encoding="utf-8-sig", newline=""
    ) as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError(f"Missing CSV header: {path}")
        missing = set(required) - set(reader.fieldnames)
        if missing:
            raise ValueError(
                f"Missing columns {sorted(missing)}: {path}"
            )
        return list(reader)


def utc_seconds(value):
    dt = datetime.fromisoformat(
        str(value).replace("Z", "+00:00")
    )
    if dt.tzinfo is None:
        raise ValueError("Timezone missing")
    return int(
        dt.astimezone(timezone.utc).timestamp()
    )


def positive_float(value):
    v = float(value)
    if not math.isfinite(v) or v <= 0:
        raise ValueError("Invalid OHLCV price")
    return v


def boolean(value):
    v = str(value).strip().lower()
    if v not in ("0", "1", "false", "true"):
        raise ValueError(
            "Invalid signal_trigger: " + v
        )
    return v in ("1", "true")


def split_of(timestamp, bounds):
    start, dev_end, validation_end, end = bounds
    if timestamp < start or timestamp > end:
        return None
    if timestamp < dev_end:
        return "DEV"
    if timestamp < validation_end:
        return "VALIDATION"
    return "TEST"


def read_market(tf, market, source, signals):
    p = read_rows(
        source,
        ["market", "candle_date_time_utc",
         "open", "high", "low", "close"]
    )
    s = read_rows(
        signals,
        ["market", "candle_date_time_utc",
         "signal_trigger", "signal_stage"]
    )

    if len(p) != len(s):
        raise ValueError(
            f"{market} {tf}: row count mismatch"
        )

    times = []
    opens = []
    highs = []
    triggers = []
    stages = []

    for a, b in zip(p, s):
        if a["market"] != market:
            raise ValueError("OHLCV market mismatch")
        if b["market"] != market:
            raise ValueError("Signal market mismatch")

        ta = utc_seconds(a["candle_date_time_utc"])
        tb = utc_seconds(b["candle_date_time_utc"])

        if ta != tb:
            raise ValueError("Timestamp mismatch")
        if times and ta <= times[-1]:
            raise ValueError("Non-increasing timestamps")

        op = positive_float(a["open"])
        hi = positive_float(a["high"])
        lo = positive_float(a["low"])
        cl = positive_float(a["close"])

        if lo > min(op, cl) or hi < max(op, cl):
            raise ValueError("Invalid OHLCV relationship")

        times.append(ta)
        opens.append(op)
        highs.append(hi)
        triggers.append(boolean(b["signal_trigger"]))
        stages.append(
            str(b["signal_stage"]).strip().upper()
        )

    return times, opens, highs, triggers, stages


def gap_prefix(times, seconds):
    result = [0] * len(times)
    for i in range(1, len(times)):
        result[i] = result[i - 1] + int(
            times[i] - times[i - 1] != seconds
        )
    return result


def window_valid(i, times, gaps, bounds):
    end = i + MAX_HORIZON

    if end >= len(times):
        return False

    group = split_of(times[i], bounds)
    if group is None:
        return False

    if split_of(times[i + 1], bounds) != group:
        return False

    if split_of(times[end], bounds) != group:
        return False

    return gaps[end] == gaps[i]


def is_hit(i, horizon, opens, highs):
    entry = i + 1
    return max(
        highs[entry:entry + horizon]
    ) >= opens[entry] * TARGET


def rank(tf, market, timestamp):
    value = f"{tf}|{market}|{timestamp}"
    return hashlib.sha256(
        value.encode("utf-8")
    ).hexdigest()


def new_counts():
    return {
        "pairs": 0,
        "signal_hits": 0,
        "control_hits": 0,
        "signal_only": 0,
        "control_only": 0,
        "both": 0,
        "neither": 0,
    }


def new_diagnostics():
    return {
        "episodes_found": 0,
        "episodes_sampled": 0,
        "signal_window_invalid": 0,
        "no_prior_control": 0,
        "overlap_excluded": 0,
        "matched_pairs": 0,
    }


def intersects(a_start, a_end, b_start, b_end):
    return a_start <= b_end and b_start <= a_end


def overlaps_any(start, end, reserved):
    return any(
        intersects(start, end, a, b)
        for a, b in reserved
    )


def evaluate_market(
    tf, market, source, signal_path, bounds,
    sample_limit, distance_seconds,
    stats, diagnostics
):
    (
        times, opens, highs,
        triggers, stages
    ) = read_market(
        tf, market, source, signal_path
    )

    if not times:
        return 0

    gaps = gap_prefix(times, STEPS[tf])
    episodes = defaultdict(list)
    controls = defaultdict(list)

    for i in range(len(times) - 1):
        group = split_of(times[i], bounds)
        if group is None:
            continue

        if triggers[i] and (
            i == 0 or not triggers[i - 1]
        ):
            episodes[group].append(i)

        if (
            not triggers[i]
            and stages[i] != "TRIGGER"
        ):
            controls[group].append(i)

    for group in SPLITS:
        d = diagnostics[(tf, group)]
        d["episodes_found"] += len(episodes[group])

        # Deterministic sampling, as in V001.
        chosen = sorted(
            episodes[group],
            key=lambda i: rank(
                tf, market, times[i]
            )
        )[:sample_limit]

        d["episodes_sampled"] += len(chosen)

        # Chronological processing is required for
        # a conservative non-overlap reservation.
        chosen.sort()

        valid_controls = [
            i for i in controls[group]
            if window_valid(
                i, times, gaps, bounds
            )
        ]

        reserved = []

        for i in chosen:
            if not window_valid(
                i, times, gaps, bounds
            ):
                d["signal_window_invalid"] += 1
                continue

            signal_start = i + 1
            signal_end = i + MAX_HORIZON

            if overlaps_any(
                signal_start, signal_end, reserved
            ):
                d["overlap_excluded"] += 1
                continue

            # Only controls whose entire outcome
            # window ends BEFORE the signal candle.
            latest_control = i - MAX_HORIZON - 1

            pos = bisect.bisect_right(
                valid_controls, latest_control
            ) - 1

            selected = None

            while pos >= 0:
                j = valid_controls[pos]
                pos -= 1

                if times[i] - times[j] > distance_seconds:
                    break

                control_start = j + 1
                control_end = j + MAX_HORIZON

                if overlaps_any(
                    control_start, control_end,
                    reserved
                ):
                    continue

                selected = j
                break

            if selected is None:
                d["no_prior_control"] += 1
                continue

            # Reserve both windows. No overlapping
            # windows for this market and split.
            reserved.append((
                selected + 1,
                selected + MAX_HORIZON
            ))
            reserved.append((
                signal_start, signal_end
            ))

            d["matched_pairs"] += 1

            for horizon in HORIZONS:
                a = is_hit(
                    i, horizon, opens, highs
                )
                b = is_hit(
                    selected, horizon, opens, highs
                )

                item = stats[(tf, group, horizon)]
                item["pairs"] += 1
                item["signal_hits"] += int(a)
                item["control_hits"] += int(b)

                if a and b:
                    item["both"] += 1
                elif a:
                    item["signal_only"] += 1
                elif b:
                    item["control_only"] += 1
                else:
                    item["neither"] += 1

    return len(times)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True)
    parser.add_argument(
        "--max-episodes-per-market-split",
        type=int, default=100
    )
    parser.add_argument(
        "--max-match-days",
        type=int, default=30
    )
    args = parser.parse_args()

    if args.max_episodes_per_market_split < 1:
        raise ValueError("Episode limit must be positive")
    if args.max_match_days < 1:
        raise ValueError("Match days must be positive")

    root = Path(args.root).resolve()
    reports = root / "data" / "reports"

    previous_dir = (
        reports / "pattern_256_matched_baseline"
    )
    previous_path = (
        previous_dir / "matched_result.json"
    )
    manifest_path = (
        previous_dir / "matched_manifest.json"
    )

    previous = read_json(previous_path)
    manifest = read_json(manifest_path)

    expected = manifest["files"]["matched_result.json"]

    if sha256(previous_path) != expected:
        raise RuntimeError(
            "Matched baseline SHA256 mismatch"
        )

    if previous["execution"] != "PASS":
        raise RuntimeError(
            "Previous evaluation did not PASS"
        )

    if previous["version"] != "Clean V001":
        raise RuntimeError(
            "Unexpected previous version"
        )

    if previous["research_decision"] != "HOLD":
        raise RuntimeError(
            "Previous research decision changed"
        )

    oos_path = (
        reports
        / "pattern_256_chronological_oos"
        / "oos_result.json"
    )
    oos = read_json(oos_path)

    boundaries = oos["calendar_boundaries_utc"]
    bounds = (
        utc_seconds(boundaries["start"]),
        utc_seconds(
            boundaries["dev_end_exclusive"]
        ),
        utc_seconds(
            boundaries["validation_end_exclusive"]
        ),
        utc_seconds(boundaries["end"]),
    )

    stats = defaultdict(new_counts)
    diagnostics = defaultdict(new_diagnostics)

    pairs = 0
    total_rows = 0

    for tf in STEPS:
        source_dir = root / "data" / "ohlcv" / tf
        signal_dir = (
            root / "data" / "signals" / "256" / tf
        )

        sources = {
            p.name: p
            for p in source_dir.glob("KRW-*.csv")
        }
        signals = {
            p.name: p
            for p in signal_dir.glob("KRW-*.csv")
        }

        if not sources or set(sources) != set(signals):
            raise RuntimeError(
                f"Input files mismatch: {tf}"
            )

        for filename in sorted(sources):
            market = filename[:-4]

            total_rows += evaluate_market(
                tf, market,
                sources[filename],
                signals[filename],
                bounds,
                args.max_episodes_per_market_split,
                args.max_match_days * 86400,
                stats,
                diagnostics,
            )

            pairs += 1

            if pairs % 25 == 0:
                print(
                    f"[PROGRESS] {pairs} / "
                    f"{previous['input_pairs']}",
                    flush=True
                )

    if pairs != previous["input_pairs"]:
        raise RuntimeError(
            "Input pair count changed"
        )

    if total_rows != previous["source_rows"]:
        raise RuntimeError(
            "Source row count changed"
        )

    comparisons = []

    for tf in STEPS:
        for group in SPLITS:
            for horizon in HORIZONS:
                s = stats[(tf, group, horizon)]
                n = s["pairs"]

                sr = (
                    100 * s["signal_hits"] / n
                    if n else None
                )
                cr = (
                    100 * s["control_hits"] / n
                    if n else None
                )

                comparisons.append({
                    "timeframe": tf,
                    "split": group,
                    "horizon": horizon,
                    "matched_pairs": n,
                    "signal_hits": s["signal_hits"],
                    "control_hits": s["control_hits"],
                    "signal_rate_pct": (
                        round(sr, 6)
                        if sr is not None else None
                    ),
                    "control_rate_pct": (
                        round(cr, 6)
                        if cr is not None else None
                    ),
                    "difference_pp": (
                        round(sr - cr, 6)
                        if sr is not None else None
                    ),
                    "signal_only_hits": s["signal_only"],
                    "control_only_hits": s["control_only"],
                    "both_hits": s["both"],
                    "neither_hits": s["neither"],
                })

    diagnostic_rows = []

    for tf in STEPS:
        for group in SPLITS:
            d = diagnostics[(tf, group)]
            diagnostic_rows.append({
                "timeframe": tf,
                "split": group,
                **d
            })

    output = (
        reports / "pattern_256_conservative_audit"
    )

    result = {
        "version": VERSION,
        "execution": "PASS",
        "classification":
            "CONSERVATIVE_MATCHING_SENSITIVITY_AUDIT",
        "input_pairs": pairs,
        "source_rows": total_rows,
        "previous_matched_result_sha256":
            sha256(previous_path),
        "max_match_days": args.max_match_days,
        "max_episodes_per_market_split":
            args.max_episodes_per_market_split,
        "entry": "next_consecutive_candle_open",
        "target_return": 0.30,
        "control_policy":
            "same_market_same_split_prior_only",
        "control_outcome_known_before_signal": True,
        "overlap_policy":
            "no_overlapping_168_candle_windows_per_market",
        "comparison": comparisons,
        "diagnostics": diagnostic_rows,
        "limitations": [
            "Previously exposed calendar TEST",
            "Not independent OOS",
            "No point-in-time market universe proof",
            "No volatility matching",
            "No fees or slippage",
            "Intrabar high does not prove execution",
            "Strong sample reduction expected",
            "Selection and historical data remain exploratory",
            "Prior control is not a simultaneous counterfactual",
        ],
        "independent_oos_proven": False,
        "point_in_time_proven": False,
        "profitability_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
    }

    save_json(
        output / "conservative_result.json",
        result
    )
    save_csv(
        output / "conservative_comparison.csv",
        comparisons,
        COLUMNS
    )
    save_csv(
        output / "conservative_diagnostics.csv",
        diagnostic_rows,
        [
            "timeframe", "split",
            "episodes_found", "episodes_sampled",
            "signal_window_invalid",
            "no_prior_control",
            "overlap_excluded", "matched_pairs",
        ]
    )

    lines = [
        "# Pattern 256 Conservative Audit",
        "",
        "Version: " + VERSION,
        "Execution: PASS",
        "Research decision: HOLD",
        "",
        "## TEST comparison",
        "",
        "| TF | Horizon | Pairs | Signal % | Control % | Difference pp |",
        "|---|---:|---:|---:|---:|---:|",
    ]

    for row in comparisons:
        if row["split"] != "TEST":
            continue

        lines.append(
            "| {timeframe} | {horizon} | "
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
        "- " + x
        for x in result["limitations"]
    )

    save_path = output / "conservative_report.md"
    atomic_text(
        save_path, "\n".join(lines) + "\n"
    )

    filenames = [
        "conservative_result.json",
        "conservative_comparison.csv",
        "conservative_diagnostics.csv",
        "conservative_report.md",
    ]

    save_json(
        output / "conservative_manifest.json",
        {
            "version": VERSION,
            "files": {
                name: sha256(output / name)
                for name in filenames
            },
        }
    )

    print(
        json.dumps(
            {
                "execution": "PASS",
                "input_pairs": pairs,
                "test_results": [
                    row for row in comparisons
                    if row["split"] == "TEST"
                ],
                "research_decision": "HOLD",
            },
            ensure_ascii=False,
            indent=2
        ),
        flush=True
    )


if __name__ == "__main__":
    main()
