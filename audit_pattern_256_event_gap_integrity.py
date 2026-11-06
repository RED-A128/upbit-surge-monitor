
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Event Gap Integrity Audit
Clean V002

Purpose:
- Recheck all 252,600 retrospective event records.
- Verify UTC candle continuity.
- Confirm +30% hits only when observable before a gap.
- Separate confirmed misses from censored observations.
- Preserve source files.
- Support SHA256-verified checkpoint/resume.

No training, trading, or independent OOS approval.

Usage:
    py audit_pattern_256_event_gap_integrity.py --root .
    py audit_pattern_256_event_gap_integrity.py --root . --force
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd


VERSION = "Clean V002"
STEPS = {"h1": 1, "h4": 4, "d1": 24}
HORIZONS = (24, 72, 168)
TARGET = 1.30

EVENT_FIELDS = [
    "timeframe",
    "market",
    "period",
    "signal_utc",
    "entry_utc",
    "horizon_hours",
    "original_outcome",
    "corrected_outcome",
    "first_gap_utc",
    "hours_to_target",
    "observed_candles",
    "expected_candles",
    "missing_candles",
    "full_window_contiguous",
    "original_outcome_agrees",
]

SUMMARY_FIELDS = [
    "timeframe",
    "period",
    "horizon_hours",
    "events",
    "confirmed_hits",
    "confirmed_misses",
    "gap_censored",
    "end_censored",
    "confirmed_hit_rate_pct",
    "original_hit_rate_pct",
    "original_outcome_disagreements",
]


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def sha256(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for block in iter(
            lambda: f.read(1024 * 1024),
            b"",
        ):
            h.update(block)
    return h.hexdigest()


def canonical_hash(data):
    raw = json.dumps(
        data,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def read_json(path):
    with Path(path).open(
        "r", encoding="utf-8-sig"
    ) as f:
        return json.load(f)


def write_json(path, data):
    path = Path(path)
    path.parent.mkdir(
        parents=True, exist_ok=True
    )
    temp = path.with_name(path.name + ".tmp")

    with temp.open(
        "w", encoding="utf-8"
    ) as f:
        json.dump(
            data,
            f,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            allow_nan=False,
        )
        f.write("\n")

    os.replace(temp, path)


def write_csv(path, fields, rows):
    path = Path(path)
    path.parent.mkdir(
        parents=True, exist_ok=True
    )
    temp = path.with_name(path.name + ".tmp")

    with temp.open(
        "w",
        encoding="utf-8-sig",
        newline="",
    ) as f:
        writer = csv.DictWriter(
            f, fieldnames=fields
        )
        writer.writeheader()
        writer.writerows(rows)

    os.replace(temp, path)


def utc_ns(value):
    """Parse both timezone-aware and naive UTC strings."""
    timestamp = pd.to_datetime(
        value, utc=True, errors="raise"
    )
    require(
        not pd.isna(timestamp),
        "Invalid UTC timestamp",
    )
    return np.datetime64(
        timestamp.tz_localize(None).to_datetime64(),
        "ns",
    )


def utc_string(value):
    return (
        pd.Timestamp(value)
        .tz_localize("UTC")
        .isoformat()
    )


def verify_previous(root):
    folder = (
        root
        / "data/reports/pattern_256_trigger_event_study"
    )

    names = [
        "event_study_result.json",
        "event_study_manifest.json",
        "event_study_files.csv",
        "event_study_events.csv",
    ]

    for name in names:
        require(
            (folder / name).is_file(),
            f"Missing previous report: {name}",
        )

    result = read_json(
        folder / "event_study_result.json"
    )
    manifest = read_json(
        folder / "event_study_manifest.json"
    )

    require(
        result.get("execution") == "PASS",
        "Previous execution not PASS",
    )
    require(
        result.get("jobs") == 870,
        "Previous jobs not 870",
    )
    require(
        result.get("completed_jobs") == 870,
        "Previous jobs incomplete",
    )
    require(
        result.get("failed_jobs") == 0,
        "Previous jobs failed",
    )
    require(
        result.get("event_records") == 252600,
        "Previous event count changed",
    )
    require(
        manifest.get("execution") == "PASS",
        "Previous manifest not PASS",
    )

    hashes = manifest.get(
        "output_sha256", {}
    )

    expected = {
        "event_study_result.json",
        "event_study_files.csv",
        "event_study_events.csv",
    }

    require(
        set(hashes) == expected,
        "Previous manifest output mismatch",
    )

    for name in expected:
        actual = sha256(folder / name)
        require(
            actual == str(hashes[name]).lower(),
            f"Previous SHA256 mismatch: {name}",
        )

    inventory = {}

    with (
        folder / "event_study_files.csv"
    ).open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as f:
        for row in csv.DictReader(f):
            key = (
                row["timeframe"],
                row["market"],
            )
            require(
                key not in inventory,
                f"Duplicate previous job: {key}",
            )
            require(
                row["status"] == "PASS",
                f"Previous job failed: {key}",
            )
            inventory[key] = row

    require(
        len(inventory) == 870,
        "Previous inventory incomplete",
    )

    return (
        folder,
        inventory,
        sha256(
            folder / "event_study_manifest.json"
        ),
    )


def load_source(path, market):
    df = pd.read_csv(
        path,
        encoding="utf-8-sig",
        usecols=[
            "market",
            "candle_date_time_utc",
            "open",
            "high",
            "low",
        ],
        low_memory=False,
    )

    require(
        len(df) > 0,
        f"Empty source: {path}",
    )
    require(
        df["market"].notna().all()
        and df["market"].astype(str).eq(market).all(),
        f"Market mismatch: {path}",
    )

    timestamps = pd.to_datetime(
        df["candle_date_time_utc"],
        utc=True,
        errors="coerce",
    )

    require(
        timestamps.notna().all(),
        f"Invalid timestamps: {path}",
    )
    require(
        timestamps.is_monotonic_increasing,
        f"Unsorted timestamps: {path}",
    )
    require(
        not timestamps.duplicated().any(),
        f"Duplicate timestamps: {path}",
    )

    times = timestamps.to_numpy(
        dtype="datetime64[ns]"
    )

    opens = pd.to_numeric(
        df["open"], errors="coerce"
    ).to_numpy(dtype=float)

    highs = pd.to_numeric(
        df["high"], errors="coerce"
    ).to_numpy(dtype=float)

    lows = pd.to_numeric(
        df["low"], errors="coerce"
    ).to_numpy(dtype=float)

    require(
        np.isfinite(opens).all()
        and np.isfinite(highs).all()
        and np.isfinite(lows).all()
        and (opens > 0).all()
        and (highs >= lows).all(),
        f"Invalid OHLC prices: {path}",
    )

    return times, opens, highs


def audit_event(
    row,
    times,
    opens,
    highs,
    step_hours,
):
    step = np.timedelta64(
        step_hours, "h"
    )
    hour = np.timedelta64(1, "h")

    signal_time = utc_ns(
        row["signal_utc"]
    )
    entry_time = utc_ns(
        row["entry_utc"]
    )

    horizon = int(
        row["horizon_hours"]
    )

    require(
        horizon in HORIZONS,
        "Invalid horizon",
    )
    require(
        horizon % step_hours == 0,
        "Invalid timeframe/horizon",
    )
    require(
        entry_time - signal_time == step,
        "Entry is not next regular candle",
    )

    entry_index = int(
        np.searchsorted(times, entry_time)
    )

    require(
        entry_index < len(times)
        and times[entry_index] == entry_time,
        "Entry candle not found",
    )

    entry_price = float(
        row["entry_price"]
    )

    require(
        np.isfinite(entry_price)
        and entry_price > 0,
        "Invalid entry price",
    )
    require(
        np.isclose(
            opens[entry_index],
            entry_price,
            rtol=1e-10,
            atol=1e-10,
        ),
        "Entry price mismatch",
    )

    expected_count = (
        horizon // step_hours
    )

    deadline = (
        entry_time
        + np.timedelta64(horizon, "h")
    )

    last_open = deadline - step

    right = int(
        np.searchsorted(
            times,
            last_open,
            side="right",
        )
    )

    observed_times = times[
        entry_index:right
    ]
    observed_highs = highs[
        entry_index:right
    ]

    require(
        len(observed_times) > 0,
        "No observation candles",
    )
    require(
        len(observed_times) <= expected_count,
        "Unexpected excess candles",
    )

    # Every observed candle must lie on the
    # expected timeframe grid.
    elapsed = (
        observed_times - entry_time
    ) / step

    require(
        np.all(
            elapsed == np.floor(elapsed)
        ),
        "Off-grid candle timestamp",
    )

    # First continuous prefix from entry.
    if len(observed_times) == 1:
        prefix_count = 1
    else:
        differences = np.diff(
            observed_times
        )
        broken = np.flatnonzero(
            differences != step
        )

        prefix_count = (
            int(broken[0]) + 1
            if len(broken)
            else len(observed_times)
        )

    contiguous = (
        prefix_count == expected_count
    )

    first_gap = (
        None
        if contiguous
        else (
            entry_time
            + prefix_count * step
        )
    )

    prefix_highs = observed_highs[
        :prefix_count
    ]

    hit_indices = np.flatnonzero(
        prefix_highs
        >= entry_price * TARGET
    )

    if len(hit_indices):
        hit_index = int(
            hit_indices[0]
        )

        corrected = "HIT"

        hit_close = (
            entry_time
            + (hit_index + 1) * step
        )

        hours_to_target = float(
            (hit_close - entry_time)
            / hour
        )

    elif contiguous:
        corrected = "MISS"
        hours_to_target = ""

    elif first_gap <= times[-1]:
        corrected = "GAP_CENSORED"
        hours_to_target = ""

    else:
        corrected = "END_CENSORED"
        hours_to_target = ""

    original = str(
        row["outcome"]
    ).strip()

    require(
        original in {
            "HIT", "MISS", "CENSORED"
        },
        "Invalid original outcome",
    )

    # Compare classifications only where
    # both are definitive.
    if corrected in {"HIT", "MISS"}:
        agrees = int(
            corrected == original
        )
    else:
        agrees = ""

    missing_count = (
        expected_count
        - len(observed_times)
    )

    return {
        "timeframe": row["timeframe"],
        "market": row["market"],
        "period": row["period"],
        "signal_utc": row["signal_utc"],
        "entry_utc": row["entry_utc"],
        "horizon_hours": horizon,
        "original_outcome": original,
        "corrected_outcome": corrected,
        "first_gap_utc": (
            utc_string(first_gap)
            if first_gap is not None
            else ""
        ),
        "hours_to_target": hours_to_target,
        "observed_candles": len(
            observed_times
        ),
        "expected_candles": expected_count,
        "missing_candles": missing_count,
        "full_window_contiguous": int(
            contiguous
        ),
        "original_outcome_agrees": agrees,
    }


def checkpoint_path(folder, tf, market):
    return (
        folder
        / f"{tf}_{market}.json"
    )


def read_checkpoint(
    path,
    tf,
    market,
    source_hash,
    previous_hash,
    script_hash,
    expected_count,
):
    if not path.is_file():
        return None

    try:
        wrapper = read_json(path)
        payload = wrapper["payload"]

        require(
            wrapper.get("payload_sha256")
            == canonical_hash(payload),
            "Checkpoint payload hash mismatch",
        )

        required = {
            "version": VERSION,
            "timeframe": tf,
            "market": market,
            "source_sha256": source_hash,
            "previous_manifest_sha256": previous_hash,
            "script_sha256": script_hash,
            "status": "PASS",
        }

        for key, value in required.items():
            require(
                payload.get(key) == value,
                f"Checkpoint field mismatch: {key}",
            )

        rows = payload["rows"]

        require(
            isinstance(rows, list),
            "Invalid checkpoint rows",
        )
        require(
            len(rows) == expected_count,
            "Checkpoint row count mismatch",
        )
        require(
            all(
                set(EVENT_FIELDS).issubset(r)
                for r in rows
            ),
            "Checkpoint schema mismatch",
        )

        return rows

    except (
        OSError,
        ValueError,
        KeyError,
        TypeError,
        RuntimeError,
    ):
        return None


def save_checkpoint(
    path,
    tf,
    market,
    source_hash,
    previous_hash,
    script_hash,
    rows,
):
    payload = {
        "version": VERSION,
        "timeframe": tf,
        "market": market,
        "source_sha256": source_hash,
        "previous_manifest_sha256": previous_hash,
        "script_sha256": script_hash,
        "status": "PASS",
        "rows": rows,
    }

    write_json(
        path,
        {
            "payload": payload,
            "payload_sha256": canonical_hash(
                payload
            ),
        },
    )


def build_summary(rows):
    groups = defaultdict(list)

    for row in rows:
        key = (
            row["timeframe"],
            row["period"],
            int(row["horizon_hours"]),
        )
        groups[key].append(row)

    result = []

    for key, items in sorted(
        groups.items()
    ):
        tf, period, horizon = key

        def count_outcome(name):
            return sum(
                r["corrected_outcome"] == name
                for r in items
            )

        hits = count_outcome("HIT")
        misses = count_outcome("MISS")
        gap = count_outcome(
            "GAP_CENSORED"
        )
        end = count_outcome(
            "END_CENSORED"
        )

        original_hits = sum(
            r["original_outcome"] == "HIT"
            for r in items
        )

        original_misses = sum(
            r["original_outcome"] == "MISS"
            for r in items
        )

        disagreements = sum(
            r["original_outcome_agrees"] == 0
            for r in items
        )

        known = hits + misses
        old_known = (
            original_hits
            + original_misses
        )

        result.append({
            "timeframe": tf,
            "period": period,
            "horizon_hours": horizon,
            "events": len(items),
            "confirmed_hits": hits,
            "confirmed_misses": misses,
            "gap_censored": gap,
            "end_censored": end,
            "confirmed_hit_rate_pct": (
                round(
                    hits / known * 100,
                    4,
                )
                if known else ""
            ),
            "original_hit_rate_pct": (
                round(
                    original_hits
                    / old_known * 100,
                    4,
                )
                if old_known else ""
            ),
            "original_outcome_disagreements": disagreements,
        })

    return result


def run(root, force=False):
    root = Path(root).resolve()

    previous_folder, inventory, previous_hash = (
        verify_previous(root)
    )

    script_hash = sha256(
        Path(__file__).resolve()
    )

    events_file = (
        previous_folder
        / "event_study_events.csv"
    )

    events = pd.read_csv(
        events_file,
        encoding="utf-8-sig",
        dtype={
            "timeframe": str,
            "market": str,
            "period": str,
            "signal_utc": str,
            "entry_utc": str,
            "outcome": str,
        },
        low_memory=False,
    )

    require(
        len(events) == 252600,
        "Unexpected event count",
    )

    required_columns = {
        "timeframe",
        "market",
        "period",
        "signal_utc",
        "entry_utc",
        "entry_price",
        "horizon_hours",
        "outcome",
    }

    require(
        required_columns.issubset(
            events.columns
        ),
        "Event CSV schema mismatch",
    )

    grouped = {
        key: group
        for key, group in events.groupby(
            ["timeframe", "market"],
            sort=True,
        )
    }

    require(
        set(grouped).issubset(
            set(inventory)
        ),
        "Unknown market/timeframe",
    )

    output = (
        root
        / "data/reports/pattern_256_event_gap_integrity"
    )

    checkpoints = (
        output / "checkpoints"
    )

    checkpoints.mkdir(
        parents=True, exist_ok=True
    )

    output_rows = []
    errors = []
    executed = 0
    resumed = 0
    completed = 0

    jobs = sorted(inventory)

    for index, (tf, market) in enumerate(
        jobs, 1
    ):
        try:
            require(
                tf in STEPS,
                f"Unknown timeframe: {tf}",
            )

            previous = inventory[
                (tf, market)
            ]

            group = grouped.get(
                (tf, market)
            )

            expected_count = int(
                previous["event_records"]
            )

            if group is None:
                require(
                    expected_count == 0,
                    "Missing event group",
                )
                group_rows = []
            else:
                group_rows = group.to_dict(
                    orient="records"
                )

            require(
                len(group_rows)
                == expected_count,
                "Event count mismatch",
            )

            source = (
                root
                / "data/ohlcv"
                / tf
                / f"{market}.csv"
            )

            require(
                source.is_file(),
                f"Missing source: {source}",
            )

            source_hash = sha256(
                source
            )

            require(
                source_hash
                == previous["ohlcv_sha256"],
                "Source changed since event study",
            )

            cp = checkpoint_path(
                checkpoints,
                tf,
                market,
            )

            cached = None

            if not force:
                cached = read_checkpoint(
                    cp,
                    tf,
                    market,
                    source_hash,
                    previous_hash,
                    script_hash,
                    expected_count,
                )

            if cached is not None:
                corrected_rows = cached
                resumed += 1

            else:
                corrected_rows = []

                if group_rows:
                    times, opens, highs = (
                        load_source(
                            source, market
                        )
                    )

                    require(
                        len(times)
                        == int(
                            previous["signal_rows"]
                        ),
                        "Source row count changed",
                    )

                    for row in group_rows:
                        corrected_rows.append(
                            audit_event(
                                row,
                                times,
                                opens,
                                highs,
                                STEPS[tf],
                            )
                        )

                require(
                    sha256(source)
                    == source_hash,
                    "Source changed during audit",
                )

                save_checkpoint(
                    cp,
                    tf,
                    market,
                    source_hash,
                    previous_hash,
                    script_hash,
                    corrected_rows,
                )

                executed += 1

            output_rows.extend(
                corrected_rows
            )

            completed += 1

        except Exception as exc:
            message = (
                f"{tf}/{market}: "
                f"{type(exc).__name__}: {exc}"
            )

            errors.append(message)

            print(
                f"[FAIL] {message}",
                flush=True,
            )

        if (
            index % 25 == 0
            or index == len(jobs)
        ):
            print(
                f"[PROGRESS] "
                f"{index}/{len(jobs)} "
                f"completed={completed} "
                f"executed={executed} "
                f"resumed={resumed} "
                f"errors={len(errors)}",
                flush=True,
            )

    success = (
        completed == 870
        and not errors
        and len(output_rows) == 252600
    )

    summary = build_summary(
        output_rows
    )

    def outcome_count(name):
        return sum(
            r["corrected_outcome"] == name
            for r in output_rows
        )

    hits = outcome_count("HIT")
    misses = outcome_count("MISS")
    gap = outcome_count(
        "GAP_CENSORED"
    )
    end = outcome_count(
        "END_CENSORED"
    )

    require(
        hits + misses + gap + end
        == len(output_rows),
        "Outcome totals inconsistent",
    )

    result = {
        "audit": "pattern_256_event_gap_integrity",
        "version": VERSION,
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "execution": (
            "PASS"
            if success
            else "REVIEW_REQUIRED"
        ),
        "jobs": 870,
        "completed_jobs": completed,
        "executed_jobs": executed,
        "resumed_jobs": resumed,
        "failed_jobs": len(errors),
        "errors": errors[:100],
        "original_event_records": len(events),
        "audited_event_records": len(
            output_rows
        ),
        "confirmed_hits": hits,
        "confirmed_misses": misses,
        "gap_censored": gap,
        "end_censored": end,
        "original_outcome_disagreements": sum(
            r["original_outcome_agrees"] == 0
            for r in output_rows
        ),
        "summary": summary,
        "gap_integrity_audit_completed": success,
        "independent_oos_proven": False,
        "point_in_time_proven": False,
        "prediction_performance_verified": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
        "limitations": [
            "Retrospective research, not independent OOS.",
            "Missing candles may reflect no trading or data gaps.",
            "Gap-censored observations are not confirmed misses.",
            "High-price touches are not verified trade fills.",
            "No fees, spread, slippage or liquidity model.",
            "Market and timeframe dependence remains.",
            "Upstream feature causality is not independently proven.",
        ],
    }

    details_file = (
        output / "gap_integrity_events.csv"
    )
    summary_file = (
        output / "gap_integrity_summary.csv"
    )
    result_file = (
        output / "gap_integrity_result.json"
    )
    manifest_file = (
        output / "gap_integrity_manifest.json"
    )

    write_csv(
        details_file,
        EVENT_FIELDS,
        output_rows,
    )

    write_csv(
        summary_file,
        SUMMARY_FIELDS,
        summary,
    )

    write_json(
        result_file,
        result,
    )

    write_json(
        manifest_file,
        {
            "audit": result["audit"],
            "version": VERSION,
            "execution": result["execution"],
            "script_sha256": script_hash,
            "previous_manifest_sha256": previous_hash,
            "output_sha256": {
                details_file.name: sha256(
                    details_file
                ),
                summary_file.name: sha256(
                    summary_file
                ),
                result_file.name: sha256(
                    result_file
                ),
            },
        },
    )

    print(
        json.dumps(
            {
                "execution": result["execution"],
                "jobs": result["jobs"],
                "completed": completed,
                "executed": executed,
                "resumed": resumed,
                "failed": len(errors),
                "audited_events": len(
                    output_rows
                ),
                "confirmed_hits": hits,
                "confirmed_misses": misses,
                "gap_censored": gap,
                "end_censored": end,
                "research_decision": "HOLD",
            },
            ensure_ascii=False,
            indent=2,
        ),
        flush=True,
    )

    return 0 if success else 1


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Pattern 256 event gap integrity audit"
        )
    )

    parser.add_argument(
        "--root",
        default=".",
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help="Ignore existing checkpoints",
    )

    args = parser.parse_args()

    try:
        return run(
            Path(args.root),
            force=args.force,
        )

    except KeyboardInterrupt:
        print(
            "[STOP] Interrupted",
            file=sys.stderr,
        )
        return 130

    except Exception as exc:
        print(
            f"[FAIL] "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
