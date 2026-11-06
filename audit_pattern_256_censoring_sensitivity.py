
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Censoring Sensitivity Audit
Clean V001

Input:
 data/reports/pattern_256_event_gap_integrity/
   gap_integrity_events.csv
   gap_integrity_summary.csv
   gap_integrity_result.json
   gap_integrity_manifest.json

Outputs:
 data/reports/pattern_256_censoring_sensitivity/
   censoring_result.json
   censoring_summary.csv
   censoring_market.csv
   censoring_manifest.json

Research only. No trading or training approval.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


VERSION = "Clean V001"
EXPECTED_EVENTS = 252600
EXPECTED_JOBS = 870

PERIODS = {
    "DEVELOPMENT",
    "VALIDATION",
    "RETROSPECTIVE_TEST",
}
TIMEFRAMES = {"h1", "h4", "d1"}
HORIZONS = {24, 72, 168}
OUTCOMES = {
    "HIT",
    "MISS",
    "GAP_CENSORED",
    "END_CENSORED",
}

SUMMARY_FIELDS = [
    "timeframe",
    "period",
    "horizon_hours",
    "events",
    "hits",
    "misses",
    "gap_censored",
    "end_censored",
    "censored_total",
    "censoring_rate_pct",
    "pessimistic_hit_rate_pct",
    "observed_hit_rate_pct",
    "optimistic_hit_rate_pct",
    "uncertainty_width_pp",
    "status",
]

MARKET_FIELDS = [
    "timeframe",
    "market",
    "events",
    "hits",
    "misses",
    "gap_censored",
    "end_censored",
    "censored_total",
    "censoring_rate_pct",
    "pessimistic_hit_rate_pct",
    "observed_hit_rate_pct",
    "optimistic_hit_rate_pct",
]


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def digest(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for block in iter(
            lambda: f.read(1024 * 1024),
            b"",
        ):
            h.update(block)
    return h.hexdigest()


def read_json(path):
    with Path(path).open(
        "r", encoding="utf-8-sig"
    ) as f:
        return json.load(f)


def write_json(path, obj):
    path = Path(path)
    path.parent.mkdir(
        parents=True, exist_ok=True
    )
    temp = path.with_name(path.name + ".tmp")

    with temp.open("w", encoding="utf-8") as f:
        json.dump(
            obj,
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


def pct(numerator, denominator):
    if denominator == 0:
        return None
    return round(
        numerator * 100.0 / denominator,
        4,
    )


def counts_from_series(series):
    counts = series.value_counts()

    return {
        name: int(counts.get(name, 0))
        for name in sorted(OUTCOMES)
    }


def calculate_metrics(counts):
    hits = counts["HIT"]
    misses = counts["MISS"]
    gap = counts["GAP_CENSORED"]
    end = counts["END_CENSORED"]

    censored = gap + end
    observed = hits + misses
    total = observed + censored

    require(total > 0, "Empty metric group")

    lower = pct(hits, total)
    middle = pct(hits, observed)
    upper = pct(hits + censored, total)

    return {
        "events": total,
        "hits": hits,
        "misses": misses,
        "gap_censored": gap,
        "end_censored": end,
        "censored_total": censored,
        "censoring_rate_pct": pct(
            censored, total
        ),
        "pessimistic_hit_rate_pct": lower,
        "observed_hit_rate_pct": middle,
        "optimistic_hit_rate_pct": upper,
        "uncertainty_width_pp": round(
            upper - lower, 4
        ),
    }


def verify_inputs(folder):
    names = [
        "gap_integrity_events.csv",
        "gap_integrity_summary.csv",
        "gap_integrity_result.json",
    ]

    manifest_path = (
        folder / "gap_integrity_manifest.json"
    )

    require(
        manifest_path.is_file(),
        "Missing gap integrity manifest",
    )

    manifest = read_json(manifest_path)

    require(
        manifest.get("execution") == "PASS",
        "Input manifest not PASS",
    )
    require(
        manifest.get("version") == "Clean V002",
        "Unexpected input version",
    )

    hashes = manifest.get(
        "output_sha256", {}
    )

    require(
        set(hashes) == set(names),
        "Input manifest file set mismatch",
    )

    for name in names:
        path = folder / name

        require(
            path.is_file(),
            f"Missing input: {name}",
        )

        require(
            digest(path)
            == str(hashes[name]).lower(),
            f"Input SHA256 mismatch: {name}",
        )

    result = read_json(
        folder / "gap_integrity_result.json"
    )

    checks = {
        "execution": "PASS",
        "version": "Clean V002",
        "jobs": EXPECTED_JOBS,
        "completed_jobs": EXPECTED_JOBS,
        "failed_jobs": 0,
        "audited_event_records": EXPECTED_EVENTS,
        "gap_integrity_audit_completed": True,
        "independent_oos_proven": False,
        "research_decision": "HOLD",
    }

    for key, expected in checks.items():
        require(
            result.get(key) == expected,
            f"Input result mismatch: {key}",
        )

    return result, digest(manifest_path)


def validate_events(df, result):
    required = {
        "timeframe",
        "market",
        "period",
        "signal_utc",
        "entry_utc",
        "horizon_hours",
        "original_outcome",
        "corrected_outcome",
        "observed_candles",
        "expected_candles",
        "missing_candles",
        "full_window_contiguous",
    }

    require(
        required.issubset(df.columns),
        "Missing event columns",
    )

    require(
        len(df) == EXPECTED_EVENTS,
        "Event count mismatch",
    )

    require(
        df["timeframe"].isin(
            TIMEFRAMES
        ).all(),
        "Unknown timeframe",
    )

    require(
        df["period"].isin(
            PERIODS
        ).all(),
        "Unknown period",
    )

    require(
        df["horizon_hours"].isin(
            HORIZONS
        ).all(),
        "Unknown horizon",
    )

    require(
        df["corrected_outcome"].isin(
            OUTCOMES
        ).all(),
        "Unknown outcome",
    )

    require(
        df["market"].notna().all()
        and df["market"].str.startswith(
            "KRW-"
        ).all(),
        "Invalid market",
    )

    require(
        df["entry_utc"].notna().all()
        and df["signal_utc"].notna().all(),
        "Missing event timestamps",
    )

    require(
        df["observed_candles"].notna().all()
        and df["expected_candles"].notna().all()
        and df["missing_candles"].notna().all(),
        "Missing candle counts",
    )

    require(
        (
            df["observed_candles"] >= 1
        ).all(),
        "Invalid observed candle count",
    )

    require(
        (
            df["expected_candles"]
            >= df["observed_candles"]
        ).all(),
        "Observed exceeds expected",
    )

    require(
        (
            df["missing_candles"]
            == df["expected_candles"]
            - df["observed_candles"]
        ).all(),
        "Missing candle count inconsistent",
    )

    counts = counts_from_series(
        df["corrected_outcome"]
    )

    expected_counts = {
        "HIT": "confirmed_hits",
        "MISS": "confirmed_misses",
        "GAP_CENSORED": "gap_censored",
        "END_CENSORED": "end_censored",
    }

    for outcome, key in expected_counts.items():
        require(
            counts[outcome]
            == int(result[key]),
            f"Outcome total mismatch: {outcome}",
        )

    require(
        sum(counts.values())
        == EXPECTED_EVENTS,
        "Outcome accounting mismatch",
    )

    # A fully continuous window cannot be censored.
    censored = df[
        "corrected_outcome"
    ].isin({
        "GAP_CENSORED",
        "END_CENSORED",
    })

    require(
        not (
            censored
            & df["full_window_contiguous"].eq(1)
        ).any(),
        "Censored event marked contiguous",
    )

    require(
        not (
            df["corrected_outcome"].eq("MISS")
            & df["full_window_contiguous"].ne(1)
        ).any(),
        "Confirmed miss has incomplete window",
    )

    # Check uniqueness within a given horizon.
    keys = [
        "timeframe",
        "market",
        "signal_utc",
        "entry_utc",
        "horizon_hours",
    ]

    require(
        not df.duplicated(keys).any(),
        "Duplicate event/horizon records",
    )

    return counts


def build_group_rows(df, columns):
    rows = []

    for key, group in df.groupby(
        columns, sort=True
    ):
        if not isinstance(key, tuple):
            key = (key,)

        dimensions = dict(
            zip(columns, key)
        )

        metrics = calculate_metrics(
            counts_from_series(
                group["corrected_outcome"]
            )
        )

        rows.append({
            **dimensions,
            **metrics,
        })

    return rows


def run(root):
    root = Path(root).resolve()

    source = (
        root
        / "data/reports/pattern_256_event_gap_integrity"
    )

    result, source_manifest_hash = (
        verify_inputs(source)
    )

    events_path = (
        source / "gap_integrity_events.csv"
    )

    print(
        "[INFO] Reading verified event records...",
        flush=True,
    )

    df = pd.read_csv(
        events_path,
        encoding="utf-8-sig",
        dtype={
            "timeframe": str,
            "market": str,
            "period": str,
            "signal_utc": str,
            "entry_utc": str,
            "original_outcome": str,
            "corrected_outcome": str,
        },
        low_memory=False,
    )

    overall_counts = validate_events(
        df, result
    )

    overall = calculate_metrics(
        overall_counts
    )

    summary = build_group_rows(
        df,
        [
            "timeframe",
            "period",
            "horizon_hours",
        ],
    )

    require(
        len(summary) == 27,
        "Expected 27 period/horizon groups",
    )

    for row in summary:
        # This status describes only the width of
        # uncertainty due to censored observations.
        width = row["uncertainty_width_pp"]

        if width == 0:
            row["status"] = "NO_CENSORING"
        elif width <= 1:
            row["status"] = "LOW_CENSORING"
        elif width <= 5:
            row["status"] = "MATERIAL_CENSORING"
        else:
            row["status"] = "HIGH_CENSORING"

    markets = build_group_rows(
        df,
        ["timeframe", "market"],
    )

    require(
        len(markets) <= EXPECTED_JOBS,
        "Unexpected market count",
    )

    markets.sort(
        key=lambda r: (
            -r["censored_total"],
            -r["censoring_rate_pct"],
            r["timeframe"],
            r["market"],
        )
    )

    period_rows = build_group_rows(
        df,
        ["period"],
    )

    timeframe_rows = build_group_rows(
        df,
        ["timeframe"],
    )

    require(
        sum(r["events"] for r in summary)
        == EXPECTED_EVENTS,
        "Summary total mismatch",
    )

    require(
        sum(r["events"] for r in markets)
        == EXPECTED_EVENTS,
        "Market total mismatch",
    )

    require(
        sum(r["events"] for r in period_rows)
        == EXPECTED_EVENTS,
        "Period total mismatch",
    )

    output = (
        root
        / "data/reports/pattern_256_censoring_sensitivity"
    )

    summary_path = (
        output / "censoring_summary.csv"
    )
    market_path = (
        output / "censoring_market.csv"
    )
    result_path = (
        output / "censoring_result.json"
    )
    manifest_path = (
        output / "censoring_manifest.json"
    )

    write_csv(
        summary_path,
        SUMMARY_FIELDS,
        summary,
    )

    write_csv(
        market_path,
        MARKET_FIELDS,
        markets,
    )

    output_result = {
        "audit": "pattern_256_censoring_sensitivity",
        "version": VERSION,
        "execution": "PASS",
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "source_version": "Clean V002",
        "source_manifest_sha256": source_manifest_hash,
        "audited_event_records": EXPECTED_EVENTS,
        "group_count": len(summary),
        "market_timeframe_groups": len(markets),
        "overall": overall,
        "by_period": period_rows,
        "by_timeframe": timeframe_rows,
        "highest_censoring_markets": markets[:20],
        "independent_oos_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
        "interpretation": {
            "pessimistic": (
                "Treat all censored events as misses."
            ),
            "observed": (
                "Exclude censored events from denominator."
            ),
            "optimistic": (
                "Treat all censored events as hits."
            ),
            "warning": (
                "These are deterministic bounds, "
                "not confidence intervals. "
                "The observed rate may be biased "
                "when censoring is informative."
            ),
        },
        "limitations": [
            "The three horizons share underlying signals.",
            "Events may be dependent within markets.",
            "No independent OOS period is proven.",
            "No profitability or fillability is proven.",
            "The optimistic bound is not a forecast.",
            "Censoring may be nonrandom.",
            "Source labels are not recalculated.",
        ],
    }

    write_json(
        result_path,
        output_result,
    )

    script_hash = digest(
        Path(__file__).resolve()
    )

    write_json(
        manifest_path,
        {
            "audit": output_result["audit"],
            "version": VERSION,
            "execution": "PASS",
            "script_sha256": script_hash,
            "source_manifest_sha256": source_manifest_hash,
            "output_sha256": {
                summary_path.name: digest(
                    summary_path
                ),
                market_path.name: digest(
                    market_path
                ),
                result_path.name: digest(
                    result_path
                ),
            },
        },
    )

    print(
        json.dumps(
            {
                "execution": "PASS",
                "events": EXPECTED_EVENTS,
                "groups": len(summary),
                "market_groups": len(markets),
                "overall": overall,
                "research_decision": "HOLD",
            },
            indent=2,
            ensure_ascii=False,
        ),
        flush=True,
    )

    return 0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--root",
        default=".",
    )

    args = parser.parse_args()

    try:
        return run(args.root)

    except KeyboardInterrupt:
        print(
            "[STOP] Interrupted",
            file=sys.stderr,
        )
        return 130

    except Exception as exc:
        print(
            f"[FAIL] {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
