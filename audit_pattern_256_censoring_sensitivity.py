
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Censoring Sensitivity Audit
Clean V002

Read-only input:
 data/reports/pattern_256_event_gap_integrity/
   gap_integrity_events.csv
   gap_integrity_summary.csv
   gap_integrity_result.json
   gap_integrity_manifest.json

Output:
 data/reports/pattern_256_censoring_sensitivity/
   censoring_result.json
   censoring_summary.csv
   censoring_market.csv
   censoring_manifest.json

Research only. Never approves training or trading.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


VERSION = "Clean V002"
SOURCE_VERSION = "Clean V002"
EXPECTED_EVENTS = 252600
EXPECTED_JOBS = 870

PERIODS = {
    "DEVELOPMENT",
    "VALIDATION",
    "RETROSPECTIVE_TEST",
}
TIMEFRAMES = {"h1", "h4", "d1"}
HORIZONS = {24, 72, 168}

OUTCOMES = (
    "HIT",
    "MISS",
    "GAP_CENSORED",
    "END_CENSORED",
)

METRIC_FIELDS = [
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
]

SUMMARY_FIELDS = [
    "timeframe",
    "period",
    "horizon_hours",
    *METRIC_FIELDS,
    "status",
]

MARKET_FIELDS = [
    "timeframe",
    "market",
    *METRIC_FIELDS,
]


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def sha256(path):
    h = hashlib.sha256()

    with Path(path).open("rb") as handle:
        for block in iter(
            lambda: handle.read(1024 * 1024),
            b"",
        ):
            h.update(block)

    return h.hexdigest()


def read_json(path):
    with Path(path).open(
        "r",
        encoding="utf-8-sig",
    ) as handle:
        return json.load(handle)


def atomic_json(path, data):
    path = Path(path)
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )
    temp = path.with_name(path.name + ".tmp")

    with temp.open(
        "w",
        encoding="utf-8",
    ) as handle:
        json.dump(
            data,
            handle,
            indent=2,
            ensure_ascii=False,
            sort_keys=True,
            allow_nan=False,
        )
        handle.write("\n")

    os.replace(temp, path)


def atomic_csv(path, columns, records):
    path = Path(path)
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )
    temp = path.with_name(path.name + ".tmp")

    with temp.open(
        "w",
        newline="",
        encoding="utf-8-sig",
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=columns,
            extrasaction="raise",
        )
        writer.writeheader()

        for index, record in enumerate(records):
            extra = set(record) - set(columns)
            missing = set(columns) - set(record)

            require(
                not extra and not missing,
                (
                    f"CSV schema mismatch "
                    f"at row {index}: "
                    f"extra={sorted(extra)}, "
                    f"missing={sorted(missing)}"
                ),
            )

            writer.writerow(record)

    os.replace(temp, path)


def percent(numerator, denominator):
    if denominator == 0:
        return None

    return round(
        100.0 * numerator / denominator,
        4,
    )


def count_outcomes(series):
    values = series.value_counts(dropna=False)

    return {
        name: int(values.get(name, 0))
        for name in OUTCOMES
    }


def metrics(counts):
    hits = counts["HIT"]
    misses = counts["MISS"]
    gap = counts["GAP_CENSORED"]
    end = counts["END_CENSORED"]

    censored = gap + end
    observed = hits + misses
    total = observed + censored

    require(total > 0, "Empty outcome group")

    lower = percent(hits, total)
    middle = percent(hits, observed)
    upper = percent(hits + censored, total)

    require(
        lower <= upper,
        "Invalid sensitivity bounds",
    )

    if middle is not None:
        require(
            lower <= middle <= upper,
            "Observed rate outside bounds",
        )

    return {
        "events": total,
        "hits": hits,
        "misses": misses,
        "gap_censored": gap,
        "end_censored": end,
        "censored_total": censored,
        "censoring_rate_pct": percent(
            censored, total
        ),
        "pessimistic_hit_rate_pct": lower,
        "observed_hit_rate_pct": middle,
        "optimistic_hit_rate_pct": upper,
        "uncertainty_width_pp": round(
            upper - lower,
            4,
        ),
    }


def verify_source(folder):
    manifest_path = (
        folder / "gap_integrity_manifest.json"
    )

    require(
        manifest_path.is_file(),
        "Source manifest missing",
    )

    manifest = read_json(manifest_path)

    require(
        manifest.get("execution") == "PASS",
        "Source manifest not PASS",
    )
    require(
        manifest.get("version") == SOURCE_VERSION,
        "Unexpected source version",
    )

    filenames = {
        "gap_integrity_events.csv",
        "gap_integrity_summary.csv",
        "gap_integrity_result.json",
    }

    hashes = manifest.get("output_sha256", {})

    require(
        isinstance(hashes, dict)
        and set(hashes) == filenames,
        "Source manifest file set mismatch",
    )

    for filename in sorted(filenames):
        path = folder / filename

        require(
            path.is_file(),
            f"Source file missing: {filename}",
        )

        actual = sha256(path)
        expected = str(
            hashes[filename]
        ).lower()

        require(
            actual == expected,
            f"Source SHA256 mismatch: {filename}",
        )

        print(
            f"[SOURCE SHA256 PASS] {filename}",
            flush=True,
        )

    result = read_json(
        folder / "gap_integrity_result.json"
    )

    expected_values = {
        "execution": "PASS",
        "version": SOURCE_VERSION,
        "jobs": EXPECTED_JOBS,
        "completed_jobs": EXPECTED_JOBS,
        "failed_jobs": 0,
        "original_event_records": EXPECTED_EVENTS,
        "audited_event_records": EXPECTED_EVENTS,
        "gap_integrity_audit_completed": True,
        "independent_oos_proven": False,
        "research_decision": "HOLD",
    }

    for key, expected in expected_values.items():
        require(
            result.get(key) == expected,
            f"Source result mismatch: {key}",
        )

    return result, sha256(manifest_path)


def validate_events(frame, source_result):
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

    missing_columns = required - set(frame.columns)

    require(
        not missing_columns,
        (
            "Missing source columns: "
            f"{sorted(missing_columns)}"
        ),
    )

    require(
        len(frame) == EXPECTED_EVENTS,
        "Source event count mismatch",
    )

    for column, valid in [
        ("timeframe", TIMEFRAMES),
        ("period", PERIODS),
        ("horizon_hours", HORIZONS),
        ("corrected_outcome", set(OUTCOMES)),
    ]:
        require(
            frame[column].isin(valid).all(),
            f"Invalid values in {column}",
        )

    require(
        frame["market"].notna().all()
        and frame["market"].str.startswith(
            "KRW-"
        ).all(),
        "Invalid market values",
    )

    for column in [
        "signal_utc",
        "entry_utc",
    ]:
        require(
            frame[column].notna().all(),
            f"Missing timestamps: {column}",
        )

    for column in [
        "observed_candles",
        "expected_candles",
        "missing_candles",
        "full_window_contiguous",
    ]:
        require(
            frame[column].notna().all(),
            f"Missing numeric values: {column}",
        )

    require(
        (
            frame["observed_candles"] >= 1
        ).all(),
        "Invalid observed candle count",
    )

    require(
        (
            frame["expected_candles"]
            >= frame["observed_candles"]
        ).all(),
        "Observed candle count exceeds expected",
    )

    require(
        (
            frame["missing_candles"]
            == frame["expected_candles"]
            - frame["observed_candles"]
        ).all(),
        "Missing candle count mismatch",
    )

    require(
        frame["full_window_contiguous"].isin(
            [0, 1]
        ).all(),
        "Invalid contiguous flag",
    )

    censored = frame[
        "corrected_outcome"
    ].isin([
        "GAP_CENSORED",
        "END_CENSORED",
    ])

    require(
        not (
            censored
            & frame["full_window_contiguous"].eq(1)
        ).any(),
        "Censored event marked fully contiguous",
    )

    require(
        not (
            frame["corrected_outcome"].eq("MISS")
            & frame["full_window_contiguous"].ne(1)
        ).any(),
        "Confirmed miss has incomplete window",
    )

    unique_columns = [
        "timeframe",
        "market",
        "signal_utc",
        "entry_utc",
        "horizon_hours",
    ]

    require(
        not frame.duplicated(
            unique_columns
        ).any(),
        "Duplicate event/horizon records",
    )

    counts = count_outcomes(
        frame["corrected_outcome"]
    )

    expected_counts = {
        "HIT": "confirmed_hits",
        "MISS": "confirmed_misses",
        "GAP_CENSORED": "gap_censored",
        "END_CENSORED": "end_censored",
    }

    for outcome, source_key in expected_counts.items():
        require(
            counts[outcome]
            == int(source_result[source_key]),
            (
                f"Source outcome count mismatch: "
                f"{outcome}"
            ),
        )

    require(
        sum(counts.values()) == EXPECTED_EVENTS,
        "Outcome accounting mismatch",
    )

    return counts


def grouped_metrics(frame, dimensions):
    records = []

    for key, group in frame.groupby(
        dimensions,
        sort=True,
        dropna=False,
    ):
        if not isinstance(key, tuple):
            key = (key,)

        row = dict(zip(dimensions, key))

        row.update(
            metrics(
                count_outcomes(
                    group["corrected_outcome"]
                )
            )
        )

        records.append(row)

    return records


def classify_uncertainty(width):
    if width == 0:
        return "NO_CENSORING"

    if width <= 1:
        return "LOW_CENSORING"

    if width <= 5:
        return "MATERIAL_CENSORING"

    return "HIGH_CENSORING"


def verify_aggregates(
    overall,
    summaries,
    markets,
    periods,
    timeframes,
):
    for name, rows in [
        ("summary", summaries),
        ("market", markets),
        ("period", periods),
        ("timeframe", timeframes),
    ]:
        for field in [
            "events",
            "hits",
            "misses",
            "gap_censored",
            "end_censored",
            "censored_total",
        ]:
            total = sum(
                int(row[field])
                for row in rows
            )

            require(
                total == overall[field],
                (
                    f"Aggregate mismatch: "
                    f"{name}/{field}"
                ),
            )


def verify_written_csv(
    path,
    expected_columns,
    expected_rows,
):
    with Path(path).open(
        "r",
        newline="",
        encoding="utf-8-sig",
    ) as handle:
        reader = csv.DictReader(handle)

        require(
            reader.fieldnames == expected_columns,
            f"Written CSV schema mismatch: {path.name}",
        )

        count = sum(1 for _ in reader)

    require(
        count == expected_rows,
        f"Written CSV row count mismatch: {path.name}",
    )


def run(root):
    root = Path(root).resolve()

    source_folder = (
        root
        / "data"
        / "reports"
        / "pattern_256_event_gap_integrity"
    )

    output_folder = (
        root
        / "data"
        / "reports"
        / "pattern_256_censoring_sensitivity"
    )

    source_result, source_manifest_hash = (
        verify_source(source_folder)
    )

    source_path = (
        source_folder / "gap_integrity_events.csv"
    )

    print(
        "[INFO] Loading verified events...",
        flush=True,
    )

    frame = pd.read_csv(
        source_path,
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

    counts = validate_events(
        frame,
        source_result,
    )

    overall = metrics(counts)

    summaries = grouped_metrics(
        frame,
        [
            "timeframe",
            "period",
            "horizon_hours",
        ],
    )

    require(
        len(summaries) == 27,
        "Expected 27 timeframe/period/horizon groups",
    )

    for row in summaries:
        row["status"] = classify_uncertainty(
            row["uncertainty_width_pp"]
        )

    markets = grouped_metrics(
        frame,
        [
            "timeframe",
            "market",
        ],
    )

    require(
        0 < len(markets) <= EXPECTED_JOBS,
        "Unexpected market group count",
    )

    markets.sort(
        key=lambda row: (
            -row["censored_total"],
            -row["censoring_rate_pct"],
            row["timeframe"],
            row["market"],
        )
    )

    periods = grouped_metrics(
        frame,
        ["period"],
    )

    timeframes = grouped_metrics(
        frame,
        ["timeframe"],
    )

    require(
        len(periods) == 3,
        "Expected three periods",
    )

    require(
        len(timeframes) == 3,
        "Expected three timeframes",
    )

    verify_aggregates(
        overall,
        summaries,
        markets,
        periods,
        timeframes,
    )

    output_folder.mkdir(
        parents=True,
        exist_ok=True,
    )

    summary_path = (
        output_folder / "censoring_summary.csv"
    )
    market_path = (
        output_folder / "censoring_market.csv"
    )
    result_path = (
        output_folder / "censoring_result.json"
    )
    manifest_path = (
        output_folder / "censoring_manifest.json"
    )

    # Never leave an old PASS result after a failed
    # or interrupted attempt to write new outputs.
    for path in [
        manifest_path,
        result_path,
    ]:
        if path.exists():
            path.unlink()

    print(
        "[INFO] Writing 27 sensitivity groups...",
        flush=True,
    )

    atomic_csv(
        summary_path,
        SUMMARY_FIELDS,
        summaries,
    )

    print(
        f"[INFO] Writing {len(markets)} market groups...",
        flush=True,
    )

    atomic_csv(
        market_path,
        MARKET_FIELDS,
        markets,
    )

    verify_written_csv(
        summary_path,
        SUMMARY_FIELDS,
        len(summaries),
    )

    verify_written_csv(
        market_path,
        MARKET_FIELDS,
        len(markets),
    )

    result = {
        "audit": "pattern_256_censoring_sensitivity",
        "version": VERSION,
        "execution": "PASS",
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "source_version": SOURCE_VERSION,
        "source_manifest_sha256": source_manifest_hash,
        "audited_event_records": EXPECTED_EVENTS,
        "group_count": len(summaries),
        "market_timeframe_groups": len(markets),
        "overall": overall,
        "by_period": periods,
        "by_timeframe": timeframes,
        "highest_censoring_markets": markets[:20],
        "independent_oos_proven": False,
        "point_in_time_proven": False,
        "prediction_performance_verified": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
        "interpretation": {
            "pessimistic": (
                "All censored events treated as misses."
            ),
            "observed": (
                "Censored events excluded from denominator."
            ),
            "optimistic": (
                "All censored events treated as hits."
            ),
            "warning": (
                "These are deterministic sensitivity "
                "bounds, not confidence intervals."
            ),
        },
        "limitations": [
            "Three horizons reuse underlying signals.",
            "Events within markets are dependent.",
            "Censoring may be nonrandom.",
            "Independent OOS is not proven.",
            "Source event labels are not recalculated.",
            "Upstream feature causality is unproven.",
            "No fees, slippage or fill model.",
            "No trading or training approval.",
        ],
    }

    atomic_json(
        result_path,
        result,
    )

    output_hashes = {
        path.name: sha256(path)
        for path in [
            summary_path,
            market_path,
            result_path,
        ]
    }

    manifest = {
        "audit": result["audit"],
        "version": VERSION,
        "execution": "PASS",
        "script_sha256": sha256(
            Path(__file__).resolve()
        ),
        "source_manifest_sha256": source_manifest_hash,
        "output_sha256": output_hashes,
    }

    # The manifest is committed last.
    atomic_json(
        manifest_path,
        manifest,
    )

    print(
        json.dumps(
            {
                "execution": "PASS",
                "version": VERSION,
                "events": EXPECTED_EVENTS,
                "summary_groups": len(summaries),
                "market_groups": len(markets),
                "overall": overall,
                "research_decision": "HOLD",
            },
            ensure_ascii=False,
            indent=2,
        ),
        flush=True,
    )

    return 0


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Pattern 256 censoring sensitivity audit"
        )
    )

    parser.add_argument(
        "--root",
        default=".",
        help="Persistent local research project root",
    )

    args = parser.parse_args()

    try:
        return run(args.root)

    except KeyboardInterrupt:
        print(
            "[STOP] Interrupted by user",
            file=sys.stderr,
            flush=True,
        )
        return 130

    except Exception:
        print(
            "[FAIL] Censoring sensitivity audit",
            file=sys.stderr,
            flush=True,
        )
        traceback.print_exc(
            file=sys.stderr
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
