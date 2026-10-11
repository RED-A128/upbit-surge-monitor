
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
compare_pattern_256_accumulation_fair.py
Clean V001

Pattern 256 vs Pattern 256 + Accumulation
Fair Matched-Window Comparison

INPUT (READ ONLY)
-----------------
data/reports/pattern_256_accumulation_30pct/
    validation_detail.csv
    validation_result.json

data/reports/pattern_256_accumulation_independent/
    working.sqlite3
    database_checkpoint.json
    independent_detail.csv
    independent_result.json

data/reports/pattern_256_accumulation_selection_audit/
    selection_audit.csv
    matched_window_comparison.csv
    selection_audit_result.json
    selection_audit_manifest.json

OUTPUT (NEW DIRECTORY)
----------------------
data/reports/pattern_256_accumulation_fair/
    fair_comparison.csv
    fair_diagnostics.csv
    fair_result.json
    fair_manifest.json

PRINCIPLES
----------
1. Never modify upstream files.
2. Use original source signals, not greedy-selected signals.
3. Compare PATTERN_256_ONLY against BOTH.
4. Match market, timeframe, horizon, period and
   fixed UTC-aligned evaluation window.
5. Select earliest eligible signal per class/window
   without consulting HIT/MISS outcome.
6. Include a pair in the primary comparison only
   when both selected outcomes are complete.
7. Report differences in entry time and overlap.
8. Cluster bootstrap by market.
9. Do not claim predictive superiority from
   overlapping historical observations.
10. No trading and no exchange API calls.

RUN
---
py compare_pattern_256_accumulation_fair.py

DEPENDENCIES
------------
Python standard library only.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
import os
import random
import sqlite3
import sys
import tempfile
import time
import traceback

from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote


PROGRAM = "compare_pattern_256_accumulation_fair.py"
VERSION = "Clean V001"

ROOT = Path(__file__).resolve().parent

REPORTS = ROOT / "data" / "reports"

VALIDATION_DIR = (
    REPORTS / "pattern_256_accumulation_30pct"
)

INDEPENDENT_DIR = (
    REPORTS / "pattern_256_accumulation_independent"
)

AUDIT_DIR = (
    REPORTS / "pattern_256_accumulation_selection_audit"
)

OUTPUT_DIR = (
    REPORTS / "pattern_256_accumulation_fair"
)

VALIDATION_DETAIL = (
    VALIDATION_DIR / "validation_detail.csv"
)

VALIDATION_RESULT = (
    VALIDATION_DIR / "validation_result.json"
)

DATABASE = (
    INDEPENDENT_DIR / "working.sqlite3"
)

DATABASE_CHECKPOINT = (
    INDEPENDENT_DIR / "database_checkpoint.json"
)

INDEPENDENT_DETAIL = (
    INDEPENDENT_DIR / "independent_detail.csv"
)

INDEPENDENT_RESULT = (
    INDEPENDENT_DIR / "independent_result.json"
)

AUDIT_SELECTION = (
    AUDIT_DIR / "selection_audit.csv"
)

AUDIT_MATCHED = (
    AUDIT_DIR / "matched_window_comparison.csv"
)

AUDIT_RESULT = (
    AUDIT_DIR / "selection_audit_result.json"
)

AUDIT_MANIFEST = (
    AUDIT_DIR / "selection_audit_manifest.json"
)

OUTPUT_COMPARISON = (
    OUTPUT_DIR / "fair_comparison.csv"
)

OUTPUT_DIAGNOSTICS = (
    OUTPUT_DIR / "fair_diagnostics.csv"
)

OUTPUT_RESULT = (
    OUTPUT_DIR / "fair_result.json"
)

OUTPUT_MANIFEST = (
    OUTPUT_DIR / "fair_manifest.json"
)

TIMEFRAMES = ("h1", "h4", "d1")

HORIZONS = (24, 72, 168)

PERIODS = ("TRAIN", "TEST")

CLASSES = (
    "PATTERN_256_ONLY",
    "BOTH",
)

SECONDS_PER_CANDLE = {
    "h1": 3600,
    "h4": 14400,
    "d1": 86400,
}

BOOTSTRAP_ITERATIONS = 2000
BOOTSTRAP_SEED = 2562026

MIN_PAIRS = 100
MIN_MARKETS = 20

COMPARISON_COLUMNS = [
    "timeframe",
    "horizon",
    "period",
    "paired_windows",
    "markets",
    "pattern_only_hits",
    "both_hits",
    "pattern_only_hit_rate_pct",
    "both_hit_rate_pct",
    "difference_pp",
    "bootstrap_ci_low_pp",
    "bootstrap_ci_high_pp",
    "bootstrap_valid_iterations",
    "discordant_both_wins",
    "discordant_pattern_only_wins",
    "equal_outcomes",
    "mean_entry_gap_hours",
    "median_entry_gap_hours",
    "mean_evaluation_overlap_pct",
    "assessment",
]

DIAGNOSTIC_COLUMNS = [
    "timeframe",
    "horizon",
    "period",
    "source_pattern_only",
    "source_both",
    "complete_pattern_only",
    "complete_both",
    "candidate_windows_pattern_only",
    "candidate_windows_both",
    "shared_candidate_windows",
    "complete_pairs",
    "incomplete_shared_windows",
    "same_entry_timestamp_pairs",
    "different_entry_timestamp_pairs",
    "zero_overlap_pairs",
    "partial_overlap_pairs",
    "full_overlap_pairs",
]


def log(message):
    stamp = datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    print(
        f"[{stamp}] {message}",
        flush=True,
    )


def utc_now():
    return datetime.now(
        timezone.utc
    ).isoformat()


def sha256(path):
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        while True:
            block = handle.read(1024 * 1024)

            if not block:
                break

            digest.update(block)

    return digest.hexdigest()


def read_json(path):
    with path.open(
        "r",
        encoding="utf-8-sig",
    ) as handle:
        return json.load(handle)


def check_hash(path, expected):
    if (
        not isinstance(expected, str)
        or len(expected) != 64
    ):
        raise ValueError(
            f"Invalid expected SHA256: {path}"
        )

    actual = sha256(path)

    if actual.lower() != expected.lower():
        raise ValueError(
            f"SHA256 mismatch: {path}"
        )

    return actual


def atomic_json(path, value):
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    fd, name = tempfile.mkstemp(
        prefix=path.name + ".",
        suffix=".tmp",
        dir=str(path.parent),
    )

    temporary = Path(name)

    try:
        with os.fdopen(
            fd,
            "w",
            encoding="utf-8",
        ) as handle:
            json.dump(
                value,
                handle,
                indent=2,
                ensure_ascii=False,
                allow_nan=False,
            )

        os.replace(
            temporary,
            path,
        )

    finally:
        if temporary.exists():
            temporary.unlink()


def atomic_csv(path, columns, rows):
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    fd, name = tempfile.mkstemp(
        prefix=path.name + ".",
        suffix=".tmp",
        dir=str(path.parent),
    )

    temporary = Path(name)

    try:
        with os.fdopen(
            fd,
            "w",
            encoding="utf-8-sig",
            newline="",
        ) as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=columns,
                extrasaction="ignore",
            )

            writer.writeheader()
            writer.writerows(rows)

        os.replace(
            temporary,
            path,
        )

    finally:
        if temporary.exists():
            temporary.unlink()


def percentage(numerator, denominator):
    if denominator == 0:
        return None

    return (
        numerator / denominator
    ) * 100.0


def numeric(value):
    if value is None:
        return ""

    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(
                "Non-finite numerical result"
            )

        return round(value, 6)

    return value


def median(values):
    if not values:
        return None

    ordered = sorted(values)
    count = len(ordered)
    middle = count // 2

    if count % 2:
        return ordered[middle]

    return (
        ordered[middle - 1]
        + ordered[middle]
    ) / 2.0


def percentile(values, fraction):
    if not values:
        return None

    ordered = sorted(values)

    position = (
        (len(ordered) - 1) * fraction
    )

    low = math.floor(position)
    high = math.ceil(position)

    if low == high:
        return ordered[low]

    weight = position - low

    return (
        ordered[low] * (1.0 - weight)
        + ordered[high] * weight
    )


def required_files():
    return [
        VALIDATION_DETAIL,
        VALIDATION_RESULT,
        DATABASE,
        DATABASE_CHECKPOINT,
        INDEPENDENT_DETAIL,
        INDEPENDENT_RESULT,
        AUDIT_SELECTION,
        AUDIT_MATCHED,
        AUDIT_RESULT,
        AUDIT_MANIFEST,
    ]


def validate_inputs():
    log("[CHECK] Required inputs")

    for path in required_files():
        if not path.is_file():
            raise FileNotFoundError(
                f"Missing input: {path}"
            )

    validation = read_json(
        VALIDATION_RESULT
    )

    independent = read_json(
        INDEPENDENT_RESULT
    )

    checkpoint = read_json(
        DATABASE_CHECKPOINT
    )

    audit = read_json(
        AUDIT_RESULT
    )

    audit_manifest = read_json(
        AUDIT_MANIFEST
    )

    for label, item in (
        ("validation", validation),
        ("independent", independent),
        ("audit", audit),
    ):
        if item.get("status") != "PASS":
            raise ValueError(
                f"{label} status is not PASS"
            )

    if checkpoint.get("status") != "COMPLETE":
        raise ValueError(
            "Database checkpoint incomplete"
        )

    if independent.get("version") != "Clean V001":
        raise ValueError(
            "Unsupported independent version"
        )

    if audit.get("version") != "Clean V001":
        raise ValueError(
            "Unsupported audit version"
        )

    if audit.get("audit_method") != (
        "GREEDY_SELECTION_REPLAY_AND_"
        "CLASSIFICATION_BIAS_AUDIT"
    ):
        raise ValueError(
            "Unsupported audit method"
        )

    if independent.get("split_method") != (
        "CHRONOLOGICAL_TIME_SPAN_PER_TIMEFRAME"
    ):
        raise ValueError(
            "Unsupported split method"
        )

    if independent.get("timeframes") != list(
        TIMEFRAMES
    ):
        raise ValueError(
            "Unexpected timeframes"
        )

    if independent.get("horizons") != list(
        HORIZONS
    ):
        raise ValueError(
            "Unexpected horizons"
        )

    if float(
        independent.get("target_return_pct", 0)
    ) != 30.0:
        raise ValueError(
            "Unexpected target return"
        )

    if (
        independent.get("overlap_policy")
        != "GREEDY_NON_OVERLAPPING_PER_MARKET_"
        "TIMEFRAME_HORIZON"
    ):
        raise ValueError(
            "Unexpected independent overlap policy"
        )

    source_detail_hash = check_hash(
        VALIDATION_DETAIL,
        validation.get("detail_sha256"),
    )

    source_result_hash = sha256(
        VALIDATION_RESULT
    )

    independent_detail_hash = check_hash(
        INDEPENDENT_DETAIL,
        independent.get("detail_sha256"),
    )

    audit_selection_hash = check_hash(
        AUDIT_SELECTION,
        audit.get("selection_audit_sha256"),
    )

    audit_matched_hash = check_hash(
        AUDIT_MATCHED,
        audit.get("matched_comparison_sha256"),
    )

    if (
        independent.get("source_detail_sha256")
        != source_detail_hash
    ):
        raise ValueError(
            "Independent source detail mismatch"
        )

    if (
        independent.get("source_result_sha256")
        != source_result_hash
    ):
        raise ValueError(
            "Independent source result mismatch"
        )

    if (
        checkpoint.get("source_sha256")
        != source_detail_hash
    ):
        raise ValueError(
            "Checkpoint source detail mismatch"
        )

    if (
        checkpoint.get("result_sha256")
        != source_result_hash
    ):
        raise ValueError(
            "Checkpoint source result mismatch"
        )

    if (
        audit.get("source_detail_sha256")
        != source_detail_hash
    ):
        raise ValueError(
            "Audit source detail mismatch"
        )

    if (
        audit.get("source_result_sha256")
        != source_result_hash
    ):
        raise ValueError(
            "Audit source result mismatch"
        )

    if (
        audit.get("independent_detail_sha256")
        != independent_detail_hash
    ):
        raise ValueError(
            "Audit independent detail mismatch"
        )

    if (
        audit_manifest.get("outputs", {}).get(
            "selection_audit.csv"
        )
        != audit_selection_hash
    ):
        raise ValueError(
            "Audit selection manifest mismatch"
        )

    if (
        audit_manifest.get("outputs", {}).get(
            "matched_window_comparison.csv"
        )
        != audit_matched_hash
    ):
        raise ValueError(
            "Audit matched manifest mismatch"
        )

    if (
        independent.get("imported_rows")
        != audit.get("source_rows")
    ):
        raise ValueError(
            "Audit source row mismatch"
        )

    if (
        independent.get("selected_rows")
        != audit.get("selected_rows")
    ):
        raise ValueError(
            "Audit selected row mismatch"
        )

    boundaries = independent.get(
        "boundaries"
    )

    if not isinstance(boundaries, dict):
        raise ValueError(
            "Missing boundaries"
        )

    for timeframe in TIMEFRAMES:
        item = boundaries.get(timeframe)

        if not isinstance(item, dict):
            raise ValueError(
                f"Missing boundary: {timeframe}"
            )

        if not isinstance(
            item.get("cutoff_epoch"),
            int,
        ):
            raise ValueError(
                f"Invalid cutoff: {timeframe}"
            )

    log("[PASS] Input integrity")

    return {
        "validation": validation,
        "independent": independent,
        "checkpoint": checkpoint,
        "audit": audit,
        "boundaries": boundaries,
        "hashes": {
            str(path.relative_to(ROOT)): sha256(path)
            for path in required_files()
            if path != DATABASE
        },
    }


def open_database():
    absolute = DATABASE.resolve().as_posix()

    uri = (
        "file:"
        + quote(absolute, safe="/:")
        + "?mode=ro"
    )

    connection = sqlite3.connect(
        uri,
        uri=True,
        timeout=120,
    )

    connection.execute(
        "PRAGMA query_only=ON"
    )

    return connection


def verify_database(connection, metadata):
    required = {
        "timeframe",
        "market",
        "signal_epoch",
        "entry_epoch",
        "classification",
        "horizon",
        "status",
        "hit",
    }

    columns = {
        row[1]
        for row in connection.execute(
            "PRAGMA table_info(signals)"
        )
    }

    if not required.issubset(columns):
        raise ValueError(
            "SQLite schema mismatch"
        )

    actual = connection.execute(
        "SELECT COUNT(*) FROM signals"
    ).fetchone()[0]

    expected = metadata[
        "independent"
    ]["imported_rows"]

    if actual != expected:
        raise ValueError(
            "SQLite row count mismatch"
        )

    if (
        metadata["checkpoint"].get("row_count")
        != actual
    ):
        raise ValueError(
            "Checkpoint row count mismatch"
        )

    log(
        f"[PASS] SQLite rows: {actual:,}"
    )

    return actual


def classify_period(
    signal_epoch,
    evaluation_end,
    cutoff,
):
    if signal_epoch < cutoff:
        if evaluation_end > cutoff:
            return "BOUNDARY_EXCLUDED"

        return "TRAIN"

    return "TEST"


def make_diagnostic():
    return {
        "source_pattern_only": 0,
        "source_both": 0,
        "complete_pattern_only": 0,
        "complete_both": 0,
        "candidate_windows_pattern_only": 0,
        "candidate_windows_both": 0,
        "shared_candidate_windows": 0,
        "complete_pairs": 0,
        "incomplete_shared_windows": 0,
        "same_entry_timestamp_pairs": 0,
        "different_entry_timestamp_pairs": 0,
        "zero_overlap_pairs": 0,
        "partial_overlap_pairs": 0,
        "full_overlap_pairs": 0,
    }


def evaluation_overlap(
    entry_a,
    entry_b,
    duration,
):
    start = max(entry_a, entry_b)

    end = min(
        entry_a + duration,
        entry_b + duration,
    )

    overlap = max(
        0,
        end - start,
    )

    return (
        overlap / duration * 100.0
    )


def collect_pairs(
    connection,
    metadata,
):
    """
    Read original SQLite signals in chronological order.

    For each fixed UTC window, choose the earliest
    eligible signal of each class without looking at
    its outcome.

    CENSORED/INVALID earliest signals remain selected
    for that window. Such a window is excluded from
    primary paired outcome analysis.

    This prevents choosing a later winner merely
    because an earlier outcome was incomplete.
    """

    diagnostics = defaultdict(
        make_diagnostic
    )

    pairs = defaultdict(list)

    source_rows = 0
    groups_processed = 0

    groups = connection.execute(
        """
        SELECT
            timeframe,
            market,
            horizon,
            COUNT(*)
        FROM signals
        GROUP BY
            timeframe,
            market,
            horizon
        ORDER BY
            timeframe,
            market,
            horizon
        """
    ).fetchall()

    for (
        timeframe,
        market,
        horizon,
        expected_count,
    ) in groups:

        if timeframe not in TIMEFRAMES:
            raise ValueError(
                f"Unexpected timeframe: {timeframe}"
            )

        if horizon not in HORIZONS:
            raise ValueError(
                f"Unexpected horizon: {horizon}"
            )

        duration = (
            horizon
            * SECONDS_PER_CANDLE[timeframe]
        )

        cutoff = metadata[
            "boundaries"
        ][timeframe]["cutoff_epoch"]

        windows = {}

        cursor = connection.execute(
            """
            SELECT
                signal_epoch,
                entry_epoch,
                classification,
                status,
                hit
            FROM signals
            WHERE timeframe = ?
              AND market = ?
              AND horizon = ?
            ORDER BY
                signal_epoch,
                entry_epoch,
                rowid
            """,
            (
                timeframe,
                market,
                horizon,
            ),
        )

        group_count = 0

        for (
            signal_epoch,
            entry_epoch,
            classification,
            status,
            hit,
        ) in cursor:

            group_count += 1
            source_rows += 1

            if classification not in CLASSES:
                continue

            if signal_epoch is None:
                raise ValueError(
                    "Missing signal epoch"
                )

            if status not in (
                "HIT",
                "MISS",
                "CENSORED",
                "INVALID",
            ):
                raise ValueError(
                    f"Unexpected status: {status}"
                )

            if entry_epoch is None:
                # No entry time: cannot establish
                # a comparable evaluation interval.
                continue

            evaluation_end = (
                entry_epoch + duration
            )

            period = classify_period(
                signal_epoch,
                evaluation_end,
                cutoff,
            )

            if period not in PERIODS:
                continue

            key = (
                timeframe,
                horizon,
                period,
            )

            diag = diagnostics[key]

            if classification == "PATTERN_256_ONLY":
                diag["source_pattern_only"] += 1

                if status in ("HIT", "MISS"):
                    diag["complete_pattern_only"] += 1

            else:
                diag["source_both"] += 1

                if status in ("HIT", "MISS"):
                    diag["complete_both"] += 1

            window_index = (
                signal_epoch // duration
            )

            window_key = (
                period,
                window_index,
            )

            window = windows.setdefault(
                window_key,
                {},
            )

            if classification not in window:
                window[classification] = {
                    "signal_epoch": signal_epoch,
                    "entry_epoch": entry_epoch,
                    "status": status,
                    "hit": int(status == "HIT"),
                }

        if group_count != expected_count:
            raise ValueError(
                "SQLite group count mismatch"
            )

        for (
            period,
            window_index,
        ), selected in windows.items():

            key = (
                timeframe,
                horizon,
                period,
            )

            diag = diagnostics[key]

            only = selected.get(
                "PATTERN_256_ONLY"
            )

            both = selected.get(
                "BOTH"
            )

            if only is not None:
                diag[
                    "candidate_windows_pattern_only"
                ] += 1

            if both is not None:
                diag[
                    "candidate_windows_both"
                ] += 1

            if only is None or both is None:
                continue

            diag[
                "shared_candidate_windows"
            ] += 1

            if (
                only["status"] not in ("HIT", "MISS")
                or both["status"] not in ("HIT", "MISS")
            ):
                diag[
                    "incomplete_shared_windows"
                ] += 1
                continue

            entry_gap_hours = (
                abs(
                    only["entry_epoch"]
                    - both["entry_epoch"]
                )
                / 3600.0
            )

            overlap_pct = evaluation_overlap(
                only["entry_epoch"],
                both["entry_epoch"],
                duration,
            )

            if entry_gap_hours == 0:
                diag[
                    "same_entry_timestamp_pairs"
                ] += 1
            else:
                diag[
                    "different_entry_timestamp_pairs"
                ] += 1

            if overlap_pct == 0:
                diag[
                    "zero_overlap_pairs"
                ] += 1

            elif overlap_pct >= 100:
                diag[
                    "full_overlap_pairs"
                ] += 1

            else:
                diag[
                    "partial_overlap_pairs"
                ] += 1

            diag["complete_pairs"] += 1

            pairs[key].append({
                "market": market,
                "window_index": window_index,
                "only_hit": only["hit"],
                "both_hit": both["hit"],
                "entry_gap_hours": entry_gap_hours,
                "overlap_pct": overlap_pct,
            })

        groups_processed += 1

        if (
            groups_processed % 50 == 0
            or groups_processed == len(groups)
        ):
            log(
                "[SCAN] "
                f"{groups_processed:,}/{len(groups):,} "
                f"groups; "
                f"{source_rows:,} source rows"
            )

    if source_rows != metadata[
        "independent"
    ]["imported_rows"]:
        raise ValueError(
            "Source row accounting mismatch"
        )

    log("[PASS] Source scan completed")

    return (
        pairs,
        diagnostics,
        source_rows,
        len(groups),
    )


def cluster_bootstrap(rows, seed):
    """
    Market-cluster bootstrap.

    Markets are sampled with replacement.
    All matched windows belonging to a sampled
    market are carried together.

    The interval is descriptive and does not
    resolve dependence across markets, calendar
    periods, or previously optimized patterns.
    """

    by_market = defaultdict(
        lambda: [0, 0, 0]
    )

    for row in rows:
        item = by_market[row["market"]]

        item[0] += 1
        item[1] += row["only_hit"]
        item[2] += row["both_hit"]

    markets = sorted(by_market)

    if len(markets) < 2:
        return None, None, 0

    clusters = [
        by_market[market]
        for market in markets
    ]

    rng = random.Random(seed)

    estimates = []

    for _ in range(
        BOOTSTRAP_ITERATIONS
    ):
        count = 0
        only_hits = 0
        both_hits = 0

        for _ in range(len(clusters)):
            cluster = clusters[
                rng.randrange(len(clusters))
            ]

            count += cluster[0]
            only_hits += cluster[1]
            both_hits += cluster[2]

        if count <= 0:
            continue

        difference = (
            (both_hits - only_hits)
            / count
            * 100.0
        )

        estimates.append(difference)

    if len(estimates) < 100:
        return None, None, len(estimates)

    return (
        percentile(estimates, 0.025),
        percentile(estimates, 0.975),
        len(estimates),
    )


def assess(
    count,
    market_count,
    difference,
    ci_low,
    ci_high,
):
    if count == 0:
        return "NO_MATCHED_PAIRS"

    if count < MIN_PAIRS:
        return "INSUFFICIENT_PAIRS"

    if market_count < MIN_MARKETS:
        return "INSUFFICIENT_MARKETS"

    if ci_low is None or ci_high is None:
        return "CI_UNAVAILABLE"

    if ci_low > 0:
        return "BOTH_HIGHER_DESCRIPTIVE_CI"

    if ci_high < 0:
        return "PATTERN_ONLY_HIGHER_DESCRIPTIVE_CI"

    return "NO_CLEAR_DIFFERENCE"


def build_comparison_rows(pairs):
    rows = []

    for timeframe in TIMEFRAMES:
        for horizon in HORIZONS:
            for period in PERIODS:
                key = (
                    timeframe,
                    horizon,
                    period,
                )

                data = pairs.get(key, [])

                count = len(data)

                markets = {
                    row["market"]
                    for row in data
                }

                only_hits = sum(
                    row["only_hit"]
                    for row in data
                )

                both_hits = sum(
                    row["both_hit"]
                    for row in data
                )

                only_rate = percentage(
                    only_hits,
                    count,
                )

                both_rate = percentage(
                    both_hits,
                    count,
                )

                if count:
                    difference = (
                        both_rate - only_rate
                    )

                    mean_gap = sum(
                        row["entry_gap_hours"]
                        for row in data
                    ) / count

                    median_gap = median([
                        row["entry_gap_hours"]
                        for row in data
                    ])

                    mean_overlap = sum(
                        row["overlap_pct"]
                        for row in data
                    ) / count

                else:
                    difference = None
                    mean_gap = None
                    median_gap = None
                    mean_overlap = None

                both_wins = sum(
                    1
                    for row in data
                    if (
                        row["both_hit"] == 1
                        and row["only_hit"] == 0
                    )
                )

                only_wins = sum(
                    1
                    for row in data
                    if (
                        row["both_hit"] == 0
                        and row["only_hit"] == 1
                    )
                )

                equal = (
                    count
                    - both_wins
                    - only_wins
                )

                seed = (
                    BOOTSTRAP_SEED
                    + TIMEFRAMES.index(timeframe) * 10000
                    + horizon * 10
                    + PERIODS.index(period)
                )

                ci_low, ci_high, iterations = (
                    cluster_bootstrap(
                        data,
                        seed,
                    )
                )

                assessment = assess(
                    count,
                    len(markets),
                    difference,
                    ci_low,
                    ci_high,
                )

                rows.append({
                    "timeframe": timeframe,
                    "horizon": horizon,
                    "period": period,
                    "paired_windows": count,
                    "markets": len(markets),
                    "pattern_only_hits": only_hits,
                    "both_hits": both_hits,
                    "pattern_only_hit_rate_pct": numeric(
                        only_rate
                    ),
                    "both_hit_rate_pct": numeric(
                        both_rate
                    ),
                    "difference_pp": numeric(
                        difference
                    ),
                    "bootstrap_ci_low_pp": numeric(
                        ci_low
                    ),
                    "bootstrap_ci_high_pp": numeric(
                        ci_high
                    ),
                    "bootstrap_valid_iterations": iterations,
                    "discordant_both_wins": both_wins,
                    "discordant_pattern_only_wins": only_wins,
                    "equal_outcomes": equal,
                    "mean_entry_gap_hours": numeric(
                        mean_gap
                    ),
                    "median_entry_gap_hours": numeric(
                        median_gap
                    ),
                    "mean_evaluation_overlap_pct": numeric(
                        mean_overlap
                    ),
                    "assessment": assessment,
                })

    return rows


def build_diagnostic_rows(diagnostics):
    rows = []

    for timeframe in TIMEFRAMES:
        for horizon in HORIZONS:
            for period in PERIODS:
                key = (
                    timeframe,
                    horizon,
                    period,
                )

                item = diagnostics[key]

                rows.append({
                    "timeframe": timeframe,
                    "horizon": horizon,
                    "period": period,
                    **item,
                })

    return rows


def verify_accounting(
    comparison_rows,
    diagnostic_rows,
):
    if len(comparison_rows) != 18:
        raise ValueError(
            "Expected 18 comparison rows"
        )

    if len(diagnostic_rows) != 18:
        raise ValueError(
            "Expected 18 diagnostic rows"
        )

    for comparison, diagnostic in zip(
        comparison_rows,
        diagnostic_rows,
    ):
        count = comparison[
            "paired_windows"
        ]

        if count != diagnostic[
            "complete_pairs"
        ]:
            raise ValueError(
                "Paired window count mismatch"
            )

        if (
            count
            + diagnostic["incomplete_shared_windows"]
            != diagnostic["shared_candidate_windows"]
        ):
            raise ValueError(
                "Shared window accounting mismatch"
            )

        if (
            comparison["discordant_both_wins"]
            + comparison["discordant_pattern_only_wins"]
            + comparison["equal_outcomes"]
            != count
        ):
            raise ValueError(
                "Outcome accounting mismatch"
            )

        if (
            diagnostic["same_entry_timestamp_pairs"]
            + diagnostic["different_entry_timestamp_pairs"]
            != count
        ):
            raise ValueError(
                "Entry timing accounting mismatch"
            )

        if (
            diagnostic["zero_overlap_pairs"]
            + diagnostic["partial_overlap_pairs"]
            + diagnostic["full_overlap_pairs"]
            != count
        ):
            raise ValueError(
                "Overlap accounting mismatch"
            )

        if (
            comparison["pattern_only_hits"] > count
            or comparison["both_hits"] > count
        ):
            raise ValueError(
                "Invalid hit count"
            )

    log("[PASS] Output accounting")


def verify_inputs_unchanged(metadata):
    log("[CHECK] Upstream files unchanged")

    for relative, expected in metadata[
        "hashes"
    ].items():
        path = ROOT / relative

        check_hash(
            path,
            expected,
        )

    log("[PASS] Upstream SHA256 unchanged")


def main():
    started = time.monotonic()

    log("=" * 72)
    log(f"{PROGRAM} - {VERSION}")
    log("=" * 72)

    metadata = validate_inputs()

    connection = open_database()

    try:
        source_count = verify_database(
            connection,
            metadata,
        )

        (
            pairs,
            diagnostics,
            scanned_rows,
            group_count,
        ) = collect_pairs(
            connection,
            metadata,
        )

    finally:
        connection.close()

    if scanned_rows != source_count:
        raise ValueError(
            "Database scan count mismatch"
        )

    log("[BUILD] Fair comparison")

    comparison_rows = build_comparison_rows(
        pairs
    )

    diagnostic_rows = build_diagnostic_rows(
        diagnostics
    )

    verify_accounting(
        comparison_rows,
        diagnostic_rows,
    )

    verify_inputs_unchanged(
        metadata
    )

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    atomic_csv(
        OUTPUT_COMPARISON,
        COMPARISON_COLUMNS,
        comparison_rows,
    )

    atomic_csv(
        OUTPUT_DIAGNOSTICS,
        DIAGNOSTIC_COLUMNS,
        diagnostic_rows,
    )

    comparison_hash = sha256(
        OUTPUT_COMPARISON
    )

    diagnostics_hash = sha256(
        OUTPUT_DIAGNOSTICS
    )

    total_pairs = sum(
        row["paired_windows"]
        for row in comparison_rows
    )

    warnings = [
        (
            "Matched windows are fixed UTC-aligned "
            "time buckets. They are not identical "
            "trade-entry events."
        ),
        (
            "The earliest eligible signal is chosen "
            "before checking outcome completeness. "
            "Incomplete pairs are excluded from "
            "the primary hit-rate comparison."
        ),
        (
            "Different entry timestamps may expose "
            "the two patterns to different price paths."
        ),
        (
            "The same market can contribute multiple "
            "correlated observations."
        ),
        (
            "Market-cluster bootstrap does not fully "
            "account for common market-wide shocks "
            "or time dependence across markets."
        ),
        (
            "The confidence intervals are exploratory "
            "and are not adjusted for multiple testing "
            "or prior pattern optimization."
        ),
        (
            "TRAIN/TEST are historical partitions "
            "of previously researched observations, "
            "not a newly collected untouched dataset."
        ),
        (
            "Future-high 30 percent attainment "
            "is not executable net trading profit."
        ),
        (
            "This analysis does not establish "
            "causality or guarantee future returns."
        ),
    ]

    elapsed = round(
        time.monotonic() - started,
        3,
    )

    result = {
        "program": PROGRAM,
        "version": VERSION,
        "status": "PASS",
        "created_at_utc": utc_now(),
        "method": (
            "EARLIEST_ELIGIBLE_SIGNAL_PER_CLASS_"
            "SHARED_FIXED_MARKET_WINDOW"
        ),
        "comparison_type": (
            "OBSERVATIONAL_MATCHED_WINDOW"
        ),
        "confidence_method": (
            "MARKET_CLUSTER_BOOTSTRAP_PERCENTILE"
        ),
        "confidence_level": 0.95,
        "bootstrap_iterations": (
            BOOTSTRAP_ITERATIONS
        ),
        "bootstrap_seed": (
            BOOTSTRAP_SEED
        ),
        "target_return_pct": 30.0,
        "timeframes": list(TIMEFRAMES),
        "horizons": list(HORIZONS),
        "periods": list(PERIODS),
        "classifications": list(CLASSES),
        "boundaries": metadata[
            "boundaries"
        ],
        "source_rows": scanned_rows,
        "group_count": group_count,
        "comparison_rows": len(
            comparison_rows
        ),
        "diagnostic_rows": len(
            diagnostic_rows
        ),
        "total_paired_windows_across_horizons": (
            total_pairs
        ),
        "fair_comparison_sha256": (
            comparison_hash
        ),
        "fair_diagnostics_sha256": (
            diagnostics_hash
        ),
        "elapsed_seconds": elapsed,
        "warnings": warnings,
    }

    manifest = {
        "program": PROGRAM,
        "version": VERSION,
        "created_at_utc": utc_now(),
        "inputs": metadata["hashes"],
        "database_path": str(
            DATABASE.relative_to(ROOT)
        ),
        "database_rows": source_count,
        "outputs": {
            "fair_comparison.csv": (
                comparison_hash
            ),
            "fair_diagnostics.csv": (
                diagnostics_hash
            ),
        },
    }

    atomic_json(
        OUTPUT_MANIFEST,
        manifest,
    )

    atomic_json(
        OUTPUT_RESULT,
        result,
    )

    log("=" * 72)
    log("[PASS] Fair comparison completed")
    log(
        f"[PASS] Source rows: {scanned_rows:,}"
    )
    log(
        f"[PASS] SQLite groups: {group_count:,}"
    )
    log(
        f"[PASS] Comparison rows: "
        f"{len(comparison_rows)}"
    )
    log(
        f"[PASS] Diagnostic rows: "
        f"{len(diagnostic_rows)}"
    )
    log(
        f"[PASS] Total paired windows: "
        f"{total_pairs:,}"
    )
    log(
        f"[PASS] Elapsed: {elapsed:.2f}s"
    )
    log(
        f"[OUTPUT] {OUTPUT_DIR}"
    )
    log("=" * 72)

    for row in comparison_rows:
        if (
            row["period"] == "TEST"
            and row["horizon"] == 168
        ):
            log(
                "[TEST 168] "
                f"{row['timeframe']} "
                f"pairs={row['paired_windows']} "
                f"markets={row['markets']} "
                f"diff_pp={row['difference_pp']} "
                f"CI=["
                f"{row['bootstrap_ci_low_pp']}, "
                f"{row['bootstrap_ci_high_pp']}] "
                f"{row['assessment']}"
            )

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())

    except KeyboardInterrupt:
        log("[STOP] Interrupted safely")
        sys.exit(130)

    except Exception:
        log("[FAIL] Fair comparison failed")
        traceback.print_exc()
        sys.exit(1)
