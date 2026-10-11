
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
audit_pattern_256_accumulation_selection.py
Clean V001

Purpose:
    Audit selection bias in the existing independent
    Pattern 256 / accumulation validation.

Read-only inputs:
    data/reports/pattern_256_accumulation_30pct/
        validation_detail.csv
        validation_result.json

    data/reports/pattern_256_accumulation_independent/
        working.sqlite3
        database_checkpoint.json
        independent_detail.csv
        independent_result.json

New outputs:
    data/reports/pattern_256_accumulation_selection_audit/
        selection_audit.csv
        matched_window_comparison.csv
        selection_audit_result.json
        selection_audit_manifest.json

Rules:
    - No modification of existing research outputs.
    - No modification of OHLCV.
    - No exchange API calls.
    - No trading.
    - Verify upstream SHA256 and row counts.
    - Independently replay original greedy selection.
    - Verify selected rows against independent_detail.csv.
    - Compare source and selected classification counts.
    - Compare pattern groups in shared market/time windows.
    - Do not claim causal or predictive superiority.

Run:
    py audit_pattern_256_accumulation_selection.py

Dependencies:
    Python standard library only.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
import os
import sqlite3
import sys
import tempfile
import time
import traceback

from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote


PROGRAM = "audit_pattern_256_accumulation_selection.py"
VERSION = "Clean V001"

ROOT = Path(__file__).resolve().parent

VALIDATION_DIR = (
    ROOT / "data" / "reports"
    / "pattern_256_accumulation_30pct"
)

INDEPENDENT_DIR = (
    ROOT / "data" / "reports"
    / "pattern_256_accumulation_independent"
)

OUTPUT_DIR = (
    ROOT / "data" / "reports"
    / "pattern_256_accumulation_selection_audit"
)

SOURCE_DETAIL = VALIDATION_DIR / "validation_detail.csv"
SOURCE_RESULT = VALIDATION_DIR / "validation_result.json"

DATABASE = INDEPENDENT_DIR / "working.sqlite3"
DATABASE_META = (
    INDEPENDENT_DIR / "database_checkpoint.json"
)

INDEPENDENT_DETAIL = (
    INDEPENDENT_DIR / "independent_detail.csv"
)

INDEPENDENT_RESULT = (
    INDEPENDENT_DIR / "independent_result.json"
)

OUTPUT_SELECTION = OUTPUT_DIR / "selection_audit.csv"

OUTPUT_MATCHED = (
    OUTPUT_DIR / "matched_window_comparison.csv"
)

OUTPUT_RESULT = (
    OUTPUT_DIR / "selection_audit_result.json"
)

OUTPUT_MANIFEST = (
    OUTPUT_DIR / "selection_audit_manifest.json"
)

TIMEFRAMES = ("h1", "h4", "d1")
HORIZONS = (24, 72, 168)

CLASSIFICATIONS = (
    "PATTERN_256_ONLY",
    "ACCUMULATION_ONLY",
    "BOTH",
    "NEITHER",
)

PERIODS = (
    "TRAIN",
    "TEST",
    "BOUNDARY_EXCLUDED",
)

SECONDS_PER_CANDLE = {
    "h1": 3600,
    "h4": 14400,
    "d1": 86400,
}

SELECTION_COLUMNS = [
    "timeframe",
    "horizon",
    "period",
    "classification",
    "source_signals",
    "selected_signals",
    "excluded_overlap",
    "excluded_no_entry",
    "selection_rate_pct",
    "source_complete",
    "source_hit",
    "source_hit_rate_pct",
    "selected_complete",
    "selected_hit",
    "selected_hit_rate_pct",
    "selection_rate_difference_from_pattern_only_pp",
]

MATCHED_COLUMNS = [
    "timeframe",
    "horizon",
    "period",
    "shared_market_windows",
    "pattern_only_complete_windows",
    "pattern_only_hit_windows",
    "pattern_only_hit_rate_pct",
    "both_complete_windows",
    "both_hit_windows",
    "both_hit_rate_pct",
    "difference_percentage_points",
    "assessment",
]


def log(message):
    timestamp = datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    print(f"[{timestamp}] {message}", flush=True)


def now_utc():
    return datetime.now(
        timezone.utc
    ).isoformat()


def read_json(path):
    with path.open(
        "r",
        encoding="utf-8-sig",
    ) as handle:
        return json.load(handle)


def file_hash(path):
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        while True:
            block = handle.read(1024 * 1024)

            if not block:
                break

            digest.update(block)

    return digest.hexdigest()


def verify_hash(path, expected):
    if not isinstance(expected, str):
        raise ValueError(
            f"Missing SHA256 for {path}"
        )

    if len(expected) != 64:
        raise ValueError(
            f"Invalid SHA256 for {path}"
        )

    actual = file_hash(path)

    if actual.lower() != expected.lower():
        raise ValueError(
            f"SHA256 mismatch: {path}"
        )

    log(f"[HASH PASS] {path.name}")

    return actual


def atomic_json(path, data):
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    fd, temporary_name = tempfile.mkstemp(
        prefix=path.name + ".",
        suffix=".tmp",
        dir=str(path.parent),
    )

    temporary = Path(temporary_name)

    try:
        with os.fdopen(
            fd,
            "w",
            encoding="utf-8",
        ) as handle:
            json.dump(
                data,
                handle,
                indent=2,
                ensure_ascii=False,
                allow_nan=False,
            )

        os.replace(temporary, path)

    finally:
        if temporary.exists():
            temporary.unlink()


def atomic_csv(path, columns, rows):
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    fd, temporary_name = tempfile.mkstemp(
        prefix=path.name + ".",
        suffix=".tmp",
        dir=str(path.parent),
    )

    temporary = Path(temporary_name)

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

        os.replace(temporary, path)

    finally:
        if temporary.exists():
            temporary.unlink()


def rate(numerator, denominator):
    if denominator <= 0:
        return None

    return (
        numerator / denominator * 100.0
    )


def display_number(value):
    if value is None:
        return ""

    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(
                "Non-finite result"
            )

        return round(value, 6)

    return value


def validate_inputs():
    required = [
        SOURCE_DETAIL,
        SOURCE_RESULT,
        DATABASE,
        DATABASE_META,
        INDEPENDENT_DETAIL,
        INDEPENDENT_RESULT,
    ]

    for path in required:
        if not path.is_file():
            raise FileNotFoundError(path)

    source = read_json(SOURCE_RESULT)
    independent = read_json(INDEPENDENT_RESULT)
    database_meta = read_json(DATABASE_META)

    if source.get("status") != "PASS":
        raise ValueError(
            "Source validation is not PASS"
        )

    if independent.get("status") != "PASS":
        raise ValueError(
            "Independent validation is not PASS"
        )

    if independent.get("version") != "Clean V001":
        raise ValueError(
            "Unexpected independent validation version"
        )

    if database_meta.get("status") != "COMPLETE":
        raise ValueError(
            "SQLite import checkpoint is incomplete"
        )

    if database_meta.get("version") != "Clean V001":
        raise ValueError(
            "Unexpected database checkpoint version"
        )

    if independent.get("overlap_policy") != (
        "GREEDY_NON_OVERLAPPING_PER_MARKET_"
        "TIMEFRAME_HORIZON"
    ):
        raise ValueError(
            "Unsupported overlap policy"
        )

    if independent.get("split_method") != (
        "CHRONOLOGICAL_TIME_SPAN_PER_TIMEFRAME"
    ):
        raise ValueError(
            "Unsupported chronological split method"
        )

    if independent.get("timeframes") != list(TIMEFRAMES):
        raise ValueError(
            "Unexpected timeframes"
        )

    if independent.get("horizons") != list(HORIZONS):
        raise ValueError(
            "Unexpected horizons"
        )

    if independent.get("classifications") != list(
        CLASSIFICATIONS
    ):
        raise ValueError(
            "Unexpected classifications"
        )

    if float(
        independent.get("target_return_pct", 0)
    ) != 30.0:
        raise ValueError(
            "Unexpected target return"
        )

    source_detail_hash = verify_hash(
        SOURCE_DETAIL,
        source.get("detail_sha256"),
    )

    source_result_hash = file_hash(
        SOURCE_RESULT
    )

    if (
        independent.get("source_detail_sha256")
        != source_detail_hash
    ):
        raise ValueError(
            "Independent source detail hash mismatch"
        )

    if (
        independent.get("source_result_sha256")
        != source_result_hash
    ):
        raise ValueError(
            "Independent source result hash mismatch"
        )

    if (
        database_meta.get("source_sha256")
        != source_detail_hash
    ):
        raise ValueError(
            "Database source detail hash mismatch"
        )

    if (
        database_meta.get("result_sha256")
        != source_result_hash
    ):
        raise ValueError(
            "Database source result hash mismatch"
        )

    independent_detail_hash = verify_hash(
        INDEPENDENT_DETAIL,
        independent.get("detail_sha256"),
    )

    boundaries = independent.get("boundaries")

    if not isinstance(boundaries, dict):
        raise ValueError(
            "Missing split boundaries"
        )

    for timeframe in TIMEFRAMES:
        if timeframe not in boundaries:
            raise ValueError(
                f"Missing boundary: {timeframe}"
            )

        cutoff = boundaries[
            timeframe
        ].get("cutoff_epoch")

        if not isinstance(cutoff, int):
            raise ValueError(
                f"Invalid cutoff: {timeframe}"
            )

    return {
        "source": source,
        "independent": independent,
        "database_meta": database_meta,
        "boundaries": boundaries,
        "source_detail_hash": source_detail_hash,
        "source_result_hash": source_result_hash,
        "independent_detail_hash": independent_detail_hash,
    }


def open_database():
    # Open the SQLite database read-only.
    # This audit must not modify the original DB.
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
    required_columns = {
        "timeframe",
        "market",
        "signal_epoch",
        "signal_timestamp",
        "entry_epoch",
        "entry_timestamp",
        "classification",
        "horizon",
        "status",
        "hit",
        "first_hit_bars",
        "observed_bars",
        "required_bars",
        "entry_price",
        "source_hash",
    }

    columns = {
        row[1]
        for row in connection.execute(
            "PRAGMA table_info(signals)"
        )
    }

    if not required_columns.issubset(columns):
        raise ValueError(
            "SQLite signals schema mismatch"
        )

    count = connection.execute(
        "SELECT COUNT(*) FROM signals"
    ).fetchone()[0]

    expected = metadata[
        "database_meta"
    ].get("row_count")

    if count != expected:
        raise ValueError(
            "SQLite checkpoint row count mismatch"
        )

    if count != metadata[
        "independent"
    ].get("imported_rows"):
        raise ValueError(
            "Independent imported row count mismatch"
        )

    log(
        f"[DATABASE PASS] {count:,} rows"
    )

    return count


def get_groups(connection):
    return connection.execute(
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


def period_for_signal(
    signal_epoch,
    evaluation_end,
    cutoff,
):
    if signal_epoch < cutoff:
        if evaluation_end > cutoff:
            return "BOUNDARY_EXCLUDED"

        return "TRAIN"

    return "TEST"


def make_counter():
    return {
        "source": 0,
        "selected": 0,
        "overlap": 0,
        "no_entry": 0,
        "source_complete": 0,
        "source_hit": 0,
        "selected_complete": 0,
        "selected_hit": 0,
    }


def make_window():
    return {
        "PATTERN_256_ONLY": None,
        "BOTH": None,
    }


def make_paired_counter():
    return {
        "shared": 0,
        "only_complete": 0,
        "only_hit": 0,
        "both_complete": 0,
        "both_hit": 0,
    }


def audit_signals(
    connection,
    groups,
    metadata,
):
    """
    Replays the original greedy selection policy.

    Selection is performed per:
        timeframe / market / horizon

    The first eligible entry blocks subsequent signals
    until its nominal evaluation interval ends.

    Classification is not used to choose the winner.

    Independently checks every selected row against
    independent_detail.csv.

    Shared-window comparison:
        One earliest complete signal per classification
        per market/timeframe/horizon/fixed calendar window.

    This is a diagnostic comparison, not a causal
    or statistically independent paired experiment.
    """
    counters = defaultdict(make_counter)
    paired = defaultdict(make_paired_counter)

    source_total = 0
    selected_total = 0
    overlap_total = 0
    no_entry_total = 0

    with INDEPENDENT_DETAIL.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as selected_handle:

        selected_reader = csv.DictReader(
            selected_handle
        )

        required_selected = {
            "timeframe",
            "market",
            "signal_timestamp",
            "entry_timestamp",
            "classification",
            "horizon",
            "period",
            "status",
            "signal_epoch",
            "entry_epoch",
            "evaluation_end_epoch",
        }

        if not required_selected.issubset(
            set(selected_reader.fieldnames or [])
        ):
            raise ValueError(
                "Independent detail schema mismatch"
            )

        def verify_selected(expected):
            actual = next(
                selected_reader,
                None,
            )

            if actual is None:
                raise ValueError(
                    "Independent detail ended early"
                )

            for key, value in expected.items():
                if str(actual[key]) != str(value):
                    raise ValueError(
                        "Independent selection mismatch: "
                        f"{key}, expected={value}, "
                        f"actual={actual[key]}"
                    )

        for group_number, group in enumerate(
            groups,
            start=1,
        ):
            timeframe, market, horizon, group_size = group

            if timeframe not in TIMEFRAMES:
                raise ValueError(
                    f"Unexpected timeframe: {timeframe}"
                )

            if horizon not in HORIZONS:
                raise ValueError(
                    f"Unexpected horizon: {horizon}"
                )

            cutoff = metadata[
                "boundaries"
            ][timeframe]["cutoff_epoch"]

            duration = (
                horizon
                * SECONDS_PER_CANDLE[timeframe]
            )

            blocked_until = None
            windows = {}

            cursor = connection.execute(
                """
                SELECT
                    signal_epoch,
                    signal_timestamp,
                    entry_epoch,
                    entry_timestamp,
                    classification,
                    status,
                    hit
                FROM signals
                WHERE
                    timeframe = ?
                    AND market = ?
                    AND horizon = ?
                ORDER BY
                    signal_epoch,
                    entry_epoch
                """,
                (timeframe, market, horizon),
            )

            group_count = 0

            for record in cursor:
                (
                    signal_epoch,
                    signal_timestamp,
                    entry_epoch,
                    entry_timestamp,
                    classification,
                    status,
                    hit,
                ) = record

                group_count += 1
                source_total += 1

                if classification not in CLASSIFICATIONS:
                    raise ValueError(
                        "Unexpected classification"
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

                # Without an entry timestamp, no actual
                # evaluation interval can be established.
                if entry_epoch is None:
                    period = (
                        "TRAIN"
                        if signal_epoch < cutoff
                        else "TEST"
                    )
                    evaluation_end = None
                else:
                    evaluation_end = (
                        entry_epoch + duration
                    )
                    period = period_for_signal(
                        signal_epoch,
                        evaluation_end,
                        cutoff,
                    )

                key = (
                    timeframe,
                    horizon,
                    period,
                    classification,
                )

                counter = counters[key]
                counter["source"] += 1

                complete = status in (
                    "HIT",
                    "MISS",
                )

                if complete:
                    counter["source_complete"] += 1

                    if status == "HIT":
                        counter["source_hit"] += 1

                # Original greedy selection replay.
                if entry_epoch is None:
                    counter["no_entry"] += 1
                    no_entry_total += 1
                    selected = False

                elif (
                    blocked_until is not None
                    and signal_epoch < blocked_until
                ):
                    counter["overlap"] += 1
                    overlap_total += 1
                    selected = False

                else:
                    selected = True
                    blocked_until = evaluation_end

                    counter["selected"] += 1
                    selected_total += 1

                    if complete:
                        counter["selected_complete"] += 1

                        if status == "HIT":
                            counter["selected_hit"] += 1

                    verify_selected({
                        "timeframe": timeframe,
                        "market": market,
                        "signal_timestamp": signal_timestamp,
                        "entry_timestamp": entry_timestamp,
                        "classification": classification,
                        "horizon": horizon,
                        "period": period,
                        "status": status,
                        "signal_epoch": signal_epoch,
                        "entry_epoch": entry_epoch,
                        "evaluation_end_epoch": evaluation_end,
                    })

                # Separate shared-window analysis uses
                # source signals, not greedy-selected rows.
                # This helps diagnose selection distortion.
                if (
                    period in ("TRAIN", "TEST")
                    and classification in (
                        "PATTERN_256_ONLY",
                        "BOTH",
                    )
                    and complete
                    and entry_epoch is not None
                ):
                    window_index = (
                        signal_epoch // duration
                    )

                    window_key = (
                        period,
                        window_index,
                    )

                    window = windows.setdefault(
                        window_key,
                        make_window(),
                    )

                    # Earliest complete signal in this
                    # classification/window is retained.
                    if window[classification] is None:
                        window[classification] = (
                            1 if status == "HIT" else 0
                        )

            if group_count != group_size:
                raise ValueError(
                    f"Group count mismatch: {group}"
                )

            # Only windows containing both classes
            # contribute to this paired diagnostic.
            for (period, _), window in windows.items():
                only = window["PATTERN_256_ONLY"]
                both = window["BOTH"]

                if only is None or both is None:
                    continue

                paired_key = (
                    timeframe,
                    horizon,
                    period,
                )

                counter = paired[paired_key]

                counter["shared"] += 1
                counter["only_complete"] += 1
                counter["both_complete"] += 1
                counter["only_hit"] += only
                counter["both_hit"] += both

            if (
                group_number % 25 == 0
                or group_number == len(groups)
            ):
                log(
                    "[AUDIT] "
                    f"{group_number:,}/{len(groups):,} "
                    f"source={source_total:,} "
                    f"selected={selected_total:,} "
                    f"overlap={overlap_total:,}"
                )

        # There must not be additional selected rows.
        extra = next(
            selected_reader,
            None,
        )

        if extra is not None:
            raise ValueError(
                "Independent detail contains extra rows"
            )

    expected = metadata["independent"]

    if source_total != expected["imported_rows"]:
        raise ValueError(
            "Source row total mismatch"
        )

    if selected_total != expected["selected_rows"]:
        raise ValueError(
            "Selected row total mismatch"
        )

    if (
        overlap_total + no_entry_total
        != expected["excluded_rows"]
    ):
        raise ValueError(
            "Excluded row total mismatch"
        )

    if (
        source_total
        != selected_total
        + overlap_total
        + no_entry_total
    ):
        raise ValueError(
            "Audit accounting mismatch"
        )

    log("[PASS] Independent selection replay verified")

    totals = {
        "source_rows": source_total,
        "selected_rows": selected_total,
        "excluded_overlap": overlap_total,
        "excluded_no_entry": no_entry_total,
        "excluded_total": (
            overlap_total + no_entry_total
        ),
    }

    return counters, paired, totals


def build_selection_rows(counters):
    rows = []

    for timeframe in TIMEFRAMES:
        for horizon in HORIZONS:
            for period in PERIODS:
                baseline_key = (
                    timeframe,
                    horizon,
                    period,
                    "PATTERN_256_ONLY",
                )

                baseline = counters[baseline_key]

                baseline_rate = rate(
                    baseline["selected"],
                    baseline["source"],
                )

                for classification in CLASSIFICATIONS:
                    key = (
                        timeframe,
                        horizon,
                        period,
                        classification,
                    )

                    data = counters[key]

                    selection_rate = rate(
                        data["selected"],
                        data["source"],
                    )

                    if (
                        baseline_rate is None
                        or selection_rate is None
                    ):
                        difference = None
                    else:
                        difference = (
                            selection_rate
                            - baseline_rate
                        )

                    rows.append({
                        "timeframe": timeframe,
                        "horizon": horizon,
                        "period": period,
                        "classification": classification,
                        "source_signals": data["source"],
                        "selected_signals": data["selected"],
                        "excluded_overlap": data["overlap"],
                        "excluded_no_entry": data["no_entry"],
                        "selection_rate_pct": display_number(
                            selection_rate
                        ),
                        "source_complete": data[
                            "source_complete"
                        ],
                        "source_hit": data["source_hit"],
                        "source_hit_rate_pct": display_number(
                            rate(
                                data["source_hit"],
                                data["source_complete"],
                            )
                        ),
                        "selected_complete": data[
                            "selected_complete"
                        ],
                        "selected_hit": data["selected_hit"],
                        "selected_hit_rate_pct": display_number(
                            rate(
                                data["selected_hit"],
                                data["selected_complete"],
                            )
                        ),
                        "selection_rate_difference_from_pattern_only_pp": (
                            display_number(difference)
                        ),
                    })

    return rows


def build_matched_rows(paired):
    rows = []

    for timeframe in TIMEFRAMES:
        for horizon in HORIZONS:
            for period in ("TRAIN", "TEST"):
                key = (
                    timeframe,
                    horizon,
                    period,
                )

                data = paired[key]

                only_rate = rate(
                    data["only_hit"],
                    data["only_complete"],
                )

                both_rate = rate(
                    data["both_hit"],
                    data["both_complete"],
                )

                if (
                    only_rate is None
                    or both_rate is None
                ):
                    difference = None
                    assessment = "NO_SHARED_COMPLETE_WINDOWS"

                else:
                    difference = (
                        both_rate - only_rate
                    )

                    if data["shared"] < 100:
                        assessment = "SMALL_SAMPLE"

                    elif difference > 0:
                        assessment = "BOTH_HIGHER_DESCRIPTIVE"

                    elif difference < 0:
                        assessment = "PATTERN_ONLY_HIGHER_DESCRIPTIVE"

                    else:
                        assessment = "EQUAL_DESCRIPTIVE"

                rows.append({
                    "timeframe": timeframe,
                    "horizon": horizon,
                    "period": period,
                    "shared_market_windows": data["shared"],
                    "pattern_only_complete_windows": data[
                        "only_complete"
                    ],
                    "pattern_only_hit_windows": data[
                        "only_hit"
                    ],
                    "pattern_only_hit_rate_pct": display_number(
                        only_rate
                    ),
                    "both_complete_windows": data[
                        "both_complete"
                    ],
                    "both_hit_windows": data["both_hit"],
                    "both_hit_rate_pct": display_number(
                        both_rate
                    ),
                    "difference_percentage_points": display_number(
                        difference
                    ),
                    "assessment": assessment,
                })

    return rows


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

        groups = get_groups(connection)

        if not groups:
            raise ValueError(
                "No signal groups found"
            )

        log(
            f"[PLAN] {len(groups):,} market/timeframe/horizon groups"
        )

        counters, paired, totals = audit_signals(
            connection,
            groups,
            metadata,
        )

    finally:
        connection.close()

    if totals["source_rows"] != source_count:
        raise ValueError(
            "Database source accounting mismatch"
        )

    selection_rows = build_selection_rows(
        counters
    )

    matched_rows = build_matched_rows(
        paired
    )

    if len(selection_rows) != 108:
        raise ValueError(
            "Expected 108 selection audit rows"
        )

    if len(matched_rows) != 18:
        raise ValueError(
            "Expected 18 matched comparison rows"
        )

    # Verify upstream files again before committing
    # the new audit outputs.
    verify_hash(
        SOURCE_DETAIL,
        metadata["source_detail_hash"],
    )

    verify_hash(
        SOURCE_RESULT,
        metadata["source_result_hash"],
    )

    verify_hash(
        INDEPENDENT_DETAIL,
        metadata["independent_detail_hash"],
    )

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    atomic_csv(
        OUTPUT_SELECTION,
        SELECTION_COLUMNS,
        selection_rows,
    )

    atomic_csv(
        OUTPUT_MATCHED,
        MATCHED_COLUMNS,
        matched_rows,
    )

    selection_hash = file_hash(
        OUTPUT_SELECTION
    )

    matched_hash = file_hash(
        OUTPUT_MATCHED
    )

    warnings = [
        (
            "This audit replays and checks the existing "
            "greedy selection algorithm. It does not "
            "prove that the algorithm is unbiased."
        ),
        (
            "Selection is chronological and classification-"
            "agnostic, but earlier Pattern 256 signals can "
            "block later BOTH signals and vice versa."
        ),
        (
            "Shared windows use fixed UTC-aligned nominal "
            "candle durations. Missing candles and "
            "different entry times can affect comparability."
        ),
        (
            "Matched-window results use the earliest "
            "complete source signal in each class/window. "
            "This is descriptive, not a causal comparison."
        ),
        (
            "Shared windows can overlap in underlying "
            "outcome periods across timeframes or markets; "
            "observations are not guaranteed independent."
        ),
        (
            "TRAIN/TEST periods are historical partitions "
            "of previously researched data, not genuinely "
            "unseen out-of-sample observations."
        ),
        (
            "30 percent future-high attainment does not "
            "represent executable net trading profit."
        ),
        (
            "The original independent validator excludes "
            "signals without entry timestamps before "
            "overlap selection. This audit reports them "
            "separately."
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
        "created_at_utc": now_utc(),
        "source_validation_status": (
            metadata["source"]["status"]
        ),
        "independent_validation_status": (
            metadata["independent"]["status"]
        ),
        "source_detail_sha256": (
            metadata["source_detail_hash"]
        ),
        "source_result_sha256": (
            metadata["source_result_hash"]
        ),
        "independent_detail_sha256": (
            metadata["independent_detail_hash"]
        ),
        "audit_method": (
            "GREEDY_SELECTION_REPLAY_AND_"
            "CLASSIFICATION_BIAS_AUDIT"
        ),
        "matched_method": (
            "SHARED_MARKET_FIXED_TIME_WINDOW_"
            "EARLIEST_COMPLETE_PER_CLASS"
        ),
        "timeframes": list(TIMEFRAMES),
        "horizons": list(HORIZONS),
        "classifications": list(CLASSIFICATIONS),
        "train_fraction": metadata[
            "independent"
        ]["train_fraction"],
        "boundaries": metadata["boundaries"],
        "group_count": len(groups),
        "source_rows": totals["source_rows"],
        "selected_rows": totals["selected_rows"],
        "excluded_rows": totals["excluded_total"],
        "excluded_overlap": totals["excluded_overlap"],
        "excluded_no_entry": totals["excluded_no_entry"],
        "selection_audit_rows": len(selection_rows),
        "matched_comparison_rows": len(matched_rows),
        "selection_audit_sha256": selection_hash,
        "matched_comparison_sha256": matched_hash,
        "elapsed_seconds": elapsed,
        "warnings": warnings,
    }

    manifest = {
        "program": PROGRAM,
        "version": VERSION,
        "created_at_utc": now_utc(),
        "inputs": {
            "validation_detail.csv": (
                metadata["source_detail_hash"]
            ),
            "validation_result.json": (
                metadata["source_result_hash"]
            ),
            "independent_detail.csv": (
                metadata["independent_detail_hash"]
            ),
        },
        "outputs": {
            "selection_audit.csv": selection_hash,
            "matched_window_comparison.csv": matched_hash,
        },
        "source_database": str(
            DATABASE.relative_to(ROOT)
        ),
        "source_database_rows": source_count,
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
    log("[PASS] Selection bias audit completed")
    log(
        f"[PASS] Source rows: {totals['source_rows']:,}"
    )
    log(
        f"[PASS] Selected rows: {totals['selected_rows']:,}"
    )
    log(
        f"[PASS] Overlap excluded: "
        f"{totals['excluded_overlap']:,}"
    )
    log(
        f"[PASS] No-entry excluded: "
        f"{totals['excluded_no_entry']:,}"
    )
    log(
        f"[PASS] Selection audit rows: "
        f"{len(selection_rows)}"
    )
    log(
        f"[PASS] Matched comparison rows: "
        f"{len(matched_rows)}"
    )
    log(
        f"[PASS] Elapsed: {elapsed:.1f}s"
    )
    log(
        f"[OUTPUT] {OUTPUT_DIR}"
    )
    log("=" * 72)

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())

    except KeyboardInterrupt:
        log("[STOP] Interrupted safely.")
        sys.exit(130)

    except Exception:
        log("[FAIL] Selection bias audit failed")
        traceback.print_exc()
        sys.exit(1)
