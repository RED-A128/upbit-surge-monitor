
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
validate_pattern_256_accumulation_candidate_oos.py
Clean V002

Purpose
-------
Perform a leakage-aware, chronological out-of-sample audit of
Pattern 256 + Accumulation candidate signals.

This program deliberately distinguishes:

1. Historical descriptive candidate-rule research.
2. Signal-time features that could have been known in real time.
3. Future outcome labels used only for evaluation.
4. Chronological train/test separation.

IMPORTANT
---------
The historical candidate_rule_research.csv contains aggregated,
outcome-dependent research results.

Those aggregate rule results are NOT valid live entry features.

This program therefore does not pretend that those historical
rules have already passed an out-of-sample test.

Instead, it evaluates a separately declared signal-time
candidate rule using individual validation records.

No automatic trading.
No live scanner activation.
No source OHLCV modification.
No upstream report modification.

Expected upstream files
-----------------------
data/reports/pattern_256_accumulation_candidate_rules/
    candidate_rule_research.csv
    candidate_rule_comparison.csv
    candidate_rule_result.json
    candidate_rule_manifest.json

data/reports/pattern_256_accumulation_30pct/
    validation_detail.csv

Output
------
data/reports/pattern_256_accumulation_candidate_oos/
    candidate_oos_summary.csv
    candidate_oos_yearly.csv
    candidate_oos_result.json
    candidate_oos_manifest.json

Execution
---------
py validate_pattern_256_accumulation_candidate_oos.py --root "C:\\Users\\USER\\Documents\\upbit-surge-monitor"

Methodology
-----------
- Chronological split by signal timestamp.
- Separate splits for each timeframe.
- Split by unique signal events, not horizon rows.
- Embargo around the train/test boundary.
- No future outcome in signal eligibility.
- No optimization of rule thresholds on the test period.
- Explicitly report insufficient sample sizes.
- Treat incomplete horizons as censored.
- Never convert censored observations into misses.
- Group repeated horizons by one unique signal event.
- No statistical independence claim.
- No trading profitability claim.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import sys
import tempfile

from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


PROGRAM = "validate_pattern_256_accumulation_candidate_oos.py"
VERSION = "Clean V002"

SOURCE_CANDIDATE_DIR = (
    "data/reports/pattern_256_accumulation_candidate_rules"
)

SOURCE_VALIDATION_DIR = (
    "data/reports/pattern_256_accumulation_30pct"
)

OUTPUT_DIR = (
    "data/reports/pattern_256_accumulation_candidate_oos"
)

CANDIDATE_FILES = (
    "candidate_rule_research.csv",
    "candidate_rule_comparison.csv",
    "candidate_rule_result.json",
    "candidate_rule_manifest.json",
)

DETAIL_FILE = "validation_detail.csv"

TIMEFRAMES = ("h1", "h4", "d1")
HORIZONS = (24, 72, 168)

CLASSIFICATIONS = (
    "PATTERN_256_ONLY",
    "BOTH",
)

# Fixed before examining the OOS results.
#
# These are provisional signal-time eligibility rules.
# They are NOT the same as the historical R00-R04 rules.
#
# Historical outcome-based thresholds must not be
# reinterpreted as real-time features.
SIGNAL_RULES = (
    {
        "rule_id": "S00_ALL_SIGNALS",
        "description": "All eligible Pattern 256 signals",
        "min_pattern_256_score": 0.0,
        "min_accumulation_score": 0.0,
        "require_both": False,
    },
    {
        "rule_id": "S01_PATTERN_60",
        "description": "Pattern 256 score >= 60",
        "min_pattern_256_score": 60.0,
        "min_accumulation_score": 0.0,
        "require_both": False,
    },
    {
        "rule_id": "S02_PATTERN_70",
        "description": "Pattern 256 score >= 70",
        "min_pattern_256_score": 70.0,
        "min_accumulation_score": 0.0,
        "require_both": False,
    },
    {
        "rule_id": "S03_BOTH_PATTERN_60",
        "description": "BOTH and Pattern 256 score >= 60",
        "min_pattern_256_score": 60.0,
        "min_accumulation_score": 0.0,
        "require_both": True,
    },
    {
        "rule_id": "S04_BOTH_STRICT",
        "description": (
            "BOTH, Pattern 256 score >= 70, "
            "Accumulation score >= 60"
        ),
        "min_pattern_256_score": 70.0,
        "min_accumulation_score": 60.0,
        "require_both": True,
    },
)

# Chronological split fraction.
TRAIN_FRACTION = 0.70

# Conservative exclusion around the split.
#
# At least 168 bars must separate the last training
# signal and the first OOS signal.
#
# This is expressed in elapsed hours for each timeframe.
TIMEFRAME_HOURS = {
    "h1": 1,
    "h4": 4,
    "d1": 24,
}

EMBARGO_BARS = 168

MIN_OOS_EVENTS = 30

PROGRESS_INTERVAL = 500_000

REQUIRED_DETAIL_COLUMNS = (
    "timeframe",
    "market",
    "signal_timestamp",
    "classification",
    "pattern_256_score",
    "accumulation_score",
    "entry_timestamp",
    "entry_price",
    "horizon",
    "status",
    "hit_30pct",
    "observed_bars",
    "required_bars",
)


def log(message: str) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] {message}", flush=True)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)

            if not chunk:
                break

            digest.update(chunk)

    return digest.hexdigest()


def parse_timestamp(value: str) -> datetime:
    text = str(value).strip()

    if not text:
        raise ValueError("Empty timestamp.")

    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    result = datetime.fromisoformat(text)

    if result.tzinfo is None:
        raise ValueError(
            "Timezone-naive timestamp is not permitted."
        )

    return result.astimezone(timezone.utc)


def parse_float(value: str, field: str) -> float:
    number = float(value)

    if not math.isfinite(number):
        raise ValueError(
            f"Non-finite value for {field}: {value}"
        )

    return number


def parse_int(value: str, field: str) -> int:
    text = str(value).strip()

    if not text:
        raise ValueError(f"Empty integer field: {field}")

    result = int(text)

    return result


def safe_rate(numerator: int, denominator: int) -> Optional[float]:
    if denominator <= 0:
        return None

    return round(
        100.0 * numerator / denominator,
        8,
    )


def atomic_write_json(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    fd, temporary_name = tempfile.mkstemp(
        prefix=path.name + ".",
        suffix=".tmp",
        dir=str(path.parent),
    )

    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(
                data,
                handle,
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )

            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())

        os.replace(temporary_name, path)

    finally:
        if os.path.exists(temporary_name):
            os.unlink(temporary_name)


def atomic_write_csv(
    path: Path,
    fieldnames: List[str],
    rows: List[Dict[str, Any]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    fd, temporary_name = tempfile.mkstemp(
        prefix=path.name + ".",
        suffix=".tmp",
        dir=str(path.parent),
    )

    try:
        with os.fdopen(
            fd,
            "w",
            encoding="utf-8",
            newline="",
        ) as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=fieldnames,
                extrasaction="ignore",
            )

            writer.writeheader()
            writer.writerows(rows)

            handle.flush()
            os.fsync(handle.fileno())

        os.replace(temporary_name, path)

    finally:
        if os.path.exists(temporary_name):
            os.unlink(temporary_name)


def verify_candidate_metadata(
    candidate_dir: Path,
) -> Dict[str, Any]:
    log("[CHECK] Candidate research metadata")

    paths = {
        name: candidate_dir / name
        for name in CANDIDATE_FILES
    }

    for name, path in paths.items():
        if not path.is_file():
            raise FileNotFoundError(
                f"Candidate research file missing: {path}"
            )

    with paths["candidate_rule_result.json"].open(
        "r",
        encoding="utf-8-sig",
    ) as handle:
        result = json.load(handle)

    with paths["candidate_rule_manifest.json"].open(
        "r",
        encoding="utf-8-sig",
    ) as handle:
        manifest = json.load(handle)

    if result.get("status") != "PASS":
        raise ValueError(
            "Candidate research result is not PASS."
        )

    if manifest.get("status") != "PASS":
        raise ValueError(
            "Candidate research manifest is not PASS."
        )

    if result.get("program") != (
        "research_pattern_256_accumulation_candidate_rules.py"
    ):
        raise ValueError(
            "Unexpected candidate research program."
        )

    expected_assessment = (
        "DESCRIPTIVE_CANDIDATE_RULE_RESEARCH_ONLY"
    )

    if result.get("assessment") != expected_assessment:
        raise ValueError(
            "Candidate research assessment mismatch."
        )

    methodology = result.get("methodology", {})

    if methodology.get(
        "historical_outcomes_allowed_as_live_features"
    ) is not False:
        raise ValueError(
            "Candidate research does not prohibit "
            "future outcome features."
        )

    if methodology.get("out_of_sample_verified") is not False:
        raise ValueError(
            "Candidate research unexpectedly claims OOS."
        )

    expected_hashes = manifest.get("outputs", {})

    for name in (
        "candidate_rule_research.csv",
        "candidate_rule_comparison.csv",
        "candidate_rule_result.json",
    ):
        expected = expected_hashes.get(name)

        if not isinstance(expected, str):
            raise ValueError(
                f"Missing manifest hash for {name}"
            )

        if len(expected) != 64:
            raise ValueError(
                f"Invalid manifest hash for {name}"
            )

        actual = sha256_file(paths[name])

        if actual.lower() != expected.lower():
            raise ValueError(
                f"Candidate research SHA256 mismatch: {name}"
            )

    log("[PASS] Candidate research metadata")

    return {
        "result": result,
        "manifest": manifest,
        "hashes": {
            name: sha256_file(path)
            for name, path in paths.items()
        },
    }


def signal_rule_passes(
    rule: Dict[str, Any],
    classification: str,
    pattern_score: float,
    accumulation_score: float,
) -> bool:
    if rule["require_both"] and classification != "BOTH":
        return False

    if pattern_score < rule["min_pattern_256_score"]:
        return False

    if accumulation_score < rule["min_accumulation_score"]:
        return False

    return True


def create_event_key(
    timeframe: str,
    market: str,
    signal_timestamp: str,
) -> Tuple[str, str, str]:
    return (
        timeframe,
        market,
        signal_timestamp,
    )


def scan_validation_detail(
    detail_path: Path,
) -> Tuple[
    Dict[str, Dict[Tuple[str, str, str], Dict[str, Any]]],
    Dict[str, Any],
]:
    log("[SCAN] Reading individual validation records")

    events = {
        timeframe: {}
        for timeframe in TIMEFRAMES
    }

    source_rows = 0
    eligible_rows = 0
    excluded_rows = 0

    with detail_path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as handle:
        reader = csv.DictReader(handle)

        columns = set(reader.fieldnames or [])

        missing = set(REQUIRED_DETAIL_COLUMNS) - columns

        if missing:
            raise ValueError(
                "Validation detail missing columns: "
                + ", ".join(sorted(missing))
            )

        for row in reader:
            source_rows += 1

            if source_rows % PROGRESS_INTERVAL == 0:
                log(
                    f"[SCAN] {source_rows:,} rows processed"
                )

            timeframe = str(
                row["timeframe"]
            ).strip()

            classification = str(
                row["classification"]
            ).strip()

            if timeframe not in TIMEFRAMES:
                excluded_rows += 1
                continue

            if classification not in CLASSIFICATIONS:
                excluded_rows += 1
                continue

            market = str(row["market"]).strip()

            if not market:
                raise ValueError(
                    f"Missing market at row {source_rows}"
                )

            signal_dt = parse_timestamp(
                row["signal_timestamp"]
            )

            entry_dt = parse_timestamp(
                row["entry_timestamp"]
            )

            if entry_dt <= signal_dt:
                raise ValueError(
                    "Entry timestamp must follow signal timestamp."
                )

            pattern_score = parse_float(
                row["pattern_256_score"],
                "pattern_256_score",
            )

            accumulation_score = parse_float(
                row["accumulation_score"],
                "accumulation_score",
            )

            entry_price = parse_float(
                row["entry_price"],
                "entry_price",
            )

            if entry_price <= 0:
                raise ValueError(
                    "Entry price must be positive."
                )

            horizon = parse_int(
                row["horizon"],
                "horizon",
            )

            if horizon not in HORIZONS:
                excluded_rows += 1
                continue

            status = str(
                row["status"]
            ).strip().upper()

            observed_bars = parse_int(
                row["observed_bars"],
                "observed_bars",
            )

            required_bars = parse_int(
                row["required_bars"],
                "required_bars",
            )

            if required_bars != horizon:
                raise ValueError(
                    "Required bars and horizon mismatch."
                )

            if observed_bars < 0:
                raise ValueError(
                    "Negative observed bars."
                )

            if observed_bars > required_bars:
                raise ValueError(
                    "Observed bars exceed required bars."
                )

            # Only a fully observed HIT/MISS is an evaluable outcome.
            # Early HIT, incomplete and unknown outcomes are censored;
            # never silently count them as MISS.
            complete = (
                observed_bars == required_bars
                and status in ("HIT", "MISS")
            )

            hit_value = str(row["hit_30pct"] or "").strip().upper()
            if hit_value in ("1", "1.0", "TRUE", "T", "YES", "Y", "HIT"):
                hit = 1
            elif hit_value in ("0", "0.0", "FALSE", "F", "NO", "N", "MISS"):
                hit = 0
            elif hit_value in ("", "NONE", "NULL", "NAN", "NA", "N/A"):
                hit = None
            else:
                raise ValueError(
                    f"Invalid HIT indicator at CSV row {source_rows + 1}: "
                    f"status={status!r}, hit_30pct={hit_value!r}"
                )

            if complete:
                if hit is None:
                    raise ValueError(
                        f"Missing HIT indicator for complete outcome "
                        f"at CSV row {source_rows + 1}: status={status!r}"
                    )
                if status == "HIT" and hit != 1:
                    raise ValueError(
                        f"HIT status and indicator mismatch "
                        f"at CSV row {source_rows + 1}"
                    )
                if status == "MISS" and hit != 0:
                    raise ValueError(
                        f"MISS status and indicator mismatch "
                        f"at CSV row {source_rows + 1}"
                    )

            key = create_event_key(
                timeframe,
                market,
                signal_dt.isoformat(),
            )

            group = events[timeframe]

            if key not in group:
                group[key] = {
                    "timeframe": timeframe,
                    "market": market,
                    "signal_timestamp": signal_dt,
                    "entry_timestamp": entry_dt,
                    "classification": classification,
                    "pattern_256_score": pattern_score,
                    "accumulation_score": accumulation_score,
                    "entry_price": entry_price,
                    "outcomes": {},
                }

            event = group[key]

            if (
                event["classification"] != classification
                or event["pattern_256_score"] != pattern_score
                or event["accumulation_score"] != accumulation_score
                or event["entry_timestamp"] != entry_dt
                or event["entry_price"] != entry_price
            ):
                raise ValueError(
                    "Inconsistent repeated signal event."
                )

            if horizon in event["outcomes"]:
                raise ValueError(
                    "Duplicate horizon for signal event."
                )

            event["outcomes"][horizon] = {
                "complete": complete,
                "hit": hit if complete else None,
                "status": status,
                "observed_bars": observed_bars,
            }

            eligible_rows += 1

    if source_rows == 0:
        raise ValueError(
            "Validation detail contains no data rows."
        )

    event_count = sum(
        len(group)
        for group in events.values()
    )

    if event_count == 0:
        raise ValueError(
            "No eligible signal events."
        )

    log(
        f"[PASS] Source rows: {source_rows:,}"
    )

    log(
        f"[PASS] Eligible rows: {eligible_rows:,}"
    )

    log(
        f"[PASS] Unique signal events: {event_count:,}"
    )

    return events, {
        "source_rows": source_rows,
        "eligible_rows": eligible_rows,
        "excluded_rows": excluded_rows,
        "unique_events": event_count,
    }


def chronological_split(
    events: Dict[
        str,
        Dict[Tuple[str, str, str], Dict[str, Any]],
    ],
) -> Tuple[
    Dict[str, Dict[str, List[Dict[str, Any]]]],
    Dict[str, Any],
]:
    log("[CHECK] Chronological train/OOS split")

    partitions = {}
    metadata = {}

    for timeframe in TIMEFRAMES:
        ordered = sorted(
            events[timeframe].values(),
            key=lambda item: (
                item["signal_timestamp"],
                item["market"],
            ),
        )

        total = len(ordered)

        if total < 2:
            partitions[timeframe] = {
                "train": [],
                "embargo": [],
                "oos": [],
            }

            metadata[timeframe] = {
                "total_events": total,
                "train_events": 0,
                "embargo_events": total,
                "oos_events": 0,
                "status": "INSUFFICIENT_EVENTS",
            }

            continue

        split_index = int(
            total * TRAIN_FRACTION
        )

        split_index = max(
            1,
            min(split_index, total - 1),
        )

        boundary = ordered[
            split_index
        ]["signal_timestamp"]

        embargo_seconds = (
            EMBARGO_BARS
            * TIMEFRAME_HOURS[timeframe]
            * 3600
        )

        train = []
        embargo = []
        oos = []

        for event in ordered:
            timestamp = event["signal_timestamp"]

            seconds_from_boundary = (
                timestamp - boundary
            ).total_seconds()

            if seconds_from_boundary < -embargo_seconds:
                train.append(event)

            elif seconds_from_boundary >= 0:
                oos.append(event)

            else:
                embargo.append(event)

        # All training labels require a fully observed
        # maximum horizon before the OOS boundary.
        #
        # The embargo removes train-side signals whose
        # maximum horizon could overlap the OOS start.

        if train and oos:
            last_train = max(
                item["signal_timestamp"]
                for item in train
            )

            first_oos = min(
                item["signal_timestamp"]
                for item in oos
            )

            if (
                first_oos - last_train
            ).total_seconds() < embargo_seconds:
                raise ValueError(
                    f"Embargo violation: {timeframe}"
                )

        partitions[timeframe] = {
            "train": train,
            "embargo": embargo,
            "oos": oos,
        }

        metadata[timeframe] = {
            "total_events": total,
            "train_events": len(train),
            "embargo_events": len(embargo),
            "oos_events": len(oos),
            "oos_boundary": boundary.isoformat(),
            "embargo_bars": EMBARGO_BARS,
            "embargo_hours": (
                EMBARGO_BARS
                * TIMEFRAME_HOURS[timeframe]
            ),
            "status": (
                "READY"
                if len(oos) >= MIN_OOS_EVENTS
                else "INSUFFICIENT_OOS_EVENTS"
            ),
        }

        log(
            f"[SPLIT] {timeframe}: "
            f"train={len(train):,}, "
            f"embargo={len(embargo):,}, "
            f"oos={len(oos):,}"
        )

    return partitions, metadata


def summarize_events(
    events: List[Dict[str, Any]],
    rule: Dict[str, Any],
    horizon: int,
) -> Dict[str, Any]:
    selected = 0
    complete = 0
    hits = 0
    misses = 0
    censored = 0

    for event in events:
        if not signal_rule_passes(
            rule,
            event["classification"],
            event["pattern_256_score"],
            event["accumulation_score"],
        ):
            continue

        selected += 1

        outcome = event["outcomes"].get(horizon)

        if outcome is None:
            censored += 1
            continue

        if not outcome["complete"]:
            censored += 1
            continue

        complete += 1

        if outcome["hit"] == 1:
            hits += 1
        else:
            misses += 1

    if complete != hits + misses:
        raise ValueError(
            "Completed outcome accounting mismatch."
        )

    if selected != complete + censored:
        raise ValueError(
            "Selected event accounting mismatch."
        )

    return {
        "selected_events": selected,
        "complete_events": complete,
        "hit_events": hits,
        "miss_events": misses,
        "censored_events": censored,
        "hit_rate_pct": safe_rate(
            hits,
            complete,
        ),
    }


def make_summary_rows(
    partitions: Dict[
        str,
        Dict[str, List[Dict[str, Any]]],
    ],
) -> List[Dict[str, Any]]:
    rows = []

    for timeframe in TIMEFRAMES:
        for rule in SIGNAL_RULES:
            for horizon in HORIZONS:
                train_stats = summarize_events(
                    partitions[timeframe]["train"],
                    rule,
                    horizon,
                )

                oos_stats = summarize_events(
                    partitions[timeframe]["oos"],
                    rule,
                    horizon,
                )

                train_rate = train_stats[
                    "hit_rate_pct"
                ]

                oos_rate = oos_stats[
                    "hit_rate_pct"
                ]

                if (
                    train_rate is None
                    or oos_rate is None
                ):
                    difference = None
                else:
                    difference = round(
                        oos_rate - train_rate,
                        8,
                    )

                if (
                    oos_stats["complete_events"]
                    < MIN_OOS_EVENTS
                ):
                    status = "INSUFFICIENT_OOS_SAMPLE"
                else:
                    status = "DESCRIPTIVE_OOS_ONLY"

                rows.append({
                    "timeframe": timeframe,
                    "rule_id": rule["rule_id"],
                    "horizon": horizon,
                    "train_selected_events": train_stats[
                        "selected_events"
                    ],
                    "train_complete_events": train_stats[
                        "complete_events"
                    ],
                    "train_hit_events": train_stats[
                        "hit_events"
                    ],
                    "train_censored_events": train_stats[
                        "censored_events"
                    ],
                    "train_hit_rate_pct": train_rate,
                    "oos_selected_events": oos_stats[
                        "selected_events"
                    ],
                    "oos_complete_events": oos_stats[
                        "complete_events"
                    ],
                    "oos_hit_events": oos_stats[
                        "hit_events"
                    ],
                    "oos_censored_events": oos_stats[
                        "censored_events"
                    ],
                    "oos_hit_rate_pct": oos_rate,
                    "oos_minus_train_pp": difference,
                    "status": status,
                })

    return rows


def make_yearly_rows(
    partitions: Dict[
        str,
        Dict[str, List[Dict[str, Any]]],
    ],
) -> List[Dict[str, Any]]:
    rows = []

    for timeframe in TIMEFRAMES:
        oos_events = partitions[
            timeframe
        ]["oos"]

        years = sorted({
            event["signal_timestamp"].year
            for event in oos_events
        })

        for year in years:
            year_events = [
                event
                for event in oos_events
                if event["signal_timestamp"].year == year
            ]

            for rule in SIGNAL_RULES:
                for horizon in HORIZONS:
                    stats = summarize_events(
                        year_events,
                        rule,
                        horizon,
                    )

                    rows.append({
                        "timeframe": timeframe,
                        "year": year,
                        "rule_id": rule["rule_id"],
                        "horizon": horizon,
                        **stats,
                        "status": (
                            "DESCRIPTIVE_YEARLY_ONLY"
                        ),
                    })

    return rows


def verify_accounting(
    summary_rows: List[Dict[str, Any]],
) -> None:
    expected = (
        len(TIMEFRAMES)
        * len(SIGNAL_RULES)
        * len(HORIZONS)
    )

    if len(summary_rows) != expected:
        raise ValueError(
            "Unexpected summary row count."
        )

    keys = set()

    for row in summary_rows:
        key = (
            row["timeframe"],
            row["rule_id"],
            row["horizon"],
        )

        if key in keys:
            raise ValueError(
                "Duplicate summary combination."
            )

        keys.add(key)

        for prefix in ("train", "oos"):
            selected = int(
                row[f"{prefix}_selected_events"]
            )

            complete = int(
                row[f"{prefix}_complete_events"]
            )

            censored = int(
                row[f"{prefix}_censored_events"]
            )

            hits = int(
                row[f"{prefix}_hit_events"]
            )

            if selected != complete + censored:
                raise ValueError(
                    "Selected event accounting mismatch."
                )

            if hits > complete:
                raise ValueError(
                    "HIT count exceeds completed count."
                )

    log("[PASS] OOS accounting verification")


def prepare_output_directory(
    root: Path,
) -> Path:
    output = root / OUTPUT_DIR

    # Never silently overwrite prior research.
    if output.exists():
        raise FileExistsError(
            "OOS output already exists. "
            "Refusing to overwrite: "
            f"{output}"
        )

    output.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    output.mkdir(
        parents=False,
        exist_ok=False,
    )

    return output


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Chronological Pattern 256 + Accumulation "
            "candidate OOS validation"
        )
    )

    parser.add_argument(
        "--root",
        required=True,
        help="Project root directory",
    )

    args = parser.parse_args()

    root = Path(
        args.root
    ).expanduser().resolve()

    if not root.is_dir():
        raise NotADirectoryError(
            f"Project root missing: {root}"
        )

    log("=" * 70)
    log(f"{PROGRAM} / {VERSION}")
    log("=" * 70)

    candidate_dir = root / SOURCE_CANDIDATE_DIR
    detail_path = root / SOURCE_VALIDATION_DIR / DETAIL_FILE

    if not detail_path.is_file():
        raise FileNotFoundError(
            f"Validation detail missing: {detail_path}"
        )

    # Capture immutable upstream hashes.
    source_hashes_before = {
        DETAIL_FILE: sha256_file(detail_path),
    }

    for filename in CANDIDATE_FILES:
        path = candidate_dir / filename

        if not path.is_file():
            raise FileNotFoundError(
                f"Candidate input missing: {path}"
            )

        source_hashes_before[
            filename
        ] = sha256_file(path)

    metadata = verify_candidate_metadata(
        candidate_dir
    )

    events, scan_metadata = scan_validation_detail(
        detail_path
    )

    partitions, split_metadata = chronological_split(
        events
    )

    summary_rows = make_summary_rows(
        partitions
    )

    yearly_rows = make_yearly_rows(
        partitions
    )

    verify_accounting(
        summary_rows
    )

    # Check upstream data before creating any outputs.
    source_hashes_after = {
        DETAIL_FILE: sha256_file(detail_path),
    }

    for filename in CANDIDATE_FILES:
        source_hashes_after[
            filename
        ] = sha256_file(
            candidate_dir / filename
        )

    if source_hashes_before != source_hashes_after:
        raise ValueError(
            "Upstream inputs changed during analysis."
        )

    log("[PASS] Upstream input hashes unchanged")

    output_dir = prepare_output_directory(
        root
    )

    summary_path = (
        output_dir / "candidate_oos_summary.csv"
    )

    yearly_path = (
        output_dir / "candidate_oos_yearly.csv"
    )

    result_path = (
        output_dir / "candidate_oos_result.json"
    )

    manifest_path = (
        output_dir / "candidate_oos_manifest.json"
    )

    summary_fields = [
        "timeframe",
        "rule_id",
        "horizon",
        "train_selected_events",
        "train_complete_events",
        "train_hit_events",
        "train_censored_events",
        "train_hit_rate_pct",
        "oos_selected_events",
        "oos_complete_events",
        "oos_hit_events",
        "oos_censored_events",
        "oos_hit_rate_pct",
        "oos_minus_train_pp",
        "status",
    ]

    yearly_fields = [
        "timeframe",
        "year",
        "rule_id",
        "horizon",
        "selected_events",
        "complete_events",
        "hit_events",
        "miss_events",
        "censored_events",
        "hit_rate_pct",
        "status",
    ]

    atomic_write_csv(
        summary_path,
        summary_fields,
        summary_rows,
    )

    atomic_write_csv(
        yearly_path,
        yearly_fields,
        yearly_rows,
    )

    result = {
        "status": "PASS",
        "program": PROGRAM,
        "version": VERSION,
        "assessment": "DESCRIPTIVE_CHRONOLOGICAL_OOS_ONLY",
        "source_program": (
            "research_pattern_256_accumulation_candidate_rules.py"
        ),
        "source_validation_file": DETAIL_FILE,
        "source_rows": scan_metadata["source_rows"],
        "eligible_rows": scan_metadata["eligible_rows"],
        "excluded_rows": scan_metadata["excluded_rows"],
        "unique_signal_events": scan_metadata["unique_events"],
        "summary_rows": len(summary_rows),
        "yearly_rows": len(yearly_rows),
        "rule_count": len(SIGNAL_RULES),
        "timeframes": list(TIMEFRAMES),
        "horizons": list(HORIZONS),
        "train_fraction": TRAIN_FRACTION,
        "embargo_bars": EMBARGO_BARS,
        "min_oos_events": MIN_OOS_EVENTS,
        "splits": split_metadata,
        "signal_rules": list(SIGNAL_RULES),
        "methodology": {
            "chronological_split": True,
            "split_by_unique_signal_event": True,
            "embargo_applied": True,
            "future_outcomes_used_as_features": False,
            "historical_aggregate_rules_used_as_live_features": False,
            "signal_time_features_only": True,
            "thresholds_optimized_on_oos": False,
            "oos_outcomes_used_for_evaluation_only": True,
            "incomplete_horizons_treated_as_censored": True,
            "statistical_significance_tested": False,
            "independent_events_proven": False,
            "trading_return_estimated": False,
            "fees_and_slippage_modeled": False,
            "live_scanner_ready": False,
            "automatic_trading_enabled": False,
        },
        "limitations": [
            (
                "The S00-S04 rules are provisional "
                "signal-time filters, not the historical "
                "R00-R04 outcome-based research rules."
            ),
            (
                "A chronological split alone does not "
                "prove predictive effectiveness."
            ),
            (
                "The signal scores must be independently "
                "verified as causal and reproducible."
            ),
            (
                "Repeated and overlapping market events "
                "may be statistically dependent."
            ),
            (
                "Intrabar order, trading fees, slippage "
                "and execution feasibility are not modeled."
            ),
            (
                "Rule selection after inspecting OOS "
                "results invalidates a pristine holdout."
            ),
        ],
        "input_sha256": source_hashes_before,
    }

    result["candidate_oos_summary_sha256"] = sha256_file(
        summary_path
    )

    result["candidate_oos_yearly_sha256"] = sha256_file(
        yearly_path
    )

    atomic_write_json(
        result_path,
        result,
    )

    manifest = {
        "status": "PASS",
        "program": PROGRAM,
        "version": VERSION,
        "inputs": source_hashes_before,
        "outputs": {
            "candidate_oos_summary.csv": sha256_file(
                summary_path
            ),
            "candidate_oos_yearly.csv": sha256_file(
                yearly_path
            ),
            "candidate_oos_result.json": sha256_file(
                result_path
            ),
        },
    }

    atomic_write_json(
        manifest_path,
        manifest,
    )

    # Final source integrity check.
    if sha256_file(detail_path) != source_hashes_before[
        DETAIL_FILE
    ]:
        raise ValueError(
            "Validation detail changed during output."
        )

    for filename in CANDIDATE_FILES:
        if sha256_file(
            candidate_dir / filename
        ) != source_hashes_before[filename]:
            raise ValueError(
                f"Upstream file changed: {filename}"
            )

    log("[PASS] Final upstream integrity verification")

    log("=" * 70)
    log("[PASS] Candidate OOS validation completed")
    log(f"Source rows: {scan_metadata['source_rows']:,}")
    log(f"Unique events: {scan_metadata['unique_events']:,}")
    log(f"Summary rows: {len(summary_rows):,}")
    log(f"Yearly rows: {len(yearly_rows):,}")
    log(f"Output: {output_dir}")
    log("=" * 70)

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())

    except Exception as exc:
        log(f"[FAIL] {exc}")
        raise
