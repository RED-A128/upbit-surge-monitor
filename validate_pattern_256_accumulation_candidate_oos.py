
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
validate_pattern_256_accumulation_candidate_oos.py
Clean V003

Pattern 256 + Accumulation Candidate OOS Validation

- Chronological train/OOS split
- Unique signal event grouping
- 168-bar embargo
- No future outcomes in signal eligibility
- No OOS threshold optimization
- Incomplete outcomes treated as censored
- Missing entry records excluded explicitly
- Upstream SHA256 integrity verification
- No source OHLCV modification
- No automatic trading

Execution:
py validate_pattern_256_accumulation_candidate_oos.py --root "C:\\Users\\USER\\Documents\\upbit-surge-monitor"
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

from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PROGRAM = "validate_pattern_256_accumulation_candidate_oos.py"
VERSION = "Clean V003"

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

TRAIN_FRACTION = 0.70

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

SUMMARY_FIELDS = [
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

YEARLY_FIELDS = [
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


def parse_timestamp(value: Any) -> datetime:
    text = str(value if value is not None else "").strip()

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


def parse_float(value: Any, field: str) -> float:
    number = float(value)

    if not math.isfinite(number):
        raise ValueError(
            f"Non-finite value for {field}: {value}"
        )

    return number


def parse_int(value: Any, field: str) -> int:
    text = str(value if value is not None else "").strip()

    if not text:
        raise ValueError(f"Empty integer field: {field}")

    return int(text)


def safe_rate(
    numerator: int,
    denominator: int,
) -> float | None:
    if denominator <= 0:
        return None

    return round(100.0 * numerator / denominator, 8)


def atomic_write_json(
    path: Path,
    data: dict[str, Any],
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
        ) as handle:
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
    fieldnames: list[str],
    rows: list[dict[str, Any]],
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
) -> dict[str, Any]:
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

    if result.get("assessment") != (
        "DESCRIPTIVE_CANDIDATE_RULE_RESEARCH_ONLY"
    ):
        raise ValueError(
            "Candidate research assessment mismatch."
        )

    methodology = result.get("methodology", {})

    if methodology.get(
        "historical_outcomes_allowed_as_live_features"
    ) is not False:
        raise ValueError(
            "Future outcome features are not prohibited."
        )

    if methodology.get(
        "out_of_sample_verified"
    ) is not False:
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
    rule: dict[str, Any],
    classification: str,
    pattern_score: float,
    accumulation_score: float,
) -> bool:
    if (
        rule["require_both"]
        and classification != "BOTH"
    ):
        return False

    if pattern_score < rule["min_pattern_256_score"]:
        return False

    if accumulation_score < rule["min_accumulation_score"]:
        return False

    return True


def parse_hit_indicator(
    value: Any,
    row_number: int,
) -> int | None:
    text = str(
        value if value is not None else ""
    ).strip().upper()

    if text in (
        "1", "1.0", "TRUE", "T",
        "YES", "Y", "HIT",
    ):
        return 1

    if text in (
        "0", "0.0", "FALSE", "F",
        "NO", "N", "MISS",
    ):
        return 0

    if text in (
        "", "NONE", "NULL",
        "NAN", "NA", "N/A",
    ):
        return None

    raise ValueError(
        f"Invalid HIT indicator at CSV row {row_number}: "
        f"{text!r}"
    )


def scan_validation_detail(
    detail_path: Path,
) -> tuple[dict[str, dict], dict[str, Any]]:
    log("[SCAN] Reading individual validation records")

    events = {
        timeframe: {}
        for timeframe in TIMEFRAMES
    }

    source_rows = 0
    eligible_rows = 0
    excluded_rows = 0

    exclusion_reasons = Counter()
    missing_entry_keys = set()
    seen_valid_keys = set()

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
            csv_row = source_rows + 1

            if source_rows % PROGRESS_INTERVAL == 0:
                log(
                    f"[SCAN] {source_rows:,} rows processed"
                )

            timeframe = str(
                row["timeframe"] or ""
            ).strip()

            classification = str(
                row["classification"] or ""
            ).strip()

            if timeframe not in TIMEFRAMES:
                excluded_rows += 1
                exclusion_reasons["unsupported_timeframe"] += 1
                continue

            if classification not in CLASSIFICATIONS:
                excluded_rows += 1
                exclusion_reasons["unsupported_classification"] += 1
                continue

            horizon = parse_int(
                row["horizon"],
                "horizon",
            )

            if horizon not in HORIZONS:
                excluded_rows += 1
                exclusion_reasons["unsupported_horizon"] += 1
                continue

            market = str(
                row["market"] or ""
            ).strip()

            if not market:
                raise ValueError(
                    f"Missing market at CSV row {csv_row}"
                )

            try:
                signal_dt = parse_timestamp(
                    row["signal_timestamp"]
                )
            except ValueError as exc:
                raise ValueError(
                    f"Invalid signal timestamp at "
                    f"CSV row {csv_row}: {exc}"
                ) from exc

            key = (
                timeframe,
                market,
                signal_dt.isoformat(),
            )

            entry_text = str(
                row["entry_timestamp"] or ""
            ).strip()

            entry_price_text = str(
                row["entry_price"] or ""
            ).strip()

            # A signal without an actual entry cannot be
            # evaluated as a successful or failed trade.
            # Preserve an explicit exclusion count.
            if not entry_text or not entry_price_text:
                excluded_rows += 1

                if not entry_text:
                    exclusion_reasons[
                        "missing_entry_timestamp"
                    ] += 1
                else:
                    exclusion_reasons[
                        "missing_entry_price"
                    ] += 1

                missing_entry_keys.add(key)
                continue

            try:
                entry_dt = parse_timestamp(entry_text)
            except ValueError as exc:
                raise ValueError(
                    f"Invalid entry timestamp at "
                    f"CSV row {csv_row}: {exc}"
                ) from exc

            if entry_dt <= signal_dt:
                raise ValueError(
                    f"Entry timestamp must follow signal "
                    f"timestamp at CSV row {csv_row}"
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
                entry_price_text,
                "entry_price",
            )

            if entry_price <= 0:
                raise ValueError(
                    f"Entry price must be positive "
                    f"at CSV row {csv_row}"
                )

            status = str(
                row["status"] or ""
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
                    f"Required bars and horizon mismatch "
                    f"at CSV row {csv_row}"
                )

            if not 0 <= observed_bars <= required_bars:
                raise ValueError(
                    f"Invalid observed bars "
                    f"at CSV row {csv_row}"
                )

            hit = parse_hit_indicator(
                row["hit_30pct"],
                csv_row,
            )

            complete = (
                observed_bars == required_bars
                and status in ("HIT", "MISS")
            )

            if complete:
                if hit is None:
                    raise ValueError(
                        f"Missing HIT indicator for complete "
                        f"outcome at CSV row {csv_row}"
                    )

                if status == "HIT" and hit != 1:
                    raise ValueError(
                        f"HIT indicator mismatch "
                        f"at CSV row {csv_row}"
                    )

                if status == "MISS" and hit != 0:
                    raise ValueError(
                        f"MISS indicator mismatch "
                        f"at CSV row {csv_row}"
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
                    f"Inconsistent repeated signal event "
                    f"at CSV row {csv_row}"
                )

            if horizon in event["outcomes"]:
                raise ValueError(
                    f"Duplicate horizon for signal event "
                    f"at CSV row {csv_row}"
                )

            event["outcomes"][horizon] = {
                "complete": complete,
                "hit": hit if complete else None,
                "status": status,
                "observed_bars": observed_bars,
            }

            seen_valid_keys.add(key)
            eligible_rows += 1

    if source_rows == 0:
        raise ValueError(
            "Validation detail contains no data rows."
        )

    # If any horizon of a signal lacks an entry, exclude
    # the whole event. Do not retain other horizons of
    # the same event as though entry data were complete.
    partially_missing = (
        missing_entry_keys & seen_valid_keys
    )

    removed_valid_rows = 0

    for key in partially_missing:
        timeframe = key[0]
        event = events[timeframe].pop(key, None)

        if event is not None:
            count = len(event["outcomes"])
            removed_valid_rows += count
            eligible_rows -= count
            excluded_rows += count

            exclusion_reasons[
                "event_with_inconsistent_entry_availability"
            ] += count

    event_count = sum(
        len(group)
        for group in events.values()
    )

    if event_count == 0:
        raise ValueError(
            "No eligible signal events after exclusions."
        )

    if source_rows != eligible_rows + excluded_rows:
        raise ValueError(
            "Source row accounting mismatch."
        )

    log(f"[PASS] Source rows: {source_rows:,}")
    log(f"[PASS] Eligible rows: {eligible_rows:,}")
    log(f"[PASS] Excluded rows: {excluded_rows:,}")
    log(f"[PASS] Unique signal events: {event_count:,}")

    for reason, count in sorted(
        exclusion_reasons.items()
    ):
        log(f"[EXCLUDED] {reason}: {count:,}")

    return events, {
        "source_rows": source_rows,
        "eligible_rows": eligible_rows,
        "excluded_rows": excluded_rows,
        "unique_events": event_count,
        "missing_entry_event_keys": len(
            missing_entry_keys
        ),
        "partially_missing_entry_events": len(
            partially_missing
        ),
        "removed_valid_rows": removed_valid_rows,
        "exclusion_reasons": dict(
            sorted(exclusion_reasons.items())
        ),
    }


def chronological_split(
    events: dict[str, dict],
) -> tuple[dict[str, dict], dict[str, Any]]:
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
                "embargo": ordered,
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
    events: list[dict[str, Any]],
    rule: dict[str, Any],
    horizon: int,
) -> dict[str, Any]:
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

        if outcome is None or not outcome["complete"]:
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
        "hit_rate_pct": safe_rate(hits, complete),
    }


def make_summary_rows(
    partitions: dict[str, dict],
) -> list[dict[str, Any]]:
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

                status = (
                    "DESCRIPTIVE_OOS_ONLY"
                    if oos_stats["complete_events"]
                    >= MIN_OOS_EVENTS
                    else "INSUFFICIENT_OOS_SAMPLE"
                )

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
    partitions: dict[str, dict],
) -> list[dict[str, Any]]:
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
                        "status": "DESCRIPTIVE_YEARLY_ONLY",
                    })

    return rows


def verify_accounting(
    summary_rows: list[dict[str, Any]],
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


def capture_input_hashes(
    candidate_dir: Path,
    detail_path: Path,
) -> dict[str, str]:
    hashes = {
        DETAIL_FILE: sha256_file(detail_path),
    }

    for filename in CANDIDATE_FILES:
        path = candidate_dir / filename

        if not path.is_file():
            raise FileNotFoundError(
                f"Candidate input missing: {path}"
            )

        hashes[filename] = sha256_file(path)

    return hashes


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

    detail_path = (
        root / SOURCE_VALIDATION_DIR / DETAIL_FILE
    )

    if not detail_path.is_file():
        raise FileNotFoundError(
            f"Validation detail missing: {detail_path}"
        )

    source_hashes_before = capture_input_hashes(
        candidate_dir,
        detail_path,
    )

    verify_candidate_metadata(candidate_dir)

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

    verify_accounting(summary_rows)

    source_hashes_after = capture_input_hashes(
        candidate_dir,
        detail_path,
    )

    if source_hashes_before != source_hashes_after:
        raise ValueError(
            "Upstream inputs changed during analysis."
        )

    log("[PASS] Upstream input hashes unchanged")

    output_dir = prepare_output_directory(root)

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

    atomic_write_csv(
        summary_path,
        SUMMARY_FIELDS,
        summary_rows,
    )

    atomic_write_csv(
        yearly_path,
        YEARLY_FIELDS,
        yearly_rows,
    )

    result = {
        "status": "PASS",
        "program": PROGRAM,
        "version": VERSION,
        "assessment": (
            "DESCRIPTIVE_CHRONOLOGICAL_OOS_ONLY"
        ),
        "source_program": (
            "research_pattern_256_accumulation_candidate_rules.py"
        ),
        "source_validation_file": DETAIL_FILE,
        "source_rows": scan_metadata["source_rows"],
        "eligible_rows": scan_metadata["eligible_rows"],
        "excluded_rows": scan_metadata["excluded_rows"],
        "unique_signal_events": scan_metadata[
            "unique_events"
        ],
        "exclusion_reasons": scan_metadata[
            "exclusion_reasons"
        ],
        "missing_entry_event_keys": scan_metadata[
            "missing_entry_event_keys"
        ],
        "partially_missing_entry_events": scan_metadata[
            "partially_missing_entry_events"
        ],
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
            "missing_entry_records_excluded": True,
            "missing_entry_events_not_counted_as_misses": True,
            "statistical_significance_tested": False,
            "independent_events_proven": False,
            "trading_return_estimated": False,
            "fees_and_slippage_modeled": False,
            "live_scanner_ready": False,
            "automatic_trading_enabled": False,
        },
        "limitations": [
            (
                "S00-S04 are provisional signal-time filters, "
                "not historical outcome-based R00-R04 rules."
            ),
            (
                "Chronological splitting alone does not "
                "prove predictive effectiveness."
            ),
            (
                "Signal scores require independent "
                "causality and reproducibility checks."
            ),
            (
                "Overlapping market events may be "
                "statistically dependent."
            ),
            (
                "Intrabar order, trading fees, slippage "
                "and execution feasibility are not modeled."
            ),
            (
                "Rules selected after examining OOS "
                "results invalidate a pristine holdout."
            ),
            (
                "Missing-entry signals are excluded "
                "rather than assigned HIT or MISS. "
                "Exclusion can introduce selection bias."
            ),
        ],
        "input_sha256": source_hashes_before,
    }

    result["candidate_oos_summary_sha256"] = (
        sha256_file(summary_path)
    )

    result["candidate_oos_yearly_sha256"] = (
        sha256_file(yearly_path)
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

    final_hashes = capture_input_hashes(
        candidate_dir,
        detail_path,
    )

    if final_hashes != source_hashes_before:
        raise ValueError(
            "Upstream inputs changed during output."
        )

    log("[PASS] Final upstream integrity verification")

    log("=" * 70)
    log("[PASS] Candidate OOS validation completed")
    log(f"Source rows: {scan_metadata['source_rows']:,}")
    log(f"Eligible rows: {scan_metadata['eligible_rows']:,}")
    log(f"Excluded rows: {scan_metadata['excluded_rows']:,}")
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
