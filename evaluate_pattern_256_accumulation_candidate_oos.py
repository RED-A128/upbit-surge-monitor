
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
evaluate_pattern_256_accumulation_candidate_oos.py
Clean V001

Purpose
-------
Evaluate candidate rule performance on chronological
train / out-of-sample (OOS) data.

Safety
------
- Read-only access to upstream research files.
- No trading or order execution.
- No changes to OHLCV files.
- No automatic deletion or overwrite of existing reports.
- SHA256 verification of source files.
- Strict validation of required columns and labels.
- No automatic claim of predictive superiority.

Usage
-----
py evaluate_pattern_256_accumulation_candidate_oos.py --root "C:\\Users\\USER\\Documents\\upbit-surge-monitor"

Optional
--------
--input-dir
--output-dir
--chunk-size
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import re
import sys
import traceback

from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


PROGRAM_NAME = "evaluate_pattern_256_accumulation_candidate_oos.py"
PROGRAM_VERSION = "Clean V001"

DEFAULT_INPUT_RELATIVE = (
    "data/reports/pattern_256_accumulation_candidate_oos"
)

DEFAULT_OUTPUT_RELATIVE = (
    "data/reports/pattern_256_accumulation_candidate_oos_evaluation"
)

OUTPUT_FILES = {
    "summary": "oos_evaluation_summary.csv",
    "comparison": "oos_evaluation_comparison.csv",
    "decision": "oos_evaluation_decision.csv",
    "result": "oos_evaluation_result.json",
    "manifest": "oos_evaluation_manifest.json",
}

CHUNK_SIZE_DEFAULT = 100_000

TIMEFRAME_ALIASES = (
    "timeframe",
    "tf",
    "interval",
    "candle_interval",
)

SPLIT_ALIASES = (
    "split",
    "dataset_split",
    "sample_split",
    "period",
    "partition",
    "data_split",
)

GROUP_ALIASES = (
    "candidate_rule",
    "rule",
    "rule_name",
    "candidate",
    "candidate_name",
    "group",
    "group_name",
    "signal_group",
    "pattern_group",
)

HIT_ALIASES = (
    "hit",
    "is_hit",
    "target_hit",
    "hit_30pct",
    "hit_30",
    "hit30",
    "hit_30_percent",
    "target_30pct_hit",
    "outcome",
    "result",
    "label",
    "hit_status",
)

STATUS_ALIASES = (
    "evaluation_status",
    "outcome_status",
    "result_status",
    "status",
    "hit_miss",
    "hit_miss_status",
)

EVENT_ALIASES = (
    "event_id",
    "signal_event_id",
    "signal_id",
    "event_key",
)

COIN_ALIASES = (
    "market",
    "ticker",
    "symbol",
    "coin",
    "market_code",
)

RETURN_ALIASES = (
    "return_pct",
    "realized_return_pct",
    "net_return_pct",
    "pnl_pct",
    "profit_pct",
)

RISK_ALIASES = (
    "max_drawdown_pct",
    "max_adverse_excursion_pct",
    "mae_pct",
    "drawdown_pct",
)

HORIZON_ALIASES = (
    "horizon",
    "horizon_candles",
    "lookahead_candles",
    "forward_candles",
    "evaluation_horizon",
)

CSV_CANDIDATES = (
    "candidate_oos_detail.csv",
    "candidate_oos_events.csv",
    "candidate_oos_signals.csv",
    "candidate_oos_validation_detail.csv",
    "oos_validation_detail.csv",
    "oos_detail.csv",
    "oos_events.csv",
    "oos_signals.csv",
    "candidate_oos_summary.csv",
    "oos_summary.csv",
)

RESULT_CANDIDATES = (
    "candidate_oos_result.json",
    "oos_result.json",
)

SPLIT_VALUES = {
    "TRAIN": "TRAIN",
    "TRAINING": "TRAIN",
    "IN_SAMPLE": "TRAIN",
    "INSAMPLE": "TRAIN",
    "IS": "TRAIN",
    "OOS": "OOS",
    "OUT_OF_SAMPLE": "OOS",
    "OUTOFSAMPLE": "OOS",
    "TEST": "OOS",
    "EMBARGO": "EMBARGO",
    "PURGED": "EMBARGO",
    "PURGE": "EMBARGO",
}

TRUE_VALUES = {
    "1",
    "TRUE",
    "YES",
    "Y",
    "HIT",
    "WIN",
    "SUCCESS",
    "TARGET_HIT",
}

FALSE_VALUES = {
    "0",
    "FALSE",
    "NO",
    "N",
    "MISS",
    "LOSE",
    "LOSS",
    "TARGET_MISS",
}

INCOMPLETE_VALUES = {
    "",
    "NONE",
    "NULL",
    "NAN",
    "NA",
    "N/A",
    "PENDING",
    "INCOMPLETE",
    "UNKNOWN",
    "CENSORED",
    "NOT_EVALUATED",
    "NOT_COMPLETE",
    "UNRESOLVED",
}


def log(message: str) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}", flush=True)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_name(value: Any) -> str:
    text = str(value).strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return text.strip("_")


def normalize_value(value: Any) -> str:
    text = str(value).strip().upper()
    text = re.sub(r"[\s\-]+", "_", text)
    return text


def clean_string(value: Any) -> str:
    if value is None:
        return ""

    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass

    return str(value).strip()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as stream:
        while True:
            block = stream.read(1024 * 1024)

            if not block:
                break

            digest.update(block)

    return digest.hexdigest()


def read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8-sig") as stream:
        result = json.load(stream)

    if not isinstance(result, dict):
        raise ValueError(
            f"JSON root must be an object: {path}"
        )

    return result


def select_column(
    columns: list[str],
    aliases: tuple[str, ...],
) -> str | None:
    normalized = {
        normalize_name(column): column
        for column in columns
    }

    for alias in aliases:
        found = normalized.get(normalize_name(alias))

        if found is not None:
            return found

    return None


def parse_float(value: Any) -> float | None:
    text = clean_string(value)

    if not text:
        return None

    text = text.replace(",", "").replace("%", "")

    try:
        result = float(text)
    except (TypeError, ValueError):
        return None

    if not math.isfinite(result):
        return None

    return result


def parse_hit(value: Any) -> bool | None:
    text = normalize_value(clean_string(value))

    if text in TRUE_VALUES:
        return True

    if text in FALSE_VALUES:
        return False

    if text in INCOMPLETE_VALUES:
        return None

    raise ValueError(
        f"Unsupported HIT/MISS value: {value!r}"
    )


def parse_status(value: Any) -> bool | None:
    text = normalize_value(clean_string(value))

    if text in TRUE_VALUES:
        return True

    if text in FALSE_VALUES:
        return False

    if text in INCOMPLETE_VALUES:
        return None

    raise ValueError(
        f"Unsupported outcome status: {value!r}"
    )


def parse_split(value: Any) -> str:
    text = normalize_value(clean_string(value))

    result = SPLIT_VALUES.get(text)

    if result is None:
        raise ValueError(
            f"Unsupported train/OOS split: {value!r}"
        )

    return result


def detect_csv_type(
    path: Path,
) -> dict[str, Any] | None:
    try:
        header = pd.read_csv(
            path,
            nrows=0,
            encoding="utf-8-sig",
        )
    except (OSError, UnicodeError, pd.errors.ParserError):
        return None

    columns = [str(column) for column in header.columns]

    split_column = select_column(
        columns,
        SPLIT_ALIASES,
    )

    hit_column = select_column(
        columns,
        HIT_ALIASES,
    )

    status_column = select_column(
        columns,
        STATUS_ALIASES,
    )

    timeframe_column = select_column(
        columns,
        TIMEFRAME_ALIASES,
    )

    group_column = select_column(
        columns,
        GROUP_ALIASES,
    )

    if (
        split_column is None
        or timeframe_column is None
        or (
            hit_column is None
            and status_column is None
        )
    ):
        return None

    return {
        "path": path,
        "columns": columns,
        "split": split_column,
        "hit": hit_column,
        "status": status_column,
        "timeframe": timeframe_column,
        "group": group_column,
        "event": select_column(
            columns,
            EVENT_ALIASES,
        ),
        "coin": select_column(
            columns,
            COIN_ALIASES,
        ),
        "return": select_column(
            columns,
            RETURN_ALIASES,
        ),
        "risk": select_column(
            columns,
            RISK_ALIASES,
        ),
        "horizon": select_column(
            columns,
            HORIZON_ALIASES,
        ),
    }


def find_input_csv(
    input_dir: Path,
) -> dict[str, Any]:
    files = {
        path.name.lower(): path
        for path in input_dir.glob("*.csv")
        if path.is_file()
    }

    ordered: list[Path] = []

    for name in CSV_CANDIDATES:
        found = files.get(name.lower())

        if found is not None:
            ordered.append(found)

    for path in sorted(files.values()):
        if path not in ordered:
            ordered.append(path)

    detected = []

    for path in ordered:
        schema = detect_csv_type(path)

        if schema is not None:
            detected.append(schema)

    if not detected:
        available = ", ".join(sorted(files)) or "(none)"

        raise ValueError(
            "No suitable event-level OOS CSV found. "
            "Required: split, timeframe, HIT/MISS columns. "
            f"Available CSV files: {available}"
        )

    if len(detected) > 1:
        names = ", ".join(
            item["path"].name
            for item in detected
        )

        raise ValueError(
            "Multiple compatible OOS CSV files found. "
            "Selection would be ambiguous: "
            f"{names}"
        )

    return detected[0]


def verify_upstream_result(
    input_dir: Path,
) -> tuple[Path, dict[str, Any]]:
    available = []

    for name in RESULT_CANDIDATES:
        path = input_dir / name

        if path.is_file():
            available.append(path)

    if not available:
        for path in sorted(input_dir.glob("*.json")):
            if "result" in path.name.lower():
                available.append(path)

    if len(available) != 1:
        raise ValueError(
            "Expected exactly one OOS result JSON. "
            f"Found {len(available)}."
        )

    path = available[0]
    result = read_json(path)

    status = normalize_value(
        result.get("status", "")
    )

    if status != "PASS":
        raise ValueError(
            "Upstream OOS result status is not PASS."
        )

    return path, result


def wilson_interval(
    successes: int,
    total: int,
    z: float = 1.96,
) -> tuple[float | None, float | None]:
    if total <= 0:
        return None, None

    p = successes / total
    z2 = z * z

    denominator = 1 + z2 / total

    center = (
        p + z2 / (2 * total)
    ) / denominator

    radius = (
        z
        * math.sqrt(
            p * (1 - p) / total
            + z2 / (4 * total * total)
        )
        / denominator
    )

    return (
        max(0.0, center - radius),
        min(1.0, center + radius),
    )


def safe_rate(
    numerator: int,
    denominator: int,
) -> float | None:
    if denominator <= 0:
        return None

    return numerator / denominator


def safe_mean(
    total: float,
    count: int,
) -> float | None:
    if count <= 0:
        return None

    return total / count


def new_bucket() -> dict[str, Any]:
    return {
        "rows": 0,
        "hit": 0,
        "miss": 0,
        "incomplete": 0,
        "return_sum": 0.0,
        "return_count": 0,
        "risk_sum": 0.0,
        "risk_count": 0,
        "coins": Counter(),
        "events": set(),
    }


def update_bucket(
    bucket: dict[str, Any],
    hit: bool | None,
    return_value: float | None,
    risk_value: float | None,
    coin: str,
    event: str,
) -> None:
    bucket["rows"] += 1

    if hit is True:
        bucket["hit"] += 1
    elif hit is False:
        bucket["miss"] += 1
    else:
        bucket["incomplete"] += 1

    if return_value is not None:
        bucket["return_sum"] += return_value
        bucket["return_count"] += 1

    if risk_value is not None:
        bucket["risk_sum"] += risk_value
        bucket["risk_count"] += 1

    if coin:
        bucket["coins"][coin] += 1

    if event:
        bucket["events"].add(event)


def make_summary_row(
    key: tuple[str, str, str],
    bucket: dict[str, Any],
) -> dict[str, Any]:
    timeframe, group, split = key

    completed = (
        bucket["hit"] + bucket["miss"]
    )

    lower, upper = wilson_interval(
        bucket["hit"],
        completed,
    )

    coins = bucket["coins"]

    dominant_coin = ""
    dominant_coin_share = None

    if coins:
        dominant_coin, dominant_count = (
            coins.most_common(1)[0]
        )

        dominant_coin_share = safe_rate(
            dominant_count,
            sum(coins.values()),
        )

    return {
        "timeframe": timeframe,
        "candidate_rule": group,
        "split": split,
        "rows": bucket["rows"],
        "completed": completed,
        "hit": bucket["hit"],
        "miss": bucket["miss"],
        "incomplete": bucket["incomplete"],
        "hit_rate": safe_rate(
            bucket["hit"],
            completed,
        ),
        "hit_rate_ci95_low": lower,
        "hit_rate_ci95_high": upper,
        "completion_rate": safe_rate(
            completed,
            bucket["rows"],
        ),
        "mean_return_pct": safe_mean(
            bucket["return_sum"],
            bucket["return_count"],
        ),
        "mean_risk_pct": safe_mean(
            bucket["risk_sum"],
            bucket["risk_count"],
        ),
        "return_observations": bucket["return_count"],
        "risk_observations": bucket["risk_count"],
        "unique_coins": len(coins),
        "dominant_coin": dominant_coin,
        "dominant_coin_share": dominant_coin_share,
        "unique_event_ids": len(bucket["events"]),
    }


def scan_events(
    schema: dict[str, Any],
    chunk_size: int,
) -> tuple[
    list[dict[str, Any]],
    dict[str, Any],
]:
    path = schema["path"]

    buckets = defaultdict(new_bucket)

    total_rows = 0
    embargo_rows = 0
    missing_group_rows = 0

    log(f"[SCAN] {path.name}")

    reader = pd.read_csv(
        path,
        chunksize=chunk_size,
        dtype=str,
        keep_default_na=False,
        encoding="utf-8-sig",
        low_memory=False,
    )

    for chunk in reader:
        for row in chunk.to_dict("records"):
            total_rows += 1

            split = parse_split(
                row[schema["split"]]
            )

            if split == "EMBARGO":
                embargo_rows += 1
                continue

            timeframe = clean_string(
                row[schema["timeframe"]]
            ).lower()

            if not timeframe:
                raise ValueError(
                    f"Missing timeframe at row {total_rows}"
                )

            if schema["group"] is None:
                group = "ALL_CANDIDATES"
                missing_group_rows += 1
            else:
                group = clean_string(
                    row[schema["group"]]
                )

                if not group:
                    raise ValueError(
                        "Empty candidate rule at row "
                        f"{total_rows}"
                    )

            hit = None

            if schema["hit"] is not None:
                hit = parse_hit(
                    row[schema["hit"]]
                )

            if schema["status"] is not None:
                status = parse_status(
                    row[schema["status"]]
                )

                if (
                    hit is not None
                    and status is not None
                    and hit != status
                ):
                    raise ValueError(
                        "Conflicting HIT and status at row "
                        f"{total_rows}"
                    )

                if hit is None:
                    hit = status

            return_value = None

            if schema["return"] is not None:
                return_value = parse_float(
                    row[schema["return"]]
                )

            risk_value = None

            if schema["risk"] is not None:
                risk_value = parse_float(
                    row[schema["risk"]]
                )

            coin = ""

            if schema["coin"] is not None:
                coin = clean_string(
                    row[schema["coin"]]
                )

            event = ""

            if schema["event"] is not None:
                event = clean_string(
                    row[schema["event"]]
                )

            horizon = ""

            if schema["horizon"] is not None:
                horizon = clean_string(
                    row[schema["horizon"]]
                )

            # Different evaluation horizons must not be pooled.
            group_key = group

            if horizon:
                group_key = (
                    f"{group} | horizon={horizon}"
                )

            key = (
                timeframe,
                group_key,
                split,
            )

            update_bucket(
                buckets[key],
                hit,
                return_value,
                risk_value,
                coin,
                event,
            )

        if total_rows % 500_000 < len(chunk):
            log(
                f"[SCAN] {total_rows:,} rows processed"
            )

    summaries = [
        make_summary_row(key, bucket)
        for key, bucket in sorted(buckets.items())
    ]

    accounting = (
        sum(item["rows"] for item in summaries)
        + embargo_rows
    )

    if accounting != total_rows:
        raise ValueError(
            "OOS accounting mismatch: "
            f"{accounting} != {total_rows}"
        )

    metadata = {
        "source_rows": total_rows,
        "evaluated_split_rows": (
            total_rows - embargo_rows
        ),
        "embargo_rows": embargo_rows,
        "summary_groups": len(summaries),
        "group_column_available": (
            schema["group"] is not None
        ),
        "rows_using_all_candidates_group": (
            missing_group_rows
        ),
        "coin_column_available": (
            schema["coin"] is not None
        ),
        "event_column_available": (
            schema["event"] is not None
        ),
        "return_column_available": (
            schema["return"] is not None
        ),
        "risk_column_available": (
            schema["risk"] is not None
        ),
        "horizon_column_available": (
            schema["horizon"] is not None
        ),
    }

    log(
        "[PASS] Accounting: "
        f"{total_rows:,} source rows"
    )

    return summaries, metadata


def build_comparisons(
    summaries: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    grouped = defaultdict(dict)

    for item in summaries:
        key = (
            item["timeframe"],
            item["candidate_rule"],
        )

        grouped[key][item["split"]] = item

    comparisons = []

    for key in sorted(grouped):
        timeframe, group = key

        train = grouped[key].get("TRAIN")
        oos = grouped[key].get("OOS")

        train_rate = (
            train["hit_rate"]
            if train is not None
            else None
        )

        oos_rate = (
            oos["hit_rate"]
            if oos is not None
            else None
        )

        difference = None

        if (
            train_rate is not None
            and oos_rate is not None
        ):
            difference = oos_rate - train_rate

        comparisons.append({
            "timeframe": timeframe,
            "candidate_rule": group,
            "train_rows": (
                train["rows"] if train else 0
            ),
            "oos_rows": (
                oos["rows"] if oos else 0
            ),
            "train_completed": (
                train["completed"] if train else 0
            ),
            "oos_completed": (
                oos["completed"] if oos else 0
            ),
            "train_hit": (
                train["hit"] if train else 0
            ),
            "oos_hit": (
                oos["hit"] if oos else 0
            ),
            "train_hit_rate": train_rate,
            "oos_hit_rate": oos_rate,
            "oos_minus_train_hit_rate": difference,
            "train_mean_return_pct": (
                train["mean_return_pct"]
                if train else None
            ),
            "oos_mean_return_pct": (
                oos["mean_return_pct"]
                if oos else None
            ),
            "train_mean_risk_pct": (
                train["mean_risk_pct"]
                if train else None
            ),
            "oos_mean_risk_pct": (
                oos["mean_risk_pct"]
                if oos else None
            ),
            "oos_unique_coins": (
                oos["unique_coins"]
                if oos else 0
            ),
            "oos_dominant_coin_share": (
                oos["dominant_coin_share"]
                if oos else None
            ),
        })

    return comparisons


def build_decisions(
    comparisons: list[dict[str, Any]],
    metadata: dict[str, Any],
) -> list[dict[str, Any]]:
    decisions = []

    for item in comparisons:
        reasons = []

        train_n = item["train_completed"]
        oos_n = item["oos_completed"]

        if train_n == 0:
            reasons.append("NO_TRAIN_EVALUATIONS")

        if oos_n == 0:
            reasons.append("NO_OOS_EVALUATIONS")

        if oos_n < 100:
            reasons.append("SMALL_OOS_SAMPLE")

        if not metadata["coin_column_available"]:
            reasons.append("COIN_CONCENTRATION_NOT_VERIFIED")
        elif (
            item["oos_dominant_coin_share"] is not None
            and item["oos_dominant_coin_share"] > 0.5
        ):
            reasons.append("HIGH_COIN_CONCENTRATION")

        if not metadata["event_column_available"]:
            reasons.append("SIGNAL_DEPENDENCE_NOT_VERIFIED")

        if not metadata["risk_column_available"]:
            reasons.append("RISK_NOT_VERIFIED")

        if not metadata["return_column_available"]:
            reasons.append("NET_RETURN_NOT_VERIFIED")

        difference = item[
            "oos_minus_train_hit_rate"
        ]

        if difference is not None:
            if difference < -0.05:
                reasons.append("OOS_HIT_RATE_DETERIORATION")
            elif difference > 0:
                reasons.append("OOS_HIT_RATE_HIGHER_THAN_TRAIN")
            else:
                reasons.append("OOS_HIT_RATE_NOT_HIGHER")

        # A high hit rate alone is not sufficient to approve
        # a trading strategy. Always require further validation.
        decision = "FURTHER_VALIDATION_REQUIRED"

        if train_n == 0 or oos_n == 0:
            decision = "INSUFFICIENT_DATA"
        elif oos_n < 100:
            decision = "INSUFFICIENT_SAMPLE"
        elif (
            difference is not None
            and difference < -0.05
        ):
            decision = "REVIEW_FOR_OVERFITTING"

        decisions.append({
            "timeframe": item["timeframe"],
            "candidate_rule": item["candidate_rule"],
            "decision": decision,
            "reasons": "|".join(reasons),
            "oos_completed": oos_n,
            "oos_hit_rate": item["oos_hit_rate"],
            "oos_minus_train_hit_rate": difference,
            "approved_for_live_trading": False,
        })

    return decisions


def write_csv(
    path: Path,
    rows: list[dict[str, Any]],
    columns: list[str],
) -> None:
    with path.open(
        "w",
        encoding="utf-8-sig",
        newline="",
    ) as stream:
        writer = csv.DictWriter(
            stream,
            fieldnames=columns,
            extrasaction="ignore",
        )

        writer.writeheader()

        for row in rows:
            writer.writerow(row)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Evaluate candidate rule OOS performance."
        )
    )

    parser.add_argument(
        "--root",
        required=True,
        help="Production project root.",
    )

    parser.add_argument(
        "--input-dir",
        default=None,
        help="Optional OOS report directory.",
    )

    parser.add_argument(
        "--output-dir",
        default=None,
        help="Optional evaluation output directory.",
    )

    parser.add_argument(
        "--chunk-size",
        type=int,
        default=CHUNK_SIZE_DEFAULT,
    )

    return parser.parse_args()


def main() -> int:
    args = parse_args()

    log("=" * 70)
    log(f"{PROGRAM_NAME} / {PROGRAM_VERSION}")
    log("=" * 70)

    if args.chunk_size <= 0:
        raise ValueError(
            "chunk-size must be positive."
        )

    root = Path(args.root).expanduser().resolve(
        strict=True
    )

    if not root.is_dir():
        raise ValueError(
            "Production root is not a directory."
        )

    input_dir = (
        Path(args.input_dir)
        if args.input_dir
        else root / DEFAULT_INPUT_RELATIVE
    ).expanduser().resolve(strict=True)

    output_dir = (
        Path(args.output_dir)
        if args.output_dir
        else root / DEFAULT_OUTPUT_RELATIVE
    ).expanduser().resolve(strict=False)

    if not input_dir.is_dir():
        raise ValueError(
            f"OOS input directory missing: {input_dir}"
        )

    if output_dir == input_dir:
        raise ValueError(
            "Input and output directories must differ."
        )

    if input_dir in output_dir.parents:
        raise ValueError(
            "Output directory cannot be inside input directory."
        )

    if output_dir in input_dir.parents:
        raise ValueError(
            "Output directory cannot contain input directory."
        )

    if output_dir.exists():
        raise FileExistsError(
            "Evaluation output already exists. "
            "Refusing to overwrite: "
            f"{output_dir}"
        )

    log("[CHECK] Upstream OOS result")

    result_path, upstream_result = (
        verify_upstream_result(input_dir)
    )

    log("[PASS] Upstream OOS result")

    log("[CHECK] Event-level CSV schema")

    schema = find_input_csv(input_dir)

    log(
        f"[PASS] Event-level CSV: "
        f"{schema['path'].name}"
    )

    log("[CHECK] Source SHA256")

    input_files = [
        schema["path"],
        result_path,
    ]

    before_hashes = {
        str(path): sha256_file(path)
        for path in input_files
    }

    summaries, metadata = scan_events(
        schema,
        args.chunk_size,
    )

    comparisons = build_comparisons(
        summaries
    )

    decisions = build_decisions(
        comparisons,
        metadata,
    )

    log("[CHECK] Source SHA256 unchanged")

    for path in input_files:
        actual = sha256_file(path)
        expected = before_hashes[str(path)]

        if actual != expected:
            raise ValueError(
                "Upstream input SHA256 changed: "
                f"{path}"
            )

    log("[PASS] Source SHA256 unchanged")

    if not summaries:
        raise ValueError(
            "No train/OOS records available."
        )

    if not comparisons:
        raise ValueError(
            "No comparable candidate groups."
        )

    output_dir.mkdir(
        parents=True,
        exist_ok=False,
    )

    summary_columns = [
        "timeframe",
        "candidate_rule",
        "split",
        "rows",
        "completed",
        "hit",
        "miss",
        "incomplete",
        "hit_rate",
        "hit_rate_ci95_low",
        "hit_rate_ci95_high",
        "completion_rate",
        "mean_return_pct",
        "mean_risk_pct",
        "return_observations",
        "risk_observations",
        "unique_coins",
        "dominant_coin",
        "dominant_coin_share",
        "unique_event_ids",
    ]

    comparison_columns = [
        "timeframe",
        "candidate_rule",
        "train_rows",
        "oos_rows",
        "train_completed",
        "oos_completed",
        "train_hit",
        "oos_hit",
        "train_hit_rate",
        "oos_hit_rate",
        "oos_minus_train_hit_rate",
        "train_mean_return_pct",
        "oos_mean_return_pct",
        "train_mean_risk_pct",
        "oos_mean_risk_pct",
        "oos_unique_coins",
        "oos_dominant_coin_share",
    ]

    decision_columns = [
        "timeframe",
        "candidate_rule",
        "decision",
        "reasons",
        "oos_completed",
        "oos_hit_rate",
        "oos_minus_train_hit_rate",
        "approved_for_live_trading",
    ]

    write_csv(
        output_dir / OUTPUT_FILES["summary"],
        summaries,
        summary_columns,
    )

    write_csv(
        output_dir / OUTPUT_FILES["comparison"],
        comparisons,
        comparison_columns,
    )

    write_csv(
        output_dir / OUTPUT_FILES["decision"],
        decisions,
        decision_columns,
    )

    result = {
        "program": PROGRAM_NAME,
        "version": PROGRAM_VERSION,
        "status": "PASS",
        "created_at_utc": utc_now(),
        "input_directory": str(input_dir),
        "input_csv": schema["path"].name,
        "upstream_result": result_path.name,
        "upstream_status": upstream_result.get(
            "status"
        ),
        "source_sha256": before_hashes,
        "metadata": metadata,
        "summary_rows": len(summaries),
        "comparison_rows": len(comparisons),
        "decision_rows": len(decisions),
        "approved_for_live_trading": False,
        "limitations": [
            "Execution PASS does not establish predictive superiority.",
            "Wilson intervals assume independent observations.",
            "Repeated signals may be correlated.",
            "Candidate rules may have been selected using earlier data.",
            "No multiple-testing adjustment is applied.",
            "Missing returns or risks cannot be inferred.",
            "HIT rate is not equivalent to realized trading profit.",
            "Further walk-forward and independent validation are required.",
        ],
    }

    result_path_output = (
        output_dir / OUTPUT_FILES["result"]
    )

    with result_path_output.open(
        "w",
        encoding="utf-8",
    ) as stream:
        json.dump(
            result,
            stream,
            ensure_ascii=False,
            indent=2,
        )

    manifest = {
        "program": PROGRAM_NAME,
        "version": PROGRAM_VERSION,
        "created_at_utc": utc_now(),
        "source_sha256": before_hashes,
        "output_sha256": {},
    }

    for key in (
        "summary",
        "comparison",
        "decision",
        "result",
    ):
        filename = OUTPUT_FILES[key]

        manifest["output_sha256"][filename] = (
            sha256_file(output_dir / filename)
        )

    with (
        output_dir / OUTPUT_FILES["manifest"]
    ).open(
        "w",
        encoding="utf-8",
    ) as stream:
        json.dump(
            manifest,
            stream,
            ensure_ascii=False,
            indent=2,
        )

    log("=" * 70)
    log("[PASS] Candidate OOS evaluation completed")
    log(
        f"Source rows: "
        f"{metadata['source_rows']:,}"
    )
    log(f"Summary rows: {len(summaries)}")
    log(f"Comparison rows: {len(comparisons)}")
    log(f"Decision rows: {len(decisions)}")
    log(f"Output: {output_dir}")
    log("=" * 70)

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        log(f"[FAIL] {exc}")
        traceback.print_exc()
        sys.exit(1)
