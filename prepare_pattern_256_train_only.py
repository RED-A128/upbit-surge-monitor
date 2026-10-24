#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
prepare_pattern_256_train_only.py

Pattern 256 TRAIN-ONLY Dataset Preparation
Clean V001

Purpose:
    Prepare strictly historical TRAIN-only event labels.

Safety:
    - Never modify source data.
    - Never include TEST observations.
    - Exclude outcomes crossing the TRAIN cutoff.
    - Exclude incomplete or censored outcomes.
    - Verify source and output SHA256.
    - Resume only after output integrity verification.
    - No model training.
    - No automated trading.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import sys
import tempfile

from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path


VERSION = "Clean V001"

DEFAULT_CUTOFF = "2025-01-01T00:00:00+00:00"

SOURCE_RELATIVE = Path(
    "data/reports/"
    "pattern_256_accumulation_candidate_independent_backtest/"
    "backtest_trades.csv"
)

AUDIT_RELATIVE = Path(
    "data/reports/"
    "pattern_256_feature_integrity/"
    "feature_integrity_summary.json"
)

OUTPUT_RELATIVE = Path(
    "data/reports/"
    "pattern_256_train_only"
)

OUTPUT_CSV = "train_only_events.csv"
OUTPUT_RESULT = "train_only_result.json"
OUTPUT_MANIFEST = "train_only_manifest.json"

TIMEFRAME_HOURS = {
    "h1": 1,
    "h4": 4,
    "d1": 24,
}

VALID_CLASSES = {
    "PATTERN_256_ONLY",
    "BOTH",
}

OUTPUT_COLUMNS = [
    "timeframe",
    "market",
    "signal_timestamp",
    "classification",
    "horizon",
    "entry_timestamp",
    "entry_price",
    "target_price",
    "status",
    "hit_30pct",
    "outcome_maturity_timestamp",
]

REQUIRED_COLUMNS = {
    "timeframe",
    "market",
    "signal_timestamp",
    "classification",
    "horizon",
    "entry_timestamp",
    "entry_price",
    "target_price",
    "status",
    "hit_30pct",
    "observed_bars",
    "required_bars",
}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        while True:
            block = handle.read(1024 * 1024)

            if not block:
                break

            digest.update(block)

    return digest.hexdigest()


def parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(
        value.strip().replace("Z", "+00:00")
    )

    if parsed.tzinfo is None:
        raise ValueError(
            f"Timezone missing: {value}"
        )

    return parsed.astimezone(timezone.utc)


def utc_string(value: datetime) -> str:
    return value.astimezone(
        timezone.utc
    ).isoformat()


def write_json_atomic(
    path: Path,
    value: dict,
) -> None:
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    descriptor, temporary = tempfile.mkstemp(
        prefix=".train_only_",
        suffix=".json.tmp",
        dir=str(path.parent),
    )

    try:
        with os.fdopen(
            descriptor,
            "w",
            encoding="utf-8",
        ) as handle:
            json.dump(
                value,
                handle,
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )

            handle.write("\n")

        os.replace(
            temporary,
            path,
        )

    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def load_json(path: Path) -> dict:
    with path.open(
        "r",
        encoding="utf-8-sig",
    ) as handle:
        result = json.load(handle)

    if not isinstance(result, dict):
        raise ValueError(
            f"JSON object expected: {path}"
        )

    return result


def verify_audit(path: Path) -> dict:
    if not path.is_file():
        raise FileNotFoundError(
            f"Feature audit missing: {path}"
        )

    audit = load_json(path)

    required_true = [
        "feature_integrity_verified",
        "gap_audit_completed",
    ]

    for field in required_true:
        if audit.get(field) is not True:
            raise ValueError(
                f"Feature audit not verified: {field}"
            )

    if audit.get("execution_status") != "PASS":
        raise ValueError(
            "Feature audit execution is not PASS"
        )

    if audit.get("approved_for_live_trading") is not False:
        raise ValueError(
            "Unexpected live trading approval"
        )

    return audit


def verify_existing(
    output_dir: Path,
    source_hash: str,
    audit_hash: str,
    cutoff: str,
) -> bool:
    csv_path = output_dir / OUTPUT_CSV
    result_path = output_dir / OUTPUT_RESULT
    manifest_path = output_dir / OUTPUT_MANIFEST

    if not all(
        path.is_file()
        for path in (
            csv_path,
            result_path,
            manifest_path,
        )
    ):
        return False

    try:
        manifest = load_json(
            manifest_path
        )

        result = load_json(
            result_path
        )

        if manifest.get("version") != VERSION:
            return False

        if result.get("version") != VERSION:
            return False

        if result.get("execution_status") != "PASS":
            return False

        if result.get("train_cutoff") != cutoff:
            return False

        inputs = manifest.get(
            "input_sha256",
            {},
        )

        outputs = manifest.get(
            "output_sha256",
            {},
        )

        if inputs.get(
            "backtest_trades.csv"
        ) != source_hash:
            return False

        if inputs.get(
            "feature_integrity_summary.json"
        ) != audit_hash:
            return False

        if sha256_file(csv_path) != outputs.get(
            OUTPUT_CSV
        ):
            return False

        if sha256_file(result_path) != outputs.get(
            OUTPUT_RESULT
        ):
            return False

        if result.get(
            "approved_for_live_trading"
        ) is not False:
            return False

        if result.get(
            "train_experiment_completed"
        ) is not False:
            return False

        expected_rows = result.get(
            "selected_rows"
        )

        if not isinstance(expected_rows, int):
            return False

        with csv_path.open(
            "r",
            encoding="utf-8",
            newline="",
        ) as handle:
            reader = csv.reader(handle)

            header = next(reader, None)

            if header != OUTPUT_COLUMNS:
                return False

            actual_rows = sum(
                1 for _ in reader
            )

        return actual_rows == expected_rows

    except (
        OSError,
        ValueError,
        TypeError,
        KeyError,
        json.JSONDecodeError,
    ):
        return False


def build_dataset(
    source: Path,
    temporary_csv: Path,
    cutoff: datetime,
) -> dict:
    counts = Counter()

    selected_by_group = Counter()

    # Prevent repeated observations in the selected dataset.
    # A key represents one market/signal/class/horizon.
    selected_keys = set()

    with source.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as source_handle:

        reader = csv.DictReader(
            source_handle
        )

        if not reader.fieldnames:
            raise ValueError(
                "Source CSV has no header"
            )

        missing = REQUIRED_COLUMNS - set(
            reader.fieldnames
        )

        if missing:
            raise ValueError(
                "Missing source columns: "
                + ", ".join(sorted(missing))
            )

        with temporary_csv.open(
            "w",
            encoding="utf-8",
            newline="",
        ) as output_handle:

            writer = csv.DictWriter(
                output_handle,
                fieldnames=OUTPUT_COLUMNS,
                lineterminator="\n",
            )

            writer.writeheader()

            for row in reader:
                counts["source_rows"] += 1

                timeframe = (
                    row["timeframe"] or ""
                ).strip()

                classification = (
                    row["classification"] or ""
                ).strip()

                status = (
                    row["status"] or ""
                ).strip().upper()

                if timeframe not in TIMEFRAME_HOURS:
                    counts[
                        "excluded_unknown_timeframe"
                    ] += 1
                    continue

                if classification not in VALID_CLASSES:
                    counts[
                        "excluded_other_classification"
                    ] += 1
                    continue

                if status not in {"HIT", "MISS"}:
                    counts[
                        "excluded_incomplete"
                    ] += 1
                    continue

                try:
                    horizon = int(
                        row["horizon"]
                    )

                    required = int(
                        row["required_bars"]
                    )

                    observed = int(
                        row["observed_bars"]
                    )

                except (ValueError, TypeError):
                    counts[
                        "excluded_invalid_bar_counts"
                    ] += 1
                    continue

                if (
                    horizon <= 0
                    or required != horizon
                    or observed < required
                ):
                    counts[
                        "excluded_insufficient_bars"
                    ] += 1
                    continue

                signal_text = (
                    row["signal_timestamp"] or ""
                ).strip()

                entry_text = (
                    row["entry_timestamp"] or ""
                ).strip()

                if not signal_text or not entry_text:
                    counts[
                        "excluded_missing_timestamps"
                    ] += 1
                    continue

                signal = parse_time(
                    signal_text
                )

                entry = parse_time(
                    entry_text
                )

                if entry <= signal:
                    counts[
                        "excluded_invalid_time_order"
                    ] += 1
                    continue

                if signal >= cutoff:
                    counts[
                        "excluded_test_period"
                    ] += 1
                    continue

                maturity = entry + timedelta(
                    hours=(
                        TIMEFRAME_HOURS[timeframe]
                        * horizon
                    )
                )

                # The entire theoretical outcome window
                # must end strictly before TRAIN cutoff.
                if maturity >= cutoff:
                    counts[
                        "excluded_cross_boundary"
                    ] += 1
                    continue

                hit_text = (
                    row["hit_30pct"] or ""
                ).strip()

                if hit_text not in {"0", "1"}:
                    counts[
                        "excluded_invalid_hit_label"
                    ] += 1
                    continue

                hit = int(hit_text)

                if (
                    (status == "HIT" and hit != 1)
                    or (status == "MISS" and hit != 0)
                ):
                    raise ValueError(
                        "Inconsistent HIT/MISS label "
                        f"at source row "
                        f"{counts['source_rows']}"
                    )

                market = (
                    row["market"] or ""
                ).strip()

                if not market:
                    counts[
                        "excluded_missing_market"
                    ] += 1
                    continue

                key = (
                    timeframe,
                    market,
                    utc_string(signal),
                    classification,
                    horizon,
                )

                if key in selected_keys:
                    raise ValueError(
                        "Duplicate selected event: "
                        + repr(key)
                    )

                selected_keys.add(key)

                output_row = {
                    "timeframe": timeframe,
                    "market": market,
                    "signal_timestamp": utc_string(
                        signal
                    ),
                    "classification": classification,
                    "horizon": horizon,
                    "entry_timestamp": utc_string(
                        entry
                    ),
                    "entry_price": row["entry_price"],
                    "target_price": row["target_price"],
                    "status": status,
                    "hit_30pct": hit,
                    "outcome_maturity_timestamp": utc_string(
                        maturity
                    ),
                }

                writer.writerow(
                    output_row
                )

                counts["selected_rows"] += 1

                if hit:
                    counts["selected_hits"] += 1

                group = (
                    timeframe,
                    horizon,
                    classification,
                )

                selected_by_group[
                    (*group, "events")
                ] += 1

                selected_by_group[
                    (*group, "hits")
                ] += hit

                if (
                    counts["source_rows"]
                    % 500000 == 0
                ):
                    print(
                        "[TRAIN] "
                        f"source={counts['source_rows']:,} "
                        f"selected={counts['selected_rows']:,}",
                        flush=True,
                    )

    groups = []

    group_keys = sorted(
        {
            key[:3]
            for key in selected_by_group
        }
    )

    for timeframe, horizon, classification in group_keys:
        events = selected_by_group[
            (
                timeframe,
                horizon,
                classification,
                "events",
            )
        ]

        hits = selected_by_group[
            (
                timeframe,
                horizon,
                classification,
                "hits",
            )
        ]

        groups.append(
            {
                "timeframe": timeframe,
                "horizon": horizon,
                "classification": classification,
                "events": events,
                "hits": hits,
                "hit_rate_pct": (
                    100.0 * hits / events
                    if events
                    else 0.0
                ),
            }
        )

    return {
        "counts": dict(counts),
        "groups": groups,
    }


def run(args: argparse.Namespace) -> None:
    root = Path(
        args.root
    ).expanduser().resolve()

    source = root / SOURCE_RELATIVE
    audit_path = root / AUDIT_RELATIVE
    output_dir = root / OUTPUT_RELATIVE

    if not source.is_file():
        raise FileNotFoundError(
            f"Source CSV missing: {source}"
        )

    audit = verify_audit(
        audit_path
    )

    cutoff = parse_time(
        args.cutoff
    )

    cutoff_string = utc_string(
        cutoff
    )

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    source_hash = sha256_file(
        source
    )

    audit_hash = sha256_file(
        audit_path
    )

    if (
        not args.force
        and verify_existing(
            output_dir,
            source_hash,
            audit_hash,
            cutoff_string,
        )
    ):
        print(
            "PASS: Existing TRAIN-only dataset "
            "verified and reused."
        )
        return

    csv_path = output_dir / OUTPUT_CSV
    result_path = output_dir / OUTPUT_RESULT
    manifest_path = output_dir / OUTPUT_MANIFEST

    descriptor, temporary_name = tempfile.mkstemp(
        prefix=".train_only_",
        suffix=".csv.tmp",
        dir=str(output_dir),
    )

    os.close(descriptor)

    temporary_csv = Path(
        temporary_name
    )

    try:
        generated = build_dataset(
            source,
            temporary_csv,
            cutoff,
        )

        # Reject changed source inputs.
        if sha256_file(source) != source_hash:
            raise RuntimeError(
                "Source CSV changed during processing"
            )

        if sha256_file(audit_path) != audit_hash:
            raise RuntimeError(
                "Feature audit changed during processing"
            )

        counts = generated["counts"]

        result = {
            "version": VERSION,
            "execution_status": "PASS",
            "train_cutoff": cutoff_string,
            "source_rows": counts.get(
                "source_rows",
                0,
            ),
            "selected_rows": counts.get(
                "selected_rows",
                0,
            ),
            "selected_hits": counts.get(
                "selected_hits",
                0,
            ),
            "exclusion_counts": {
                key: value
                for key, value in counts.items()
                if key.startswith("excluded_")
            },
            "comparison_groups": generated["groups"],
            "feature_audit_market_files": audit.get(
                "market_files"
            ),
            "feature_audit_train_eligible_rows": (
                audit.get(
                    "totals",
                    {},
                ).get(
                    "train_eligible_rows"
                )
            ),
            "feature_values_joined": False,
            "train_dataset_prepared": True,
            "train_experiment_completed": False,
            "independent_oos_proven": False,
            "statistical_independence_proven": False,
            "profitability_proven": False,
            "approved_for_live_trading": False,
            "automated_trading_enabled": False,
            "research_decision": "HOLD",
            "next_stage": (
                "JOIN_AUDITED_FEATURES_AND_RUN_TRAIN_ONLY_EXPERIMENT"
            ),
            "limitations": [
                "This dataset contains event labels, not joined feature values.",
                "Existing historical backtest labels are reused.",
                "Theoretical maturity excludes TRAIN boundary crossings.",
                "Event observations can remain statistically dependent.",
                "No new independent OOS period is established.",
                "No model has been trained.",
                "No automated trading is enabled.",
            ],
        }

        # Publish the CSV before its manifest.
        # An interrupted run cannot be mistaken for
        # a verified completed run.
        os.replace(
            temporary_csv,
            csv_path,
        )

        write_json_atomic(
            result_path,
            result,
        )

        manifest = {
            "version": VERSION,
            "train_cutoff": cutoff_string,
            "input_sha256": {
                "backtest_trades.csv": source_hash,
                "feature_integrity_summary.json": audit_hash,
            },
            "output_sha256": {
                OUTPUT_CSV: sha256_file(
                    csv_path
                ),
                OUTPUT_RESULT: sha256_file(
                    result_path
                ),
            },
        }

        write_json_atomic(
            manifest_path,
            manifest,
        )

        if not verify_existing(
            output_dir,
            source_hash,
            audit_hash,
            cutoff_string,
        ):
            raise RuntimeError(
                "Final dataset verification failed"
            )

        print(
            "PASS: TRAIN-only dataset prepared"
        )

        print(
            f"Source rows: {counts.get('source_rows', 0):,}"
        )

        print(
            f"Selected rows: {counts.get('selected_rows', 0):,}"
        )

        print(
            f"Selected hits: {counts.get('selected_hits', 0):,}"
        )

        print(
            f"Output: {output_dir}"
        )

        print(
            "Research decision: HOLD"
        )

    finally:
        if temporary_csv.exists():
            temporary_csv.unlink()


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Prepare Pattern 256 TRAIN-only "
            "event dataset"
        )
    )

    parser.add_argument(
        "--root",
        required=True,
        help="Project data root",
    )

    parser.add_argument(
        "--cutoff",
        default=DEFAULT_CUTOFF,
        help="Exclusive TRAIN boundary",
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help="Rebuild even when verified outputs exist",
    )

    args = parser.parse_args()

    try:
        run(args)
        return 0

    except Exception as exc:
        print(
            f"FATAL: {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(
        main()
    )
