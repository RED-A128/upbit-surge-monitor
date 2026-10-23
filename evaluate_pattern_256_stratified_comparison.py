
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Clean V003-7
Pattern 256 market-month stratified comparison.

Research only. No trading.

- Reads the original independent-backtest CSV.
- Uses exact source SHA256 verification.
- Separates 2025 and 2026.
- Compares only market-month strata containing both cohorts.
- Requires at least 10 completed observations per cohort
  in each included stratum.
- Uses harmonic sample-size weights.
- Excludes incomplete and boundary-crossing outcomes.
- Supports validated checkpoint/resume.
- Writes CSV, JSON and SHA256 manifest atomically.
"""

import argparse
import csv
import hashlib
import json
import math
import os
import sys

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path


VERSION = "Clean V003-7"

SOURCE_SHA256 = (
    "ed4b4e44fcb387ab70eaac465b3eaa87036466298fde344195e25d06ad7996e0"
)

SOURCE_REL = (
    "data/reports/"
    "pattern_256_accumulation_candidate_independent_backtest/"
    "backtest_trades.csv"
)

OUTPUT_REL = (
    "data/reports/pattern_256_stratified_comparison"
)

TIMEFRAMES = {
    "h1": 1,
    "h4": 4,
}

HORIZON = 24
MIN_PER_COHORT = 10
CHECKPOINT_EVERY = 250000

BASELINE = "PATTERN_256_ONLY"
COMBINED = "BOTH"

DETAIL_FIELDS = [
    "timeframe",
    "test_year",
    "market",
    "month",
    "baseline_n",
    "baseline_hits",
    "combined_n",
    "combined_hits",
    "baseline_rate_pct",
    "combined_rate_pct",
    "difference_pp",
    "harmonic_weight",
    "eligible_stratum",
]

SUMMARY_FIELDS = [
    "timeframe",
    "test_year",
    "total_strata",
    "eligible_strata",
    "excluded_strata",
    "eligible_baseline_n",
    "eligible_combined_n",
    "eligible_baseline_hits",
    "eligible_combined_hits",
    "weighted_difference_pp",
    "positive_strata",
    "negative_strata",
    "zero_strata",
    "decision",
]


def digest(path):
    h = hashlib.sha256()

    with path.open("rb") as handle:
        for chunk in iter(
            lambda: handle.read(4 * 1024 * 1024),
            b"",
        ):
            h.update(chunk)

    return h.hexdigest()


def atomic_json(path, obj):
    tmp = path.with_suffix(path.suffix + ".tmp")

    with tmp.open("w", encoding="utf-8") as handle:
        json.dump(
            obj,
            handle,
            indent=2,
            ensure_ascii=False,
            sort_keys=True,
            allow_nan=False,
        )
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())

    os.replace(tmp, path)


def atomic_csv(path, fields, rows):
    tmp = path.with_suffix(path.suffix + ".tmp")

    with tmp.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=fields,
            extrasaction="raise",
        )
        writer.writeheader()
        writer.writerows(rows)
        handle.flush()
        os.fsync(handle.fileno())

    os.replace(tmp, path)


def timestamp(value):
    if not value:
        raise ValueError("Missing timestamp")

    dt = datetime.fromisoformat(
        value.strip().replace("Z", "+00:00")
    )

    if dt.tzinfo is None:
        raise ValueError("Timestamp lacks timezone")

    return dt.astimezone(timezone.utc)


def year_period(year):
    if year == 2025:
        return (
            datetime(2025, 1, 1, tzinfo=timezone.utc),
            datetime(2026, 1, 1, tzinfo=timezone.utc),
        )

    if year == 2026:
        return (
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            datetime(2026, 9, 1, tzinfo=timezone.utc),
        )

    raise ValueError("Unsupported year")


def empty_counts():
    return [0, 0, 0, 0]


def save_checkpoint(path, source_hash, processed, groups):
    records = [
        [*key, *counts]
        for key, counts in sorted(groups.items())
    ]

    atomic_json(
        path,
        {
            "version": VERSION,
            "source_sha256": source_hash,
            "processed_rows": processed,
            "groups": records,
        },
    )


def load_checkpoint(path, source_hash):
    if not path.is_file():
        return 0, defaultdict(empty_counts)

    obj = json.loads(path.read_text(encoding="utf-8"))

    if obj.get("version") != VERSION:
        raise ValueError("Checkpoint version mismatch")

    if obj.get("source_sha256") != source_hash:
        raise ValueError("Checkpoint source hash mismatch")

    processed = obj.get("processed_rows")

    if type(processed) is not int or processed < 0:
        raise ValueError("Invalid checkpoint row count")

    groups = defaultdict(empty_counts)
    records = obj.get("groups")

    if not isinstance(records, list):
        raise ValueError("Invalid checkpoint records")

    for record in records:
        if not isinstance(record, list) or len(record) != 8:
            raise ValueError("Invalid checkpoint group")

        tf, year, market, month = record[:4]
        counts = record[4:]

        if (
            tf not in TIMEFRAMES
            or year not in (2025, 2026)
            or not isinstance(market, str)
            or not market.startswith("KRW-")
            or not isinstance(month, str)
            or len(month) != 7
            or any(
                type(v) is not int or v < 0
                for v in counts
            )
            or counts[1] > counts[0]
            or counts[3] > counts[2]
        ):
            raise ValueError("Invalid checkpoint data")

        key = (tf, year, market, month)

        if key in groups:
            raise ValueError("Duplicate checkpoint key")

        groups[key] = counts

    return processed, groups


def analyze(source, checkpoint, source_hash):
    processed, groups = load_checkpoint(
        checkpoint, source_hash
    )

    print(
        f"[RESUME] checkpoint_rows={processed:,}",
        flush=True,
    )

    required = {
        "timeframe",
        "market",
        "signal_timestamp",
        "classification",
        "horizon",
        "entry_timestamp",
        "status",
        "hit_30pct",
        "observed_bars",
        "required_bars",
    }

    total = 0

    with source.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as handle:
        reader = csv.DictReader(handle)

        if not required.issubset(reader.fieldnames or []):
            raise ValueError("Required source columns missing")

        for row in reader:
            total += 1

            if total <= processed:
                continue

            tf = row["timeframe"].strip()

            if tf in TIMEFRAMES:
                classification = row["classification"].strip()

                if classification in (BASELINE, COMBINED):
                    try:
                        horizon = int(row["horizon"])
                    except ValueError as exc:
                        raise ValueError(
                            f"Invalid horizon at row {total}"
                        ) from exc

                    if horizon == HORIZON:
                        status = row["status"].strip()

                        if status in ("HIT", "MISS"):
                            observed = int(row["observed_bars"])
                            required_bars = int(
                                row["required_bars"]
                            )

                            if (
                                observed >= HORIZON
                                and required_bars == HORIZON
                            ):
                                hit = int(row["hit_30pct"])

                                if hit not in (0, 1):
                                    raise ValueError(
                                        f"Invalid hit at row {total}"
                                    )

                                if (status == "HIT") != (hit == 1):
                                    raise ValueError(
                                        f"Status mismatch at row {total}"
                                    )

                                signal = timestamp(
                                    row["signal_timestamp"]
                                )
                                entry = timestamp(
                                    row["entry_timestamp"]
                                )

                                if entry < signal:
                                    raise ValueError(
                                        f"Entry precedes signal: {total}"
                                    )

                                maturity = entry + timedelta(
                                    hours=HORIZON * TIMEFRAMES[tf]
                                )

                                for year in (2025, 2026):
                                    start, end = year_period(year)

                                    if (
                                        start <= signal < end
                                        and maturity <= end
                                    ):
                                        market = row["market"].strip()

                                        if not market.startswith("KRW-"):
                                            raise ValueError(
                                                f"Invalid market: {total}"
                                            )

                                        month = signal.strftime("%Y-%m")

                                        key = (
                                            tf,
                                            year,
                                            market,
                                            month,
                                        )

                                        counts = groups[key]

                                        offset = (
                                            0
                                            if classification == BASELINE
                                            else 2
                                        )

                                        counts[offset] += 1
                                        counts[offset + 1] += hit
                                        break

            if total % CHECKPOINT_EVERY == 0:
                save_checkpoint(
                    checkpoint,
                    source_hash,
                    total,
                    groups,
                )

                print(
                    f"[PROGRESS] rows={total:,} "
                    f"strata={len(groups):,}",
                    flush=True,
                )

    if total < processed:
        raise ValueError("Checkpoint exceeds source length")

    save_checkpoint(
        checkpoint,
        source_hash,
        total,
        groups,
    )

    return total, groups


def percent(hits, count):
    if count == 0:
        return None

    return round(
        hits * 100.0 / count,
        8,
    )


def build_results(groups):
    detail = []
    summaries = []

    accum = defaultdict(
        lambda: {
            "total": 0,
            "eligible": 0,
            "bn": 0,
            "cn": 0,
            "bh": 0,
            "ch": 0,
            "positive": 0,
            "negative": 0,
            "zero": 0,
            "weighted_sum": 0.0,
            "weight_sum": 0.0,
        }
    )

    for key, counts in sorted(groups.items()):
        tf, year, market, month = key
        bn, bh, cn, ch = counts

        baseline_rate = percent(bh, bn)
        combined_rate = percent(ch, cn)

        eligible = (
            bn >= MIN_PER_COHORT
            and cn >= MIN_PER_COHORT
        )

        difference = None
        weight = None

        bucket = accum[(tf, year)]
        bucket["total"] += 1

        if eligible:
            difference = (
                100.0 * ch / cn
                - 100.0 * bh / bn
            )

            weight = (
                2.0 * bn * cn / (bn + cn)
            )

            bucket["eligible"] += 1
            bucket["bn"] += bn
            bucket["bh"] += bh
            bucket["cn"] += cn
            bucket["ch"] += ch

            bucket["weighted_sum"] += weight * difference
            bucket["weight_sum"] += weight

            if difference > 0:
                bucket["positive"] += 1
            elif difference < 0:
                bucket["negative"] += 1
            else:
                bucket["zero"] += 1

        detail.append(
            {
                "timeframe": tf,
                "test_year": year,
                "market": market,
                "month": month,
                "baseline_n": bn,
                "baseline_hits": bh,
                "combined_n": cn,
                "combined_hits": ch,
                "baseline_rate_pct": baseline_rate,
                "combined_rate_pct": combined_rate,
                "difference_pp": (
                    round(difference, 8)
                    if difference is not None
                    else None
                ),
                "harmonic_weight": (
                    round(weight, 8)
                    if weight is not None
                    else None
                ),
                "eligible_stratum": eligible,
            }
        )

    for tf in TIMEFRAMES:
        for year in (2025, 2026):
            bucket = accum[(tf, year)]

            weighted = None

            if bucket["weight_sum"] > 0:
                weighted = round(
                    bucket["weighted_sum"]
                    / bucket["weight_sum"],
                    8,
                )

            summaries.append(
                {
                    "timeframe": tf,
                    "test_year": year,
                    "total_strata": bucket["total"],
                    "eligible_strata": bucket["eligible"],
                    "excluded_strata": (
                        bucket["total"] - bucket["eligible"]
                    ),
                    "eligible_baseline_n": bucket["bn"],
                    "eligible_combined_n": bucket["cn"],
                    "eligible_baseline_hits": bucket["bh"],
                    "eligible_combined_hits": bucket["ch"],
                    "weighted_difference_pp": weighted,
                    "positive_strata": bucket["positive"],
                    "negative_strata": bucket["negative"],
                    "zero_strata": bucket["zero"],
                    "decision": "HOLD",
                }
            )

    return detail, summaries


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--root",
        default=".",
    )

    args = parser.parse_args()

    root = Path(args.root).resolve()
    source = (root / SOURCE_REL).resolve()
    output = (root / OUTPUT_REL).resolve()

    if not source.is_file():
        raise FileNotFoundError(source)

    if (
        output == source.parent
        or output in source.parents
        or source.parent in output.parents
    ):
        raise ValueError("Unsafe output directory")

    print("[START]", VERSION, flush=True)

    source_hash = digest(source)

    if source_hash != SOURCE_SHA256:
        raise ValueError("Source SHA256 mismatch")

    output.mkdir(parents=True, exist_ok=True)

    checkpoint = output / "stratified_checkpoint.json"

    total, groups = analyze(
        source,
        checkpoint,
        source_hash,
    )

    if total != 7949826:
        raise ValueError(
            f"Unexpected source rows: {total}"
        )

    if digest(source) != SOURCE_SHA256:
        raise ValueError("Source changed during execution")

    detail, summaries = build_results(groups)

    if len(summaries) != 4:
        raise ValueError("Expected four summaries")

    detail_path = output / "stratified_detail.csv"
    summary_path = output / "stratified_summary.csv"
    result_path = output / "stratified_result.json"
    manifest_path = output / "stratified_manifest.json"

    atomic_csv(
        detail_path,
        DETAIL_FIELDS,
        detail,
    )

    atomic_csv(
        summary_path,
        SUMMARY_FIELDS,
        summaries,
    )

    result = {
        "version": VERSION,
        "execution_status": "PASS",
        "source_sha256": SOURCE_SHA256,
        "source_rows": total,
        "detail_rows": len(detail),
        "summary_rows": len(summaries),
        "minimum_events_per_cohort": MIN_PER_COHORT,
        "stratification": [
            "timeframe",
            "test_year",
            "market",
            "calendar_month",
        ],
        "weighting": (
            "Harmonic sample size: 2*n1*n2/(n1+n2)"
        ),
        "summary": summaries,
        "limitations": [
            "Observational comparison, not random assignment.",
            "Cohorts contain different signal timestamps.",
            "Within-market-month confounding remains possible.",
            "Overlapping signal horizons remain dependent.",
            "No multiple-testing adjustment is performed.",
            "Prior candidate selection used test data.",
            "No independent future holdout exists.",
            "No profitability or execution-cost proof.",
        ],
        "independent_oos_proven": False,
        "causal_improvement_proven": False,
        "profitability_proven": False,
        "approved_for_live_trading": False,
        "overall_decision": "HOLD",
    }

    atomic_json(result_path, result)

    hashes = {
        path.name: digest(path)
        for path in (
            detail_path,
            summary_path,
            result_path,
        )
    }

    atomic_json(
        manifest_path,
        {
            "version": VERSION,
            "source_sha256": SOURCE_SHA256,
            "output_sha256": hashes,
            "detail_rows": len(detail),
            "summary_rows": len(summaries),
        },
    )

    for filename, expected in hashes.items():
        if digest(output / filename) != expected:
            raise ValueError(
                "Output SHA256 mismatch: " + filename
            )

    print(
        f"[PASS] Source rows: {total:,}",
        flush=True,
    )

    print(
        f"[PASS] Market-month strata: {len(detail):,}",
        flush=True,
    )

    for row in summaries:
        print(
            "[RESULT]",
            row["timeframe"],
            row["test_year"],
            "eligible=",
            row["eligible_strata"],
            "weighted_pp=",
            row["weighted_difference_pp"],
            flush=True,
        )

    print("[PASS] SHA256 integrity", flush=True)
    print("[DECISION] HOLD", flush=True)

    # Only remove checkpoint after complete output verification.
    checkpoint.unlink(missing_ok=True)

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())

    except KeyboardInterrupt:
        print(
            "[INTERRUPTED] Checkpoint preserved",
            file=sys.stderr,
            flush=True,
        )
        sys.exit(130)

    except Exception as exc:
        print(
            "[FAIL] " + str(exc),
            file=sys.stderr,
            flush=True,
        )
        sys.exit(1)
