
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
evaluate_pattern_256_temporal_holdout.py
Clean V001

Temporal holdout diagnostic.
Standard library only.

- Read-only production input
- Time-ordered train/test separation
- Conservative horizon boundary exclusion
- +30% HIT/MISS comparison
- SHA256 input/output audit
- Integrity-verified resume
- No automatic trading
- No claim of independent OOS proof
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import tempfile

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path


VERSION = "Clean V001"

SOURCE = Path(
    "data/reports/"
    "pattern_256_accumulation_candidate_independent_backtest/"
    "backtest_trades.csv"
)

DEST = Path(
    "data/reports/pattern_256_temporal_holdout"
)

COLUMNS = [
    "partition",
    "timeframe",
    "horizon",
    "baseline_completed",
    "baseline_hits",
    "baseline_rate_pct",
    "combined_completed",
    "combined_hits",
    "combined_rate_pct",
    "difference_pp",
    "status",
]

PERIOD = {
    "h1": timedelta(hours=1),
    "h4": timedelta(hours=4),
    "d1": timedelta(days=1),
}


def fail(message):
    raise RuntimeError(message)


def digest(path):
    h = hashlib.sha256()

    with path.open("rb") as stream:
        for part in iter(
            lambda: stream.read(1024 * 1024),
            b"",
        ):
            h.update(part)

    return h.hexdigest()


def parse_time(text, label):
    try:
        result = datetime.fromisoformat(
            text.replace("Z", "+00:00")
        )
    except (ValueError, AttributeError):
        fail(f"Invalid {label}: {text}")

    if result.tzinfo is None:
        fail(f"Missing timezone in {label}: {text}")

    return result.astimezone(timezone.utc)


def atomic(path, write):
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    fd, name = tempfile.mkstemp(
        dir=path.parent,
        prefix="." + path.name,
    )

    temp = Path(name)

    try:
        with os.fdopen(
            fd,
            "w",
            encoding="utf-8",
            newline="",
        ) as stream:
            write(stream)
            stream.flush()
            os.fsync(stream.fileno())

        os.replace(temp, path)

    finally:
        if temp.exists():
            temp.unlink()


def save_json(path, obj):
    def write(stream):
        json.dump(
            obj,
            stream,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            allow_nan=False,
        )
        stream.write("\n")

    atomic(path, write)


def save_csv(path, rows):
    def write(stream):
        writer = csv.DictWriter(
            stream,
            fieldnames=COLUMNS,
        )
        writer.writeheader()
        writer.writerows(rows)

    atomic(path, write)


def check_resume(dest, source_hash, settings):
    manifest_file = (
        dest / "temporal_holdout_manifest.json"
    )

    if not manifest_file.exists():
        return False

    manifest = json.loads(
        manifest_file.read_text(
            encoding="utf-8"
        )
    )

    if (
        manifest.get("version") != VERSION
        or manifest.get("source_sha256") != source_hash
        or manifest.get("settings") != settings
    ):
        fail(
            "Existing output metadata differs; "
            "refusing overwrite"
        )

    hashes = manifest.get(
        "output_sha256",
        {},
    )

    required = {
        "temporal_holdout_comparison.csv",
        "temporal_holdout_result.json",
    }

    if set(hashes) != required:
        fail("Invalid existing output manifest")

    for filename, expected in hashes.items():
        path = dest / filename

        if (
            not path.is_file()
            or digest(path) != expected
        ):
            fail(
                "Existing output integrity failure: "
                + filename
            )

    result = json.loads(
        (
            dest / "temporal_holdout_result.json"
        ).read_text(encoding="utf-8")
    )

    if (
        result.get("execution_status") != "PASS"
        or result.get(
            "approved_for_live_trading"
        ) is not False
    ):
        fail(
            "Existing output safety/status failure"
        )

    return True


def analyze(source, cutoff, test_end):
    counts = defaultdict(
        lambda: [0, 0]
    )

    excluded = defaultdict(int)
    total = 0

    with source.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as stream:
        reader = csv.DictReader(stream)

        needed = {
            "timeframe",
            "market",
            "signal_timestamp",
            "entry_timestamp",
            "classification",
            "horizon",
            "status",
            "hit_30pct",
            "observed_bars",
        }

        if (
            not reader.fieldnames
            or not needed.issubset(reader.fieldnames)
        ):
            fail("Missing source CSV columns")

        for line, row in enumerate(reader, 2):
            total += 1

            if None in row:
                fail(
                    f"Malformed CSV line {line}"
                )

            tf = row[
                "timeframe"
            ].strip().lower()

            cls = row[
                "classification"
            ].strip().upper()

            status = row[
                "status"
            ].strip().upper()

            if tf not in PERIOD:
                fail(
                    f"Unknown timeframe line {line}"
                )

            valid_classes = {
                "PATTERN_256_ONLY",
                "BOTH",
                "ACCUMULATION_ONLY",
                "NEITHER",
            }

            if cls not in valid_classes:
                fail(
                    f"Unknown classification line {line}"
                )

            try:
                horizon = int(
                    row["horizon"]
                )
                observed = int(
                    row["observed_bars"]
                )
            except ValueError:
                fail(
                    f"Invalid horizon/observed "
                    f"line {line}"
                )

            if (
                horizon <= 0
                or observed < 0
                or observed > horizon
            ):
                fail(
                    f"Invalid horizon bounds "
                    f"line {line}"
                )

            signal = parse_time(
                row[
                    "signal_timestamp"
                ].strip(),
                f"signal line {line}",
            )

            if status not in {
                "HIT",
                "MISS",
            }:
                excluded["incomplete"] += 1
                continue

            if cls not in {
                "PATTERN_256_ONLY",
                "BOTH",
            }:
                excluded[
                    "other_classification"
                ] += 1
                continue

            entry_text = row[
                "entry_timestamp"
            ].strip()

            if not entry_text:
                fail(
                    f"Missing completed entry "
                    f"line {line}"
                )

            entry = parse_time(
                entry_text,
                f"entry line {line}",
            )

            if entry <= signal:
                fail(
                    f"Invalid entry chronology "
                    f"line {line}"
                )

            if (
                status == "MISS"
                and observed != horizon
            ):
                fail(
                    f"Incomplete MISS line {line}"
                )

            if (
                status == "HIT"
                and observed < 1
            ):
                fail(
                    f"Empty HIT line {line}"
                )

            hit = row[
                "hit_30pct"
            ].strip()

            if (
                hit not in {"0", "1"}
                or (hit == "1")
                != (status == "HIT")
            ):
                fail(
                    f"Inconsistent status/hit "
                    f"line {line}"
                )

            # Conservative calendar-time boundary.
            # Candle gaps require further verification.
            maturity = (
                entry
                + PERIOD[tf] * horizon
            )

            if signal < cutoff:
                if maturity > cutoff:
                    excluded[
                        "train_boundary"
                    ] += 1
                    continue

                partition = "TRAIN"

            elif signal < test_end:
                if maturity > test_end:
                    excluded[
                        "test_boundary"
                    ] += 1
                    continue

                partition = "TEST"

            else:
                excluded[
                    "after_test_end"
                ] += 1
                continue

            key = (
                partition,
                tf,
                horizon,
                cls,
            )

            value = counts[key]
            value[0] += 1
            value[1] += int(hit)

            if total % 500000 == 0:
                print(
                    f"[PROGRESS] rows={total:,}",
                    flush=True,
                )

    rows = []

    for partition in ("TRAIN", "TEST"):
        keys = sorted({
            (tf, h)
            for (p, tf, h, _) in counts
            if p == partition
        })

        for tf, h in keys:
            b = counts[
                (
                    partition,
                    tf,
                    h,
                    "PATTERN_256_ONLY",
                )
            ]

            c = counts[
                (
                    partition,
                    tf,
                    h,
                    "BOTH",
                )
            ]

            br = (
                100 * b[1] / b[0]
                if b[0]
                else None
            )

            cr = (
                100 * c[1] / c[0]
                if c[0]
                else None
            )

            diff = (
                cr - br
                if (
                    br is not None
                    and cr is not None
                )
                else None
            )

            if not b[0] or not c[0]:
                label = "INSUFFICIENT_GROUPS"

            elif min(b[0], c[0]) < 100:
                label = "SMALL_SAMPLE"

            elif diff > 0:
                label = "OBSERVED_IMPROVEMENT"

            elif diff < 0:
                label = "OBSERVED_DETERIORATION"

            else:
                label = "OBSERVED_TIE"

            rows.append({
                "partition": partition,
                "timeframe": tf,
                "horizon": h,
                "baseline_completed": b[0],
                "baseline_hits": b[1],
                "baseline_rate_pct": (
                    round(br, 6)
                    if br is not None
                    else None
                ),
                "combined_completed": c[0],
                "combined_hits": c[1],
                "combined_rate_pct": (
                    round(cr, 6)
                    if cr is not None
                    else None
                ),
                "difference_pp": (
                    round(diff, 6)
                    if diff is not None
                    else None
                ),
                "status": label,
            })

    return rows, total, dict(excluded)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--root",
        required=True,
    )

    parser.add_argument(
        "--cutoff",
        default="2025-01-01T00:00:00+00:00",
    )

    parser.add_argument(
        "--test-end",
        default="2026-09-01T00:00:00+00:00",
    )

    parser.add_argument("--input")
    parser.add_argument("--output")

    args = parser.parse_args()

    root = Path(
        args.root
    ).resolve()

    source = (
        Path(args.input).resolve()
        if args.input
        else root / SOURCE
    )

    dest = (
        Path(args.output).resolve()
        if args.output
        else root / DEST
    )

    if not source.is_file():
        fail(
            f"Source missing: {source}"
        )

    if (
        dest == root
        or dest == source.parent
        or dest in source.parents
        or source.parent in dest.parents
    ):
        fail("Unsafe output directory")

    cutoff = parse_time(
        args.cutoff,
        "cutoff",
    )

    test_end = parse_time(
        args.test_end,
        "test-end",
    )

    if cutoff >= test_end:
        fail(
            "cutoff must precede test-end"
        )

    settings = {
        "cutoff": cutoff.isoformat(),
        "test_end": test_end.isoformat(),
    }

    source_hash = digest(source)

    if check_resume(
        dest,
        source_hash,
        settings,
    ):
        if digest(source) != source_hash:
            fail(
                "Source changed during resume"
            )

        print(
            "[RESUME PASS] "
            "Verified completed output"
        )
        return 0

    if (
        dest.exists()
        and any(dest.iterdir())
    ):
        fail(
            "Output directory is not empty; "
            "refusing overwrite"
        )

    rows, total, excluded = analyze(
        source,
        cutoff,
        test_end,
    )

    if digest(source) != source_hash:
        fail(
            "Source SHA256 changed "
            "during evaluation"
        )

    if (
        not rows
        or not any(
            r["partition"] == "TEST"
            and r["combined_completed"]
            for r in rows
        )
    ):
        fail(
            "No evaluable holdout "
            "combined-pattern data"
        )

    dest.mkdir(
        parents=True,
        exist_ok=True,
    )

    result = {
        "version": VERSION,
        "execution_status": "PASS",
        "source_rows": total,
        "source_sha256": source_hash,
        "settings": settings,
        "excluded": excluded,
        "comparison_rows": len(rows),
        "independent_oos_proven": False,
        "statistical_independence_proven": False,
        "approved_for_live_trading": False,
        "caution": (
            "Time split diagnostic only; "
            "signal overlap, selection bias, "
            "candle gaps and market regimes "
            "require further testing."
        ),
    }

    save_csv(
        dest / "temporal_holdout_comparison.csv",
        rows,
    )

    save_json(
        dest / "temporal_holdout_result.json",
        result,
    )

    names = (
        "temporal_holdout_comparison.csv",
        "temporal_holdout_result.json",
    )

    hashes = {
        name: digest(dest / name)
        for name in names
    }

    save_json(
        dest / "temporal_holdout_manifest.json",
        {
            "version": VERSION,
            "source_sha256": source_hash,
            "settings": settings,
            "output_sha256": hashes,
        },
    )

    if (
        digest(source) != source_hash
        or not check_resume(
            dest,
            source_hash,
            settings,
        )
    ):
        fail(
            "Final integrity verification failed"
        )

    print(
        f"[PASS] {total:,} source rows; "
        f"{len(rows)} comparison rows"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
