#!/usr/bin/env python3
"""Pattern 256 signal timestamp convention audit. Read-only source audit."""

import argparse
import csv
import hashlib
import json
import os
import sys
import tempfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

VERSION = "Clean V001"

SHARD_REL = Path(
    "data/reports/pattern_256_train_feature_join/shards"
)
REPORT_REL = Path(
    "data/reports/pattern_256_signal_timestamp_convention"
)

REQUIRED = (
    "timeframe",
    "market",
    "signal_timestamp",
    "entry_timestamp",
    "feature_candle_timestamp",
    "feature_candle_end_timestamp",
)

SOURCE_FILES = (
    "data/reports/pattern_256_feature_point_in_time/"
    "feature_point_in_time_result.json",
    "data/reports/pattern_256_train_feature_join/"
    "train_feature_join_result.json",
    "data/reports/pattern_256_train_feature_join/"
    "train_feature_join_manifest.json",
)

COUNTS = (
    "CANDLE_OPEN_NEXT_OPEN_COMPATIBLE",
    "FEATURE_CLOSED_BY_SIGNAL",
    "ENTRY_BEFORE_FEATURE_CLOSE",
    "OTHER_TIMING",
    "INVALID_TIMESTAMP",
)


def sha256(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(
            lambda: f.read(1024 * 1024), b""
        ):
            h.update(chunk)
    return h.hexdigest()


def parse_time(value):
    if not value:
        raise ValueError("empty timestamp")

    d = datetime.fromisoformat(
        value.strip().replace("Z", "+00:00")
    )

    if d.tzinfo is None or d.utcoffset() is None:
        raise ValueError("timezone required")

    return d.astimezone(timezone.utc)


def write_json_atomic(path, obj):
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    fd, tmp = tempfile.mkstemp(
        prefix=".tmp_",
        suffix=".json",
        dir=str(path.parent),
    )

    try:
        with os.fdopen(
            fd,
            "w",
            encoding="utf-8",
            newline="\n",
        ) as f:
            json.dump(
                obj,
                f,
                indent=2,
                sort_keys=True,
                ensure_ascii=False,
            )
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())

        os.replace(tmp, path)

    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def classify(row):
    try:
        signal = parse_time(
            row["signal_timestamp"]
        )
        entry = parse_time(
            row["entry_timestamp"]
        )
        candle_start = parse_time(
            row["feature_candle_timestamp"]
        )
        candle_end = parse_time(
            row["feature_candle_end_timestamp"]
        )

    except (
        ValueError,
        TypeError,
        KeyError,
    ):
        return "INVALID_TIMESTAMP"

    if (
        not candle_start < candle_end
        or entry <= signal
    ):
        return "INVALID_TIMESTAMP"

    if candle_end > entry:
        return "ENTRY_BEFORE_FEATURE_CLOSE"

    if candle_end <= signal:
        return "FEATURE_CLOSED_BY_SIGNAL"

    if (
        signal == candle_start
        and entry == candle_end
    ):
        return "CANDLE_OPEN_NEXT_OPEN_COMPATIBLE"

    return "OTHER_TIMING"


def main():
    ap = argparse.ArgumentParser()

    ap.add_argument(
        "--root",
        required=True,
    )
    ap.add_argument(
        "--max-issues",
        type=int,
        default=1000,
    )

    args = ap.parse_args()

    if args.max_issues < 0:
        raise ValueError(
            "max-issues must be nonnegative"
        )

    root = Path(
        args.root
    ).expanduser().resolve()

    shard_root = root / SHARD_REL
    report_dir = root / REPORT_REL

    if not shard_root.is_dir():
        raise FileNotFoundError(
            f"Missing JOIN shards: {shard_root}"
        )

    sources = {}

    for relative in SOURCE_FILES:
        path = root / relative

        if not path.is_file():
            raise FileNotFoundError(
                f"Missing source: {path}"
            )

        sources[relative] = sha256(path)

    previous = json.loads(
        (root / SOURCE_FILES[0]).read_text(
            encoding="utf-8-sig"
        )
    )

    if previous.get("execution_status") != "PASS":
        raise ValueError(
            "Previous point-in-time audit "
            "did not execute successfully"
        )

    shards = sorted(
        shard_root.rglob("*.csv")
    )

    if not shards:
        raise ValueError(
            "No JOIN CSV shards"
        )

    report_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    counts = Counter()
    by_group = Counter()
    issues = []

    total = 0

    for index, shard in enumerate(
        shards,
        1,
    ):
        with shard.open(
            "r",
            encoding="utf-8-sig",
            newline="",
        ) as f:
            reader = csv.DictReader(f)

            if (
                not reader.fieldnames
                or any(
                    x not in reader.fieldnames
                    for x in REQUIRED
                )
            ):
                raise ValueError(
                    f"Missing required columns: {shard}"
                )

            for line, row in enumerate(
                reader,
                2,
            ):
                label = classify(row)

                counts[label] += 1

                by_group[
                    (
                        row["timeframe"],
                        label,
                    )
                ] += 1

                total += 1

                if label in (
                    "ENTRY_BEFORE_FEATURE_CLOSE",
                    "OTHER_TIMING",
                    "INVALID_TIMESTAMP",
                ):
                    if len(issues) < args.max_issues:
                        issues.append(
                            {
                                "file": shard.relative_to(
                                    root
                                ).as_posix(),
                                "line": line,
                                "market": row["market"],
                                "timeframe": row["timeframe"],
                                "classification": label,
                                "signal_timestamp": row[
                                    "signal_timestamp"
                                ],
                                "entry_timestamp": row[
                                    "entry_timestamp"
                                ],
                                "feature_candle_timestamp": row[
                                    "feature_candle_timestamp"
                                ],
                                "feature_candle_end_timestamp": row[
                                    "feature_candle_end_timestamp"
                                ],
                            }
                        )

        if (
            index % 25 == 0
            or index == len(shards)
        ):
            print(
                f"[AUDIT] shards={index}/{len(shards)} "
                f"rows={total}",
                flush=True,
            )

    expected = previous.get(
        "counts",
        {},
    ).get("joined_rows")

    if (
        expected is None
        or int(expected) != total
    ):
        raise ValueError(
            "JOIN row mismatch: "
            f"previous={expected}, "
            f"actual={total}"
        )

    if sum(counts.values()) != total:
        raise AssertionError(
            "Classification count mismatch"
        )

    unsafe = (
        counts["ENTRY_BEFORE_FEATURE_CLOSE"]
        + counts["INVALID_TIMESTAMP"]
    )

    review = counts["OTHER_TIMING"]

    decision = (
        "BLOCK"
        if unsafe
        else "HOLD"
    )

    csv_path = (
        report_dir
        / "signal_timestamp_convention_comparison.csv"
    )

    issue_path = (
        report_dir
        / "signal_timestamp_convention_issues.csv"
    )

    result_path = (
        report_dir
        / "signal_timestamp_convention_result.json"
    )

    manifest_path = (
        report_dir
        / "signal_timestamp_convention_manifest.json"
    )

    with csv_path.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as f:
        w = csv.writer(f)

        w.writerow(
            [
                "timeframe",
                "classification",
                "rows",
            ]
        )

        for (
            tf,
            label,
        ), n in sorted(
            by_group.items()
        ):
            w.writerow(
                [
                    tf,
                    label,
                    n,
                ]
            )

    with issue_path.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as f:
        fields = [
            "file",
            "line",
            "market",
            "timeframe",
            "classification",
            "signal_timestamp",
            "entry_timestamp",
            "feature_candle_timestamp",
            "feature_candle_end_timestamp",
        ]

        w = csv.DictWriter(
            f,
            fieldnames=fields,
        )

        w.writeheader()
        w.writerows(issues)

    result = {
        "version": VERSION,
        "execution_status": "PASS",
        "source_rows": total,
        "source_shards": len(shards),
        "classification_counts": {
            k: counts[k]
            for k in COUNTS
        },
        "issues_recorded": len(issues),
        "issues_truncated": (
            unsafe + review > len(issues)
        ),
        "research_decision": decision,
        "signal_timestamp_convention_confirmed_by_upstream_code": False,
        "point_in_time_proven": False,
        "train_experiment_approved": False,
        "independent_oos_proven": False,
        "profitability_proven": False,
        "approved_for_live_trading": False,
        "notes": [
            "CANDLE_OPEN_NEXT_OPEN_COMPATIBLE "
            "is a timing hypothesis, not leakage proof.",
            "The upstream signal generation "
            "implementation must be inspected.",
            "No source files are modified.",
            "Source shard content hashes are not "
            "independently verified by this audit.",
        ],
    }

    write_json_atomic(
        result_path,
        result,
    )

    manifest = {
        "version": VERSION,
        "input_sha256": sources,
        "source_shard_content_hashes_not_verified": True,
        "output_sha256": {
            p.name: sha256(p)
            for p in (
                csv_path,
                issue_path,
                result_path,
            )
        },
    }

    write_json_atomic(
        manifest_path,
        manifest,
    )

    print(
        json.dumps(
            result,
            indent=2,
            ensure_ascii=False,
        ),
        flush=True,
    )

    print(
        f"[DONE] {report_dir}",
        flush=True,
    )


if __name__ == "__main__":
    try:
        main()

    except Exception as exc:
        print(
            f"FATAL: {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        sys.exit(1)
