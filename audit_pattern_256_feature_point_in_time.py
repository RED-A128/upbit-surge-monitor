#!/usr/bin/env python3
"""
Pattern 256 Feature Point-in-Time & Coverage Gate
Clean V001

Read-only research audit.
Fails closed when point-in-time provenance is absent.
"""

import argparse
import csv
import hashlib
import json
import os
import sys

from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path


VERSION = "Clean V001"

TRAIN = Path(
    "data/reports/pattern_256_train_only/train_only_events.csv"
)

JOIN = Path(
    "data/reports/pattern_256_train_feature_join"
)

AUDIT = Path(
    "data/reports/pattern_256_feature_join_integrity"
)

OUT = Path(
    "data/reports/pattern_256_feature_point_in_time"
)

TIME_KEYS = (
    "signal_timestamp",
    "signal_time",
    "timestamp",
    "event_timestamp",
    "datetime",
    "time",
)

FEATURE_TIME_KEYS = (
    "feature_timestamp",
    "feature_time",
    "feature_candle_timestamp",
    "candle_timestamp",
    "feature_available_at",
    "available_at",
)

MARKET_KEYS = (
    "market",
    "ticker",
    "symbol",
    "code",
)

FRAME_KEYS = (
    "timeframe",
    "interval",
    "tf",
)

LABEL_KEYS = (
    "hit_30pct",
    "hit_30",
    "target_hit",
    "hit",
    "label",
    "is_hit",
    "success",
    "status",
)


def sha(path):
    h = hashlib.sha256()

    with path.open("rb") as f:
        for chunk in iter(
            lambda: f.read(1024 * 1024),
            b"",
        ):
            h.update(chunk)

    return h.hexdigest()


def read_json(path):
    with path.open(encoding="utf-8-sig") as f:
        return json.load(f)


def write_json(path, obj):
    tmp = path.with_suffix(path.suffix + ".tmp")

    with tmp.open("w", encoding="utf-8") as f:
        json.dump(
            obj,
            f,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        f.write("\n")

    os.replace(tmp, path)


def write_csv(path, columns, rows):
    tmp = path.with_suffix(path.suffix + ".tmp")

    with tmp.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as f:
        writer = csv.DictWriter(
            f,
            fieldnames=columns,
            extrasaction="ignore",
        )

        writer.writeheader()
        writer.writerows(rows)

    os.replace(tmp, path)


def choose(columns, candidates):
    lookup = {
        column.lower().strip(): column
        for column in columns
    }

    return next(
        (
            lookup[key]
            for key in candidates
            if key in lookup
        ),
        None,
    )


def stamp(value):
    value = str(value or "").strip()

    if not value:
        return None

    if value.endswith("Z"):
        value = value[:-1] + "+00:00"

    try:
        result = datetime.fromisoformat(value)
    except ValueError:
        return None

    if result.tzinfo is None:
        result = result.replace(
            tzinfo=timezone.utc
        )

    return result.astimezone(timezone.utc)


def label(value):
    value = str(value or "").strip().upper()

    if value in (
        "1",
        "TRUE",
        "YES",
        "HIT",
        "WIN",
        "SUCCESS",
        "TARGET_HIT",
    ):
        return "HIT"

    if value in (
        "0",
        "FALSE",
        "NO",
        "MISS",
        "LOSE",
        "FAIL",
        "FAILURE",
    ):
        return "MISS"

    return "UNKNOWN"


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--root",
        default=".",
    )

    parser.add_argument(
        "--output",
        default=None,
    )

    parser.add_argument(
        "--progress-every",
        type=int,
        default=250000,
    )

    args = parser.parse_args()

    root = Path(args.root).resolve()

    if args.output:
        out = (root / args.output).resolve()
    else:
        out = root / OUT

    train = root / TRAIN
    join = root / JOIN
    audit = root / AUDIT

    if not train.is_file():
        raise RuntimeError(
            f"Missing TRAIN: {train}"
        )

    if not join.is_dir():
        raise RuntimeError(
            f"Missing JOIN directory: {join}"
        )

    if (
        out == root
        or out == join
        or out == audit
        or out == train.parent
        or join in out.parents
    ):
        raise RuntimeError(
            "Unsafe output directory"
        )

    out.mkdir(
        parents=True,
        exist_ok=True,
    )

    problems = []
    checks = {}
    sources = {}

    input_paths = (
        train,
        join / "train_feature_join_manifest.json",
        join / "train_feature_join_result.json",
        audit / "feature_join_integrity_result.json",
        audit / "feature_join_integrity_manifest.json",
    )

    for path in input_paths:
        if path.exists():
            sources[
                str(path.relative_to(root))
            ] = sha(path)

    prior_path = (
        audit / "feature_join_integrity_result.json"
    )

    manifest_path = (
        audit / "feature_join_integrity_manifest.json"
    )

    prior = (
        read_json(prior_path)
        if prior_path.exists()
        else {}
    )

    manifest = (
        read_json(manifest_path)
        if manifest_path.exists()
        else {}
    )

    expected = manifest.get(
        "shard_sha256",
        {},
    )

    if not expected:
        problems.append(
            "Missing prior shard SHA256 manifest"
        )

    shard_files = sorted(
        (join / "shards").rglob("*.csv")
    )

    if not shard_files:
        raise RuntimeError(
            "No JOIN shards found"
        )

    stats = defaultdict(Counter)
    shard_rows = []

    future = 0
    bad_timestamp = 0
    no_feature_time = 0
    rows_joined = 0

    schema_set = set()

    signal_col_seen = False
    feature_col_seen = False
    market_col_seen = False
    frame_col_seen = False

    # ----------------------------------------
    # JOIN shard integrity and time audit
    # ----------------------------------------

    for index, path in enumerate(
        shard_files,
        1,
    ):
        rel = str(
            path.relative_to(join)
        ).replace("/", "\\")

        digest = sha(path)

        expected_digest = (
            expected.get(rel)
            or expected.get(
                rel.replace("\\", "/")
            )
        )

        hash_ok = (
            expected_digest == digest
        )

        if not hash_ok:
            problems.append(
                f"Shard SHA256 missing/mismatch: {rel}"
            )

        file_rows = 0

        with path.open(
            newline="",
            encoding="utf-8-sig",
        ) as f:
            reader = csv.DictReader(f)

            columns = reader.fieldnames or []

            schema_set.add(
                tuple(columns)
            )

            mc = choose(
                columns,
                MARKET_KEYS,
            )

            fc = choose(
                columns,
                FRAME_KEYS,
            )

            tc = choose(
                columns,
                TIME_KEYS,
            )

            ftc = choose(
                columns,
                FEATURE_TIME_KEYS,
            )

            lc = choose(
                columns,
                LABEL_KEYS,
            )

            signal_col_seen |= bool(tc)
            feature_col_seen |= bool(ftc)
            market_col_seen |= bool(mc)
            frame_col_seen |= bool(fc)

            if not tc:
                problems.append(
                    f"No recognized signal timestamp: {rel}"
                )

            for row in reader:
                file_rows += 1
                rows_joined += 1

                market = (
                    (row.get(mc) or "").strip()
                    if mc
                    else path.stem
                )

                frame = (
                    (row.get(fc) or "").strip()
                    if fc
                    else path.parent.name
                )

                group = (
                    frame,
                    market,
                )

                stats[group]["joined"] += 1

                if lc:
                    stats[group][
                        "joined_" + label(row.get(lc))
                    ] += 1

                t = (
                    stamp(row.get(tc))
                    if tc
                    else None
                )

                ft = (
                    stamp(row.get(ftc))
                    if ftc
                    else None
                )

                if tc and t is None:
                    bad_timestamp += 1

                if ftc and ft is None:
                    no_feature_time += 1

                if t and ft and ft > t:
                    future += 1

        shard_rows.append(
            {
                "file": rel,
                "rows": file_rows,
                "sha256_verified": hash_ok,
            }
        )

        if (
            index % 25 == 0
            or index == len(shard_files)
        ):
            print(
                f"[SHARDS] "
                f"{index}/{len(shard_files)} "
                f"rows={rows_joined:,}",
                flush=True,
            )

    if (
        not signal_col_seen
        or not market_col_seen
        or not frame_col_seen
    ):
        problems.append(
            "Cannot establish all "
            "market/timeframe/signal timestamp columns"
        )

    if not feature_col_seen:
        problems.append(
            "No recognized feature availability timestamp; "
            "point-in-time not proven"
        )

    if bad_timestamp:
        problems.append(
            f"Invalid signal timestamps: {bad_timestamp}"
        )

    if no_feature_time:
        problems.append(
            f"Invalid/missing feature timestamps: "
            f"{no_feature_time}"
        )

    if future:
        problems.append(
            f"Feature timestamps after signal: {future}"
        )

    expected_rows = prior.get(
        "joined_rows"
    )

    if (
        expected_rows is not None
        and rows_joined != int(expected_rows)
    ):
        problems.append(
            f"JOIN row count mismatch: "
            f"{rows_joined} vs {expected_rows}"
        )

    # ----------------------------------------
    # TRAIN coverage audit
    # ----------------------------------------

    train_rows = 0
    train_bad_time = 0
    train_label_col = None

    with train.open(
        newline="",
        encoding="utf-8-sig",
    ) as f:
        reader = csv.DictReader(f)

        columns = reader.fieldnames or []

        mc = choose(
            columns,
            MARKET_KEYS,
        )

        fc = choose(
            columns,
            FRAME_KEYS,
        )

        tc = choose(
            columns,
            TIME_KEYS,
        )

        lc = choose(
            columns,
            LABEL_KEYS,
        )

        train_label_col = lc

        if not mc or not fc or not tc:
            problems.append(
                "TRAIN market/timeframe/signal "
                "timestamp schema not fully recognized"
            )

        for row in reader:
            train_rows += 1

            if mc and fc:
                group = (
                    (row.get(fc) or "").strip(),
                    (row.get(mc) or "").strip(),
                )

                stats[group]["source"] += 1

                if lc:
                    stats[group][
                        "source_" + label(row.get(lc))
                    ] += 1

            if tc and stamp(row.get(tc)) is None:
                train_bad_time += 1

            if (
                args.progress_every > 0
                and train_rows % args.progress_every == 0
            ):
                print(
                    f"[TRAIN] {train_rows:,}",
                    flush=True,
                )

    if train_bad_time:
        problems.append(
            f"Invalid TRAIN timestamps: "
            f"{train_bad_time}"
        )

    if (
        prior.get("source_rows") is not None
        and train_rows != int(
            prior["source_rows"]
        )
    ):
        problems.append(
            "TRAIN source count differs "
            "from previous audit"
        )

    # ----------------------------------------
    # Coverage summary
    # ----------------------------------------

    coverage = []

    for (
        frame,
        market,
    ), counts in sorted(stats.items()):

        source = counts["source"]
        joined = counts["joined"]

        record = {
            "timeframe": frame,
            "market": market,
            "source_rows": source,
            "joined_rows": joined,
            "difference": source - joined,
            "join_rate_pct": (
                round(
                    100 * joined / source,
                    6,
                )
                if source
                else None
            ),
        }

        for lab in (
            "HIT",
            "MISS",
            "UNKNOWN",
        ):
            record[
                f"source_{lab.lower()}"
            ] = counts[
                "source_" + lab
            ]

            record[
                f"joined_{lab.lower()}"
            ] = counts[
                "joined_" + lab
            ]

        coverage.append(record)

    # ----------------------------------------
    # Research safety gates
    # ----------------------------------------

    if (
        prior.get(
            "row_level_reconciliation_proven"
        )
        is not True
    ):
        problems.append(
            "Prior row-level reconciliation "
            "is not proven"
        )

    if not prior.get(
        "integrity_verified"
    ):
        problems.append(
            "Prior JOIN integrity gate "
            "is not verified"
        )

    if not prior.get(
        "train_experiment_approved",
        False,
    ):
        problems.append(
            "Prior TRAIN experiment approval "
            "remains false"
        )

    if not train_label_col:
        problems.append(
            "TRAIN +30% label column not recognized; "
            "label coverage unverified"
        )

    # Timestamp ordering is necessary but
    # insufficient to establish no leakage.

    problems.append(
        "Feature formula lookback and "
        "closed-candle provenance not "
        "independently verified"
    )

    problems.append(
        "Exact row-level JOIN selection bias "
        "not independently verified"
    )

    # ----------------------------------------
    # Final checks
    # ----------------------------------------

    checks[
        "input_shard_hashes_verified"
    ] = (
        bool(expected)
        and all(
            row["sha256_verified"]
            for row in shard_rows
        )
    )

    checks[
        "timestamp_order_violations"
    ] = future

    checks[
        "feature_timestamp_missing_or_invalid"
    ] = no_feature_time

    checks[
        "signal_timestamp_invalid"
    ] = bad_timestamp

    checks[
        "train_timestamp_invalid"
    ] = train_bad_time

    checks[
        "feature_formula_proven"
    ] = False

    checks[
        "closed_candle_proven"
    ] = False

    checks[
        "exact_join_selection_bias_proven"
    ] = False

    checks[
        "prior_integrity_verified"
    ] = (
        prior.get("integrity_verified")
        is True
    )

    checks[
        "train_label_column"
    ] = train_label_col

    checks[
        "schema_variants"
    ] = len(schema_set)

    checks[
        "shard_count"
    ] = len(shard_files)

    checks[
        "source_rows"
    ] = train_rows

    checks[
        "joined_rows"
    ] = rows_joined

    checks[
        "join_rate_pct"
    ] = (
        round(
            rows_joined * 100 / train_rows,
            6,
        )
        if train_rows
        else 0
    )

    checks[
        "research_decision"
    ] = "HOLD"

    checks[
        "train_experiment_approved"
    ] = False

    checks[
        "independent_oos_proven"
    ] = False

    checks[
        "approved_for_live_trading"
    ] = False

    checks[
        "version"
    ] = VERSION

    checks[
        "execution_status"
    ] = (
        "PASS"
        if (
            checks[
                "input_shard_hashes_verified"
            ]
            and not future
            and not bad_timestamp
            and not train_bad_time
            and rows_joined
            == prior.get("joined_rows")
        )
        else "FAIL"
    )

    checks[
        "limitations"
    ] = sorted(
        set(problems)
    )

    # ----------------------------------------
    # Save reports
    # ----------------------------------------

    write_csv(
        out / "point_in_time_coverage.csv",
        [
            "timeframe",
            "market",
            "source_rows",
            "joined_rows",
            "difference",
            "join_rate_pct",
            "source_hit",
            "joined_hit",
            "source_miss",
            "joined_miss",
            "source_unknown",
            "joined_unknown",
        ],
        coverage,
    )

    write_csv(
        out / "point_in_time_shards.csv",
        [
            "file",
            "rows",
            "sha256_verified",
        ],
        shard_rows,
    )

    write_json(
        out / "point_in_time_result.json",
        checks,
    )

    output_hashes = {
        path.name: sha(path)
        for path in sorted(
            out.glob("point_in_time_*")
        )
        if (
            path.name
            != "point_in_time_manifest.json"
            and path.is_file()
        )
    }

    write_json(
        out / "point_in_time_manifest.json",
        {
            "version": VERSION,
            "input_sha256": sources,
            "output_sha256": output_hashes,
        },
    )

    # ----------------------------------------
    # GitHub Actions summary
    # ----------------------------------------

    summary = os.environ.get(
        "GITHUB_STEP_SUMMARY"
    )

    if summary:
        with open(
            summary,
            "a",
            encoding="utf-8",
        ) as f:

            f.write(
                "## Pattern 256 Feature "
                "Point-in-Time & Coverage Gate\n\n"
            )

            f.write(
                "| Check | Result |\n"
                "|---|---|\n"
            )

            keys = (
                "execution_status",
                "version",
                "source_rows",
                "joined_rows",
                "join_rate_pct",
                "shard_count",
                "schema_variants",
                "timestamp_order_violations",
                "feature_formula_proven",
                "closed_candle_proven",
                "train_experiment_approved",
                "research_decision",
            )

            for key in keys:
                f.write(
                    f"| {key} | {checks[key]} |\n"
                )

            f.write(
                "\n**Limitations**\n"
            )

            for item in checks[
                "limitations"
            ][:20]:
                f.write(
                    f"- {item}\n"
                )

    print(
        json.dumps(
            checks,
            indent=2,
            ensure_ascii=False,
        ),
        flush=True,
    )

    if checks["execution_status"] == "PASS":
        return 0

    return 1


if __name__ == "__main__":
    try:
        sys.exit(main())

    except Exception as exc:
        print(
            f"FATAL: "
            f"{type(exc).__name__}: {exc}",
            file=sys.stderr,
        )

        sys.exit(1)
