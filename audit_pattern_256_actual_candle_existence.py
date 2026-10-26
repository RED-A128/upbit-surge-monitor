#!/usr/bin/env python3
"""
Pattern 256 Actual Candle Existence Audit
Version: Clean V001

Purpose:
    Verify JOIN entry candles against actual OHLCV candles.

Safety:
    - No automated trading.
    - No source data modification.
    - No interpolation of missing candles.
    - No claim of leakage-free features.
    - Research decision remains HOLD.
"""

import argparse
import csv
import hashlib
import json
import math
import os
import sqlite3
import sys

from collections import Counter
from datetime import datetime, timezone
from pathlib import Path


VERSION = "Clean V001"

TF_SECONDS = {
    "h1": 3600,
    "h4": 14400,
    "d1": 86400,
}

JOIN_COLUMNS = {
    "timeframe",
    "market",
    "signal_timestamp",
    "entry_timestamp",
    "entry_price",
    "feature_candle_timestamp",
    "feature_candle_end_timestamp",
}

OHLCV_COLUMNS = {
    "market",
    "candle_date_time_utc",
    "open",
    "high",
    "low",
    "close",
}

ISSUE_LIMIT = 20000


def sha256(path):
    h = hashlib.sha256()

    with path.open("rb") as f:
        for chunk in iter(
            lambda: f.read(1024 * 1024),
            b"",
        ):
            h.update(chunk)

    return h.hexdigest()


def utc(value):
    d = datetime.fromisoformat(
        value.strip().replace("Z", "+00:00")
    )

    if d.tzinfo is None:
        raise ValueError("naive timestamp")

    return d.astimezone(timezone.utc)


def ms(value):
    return int(
        utc(value).timestamp() * 1000
    )


def num(value):
    n = float(value)

    if not math.isfinite(n):
        raise ValueError("nonfinite number")

    return n


def equal_price(a, b):
    return math.isclose(
        a,
        b,
        rel_tol=1e-8,
        abs_tol=1e-10,
    )


def atomic_json(path, obj):
    tmp = path.with_name(
        path.name + ".tmp"
    )

    with tmp.open(
        "w",
        encoding="utf-8",
        newline="\n",
    ) as f:
        json.dump(
            obj,
            f,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        f.write("\n")

    os.replace(tmp, path)


def list_shards(join_dir):
    files = sorted(
        (join_dir / "shards").glob("*/*.csv")
    )

    if not files:
        raise FileNotFoundError(
            "No JOIN shards found under "
            + str(join_dir / "shards")
        )

    for p in files:
        if (
            p.parent.name not in TF_SECONDS
            or not p.name.startswith("KRW-")
        ):
            raise ValueError(
                "Unexpected shard path: "
                + str(p)
            )

    return files


def read_csv(path, required):
    f = path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    )

    reader = csv.DictReader(f)

    if (
        not reader.fieldnames
        or not required.issubset(
            set(reader.fieldnames)
        )
    ):
        f.close()

        raise ValueError(
            f"Missing columns in {path}: "
            f"{sorted(required - set(reader.fieldnames or []))}"
        )

    return f, reader


def audit(root, output_dir, issue_limit, strict):
    join_dir = (
        root
        / "data"
        / "reports"
        / "pattern_256_train_feature_join"
    )

    market_dir = (
        root
        / "data"
        / "ohlcv"
    )

    shards = list_shards(join_dir)

    resolved_output = output_dir.resolve()
    resolved_root = root.resolve()

    if resolved_output == resolved_root:
        raise ValueError(
            "Unsafe output directory"
        )

    if (
        resolved_output == market_dir.resolve()
        or resolved_output == join_dir.resolve()
    ):
        raise ValueError(
            "Output cannot overwrite source directory"
        )

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    issues_path = (
        output_dir
        / "actual_candle_existence_issues.csv"
    )

    summary_path = (
        output_dir
        / "actual_candle_existence_by_market.csv"
    )

    result_path = (
        output_dir
        / "actual_candle_existence_result.json"
    )

    manifest_path = (
        output_dir
        / "actual_candle_existence_manifest.json"
    )

    db_path = (
        output_dir
        / "actual_candle_existence_index.sqlite"
    )

    # Only a derived temporary index is removed.
    # Original OHLCV and JOIN files are untouched.

    if db_path.exists():
        db_path.unlink()

    con = sqlite3.connect(
        str(db_path)
    )

    con.execute(
        "PRAGMA journal_mode=DELETE"
    )

    con.execute(
        "PRAGMA synchronous=NORMAL"
    )

    con.execute(
        """
        CREATE TABLE candles (
            t INTEGER PRIMARY KEY,
            o REAL NOT NULL
        )
        """
    )

    totals = Counter()
    per_market = []
    inputs = {}

    issues_written = 0

    issue_fields = [
        "timeframe",
        "market",
        "signal_timestamp",
        "entry_timestamp",
        "issue",
        "detail",
    ]

    with issues_path.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as issue_f:

        iw = csv.DictWriter(
            issue_f,
            fieldnames=issue_fields,
        )

        iw.writeheader()

        for index, shard in enumerate(
            shards,
            1,
        ):
            tf = shard.parent.name
            market = shard.stem

            ohlcv = (
                market_dir
                / tf
                / shard.name
            )

            local = Counter()

            con.execute(
                "DELETE FROM candles"
            )

            if not ohlcv.is_file():
                local[
                    "missing_ohlcv_file"
                ] += 1

                totals[
                    "missing_ohlcv_files"
                ] += 1

                candles_available = False

            else:
                candles_available = True

                inputs[
                    ohlcv.relative_to(
                        root
                    ).as_posix()
                ] = sha256(ohlcv)

                f, rows = read_csv(
                    ohlcv,
                    OHLCV_COLUMNS,
                )

                with f:
                    batch = []
                    previous = None

                    for row in rows:
                        local[
                            "ohlcv_rows"
                        ] += 1

                        if row["market"] != market:
                            local[
                                "ohlcv_wrong_market"
                            ] += 1
                            continue

                        try:
                            t = ms(
                                row[
                                    "candle_date_time_utc"
                                ]
                            )

                            op = num(
                                row["open"]
                            )

                            if op <= 0:
                                raise ValueError(
                                    "nonpositive open"
                                )

                        except (
                            ValueError,
                            OverflowError,
                        ):
                            local[
                                "ohlcv_invalid_rows"
                            ] += 1
                            continue

                        if (
                            previous is not None
                            and t <= previous
                        ):
                            local[
                                "ohlcv_nonincreasing_rows"
                            ] += 1

                        previous = t

                        batch.append(
                            (t, op)
                        )

                        if len(batch) >= 5000:
                            for v in batch:
                                try:
                                    con.execute(
                                        """
                                        INSERT INTO candles(t,o)
                                        VALUES(?,?)
                                        """,
                                        v,
                                    )

                                except sqlite3.IntegrityError:
                                    local[
                                        "ohlcv_duplicate_timestamps"
                                    ] += 1

                            batch.clear()

                    for v in batch:
                        try:
                            con.execute(
                                """
                                INSERT INTO candles(t,o)
                                VALUES(?,?)
                                """,
                                v,
                            )

                        except sqlite3.IntegrityError:
                            local[
                                "ohlcv_duplicate_timestamps"
                            ] += 1

                con.commit()

            inputs[
                shard.relative_to(
                    root
                ).as_posix()
            ] = sha256(shard)

            f, rows = read_csv(
                shard,
                JOIN_COLUMNS,
            )

            with f:
                for row in rows:
                    local[
                        "join_rows"
                    ] += 1

                    signal = row.get(
                        "signal_timestamp",
                        "",
                    )

                    entry = row.get(
                        "entry_timestamp",
                        "",
                    )

                    issue = None
                    detail = ""

                    if (
                        row["timeframe"] != tf
                        or row["market"] != market
                    ):
                        issue = (
                            "JOIN_SHARD_KEY_MISMATCH"
                        )

                    else:
                        try:
                            entry_ms = ms(entry)

                            signal_ms = ms(
                                signal
                            )

                            feat_ms = ms(
                                row[
                                    "feature_candle_timestamp"
                                ]
                            )

                            feat_end_ms = ms(
                                row[
                                    "feature_candle_end_timestamp"
                                ]
                            )

                            price = num(
                                row[
                                    "entry_price"
                                ]
                            )

                            if price <= 0:
                                raise ValueError(
                                    "nonpositive entry"
                                )

                            step_ms = (
                                TF_SECONDS[tf]
                                * 1000
                            )

                            if (
                                feat_end_ms
                                != feat_ms + step_ms
                            ):
                                issue = (
                                    "FEATURE_END_MISMATCH"
                                )

                            elif (
                                entry_ms
                                < feat_end_ms
                            ):
                                issue = (
                                    "ENTRY_BEFORE_FEATURE_CLOSE"
                                )

                            elif not candles_available:
                                issue = (
                                    "OHLCV_FILE_MISSING"
                                )

                            else:
                                found = con.execute(
                                    """
                                    SELECT o
                                    FROM candles
                                    WHERE t=?
                                    """,
                                    (entry_ms,),
                                ).fetchone()

                                if found is None:
                                    issue = (
                                        "ENTRY_CANDLE_MISSING"
                                    )

                                elif not equal_price(
                                    price,
                                    found[0],
                                ):
                                    issue = (
                                        "ENTRY_OPEN_PRICE_MISMATCH"
                                    )

                                    detail = (
                                        f"join={price:.12g};"
                                        f"ohlcv_open={found[0]:.12g}"
                                    )

                                else:
                                    local[
                                        "entry_candle_and_price_verified"
                                    ] += 1

                                if (
                                    entry_ms
                                    > feat_end_ms
                                ):
                                    local[
                                        "delayed_entry_rows"
                                    ] += 1

                                if (
                                    feat_ms
                                    != signal_ms
                                ):
                                    local[
                                        "feature_signal_timestamp_different"
                                    ] += 1

                                feature_found = con.execute(
                                    """
                                    SELECT 1
                                    FROM candles
                                    WHERE t=?
                                    """,
                                    (feat_ms,),
                                ).fetchone()

                                if feature_found is None:
                                    local[
                                        "feature_candle_missing"
                                    ] += 1

                                    if issue is None:
                                        issue = (
                                            "FEATURE_CANDLE_MISSING"
                                        )

                        except (
                            ValueError,
                            OverflowError,
                        ) as e:
                            issue = (
                                "INVALID_JOIN_VALUE"
                            )

                            detail = str(e)

                    if issue:
                        local[issue] += 1

                        local[
                            "issue_rows"
                        ] += 1

                        if issues_written < issue_limit:
                            iw.writerow(
                                {
                                    "timeframe": tf,
                                    "market": market,
                                    "signal_timestamp": signal,
                                    "entry_timestamp": entry,
                                    "issue": issue,
                                    "detail": detail,
                                }
                            )

                            issues_written += 1

            totals.update(local)

            per_market.append(
                {
                    "timeframe": tf,
                    "market": market,
                    **local,
                }
            )

            if (
                index % 10 == 0
                or index == len(shards)
            ):
                print(
                    f"[AUDIT] {index}/{len(shards)} "
                    f"join_rows={totals['join_rows']} "
                    f"issue_rows={totals['issue_rows']}",
                    flush=True,
                )

    con.close()

    db_path.unlink(
        missing_ok=True
    )

    keys = [
        "timeframe",
        "market",
        "join_rows",
        "entry_candle_and_price_verified",
        "delayed_entry_rows",
        "feature_candle_missing",
        "feature_signal_timestamp_different",
        "issue_rows",
        "ENTRY_CANDLE_MISSING",
        "ENTRY_OPEN_PRICE_MISMATCH",
        "FEATURE_CANDLE_MISSING",
        "OHLCV_FILE_MISSING",
        "INVALID_JOIN_VALUE",
        "JOIN_SHARD_KEY_MISMATCH",
        "FEATURE_END_MISMATCH",
        "ENTRY_BEFORE_FEATURE_CLOSE",
        "ohlcv_rows",
        "ohlcv_invalid_rows",
        "ohlcv_wrong_market",
        "ohlcv_duplicate_timestamps",
        "ohlcv_nonincreasing_rows",
    ]

    with summary_path.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as f:
        writer = csv.DictWriter(
            f,
            fieldnames=keys,
            extrasaction="ignore",
        )

        writer.writeheader()

        for r in per_market:
            writer.writerow(r)

    critical = (
        totals["issue_rows"]
        + totals["ohlcv_invalid_rows"]
        + totals["ohlcv_wrong_market"]
        + totals["ohlcv_duplicate_timestamps"]
        + totals["ohlcv_nonincreasing_rows"]
    )

    result = {
        "version": VERSION,
        "execution_status": (
            "PASS"
            if critical == 0
            else "REVIEW_REQUIRED"
        ),
        "source_shards": len(shards),
        "source_rows": totals["join_rows"],
        "verified_entry_candles_and_prices": (
            totals[
                "entry_candle_and_price_verified"
            ]
        ),
        "delayed_entry_rows": (
            totals["delayed_entry_rows"]
        ),
        "feature_candle_missing": (
            totals["feature_candle_missing"]
        ),
        "issues_total": (
            totals["issue_rows"]
        ),
        "issues_recorded": issues_written,
        "issues_truncated": (
            totals["issue_rows"]
            > issues_written
        ),
        "ohlcv_integrity_flags": {
            k: totals[k]
            for k in [
                "ohlcv_invalid_rows",
                "ohlcv_wrong_market",
                "ohlcv_duplicate_timestamps",
                "ohlcv_nonincreasing_rows",
                "missing_ohlcv_files",
            ]
        },
        "classification_counts": dict(
            sorted(
                (k, v)
                for k, v in totals.items()
                if k.isupper()
            )
        ),
        "research_decision": "HOLD",
        "train_experiment_approved": False,
        "point_in_time_proven": False,
        "independent_oos_proven": False,
        "approved_for_live_trading": False,
        "notes": [
            (
                "This verifies timestamp existence "
                "and open price equality in locally "
                "stored OHLCV files."
            ),
            (
                "This does not establish upstream "
                "signal/feature formula leakage safety."
            ),
            (
                "Missing OHLCV intervals are not "
                "filled or interpolated."
            ),
            (
                "Source OHLCV and JOIN files "
                "were not modified."
            ),
        ],
    }

    atomic_json(
        result_path,
        result,
    )

    outputs = {
        p.name: sha256(p)
        for p in (
            issues_path,
            summary_path,
            result_path,
        )
    }

    atomic_json(
        manifest_path,
        {
            "version": VERSION,
            "input_sha256": dict(
                sorted(
                    inputs.items()
                )
            ),
            "output_sha256": outputs,
            "source_shards": len(shards),
            "source_rows": totals["join_rows"],
        },
    )

    print(
        json.dumps(
            result,
            indent=2,
        ),
        flush=True,
    )

    if strict and critical:
        return 2

    return 0


def main():
    ap = argparse.ArgumentParser()

    ap.add_argument(
        "--root",
        default=".",
    )

    ap.add_argument(
        "--output",
        default=None,
    )

    ap.add_argument(
        "--issue-limit",
        type=int,
        default=ISSUE_LIMIT,
    )

    ap.add_argument(
        "--strict",
        action="store_true",
        help=(
            "Return nonzero when audit "
            "detects critical issues"
        ),
    )

    args = ap.parse_args()

    root = Path(
        args.root
    ).resolve()

    output = (
        Path(
            args.output
        ).resolve()
        if args.output
        else (
            root
            / "data"
            / "reports"
            / "pattern_256_actual_candle_existence"
        )
    )

    if args.issue_limit < 0:
        ap.error(
            "--issue-limit must be nonnegative"
        )

    try:
        return audit(
            root,
            output,
            args.issue_limit,
            args.strict,
        )

    except Exception as exc:
        print(
            f"FATAL: "
            f"{type(exc).__name__}: "
            f"{exc}",
            file=sys.stderr,
            flush=True,
        )

        return 1


if __name__ == "__main__":
    sys.exit(main())
