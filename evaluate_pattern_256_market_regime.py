#!/usr/bin/env python3
"""
Pattern 256 Market Regime Validation
Version: Clean V001

Research only.
Historical regime uses only candles closed by signal time.
"""

import argparse
import bisect
import csv
import hashlib
import json
import math
import os
import sys

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path


VERSION = "Clean V001"

SOURCE_REL = Path(
    "data/reports/"
    "pattern_256_accumulation_candidate_independent_backtest/"
    "backtest_trades.csv"
)

OUT_REL = Path(
    "data/reports/pattern_256_market_regime"
)

PERIODS = {
    "h1": timedelta(hours=1),
    "h4": timedelta(hours=4),
    "d1": timedelta(days=1),
}

GROUPS = (
    "PATTERN_256_ONLY",
    "BOTH",
)

FIELDS = [
    "split",
    "timeframe",
    "horizon",
    "regime",
    "classification",
    "events",
    "hits",
    "hit_rate_pct",
]


def sha256(path):
    h = hashlib.sha256()

    with path.open("rb") as f:
        for block in iter(
            lambda: f.read(4 * 1024 * 1024),
            b"",
        ):
            h.update(block)

    return h.hexdigest()


def utc(value):
    s = str(value).strip()

    if s.endswith("Z"):
        s = s[:-1] + "+00:00"

    d = datetime.fromisoformat(s)

    if d.tzinfo is None:
        raise ValueError(
            "Timezone missing: " + s
        )

    return d.astimezone(timezone.utc)


def atomic_json(path, obj):
    temp = path.with_suffix(
        path.suffix + ".tmp"
    )

    with temp.open(
        "w",
        encoding="utf-8",
        newline="\n",
    ) as f:
        json.dump(
            obj,
            f,
            indent=2,
            ensure_ascii=False,
            sort_keys=True,
        )
        f.write("\n")

    os.replace(temp, path)


def atomic_csv(path, rows):
    temp = path.with_suffix(
        path.suffix + ".tmp"
    )

    with temp.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as f:
        writer = csv.DictWriter(
            f,
            fieldnames=FIELDS,
        )

        writer.writeheader()
        writer.writerows(rows)

    os.replace(temp, path)


def locate_candles(base, tf, market):
    """
    Accept common OHLCV layouts.
    Reject ambiguous matches instead of guessing.
    """

    tokens = [
        market,
        market.replace("-", "_"),
        market.lower(),
        market.lower().replace("-", "_"),
    ]

    paths = []

    for token in tokens:
        for name in (
            token + ".csv",
            token + "_" + tf + ".csv",
            tf + "_" + token + ".csv",
        ):
            paths.extend(
                (
                    base / tf / name,
                    base / name,
                    base / market / (tf + ".csv"),
                )
            )

    matches = sorted(
        {
            p.resolve()
            for p in paths
            if p.is_file()
        }
    )

    if not matches:
        return None

    if len(matches) != 1:
        raise ValueError(
            "Ambiguous OHLCV files for "
            + tf
            + " "
            + market
            + ": "
            + str(matches)
        )

    return matches[0]


def read_candles(path, tf):
    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as f:

        reader = csv.DictReader(f)

        if not reader.fieldnames:
            raise ValueError(
                "Empty candle file: "
                + str(path)
            )

        columns = {
            name.strip().lower(): name
            for name in reader.fieldnames
        }

        tcol = next(
            (
                columns[x]
                for x in (
                    "timestamp",
                    "time",
                    "datetime",
                    "candle_date_time_utc",
                )
                if x in columns
            ),
            None,
        )

        ccol = next(
            (
                columns[x]
                for x in (
                    "close",
                    "trade_price",
                    "closing_price",
                )
                if x in columns
            ),
            None,
        )

        if not tcol or not ccol:
            raise ValueError(
                "Candle timestamp/close "
                "column missing: "
                + str(path)
            )

        candles = []
        last = None

        for line, row in enumerate(
            reader,
            2,
        ):
            try:
                t = utc(row[tcol])
                close = float(row[ccol])

                if (
                    not math.isfinite(close)
                    or close <= 0
                ):
                    raise ValueError(
                        "invalid close"
                    )

                if (
                    last is not None
                    and t <= last
                ):
                    raise ValueError(
                        "non-increasing or "
                        "duplicate candle timestamp"
                    )

                candles.append((t, close))
                last = t

            except Exception as e:
                raise ValueError(
                    f"Bad candle {path}:{line}: {e}"
                ) from e

    return (
        [x[0] for x in candles],
        [x[1] for x in candles],
    )


def regime_at(
    times,
    closes,
    signal,
    tf,
    lookback,
    up,
    down,
):
    """
    Candle timestamps represent candle OPEN times.

    Only candles with:
        candle_open + timeframe <= signal

    are used.
    """

    last_open = signal - PERIODS[tf]

    idx = (
        bisect.bisect_right(
            times,
            last_open,
        )
        - 1
    )

    if idx < lookback:
        return "INSUFFICIENT_HISTORY"

    # Reject missing candles within the lookback.
    for j in range(
        idx - lookback + 1,
        idx + 1,
    ):
        if (
            times[j] - times[j - 1]
            != PERIODS[tf]
        ):
            return "CANDLE_GAP"

    ret = (
        closes[idx]
        / closes[idx - lookback]
        - 1.0
    )

    if ret >= up:
        return "UP"

    if ret <= down:
        return "DOWN"

    return "SIDEWAYS"


def run(args):
    root = args.root.resolve()

    source = root / SOURCE_REL
    out = root / OUT_REL

    candles_root = (
        root / args.ohlcv_root
    ).resolve()

    if not source.is_file():
        raise FileNotFoundError(
            "Missing source CSV: "
            + str(source)
        )

    if not candles_root.is_dir():
        raise FileNotFoundError(
            "Missing OHLCV directory: "
            + str(candles_root)
        )

    if not (
        0
        < args.down_threshold
        < args.up_threshold
    ):
        raise ValueError(
            "Thresholds must satisfy "
            "0 < down < up"
        )

    cutoff = utc(args.cutoff)
    test_end = utc(args.test_end)

    if test_end <= cutoff:
        raise ValueError(
            "test-end must be after cutoff"
        )

    if args.lookback < 2:
        raise ValueError(
            "lookback must be >= 2"
        )

    out.mkdir(
        parents=True,
        exist_ok=True,
    )

    result_path = (
        out / "market_regime_result.json"
    )

    csv_path = (
        out / "market_regime_comparison.csv"
    )

    manifest_path = (
        out / "market_regime_manifest.json"
    )

    source_hash = sha256(source)

    config = {
        "version": VERSION,
        "lookback": args.lookback,
        "up_threshold": args.up_threshold,
        "down_threshold": args.down_threshold,
        "cutoff": cutoff.isoformat(),
        "test_end": test_end.isoformat(),
        "ohlcv_root": str(candles_root),
    }

    # Resume:
    # Skip only when source, settings and outputs
    # match the recorded SHA256 values.
    if (
        not args.force
        and all(
            p.is_file()
            for p in (
                result_path,
                csv_path,
                manifest_path,
            )
        )
    ):
        try:
            manifest = json.loads(
                manifest_path.read_text(
                    encoding="utf-8"
                )
            )

            if (
                manifest.get("source_sha256")
                == source_hash
                and manifest.get("config")
                == config
                and all(
                    sha256(out / name) == digest
                    for name, digest
                    in manifest[
                        "output_sha256"
                    ].items()
                )
            ):
                print(
                    "Verified existing output: SKIP"
                )
                return

        except (
            ValueError,
            KeyError,
            OSError,
            TypeError,
        ):
            pass

    counts = defaultdict(
        lambda: [0, 0]
    )

    excluded = Counter()
    input_rows = 0
    files_used = {}

    current_key = None
    times = []
    closes = []

    with source.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as f:

        reader = csv.DictReader(f)

        required = {
            "timeframe",
            "market",
            "signal_timestamp",
            "classification",
            "horizon",
            "status",
            "hit_30pct",
            "entry_timestamp",
        }

        existing = set(
            reader.fieldnames or []
        )

        if not required.issubset(existing):
            raise ValueError(
                "Missing required source columns: "
                + str(
                    sorted(
                        required - existing
                    )
                )
            )

        for row in reader:
            input_rows += 1

            tf = row[
                "timeframe"
            ].strip()

            market = row[
                "market"
            ].strip()

            group = row[
                "classification"
            ].strip()

            if (
                tf not in PERIODS
                or group not in GROUPS
            ):
                excluded[
                    "unsupported_group"
                ] += 1
                continue

            status = row[
                "status"
            ].strip().upper()

            if status not in (
                "HIT",
                "MISS",
            ):
                excluded[
                    "incomplete_or_other"
                ] += 1
                continue

            hit = row[
                "hit_30pct"
            ].strip()

            if (
                hit not in ("0", "1")
                or (
                    (hit == "1")
                    != (status == "HIT")
                )
            ):
                raise ValueError(
                    "Contradictory outcome at "
                    f"source row {input_rows + 1}"
                )

            signal = utc(
                row["signal_timestamp"]
            )

            entry = utc(
                row["entry_timestamp"]
            )

            horizon = int(
                row["horizon"]
            )

            if (
                horizon <= 0
                or entry < signal
            ):
                raise ValueError(
                    "Invalid entry/horizon at "
                    f"source row {input_rows + 1}"
                )

            maturity = (
                entry
                + PERIODS[tf] * horizon
            )

            # Conservative temporal separation.
            if (
                signal < cutoff
                and maturity <= cutoff
            ):
                split = "TRAIN"

            elif (
                signal >= cutoff
                and maturity <= test_end
            ):
                split = "TEST"

            else:
                excluded[
                    "boundary_or_after_test"
                ] += 1
                continue

            key = (
                tf,
                market,
            )

            if key != current_key:
                candle_path = locate_candles(
                    candles_root,
                    tf,
                    market,
                )

                if candle_path is None:
                    if args.strict_candles:
                        raise FileNotFoundError(
                            "OHLCV not found for "
                            f"{tf} {market} "
                            f"under {candles_root}"
                        )

                    times = []
                    closes = []

                else:
                    times, closes = read_candles(
                        candle_path,
                        tf,
                    )

                    files_used[
                        str(candle_path)
                    ] = sha256(
                        candle_path
                    )

                current_key = key

            reg = regime_at(
                times,
                closes,
                signal,
                tf,
                args.lookback,
                args.up_threshold,
                -args.down_threshold,
            )

            if reg not in (
                "UP",
                "DOWN",
                "SIDEWAYS",
            ):
                excluded[reg] += 1
                continue

            c = counts[
                (
                    split,
                    tf,
                    horizon,
                    reg,
                    group,
                )
            ]

            c[0] += 1
            c[1] += int(hit)

            if input_rows % 500000 == 0:
                print(
                    f"processed={input_rows:,}",
                    flush=True,
                )

    rows = []

    for key in sorted(counts):
        n, hits = counts[key]

        rows.append(
            dict(
                zip(
                    FIELDS[:5],
                    key,
                ),
                events=n,
                hits=hits,
                hit_rate_pct=round(
                    100 * hits / n,
                    6,
                ),
            )
        )

    atomic_csv(
        csv_path,
        rows,
    )

    result = {
        "version": VERSION,
        "execution_status": "PASS",
        "research_decision": "HOLD",
        "approved_for_live_trading": False,
        "independent_oos_proven": False,
        "statistical_independence_proven": False,
        "profitability_proven": False,
        "source_rows": input_rows,
        "source_sha256": source_hash,
        "comparison_rows": len(rows),
        "excluded": dict(excluded),
        "used_candle_files": len(files_used),
        "config": config,
        "caution": (
            "Per-market trailing trend, "
            "not market-wide regime; "
            "observational only. "
            "Candle OPEN timestamps assumed; "
            "completed candles only. "
            "No inference of independence "
            "or live-trading readiness."
        ),
    }

    atomic_json(
        result_path,
        result,
    )

    manifest = {
        "version": VERSION,
        "source_sha256": source_hash,
        "config": config,
        "candle_sha256": files_used,
        "output_sha256": {
            csv_path.name: sha256(
                csv_path
            ),
            result_path.name: sha256(
                result_path
            ),
        },
    }

    atomic_json(
        manifest_path,
        manifest,
    )

    print(
        json.dumps(
            {
                "execution_status": "PASS",
                "research_decision": "HOLD",
                "source_rows": input_rows,
                "comparison_rows": len(rows),
                "excluded": dict(excluded),
            },
            indent=2,
        )
    )


def main():
    p = argparse.ArgumentParser(
        description=__doc__
    )

    p.add_argument(
        "--root",
        type=Path,
        default=Path("."),
    )

    p.add_argument(
        "--ohlcv-root",
        default="data/ohlcv",
    )

    p.add_argument(
        "--lookback",
        type=int,
        default=60,
    )

    p.add_argument(
        "--up-threshold",
        type=float,
        default=0.10,
    )

    p.add_argument(
        "--down-threshold",
        type=float,
        default=0.10,
    )

    p.add_argument(
        "--cutoff",
        default=(
            "2025-01-01T00:00:00+00:00"
        ),
    )

    p.add_argument(
        "--test-end",
        default=(
            "2026-09-01T00:00:00+00:00"
        ),
    )

    p.add_argument(
        "--strict-candles",
        action="store_true",
    )

    p.add_argument(
        "--force",
        action="store_true",
    )

    try:
        run(
            p.parse_args()
        )

    except Exception as e:
        print(
            "FATAL:",
            type(e).__name__,
            str(e),
            file=sys.stderr,
        )

        sys.exit(1)


if __name__ == "__main__":
    main()
