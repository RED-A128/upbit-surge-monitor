#!/usr/bin/env python3
"""
Pattern 256 Market Regime Validation

Version: Clean V002

Research only.
No automated trading.

Changes:
- Correct Upbit OHLCV timestamp parsing.
- Prefer candle_date_time_utc over numeric timestamp.
- Support Unix timestamps in seconds, milliseconds,
  microseconds, and nanoseconds.
- Validate candle order and prices.
- Preserve historical point-in-time safety.
- Verify source and OHLCV SHA256 for resume.
- Never modify source OHLCV or backtest files.
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


VERSION = "Clean V002"

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

OUTPUT_CSV = "market_regime_comparison.csv"
OUTPUT_JSON = "market_regime_result.json"
OUTPUT_MANIFEST = "market_regime_manifest.json"


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
    """
    Parse ISO 8601 timestamps.

    Naive timestamps are rejected.
    """

    s = str(value).strip()

    if not s:
        raise ValueError("Empty datetime")

    if s.endswith("Z"):
        s = s[:-1] + "+00:00"

    d = datetime.fromisoformat(s)

    if d.tzinfo is None:
        raise ValueError(
            "Timezone missing: " + s
        )

    return d.astimezone(timezone.utc)


def unix_timestamp(value):
    """
    Convert numeric Unix timestamp to UTC.

    Supported units:
    - seconds
    - milliseconds
    - microseconds
    - nanoseconds
    """

    s = str(value).strip()

    if not s:
        raise ValueError("Empty Unix timestamp")

    number = float(s)

    if not math.isfinite(number):
        raise ValueError(
            "Non-finite Unix timestamp"
        )

    absolute = abs(number)

    if absolute >= 1e17:
        seconds = number / 1_000_000_000

    elif absolute >= 1e14:
        seconds = number / 1_000_000

    elif absolute >= 1e11:
        seconds = number / 1_000

    else:
        seconds = number

    try:
        return datetime.fromtimestamp(
            seconds,
            tz=timezone.utc,
        )

    except (
        OverflowError,
        OSError,
        ValueError,
    ) as e:
        raise ValueError(
            "Invalid Unix timestamp: " + s
        ) from e


def parse_candle_time(value, column):
    """
    Parse OHLCV candle OPEN timestamp.
    """

    name = str(column).strip().lower()

    if name == "timestamp":
        return unix_timestamp(value)

    s = str(value).strip()

    if not s:
        raise ValueError(
            "Empty candle timestamp"
        )

    try:
        return utc(s)

    except ValueError:
        if name in (
            "time",
            "datetime",
        ):
            try:
                return unix_timestamp(s)
            except ValueError:
                pass

        raise


def atomic_json(path, obj):
    temp = path.with_suffix(
        path.suffix + ".tmp"
    )

    try:
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

    finally:
        if temp.exists():
            temp.unlink()


def atomic_csv(path, rows):
    temp = path.with_suffix(
        path.suffix + ".tmp"
    )

    try:
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

    finally:
        if temp.exists():
            temp.unlink()


def locate_candles(base, tf, market):
    """
    Accept common OHLCV layouts.

    Reject ambiguous matches.
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
    """
    Read and validate candle OPEN times and closes.

    Upbit columns:
    market,
    candle_date_time_utc,
    candle_date_time_kst,
    timestamp,
    open,
    high,
    low,
    close,
    volume,
    trade_value

    Prefer candle_date_time_utc.
    """

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

        # Important:
        # Prefer ISO UTC over numeric timestamp.
        time_candidates = (
            "candle_date_time_utc",
            "timestamp",
            "time",
            "datetime",
        )

        tcol = next(
            (
                columns[x]
                for x in time_candidates
                if x in columns
            ),
            None,
        )

        close_candidates = (
            "close",
            "trade_price",
            "closing_price",
        )

        ccol = next(
            (
                columns[x]
                for x in close_candidates
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

        times = []
        closes = []

        last = None

        for line, row in enumerate(
            reader,
            2,
        ):
            try:
                raw_time = row.get(tcol)

                raw_close = row.get(ccol)

                if raw_time is None:
                    raise ValueError(
                        "Missing timestamp value"
                    )

                if raw_close is None:
                    raise ValueError(
                        "Missing close value"
                    )

                t = parse_candle_time(
                    raw_time,
                    tcol,
                )

                close = float(raw_close)

                if (
                    not math.isfinite(close)
                    or close <= 0
                ):
                    raise ValueError(
                        "Invalid close"
                    )

                if (
                    last is not None
                    and t <= last
                ):
                    raise ValueError(
                        "Non-increasing or "
                        "duplicate candle timestamp"
                    )

                # Check alignment with timeframe.
                period_seconds = int(
                    PERIODS[tf].total_seconds()
                )

                epoch_seconds = int(
                    t.timestamp()
                )

                if (
                    epoch_seconds
                    % period_seconds
                    != 0
                ):
                    raise ValueError(
                        "Candle timestamp "
                        "not aligned to timeframe"
                    )

                times.append(t)
                closes.append(close)

                last = t

            except Exception as e:
                raise ValueError(
                    f"Bad candle {path}:{line}: {e}"
                ) from e

    return times, closes


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

    Only candles satisfying:

        candle_open + timeframe <= signal

    may be used.

    This prevents future information leakage.
    """

    if not times:
        return "MISSING_CANDLES"

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

    # Ensure selected candle really closed
    # no later than signal time.
    if (
        times[idx] + PERIODS[tf]
        > signal
    ):
        raise ValueError(
            "Future candle leakage detected"
        )

    # Reject gaps inside the lookback.
    for j in range(
        idx - lookback + 1,
        idx + 1,
    ):
        if (
            times[j] - times[j - 1]
            != PERIODS[tf]
        ):
            return "CANDLE_GAP"

    previous_close = closes[
        idx - lookback
    ]

    current_close = closes[idx]

    ret = (
        current_close
        / previous_close
        - 1.0
    )

    if ret >= up:
        return "UP"

    if ret <= down:
        return "DOWN"

    return "SIDEWAYS"


def validate_resume(
    result_path,
    csv_path,
    manifest_path,
    source_hash,
    config,
):
    """
    Skip only when:
    - source hash matches
    - settings match
    - output hashes match
    - all previously used candle hashes match
    - output structure is valid
    """

    required_paths = (
        result_path,
        csv_path,
        manifest_path,
    )

    if not all(
        p.is_file()
        for p in required_paths
    ):
        return False

    try:
        manifest = json.loads(
            manifest_path.read_text(
                encoding="utf-8"
            )
        )

        if manifest.get("version") != VERSION:
            return False

        if (
            manifest.get("source_sha256")
            != source_hash
        ):
            return False

        if manifest.get("config") != config:
            return False

        output_hashes = manifest.get(
            "output_sha256"
        )

        if not isinstance(
            output_hashes,
            dict,
        ):
            return False

        expected_outputs = {
            csv_path.name,
            result_path.name,
        }

        if set(output_hashes) != expected_outputs:
            return False

        for name, expected_hash in (
            output_hashes.items()
        ):
            path = result_path.parent / name

            if not path.is_file():
                return False

            if sha256(path) != expected_hash:
                return False

        candle_hashes = manifest.get(
            "candle_sha256"
        )

        if not isinstance(
            candle_hashes,
            dict,
        ):
            return False

        for name, expected_hash in (
            candle_hashes.items()
        ):
            path = Path(name)

            if not path.is_file():
                return False

            if sha256(path) != expected_hash:
                return False

        result = json.loads(
            result_path.read_text(
                encoding="utf-8"
            )
        )

        if result.get("version") != VERSION:
            return False

        if (
            result.get("execution_status")
            != "PASS"
        ):
            return False

        if (
            result.get("source_sha256")
            != source_hash
        ):
            return False

        if (
            result.get("approved_for_live_trading")
            is not False
        ):
            return False

        if (
            result.get("config")
            != config
        ):
            return False

        with csv_path.open(
            "r",
            encoding="utf-8-sig",
            newline="",
        ) as f:
            reader = csv.DictReader(f)

            if reader.fieldnames != FIELDS:
                return False

            row_count = sum(
                1 for _ in reader
            )

        if (
            row_count
            != result.get("comparison_rows")
        ):
            return False

        return True

    except (
        ValueError,
        KeyError,
        OSError,
        TypeError,
    ):
        return False


def run(args):
    root = args.root.resolve()

    source = (
        root / SOURCE_REL
    ).resolve()

    out = (
        root / OUT_REL
    ).resolve()

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

    if (
        not math.isfinite(
            args.down_threshold
        )
        or not math.isfinite(
            args.up_threshold
        )
    ):
        raise ValueError(
            "Thresholds must be finite"
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

    # Protect source data from output collisions.
    if (
        out == source.parent
        or out == candles_root
        or out in source.parents
        or out in candles_root.parents
        or candles_root in out.parents
    ):
        raise ValueError(
            "Unsafe output directory"
        )

    out.mkdir(
        parents=True,
        exist_ok=True,
    )

    result_path = (
        out / OUTPUT_JSON
    )

    csv_path = (
        out / OUTPUT_CSV
    )

    manifest_path = (
        out / OUTPUT_MANIFEST
    )

    print(
        "Pattern 256 Market Regime "
        + VERSION,
        flush=True,
    )

    print(
        "Calculating source SHA256...",
        flush=True,
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
        "strict_candles": args.strict_candles,
    }

    if not args.force:
        if validate_resume(
            result_path,
            csv_path,
            manifest_path,
            source_hash,
            config,
        ):
            print(
                "Verified existing output: SKIP",
                flush=True,
            )
            return

    counts = defaultdict(
        lambda: [0, 0]
    )

    excluded = Counter()

    input_rows = 0

    files_used = {}

    current_key = None

    times = []
    closes = []

    print(
        "Starting market regime analysis...",
        flush=True,
    )

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

            if input_rows % 500000 == 0:
                print(
                    f"processed={input_rows:,}",
                    flush=True,
                )

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

            try:
                horizon = int(
                    row["horizon"]
                )

            except (
                ValueError,
                TypeError,
            ) as e:
                raise ValueError(
                    "Invalid horizon at "
                    f"source row {input_rows + 1}"
                ) from e

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

            count_key = (
                split,
                tf,
                horizon,
                reg,
                group,
            )

            c = counts[count_key]

            c[0] += 1
            c[1] += int(hit)

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

    # Verify source data did not change
    # during analysis.
    final_source_hash = sha256(source)

    if final_source_hash != source_hash:
        raise RuntimeError(
            "Source CSV changed during analysis"
        )

    # Verify used candle files did not change.
    for name, expected_hash in (
        files_used.items()
    ):
        if sha256(Path(name)) != expected_hash:
            raise RuntimeError(
                "OHLCV changed during analysis: "
                + name
            )

    atomic_csv(
        csv_path,
        rows,
    )

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

    if not validate_resume(
        result_path,
        csv_path,
        manifest_path,
        source_hash,
        config,
    ):
        raise RuntimeError(
            "Final output integrity "
            "verification failed"
        )

    print(
        json.dumps(
            {
                "version": VERSION,
                "execution_status": "PASS",
                "research_decision": "HOLD",
                "source_rows": input_rows,
                "comparison_rows": len(rows),
                "used_candle_files": len(files_used),
                "excluded": dict(excluded),
                "sha256_verified": True,
                "approved_for_live_trading": False,
            },
            indent=2,
        ),
        flush=True,
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
