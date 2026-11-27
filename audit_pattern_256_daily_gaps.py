
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Daily Gap Triage
Clean V007

Read-only audit.
Classifies aligned calendar gaps without assuming
they are exchange-confirmed or safe for backtesting.
"""

import argparse
import csv
import hashlib
import json
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

VERSION = "Clean V007"

SOURCE = (
    "data/reports/pattern_256_pit_alignment"
)

OUTPUT = (
    "data/reports/pattern_256_daily_gaps"
)

LAYERS = {
    "ohlcv": "data/ohlcv/d1",
    "features": "data/features/d1",
    "signals": "data/signals/256/d1",
}

FIELDS = (
    "open", "high", "low", "close",
    "volume", "trade_value",
)


def sha256(path):
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(
            lambda: stream.read(1024 * 1024), b""
        ):
            h.update(chunk)
    return h.hexdigest()


def read_json(path):
    with path.open("r", encoding="utf-8-sig") as f:
        return json.load(f)


def write_json(path, data):
    with path.open("w", encoding="utf-8") as f:
        json.dump(
            data, f, indent=2, ensure_ascii=False,
            allow_nan=False
        )
        f.write("\n")


def utc_time(value):
    value = str(value).strip()

    dt = datetime.fromisoformat(
        value.replace("Z", "+00:00")
    )

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc)


def read_candles(path, expected_market):
    rows = []

    with path.open(
        "r", encoding="utf-8-sig", newline=""
    ) as f:
        reader = csv.DictReader(f)

        required = {
            "market",
            "candle_date_time_utc",
            *FIELDS,
        }

        if not reader.fieldnames:
            raise RuntimeError(
                f"Missing header: {path}"
            )

        missing = required - set(reader.fieldnames)

        if missing:
            raise RuntimeError(
                f"Missing columns in {path}: "
                f"{sorted(missing)}"
            )

        for line, row in enumerate(reader, start=2):
            if row.get("market") != expected_market:
                raise RuntimeError(
                    f"Market mismatch: {path}:{line}"
                )

            stamp = utc_time(
                row["candle_date_time_utc"]
            )

            rows.append({
                "line": line,
                "timestamp": stamp,
                "values": {
                    name: row[name]
                    for name in FIELDS
                },
            })

    if not rows:
        raise RuntimeError(
            f"Empty CSV: {path}"
        )

    return rows


def find_gaps(rows):
    gaps = []

    for index in range(1, len(rows)):
        before = rows[index - 1]
        after = rows[index]

        delta = (
            after["timestamp"] - before["timestamp"]
        )

        if delta <= timedelta(0):
            raise RuntimeError(
                "Non-increasing timestamps"
            )

        if delta <= timedelta(days=1, seconds=1):
            continue

        days = delta.total_seconds() / 86400

        # Daily candles must fall on a whole-day grid.
        rounded = round(days)

        if abs(days - rounded) > 0.00002:
            raise RuntimeError(
                "Non-daily interval detected"
            )

        missing_days = rounded - 1

        if missing_days <= 0:
            continue

        gaps.append({
            "before": before["timestamp"].isoformat(),
            "after": after["timestamp"].isoformat(),
            "missing_days": missing_days,
            "missing_start": (
                before["timestamp"]
                + timedelta(days=1)
            ).date().isoformat(),
            "missing_end": (
                after["timestamp"]
                - timedelta(days=1)
            ).date().isoformat(),
            "before_row": before["line"],
            "after_row": after["line"],
            "before_ohlcv": before["values"],
            "after_ohlcv": after["values"],
        })

    return gaps


def gap_key(gap):
    return (
        gap["before"],
        gap["after"],
        gap["missing_days"],
    )


def verify_source(root):
    folder = root / SOURCE
    result_path = folder / "result.json"
    manifest_path = folder / "manifest.json"

    if not result_path.is_file():
        raise RuntimeError(
            "Clean V006 result missing"
        )

    if not manifest_path.is_file():
        raise RuntimeError(
            "Clean V006 manifest missing"
        )

    result = read_json(result_path)
    manifest = read_json(manifest_path)

    if result.get("version") != "Clean V006":
        raise RuntimeError(
            "Unexpected source version"
        )

    if result.get("execution") != "PASS":
        raise RuntimeError(
            "Source execution is not PASS"
        )

    if result.get("candidate") != "d1|DOWN|LOW":
        raise RuntimeError(
            "Candidate mismatch"
        )

    if result.get("research_decision") != "HOLD":
        raise RuntimeError(
            "Source research decision mismatch"
        )

    actual = sha256(result_path)
    expected = manifest.get(
        "sha256", {}
    ).get("result.json", "")

    if actual != expected:
        raise RuntimeError(
            "Clean V006 result SHA256 mismatch"
        )

    if result.get("issue_counts") != {
        "DAILY_GAP": 51
    }:
        raise RuntimeError(
            "Unexpected V006 issue counts; "
            "review source before running V007"
        )

    return result, actual


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--root", required=True
    )

    args = parser.parse_args()
    root = Path(args.root).resolve()

    if not root.is_dir():
        raise RuntimeError(
            "Project root not found"
        )

    source, source_hash = verify_source(root)

    markets = sorted(
        item["market"]
        for item in source["details"]
    )

    if len(markets) != 290:
        raise RuntimeError(
            "Unexpected market count"
        )

    if len(markets) != len(set(markets)):
        raise RuntimeError(
            "Duplicate market in source"
        )

    source_details = {
        item["market"]: item
        for item in source["details"]
    }

    all_gaps = []
    affected = set()
    counts = Counter()
    mismatches = []

    for market in markets:
        layer_gaps = {}

        for layer, folder in LAYERS.items():
            path = (
                root / folder / f"{market}.csv"
            )

            if not path.is_file():
                raise RuntimeError(
                    f"Missing CSV: {path}"
                )

            expected_hash = (
                source_details[market]
                ["files"][layer]["sha256"]
            )

            actual_hash = sha256(path)

            if actual_hash != expected_hash:
                raise RuntimeError(
                    f"Input changed since V006: "
                    f"{layer}/{market}"
                )

            candles = read_candles(
                path, market
            )

            layer_gaps[layer] = find_gaps(
                candles
            )

            counts[layer] += len(
                layer_gaps[layer]
            )

        base = {
            gap_key(g)
            for g in layer_gaps["ohlcv"]
        }

        for layer in ("features", "signals"):
            target = {
                gap_key(g)
                for g in layer_gaps[layer]
            }

            if target != base:
                mismatches.append({
                    "market": market,
                    "layer": layer,
                    "ohlcv_only": [
                        list(x)
                        for x in sorted(base - target)
                    ],
                    "layer_only": [
                        list(x)
                        for x in sorted(target - base)
                    ],
                })

        for gap in layer_gaps["ohlcv"]:
            affected.add(market)

            all_gaps.append({
                "market": market,
                **gap,
                "present_in_all_layers": all(
                    gap_key(gap) in {
                        gap_key(x)
                        for x in layer_gaps[layer]
                    }
                    for layer in LAYERS
                ),
                "cause": "UNVERIFIED",
                "exchange_confirmed": False,
                "safe_for_backtest": False,
                "action": (
                    "VERIFY_EXCHANGE_SOURCE_"
                    "AND_EXCLUDE_CROSS_GAP_WINDOWS"
                ),
            })

        print(
            f"[SCAN] {market}: "
            f"{len(layer_gaps['ohlcv'])} gaps",
            flush=True,
        )

    observed = sum(counts.values())
    expected = source["issue_counts"]["DAILY_GAP"]

    if observed != expected:
        raise RuntimeError(
            f"Gap count changed: "
            f"expected={expected}, "
            f"observed={observed}"
        )

    status = (
        "TRIAGED_UNVERIFIED"
        if not mismatches
        else "ALIGNMENT_MISMATCH"
    )

    output = root / OUTPUT
    output.mkdir(
        parents=True, exist_ok=True
    )

    result = {
        "version": VERSION,
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "execution": "PASS",
        "source_version": "Clean V006",
        "source_result_sha256": source_hash,
        "candidate": "d1|DOWN|LOW",
        "common_markets": len(markets),
        "affected_markets": len(affected),
        "unique_gaps": len(all_gaps),
        "layer_gap_counts": dict(counts),
        "alignment_mismatches": mismatches,
        "classification": status,
        "gaps": all_gaps,
        "exchange_source_verified": False,
        "data_readiness": "BLOCKED",
        "point_in_time_proven": False,
        "independent_oos_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
    }

    result_path = output / "result.json"
    report_path = output / "report.md"
    manifest_path = output / "manifest.json"

    write_json(result_path, result)

    report = [
        "# Pattern 256 Daily Gap Triage",
        "",
        f"Version: {VERSION}",
        "",
        "Execution: PASS",
        "",
        f"Classification: {status}",
        f"Markets checked: {len(markets)}",
        f"Affected markets: {len(affected)}",
        f"Unique gaps: {len(all_gaps)}",
        f"Layer observations: {observed}",
        "",
        "## Gap details",
        "",
        "| Market | Missing start | Missing end | Days |",
        "|---|---|---|---:|",
    ]

    for gap in all_gaps:
        report.append(
            f"| {gap['market']} "
            f"| {gap['missing_start']} "
            f"| {gap['missing_end']} "
            f"| {gap['missing_days']} |"
        )

    report.extend([
        "",
        "## Interpretation",
        "",
        "- Exchange source has not been verified.",
        "- Gaps may be real or collection-related.",
        "- Do not interpolate missing candles.",
        "- Exclude windows crossing unverified gaps.",
        "- Alignment does not establish causality.",
        "- No OOS or profitability approval.",
        "- Research decision remains HOLD.",
        "",
    ])

    report_path.write_text(
        "\n".join(report),
        encoding="utf-8"
    )

    write_json(manifest_path, {
        "version": VERSION,
        "source_result_sha256": source_hash,
        "sha256": {
            "result.json": sha256(result_path),
            "report.md": sha256(report_path),
        },
    })

    print()
    print("[PASS] Clean V007 execution")
    print("Affected markets:", len(affected))
    print("Unique gaps:", len(all_gaps))
    print("Layer observations:", observed)
    print("Classification:", status)
    print("Research decision: HOLD")
    print("Output:", output)

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(
            "[FAIL]",
            type(exc).__name__,
            str(exc),
            file=sys.stderr,
        )
        sys.exit(1)
