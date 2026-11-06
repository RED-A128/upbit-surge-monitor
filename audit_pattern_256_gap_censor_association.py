
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Pattern 256 Gap-Censor Association Audit - Clean V001.

Read-only descriptive market-level association analysis.
Not an event-level causal audit or independent OOS test.
"""
import argparse
import csv
import hashlib
import json
import math
import os
import sys
import traceback
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

VERSION = "Clean V001"
INPUT_DIR = "pattern_256_ohlcv_gap_integrity"
OUTPUT_DIR = "pattern_256_gap_censor_association"
SOURCE_FILES = (
    "gap_integrity_market.csv",
    "gap_integrity_samples.csv",
    "gap_integrity_summary.csv",
    "gap_integrity_result.json",
)
MARKET_COLUMNS = (
    "timeframe", "market", "source_events",
    "source_censored", "status",
    "timestamp_column", "rows",
    "unique_timestamps", "duplicates",
    "nonmonotonic_rows", "off_grid_intervals",
    "gap_intervals", "missing_candle_slots",
    "max_gap_slots", "first_utc", "last_utc",
    "error",
)
SCOPES = ("ALL", "h1", "h4", "d1")


def check(condition, message):
    if not condition:
        raise RuntimeError(message)


def digest(path):
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(
            lambda: stream.read(1024 * 1024), b""
        ):
            h.update(chunk)
    return h.hexdigest()


def load_json(path):
    with path.open("r", encoding="utf-8-sig") as f:
        return json.load(f)


def load_csv(path):
    with path.open(
        "r", encoding="utf-8-sig", newline=""
    ) as f:
        reader = csv.DictReader(f)
        check(reader.fieldnames, f"No CSV header: {path}")
        return reader.fieldnames, list(reader)


def save_json(path, obj):
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(
            obj, f, indent=2, sort_keys=True,
            ensure_ascii=False, allow_nan=False
        )
        f.write("\n")
    os.replace(tmp, path)


def save_csv(path, columns, rows):
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open(
        "w", encoding="utf-8-sig", newline=""
    ) as f:
        writer = csv.DictWriter(
            f, fieldnames=columns,
            extrasaction="raise"
        )
        writer.writeheader()
        writer.writerows(rows)
    os.replace(tmp, path)


def pct(a, b):
    return round(100.0 * a / b, 6) if b else 0.0


def pearson(xs, ys):
    n = len(xs)
    if n < 3:
        return None
    mx = sum(xs) / n
    my = sum(ys) / n
    xx = sum((x - mx) ** 2 for x in xs)
    yy = sum((y - my) ** 2 for y in ys)
    if xx <= 0 or yy <= 0:
        return None
    xy = sum(
        (x - mx) * (y - my)
        for x, y in zip(xs, ys)
    )
    value = xy / math.sqrt(xx * yy)
    return round(max(-1.0, min(1.0, value)), 6)


def average_ranks(values):
    order = sorted(
        range(len(values)),
        key=lambda i: values[i]
    )
    ranks = [0.0] * len(values)
    i = 0
    while i < len(order):
        j = i + 1
        while (
            j < len(order)
            and values[order[j]] == values[order[i]]
        ):
            j += 1
        rank = (i + 1 + j) / 2.0
        for k in range(i, j):
            ranks[order[k]] = rank
        i = j
    return ranks


def spearman(xs, ys):
    return pearson(
        average_ranks(xs),
        average_ranks(ys)
    )


def verify_source(folder):
    manifest_path = (
        folder / "gap_integrity_manifest.json"
    )
    check(
        manifest_path.is_file(),
        f"Missing manifest: {manifest_path}"
    )
    manifest = load_json(manifest_path)

    check(
        manifest.get("audit")
        == "pattern_256_ohlcv_gap_integrity"
        and manifest.get("version") == "Clean V001"
        and manifest.get("execution") == "PASS",
        "Source manifest is not approved for analysis"
    )

    hashes = manifest.get("output_sha256", {})
    check(
        set(hashes) == set(SOURCE_FILES),
        "Source manifest file list mismatch"
    )

    for name in SOURCE_FILES:
        path = folder / name
        check(path.is_file(), f"Missing input: {path}")
        check(
            digest(path) == str(hashes[name]).lower(),
            f"SHA256 mismatch: {name}"
        )

    result = load_json(
        folder / "gap_integrity_result.json"
    )
    check(
        result.get("execution") == "PASS"
        and result.get("version") == "Clean V001"
        and result.get("source_market_groups") == 853
        and result.get("source_event_records") == 252600
        and result.get("source_censored_events") == 19921
        and result.get("research_decision") == "HOLD",
        "Source result reconciliation failed"
    )

    fields, raw = load_csv(
        folder / "gap_integrity_market.csv"
    )
    check(
        fields == list(MARKET_COLUMNS),
        "Market CSV schema mismatch"
    )
    check(len(raw) == 853, "Market row count mismatch")

    rows = []
    seen = set()
    for row in raw:
        tf = row["timeframe"]
        market = row["market"]
        key = (tf, market)

        check(
            tf in SCOPES[1:]
            and market.startswith("KRW-")
            and key not in seen,
            f"Invalid or duplicate identity: {key}"
        )
        seen.add(key)

        check(
            row["status"] in ("OK", "ANOMALY"),
            f"Unusable market: {key}"
        )

        numeric = (
            "source_events", "source_censored",
            "rows", "unique_timestamps",
            "duplicates", "nonmonotonic_rows",
            "off_grid_intervals", "gap_intervals",
            "missing_candle_slots", "max_gap_slots",
        )
        obj = dict(row)
        for field in numeric:
            obj[field] = int(row[field])
            check(
                obj[field] >= 0,
                f"Negative {field}: {key}"
            )

        check(
            obj["source_censored"]
            <= obj["source_events"],
            f"Censor count exceeds events: {key}"
        )
        check(
            obj["rows"]
            == obj["unique_timestamps"]
            + obj["duplicates"],
            f"OHLCV row reconciliation failed: {key}"
        )
        rows.append(obj)

    check(
        sum(r["source_events"] for r in rows)
        == 252600,
        "Event count mismatch"
    )
    check(
        sum(r["source_censored"] for r in rows)
        == 19921,
        "Censored count mismatch"
    )
    check(
        sum(r["missing_candle_slots"] for r in rows)
        == 57412,
        "Missing slot count mismatch"
    )

    return rows, digest(manifest_path)


def build_analysis(rows):
    market_rows = []

    for r in rows:
        events = r["source_events"]
        censored = r["source_censored"]
        slots = r["missing_candle_slots"]
        candles = r["unique_timestamps"]
        total_slots = candles + slots

        market_rows.append({
            "timeframe": r["timeframe"],
            "market": r["market"],
            "source_events": events,
            "source_censored": censored,
            "censoring_pct": pct(censored, events),
            "ohlcv_candles": candles,
            "missing_candle_slots": slots,
            "gap_intervals": r["gap_intervals"],
            "missing_slot_pct": pct(
                slots, total_slots
            ),
            "max_gap_slots": r["max_gap_slots"],
            "first_utc": r["first_utc"],
            "last_utc": r["last_utc"],
            "gap_present": int(slots > 0),
            "censor_present": int(censored > 0),
        })

    association_rows = []
    exceptions = []

    for scope in SCOPES:
        subset = [
            r for r in market_rows
            if scope == "ALL"
            or r["timeframe"] == scope
        ]
        xs = [
            r["missing_slot_pct"] for r in subset
        ]
        ys = [
            r["censoring_pct"] for r in subset
        ]
        slot_counts = [
            r["missing_candle_slots"]
            for r in subset
        ]
        censor_counts = [
            r["source_censored"] for r in subset
        ]

        association_rows.append({
            "scope": scope,
            "market_groups": len(subset),
            "source_events": sum(
                r["source_events"] for r in subset
            ),
            "censored_events": sum(censor_counts),
            "missing_candle_slots": sum(slot_counts),
            "gap_market_groups": sum(
                r["gap_present"] for r in subset
            ),
            "censor_market_groups": sum(
                r["censor_present"] for r in subset
            ),
            "pearson_rates": pearson(xs, ys),
            "spearman_rates": spearman(xs, ys),
            "pearson_counts": pearson(
                slot_counts, censor_counts
            ),
            "spearman_counts": spearman(
                slot_counts, censor_counts
            ),
        })

        # Descriptive ranking only; no threshold
        # selection for model training.
        ranked = sorted(
            subset,
            key=lambda r: (
                -r["source_censored"],
                r["missing_candle_slots"],
                r["market"],
            )
        )
        for rank, r in enumerate(ranked[:20], 1):
            exceptions.append({
                "scope": scope,
                "category": "HIGH_CENSOR_COUNT",
                "rank": rank,
                "timeframe": r["timeframe"],
                "market": r["market"],
                "source_censored": r["source_censored"],
                "censoring_pct": r["censoring_pct"],
                "missing_candle_slots":
                    r["missing_candle_slots"],
                "missing_slot_pct":
                    r["missing_slot_pct"],
            })

        ranked = sorted(
            subset,
            key=lambda r: (
                -r["missing_candle_slots"],
                r["source_censored"],
                r["market"],
            )
        )
        for rank, r in enumerate(ranked[:20], 1):
            exceptions.append({
                "scope": scope,
                "category": "HIGH_GAP_COUNT",
                "rank": rank,
                "timeframe": r["timeframe"],
                "market": r["market"],
                "source_censored": r["source_censored"],
                "censoring_pct": r["censoring_pct"],
                "missing_candle_slots":
                    r["missing_candle_slots"],
                "missing_slot_pct":
                    r["missing_slot_pct"],
            })

    return market_rows, association_rows, exceptions


def main(root):
    root = Path(root).resolve()
    base = root / "data" / "reports"
    source = base / INPUT_DIR

    source_rows, source_hash = verify_source(source)
    markets, associations, exceptions = (
        build_analysis(source_rows)
    )

    out = base / OUTPUT_DIR
    out.mkdir(parents=True, exist_ok=True)

    names = (
        "association_market.csv",
        "association_summary.csv",
        "association_rankings.csv",
        "association_result.json",
    )
    manifest_path = out / "association_manifest.json"

    # Prevent previous success metadata being reused.
    for name in (*names, manifest_path.name):
        path = out / name
        if path.exists():
            path.unlink()

    save_csv(
        out / names[0],
        list(markets[0]), markets
    )
    save_csv(
        out / names[1],
        list(associations[0]), associations
    )
    save_csv(
        out / names[2],
        list(exceptions[0]), exceptions
    )

    result = {
        "audit": "pattern_256_gap_censor_association",
        "version": VERSION,
        "execution": "PASS",
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "source_manifest_sha256": source_hash,
        "market_groups": len(markets),
        "source_events": sum(
            r["source_events"] for r in markets
        ),
        "source_censored": sum(
            r["source_censored"] for r in markets
        ),
        "missing_candle_slots": sum(
            r["missing_candle_slots"] for r in markets
        ),
        "associations": associations,
        "ranking_rows": len(exceptions),
        "independent_oos_proven": False,
        "point_in_time_proven": False,
        "causal_relationship_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
        "limitations": [
            "Market-level association does not prove "
            "event-level causality.",
            "Rate correlations are descriptive.",
            "Markets and signals are not independent.",
            "Historical OHLCV coverage varies.",
            "Missing candle slots may include "
            "no-trade periods.",
            "No future-return prediction is tested.",
            "No independent OOS validation.",
            "No profitability or fill validation.",
        ],
    }
    save_json(out / names[3], result)

    manifest = {
        "audit": result["audit"],
        "version": VERSION,
        "execution": "PASS",
        "script_sha256": digest(
            Path(__file__).resolve()
        ),
        "source_manifest_sha256": source_hash,
        "output_sha256": {
            name: digest(out / name)
            for name in names
        },
    }
    save_json(manifest_path, manifest)

    print(json.dumps({
        "execution": "PASS",
        "version": VERSION,
        "market_groups": len(markets),
        "source_events": result["source_events"],
        "source_censored": result["source_censored"],
        "missing_slots": result["missing_candle_slots"],
        "associations": associations,
        "research_decision": "HOLD",
    }, indent=2), flush=True)

    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=".")
    args = parser.parse_args()
    try:
        sys.exit(main(args.root))
    except KeyboardInterrupt:
        print("[STOP] Interrupted", file=sys.stderr)
        sys.exit(130)
    except Exception:
        traceback.print_exc()
        sys.exit(1)
