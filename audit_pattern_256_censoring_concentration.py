
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Pattern 256 Censoring Concentration Audit — Clean V001.

Read-only: verified censoring sensitivity reports.
No trading approval.
"""
import argparse
import csv
import hashlib
import json
import os
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path

VERSION = "Clean V001"
SOURCE_VERSION = "Clean V002"
INPUT_DIR = "pattern_256_censoring_sensitivity"
OUTPUT_DIR = "pattern_256_censoring_concentration"
INPUT_FILES = (
    "censoring_result.json",
    "censoring_summary.csv",
    "censoring_market.csv",
)
MARKET_FIELDS = (
    "timeframe", "market", "events", "hits", "misses",
    "gap_censored", "end_censored", "censored_total",
    "censoring_rate_pct",
)
COUNT_FIELDS = (
    "events", "hits", "misses", "gap_censored",
    "end_censored", "censored_total",
)
TIMEFRAMES = ("h1", "h4", "d1")
THRESHOLDS = (0, 1, 5, 10, 20)
TOP_K = (10, 20, 50)


def check(condition, message):
    if not condition:
        raise RuntimeError(message)


def digest(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for part in iter(
            lambda: f.read(1024 * 1024), b""
        ):
            h.update(part)
    return h.hexdigest()


def load_json(path):
    with path.open("r", encoding="utf-8-sig") as f:
        return json.load(f)


def load_csv(path):
    with path.open(
        "r", encoding="utf-8-sig", newline=""
    ) as f:
        reader = csv.DictReader(f)
        check(
            reader.fieldnames is not None,
            f"Empty CSV: {path}",
        )
        return list(reader.fieldnames), list(reader)


def atomic_json(path, value):
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(
            value, f, ensure_ascii=False, indent=2,
            sort_keys=True, allow_nan=False,
        )
        f.write("\n")
    os.replace(tmp, path)


def atomic_csv(path, fields, rows):
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open(
        "w", encoding="utf-8-sig", newline=""
    ) as f:
        writer = csv.DictWriter(
            f, fieldnames=fields, extrasaction="raise"
        )
        writer.writeheader()
        for row in rows:
            check(
                set(row) == set(fields),
                f"CSV schema mismatch: {path.name}",
            )
            writer.writerow(row)
    os.replace(tmp, path)


def pct(n, d):
    return round(n * 100 / d, 4) if d else None


def totals(rows):
    return {
        k: sum(r[k] for r in rows)
        for k in COUNT_FIELDS
    }


def rate_info(t):
    n = t["events"]
    observed = t["hits"] + t["misses"]
    censored = t["censored_total"]
    return {
        "events": n,
        "hits": t["hits"],
        "censored": censored,
        "censoring_rate_pct": pct(censored, n),
        "pessimistic_hit_rate_pct": pct(t["hits"], n),
        "observed_hit_rate_pct": pct(
            t["hits"], observed
        ),
        "optimistic_hit_rate_pct": pct(
            t["hits"] + censored, n
        ),
        "uncertainty_width_pp": pct(censored, n),
    }


def verify_source(folder):
    manifest_path = (
        folder / "censoring_manifest.json"
    )
    check(
        manifest_path.is_file(),
        "Source manifest missing",
    )
    manifest = load_json(manifest_path)
    check(
        manifest.get("execution") == "PASS",
        "Source manifest not PASS",
    )
    check(
        manifest.get("version") == SOURCE_VERSION,
        "Source manifest version mismatch",
    )
    hashes = manifest.get("output_sha256")
    check(
        isinstance(hashes, dict)
        and set(hashes) == set(INPUT_FILES),
        "Source file list mismatch",
    )
    for name in INPUT_FILES:
        path = folder / name
        check(
            path.is_file(),
            f"Missing source: {path}",
        )
        check(
            digest(path) == str(hashes[name]).lower(),
            f"Source SHA256 mismatch: {name}",
        )
        print(f"[SOURCE PASS] {name}", flush=True)

    result = load_json(
        folder / "censoring_result.json"
    )
    expected = {
        "version": SOURCE_VERSION,
        "execution": "PASS",
        "audited_event_records": 252600,
        "group_count": 27,
        "market_timeframe_groups": 853,
        "research_decision": "HOLD",
        "independent_oos_proven": False,
    }
    for key, value in expected.items():
        check(
            result.get(key) == value,
            f"Source result mismatch: {key}",
        )
    return result, digest(manifest_path)


def main(root):
    reports = (
        Path(root).resolve() / "data" / "reports"
    )
    source = reports / INPUT_DIR
    result, source_hash = verify_source(source)

    fields, raw = load_csv(
        source / "censoring_market.csv"
    )
    check(
        set(MARKET_FIELDS).issubset(fields),
        "Market CSV missing fields",
    )
    check(
        len(raw) == 853,
        "Market group count mismatch",
    )

    markets = []
    for index, row in enumerate(raw, 1):
        item = {
            "timeframe": row["timeframe"],
            "market": row["market"],
        }
        check(
            item["timeframe"] in TIMEFRAMES
            and item["market"].startswith("KRW-"),
            f"Invalid identity at row {index}",
        )
        for key in COUNT_FIELDS:
            item[key] = int(row[key])
            check(
                item[key] >= 0,
                f"Negative count: {index}/{key}",
            )
        check(
            item["events"]
            == item["hits"]
            + item["misses"]
            + item["censored_total"],
            f"Event accounting: {index}",
        )
        check(
            item["censored_total"]
            == item["gap_censored"]
            + item["end_censored"],
            f"Censor accounting: {index}",
        )
        check(
            abs(
                float(row["censoring_rate_pct"])
                - pct(
                    item["censored_total"],
                    item["events"],
                )
            ) <= 0.00011,
            f"Rate mismatch: {index}",
        )
        check(
            item["events"] > 0,
            f"Empty market: {index}",
        )
        markets.append(item)

    check(
        len({
            (r["timeframe"], r["market"])
            for r in markets
        }) == len(markets),
        "Duplicate market/timeframe",
    )

    overall = totals(markets)
    expected = result["overall"]
    for key in COUNT_FIELDS:
        check(
            overall[key] == expected[key],
            f"Overall mismatch: {key}",
        )

    for tf in TIMEFRAMES:
        subset = totals([
            r for r in markets
            if r["timeframe"] == tf
        ])
        source_tf = next(
            x for x in result["by_timeframe"]
            if x["timeframe"] == tf
        )
        for key in COUNT_FIELDS:
            check(
                subset[key] == source_tf[key],
                f"Timeframe mismatch: {tf}/{key}",
            )

    ranking = []
    concentration = []
    exclusions = []

    for tf in ("ALL",) + TIMEFRAMES:
        subset = [
            r for r in markets
            if tf == "ALL"
            or r["timeframe"] == tf
        ]
        base = totals(subset)

        sorted_rows = sorted(
            subset,
            key=lambda r: (
                -r["censored_total"],
                -r["events"],
                r["market"],
                r["timeframe"],
            ),
        )

        for rank, row in enumerate(sorted_rows, 1):
            ranking.append({
                "scope": tf,
                "rank": rank,
                "timeframe": row["timeframe"],
                "market": row["market"],
                "events": row["events"],
                "censored_total": row["censored_total"],
                "censoring_rate_pct": pct(
                    row["censored_total"],
                    row["events"],
                ),
                "share_of_scope_censoring_pct": pct(
                    row["censored_total"],
                    base["censored_total"],
                ),
            })

        for k in TOP_K:
            top = sorted_rows[:k]
            count = sum(
                r["censored_total"] for r in top
            )
            concentration.append({
                "scope": tf,
                "top_k": k,
                "available_market_groups": len(subset),
                "top_censored": count,
                "scope_censored": base["censored_total"],
                "share_of_censoring_pct": pct(
                    count, base["censored_total"]
                ),
            })

        for threshold in THRESHOLDS:
            # Exclude only markets strictly above threshold.
            kept = [
                r for r in subset
                if pct(
                    r["censored_total"],
                    r["events"],
                ) <= threshold
            ]
            retained = totals(kept)
            item = {
                "scope": tf,
                "max_market_censoring_pct": threshold,
                "market_groups_total": len(subset),
                "market_groups_retained": len(kept),
                "market_groups_excluded": (
                    len(subset) - len(kept)
                ),
                "events_retained_pct": pct(
                    retained["events"],
                    base["events"],
                ),
            }
            item.update(rate_info(retained))
            exclusions.append(item)

    out = reports / OUTPUT_DIR
    out.mkdir(parents=True, exist_ok=True)
    paths = {
        "ranking": out / "concentration_ranking.csv",
        "topk": out / "concentration_topk.csv",
        "exclusions": (
            out / "concentration_exclusions.csv"
        ),
        "result": out / "concentration_result.json",
        "manifest": (
            out / "concentration_manifest.json"
        ),
    }

    # Remove stale PASS metadata, never source files.
    for key in ("result", "manifest"):
        if paths[key].exists():
            paths[key].unlink()

    ranking_fields = (
        "scope", "rank", "timeframe", "market",
        "events", "censored_total",
        "censoring_rate_pct",
        "share_of_scope_censoring_pct",
    )
    topk_fields = (
        "scope", "top_k", "available_market_groups",
        "top_censored", "scope_censored",
        "share_of_censoring_pct",
    )
    exclusions_fields = (
        "scope", "max_market_censoring_pct",
        "market_groups_total",
        "market_groups_retained",
        "market_groups_excluded",
        "events_retained_pct",
        "events", "hits", "censored",
        "censoring_rate_pct",
        "pessimistic_hit_rate_pct",
        "observed_hit_rate_pct",
        "optimistic_hit_rate_pct",
        "uncertainty_width_pp",
    )

    for key, columns, rows in [
        ("ranking", ranking_fields, ranking),
        ("topk", topk_fields, concentration),
        ("exclusions", exclusions_fields, exclusions),
    ]:
        atomic_csv(paths[key], columns, rows)
        written_columns, written_rows = load_csv(
            paths[key]
        )
        check(
            written_columns == list(columns)
            and len(written_rows) == len(rows),
            f"Output CSV verification failed: {key}",
        )

    output = {
        "audit": "pattern_256_censoring_concentration",
        "version": VERSION,
        "execution": "PASS",
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "source_version": SOURCE_VERSION,
        "source_manifest_sha256": source_hash,
        "audited_market_timeframe_groups": len(markets),
        "audited_event_records": overall["events"],
        "overall": rate_info(overall),
        "by_timeframe": {
            tf: rate_info(totals([
                r for r in markets
                if r["timeframe"] == tf
            ]))
            for tf in TIMEFRAMES
        },
        "topk": concentration,
        "threshold_sensitivity": exclusions,
        "ranking_rows": len(ranking),
        "topk_rows": len(concentration),
        "exclusion_rows": len(exclusions),
        "independent_oos_proven": False,
        "point_in_time_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
        "limitations": [
            "Three horizons reuse underlying signals.",
            "Events within markets are dependent.",
            "Market exclusion is descriptive only.",
            "Aggregates cannot locate missing candles.",
            "Independent OOS remains unproven.",
            "No execution or profitability validation.",
        ],
    }

    atomic_json(paths["result"], output)

    hashes = {
        paths[k].name: digest(paths[k])
        for k in (
            "ranking", "topk", "exclusions", "result"
        )
    }
    atomic_json(paths["manifest"], {
        "audit": output["audit"],
        "version": VERSION,
        "execution": "PASS",
        "script_sha256": digest(
            Path(__file__).resolve()
        ),
        "source_manifest_sha256": source_hash,
        "output_sha256": hashes,
    })

    print(json.dumps({
        "execution": "PASS",
        "version": VERSION,
        "events": overall["events"],
        "market_groups": len(markets),
        "overall_censored": overall["censored_total"],
        "top10_all_share_pct": next(
            r["share_of_censoring_pct"]
            for r in concentration
            if r["scope"] == "ALL"
            and r["top_k"] == 10
        ),
        "research_decision": "HOLD",
    }, ensure_ascii=False, indent=2), flush=True)
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
