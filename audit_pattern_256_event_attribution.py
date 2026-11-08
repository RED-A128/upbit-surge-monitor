
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Event Attribution - Horizon-Aware Audit
Clean V002

Purpose:
- Diagnose the matching-key problem in Clean V001.
- Preserve evaluation horizon in every event key.
- Identify duplicate keys and conflicting records.
- Compare outcomes only for unambiguous exact matches.
- Report period naming differences separately.

This script is read-only with respect to source files.
It does not authorize model training or trading.
"""

import argparse
import csv
import hashlib
import json
import re
import sqlite3
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path


VERSION = "Clean V002"
AUDIT = "pattern_256_event_attribution"

GAP_SOURCE = (
    "data/reports/pattern_256_event_gap_integrity/"
    "gap_integrity_events.csv"
)

INDEPENDENT_SOURCE = (
    "data/reports/pattern_256_accumulation_independent/"
    "independent_detail.csv"
)

GAP_COLUMNS = {
    "timeframe", "market", "signal_utc", "entry_utc",
    "horizon_hours", "period", "original_outcome",
    "corrected_outcome", "missing_candles",
}

INDEPENDENT_COLUMNS = {
    "timeframe", "market", "signal_timestamp",
    "entry_timestamp", "horizon", "period",
    "status", "hit_30pct",
}

TIMEFRAMES = ("h1", "h4", "d1")


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1048576), b""):
            digest.update(block)
    return digest.hexdigest()


def save_json(path, data):
    path.write_text(
        json.dumps(
            data, ensure_ascii=False, indent=2,
            sort_keys=True,
        ) + "\n",
        encoding="utf-8",
    )


def timestamp(value):
    value = str(value).strip()

    if re.fullmatch(r"\d{10}(?:\.\d+)?", value):
        return round(float(value), 3)

    if re.fullmatch(r"\d{13}", value):
        return round(int(value) / 1000, 3)

    if not value:
        raise ValueError("empty timestamp")

    try:
        parsed = datetime.fromisoformat(
            value.replace("Z", "+00:00")
        )
    except ValueError as exc:
        raise ValueError(
            f"unsupported timestamp: {value[:60]}"
        ) from exc

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    return round(parsed.timestamp(), 3)


def positive_number(value):
    value = str(value).strip()

    try:
        number = float(value)
    except ValueError as exc:
        raise ValueError(
            f"invalid horizon: {value}"
        ) from exc

    if not 0 < number <= 100000:
        raise ValueError(
            f"horizon out of range: {value}"
        )

    return round(number, 6)


def normalize_status(value):
    value = str(value).strip().upper()

    if value in {"HIT", "MISS"}:
        return value

    if value in {
        "CENSORED", "GAP_CENSORED", "END_CENSORED",
    }:
        return "CENSORED"

    return "UNKNOWN"


def event_identity(row, signal_col, entry_col):
    tf = str(row.get("timeframe", "")).strip().lower()
    market = str(row.get("market", "")).strip().upper()

    if tf not in TIMEFRAMES:
        raise ValueError(f"invalid timeframe: {tf}")

    if not market.startswith("KRW-"):
        raise ValueError(f"invalid market: {market}")

    signal = timestamp(row.get(signal_col, ""))
    entry = timestamp(row.get(entry_col, ""))

    if entry < signal:
        raise ValueError("entry precedes signal")

    return tf, market, signal, entry


def read_csv(path, required):
    handle = path.open(
        "r", encoding="utf-8-sig", newline=""
    )
    reader = csv.DictReader(handle)
    columns = set(reader.fieldnames or [])
    missing = required - columns

    if missing:
        handle.close()
        raise RuntimeError(
            f"{path.name}: missing columns {sorted(missing)}"
        )

    return handle, reader


def prepare_db(path):
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA journal_mode=DELETE")
    conn.execute("PRAGMA temp_store=FILE")

    conn.executescript("""
        CREATE TABLE gap_events (
            tf TEXT NOT NULL,
            market TEXT NOT NULL,
            signal REAL NOT NULL,
            entry REAL NOT NULL,
            horizon REAL NOT NULL,
            period TEXT,
            original TEXT,
            corrected TEXT,
            missing TEXT,
            occurrences INTEGER NOT NULL DEFAULT 1,
            conflicts INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(tf, market, signal, entry, horizon)
        );

        CREATE TABLE independent_events (
            tf TEXT NOT NULL,
            market TEXT NOT NULL,
            signal REAL NOT NULL,
            entry REAL NOT NULL,
            horizon REAL NOT NULL,
            period TEXT,
            status TEXT,
            hit TEXT,
            occurrences INTEGER NOT NULL DEFAULT 1,
            conflicts INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(tf, market, signal, entry, horizon)
        );

        CREATE INDEX idx_gap_base
        ON gap_events(tf, market, signal, entry);

        CREATE INDEX idx_independent_base
        ON independent_events(tf, market, signal, entry);
    """)
    return conn


def ingest(conn, path, kind, issues, limit):
    is_gap = kind == "gap"
    required = (
        GAP_COLUMNS if is_gap else INDEPENDENT_COLUMNS
    )
    table = (
        "gap_events" if is_gap else "independent_events"
    )

    counts = Counter()
    horizons = Counter()
    periods = Counter()

    handle, reader = read_csv(path, required)

    try:
        with conn:
            for line, row in enumerate(reader, 2):
                counts["source_rows"] += 1

                try:
                    tf, market, signal, entry = (
                        event_identity(
                            row,
                            "signal_utc" if is_gap
                            else "signal_timestamp",
                            "entry_utc" if is_gap
                            else "entry_timestamp",
                        )
                    )

                    horizon = positive_number(
                        row.get(
                            "horizon_hours" if is_gap
                            else "horizon", ""
                        )
                    )
                except ValueError as exc:
                    counts["invalid_rows"] += 1
                    if len(issues) < limit:
                        issues.append({
                            "kind": f"{kind}_invalid",
                            "line": line,
                            "detail": str(exc),
                        })
                    continue

                period = str(
                    row.get("period", "")
                ).strip().upper()

                horizons[str(horizon)] += 1
                periods[period] += 1

                if is_gap:
                    values = (
                        period,
                        normalize_status(
                            row.get("original_outcome", "")
                        ),
                        normalize_status(
                            row.get("corrected_outcome", "")
                        ),
                        str(
                            row.get("missing_candles", "")
                        ).strip(),
                    )
                    columns = (
                        "period,original,corrected,missing"
                    )
                else:
                    values = (
                        period,
                        normalize_status(
                            row.get("status", "")
                        ),
                        str(
                            row.get("hit_30pct", "")
                        ).strip(),
                    )
                    columns = "period,status,hit"

                key = (
                    tf, market, signal, entry, horizon
                )

                placeholders = ",".join(
                    "?" for _ in range(5 + len(values))
                )

                statement = (
                    f"INSERT OR IGNORE INTO {table} "
                    f"(tf,market,signal,entry,horizon,"
                    f"{columns}) VALUES ({placeholders})"
                )

                inserted = conn.execute(
                    statement, key + values
                ).rowcount

                if inserted:
                    counts["unique_keys"] += 1
                else:
                    counts["duplicate_rows"] += 1

                    compare_columns = (
                        ["period", "original",
                         "corrected", "missing"]
                        if is_gap else
                        ["period", "status", "hit"]
                    )

                    old = conn.execute(
                        f"SELECT {','.join(compare_columns)} "
                        f"FROM {table} WHERE tf=? AND market=? "
                        f"AND signal=? AND entry=? "
                        f"AND horizon=?",
                        key,
                    ).fetchone()

                    conflict = tuple(values) != tuple(old)

                    conn.execute(
                        f"UPDATE {table} "
                        f"SET occurrences=occurrences+1, "
                        f"conflicts=conflicts+? "
                        f"WHERE tf=? AND market=? "
                        f"AND signal=? AND entry=? "
                        f"AND horizon=?",
                        (int(conflict),) + key,
                    )

                    if conflict:
                        counts["conflicting_rows"] += 1

                if counts["source_rows"] % 50000 == 0:
                    print(
                        f"[{kind}] "
                        f"{counts['source_rows']:,} rows",
                        flush=True,
                    )
    finally:
        handle.close()

    return {
        "counts": dict(counts),
        "horizon_values": dict(
            sorted(horizons.items())
        ),
        "period_values": dict(
            sorted(periods.items())
        ),
    }


def analyze(conn, issues, limit):
    totals = Counter()
    by_tf = {
        tf: Counter() for tf in TIMEFRAMES
    }
    period_pairs = Counter()

    query = """
        SELECT
            g.tf, g.market, g.signal, g.entry,
            g.horizon, g.period, g.original,
            g.corrected, g.occurrences, g.conflicts,
            i.period, i.status, i.hit,
            i.occurrences, i.conflicts
        FROM gap_events g
        JOIN independent_events i
          ON g.tf=i.tf
         AND g.market=i.market
         AND g.signal=i.signal
         AND g.entry=i.entry
         AND g.horizon=i.horizon
    """

    for record in conn.execute(query):
        (
            tf, market, signal, entry, horizon,
            gap_period, original, corrected,
            gap_occurrences, gap_conflicts,
            ind_period, status, hit,
            ind_occurrences, ind_conflicts,
        ) = record

        totals["exact_horizon_matches"] += 1
        by_tf[tf]["exact_horizon_matches"] += 1

        period_pairs[
            (gap_period, ind_period)
        ] += 1

        if gap_period != ind_period:
            totals["different_period_labels"] += 1
            by_tf[tf]["different_period_labels"] += 1

        ambiguous = (
            gap_conflicts > 0 or ind_conflicts > 0
        )

        if ambiguous:
            totals["conflicting_duplicate_matches"] += 1
            by_tf[tf][
                "conflicting_duplicate_matches"
            ] += 1
            continue

        totals["comparable_matches"] += 1
        by_tf[tf]["comparable_matches"] += 1

        if original != status:
            totals["original_status_differences"] += 1
            by_tf[tf][
                "original_status_differences"
            ] += 1

            if len(issues) < limit:
                issues.append({
                    "kind": "original_status_difference",
                    "line": 0,
                    "detail": (
                        f"{tf}/{market} signal={signal} "
                        f"entry={entry} horizon={horizon} "
                        f"gap={original} independent={status}"
                    ),
                })

        if corrected != status:
            totals["corrected_status_differences"] += 1
            by_tf[tf][
                "corrected_status_differences"
            ] += 1

        if status in {"HIT", "MISS"}:
            expected = (
                "1" if status == "HIT" else "0"
            )

            if hit != expected:
                totals["hit_flag_differences"] += 1
                by_tf[tf][
                    "hit_flag_differences"
                ] += 1

    for table, prefix in (
        ("gap_events", "gap"),
        ("independent_events", "independent"),
    ):
        for tf, unique, duplicates, conflicts in (
            conn.execute(
                f"SELECT tf,COUNT(*),"
                f"SUM(CASE WHEN occurrences>1 "
                f"THEN 1 ELSE 0 END),"
                f"SUM(CASE WHEN conflicts>0 "
                f"THEN 1 ELSE 0 END) "
                f"FROM {table} GROUP BY tf"
            )
        ):
            by_tf[tf][f"{prefix}_unique_keys"] = unique
            by_tf[tf][
                f"{prefix}_duplicate_keys"
            ] = duplicates
            by_tf[tf][
                f"{prefix}_conflicting_keys"
            ] = conflicts

            totals[f"{prefix}_unique_keys"] += unique
            totals[f"{prefix}_duplicate_keys"] += duplicates
            totals[f"{prefix}_conflicting_keys"] += conflicts

    totals["unmatched_gap_keys"] = (
        totals["gap_unique_keys"]
        - totals["exact_horizon_matches"]
    )

    totals["unmatched_independent_keys"] = (
        totals["independent_unique_keys"]
        - totals["exact_horizon_matches"]
    )

    for tf in TIMEFRAMES:
        local = by_tf[tf]
        local["unmatched_gap_keys"] = (
            local["gap_unique_keys"]
            - local["exact_horizon_matches"]
        )
        local["unmatched_independent_keys"] = (
            local["independent_unique_keys"]
            - local["exact_horizon_matches"]
        )

    # Diagnostic only: same event identity, regardless of horizon.
    # This is NOT an outcome match.
    base_overlap = conn.execute("""
        SELECT COUNT(*)
        FROM (
            SELECT DISTINCT
                g.tf, g.market, g.signal, g.entry
            FROM gap_events g
            WHERE EXISTS (
                SELECT 1 FROM independent_events i
                WHERE i.tf=g.tf
                  AND i.market=g.market
                  AND i.signal=g.signal
                  AND i.entry=g.entry
            )
        )
    """).fetchone()[0]

    totals["base_identity_overlap"] = base_overlap

    return (
        dict(totals),
        {
            tf: dict(by_tf[tf])
            for tf in TIMEFRAMES
        },
        [
            {
                "gap_period": a,
                "independent_period": b,
                "matches": count,
            }
            for (a, b), count in sorted(
                period_pairs.items()
            )
        ],
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument(
        "--max-issues", type=int, default=100
    )
    args = parser.parse_args()

    if not 1 <= args.max_issues <= 1000:
        raise RuntimeError("Invalid max-issues")

    root = Path(args.root).resolve()
    output = Path(args.output).resolve()

    if not root.is_dir():
        raise RuntimeError("Data root missing")

    if output == root or root in output.parents:
        raise RuntimeError(
            "Output must be outside the data root"
        )

    gap = (root / GAP_SOURCE).resolve()
    independent = (
        root / INDEPENDENT_SOURCE
    ).resolve()

    for path in (gap, independent):
        if not path.is_file():
            raise RuntimeError(
                f"Missing source: {path}"
            )

        if root not in path.parents:
            raise RuntimeError(
                "Source path escaped data root"
            )

    output.mkdir(parents=True, exist_ok=True)

    db_path = output / "attribution_v002.sqlite3"

    if db_path.exists():
        raise RuntimeError(
            "Output database already exists"
        )

    issues = []
    conn = prepare_db(db_path)

    try:
        gap_info = ingest(
            conn, gap, "gap",
            issues, args.max_issues,
        )

        independent_info = ingest(
            conn, independent, "independent",
            issues, args.max_issues,
        )

        totals, by_tf, pairs = analyze(
            conn, issues, args.max_issues
        )
    finally:
        conn.close()

    # This is a diagnostic audit, not proof of attribution.
    result = {
        "audit": AUDIT,
        "version": VERSION,
        "execution": "PASS",
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "sources": {
            "gap": {
                "path": GAP_SOURCE,
                "sha256": sha256(gap),
                **gap_info,
            },
            "independent": {
                "path": INDEPENDENT_SOURCE,
                "sha256": sha256(independent),
                **independent_info,
            },
        },
        "matching_key": [
            "timeframe", "market",
            "signal_timestamp_utc",
            "entry_timestamp_utc",
            "horizon",
        ],
        "horizon_rule": (
            "Numeric exact match only; "
            "no automatic bars-to-hours conversion"
        ),
        "totals": totals,
        "by_timeframe": by_tf,
        "period_label_pairs": pairs,
        "sample_issues": issues,
        "limitations": [
            "Horizon units have not been independently verified",
            "Different period names do not prove errors",
            "Conflicting duplicates remain unresolved",
            "Event matching does not prove causality",
            "No independent OHLCV replay",
            "No independent OOS or profitability proof",
        ],
        "event_level_attribution_proven": False,
        "independent_oos_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
    }

    result_path = output / "attribution_v002_result.json"
    tf_path = output / "attribution_v002_timeframes.csv"
    pairs_path = output / "attribution_v002_period_pairs.csv"
    issues_path = output / "attribution_v002_issues.csv"
    manifest_path = output / "attribution_v002_manifest.json"

    save_json(result_path, result)

    fields = [
        "timeframe",
        "gap_unique_keys",
        "independent_unique_keys",
        "exact_horizon_matches",
        "comparable_matches",
        "unmatched_gap_keys",
        "unmatched_independent_keys",
        "gap_duplicate_keys",
        "independent_duplicate_keys",
        "gap_conflicting_keys",
        "independent_conflicting_keys",
        "conflicting_duplicate_matches",
        "different_period_labels",
        "original_status_differences",
        "corrected_status_differences",
        "hit_flag_differences",
    ]

    with tf_path.open(
        "w", encoding="utf-8", newline=""
    ) as handle:
        writer = csv.DictWriter(
            handle, fieldnames=fields
        )
        writer.writeheader()

        for tf in TIMEFRAMES:
            writer.writerow({
                "timeframe": tf,
                **by_tf[tf],
            })

    with pairs_path.open(
        "w", encoding="utf-8", newline=""
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "gap_period",
                "independent_period",
                "matches",
            ],
        )
        writer.writeheader()
        writer.writerows(pairs)

    with issues_path.open(
        "w", encoding="utf-8", newline=""
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["kind", "line", "detail"],
        )
        writer.writeheader()
        writer.writerows(issues)

    files = [
        result_path, tf_path,
        pairs_path, issues_path,
    ]

    manifest = {
        "audit": AUDIT,
        "version": VERSION,
        "execution": "PASS",
        "script_sha256": sha256(
            Path(__file__).resolve()
        ),
        "output_sha256": {
            path.name: sha256(path)
            for path in files
        },
        "research_decision": "HOLD",
    }

    save_json(manifest_path, manifest)

    print(
        json.dumps(
            {
                "execution": "PASS",
                "totals": totals,
                "research_decision": "HOLD",
            },
            ensure_ascii=False,
            indent=2,
        ),
        flush=True,
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(
            f"[FAIL] {type(exc).__name__}: {exc}",
            file=sys.stderr,
            flush=True,
        )
        sys.exit(1)
