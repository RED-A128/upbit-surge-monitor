
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Event Population Diagnostics
Clean V001

Read-only source analysis:
1. Exact event overlap by timeframe and horizon
2. Unmatched population decomposition
3. Period-label cross-tabulation
4. Original/corrected outcome cross-tabulation
5. HIT flag consistency
6. Source SHA256 verification

No training or trading authorization.
"""

import argparse
import csv
import hashlib
import json
import sqlite3
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path


VERSION = "Clean V001"
AUDIT = "pattern_256_event_population"

GAP_SOURCE = (
    "data/reports/pattern_256_event_gap_integrity/"
    "gap_integrity_events.csv"
)

IND_SOURCE = (
    "data/reports/pattern_256_accumulation_independent/"
    "independent_detail.csv"
)

REQUIRED_GAP = {
    "timeframe", "market", "signal_utc",
    "entry_utc", "horizon_hours", "period",
    "original_outcome", "corrected_outcome",
}

REQUIRED_IND = {
    "timeframe", "market", "signal_timestamp",
    "entry_timestamp", "horizon", "period",
    "status", "hit_30pct",
}


def digest(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1048576), b""):
            h.update(block)
    return h.hexdigest()


def write_json(path, value):
    path.write_text(
        json.dumps(
            value,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        ) + "\n",
        encoding="utf-8",
    )


def write_csv(path, fields, rows):
    with path.open(
        "w", encoding="utf-8", newline=""
    ) as f:
        writer = csv.DictWriter(
            f, fieldnames=fields
        )
        writer.writeheader()
        writer.writerows(rows)


def epoch(value):
    value = str(value).strip()

    if not value:
        raise ValueError("empty timestamp")

    try:
        number = float(value)
        if number > 100000000000:
            number /= 1000
        return round(number, 3)
    except ValueError:
        pass

    dt = datetime.fromisoformat(
        value.replace("Z", "+00:00")
    )

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return round(dt.timestamp(), 3)


def horizon(value):
    number = float(value)

    if not 0 < number <= 100000:
        raise ValueError("invalid horizon")

    return round(number, 6)


def normalize_status(value):
    value = str(value).strip().upper()

    if value in {"HIT", "MISS", "CENSORED"}:
        return value

    return "UNKNOWN"


def normalize_hit(value):
    value = str(value).strip().lower()

    if value in {"true", "1", "yes", "y"}:
        return "HIT"

    if value in {"false", "0", "no", "n"}:
        return "MISS"

    if value in {"", "none", "nan", "null"}:
        return "UNKNOWN"

    return "INVALID"


def create_db(path):
    db = sqlite3.connect(str(path))

    db.executescript("""
        CREATE TABLE gap (
            tf TEXT,
            market TEXT,
            signal REAL,
            entry REAL,
            horizon REAL,
            period TEXT,
            original TEXT,
            corrected TEXT,
            PRIMARY KEY (
                tf, market, signal, entry, horizon
            )
        );

        CREATE TABLE ind (
            tf TEXT,
            market TEXT,
            signal REAL,
            entry REAL,
            horizon REAL,
            period TEXT,
            status TEXT,
            hit TEXT,
            PRIMARY KEY (
                tf, market, signal, entry, horizon
            )
        );

        CREATE INDEX gap_base
        ON gap(tf,market,signal,entry);

        CREATE INDEX ind_base
        ON ind(tf,market,signal,entry);
    """)

    return db


def load_source(db, path, kind):
    is_gap = kind == "gap"

    required = (
        REQUIRED_GAP if is_gap else REQUIRED_IND
    )

    counts = Counter()

    with path.open(
        "r", encoding="utf-8-sig", newline=""
    ) as f:
        reader = csv.DictReader(f)

        missing = required - set(
            reader.fieldnames or []
        )

        if missing:
            raise RuntimeError(
                f"{path.name} missing columns: "
                f"{sorted(missing)}"
            )

        with db:
            for line, row in enumerate(reader, 2):
                counts["rows"] += 1

                tf = row["timeframe"].strip().lower()
                market = row["market"].strip().upper()

                if tf not in {"h1", "h4", "d1"}:
                    raise RuntimeError(
                        f"{path.name}:{line} bad timeframe"
                    )

                if not market.startswith("KRW-"):
                    raise RuntimeError(
                        f"{path.name}:{line} bad market"
                    )

                try:
                    signal = epoch(
                        row[
                            "signal_utc" if is_gap
                            else "signal_timestamp"
                        ]
                    )

                    entry = epoch(
                        row[
                            "entry_utc" if is_gap
                            else "entry_timestamp"
                        ]
                    )

                    hours = horizon(
                        row[
                            "horizon_hours" if is_gap
                            else "horizon"
                        ]
                    )
                except (ValueError, OverflowError) as exc:
                    raise RuntimeError(
                        f"{path.name}:{line} "
                        f"invalid event key: {exc}"
                    ) from exc

                period = row["period"].strip().upper()

                key = (
                    tf, market, signal, entry, hours,
                    period,
                )

                if is_gap:
                    values = key + (
                        normalize_status(
                            row["original_outcome"]
                        ),
                        normalize_status(
                            row["corrected_outcome"]
                        ),
                    )

                    sql = """
                        INSERT INTO gap VALUES (
                            ?,?,?,?,?,?,?,?
                        )
                    """
                else:
                    values = key + (
                        normalize_status(row["status"]),
                        normalize_hit(row["hit_30pct"]),
                    )

                    sql = """
                        INSERT INTO ind VALUES (
                            ?,?,?,?,?,?,?,?
                        )
                    """

                try:
                    db.execute(sql, values)
                except sqlite3.IntegrityError as exc:
                    raise RuntimeError(
                        f"Duplicate event key: "
                        f"{path.name}:{line}"
                    ) from exc

                if counts["rows"] % 50000 == 0:
                    print(
                        f"[{kind}] "
                        f"{counts['rows']:,} rows",
                        flush=True,
                    )

    return dict(counts)


def query_rows(db, sql):
    cursor = db.execute(sql)
    columns = [
        item[0] for item in cursor.description
    ]
    return [
        dict(zip(columns, row))
        for row in cursor
    ]


def analyze(db):
    # Exact matching includes horizon.
    exact = """
        g.tf=i.tf
        AND g.market=i.market
        AND g.signal=i.signal
        AND g.entry=i.entry
        AND g.horizon=i.horizon
    """

    # Base matching intentionally excludes horizon.
    base = """
        g.tf=i.tf
        AND g.market=i.market
        AND g.signal=i.signal
        AND g.entry=i.entry
    """

    population = query_rows(db, f"""
        SELECT
            g.tf AS timeframe,
            g.horizon AS horizon_hours,
            COUNT(*) AS gap_events,
            SUM(
                CASE WHEN EXISTS (
                    SELECT 1 FROM ind i
                    WHERE {exact}
                ) THEN 1 ELSE 0 END
            ) AS exact_matches,
            SUM(
                CASE WHEN NOT EXISTS (
                    SELECT 1 FROM ind i
                    WHERE {base}
                ) THEN 1 ELSE 0 END
            ) AS no_base_identity,
            SUM(
                CASE WHEN EXISTS (
                    SELECT 1 FROM ind i
                    WHERE {base}
                )
                AND NOT EXISTS (
                    SELECT 1 FROM ind i
                    WHERE {exact}
                )
                THEN 1 ELSE 0 END
            ) AS base_only_horizon_mismatch
        FROM gap g
        GROUP BY g.tf, g.horizon
        ORDER BY g.tf, g.horizon
    """)

    reverse = query_rows(db, f"""
        SELECT
            i.tf AS timeframe,
            i.horizon AS horizon_hours,
            COUNT(*) AS independent_events,
            SUM(
                CASE WHEN EXISTS (
                    SELECT 1 FROM gap g
                    WHERE {exact}
                ) THEN 1 ELSE 0 END
            ) AS exact_matches,
            SUM(
                CASE WHEN NOT EXISTS (
                    SELECT 1 FROM gap g
                    WHERE {base}
                ) THEN 1 ELSE 0 END
            ) AS no_base_identity,
            SUM(
                CASE WHEN EXISTS (
                    SELECT 1 FROM gap g
                    WHERE {base}
                )
                AND NOT EXISTS (
                    SELECT 1 FROM gap g
                    WHERE {exact}
                )
                THEN 1 ELSE 0 END
            ) AS base_only_horizon_mismatch
        FROM ind i
        GROUP BY i.tf, i.horizon
        ORDER BY i.tf, i.horizon
    """)

    period = query_rows(db, f"""
        SELECT
            g.tf AS timeframe,
            g.period AS gap_period,
            i.period AS independent_period,
            COUNT(*) AS matches
        FROM gap g
        JOIN ind i ON {exact}
        GROUP BY
            g.tf, g.period, i.period
        ORDER BY
            g.tf, g.period, i.period
    """)

    outcome = query_rows(db, f"""
        SELECT
            g.tf AS timeframe,
            g.horizon AS horizon_hours,
            g.original AS gap_original,
            g.corrected AS gap_corrected,
            i.status AS independent_status,
            i.hit AS independent_hit_flag,
            COUNT(*) AS matches
        FROM gap g
        JOIN ind i ON {exact}
        GROUP BY
            g.tf, g.horizon,
            g.original, g.corrected,
            i.status, i.hit
        ORDER BY
            g.tf, g.horizon,
            g.original, i.status
    """)

    examples = query_rows(db, f"""
        SELECT
            g.tf AS timeframe,
            g.market,
            g.signal AS signal_epoch,
            g.entry AS entry_epoch,
            g.horizon AS horizon_hours,
            g.period AS gap_period,
            i.period AS independent_period,
            g.original AS gap_original,
            g.corrected AS gap_corrected,
            i.status AS independent_status,
            i.hit AS independent_hit_flag
        FROM gap g
        JOIN ind i ON {exact}
        WHERE
            g.original != i.status
            OR g.corrected != i.status
            OR (
                i.status IN ('HIT','MISS')
                AND i.hit IN ('HIT','MISS')
                AND i.status != i.hit
            )
        ORDER BY
            g.tf, g.market, g.signal, g.horizon
        LIMIT 500
    """)

    total = query_rows(db, f"""
        SELECT
            (SELECT COUNT(*) FROM gap)
                AS gap_events,
            (SELECT COUNT(*) FROM ind)
                AS independent_events,
            (
                SELECT COUNT(*)
                FROM gap g
                JOIN ind i ON {exact}
            ) AS exact_matches,
            (
                SELECT COUNT(*)
                FROM gap g
                JOIN ind i ON {exact}
                WHERE g.original != i.status
            ) AS original_differences,
            (
                SELECT COUNT(*)
                FROM gap g
                JOIN ind i ON {exact}
                WHERE g.corrected != i.status
            ) AS corrected_differences,
            (
                SELECT COUNT(*)
                FROM gap g
                JOIN ind i ON {exact}
                WHERE i.status IN ('HIT','MISS')
                  AND i.hit IN ('HIT','MISS')
                  AND i.status != i.hit
            ) AS hit_flag_differences,
            (
                SELECT COUNT(*)
                FROM gap g
                JOIN ind i ON {exact}
                WHERE i.hit IN ('UNKNOWN','INVALID')
            ) AS unknown_hit_flags
    """)[0]

    return {
        "totals": total,
        "gap_population": population,
        "independent_population": reverse,
        "period_cross_tab": period,
        "outcome_cross_tab": outcome,
        "difference_examples": examples,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    root = Path(args.root).resolve()
    output = Path(args.output).resolve()

    if not root.is_dir():
        raise RuntimeError("Source root missing")

    if output == root or root in output.parents:
        raise RuntimeError(
            "Output must be outside source root"
        )

    gap = (root / GAP_SOURCE).resolve()
    independent = (root / IND_SOURCE).resolve()

    for path in (gap, independent):
        if not path.is_file():
            raise RuntimeError(
                f"Missing source: {path}"
            )

        if root not in path.parents:
            raise RuntimeError(
                "Source escaped project root"
            )

    if output.exists():
        raise RuntimeError(
            "Output directory already exists"
        )

    output.mkdir(parents=True)

    before = {
        "gap": digest(gap),
        "independent": digest(independent),
    }

    db_path = output / "population_work.sqlite3"
    db = create_db(db_path)

    try:
        gap_counts = load_source(
            db, gap, "gap"
        )

        independent_counts = load_source(
            db, independent, "independent"
        )

        analysis = analyze(db)
    finally:
        db.close()

    after = {
        "gap": digest(gap),
        "independent": digest(independent),
    }

    if before != after:
        raise RuntimeError(
            "Source files changed during audit"
        )

    result = {
        "audit": AUDIT,
        "version": VERSION,
        "execution": "PASS",
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "source_sha256": before,
        "source_counts": {
            "gap": gap_counts,
            "independent": independent_counts,
        },
        "totals": analysis["totals"],
        "limitations": [
            "Source populations may use different filters",
            "Horizon units are not independently proven",
            "Period labels may describe different split rules",
            "Matching does not prove causal attribution",
            "No independent OHLCV outcome replay",
            "No independent OOS validation",
            "No executable trading simulation",
        ],
        "event_level_attribution_proven": False,
        "independent_oos_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
    }

    files = []

    result_path = output / "population_result.json"
    write_json(result_path, result)
    files.append(result_path)

    definitions = [
        (
            "population_gap.csv",
            [
                "timeframe", "horizon_hours",
                "gap_events", "exact_matches",
                "no_base_identity",
                "base_only_horizon_mismatch",
            ],
            analysis["gap_population"],
        ),
        (
            "population_independent.csv",
            [
                "timeframe", "horizon_hours",
                "independent_events", "exact_matches",
                "no_base_identity",
                "base_only_horizon_mismatch",
            ],
            analysis["independent_population"],
        ),
        (
            "population_periods.csv",
            [
                "timeframe", "gap_period",
                "independent_period", "matches",
            ],
            analysis["period_cross_tab"],
        ),
        (
            "population_outcomes.csv",
            [
                "timeframe", "horizon_hours",
                "gap_original", "gap_corrected",
                "independent_status",
                "independent_hit_flag", "matches",
            ],
            analysis["outcome_cross_tab"],
        ),
        (
            "population_examples.csv",
            [
                "timeframe", "market",
                "signal_epoch", "entry_epoch",
                "horizon_hours",
                "gap_period", "independent_period",
                "gap_original", "gap_corrected",
                "independent_status",
                "independent_hit_flag",
            ],
            analysis["difference_examples"],
        ),
    ]

    for name, columns, rows in definitions:
        path = output / name
        write_csv(path, columns, rows)
        files.append(path)

    manifest = {
        "audit": AUDIT,
        "version": VERSION,
        "execution": "PASS",
        "script_sha256": digest(
            Path(__file__).resolve()
        ),
        "output_sha256": {
            path.name: digest(path)
            for path in files
        },
        "research_decision": "HOLD",
    }

    write_json(
        output / "population_manifest.json",
        manifest,
    )

    print(
        json.dumps(
            result,
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
