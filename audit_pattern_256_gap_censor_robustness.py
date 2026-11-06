
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Gap-Censor Robustness Audit
Clean V001

Descriptive sensitivity analysis only.
No independent OOS, causality, or trading approval.
"""

import argparse
import csv
import hashlib
import json
import math
import os
import random
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path

VERSION = "Clean V001"
SOURCE_DIR = "pattern_256_gap_censor_association"
OUTPUT_DIR = "pattern_256_gap_censor_robustness"
SCOPES = ("ALL", "h1", "h4", "d1")

INPUT_FILES = (
    "association_market.csv",
    "association_summary.csv",
    "association_rankings.csv",
    "association_result.json",
)

OUTPUT_FILES = (
    "robustness_summary.csv",
    "robustness_strata.csv",
    "robustness_market.csv",
    "robustness_result.json",
)

MARKET_COLUMNS = (
    "timeframe",
    "market",
    "source_events",
    "source_censored",
    "censoring_pct",
    "ohlcv_candles",
    "missing_candle_slots",
    "gap_intervals",
    "missing_slot_pct",
    "max_gap_slots",
    "first_utc",
    "last_utc",
    "gap_present",
    "censor_present",
)


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def sha256(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(
            lambda: f.read(1024 * 1024), b""
        ):
            h.update(chunk)
    return h.hexdigest()


def read_json(path):
    with path.open("r", encoding="utf-8-sig") as f:
        return json.load(f)


def read_csv(path):
    with path.open(
        "r", encoding="utf-8-sig", newline=""
    ) as f:
        reader = csv.DictReader(f)
        require(reader.fieldnames, f"No header: {path}")
        return list(reader.fieldnames), list(reader)


def write_json(path, data):
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(
            data,
            f,
            indent=2,
            ensure_ascii=False,
            sort_keys=True,
            allow_nan=False,
        )
        f.write("\n")
    os.replace(tmp, path)


def write_csv(path, rows, columns):
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open(
        "w", encoding="utf-8-sig", newline=""
    ) as f:
        writer = csv.DictWriter(
            f, fieldnames=columns
        )
        writer.writeheader()
        writer.writerows(rows)
    os.replace(tmp, path)


def ranks(values):
    n = len(values)
    order = sorted(range(n), key=lambda i: values[i])
    result = [0.0] * n
    i = 0

    while i < n:
        j = i + 1
        while (
            j < n
            and values[order[j]] == values[order[i]]
        ):
            j += 1

        rank = (i + 1 + j) / 2.0

        for k in range(i, j):
            result[order[k]] = rank

        i = j

    return result


def pearson(x, y):
    n = len(x)
    if n < 3 or n != len(y):
        return None

    mx = sum(x) / n
    my = sum(y) / n

    vx = sum((a - mx) ** 2 for a in x)
    vy = sum((b - my) ** 2 for b in y)

    if vx <= 0 or vy <= 0:
        return None

    cov = sum(
        (a - mx) * (b - my)
        for a, b in zip(x, y)
    )

    return max(
        -1.0,
        min(1.0, cov / math.sqrt(vx * vy))
    )


def spearman(x, y):
    return pearson(ranks(x), ranks(y))


def residualize(target, controls):
    """
    Ordinary least squares residuals using a
    small ridge term for numerical stability.

    Returns None for insufficient sample size.
    """
    n = len(target)
    p = len(controls) + 1

    if n <= p + 2:
        return None

    columns = [[1.0] * n] + controls

    means = []
    scales = []

    for col in columns:
        mean = sum(col) / n
        variance = sum(
            (v - mean) ** 2 for v in col
        ) / n
        scale = math.sqrt(variance)

        means.append(mean)
        scales.append(scale)

    normalized = [[1.0] * n]

    for j in range(1, p):
        if scales[j] <= 1e-12:
            normalized.append([0.0] * n)
        else:
            normalized.append([
                (v - means[j]) / scales[j]
                for v in columns[j]
            ])

    matrix = [
        [
            sum(
                normalized[i][k] * normalized[j][k]
                for k in range(n)
            )
            for j in range(p)
        ]
        for i in range(p)
    ]

    rhs = [
        sum(
            normalized[i][k] * target[k]
            for k in range(n)
        )
        for i in range(p)
    ]

    # Regularize slopes, not intercept.
    for i in range(1, p):
        matrix[i][i] += 1e-8

    for i in range(p):
        pivot = max(
            range(i, p),
            key=lambda j: abs(matrix[j][i])
        )

        if abs(matrix[pivot][i]) < 1e-12:
            return None

        matrix[i], matrix[pivot] = (
            matrix[pivot], matrix[i]
        )
        rhs[i], rhs[pivot] = rhs[pivot], rhs[i]

        divisor = matrix[i][i]

        for j in range(i, p):
            matrix[i][j] /= divisor

        rhs[i] /= divisor

        for k in range(p):
            if k == i:
                continue

            factor = matrix[k][i]

            for j in range(i, p):
                matrix[k][j] -= (
                    factor * matrix[i][j]
                )

            rhs[k] -= factor * rhs[i]

    predicted = [
        sum(
            rhs[j] * normalized[j][i]
            for j in range(p)
        )
        for i in range(n)
    ]

    return [
        target[i] - predicted[i]
        for i in range(n)
    ]


def partial_spearman(rows):
    if len(rows) < 10:
        return None

    x = ranks([
        r["missing_slot_pct"] for r in rows
    ])
    y = ranks([
        r["censoring_pct"] for r in rows
    ])

    controls = [
        [
            math.log1p(r["ohlcv_candles"])
            for r in rows
        ],
        [
            math.log1p(r["source_events"])
            for r in rows
        ],
    ]

    rx = residualize(x, controls)
    ry = residualize(y, controls)

    if rx is None or ry is None:
        return None

    return pearson(rx, ry)


def permutation_pvalue(rows, iterations, seed):
    """
    Two-sided permutation diagnostic.
    Shuffles market-level censor rates.

    Does not account for dependence between
    related markets or timeframe observations.
    """
    x = [
        r["missing_slot_pct"] for r in rows
    ]
    y = [
        r["censoring_pct"] for r in rows
    ]

    observed = spearman(x, y)

    if observed is None:
        return None

    rng = random.Random(seed)
    ranked_x = ranks(x)
    ranked_y = ranks(y)

    extreme = 0

    for _ in range(iterations):
        shuffled = ranked_y.copy()
        rng.shuffle(shuffled)

        value = pearson(ranked_x, shuffled)

        if (
            value is not None
            and abs(value) >= abs(observed) - 1e-12
        ):
            extreme += 1

    return (extreme + 1) / (iterations + 1)


def select_scope(rows, scope):
    if scope == "ALL":
        return rows

    return [
        r for r in rows
        if r["timeframe"] == scope
    ]


def correlations(rows):
    if len(rows) < 3:
        return None, None

    x = [
        r["missing_slot_pct"] for r in rows
    ]
    y = [
        r["censoring_pct"] for r in rows
    ]

    return pearson(x, y), spearman(x, y)


def round_or_none(value):
    if value is None:
        return None
    return round(value, 6)


def verify_source(folder):
    manifest_path = (
        folder / "association_manifest.json"
    )

    require(
        manifest_path.is_file(),
        f"Missing manifest: {manifest_path}"
    )

    manifest = read_json(manifest_path)

    require(
        manifest.get("audit")
        == "pattern_256_gap_censor_association",
        "Unexpected source audit"
    )
    require(
        manifest.get("version") == "Clean V001"
        and manifest.get("execution") == "PASS",
        "Source audit not PASS"
    )

    hashes = manifest.get("output_sha256", {})

    require(
        set(hashes) == set(INPUT_FILES),
        "Source manifest inventory mismatch"
    )

    for name in INPUT_FILES:
        path = folder / name
        require(path.is_file(), f"Missing: {path}")

        require(
            sha256(path) == str(hashes[name]).lower(),
            f"SHA256 mismatch: {name}"
        )

    result = read_json(
        folder / "association_result.json"
    )

    require(
        result.get("execution") == "PASS"
        and result.get("market_groups") == 853
        and result.get("source_events") == 252600
        and result.get("source_censored") == 19921
        and result.get("missing_candle_slots") == 57412
        and result.get("research_decision") == "HOLD",
        "Source totals or status mismatch"
    )

    fields, raw = read_csv(
        folder / "association_market.csv"
    )

    require(
        fields == list(MARKET_COLUMNS),
        "Unexpected market CSV schema"
    )
    require(
        len(raw) == 853,
        "Unexpected market row count"
    )

    rows = []
    identities = set()

    integer_fields = (
        "source_events",
        "source_censored",
        "ohlcv_candles",
        "missing_candle_slots",
        "gap_intervals",
        "max_gap_slots",
        "gap_present",
        "censor_present",
    )

    float_fields = (
        "censoring_pct",
        "missing_slot_pct",
    )

    for raw_row in raw:
        row = dict(raw_row)

        for name in integer_fields:
            row[name] = int(row[name])
            require(
                row[name] >= 0,
                f"Negative {name}"
            )

        for name in float_fields:
            row[name] = float(row[name])
            require(
                math.isfinite(row[name])
                and 0 <= row[name] <= 100,
                f"Invalid rate: {name}"
            )

        identity = (
            row["timeframe"], row["market"]
        )

        require(
            identity not in identities,
            f"Duplicate market: {identity}"
        )
        identities.add(identity)

        require(
            row["timeframe"] in SCOPES[1:]
            and row["market"].startswith("KRW-"),
            f"Invalid identity: {identity}"
        )

        require(
            row["source_censored"]
            <= row["source_events"],
            f"Invalid censor counts: {identity}"
        )

        require(
            row["gap_present"]
            == int(row["missing_candle_slots"] > 0),
            f"Gap flag mismatch: {identity}"
        )

        require(
            row["censor_present"]
            == int(row["source_censored"] > 0),
            f"Censor flag mismatch: {identity}"
        )

        rows.append(row)

    require(
        sum(r["source_events"] for r in rows)
        == 252600,
        "Event total mismatch"
    )
    require(
        sum(r["source_censored"] for r in rows)
        == 19921,
        "Censor total mismatch"
    )
    require(
        sum(r["missing_candle_slots"] for r in rows)
        == 57412,
        "Gap total mismatch"
    )

    # Verify association summary against source rows.
    _, source_summary = read_csv(
        folder / "association_summary.csv"
    )

    require(
        len(source_summary) == 4,
        "Unexpected source summary count"
    )

    for s in source_summary:
        scope = s["scope"]
        require(scope in SCOPES, "Invalid scope")

        subset = select_scope(rows, scope)
        p, sp = correlations(subset)

        require(
            int(s["market_groups"]) == len(subset),
            f"Scope count mismatch: {scope}"
        )

        require(
            p is not None and sp is not None,
            f"Undefined source correlation: {scope}"
        )

        require(
            abs(p - float(s["pearson_rates"]))
            <= 0.000002
            and abs(
                sp - float(s["spearman_rates"])
            ) <= 0.000002,
            f"Correlation mismatch: {scope}"
        )

    return rows, sha256(manifest_path)


def trimmed_rows(rows, fraction):
    """
    Remove highest gap-count groups.
    Stable ordering makes runs reproducible.
    """
    n = len(rows)
    remove_count = math.ceil(n * fraction)

    ranked = sorted(
        rows,
        key=lambda r: (
            -r["missing_candle_slots"],
            r["timeframe"],
            r["market"],
        )
    )

    return ranked[remove_count:]


def make_summary(rows, iterations):
    summary = []
    strata = []

    for scope_index, scope in enumerate(SCOPES):
        subset = select_scope(rows, scope)

        p, sp = correlations(subset)

        summary.append({
            "scope": scope,
            "scenario": "BASELINE",
            "market_groups": len(subset),
            "removed_groups": 0,
            "pearson_rates": round_or_none(p),
            "spearman_rates": round_or_none(sp),
            "partial_spearman": round_or_none(
                partial_spearman(subset)
            ),
            "permutation_pvalue": round_or_none(
                permutation_pvalue(
                    subset,
                    iterations,
                    256000 + scope_index,
                )
            ),
            "iterations": iterations,
        })

        for fraction in (0.01, 0.05, 0.10):
            retained = trimmed_rows(
                subset, fraction
            )

            p, sp = correlations(retained)

            summary.append({
                "scope": scope,
                "scenario": (
                    f"EXCLUDE_TOP_{int(fraction * 100)}"
                    "_PCT_GAP_COUNT"
                ),
                "market_groups": len(retained),
                "removed_groups": (
                    len(subset) - len(retained)
                ),
                "pearson_rates": round_or_none(p),
                "spearman_rates": round_or_none(sp),
                "partial_spearman": round_or_none(
                    partial_spearman(retained)
                ),
                "permutation_pvalue": None,
                "iterations": 0,
            })

        # Quartiles by historical OHLCV candle count.
        ordered = sorted(
            subset,
            key=lambda r: (
                r["ohlcv_candles"],
                r["timeframe"],
                r["market"],
            )
        )

        n = len(ordered)

        for quartile in range(4):
            start = n * quartile // 4
            end = n * (quartile + 1) // 4

            group = ordered[start:end]
            p, sp = correlations(group)

            strata.append({
                "scope": scope,
                "coverage_quartile": quartile + 1,
                "market_groups": len(group),
                "min_candles": min(
                    r["ohlcv_candles"]
                    for r in group
                ),
                "max_candles": max(
                    r["ohlcv_candles"]
                    for r in group
                ),
                "source_events": sum(
                    r["source_events"]
                    for r in group
                ),
                "censored_events": sum(
                    r["source_censored"]
                    for r in group
                ),
                "missing_slots": sum(
                    r["missing_candle_slots"]
                    for r in group
                ),
                "pearson_rates": round_or_none(p),
                "spearman_rates": round_or_none(sp),
            })

    return summary, strata


def main(root, iterations):
    require(
        100 <= iterations <= 100000,
        "Iterations must be 100..100000"
    )

    root = Path(root).resolve()

    reports = root / "data" / "reports"
    source = reports / SOURCE_DIR

    rows, source_hash = verify_source(source)

    summary, strata = make_summary(
        rows, iterations
    )

    output = reports / OUTPUT_DIR
    output.mkdir(parents=True, exist_ok=True)

    # Invalidate previous successful metadata first.
    for name in (
        *OUTPUT_FILES,
        "robustness_manifest.json",
    ):
        path = output / name
        if path.exists():
            path.unlink()

    market_output = []

    for row in rows:
        market_output.append({
            "timeframe": row["timeframe"],
            "market": row["market"],
            "source_events": row["source_events"],
            "source_censored": row["source_censored"],
            "censoring_pct": row["censoring_pct"],
            "ohlcv_candles": row["ohlcv_candles"],
            "missing_candle_slots":
                row["missing_candle_slots"],
            "missing_slot_pct":
                row["missing_slot_pct"],
            "gap_present": row["gap_present"],
            "censor_present": row["censor_present"],
        })

    write_csv(
        output / OUTPUT_FILES[0],
        summary,
        list(summary[0]),
    )
    write_csv(
        output / OUTPUT_FILES[1],
        strata,
        list(strata[0]),
    )
    write_csv(
        output / OUTPUT_FILES[2],
        market_output,
        list(market_output[0]),
    )

    result = {
        "audit": "pattern_256_gap_censor_robustness",
        "version": VERSION,
        "execution": "PASS",
        "created_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "source_manifest_sha256": source_hash,
        "market_groups": len(rows),
        "source_events": sum(
            r["source_events"] for r in rows
        ),
        "source_censored": sum(
            r["source_censored"] for r in rows
        ),
        "missing_candle_slots": sum(
            r["missing_candle_slots"]
            for r in rows
        ),
        "permutation_iterations": iterations,
        "scenario_rows": len(summary),
        "coverage_strata_rows": len(strata),
        "scenarios": summary,
        "coverage_strata": strata,
        "causal_relationship_proven": False,
        "independent_oos_proven": False,
        "point_in_time_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
        "limitations": [
            "Market-level correlations are not causal.",
            "Partial correlation controls only for "
            "specified market-level covariates.",
            "Permutation p-values assume exchangeable "
            "market-level observations.",
            "ALL scope mixes timeframes and may "
            "double-count related markets.",
            "Timeframes and markets are not "
            "guaranteed independent.",
            "Quartile boundaries are sample-based.",
            "Extreme-group exclusion is a "
            "sensitivity diagnostic, not a "
            "model-selection rule.",
            "No event-level gap attribution.",
            "No independent OOS validation.",
            "No profitability or execution test.",
        ],
    }

    write_json(
        output / OUTPUT_FILES[3],
        result,
    )

    manifest = {
        "audit": result["audit"],
        "version": VERSION,
        "execution": "PASS",
        "script_sha256": sha256(
            Path(__file__).resolve()
        ),
        "source_manifest_sha256": source_hash,
        "output_sha256": {
            name: sha256(output / name)
            for name in OUTPUT_FILES
        },
    }

    write_json(
        output / "robustness_manifest.json",
        manifest,
    )

    print(
        json.dumps({
            "execution": "PASS",
            "version": VERSION,
            "market_groups": len(rows),
            "scenario_rows": len(summary),
            "coverage_strata_rows": len(strata),
            "research_decision": "HOLD",
            "scenarios": summary,
        }, indent=2, ensure_ascii=False),
        flush=True,
    )

    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Pattern 256 gap-censor robustness audit"
        )
    )
    parser.add_argument(
        "--root", default="."
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=2000,
    )

    args = parser.parse_args()

    try:
        sys.exit(
            main(args.root, args.iterations)
        )
    except KeyboardInterrupt:
        print(
            "[STOP] Interrupted",
            file=sys.stderr,
        )
        sys.exit(130)
    except Exception:
        traceback.print_exc()
        sys.exit(1)
