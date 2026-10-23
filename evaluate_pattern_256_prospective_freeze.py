
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Clean V003-16
Prospective independent validation protocol freeze.

Research only.
No trading, scanner or performance claims.
"""

import argparse
import csv
import hashlib
import io
import json
import os
import sys

from collections import Counter
from datetime import datetime
from pathlib import Path


VERSION = "Clean V003-16"
SOURCE_VERSION = "Clean V003-15"

SOURCE_REL = Path(
    "data/reports/pattern_256_validation_priority"
)

OUTPUT_REL = Path(
    "data/reports/pattern_256_prospective_freeze"
)

SOURCE_FILES = (
    "validation_priority_markets.csv",
    "validation_priority_summary.csv",
    "validation_priority_result.json",
)

OUTPUT_FILES = (
    "frozen_candidates.csv",
    "prospective_protocol.json",
    "freeze_result.json",
)

FREEZE_CUTOFF = "2026-10-24T00:00:00+00:00"

TIERS = {
    "P1_STRICT_REPLICATION": 1,
    "P2_SAMPLE_EXTENSION": 7,
    "P3_EFFECT_RECHECK": 14,
    "P4_SMALL_SAMPLE": 4,
    "P5_DIRECTION_CONFLICT": 31,
    "P6_NO_REPLICATED_EFFECT": 152,
}

FIELDS = (
    "market",
    "frozen_priority_tier",
    "frozen_minimum_observations",
    "frozen_2025_effect_pp",
    "frozen_2026_effect_pp",
    "frozen_minimum_effect_pp",
    "frozen_strict_candidate",
    "eligible_for_primary_replication",
    "approved_for_live_trading",
)


def digest(path):
    h = hashlib.sha256()

    with path.open("rb") as stream:
        for block in iter(
            lambda: stream.read(1024 * 1024),
            b"",
        ):
            h.update(block)

    return h.hexdigest()


def read_json(path):
    return json.loads(
        path.read_text(encoding="utf-8")
    )


def read_csv(path):
    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as stream:
        return list(csv.DictReader(stream))


def write_atomic(path, content):
    temp = path.with_name(path.name + ".tmp")

    try:
        with temp.open(
            "w",
            encoding="utf-8",
            newline="",
        ) as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())

        os.replace(temp, path)

    finally:
        if temp.exists():
            temp.unlink()


def write_json(path, value):
    write_atomic(
        path,
        json.dumps(
            value,
            indent=2,
            sort_keys=True,
            ensure_ascii=False,
            allow_nan=False,
        ) + "\n",
    )


def write_csv(path, rows):
    buffer = io.StringIO(newline="")

    writer = csv.DictWriter(
        buffer,
        fieldnames=FIELDS,
        lineterminator="\n",
    )

    writer.writeheader()
    writer.writerows(rows)

    write_atomic(path, buffer.getvalue())


def verify_inputs(source):
    manifest_path = (
        source / "validation_priority_manifest.json"
    )

    if not manifest_path.is_file():
        raise FileNotFoundError(manifest_path)

    manifest = read_json(manifest_path)

    result = read_json(
        source / "validation_priority_result.json"
    )

    if (
        manifest.get("version") != SOURCE_VERSION
        or result.get("version") != SOURCE_VERSION
    ):
        raise ValueError("Source version mismatch")

    if (
        result.get("execution_status") != "PASS"
        or result.get("overall_decision") != "HOLD"
    ):
        raise ValueError("Source status mismatch")

    for key in (
        "independent_oos_proven",
        "statistical_independence_proven",
        "profitability_proven",
        "approved_for_live_trading",
    ):
        if result.get(key) is not False:
            raise ValueError(
                "Unsafe source flag: " + key
            )

    if (
        result.get("market_count") != 209
        or result.get("original_candidate_count") != 22
    ):
        raise ValueError("Source count mismatch")

    if result.get("strict_candidate_markets") != [
        "KRW-DKA"
    ]:
        raise ValueError(
            "Source strict candidate mismatch"
        )

    if result.get("priority_counts") != TIERS:
        raise ValueError(
            "Source priority counts mismatch"
        )

    hashes = manifest.get("output_sha256")

    if (
        not isinstance(hashes, dict)
        or set(hashes) != set(SOURCE_FILES)
    ):
        raise ValueError(
            "Source hash manifest invalid"
        )

    for name in SOURCE_FILES:
        if digest(source / name) != hashes[name]:
            raise ValueError(
                "Source hash mismatch: " + name
            )

    rows = read_csv(
        source / "validation_priority_markets.csv"
    )

    summary = read_csv(
        source / "validation_priority_summary.csv"
    )

    if len(rows) != 209 or len(summary) != 6:
        raise ValueError(
            "Source row counts invalid"
        )

    if len({
        r["market"] for r in rows
    }) != 209:
        raise ValueError("Duplicate market")

    if Counter(
        r["priority_tier"] for r in rows
    ) != Counter(TIERS):
        raise ValueError(
            "Priority tier count mismatch"
        )

    for r in rows:
        if (
            not r["market"].startswith("KRW-")
            or r["priority_tier"] not in TIERS
        ):
            raise ValueError(
                "Invalid market or tier"
            )

        if (
            r["approved_for_live_trading"] != "False"
            or r["independent_oos_proven"] != "False"
        ):
            raise ValueError(
                "Unsafe market flags"
            )

        if (
            r["requires_new_independent_data"]
            != "True"
        ):
            raise ValueError(
                "Missing independent validation requirement"
            )

    if [
        r["market"]
        for r in rows
        if r["passes_strict_200_05"] == "True"
    ] != ["KRW-DKA"]:
        raise ValueError(
            "Strict candidate mismatch"
        )

    if sum(
        r["original_candidate"] == "True"
        for r in rows
    ) != 22:
        raise ValueError(
            "Original candidate count mismatch"
        )

    return rows, hashes


def verify_completed(output, hashes):
    try:
        manifest = read_json(
            output / "freeze_manifest.json"
        )

        result = read_json(
            output / "freeze_result.json"
        )

        if (
            manifest.get("version") != VERSION
            or manifest.get("input_sha256") != hashes
        ):
            return False

        if (
            result.get("version") != VERSION
            or result.get("execution_status") != "PASS"
        ):
            return False

        if (
            result.get("overall_decision") != "HOLD"
            or result.get(
                "approved_for_live_trading"
            ) is not False
        ):
            return False

        if (
            result.get("freeze_cutoff_utc")
            != FREEZE_CUTOFF
        ):
            return False

        output_hashes = manifest.get(
            "output_sha256", {}
        )

        if set(output_hashes) != set(OUTPUT_FILES):
            return False

        for name in OUTPUT_FILES:
            if digest(output / name) != output_hashes[name]:
                return False

        protocol = read_json(
            output / "prospective_protocol.json"
        )

        if (
            protocol.get("freeze_cutoff_utc")
            != FREEZE_CUTOFF
        ):
            return False

        rows = read_csv(
            output / "frozen_candidates.csv"
        )

        return (
            len(rows) == 209
            and len({
                r["market"] for r in rows
            }) == 209
            and Counter(
                r["frozen_priority_tier"]
                for r in rows
            ) == Counter(TIERS)
            and [
                r["market"]
                for r in rows
                if r["frozen_strict_candidate"]
                == "True"
            ] == ["KRW-DKA"]
        )

    except (
        OSError,
        ValueError,
        KeyError,
        TypeError,
        json.JSONDecodeError,
    ):
        return False


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--root",
        default=".",
    )

    args = parser.parse_args()

    root = Path(args.root).resolve()
    source = (root / SOURCE_REL).resolve()
    output = (root / OUTPUT_REL).resolve()

    if (
        source == output
        or source in output.parents
        or output in source.parents
    ):
        raise ValueError(
            "Unsafe directory relationship"
        )

    rows, hashes = verify_inputs(source)

    if verify_completed(output, hashes):
        print(
            "[SKIP] Verified frozen protocol",
            flush=True,
        )
        print(
            "[DECISION] HOLD",
            flush=True,
        )
        return 0

    # Never silently rewrite a previously frozen
    # protocol, including an incomplete snapshot.
    if output.exists() and any(output.iterdir()):
        raise ValueError(
            "Freeze directory already contains files; "
            "manual audit required, no overwrite"
        )

    cutoff = datetime.fromisoformat(
        FREEZE_CUTOFF
    )

    if (
        cutoff.tzinfo is None
        or cutoff.utcoffset().total_seconds() != 0
    ):
        raise ValueError("Invalid UTC cutoff")

    frozen = []

    for r in sorted(
        rows,
        key=lambda x: x["market"],
    ):
        frozen.append({
            "market": r["market"],
            "frozen_priority_tier": (
                r["priority_tier"]
            ),
            "frozen_minimum_observations": (
                r["minimum_observations"]
            ),
            "frozen_2025_effect_pp": (
                r["year_2025_difference_pp"]
            ),
            "frozen_2026_effect_pp": (
                r["year_2026_difference_pp"]
            ),
            "frozen_minimum_effect_pp": (
                r["minimum_yearly_difference_pp"]
            ),
            "frozen_strict_candidate": (
                r["passes_strict_200_05"]
            ),
            "eligible_for_primary_replication": str(
                r["priority_tier"]
                == "P1_STRICT_REPLICATION"
            ),
            "approved_for_live_trading": "False",
        })

    secondary = [
        r["market"]
        for r in frozen
        if r["frozen_priority_tier"] in (
            "P2_SAMPLE_EXTENSION",
            "P3_EFFECT_RECHECK",
        )
    ]

    protocol = {
        "version": VERSION,
        "purpose": (
            "Pre-register prospective independent "
            "event validation; research only"
        ),
        "freeze_cutoff_utc": FREEZE_CUTOFF,
        "primary_market": "KRW-DKA",
        "primary_market_selection": (
            "Historical P1 strict candidate, "
            "selected before prospective data"
        ),
        "secondary_markets": secondary,
        "historical_market_count": 209,
        "historical_candidate_count": 22,
        "frozen_historical_thresholds": {
            "minimum_yearly_observations": 200,
            "minimum_each_year_effect_percentage_points_exclusive": 0.5,
            "minimum_30pct_price_gain": 0.30,
        },
        "prospective_eligibility": {
            "signal_timestamp_rule": (
                "UTC signal_timestamp >= freeze_cutoff_utc"
            ),
            "source_rule": (
                "Newly collected independent events only; "
                "historical aggregate files forbidden as outcomes"
            ),
            "no_overlap_rule": (
                "Deduplicate signal events and assess "
                "correlated/overlapping holding periods "
                "before inference"
            ),
            "maturity_rule": (
                "Count only complete forward outcome "
                "windows; censor incomplete windows"
            ),
            "selection_rule": (
                "Do not change candidate list, thresholds "
                "or hypotheses based on prospective outcomes"
            ),
            "comparison_rule": (
                "Use the same independently verified event "
                "and time horizon for 256-only and "
                "256-plus-accumulation"
            ),
            "leakage_rule": (
                "Only information available at signal time "
                "may be used to construct signals"
            ),
            "audit_rule": (
                "Record source SHA256, exact signal definitions, "
                "market universe, observation windows and costs"
            ),
        },
        "future_study_design_required": [
            "Specify event-level comparison and dependence adjustment",
            "Specify target estimand, sample-size/power and multiple-testing correction",
            "Specify fees, slippage, liquidity and net performance",
            "Validate genuinely new observations after cutoff",
        ],
        "prospective_events_collected": 0,
        "independent_oos_proven": False,
        "approved_for_live_trading": False,
    }

    result = {
        "version": VERSION,
        "input_version": SOURCE_VERSION,
        "execution_status": "PASS",
        "overall_decision": "HOLD",
        "freeze_cutoff_utc": FREEZE_CUTOFF,
        "market_count": 209,
        "original_candidate_count": 22,
        "primary_market": "KRW-DKA",
        "prospective_events_collected": 0,
        "independent_oos_proven": False,
        "profitability_proven": False,
        "statistical_independence_proven": False,
        "approved_for_live_trading": False,
        "next_required_validation": (
            "Collect genuinely new post-cutoff event-level "
            "data and evaluate a preregistered independent OOS test"
        ),
    }

    for name, expected in hashes.items():
        if digest(source / name) != expected:
            raise ValueError(
                "Input changed during processing: " + name
            )

    output.mkdir(
        parents=True,
        exist_ok=True,
    )

    write_csv(
        output / OUTPUT_FILES[0],
        frozen,
    )

    write_json(
        output / OUTPUT_FILES[1],
        protocol,
    )

    write_json(
        output / OUTPUT_FILES[2],
        result,
    )

    manifest = {
        "version": VERSION,
        "input_version": SOURCE_VERSION,
        "input_sha256": hashes,
        "output_sha256": {
            name: digest(output / name)
            for name in OUTPUT_FILES
        },
        "market_rows": len(frozen),
        "freeze_cutoff_utc": FREEZE_CUTOFF,
    }

    write_json(
        output / "freeze_manifest.json",
        manifest,
    )

    if not verify_completed(output, hashes):
        raise ValueError(
            "Final freeze verification failed"
        )

    print(
        "[PASS] Clean V003-16",
        flush=True,
    )
    print(
        "[PASS] 209 markets | primary KRW-DKA",
        flush=True,
    )
    print(
        "[PASS] SHA256 freeze integrity",
        flush=True,
    )
    print(
        "[DECISION] HOLD | no prospective observations",
        flush=True,
    )

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())

    except KeyboardInterrupt:
        print(
            "[INTERRUPTED]",
            file=sys.stderr,
        )
        sys.exit(130)

    except Exception as exc:
        print(
            "[FAIL] " + str(exc),
            file=sys.stderr,
        )
        sys.exit(1)
