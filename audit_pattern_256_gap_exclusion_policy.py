
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Gap Exclusion Policy
Clean V009

Read-only gap policy audit.

- Verify Clean V008 source integrity
- Verify raw API evidence
- Scan all 290 daily OHLCV markets
- Verify actual gaps against source evidence
- Verify feature and signal timestamp alignment
- Build continuous daily segments
- Block existing post-gap indicators
- Define calendar-based forward-window exclusion
- Atomic checkpoints
- SHA256 integrity
- Safe resume
- No interpolation
- No source CSV modification
- No training or trading approval
"""

import argparse
import csv
import hashlib
import io
import json
import re
import sys

from collections import Counter
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path


VERSION = "Clean V009"

SOURCE_VERSION = "Clean V008"

SOURCE_DIR = (
    "data/reports/pattern_256_exchange_source"
)

OUTPUT_DIR = (
    "data/reports/pattern_256_gap_exclusion_policy"
)

LAYER_DIRS = {
    "ohlcv": "data/ohlcv/d1",
    "features": "data/features/d1",
    "signals": "data/signals/256/d1",
}

EXPECTED_MARKETS = 290
EXPECTED_AFFECTED = 16
EXPECTED_GAPS = 17
EXPECTED_MISSING_DAYS = 65

DAY = timedelta(days=1)

TIME_COLUMN = "candle_date_time_utc"

POLICY = {
    "policy_id": "D1_STRICT_SEGMENT_V001",
    "timeframe": "d1",
    "candidate": "d1|DOWN|LOW",
    "calendar_day_continuity_required": True,
    "synthetic_candles_allowed": False,
    "interpolation_allowed": False,
    "cross_gap_feature_history_allowed": False,
    "cross_gap_forward_window_allowed": False,
    "existing_post_gap_features_allowed": False,
    "existing_post_gap_signals_allowed": False,
    "segment_rebuild_required": True,
    "warmup_validation_required": True,
    "completed_candle_validation_required": True,
    "actual_evaluator_integration_required": True,
    "training_approved": False,
    "live_trading_approved": False,
}

FIELD_MAP = {
    "open": "opening_price",
    "high": "high_price",
    "low": "low_price",
    "close": "trade_price",
    "volume": "candle_acc_trade_volume",
    "trade_value": "candle_acc_trade_price",
}


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def utc_now():
    return datetime.now(
        timezone.utc
    ).isoformat()


def sha256_bytes(data):
    return hashlib.sha256(
        data
    ).hexdigest()


def sha256_file(path):
    digest = hashlib.sha256()

    with path.open("rb") as f:
        for block in iter(
            lambda: f.read(1024 * 1024),
            b"",
        ):
            digest.update(block)

    return digest.hexdigest()


def json_bytes(data):
    return (
        json.dumps(
            data,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            allow_nan=False,
        ) + "\n"
    ).encode("utf-8")


def read_json(path):
    with path.open(
        "r",
        encoding="utf-8-sig",
    ) as f:
        return json.load(f)


def atomic_write(path, data):
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temp = path.with_name(
        path.name + ".tmp"
    )

    with temp.open("wb") as f:
        f.write(data)
        f.flush()

    temp.replace(path)


def atomic_json(path, data):
    atomic_write(
        path,
        json_bytes(data),
    )


def write_csv(path, rows, fields):
    buffer = io.StringIO(newline="")

    writer = csv.DictWriter(
        buffer,
        fieldnames=fields,
        extrasaction="raise",
    )

    writer.writeheader()

    for row in rows:
        writer.writerow(row)

    atomic_write(
        path,
        buffer.getvalue().encode(
            "utf-8-sig"
        ),
    )


def parse_utc(value):
    stamp = datetime.fromisoformat(
        str(value).replace(
            "Z",
            "+00:00",
        )
    )

    if stamp.tzinfo is None:
        stamp = stamp.replace(
            tzinfo=timezone.utc
        )

    return stamp.astimezone(
        timezone.utc
    )


def daily_stamp(value):
    stamp = parse_utc(value)

    require(
        stamp.hour == 0
        and stamp.minute == 0
        and stamp.second == 0
        and stamp.microsecond == 0,
        f"Invalid daily timestamp: {value}",
    )

    return stamp


def date_text(stamp):
    return stamp.date().isoformat()


def valid_market(market):
    return bool(
        re.fullmatch(
            r"KRW-[A-Z0-9]+",
            market,
        )
    )


def safe_path(folder, relative):
    base = folder.resolve()

    path = (
        base / relative
    ).resolve()

    require(
        path != base
        and base in path.parents,
        f"Unsafe manifest path: {relative}",
    )

    return path


def numeric(value):
    number = Decimal(str(value))

    require(
        number.is_finite(),
        f"Non-finite numeric value: {value}",
    )

    return number


def numeric_match(a, b):
    a = numeric(a)
    b = numeric(b)

    tolerance = max(
        Decimal("0.00000001"),
        Decimal("0.00000001")
        * max(abs(a), abs(b)),
    )

    return abs(a - b) <= tolerance


def gap_key(gap):
    return (
        gap["market"],
        daily_stamp(
            gap["before"]
        ).isoformat(),
        daily_stamp(
            gap["after"]
        ).isoformat(),
    )


def verify_raw_evidence(
    source_folder,
    detail,
    manifest,
):
    market = detail["market"]

    before = daily_stamp(
        detail["before"]
    )

    after = daily_stamp(
        detail["after"]
    )

    relative = detail["raw_api_file"]

    expected_relative = (
        "raw_api/"
        + market
        + "_"
        + detail["missing_start"]
        + "_"
        + detail["missing_end"]
        + ".json"
    )

    require(
        relative == expected_relative,
        f"Unexpected raw API path: {relative}",
    )

    path = safe_path(
        source_folder,
        relative,
    )

    expected_hash = detail[
        "raw_api_sha256"
    ]

    require(
        manifest["sha256"].get(relative)
        == expected_hash,
        f"Raw API manifest mismatch: {market}",
    )

    require(
        sha256_file(path) == expected_hash,
        f"Raw API SHA256 mismatch: {market}",
    )

    payload = read_json(path)

    require(
        isinstance(payload, list)
        and len(payload) > 0,
        f"Invalid raw API response: {market}",
    )

    indexed = {}

    previous = None

    for candle in payload:
        require(
            candle.get("market") == market,
            f"Raw API market mismatch: {market}",
        )

        stamp = daily_stamp(
            candle["candle_date_time_utc"]
        )

        require(
            stamp not in indexed,
            f"Duplicate API timestamp: {market}",
        )

        if previous is not None:
            require(
                stamp < previous,
                f"API order mismatch: {market}",
            )

        previous = stamp

        for field in FIELD_MAP.values():
            numeric(candle[field])

        indexed[stamp] = candle

    require(
        before in indexed
        and after in indexed,
        f"API boundary missing: {market}",
    )

    current = before + DAY

    absent = []

    while current < after:
        require(
            current not in indexed,
            f"Unexpected API candle: "
            f"{market} {current}",
        )

        absent.append(
            date_text(current)
        )

        current += DAY

    classification = detail[
        "classification"
    ]

    require(
        absent
        == classification["api_absent_dates"],
        f"API absent date mismatch: {market}",
    )

    require(
        len(absent)
        == detail["missing_days"],
        f"API missing day mismatch: {market}",
    )

    for side, stamp in (
        ("before", before),
        ("after", after),
    ):
        comparison = classification[
            "boundary_comparison"
        ][side]

        require(
            comparison["present"] is True
            and comparison["match"] is True,
            f"Boundary comparison failed: "
            f"{market} {side}",
        )

        for local_field, api_field in (
            FIELD_MAP.items()
        ):
            evidence = comparison[
                "fields"
            ][local_field]

            require(
                evidence["match"] is True,
                f"Boundary field failed: "
                f"{market} {local_field}",
            )

            actual_api = indexed[
                stamp
            ][api_field]

            require(
                numeric(actual_api)
                == numeric(evidence["api"]),
                f"Raw API value changed: "
                f"{market} {local_field}",
            )

            require(
                numeric_match(
                    evidence["local"],
                    actual_api,
                ),
                f"Boundary numeric mismatch: "
                f"{market} {local_field}",
            )

    return {
        "market": market,
        "raw_api_file": relative,
        "sha256": expected_hash,
        "missing_days": len(absent),
        "verified": True,
    }


def verify_source(root):
    folder = root / SOURCE_DIR

    result_path = folder / "result.json"

    manifest_path = folder / "manifest.json"

    require(
        result_path.is_file(),
        f"Missing V008 result: {result_path}",
    )

    require(
        manifest_path.is_file(),
        f"Missing V008 manifest: {manifest_path}",
    )

    result = read_json(result_path)

    manifest = read_json(manifest_path)

    require(
        result["version"] == SOURCE_VERSION
        and manifest["version"] == SOURCE_VERSION,
        "V008 version mismatch",
    )

    require(
        result["run_id"]
        == manifest["run_id"],
        "V008 run ID mismatch",
    )

    expected_fields = {
        "execution": "PASS",
        "candidate": "d1|DOWN|LOW",
        "common_markets": EXPECTED_MARKETS,
        "affected_markets": EXPECTED_AFFECTED,
        "unique_gaps": EXPECTED_GAPS,
        "expected_missing_days": (
            EXPECTED_MISSING_DAYS
        ),
        "api_found_days": 0,
        "api_absent_days": EXPECTED_MISSING_DAYS,
        "unresolved_days": 0,
        "data_readiness": "BLOCKED",
        "research_decision": "HOLD",
        "next_action": (
            "GAP_EXCLUSION_POLICY_REQUIRED"
        ),
    }

    for field, expected in (
        expected_fields.items()
    ):
        require(
            result.get(field) == expected,
            f"Unexpected V008 field: {field}",
        )

    for field in (
        "api_verification_complete",
        "all_boundaries_match",
        "all_api_gaps_match",
        "exchange_source_verified",
    ):
        require(
            result.get(field) is True,
            f"V008 verification missing: {field}",
        )

    for field in (
        "historical_no_trade_proven",
        "source_csv_modified_by_audit",
        "gap_policy_implemented",
        "point_in_time_proven",
        "independent_oos_proven",
        "profitability_proven",
        "train_approved",
        "live_trading_approved",
    ):
        require(
            result.get(field) is False,
            f"Unexpected V008 approval: {field}",
        )

    require(
        result["status_counts"] == {
            "API_GAP_MATCH": EXPECTED_GAPS,
            "API_CANDLES_FOUND": 0,
            "BOUNDARY_MISMATCH": 0,
            "UNVERIFIED": 0,
        },
        "Unexpected V008 status counts",
    )

    for field in (
        "source_result_sha256",
        "source_manifest_sha256",
        "script_sha256",
    ):
        require(
            result[field] == manifest[field],
            f"V008 source linkage mismatch: {field}",
        )

    hashes = manifest["sha256"]

    for name in (
        "result.json",
        "report.md",
        "gap_verification.csv",
    ):
        require(
            name in hashes,
            f"Missing manifest entry: {name}",
        )

    for relative, expected in hashes.items():
        path = safe_path(
            folder,
            relative,
        )

        require(
            path.is_file(),
            f"Missing source evidence: {path}",
        )

        require(
            sha256_file(path) == expected,
            f"Source SHA256 mismatch: {relative}",
        )

    details = result["details"]

    require(
        len(details) == EXPECTED_GAPS,
        "V008 detail count mismatch",
    )

    grouped = {}

    evidence = []

    keys = set()

    raw_paths = set()

    total_missing = 0

    for detail in details:
        market = detail["market"]

        require(
            valid_market(market),
            f"Invalid market: {market}",
        )

        require(
            detail["status"] == "API_GAP_MATCH",
            f"Unexpected gap status: {market}",
        )

        require(
            detail["http_status"] == 200,
            f"Invalid API HTTP status: {market}",
        )

        classification = detail[
            "classification"
        ]

        for field in (
            "api_coverage_verified",
            "boundaries_present",
            "boundaries_match",
        ):
            require(
                classification[field] is True,
                f"Invalid classification: {field}",
            )

        require(
            classification["status"]
            == "API_GAP_MATCH",
            f"Classification mismatch: {market}",
        )

        require(
            classification["api_found_days"] == 0,
            f"Unexpected recovered candles: {market}",
        )

        require(
            classification["api_absent_days"]
            == detail["missing_days"],
            f"Absent day mismatch: {market}",
        )

        key = gap_key(detail)

        require(
            key not in keys,
            f"Duplicate source gap: {key}",
        )

        keys.add(key)

        before = daily_stamp(
            detail["before"]
        )

        after = daily_stamp(
            detail["after"]
        )

        require(
            (after - before).days - 1
            == detail["missing_days"],
            f"Gap length mismatch: {market}",
        )

        require(
            detail["missing_days"] > 0,
            f"Invalid gap length: {market}",
        )

        require(
            date_text(before + DAY)
            == detail["missing_start"],
            f"Gap start mismatch: {market}",
        )

        require(
            date_text(after - DAY)
            == detail["missing_end"],
            f"Gap end mismatch: {market}",
        )

        context = detail["context"]

        require(
            context["version"] == SOURCE_VERSION,
            f"Checkpoint version mismatch: {market}",
        )

        require(
            context["script_sha256"]
            == result["script_sha256"],
            f"Checkpoint script mismatch: {market}",
        )

        require(
            context["source_result_sha256"]
            == result["source_result_sha256"],
            f"Checkpoint source mismatch: {market}",
        )

        require(
            context["local_ohlcv_sha256"]
            == result["local_ohlcv_sha256"][market],
            f"Checkpoint input mismatch: {market}",
        )

        raw = verify_raw_evidence(
            folder,
            detail,
            manifest,
        )

        require(
            raw["raw_api_file"] not in raw_paths,
            f"Duplicate raw evidence: {market}",
        )

        raw_paths.add(
            raw["raw_api_file"]
        )

        evidence.append(raw)

        grouped.setdefault(
            market,
            [],
        ).append(detail)

        total_missing += detail[
            "missing_days"
        ]

    require(
        len(grouped) == EXPECTED_AFFECTED,
        "Affected market count mismatch",
    )

    require(
        total_missing == EXPECTED_MISSING_DAYS,
        "Missing day count mismatch",
    )

    require(
        set(result["local_ohlcv_sha256"])
        == set(grouped),
        "Affected OHLCV hash set mismatch",
    )

    require(
        len(raw_paths) == EXPECTED_GAPS,
        "Raw API evidence count mismatch",
    )

    for market in grouped:
        grouped[market].sort(
            key=lambda item: daily_stamp(
                item["before"]
            )
        )

    print(
        "[PASS] V008 source and "
        "17 raw API evidence files",
        flush=True,
    )

    return {
        "result": result,
        "result_sha256": sha256_file(
            result_path
        ),
        "manifest_sha256": sha256_file(
            manifest_path
        ),
        "grouped": grouped,
        "evidence": evidence,
    }


def discover_markets(root):
    layers = {}

    for layer, relative in (
        LAYER_DIRS.items()
    ):
        folder = root / relative

        require(
            folder.is_dir(),
            f"Missing layer: {folder}",
        )

        markets = {}

        for path in sorted(
            folder.glob("KRW-*.csv")
        ):
            market = path.stem

            require(
                valid_market(market),
                f"Invalid filename: {path}",
            )

            require(
                market not in markets,
                f"Duplicate market: {market}",
            )

            markets[market] = path

        require(
            len(markets) == EXPECTED_MARKETS,
            f"{layer}: expected "
            f"{EXPECTED_MARKETS} markets, "
            f"got {len(markets)}",
        )

        layers[layer] = markets

    reference = set(
        layers["ohlcv"]
    )

    for layer, markets in layers.items():
        require(
            set(markets) == reference,
            f"Layer market mismatch: {layer}",
        )

    return layers, sorted(reference)


def read_layer(path, market, layer):
    timestamps = []

    boundaries = {}

    previous = None

    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as f:
        reader = csv.DictReader(f)

        fields = set(
            reader.fieldnames or []
        )

        require(
            TIME_COLUMN in fields,
            f"Missing timestamp column: {path}",
        )

        if layer == "ohlcv":
            required = {
                "market",
                *FIELD_MAP.keys(),
            }

            require(
                required.issubset(fields),
                f"Missing OHLCV fields: {path}",
            )

        for row in reader:
            if "market" in fields:
                require(
                    row["market"] == market,
                    f"Market mismatch: {path}",
                )

            stamp = daily_stamp(
                row[TIME_COLUMN]
            )

            if previous is not None:
                require(
                    stamp > previous,
                    f"Timestamp order mismatch: {path}",
                )

            previous = stamp

            timestamps.append(stamp)

            if layer == "ohlcv":
                values = {
                    field: numeric(row[field])
                    for field in FIELD_MAP
                }

                require(
                    values["low"] > 0
                    and values["low"]
                    <= min(
                        values["open"],
                        values["close"],
                    )
                    and values["high"]
                    >= max(
                        values["open"],
                        values["close"],
                    )
                    and values["volume"] >= 0
                    and values["trade_value"] >= 0,
                    f"Invalid OHLCV: {market} {stamp}",
                )

                boundaries[stamp] = {
                    field: str(value)
                    for field, value in values.items()
                }

    require(
        len(timestamps) > 0,
        f"Empty layer: {path}",
    )

    return timestamps, boundaries


def build_segments(market, timestamps):
    gaps = []

    segments = []

    start = 0

    for index in range(
        1,
        len(timestamps),
    ):
        before = timestamps[index - 1]

        after = timestamps[index]

        difference = after - before

        require(
            difference >= DAY,
            f"Invalid daily interval: {market}",
        )

        if difference == DAY:
            continue

        segment_id = len(segments) + 1

        segments.append({
            "market": market,
            "segment_id": segment_id,
            "start_index": start,
            "end_index": index - 1,
            "start_utc": (
                timestamps[start].isoformat()
            ),
            "end_utc": before.isoformat(),
            "rows": index - start,
            "existing_features_status": (
                "PENDING_WARMUP_PIT"
                if segment_id == 1
                else "BLOCKED_REBUILD_REQUIRED"
            ),
        })

        gaps.append({
            "market": market,
            "before": before.isoformat(),
            "after": after.isoformat(),
            "missing_start": date_text(
                before + DAY
            ),
            "missing_end": date_text(
                after - DAY
            ),
            "missing_days": difference.days - 1,
        })

        start = index

    segment_id = len(segments) + 1

    segments.append({
        "market": market,
        "segment_id": segment_id,
        "start_index": start,
        "end_index": len(timestamps) - 1,
        "start_utc": (
            timestamps[start].isoformat()
        ),
        "end_utc": (
            timestamps[-1].isoformat()
        ),
        "rows": len(timestamps) - start,
        "existing_features_status": (
            "PENDING_WARMUP_PIT"
            if segment_id == 1
            else "BLOCKED_REBUILD_REQUIRED"
        ),
    })

    require(
        sum(
            segment["rows"]
            for segment in segments
        ) == len(timestamps),
        f"Segment accounting mismatch: {market}",
    )

    return gaps, segments


def scan_market(
    market,
    paths,
    expected_gaps,
):
    timestamps, boundaries = read_layer(
        paths["ohlcv"],
        market,
        "ohlcv",
    )

    for layer in (
        "features",
        "signals",
    ):
        other, _ = read_layer(
            paths[layer],
            market,
            layer,
        )

        require(
            timestamps == other,
            f"Layer alignment mismatch: "
            f"{market} {layer}",
        )

    gaps, segments = build_segments(
        market,
        timestamps,
    )

    actual_keys = [
        gap_key(gap)
        for gap in gaps
    ]

    expected_keys = [
        gap_key(gap)
        for gap in expected_gaps
    ]

    require(
        actual_keys == expected_keys,
        f"Actual gap set mismatch: {market}",
    )

    for actual, expected in zip(
        gaps,
        expected_gaps,
    ):
        require(
            actual["missing_days"]
            == expected["missing_days"],
            f"Missing day mismatch: {market}",
        )

        for side in (
            "before",
            "after",
        ):
            stamp = daily_stamp(
                expected[side]
            )

            local = boundaries[stamp]

            comparison = expected[
                "classification"
            ][
                "boundary_comparison"
            ][side]["fields"]

            for field in FIELD_MAP:
                require(
                    numeric(local[field])
                    == numeric(
                        comparison[field]["local"]
                    ),
                    f"Local boundary changed: "
                    f"{market} {side} {field}",
                )

    blocked_rows = sum(
        segment["rows"]
        for segment in segments
        if segment["segment_id"] > 1
    )

    return {
        "market": market,
        "rows": len(timestamps),
        "first_utc": (
            timestamps[0].isoformat()
        ),
        "last_utc": (
            timestamps[-1].isoformat()
        ),
        "gap_count": len(gaps),
        "missing_days": sum(
            gap["missing_days"]
            for gap in gaps
        ),
        "segment_count": len(segments),
        "existing_post_gap_rows_blocked": (
            blocked_rows
        ),
        "pre_gap_or_gap_free_rows": (
            len(timestamps) - blocked_rows
        ),
        "gaps": gaps,
        "segments": segments,
        "alignment_verified": True,
        "status": "PASS",
    }


def market_hashes(paths):
    return {
        layer: sha256_file(path)
        for layer, path in paths.items()
    }


def checkpoint_paths(folder, market):
    base = folder / "checkpoints"

    return (
        base / f"{market}.json",
        base / f"{market}.sha256",
    )


def validate_cached_record(
    record,
    market,
    expected_gaps,
):
    require(
        record["market"] == market,
        "Cached market mismatch",
    )

    require(
        record["status"] == "PASS",
        "Cached status mismatch",
    )

    require(
        record["alignment_verified"] is True,
        "Cached alignment mismatch",
    )

    require(
        record["rows"] > 0,
        "Cached row count invalid",
    )

    gaps = record["gaps"]

    segments = record["segments"]

    require(
        [
            gap_key(gap)
            for gap in gaps
        ] == [
            gap_key(gap)
            for gap in expected_gaps
        ],
        "Cached gap mismatch",
    )

    require(
        record["gap_count"] == len(gaps),
        "Cached gap count mismatch",
    )

    require(
        record["missing_days"]
        == sum(
            gap["missing_days"]
            for gap in gaps
        ),
        "Cached missing days mismatch",
    )

    require(
        len(segments) == len(gaps) + 1,
        "Cached segment count mismatch",
    )

    require(
        record["segment_count"] == len(segments),
        "Cached segment count field mismatch",
    )

    next_index = 0

    blocked_rows = 0

    for index, segment in enumerate(
        segments,
        start=1,
    ):
        require(
            segment["market"] == market,
            "Cached segment market mismatch",
        )

        require(
            segment["segment_id"] == index,
            "Cached segment ID mismatch",
        )

        require(
            segment["start_index"] == next_index,
            "Cached segment index mismatch",
        )

        rows = segment["rows"]

        require(
            rows > 0,
            "Cached segment row count invalid",
        )

        require(
            segment["end_index"]
            - segment["start_index"] + 1
            == rows,
            "Cached segment index accounting",
        )

        start = daily_stamp(
            segment["start_utc"]
        )

        end = daily_stamp(
            segment["end_utc"]
        )

        require(
            (end - start).days + 1 == rows,
            "Cached segment continuity mismatch",
        )

        if index > 1:
            blocked_rows += rows

            require(
                segment["existing_features_status"]
                == "BLOCKED_REBUILD_REQUIRED",
                "Cached post-gap status mismatch",
            )

            require(
                start == daily_stamp(
                    gaps[index - 2]["after"]
                ),
                "Cached gap boundary mismatch",
            )

        else:
            require(
                segment["existing_features_status"]
                == "PENDING_WARMUP_PIT",
                "Cached initial segment status",
            )

        if index <= len(gaps):
            require(
                end == daily_stamp(
                    gaps[index - 1]["before"]
                ),
                "Cached gap end mismatch",
            )

        next_index += rows

    require(
        next_index == record["rows"],
        "Cached total rows mismatch",
    )

    require(
        record["first_utc"]
        == segments[0]["start_utc"],
        "Cached first timestamp mismatch",
    )

    require(
        record["last_utc"]
        == segments[-1]["end_utc"],
        "Cached last timestamp mismatch",
    )

    require(
        blocked_rows
        == record["existing_post_gap_rows_blocked"],
        "Cached blocked row mismatch",
    )

    require(
        record["pre_gap_or_gap_free_rows"]
        + blocked_rows
        == record["rows"],
        "Cached row accounting mismatch",
    )


def load_checkpoint(
    folder,
    market,
    context,
    expected_gaps,
):
    path, checksum = checkpoint_paths(
        folder,
        market,
    )

    try:
        if not (
            path.is_file()
            and checksum.is_file()
        ):
            return None

        expected = checksum.read_text(
            encoding="utf-8"
        ).strip()

        if sha256_file(path) != expected:
            return None

        payload = read_json(path)

        if payload["context"] != context:
            return None

        record = payload["record"]

        validate_cached_record(
            record,
            market,
            expected_gaps,
        )

        return record

    except (
        OSError,
        ValueError,
        TypeError,
        KeyError,
        RuntimeError,
        ArithmeticError,
    ):
        return None


def save_checkpoint(
    folder,
    market,
    context,
    record,
):
    path, checksum = checkpoint_paths(
        folder,
        market,
    )

    atomic_json(
        path,
        {
            "context": context,
            "record": record,
        },
    )

    atomic_write(
        checksum,
        (
            sha256_file(path) + "\n"
        ).encode("utf-8"),
    )


def process_market(
    market,
    paths,
    expected_gaps,
    source,
    script_hash,
    folder,
    refresh,
):
    hashes = market_hashes(paths)

    if expected_gaps:
        expected_hash = source[
            "result"
        ][
            "local_ohlcv_sha256"
        ][market]

        require(
            hashes["ohlcv"] == expected_hash,
            f"V008 OHLCV changed: {market}",
        )

    context = {
        "version": VERSION,
        "script_sha256": script_hash,
        "source_result_sha256": source[
            "result_sha256"
        ],
        "source_manifest_sha256": source[
            "manifest_sha256"
        ],
        "policy_sha256": sha256_bytes(
            json_bytes(POLICY)
        ),
        "market": market,
        "input_sha256": hashes,
        "expected_gaps_sha256": sha256_bytes(
            json_bytes(expected_gaps)
        ),
    }

    if not refresh:
        cached = load_checkpoint(
            folder,
            market,
            context,
            expected_gaps,
        )

        if cached is not None:
            require(
                market_hashes(paths) == hashes,
                f"Input changed: {market}",
            )

            print(
                f"[RESUME] {market}",
                flush=True,
            )

            return {
                **cached,
                "input_sha256": hashes,
                "resumed": True,
            }

    record = scan_market(
        market,
        paths,
        expected_gaps,
    )

    validate_cached_record(
        record,
        market,
        expected_gaps,
    )

    require(
        market_hashes(paths) == hashes,
        f"Input changed during scan: {market}",
    )

    save_checkpoint(
        folder,
        market,
        context,
        record,
    )

    print(
        f"[PASS] {market} "
        f"rows={record['rows']} "
        f"gaps={record['gap_count']} "
        f"segments={record['segment_count']} "
        f"blocked="
        f"{record['existing_post_gap_rows_blocked']}",
        flush=True,
    )

    return {
        **record,
        "input_sha256": hashes,
        "resumed": False,
    }


def evaluate_window(
    signal_time,
    segment_start,
    segment_end,
    history_bars,
    forward_bars,
    existing_post_gap,
    segment_rebuilt,
    completed_through,
):
    """
    Reference policy function.

    This function does not approve a real
    backtest event by itself.

    The actual evaluator must supply its
    verified history requirements, horizon,
    completed candle cutoff and rebuild state.
    """

    require(
        isinstance(history_bars, int)
        and not isinstance(history_bars, bool)
        and history_bars >= 1,
        "Invalid history length",
    )

    require(
        isinstance(forward_bars, int)
        and not isinstance(forward_bars, bool)
        and forward_bars >= 1,
        "Invalid forward horizon",
    )

    signal_time = daily_stamp(signal_time)

    segment_start = daily_stamp(segment_start)

    segment_end = daily_stamp(segment_end)

    completed_through = daily_stamp(
        completed_through
    )

    require(
        segment_start <= segment_end,
        "Invalid segment bounds",
    )

    reasons = []

    if not (
        segment_start
        <= signal_time
        <= segment_end
    ):
        reasons.append(
            "SIGNAL_OUTSIDE_SEGMENT"
        )

    if (
        existing_post_gap
        and not segment_rebuilt
    ):
        reasons.append(
            "POST_GAP_REBUILD_REQUIRED"
        )

    history_start = (
        signal_time
        - DAY * (history_bars - 1)
    )

    forward_end = (
        signal_time
        + DAY * forward_bars
    )

    if history_start < segment_start:
        reasons.append(
            "INSUFFICIENT_CONTINUOUS_HISTORY"
        )

    if forward_end > segment_end:
        reasons.append(
            "FORWARD_WINDOW_OUTSIDE_SEGMENT"
        )

    if signal_time > completed_through:
        reasons.append(
            "SIGNAL_CANDLE_INCOMPLETE"
        )

    if forward_end > completed_through:
        reasons.append(
            "FORWARD_WINDOW_INCOMPLETE"
        )

    return {
        "window_policy_pass": len(reasons) == 0,
        "history_start": (
            history_start.isoformat()
        ),
        "forward_end": (
            forward_end.isoformat()
        ),
        "reasons": reasons,
    }


def run_policy_self_tests():
    start = "2020-01-01T00:00:00Z"

    end = "2020-01-10T00:00:00Z"

    cases = [
        {
            "name": "valid_continuous_window",
            "signal": "2020-01-05T00:00:00Z",
            "history": 3,
            "forward": 3,
            "post_gap": False,
            "rebuilt": False,
            "completed": end,
            "expected": True,
            "reason": None,
        },
        {
            "name": "insufficient_history",
            "signal": "2020-01-02T00:00:00Z",
            "history": 3,
            "forward": 3,
            "post_gap": False,
            "rebuilt": False,
            "completed": end,
            "expected": False,
            "reason": (
                "INSUFFICIENT_CONTINUOUS_HISTORY"
            ),
        },
        {
            "name": "cross_segment_forward",
            "signal": "2020-01-09T00:00:00Z",
            "history": 3,
            "forward": 3,
            "post_gap": False,
            "rebuilt": False,
            "completed": end,
            "expected": False,
            "reason": (
                "FORWARD_WINDOW_OUTSIDE_SEGMENT"
            ),
        },
        {
            "name": "post_gap_not_rebuilt",
            "signal": "2020-01-05T00:00:00Z",
            "history": 3,
            "forward": 3,
            "post_gap": True,
            "rebuilt": False,
            "completed": end,
            "expected": False,
            "reason": (
                "POST_GAP_REBUILD_REQUIRED"
            ),
        },
        {
            "name": "post_gap_rebuilt",
            "signal": "2020-01-05T00:00:00Z",
            "history": 3,
            "forward": 3,
            "post_gap": True,
            "rebuilt": True,
            "completed": end,
            "expected": True,
            "reason": None,
        },
        {
            "name": "incomplete_forward_window",
            "signal": "2020-01-05T00:00:00Z",
            "history": 3,
            "forward": 3,
            "post_gap": False,
            "rebuilt": False,
            "completed": (
                "2020-01-07T00:00:00Z"
            ),
            "expected": False,
            "reason": (
                "FORWARD_WINDOW_INCOMPLETE"
            ),
        },
        {
            "name": "incomplete_signal_candle",
            "signal": "2020-01-05T00:00:00Z",
            "history": 3,
            "forward": 1,
            "post_gap": False,
            "rebuilt": False,
            "completed": (
                "2020-01-04T00:00:00Z"
            ),
            "expected": False,
            "reason": (
                "SIGNAL_CANDLE_INCOMPLETE"
            ),
        },
        {
            "name": "exact_segment_boundary",
            "signal": "2020-01-08T00:00:00Z",
            "history": 8,
            "forward": 2,
            "post_gap": False,
            "rebuilt": False,
            "completed": end,
            "expected": True,
            "reason": None,
        },
        {
            "name": "signal_outside_segment",
            "signal": "2019-12-31T00:00:00Z",
            "history": 1,
            "forward": 1,
            "post_gap": False,
            "rebuilt": False,
            "completed": end,
            "expected": False,
            "reason": (
                "SIGNAL_OUTSIDE_SEGMENT"
            ),
        },
    ]

    results = []

    for case in cases:
        result = evaluate_window(
            signal_time=case["signal"],
            segment_start=start,
            segment_end=end,
            history_bars=case["history"],
            forward_bars=case["forward"],
            existing_post_gap=case["post_gap"],
            segment_rebuilt=case["rebuilt"],
            completed_through=case["completed"],
        )

        require(
            result["window_policy_pass"]
            == case["expected"],
            f"Policy test failed: {case['name']}",
        )

        if case["reason"] is not None:
            require(
                case["reason"] in result["reasons"],
                f"Policy reason failed: "
                f"{case['name']}",
            )

        results.append({
            "name": case["name"],
            "status": "PASS",
            "result": result,
        })

    for name, history, forward in (
        ("invalid_history_zero", 0, 1),
        ("invalid_forward_zero", 1, 0),
        ("invalid_history_boolean", True, 1),
    ):
        rejected = False

        try:
            evaluate_window(
                signal_time=(
                    "2020-01-05T00:00:00Z"
                ),
                segment_start=start,
                segment_end=end,
                history_bars=history,
                forward_bars=forward,
                existing_post_gap=False,
                segment_rebuilt=False,
                completed_through=end,
            )

        except RuntimeError:
            rejected = True

        require(
            rejected,
            f"Invalid input accepted: {name}",
        )

        results.append({
            "name": name,
            "status": "PASS",
        })

    timestamps = [
        daily_stamp(value)
        for value in (
            "2020-01-01T00:00:00Z",
            "2020-01-02T00:00:00Z",
            "2020-01-05T00:00:00Z",
            "2020-01-06T00:00:00Z",
            "2020-01-09T00:00:00Z",
        )
    ]

    gaps, segments = build_segments(
        "KRW-TEST",
        timestamps,
    )

    require(
        len(gaps) == 2
        and len(segments) == 3
        and sum(
            gap["missing_days"]
            for gap in gaps
        ) == 4
        and [
            segment["rows"]
            for segment in segments
        ] == [2, 2, 1],
        "Multiple gap segment test failed",
    )

    results.append({
        "name": "multiple_gap_segments",
        "status": "PASS",
    })

    print(
        f"[PASS] Policy self-tests: "
        f"{len(results)}/{len(results)}",
        flush=True,
    )

    return results


def verify_all_inputs(
    layers,
    records,
):
    for record in records:
        market = record["market"]

        paths = {
            layer: markets[market]
            for layer, markets in layers.items()
        }

        require(
            market_hashes(paths)
            == record["input_sha256"],
            f"Final input hash mismatch: {market}",
        )


def write_reports(
    folder,
    source,
    records,
    self_tests,
    script_hash,
    run_id,
):
    affected = [
        record
        for record in records
        if record["gap_count"] > 0
    ]

    all_gaps = []

    all_segments = []

    market_rows = []

    exclusion_rows = []

    for record in records:
        all_gaps.extend(
            record["gaps"]
        )

        all_segments.extend(
            record["segments"]
        )

        market_rows.append({
            "market": record["market"],
            "rows": record["rows"],
            "first_utc": record["first_utc"],
            "last_utc": record["last_utc"],
            "gap_count": record["gap_count"],
            "missing_days": record["missing_days"],
            "segment_count": record["segment_count"],
            "existing_post_gap_rows_blocked": (
                record[
                    "existing_post_gap_rows_blocked"
                ]
            ),
            "pre_gap_or_gap_free_rows": (
                record[
                    "pre_gap_or_gap_free_rows"
                ]
            ),
            "alignment_verified": (
                record["alignment_verified"]
            ),
            "resumed": record["resumed"],
        })

        for gap in record["gaps"]:
            exclusion_rows.append({
                "market": gap["market"],
                "rule": "MISSING_CANDLES",
                "start_utc": daily_stamp(
                    gap["before"]
                ).__add__(DAY).isoformat(),
                "end_utc": daily_stamp(
                    gap["after"]
                ).__sub__(DAY).isoformat(),
                "rows_or_days": gap["missing_days"],
                "action": "NO_SYNTHETIC_CANDLES",
            })

        for segment in record["segments"]:
            if segment["segment_id"] > 1:
                exclusion_rows.append({
                    "market": record["market"],
                    "rule": (
                        "EXISTING_POST_GAP_INDICATORS"
                    ),
                    "start_utc": (
                        segment["start_utc"]
                    ),
                    "end_utc": (
                        segment["end_utc"]
                    ),
                    "rows_or_days": segment["rows"],
                    "action": (
                        "BLOCK_UNTIL_SEGMENT_REBUILD"
                    ),
                })

    total_rows = sum(
        record["rows"]
        for record in records
    )

    blocked_rows = sum(
        record["existing_post_gap_rows_blocked"]
        for record in records
    )

    remaining_rows = sum(
        record["pre_gap_or_gap_free_rows"]
        for record in records
    )

    missing_days = sum(
        gap["missing_days"]
        for gap in all_gaps
    )

    require(
        len(records) == EXPECTED_MARKETS,
        "Final market count mismatch",
    )

    require(
        len(affected) == EXPECTED_AFFECTED,
        "Final affected market count mismatch",
    )

    require(
        len(all_gaps) == EXPECTED_GAPS,
        "Final gap count mismatch",
    )

    require(
        missing_days == EXPECTED_MISSING_DAYS,
        "Final missing day mismatch",
    )

    require(
        len(all_segments)
        == EXPECTED_MARKETS + EXPECTED_GAPS,
        "Final segment count mismatch",
    )

    require(
        blocked_rows + remaining_rows
        == total_rows,
        "Final row accounting mismatch",
    )

    require(
        sum(
            segment["rows"]
            for segment in all_segments
        ) == total_rows,
        "Final segment row mismatch",
    )

    require(
        len(exclusion_rows)
        == EXPECTED_GAPS * 2,
        "Final exclusion rule count mismatch",
    )

    result = {
        "version": VERSION,
        "created_utc": utc_now(),
        "run_id": run_id,
        "execution": "PASS",
        "classification": (
            "POLICY_DEFINED_PENDING_INTEGRATION"
        ),
        "source_version": SOURCE_VERSION,
        "source_result_sha256": source[
            "result_sha256"
        ],
        "source_manifest_sha256": source[
            "manifest_sha256"
        ],
        "script_sha256": script_hash,
        "candidate": "d1|DOWN|LOW",
        "markets_checked": len(records),
        "gap_free_markets": (
            len(records) - len(affected)
        ),
        "affected_markets": len(affected),
        "unique_gaps": len(all_gaps),
        "missing_days": missing_days,
        "continuous_segments": len(all_segments),
        "raw_api_files_verified": len(
            source["evidence"]
        ),
        "layer_alignment_mismatches": 0,
        "total_daily_rows": total_rows,
        "existing_post_gap_rows_blocked": (
            blocked_rows
        ),
        "pre_gap_or_gap_free_rows": (
            remaining_rows
        ),
        "blocked_row_fraction": (
            blocked_rows / total_rows
        ),
        "resumed_markets": sum(
            record["resumed"]
            for record in records
        ),
        "policy_self_tests_passed": len(
            self_tests
        ),
        "policy_self_tests_failed": 0,
        "gap_policy_defined": True,
        "reference_window_policy_tested": True,
        "exclusion_plan_generated": True,
        "gap_policy_implemented_in_evaluator": False,
        "actual_forward_horizon_verified": False,
        "actual_feature_warmup_verified": False,
        "segment_rebuild_completed": False,
        "remaining_rows_approved_for_backtest": False,
        "source_csv_modified_by_audit": False,
        "historical_no_trade_proven": False,
        "point_in_time_proven": False,
        "independent_oos_proven": False,
        "profitability_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "data_readiness": "BLOCKED",
        "research_decision": "HOLD",
        "next_action": (
            "SEGMENT_REBUILD_AND_"
            "EVALUATOR_INTEGRATION_REQUIRED"
        ),
        "policy": POLICY,
        "raw_api_evidence": source["evidence"],
        "markets": records,
    }

    atomic_json(
        folder / "result.json",
        result,
    )

    atomic_json(
        folder / "policy.json",
        POLICY,
    )

    atomic_json(
        folder / "policy_self_tests.json",
        {
            "version": VERSION,
            "passed": len(self_tests),
            "failed": 0,
            "tests": self_tests,
        },
    )

    write_csv(
        folder / "market_summary.csv",
        market_rows,
        [
            "market",
            "rows",
            "first_utc",
            "last_utc",
            "gap_count",
            "missing_days",
            "segment_count",
            "existing_post_gap_rows_blocked",
            "pre_gap_or_gap_free_rows",
            "alignment_verified",
            "resumed",
        ],
    )

    write_csv(
        folder / "gap_inventory.csv",
        all_gaps,
        [
            "market",
            "before",
            "after",
            "missing_start",
            "missing_end",
            "missing_days",
        ],
    )

    write_csv(
        folder / "continuous_segments.csv",
        all_segments,
        [
            "market",
            "segment_id",
            "start_index",
            "end_index",
            "start_utc",
            "end_utc",
            "rows",
            "existing_features_status",
        ],
    )

    write_csv(
        folder / "exclusion_plan.csv",
        exclusion_rows,
        [
            "market",
            "rule",
            "start_utc",
            "end_utc",
            "rows_or_days",
            "action",
        ],
    )

    lines = [
        "# Pattern 256 Gap Exclusion Policy",
        "",
        f"Version: {VERSION}",
        f"Run ID: {run_id}",
        "",
        "| Metric | Result |",
        "|---|---|",
        "| Execution | PASS |",
        f"| Markets checked | {len(records)} |",
        f"| Gap-free markets | "
        f"{len(records) - len(affected)} |",
        f"| Affected markets | {len(affected)} |",
        f"| Unique gaps | {len(all_gaps)} |",
        f"| Missing days | {missing_days} |",
        f"| Continuous segments | "
        f"{len(all_segments)} |",
        f"| Total daily rows | {total_rows} |",
        f"| Existing post-gap rows blocked | "
        f"{blocked_rows} |",
        f"| Pre-gap or gap-free rows | "
        f"{remaining_rows} |",
        f"| Blocked row fraction | "
        f"{blocked_rows / total_rows:.6%} |",
        f"| Raw API files verified | "
        f"{len(source['evidence'])} |",
        f"| Policy self-tests passed | "
        f"{len(self_tests)} |",
        "| Data readiness | BLOCKED |",
        "| Research decision | HOLD |",
        "",
        "## Affected markets",
        "",
        "| Market | Gaps | Missing days | "
        "Segments | Post-gap rows blocked |",
        "|---|---:|---:|---:|---:|",
    ]

    for record in affected:
        lines.append(
            f"| {record['market']} "
            f"| {record['gap_count']} "
            f"| {record['missing_days']} "
            f"| {record['segment_count']} "
            f"| "
            f"{record['existing_post_gap_rows_blocked']} |"
        )

    lines.extend([
        "",
        "## Policy",
        "",
        "- Preserve all original input CSV files.",
        "- Do not fabricate missing candles.",
        "- Split markets into continuous daily segments.",
        "- Block existing post-gap indicators and signals.",
        "- Rebuild features independently per segment.",
        "- Verify actual indicator warmup requirements.",
        "- Reject history windows crossing a gap.",
        "- Reject forward windows crossing a gap.",
        "- Reject incomplete candle windows.",
        "- Apply identical eligibility rules to signals "
        "and baseline candidates.",
        "",
        "## Limitations",
        "",
        "- This is a policy and exclusion-plan audit.",
        "- The production evaluator is not modified.",
        "- The actual evaluation horizon is not verified.",
        "- The actual feature warmup is not verified.",
        "- Segment-level feature rebuild is not performed.",
        "- Remaining rows are not approved backtest rows.",
        "- Historical absence of trading is not proven.",
        "- Independent OOS and profitability are not proven.",
        "- Training and trading remain unapproved.",
        "",
        "## Next action",
        "",
        "SEGMENT_REBUILD_AND_EVALUATOR_INTEGRATION_REQUIRED",
        "",
    ])

    atomic_write(
        folder / "report.md",
        "\n".join(lines).encode("utf-8"),
    )

    output_names = [
        "result.json",
        "report.md",
        "policy.json",
        "policy_self_tests.json",
        "market_summary.csv",
        "gap_inventory.csv",
        "continuous_segments.csv",
        "exclusion_plan.csv",
    ]

    manifest = {
        "version": VERSION,
        "created_utc": utc_now(),
        "run_id": run_id,
        "source_version": SOURCE_VERSION,
        "source_result_sha256": source[
            "result_sha256"
        ],
        "source_manifest_sha256": source[
            "manifest_sha256"
        ],
        "script_sha256": script_hash,
        "sha256": {
            name: sha256_file(
                folder / name
            )
            for name in output_names
        },
    }

    atomic_json(
        folder / "manifest.json",
        manifest,
    )

    return result


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--root",
        required=True,
    )

    parser.add_argument(
        "--run-id",
        default="manual",
    )

    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Ignore market checkpoints",
    )

    args = parser.parse_args()

    root = Path(
        args.root
    ).resolve()

    require(
        root.is_dir(),
        f"Project root missing: {root}",
    )

    folder = root / OUTPUT_DIR

    folder.mkdir(
        parents=True,
        exist_ok=True,
    )

    script_path = Path(
        __file__
    ).resolve()

    script_hash = sha256_file(
        script_path
    )

    print("=" * 72)
    print("Pattern 256 Gap Exclusion Policy")
    print("Version:", VERSION)
    print("Root:", root)
    print("Run ID:", args.run_id)
    print("Refresh:", args.refresh)
    print("=" * 72)

    source = verify_source(root)

    self_tests = run_policy_self_tests()

    layers, markets = discover_markets(root)

    require(
        set(source["grouped"]).issubset(
            set(markets)
        ),
        "Affected market missing from inputs",
    )

    records = []

    for index, market in enumerate(
        markets,
        start=1,
    ):
        print(
            f"[{index}/{len(markets)}] {market}",
            flush=True,
        )

        paths = {
            layer: files[market]
            for layer, files in layers.items()
        }

        expected_gaps = source[
            "grouped"
        ].get(
            market,
            [],
        )

        record = process_market(
            market=market,
            paths=paths,
            expected_gaps=expected_gaps,
            source=source,
            script_hash=script_hash,
            folder=folder,
            refresh=args.refresh,
        )

        records.append(record)

    final_layers, final_markets = (
        discover_markets(root)
    )

    require(
        final_markets == markets,
        "Market set changed during audit",
    )

    verify_all_inputs(
        final_layers,
        records,
    )

    final_source = verify_source(root)

    for key in (
        "result_sha256",
        "manifest_sha256",
    ):
        require(
            final_source[key] == source[key],
            f"V008 source changed: {key}",
        )

    require(
        sha256_file(script_path) == script_hash,
        "Audit script changed during execution",
    )

    result = write_reports(
        folder=folder,
        source=source,
        records=records,
        self_tests=self_tests,
        script_hash=script_hash,
        run_id=args.run_id,
    )

    print()
    print("=" * 72)
    print("[PASS] Clean V009")
    print(
        "Markets:",
        result["markets_checked"],
    )
    print(
        "Affected markets:",
        result["affected_markets"],
    )
    print(
        "Gaps:",
        result["unique_gaps"],
    )
    print(
        "Missing days:",
        result["missing_days"],
    )
    print(
        "Continuous segments:",
        result["continuous_segments"],
    )
    print(
        "Blocked post-gap rows:",
        result["existing_post_gap_rows_blocked"],
    )
    print(
        "Policy self-tests:",
        result["policy_self_tests_passed"],
    )
    print(
        "Resumed markets:",
        result["resumed_markets"],
    )
    print("Data readiness: BLOCKED")
    print("Research decision: HOLD")
    print("Output:", folder)
    print("=" * 72)

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())

    except KeyboardInterrupt:
        print(
            "[STOP] Interrupted. "
            "Valid checkpoints retained.",
            file=sys.stderr,
            flush=True,
        )

        sys.exit(130)

    except Exception as exc:
        print(
            "[FAIL]",
            type(exc).__name__,
            str(exc),
            file=sys.stderr,
            flush=True,
        )

        sys.exit(1)
