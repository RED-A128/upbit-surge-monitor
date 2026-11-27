
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pattern 256 Exchange Source Verification Audit
Clean V008

- Read-only verification of existing research data
- Upbit public daily candle API
- Atomic checkpoints
- SHA256 integrity verification
- Safe resume
- No interpolation
- No training or trading approval
"""

import argparse
import csv
import hashlib
import json
import re
import sys
import time

from collections import Counter
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


VERSION = "Clean V008"

API_URL = "https://api.upbit.com/v1/candles/days"

SOURCE_DIR = (
    "data/reports/pattern_256_daily_gaps"
)

OUTPUT_DIR = (
    "data/reports/pattern_256_exchange_source"
)

OHLCV_DIR = "data/ohlcv/d1"

API_INTERVAL = 0.5
API_TIMEOUT = 30
MAX_RETRIES = 5

EXPECTED_MARKETS = 290
EXPECTED_AFFECTED = 16
EXPECTED_GAPS = 17
EXPECTED_MISSING_DAYS = 65

FIELD_MAP = {
    "open": "opening_price",
    "high": "high_price",
    "low": "low_price",
    "close": "trade_price",
    "volume": "candle_acc_trade_volume",
    "trade_value": "candle_acc_trade_price",
}

RELATIVE_TOLERANCE = Decimal("0.00000001")
ABSOLUTE_TOLERANCE = Decimal("0.00000001")

COMPLETED_STATUSES = {
    "API_GAP_MATCH",
    "API_CANDLES_FOUND",
    "BOUNDARY_MISMATCH",
}


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def sha256_bytes(data):
    return hashlib.sha256(data).hexdigest()


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
            allow_nan=False,
            sort_keys=True,
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

    temporary = path.with_name(
        path.name + ".tmp"
    )

    with temporary.open("wb") as f:
        f.write(data)
        f.flush()

    temporary.replace(path)


def atomic_json(path, data):
    atomic_write(
        path,
        json_bytes(data),
    )


def parse_utc(value):
    stamp = datetime.fromisoformat(
        str(value).replace("Z", "+00:00")
    )

    if stamp.tzinfo is None:
        stamp = stamp.replace(
            tzinfo=timezone.utc
        )

    return stamp.astimezone(
        timezone.utc
    )


def api_time(stamp):
    return stamp.astimezone(
        timezone.utc
    ).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


def date_range(start, end):
    current = start

    while current <= end:
        yield current
        current += timedelta(days=1)


def valid_market(market):
    return bool(
        re.fullmatch(
            r"KRW-[A-Z0-9]+",
            market,
        )
    )


def decimal_value(value):
    number = Decimal(str(value))

    if not number.is_finite():
        raise ValueError(
            f"Non-finite numeric value: {value}"
        )

    return number


def numeric_match(local, remote):
    a = decimal_value(local)
    b = decimal_value(remote)

    difference = abs(a - b)

    tolerance = max(
        ABSOLUTE_TOLERANCE,
        RELATIVE_TOLERANCE
        * max(abs(a), abs(b)),
    )

    return {
        "local": str(a),
        "api": str(b),
        "difference": str(difference),
        "tolerance": str(tolerance),
        "match": difference <= tolerance,
    }


def verify_source(root):
    folder = root / SOURCE_DIR

    result_path = folder / "result.json"
    report_path = folder / "report.md"
    manifest_path = folder / "manifest.json"

    for path in (
        result_path,
        report_path,
        manifest_path,
    ):
        if not path.is_file():
            raise RuntimeError(
                f"Missing V007 file: {path}"
            )

    result = read_json(result_path)
    manifest = read_json(manifest_path)

    if result.get("version") != "Clean V007":
        raise RuntimeError(
            "Unexpected source version"
        )

    if manifest.get("version") != "Clean V007":
        raise RuntimeError(
            "Unexpected manifest version"
        )

    expected_fields = {
        "execution": "PASS",
        "classification": "TRIAGED_UNVERIFIED",
        "candidate": "d1|DOWN|LOW",
        "common_markets": EXPECTED_MARKETS,
        "affected_markets": EXPECTED_AFFECTED,
        "unique_gaps": EXPECTED_GAPS,
        "data_readiness": "BLOCKED",
        "research_decision": "HOLD",
    }

    for field, expected in expected_fields.items():
        actual = result.get(field)

        if actual != expected:
            raise RuntimeError(
                f"Source mismatch: {field} "
                f"expected={expected} actual={actual}"
            )

    if result.get("alignment_mismatches") != []:
        raise RuntimeError(
            "Source alignment mismatch exists"
        )

    if result.get("layer_gap_counts") != {
        "ohlcv": 17,
        "features": 17,
        "signals": 17,
    }:
        raise RuntimeError(
            "Unexpected layer gap counts"
        )

    for field in (
        "exchange_source_verified",
        "point_in_time_proven",
        "independent_oos_proven",
        "train_approved",
        "live_trading_approved",
    ):
        if result.get(field) is not False:
            raise RuntimeError(
                f"Unexpected source approval: {field}"
            )

    for path in (
        result_path,
        report_path,
    ):
        actual = sha256_file(path)

        expected = manifest.get(
            "sha256", {}
        ).get(path.name)

        if actual != expected:
            raise RuntimeError(
                f"Source SHA256 mismatch: {path.name}"
            )

    if (
        result.get("source_result_sha256")
        != manifest.get("source_result_sha256")
    ):
        raise RuntimeError(
            "V006 source linkage mismatch"
        )

    gaps = result.get("gaps", [])

    if len(gaps) != EXPECTED_GAPS:
        raise RuntimeError(
            "Unexpected gap list length"
        )

    markets = set()
    keys = set()
    total_missing = 0

    for gap in gaps:
        market = gap["market"]

        if not valid_market(market):
            raise RuntimeError(
                f"Invalid market: {market}"
            )

        before = parse_utc(gap["before"])
        after = parse_utc(gap["after"])

        if (
            before.hour
            or before.minute
            or before.second
            or before.microsecond
            or after.hour
            or after.minute
            or after.second
            or after.microsecond
        ):
            raise RuntimeError(
                f"Non-midnight daily candle: {market}"
            )

        missing_days = (
            after - before
        ).days - 1

        if not 1 <= missing_days <= 197:
            raise RuntimeError(
                f"Unsupported gap size: {market}"
            )

        if missing_days != gap["missing_days"]:
            raise RuntimeError(
                f"Gap length mismatch: {market}"
            )

        missing_start = (
            before + timedelta(days=1)
        ).date().isoformat()

        missing_end = (
            after - timedelta(days=1)
        ).date().isoformat()

        if (
            gap["missing_start"] != missing_start
            or gap["missing_end"] != missing_end
        ):
            raise RuntimeError(
                f"Gap date mismatch: {market}"
            )

        if gap.get("present_in_all_layers") is not True:
            raise RuntimeError(
                f"Layer mismatch: {market}"
            )

        key = (
            market,
            before.isoformat(),
            after.isoformat(),
        )

        if key in keys:
            raise RuntimeError(
                f"Duplicate gap: {key}"
            )

        keys.add(key)
        markets.add(market)
        total_missing += missing_days

    if len(markets) != EXPECTED_AFFECTED:
        raise RuntimeError(
            "Affected market count mismatch"
        )

    if total_missing != EXPECTED_MISSING_DAYS:
        raise RuntimeError(
            "Missing candle count mismatch"
        )

    return {
        "result": result,
        "result_sha256": sha256_file(result_path),
        "manifest_sha256": sha256_file(manifest_path),
        "report_sha256": sha256_file(report_path),
    }


def verify_local_ohlcv(root, gaps):
    grouped = {}

    for gap in gaps:
        grouped.setdefault(
            gap["market"], []
        ).append(gap)

    hashes = {}

    for market, market_gaps in grouped.items():
        path = (
            root
            / OHLCV_DIR
            / f"{market}.csv"
        )

        if not path.is_file():
            raise RuntimeError(
                f"Local OHLCV missing: {path}"
            )

        hash_before = sha256_file(path)

        candles = {}

        with path.open(
            "r",
            encoding="utf-8-sig",
            newline="",
        ) as f:
            reader = csv.DictReader(f)

            required = {
                "market",
                "candle_date_time_utc",
                *FIELD_MAP.keys(),
            }

            missing = required - set(
                reader.fieldnames or []
            )

            if missing:
                raise RuntimeError(
                    f"Missing local columns: "
                    f"{market}: {sorted(missing)}"
                )

            previous = None

            for row in reader:
                if row["market"] != market:
                    raise RuntimeError(
                        f"Local market mismatch: {market}"
                    )

                stamp = parse_utc(
                    row["candle_date_time_utc"]
                )

                if previous is not None:
                    if stamp <= previous:
                        raise RuntimeError(
                            f"Local timestamp order error: "
                            f"{market}"
                        )

                previous = stamp

                candles[stamp] = {
                    field: row[field]
                    for field in FIELD_MAP
                }

        for gap in market_gaps:
            before = parse_utc(gap["before"])
            after = parse_utc(gap["after"])

            for side, stamp in (
                ("before", before),
                ("after", after),
            ):
                if stamp not in candles:
                    raise RuntimeError(
                        f"Local boundary missing: "
                        f"{market} {side}"
                    )

                expected = gap[
                    f"{side}_ohlcv"
                ]

                actual = candles[stamp]

                for field in FIELD_MAP:
                    if (
                        decimal_value(actual[field])
                        != decimal_value(expected[field])
                    ):
                        raise RuntimeError(
                            f"Local boundary changed: "
                            f"{market} {side} {field}"
                        )

            for stamp in date_range(
                before + timedelta(days=1),
                after - timedelta(days=1),
            ):
                if stamp in candles:
                    raise RuntimeError(
                        f"Local gap changed: "
                        f"{market} {stamp}"
                    )

        hash_after = sha256_file(path)

        if hash_before != hash_after:
            raise RuntimeError(
                f"Local file changed during scan: {market}"
            )

        hashes[market] = hash_after

        print(
            f"[LOCAL OK] {market}",
            flush=True,
        )

    return hashes


def verify_local_hashes(root, hashes):
    for market, expected in hashes.items():
        path = (
            root
            / OHLCV_DIR
            / f"{market}.csv"
        )

        if sha256_file(path) != expected:
            raise RuntimeError(
                f"Input changed during audit: {market}"
            )


def request_candles(market, before, after):
    # Upbit "to" is treated as an exclusive boundary.
    # Request through the day after the right boundary.

    count = min(
        200,
        (after - before).days + 5,
    )

    params = {
        "market": market,
        "to": api_time(
            after + timedelta(days=1)
        ),
        "count": count,
    }

    url = (
        API_URL + "?" + urlencode(params)
    )

    last_error = None

    for attempt in range(
        1, MAX_RETRIES + 1
    ):
        time.sleep(API_INTERVAL)

        request = Request(
            url,
            headers={
                "Accept": "application/json",
                "User-Agent": (
                    "UpbitResearchAudit/8.0"
                ),
            },
            method="GET",
        )

        try:
            with urlopen(
                request,
                timeout=API_TIMEOUT,
            ) as response:
                status = response.status
                raw = response.read()

                remaining = response.headers.get(
                    "Remaining-Req", ""
                )

            if status != 200:
                raise RuntimeError(
                    f"Unexpected HTTP status: {status}"
                )

            payload = json.loads(
                raw.decode("utf-8")
            )

            if not isinstance(payload, list):
                raise RuntimeError(
                    "API response is not a list"
                )

            match = re.search(
                r"sec=(\d+)",
                remaining,
            )

            if match and int(match.group(1)) <= 1:
                time.sleep(1.1)

            return {
                "success": True,
                "url": url,
                "http_status": status,
                "fetched_utc": utc_now(),
                "remaining_req": remaining,
                "raw": raw,
                "payload": payload,
                "attempts": attempt,
            }

        except HTTPError as exc:
            last_error = (
                f"HTTP {exc.code}: {exc.reason}"
            )

            print(
                f"[API ERROR] {market} "
                f"attempt={attempt} {last_error}",
                flush=True,
            )

            if exc.code == 418:
                # Do not bypass a temporary API ban.
                return {
                    "success": False,
                    "url": url,
                    "error": last_error,
                    "attempts": attempt,
                    "stop_requests": True,
                }

            if exc.code == 429:
                retry_after = exc.headers.get(
                    "Retry-After", "2"
                )

                try:
                    delay = max(
                        2.0,
                        float(retry_after),
                    )
                except ValueError:
                    delay = 2.0

                if attempt < MAX_RETRIES:
                    time.sleep(delay)

            elif exc.code >= 500:
                if attempt < MAX_RETRIES:
                    time.sleep(
                        min(30, 2 ** attempt)
                    )

            else:
                break

        except (
            URLError,
            TimeoutError,
            OSError,
            ValueError,
            RuntimeError,
        ) as exc:
            last_error = (
                f"{type(exc).__name__}: {exc}"
            )

            print(
                f"[API ERROR] {market} "
                f"attempt={attempt} {last_error}",
                flush=True,
            )

            if attempt < MAX_RETRIES:
                time.sleep(
                    min(30, 2 ** attempt)
                )

    return {
        "success": False,
        "url": url,
        "error": last_error,
        "attempts": attempt,
        "stop_requests": False,
    }


def index_api_candles(payload, market):
    indexed = {}

    previous = None

    for candle in payload:
        if not isinstance(candle, dict):
            raise RuntimeError(
                "Invalid API candle object"
            )

        if candle.get("market") != market:
            raise RuntimeError(
                f"API market mismatch: {market}"
            )

        stamp = parse_utc(
            candle["candle_date_time_utc"]
        )

        if (
            stamp.hour
            or stamp.minute
            or stamp.second
            or stamp.microsecond
        ):
            raise RuntimeError(
                f"Invalid API daily timestamp: {stamp}"
            )

        if stamp in indexed:
            raise RuntimeError(
                f"Duplicate API candle: {stamp}"
            )

        if previous is not None:
            if stamp >= previous:
                raise RuntimeError(
                    "Unexpected API candle order"
                )

        previous = stamp

        for field in FIELD_MAP.values():
            if field not in candle:
                raise RuntimeError(
                    f"Missing API field: {field}"
                )

            decimal_value(candle[field])

        opening = decimal_value(
            candle["opening_price"]
        )

        high = decimal_value(
            candle["high_price"]
        )

        low = decimal_value(
            candle["low_price"]
        )

        close = decimal_value(
            candle["trade_price"]
        )

        volume = decimal_value(
            candle["candle_acc_trade_volume"]
        )

        value = decimal_value(
            candle["candle_acc_trade_price"]
        )

        if not (
            low > 0
            and low <= min(opening, close)
            and high >= max(opening, close)
            and volume >= 0
            and value >= 0
        ):
            raise RuntimeError(
                f"Invalid API OHLCV: {market} {stamp}"
            )

        indexed[stamp] = candle

    return indexed


def compare_boundary(local, remote):
    comparisons = {}

    for local_field, api_field in FIELD_MAP.items():
        comparisons[local_field] = numeric_match(
            local[local_field],
            remote[api_field],
        )

    return {
        "match": all(
            item["match"]
            for item in comparisons.values()
        ),
        "fields": comparisons,
    }


def classify_gap(gap, payload):
    market = gap["market"]

    before = parse_utc(gap["before"])
    after = parse_utc(gap["after"])

    indexed = index_api_candles(
        payload,
        market,
    )

    expected_dates = list(
        date_range(
            before + timedelta(days=1),
            after - timedelta(days=1),
        )
    )

    found = [
        stamp.date().isoformat()
        for stamp in expected_dates
        if stamp in indexed
    ]

    absent = [
        stamp.date().isoformat()
        for stamp in expected_dates
        if stamp not in indexed
    ]

    boundary_results = {}

    for side, stamp in (
        ("before", before),
        ("after", after),
    ):
        if stamp not in indexed:
            boundary_results[side] = {
                "present": False,
                "match": False,
            }

        else:
            boundary_results[side] = {
                "present": True,
                **compare_boundary(
                    gap[f"{side}_ohlcv"],
                    indexed[stamp],
                ),
            }

    boundaries_present = all(
        item["present"]
        for item in boundary_results.values()
    )

    boundaries_match = all(
        item["match"]
        for item in boundary_results.values()
    )

    coverage_verified = (
        boundaries_present
        and bool(indexed)
        and min(indexed) <= before
        and max(indexed) >= after
    )

    if not coverage_verified:
        status = "UNVERIFIED"

    elif not boundaries_match:
        status = "BOUNDARY_MISMATCH"

    elif found:
        status = "API_CANDLES_FOUND"

    else:
        status = "API_GAP_MATCH"

    return {
        "status": status,
        "api_candle_count": len(indexed),
        "api_coverage_verified": coverage_verified,
        "boundaries_present": boundaries_present,
        "boundaries_match": boundaries_match,
        "boundary_comparison": boundary_results,
        "expected_missing_days": len(expected_dates),
        "api_found_days": len(found),
        "api_absent_days": len(absent),
        "api_found_dates": found,
        "api_absent_dates": absent,
        "api_found_candles": [
            indexed[stamp]
            for stamp in expected_dates
            if stamp in indexed
        ],
        "historical_no_trade_proven": False,
        "safe_for_backtest": False,
    }


def gap_identifier(gap):
    return (
        gap["market"]
        + "_"
        + gap["missing_start"]
        + "_"
        + gap["missing_end"]
    )


def checkpoint_paths(folder, gap):
    key = gap_identifier(gap)

    return {
        "checkpoint": (
            folder
            / "checkpoints"
            / f"{key}.json"
        ),
        "checksum": (
            folder
            / "checkpoints"
            / f"{key}.sha256"
        ),
        "raw": (
            folder
            / "raw_api"
            / f"{key}.json"
        ),
    }


def checkpoint_context(
    gap,
    source_hash,
    script_hash,
    local_hash,
):
    return {
        "version": VERSION,
        "source_result_sha256": source_hash,
        "script_sha256": script_hash,
        "local_ohlcv_sha256": local_hash,
        "gap_sha256": sha256_bytes(
            json_bytes(gap)
        ),
    }


def load_checkpoint(
    paths,
    context,
    gap,
):
    try:
        for path in paths.values():
            if not path.is_file():
                return None

        expected = paths[
            "checksum"
        ].read_text(
            encoding="utf-8"
        ).strip()

        actual = sha256_file(
            paths["checkpoint"]
        )

        if actual != expected:
            return None

        checkpoint = read_json(
            paths["checkpoint"]
        )

        if checkpoint.get("context") != context:
            return None

        if checkpoint.get("status") not in (
            COMPLETED_STATUSES
        ):
            return None

        raw_hash = sha256_file(
            paths["raw"]
        )

        if (
            raw_hash
            != checkpoint.get("raw_api_sha256")
        ):
            return None

        payload = read_json(
            paths["raw"]
        )

        rebuilt = classify_gap(
            gap,
            payload,
        )

        if (
            rebuilt
            != checkpoint.get("classification")
        ):
            return None

        if (
            checkpoint["status"]
            != rebuilt["status"]
        ):
            return None

        return checkpoint

    except (
        OSError,
        ValueError,
        KeyError,
        TypeError,
        RuntimeError,
        ArithmeticError,
    ):
        return None


def save_checkpoint(paths, record, raw):
    atomic_write(
        paths["raw"],
        raw,
    )

    atomic_json(
        paths["checkpoint"],
        record,
    )

    checksum = sha256_file(
        paths["checkpoint"]
    )

    atomic_write(
        paths["checksum"],
        (checksum + "\n").encode("utf-8"),
    )


def base_record(gap, context):
    return {
        "gap_id": gap_identifier(gap),
        "market": gap["market"],
        "before": gap["before"],
        "after": gap["after"],
        "missing_start": gap["missing_start"],
        "missing_end": gap["missing_end"],
        "missing_days": gap["missing_days"],
        "context": context,
    }


def verify_one(
    gap,
    folder,
    source_hash,
    script_hash,
    local_hash,
    refresh,
):
    paths = checkpoint_paths(
        folder,
        gap,
    )

    context = checkpoint_context(
        gap,
        source_hash,
        script_hash,
        local_hash,
    )

    if not refresh:
        cached = load_checkpoint(
            paths,
            context,
            gap,
        )

        if cached is not None:
            print(
                f"[RESUME] {cached['gap_id']} "
                f"{cached['status']}",
                flush=True,
            )

            return {
                **cached,
                "resumed": True,
            }

    record = base_record(
        gap,
        context,
    )

    before = parse_utc(gap["before"])
    after = parse_utc(gap["after"])

    response = request_candles(
        gap["market"],
        before,
        after,
    )

    if not response["success"]:
        return {
            **record,
            "status": "UNVERIFIED",
            "api_url": response["url"],
            "error": response["error"],
            "attempts": response["attempts"],
            "stop_requests": response[
                "stop_requests"
            ],
            "resumed": False,
        }

    raw = response["raw"]

    try:
        classification = classify_gap(
            gap,
            response["payload"],
        )

    except (
        ValueError,
        KeyError,
        TypeError,
        RuntimeError,
        ArithmeticError,
    ) as exc:
        atomic_write(
            paths["raw"],
            raw,
        )

        return {
            **record,
            "status": "UNVERIFIED",
            "api_url": response["url"],
            "raw_api_sha256": sha256_bytes(raw),
            "error": (
                f"API validation failed: "
                f"{type(exc).__name__}: {exc}"
            ),
            "resumed": False,
        }

    record.update({
        "status": classification["status"],
        "api_url": response["url"],
        "http_status": response["http_status"],
        "fetched_utc": response["fetched_utc"],
        "attempts": response["attempts"],
        "raw_api_sha256": sha256_bytes(raw),
        "raw_api_file": (
            paths["raw"]
            .relative_to(folder)
            .as_posix()
        ),
        "classification": classification,
    })

    save_checkpoint(
        paths,
        record,
        raw,
    )

    print(
        f"[RESULT] {record['gap_id']} "
        f"{record['status']} "
        f"found={classification['api_found_days']} "
        f"absent={classification['api_absent_days']}",
        flush=True,
    )

    return {
        **record,
        "resumed": False,
    }


def write_reports(
    folder,
    source,
    script_hash,
    local_hashes,
    records,
    run_id,
):
    counts = Counter(
        item["status"]
        for item in records
    )

    verified_found = 0
    verified_absent = 0
    unresolved_days = 0

    evidence_files = {}

    for item in records:
        classification = item.get(
            "classification", {}
        )

        if classification.get(
            "api_coverage_verified"
        ) is True:
            verified_found += classification[
                "api_found_days"
            ]

            verified_absent += classification[
                "api_absent_days"
            ]

        else:
            unresolved_days += item[
                "missing_days"
            ]

        raw_file = item.get("raw_api_file")

        if raw_file:
            path = folder / raw_file

            actual = sha256_file(path)

            if actual != item[
                "raw_api_sha256"
            ]:
                raise RuntimeError(
                    f"Raw evidence hash mismatch: {path}"
                )

            evidence_files[raw_file] = actual

    if (
        verified_found
        + verified_absent
        + unresolved_days
        != EXPECTED_MISSING_DAYS
    ):
        raise RuntimeError(
            "Missing day accounting mismatch"
        )

    all_resolved = (
        len(records) == EXPECTED_GAPS
        and counts["UNVERIFIED"] == 0
    )

    all_boundaries_match = all(
        item.get(
            "classification", {}
        ).get("boundaries_match") is True
        for item in records
    )

    all_api_gaps_match = (
        counts["API_GAP_MATCH"]
        == EXPECTED_GAPS
    )

    if not all_resolved:
        decision = (
            "API_VERIFICATION_INCOMPLETE"
        )

    elif counts["BOUNDARY_MISMATCH"]:
        decision = (
            "SOURCE_DISCREPANCY_REVIEW_REQUIRED"
        )

    elif counts["API_CANDLES_FOUND"]:
        decision = (
            "MISSING_CANDLE_REPAIR_PLAN_REQUIRED"
        )

    elif all_api_gaps_match:
        decision = (
            "GAP_EXCLUSION_POLICY_REQUIRED"
        )

    else:
        decision = (
            "MANUAL_REVIEW_REQUIRED"
        )

    result = {
        "version": VERSION,
        "created_utc": utc_now(),
        "run_id": run_id,
        "execution": "PASS",
        "source_version": "Clean V007",
        "source_result_sha256": source[
            "result_sha256"
        ],
        "source_manifest_sha256": source[
            "manifest_sha256"
        ],
        "source_report_sha256": source[
            "report_sha256"
        ],
        "script_sha256": script_hash,
        "candidate": "d1|DOWN|LOW",
        "common_markets": EXPECTED_MARKETS,
        "affected_markets": EXPECTED_AFFECTED,
        "unique_gaps": EXPECTED_GAPS,
        "expected_missing_days": (
            EXPECTED_MISSING_DAYS
        ),
        "api_found_days": verified_found,
        "api_absent_days": verified_absent,
        "unresolved_days": unresolved_days,
        "status_counts": {
            name: counts[name]
            for name in (
                "API_GAP_MATCH",
                "API_CANDLES_FOUND",
                "BOUNDARY_MISMATCH",
                "UNVERIFIED",
            )
        },
        "resumed_gaps": sum(
            item.get("resumed") is True
            for item in records
        ),
        "api_verification_complete": (
            all_resolved
        ),
        "all_boundaries_match": (
            all_boundaries_match
        ),
        "all_api_gaps_match": (
            all_api_gaps_match
        ),
        "exchange_source_verified": (
            all_resolved
            and all_boundaries_match
        ),
        "historical_no_trade_proven": False,
        "local_ohlcv_sha256": local_hashes,
        "source_csv_modified_by_audit": False,
        "gap_policy_implemented": False,
        "data_readiness": "BLOCKED",
        "point_in_time_proven": False,
        "independent_oos_proven": False,
        "profitability_proven": False,
        "train_approved": False,
        "live_trading_approved": False,
        "research_decision": "HOLD",
        "next_action": decision,
        "details": records,
    }

    result_path = folder / "result.json"
    report_path = folder / "report.md"
    csv_path = folder / "gap_verification.csv"
    manifest_path = folder / "manifest.json"

    atomic_json(
        result_path,
        result,
    )

    lines = [
        "# Pattern 256 Exchange Source Verification",
        "",
        f"Version: {VERSION}",
        f"Execution: PASS",
        f"Run ID: {run_id}",
        "",
        "## Summary",
        "",
        "| Metric | Result |",
        "|---|---|",
        f"| Common markets | {EXPECTED_MARKETS} |",
        f"| Affected markets | {EXPECTED_AFFECTED} |",
        f"| Unique gaps | {EXPECTED_GAPS} |",
        f"| Expected missing days | {EXPECTED_MISSING_DAYS} |",
        f"| API found days | {verified_found} |",
        f"| API absent days | {verified_absent} |",
        f"| Unresolved days | {unresolved_days} |",
        f"| API gap match | {counts['API_GAP_MATCH']} |",
        f"| API candles found | {counts['API_CANDLES_FOUND']} |",
        f"| Boundary mismatch | {counts['BOUNDARY_MISMATCH']} |",
        f"| Unverified | {counts['UNVERIFIED']} |",
        f"| Verification complete | {all_resolved} |",
        f"| All boundaries match | {all_boundaries_match} |",
        "| Data readiness | BLOCKED |",
        "| Research decision | HOLD |",
        f"| Next action | {decision} |",
        "",
        "## Gap details",
        "",
        "| Market | Start | End | Days | Found | Status |",
        "|---|---|---|---:|---:|---|",
    ]

    for item in records:
        classification = item.get(
            "classification", {}
        )

        found = classification.get(
            "api_found_days", "N/A"
        )

        lines.append(
            f"| {item['market']} "
            f"| {item['missing_start']} "
            f"| {item['missing_end']} "
            f"| {item['missing_days']} "
            f"| {found} "
            f"| {item['status']} |"
        )

    lines.extend([
        "",
        "## Limitations",
        "",
        "- Current API responses are not historical snapshots.",
        "- API gaps do not prove that no trades occurred.",
        "- Only affected markets and gap boundaries are checked.",
        "- Numeric comparison uses explicit tolerances.",
        "- No candles have been interpolated or repaired.",
        "- Gap exclusion is not implemented by this audit.",
        "- Point-in-time causality is not established.",
        "- Independent OOS is not established.",
        "- Profitability is not established.",
        "- Training and trading remain unapproved.",
        "",
    ])

    atomic_write(
        report_path,
        "\n".join(lines).encode("utf-8"),
    )

    import io

    buffer = io.StringIO(
        newline=""
    )

    fieldnames = [
        "market",
        "missing_start",
        "missing_end",
        "missing_days",
        "status",
        "api_found_days",
        "api_absent_days",
        "api_coverage_verified",
        "boundaries_match",
        "resumed",
    ]

    writer = csv.DictWriter(
        buffer,
        fieldnames=fieldnames,
    )

    writer.writeheader()

    for item in records:
        classification = item.get(
            "classification", {}
        )

        writer.writerow({
            "market": item["market"],
            "missing_start": item["missing_start"],
            "missing_end": item["missing_end"],
            "missing_days": item["missing_days"],
            "status": item["status"],
            "api_found_days": classification.get(
                "api_found_days", ""
            ),
            "api_absent_days": classification.get(
                "api_absent_days", ""
            ),
            "api_coverage_verified": classification.get(
                "api_coverage_verified", False
            ),
            "boundaries_match": classification.get(
                "boundaries_match", False
            ),
            "resumed": item.get(
                "resumed", False
            ),
        })

    atomic_write(
        csv_path,
        buffer.getvalue().encode("utf-8-sig"),
    )

    manifest = {
        "version": VERSION,
        "created_utc": utc_now(),
        "run_id": run_id,
        "source_result_sha256": source[
            "result_sha256"
        ],
        "source_manifest_sha256": source[
            "manifest_sha256"
        ],
        "script_sha256": script_hash,
        "sha256": {
            "result.json": sha256_file(result_path),
            "report.md": sha256_file(report_path),
            "gap_verification.csv": sha256_file(
                csv_path
            ),
            **evidence_files,
        },
    }

    atomic_json(
        manifest_path,
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
        help=(
            "Ignore completed checkpoints "
            "and query API again"
        ),
    )

    args = parser.parse_args()

    root = Path(
        args.root
    ).resolve()

    if not root.is_dir():
        raise RuntimeError(
            f"Project root missing: {root}"
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
    print(
        "Pattern 256 Exchange Source Verification"
    )
    print("Version:", VERSION)
    print("Root:", root)
    print("Run ID:", args.run_id)
    print("Refresh:", args.refresh)
    print("=" * 72)

    source = verify_source(root)

    gaps = source["result"]["gaps"]

    print(
        "[PASS] V007 source integrity",
        flush=True,
    )

    local_hashes = verify_local_ohlcv(
        root,
        gaps,
    )

    records = []
    stop_requests = False

    for index, gap in enumerate(
        gaps,
        start=1,
    ):
        print(
            f"[{index}/{len(gaps)}] "
            f"{gap_identifier(gap)}",
            flush=True,
        )

        local_hash = local_hashes[
            gap["market"]
        ]

        if stop_requests:
            context = checkpoint_context(
                gap,
                source["result_sha256"],
                script_hash,
                local_hash,
            )

            record = {
                **base_record(gap, context),
                "status": "UNVERIFIED",
                "error": (
                    "API requests stopped after "
                    "temporary access restriction"
                ),
                "resumed": False,
            }

        else:
            record = verify_one(
                gap=gap,
                folder=folder,
                source_hash=source[
                    "result_sha256"
                ],
                script_hash=script_hash,
                local_hash=local_hash,
                refresh=args.refresh,
            )

        records.append(record)

        if record.get("stop_requests"):
            stop_requests = True

    if len(records) != EXPECTED_GAPS:
        raise RuntimeError(
            "Incomplete gap accounting"
        )

    verify_local_hashes(
        root,
        local_hashes,
    )

    final_source = verify_source(root)

    for key in (
        "result_sha256",
        "manifest_sha256",
        "report_sha256",
    ):
        if final_source[key] != source[key]:
            raise RuntimeError(
                f"V007 source changed: {key}"
            )

    if sha256_file(script_path) != script_hash:
        raise RuntimeError(
            "Audit script changed during execution"
        )

    result = write_reports(
        folder=folder,
        source=source,
        script_hash=script_hash,
        local_hashes=local_hashes,
        records=records,
        run_id=args.run_id,
    )

    print()
    print("=" * 72)
    print("[PASS] Clean V008 execution")
    print(
        "Status counts:",
        result["status_counts"],
    )
    print(
        "API found days:",
        result["api_found_days"],
    )
    print(
        "API absent days:",
        result["api_absent_days"],
    )
    print(
        "Unresolved days:",
        result["unresolved_days"],
    )
    print(
        "Resumed gaps:",
        result["resumed_gaps"],
    )
    print(
        "Next action:",
        result["next_action"],
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
            "[STOP] Interrupted by user. "
            "Valid completed checkpoints retained.",
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
