#!/usr/bin/env python3

import csv
import json
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from centralization_stats import combine_rows, extract_domain_counts, filter_rows, stats_from_csv, _shannon

DATA_JS_PATH = REPO_ROOT / "www" / "data.js"
DATA_HISTORY_DIR = REPO_ROOT / "data" / "historical"

TIMESTAMP_RE = re.compile(
    r"(?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?P<tz>Z|[+-]\d{2}:?\d{2})?"
)


def parse_timestamp_from_name(name):
    match = TIMESTAMP_RE.search(name)
    if not match:
        return None
    ts = match.group("ts")
    tz = match.group("tz") or ""
    if tz == "Z":
        tz = "+00:00"
    iso = f"{ts}{tz}"
    try:
        dt = datetime.fromisoformat(iso)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def find_newest_file(directory):
    candidates = []
    for path in Path(directory).iterdir():
        if not path.is_file():
            continue
        dt = parse_timestamp_from_name(path.name)
        if dt is None:
            continue
        candidates.append((dt, path))
    if not candidates:
        raise RuntimeError(f"No timestamped files found in {directory}")
    candidates.sort(key=lambda item: item[0])
    return candidates[-1][1]


def find_closest_to(directory, target_dt):
    best_path = None
    best_diff = None
    for path in Path(directory).iterdir():
        if not path.is_file():
            continue
        dt = parse_timestamp_from_name(path.name)
        if dt is None:
            continue
        diff = abs((dt - target_dt).total_seconds())
        if best_diff is None or diff < best_diff:
            best_diff = diff
            best_path = path
    if best_path is None:
        raise RuntimeError(f"No timestamped files found in {directory}")
    return best_path


def load_data_js(path):
    text = path.read_text()
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise RuntimeError(f"Unable to parse {path}")
    payload = text[start : end + 1]
    return json.loads(payload)


def write_data_js(path, data):
    payload = json.dumps(data, indent=2, ensure_ascii=True)
    path.write_text(f"var data = {payload}\n")


def write_history_json(directory, data):
    directory.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    payload = json.dumps(data, indent=2, ensure_ascii=True)
    (directory / f"{stamp}.json").write_text(payload + "\n")


def update_network(data, key, csv_path, last_update, data_file=None):
    stats = stats_from_csv(csv_path)
    entry = data.get(key, {})
    entry.update(stats)
    entry["lastUpdate"] = last_update
    if data_file is not None:
        entry["dataFile"] = data_file
    data[key] = entry


def update_period_trend(data, key, period, current_csv, previous_csv):
    current = stats_from_csv(current_csv)
    previous = stats_from_csv(previous_csv)
    diff = round(current["shannon"] - previous["shannon"], 4)
    data.setdefault("trends", {}).setdefault(key, {}).setdefault(period, {})["shannon"] = diff

    current_terms = shannon_terms_by_host(current_csv)
    previous_terms = shannon_terms_by_host(previous_csv)
    current_counts = user_counts_by_host(current_csv)
    previous_counts = user_counts_by_host(previous_csv)
    all_hosts = set(current_terms) | set(previous_terms)
    diffs = []
    for host in sorted(all_hosts):
        term_diff = current_terms.get(host, 0.0) - previous_terms.get(host, 0.0)
        user_diff = current_counts.get(host, 0) - previous_counts.get(host, 0)
        diffs.append((host, term_diff, user_diff))
    top_increase = [
        {
            "host": host,
            "change": round(term_diff, 6),
            "user_change": user_diff,
        }
        for host, term_diff, user_diff in sorted(diffs, key=lambda item: item[1], reverse=True)
        if term_diff > 0
    ][:10]
    top_decrease = [
        {
            "host": host,
            "change": round(term_diff, 6),
            "user_change": user_diff,
        }
        for host, term_diff, user_diff in sorted(diffs, key=lambda item: item[1])
        if term_diff < 0
    ][:10]
    data.setdefault("trends", {}).setdefault(key, {}).setdefault(period, {})["shannon_contrib"] = {
        "increase": top_increase,
        "decrease": top_decrease,
    }


def shannon_terms_by_host(csv_path):
    combined = combined_counts(csv_path)
    if not combined:
        return {}
    total = sum(item["count"] for item in combined)
    if total == 0:
        return {}
    counts = [item["count"] for item in combined]
    terms = _shannon(counts, return_terms=True)
    return {item["domain"]: term for item, term in zip(combined, terms)}


def user_counts_by_host(csv_path):
    combined = combined_counts(csv_path)
    return {item["domain"]: item["count"] for item in combined}


def combined_counts(csv_path):
    rows = load_csv_rows(csv_path)
    rows = filter_rows(rows)
    extracted = [extract_domain_counts(row) for row in rows]
    return combine_rows(extracted)


def load_csv_rows(csv_path):
    with open(csv_path, newline="") as handle:
        return list(csv.DictReader(handle))




def main():
    data = load_data_js(DATA_JS_PATH)

    fedi_csv = find_newest_file(REPO_ROOT / "data" / "fedi-mau")
    fedi_software_csv = find_newest_file(REPO_ROOT / "data" / "fedi-software")
    at_csv = find_newest_file(REPO_ROOT / "data" / "at-mau")
    git_csv = find_newest_file(REPO_ROOT / "data" / "git")
    bsky_verifiers_csv = find_newest_file(REPO_ROOT / "data" / "bsky-verifiers")

    fedi_dt = parse_timestamp_from_name(fedi_csv.name)
    fedi_software_dt = parse_timestamp_from_name(fedi_software_csv.name)
    at_dt = parse_timestamp_from_name(at_csv.name)
    git_dt = parse_timestamp_from_name(git_csv.name)
    bsky_verifiers_dt = parse_timestamp_from_name(bsky_verifiers_csv.name)
    if (
        fedi_dt is None
        or fedi_software_dt is None
        or at_dt is None
        or git_dt is None
        or bsky_verifiers_dt is None
    ):
        raise RuntimeError("Unable to parse timestamps for latest files")

    update_network(
        data,
        "fedi",
        fedi_csv,
        fedi_dt.strftime("%m-%d-%Y"),
        data_file=str(fedi_csv.relative_to(REPO_ROOT)),
    )
    update_network(
        data,
        "fedi_software",
        fedi_software_csv,
        fedi_software_dt.strftime("%m-%d-%Y"),
        data_file=str(fedi_software_csv.relative_to(REPO_ROOT)),
    )
    update_network(
        data,
        "at",
        at_csv,
        at_dt.strftime("%m-%d-%Y"),
        data_file=str(at_csv.relative_to(REPO_ROOT)),
    )
    update_network(
        data,
        "git",
        git_csv,
        git_dt.strftime("%m-%d-%Y"),
        data_file=str(git_csv.relative_to(REPO_ROOT)),
    )
    update_network(
        data,
        "bsky_verifiers",
        bsky_verifiers_csv,
        bsky_verifiers_dt.strftime("%m-%d-%Y"),
        data_file=str(bsky_verifiers_csv.relative_to(REPO_ROOT)),
    )

    week_target = datetime.now(timezone.utc) - timedelta(days=7)
    month_target = datetime.now(timezone.utc) - timedelta(days=30)
    update_period_trend(
        data,
        "fedi",
        "weekly",
        fedi_csv,
        find_closest_to(REPO_ROOT / "data" / "fedi-mau", week_target),
    )
    update_period_trend(
        data,
        "fedi_software",
        "weekly",
        fedi_software_csv,
        find_closest_to(REPO_ROOT / "data" / "fedi-software", week_target),
    )
    update_period_trend(
        data,
        "at",
        "weekly",
        at_csv,
        find_closest_to(REPO_ROOT / "data" / "at-mau", week_target),
    )
    update_period_trend(
        data,
        "git",
        "weekly",
        git_csv,
        find_closest_to(REPO_ROOT / "data" / "git", week_target),
    )
    update_period_trend(
        data,
        "fedi",
        "monthly",
        fedi_csv,
        find_closest_to(REPO_ROOT / "data" / "fedi-mau", month_target),
    )
    update_period_trend(
        data,
        "fedi_software",
        "monthly",
        fedi_software_csv,
        find_closest_to(REPO_ROOT / "data" / "fedi-software", month_target),
    )
    update_period_trend(
        data,
        "at",
        "monthly",
        at_csv,
        find_closest_to(REPO_ROOT / "data" / "at-mau", month_target),
    )
    update_period_trend(
        data,
        "git",
        "monthly",
        git_csv,
        find_closest_to(REPO_ROOT / "data" / "git", month_target),
    )

    write_data_js(DATA_JS_PATH, data)
    write_history_json(DATA_HISTORY_DIR, data)


if __name__ == "__main__":
    main()
