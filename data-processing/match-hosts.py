#!/usr/bin/env python3
import argparse
import csv
import json
import re
import sys
from bisect import bisect_left
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_AT_DIR = REPO_ROOT / "data" / "at-mau"
DEFAULT_FEDI_DIR = REPO_ROOT / "data" / "fedi-mau"

TIMESTAMP_RE = re.compile(
    r"(?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?P<tz>Z|[+-]\d{2}:?\d{2})?"
)


@dataclass
class Trend:
    delta: int
    pct: Optional[float]


def parse_timestamp_from_name(name: str) -> Optional[datetime]:
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


def parse_int(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    value = value.strip()
    if value == "":
        return None
    try:
        return int(value)
    except ValueError:
        return None


def iter_timestamped_files(path: Path) -> List[Tuple[datetime, Path]]:
    files: List[Tuple[datetime, Path]] = []
    for item in path.iterdir():
        if not item.is_file():
            continue
        ts = parse_timestamp_from_name(item.name)
        if ts is None:
            continue
        files.append((ts, item))
    files.sort(key=lambda item: item[0])
    return files


def load_snapshot(network: str, path: Path, combine_bsky: bool) -> Dict[str, int]:
    data: Dict[str, int] = {}
    bsky_total = 0
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if network == "at":
            for row in reader:
                host = row.get("domain", "")
                mau = parse_int(row.get("mau"))
                if not host or mau is None or mau < 0:
                    continue
                if combine_bsky and (
                    host == "bsky.network" or host.endswith(".bsky.network")
                ):
                    bsky_total += mau
                    continue
                data[host] = mau
        elif network == "fedi":
            for row in reader:
                host = row.get("hostname", "")
                mau = parse_int(row.get("active_month"))
                if not host or mau is None or mau < 0:
                    continue
                data[host] = mau
        else:
            raise RuntimeError(f"Unknown network '{network}'")
    if combine_bsky and bsky_total > 0:
        data["bsky.network"] = bsky_total
    return data


def load_snapshots(
    network: str, current_path: Path, trend_path: Path
) -> Tuple[Path, Dict[str, int], List[Tuple[datetime, Dict[str, int]]]]:
    if current_path.is_dir():
        files = iter_timestamped_files(current_path)
        if not files:
            raise RuntimeError(f"No timestamped files found in {current_path}")
        current_file = files[-1][1]
    else:
        current_file = current_path

    if trend_path.is_dir():
        trend_files = iter_timestamped_files(trend_path)
        if not trend_files:
            trend_files = []
    else:
        trend_files = []

    snapshots: List[Tuple[datetime, Dict[str, int]]] = []
    snapshot_by_path: Dict[Path, Dict[str, int]] = {}
    for ts, path in trend_files:
        data = load_snapshot(network, path, combine_bsky=True)
        snapshots.append((ts, data))
        snapshot_by_path[path] = data

    if current_file in snapshot_by_path:
        current_data = snapshot_by_path[current_file]
    else:
        current_data = load_snapshot(network, current_file, combine_bsky=True)

    return current_file, current_data, snapshots


def build_ranks(data: Dict[str, int]) -> Dict[str, int]:
    ordered = sorted(data.items(), key=lambda item: (-item[1], item[0]))
    ranks: Dict[str, int] = {}
    for idx, (host, _mau) in enumerate(ordered, start=1):
        ranks[host] = idx
    return ranks


def build_rank_index(ranks: Dict[str, int]) -> List[Optional[str]]:
    index: List[Optional[str]] = [None] * len(ranks)
    for host, rank in ranks.items():
        if 1 <= rank <= len(index):
            index[rank - 1] = host
    return index


def build_trends(snapshots: List[Tuple[datetime, Dict[str, int]]]) -> Dict[str, Trend]:
    if len(snapshots) < 2:
        return {}
    snapshots.sort(key=lambda item: item[0])
    first_seen: Dict[str, int] = {}
    last_seen: Dict[str, int] = {}
    for _ts, data in snapshots:
        for host, mau in data.items():
            if host not in first_seen:
                first_seen[host] = mau
            last_seen[host] = mau
    trends: Dict[str, Trend] = {}
    for host, last in last_seen.items():
        first = first_seen.get(host)
        if first is None:
            continue
        delta = last - first
        pct: Optional[float]
        if first <= 0:
            pct = None
        else:
            pct = (delta / first) * 100.0
        trends[host] = Trend(delta=delta, pct=pct)
    return trends


def build_mau_index(data: Dict[str, int]) -> Tuple[List[float], List[str]]:
    ordered = sorted(((mau, host) for host, mau in data.items()), key=lambda item: (item[0], item[1]))
    return [float(mau) for mau, _host in ordered], [host for _mau, host in ordered]


def find_closest_by_value(values: List[float], hosts: List[str], target: float) -> Optional[str]:
    if not values:
        return None
    idx = bisect_left(values, target)
    candidates: List[Tuple[int, str]] = []
    if idx < len(values):
        candidates.append((abs(values[idx] - target), hosts[idx]))
    if idx > 0:
        candidates.append((abs(values[idx - 1] - target), hosts[idx - 1]))
    candidates.sort(key=lambda item: (item[0], item[1]))
    return candidates[0][1]


def build_trend_index(trends: Dict[str, Trend]) -> Tuple[List[float], List[str]]:
    ordered = sorted(
        ((trend.pct, host) for host, trend in trends.items() if trend.pct is not None),
        key=lambda item: (item[0], item[1]),
    )
    return [pct for pct, _host in ordered], [host for _pct, host in ordered]


def score_mau(source: int, match: int) -> float:
    denom = max(source, 1)
    return abs(source - match) / denom


def score_rank(source: int, match: int, total_source: int, total_match: int) -> float:
    denom = max(total_source, total_match, 1)
    return abs(source - match) / denom


def score_trend(source: float, match: float) -> float:
    denom = max(abs(source), abs(match), 1.0)
    return abs(source - match) / denom


def pick_best_match(
    source_host: str,
    source_mau: int,
    source_rank: int,
    source_trend: Optional[Trend],
    other_data: Dict[str, int],
    other_ranks: Dict[str, int],
    other_rank_index: List[Optional[str]],
    other_trends: Dict[str, Trend],
    other_mau_values: List[float],
    other_mau_hosts: List[str],
    other_trend_values: List[float],
    other_trend_hosts: List[str],
    total_source: int,
    total_other: int,
) -> Dict[str, object]:
    candidates: List[Tuple[str, str, float]] = []
    weights = {"mau": 0.2, "rank": 1.0, "trend": 1.0}
    rank_floor = 0.05

    mau_match = find_closest_by_value(other_mau_values, other_mau_hosts, source_mau)
    if mau_match is not None:
        score = score_mau(source_mau, other_data[mau_match]) * weights["mau"]
        candidates.append(("mau", mau_match, score))

    if total_other > 0:
        target_rank = min(max(source_rank, 1), total_other)
        rank_match = other_rank_index[target_rank - 1] if other_rank_index else None
        if rank_match is not None:
            score = score_rank(
                source_rank, other_ranks[rank_match], total_source, total_other
            )
            score = (score + rank_floor) * weights["rank"]
            candidates.append(("rank", rank_match, score))

    if source_trend is not None and source_trend.pct is not None:
        trend_match = find_closest_by_value(
            other_trend_values, other_trend_hosts, source_trend.pct
        )
        if trend_match is not None:
            match_trend = other_trends[trend_match].pct
            if match_trend is not None:
                score = score_trend(source_trend.pct, match_trend) * weights["trend"]
                candidates.append(("trend", trend_match, score))

    if not candidates:
        return {
            "match": None,
            "rule": None,
            "rule_value": None,
            "differences": None,
        }

    candidates.sort(key=lambda item: (item[2], ["mau", "rank", "trend"].index(item[0])))
    rule, match_host, score = candidates[0]

    match_mau = other_data.get(match_host)
    match_rank = other_ranks.get(match_host)
    match_trend = other_trends.get(match_host)

    rule_value: Dict[str, object] = {"score": score}
    if rule == "mau":
        rule_value.update(
            {
                "source_mau": source_mau,
                "match_mau": match_mau,
                "delta": abs(source_mau - match_mau),
            }
        )
    elif rule == "rank":
        rule_value.update(
            {
                "source_rank": source_rank,
                "match_rank": match_rank,
                "delta": abs(source_rank - match_rank),
            }
        )
    elif rule == "trend":
        rule_value.update(
            {
                "source_trend_pct": source_trend.pct if source_trend else None,
                "match_trend_pct": match_trend.pct if match_trend else None,
                "delta": None
                if source_trend is None
                or source_trend.pct is None
                or match_trend is None
                or match_trend.pct is None
                else abs(source_trend.pct - match_trend.pct),
            }
        )

    differences = {
        "mau_diff": None if match_mau is None else abs(source_mau - match_mau),
        "rank_diff": None
        if match_rank is None
        else abs(source_rank - match_rank),
        "trend_pct_diff": None
        if source_trend is None
        or source_trend.pct is None
        or match_trend is None
        or match_trend.pct is None
        else abs(source_trend.pct - match_trend.pct),
        "trend_delta_diff": None
        if source_trend is None
        or match_trend is None
        else abs(source_trend.delta - match_trend.delta),
    }

    return {
        "match": match_host,
        "rule": rule,
        "rule_value": rule_value,
        "differences": differences,
    }


def build_matches(
    source_data: Dict[str, int],
    source_ranks: Dict[str, int],
    source_trends: Dict[str, Trend],
    other_data: Dict[str, int],
    other_ranks: Dict[str, int],
    other_trends: Dict[str, Trend],
) -> Dict[str, Dict[str, object]]:
    other_mau_values, other_mau_hosts = build_mau_index(other_data)
    other_trend_values, other_trend_hosts = build_trend_index(other_trends)
    other_rank_index = build_rank_index(other_ranks)
    total_source = len(source_ranks)
    total_other = len(other_ranks)

    matches: Dict[str, Dict[str, object]] = {}
    for host, mau in source_data.items():
        match_info = pick_best_match(
            source_host=host,
            source_mau=mau,
            source_rank=source_ranks[host],
            source_trend=source_trends.get(host),
            other_data=other_data,
            other_ranks=other_ranks,
            other_rank_index=other_rank_index,
            other_trends=other_trends,
            other_mau_values=other_mau_values,
            other_mau_hosts=other_mau_hosts,
            other_trend_values=other_trend_values,
            other_trend_hosts=other_trend_hosts,
            total_source=total_source,
            total_other=total_other,
        )
        matches[host] = match_info
    return matches


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Match hosts between at and fedi CSVs based on similarity.",
    )
    parser.add_argument(
        "--at",
        default=str(DEFAULT_AT_DIR),
        help="AT CSV file or directory (default: data/at-mau).",
    )
    parser.add_argument(
        "--fedi",
        default=str(DEFAULT_FEDI_DIR),
        help="Fedi CSV file or directory (default: data/fedi-mau).",
    )
    parser.add_argument(
        "--output",
        default=str(REPO_ROOT / "data" / "host-similarities.json"),
        help="Output JSON file path.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    at_path = Path(args.at)
    fedi_path = Path(args.fedi)

    if not at_path.exists():
        raise RuntimeError(f"AT path does not exist: {at_path}")
    if not fedi_path.exists():
        raise RuntimeError(f"Fedi path does not exist: {fedi_path}")

    _at_current_file, at_current, at_snapshots = load_snapshots(
        "at", at_path, at_path if at_path.is_dir() else at_path.parent
    )
    _fedi_current_file, fedi_current, fedi_snapshots = load_snapshots(
        "fedi", fedi_path, fedi_path if fedi_path.is_dir() else fedi_path.parent
    )

    at_ranks = build_ranks(at_current)
    fedi_ranks = build_ranks(fedi_current)
    at_trends = build_trends(at_snapshots)
    fedi_trends = build_trends(fedi_snapshots)

    at_matches = build_matches(
        source_data=at_current,
        source_ranks=at_ranks,
        source_trends=at_trends,
        other_data=fedi_current,
        other_ranks=fedi_ranks,
        other_trends=fedi_trends,
    )
    fedi_matches = build_matches(
        source_data=fedi_current,
        source_ranks=fedi_ranks,
        source_trends=fedi_trends,
        other_data=at_current,
        other_ranks=at_ranks,
        other_trends=at_trends,
    )

    output = {"at": at_matches, "fedi": fedi_matches}

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, sort_keys=True)
        f.write("\n")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
