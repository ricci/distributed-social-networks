#!/usr/bin/env python3
import argparse
import csv
import json
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional


REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIRS = {
    "at": REPO_ROOT / "data" / "at-mau",
    "fedi": REPO_ROOT / "data" / "fedi-mau",
}

TIMESTAMP_RE = re.compile(
    r"(?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?P<tz>Z|[+-]\d{2}:?\d{2})?"
)


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


def find_newest_file(directory: Path) -> Path:
    candidates = []
    for path in directory.iterdir():
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


def find_closest_to(directory: Path, target_dt: datetime) -> Path:
    best_path = None
    best_diff = None
    for path in directory.iterdir():
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


def load_config(path: Path) -> Dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as f:
            config = json.load(f)
    except Exception as exc:
        raise RuntimeError(f"Unable to load config {path}: {exc}") from exc
    if not isinstance(config, dict) or "rules" not in config:
        raise RuntimeError(f"Config {path} must contain a top-level 'rules' list")
    return config


def parse_int(value: str) -> Optional[int]:
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


def load_snapshot(network: str, path: Path) -> Dict[str, int]:
    data: Dict[str, int] = {}
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if network == "at":
            for row in reader:
                host = row.get("domain", "")
                mau = parse_int(row.get("mau"))
                if host.endswith(".bsky.network") or host == "bsky.network":
                    continue
                if not host or mau is None or mau < 0:
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
    return data


def build_ranks(data: Dict[str, int]) -> Dict[str, int]:
    ordered = sorted(data.items(), key=lambda item: (-item[1], item[0]))
    ranks: Dict[str, int] = {}
    for idx, (host, _mau) in enumerate(ordered, start=1):
        ranks[host] = idx
    return ranks


def mau_percent_jump(prev_mau: int, cur_mau: int) -> Optional[float]:
    if prev_mau <= 0:
        return None
    delta = cur_mau - prev_mau
    if delta <= 0:
        return None
    return (delta / prev_mau) * 100.0


def ensure_output_path(output: str, network: str) -> Path:
    out_path = Path(output)
    if out_path.suffix.lower() == ".json":
        out_path.parent.mkdir(parents=True, exist_ok=True)
        return out_path

    if not out_path.exists():
        out_path.mkdir(parents=True, exist_ok=True)

    if out_path.exists() and out_path.is_dir():
        stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        return out_path / f"{network}-trends-{stamp}.json"

    out_path.parent.mkdir(parents=True, exist_ok=True)
    return out_path


def rule_applies(rule: Dict[str, Any], network: str) -> bool:
    networks = rule.get("networks")
    if networks is None:
        return True
    if isinstance(networks, list):
        return network in networks
    return False


def evaluate_rules(
    network: str,
    current_path: Path,
    config: Dict[str, Any],
) -> Dict[str, Any]:
    now = datetime.now(timezone.utc)
    current_data = load_snapshot(network, current_path)
    current_ranks = build_ranks(current_data)

    results: Dict[str, Dict[str, Any]] = {}
    rule_outputs: Dict[str, Any] = {}
    lookback_cache: Dict[int, Tuple[Path, Dict[str, int], Dict[str, int]]] = {}

    for rule in config.get("rules", []):
        if not rule_applies(rule, network):
            continue

        rule_name = rule.get("name") or rule.get("type")
        rule_type = rule.get("type")
        if not rule_name or not rule_type:
            continue

        lookback_days = rule.get("lookback_days")
        if not isinstance(lookback_days, int) or lookback_days <= 0:
            raise RuntimeError(f"Rule '{rule_name}' must set lookback_days > 0")

        if lookback_days not in lookback_cache:
            target = now - timedelta(days=lookback_days)
            prev_path = find_closest_to(DATA_DIRS[network], target)
            prev_data = load_snapshot(network, prev_path)
            prev_ranks = build_ranks(prev_data)
            lookback_cache[lookback_days] = (prev_path, prev_data, prev_ranks)

        prev_path, prev_data, prev_ranks = lookback_cache[lookback_days]

        if rule_type == "absolute_jump":
            min_delta = rule.get("min_delta")
            min_current_mau = rule.get("min_current_mau")
            min_delta_pct = rule.get("min_delta_pct")
            if not isinstance(min_delta, int):
                raise RuntimeError(f"Rule '{rule_name}' must set min_delta")
            for host, cur_mau in current_data.items():
                if host not in prev_data:
                    continue
                prev_mau = prev_data[host]
                delta = cur_mau - prev_mau
                if delta < min_delta:
                    continue
                if isinstance(min_delta_pct, int):
                    if prev_mau <= 0:
                        continue
                    delta_pct = (delta / prev_mau) * 100.0
                    if delta_pct < min_delta_pct:
                        continue
                if isinstance(min_current_mau, int) and cur_mau < min_current_mau:
                    continue
                entry = results.setdefault(host, {"rules": {}})
                entry["rules"][rule_name] = {
                    "type": rule_type,
                    "lookback_days": lookback_days,
                    "previous_file": str(prev_path.relative_to(REPO_ROOT)),
                    "previous_mau": prev_mau,
                    "current_mau": cur_mau,
                    "delta": delta,
                }
        elif rule_type == "rank_jump":
            min_rank_jump = rule.get("min_rank_jump")
            min_current_rank = rule.get("min_current_rank")
            if not isinstance(min_rank_jump, int):
                raise RuntimeError(f"Rule '{rule_name}' must set min_rank_jump")
            for host, cur_rank in current_ranks.items():
                prev_rank = prev_ranks.get(host)
                if prev_rank is None:
                    continue
                rank_jump = prev_rank - cur_rank
                if rank_jump < min_rank_jump:
                    continue
                if isinstance(min_current_rank, int) and cur_rank > min_current_rank:
                    continue
                entry = results.setdefault(host, {"rules": {}})
                entry["rules"][rule_name] = {
                    "type": rule_type,
                    "lookback_days": lookback_days,
                    "previous_file": str(prev_path.relative_to(REPO_ROOT)),
                    "previous_rank": prev_rank,
                    "current_rank": cur_rank,
                    "rank_jump": rank_jump,
                    "previous_mau": prev_data.get(host),
                    "current_mau": current_data.get(host),
                }
        elif rule_type == "new_host":
            min_current_mau = rule.get("min_current_mau")
            max_results = rule.get("max_results")
            candidates = []
            for host, cur_mau in current_data.items():
                if host in prev_data:
                    continue
                if isinstance(min_current_mau, int) and cur_mau < min_current_mau:
                    continue
                candidates.append((cur_mau, host))
            candidates.sort(key=lambda item: (-item[0], item[1]))
            if isinstance(max_results, int) and max_results > 0:
                candidates = candidates[:max_results]
            for cur_mau, host in candidates:
                entry = results.setdefault(host, {"rules": {}})
                entry["rules"][rule_name] = {
                    "type": rule_type,
                    "lookback_days": lookback_days,
                    "previous_file": str(prev_path.relative_to(REPO_ROOT)),
                    "current_rank": current_ranks.get(host),
                    "current_mau": cur_mau,
                }
        elif rule_type == "biggest_rank_jump":
            min_current_rank = rule.get("min_current_rank")
            best_host = None
            best_jump = None
            for host, cur_rank in current_ranks.items():
                prev_rank = prev_ranks.get(host)
                if prev_rank is None:
                    continue
                rank_jump = prev_rank - cur_rank
                if rank_jump <= 0:
                    continue
                if isinstance(min_current_rank, int) and cur_rank > min_current_rank:
                    continue
                if best_jump is None or rank_jump > best_jump:
                    best_jump = rank_jump
                    best_host = host
            if best_host is not None:
                entry = results.setdefault(best_host, {"rules": {}})
                entry["rules"][rule_name] = {
                    "type": rule_type,
                    "lookback_days": lookback_days,
                    "previous_file": str(prev_path.relative_to(REPO_ROOT)),
                    "previous_rank": prev_ranks.get(best_host),
                    "current_rank": current_ranks.get(best_host),
                    "rank_jump": best_jump,
                    "previous_mau": prev_data.get(best_host),
                    "current_mau": current_data.get(best_host),
                }
        elif rule_type == "biggest_mau_percent_jump":
            min_previous_mau = rule.get("min_previous_mau")
            best_host = None
            best_pct = None
            for host, cur_mau in current_data.items():
                prev_mau = prev_data.get(host)
                if prev_mau is None:
                    continue
                if isinstance(min_previous_mau, int) and prev_mau < min_previous_mau:
                    continue
                pct = mau_percent_jump(prev_mau, cur_mau)
                if pct is None:
                    continue
                if best_pct is None or pct > best_pct:
                    best_pct = pct
                    best_host = host
            if best_host is not None:
                entry = results.setdefault(best_host, {"rules": {}})
                entry["rules"][rule_name] = {
                    "type": rule_type,
                    "lookback_days": lookback_days,
                    "previous_file": str(prev_path.relative_to(REPO_ROOT)),
                    "previous_mau": prev_data.get(best_host),
                    "current_mau": current_data.get(best_host),
                    "percent_jump": best_pct,
                }
        elif rule_type == "biggest_abs_mau_jump":
            best_host = None
            best_delta = None
            for host, cur_mau in current_data.items():
                prev_mau = prev_data.get(host)
                if prev_mau is None:
                    continue
                delta = cur_mau - prev_mau
                if delta <= 0:
                    continue
                if best_delta is None or delta > best_delta:
                    best_delta = delta
                    best_host = host
            if best_host is not None:
                entry = results.setdefault(best_host, {"rules": {}})
                entry["rules"][rule_name] = {
                    "type": rule_type,
                    "lookback_days": lookback_days,
                    "previous_file": str(prev_path.relative_to(REPO_ROOT)),
                    "previous_mau": prev_data.get(best_host),
                    "current_mau": current_data.get(best_host),
                    "delta": best_delta,
                }
        else:
            raise RuntimeError(f"Unknown rule type '{rule_type}' in '{rule_name}'")

        rule_outputs[rule_name] = rule

    return {
        "network": network,
        "generated_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "current_file": str(current_path.relative_to(REPO_ROOT)),
        "rules": rule_outputs,
        "results": results,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Find host-level trends based on configurable rules.",
    )
    parser.add_argument(
        "--network",
        choices=sorted(DATA_DIRS.keys()),
        required=True,
        help="Network to analyze (at or fedi).",
    )
    parser.add_argument(
        "--config",
        default=str(REPO_ROOT / "data-processing" / "trends-config.json"),
        help="Path to JSON config file defining rules.",
    )
    parser.add_argument(
        "--output",
        default=str(REPO_ROOT / "data" / "trends"),
        help="Output file or directory (default: data/trends).",
    )
    parser.add_argument(
        "--current",
        default=None,
        help="Override the current snapshot file path.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(Path(args.config))
    network = args.network

    if args.current:
        current_path = Path(args.current)
        if not current_path.is_file():
            raise RuntimeError(f"Current snapshot '{current_path}' not found")
    else:
        current_path = find_newest_file(DATA_DIRS[network])

    output_path = ensure_output_path(args.output, network)
    payload = evaluate_rules(network, current_path, config)
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n")
    print(f"Wrote trends to {output_path}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
