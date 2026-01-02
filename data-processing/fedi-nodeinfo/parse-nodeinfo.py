#!/usr/bin/env python3
import sys
import os
import json
import csv
from typing import List, Optional, Tuple, Dict, Set

import yaml
def _coerce_int(value: object) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None

QUIRKS_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "nodeinfo-quirks.yaml")

def _extract_metadata_non_activitypub_users(nodeinfo_wrapper: dict) -> Optional[int]:
    ni = nodeinfo_wrapper.get("nodeinfo") or {}
    metadata = ni.get("metadata") or {}
    if not isinstance(metadata, dict):
        return None
    users = metadata.get("users")
    if isinstance(users, dict):
        total = 0
        for key, value in users.items():
            if str(key).lower() == "activitypub":
                continue
            count = _coerce_int(value)
            if count is not None:
                total += count
        return total
    stats = metadata.get("stats")
    if isinstance(stats, dict):
        total = 0
        for key, value in stats.items():
            if str(key).lower() == "activitypub" or not isinstance(value, dict):
                continue
            count = _coerce_int(value.get("users"))
            if count is not None:
                total += count
        return total
    return None

def _extract_local_posts(nodeinfo_wrapper: dict) -> Optional[int]:
    ni = nodeinfo_wrapper.get("nodeinfo") or {}
    usage = ni.get("usage") or {}
    if not isinstance(usage, dict):
        return None
    return _coerce_int(usage.get("localPosts"))

def _extract_local_comments(nodeinfo_wrapper: dict) -> Optional[int]:
    ni = nodeinfo_wrapper.get("nodeinfo") or {}
    usage = ni.get("usage") or {}
    if not isinstance(usage, dict):
        return None
    return _coerce_int(usage.get("localComments"))

def _load_quirks_config(path: str) -> Tuple[Dict[str, Set[str]], Set[str], Set[str], Set[str]]:
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise ValueError("quirks config must be a YAML mapping")

    quirks_by_software: Dict[str, Set[str]] = {}
    known_software: Set[str] = set()

    def add_quirk(name: str, quirk: str) -> None:
        key = str(name).lower()
        qkey = str(quirk).lower()
        quirks_by_software.setdefault(key, set()).add(qkey)

    software_section = data.get("software", {})
    if isinstance(software_section, dict):
        for name, quirks in software_section.items():
            if quirks is None:
                quirks_list = []
            elif isinstance(quirks, str):
                quirks_list = [quirks]
            elif isinstance(quirks, list):
                quirks_list = quirks
            else:
                continue
            if not quirks_list or "none" in [str(q).lower() for q in quirks_list]:
                known_software.add(str(name).lower())
            for quirk in quirks_list:
                if str(quirk).lower() == "none":
                    continue
                add_quirk(name, quirk)

    quirks_section = data.get("quirks", {})
    if isinstance(quirks_section, dict):
        for quirk, names in quirks_section.items():
            if isinstance(names, str):
                names_list = [names]
            elif isinstance(names, list):
                names_list = names
            else:
                continue
            if str(quirk).lower() == "none":
                for name in names_list:
                    known_software.add(str(name).lower())
                continue
            for name in names_list:
                add_quirk(name, quirk)

    forks_section = data.get("forks", {})
    misskey_forks: Set[str] = set()
    akkoma_forks: Set[str] = set()
    if isinstance(forks_section, dict):
        misskey = forks_section.get("misskey", [])
        if isinstance(misskey, list):
            misskey_forks = {str(name).lower() for name in misskey}
        akkoma = forks_section.get("akkoma", [])
        if isinstance(akkoma, list):
            akkoma_forks = {str(name).lower() for name in akkoma}

    return quirks_by_software, known_software, misskey_forks, akkoma_forks

def _get_quirks(
    software_name: Optional[str],
    quirks_by_software: Dict[str, Set[str]],
    misskey_forks: Set[str],
    akkoma_forks: Set[str],
) -> Tuple[str, Dict[str, bool]]:
    software_key = (software_name or "").lower()
    quirks = {q: True for q in quirks_by_software.get(software_key, set())}
    if software_key in misskey_forks:
        quirks["no_monthly_users"] = True
    if software_key in akkoma_forks:
        quirks["trust_monthly_gt_total"] = True
    return software_key, quirks

def extract_fields(nodeinfo_wrapper: dict):
    hostname = nodeinfo_wrapper.get("hostname", "")

    ni = nodeinfo_wrapper.get("nodeinfo") or {}

    software_name: Optional[str] = None
    users_total: Optional[int] = None
    active_month: Optional[int] = None
    protocols: Optional[List[str]] = None

    software = ni.get("software") or {}
    if isinstance(software, dict):
        software_name = software.get("name")

    usage = ni.get("usage") or {}
    if isinstance(usage, dict):
        users = usage.get("users") or {}
        if isinstance(users, dict):
            users_total = _coerce_int(users.get("total"))
            active_month = _coerce_int(users.get("activeMonth"))

    if isinstance(ni.get("protocols"), list):
        protocols = [str(p) for p in ni["protocols"]]
    else:
        metadata = ni.get("metadata")
        if isinstance(metadata, dict) and isinstance(metadata.get("protocols"), list):
            protocols = [str(p) for p in metadata["protocols"]]

    protocols_str = ";".join(protocols) if protocols else ""

    return hostname, software_name, users_total, active_month, protocols, protocols_str

def main() -> None:
    import datetime

    if len(sys.argv) < 3 or len(sys.argv) > 4:
        print(f"Usage: {sys.argv[0]} nodeinfo_dir output.csv [max_age_days=30]", file=sys.stderr)
        sys.exit(1)

    nodeinfo_dir = sys.argv[1]
    output_csv = sys.argv[2]
    max_age_days = int(sys.argv[3]) if len(sys.argv) == 4 else 30

    cutoff = datetime.timedelta(days=max_age_days)
    # Make 'now' timezone-aware (UTC)
    now = datetime.datetime.now(datetime.timezone.utc)

    hostname_dirs = [
        os.path.join(nodeinfo_dir, d)
        for d in os.listdir(nodeinfo_dir)
        if os.path.isdir(os.path.join(nodeinfo_dir, d))
    ]

    selected_files = []

    for hdir in hostname_dirs:
        hostname = os.path.basename(hdir)
        candidates = []

        for fn in os.listdir(hdir):
            if not fn.endswith(".json"):
                continue
            path = os.path.join(hdir, fn)
            stem = fn[:-5]  # strip ".json"
            try:
                ts = datetime.datetime.fromisoformat(stem.replace("Z", "+00:00"))
                # Ensure parsed timestamp is also aware UTC
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=datetime.timezone.utc)
                else:
                    ts = ts.astimezone(datetime.timezone.utc)
            except Exception:
                continue
            candidates.append((ts, path))

        if not candidates:
            continue

        candidates_recent = [c for c in candidates if now - c[0] <= cutoff]
        if not candidates_recent:
            continue

        ts_newest, path_newest = max(candidates_recent, key=lambda x: x[0])
        ts_oldest, path_oldest = min(candidates_recent, key=lambda x: x[0])

        selected_files.append({
            "newest": path_newest,
            "oldest": path_oldest,
        })

    selected_files.sort(key=lambda item: item["newest"])

    quirks_by_software, known_software, misskey_forks, akkoma_forks = _load_quirks_config(
        QUIRKS_CONFIG_PATH
    )
    configured_software = set(known_software) | set(quirks_by_software.keys()) | set(misskey_forks) | set(akkoma_forks)

    unknown_software_report = {}
    quirk_report = {}

    def bump_quirk(quirk: str) -> None:
        quirk_report[quirk] = quirk_report.get(quirk, 0) + 1

    with open(output_csv, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["hostname", "software", "users_total", "active_month", "protocols"])

        for item in selected_files:
            path = item["newest"]
            try:
                with open(path, "r", encoding="utf-8") as jf:
                    wrapper = json.load(jf)
            except Exception as e:
                print(f"# Skipping {path}: {e}", file=sys.stderr)
                continue

            hostname, software_name, users_total, active_month, protocols, protocols_str = extract_fields(wrapper)

            if not protocols or not any(str(p).lower() == "activitypub" for p in protocols):
                continue

            software_key, quirks = _get_quirks(
                software_name,
                quirks_by_software,
                misskey_forks,
                akkoma_forks,
            )

            if quirks.get("no_monthly_users"):
                bump_quirk("no_monthly_users_skip")
                continue
            if quirks.get("relay"):
                bump_quirk("relay_skip")
                continue
            if quirks.get("use_metadata_non_activitypub_users"):
                bridge_users = _extract_metadata_non_activitypub_users(wrapper)
                users_total = bridge_users
                active_month = bridge_users
                bump_quirk("use_metadata_non_activitypub_users")
            if quirks.get("detect_activity_from_posts"):
                try:
                    with open(item["oldest"], "r", encoding="utf-8") as jf:
                        oldest_wrapper = json.load(jf)
                except Exception:
                    continue
                oldest_posts = _extract_local_posts(oldest_wrapper)
                newest_posts = _extract_local_posts(wrapper)
                if oldest_posts is None or newest_posts is None:
                    continue
                if newest_posts > oldest_posts:
                    active_month = users_total
                    bump_quirk("detect_activity_from_posts_active")
                else:
                    bump_quirk("detect_activity_from_posts_inactive")
                    continue
            if quirks.get("detect_activity_from_posts_and_comments"):
                try:
                    with open(item["oldest"], "r", encoding="utf-8") as jf:
                        oldest_wrapper = json.load(jf)
                except Exception:
                    continue
                oldest_posts = _extract_local_posts(oldest_wrapper)
                newest_posts = _extract_local_posts(wrapper)
                oldest_comments = _extract_local_comments(oldest_wrapper)
                newest_comments = _extract_local_comments(wrapper)
                if (
                    oldest_posts is None
                    or newest_posts is None
                    or oldest_comments is None
                    or newest_comments is None
                ):
                    continue
                if newest_posts > oldest_posts or newest_comments > oldest_comments:
                    active_month = users_total
                    bump_quirk("detect_activity_from_posts_and_comments_active")
                else:
                    bump_quirk("detect_activity_from_posts_and_comments_inactive")
                    continue
            if quirks.get("monthly_from_total"):
                active_month = users_total
                bump_quirk("monthly_from_total")
            if quirks.get("zero_monthly_skip") and active_month == 0:
                bump_quirk("zero_monthly_skip")
                continue
            if (
                (users_total is not None and users_total < 0)
                or (active_month is not None and active_month < 0)
            ):
                print(
                    f"# Skipping {path}: negative users_total ({users_total}) or active_month ({active_month})",
                    file=sys.stderr,
                )
                continue
            if software_key not in configured_software:
                report_entry = unknown_software_report.setdefault(
                    software_key or "(unknown)",
                    {"count": 0, "active_month_total": 0, "users_total_total": 0},
                )
                report_entry["count"] += 1
                if isinstance(active_month, int):
                    report_entry["active_month_total"] += active_month
                if isinstance(users_total, int):
                    report_entry["users_total_total"] += users_total

            if active_month is not None and users_total is not None and active_month > users_total:
                if quirks.get("cap_monthly_to_total"):
                    # Monthly users counts posters while total users counts enabled accounts.
                    active_month = users_total
                    bump_quirk("cap_monthly_to_total")
                elif quirks.get("trust_monthly_gt_total"):
                    bump_quirk("trust_monthly_gt_total")
                    pass
                else:
                    print(
                        f"# Skipping {path}: active_month ({active_month}) exceeds users_total ({users_total})",
                        file=sys.stderr,
                    )
                    continue
            if (
                active_month is not None
                and users_total is not None
                and users_total > active_month
                and quirks.get("cap_total_to_monthly")
            ):
                users_total = active_month
                bump_quirk("cap_total_to_monthly")

            writer.writerow([
                hostname or "",
                software_name or "",
                users_total if users_total is not None else "",
                active_month if active_month is not None else "",
                protocols_str,
            ])

    if unknown_software_report:
        print("# Unknown software report (top 5):", file=sys.stderr)

        def top_entries(key):
            return sorted(
                unknown_software_report.items(),
                key=lambda item: item[1][key],
                reverse=True,
            )[:5]

        for label, key in [
            ("instances", "count"),
            ("monthly", "active_month_total"),
            ("total", "users_total_total"),
        ]:
            print(f"# - Top by {label}:", file=sys.stderr)
            for software, entry in top_entries(key):
                print(
                    f"#   {software}: {entry['count']} "
                    f"(active_month total {entry['active_month_total']}, "
                    f"users_total total {entry['users_total_total']})",
                    file=sys.stderr,
                )

    if quirk_report:
        print("# Known software quirk report:", file=sys.stderr)
        for quirk in sorted(quirk_report):
            print(f"# - {quirk}: {quirk_report[quirk]}", file=sys.stderr)

    print(f"# Wrote {len(selected_files)} rows to {output_csv}", file=sys.stderr)

if __name__ == "__main__":
    main()
