#!/usr/bin/env python3

import argparse
import asyncio
import csv
import ipaddress
import json
import os
import socket
import tarfile
import urllib.request
from pathlib import Path

try:
    import ipinfo
except ImportError as exc:
    raise SystemExit(
        "Missing dependency: ipinfo. Install with `pip install ipinfo`."
    ) from exc


REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = REPO_ROOT / "data"
GEO_DIR = DATA_DIR / "geo"
ICON_DIR = Path("data-static") / "icons"

AT_BSKY_STYLE = {
    "color": "#1185FE",
    "icon": str(ICON_DIR / "bluesky.png"),
    "type": "bluesky",
}

AT_PROTO_STYLE = {
    "color": "#CBD5E1",
    "icon": str(ICON_DIR / "atproto.png"),
    "type": "atmosphere",
}

AT_TYPE_STYLE = {
    "bridgy-fed": {
        "name": "Bridgy",
        "color": "#FFFFFF",
        "icon": str(ICON_DIR / "bridgy.png"),
        "type": "bridgy-fed",
    },
}

AT_HOST_TYPES = {
    "atproto.brid.gy": "bridgy-fed",
}

FEDI_SOFTWARE_STYLE = {
    "ghost": {"color": "#E5E7EB", "icon": str(ICON_DIR / "ghost.png")},
    "mastodon": {"color": "#6364FF", "icon": str(ICON_DIR / "mastodon.png")},
    "wordpress": {"color": "#21759B", "icon": str(ICON_DIR / "wordpress.png")},
    "peertube": {"color": "#F1680D", "icon": str(ICON_DIR / "peertube.png")},
    "lemmy": {"color": "#FFFFFF", "icon": str(ICON_DIR / "lemmy.png")},
    "pixelfed": {"color": "#00A86B", "icon": str(ICON_DIR / "pixelfed.png")},
    "pleroma": {"color": "#8B5CF6", "icon": str(ICON_DIR / "pleroma.png")},
    "friendica": {"color": "#9CA3AF", "icon": str(ICON_DIR / "fediverse.png")},
    "akkoma": {"color": "#9CA3AF", "icon": str(ICON_DIR / "fediverse.png")},
    "owncast": {"color": "#9CA3AF", "icon": str(ICON_DIR / "fediverse.png")},
    "bookwyrm": {"color": "#F8F8F8", "icon": str(ICON_DIR / "bookwyrm.png")},
    "hometown": {"color": "#9CA3AF", "icon": str(ICON_DIR / "fediverse.png")},
    "iceshrimp.net": {"color": "#9CA3AF", "icon": str(ICON_DIR / "fediverse.png")},
    "hubzilla": {"color": "#9CA3AF", "icon": str(ICON_DIR / "fediverse.png")},
    "writefreely": {"color": "#9CA3AF", "icon": str(ICON_DIR / "fediverse.png")},
    "wafrn": {"color": "#28B8F8", "icon": str(ICON_DIR / "wafrn.png")},
}

FEDI_DEFAULT_STYLE = {
    "color": "#000000",
    "icon": str(ICON_DIR / "fediverse.png"),
}


def latest_csv_path(dir_path: Path) -> Path:
    csv_paths = [
        path for path in dir_path.glob("*.csv")
    ]
    if not csv_paths:
        raise FileNotFoundError(f"No CSV files found in {dir_path}")
    return max(csv_paths, key=lambda path: path.stat().st_mtime)


def load_hosts(source: str) -> tuple[Path, list[dict]]:
    if source == "fedi-mau":
        csv_path = latest_csv_path(DATA_DIR / "fedi-mau")
        with csv_path.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = [
                {
                    "hostname": row["hostname"].strip(),
                    "users": int(row["active_month"] or 0),
                    "software": (row.get("software") or "").strip(),
                }
                for row in reader
                if row.get("hostname")
            ]
    elif source == "at-mau":
        csv_path = latest_csv_path(DATA_DIR / "at-mau")
        with csv_path.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = [
                {"hostname": row["domain"].strip(), "users": int(row["mau"] or 0)}
                for row in reader
                if row.get("domain")
            ]
    else:
        raise ValueError(f"Unsupported source: {source}")

    return csv_path, rows


def load_at_overrides(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        with path.open(encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in overrides file: {path}") from exc
    if not isinstance(data, dict):
        raise ValueError("Overrides JSON must be an object keyed by hostname")
    return data


def load_fedi_overrides(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        with path.open(encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in fedi overrides file: {path}") from exc
    if not isinstance(data, dict):
        raise ValueError("Fedi overrides JSON must be an object keyed by hostname")
    return data


def load_ipinfo_token(config_path: Path) -> str | None:
    if not config_path.exists():
        return None
    try:
        with config_path.open(encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in ipinfo config: {config_path}") from exc
    if not isinstance(data, dict):
        raise ValueError("ipinfo config must be a JSON object")
    token = data.get("token")
    if token:
        return str(token)
    return None


def get_detail_field(details, name: str):
    if isinstance(details, dict):
        return details.get(name)
    return getattr(details, name, None)


def normalize_details(details) -> dict:
    if isinstance(details, dict):
        return details
    all_field = getattr(details, "all", None)
    if isinstance(all_field, dict):
        return all_field
    if hasattr(details, "__dict__"):
        return {k: v for k, v in vars(details).items() if not k.startswith("_")}
    return {}


def load_cache(cache_path: Path) -> dict:
    if not cache_path.exists():
        return {}
    try:
        with cache_path.open(encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in cache file: {cache_path}") from exc
    if not isinstance(data, dict):
        raise ValueError("Cache file must be a JSON object keyed by IP")
    return data


def extract_network(details) -> str | None:
    org = get_detail_field(details, "org")
    if org:
        return org
    asn = get_detail_field(details, "asn")
    if isinstance(asn, dict):
        return asn.get("name")
    return getattr(asn, "name", None)


def extract_lat_lon(details) -> tuple[float | None, float | None]:
    loc = get_detail_field(details, "loc")
    if not loc or "," not in loc:
        return None, None
    lat_str, lon_str = loc.split(",", 1)
    try:
        return float(lat_str), float(lon_str)
    except ValueError:
        return None, None


ALL_CDN_ORG_MARKERS = [
    "akamai",
    "akamaiedge",
    "akamaitechnologies",
    "cloudflare",
    "edgesuite",
    "fastly",
]


def fetch_url_text(url: str) -> str:
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "are-we-decentralized-yet/1.0"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return response.read().decode("utf-8")


def fetch_url_json(url: str) -> dict:
    return json.loads(fetch_url_text(url))


def ensure_cdn_ip_files(refresh: bool) -> dict[str, Path]:
    cdn_files = {
        "cloudflare": GEO_DIR / "cloudflare-ips.txt",
        "aws": GEO_DIR / "aws-ip-ranges.json",
        "gcp": GEO_DIR / "gcp-ip-ranges.json",
        "fastly": GEO_DIR / "fastly-ip-list.json",
    }
    if not refresh and all(path.exists() for path in cdn_files.values()):
        return cdn_files

    GEO_DIR.mkdir(parents=True, exist_ok=True)

    if refresh or not cdn_files["cloudflare"].exists():
        ipv4 = fetch_url_text("https://www.cloudflare.com/ips-v4/")
        ipv6 = fetch_url_text("https://www.cloudflare.com/ips-v6/")
        combined = "\n".join(
            line.strip()
            for line in (ipv4 + "\n" + ipv6).splitlines()
            if line.strip()
        )
        cdn_files["cloudflare"].write_text(combined + "\n", encoding="utf-8")

    if refresh or not cdn_files["aws"].exists():
        aws_data = fetch_url_json(
            "https://ip-ranges.amazonaws.com/ip-ranges.json"
        )
        cdn_files["aws"].write_text(
            json.dumps(aws_data, ensure_ascii=True, indent=2), encoding="utf-8"
        )

    if refresh or not cdn_files["gcp"].exists():
        gcp_data = fetch_url_json(
            "https://www.gstatic.com/ipranges/cloud.json"
        )
        cdn_files["gcp"].write_text(
            json.dumps(gcp_data, ensure_ascii=True, indent=2), encoding="utf-8"
        )

    if refresh or not cdn_files["fastly"].exists():
        fastly_data = fetch_url_json("https://api.fastly.com/public-ip-list")
        cdn_files["fastly"].write_text(
            json.dumps(fastly_data, ensure_ascii=True, indent=2), encoding="utf-8"
        )

    return cdn_files


def load_cloudflare_prefixes(path: Path) -> list[str]:
    if not path.exists():
        return []
    prefixes = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            prefixes.append(line)
    return prefixes


def load_aws_cloudfront_prefixes(path: Path) -> list[str]:
    if not path.exists():
        return []
    data = json.loads(path.read_text(encoding="utf-8"))
    prefixes = []
    for entry in data.get("prefixes", []):
        if entry.get("service") == "CLOUDFRONT":
            prefix = entry.get("ip_prefix")
            if prefix:
                prefixes.append(prefix)
    for entry in data.get("ipv6_prefixes", []):
        if entry.get("service") == "CLOUDFRONT":
            prefix = entry.get("ipv6_prefix")
            if prefix:
                prefixes.append(prefix)
    return prefixes


def load_gcp_cdn_prefixes(path: Path) -> list[str]:
    if not path.exists():
        return []
    data = json.loads(path.read_text(encoding="utf-8"))
    prefixes = []
    for entry in data.get("prefixes", []):
        if entry.get("service") != "Google Cloud CDN":
            continue
        prefix = entry.get("ipv4Prefix") or entry.get("ipv6Prefix")
        if prefix:
            prefixes.append(prefix)
    return prefixes


def load_fastly_prefixes(path: Path) -> list[str]:
    if not path.exists():
        return []
    data = json.loads(path.read_text(encoding="utf-8"))
    prefixes = []
    for key in ("addresses", "ipv6_addresses"):
        for entry in data.get(key, []):
            if entry:
                prefixes.append(entry)
    return prefixes


def build_cdn_networks(refresh: bool) -> list[ipaddress._BaseNetwork]:
    cdn_files = ensure_cdn_ip_files(refresh)
    prefix_lists = [
        load_cloudflare_prefixes(cdn_files["cloudflare"]),
        load_aws_cloudfront_prefixes(cdn_files["aws"]),
        load_gcp_cdn_prefixes(cdn_files["gcp"]),
        load_fastly_prefixes(cdn_files["fastly"]),
    ]
    networks = []
    for prefixes in prefix_lists:
        for prefix in prefixes:
            try:
                networks.append(ipaddress.ip_network(prefix, strict=False))
            except ValueError:
                continue
    return networks


def is_cdn(details, ip: str | None, cdn_networks: list[ipaddress._BaseNetwork]) -> bool:
    org = get_detail_field(details, "org")
    asn = get_detail_field(details, "asn")
    asn_domain = None
    if isinstance(asn, dict):
        asn_domain = asn.get("domain")
        asn_name = asn.get("name")
    else:
        asn_name = None

    haystacks = [
        s.lower()
        for s in [
            org,
            asn_domain,
            asn_name,
        ]
        if s
    ]
    if any(
        any(marker in s for marker in ALL_CDN_ORG_MARKERS) for s in haystacks
    ):
        return True

    if not ip:
        return False
    try:
        ip_value = ipaddress.ip_address(ip)
    except ValueError:
        return False
    return any(ip_value in network for network in cdn_networks)


async def resolve_hostnames(
    hostnames: list[str],
    dns_cache: dict,
    progress_every: int = 200,
) -> tuple[dict, dict]:
    hostname_to_ip: dict[str, str] = {}
    ip_to_hosts: dict[str, list[str]] = {}
    sem = asyncio.Semaphore(200)
    lock = asyncio.Lock()
    completed = 0
    total = len(hostnames)

    for hostname in hostnames:
        cached_ip = dns_cache.get(hostname)
        if cached_ip:
            hostname_to_ip[hostname] = cached_ip
            ip_to_hosts.setdefault(cached_ip, []).append(hostname)
            completed += 1

    async def resolve(hostname: str) -> None:
        nonlocal completed
        async with sem:
            try:
                infos = await asyncio.get_running_loop().getaddrinfo(
                    hostname, None, proto=socket.IPPROTO_TCP
                )
            except socket.gaierror:
                infos = None
            ip = None
            if infos:
                for family, _, _, _, sockaddr in infos:
                    if family == socket.AF_INET:
                        ip = sockaddr[0]
                        break
                if not ip:
                    ip = infos[0][4][0]
                if ip:
                    hostname_to_ip[hostname] = ip
                    ip_to_hosts.setdefault(ip, []).append(hostname)
                    dns_cache[hostname] = ip
            async with lock:
                completed += 1
                if progress_every and completed % progress_every == 0:
                    print(f"Resolved {completed}/{total} hostnames")

    unresolved = [h for h in hostnames if h not in hostname_to_ip]
    await asyncio.gather(*(resolve(host) for host in unresolved))
    return hostname_to_ip, ip_to_hosts


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate geocoded host summary JSON from the latest MAU CSV."
    )
    parser.add_argument("source", choices=["fedi-mau", "at-mau"])
    parser.add_argument(
        "-o",
        "--output",
        help="Output JSON path (default: data/<source>-geo.json)",
    )
    parser.add_argument(
        "--token",
        help="IPinfo access token (default: let ipinfo resolve from env/config)",
    )
    parser.add_argument(
        "--at-overrides",
        default=str(REPO_ROOT / "data-fetchers/geo/at-host-overrides.json"),
        help="Overrides JSON keyed by hostname for at-mau (default: data-fetchers/geo/at-host-overrides.json)",
    )
    parser.add_argument(
        "--fedi-overrides",
        default=str(REPO_ROOT / "data-fetchers/geo/fedi-host-overrides.json"),
        help="Overrides JSON keyed by hostname for fedi-mau (default: data-fetchers/geo/fedi-host-overrides.json)",
    )
    parser.add_argument(
        "--tarball",
        help="Optional tar.gz output including JSON and referenced icons",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Print debug information about DNS and ipinfo responses",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit number of hosts processed (for testing)",
    )
    parser.add_argument(
        "--cache",
        default=str(DATA_DIR / "cache/ipinfo-cache.json"),
        help="Path to IPinfo cache JSON (default: data/cache/ipinfo-cache.json)",
    )
    parser.add_argument(
        "--dns-cache",
        default=str(DATA_DIR / "cache/dns-cache.json"),
        help="Path to DNS cache JSON (default: data/cache/dns-cache.json)",
    )
    parser.add_argument(
        "--refresh-cdn-ips",
        action="store_true",
        help="Refetch CDN IP range data even if cached files exist",
    )
    args = parser.parse_args()

    csv_path, hosts = load_hosts(args.source)
    updated_timestamp = csv_path.stem
    if args.limit is not None:
        hosts = hosts[: max(args.limit, 0)]

    output_path = (
        Path(args.output)
        if args.output
        else DATA_DIR / f"geo/{args.source}-geo.json"
    )

    token = args.token
    if not token:
        token = load_ipinfo_token(Path("~/.config/ipinfo/config.json").expanduser())
    handler = ipinfo.getHandler(token) if token else ipinfo.getHandler()
    at_overrides = (
        load_at_overrides(Path(args.at_overrides))
        if args.source == "at-mau"
        else {}
    )
    fedi_overrides = (
        load_fedi_overrides(Path(args.fedi_overrides))
        if args.source == "fedi-mau"
        else {}
    )

    cache_path = Path(args.cache)
    cache = load_cache(cache_path)
    dns_cache_path = Path(args.dns_cache)
    dns_cache = load_cache(dns_cache_path)
    cdn_networks = build_cdn_networks(args.refresh_cdn_ips)

    hostnames = [entry["hostname"] for entry in hosts]
    details_by_ip: dict[str, object] = {}
    details_by_host: dict[str, object] = {}
    batch_size = 1000

    hostname_to_ip, ip_to_hosts = asyncio.run(
        resolve_hostnames(hostnames, dns_cache)
    )
    if args.debug:
        unresolved = [h for h in hostnames if h not in hostname_to_ip]
        print(f"Resolved {len(hostname_to_ip)}/{len(hostnames)} hostnames to IPs")
        if unresolved:
            sample = ", ".join(unresolved[:10])
            print(f"Sample unresolved hostnames: {sample}")
    for ip, detail in cache.items():
        details_by_ip[ip] = detail
        for hostname in ip_to_hosts.get(ip, []):
            details_by_host[hostname] = detail

    targets = [ip for ip in ip_to_hosts.keys() if ip not in cache]
    target_label = "IPs"

    for i in range(0, len(targets), batch_size):
        batch = targets[i : i + batch_size]
        batch_num = i // batch_size + 1
        batch_total = (len(targets) + batch_size - 1) // batch_size
        print(
            f"Looking up ipinfo batch {batch_num}/{batch_total} ({len(batch)} {target_label})"
        )
        try:
            batch_details = handler.getBatchDetails(batch)
        except Exception as exc:
            print(f"Warning: ipinfo batch lookup failed: {exc}")
            batch_details = {}

        def item_field(item, name: str):
            if isinstance(item, dict):
                return item.get(name)
            return getattr(item, name, None)

        def store_detail(key: str, detail: object) -> None:
            normalized = normalize_details(detail)
            details_by_ip[key] = normalized
            for hostname in ip_to_hosts.get(key, []):
                details_by_host[hostname] = normalized
            cache[key] = normalized

        if isinstance(batch_details, dict):
            for key, detail in batch_details.items():
                if key:
                    store_detail(key, detail)
        elif isinstance(batch_details, list):
            for item in batch_details:
                key = (
                    item_field(item, "ip")
                    or item_field(item, "query")
                    or item_field(item, "hostname")
                    or item_field(item, "host")
                )
                if key:
                    store_detail(key, item)
        if args.debug:
            print(
                f"Batch {batch_num} returned {len(batch_details) if hasattr(batch_details, '__len__') else 'unknown'} records"
            )
            if isinstance(batch_details, dict):
                sample_keys = list(batch_details.keys())[:5]
                if sample_keys:
                    print(f"Sample ipinfo keys: {sample_keys}")
            elif isinstance(batch_details, list) and batch_details:
                sample_item = batch_details[0]
                key_sample = (
                    item_field(sample_item, "ip")
                    or item_field(sample_item, "query")
                    or item_field(sample_item, "hostname")
                    or item_field(sample_item, "host")
                )
                print(f"Sample ipinfo item key: {key_sample}")

    results = []
    for entry in hosts:
        hostname = entry["hostname"]
        users = entry["users"]
        if users <= 0:
            continue
        software = (entry.get("software") or "").strip()
        ip = hostname_to_ip.get(hostname)
        details = (
            details_by_host.get(hostname)
            or details_by_ip.get(ip)
            or details_by_ip.get(hostname)
        )
        if not details:
            print(f"Warning: ipinfo lookup missing for {hostname}")
            continue

        if args.source == "at-mau":
            if hostname.endswith(".bsky.network"):
                style = AT_BSKY_STYLE
            else:
                override = at_overrides.get(hostname, {})
                if not isinstance(override, dict):
                    override = {}
                resolved_type = override.get("type") or AT_HOST_TYPES.get(hostname)
                style = AT_PROTO_STYLE
                if resolved_type in AT_TYPE_STYLE:
                    style = {**style, **AT_TYPE_STYLE[resolved_type]}
                style = {**style, **override}
        else:
            software_key = software.lower() if software else ""
            style = FEDI_SOFTWARE_STYLE.get(software_key, FEDI_DEFAULT_STYLE)
            if software_key in FEDI_SOFTWARE_STYLE:
                style = {**style, "type": software_key}
            else:
                style = {**style, "type": "other"}

        lat, lon = extract_lat_lon(details)
        record = {
            "hostname": hostname,
            "city": get_detail_field(details, "city"),
            "country": get_detail_field(details, "country"),
            "users": users,
            "lat": lat,
            "lon": lon,
            "network": extract_network(details),
            "cdn": is_cdn(details, ip, cdn_networks),
            "anycast": bool(get_detail_field(details, "anycast")),
            "color": style.get("color"),
            "icon": style.get("icon"),
            "type": style.get("type"),
            "updated": updated_timestamp,
        }

        if args.source == "fedi-mau":
            override = fedi_overrides.get(hostname, {})
            if isinstance(override, dict):
                record.update(override)

        results.append(record)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=True, indent=2)

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with cache_path.open("w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=True, indent=2)

    dns_cache_path.parent.mkdir(parents=True, exist_ok=True)
    with dns_cache_path.open("w", encoding="utf-8") as f:
        json.dump(dns_cache, f, ensure_ascii=True, indent=2)

    if args.tarball:
        tar_path = Path(args.tarball)
        tar_path.parent.mkdir(parents=True, exist_ok=True)
        tar_root = Path(args.source)
        with tarfile.open(tar_path, "w:gz") as tar:
            tar.add(output_path, arcname=str(tar_root / output_path.name))
            icon_root = REPO_ROOT / "data-static" / "icons"
            if icon_root.exists():
                tar.add(
                    icon_root,
                    arcname=str(tar_root / icon_root.relative_to(REPO_ROOT)),
                )
            else:
                print(f"Warning: missing icon directory {icon_root}")
        print(f"Wrote tarball to {tar_path}")

    print(f"Loaded {len(hosts)} hosts from {csv_path.name}")
    print(f"Wrote {len(results)} geocoded hosts to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
