#!/usr/bin/env python3
import sys
import os
import json
import asyncio
import aiohttp
import urllib.parse
import urllib.robotparser
import re
import argparse
import time
import ipaddress
import socket
from collections import deque
from datetime import datetime, timezone
from typing import Optional, Dict, Tuple, List
from contextlib import asynccontextmanager

USER_AGENT = "fetch-nodeinfo-bot (+https://arewedecentralizedyet.online/)"
USER_AGENT_CURL = "User-Agent: curl/8.17.0"

REQUEST_TIMEOUT = 10  # seconds
MAX_CONCURRENT = 30   # concurrent host checks
DNS_CACHE_TTL_SECS = 10 * 60
MAX_429_RETRIES = 3

# Globals for config & state
ROBOTS_TTL_SECS: float = 24 * 3600
NODEINFO_TTL_SECS: float = 24 * 3600
ERROR_TTL_SECS: float = 6 * 3600

state_hosts: Dict[str, Dict] = {}
stats_hosts: Dict[str, Dict] = {}

# ---------------------------------------------------------------------
# Helpers: filenames, host list, state
# ---------------------------------------------------------------------
def headers_for_url(url: str) -> Optional[dict]:
    host = urllib.parse.urlparse(url).hostname
    #if host == "public-api.wordpress.com":
    #    return {"User-Agent": USER_AGENT_CURL}
    return None

def sanitize_filename(host: str) -> str:
    """Make sure hostname is safe for filenames."""
    return re.sub(r"[^A-Za-z0-9._-]", "_", host)

def load_hostnames(path: str) -> List[str]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError("Input JSON must be an array of hostnames")
    return data

def load_state(path: str) -> None:
    """Load state.json into global `state_hosts`."""
    global  state_hosts
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                state_hosts = json.load(f)
        except Exception as e:
            print(f"# Warning: could not read state file {path}: {e}", file=sys.stderr)
            state_hosts = {}
    else:
        state_hosts = {}

def save_state(path: str) -> None:
    global state_hosts
    """Write state.json"""
    tmp = path + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state_hosts, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    except Exception as e:
        print(f"# Warning: could not write state file {path}: {e}", file=sys.stderr)

def get_host_state(host: str) -> Dict:
    hs = state_hosts.get(host)
    if hs is None:
        hs = {}
        state_hosts[host] = hs
    return hs

def parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None

def get_stats(host: str) -> Dict:
    hs = stats_hosts.get(host)
    if hs is None:
        hs = {
            "requests": 0,
            "success": 0,
            "robots_disallow": 0,
            "network_error": 0,
            "json_error": 0,
            "http_statuses": {},
        }
        stats_hosts[host] = hs
    return hs

def record_robots_disallow(host: str) -> None:
    hs = get_stats(host)
    hs["robots_disallow"] += 1

def record_network_error(host: str) -> None:
    hs = get_stats(host)
    hs["requests"] += 1
    hs["network_error"] += 1

def record_http_status(host: str, status: int) -> None:
    hs = get_stats(host)
    hs["requests"] += 1
    statuses = hs["http_statuses"]
    key = str(status)
    statuses[key] = statuses.get(key, 0) + 1

def record_success(host: str) -> None:
    hs = get_stats(host)
    hs["success"] += 1

def record_json_error(host: str) -> None:
    hs = get_stats(host)
    hs["json_error"] += 1

def host_for_url(url: str) -> str:
    host = urllib.parse.urlparse(url).hostname
    return host or "<no-host>"

WORDPRESS_V4_NETS = [
    ipaddress.ip_network("192.0.78.0/24"),
]

def is_wordpress_key(key: str) -> bool:
    try:
        net = ipaddress.ip_network(key, strict=False)
        if net.version != 4:
            return False
        return any(net.subnet_of(wp) or wp.subnet_of(net) for wp in WORDPRESS_V4_NETS)
    except ValueError:
        pass
    try:
        addr = ipaddress.ip_address(key)
        if addr.version != 4:
            return False
        return any(addr in wp for wp in WORDPRESS_V4_NETS)
    except ValueError:
        return False

def print_stats(stream=sys.stderr) -> None:
    if not stats_hosts:
        print("# No fetch stats recorded.", file=stream)
        return

    print("# Per-host fetch stats (success/requests, rate, 429s)", file=stream)
    def sort_key(item: Tuple[str, Dict]) -> Tuple[float, int, str]:
        host, hs = item
        reqs = hs.get("requests", 0)
        succ = hs.get("success", 0)
        rate = (succ / reqs) if reqs else 0.0
        return (rate, -reqs, host)

    for host, hs in sorted(stats_hosts.items(), key=sort_key):
        reqs = hs.get("requests", 0)
        if reqs < 5:
            continue
        succ = hs.get("success", 0)
        rate = succ / reqs if reqs else 0.0
        statuses = hs.get("http_statuses", {})
        count_429 = statuses.get("429", 0)
        net_err = hs.get("network_error", 0)
        robots_disallow = hs.get("robots_disallow", 0)
        json_err = hs.get("json_error", 0)
        print(
            f"# {host} {succ}/{reqs} ({rate:.1%})"
            f" 429={count_429} net_err={net_err} robots={robots_disallow} json_err={json_err}",
            file=stream,
        )

# ---------------------------------------------------------------------
# Concurrency-limited session
# ---------------------------------------------------------------------
class RateLimitedSession:
    """
    Thin wrapper around aiohttp.ClientSession enforcing a global concurrency limit.
    """

    def __init__(self, sem, *args, **kwargs):
        self._session = aiohttp.ClientSession(*args, **kwargs)
        self._sem = sem
        self._sem_waiters = 0
        self._sem_lock = asyncio.Lock()

    async def _inc_sem_waiters(self) -> None:
        async with self._sem_lock:
            self._sem_waiters += 1

    async def _dec_sem_waiters(self) -> None:
        async with self._sem_lock:
            self._sem_waiters = max(0, self._sem_waiters - 1)

    async def sem_waiters(self) -> int:
        async with self._sem_lock:
            return self._sem_waiters

    @asynccontextmanager
    async def request(self, method, url, **kwargs):
        if self._sem is not None:
            await self._inc_sem_waiters()
            try:
                await self._sem.acquire()
            finally:
                await self._dec_sem_waiters()
        try:
            async with self._session.request(method, url, **kwargs) as resp:
                yield resp
        finally:
            if self._sem is not None:
                self._sem.release()

    @asynccontextmanager
    async def get(self, url, **kwargs):
        async with self.request("GET", url, **kwargs) as resp:
            yield resp

    @asynccontextmanager
    async def post(self, url, **kwargs):
        async with self.request("POST", url, **kwargs) as resp:
            yield resp

    async def close(self):
        await self._session.close()

    async def __aenter__(self):
        await self._session.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return await self._session.__aexit__(exc_type, exc, tb)

    @property
    def connector(self):
        return self._session.connector

class RateLimitKeyer:
    def __init__(
        self,
        mode: str,
        subnet_bits_v4: int,
        subnet_bits_v6: int,
        cache_ttl: float = DNS_CACHE_TTL_SECS,
    ):
        self._mode = mode
        self._subnet_bits_v4 = subnet_bits_v4
        self._subnet_bits_v6 = subnet_bits_v6
        self._cache_ttl = cache_ttl
        self._cache: Dict[str, Tuple[float, str]] = {}
        self._lock = asyncio.Lock()

    async def get_key_for_host(self, host: str) -> str:
        host = host or "<no-host>"
        if self._mode == "host":
            return host

        cached = await self._get_cached(host)
        if cached is not None:
            return cached

        key = await self._resolve_key(host)
        await self._set_cached(host, key)
        return key

    async def _get_cached(self, host: str) -> Optional[str]:
        now = time.monotonic()
        async with self._lock:
            item = self._cache.get(host)
            if item is None:
                return None
            expires_at, key = item
            if now >= expires_at:
                self._cache.pop(host, None)
                return None
            return key

    async def _set_cached(self, host: str, key: str) -> None:
        expires_at = time.monotonic() + self._cache_ttl
        async with self._lock:
            self._cache[host] = (expires_at, key)

    async def _resolve_key(self, host: str) -> str:
        loop = asyncio.get_running_loop()
        try:
            infos = await loop.getaddrinfo(host, None, type=socket.SOCK_STREAM)
        except socket.gaierror:
            return host

        addrs = []
        for family, _, _, _, sockaddr in infos:
            if family == socket.AF_INET:
                addrs.append(sockaddr[0])
            elif family == socket.AF_INET6:
                addrs.append(sockaddr[0])

        if not addrs:
            return host

        if self._mode == "ip":
            return sorted(addrs)[0]

        networks = []
        for addr in addrs:
            try:
                ip = ipaddress.ip_address(addr)
            except ValueError:
                continue
            if ip.version == 4:
                net = ipaddress.ip_network(f"{addr}/{self._subnet_bits_v4}", strict=False)
            else:
                net = ipaddress.ip_network(f"{addr}/{self._subnet_bits_v6}", strict=False)
            networks.append(str(net))

        if not networks:
            return host

        return sorted(networks)[0]

# ---------------------------------------------------------------------
# Robots.txt handling (with TTL & state tracking)
# ---------------------------------------------------------------------
async def is_allowed(session: aiohttp.ClientSession, url: str, now: datetime) -> bool:
    """
    Check robots.txt for the given URL for our USER_AGENT.
    Uses state.json to rate-limit robots.txt fetching via TTL.
    """
    parsed = urllib.parse.urlparse(url)
    scheme = parsed.scheme or "https"
    netloc = parsed.netloc
    path = parsed.path or "/"

    host_state = get_host_state(netloc)
    robots_state = host_state.setdefault("robots", {})

    # Special case for wordpress.com - We fetch it because they *do* provide
    # public access to nodeinfo.json file, but then direct you to a different
    # host that just has a blanket deny-everything rule - gotta say, the intent
    # seems clear that you should be able to follow these links
    if netloc == "public-api.wordpress.com":
        return True

    # TTL check: reuse previous decision if fresh
    last_checked_str = robots_state.get("last_checked")
    if last_checked_str and ROBOTS_TTL_SECS > 0:
        try:
            last_dt = datetime.fromisoformat(last_checked_str)
            age = (now - last_dt).total_seconds()
        except Exception:
            age = None
        if age is not None and age < ROBOTS_TTL_SECS:
            allowed = robots_state.get("allowed")
            if isinstance(allowed, bool):
                if not allowed:
                    record_robots_disallow(netloc)
                return allowed

    # Need to (re)fetch robots.txt
    robots_url = f"{scheme}://{netloc}/robots.txt"
    allowed = True
    error_str: Optional[str] = None

    try:
        async with session.get(robots_url, headers=headers_for_url(robots_url)) as resp:
            if resp.status >= 400:
                # Treat missing/forbidden robots as "no robots" => allowed
                error_str = f"HTTP {resp.status}"
            else:
                text = await resp.text()
                rp = urllib.robotparser.RobotFileParser()
                rp.set_url(robots_url)
                rp.parse(text.splitlines())
                allowed = rp.can_fetch(USER_AGENT, path)
    except aiohttp.ClientError as e:
        error_str = f"{type(e).__name__}: {e}"
        # robots spec says: when robots unavailable, crawling is allowed
        allowed = True

    robots_state["last_checked"] = now.isoformat()
    robots_state["allowed"] = allowed
    robots_state["error"] = error_str

    if not allowed:
        record_robots_disallow(netloc)
    return allowed

# ---------------------------------------------------------------------
# JSON fetch with error propagation
# ---------------------------------------------------------------------
async def fetch_json(session: aiohttp.ClientSession, url: str, now: datetime) -> Tuple[Optional[dict], Optional[str]]:
    """
    Fetch JSON from URL, respecting robots. Returns (data, error_str).
    error_str is None on success, otherwise a short description.
    """
    if not await is_allowed(session, url, now):
        err = "disallowed by robots.txt"
        print(f"# robots.txt disallows {url}", file=sys.stderr)
        return None, err

    try:
        async with session.get(url, headers=headers_for_url(url)) as resp:
            host = host_for_url(url)
            record_http_status(host, resp.status)
            if resp.status != 200:
                err = f"HTTP {resp.status}"
                print(f"# {err} for {url}", file=sys.stderr)
                #print("\n===== HTTP ERROR =====")
                #print(f"URL: {url}")
                #print(f"Status: {resp.status}")
                #print("Headers:")
                #for k, v in resp.headers.items():
                #    print(f"  {k}: {v}")
                #body = await resp.text()
                #print("Body:")
                #print(body)
                #print("===== END ERROR =====\n")
                return None, err
            try:
                data = await resp.json(content_type=None)
            except ValueError as e:
                err = f"JSON decode error: {e}"
                print(f"# {err} for {url}", file=sys.stderr)
                record_json_error(host)
                return None, err
            record_success(host)
            return data, None
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        err = f"{type(e).__name__}: {e}"
        print(f"# Error fetching {url}: {err}", file=sys.stderr)
        record_network_error(host_for_url(url))
        return None, err

# ---------------------------------------------------------------------
# NodeInfo helpers
# ---------------------------------------------------------------------
def pick_best_nodeinfo_link(links: list) -> Optional[str]:
    """
    Given the 'links' array from /.well-known/nodeinfo, pick the highest version
    rel example:
      "http://nodeinfo.diaspora.software/ns/schema/2.1"
    """
    best = None
    best_version = (-1, -1)  # major, minor

    for link in links:
        rel = link.get("rel", "")
        href = link.get("href")
        if not href:
            continue

        try:
            version_str = rel.rstrip("/").split("/")[-1]
            parts = version_str.split(".")
            major = int(parts[0])
            minor = int(parts[1]) if len(parts) > 1 else 0
            version_tuple = (major, minor)
        except Exception:
            version_tuple = (0, 0)

        if version_tuple > best_version:
            best_version = version_tuple
            best = href

    return best

async def fetch_nodeinfo_for_host(
    session: aiohttp.ClientSession,
    netloc: str,
    now: datetime
) -> Tuple[Optional[str], Optional[dict], str, Optional[str]]:
    """
    For a given host (netloc), return:
      (nodeinfo_url, nodeinfo_data, status, error_str)

    status is one of:
      "ok", "no_wellknown", "no_links", "fetch_error"
    error_str describes the problem for non-ok statuses.
    """
    # 1) Fetch /.well-known/nodeinfo
    well_url = f"https://{netloc}/.well-known/nodeinfo"
    well_data, well_err = await fetch_json(session, well_url, now)
    if well_data is None or "links" not in well_data:
        return None, None, "no_wellknown", well_err

    # 2) Choose best link
    href = pick_best_nodeinfo_link(well_data.get("links", []))
    if not href:
        return None, None, "no_links", None

    # 3) Fetch NodeInfo document
    parsed = urllib.parse.urlparse(href)
    if not parsed.scheme:
        if not well_url:
            return None, None, "fetch_error", "relative href but no base URL"
        href = urllib.parse.urljoin(well_url, href)

    nodeinfo_data, node_err = await fetch_json(session, href, now)
    if nodeinfo_data is None:
        return href, None, "fetch_error", node_err

    return href, nodeinfo_data, "ok", None


# ---------------------------------------------------------------------
# worker task
# ---------------------------------------------------------------------
async def process_host(
    host: str,
    session: aiohttp.ClientSession,
    nodeinfo_dir: str,
) -> Tuple[str, Optional[str]]:
    """
    Process a single host:
      - Fetch NodeInfo if needed
      - Save nodeinfo_dir/hostname/<datetime>.json
      - Update state.json in-memory
    """
    now = datetime.now(timezone.utc)
    timestr = now.isoformat().replace("+00:00", "Z")

    out_dir = os.path.join(nodeinfo_dir, sanitize_filename(host))
    os.makedirs(out_dir, exist_ok=True)

    out_path = os.path.join(out_dir, timestr + ".json")


    # We are going to attempt a NodeInfo fetch
    try:
        nodeinfo_url, nodeinfo_data, status, error_str = await fetch_nodeinfo_for_host(session, host, now)
    except Exception as e:
        status = "fetch_error"
        error_str = f"{type(e).__name__}: {e}"
        nodeinfo_url = None
        nodeinfo_data = None

    # Update nodeinfo state
    host_state = get_host_state(host)
    nodeinfo_state = host_state.setdefault("nodeinfo", {})
    nodeinfo_state["last_checked"] = now.isoformat()
    nodeinfo_state["status"] = status
    nodeinfo_state["error"] = error_str
    if status == "ok":
        nodeinfo_state["last_success"] = now.isoformat()
        nodeinfo_state.pop("last_error", None)
    else:
        nodeinfo_state["last_error"] = now.isoformat()

    # Save NodeInfo document if OK
    if status == "ok" and nodeinfo_data is not None:
        record = {
            "hostname": host,
            "nodeinfo_url": nodeinfo_url,
            "nodeinfo": nodeinfo_data,
        }
        with open(out_path, "w", encoding="utf-8") as jf:
            json.dump(record, jf, ensure_ascii=False, indent=2)

    return status, error_str

def should_skip_nodeinfo(host: str, now: datetime) -> bool:
    host_state = get_host_state(host)
    nodeinfo_state = host_state.get("nodeinfo")
    if nodeinfo_state is None:
        return False

    last_dt = parse_dt(nodeinfo_state.get("last_checked"))
    if last_dt is None:
        return False

    if NODEINFO_TTL_SECS <= 0:
        return False

    return (now - last_dt).total_seconds() < NODEINFO_TTL_SECS

def should_skip_robots(host: str, now: datetime) -> bool:
    host_state = get_host_state(host)
    robots_state = host_state.get("robots") or {}
    allowed = robots_state.get("allowed")
    if allowed is not False:
        return False
    last_checked = parse_dt(robots_state.get("last_checked"))
    if last_checked is None or ROBOTS_TTL_SECS <= 0:
        return False
    return (now - last_checked).total_seconds() < ROBOTS_TTL_SECS

def should_skip_error(host: str, now: datetime) -> bool:
    host_state = get_host_state(host)
    nodeinfo_state = host_state.get("nodeinfo") or {}
    last_error = parse_dt(nodeinfo_state.get("last_error"))
    if last_error is None or ERROR_TTL_SECS <= 0:
        return False
    return (now - last_error).total_seconds() < ERROR_TTL_SECS

def last_success_dt(host: str) -> Optional[datetime]:
    host_state = get_host_state(host)
    nodeinfo_state = host_state.get("nodeinfo") or {}
    last_success = parse_dt(nodeinfo_state.get("last_success"))
    if last_success is not None:
        return last_success
    if nodeinfo_state.get("status") == "ok":
        return parse_dt(nodeinfo_state.get("last_checked"))
    return None

# ---------------------------------------------------------------------
# Main async driver
# ---------------------------------------------------------------------
async def main_async(
    hosts: list,
    nodeinfo_dir: str,
    state_path: str,
    ratelimit: float,
    ratelimit_key: str,
    subnet_bits_v4: int,
    subnet_bits_v6: int,
    status_interval: float,
    status_max_keys: int,
    limit_n: int,
) -> None:

    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT, limit_per_host=1)
    keyer = RateLimitKeyer(
        mode=ratelimit_key,
        subnet_bits_v4=subnet_bits_v4,
        subnet_bits_v6=subnet_bits_v6,
    )

    now = datetime.now(timezone.utc)
    excluded = {"nodeinfo_ttl": 0, "robots_ttl": 0, "error_ttl": 0}
    candidates: List[Tuple[Optional[datetime], str]] = []
    for host in hosts:
        if should_skip_nodeinfo(host, now):
            excluded["nodeinfo_ttl"] += 1
            continue
        if should_skip_robots(host, now):
            excluded["robots_ttl"] += 1
            continue
        if should_skip_error(host, now):
            excluded["error_ttl"] += 1
            continue
        candidates.append((last_success_dt(host), host))

    candidates.sort(
        key=lambda item: (0 if item[0] is None else 1, item[0] or datetime.min)
    )
    eligible_total = len(candidates)
    selected_hosts = [host for _, host in candidates]
    if limit_n:
        selected_hosts = selected_hosts[:limit_n]

    dns_sem = asyncio.Semaphore(MAX_CONCURRENT)

    progress = {"total": len(selected_hosts), "done": 0}
    attempts = 0
    timing = {"sum_durations": 0.0}
    pending_resolves = 0
    inflight = 0
    per_key_queues: Dict[str, deque] = {}
    per_key_next: Dict[str, float] = {}
    per_key_active: Dict[str, int] = {}
    per_key_interval: Dict[str, float] = {}
    per_key_429_remaining: Dict[str, int] = {}
    queue_cond = asyncio.Condition()
    max_rate = max(1.0, ratelimit)
    min_rate = 1.0

    def key_min_interval(key: str) -> float:
        key_max_rate = max_rate
        if is_wordpress_key(key):
            key_max_rate = 1.0
        return 1.0 / key_max_rate

    def key_max_interval() -> float:
        return 1.0 / min_rate

    async def status_reporter(session: RateLimitedSession) -> None:
        start = time.monotonic()
        last_time = start
        last_done = 0
        last_sum = 0.0
        while True:
            await asyncio.sleep(status_interval)
            now = time.monotonic()
            async with queue_cond:
                done = progress["done"]
                total = progress["total"]
                queued_total = sum(len(q) for q in per_key_queues.values())
                active_total = sum(per_key_active.values())
                resolving = pending_resolves
                sum_durations = timing["sum_durations"]
                attempts_now = attempts
                ready_keys = sum(
                    1
                    for key, q in per_key_queues.items()
                    if q and now >= per_key_next.get(key, 0.0)
                )
                entries = []
                for key in set(per_key_queues.keys()) | set(per_key_active.keys()):
                    queued = len(per_key_queues.get(key, deque()))
                    active = per_key_active.get(key, 0)
                    next_in = max(0.0, per_key_next.get(key, 0.0) - now)
                    if queued or active:
                        interval = per_key_interval.get(key, key_min_interval(key))
                        entries.append((key, queued, active, next_in, interval))
            sem_wait_total = await session.sem_waiters()
            elapsed = time.monotonic() - start
            interval_secs = max(0.001, now - last_time)
            attempts_delta = attempts_now - last_done
            sum_delta = sum_durations - last_sum
            rate = attempts_delta / interval_secs
            avg_ms = (sum_delta / attempts_delta * 1000.0) if attempts_delta > 0 else 0.0
            print(
                f"# Status t={elapsed:.1f}s done={done}/{total}"
                f" active={active_total}/{MAX_CONCURRENT} queued={queued_total}"
                f" ready_keys={ready_keys} resolving={resolving} sem_waiting={sem_wait_total}"
                f" rate={rate:.2f}/s avg_ms={avg_ms:.0f}",
                file=sys.stderr,
            )
            last_time = now
            last_done = attempts_now
            last_sum = sum_durations
            entries.sort(key=lambda x: (x[1], x[2], x[0]), reverse=True)
            if status_max_keys > 0:
                entries = entries[:status_max_keys]
            for key, queued, active, next_in, interval in entries:
                rate = 1.0 / interval if interval > 0 else 0.0
                print(
                    f"#   key={key} queued={queued} active={active}"
                    f" next_ready_in={next_in:.2f}s rate={rate:.2f}/s",
                    file=sys.stderr,
                )

    async def resolve_and_enqueue(host: str) -> None:
        nonlocal pending_resolves
        async with dns_sem:
            try:
                key = await keyer.get_key_for_host(host)
            except Exception:
                key = host
        async with queue_cond:
            per_key_queues.setdefault(key, deque()).append(host)
            pending_resolves -= 1
            queue_cond.notify()

    async def dispatch_loop(session: RateLimitedSession) -> None:
        nonlocal inflight, attempts
        while True:
            async with queue_cond:
                queues_nonempty = any(per_key_queues.get(k) for k in per_key_queues.keys())
                if pending_resolves == 0 and not queues_nonempty and inflight == 0:
                    return

                now = time.monotonic()
                ready_keys = [
                    key for key, q in per_key_queues.items()
                    if q and now >= per_key_next.get(key, 0.0)
                ]
                if not ready_keys:
                    next_times = [
                        per_key_next.get(key, 0.0)
                        for key, q in per_key_queues.items()
                        if q
                    ]
                    if not next_times:
                        try:
                            await asyncio.wait_for(queue_cond.wait(), timeout=1.0)
                        except asyncio.TimeoutError:
                            pass
                    else:
                        wait = max(0.0, min(next_times) - now)
                        try:
                            await asyncio.wait_for(queue_cond.wait(), timeout=wait)
                        except asyncio.TimeoutError:
                            pass
                    continue

                key = max(ready_keys, key=lambda k: len(per_key_queues[k]))
                host = per_key_queues[key].popleft()
                interval = per_key_interval.get(key, key_min_interval(key))
                next_time = per_key_next.get(key, now)
                per_key_next[key] = max(now, next_time) + interval
                inflight += 1
                per_key_active[key] = per_key_active.get(key, 0) + 1

            start_time = time.monotonic()
            status = "fetch_error"
            error_str = None
            try:
                status, error_str = await process_host(host, session, nodeinfo_dir)
            except Exception as e:
                status = "fetch_error"
                error_str = f"{type(e).__name__}: {e}"
            finally:
                duration = time.monotonic() - start_time
                async with queue_cond:
                    inflight -= 1
                    per_key_active[key] = per_key_active.get(key, 0) - 1
                    if per_key_active[key] <= 0:
                        per_key_active.pop(key, None)
                    attempts += 1
                    if error_str == "HTTP 429":
                        remaining = per_key_429_remaining.get(key, MAX_429_RETRIES)
                        if remaining > 0:
                            per_key_429_remaining[key] = remaining - 1
                            interval = min(
                                key_max_interval(),
                                per_key_interval.get(key, key_min_interval(key)) * 2.0,
                            )
                            per_key_interval[key] = interval
                            per_key_next[key] = max(time.monotonic(), per_key_next.get(key, 0.0)) + interval
                            per_key_queues.setdefault(key, deque()).append(host)
                        else:
                            progress["done"] += 1
                    else:
                        progress["done"] += 1
                        if status == "ok":
                            interval = per_key_interval.get(key, key_min_interval(key))
                            per_key_interval[key] = max(key_min_interval(key), interval * 0.9)
                            per_key_429_remaining[key] = MAX_429_RETRIES
                    timing["sum_durations"] += duration
                    queue_cond.notify()

    async with RateLimitedSession(
        timeout=timeout,
        connector=connector,
        sem=None,
        headers={"User-Agent": USER_AGENT},
    ) as session:
        pending_resolves = len(selected_hosts)
        print(
            f"# Eligible hosts for fetch: {len(selected_hosts)}"
            f" (skipped nodeinfo_ttl={excluded['nodeinfo_ttl']}"
            f" robots_ttl={excluded['robots_ttl']}"
            f" error_ttl={excluded['error_ttl']}"
            f" excluded_by_n={max(0, eligible_total - len(selected_hosts))})",
            file=sys.stderr,
        )

        resolve_tasks = [
            asyncio.create_task(resolve_and_enqueue(host))
            for host in selected_hosts
        ]

        status_task = None
        if status_interval > 0:
            status_task = asyncio.create_task(status_reporter(session))

        try:
            dispatchers = [
                asyncio.create_task(dispatch_loop(session))
                for _ in range(MAX_CONCURRENT)
            ]
            if resolve_tasks:
                await asyncio.gather(*resolve_tasks)
                async with queue_cond:
                    queue_cond.notify_all()
            await asyncio.gather(*dispatchers)
        finally:
            if status_task is not None:
                status_task.cancel()
                try:
                    await status_task
                except asyncio.CancelledError:
                    pass

    # Final state save on shutdown
    save_state(state_path)
    print("# Done.", file=sys.stderr)

async def run_rate_limit_self_test(rate: float, seconds: float, hosts: int, workers: int) -> None:
    if rate <= 0:
        print("# Self-test requires a positive --ratelimit value.", file=sys.stderr)
        return

    interval = 1.0 / rate
    start = time.monotonic()
    stop = start + seconds
    results: Dict[str, List[float]] = {}
    per_key_next: Dict[str, float] = {}
    queues: Dict[str, deque] = {}

    items_per_key = max(1, int(rate * seconds * max(1, workers)))
    for i in range(hosts):
        key = f"host{i+1}"
        results[key] = []
        queues[key] = deque(range(items_per_key))

    while time.monotonic() < stop and any(queues.values()):
        now = time.monotonic()
        ready_keys = [
            key for key, q in queues.items()
            if q and now >= per_key_next.get(key, 0.0)
        ]
        if not ready_keys:
            next_times = [
                per_key_next.get(key, 0.0)
                for key, q in queues.items()
                if q
            ]
            if not next_times:
                break
            wait = max(0.0, min(next_times) - now)
            await asyncio.sleep(wait)
            continue

        key = max(ready_keys, key=lambda k: len(queues[k]))
        queues[key].popleft()
        results[key].append(time.monotonic())
        per_key_next[key] = max(now, per_key_next.get(key, now)) + interval

    observed_stop = time.monotonic()
    observed_time = observed_stop - start

    print("# Rate-limit self-test results", file=sys.stderr)
    expected_interval = interval
    for host, times in results.items():
        times.sort()
        total = len(times)
        if total < 2:
            print(f"# {host} total={total} (insufficient samples)", file=sys.stderr)
            continue
        intervals = [b - a for a, b in zip(times, times[1:])]
        avg_interval = sum(intervals) / len(intervals)
        min_interval = min(intervals)
        observed_rate = total / observed_time
        warn = ""
        if observed_rate > rate * 1.1 or min_interval < expected_interval * 0.7:
            warn = " WARNING"
        print(
            f"# {host} total={total} rate={observed_rate:.2f}/s"
            f" avg_interval={avg_interval:.3f}s min_interval={min_interval:.3f}s{warn}",
            file=sys.stderr,
        )


# ---------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------
def main() -> None:
    global ROBOTS_TTL_SECS, NODEINFO_TTL_SECS, ERROR_TTL_SECS

    parser = argparse.ArgumentParser(description="Fetch NodeInfo documents for hosts.")
    parser.add_argument("hosts_json", nargs="?", help="JSON file containing array of hostnames")
    parser.add_argument("nodeinfo_dir", nargs="?", help="Directory to store hostname.json NodeInfo docs")
    parser.add_argument("state_file", nargs="?", help="JSON file to track robots/nodeinfo state across runs")
    parser.add_argument(
        "--robots-ttl-hours",
        type=float,
        default=24.0*7, # One week
        help="Minimum hours between re-fetching robots.txt for a host (0 = always re-fetch)",
    )
    parser.add_argument(
        "--nodeinfo-ttl-hours",
        type=float,
        default=24.0, # One day
        help="Minimum hours between re-fetching NodeInfo for a host (0 = always re-fetch)",
    )
    parser.add_argument(
        "--error-ttl-hours",
        type=float,
        default=6.0,
        help="Minimum hours between retries after an error (0 = disable error TTL)",
    )
    parser.add_argument(
        "--N",
        type=int,
        default=0,
        help="Number of hosts to fetch (oldest successful fetch first)"
    )
    parser.add_argument(
        "--ratelimit",
        type=float,
        default=5,
        help="Per-key rate limit (requests per second)"
    )
    parser.add_argument(
        "--ratelimit-key",
        choices=["host", "ip", "subnet"],
        default="host",
        help="Key rate limiting by hostname, resolved IP, or resolved IP subnet"
    )
    parser.add_argument(
        "--ratelimit-subnet-v4",
        type=int,
        default=24,
        help="IPv4 subnet size for --ratelimit-key=subnet"
    )
    parser.add_argument(
        "--ratelimit-subnet-v6",
        type=int,
        default=64,
        help="IPv6 subnet size for --ratelimit-key=subnet"
    )
    parser.add_argument(
        "--status-interval",
        type=float,
        default=10.0,
        help="Seconds between periodic status reports (0 to disable)"
    )
    parser.add_argument(
        "--status-max-keys",
        type=int,
        default=20,
        help="Max keys to include per status report (0 = all)"
    )
    parser.add_argument(
        "--max-concurrent",
        type=int,
        default=0,
        help="Maximum number of concurrent connections"
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="Run a local rate-limiter self-test and exit"
    )
    parser.add_argument(
        "--self-test-seconds",
        type=float,
        default=3.0,
        help="Duration of self-test in seconds"
    )
    parser.add_argument(
        "--self-test-hosts",
        type=int,
        default=2,
        help="Number of synthetic hosts to test"
    )
    parser.add_argument(
        "--self-test-workers",
        type=int,
        default=4,
        help="Queue depth multiplier per synthetic host"
    )

    args = parser.parse_args()

    if args.self_test:
        asyncio.run(
            run_rate_limit_self_test(
                rate=args.ratelimit,
                seconds=args.self_test_seconds,
                hosts=args.self_test_hosts,
                workers=args.self_test_workers,
            )
        )
        return

    if not args.hosts_json or not args.nodeinfo_dir or not args.state_file:
        parser.error("hosts_json, nodeinfo_dir, and state_file are required unless --self-test is set")

    ROBOTS_TTL_SECS = max(0.0, args.robots_ttl_hours) * 3600.0
    NODEINFO_TTL_SECS = max(0.0, args.nodeinfo_ttl_hours) * 3600.0
    ERROR_TTL_SECS = max(0.0, args.error_ttl_hours) * 3600.0

    if args.max_concurrent:
        global MAX_CONCURRENT
        MAX_CONCURRENT = args.max_concurrent

    hosts = load_hostnames(args.hosts_json)

    print(f"# Loaded {len(hosts)} unique hosts from {args.hosts_json}", file=sys.stderr)

    # Ordering and limiting happens in main_async.

    load_state(args.state_file)
    global state_hosts
    print(f"# Loaded {len(state_hosts)} state entries from {args.state_file}")

    os.makedirs(args.nodeinfo_dir, exist_ok=True)
    print(f"# Created {args.nodeinfo_dir}")

    try:
        asyncio.run(
            main_async(
                hosts,
                args.nodeinfo_dir,
                args.state_file,
                args.ratelimit,
                args.ratelimit_key,
                args.ratelimit_subnet_v4,
                args.ratelimit_subnet_v6,
                args.status_interval,
                args.status_max_keys,
                args.N,
            )
        )
    except KeyboardInterrupt:
        # Ctrl-C: try to persist whatever is in state_hosts right now
        print("# Caught KeyboardInterrupt, saving state before exit...", file=sys.stderr)
        save_state(args.state_file)
        sys.exit(1)
    finally:
        print_stats(sys.stderr)

if __name__ == "__main__":
    main()
