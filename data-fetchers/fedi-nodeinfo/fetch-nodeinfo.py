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
import random
import time
from datetime import datetime, timezone
from typing import Optional, Dict, Tuple, List
from contextlib import asynccontextmanager

USER_AGENT = "fetch-nodeinfo-bot (+https://arewedecentralizedyet.online/)"

REQUEST_TIMEOUT = 10  # seconds
MAX_CONCURRENT = 30   # concurrent host checks

# Globals for config & state
ROBOTS_TTL_SECS: float = 24 * 3600
NODEINFO_TTL_SECS: float = 24 * 3600

state_hosts: Dict[str, Dict] = {}

# ---------------------------------------------------------------------
# Helpers: filenames, host list, state
# ---------------------------------------------------------------------
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

# ---------------------------------------------------------------------
# Per-host tate limiter
# ---------------------------------------------------------------------
class TokenBucket:
    def __init__(self, rate: float, capacity: int):
        """
        rate: tokens per second
        capacity: max burst size
        """
        self.rate = rate
        self.capacity = capacity
        self.tokens = float(capacity)
        self.timestamp = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self):
        while True:
            async with self._lock:
                now = time.monotonic()
                delta = now - self.timestamp
                self.timestamp = now

                # Refill
                self.tokens = min(self.capacity, self.tokens + delta * self.rate)

                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return  # got a token, go ahead

                # Need to wait for enough tokens to accumulate
                missing = 1.0 - self.tokens
                wait = missing / self.rate if self.rate > 0 else 1.0

            # Sleep *outside* the lock so others can progress
            await asyncio.sleep(wait)


class PerHostLimiter:
    def __init__(self, rate: float = 5.0, capacity: int = 5):
        """
        rate/capacity are the default per-host token bucket params.
        """
        self.rate = rate
        self.capacity = capacity
        self._buckets: dict[str, TokenBucket] = {}
        self._dict_lock = asyncio.Lock()

    async def _get_bucket(self, host: str | None) -> TokenBucket:
        key = host or "<no-host>"
        async with self._dict_lock:
            bucket = self._buckets.get(key)
            if bucket is None:
                bucket = TokenBucket(self.rate, self.capacity)
                self._buckets[key] = bucket
            return bucket

    async def acquire(self, host: str | None):
        bucket = await self._get_bucket(host)
        await bucket.acquire()

class RateLimitedSession:
    """
    Thin wrapper around aiohttp.ClientSession enforcing per-host rate limits.
    """

    def __init__(self, limiter: PerHostLimiter, *args, **kwargs):
        self._session = aiohttp.ClientSession(*args, **kwargs)
        self._limiter = limiter

    @asynccontextmanager
    async def request(self, method, url, **kwargs):
        host = urllib.parse.urlparse(url).hostname
        await self._limiter.acquire(host)
        async with self._session.request(method, url, **kwargs) as resp:
            yield resp

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
    #if netloc == "public-api.wordpress.com":
    #    return True

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
                return allowed

    # Need to (re)fetch robots.txt
    robots_url = f"{scheme}://{netloc}/robots.txt"
    allowed = True
    error_str: Optional[str] = None

    try:
        async with session.get(robots_url) as resp:
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
        async with session.get(url) as resp:
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
                return None, err
            return data, None
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        err = f"{type(e).__name__}: {e}"
        print(f"# Error fetching {url}: {err}", file=sys.stderr)
        return None, err

async def try_schemes(
    session: aiohttp.ClientSession,
    netloc: str,
    path: str,
    now: datetime
) -> Tuple[Optional[str], Optional[dict], Optional[str]]:
    """
    Try https and then http for a given netloc+path.
    Returns (url, json, error_str).
    json is None if both failed; error_str is from the last attempt.
    """
    last_err: Optional[str] = None
    for scheme in ("https", "http"):
        url = f"{scheme}://{netloc}{path}"
        data, err = await fetch_json(session, url, now)
        if data is not None:
            return url, data, None
        last_err = err
    return None, None, last_err

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
    well_url, well_data, well_err = await try_schemes(session, netloc, "/.well-known/nodeinfo",now)
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
    sem: asyncio.Semaphore,
    nodeinfo_dir: str,
) -> None:
    """
    Process a single host:
      - Honor NodeInfo TTL based on state.json
      - Fetch NodeInfo if needed
      - Save nodeinfo_dir/hostname/<datetime>.json
      - Update state.json in-memory
    """
    now = datetime.now(timezone.utc)
    timestr = now.isoformat().replace("+00:00", "Z")

    out_dir = os.path.join(nodeinfo_dir, sanitize_filename(host))
    os.makedirs(out_dir, exist_ok=True)

    out_path = os.path.join(out_dir, timestr + ".json")

    host_state = get_host_state(host)
    nodeinfo_state = host_state.get("nodeinfo")

    # Decide whether to skip based on TTL
    skip = False
    if nodeinfo_state is not None:
        status = nodeinfo_state.get("status")
        last_checked_str = nodeinfo_state.get("last_checked")
        last_dt = None
        if last_checked_str:
            try:
                last_dt = datetime.fromisoformat(last_checked_str)
            except Exception:
                last_dt = None

        if (NODEINFO_TTL_SECS > 0
            and last_dt is not None
            and (now - last_dt).total_seconds() < NODEINFO_TTL_SECS):
            skip = True

    if skip:
        print(f"# Skipping {host}")
        return

    print(f"# Considering {host}")

    # We are going to attempt a NodeInfo fetch
    async with sem:
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

    # Save NodeInfo document if OK
    if status == "ok" and nodeinfo_data is not None:
        record = {
            "hostname": host,
            "nodeinfo_url": nodeinfo_url,
            "nodeinfo": nodeinfo_data,
        }
        with open(out_path, "w", encoding="utf-8") as jf:
            json.dump(record, jf, ensure_ascii=False, indent=2)

# ---------------------------------------------------------------------
# Main async driver
# ---------------------------------------------------------------------
async def main_async(hosts: list, nodeinfo_dir: str, state_path: str, ratelimit: float) -> None:

    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT, limit_per_host=1)
    limiter = PerHostLimiter(rate=ratelimit, capacity=int(ratelimit) + 5)

    async with RateLimitedSession(
        timeout=timeout,
        connector=connector,
        limiter=limiter,
        headers={"User-Agent": USER_AGENT},
    ) as session:
        sem = asyncio.Semaphore(MAX_CONCURRENT)

        tasks = [
            process_host(
                host,
                session,
                sem,
                nodeinfo_dir
            )
            for host in hosts
        ]

        if tasks:
            await asyncio.gather(*tasks)

    # Final state save on shutdown
    save_state(state_path)
    print("# Done.", file=sys.stderr)


# ---------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------
def main() -> None:
    global ROBOTS_TTL_SECS, NODEINFO_TTL_SECS

    parser = argparse.ArgumentParser(description="Fetch NodeInfo documents for hosts.")
    parser.add_argument("hosts_json", help="JSON file containing array of hostnames")
    parser.add_argument("nodeinfo_dir", help="Directory to store hostname.json NodeInfo docs")
    parser.add_argument("state_file", help="JSON file to track robots/nodeinfo state across runs")
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
        "--N",
        type=int,
        default=0,
        help="Number of hosts to fetch (selected randomly)"
    )
    parser.add_argument(
        "--ratelimit",
        type=float,
        default=5,
        help="Per-host rate limit (requests per second)"
    )
    parser.add_argument(
        "--max-concurrent",
        type=int,
        default=0,
        help="Maximum number of concurrent connections"
    )

    args = parser.parse_args()

    ROBOTS_TTL_SECS = max(0.0, args.robots_ttl_hours) * 3600.0
    NODEINFO_TTL_SECS = max(0.0, args.nodeinfo_ttl_hours) * 3600.0

    if args.max_concurrent:
        global MAX_CONCURRENT
        MAX_CONCURRENT = args.max_concurrent

    hosts = load_hostnames(args.hosts_json)

    print(f"# Loaded {len(hosts)} unique hosts from {args.hosts_json}", file=sys.stderr)

    if args.N:
        hosts = random.sample(hosts,args.N)

    load_state(args.state_file)
    global state_hosts
    print(f"# Loaded {len(state_hosts)} state entries from {args.state_file}")

    os.makedirs(args.nodeinfo_dir, exist_ok=True)
    print(f"# Created {args.nodeinfo_dir}")

    try:
        asyncio.run(main_async(hosts, args.nodeinfo_dir, args.state_file, args.ratelimit))
    except KeyboardInterrupt:
        # Ctrl-C: try to persist whatever is in state_hosts right now
        print("# Caught KeyboardInterrupt, saving state before exit...", file=sys.stderr)
        save_state(args.state_file)
        sys.exit(1)

if __name__ == "__main__":
    main()

