#!/usr/bin/env /usr/local/bin/python3
import argparse
import asyncio
import json
import os
import signal
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, Tuple

import aiohttp
from atproto import (
    AsyncFirehoseSubscribeReposClient,
    parse_subscribe_repos_message,
    models,
)

# -----------------------------
# Constants
# -----------------------------
SNAPSHOT_FILE = "accounts_snapshot.json"
SNAPSHOT_INTERVAL = 300  # seconds
RESOLVE_TTL_SECONDS = 24 * 60 * 60  # 1 day
RESOLUTION_QUEUE_SIZE = 10000
RESOLUTION_WORKERS = 10

# -----------------------------
# In-memory state
# -----------------------------
# did -> {
#   "pds": str | None,
#   "handle": str | None,
#   "last_seen": datetime,
#   "last_resolved": datetime | None,  # last time we ATTEMPTED resolution
# }
accounts: Dict[str, Dict[str, object]] = {}

# Lock to protect `accounts`
accounts_lock = asyncio.Lock()


# -----------------------------
# Snapshot load/save
# -----------------------------
def load_snapshot(path: str) -> None:
    """
    Load snapshot from JSON into `accounts`.
    Safe to call before the event loop starts (no concurrent access yet).
    """
    if not os.path.exists(path):
        return

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    loaded = 0
    for did, entry in data.items():
        try:
            last_seen_str = entry["last_seen"]
            last_seen = datetime.fromisoformat(last_seen_str)
        except Exception:
            continue

        last_resolved_str = entry.get("last_resolved")
        last_resolved = None
        if isinstance(last_resolved_str, str):
            try:
                last_resolved = datetime.fromisoformat(last_resolved_str)
            except Exception:
                last_resolved = None

        accounts[did] = {
            "pds": entry.get("pds"),
            "handle": entry.get("handle"),
            "last_seen": last_seen,
            "last_resolved": last_resolved,
        }
        loaded += 1

    print(f"Loaded snapshot from {path}: {loaded} accounts")


def _write_snapshot_file(path: str, snapshot_copy: Dict[str, Dict[str, object]], verbose: bool) -> None:
    tmp_path = path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(snapshot_copy, f, indent=2, sort_keys=True)
    os.replace(tmp_path, path)
    if verbose:
        print(f"Saved snapshot to {path}: {len(snapshot_copy)} accounts")


async def async_save_snapshot(path: str, verbose: bool = True) -> None:
    """
    Take a consistent snapshot of `accounts` under the lock,
    then write it to disk in a thread.
    """
    async with accounts_lock:
        snapshot_copy: Dict[str, Dict[str, object]] = {}
        for did, entry in accounts.items():
            ls = entry["last_seen"]
            if isinstance(ls, datetime):
                last_seen_str = ls.isoformat()
            else:
                last_seen_str = str(ls)

            lr = entry.get("last_resolved")
            if isinstance(lr, datetime):
                last_resolved_str = lr.isoformat()
            elif lr is None:
                last_resolved_str = None
            else:
                last_resolved_str = str(lr)

            snapshot_copy[did] = {
                "pds": entry.get("pds"),
                "handle": entry.get("handle"),
                "last_seen": last_seen_str,
                "last_resolved": last_resolved_str,
            }

    await asyncio.to_thread(_write_snapshot_file, path, snapshot_copy, verbose)


async def periodic_snapshot(path: str, interval_seconds: int) -> None:
    """
    Periodically write the current state to disk.
    No stdout from here (verbose=False).
    """
    while True:
        await asyncio.sleep(interval_seconds)
        await async_save_snapshot(path, verbose=False)


# -----------------------------
# DID / PDS resolution helpers
# -----------------------------
async def resolve_did_document(
    did: str,
    session: aiohttp.ClientSession,
    timeout_seconds: float = 5.0,
) -> Optional[dict]:
    """
    Fetch the DID document for `did`.

    - did:plc:...      -> https://plc.directory/<did>
    - did:web:foo.bar  -> https://foo.bar/.well-known/did.json
    """
    if did.startswith("did:plc:"):
        url = f"https://plc.directory/{did}"
    elif did.startswith("did:web:"):
        domain = did[len("did:web:") :]
        url = f"https://{domain}/.well-known/did.json"
    else:
        return None

    try:
        timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        async with session.get(url, timeout=timeout) as resp:
            if resp.status != 200:
                return None
            return await resp.json()
    except Exception:
        return None


def extract_pds_from_diddoc(doc: dict) -> Optional[str]:
    """
    From a DID document, find the PDS endpoint:

      service[].id endswith '#atproto_pds'
      type == 'AtprotoPersonalDataServer'
      serviceEndpoint is the PDS URL
    """
    services = doc.get("service") or doc.get("services") or []
    if not isinstance(services, list):
        return None

    for svc in services:
        try:
            svc_id = svc.get("id", "")
            svc_type = svc.get("type", "")
            endpoint = svc.get("serviceEndpoint") or svc.get("endpoint")
            if (
                isinstance(svc_id, str)
                and svc_id.endswith("#atproto_pds")
                and svc_type == "AtprotoPersonalDataServer"
                and isinstance(endpoint, str)
            ):
                return endpoint
        except Exception:
            continue

    return None


def extract_handle_from_diddoc(doc: dict) -> Optional[str]:
    """
    From a DID document, try to get the handle from alsoKnownAs:
      e.g., "alsoKnownAs": ["at://alice.bsky.social"]
    """
    also = doc.get("alsoKnownAs") or []
    if not isinstance(also, list):
        return None

    for aka in also:
        if isinstance(aka, str) and aka.startswith("at://"):
            return aka[len("at://") :]
    return None


async def resolve_if_needed(
    did: str,
    session: aiohttp.ClientSession,
    force_resolve: bool,
    resolve_ttl_seconds: int = RESOLVE_TTL_SECONDS,
) -> None:
    """
    Resolution worker entry point.

    - Checks last_resolved and TTL under lock.
    - If we actually need to resolve, updates last_resolved, then
      does network I/O and updates pds/handle.
    """
    now = datetime.now(timezone.utc)

    async with accounts_lock:
        entry = accounts.get(did)
        if entry is None:
            # We somehow got a DID that no longer exists; ignore.
            return

        need_resolve = False
        lr = entry.get("last_resolved")

        if force_resolve:
            need_resolve = True
        else:
            if lr is None:
                need_resolve = True
            elif isinstance(lr, datetime):
                age = (now - lr).total_seconds()
                if age >= resolve_ttl_seconds:
                    need_resolve = True

        if not need_resolve:
            return

        # Mark that we attempted a resolution at this time
        entry["last_resolved"] = now

    # Network work OUTSIDE the lock
    doc = await resolve_did_document(did, session)
    if not doc:
        return

    pds = extract_pds_from_diddoc(doc)
    handle = extract_handle_from_diddoc(doc)

    async with accounts_lock:
        entry2 = accounts.get(did)
        if entry2 is None:
            return
        if pds is not None:
            entry2["pds"] = pds
        if handle is not None:
            entry2["handle"] = handle


# -----------------------------
# Update on firehose events
# -----------------------------
async def update_account_and_maybe_queue(
    did: str,
    *,
    force_resolve: bool,
    resolve_queue: "asyncio.Queue[Tuple[str, bool]]",
) -> None:
    """
    Update last_seen for DID and decide whether to enqueue for resolution.

    We keep this very cheap: only touch the dict and do simple TTL logic,
    then push to the queue. No network here.
    """
    now = datetime.now(timezone.utc)

    async with accounts_lock:
        entry = accounts.get(did)
        if entry is None:
            entry = {
                "pds": None,
                "handle": None,
                "last_seen": now,
                "last_resolved": None,
            }
            accounts[did] = entry
        else:
            entry["last_seen"] = now

        # Decide if we should enqueue resolution:
        need_resolve = False

        if force_resolve:
            need_resolve = True
        else:
            lr = entry.get("last_resolved")
            if lr is None:
                need_resolve = True
            elif isinstance(lr, datetime):
                age = (now - lr).total_seconds()
                if age >= RESOLVE_TTL_SECONDS:
                    need_resolve = True

    if need_resolve:
        try:
            resolve_queue.put_nowait((did, force_resolve))
        except asyncio.QueueFull:
            # We could log this once or keep a counter if you care.
            pass


# -----------------------------
# Resolution worker
# -----------------------------
async def resolution_worker(
    resolve_queue: "asyncio.Queue[Tuple[str, bool]]",
    session: aiohttp.ClientSession,
    worker_id: int,
) -> None:
    """
    Background worker that processes DID resolution tasks from the queue.
    """
    while True:
        did, force = await resolve_queue.get()
        try:
            await resolve_if_needed(did, session, force_resolve=force)
        finally:
            resolve_queue.task_done()


# -----------------------------
# Firehose callback
# -----------------------------
def make_on_message_handler(
    resolve_queue: "asyncio.Queue[Tuple[str, bool]]",
):
    async def on_message_handler(message) -> None:
        evt = parse_subscribe_repos_message(message)

        did: Optional[str] = None
        force_resolve = False

        if isinstance(evt, models.ComAtprotoSyncSubscribeRepos.Commit):
            did = evt.repo
        elif isinstance(evt, models.ComAtprotoSyncSubscribeRepos.Account):
            # Account events should force an immediate re-resolve
            did = evt.did
            force_resolve = True
        elif isinstance(evt, models.ComAtprotoSyncSubscribeRepos.Identity):
            did = evt.did
        else:
            return

        if did is None:
            return

        # Fast path: update last_seen, TTL-check, enqueue resolution if needed.
        await update_account_and_maybe_queue(
            did,
            force_resolve=force_resolve,
            resolve_queue=resolve_queue,
        )

    return on_message_handler


# -----------------------------
# CLI parsing
# -----------------------------
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Track accounts seen on an ATProto relay firehose."
    )
    parser.add_argument(
        "--relay",
        help=(
            "Base URI for the relay xrpc endpoint, e.g. "
            "wss://bsky.network/xrpc or "
            "wss://relay1.us-east.bsky.network/xrpc "
            "(default: library default)"
        ),
        default=None,
    )
    parser.add_argument(
        "--snapshot-file",
        help=f"Path to snapshot JSON file (default: {SNAPSHOT_FILE})",
        default=SNAPSHOT_FILE,
    )
    parser.add_argument(
        "--snapshot-interval",
        type=int,
        help=f"Seconds between periodic snapshots (default: {SNAPSHOT_INTERVAL})",
        default=SNAPSHOT_INTERVAL,
    )
    parser.add_argument(
        "--resolve-workers",
        type=int,
        help=f"Number of concurrent DID resolution workers (default: {RESOLUTION_WORKERS})",
        default=RESOLUTION_WORKERS,
    )
    parser.add_argument(
        "--resolve-ttl-seconds",
        type=int,
        help=f"Re-resolve DIDs older than this many seconds (default: {RESOLVE_TTL_SECONDS})",
        default=RESOLVE_TTL_SECONDS,
    )
    return parser.parse_args()


# -----------------------------
# Main async logic
# -----------------------------
async def run(args: argparse.Namespace) -> None:
    snapshot_path = args.snapshot_file
    snapshot_interval = args.snapshot_interval

    # Adjust TTL if overridden
    global RESOLVE_TTL_SECONDS
    RESOLVE_TTL_SECONDS = args.resolve_ttl_seconds

    loop = asyncio.get_running_loop()

    # Install SIGUSR1 handler to trigger an immediate snapshot
    def on_sigusr1():
        print(f"Received SIGUSR1, scheduling snapshot to {snapshot_path}")
        asyncio.create_task(async_save_snapshot(snapshot_path, verbose=True))

    try:
        loop.add_signal_handler(signal.SIGUSR1, on_sigusr1)
    except (NotImplementedError, AttributeError):
        print("SIGUSR1 handler not available on this platform.")

    resolve_queue: "asyncio.Queue[Tuple[str, bool]]" = asyncio.Queue(
        maxsize=RESOLUTION_QUEUE_SIZE
    )

    async with aiohttp.ClientSession() as session:
        if args.relay:
            client = AsyncFirehoseSubscribeReposClient(base_uri=args.relay)
            print(f"Using relay: {args.relay}")
        else:
            client = AsyncFirehoseSubscribeReposClient()
            print("Using default relay from atproto library")

        # Start resolution workers
        for i in range(args.resolve_workers):
            asyncio.create_task(resolution_worker(resolve_queue, session, i))

        on_message = make_on_message_handler(resolve_queue)

        # Background task: periodic snapshot (no stdout prints)
        asyncio.create_task(periodic_snapshot(snapshot_path, snapshot_interval))

        try:
            await client.start(on_message)
        finally:
            # On exit, save one last snapshot
            await async_save_snapshot(snapshot_path, verbose=True)


# -----------------------------
# Entry point
# -----------------------------
def main():
    args = parse_args()
    # Load snapshot BEFORE starting the event loop (no need for locking yet)
    load_snapshot(args.snapshot_file)
    asyncio.run(run(args))


if __name__ == "__main__":
    main()

