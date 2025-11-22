#!/usr/bin/env python3
import argparse
import asyncio
import json
import os
import signal
from datetime import datetime, timezone
from typing import Dict, Optional

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

# -----------------------------
# In-memory state
# -----------------------------
# did -> {
#   "pds": str | None,
#   "handle": str | None,
#   "last_seen": datetime,
#   "last_resolved": datetime | None,
# }
accounts: Dict[str, Dict[str, object]] = {}
accounts_lock = asyncio.Lock()


# -----------------------------
# Snapshot load/save
# -----------------------------
def load_snapshot(path: str) -> None:
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
            # If parsing fails, skip that entry
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


def save_snapshot(path: str, verbose: bool = True) -> None:
    data = {}
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

        data[did] = {
            "pds": entry.get("pds"),
            "handle": entry.get("handle"),
            "last_seen": last_seen_str,
            "last_resolved": last_resolved_str,
        }

    tmp_path = path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=True)
    os.replace(tmp_path, path)

    if verbose:
        print(f"Saved snapshot to {path}: {len(data)} accounts")

async def async_save_snapshot(path: str, verbose: bool = True) -> None:
    # Make a deep-ish copy under lock
    async with accounts_lock:
        snapshot_copy = {
            did: {
                "pds": e.get("pds"),
                "handle": e.get("handle"),
                "last_seen": (
                    e["last_seen"].isoformat()
                    if isinstance(e["last_seen"], datetime)
                    else str(e["last_seen"])
                ),
                "last_resolved": (
                    e["last_resolved"].isoformat()
                    if isinstance(e.get("last_resolved"), datetime)
                    else None
                ),
            }
            for did, e in accounts.items()
        }

    # File writing happens outside the lock
    await asyncio.to_thread(_write_snapshot_file, path, snapshot_copy, verbose)

def _write_snapshot_file(path: str, snapshot_copy: dict, verbose: bool):
    tmp_path = path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(snapshot_copy, f, indent=2, sort_keys=True)
    os.replace(tmp_path, path)
    if verbose:
        print(f"Saved snapshot to {path}: {len(snapshot_copy)} accounts")

async def periodic_snapshot(path: str, interval_seconds: int) -> None:
    """
    Periodically write the current state to disk.
    No stdout prints from here; it just rewrites the JSON file.
    """
    while True:
        await asyncio.sleep(interval_seconds)
        await async_save_snapshot(path, verbose=False)


# -----------------------------
# DID / PDS resolution helpers
# -----------------------------
async def resolve_did_document(
    did: str, session: aiohttp.ClientSession, timeout_seconds: float = 5.0
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
        # Extend here for other DID methods if they start showing up.
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

async def resolve_and_update_entry(did, entry, session):
    now = datetime.now(timezone.utc)
    async with accounts_lock:
        entry["last_resolved"] = now

    doc = await resolve_did_document(did, session)
    if not doc:
        return

    pds = extract_pds_from_diddoc(doc)
    handle = extract_handle_from_diddoc(doc)

    async with accounts_lock:
        if pds is not None:
            entry["pds"] = pds
        if handle is not None:
            entry["handle"] = handle

async def update_account(
    did: str,
    session: aiohttp.ClientSession,
    *,
    force_resolve: bool = False,
    resolve_ttl_seconds: int = RESOLVE_TTL_SECONDS,
) -> None:
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

        need_resolve = False

        if force_resolve:
            need_resolve = True
        else:
            lr = entry.get("last_resolved")
            if lr is None:
                need_resolve = True
            elif isinstance(lr, datetime):
                age = (now - lr).total_seconds()
                if age >= resolve_ttl_seconds:
                    need_resolve = True

    # Do the network resolution OUTSIDE the lock
    if need_resolve:
        await resolve_and_update_entry(did, entry, session)

# -----------------------------
# Firehose callback
# -----------------------------
def make_on_message_handler(session: aiohttp.ClientSession):
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
            # Ignore other event types
            return

        if did is None:
            return

        await update_account(did, session, force_resolve=force_resolve)

        entry = accounts[did]
        # Still log individual events – easy enough to pipe through logger later.
        print(
            f"[{entry['last_seen'].isoformat()}] "
            f"did={did} "
            f"handle={entry.get('handle') or '-'} "
            f"pds={entry.get('pds') or '-'} "
            f"last_resolved="
            f"{entry.get('last_resolved').isoformat() if entry.get('last_resolved') else '-'}"
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
    return parser.parse_args()


# -----------------------------
# Main async run
# -----------------------------
async def run(args: argparse.Namespace) -> None:
    snapshot_path = args.snapshot_file
    snapshot_interval = args.snapshot_interval

    # Load snapshot BEFORE starting
    load_snapshot(snapshot_path)

    loop = asyncio.get_running_loop()

    # Install SIGUSR1 handler to trigger an immediate snapshot
    def on_sigusr1():
        print(f"Received SIGUSR1, scheduling snapshot to {snapshot_path}")
        asyncio.create_task(async_save_snapshot(snapshot_path, verbose=True))

    try:
        loop.add_signal_handler(signal.SIGUSR1, on_sigusr1)
    except (NotImplementedError, AttributeError):
        # Not available on some platforms (e.g., Windows)
        print("SIGUSR1 handler not available on this platform.")

    async with aiohttp.ClientSession() as session:
        if args.relay:
            client = AsyncFirehoseSubscribeReposClient(base_uri=args.relay)
            print(f"Using relay: {args.relay}")
        else:
            client = AsyncFirehoseSubscribeReposClient()
            print("Using default relay from atproto library")

        on_message = make_on_message_handler(session)

        # Background task: periodic snapshot (no stdout prints)
        asyncio.create_task(periodic_snapshot(snapshot_path, snapshot_interval))

        try:
            # This will run until interrupted; auto-reconnects on inactivity.
            await client.start(on_message)
        finally:
            # On exit, save one last snapshot (with a log line)
            await async_save_snapshot(snapshot_path, verbose=True)


# -----------------------------
# Entry point
# -----------------------------
if __name__ == "__main__":
    args = parse_args()
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        # Best-effort snapshot on Ctrl+C, with the correct path
        save_snapshot(args.snapshot_file, verbose=True)
        print("Shutdown via KeyboardInterrupt, snapshot saved.")
