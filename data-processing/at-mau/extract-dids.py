#!/usr/bin/env python3
import os
import sys
import json
import argparse


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("indir", help="Directory containing JSONL batch files")
    p.add_argument("--keep-duplicates", action="store_true",
                   help="Do not deduplicate DIDs")
    return p.parse_args()


def main():
    args = parse_args()

    dids = [] if args.keep_duplicates else set()

    for name in sorted(os.listdir(args.indir)):
        if not name.endswith(".jsonl"):
            continue

        path = os.path.join(args.indir, name)

        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue

                did = obj.get("did")
                if not did:
                    continue

                if args.keep_duplicates:
                    dids.append(did)
                else:
                    dids.add(did)

    # ✅ Output one DID per line
    if args.keep_duplicates:
        for did in dids:
            print(did)
    else:
        for did in sorted(dids):
            print(did)


if __name__ == "__main__":
    main()

