#!/usr/bin/env python3
import os
import sys
import argparse
import shutil


def parse_args():
    p = argparse.ArgumentParser(
        description="Move <did>.json files listed in a DID file into O/ (dry-run by default)"
    )
    p.add_argument("did_file", help="File with one DID per line")
    p.add_argument("source_dir", help="Directory containing <did>.json files")
    p.add_argument(
        "-m", "--move",
        action="store_true",
        help="Actually move the files (default is dry-run)"
    )
    p.add_argument(
        "-o", "--outdir",
        default="O",
        help="Output directory (default: ./O)"
    )
    return p.parse_args()


def main():
    args = parse_args()

    os.makedirs(args.outdir, exist_ok=True)

    with open(args.did_file, "r", encoding="utf-8") as f:
        for line in f:
            did = line.strip()
            if not did:
                continue

            src = os.path.join(args.source_dir, f"{did}.json")
            dst = os.path.join(args.outdir, f"{did}.json")

            if os.path.exists(src):
                if args.move:
                    print(f"MOVING  {src} -> {dst}")
                    shutil.move(src, dst)
                else:
                    print(f"WOULD MOVE  {src} -> {dst}")
            else:
                print(f"MISSING  {src}")


if __name__ == "__main__":
    main()

