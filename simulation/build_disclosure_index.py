#!/usr/bin/env python3.11
"""build_disclosure_index.py - precompute the ground truth disclosure of an event stream contributes for the privacy evaluation

reads the streams manifest.jsonl
writes disclosure_index.json.gz next to the manifest

compute_privacy.py needs three things from a stream's manifest.jsonl:

  the compacted log      per case, the activity multiset and the trace. used for case exposure and to build background knowledge candidates
  the variant index      the distinct traces with their frequencies
  the log baseline       the denominators quoted next to every disclosure number: trace uniqueness, distinct traces, background knowledge sizes and the
                         disclosure of the log itself under full knowledge

  # real log: build the variant index too, it is what the disclosure measures resolve against
  python3.11 simulation/build_disclosure_index.py --manifest results/_streams/sepsis/manifest.jsonl --bk-type set mult seq

  # synthetic stream: the log alone, so the stream directory can be deleted afterwards
  python3.11 simulation/build_disclosure_index.py --manifest results/_streams/synthetic_<key>/manifest.jsonl --no-disclosure
"""

from __future__ import annotations

import argparse
import gzip
import json
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from compute_privacy import (BK_TYPES, DisclosureIndex, load_ground_truth, log_baseline_disclosure)

SCHEMA_VERSION = 1
DEFAULT_NAME = "disclosure_index.json.gz"


def index_path_for(manifest: Path) -> Path:
    return manifest.parent / DEFAULT_NAME


def build(manifest: Path, bk_types: tuple[str, ...]) -> dict:
    log = load_ground_truth(manifest)
    if not log:
        raise SystemExit(f"{manifest}: no cases parsed, is this a manifest.jsonl?")

    index = DisclosureIndex(log) if bk_types else None
    baseline = log_baseline_disclosure(log, index, bk_types)

    payload = {
        "schema_version": SCHEMA_VERSION,
        "manifest": str(manifest.resolve()),
        "manifest_bytes": manifest.stat().st_size,
        "bk_types": list(bk_types),
        # the compacted log
        "cases": {case_id: {"activities": case["activities"], "trace": list(case["trace"])}
                  for case_id, case in log.items()},
        # the log baseline under the bk types this file was built with
        "baselines": {"|".join(bk_types): baseline},
    }
    if index is not None:
        # activity_mask and count_mask are rebuilt from the variants in a single pass, so only the variants and their frequencies are stored
        payload["variants"] = [list(variant) for variant in index.variants]
        payload["variant_counts"] = list(index.counts)
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--manifest", type=Path, required=True, action="append",
                        help="manifest.jsonl of the stream (repeatable)")
    parser.add_argument("--out", type=Path, default=None,
                        help=f"output path (default: {DEFAULT_NAME} next to each manifest)")
    parser.add_argument("--bk-type", nargs="+", choices=BK_TYPES, default=["set", "mult", "seq"],
                        help="background knowledge types to build the variant index and baseline for "
                             "(default: all three). only meaningful on a real log")
    parser.add_argument("--no-disclosure", action="store_true",
                        help="compacted log only, no variant index. use this on synthetic streams, where "
                             "nearly every trace is unique so the disclosure measures describe the "
                             "generator rather than the deployment")
    parser.add_argument("--force", action="store_true", help="rebuild even if the output already exists")
    args = parser.parse_args()

    if args.out and len(args.manifest) > 1:
        return parser.error("--out takes a single manifest")

    bk_types = () if args.no_disclosure else tuple(args.bk_type)
    for manifest in args.manifest:
        if not manifest.exists():
            print(f"skip {manifest}: not found")
            continue
        out_path = args.out or index_path_for(manifest)
        if out_path.exists() and not args.force:
            print(f"skip {out_path}: already there (--force to rebuild)")
            continue
        payload = build(manifest, bk_types)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(out_path, "wt") as handle:
            json.dump(payload, handle)
        baseline = payload["baselines"]["|".join(bk_types)]
        print(f"wrote {out_path} ({out_path.stat().st_size / 1e6:.1f} MB): "
              f"{baseline['cases']} cases, {baseline['events']} events, "
              f"{baseline['distinct_traces']} distinct traces, "
              f"trace uniqueness {baseline['trace_uniqueness']:.3f}"
              + (f", variant index for {', '.join(bk_types)}" if bk_types else ", no variant index"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())