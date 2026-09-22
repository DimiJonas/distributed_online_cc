#!/usr/bin/env python3.11
"""sample_stream.py - take a case level sample of a stream produced by convert_log_to_stream.py

works on the CONVERTED stream, not on the XES log: it filters manifest.jsonl down to the sampled
cases, renumbers seq, and links the event files that survive.

why sample at all: traffic fines has ~150k cases and ~14 constraint pairs per case, so a full run
produces ~2.1M verdicts. the central node settles those at roughly 1000 pairs/s no matter
what rate is offered, which is ~33 minutes per run before anything else. a case sample cuts that
proportionally, and the privacy metrics do not need the whole log.

usage:
  sample_stream.py --stream traffic_fines/events --out traffic_fines/events_30k --cases 30000
  sample_stream.py --stream traffic_fines/events --cases 30000 --dry-run

after sampling:
  compute-expected-results --config-dir <a config dir> --manifest <out>/manifest.jsonl --out <out>/expected_results.jsonl
"""

from __future__ import annotations

import argparse
import json
import os
import random
from collections import Counter
from pathlib import Path


def read_manifest(manifest: Path) -> list[dict]:
    rows = []
    with open(manifest) as handle:
        for line in handle:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


# the same collapsing compute_privacy does: every row counts, including the synthetic end events.
# those append the same activity to every case, so they cannot merge or split variants
def variant_stats(rows: list[dict]) -> dict:
    traces: dict[str, list] = {}
    for row in rows:
        case_id, event_id = row.get("case_id"), row.get("event_id")
        if case_id is None or event_id is None:
            continue
        traces.setdefault(case_id, []).append(event_id)
    counts = Counter(tuple(trace) for trace in traces.values())
    singletons = sum(1 for count in counts.values() if count == 1)
    return {"cases": len(traces), "events": sum(len(t) for t in traces.values()),
            "distinct_traces": len(counts), "singleton_variants": singletons,
            "trace_uniqueness": (len(counts) / len(traces)) if traces else 0.0,
            "singleton_share": (singletons / len(traces)) if traces else 0.0,
            "largest_variant_share": (max(counts.values()) / len(traces)) if traces else 0.0,
            "activities": len({event for trace in traces.values() for event in trace})}


def report(label: str, stats: dict) -> None:
    print(f"  {label:<18} {stats['cases']:>8} cases  {stats['events']:>9} events  "
          f"{stats['distinct_traces']:>6} variants  uniqueness {stats['trace_uniqueness']:.4f}  "
          f"singleton share {stats['singleton_share']:.4f}  "
          f"largest variant {stats['largest_variant_share']:.1%}  {stats['activities']} activities")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--stream", type=Path, required=True,
                        help="directory holding manifest.jsonl and the event .xes files")
    parser.add_argument("--out", type=Path, default=None, help="output stream directory")
    parser.add_argument("--cases", type=int, required=True, help="how many cases to keep")
    parser.add_argument("--seed", type=int, default=1,
                        help="sampling seed, so the sample is reproducible (default 1)")
    parser.add_argument("--strategy", choices=("random", "first"), default="random",
                        help="random (default) keeps the variant mix; first takes a time slice and "
                             "is only for reproducing an earlier cut")
    parser.add_argument("--copy", action="store_true",
                        help="copy the event files instead of symlinking them. symlinks are the "
                             "default because they cost nothing, but they break if the source "
                             "stream is deleted")
    parser.add_argument("--dry-run", action="store_true", help="report the effect, write nothing")
    args = parser.parse_args()

    manifest = args.stream / "manifest.jsonl"
    if not manifest.exists():
        return parser.error(f"{manifest} not found")

    rows = read_manifest(manifest)
    full = variant_stats(rows)

    # order of first appearance, so `first` is a time slice and `random` is reproducible
    case_order: list[str] = []
    seen: set[str] = set()
    for row in rows:
        case_id = row.get("case_id")
        if case_id is not None and case_id not in seen:
            seen.add(case_id)
            case_order.append(case_id)

    if args.cases >= len(case_order):
        print(f"{args.cases} >= {len(case_order)} cases in the stream, nothing to sample")
        return 0

    if args.strategy == "first":
        keep = set(case_order[:args.cases])
    else:
        keep = set(random.Random(args.seed).sample(case_order, args.cases))

    kept_rows = [row for row in rows if row.get("case_id") in keep]
    sample = variant_stats(kept_rows)

    print(f"\n{args.strategy} sample of {args.cases} cases (seed {args.seed})")
    report("full stream", full)
    report("sample", sample)
    ratio = (sample["trace_uniqueness"] / full["trace_uniqueness"]) if full["trace_uniqueness"] else float("inf")
    print(f"\n  events kept        {sample['events'] / full['events']:.1%}"
          f"   -> expect run time and verdict count to scale by about the same")
    print(f"  uniqueness moved   {full['trace_uniqueness']:.4f} -> {sample['trace_uniqueness']:.4f}"
          f"  ({ratio:.2f}x)")
    if sample["activities"] < full["activities"]:
        print(f"  !! the sample lost {full['activities'] - sample['activities']} activity/activities. "
              f"the node configs are built from the catalog, not from the stream, so those nodes "
              f"would sit idle - raise --cases or check the sample")
    # a ratio on two near zero numbers is arithmetic, not a problem: 0.0001 -> 0.0012 is "24x" and
    # still means essentially no case is identifiable by its trace. the warning needs the sampled
    # value to be large in absolute terms as well
    if ratio > 1.5 and sample["trace_uniqueness"] > 0.05:
        print("  !! uniqueness rose by more than half AND is now substantial. the disclosure "
              "measures resolve against the variant distribution, so quote both numbers or sample "
              "less aggressively")
    elif sample["trace_uniqueness"] <= 0.05:
        print("  both uniqueness figures are near zero, so the sample is safe for the disclosure "
              "measures: almost no case is identifiable by its trace in either")

    if args.dry_run or args.out is None:
        print("\n(dry run, nothing written)" if args.dry_run else "\n(no --out given, nothing written)")
        return 0

    args.out.mkdir(parents=True, exist_ok=True)
    source_dir = args.stream.resolve()
    written = 0
    with open(args.out / "manifest.jsonl", "w") as handle:
        for seq, row in enumerate(kept_rows):
            # seq fixes the order of the stream, so it has to be dense in the new manifest
            row = dict(row, seq=seq)
            handle.write(json.dumps(row) + "\n")
            name = row.get("file")
            if not name:
                continue
            target, link = source_dir / name, args.out / name
            if link.exists() or link.is_symlink():
                continue
            if args.copy:
                link.write_bytes(target.read_bytes())
            else:
                os.symlink(target, link)
            written += 1

    print(f"\nwrote {args.out}/manifest.jsonl: {len(kept_rows)} events, "
          f"{written} event files {'copied' if args.copy else 'symlinked'}")
    print("next:")
    print(f"  compute-expected-results --config-dir <config dir> "
          f"--manifest {args.out}/manifest.jsonl --out {args.out}/expected_results.jsonl")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())