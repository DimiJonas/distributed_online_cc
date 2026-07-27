#!/usr/bin/env python3
"""
compare the result databases of the distributed and the central system (and optionally the generators 'ground truth').

for every (case_id, constraint_id) pair the final result is compared (the last row written for that pair)

Reported per pair, in three categories:
  DIFFERENT  - both systems have a result but they disagree
  MISSING    - one system has a result, the other has no row at all
  WRONG      - (only with --expected) the systems agree with each other but not with the generated 'ground truth'

Usage:
  compare_results.py --distributed run/distributed-results.db \
                     --central run/central-results.db \
                     [--expected run/events/expected_results.jsonl] \
                     [--show-info] [--show-matches]
"""

import argparse
import json
import sqlite3
import sys
from pathlib import Path


def load_final_results(db_path: str):
    # (case_id, constraint_id) -> (result, info, created_at, n_rows) of the last row
    if not Path(db_path).exists():
        sys.exit(f"error: database not found: {db_path}")
    con = sqlite3.connect(db_path)
    final = {}
    counts = {}
    try:
        rows = con.execute(
            "SELECT case_id, constraint_id, result, info, created_at "
            "FROM results ORDER BY id"
        )
    except sqlite3.OperationalError as e:
        sys.exit(f"error: cannot read results from {db_path}: {e}")
    for case_id, cid, result, info, created_at in rows:
        key = (case_id, cid)
        counts[key] = counts.get(key, 0) + 1
        final[key] = (result, info, created_at)
    return {k: (*v, counts[k]) for k, v in final.items()}


def load_expected(path: str):
    if not Path(path).exists():
        sys.exit(f"error: expected results file not found: {path}")
    out = {}
    for line in open(path):
        line = line.strip()
        if not line:
            continue
        j = json.loads(line)
        out[(j["case_id"], j["constraint_id"])] = j["expected"]
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--distributed", default="run/distributed/results.db", help="path to the distributed system's sqlite db")
    ap.add_argument("--central", default="run/central/results.db", help="path to the central system's sqlite db")
    ap.add_argument("--expected", default=None, help="optional expected_results.jsonl from gen-events")
    ap.add_argument("--show-info", action="store_true", help="print the info text of differing results")
    ap.add_argument("--show-matches", action="store_true", help="also list all matching pairs")
    args = ap.parse_args()

    dist = load_final_results(args.distributed)
    cent = load_final_results(args.central)
    expected = load_expected(args.expected) if args.expected else None

    keys = sorted(set(dist) | set(cent) | (set(expected) if expected else set()))

    different, missing, wrong, matches = [], [], [], []
    for k in keys:
        d = dist.get(k)
        c = cent.get(k)
        d_res = d[0] if d else None
        c_res = c[0] if c else None

        if d_res is None or c_res is None:
            missing.append((k, d_res, c_res))
            continue
        if d_res != c_res:
            different.append((k, d, c))
            continue
        # the systems agree; check against the ground truth if provided
        if expected is not None:
            e = expected.get(k)
            if e is not None and e != d_res:
                wrong.append((k, e, d_res))
                continue
        matches.append((k, d_res))

    for (case, cid), d, c in different:
        print(f"DIFFERENT {case}/{cid}: distributed={d[0]} central={c[0]}")
        if args.show_info:
            print(f"    distributed: {d[1]}  ({d[2]}, {d[3]} row(s))")
            print(f"    central:     {c[1]}  ({c[2]}, {c[3]} row(s))")
    for (case, cid), d_res, c_res in missing:
        side = "central" if d_res else "distributed"
        have = d_res or c_res
        print(f"MISSING   {case}/{cid}: no result in the {side} db "
              f"(other side says {have})")
    for (case, cid), e, got in wrong:
        print(f"WRONG     {case}/{cid}: both systems say {got}, "
              f"ground truth expects {e}")
    if args.show_matches:
        for (case, cid), res in matches:
            print(f"MATCH     {case}/{cid}: {res}")

    n_bad = len(different) + len(missing) + len(wrong)
    print(f"\n{len(keys)} (case, constraint) pairs compared: "
          f"{len(matches)} match, {len(different)} differ, "
          f"{len(missing)} missing on one side"
          + (f", {len(wrong)} wrong vs ground truth" if expected else ""))
    sys.exit(1 if n_bad else 0)


if __name__ == "__main__":
    main()