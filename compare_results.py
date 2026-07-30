#!/usr/bin/env python3
"""
compare the result databases of the distributed and the central system (and optionally the generators 'ground truth').

for every (case_id, constraint_id) pair the final result is compared (the last row written for that pair)

Reported per pair:
  DIFFERENT  - both systems have a result but they disagree (with --expected: also says which side matches the ground truth)
  MISSING    - one system has a result, the other has no row at all
  ABSENT     - (only with --expected) neither system produced a result
  WRONG      - (only with --expected) the systems agree with each other but not with the ground truth

Usage:
  compare_results.py --distributed run/distributed/cc-results.db \
                     --central run/central/cc-results.db \
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
    ap.add_argument("--distributed", default="run/distributed/cc-results.db", help="path to the distributed system's sqlite db")
    ap.add_argument("--central", default="run/central/cc-results.db", help="path to the central system's sqlite db")
    ap.add_argument("--expected", default=None, help="optional expected_results.jsonl from gen-events")
    ap.add_argument("--show-info", action="store_true", help="print the info text of differing results")
    ap.add_argument("--show-matches", action="store_true", help="also list all matching pairs")
    args = ap.parse_args()

    dist = load_final_results(args.distributed)
    cent = load_final_results(args.central)
    expected = load_expected(args.expected) if args.expected else None

    keys = sorted(set(dist) | set(cent) | (set(expected) if expected else set()))

    different, missing, absent, wrong, matches = [], [], [], [], []
    for k in keys:
        d = dist.get(k)
        c = cent.get(k)
        d_res = d[0] if d else None
        c_res = c[0] if c else None
        e = expected.get(k) if expected else None

        if d_res is None and c_res is None:
            absent.append((k, e))
            continue
        if d_res is None or c_res is None:
            missing.append((k, d_res, c_res, e))
            continue
        if d_res != c_res:
            different.append((k, d, c, e))
            continue
        # central and distributed are equal - check against the ground truth if provided
        if e is not None and e != d_res:
            wrong.append((k, e, d_res))
            continue
        matches.append((k, d_res))

    for (case, cid), d, c, e in different:
        line = f"DIFFERENT {case}/{cid}: distributed={d[0]} central={c[0]}"
        if e is not None:
            if e == d[0]:
                line += f"  -> ground truth {e}: distributed is right, central is wrong"
            elif e == c[0]:
                line += f"  -> ground truth {e}: central is right, distributed is wrong"
            else:
                line += f"  -> ground truth {e}: both are wrong"
        print(line)
        if args.show_info:
            print(f"    distributed: {d[1]}  ({d[2]}, {d[3]} row(s))")
            print(f"    central:     {c[1]}  ({c[2]}, {c[3]} row(s))")
    for (case, cid), d_res, c_res, e in missing:
        side = "central" if d_res else "distributed"
        have = d_res or c_res
        line = (f"MISSING   {case}/{cid}: no result in the {side} db "
                f"(other side says {have})")
        if e is not None:
            line += f"  [ground truth {e}]"
        print(line)
    for (case, cid), e in absent:
        print(f"ABSENT    {case}/{cid}: neither system produced a result "
              f"(ground truth expects {e})")
    for (case, cid), e, got in wrong:
        print(f"WRONG     {case}/{cid}: both systems say {got}, "
              f"ground truth expects {e}")
    if args.show_matches:
        for (case, cid), res in matches:
            print(f"MATCH     {case}/{cid}: {res}")

    n_bad = len(different) + len(missing) + len(absent) + len(wrong)
    summary = (f"\n{len(keys)} (case, constraint) pairs compared: "
               f"{len(matches)} match, {len(different)} differ, "
               f"{len(missing)} missing on one side")
    if expected:
        summary += f", {len(absent)} absent from both, {len(wrong)} wrong vs ground truth"
        d_right = sum(1 for _, d, _, e in different if e is not None and e == d[0])
        c_right = sum(1 for _, _, c, e in different if e is not None and e == c[0])
        both_wrong = len(different) - d_right - c_right
        if different:
            summary += (f"\nof the {len(different)} disagreements: "
                        f"distributed correct {d_right}x, central correct {c_right}x, "
                        f"both wrong {both_wrong}x")
    print(summary)
    sys.exit(1 if n_bad else 0)


if __name__ == "__main__":
    main()