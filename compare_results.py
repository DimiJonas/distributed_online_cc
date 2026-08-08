#!/usr/bin/env python3
"""
compare the result databases of the decentralized and the central system (and optionally the generators 'ground truth').

for every (case_id, constraint_id) pair the final (by emission or by insertion) result is compared

Reported per pair:
  DIFFERENT  - both systems have a result but they disagree (with --expected: also says which side matches the ground truth)
  MISSING    - one system has a result, the other has no row at all
  ABSENT     - (only with --expected) neither system produced a result
  WRONG      - (only with --expected) the systems agree with each other but not with the ground truth

Usage:
  compare_results.py --decentralized run/decentralized/cc-results.db \
                     --central run/central/cc-results.db \
                     [--expected run/events/expected_results.jsonl] \
                     [--rank-by emission|insertion] [--rank-report] \
                     [--show-info] [--show-history] [--show-matches]
"""

import argparse
import json
import re
import sqlite3
import sys
from pathlib import Path

_TS_RE = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2}):(\d{2})"
    r"(?:\.(\d{1,9}))?"
    r"(Z|z|[+-]\d{2}:?\d{2})?$"
)


def parse_created_at(s):
    # parse an RFC3339 timestamp into integer nanoseconds since the epoch.datetime.fromisoformat truncates to microseconds
    m = _TS_RE.match(s.strip())
    if not m:
        raise ValueError(f"unparseable created_at: {s!r}")
    year, month, day, hour, minute, second, frac, offset = m.groups()

    # days since epoch via the civil-from-days algorithm (no calendar imports needed)
    y, mo, d = int(year), int(month), int(day)
    y -= mo <= 2
    era = (y if y >= 0 else y - 399) // 400
    yoe = y - era * 400
    doy = (153 * (mo + (-3 if mo > 2 else 9)) + 2) // 5 + d - 1
    doe = yoe * 365 + yoe // 4 - yoe // 100 + doy
    days = era * 146097 + doe - 719468

    secs = days * 86400 + int(hour) * 3600 + int(minute) * 60 + int(second)
    nanos = int((frac or "").ljust(9, "0")) if frac else 0

    if offset and offset not in ("Z", "z"):
        sign = 1 if offset[0] == "+" else -1
        off = offset[1:].replace(":", "")
        secs -= sign * (int(off[:2]) * 3600 + int(off[2:]) * 60)

    return secs * 1_000_000_000 + nanos


def load_final_results(db_path: str, rank_by: str):
    # (case_id, constraint_id) -> (result, info, created_at, n_rows, reordered)
    # 'reordered is true when ranking by emission time picks a different row than ranking by insertion order would have
    if not Path(db_path).exists():
        sys.exit(f"error: database not found: {db_path}")
    con = sqlite3.connect(db_path)
    try:
        rows = con.execute(
            "SELECT case_id, constraint_id, result, info, created_at, id FROM results"
        ).fetchall()
    except sqlite3.OperationalError as e:
        sys.exit(f"error: cannot read results from {db_path}: {e}")

    revisions = {}
    for case_id, cid, result, info, created_at, row_id in rows:
        try:
            ts = parse_created_at(created_at)
        except ValueError as e:
            sys.exit(f"error: in {db_path}: {e}")
        revisions.setdefault((case_id, cid), []).append(
            (ts, row_id, result, info, created_at)
        )

    final = {}
    for key, revs in revisions.items():
        by_emission = max(revs, key=lambda r: (r[0], r[1]))
        by_insertion = max(revs, key=lambda r: r[1])
        chosen = by_emission if rank_by == "emission" else by_insertion
        reordered = by_emission[1] != by_insertion[1]
        final[key] = (chosen[2], chosen[3], chosen[4], len(revs), reordered)

    return final, revisions


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


def print_history(label, revs):
    for ts, row_id, result, info, created_at in sorted(revs, key=lambda r: (r[0], r[1])):
        print(f"      {label:14s} id={row_id:<7} {created_at}  {result:<20} {info}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--decentralized", default="run/decentralized/cc-results.db", help="path to the decentralized system's sqlite db")
    ap.add_argument("--central", default="run/central/cc-results.db", help="path to the central system's sqlite db")
    ap.add_argument("--expected", default=None, help="optional expected_results.jsonl from gen-events")
    ap.add_argument("--rank-by", choices=["emission", "insertion"], default="emission",
                    help="which revision counts as final: 'emission' uses created_at (default), 'insertion' uses the sqlite id (old behaviour)")
    ap.add_argument("--rank-report", action="store_true", help="report how many pairs were reordered between emission and insertion")
    ap.add_argument("--show-info", action="store_true", help="print the info text of differing results")
    ap.add_argument("--show-history", action="store_true", help="print every revision of differing pairs")
    ap.add_argument("--show-matches", action="store_true", help="also list all matching pairs")
    args = ap.parse_args()

    decent, decent_revs = load_final_results(args.decentralized, args.rank_by)
    cent, cent_revs = load_final_results(args.central, args.rank_by)
    expected = load_expected(args.expected) if args.expected else None

    keys = sorted(set(decent) | set(cent) | (set(expected) if expected else set()))

    different, missing, absent, wrong, matches = [], [], [], [], []
    for k in keys:
        d = decent.get(k)
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
        # central and decentralized are equal - check against the ground truth if provided
        if e is not None and e != d_res:
            wrong.append((k, e, d_res))
            continue
        matches.append((k, d_res))

    for (case, cid), d, c, e in different:
        line = f"DIFFERENT {case}/{cid}: decentralized={d[0]} central={c[0]}"
        if e is not None:
            if e == d[0]:
                line += f"  -> ground truth {e}: decentralized is right, central is wrong"
            elif e == c[0]:
                line += f"  -> ground truth {e}: central is right, decentralized is wrong"
            else:
                line += f"  -> ground truth {e}: both are wrong"
        print(line)
        if args.show_info:
            print(f"    decentralized: {d[1]}  ({d[2]}, {d[3]} revision(s){', reordered in transit' if d[4] else ''})")
            print(f"    central:       {c[1]}  ({c[2]}, {c[3]} revision(s){', reordered in transit' if c[4] else ''})")
        if args.show_history:
            print("    revision history:")
            print_history("decentralized", decent_revs.get((case, cid), []))
            print_history("central", cent_revs.get((case, cid), []))
    for (case, cid), d_res, c_res, e in missing:
        side = "central" if d_res else "decentralized"
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
    summary = (f"\n{len(keys)} (case, constraint) pairs compared "
               f"[final revision ranked by {args.rank_by}]: "
               f"{len(matches)} match, {len(different)} differ, "
               f"{len(missing)} missing on one side")
    if expected:
        summary += f", {len(absent)} absent from both, {len(wrong)} wrong vs ground truth"
        d_right = sum(1 for _, d, _, e in different if e is not None and e == d[0])
        c_right = sum(1 for _, _, c, e in different if e is not None and e == c[0])
        both_wrong = len(different) - d_right - c_right
        if different:
            summary += (f"\nof the {len(different)} disagreements: "
                        f"decentralized correct {d_right}x, central correct {c_right}x, "
                        f"both wrong {both_wrong}x")
    print(summary)

    if args.rank_report:
        for label, final, revs in (("decentralized", decent, decent_revs), ("central", cent, cent_revs)):
            multi = sum(1 for v in revs.values() if len(v) > 1)
            reordered = sum(1 for v in final.values() if v[4])
            print(f"{label:14s}: {len(revs)} pairs, {multi} with >1 revision, "
                  f"{reordered} where the sink reordered the final revision")

    sys.exit(1 if n_bad else 0)


if __name__ == "__main__":
    main()