#!/usr/bin/env python3
"""
compare the result databases of the decentralized and the central system (and optionally the generators 'ground truth').

for every (case_id, constraint_id) pair the final result is compared

which revision counts as final is decided by --rank-by:
  revision   - the per (case, constraint) counter the emitting node assigns at decision
               time (default)
  emission   - created_at, the wall clock stamp the handler took when it decided
  insertion  - the sqlite row id, i.e. arrival order at the sink

Reported per pair:
  DIFFERENT  - both systems have a result but they disagree (with --expected: also says which side matches the ground truth)
  MISSING    - one system has a result, the other has no row at all
  ABSENT     - (only with --expected) neither system produced a result
  WRONG      - (only with --expected) the systems agree with each other but not with the ground truth

Usage:
  compare_results.py --decentralized run/decentralized/cc-results.db \
                     --central run/central/cc-results.db \
                     [--expected run/events/expected_results.jsonl] \
                     [--rank-by revision|emission|insertion] [--rank-report] \
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


def has_revision_column(con):
    return any(row[1] == "revision" for row in con.execute("PRAGMA table_info(results)"))


def load_final_results(db_path: str, rank_by: str):
    # (case_id, constraint_id) -> (result, info, created_at, revision, n_rows, reordered)
    # 'reordered' is true when the chosen ranking picks a different row than ranking by insertion order
    if not Path(db_path).exists():
        sys.exit(f"error: database not found: {db_path}")
    con = sqlite3.connect(db_path)

    columns = "case_id, constraint_id, result, info, created_at, id, revision"
    try:
        rows = con.execute(f"SELECT {columns} FROM results").fetchall()
    except sqlite3.OperationalError as e:
        sys.exit(f"error: cannot read results from {db_path}: {e}")

    revs = {}
    for row in rows:
        case_id, constraint_id, result, info, created_at, row_id, revision = row[:7]
        try:
            ts = parse_created_at(created_at)
        except ValueError as e:
            sys.exit(f"error: in {db_path}: {e}")
        revs.setdefault((case_id, constraint_id), []).append(
            (ts, row_id, result, info, created_at, revision)
        )

    rankers = {
        # ties inside one ranking fall back to the row id so the choice is deterministic
        "revision": lambda r: (r[5], r[1]),
        "emission": lambda r: (r[0], r[1]),
        "insertion": lambda r: r[1],
    }
    rank_key = rankers[rank_by]

    final = {}
    for key, entries in revs.items():
        chosen = max(entries, key=rank_key)
        by_insertion = max(entries, key=rankers["insertion"])
        reordered = chosen[1] != by_insertion[1]
        final[key] = (
            chosen[2],
            chosen[3],
            chosen[4],
            chosen[5],
            len(entries),
            reordered,
        )

    return final, revs


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


def print_history(label, entries):
    for ts, row_id, result, info, created_at, revision in sorted(
        entries, key=lambda r: (r[5], r[1])
    ):
        print(
            f"      {label:14s} rev={revision:<4} id={row_id:<7} {created_at}  "
            f"{result:<20} {info}"
        )


def rank_disagreements(revs):
    # how many pairs would get a different final verdict depending on the ranking
    def final_result(entries, key):
        return max(entries, key=key)[2]

    by_revision = lambda r: (r[5], r[1])
    by_emission = lambda r: (r[0], r[1])
    by_insertion = lambda r: r[1]

    vs_emission = vs_insertion = 0
    for entries in revs.values():
        if len(entries) < 2:
            continue
        ref = final_result(entries, by_revision)
        if final_result(entries, by_emission) != ref:
            vs_emission += 1
        if final_result(entries, by_insertion) != ref:
            vs_insertion += 1
    return vs_emission, vs_insertion


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--decentralized", default="run/decentralized/cc-results.db", help="path to the decentralized system's sqlite db")
    ap.add_argument("--central", default="run/central/cc-results.db", help="path to the central system's sqlite db")
    ap.add_argument("--expected", default=None, help="optional expected_results.jsonl (from gen-events or expected-results)")
    ap.add_argument("--rank-by", choices=["revision", "emission", "insertion"], default="revision",
                    help="which revision counts as final: 'revision' uses the per (case, constraint) counter assigned by the emitting node (default)," \
                    "'emission' uses created_at, 'insertion' uses the sqlite id")
    ap.add_argument("--rank-report", action="store_true", help="report how many pairs the rankings disagree about")
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

    for (case, constraint_id), d, c, e in different:
        line = f"DIFFERENT {case}/{constraint_id}: decentralized={d[0]} central={c[0]}"
        if e is not None:
            if e == d[0]:
                line += f"  -> ground truth {e}: decentralized is right, central is wrong"
            elif e == c[0]:
                line += f"  -> ground truth {e}: central is right, decentralized is wrong"
            else:
                line += f"  -> ground truth {e}: both are wrong"
        print(line)
        if args.show_info:
            print(f"    decentralized: {d[1]}  (rev {d[3]}, {d[2]}, {d[4]} revision(s)"
                  f"{', reordered in transit' if d[5] else ''})")
            print(f"    central:       {c[1]}  (rev {c[3]}, {c[2]}, {c[4]} revision(s)"
                  f"{', reordered in transit' if c[5] else ''})")
        if args.show_history:
            print("    revision history:")
            print_history("decentralized", decent_revs.get((case, constraint_id), []))
            print_history("central", cent_revs.get((case, constraint_id), []))
    for (case, constraint_id), d_res, c_res, e in missing:
        side = "central" if d_res else "decentralized"
        have = d_res or c_res
        line = (f"MISSING   {case}/{constraint_id}: no result in the {side} db "
                f"(other side says {have})")
        if e is not None:
            line += f"  [ground truth {e}]"
        print(line)
    for (case, constraint_id), e in absent:
        print(f"ABSENT    {case}/{constraint_id}: neither system produced a result "
              f"(ground truth expects {e})")
    for (case, constraint_id), e, got in wrong:
        print(f"WRONG     {case}/{constraint_id}: both systems say {got}, "
              f"ground truth expects {e}")
    if args.show_matches:
        for (case, constraint_id), res in matches:
            print(f"MATCH     {case}/{constraint_id}: {res}")

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
        for label, final, revs in (
            ("decentralized", decent, decent_revs),
            ("central", cent, cent_revs),
        ):
            multi = sum(1 for v in revs.values() if len(v) > 1)
            reordered = sum(1 for v in final.values() if v[5])
            print(f"{label:14s}: {len(revs)} pairs, {multi} with >1 revision, "
                  f"{reordered} where the chosen final revision is not the last row inserted")
            vs_emission, vs_insertion = rank_disagreements(revs)
            print(f"{'':14s}  vs the revision ranking: created_at would pick a different "
                    f"verdict for {vs_emission} pair(s), row id for {vs_insertion} pair(s)")

    sys.exit(1 if n_bad else 0)


if __name__ == "__main__":
    main()