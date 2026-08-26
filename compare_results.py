#!/usr/bin/env python3
"""
for every (case_id, constraint_id) pair the final result is compared

Reported per pair, pairwise mode:
  DIFFERENT  - both systems have a result but they disagree (with --expected: also says which side matches the ground truth)
  MISSING    - one system has a result, the other has no row at all
  ABSENT     - (only with --expected) neither system produced a result
  WRONG      - (only with --expected) the systems agree with each other but not with the ground truth
  SPURIOUS   - (only with --expected) the systems agree on a pair the ground truth does not contain at all

Reported per pair, single mode (all require --expected):
  WRONG      - the system produced a result that disagrees with the ground truth
  ABSENT     - the ground truth expects a result the system never produced
  SPURIOUS   - the system produced a result for a pair the ground truth does not contain

Exit codes:
  0 - the comparison ran and found nothing to report
  1 - the comparison ran and found discrepancies
  2 - the comparison could not be performed (db missing/unreadable, bad timestamp, no expected file, bad usage)

Usage:
  compare_results.py --decentralized run/decentralized/cc-results.db \
                     --central run/central/cc-results.db \
                     [--expected run/events/expected_results.jsonl] \
                     [--rank-by revision|emission|insertion] [--rank-report] \
                     [--json out.json] \
                     [--show-info] [--show-history] [--show-matches]

  compare_results.py --decentralized run/decentralized/cc-results.db \
                     --expected run/events/expected_results.jsonl
"""

import argparse
import json
import re
import sqlite3
import sys
from pathlib import Path

EXIT_OK = 0             # comparison ran, nothing to report
EXIT_DISCREPANCIES = 1  # comparison ran, found differences/wrong/missing/spurious pairs
EXIT_ERROR = 2          # comparison could not be performed at all


def exit_with_message(message: str):
    print(message, file=sys.stderr)
    sys.exit(EXIT_ERROR)


_TIMESTAMP_REGEX = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2}):(\d{2})"
    r"(?:\.(\d{1,9}))?"
    r"(Z|z|[+-]\d{2}:?\d{2})?$"
)


def parse_created_at(timestamp_string):
    # parse an RFC3339 timestamp into integer nanoseconds since the epoch.datetime.fromisoformat truncates to microseconds
    matched_timestamp = _TIMESTAMP_REGEX.match(timestamp_string.strip())
    if not matched_timestamp:
        raise ValueError(f"unparseable created_at: {timestamp_string!r}")
    year, month, day, hour, minute, second, frac, offset = matched_timestamp.groups()

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
        exit_with_message(f"error: database not found: {db_path}")
    connection = sqlite3.connect(db_path)

    columns = "case_id, constraint_id, result, info, created_at, id, revision"
    try:
        rows = connection.execute(f"SELECT {columns} FROM results").fetchall()
    except sqlite3.OperationalError as e:
        exit_with_message(f"error: cannot read results from {db_path}: {e}")

    revisions = {}
    for row in rows:
        case_id, constraint_id, result, info, created_at, row_id, revision = row[:7]
        try:
            timestamp = parse_created_at(created_at)
        except ValueError as e:
            exit_with_message(f"error: in {db_path}: {e}")
        revisions.setdefault((case_id, constraint_id), []).append(
            (timestamp, row_id, result, info, created_at, revision)
        )

    rankers = {
        # ties inside one ranking fall back to the row id so the choice is deterministic
        "revision": lambda record: (record[5], record[1]),
        "emission": lambda record: (record[0], record[1]),
        "insertion": lambda record: record[1],
    }
    rank_key = rankers[rank_by]

    final_revisions = {}
    for key, entries in revisions.items():
        chosen = max(entries, key=rank_key)
        by_insertion = max(entries, key=rankers["insertion"])
        reordered = chosen[1] != by_insertion[1]
        final_revisions[key] = (
            chosen[2],
            chosen[3],
            chosen[4],
            chosen[5],
            len(entries),
            reordered,
        )

    return final_revisions, revisions


def load_expected(path: str):
    if not Path(path).exists():
        exit_with_message(f"error: expected results file not found: {path}")
    expected_results = {}
    for line in open(path):
        line = line.strip()
        if not line:
            continue
        parsed_line = json.loads(line)
        expected_results[(parsed_line["case_id"], parsed_line["constraint_id"])] = parsed_line["expected"]
    return expected_results


def print_history(label, entries):
    for timestamp, row_id, result, info, created_at, revision in sorted(
        entries, key=lambda entry: (entry[5], entry[1])
    ):
        print(
            f"      {label:14s} rev={revision:<4} id={row_id:<7} {created_at}  "
            f"{result:<20} {info}"
        )


def rank_disagreements(revisions):
    # how many pairs would get a different final verdict depending on the ranking
    def final_result(entries, key):
        return max(entries, key=key)[2]

    by_revision = lambda row: (row[5], row[1])
    by_emission = lambda row: (row[0], row[1])
    by_insertion = lambda row: row[1]

    vs_emission = vs_insertion = 0
    for entries in revisions.values():
        if len(entries) < 2:
            continue
        final_by_revision = final_result(entries, by_revision)
        if final_result(entries, by_emission) != final_by_revision:
            vs_emission += 1
        if final_result(entries, by_insertion) != final_by_revision:
            vs_insertion += 1
    return vs_emission, vs_insertion


def score_against_expected(final: dict, expected: dict) -> dict:
    # score one system on its own against the ground truth. this is mode independent: in pairwise mode it is
    # computed for both systems, which is what a 'correctness vs rate' plot per deployment needs
    keys_expected, keys_have = set(expected), set(final)
    both = keys_expected & keys_have
    correct = sum(1 for key in both if final[key][0] == expected[key])
    return {
        "expected_pairs": len(keys_expected),
        "produced_pairs": len(keys_have),
        "correct": correct,
        "wrong": len(both) - correct,
        "absent": len(keys_expected - keys_have),        # ground truth expects it, system never produced it
        "spurious": len(keys_have - keys_expected),      # system produced it, ground truth has no such pair
        "accuracy": (correct / len(keys_expected)) if keys_expected else None,
    }


def revision_stats(final: dict, revs: dict) -> dict:
    vs_emission, vs_insertion = rank_disagreements(revs)
    return {
        "pairs": len(revs),
        "revisions_total": sum(len(value) for value in revs.values()),
        "pairs_with_multiple_revisions": sum(1 for value in revs.values() if len(value) > 1),
        "max_revisions_on_a_pair": max((len(value) for value in revs.values()), default=0),
        "final_not_last_inserted": sum(1 for value in final.values() if value[5]),
        "rank_disagreement_emission": vs_emission,
        "rank_disagreement_insertion": vs_insertion,
    }


def report_single(name, final, revisions, expected, args) -> tuple[dict, int]:
    # score exactly one system against the ground truth
    keys = sorted(set(final) | set(expected))

    wrong, absent, spurious, matches = [], [], [], []
    for key in keys:
        actual = final[key][0] if key in final else None
        expected_result = expected.get(key)
        if key not in expected:
            spurious.append((key, actual))
        elif actual is None:
            absent.append((key, expected_result))
        elif actual != expected_result:
            wrong.append((key, expected_result, actual))
        else:
            matches.append((key, actual))

    for (case, constraint_id), expected_result, actual in wrong:
        print(f"WRONG     {case}/{constraint_id}: {name} says {actual}, ground truth expects {expected_result}")
        if args.show_info:
            final_row = final[(case, constraint_id)]
            print(f"    {name}: {final_row[1]}  (rev {final_row[3]}, {final_row[2]}, {final_row[4]} revision(s)"
                  f"{', reordered in transit' if final_row[5] else ''})")
        if args.show_history:
            print("    revision history:")
            print_history(name, revisions.get((case, constraint_id), []))
    for (case, constraint_id), expected_result in absent:
        print(f"ABSENT    {case}/{constraint_id}: {name} produced no result "
              f"(ground truth expects {expected_result})")
    for (case, constraint_id), actual in spurious:
        print(f"SPURIOUS  {case}/{constraint_id}: {name} says {actual}, "
              f"the ground truth has no such pair")
    if args.show_matches:
        for (case, constraint_id), match in matches:
            print(f"MATCH     {case}/{constraint_id}: {match}")

    num_bad_matches = len(wrong) + len(absent) + len(spurious)
    print(f"\n{len(keys)} (case, constraint) pairs scored "
          f"[final revision ranked by {args.rank_by}]: "
          f"{len(matches)} correct, {len(wrong)} wrong, "
          f"{len(absent)} absent (expected but not produced), "
          f"{len(spurious)} spurious (produced but not expected)")

    return {"pairs": len(keys)}, num_bad_matches


def report_pairwise(decentralized_final_revisions, decentralized_revisions, central_final_revisions, central_revisions, expected, args) -> tuple[dict, int]:
    keys = sorted(set(decentralized_final_revisions) | set(central_final_revisions) | (set(expected) if expected is not None else set()))

    different, missing, absent, wrong, spurious, matches = [], [], [], [], [], []
    for key in keys:
        decentralized_final_revision = decentralized_final_revisions.get(key)
        central_final_revision = central_final_revisions.get(key)
        decentralized_result = decentralized_final_revision[0] if decentralized_final_revision else None
        central_result = central_final_revision[0] if central_final_revision else None
        expected_result = expected.get(key) if expected is not None else None
        # distinguish 'no ground truth file' from 'this pair is not in the ground truth'
        in_expected = expected is not None and key in expected

        if decentralized_result is None and central_result is None:
            absent.append((key, expected_result))
            continue
        if decentralized_result is None or central_result is None:
            missing.append((key, decentralized_result, central_result, expected_result))
            continue
        if decentralized_result != central_result:
            different.append((key, decentralized_final_revision, central_final_revision, expected_result))
            continue
        # central and decentralized are equal - check against the ground truth if provided
        if expected is not None and not in_expected:
            spurious.append((key, decentralized_result))
            continue
        if in_expected and expected_result != decentralized_result:
            wrong.append((key, expected_result, decentralized_result))
            continue
        matches.append((key, decentralized_result))

    for (case, constraint_id), decentralized_final_revision, central_final_revision, expected_result in different:
        line = f"DIFFERENT {case}/{constraint_id}: decentralized={decentralized_final_revision[0]} central={central_final_revision[0]}"
        if expected_result is not None:
            if expected_result == decentralized_final_revision[0]:
                line += f"  -> ground truth {expected_result}: decentralized is right, central is wrong"
            elif expected_result == central_final_revision[0]:
                line += f"  -> ground truth {expected_result}: central is right, decentralized is wrong"
            else:
                line += f"  -> ground truth {expected_result}: both are wrong"
        print(line)
        if args.show_info:
            print(f"    decentralized: {decentralized_final_revision[1]}  (rev {decentralized_final_revision[3]}, {decentralized_final_revision[2]}, {decentralized_final_revision[4]} revision(s)"
                  f"{', reordered in transit' if decentralized_final_revision[5] else ''})")
            print(f"    central:       {central_final_revision[1]}  (rev {central_final_revision[3]}, {central_final_revision[2]}, {central_final_revision[4]} revision(s)"
                  f"{', reordered in transit' if central_final_revision[5] else ''})")
        if args.show_history:
            print("    revision history:")
            print_history("decentralized", decentralized_revisions.get((case, constraint_id), []))
            print_history("central", central_revisions.get((case, constraint_id), []))
    for (case, constraint_id), decentralized_result, central_result, expected_result in missing:
        missing_side = "central" if decentralized_result else "decentralized"
        present_sides_result = decentralized_result or central_result
        line = (f"MISSING   {case}/{constraint_id}: no result in the {missing_side} db "
                f"(other side says {present_sides_result})")
        if expected_result is not None:
            line += f"  [ground truth {expected_result}]"
        print(line)
    for (case, constraint_id), expected_result in absent:
        print(f"ABSENT    {case}/{constraint_id}: neither system produced a result "
              f"(ground truth expects {expected_result})")
    for (case, constraint_id), expected_result, got in wrong:
        print(f"WRONG     {case}/{constraint_id}: both systems say {got}, "
              f"ground truth expects {expected_result}")
    for (case, constraint_id), got in spurious:
        print(f"SPURIOUS  {case}/{constraint_id}: both systems say {got}, "
              f"the ground truth has no such pair")
    if args.show_matches:
        for (case, constraint_id), result in matches:
            print(f"MATCH     {case}/{constraint_id}: {result}")

    num_bad_matches = len(different) + len(missing) + len(absent) + len(wrong) + len(spurious)
    summary = (f"\n{len(keys)} (case, constraint) pairs compared "
               f"[final revision ranked by {args.rank_by}]: "
               f"{len(matches)} match, {len(different)} differ, "
               f"{len(missing)} missing on one side")
    decentralized_right = central_right = both_wrong = None
    if expected is not None:
        summary += (f", {len(absent)} absent from both, {len(wrong)} wrong vs ground truth, "
                    f"{len(spurious)} spurious")
        decentralized_right = sum(1 for _, decentralized_result, _, expected_result in different if expected_result is not None and expected_result == decentralized_result[0])
        central_right = sum(1 for _, _, central_result, expected_result in different if expected_result is not None and expected_result == central_result[0])
        both_wrong = len(different) - decentralized_right - central_right
        if different:
            summary += (f"\nof the {len(different)} disagreements: "
                        f"decentralized correct {decentralized_right}x, central correct {central_right}x, "
                        f"both wrong {both_wrong}x")
    print(summary)

    stats = {
        "pairs": len(keys),
        "match": len(matches),
        "different": len(different),
        "missing_one_side": len(missing),
        "absent_from_both": len(absent),
        "wrong_but_agreeing": len(wrong),
        "spurious_but_agreeing": len(spurious),
        "agreement": (len(keys) - len(different) - len(missing)) / len(keys) if keys else None,
    }
    if expected is not None:
        stats["disagreements_decentralized_correct"] = decentralized_right
        stats["disagreements_central_correct"] = central_right
        stats["disagreements_both_wrong"] = both_wrong
    return stats, num_bad_matches


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    # NOTE: no path defaults on purpose. 'this argument was given' is what selects pairwise vs single mode,
    # so a default would make a crashed central run look like an intentional single-system run
    ap.add_argument("--decentralized", default=None, help="path to the decentralized system's sqlite db")
    ap.add_argument("--central", default=None, help="path to the central system's sqlite db")
    ap.add_argument("--expected", default=None, help="expected_results.jsonl (from gen-events or expected-results); optional in pairwise mode, required in single-system mode")
    ap.add_argument("--rank-by", choices=["revision", "emission", "insertion"], default="revision",
                    help="which revision counts as final: 'revision' uses the per (case, constraint) counter assigned by the emitting node (default)," \
                    "'emission' uses created_at, 'insertion' uses the sqlite id")
    ap.add_argument("--rank-report", action="store_true", help="report how many pairs the rankings disagree about")
    ap.add_argument("--json", default=None, help="also write the summary counts to this path as json, for batch aggregation")
    ap.add_argument("--show-info", action="store_true", help="print the info text of differing results")
    ap.add_argument("--show-history", action="store_true", help="print every revision of differing pairs")
    ap.add_argument("--show-matches", action="store_true", help="also list all matching pairs")
    args = ap.parse_args()

    named = [(system_name, db_file_path) for system_name, db_file_path in (("decentralized", args.decentralized), ("central", args.central)) if db_file_path]
    if not named:
        ap.error("name at least one database with --decentralized and/or --central")
    if len(named) == 1 and not args.expected:
        ap.error(f"single-system mode ({named[0][0]} only) needs --expected: there is no second "
                 f"system to compare against")

    # a named database that does not exist is fatal (inside load_final_results), never a silent fallback
    loaded = {system_name: load_final_results(db_file_path, args.rank_by) for system_name, db_file_path in named}
    expected = load_expected(args.expected) if args.expected else None

    if len(named) == 2:
        decentralized_final_revisions, decentralized_revisions = loaded["decentralized"]
        central_final_revisions, central_revisions = loaded["central"]
        mode = "pairwise"
        stats, num_bad_matches = report_pairwise(decentralized_final_revisions, decentralized_revisions, central_final_revisions, central_revisions, expected, args)
    else:
        name = named[0][0]
        final_revisions, revisions = loaded[name]
        mode = "single"
        stats, num_bad_matches = report_single(name, final_revisions, revisions, expected, args)

    # per system ground truth scoring: available in both modes, and the thing a
    # 'correctness vs rate' or 'correctness vs nodes' plot should read
    per_system = {}
    for name, (final_revisions, revisions) in loaded.items():
        stats = revision_stats(final_revisions, revisions)
        if expected is not None:
            stats.update(score_against_expected(final_revisions, expected))
        per_system[name] = stats

    if expected is not None:
        print()
        for name in (system_name for system_name, _ in named):
            system_stats = per_system[name]
            accuracy = system_stats["accuracy"]
            print(f"{name:14s}: vs ground truth {system_stats['correct']}/{system_stats['expected_pairs']} correct"
                  f"{f' ({accuracy:.4f})' if accuracy is not None else ''}, "
                  f"{system_stats['wrong']} wrong, {system_stats['absent']} absent, {system_stats['spurious']} spurious")

    if args.rank_report:
        for name in (system_name for system_name, _ in named):
            system_stats = per_system[name]
            print(f"{name:14s}: {system_stats['pairs']} pairs, {system_stats['pairs_with_multiple_revisions']} with >1 revision "
                  f"(max {system_stats['max_revisions_on_a_pair']}, {system_stats['revisions_total']} rows total), "
                  f"{system_stats['final_not_last_inserted']} where the chosen final revision is not the last row inserted")
            print(f"{'':14s}  vs the revision ranking: created_at would pick a different "
                  f"verdict for {system_stats['rank_disagreement_emission']} pair(s), "
                  f"row id for {system_stats['rank_disagreement_insertion']} pair(s)")

    if args.json:
        payload = {
            "mode": mode,
            "systems": [system_name for system_name, _ in named],
            "rank_by": args.rank_by,
            "has_expected": expected is not None,
            "comparison": stats,
            "per_system": per_system,
            "n_bad": num_bad_matches,
        }
        Path(args.json).parent.mkdir(parents=True, exist_ok=True)
        Path(args.json).write_text(json.dumps(payload, indent=2))

    sys.exit(EXIT_DISCREPANCIES if num_bad_matches else EXIT_OK)


if __name__ == "__main__":
    main()