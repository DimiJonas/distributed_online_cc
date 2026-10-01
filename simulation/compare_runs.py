#!/usr/bin/env python3
"""
compare_runs.py - compare the levels of one run factor against each other (per system)

reads runs.csv (written by plot_results.py) and answers "how much does changing factor X cost?"

  python3.11 simulation/compare_runs.py results/_plots/runs.csv --experiment E0e --factor msgtrace --baseline false
  python3.11 simulation/compare_runs.py results/_plots/runs.csv --experiment E0b --factor worker_threads --baseline per-cpu --hold rate
  python3.11 simulation/compare_runs.py results/_plots/runs.csv --experiment E7a --factor netem_ms --baseline 0 --rank-by detection_p95_ms

every comparison is made for (system, held parameter)
replicates and seeds aggregate by median
levels are never mixed across different rates or node counts

  --rank-by  additionally reports which system is ahead on that metric at each level of the factor and flags the levels where the ranking flips relative to the baseline level
"""
from __future__ import annotations

import argparse
import csv
import statistics
import sys
from collections import defaultdict
from pathlib import Path


DEFAULT_METRICS = ("achieved_rate", "detection_p50_ms", "detection_p95_ms", "cpu_s_total",
                   "cpu_pct_max_node", "cpu_ms_per_event", "load_msgs_per_event", "load_bytes_per_event",
                   "fixed_bytes_per_node_s_idle",
                   "rss_mb_total", "pacer_max_lag_ms", "event_q_max", "accuracy")

# metrics where a larger value is the better outcome (everything else: smaller is better)
HIGHER_IS_BETTER = {"achieved_rate", "accuracy"}


def to_float(text: str):
    if text is None or text == "":
        return None
    try:
        return float(text)
    except ValueError:
        return {"true": 1.0, "false": 0.0, "True": 1.0, "False": 0.0}.get(text)


def median_of(values):
    numbers = [value for value in values if value is not None]
    return statistics.median(numbers) if numbers else None


def load_csv(csv_path: Path, experiment: str | None) -> list[dict]:
    with open(csv_path, newline="") as handle:
        rows = list(csv.DictReader(handle))
    if experiment:
        rows = [row for row in rows if row.get("experiment") == experiment]
    if not rows:
        sys.exit(f"no rows in {csv_path}" + (f" for experiment {experiment}" if experiment else ""))
    return rows


# (signed) relative change and whether its better now
def relative(new, base, metric: str):
    if new is None or base is None or base == 0:
        return None, None
    change = (new - base) / abs(base)
    better = change > 0 if metric in HIGHER_IS_BETTER else change < 0
    return change, better


COLUMN_WIDTH = 24


# the printed cell for one (metric, level): the value, and the change against the baseline when one is defined
def format_cell(value, base_value, metric: str, is_baseline: bool) -> str:
    if value is None:
        return "-"
    if is_baseline:
        return f"{value:.4g}"
    change, better = relative(value, base_value, metric)
    if change is None:
        # undefined against a zero or missing baseline
        return f"{value:.4g} (vs 0)" if base_value == 0 else f"{value:.4g} (n/a)"
    marker = "=" if change == 0 else ("+" if better else "!")
    return f"{value:.4g} ({change:+.1%}){marker}"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("runs_csv", type=Path, help="runs.csv from plot_results.py")
    parser.add_argument("--experiment", default=None, help="restrict to one experiment id")
    parser.add_argument("--factor", required=True, help="the parameter whose levels are compared (e.g. msgtrace)")
    parser.add_argument("--baseline", default=None, help="the level everything is compared against (default: first seen)")
    parser.add_argument("--hold", nargs="*", default=[], help="parameters that must match inside a comparison (e.g. rate)")
    parser.add_argument("--metrics", nargs="*", default=list(DEFAULT_METRICS))
    parser.add_argument("--rank-by", default=None, help="also report which system leads on this metric, and where it flips")
    parser.add_argument("--csv", type=Path, default=None, help="write the table here as well")
    args = parser.parse_args()

    rows = load_csv(args.runs_csv, args.experiment)
    if args.factor not in rows[0]:
        sys.exit(f"no column '{args.factor}' in {args.runs_csv}. available: {', '.join(sorted(rows[0]))}")
    for held in args.hold:
        if held not in rows[0]:
            sys.exit(f"no column '{held}' in {args.runs_csv}")

    # (system, held..., level) -> metric -> [values]
    cells: dict[tuple, dict[str, list]] = defaultdict(lambda: defaultdict(list))
    levels: list[str] = []
    for row in rows:
        level = row[args.factor]
        if level not in levels:
            levels.append(level)
        key = (row["system"], tuple(row[held] for held in args.hold), level)
        for metric in args.metrics:
            if metric in row:
                cells[key][metric].append(to_float(row[metric]))

    baseline_level = args.baseline if args.baseline is not None else levels[0]
    if baseline_level not in levels:
        sys.exit(f"baseline level '{baseline_level}' not among the observed levels: {', '.join(levels)}")

    out_rows = []
    groups = sorted({(system, held) for system, held, _level in cells}, key=str)
    for system, held in groups:
        held_label = ", ".join(f"{name}={value}" for name, value in zip(args.hold, held)) or "all"
        print(f"\n=== {system}  [{held_label}]   baseline {args.factor}={baseline_level}")
        base_cell = cells.get((system, held, baseline_level))
        if not base_cell:
            print("  no baseline run in this cell, skipped")
            continue
        print(f"  {'metric':<28}" + "".join(f"{args.factor}={level}".ljust(COLUMN_WIDTH) for level in levels))
        incomparable: list[str] = []
        for metric in args.metrics:
            base_value = median_of(base_cell.get(metric, []))
            if base_value is None:
                continue
            line = f"  {metric:<28}"
            for level in levels:
                value = median_of(cells.get((system, held, level), {}).get(metric, []))
                is_baseline = level == baseline_level
                line += format_cell(value, base_value, metric, is_baseline).ljust(COLUMN_WIDTH)
                if value is None:
                    continue
                change = None if is_baseline else relative(value, base_value, metric)[0]
                if not is_baseline and change is None and metric not in incomparable:
                    incomparable.append(metric)
                out_rows.append({"system": system, "held": held_label, "level": level, "metric": metric, "value": value, "relative_to_baseline": change})
            print(line)
        print("  (+ = better than baseline, ! = worse, = unchanged)")
        if incomparable:
            print(f"  note: {', '.join(incomparable)} had a zero baseline, so no relative change is defined.")

    if args.rank_by:
        print(f"\n=== system ranking by {args.rank_by} (lower is better"
              f"{', but higher is better here' if args.rank_by in HIGHER_IS_BETTER else ''})")
        held_sets = sorted({held for _system, held, _level in cells}, key=str)
        for held in held_sets:
            held_label = ", ".join(f"{name}={value}" for name, value in zip(args.hold, held)) or "all"
            leader_at_baseline = None
            for level in levels:
                per_system = {system: median_of(cells.get((system, held, level), {}).get(args.rank_by, []))
                              for system, _held, lvl in cells if lvl == level}
                per_system = {system: value for system, value in per_system.items() if value is not None}
                if len(per_system) < 2:
                    continue
                pick = max if args.rank_by in HIGHER_IS_BETTER else min
                leader = pick(per_system, key=per_system.get)
                ordered = sorted(per_system.values())
                spread = (ordered[-1] / ordered[0]) if ordered[0] else float("inf")
                if level == baseline_level:
                    leader_at_baseline = leader
                flip = " <-- RANKING FLIP" if leader_at_baseline and leader != leader_at_baseline else ""
                values = "  ".join(f"{system}={value:.4g}" for system, value in sorted(per_system.items()))
                print(f"  [{held_label}] {args.factor}={level:<8} leader: {leader:<14} ({values}, ratio {spread:.2f}x){flip}")

    if args.csv and out_rows:
        args.csv.parent.mkdir(parents=True, exist_ok=True)
        with open(args.csv, "w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(out_rows[0]))
            writer.writeheader()
            writer.writerows(out_rows)
        print(f"\nwrote {args.csv}")


if __name__ == "__main__":
    main()