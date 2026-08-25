#!/usr/bin/env python3
"""
compute_privacy.py - privacy evaluation using the observations of a run


reads observations.json.gz (written by extract_metrics.py) and the ground truth event stream

writes privacy.json

  python3.11 simulation/compute_privacy.py --results results/
  python3.11 simulation/compute_privacy.py --results results/ --seed 7 --draws 500 --out privacy_seed7.json

metrics per compromise size

  case_exposure = fraction of a case visible to the attacker (by distinct activity and weighted by event occurrences)
  full_cases = fraction of cases the attacker can reconstruct completely
  model_fact_disclosure = fraction of the process model facts the attacker learns
  case_disclosure = fraction of cases for which the attacker links a case to its trace (case/attribute disclosure in the sense of rafiei and van der aalst)
  trace_disclosure = fraction of distinct traces the attacker can single out
  first_spy = precision and recall of guessing the origin of the case end gossip as the source_peer of the earliest sighting among compromised nodes
"""
from __future__ import annotations

import argparse
import gzip
import json
import random
import sys
from collections import Counter
from pathlib import Path
from helpers import percentiles, discover_runs


SCHEMA_VERSION = 1


# loads the actual event stream (event log) as ground truth
# case -> (activities: (event_id -> count), trace -> (event_id, ...))
def load_ground_truth(manifest: Path) -> dict:

    cases: dict[str, dict] = {}
    with open(manifest) as manifest_file:
        for line in manifest_file:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            case_id, event_id = row.get("case_id"), row.get("event_id")
            if case_id is None or event_id is None:
                continue
            case = cases.setdefault(case_id, {"activities": Counter(), "trace": []})
            case["activities"][event_id] += 1
            case["trace"].append(event_id)
    return {case_id: {"activities": dict(case["activities"]), "trace": tuple(case["trace"])}
            for case_id, case in cases.items()}


# observations about one system
class SystemView:

    def __init__(self, observations: dict, truth: dict, use_inferred: bool):
        self.system = observations["system"]
        self.pool = list(observations["attacker_pool"])
        self.node_roles = observations.get("node_roles", {})
        self.truth = truth

        self.visible: dict[int, dict[str, set[str]]] = {} # node id -> (case -> event ids)
        for source in ("local_events",) + (("inferred_events",) if use_inferred else ()):
            for node, cases in observations.get(source, {}).items():
                per_node = self.visible.setdefault(int(node), {})
                for case_id, events in cases.items():
                    per_node.setdefault(case_id, set()).update(events)

        self.facts: dict[int, set[str]] = {int(node): set(facts) for node, facts in observations["model_facts"]["per_node"].items()}
        self.facts_total = observations["model_facts"]["total"]

        self.case_end: dict[int, dict[str, dict]] = {int(node): cases for node, cases in observations.get("case_end_seen", {}).items()}
        self.case_end_truth = observations.get("case_end_truth", {})
        self.peer_to_node = {address: int(node) for address, node in observations.get("peer_to_node", {}).items()}

        # cases that appear anywhere in this run and in the ground truth
        seen = {case for cases in self.visible.values() for case in cases}
        self.cases = sorted(seen & set(truth))

        # trace multiplicity: a trace shared by many cases hides them from each other
        trace_counts = Counter(truth[case]["trace"] for case in self.cases)
        self.trace_counts = trace_counts


    # evaluate a draw of compromised nodes
    def evaluate(self, compromised: list[int]) -> dict:
        compromised_set = set(compromised)

        merged: dict[str, set[str]] = {} # case -> [event ids]
        for node in compromised_set:
            for case_id, events in self.visible.get(node, {}).items():
                merged.setdefault(case_id, set()).update(events)

        by_activity, by_occurrence = [], []
        full_cases = 0
        case_disclosed = 0
        traces_singled: set[tuple] = set()
        for case_id in self.cases:
            truth_case = self.truth[case_id]
            activities = truth_case["activities"]
            visible = merged.get(case_id, set())
            if not visible:
                by_activity.append(0.0)
                by_occurrence.append(0.0)
                continue
            fraction_activity = len(visible & set(activities)) / len(activities)
            occurrences = sum(activities.values())
            fraction_occurrence = sum(count for event, count in activities.items()
                                      if event in visible) / occurrences
            by_activity.append(fraction_activity)
            by_occurrence.append(fraction_occurrence)
            if fraction_activity >= 1.0:
                full_cases += 1
                # check if trace is unique
                if self.trace_counts[truth_case["trace"]] == 1:
                    case_disclosed += 1
                    traces_singled.add(truth_case["trace"])

        learned_facts: set[str] = set()
        for node in compromised_set:
            learned_facts |= self.facts.get(node, set())

        first_spy = self._first_spy(compromised_set)

        total_reconstruction = 1.0 if (self.cases and full_cases == len(self.cases)) else 0.0
        any_full_case = 1.0 if full_cases else 0.0

        return {
            "total_reconstruction_probability": total_reconstruction,
            "any_full_case_probability": any_full_case,
            "case_exposure_activity": (sum(by_activity) / len(by_activity)) if by_activity else 0.0,
            "case_exposure_occurrence": (sum(by_occurrence) / len(by_occurrence)) if by_occurrence else 0.0,
            "full_case_fraction": (full_cases / len(self.cases)) if self.cases else 0.0,
            "case_disclosure": (case_disclosed / len(self.cases)) if self.cases else 0.0,
            "trace_disclosure": (len(traces_singled) / len(self.trace_counts))
                                if self.trace_counts else 0.0,
            "model_fact_disclosure": (len(learned_facts) / self.facts_total)
                                     if self.facts_total else 0.0,
            **first_spy,
        }

    def _first_spy(self, compromised: set[int]) -> dict:
        guesses = 0
        correct = 0
        observable = 0
        for case_id, true_source in self.case_end_truth.items():
            sightings = [(self.case_end[node][case_id], node) for node in compromised
                         if node in self.case_end and case_id in self.case_end[node]]
            if not sightings:
                continue
            observable += 1
            sighting, node = min(sightings, key=lambda item: item[0]["t_us"])
            if sighting["source"] == "end_event":
                guess = node          # the attacker owns the origin itself
            else:
                guess = self.peer_to_node.get(sighting.get("source_peer"))
            if guess is None:
                continue
            guesses += 1
            if guess == true_source:
                correct += 1
        total = len(self.case_end_truth)
        return {
            "first_spy_precision": (correct / guesses) if guesses else None,
            "first_spy_recall": (correct / total) if total else None,
            "first_spy_observable_fraction": (observable / total) if total else None,
        }


    # greedy set cover per case to find out how many nodes are enough to see the whole case
    def min_nodes_per_case(self) -> dict:

        counts = []
        for case_id in self.cases:
            needed = set(self.truth[case_id]["activities"])
            available = {node: self.visible.get(node, {}).get(case_id, set()) & needed for node in self.pool}
            chosen = 0
            remaining = set(needed)
            while remaining:
                best = max(available.items(), key=lambda item: len(item[1] & remaining),
                           default=(None, set()))
                if not best[1] & remaining:
                    chosen = None
                    break
                remaining -= best[1]
                available.pop(best[0])
                chosen += 1
            if chosen is not None:
                counts.append(chosen)
        histogram: dict[str, int] = {}
        
        for count in counts:
            histogram[str(count)] = histogram.get(str(count), 0) + 1
        return {"cases_covered": len(counts), "cases_total": len(self.cases), "histogram": histogram, **percentiles([float(count) for count in counts], points=(5, 50, 95))}


def compromise_sizes(pool_size: int, requested: list[int] | None) -> list[int]:
    if requested:
        return [size for size in requested if 1 <= size <= pool_size]
    if pool_size <= 12:
        return list(range(1, pool_size + 1))
    sizes = {1, 2, 3, pool_size}
    fraction = 0.05
    while fraction < 1.0:
        sizes.add(max(1, round(pool_size * fraction)))
        fraction *= 1.6
    return sorted(sizes)


def evaluate_system(view: SystemView, draws: int, seed: int, sizes: list[int] | None) -> dict:
    rng = random.Random(seed)
    pool = view.pool
    points = compromise_sizes(len(pool), sizes)
    curves: dict[str, dict[str, list]] = {}
    per_size = []

    for size in points:
        results: dict[str, list[float]] = {}
        # exhaustive when there is only one distinct subset, sampled otherwise
        repeats = 1 if size >= len(pool) else draws
        for _ in range(repeats):
            subset = pool if size >= len(pool) else rng.sample(pool, size)
            for key, value in view.evaluate(subset).items():
                if value is not None:
                    results.setdefault(key, []).append(value)
                    
        per_size.append({"compromised": size,
                         "compromised_fraction": size / len(pool) if pool else None,
                         "draws": repeats,
                         **{key: percentiles(values, points=(5, 50, 95)) for key, values in results.items()}})

    for key in {key for entry in per_size for key in entry if isinstance(entry[key], dict)}:
        curves[key] = {
            "compromised": [entry["compromised"] for entry in per_size],
            "mean": [entry.get(key, {}).get("mean") for entry in per_size],
            "p5": [entry.get(key, {}).get("p5") for entry in per_size],
            "p95": [entry.get(key, {}).get("p95") for entry in per_size],
        }

    return {
        "pool": pool,
        "pool_size": len(pool),
        "node_roles": view.node_roles,
        "cases_evaluated": len(view.cases),
        "model_facts_total": view.facts_total,
        "per_size": per_size,
        "curves": curves,
        "min_nodes_full_case": view.min_nodes_per_case(),
    }


# metadata volume the global attacker sees
def global_attacker(observations: dict) -> dict:

    case_end = observations.get("case_end_seen", {})
    sightings = sum(len(cases) for cases in case_end.values())
    cases = {case for per_node in case_end.values() for case in per_node}
    return {
        "case_end_sightings": sightings,
        "cases_with_gossip": len(cases),
        "sightings_per_case": (sightings / len(cases)) if cases else 0.0,
        "note": "metadata only: message timing, size, sender and receiver, no payload",
    }


def process_run(run_dir: Path, args) -> str:
    observations_path = run_dir / "observations.json.gz"
    if not observations_path.exists():
        return f"skip {run_dir.name}: no observations.json.gz (run extract_metrics.py first)"
    with gzip.open(observations_path, "rt") as observations_file:
        observations = json.load(observations_file)

    manifest = args.manifest
    if manifest is None:
        run_spec = json.loads((run_dir / "run.json").read_text())
        candidate = (run_spec.get("stream") or {}).get("manifest")
        manifest = Path(candidate) if candidate else None
    if manifest is None or not Path(manifest).exists():
        return f"skip {run_dir.name}: no manifest (pass --manifest)"
    ground_truth_event_log = load_ground_truth(Path(manifest))

    payload = {
        "schema_version": SCHEMA_VERSION,
        "run_id": observations.get("run_id"),
        "experiment": observations.get("experiment"),
        "params": observations.get("params"),
        "settings": {"seed": args.seed, "draws": args.draws,
                     "use_inferred_events": not args.local_events_only,
                     "compromise_sizes": args.sizes,
                     "manifest": str(manifest)},
        "ground_truth": {"cases": len(ground_truth_event_log),
                         "distinct_traces": len({case["trace"] for case in ground_truth_event_log.values()})},
        "systems": {},
    }

    for system, system_observations in observations.get("systems", {}).items():
        view = SystemView(system_observations, ground_truth_event_log, use_inferred=not args.local_events_only)
        payload["systems"][system] = {
            "local_attacker": evaluate_system(view, args.draws, args.seed, args.sizes),
            "global_attacker": global_attacker(system_observations),
        }

    out_path = run_dir / args.out
    out_path.write_text(json.dumps(payload, indent=2, default=str))
    return f"wrote {out_path}"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--results", type=Path, help="results directory to walk")
    parser.add_argument("--run", type=Path, action="append", default=[])
    parser.add_argument("--experiment", default=None, help="only experiments matching this glob")
    parser.add_argument("--manifest", type=Path, default=None, help="manifest.jsonl of the stream (default: from run.json)")
    parser.add_argument("--seed", type=int, default=1234, help="seed for drawing compromised nodes")
    parser.add_argument("--draws", type=int, default=200, help="random subsets per compromise size")
    parser.add_argument("--sizes", type=int, nargs="*", default=None, help="explicit compromise sizes (default: spaced over the pool)")
    parser.add_argument("--local-events-only", action="store_true", help="ignore what coordination traffic reveals (lower bound on exposure)")
    parser.add_argument("--out", default="privacy.json", help="output file name inside the run dir")
    args = parser.parse_args()

    runs = list(args.run)
    if args.results:
        runs += discover_runs(args.results, args.experiment, "observations.json.gz")
    if not runs:
        sys.exit("nothing to do: pass --results and/or --run")
    for run_dir in runs:
        print(process_run(run_dir, args))


if __name__ == "__main__":
    main()