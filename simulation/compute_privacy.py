#!/usr/bin/env python3
"""
compute_privacy.py - privacy evaluation using the observations of a run


reads observations.json.gz (written by extract_metrics.py) and the ground truth event stream

writes privacy.json

  python3.11 simulation/compute_privacy.py --results results/
  python3.11 simulation/compute_privacy.py --results results/ --seed 7 --draws 500 --out privacy_seed7.json
  python3.11 simulation/compute_privacy.py --results results/ --bk-type set mult seq
  python3.11 simulation/compute_privacy.py --results results/ --no-disclosure 

metrics per compromise size

  case_exposure_named = fraction of a case's distinct activities the attacker can name
  case_exposure_occurrence = the same, weighted by how often each activity occurs in the case
  case_exposure_with_anonymous = named plus anonymous occurrences
  full_case_fraction = fraction of cases whose activities are all named
  total_reconstruction_probability = fraction of draws in which every case is fully named
  model_fact_disclosure = fraction of the declarative model the attacker learns (exists(c), type(c, role) and watch(event))
  placement_fact_disclosure = fraction of the placement the attacker learns (place(c, role, node) and place(c, partner, node))
  case_disclosure_<type> = identity disclosure (rafiei and van der aalst (2021))
  trace_disclosure_<type> = attribute disclosure (rafiei and van der aalst (2021))
  first_spy = precision and recall of guessing the origin of the case end gossip as the source_peer of the earliest observation among the compromised nodes
"""
from __future__ import annotations

import argparse
import gzip
import json
import math
import random
import sys
from collections import Counter
from pathlib import Path
from helpers import percentiles, discover_runs, Progress


SCHEMA_VERSION = 2
BK_TYPES = ("set", "mult", "seq")


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



# the disclosure measures of rafiei and van der aalst (2021) evaluated against the ground truth log
#
# the log is collapsed to its variants (unique traces) with their frequencies
# a candidate background knowledge selects a subset of variants and the multiset of matching traces comes from their frequencies
# the variants containing a given activity are precomputed as bitmasks, so resolving a candidate intersects a few integers instead of scanning the log
class DisclosureIndex:

    def __init__(self, ground_truth_event_log: dict):
        variant_counts = Counter(case["trace"] for case in ground_truth_event_log.values())
        self.variants = list(variant_counts.keys())
        self.counts = [variant_counts[variant] for variant in self.variants]
        self.total_traces = sum(self.counts)

        self.activity_mask: dict[str, int] = {} # activity -> bitmask of variants containing it
        self.count_mask: dict[tuple[str, int], int] = {} # (activity, k) -> variants with >= k of it
        for index, variant in enumerate(self.variants):
            bit = 1 << index
            for activity, occurrences in Counter(variant).items():
                self.activity_mask[activity] = self.activity_mask.get(activity, 0) | bit
                for k in range(1, occurrences + 1):
                    self.count_mask[(activity, k)] = self.count_mask.get((activity, k), 0) | bit

        self._mask_cache: dict[tuple, int] = {}
        self._stats_cache: dict[int, tuple[int, float]] = {}

    # bitmask of the variants matching a candidate background knowledge
    def _mask(self, bk_type: str, candidate) -> int:
        cache_key = (bk_type, candidate)
        cached = self._mask_cache.get(cache_key)
        if cached is not None:
            return cached

        mask = (1 << len(self.variants)) - 1
        if bk_type == "set":
            for activity in candidate:
                mask &= self.activity_mask.get(activity, 0)
                if not mask:
                    break
        elif bk_type == "mult":
            for activity, occurrences in candidate:
                mask &= self.count_mask.get((activity, occurrences), 0)
                if not mask:
                    break
        else: # seq: prune with the set mask first, then verify the subsequence directly
            for activity in set(candidate):
                mask &= self.activity_mask.get(activity, 0)
                if not mask:
                    break
            for index in list(_bits(mask)):
                if not _is_subsequence(candidate, self.variants[index]):
                    mask &= ~(1 << index)

        self._mask_cache[cache_key] = mask
        return mask

    # |proj(x)| and ent(proj(x)) / max_ent(proj(x)) for one candidate
    def stats(self, bk_type: str, candidate) -> tuple[int, float]:
        mask = self._mask(bk_type, candidate)
        cached = self._stats_cache.get(mask)
        if cached is not None:
            return cached

        matching = [self.counts[index] for index in _bits(mask)]
        total = sum(matching)
        if total == 0:
            result = (0, 0.0)
        elif len(matching) <= 1:
            # a single matching variant: no uncertainty about the trace is left (entropy ratio = 0, trace disclosure contribution = 1)
            result = (total, 0.0)
        else:
            entropy = 0.0
            for count in matching:
                frequency = count / total
                entropy -= frequency * math.log2(frequency)
            ratio = min(1.0, max(0.0, entropy / math.log2(len(matching))))
            result = (total, ratio)

        self._stats_cache[mask] = result
        return result


# iterate the set bit positions of a bitmask
def _bits(mask: int):
    while mask:
        lowest = mask & -mask
        yield lowest.bit_length() - 1
        mask ^= lowest


def _is_subsequence(candidate: tuple, trace: tuple) -> bool:
    position = 0
    for activity in trace:
        if position < len(candidate) and activity == candidate[position]:
            position += 1
    return position == len(candidate)


# the background knowledge that a set of named activities gives about one case, per type
def build_candidates(named: set[str], truth_case: dict, bk_types) -> dict:
    candidates = {}
    if "set" in bk_types:
        candidates["set"] = frozenset(named)
    if "mult" in bk_types:
        candidates["mult"] = tuple(sorted((activity, count)
                                          for activity, count in truth_case["activities"].items()
                                          if activity in named))
    if "seq" in bk_types:
        candidates["seq"] = tuple(activity for activity in truth_case["trace"] if activity in named)
    return candidates


# observations about one system
class SystemView:

    def __init__(self, observations: dict, truth: dict, index: "DisclosureIndex", use_coordination: bool, bk_types):
        self.system = observations["system"]
        self.pool = list(observations["attacker_pool"])
        self.node_roles = observations.get("node_roles", {})
        self.truth = truth
        self.index = index
        self.bk_types = tuple(bk_types)
        self.use_coordination = use_coordination

        # activities a node can name because they were routed to it
        self.local: dict[int, dict[str, set[str]]] = {} # node id -> (case -> event ids)
        for node, cases in observations.get("local_events", {}).items():
            per_node = self.local.setdefault(int(node), {})
            for case_id, events in cases.items():
                per_node.setdefault(case_id, set()).update(events)

        # occurrences a node learns about from coordination traffic without the activity name
        self.partners: dict[int, dict[str, set[tuple]]] = {}
        for node, cases in observations.get("inferred_partners", {}).items():
            per_node = self.partners.setdefault(int(node), {})
            for case_id, pairs in cases.items():
                per_node.setdefault(case_id, set()).update((constraint_id, int(partner)) for constraint_id, partner in pairs)

        self.model_facts: dict[int, set[str]] = {int(node): set(facts) for node, facts in observations["model_facts"]["per_node"].items()}
        self.model_facts_total = observations["model_facts"]["total"]
        placement = observations.get("placement_facts") or {"per_node": {}, "total": 0}
        self.placement_facts: dict[int, set[str]] = {int(node): set(facts) for node, facts in placement["per_node"].items()}
        self.placement_facts_total = placement["total"]

        self.case_end: dict[int, dict[str, dict]] = {int(node): cases for node, cases in observations.get("case_end_seen", {}).items()}
        self.case_end_truth = observations.get("case_end_truth", {})
        self.peer_to_node = {address: int(node) for address, node in observations.get("peer_to_node", {}).items()}

        # cases that appear anywhere in this run and in the ground truth
        seen = {case for cases in self.local.values() for case in cases}
        seen |= {case for cases in self.partners.values() for case in cases}
        self.cases = sorted(seen & set(truth))


    # evaluate a draw of compromised nodes
    def evaluate(self, compromised: list[int]) -> dict:
        compromised_set = set(compromised)

        named_per_case: dict[str, set[str]] = {} # case -> named activities
        for node in compromised_set:
            for case_id, events in self.local.get(node, {}).items():
                named_per_case.setdefault(case_id, set()).update(events)

        anonymous_per_case: dict[str, set[tuple]] = {} # case -> (constraint, partner node)
        if self.use_coordination:
            for node in compromised_set:
                for case_id, pairs in self.partners.get(node, {}).items():
                    # a compromised partner contributes its activities by name
                    outside = {pair for pair in pairs if pair[1] not in compromised_set}
                    if outside:
                        anonymous_per_case.setdefault(case_id, set()).update(outside)

        by_activity, by_occurrence, with_anonymous = [], [], []
        full_cases = 0
        disclosure = {bk_type: {"uniqueness": [], "entropy_ratio": []} for bk_type in self.bk_types}
        knowledge_sizes = []

        for case_id in self.cases:
            truth_case = self.truth[case_id]
            activities = truth_case["activities"]
            named = named_per_case.get(case_id, set()) & set(activities)
            occurrences = sum(activities.values())

            fraction_named = len(named) / len(activities)
            fraction_occurrence = sum(count for activity, count in activities.items() if activity in named) / occurrences
            # every anonymous occurrence says that one more activity of this case happened without saying which one
            anonymous = len(anonymous_per_case.get(case_id, set()))
            fraction_with_anonymous = min(1.0, (len(named) + anonymous) / len(activities))

            by_activity.append(fraction_named)
            by_occurrence.append(fraction_occurrence)
            with_anonymous.append(fraction_with_anonymous)
            if fraction_named >= 1.0:
                full_cases += 1

            # anonymous occurrences cannot match a trace, so they do not count
            if not named:
                continue
            knowledge_sizes.append(len(named))
            for bk_type, candidate in build_candidates(named, truth_case, self.bk_types).items():
                matching_traces, entropy_ratio = self.index.stats(bk_type, candidate)
                if matching_traces == 0:
                    continue
                disclosure[bk_type]["uniqueness"].append(1.0 / matching_traces)
                disclosure[bk_type]["entropy_ratio"].append(entropy_ratio)

        learned_model: set[str] = set()
        learned_placement: set[str] = set()
        for node in compromised_set:
            learned_model |= self.model_facts.get(node, set())
            learned_placement |= self.placement_facts.get(node, set())

        first_spy = self._first_spy(compromised_set)

        total_reconstruction = 1.0 if (self.cases and full_cases == len(self.cases)) else 0.0
        any_full_case = 1.0 if full_cases else 0.0

        result = {
            "total_reconstruction_probability": total_reconstruction,
            "any_full_case_probability": any_full_case,
            "case_exposure_named": (sum(by_activity) / len(by_activity)) if by_activity else 0.0,
            "case_exposure_occurrence": (sum(by_occurrence) / len(by_occurrence)) if by_occurrence else 0.0,
            "case_exposure_with_anonymous": (sum(with_anonymous) / len(with_anonymous)) if with_anonymous else 0.0,
            "full_case_fraction": (full_cases / len(self.cases)) if self.cases else 0.0,
            "model_fact_disclosure": (len(learned_model) / self.model_facts_total) if self.model_facts_total else 0.0,
            "placement_fact_disclosure": (len(learned_placement) / self.placement_facts_total) if self.placement_facts_total else 0.0,
            "cases_with_knowledge_fraction": (len(knowledge_sizes) / len(self.cases)) if self.cases else 0.0,
            "background_knowledge_size": (sum(knowledge_sizes) / len(knowledge_sizes)) if knowledge_sizes else 0.0,
            **first_spy,
        }

        for bk_type in self.bk_types:
            uniqueness = disclosure[bk_type]["uniqueness"]
            entropy_ratios = disclosure[bk_type]["entropy_ratio"]
            result[f"case_disclosure_{bk_type}"] = (sum(uniqueness) / len(uniqueness)) if uniqueness else 0.0
            result[f"trace_disclosure_{bk_type}"] = (max(0.0, 1.0 - sum(entropy_ratios) / len(entropy_ratios)) if entropy_ratios else 0.0)
        return result

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


    # greedy set cover per case to find out how many nodes are enough to name the whole case
    def min_nodes_per_case(self) -> dict:

        counts = []
        for case_id in self.cases:
            needed = set(self.truth[case_id]["activities"])
            available = {node: self.local.get(node, {}).get(case_id, set()) & needed for node in self.pool}
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


# keep the announced attacker pool sizes so its not repeated for every run and system
_ANNOUNCED_POOL_SIZES: set[tuple] = set()


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


def evaluate_system(view: SystemView, draws: int, seed: int, sizes: list[int] | None, reporter=None, label: str = "") -> dict:
    rng = random.Random(seed)
    pool = view.pool
    points = compromise_sizes(len(pool), sizes)
    curves: dict[str, dict[str, list]] = {}
    per_size = []

    announcement_key = (len(pool), tuple(points))
    if reporter is not None and announcement_key not in _ANNOUNCED_POOL_SIZES:
        _ANNOUNCED_POOL_SIZES.add(announcement_key)
        spacing = "given with --sizes" if sizes else ("every size" if len(pool) <= 12 else "spaced over the pool")
        reporter.log(f"attacker set sizes for a pool of {len(pool)} node(s), {len(points)} point(s), "
                     f"{spacing}: {', '.join(str(point) for point in points)}")

    for size_index, size in enumerate(points, start=1):
        if reporter is not None:
            reporter.note(f"{label}: {size} of {len(pool)} nodes compromised, point {size_index}/{len(points)}",
                          fraction=(size_index - 1) / len(points))
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
        "model_facts_total": view.model_facts_total,
        "placement_facts_total": view.placement_facts_total,
        "compromise_sizes": points,
        "background_knowledge_size_observed": {str(entry["compromised"]):
                                               entry.get("background_knowledge_size", {}).get("mean")
                                               for entry in per_size},
        "per_size": per_size,
        "curves": curves,
        "min_nodes_full_case": view.min_nodes_per_case(),
    }


# metadata volume the global attacker sees
def global_attacker(observations: dict) -> dict:

    node_roles = observations.get("node_roles", {})
    case_end = {node: cases for node, cases in observations.get("case_end_seen", {}).items()
                if node_roles.get(str(node)) != "sink"}
    sightings = sum(len(cases) for cases in case_end.values())
    cases = {case for per_node in case_end.values() for case in per_node}
    return {
        "nodes_observed": len(case_end),
        "case_end_sightings": sightings,
        "cases_with_gossip": len(cases),
        "sightings_per_case": (sightings / len(cases)) if cases else 0.0,
        "note": "metadata only: message timing, size, sender and receiver, no payload. the result "
                "sink is excluded: it is evaluation scaffolding, not part of the system",
    }


# the disclosure risk of the ground truth log itself
def log_baseline_disclosure(ground_truth_event_log: dict, index, bk_types) -> dict:
    distinct_traces = (len(index.variants) if index is not None
                       else len({case["trace"] for case in ground_truth_event_log.values()}))
    activities_per_case = [float(len(case["activities"])) for case in ground_truth_event_log.values()]
    events_per_case = [float(len(case["trace"])) for case in ground_truth_event_log.values()]
    baseline = {"cases": len(ground_truth_event_log),
                "events": int(sum(events_per_case)),
                "distinct_traces": distinct_traces,
                "trace_uniqueness": (distinct_traces / len(ground_truth_event_log))
                                    if ground_truth_event_log else 0.0,
                "distinct_activities": len({activity for case in ground_truth_event_log.values()
                                            for activity in case["activities"]}),
                "background_knowledge_size_max": percentiles(activities_per_case, points=(5, 50, 95)),
                "events_per_case": percentiles(events_per_case, points=(5, 50, 95))}
    if index is None:
        return baseline
    for bk_type in bk_types:
        uniqueness, entropy_ratios = [], []
        for case in ground_truth_event_log.values():
            candidate = build_candidates(set(case["activities"]), case, (bk_type,))[bk_type]
            matching_traces, entropy_ratio = index.stats(bk_type, candidate)
            if matching_traces:
                uniqueness.append(1.0 / matching_traces)
                entropy_ratios.append(entropy_ratio)
        baseline[f"case_disclosure_{bk_type}"] = (sum(uniqueness) / len(uniqueness)) if uniqueness else 0.0
        baseline[f"trace_disclosure_{bk_type}"] = (max(0.0, 1.0 - sum(entropy_ratios) / len(entropy_ratios)) if entropy_ratios else 0.0)
    return baseline


# cache ground truth (from event log/manifest) - holds parsed log, log baseline disclosure and trace variant index
_GROUND_TRUTH_CACHE: dict[tuple, dict] = {} # (manifest identity uple) -> (log, index, baselines)
_GROUND_TRUTH_CACHE_LIMIT = 4


def _manifest_identity(manifest: Path) -> tuple:
    stat = manifest.stat()
    return (str(manifest.resolve()), stat.st_size, int(stat.st_mtime))


# returns (log, index, baselines) and builds only what is missing
def ground_truth_for(manifest: Path, bk_types: tuple, reporter=None, label: str = "") -> tuple:
    key = _manifest_identity(manifest)
    entry = _GROUND_TRUTH_CACHE.get(key)

    if entry is None:
        if reporter is not None:
            reporter.note(f"{label}: reading the ground truth log")
        entry = {"log": load_ground_truth(manifest), "index": None, "baselines": {}}
        _GROUND_TRUTH_CACHE[key] = entry
        while len(_GROUND_TRUTH_CACHE) > _GROUND_TRUTH_CACHE_LIMIT:
            _GROUND_TRUTH_CACHE.pop(next(iter(_GROUND_TRUTH_CACHE)))
    elif reporter is not None:
        reporter.note(f"{label}: ground truth log from cache ({len(entry['log'])} cases)")

    if bk_types and entry["index"] is None:
        if reporter is not None:
            reporter.note(f"{label}: indexing {len(entry['log'])} cases")
        entry["index"] = DisclosureIndex(entry["log"])

    if bk_types not in entry["baselines"]:
        if reporter is not None and bk_types:
            reporter.note(f"{label}: log baseline for {', '.join(bk_types)}")
        entry["baselines"][bk_types] = log_baseline_disclosure(entry["log"], entry["index"], bk_types)

    return entry["log"], entry["index"], entry["baselines"][bk_types]


def process_run(run_dir: Path, args, reporter=None) -> str:
    bk_types = tuple(args.bk_type) if not args.no_disclosure else ()
    observations_path = run_dir / "observations.json.gz"
    if not observations_path.exists():
        if reporter is not None:
            reporter.step(f"{run_dir.name} skipped")
        return f"skip {run_dir.name}: no observations.json.gz (run extract_metrics.py first)"
    with gzip.open(observations_path, "rt") as observations_file:
        observations = json.load(observations_file)

    manifest = args.manifest
    if manifest is None:
        run_spec = json.loads((run_dir / "run.json").read_text())
        candidate = (run_spec.get("stream") or {}).get("manifest")
        manifest = Path(candidate) if candidate else None
    if manifest is None or not Path(manifest).exists():
        if reporter is not None:
            reporter.step(f"{run_dir.name} skipped")
        return f"skip {run_dir.name}: no manifest (pass --manifest)"
    ground_truth_event_log, index, baseline = ground_truth_for(Path(manifest), bk_types, reporter, run_dir.name)

    payload = {
        "schema_version": SCHEMA_VERSION,
        "run_id": observations.get("run_id"),
        "experiment": observations.get("experiment"),
        "params": observations.get("params"),
        "settings": {"seed": args.seed, "draws": args.draws,
                     "use_coordination_traffic": not args.local_events_only,
                     "bk_types": list(bk_types),
                     "disclosure_measures": bool(bk_types),
                     "compromise_sizes": args.sizes,
                     "manifest": str(manifest)},
        "ground_truth": baseline,
        "systems": {},
    }

    for system, system_observations in observations.get("systems", {}).items():
        view = SystemView(system_observations, ground_truth_event_log, index,
                          use_coordination=not args.local_events_only, bk_types=bk_types)
        payload["systems"][system] = {
            "local_attacker": evaluate_system(view, args.draws, args.seed, args.sizes, reporter,
                                              f"{run_dir.name} {system}"),
            "global_attacker": global_attacker(system_observations),
        }
        if reporter is not None:
            reporter.step(f"{run_dir.name} {system}")

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
    parser.add_argument("--bk-type", nargs="+", choices=BK_TYPES, default=["set"],
                        help="background knowledge types for the disclosure measures (default: set).  'set' is the cheap one; 'mult' and 'seq' " \
                        "cost roughly one extra pass each and only pay off on a log with repeated variants (sepsis, traffic fines)")
    parser.add_argument("--no-disclosure", action="store_true",
                        help="skip case_disclosure and trace_disclosure entirely and do not build the variant index. use this on synthetic streams, " \
                        "where nearly every trace is unique, so both measures saturate and describe the generator rather than the deployment. every " \
                        "other metric (exposure, model facts, first spy, min nodes) is unaffected")
    parser.add_argument("--local-events-only", action="store_true", help="ignore what coordination traffic reveals (lower bound on exposure)")
    parser.add_argument("--out", default="privacy.json", help="output file name inside the run dir")
    parser.add_argument("--quiet", action="store_true", help="no progress output")
    args = parser.parse_args()

    runs = list(args.run)
    if args.results:
        runs += discover_runs(args.results, args.experiment, "observations.json.gz")
    if not runs:
        sys.exit("nothing to do: pass --results and/or --run")
    def system_count(run_dir: Path) -> int:
        try:
            run_spec = json.loads((run_dir / "run.json").read_text())
            return max(1, len(run_spec.get("systems_timeline", [])))
        except (OSError, json.JSONDecodeError):
            return 2

    progress = Progress(sum(system_count(run_dir) for run_dir in runs), "privacy", enabled=not args.quiet)
    for run_dir in runs:
        progress.note(f"{run_dir.name}: reading observations")
        message = process_run(run_dir, args, progress)   # steps once per system
        progress.log(message)
    progress.finish()


if __name__ == "__main__":
    main()