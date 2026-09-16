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
  model_disclosure = fraction of the process model atoms the attacker learns (activity, constraint, constraint_type, binding, param)
  deployment_disclosure = fraction of the deployment atoms the attacker learns (place, counterpart, watch)
  model_disclosure_<kind> / deployment_disclosure_<kind> = disclosure per atom kind
  model_disclosure_balanced = mean of the per kind fractions (robustness against the choice of atomization)
  *_config_only = the same fractions when coordination traffic is ignored (isolates what the traffic itself leaks)
  case_disclosure_<type> = identity disclosure (rafiei and van der aalst (2021))
  trace_disclosure_<type> = attribute disclosure (rafiei and van der aalst (2021))
  first_spy = precision and recall of guessing the origin of the case end gossip as the source_peer of the earliest observation among the compromised nodes
"""
from __future__ import annotations

import argparse
import gzip
import itertools
import json
import math
import random
import sys
from collections import Counter
from pathlib import Path
from helpers import (percentiles, discover_runs, load_system_configs, Progress,
                     BK_TYPES, DISCLOSURE_INDEX_NAME, DisclosureIndex, build_candidates,
                     load_disclosure_index, load_ground_truth, log_baseline_disclosure)


SCHEMA_VERSION = 4

# cap the recorded distinct values at 128 since thats enough to draw a smooth curve
DISTRIBUTION_MAX_DISTINCT = 128
DISTRIBUTION_QUANTILES = (1, 5, 10, 25, 50, 75, 90, 95, 99)
# round to 6 decimals to reduce fraction noise
DISTRIBUTION_DECIMALS = 6

# below this pool size every compromise size is evaluated
# above it the sizes are spaced over the pool, since the node sweeps go up to pool sizes where every size would be wasteful
EVERY_SIZE_UP_TO = 24


# TYPE(r): role -> constraint type
ROLE_TYPE = {
    "ResponseProducer": "Response",
    "ResponseConsumer": "Response",
    "EndActivity": "EndActivity",
    "PrecedenceProducer": "Precedence",
    "PrecedenceConsumer": "Precedence",
    "OrderFirst": "Order",
    "OrderSecond": "Order",
    "SensorCollector": "Sensor",
    "SensorTrigger": "Sensor",
    "TimeStart": "Time",
    "TimeStop": "Time",
    "NotCoexistenceA": "NotCoexistence",
    "NotCoexistenceB": "NotCoexistence",
    "AppearanceCounter": "Appearance",
    "AppearanceTrigger": "Appearance",
    "DistributedAppearanceCounter": "DistributedAppearance",
    "DistributedAppearanceTrigger": "DistributedAppearance",
}

# ROLES(T): template/constraint type -> roles
TYPE_ROLES: dict[str, tuple[str, ...]] = {}
for _role, _type in ROLE_TYPE.items():
    TYPE_ROLES[_type] = tuple(sorted(TYPE_ROLES.get(_type, ()) + (_role,)))

# OTHER(r): role -> counterpart role
COUNTERPART_ROLE = {role: next((other for other in TYPE_ROLES[constraint_type] if other != role), None)
                    for role, constraint_type in ROLE_TYPE.items()}

MODEL_KINDS = ("activity", "constraint", "constraint_type", "binding", "param")
DEPLOYMENT_KINDS = ("place", "counterpart", "watch")
DERIVED_ONLY_KINDS = ("role",)

# config keys that are not parameters of the declarative model
STRUCTURAL_KEYS = ("constraint_role", "constraint_id", "event_id")


def is_peer_key(key: str) -> bool:
    return key.endswith("_peer") or key.endswith("_peers")


# a stable string for a parameter value so that param(c, k, v) atoms compare by value
def param_value(value) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(param_value(item) for item in value) + "]"
    return str(value)


def atom_text(atom: tuple) -> str:
    return f"{atom[0]}({','.join(str(argument) for argument in atom[1:])})"


# derive facts from what is written in the config using the following rules
#   R1:  place(c, r, n)                    -> constraint(c), role(c, r)
#   R2:  binding(c, r, a)                  -> constraint(c), role(c, r), activity(a)
#   R3:  role(c, r)                        -> constraint_type(c, TYPE(r))
#   R4:  constraint_type(c, T)             -> role(c, r') for all r' in ROLES(T)
#   R5:  counterpart(c, r, p)              -> constraint(c), place(c, OTHER(r), p)
#   R6:  watch(a, n)                       -> activity(a)
#   R7:  place(c, r, n) & binding(c, r, a) -> watch(a, n)
#   R8:  param(c, k, v)                    -> constraint(c)
#   R9:  coordination traffic for c seen at n holding role(c, r) from remote p 
#                                          -> counterpart(c, r, p)
#   R10: place(c, r, n) & place(c, r', p), r' = OTHER(r), n != p 
#                                          -> counterpart(c, r, p), counterpart(c, r', n)
# R9 is actually applied after everything else has been derived in traffic_facts()
def derive_facts(written_config_facts: set) -> set:
    known_facts = set(written_config_facts)
    fact_work_list = list(written_config_facts)
    places: dict[str, set] = {} # constraint id -> {(role, node)}
    bindings: dict[tuple, set] = {} # (constraint id, role) -> {activity}

    def add(atom: tuple) -> None:
        if atom not in known_facts:
            known_facts.add(atom)
            fact_work_list.append(atom)

    while fact_work_list:
        atom = fact_work_list.pop()
        kind = atom[0]

        if kind == "place":
            _, constraint_id, role, node = atom
            add(("constraint", constraint_id))                                       # R1
            add(("role", constraint_id, role))                                       # R1
            for activity in bindings.get((constraint_id, role), ()):
                add(("watch", activity, node))                                       # R7
            other = COUNTERPART_ROLE.get(role)
            placed = places.setdefault(constraint_id, set())
            if other is not None:
                for other_role, other_node in placed:
                    if other_role == other and other_node != node:
                        add(("counterpart", constraint_id, role, other_node))        # R10
                        add(("counterpart", constraint_id, other_role, node))        # R10
            placed.add((role, node))

        elif kind == "binding":
            _, constraint_id, role, activity = atom
            add(("constraint", constraint_id))                                       # R2
            add(("role", constraint_id, role))                                       # R2
            add(("activity", activity))                                              # R2
            bindings.setdefault((constraint_id, role), set()).add(activity)
            for placed_role, node in places.get(constraint_id, ()):
                if placed_role == role:
                    add(("watch", activity, node))                                   # R7

        elif kind == "role":
            _, constraint_id, role = atom
            add(("constraint_type", constraint_id, ROLE_TYPE.get(role, role)))       # R3

        elif kind == "constraint_type":
            _, constraint_id, constraint_type = atom
            for role in TYPE_ROLES.get(constraint_type, ()):
                add(("role", constraint_id, role))                                   # R4

        elif kind == "counterpart":
            _, constraint_id, role, peer = atom
            add(("constraint", constraint_id))                                       # R5
            other = COUNTERPART_ROLE.get(role)
            if other is not None:
                add(("place", constraint_id, other, peer))                           # R5

        elif kind == "watch":
            add(("activity", atom[1]))                                               # R6

        elif kind == "param":
            add(("constraint", atom[1]))                                             # R8

    return known_facts


def count_by_kind(atoms, kinds) -> dict[str, int]:
    counts = {kind: 0 for kind in kinds}
    for atom in atoms:
        if atom[0] in counts:
            counts[atom[0]] += 1
    return counts


# the facts of one system: base facts per node, the universe and the disclosure of a compromised set
class FactModel:

    def __init__(self, nodes, pool: list[int], observed_partners: dict[int, set[tuple]] | None = None):
        pool_set = set(pool)
        self.pool = list(pool)

        self.node_address: dict[int, str] = {}  # node id -> tcp address
        self.address_owner: dict[str, int] = {} # any listen address -> node id
        for node in nodes:
            address = str(node.tcp) if node.tcp else f"node-{node.node_id}"
            self.node_address[node.node_id] = address
            if node.tcp:
                self.address_owner[str(node.tcp)] = node.node_id
            if node.overlay:
                self.address_owner[str(node.overlay)] = node.node_id

        self.diagnostics = {"unresolved_peer_addresses": 0, "forwarders_without_event_ids": 0,
                            "nodes_without_tcp_address": sum(1 for node in nodes if not node.tcp),
                            "peer_addresses_resolved_via_overlay": 0,
                            "unknown_roles": [], "coordination_pairs": 0,
                            "coordination_without_local_role": 0, "coordination_outside_universe": 0}

        # base facts each node can read off its own config
        self.config_facts: dict[int, set] = {} # node id -> fact set
        self.roles_at_node: dict[int, dict[str, set]] = {}  # node id -> (constraint id -> {roles})
        for node in nodes:
            if node.node_id not in pool_set:
                # filters out the sink
                continue
            self.config_facts[node.node_id] = self._config_facts(node)

        # the universe: all facts over all nodes
        self.universe = derive_facts(set().union(*self.config_facts.values()) if self.config_facts else set())

        # apply R9 (whats learned additionally to the config from coordination traffic)
        self.traffic_facts: dict[int, set] = {}
        if observed_partners:
            for node, pairs in observed_partners.items():
                if node in pool_set:
                    self.traffic_facts[node] = self._traffic_facts(node, pairs)

        self.model_totals = count_by_kind(self.universe, MODEL_KINDS)
        self.deployment_totals = count_by_kind(self.universe, DEPLOYMENT_KINDS)
        self.model_total = sum(self.model_totals.values())
        self.deployment_total = sum(self.deployment_totals.values())
        self.has_traffic_facts = any(self.traffic_facts.values())

    # every constraint entry of a node config, plus what a forwarder gives away through its event filter list
    def _config_facts(self, node) -> set:
        facts: set = set()
        node_id = node.node_id
        address = self.node_address[node_id]
        roles_here: dict[str, set] = {}

        for constraint in node.constraints:
            constraint_id = constraint.get("constraint_id")
            role = constraint.get("constraint_role")
            activity = constraint.get("event_id")
            if constraint_id is None or role is None:
                continue
            if role not in ROLE_TYPE and role not in self.diagnostics["unknown_roles"]:
                self.diagnostics["unknown_roles"].append(role)
            roles_here.setdefault(constraint_id, set()).add(role)

            facts.add(("constraint", constraint_id))
            facts.add(("role", constraint_id, role))
            facts.add(("place", constraint_id, role, address))
            if activity is not None:
                facts.add(("binding", constraint_id, role, activity))
                facts.add(("activity", activity))
                facts.add(("watch", activity, address))

            for key, value in constraint.items():
                # skip event_id, constraint_id and constraint role
                if key in STRUCTURAL_KEYS:
                    continue
                if is_peer_key(key):
                    # a peer address places the counterpart (the config form of R9)
                    peer_addresses = [value] if isinstance(value, str) else list(value or [])
                    for peer_address in peer_addresses:
                        owner = self.address_owner.get(str(peer_address))
                        if owner is None:
                            self.diagnostics["unresolved_peer_addresses"] += 1
                            continue
                        if str(peer_address) != self.node_address[owner]:
                            self.diagnostics["peer_addresses_resolved_via_overlay"] += 1
                        if owner != node_id:
                            facts.add(("counterpart", constraint_id, role, self.node_address[owner]))
                    continue
                facts.add(("param", constraint_id, key, param_value(value)))

        # a forwarder discloses which activities it is asked to forward (activity(a) and watch(a, n))
        if node.role == "forwarder":
            if not node.event_ids:
                self.diagnostics["forwarders_without_event_ids"] += 1
            for activity in node.event_ids:
                facts.add(("watch", activity, address))
                facts.add(("activity", activity))

        self.roles_at_node[node_id] = roles_here
        return facts

    # R9: a coordination message for c from remote p, seen at a node that hosts role r of c, is like a peer field
    def _traffic_facts(self, node: int, pairs) -> set:
        facts: set = set()
        roles_here = self.roles_at_node.get(node, {})
        for constraint_id, peer in pairs:
            self.diagnostics["coordination_pairs"] += 1
            roles = roles_here.get(constraint_id)
            if not roles:
                # a message for a constraint this node hosts no role of (should not happen -> should always be 0)
                self.diagnostics["coordination_without_local_role"] += 1
                continue
            if peer == node:
                continue
            peer_address = self.node_address.get(peer)
            if peer_address is None:
                self.diagnostics["unresolved_peer_addresses"] += 1
                continue
            for role in roles:
                atom = ("counterpart", constraint_id, role, peer_address)
                if atom in self.universe:
                    facts.add(atom)
                else:
                    self.diagnostics["coordination_outside_universe"] += 1
        return facts

    # what a compromised set learns (union of all compromised nodes)
    def learned(self, compromised, use_coordination: bool) -> set:
        base: set = set()
        for node in compromised:
            base |= self.config_facts.get(node, frozenset())
            if use_coordination:
                base |= self.traffic_facts.get(node, frozenset())
        return derive_facts(base) if base else set()

    # model_disclosure, deployment_disclosure, the per kind fractions and the balanced means
    def disclosure(self, learned: set, suffix: str = "") -> dict:
        model_counts = count_by_kind(learned, MODEL_KINDS)
        deployment_counts = count_by_kind(learned, DEPLOYMENT_KINDS)
        result = {
            f"model_disclosure{suffix}": (sum(model_counts.values()) / self.model_total) if self.model_total else 0.0,
            f"deployment_disclosure{suffix}": (sum(deployment_counts.values()) / self.deployment_total)
                                              if self.deployment_total else 0.0,
        }
        model_fractions, deployment_fractions = [], []
        for kind, count in model_counts.items():
            total = self.model_totals[kind]
            if total:
                fraction = count / total
                result[f"model_disclosure_{kind}{suffix}"] = fraction
                model_fractions.append(fraction)
        for kind, count in deployment_counts.items():
            total = self.deployment_totals[kind]
            if total:
                fraction = count / total
                result[f"deployment_disclosure_{kind}{suffix}"] = fraction
                deployment_fractions.append(fraction)
        # the mean of the per kind fractions to check that a conclusion is not just an artifact of the chosn atomization
        if model_fractions:
            result[f"model_disclosure_balanced{suffix}"] = sum(model_fractions) / len(model_fractions)
        if deployment_fractions:
            result[f"deployment_disclosure_balanced{suffix}"] = sum(deployment_fractions) / len(deployment_fractions)
        return result

    # the static facts
    def summary(self) -> dict:
        constraint_types: dict[str, int] = {}
        for atom in self.universe:
            if atom[0] == "constraint_type":
                constraint_types[atom[2]] = constraint_types.get(atom[2], 0) + 1
        return {
            "model_total": self.model_total,
            "deployment_total": self.deployment_total,
            "model_totals_by_kind": self.model_totals,
            "deployment_totals_by_kind": self.deployment_totals,
            "catalog": {
                "activities": self.model_totals["activity"],
                "constraints": self.model_totals["constraint"],
                "roles": sum(1 for atom in self.universe if atom[0] == "binding"),
                "constraint_types": dict(sorted(constraint_types.items())),
            },
            "nodes_with_facts": len(self.config_facts),
            "coordination_traffic_available": self.has_traffic_facts,
            "diagnostics": self.diagnostics,
        }


# loads the actual event stream (event log) as ground truth
# case -> (activities: (event_id -> count), trace -> (event_id, ...))
class SystemView:

    def __init__(self, observations: dict, truth: dict, index: "DisclosureIndex", use_coordination: bool, bk_types, nodes=None):
        self.system = observations["system"]
        self.pool = list(observations["attacker_pool"])
        self.node_roles = observations.get("node_roles", {})
        self.truth = truth
        self.placement = observations.get("node_placement") or {}
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

        self.facts = None
        if nodes:
            # the distinct (constraint, remote node) pairs a node saw in coordination traffic over all cases (for R9)
            observed_partners: dict[int, set[tuple]] = {} # node id -> (constraint id, node id)
            for node, cases in self.partners.items():
                pairs = observed_partners.setdefault(node, set())
                for case_pairs in cases.values():
                    pairs |= case_pairs
            self.facts = FactModel(nodes, self.pool, observed_partners)

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

        fact_disclosure: dict[str, float] = {}
        if self.facts is not None:
            fact_disclosure = self.facts.disclosure(self.facts.learned(compromised_set, self.use_coordination))
            if self.use_coordination and self.facts.has_traffic_facts:
                fact_disclosure.update(self.facts.disclosure(self.facts.learned(compromised_set, False), suffix="_config_only"))
                fact_disclosure["deployment_disclosure_traffic_gain"] = (
                    fact_disclosure["deployment_disclosure"] - fact_disclosure["deployment_disclosure_config_only"])
                fact_disclosure["model_disclosure_traffic_gain"] = (
                    fact_disclosure["model_disclosure"] - fact_disclosure["model_disclosure_config_only"])

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
            "cases_with_knowledge_fraction": (len(knowledge_sizes) / len(self.cases)) if self.cases else 0.0,
            "background_knowledge_size": (sum(knowledge_sizes) / len(knowledge_sizes)) if knowledge_sizes else 0.0,
            **fact_disclosure,
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
        by_distinct_activities: dict[int, list[int]] = {}
        witnesses: dict[int, dict] = {}
        for case_id in self.cases:
            needed = set(self.truth[case_id]["activities"])
            available = {node: self.local.get(node, {}).get(case_id, set()) & needed for node in self.pool}
            chosen = 0
            picked: list = []
            remaining = set(needed)
            while remaining:
                best = max(available.items(), key=lambda item: len(item[1] & remaining),
                           default=(None, set()))
                if not best[1] & remaining:
                    chosen = None
                    break
                picked.append((best[0], sorted(best[1] & remaining)))
                remaining -= best[1]
                available.pop(best[0])
                chosen += 1
            if chosen is not None:
                counts.append(chosen)
                by_distinct_activities.setdefault(len(needed), []).append(chosen)
                witness = {"case_id": case_id, "nodes_needed": chosen,
                           "distinct_activities": sorted(needed),
                           "trace": list(self.truth[case_id]["trace"]),
                           "cover": [{"node": node, "named": activities,
                                      "watches": (self.placement.get(str(node), {})
                                                  .get("watches"))}
                                     for node, activities in picked]}
                slot = witnesses.setdefault(len(needed), {})
                if not slot or chosen < slot["cheapest"]["nodes_needed"]:
                    slot["cheapest"] = witness
                if not slot.get("dearest") or chosen > slot["dearest"]["nodes_needed"]:
                    slot["dearest"] = witness
        histogram: dict[str, int] = {}
        for count in counts:
            histogram[str(count)] = histogram.get(str(count), 0) + 1

        by_distinct: dict[str, dict] = {}
        for distinct, group in sorted(by_distinct_activities.items()):
            slot = witnesses.get(distinct, {})
            by_distinct[str(distinct)] = {"cases": len(group),
                                          **percentiles([float(count) for count in group],
                                                        points=(5, 50, 95)),
                                          "cheapest_case": slot.get("cheapest"),
                                          "dearest_case": slot.get("dearest")}

        return {"cases_covered": len(counts), "cases_total": len(self.cases), "histogram": histogram,
                "by_distinct_activities": by_distinct,
                **percentiles([float(count) for count in counts], points=(5, 50, 95))}


# keep the announced attacker pool sizes so its not repeated for every run and system
_ANNOUNCED_POOL_SIZES: set[tuple] = set()


def compromise_sizes(pool_size: int, requested: list[int] | None) -> list[int]:
    if requested:
        return [size for size in requested if 1 <= size <= pool_size]
    if pool_size <= EVERY_SIZE_UP_TO:
        return list(range(1, pool_size + 1))
    sizes = {1, 2, 3, pool_size}
    fraction = 0.05
    while fraction < 1.0:
        sizes.add(max(1, round(pool_size * fraction)))
        fraction *= 1.6
    return sorted(sizes)


# the draws of one metric as a distribution (for drawing an ecdf)
# exact counts while the metric takes few distinct values, a quantile ladder once it does not
def distribution_of(values: list[float]) -> dict:
    counts = Counter(round(value, DISTRIBUTION_DECIMALS) for value in values)
    if len(counts) <= DISTRIBUTION_MAX_DISTINCT:
        ordered = sorted(counts.items())
        return {"kind": "counts",
                "values": [value for value, _ in ordered],
                "counts": [count for _, count in ordered]}
    ladder = percentiles(values, points=DISTRIBUTION_QUANTILES)
    return {"kind": "quantiles",
            "points": list(DISTRIBUTION_QUANTILES),
            "values": [ladder[f"p{point}"] for point in DISTRIBUTION_QUANTILES]}


def evaluate_system(view: SystemView, draws: int, seed: int, sizes: list[int] | None, reporter=None, label: str = "") -> dict:
    rng = random.Random(seed)
    pool = view.pool
    points = compromise_sizes(len(pool), sizes)
    curves: dict[str, dict[str, list]] = {}
    per_size = []

    announcement_key = (len(pool), tuple(points))
    if reporter is not None and announcement_key not in _ANNOUNCED_POOL_SIZES:
        _ANNOUNCED_POOL_SIZES.add(announcement_key)
        spacing = "given with --sizes" if sizes else ("every size" if len(pool) <= EVERY_SIZE_UP_TO else "spaced over the pool")
        reporter.log(f"attacker set sizes for a pool of {len(pool)} node(s), {len(points)} point(s), "
                     f"{spacing}: {', '.join(str(point) for point in points)}")

    for size_index, size in enumerate(points, start=1):
        if reporter is not None:
            reporter.note(f"{label}: {size} of {len(pool)} nodes compromised, point {size_index}/{len(points)}",
                          fraction=(size_index - 1) / len(points))
        results: dict[str, list[float]] = {}
        # exhaustive when there is only one distinct subset, sampled otherwise
        subset_count = math.comb(len(pool), size)
        exhaustive = subset_count <= draws
        subsets = (itertools.combinations(pool, size) if exhaustive
                   else (rng.sample(pool, size) for _ in range(draws)))
        repeats = subset_count if exhaustive else draws
        for subset in subsets:
            for key, value in view.evaluate(list(subset)).items():
                if value is not None:
                    results.setdefault(key, []).append(value)

        per_size.append({"compromised": size,
                         "compromised_fraction": size / len(pool) if pool else None,
                         "draws": repeats,
                         "exhaustive": exhaustive,
                         "distributions": {key: distribution_of(values) for key, values in results.items()},
                         **{key: percentiles(values, points=(5, 50, 95)) for key, values in results.items()}})

    for key in {key for entry in per_size for key in entry
                if isinstance(entry[key], dict) and "n" in entry[key]}:
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
        "facts": view.facts.summary() if view.facts is not None else None,
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
def _index_file_for(manifest: Path, explicit: Path | None) -> Path | None:
    if explicit is not None:
        return explicit if explicit.exists() else None
    candidate = manifest.parent / DISCLOSURE_INDEX_NAME
    return candidate if candidate.exists() else None


# cache ground truth (from event log/manifest) - holds parsed log, log baseline disclosure and trace variant index
_GROUND_TRUTH_CACHE: dict[tuple, dict] = {} # (manifest identity uple) -> (log, index, baselines)
_GROUND_TRUTH_CACHE_LIMIT = 4


def _manifest_identity(manifest: Path) -> tuple:
    stat = manifest.stat()
    return (str(manifest.resolve()), stat.st_size, int(stat.st_mtime))


# returns (log, index, baselines) and builds only what is missing
def ground_truth_for(manifest: Path, bk_types: tuple, reporter=None, label: str = "",
                     index_file: Path | None = None) -> tuple:
    # a prebuilt index identifies the stream on its own; the manifest may well be gone by now
    if index_file is not None:
        key = ("index", str(index_file))
    elif manifest is not None:
        key = _manifest_identity(manifest)
    else:
        raise SystemExit("ground_truth_for needs a manifest or a prebuilt disclosure index")
    entry = _GROUND_TRUTH_CACHE.get(key)

    if entry is None:
        prebuilt = load_disclosure_index(index_file) if index_file is not None else None
        if prebuilt is not None:
            if reporter is not None:
                reporter.note(f"{label}: ground truth from {index_file.name}")
            entry = {"log": {case_id: {"activities": case["activities"],
                                       "trace": tuple(case["trace"])}
                             for case_id, case in prebuilt["cases"].items()},
                     "index": None, "baselines": {}, "prebuilt": prebuilt}
            if prebuilt.get("variants") is not None:
                entry["index"] = DisclosureIndex(variants=prebuilt["variants"],
                                                 counts=prebuilt.get("variant_counts"))
            stored = prebuilt.get("baselines") or {}
            for key_text, baseline in stored.items():
                entry["baselines"][tuple(part for part in key_text.split("|") if part)] = baseline
        else:
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
    index_file = None
    if args.disclosure_index is not None:
        index_file = args.disclosure_index if args.disclosure_index.exists() else None
    elif manifest is not None:
        index_file = _index_file_for(Path(manifest), None)
    if index_file is None and (manifest is None or not Path(manifest).exists()):
        if reporter is not None:
            reporter.step(f"{run_dir.name} skipped")
        return (f"skip {run_dir.name}: no manifest and no {DISCLOSURE_INDEX_NAME} "
                f"(pass --manifest or --disclosure-index, or run build_disclosure_index.py)")
    ground_truth_event_log, index, baseline = ground_truth_for(
        Path(manifest) if manifest is not None else None, bk_types, reporter, run_dir.name,
        index_file=index_file)

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
                     "manifest": str(manifest) if manifest is not None else None,
                     "disclosure_index": str(index_file) if index_file is not None else None},
        "ground_truth": baseline,
        "systems": {},
    }

    for system, system_observations in observations.get("systems", {}).items():
        config_dir = run_dir / "sys" / system / "configs"
        nodes = load_system_configs(config_dir) if config_dir.is_dir() else []
        if not nodes and reporter is not None:
            reporter.log(f"{run_dir.name} {system}: no node configs under {config_dir}, fact disclosure skipped")

        view = SystemView(system_observations, ground_truth_event_log, index, use_coordination=not args.local_events_only, bk_types=bk_types, nodes=nodes)
        payload["systems"][system] = {
            "placement": system_observations.get("node_placement") or {},
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
    parser.add_argument("--disclosure-index", type=Path, default=None,
                        help=f"prebuilt ground truth from build_disclosure_index.py. by default the "
                             f"{DISCLOSURE_INDEX_NAME} next to the manifest is used when it exists, which "
                             f"skips re-parsing and re-indexing the stream on every run and lets the stream "
                             f"directory be deleted")
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