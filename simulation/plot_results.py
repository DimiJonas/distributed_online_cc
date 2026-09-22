#!/usr/bin/env python3
"""
plot_results.py - plot the results of the runs

reads the metrics.json (extract_metrics.py) and the privacy.json (compute_privacy.py)
aggregates over seeds and replicates and writes figures and tables

  python3.11 simulation/plot_results.py --results results/
  python3.11 simulation/plot_results.py --results results/ --experiment 'E5*' --format pdf
  python3.11 simulation/plot_results.py --results results/ --out thesis/figures

output (under <results>/_plots/ unless --out says otherwise)

  runs.csv = one row per (run, system): parameters and the headline metrics
  privacy.csv = one row per (run, system, compromise size)
  fact_universe.csv = the denominators behind the disclosure fractions (one row per (run, system))
  feasibility.csv = correctness aggregated over all runs
  <experiment>/*.png = the figures of that experiment

which figures an experiment gets is decided by what is in its runs:

  a parameter sweep -> scalability panels against that parameter (rate, nodes, repeat, cap, ...)
  runs with no workload -> idle cost panels (per node cost against node count)
  netem varies -> latency against the delay (one line per rate)
  privacy.json present -> exposure, disclosure and first spy curves against the compromise size,
  one factor at a time -> sensitivity panels (one small multiple per varied parameter)

the x axis is the parameter that actually varies inside the experiment
if two parameters vary, the second becomes the series
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

sys.path.insert(0, str(Path(__file__).resolve().parent))
from helpers import discover_runs, Progress


SYSTEM_STYLE = {
    "decentralized": {"color": "#0072B2", "marker": "o", "label": "decentralized"},
    "central":       {"color": "#D55E00", "marker": "s", "label": "central"},
}
SERIES_LINESTYLES = ["-", "--", ":", "-."]
PAYLOAD_COLORS = {"conformance": "#0072B2", "ce_gossip": "#56B4E9", "overlay": "#009E73", "event": "#D55E00", "result": "#CC79A7", 
                  "hello": "#999999", "cli": "#666666"}

LOAD_CLASSES = ("conformance", "gossip_case_end", "event", "result")
# exclude the result message form the message breakdown panels because its not part of the system
LOAD_BREAKDOWN_CLASSES = tuple(name for name in LOAD_CLASSES if name != "result")
FIXED_CLASSES = ("overlay", "gossip_anti_entropy", "connection_setup")
CLASS_COLORS = {"conformance": "#0072B2", "gossip_case_end": "#56B4E9", "event": "#D55E00",
                "result": "#CC79A7", "overlay": "#009E73", "gossip_anti_entropy": "#E69F00",
                "connection_setup": "#999999"}
CLASS_LABELS = {"conformance": "constraint coordination", "gossip_case_end": "case end gossip (push + repair)",
                "event": "event transport", "result": "result delivery",
                "overlay": "overlay maintenance", "gossip_anti_entropy": "anti entropy digests",
                "connection_setup": "connection setup"}
# parameters that identify a repetition
REPETITION_PARAMS = {"seed", "replicate", "start", "order_index"}
IGNORED_PARAMS = {"stream_dir", "manifest", "metrics", "violation_sampling", "quiescence_s", "max_wait_for_settlement_s", "idle_window_s", 
                  "msgtrace", "external_central_sink", "cases", "nodes"}

AXIS_LABELS = {
    "constraint_nodes": "nodes",
    "rate": "offered rate (events/s)",
    "repeat": "catalog repeats (model size)",
    "case_gap_secs": "case gap (s), smaller = more concurrency",
    "netem_ms": "added link delay (ms)",
    "cpu_cap_cores": "cpu cap (cores)",
}


def style_axes(axes, title: str, xlabel: str, ylabel: str, logx=False, logy=False) -> None:
    axes.set_title(title, fontsize=10)
    axes.set_xlabel(AXIS_LABELS.get(xlabel, xlabel), fontsize=9)
    axes.set_ylabel(ylabel, fontsize=9)
    axes.grid(True, alpha=0.25, linewidth=0.6)
    axes.tick_params(labelsize=8)
    if logx:
        axes.set_xscale("log")
    if logy:
        axes.set_yscale("log")


# find a value in a nested dict
def find_nested_value(data, path: str):
    current = data
    for part in path.split("."):
        if not isinstance(current, dict) or part not in current:
            return None
        current = current[part]
    return current


def derive_params(params: dict) -> dict:
    params = dict(params)
    params["netem_ms"] = params.get("netem_ms", 0)
    node_count = params.get("nodes")
    if isinstance(node_count, int) and not isinstance(node_count, bool) and node_count > 1:
        params["constraint_nodes"] = node_count - 1
    return params


def _bucket_payload(bucket: dict, payload: str, field: str) -> float:
    entry = (bucket.get("by_payload_type") or {}).get(payload) or {}
    return float(entry.get(field) or 0)


def _bucket_subtype(bucket: dict, key: str, subtype: str, field: str) -> float | None:
    entry = (bucket.get(key) or {}).get(subtype)
    if entry is None:
        return 0.0 if field in ("sent", "received") else None
    if isinstance(entry, dict):
        return float(entry.get(field) or 0)
    return float(entry) if field == "sent" else None


# split every window into the load driven and the fixed traffic classes and normalise each by the denominator that fits it
def traffic_decomposition(metrics: dict) -> dict:
    messages = metrics.get("messages") or {}
    windows = metrics.get("windows") or {}
    throughput = metrics.get("throughput") or {}
    by_role = ((metrics.get("nodes") or {}).get("by_role") or {})
    # the sink is out of scope: it exists to collect verdicts for the evaluation, not as part of the deployment
    nodes = max(1, sum(int(count) for role, count in by_role.items() if role != "sink"))

    events = float(throughput.get("events_sent") or 0)
    cases = float(throughput.get("cases") or 0)
    verdicts = float((metrics.get("revisions") or {}).get("revisions_total") or 0)

    idle_seconds = float(windows.get("idle_seconds") or 0.0)
    load_seconds = float(windows.get("load_seconds") or 0.0)
    settle = [bound for bound in (windows.get("settle") or []) if isinstance(bound, (int, float))]
    settle_seconds = max(0.0, (settle[1] - settle[0]) / 1e6) if len(settle) == 2 else 0.0
    workload_seconds = load_seconds + settle_seconds

    classes: dict[str, dict] = {}
    for window in ("workload", "load", "settle", "idle"):
        bucket = messages.get(window) or {}
        if not bucket:
            continue
        gossip_sent = _bucket_payload(bucket, "ce_gossip", "sent")
        gossip_bytes = _bucket_payload(bucket, "ce_gossip", "bytes_sent")
        digests = _bucket_subtype(bucket, "by_gossip_msg_type", "AeDigest", "sent") or 0.0
        digest_bytes = _bucket_subtype(bucket, "by_gossip_msg_type", "AeDigest", "bytes_sent")
        if digest_bytes is None:
            digests, digest_bytes = 0.0, 0.0
        digest_bytes = min(gossip_bytes, digest_bytes)
        classes[window] = {
            "conformance": {"sent": _bucket_payload(bucket, "conformance", "sent"),
                            "bytes": _bucket_payload(bucket, "conformance", "bytes_sent")},
            "gossip_case_end": {"sent": max(0.0, gossip_sent - digests),
                                "bytes": max(0.0, gossip_bytes - digest_bytes)},
            "event": {"sent": _bucket_payload(bucket, "event", "sent"),
                      "bytes": _bucket_payload(bucket, "event", "bytes_sent")},
            "result": {"sent": _bucket_payload(bucket, "result", "sent"),
                       "bytes": _bucket_payload(bucket, "result", "bytes_sent")},
            "overlay": {"sent": _bucket_payload(bucket, "overlay", "sent"),
                        "bytes": _bucket_payload(bucket, "overlay", "bytes_sent")},
            "gossip_anti_entropy": {"sent": digests, "bytes": digest_bytes},
            "connection_setup": {"sent": _bucket_payload(bucket, "hello", "sent") + _bucket_payload(bucket, "cli", "sent"),
                                 "bytes": _bucket_payload(bucket, "hello", "bytes_sent") + _bucket_payload(bucket, "cli", "bytes_sent")},
        }

    out: dict = {"classes": classes,
                 "gossip_split_recorded": _bucket_subtype(messages.get("workload") or {},
                                                          "by_gossip_msg_type", "AeDigest",
                                                          "bytes_sent") is not None,
                 "nodes_excl_sink": nodes,
                 "idle_seconds": idle_seconds,
                 "workload_seconds": workload_seconds}

    workload = classes.get("workload") or {}
    if workload:
        # load driven traffic, normalised per event and per case
        coordination_sent = workload["conformance"]["sent"] + workload["gossip_case_end"]["sent"]
        coordination_bytes = workload["conformance"]["bytes"] + workload["gossip_case_end"]["bytes"]
        load_sent = coordination_sent + workload["event"]["sent"]
        load_bytes = coordination_bytes + workload["event"]["bytes"]
        for unit_name, denominator in (("per_event", events), ("per_case", cases)):
            if not denominator:
                continue
            out[f"bytes_{unit_name}"] = {name: workload[name]["bytes"] / denominator for name in LOAD_CLASSES}
            out[f"messages_{unit_name}"] = {name: workload[name]["sent"] / denominator for name in LOAD_CLASSES}
            out[f"bytes_{unit_name}"]["coordination"] = coordination_bytes / denominator
            out[f"messages_{unit_name}"]["coordination"] = coordination_sent / denominator
            out[f"bytes_{unit_name}"]["load_driven_excl_sink"] = load_bytes / denominator
            out[f"messages_{unit_name}"]["load_driven_excl_sink"] = load_sent / denominator

    # fixed traffic, normalised per node and second
    fixed: dict = {}
    for window, seconds in (("idle", idle_seconds), ("workload", workload_seconds)):
        per_class = classes.get(window)
        if not per_class or seconds <= 0:
            continue
        scale = 1.0 / (seconds * nodes)
        fixed[window] = {
            "bytes_per_node_second": {name: per_class[name]["bytes"] * scale for name in FIXED_CLASSES},
            "messages_per_node_second": {name: per_class[name]["sent"] * scale for name in FIXED_CLASSES},
        }
        fixed[window]["bytes_per_node_second"]["total"] = sum(per_class[name]["bytes"] for name in FIXED_CLASSES) * scale
        fixed[window]["messages_per_node_second"]["total"] = sum(per_class[name]["sent"] for name in FIXED_CLASSES) * scale
    out["fixed"] = fixed

    # gossip composition: push and repair (ae set) are load driven, the digest probe is not
    workload_bucket = messages.get("workload") or {}
    push = _bucket_subtype(workload_bucket, "by_gossip_msg_type", "Push", "sent") or 0.0
    ae_set = _bucket_subtype(workload_bucket, "by_gossip_msg_type", "AeSet", "sent") or 0.0
    ae_digest = _bucket_subtype(workload_bucket, "by_gossip_msg_type", "AeDigest", "sent") or 0.0
    total_gossip = push + ae_set + ae_digest
    out["gossip"] = {
        "push": push, "ae_set": ae_set, "ae_digest": ae_digest,
        "push_share": (push / total_gossip) if total_gossip else None,
        "repair_share": (ae_set / total_gossip) if total_gossip else None,
        "push_per_case": (push / cases) if cases else None,
        "ae_set_per_case": (ae_set / cases) if cases else None,
        "digests_per_node_second": (ae_digest / (workload_seconds * nodes)) if workload_seconds > 0 else None,
    }
    return out


# parameters and metrics of a (run, system) pair
class Row:

    def __init__(self, metrics: dict, system: str, run_dir: Path):
        self.run_dir = run_dir
        self.experiment = metrics.get("experiment") or run_dir.parent.name
        self.run_id = metrics.get("run_id") or run_dir.name
        self.replicate = metrics.get("replicate", 0)
        self.system = system
        self.params = dict(metrics.get("params") or {})
        self.metrics = metrics["systems"][system]
        self.correctness = (metrics.get("correctness") or {}).get("per_system", {}).get(system, {})
        cap = self.metrics.get("cpu_cap") or {}
        # check if cap is set - consider as swept parameter if so
        if cap.get("mode") and cap.get("mode") != "off":
            self.params.setdefault("cpu_cap_cores", cap.get("cores"))
        self.params = derive_params(self.params)
        self.processes = (self.metrics.get("nodes") or {}).get("by_role") or {}
        self.traffic = traffic_decomposition(self.metrics)

    def value(self, path: str):
        if path.startswith("correctness."):
            return find_nested_value(self.correctness, path.split(".", 1)[1])
        if path.startswith("traffic."):
            return find_nested_value(self.traffic, path.split(".", 1)[1])
        return find_nested_value(self.metrics, path)

    # a run with a workload sent nothing if the pacer never ran
    @property
    def has_workload(self) -> bool:
        return bool(self.value("throughput.events_sent"))


def load_rows(results: Path, pattern: str | None, progress: Progress | None) -> tuple[list[Row], dict]:
    rows: list[Row] = [] # per (run, system) (general) data
    privacy: dict[tuple, list] = {} # per (run, system) privacy data
    run_dirs = discover_runs(results, pattern, "metrics.json")
    for run_dir in run_dirs:
        if progress is not None:
            progress.note(run_dir.name)
        try:
            metrics = json.loads((run_dir / "metrics.json").read_text())
        except (OSError, json.JSONDecodeError) as error:
            print(f"skip {run_dir}: {error}", file=sys.stderr)
            continue
        for system in metrics.get("systems", {}):
            rows.append(Row(metrics, system, run_dir))

        privacy_path = run_dir / "privacy.json"
        if privacy_path.exists():
            try:
                privacy_data = json.loads(privacy_path.read_text())
            except (OSError, json.JSONDecodeError):
                continue
            for system, block in privacy_data.get("systems", {}).items():
                key = (metrics.get("experiment"), metrics.get("run_id"), system)
                privacy[key] = {"params": derive_params(metrics.get("params") or {}),
                                "local": block.get("local_attacker", {}),
                                "global": block.get("global_attacker", {}),
                                "ground_truth": privacy_data.get("ground_truth", {})}
        if progress is not None:
            progress.step(run_dir.name)
    return rows, privacy


# the parameters that actually vary inside an experiment (most distinct values first)
def find_varying_params(rows: list[Row]) -> list[str]:
    values: dict[str, set] = defaultdict(set)
    for row in rows:
        for key, value in row.params.items():
            if key in REPETITION_PARAMS or key in IGNORED_PARAMS:
                continue
            if isinstance(value, (list, dict)):
                continue
            values[key].add(value)
    varying = [(key, len(distinct)) for key, distinct in values.items() if len(distinct) > 1]
    varying.sort(key=lambda item: (-item[1], item[0]))
    return [key for key, _count in varying]


# check if a run parameter is numberic
def is_numeric(rows: list[Row], key: str) -> bool:
    for row in rows:
        value = row.params.get(key)
        if value is not None and not isinstance(value, (int, float)):
            return False
    return True


# get the base config for a sensitivity run
def base_configuration(rows: list[Row], varied: list[str]) -> dict:
    base = {}
    for key in varied:
        counts: dict = defaultdict(int)
        for row in rows:
            counts[row.params.get(key)] += 1
        base[key] = max(counts.items(), key=lambda item: item[1])[0]
    return base


def is_sensitivity(rows: list[Row], varied: list[str]) -> bool:
    if len(varied) < 3:
        return False
    base = base_configuration(rows, varied)
    for row in rows:
        differing = sum(1 for key in varied if row.params.get(key) != base[key])
        if differing > 1:
            return False
    return True


# restricts each panel to its own spoke (each sensitivity parameter is a spoke from the base config)
def ofat_rows(rows: list[Row], varied: list[str], parameter: str) -> list[Row]:
    base = base_configuration(rows, varied)
    selected = []
    for row in rows:
        differing = [key for key in varied if row.params.get(key) != base[key]]
        if not differing or differing == [parameter]:
            selected.append(row)
    return selected


# median and inter quartile range (IRQ) over seeds and replicates
def summarize(values: list[float]) -> tuple[float, float, float]:
    ordered = sorted(value for value in values if value is not None)
    if not ordered:
        return (math.nan, math.nan, math.nan)
    middle = len(ordered) // 2
    median = ordered[middle] if len(ordered) % 2 else (ordered[middle - 1] + ordered[middle]) / 2
    if len(ordered) < 3:
        return (median, ordered[0], ordered[-1])
    lower = ordered[max(0, round(0.25 * (len(ordered) - 1)))]
    upper = ordered[min(len(ordered) - 1, round(0.75 * (len(ordered) - 1)))]
    return (median, lower, upper)


# group rows into series -> sorted [(x, median, IRQ low, IRQ high)]
def curve(rows: list[Row], x_key: str, metric: str, series_key: str | None, scale: float = 1.0):
    buckets: dict[tuple, list[float]] = defaultdict(list)
    for row in rows:
        x = row.params.get(x_key)
        y = row.value(metric)
        if x is None or y is None or not isinstance(y, (int, float)):
            continue
        series = (row.system, row.params.get(series_key) if series_key else None)
        buckets[(series, x)].append(y * scale)

    by_series: dict[tuple, list] = defaultdict(list)
    for (series, x), values in buckets.items():
        median, irq_low, irq_high = summarize(values)
        by_series[series].append((x, median, irq_low, irq_high, len(values)))
    for series in by_series:
        by_series[series].sort(key=lambda point: point[0])
    return by_series


def draw_curve(axes, by_series: dict, series_key: str | None, marker_every=True) -> None:
    linestyle_of: dict = {}
    for (system, series_value), points in sorted(by_series.items(), key=lambda item: str(item[0])):
        if not points:
            continue
        style = SYSTEM_STYLE.get(system, {"color": "#444444", "marker": "^", "label": system})
        if series_value not in linestyle_of:
            linestyle_of[series_value] = SERIES_LINESTYLES[len(linestyle_of) % len(SERIES_LINESTYLES)]
        label = style["label"]
        if series_key and series_value is not None:
            label += f" ({series_key}={series_value})"
        xs = [point[0] for point in points]
        median = [point[1] for point in points]
        irq_low = [point[2] for point in points]
        irq_high = [point[3] for point in points]
        axes.plot(xs, median, color=style["color"], marker=style["marker"] if marker_every else None,
                  markersize=4, linewidth=1.6, linestyle=linestyle_of[series_value], label=label)
        if any(hi != med for hi, med in zip(irq_high, median)):
            axes.fill_between(xs, irq_low, irq_high, color=style["color"], alpha=0.15, linewidth=0)


# panel definitions
# (metric path, panel title, y label, scale)
THROUGHPUT_PANELS = [
    ("throughput.effective_rate", "achieved rate", "events/s", 1.0, False),
    ("throughput.pacer_max_lag_ms", "pacer lag (max)", "ms", 1.0, False),
    ("throughput.queues.event_q.p95", "event queue depth p95", "events", 1.0, False),
    ("throughput.queues.event_q.max", "event queue peak depth", "events", 1.0, False),
    ("throughput.message_drops.total", "message drops", "messages", 1.0, False),
    ("throughput.closed_case_drops", "dropped after the case closed", "events + messages", 1.0, False),
]
LATENCY_INTERVALS = [
    ("routing_t1_t2", "routing delay (t1 to t2)"),
    ("checking_t2_t3", "checking delay (t2 to t3)"),
    ("detection_t1_t3", "detection latency (t1 to t3)"),
    ("provisional_t3_t4", "provisional window (t3 to t4)"),
    ("end_to_end_t1_t4", "event to final verdict (t1 to t4)"),
    ("case_end_verdict", "case end verdict latency"),
    ("gossip_dissemination", "case end dissemination"),
]
LATENCY_PERCENTILES = ("p50", "p95", "p99")

# overview figure: every interval at p95
LATENCY_PANELS = [(f"latency_us.{key}.p95", f"{label} p95", "ms", 1e-3, False)
                  for key, label in LATENCY_INTERVALS]

RESOURCE_PANELS = [
    ("resources.cpu_seconds_per_event", "cpu per event", "cpu-ms/event", 1e3, False),
    ("resources.cpu_seconds_excl_sink", "cpu total", "cpu-s", 1.0, False),
    ("resources.cpu_percent_max_node", "cpu of the busiest node", "% of one core", 1.0, False),
    ("resources.busiest_thread.cpu_percent", "cpu of the busiest thread", "% of one core", 1.0, False),
    ("resources.rss_kb_total_max", "resident memory (all nodes)", "MB", 1 / 1024, False),
]
GOSSIP_PANELS = [
    ("throughput.queues.gossip_in_q.p95", "gossip inbound queue p95", "messages", 1.0, False),
    ("throughput.queues.gossip_in_q.max", "gossip inbound queue peak", "messages", 1.0, False),
    ("throughput.queues.gossip_out_q.p95", "gossip outbound queue p95", "messages", 1.0, False),
    ("throughput.queues.gossip_out_q.max", "gossip outbound queue peak", "messages", 1.0, False),
    ("latency_us.gossip_dissemination.p95", "case end dissemination p95", "ms", 1e-3, False),
    ("latency_us.gossip_dissemination.reach_fraction", "constraint nodes reached", "fraction", 1.0, False),
    ("traffic.gossip.push_per_case", "push gossip messages per case", "messages", 1.0, False),
    ("traffic.gossip.ae_set_per_case", "anti entropy repairs per case", "messages", 1.0, False),
    ("traffic.gossip.push_share", "push share of gossip messages", "fraction", 1.0, False),
    ("traffic.gossip.digests_per_node_second", "anti entropy digests per node", "messages/s per node", 1.0, False),
]
# load driven traffic only to a per event denominator
MESSAGE_PANELS = [
    ("traffic.bytes_per_event.coordination", "coordination bytes per event", "bytes", 1.0, False),
    ("traffic.messages_per_event.coordination", "coordination messages per event", "messages", 1.0, False),
    ("traffic.messages_per_case.coordination", "coordination messages per case", "messages", 1.0, False),
    ("traffic.bytes_per_event.load_driven_excl_sink", "load driven bytes per event", "bytes", 1.0, False),
    ("traffic.bytes_per_event.event", "event transport bytes per event", "bytes", 1.0, False),
    ("traffic.messages_per_case.gossip_case_end", "case end gossip messages per case", "messages", 1.0, False),
]
CORRECTNESS_PANELS = [
    ("correctness.wrong", "wrong verdicts", "pairs", 1.0, False),
    ("correctness.absent", "missing verdicts", "pairs", 1.0, False),
    ("revisions.pairs_with_multiple", "pairs with more than one revision", "pairs", 1.0, False),
    ("revisions.revisions_total", "revisions in total", "revisions", 1.0, False),
    ("revisions.final_after_send_end", "verdicts finalised after the stream ended", "pairs", 1.0, False),
]
# the fixed cost of holding a deployment open
IDLE_PANELS = [
    ("traffic.fixed.idle.bytes_per_node_second.total", "fixed traffic per node (idle)", "bytes/s per node", 1.0, False),
    ("traffic.fixed.idle.bytes_per_node_second.overlay", "overlay maintenance per node", "bytes/s per node", 1.0, False),
    ("traffic.fixed.idle.bytes_per_node_second.gossip_anti_entropy", "anti entropy digests per node", "bytes/s per node", 1.0, False),
    ("traffic.fixed.workload.bytes_per_node_second.total", "fixed traffic per node (during load)", "bytes/s per node", 1.0, False),
    ("resources.idle.cpu_seconds_per_node_second", "idle cpu per node", "cpu-s per node-second", 1.0, False),
    ("resources.idle.rss_kb_total", "idle resident memory (all nodes)", "MB", 1 / 1024, False),
]
PANEL_GROUPS = [
    ("throughput", THROUGHPUT_PANELS),
    ("gossip", GOSSIP_PANELS),
    ("latency", LATENCY_PANELS),
    ("resources", RESOURCE_PANELS),
    ("messages", MESSAGE_PANELS),
    ("correctness_cost", CORRECTNESS_PANELS),
]


# all three percentiles drawn into its own figure
def latency_panels(key: str, label: str) -> list[tuple]:
    return [(f"latency_us.{key}.{point}", f"{label} {point}", "ms", 1e-3, False)
            for point in LATENCY_PERCENTILES]


# what each deployment actually builds from one value of `constraint_nodes` (those who receive the events)
def write_process_table(rows: list[Row], x_key: str, out_path: Path) -> Path | None:
    if x_key != "constraint_nodes":
        return None
    table: dict[tuple, dict] = {}
    for row in rows:
        x = row.params.get(x_key)
        if x is None or not row.processes:
            continue
        entry = table.setdefault((x, row.system), {})
        for role, count in row.processes.items():
            entry[role] = max(entry.get(role, 0), count)
    if not table:
        return None
    roles = sorted({role for entry in table.values() for role in entry if role != "sink"})
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow([x_key, "system"] + roles + ["processes_total"])
        for (x, system), entry in sorted(table.items()):
            counts = [entry.get(role, 0) for role in roles]
            writer.writerow([x, system] + counts + [sum(counts)])
    return out_path


# the placement boundary: at one role per node, adding nodes stops buying anything and only adds gossip and overlay cost
def mark_placement_boundary(axes, rows: list[Row], x_key: str) -> None:
    if x_key != "constraint_nodes":
        return
    boundary = max((row.params.get("constraint_nodes") for row in rows
                    if row.params.get("constraint_nodes") is not None), default=None)
    catalog_roles = max((row.value("nodes.by_role.constraint_node") or 0 for row in rows), default=0)
    if not catalog_roles or boundary is None or catalog_roles >= boundary:
        return
    axes.axvline(catalog_roles, color="#666666", linestyle=":", linewidth=1.0, zorder=0)
    axes.annotate("one role per node", xy=(catalog_roles, axes.get_ylim()[1]),
                  xytext=(2, -8), textcoords="offset points", fontsize=7, color="#666666",
                  va="top", rotation=90)


REVISION_PANELS = [
    ("revisions.pairs_with_multiple", "pairs needing more than one revision", "pairs", 1.0, False),
    ("revisions.correction_drops.total", "corrections that could not be applied", "corrections", 1.0, False),
    ("latency_us.provisional_t3_t4.p95", "provisional window p95 (t3 to t4)", "ms", 1e-3, False),
    ("latency_us.checking_t2_t3.p95", "checking delay p95 (t2 to t3)", "ms", 1e-3, False),
    ("traffic.messages_per_case.result", "result messages per case", "messages", 1.0, False),
]


MEMORY_PANELS = [
    ("resources.rss_kb_max_node", "resident memory, busiest node", "MB", 1 / 1024, False),
    ("resources.rss_kb_mean_node", "resident memory, mean node", "MB", 1 / 1024, False),
    ("resources.rss_kb_total_max", "resident memory, all nodes", "MB", 1 / 1024, False),
    ("throughput.queues.event_q.max", "event queue peak depth", "events", 1.0, False),
    ("revisions.pairs", "pairs the run produced", "pairs", 1.0, False),
    ("quality.expected_pairs", "pairs expected", "pairs", 1.0, False),
]


RSS_TIMELINE_SERIES = [
    ("rss_kb_total", "all nodes", "memory_timeline"),
    ("rss_kb_max_node", "busiest node", "memory_timeline_max_node"),
]
RSS_TIMELINE_MAX_PANELS = 6


# seconds since the start of the load window for one run
def rss_series(row, metric: str = "rss_kb_total") -> tuple[list[float], list[float]] | None:
    timeline = row.value("resources.rss_timeline")
    if not timeline or not timeline.get(metric):
        return None
    values = timeline[metric]
    bin_seconds = timeline.get("bin_seconds") or 1.0
    first_us = timeline.get("first_bin_us") or 0
    window = (row.value("windows.load") or [None, None]) + [None, None]
    anchor_us = window[0] or first_us
    start = (first_us - anchor_us) / 1e6
    times = [start + index * bin_seconds for index in range(len(values))]
    return times, [value / 1024.0 for value in values]


def memory_timeline_figure(rows, x_key, out_path, formats, metric="rss_kb_total",
                           scope="all nodes", max_panels=RSS_TIMELINE_MAX_PANELS):
    by_value: dict = defaultdict(dict)
    for row in sorted(rows, key=lambda item: (item.replicate, item.run_id)):
        value = row.params.get(x_key) if x_key else None
        by_value[value].setdefault(row.system, row) # first replicate stands for the configuration
    values = sorted(by_value, key=lambda item: (item is None, item))
    if not values:
        return None
    if len(values) > max_panels:
        step = (len(values) - 1) / (max_panels - 1)
        values = [values[round(index * step)] for index in range(max_panels)]

    columns = min(3, len(values))
    figure_rows = math.ceil(len(values) / columns)
    figure, axes_grid = plt.subplots(figure_rows, columns, figsize=(4.6 * columns, 3.4 * figure_rows),
                                     squeeze=False)
    drawn = False
    for index, value in enumerate(values):
        axes = axes_grid[index // columns][index % columns]
        for system, row in sorted(by_value[value].items()):
            series = rss_series(row, metric)
            if series is None:
                continue
            times, totals = series
            style = SYSTEM_STYLE.get(system, {})
            axes.plot(times, totals, color=style.get("color"),
                      label=style.get("label", system), linewidth=1.4)
            window = (row.value("windows.load") or [None, None]) + [None, None]
            if window[0] and window[1]:
                axes.axvline((window[1] - window[0]) / 1e6, color=style.get("color"),
                             linestyle=":", linewidth=1.0)
            drawn = True
        axes.axvline(0.0, color="#999999", linestyle="--", linewidth=0.8)
        title = "resident memory" if value is None else f"{x_key} = {value}"
        style_axes(axes, title, "seconds since the load window started", f"MB, {scope}")
    if not drawn:
        plt.close(figure)
        return None
    for index in range(len(values), figure_rows * columns):
        axes_grid[index // columns][index % columns].axis("off")
    figure.suptitle(f"resident memory over the run, {scope} "
                    f"(dotted = end of the load window)")
    handles, labels = figure_handles(figure)
    place_legend(figure, handles, labels)
    return write_figure(figure, out_path, formats)


def panel_figure(rows, x_key, panels, series_key, title, out_path, formats, logx=False):
    usable_panels = [panel for panel in panels if any(row.value(panel[0]) is not None for row in rows)]
    if not usable_panels:
        return None
    columns = min(3, len(usable_panels))
    figure_rows = math.ceil(len(usable_panels) / columns)
    figure, axes_grid = plt.subplots(figure_rows, columns, figsize=(4.6 * columns, 3.4 * figure_rows), squeeze=False)
    for index, (metric, panel_title, ylabel, scale, logy) in enumerate(usable_panels):
        axes = axes_grid[index // columns][index % columns]
        draw_curve(axes, curve(rows, x_key, metric, series_key, scale), series_key)
        style_axes(axes, panel_title, x_key, ylabel, logx=logx, logy=logy)
        mark_placement_boundary(axes, rows, x_key)
    for index in range(len(usable_panels), figure_rows * columns):
        axes_grid[index // columns][index % columns].axis("off")

    handles, labels = figure_handles(figure)
    place_legend(figure, handles, labels)
    figure.suptitle(title, fontsize=12)
    return write_figure(figure, out_path, formats)


def figure_handles(figure) -> tuple[list, list]:
    seen: dict[str, object] = {}
    for axes in figure.axes:
        for handle, label in zip(*axes.get_legend_handles_labels()):
            seen.setdefault(label, handle)
    labels = sorted(seen, key=lambda label: (label.split(" (")[0], _label_sort_key(label)))
    return [seen[label] for label in labels], labels


def _label_sort_key(label: str):
    digits = "".join(character for character in label.split("=")[-1] if character.isdigit())
    return (int(digits) if digits else 0, label)


# one legend placement for every figure that needs one
def place_legend(figure, handles, labels, max_rows: int = 2, fontsize: int = 9) -> None:
    if not labels:
        figure.tight_layout(rect=(0, 0.02, 1, 0.94))
        return
    columns = max(1, min(6, -(-len(labels) // max_rows)))
    rows = -(-len(labels) // columns)
    height = figure.get_size_inches()[1] or 4.0
    reserved = min(0.42, (0.30 + 0.19 * rows) / height)
    figure.tight_layout(rect=(0, reserved, 1, 0.94))
    figure.legend(handles, labels, loc="lower center", ncol=columns,
                  fontsize=fontsize if len(labels) <= 8 else max(6, fontsize - 2),
                  frameon=False, bbox_to_anchor=(0.5, 0.0))


def write_figure(figure, out_path: Path, formats) -> Path:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    written = None
    for suffix in formats:
        target = out_path.with_suffix(f".{suffix}")
        figure.savefig(target, dpi=150, bbox_inches="tight")
        written = written or target
    plt.close(figure)
    return written


# stacked bars of one traffic class set
def stacked_breakdown_figure(rows, x_key, panels, suptitle, out_path, formats, note: str | None = None,
                             share_y: list[tuple[int, ...]] | None = None):
    collected: dict[tuple, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    for row in rows:
        for panel_index, (_title, _ylabel, prefix, classes) in enumerate(panels):
            for name in classes:
                value = row.value(f"{prefix}.{name}")
                if isinstance(value, (int, float)) and not isinstance(value, bool):
                    collected[(panel_index, row.system, row.params.get(x_key))][name].append(float(value))
    if not collected:
        return None

    usable = [index for index in range(len(panels))
              if any(key[0] == index and any(values for values in per_class.values())
                     for key, per_class in collected.items())]
    if not usable:
        return None

    systems = sorted({system for _index, system, _x in collected})
    x_values = sorted({x for _index, _system, x in collected if x is not None},
                      key=lambda value: (value is None, value))
    if not x_values:
        x_values = [None]

    columns = min(3, len(usable))
    figure_rows = math.ceil(len(usable) / columns)
    width_per_panel = min(5.5, 2.0 + 0.75 * max(1, len(x_values)) * max(1, len(systems)))
    figure, axes_grid = plt.subplots(figure_rows, columns,
                                     figsize=(width_per_panel * columns, 3.8 * figure_rows),
                                     squeeze=False)
    bar_width = 0.8 / max(1, len(systems))
    axes_of_panel: dict[int, object] = {}
    for slot, panel_index in enumerate(usable):
        title, ylabel, _prefix, classes = panels[panel_index]
        axes = axes_grid[slot // columns][slot % columns]
        axes_of_panel[panel_index] = axes
        for system_index, system in enumerate(systems):
            bottoms = [0.0] * len(x_values)
            positions = [index + system_index * bar_width - 0.4 + bar_width / 2
                         for index in range(len(x_values))]
            for name in classes:
                heights = []
                for x in x_values:
                    values = collected[(panel_index, system, x)].get(name, [])
                    height = summarize(values)[0] if values else 0.0
                    heights.append(0.0 if math.isnan(height) else height)
                if not any(heights):
                    continue
                axes.bar(positions, heights, width=bar_width * 0.92, bottom=bottoms,
                         color=CLASS_COLORS.get(name, "#888888"), edgecolor="white", linewidth=0.4,
                         label=CLASS_LABELS.get(name, name) if slot == 0 else None)
                bottoms = [bottom + height for bottom, height in zip(bottoms, heights)]
            for position, total in zip(positions, bottoms):
                if total:
                    axes.text(position, total, system[:3], ha="center", va="bottom",
                              fontsize=7, color="#333333")
        axes.set_xticks(range(len(x_values)))
        axes.set_xticklabels(["" if x is None else str(x) for x in x_values])
        style_axes(axes, title, x_key, ylabel)
    for slot in range(len(usable), figure_rows * columns):
        axes_grid[slot // columns][slot % columns].axis("off")

    # panels that are meant to be read against each other need the same scale
    for group in (share_y or []):
        drawn = [axes_of_panel[index] for index in group if index in axes_of_panel]
        if len(drawn) > 1:
            top = max(axes.get_ylim()[1] for axes in drawn)
            for axes in drawn:
                axes.set_ylim(0, top)

    handles, labels = figure_handles(figure)
    place_legend(figure, handles, labels)
    figure.suptitle(suptitle + "  (bar label: system)", fontsize=12)
    if note:
        figure.text(0.5, 0.005, note, ha="center", fontsize=7, color="#666666")
    return write_figure(figure, out_path, formats)


# traffic that scales with the workload, normalised per event and per case
def load_traffic_breakdown_figure(rows, x_key, out_path, formats):
    panels = [("bytes per event", "bytes/event", "traffic.bytes_per_event", LOAD_BREAKDOWN_CLASSES),
              ("messages per event", "messages/event", "traffic.messages_per_event", LOAD_BREAKDOWN_CLASSES),
              ("messages per case", "messages/case", "traffic.messages_per_case", LOAD_BREAKDOWN_CLASSES)]
    split = all(row.value("traffic.gossip_split_recorded") for row in rows)
    note = ("case end gossip is push and repair only; the periodic anti entropy digests are in the "
            "fixed cost figure" if split else
            "these runs predate per subtype byte recording, so the periodic anti entropy digests "
            "are counted inside case end gossip")
    return stacked_breakdown_figure(rows, x_key, panels, "load driven traffic by class",
                                    out_path, formats, note)


# fixed traffic that is paid per node and second
def fixed_cost_breakdown_figure(rows, x_key, out_path, formats):
    panels = [("bytes per node-second, idle window", "bytes/s per node",
               "traffic.fixed.idle.bytes_per_node_second", FIXED_CLASSES),
              ("bytes per node-second, during load and settle", "bytes/s per node",
               "traffic.fixed.workload.bytes_per_node_second", FIXED_CLASSES),
              ("messages per node-second, idle window", "messages/s per node",
               "traffic.fixed.idle.messages_per_node_second", FIXED_CLASSES)]
    note = ("the two byte panels share a scale: a class that is truly workload independent "
            "has the same height in both")
    return stacked_breakdown_figure(rows, x_key, panels, "fixed cost of the deployment by class",
                                    out_path, formats, note, share_y=[(0, 1)])


PRIVACY_PANELS = [
    ("case_exposure_named", "case exposure (named activities)", "fraction of a case"),
    ("case_exposure_with_anonymous", "case exposure incl. anonymous occurrences", "fraction of a case"),
    ("total_reconstruction_probability", "probability of reconstructing every case", "probability"),
    ("full_case_fraction", "cases fully named", "fraction of cases"),
    ("model_disclosure", "process model disclosed", "fraction of model atoms"),
    ("model_disclosure_binding", "bindings disclosed", "fraction of binding atoms"),
    ("deployment_disclosure", "deployment disclosed", "fraction of deployment atoms"),
    ("first_spy_precision", "first spy precision", "precision"),
    ("first_spy_recall", "first spy recall", "recall"),
    ("case_disclosure_set", "case disclosure (set knowledge)", "cd"),
    ("case_disclosure_mult", "case disclosure (multiset knowledge)", "cd"),
    ("case_disclosure_seq", "case disclosure (sequence knowledge)", "cd"),
    ("trace_disclosure_set", "trace disclosure (set knowledge)", "td"),
    ("trace_disclosure_mult", "trace disclosure (multiset knowledge)", "td"),
    ("trace_disclosure_seq", "trace disclosure (sequence knowledge)", "td"),
]


# collect privacy curve data
def privacy_curves(privacy: dict, experiment: str, x_key: str | None):

    collected: dict = defaultdict(lambda: defaultdict(lambda: defaultdict(list))) # (system, x) -> (compromised nodes -> (metric -> [values]))
    pools: dict = {} # (system, x) -> pool size
    for (exp, _run_id, system), block in privacy.items():
        if exp != experiment:
            continue
        x_param = block["params"].get(x_key) if x_key else None
        local = block["local"]
        pools[(system, x_param)] = local.get("pool_size")
        for entry in local.get("per_size", []):
            compromised = entry.get("compromised")
            for metric, value in entry.items():
                if isinstance(value, dict) and value.get("n"):
                    collected[(system, x_param)][compromised][metric].append(value["mean"])
    return collected, pools


def privacy_figure(privacy, experiment, x_key, out_path, formats, by_fraction=False):
    collected, pools = privacy_curves(privacy, experiment, x_key)
    if not collected:
        return None
    usable_panels = [panel for panel in PRIVACY_PANELS if any(panel[0] in per_size for per_x in collected.values() for per_size in per_x.values())]
    if not usable_panels:
        return None

    columns = min(3, len(usable_panels))
    figure_rows = math.ceil(len(usable_panels) / columns)
    figure, axes_grid = plt.subplots(figure_rows, columns, figsize=(4.6 * columns, 3.4 * figure_rows), squeeze=False)
    linestyle_of: dict = {}
    for index, (metric, title, ylabel) in enumerate(usable_panels):
        axes = axes_grid[index // columns][index % columns]
        for (system, x), per_size in sorted(collected.items(), key=lambda item: str(item[0])):
            points = []
            for compromised, metrics in sorted(per_size.items()):
                if metric not in metrics:
                    continue
                median, low, high = summarize(metrics[metric])
                pool = pools.get((system, x)) or 1
                position = compromised / pool if by_fraction else compromised
                points.append((position, median, low, high))
            if not points:
                continue
            style = SYSTEM_STYLE.get(system, {"color": "#444444", "marker": "^", "label": system})
            if x not in linestyle_of:
                linestyle_of[x] = SERIES_LINESTYLES[len(linestyle_of) % len(SERIES_LINESTYLES)]
            label = style["label"] + (f" ({x_key}={x})" if x_key and x is not None else "")
            axes.plot([p[0] for p in points], [p[1] for p in points], color=style["color"],
                      marker=style["marker"], markersize=4, linewidth=1.6,
                      linestyle=linestyle_of[x], label=label)
            axes.fill_between([p[0] for p in points], [p[2] for p in points], [p[3] for p in points],
                              color=style["color"], alpha=0.15, linewidth=0)
        style_axes(axes, title, "compromised fraction of the pool" if by_fraction else "compromised nodes", ylabel)
        axes.set_ylim(-0.02, 1.05)
    for index in range(len(usable_panels), figure_rows * columns):
        axes_grid[index // columns][index % columns].axis("off")

    handles, labels = figure_handles(figure)
    place_legend(figure, handles, labels)
    figure.suptitle(f"{experiment}: local attacker" + (" (pool fraction)" if by_fraction else ""), fontsize=12)
    return write_figure(figure, out_path, formats)


ECDF_METRICS = [
    ("model_disclosure", "model disclosure"),
    ("deployment_disclosure", "deployment disclosure"),
    ("case_exposure_named", "case exposure (named activities)"),
    ("case_exposure_with_anonymous", "case exposure incl. pseudonymous"),
    ("first_spy_precision", "first spy precision"),
    ("case_disclosure_set", "case disclosure (set knowledge)"),
]
ECDF_PANELS = 4
# only used for the attacker pools that the systems dont share
ECDF_POOL_FRACTIONS = (0.1, 0.25, 0.5, 0.75)


# (value, count) pairs -> the staircase of the empirical distribution function
def ecdf_points(counts: dict) -> tuple[list[float], list[float]] | None:
    if not counts:
        return None
    total = sum(counts.values())
    if not total:
        return None
    xs: list[float] = []
    ys: list[float] = []
    seen = 0
    for value in sorted(counts):
        seen += counts[value]
        xs += [value, value]
        ys += [seen / total - counts[value] / total, seen / total]
    return xs, ys


# distributions per (system, compromise size), keeping the run identity so every replicate can be drawn as its own line
def privacy_distributions(privacy: dict, experiment: str, metric: str):
    per_run: dict = defaultdict(dict) # (system, size) -> run_id -> {value: count}
    pools: dict = {} # system -> pool size
    for (exp, run_id, system), block in privacy.items():
        if exp != experiment:
            continue
        local = block["local"]
        pools[system] = local.get("pool_size")
        for entry in local.get("per_size", []):
            distribution = (entry.get("distributions") or {}).get(metric)
            if not distribution or distribution.get("kind") != "counts":
                continue
            counts = dict(zip(distribution["values"], distribution["counts"]))
            per_run[(system, entry.get("compromised"))][run_id] = counts
    return per_run, pools


def ecdf_figure(privacy, experiment, metric, label, out_path, formats):
    per_run, pools = privacy_distributions(privacy, experiment, metric)
    if not per_run:
        return None

    sizes_by_system: dict = defaultdict(set)
    for (system, size) in per_run:
        if size:
            sizes_by_system[system].add(size)
    if not sizes_by_system:
        return None

    # the same compromise size in both deployments is the honest comparison
    shared = sorted(set.intersection(*sizes_by_system.values())) if len(sizes_by_system) > 1 \
        else sorted(next(iter(sizes_by_system.values())))
    panels: list[tuple[str, dict]] = []
    if shared:
        picks = [shared[round(index * (len(shared) - 1) / (ECDF_PANELS - 1))]
                 for index in range(min(ECDF_PANELS, len(shared)))]
        for size in sorted(set(picks)):
            panels.append((f"{size} of the nodes compromised",
                           {system: size for system in sizes_by_system}))
    else:
        # no size was evaluated in both, so fall back to comparable shares of the pool
        for target in ECDF_POOL_FRACTIONS:
            chosen = {system: min(sizes, key=lambda size: abs(size / (pools.get(system) or 1) - target))
                      for system, sizes in sizes_by_system.items() if pools.get(system)}
            if chosen:
                panels.append((f"{round(target * 100)}% of the pool", chosen))
    if not panels:
        return None

    columns = min(len(panels), 4)
    figure_rows = math.ceil(len(panels) / columns)
    figure, axes_grid = plt.subplots(figure_rows, columns, figsize=(4.4 * columns, 3.4 * figure_rows),
                                     squeeze=False)
    drawn = False
    for index, (title, chosen) in enumerate(panels):
        axes = axes_grid[index // columns][index % columns]
        annotations = []
        for system, size in sorted(chosen.items()):
            runs = per_run.get((system, size)) or {}
            if not runs:
                continue
            style = SYSTEM_STYLE.get(system, {"color": "#444444", "label": system})
            for counts in runs.values(): # one thin line per replicate
                points = ecdf_points(counts)
                if points:
                    axes.plot(points[0], points[1], color=style["color"], linewidth=0.7, alpha=0.25)
            pooled: dict = defaultdict(int)
            for counts in runs.values():
                for value, count in counts.items():
                    pooled[value] += count
            points = ecdf_points(pooled)
            if points:
                axes.plot(points[0], points[1], color=style["color"], linewidth=2.0,
                          label=style["label"])
                drawn = True
            pool = pools.get(system)
            share = f", {round(100 * size / pool)}% of {pool}" if pool else ""
            annotations.append(f"{style['label']}: k={size}{share}, {len(runs)} runs")
        style_axes(axes, title, label, "fraction of draws at or below")
        if annotations:
            axes.text(0.97, 0.05, "\n".join(annotations), transform=axes.transAxes,
                      ha="right", va="bottom", fontsize=7, color="#444444")
        axes.set_xlim(-0.02, 1.02)
        axes.set_ylim(-0.02, 1.02)
    if not drawn:
        plt.close(figure)
        return None
    for index in range(len(panels), figure_rows * columns):
        axes_grid[index // columns][index % columns].axis("off")
    handles, labels = figure_handles(figure)
    place_legend(figure, handles, labels)
    figure.suptitle(f"{experiment}: distribution of {label} over the compromised sets "
                    f"(thin = one replicate, thick = pooled)", fontsize=12)
    return write_figure(figure, out_path, formats)


# best and worst subset of each size, as a bracket around the uniform average
def privacy_extremes_figure(privacy, experiment, x_key, out_path, formats):
    collected: dict = defaultdict(lambda: defaultdict(dict)) # system -> size -> metric -> (min, max)
    pools: dict = {}
    for (exp, _run_id, system), block in privacy.items():
        if exp != experiment:
            continue
        local = block["local"]
        pools[system] = local.get("pool_size")
        for entry in local.get("per_size", []):
            size = entry.get("compromised")
            for metric, value in entry.items():
                if not (isinstance(value, dict) and value.get("n")):
                    continue
                low, high = collected[system][size].get(metric, (value["min"], value["max"]))
                collected[system][size][metric] = (min(low, value["min"]), max(high, value["max"]))
    if not collected:
        return None

    usable = [panel for panel in PRIVACY_PANELS
              if any(panel[0] in metrics for sizes in collected.values() for metrics in sizes.values())]
    if not usable:
        return None
    columns = min(3, len(usable))
    figure_rows = math.ceil(len(usable) / columns)
    figure, axes_grid = plt.subplots(figure_rows, columns, figsize=(4.6 * columns, 3.4 * figure_rows),
                                     squeeze=False)
    for index, (metric, title, ylabel) in enumerate(usable):
        axes = axes_grid[index // columns][index % columns]
        for system, sizes in sorted(collected.items()):
            points = [(size, sizes[size][metric]) for size in sorted(sizes) if metric in sizes[size]]
            if not points:
                continue
            style = SYSTEM_STYLE.get(system, {"color": "#444444", "marker": "^", "label": system})
            positions = [point[0] for point in points]
            axes.plot(positions, [point[1][1] for point in points], color=style["color"],
                      marker=style["marker"], markersize=3, linewidth=1.5,
                      label=f"{style['label']} best subset")
            axes.plot(positions, [point[1][0] for point in points], color=style["color"],
                      marker=style["marker"], markersize=3, linewidth=1.2, linestyle="--",
                      label=f"{style['label']} worst subset")
            axes.fill_between(positions, [point[1][0] for point in points],
                              [point[1][1] for point in points], color=style["color"],
                              alpha=0.10, linewidth=0)
        style_axes(axes, title, "compromised nodes", ylabel)
        axes.set_ylim(-0.02, 1.05)
    for index in range(len(usable), figure_rows * columns):
        axes_grid[index // columns][index % columns].axis("off")
    handles, labels = figure_handles(figure)
    place_legend(figure, handles, labels)
    figure.suptitle(f"{experiment}: best and worst compromised set of each size", fontsize=12)
    return write_figure(figure, out_path, formats)


# per atom kind facts
FACT_KIND_PANELS = [
    ("model_disclosure_activity", "activity(a)", "model"),
    ("model_disclosure_constraint", "constraint(c)", "model"),
    ("model_disclosure_constraint_type", "constraint_type(c, T)", "model"),
    ("model_disclosure_binding", "binding(c, r, a)", "model"),
    ("model_disclosure_param", "param(c, k, v)", "model"),
    ("model_disclosure_balanced", "balanced mean over kinds", "model"),
    ("deployment_disclosure_place", "place(c, r, n)", "deployment"),
    ("deployment_disclosure_counterpart", "counterpart(c, r, n)", "deployment"),
    ("deployment_disclosure_watch", "watch(a, n)", "deployment"),
]


def fact_breakdown_figure(privacy, experiment, x_key, out_path, formats):
    collected, pools = privacy_curves(privacy, experiment, x_key)
    if not collected:
        return None
    usable = [panel for panel in FACT_KIND_PANELS
              if any(panel[0] in per_size for per_x in collected.values() for per_size in per_x.values())]
    if not usable:
        return None

    columns = min(3, len(usable))
    figure_rows = math.ceil(len(usable) / columns)
    figure, axes_grid = plt.subplots(figure_rows, columns, figsize=(4.6 * columns, 3.4 * figure_rows), squeeze=False)
    linestyle_of: dict = {}
    for index, (metric, title, layer) in enumerate(usable):
        axes = axes_grid[index // columns][index % columns]
        for (system, x), per_size in sorted(collected.items(), key=lambda item: str(item[0])):
            points = []
            for compromised, metrics in sorted(per_size.items()):
                if metric in metrics:
                    median, low, high = summarize(metrics[metric])
                    points.append((compromised / (pools.get((system, x)) or 1), median, low, high))
            if not points:
                continue
            style = SYSTEM_STYLE.get(system, {"color": "#444444", "marker": "^", "label": system})
            if x not in linestyle_of:
                linestyle_of[x] = SERIES_LINESTYLES[len(linestyle_of) % len(SERIES_LINESTYLES)]
            axes.plot([p[0] for p in points], [p[1] for p in points], color=style["color"],
                      marker=style["marker"], markersize=4, linewidth=1.6, linestyle=linestyle_of[x],
                      label=style["label"] + (f" ({x_key}={x})" if x_key and x is not None else ""))
            axes.fill_between([p[0] for p in points], [p[2] for p in points], [p[3] for p in points],
                              color=style["color"], alpha=0.15, linewidth=0)
        style_axes(axes, f"{title}  [{layer}]", "compromised fraction of the pool", "fraction learned")
        axes.set_ylim(-0.02, 1.05)
    for index in range(len(usable), figure_rows * columns):
        axes_grid[index // columns][index % columns].axis("off")

    handles, labels = figure_handles(figure)
    place_legend(figure, handles, labels)
    figure.suptitle(f"{experiment}: disclosure per atom kind", fontsize=12)
    return write_figure(figure, out_path, formats)


# what observing the coordination traffic adds on top of the configs
def fact_traffic_figure(privacy, experiment, x_key, out_path, formats):
    collected, pools = privacy_curves(privacy, experiment, x_key)
    if not collected:
        return None
    conditions = [("_config_only", "config only", ":"), ("", "with coordination traffic", "-")]
    panels = [("model_disclosure", "process model", "fraction of model atoms"),
              ("deployment_disclosure", "deployment", "fraction of deployment atoms")]
    if not any(f"{panels[1][0]}_config_only" in per_size for per_x in collected.values() for per_size in per_x.values()):
        return None  # no msgtrace in this experiment so there is nothing to compare

    figure, axes_grid = plt.subplots(1, 2, figsize=(9.6, 3.6), squeeze=False)
    for index, (base_metric, title, ylabel) in enumerate(panels):
        axes = axes_grid[0][index]
        for (system, x), per_size in sorted(collected.items(), key=lambda item: str(item[0])):
            for suffix, condition, linestyle in conditions:
                metric = base_metric + suffix
                points = []
                for compromised, metrics in sorted(per_size.items()):
                    if metric in metrics:
                        median, _low, _high = summarize(metrics[metric])
                        points.append((compromised / (pools.get((system, x)) or 1), median))
                if not points:
                    continue
                style = SYSTEM_STYLE.get(system, {"color": "#444444", "marker": "^", "label": system})
                axes.plot([p[0] for p in points], [p[1] for p in points], color=style["color"],
                          marker=style["marker"], markersize=4, linewidth=1.6, linestyle=linestyle,
                          label=f"{style['label']}, {condition}")
        style_axes(axes, title, "compromised fraction of the pool", ylabel)
        axes.set_ylim(-0.02, 1.05)
    handles, labels = figure_handles(figure)
    place_legend(figure, handles, labels)
    figure.suptitle(f"{experiment}: what the coordination traffic adds", fontsize=12)
    return write_figure(figure, out_path, formats)


FACT_KINDS = ("activity", "constraint", "constraint_type", "binding", "param", "place", "counterpart", "watch")


# the denominators for every fact disclosure fraction
def write_fact_universe_csv(privacy: dict, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["experiment", "run_id", "system", "nodes", "pool_size", "activities", "constraints", "roles",
                         "model_total", "deployment_total"] + [f"total_{kind}" for kind in FACT_KINDS]
                        + ["coordination_traffic_available"])
        for (experiment, run_id, system), block in sorted(privacy.items(), key=str):
            facts = (block["local"] or {}).get("facts")
            if not facts:
                continue
            totals = {**facts["model_totals_by_kind"], **facts["deployment_totals_by_kind"]}
            writer.writerow([experiment, run_id, system, block["params"].get("nodes"),
                             block["local"].get("pool_size"), facts["catalog"]["activities"],
                             facts["catalog"]["constraints"], facts["catalog"]["roles"],
                             facts["model_total"], facts["deployment_total"]]
                            + [totals.get(kind) for kind in FACT_KINDS]
                            + [facts.get("coordination_traffic_available")])


# privacy against the swept parameter
PRIVACY_VS_PARAMETER = [
    ("case_exposure_named", "case exposure (named)", "fraction of a case"),
    ("case_exposure_with_anonymous", "case exposure incl. anonymous", "fraction of a case"),
    ("model_disclosure", "process model disclosed", "fraction of model atoms"),
    ("deployment_disclosure", "deployment disclosed", "fraction of deployment atoms"),
    ("first_spy_precision", "first spy precision", "precision"),
]
PRIVACY_LEVELS = [("1 node", None, 1), ("25% of pool", 0.25, None), ("50% of pool", 0.5, None)]


# interpolate between the fraction points
def _value_at_fraction(per_size: dict, metric: str, pool: int, fraction: float):
    sizes = sorted(size for size in per_size if metric in per_size[size])
    if not sizes or pool < 2:
        return None
    target = fraction * pool
    # skip if the pool does not have enough points to show 1 node, 25% of nodes and 50% of nodes
    if target < sizes[0] or target > sizes[-1]:
        return None
    below = [size for size in sizes if size <= target]
    above = [size for size in sizes if size >= target]
    low, high = max(below), min(above)
    low_value = summarize(per_size[low][metric])[0]
    if low == high:
        return low_value
    high_value = summarize(per_size[high][metric])[0]
    weight = (target - low) / (high - low)
    return low_value + weight * (high_value - low_value)


def privacy_vs_parameter_figure(privacy, experiment, x_key, out_path, formats):
    collected, pools = privacy_curves(privacy, experiment, x_key)
    if not collected or all(x is None for _system, x in collected):
        return None

    usable_panels = [panel for panel in PRIVACY_VS_PARAMETER
              if any(panel[0] in per_size for per_x in collected.values() for per_size in per_x.values())]
    if not usable_panels:
        return None

    figure, axes_grid = plt.subplots(1, len(usable_panels), figsize=(4.6 * len(usable_panels), 3.6), squeeze=False)
    for index, (metric, title, ylabel) in enumerate(usable_panels):
        axes = axes_grid[0][index]
        for level_index, (level_name, fraction, exact) in enumerate(PRIVACY_LEVELS):
            for system in sorted({system for system, _x in collected}):
                points = []
                for (row_system, x), per_size in collected.items():
                    if row_system != system or x is None:
                        continue
                    pool = pools.get((system, x)) or 1
                    if exact is not None:
                        chosen = exact if exact in per_size else None
                        if chosen is None or metric not in per_size.get(chosen, {}):
                            continue
                        points.append((x, summarize(per_size[chosen][metric])[0]))
                    else:
                        value = _value_at_fraction(per_size, metric, pool, fraction)
                        if value is None:
                            continue
                        points.append((x, value))
                if not points:
                    continue
                points.sort(key=lambda point: point[0])
                style = SYSTEM_STYLE.get(system, {"color": "#444444", "marker": "^", "label": system})
                axes.plot([p[0] for p in points], [p[1] for p in points], color=style["color"], marker=style["marker"], markersize=4, linewidth=1.6, 
                          linestyle=SERIES_LINESTYLES[level_index % len(SERIES_LINESTYLES)], label=f"{style['label']}, {level_name}")
        style_axes(axes, title, x_key, ylabel)
        axes.set_ylim(-0.02, 1.05)

    handles, labels = figure_handles(figure)
    place_legend(figure, handles, labels)
    figure.suptitle(f"{experiment}: privacy against {x_key}", fontsize=12)
    return write_figure(figure, out_path, formats)


# how many nodes an attacker needs before a case can be reconstructed 
def min_nodes_figure(privacy, experiment, out_path, formats):
    by_length: dict[str, dict[int, list[float]]] = defaultdict(lambda: defaultdict(list))
    by_length_min: dict[str, dict[int, list[float]]] = defaultdict(lambda: defaultdict(list))
    histogram: dict[str, dict[int, int]] = defaultdict(lambda: defaultdict(int))
    headline: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    coverage: dict[str, list[float]] = defaultdict(list)

    for (exp, _run_id, system), block in privacy.items():
        if exp != experiment:
            continue
        stats = block["local"].get("min_nodes_full_case") or {}
        if not stats:
            continue
        for key in ("min", "p5", "p50", "p95", "max"):
            if stats.get(key) is not None:
                headline[system][key].append(float(stats[key]))
        covered, total = stats.get("cases_covered"), stats.get("cases_total")
        if covered is not None and total:
            coverage[system].append(covered / total)
        for count, cases in (stats.get("histogram") or {}).items():
            histogram[system][int(count)] += int(cases)
        for length, group in (stats.get("by_distinct_activities") or {}).items():
            if group.get("p50") is not None:
                by_length[system][int(length)].append(float(group["p50"]))
            if group.get("min") is not None:
                by_length_min[system][int(length)].append(float(group["min"]))

    if not headline:
        return None

    figure, (left, middle, right) = plt.subplots(1, 3, figsize=(13.2, 3.8))
    systems = sorted(headline)

    # left: nodes needed against the length of the case
    all_lengths: set[int] = set()
    for system in systems:
        style = SYSTEM_STYLE.get(system, {})
        lengths = sorted(by_length.get(system, {}))
        if not lengths:
            continue
        all_lengths |= set(lengths)
        left.plot(lengths, [summarize(by_length[system][length])[0] for length in lengths],
                  marker=style.get("marker", "o"), markersize=3.5, linewidth=1.4,
                  color=style.get("color", "#888"), label=f"{style.get('label', system)} (median case)")
        if by_length_min.get(system):
            left.plot(lengths, [summarize(by_length_min[system][length])[0] for length in lengths],
                      linestyle="--", linewidth=1.0, color=style.get("color", "#888"), alpha=0.7,
                      label=f"{style.get('label', system)} (easiest case)")
    if all_lengths:
        span = [min(all_lengths), max(all_lengths)]
        left.plot(span, span, linestyle=":", linewidth=1.4, color="#111111", zorder=6,
                  label="one node per activity")
    style_axes(left, "nodes needed against case length", "distinct activities in the case",
               "compromised nodes")
    left.legend(fontsize=7)

    # middle: the distribution the median summarises
    for system in systems:
        counts = histogram.get(system) or {}
        total = sum(counts.values())
        if not total:
            continue
        style = SYSTEM_STYLE.get(system, {})
        ordered = sorted(counts)
        middle.bar([count for count in ordered], [counts[count] / total for count in ordered],
                   width=0.8, alpha=0.55, color=style.get("color", "#888"),
                   label=style.get("label", system))
    style_axes(middle, "distribution over cases", "compromised nodes", "share of cases")
    middle.legend(fontsize=7)

    # right: the headline numbers, easiest case next to the typical one
    labels = [("min", "easiest case"), ("p5", "p5"), ("p50", "median case")]
    width = 0.8 / max(1, len(systems))
    for index, system in enumerate(systems):
        style = SYSTEM_STYLE.get(system, {})
        positions = [position + index * width for position in range(len(labels))]
        values = [summarize(headline[system].get(key, [0]))[0] for key, _text in labels]
        right.bar(positions, values, width=width, color=style.get("color", "#888"),
                  label=style.get("label", system))
        for position, value in zip(positions, values):
            right.text(position, value, f"{value:.0f}", ha="center", va="bottom", fontsize=8)
    right.set_xticks([position + width * (len(systems) - 1) / 2 for position in range(len(labels))])
    right.set_xticklabels([text for _key, text in labels], fontsize=8)
    style_axes(right, "median across runs", "", "compromised nodes")
    right.legend(fontsize=7)

    dropped = [f"{SYSTEM_STYLE.get(system, {}).get('label', system)}: "
               f"{summarize(coverage[system])[0]:.0%} of cases coverable"
               for system in systems if coverage.get(system) and summarize(coverage[system])[0] < 0.999]
    subtitle = f"{experiment}: nodes needed to reconstruct a case"
    if dropped:
        subtitle += "   (" + "; ".join(dropped) + ")"
    figure.suptitle(subtitle, fontsize=11)
    figure.tight_layout(rect=(0, 0, 1, 0.93))
    return write_figure(figure, out_path, formats)


# nodes needed against the number of distinct activities in the case
NODES_VS_ACTIVITIES_PANELS = [("min", "easiest case (min over cases)"),
                              ("p50", "median case"),
                              ("max", "hardest case (max over cases)")]


# entries is a list of (system, privacy block) for the runs this figure should cover
def _nodes_vs_activities_panels(entries, out_path, formats, subtitle_prefix: str):
    # system -> statistic -> distinct activity count -> one value per run
    collected: dict = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    cases_at: dict[str, dict[int, int]] = defaultdict(lambda: defaultdict(int))
    exact: dict[str, list[int]] = defaultdict(lambda: [0, 0])

    for system, block in entries:
        groups = ((block["local"].get("min_nodes_full_case") or {})
                  .get("by_distinct_activities") or {})
        for distinct, group in groups.items():
            distinct = int(distinct)
            cases = int(group.get("cases") or 0)
            cases_at[system][distinct] += cases
            for statistic, _title in NODES_VS_ACTIVITIES_PANELS:
                if group.get(statistic) is not None:
                    collected[system][statistic][distinct].append(float(group[statistic]))
            exact[system][1] += cases
            if group.get("min") == distinct and group.get("max") == distinct:
                exact[system][0] += cases

    if not collected:
        return None, {}

    all_x = {distinct for system in collected for statistic in collected[system]
             for distinct in collected[system][statistic]}
    all_y = [summarize(values)[0] for system in collected for statistic in collected[system]
             for values in collected[system][statistic].values()]
    x_low, x_high = min(all_x), max(all_x)
    y_low, y_high = min(all_y), max(all_y)
    figure, axes_row = plt.subplots(1, 3, figsize=(13.5, 4.4))

    for axes, (statistic, panel_title) in zip(axes_row, NODES_VS_ACTIVITIES_PANELS):
        for system in sorted(collected):
            points = collected[system].get(statistic) or {}
            if not points:
                continue
            style = SYSTEM_STYLE.get(system, {})
            xs = sorted(points)
            ys = [summarize(points[x])[0] for x in xs]
            weights = [cases_at[system][x] for x in xs]
            largest = max(weights) or 1
            axes.plot(xs, ys, linewidth=1.4, color=style.get("color", "#888"), zorder=3,
                      label=style.get("label", system))
            axes.scatter(xs, ys, s=[12 + 80 * weight / largest for weight in weights],
                         color=style.get("color", "#888"), alpha=0.75, zorder=4,
                         marker=style.get("marker", "o"))
        pad_x = max(1.0, 0.06 * (x_high - x_low))
        pad_y = max(0.5, 0.10 * (y_high - y_low))
        axes.set_xlim(x_low - pad_x, x_high + pad_x)
        axes.set_ylim(max(0.0, y_low - pad_y), y_high + pad_y)
        style_axes(axes, panel_title, "distinct activities in the case",
                   "nodes needed to reconstruct the case")
        axes.legend(fontsize=7, loc="upper left", framealpha=0.9)

    notes = [f"{SYSTEM_STYLE.get(system, {}).get('label', system)}: "
             f"{on_line / total:.0%} of cases need exactly one node per distinct activity"
             for system, (on_line, total) in sorted(exact.items()) if total]
    figure.suptitle(f"{subtitle_prefix}: nodes needed against distinct activities in the case"
                    + ("   (" + "; ".join(notes) + ")" if notes else ""), fontsize=11)
    figure.tight_layout(rect=(0, 0, 1, 0.94))
    exact_counts = {name: tuple(counts) for name, counts in exact.items()}
    return write_figure(figure, out_path, formats), exact_counts


# parameters that influence role distribution in sweeps
PLACEMENT_PARAMS = {"constraint_nodes", "nodes", "repeat"}


def min_nodes_vs_activities_figures(privacy, experiment, target, formats, sweep_key) -> list[Path]:
    if sweep_key not in PLACEMENT_PARAMS:
        sweep_key = None
    entries = [(block["params"].get(sweep_key) if sweep_key else None, system, block)
               for (exp, _run_id, system), block in privacy.items() if exp == experiment]
    if not entries:
        return []
    values = sorted({value for value, _system, _block in entries if value is not None},
                    key=lambda value: (isinstance(value, str), value))

    if sweep_key is None or len(values) <= 1:
        result = _nodes_vs_activities_panels([(system, block) for _value, system, block in entries],
                                             target / "privacy_nodes_vs_activities", formats,
                                             experiment)
        return [result[0]] if result and result[0] else []

    written: list[Path] = []
    exact_by_value: dict = {}
    for value in values:
        subset = [(system, block) for other, system, block in entries if other == value]
        # a filename fragment that survives a float or a string value
        tag = str(value).replace(".", "p").replace("/", "_").replace(" ", "")
        result = _nodes_vs_activities_panels(
            subset, target / f"privacy_nodes_vs_activities__{sweep_key}{tag}", formats,
            f"{experiment}, {sweep_key} = {value}")
        if result and result[0]:
            written.append(result[0])
            exact_by_value[value] = result[1]

    summary = _nodes_vs_activities_summary(exact_by_value, experiment, sweep_key,
                                           target / "privacy_nodes_vs_activities_summary", formats)
    if summary:
        written.append(summary)
    return written


def _nodes_vs_activities_summary(exact_by_value: dict, experiment, sweep_key, out_path, formats):
    if len(exact_by_value) < 2:
        return None
    systems = sorted({system for counts in exact_by_value.values() for system in counts})
    figure, axes = plt.subplots(figsize=(5.6, 3.8))
    values = sorted(exact_by_value)
    for system in systems:
        style = SYSTEM_STYLE.get(system, {})
        xs, ys = [], []
        for value in values:
            on_line, total = exact_by_value[value].get(system, (0, 0))
            if total:
                xs.append(value)
                ys.append(on_line / total)
        if xs:
            axes.plot(xs, ys, marker=style.get("marker", "o"), markersize=4, linewidth=1.4,
                      color=style.get("color", "#888"), label=style.get("label", system))
    axes.set_ylim(-0.03, 1.03)
    style_axes(axes, "cases needing one node per distinct activity", sweep_key, "share of cases")
    axes.legend(fontsize=8)
    figure.suptitle(f"{experiment}: does the 1:1 relation hold across the sweep?", fontsize=11)
    figure.tight_layout(rect=(0, 0, 1, 0.93))
    return write_figure(figure, out_path, formats)


# one factor at a time (ofat)
def sensitivity_figure(rows, varied, metrics, title, out_path, formats):
    if not any(row.value(metric[0]) is not None for row in rows for metric in metrics):
        return None
    columns = len(metrics)
    figure, axes_grid = plt.subplots(len(varied), columns, figsize=(4.2 * columns, 3.0 * len(varied)), squeeze=False)
    for param_index, param in enumerate(varied):
        spoke = ofat_rows(rows, varied, param)
        for metric_index, (metric, panel_title, ylabel, scale, logy) in enumerate(metrics):
            axes = axes_grid[param_index][metric_index]
            draw_curve(axes, curve(spoke, param, metric, None, scale), None)
            style_axes(axes, panel_title if param_index == 0 else "", param, ylabel, logy=logy)

    handles, labels = figure_handles(figure)
    place_legend(figure, handles, labels)
    figure.suptitle(title, fontsize=12)
    return write_figure(figure, out_path, formats)


# output tables
CSV_METRICS = [
    ("achieved_rate", "throughput.effective_rate", 1.0),
    ("offered_rate", "throughput.offered_rate", 1.0),
    ("pacer_max_lag_ms", "throughput.pacer_max_lag_ms", 1.0),
    ("message_drops", "throughput.message_drops.total", 1.0),
    ("event_drops", "throughput.event_drops.total", 1.0),
    ("closed_case_drops", "throughput.closed_case_drops", 1.0),
    ("event_q_max", "throughput.queues.event_q.max", 1.0),
    ("detection_p50_ms", "latency_us.detection_t1_t3.p50", 1e-3),
    ("detection_p95_ms", "latency_us.detection_t1_t3.p95", 1e-3),
    ("detection_p99_ms", "latency_us.detection_t1_t3.p99", 1e-3),
    ("routing_p95_ms", "latency_us.routing_t1_t2.p95", 1e-3),
    ("checking_p95_ms", "latency_us.checking_t2_t3.p95", 1e-3),
    ("provisional_p95_ms", "latency_us.provisional_t3_t4.p95", 1e-3),
    ("case_end_verdict_p95_ms", "latency_us.case_end_verdict.p95", 1e-3),
    ("gossip_p95_ms", "latency_us.gossip_dissemination.p95", 1e-3),
    ("cpu_s_total", "resources.cpu_seconds_excl_sink", 1.0),
    ("cpu_ms_per_event", "resources.cpu_seconds_per_event", 1e3),
    ("cpu_pct_max_node", "resources.cpu_percent_max_node", 1.0),
    ("rss_mb_total", "resources.rss_kb_total_max", 1 / 1024),
    ("idle_cpu_per_node_s", "resources.idle.cpu_seconds_per_node_second", 1.0),
    ("coord_bytes_per_event", "traffic.bytes_per_event.coordination", 1.0),
    ("coord_msgs_per_event", "traffic.messages_per_event.coordination", 1.0),
    ("coord_msgs_per_case", "traffic.messages_per_case.coordination", 1.0),
    ("transport_bytes_per_event", "traffic.bytes_per_event.event", 1.0),
    ("load_bytes_per_event", "traffic.bytes_per_event.load_driven_excl_sink", 1.0),
    ("gossip_push_per_case", "traffic.gossip.push_per_case", 1.0),
    ("gossip_repair_per_case", "traffic.gossip.ae_set_per_case", 1.0),
    ("gossip_push_share", "traffic.gossip.push_share", 1.0),
    ("fixed_bytes_per_node_s_idle", "traffic.fixed.idle.bytes_per_node_second.total", 1.0),
    ("fixed_bytes_per_node_s_load", "traffic.fixed.workload.bytes_per_node_second.total", 1.0),
    ("overlay_bytes_per_node_s", "traffic.fixed.idle.bytes_per_node_second.overlay", 1.0),
    ("ae_digest_bytes_per_node_s", "traffic.fixed.idle.bytes_per_node_second.gossip_anti_entropy", 1.0),
    ("result_msgs_per_case", "traffic.messages_per_case.result", 1.0),
    ("gossip_split_recorded", "traffic.gossip_split_recorded", 1.0),
    ("pairs_multi_revision", "revisions.pairs_with_multiple", 1.0),
    ("revisions_total", "revisions.revisions_total", 1.0),
    ("event_q_p95", "throughput.queues.event_q.p95", 1.0),
    ("gossip_in_q_p95", "throughput.queues.gossip_in_q.p95", 1.0),
    ("gossip_out_q_p95", "throughput.queues.gossip_out_q.p95", 1.0),
    ("sender_cpu_pct", "resources.non_system.tooling.cpu_percent", 1.0),
    ("host_busy_pct", "resources.host_cpu_busy_fraction", 100.0),
    ("threads_total_max", "resources.threads_total_max", 1.0),
    ("rss_mb_max_node", "resources.rss_kb_max_node", 1 / 1024),
    ("gossip_reach_fraction", "latency_us.gossip_dissemination.reach_fraction", 1.0),
    ("revision_pairs", "revisions.pairs", 1.0),
    ("expected_pairs", "quality.expected_pairs", 1.0),
    ("peer_churn_created", "messages.peer_churn.created", 1.0),
    ("peer_churn_removed", "messages.peer_churn.removed", 1.0),
    ("repair_drops", "revisions.correction_drops.total", 1.0),
    ("result_drops", "revisions.result_drops.total", 1.0),
    ("saturated", "throughput.saturated", 1.0),
    ("complete", "quality.complete", 1.0),
]


def write_runs_csv(rows: list[Row], out_path: Path) -> None:
    param_keys = sorted({key for row in rows for key, value in row.params.items() if not isinstance(value, (list, dict))})
    header = (["experiment", "run_id", "replicate", "system"] + param_keys + [name for name, _path, _scale in CSV_METRICS]
              + ["correct", "wrong", "absent", "spurious", "accuracy"])
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        for row in sorted(rows, key=lambda item: (item.experiment, item.run_id, item.system)):
            line = [row.experiment, row.run_id, row.replicate, row.system]
            line += [row.params.get(key) for key in param_keys]
            for _name, path, scale in CSV_METRICS:
                value = row.value(path)
                line.append(value * scale if isinstance(value, (int, float)) and not isinstance(value, bool) else value)
            line += [row.correctness.get(field) for field in ("correct", "wrong", "absent", "spurious", "accuracy")]
            writer.writerow(line)


def write_privacy_csv(privacy: dict, out_path: Path) -> None:
    metric_names: set = set()
    for block in privacy.values():
        for entry in block["local"].get("per_size", []):
            metric_names |= {key for key, value in entry.items() if isinstance(value, dict) and "n" in value}
    metric_names = sorted(metric_names)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["experiment", "run_id", "system", "nodes", "pool_size", "compromised", "compromised_fraction"] + metric_names)
        for (experiment, run_id, system), block in sorted(privacy.items(), key=str):
            local = block["local"]
            for entry in local.get("per_size", []):
                writer.writerow([experiment, run_id, system, block["params"].get("nodes"), local.get("pool_size"), entry.get("compromised"), entry.get("compromised_fraction")]
                                 + [entry.get(name, {}).get("mean") for name in metric_names])


# feasibility: correctness over all runs split into nominal, overloaded, incomplete and degraded runs
def run_regime(row: Row) -> str:
    # idle only runs send no events, so every expected pair is missing by construction and they should not be mixed with the other fasibility measurements
    if not row.has_workload:
        return "idle"
    if not row.value("quality.complete"):
        return "incomplete"
    saturated = bool(row.value("throughput.saturated"))
    clean = not any(row.correctness.get(field) or 0 for field in ("wrong", "absent", "spurious"))
    if saturated:
        return "overload"
    return "nominal" if clean else "degraded"


FEASIBILITY_FIELDS = ("expected_pairs", "produced_pairs", "correct", "wrong", "absent", "spurious")


def write_feasibility(rows: list[Row], out_path: Path) -> dict:
    per_group: dict = defaultdict(lambda: defaultdict(int))
    for row in rows:
        target = per_group[(run_regime(row), row.experiment, row.system)]
        target["runs"] += 1
        target["events"] += row.value("throughput.events_sent") or 0
        for field in FEASIBILITY_FIELDS:
            target[field] += row.correctness.get(field) or 0

    def line(writer, regime, experiment, system, values):
        accuracy = (values["correct"] / values["expected_pairs"]) if values["expected_pairs"] else None
        writer.writerow([regime, experiment, system, values["runs"], values["events"],
                         values["expected_pairs"], values["produced_pairs"], values["correct"],
                         values["wrong"], values["absent"], values["spurious"],
                         f"{accuracy:.6f}" if accuracy is not None else ""])

    by_regime: dict = defaultdict(lambda: defaultdict(int))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["regime", "experiment", "system", "runs", "events", "expected_pairs",
                         "produced_pairs", "correct", "wrong", "absent", "spurious", "accuracy"])
        for (regime, experiment, system), values in sorted(per_group.items()):
            line(writer, regime, experiment, system, values)
            for key, value in values.items():
                by_regime[regime][key] += value
        for regime in ("nominal", "overload", "degraded", "incomplete", "idle"):
            if by_regime.get(regime):
                line(writer, regime, "ALL", "ALL", by_regime[regime])
    return {regime: dict(values) for regime, values in by_regime.items()}


def privacy_figures(experiment, privacy, experiment_privacy, target, formats, privacy_x, sensitivity, note) -> list[Path]:
    written: list[Path] = []
    if not any(key[0] == experiment for key in privacy):
        return written
    note("privacy curves")
    for name, kwargs in (("privacy", {}), ("privacy_by_fraction", {"by_fraction": True})):
        figure = privacy_figure(privacy, experiment, privacy_x, target / name, formats, **kwargs)
        if figure:
            written.append(figure)
    if not sensitivity:
        figure = privacy_vs_parameter_figure(privacy, experiment, privacy_x,
                                             target / "privacy_vs_parameter", formats)
        if figure:
            written.append(figure)
    figure = min_nodes_figure(privacy, experiment, target / "privacy_min_nodes", formats)
    if figure:
        written.append(figure)
    written += min_nodes_vs_activities_figures(privacy, experiment, target, formats, privacy_x)

    note("best and worst compromised set")
    figure = privacy_extremes_figure(privacy, experiment, privacy_x, target / "privacy_extremes", formats)
    if figure:
        written.append(figure)

    note("distribution over the compromised sets")
    for metric, label in ECDF_METRICS:
        figure = ecdf_figure(privacy, experiment, metric, label, target / f"privacy_ecdf_{metric}", formats)
        if figure:
            written.append(figure)

    note("fact disclosure per atom kind")
    for name, builder in (("privacy_facts_breakdown", fact_breakdown_figure), ("privacy_facts_traffic", fact_traffic_figure)):
        figure = builder(privacy, experiment, privacy_x, target / name, formats)
        if figure:
            written.append(figure)
    write_fact_universe_csv(experiment_privacy, target / "fact_universe.csv")
    return written


def plot_experiment(experiment: str, rows: list[Row], privacy: dict, out_dir: Path, formats, progress: Progress | None) -> list[Path]:
    written: list[Path] = []
    target = out_dir / experiment.replace("/", "_")

    experiment_privacy = {key: value for key, value in privacy.items() if key[0] == experiment}
    write_runs_csv(rows, target / "runs.csv")
    write_privacy_csv(experiment_privacy, target / "privacy.csv")
    write_feasibility(rows, target / "feasibility.csv")

    varied = find_varying_params(rows)
    numeric = [key for key in varied if is_numeric(rows, key)]
    workload_rows = [row for row in rows if row.has_workload]
    idle_only = not workload_rows

    def note(message):
        if progress is not None:
            progress.note(f"{experiment}: {message}")

    sensitivity = is_sensitivity(rows, numeric) and len(numeric) >= 3
    if sensitivity:
        note("sensitivity panels")
        figure = sensitivity_figure(rows, numeric, [MESSAGE_PANELS[1], LATENCY_PANELS[1], RESOURCE_PANELS[1]],
                                    f"{experiment}: one factor at a time", target / "sensitivity", formats)
        if figure:
            written.append(figure)
        # one privacy figure per spoke
        for parameter in numeric:
            spoke_ids = {row.run_id for row in ofat_rows(rows, numeric, parameter)}
            spoke_privacy = {key: value for key, value in privacy.items()
                             if key[0] == experiment and key[1] in spoke_ids}
            if not spoke_privacy:
                continue
            note(f"privacy against {parameter}")
            figure = privacy_vs_parameter_figure(spoke_privacy, experiment, parameter, target / f"privacy_vs_{parameter}", formats)
            if figure:
                written.append(figure)

    x_key = numeric[0] if numeric else (varied[0] if varied else None)
    series_key = numeric[1] if len(numeric) > 1 and not is_sensitivity(rows, numeric) else None
    if x_key is None:
        # nothing varies so there are no scalability panels to draw, except for the privacy figures per compromise size
        return written + privacy_figures(experiment, privacy, experiment_privacy, target, formats, None, sensitivity, note)

    distinct_x = len({row.params.get(x_key) for row in rows})
    logx = bool(x_key in ("rate", "nodes", "repeat", "cases") and distinct_x > 3)

    if idle_only:
        note("fixed cost")
        figure = panel_figure(rows, x_key, IDLE_PANELS, series_key,
                              f"{experiment}: fixed cost of the deployment", target / "idle", formats, logx)
        if figure:
            written.append(figure)
        note("fixed cost breakdown")
        figure = fixed_cost_breakdown_figure(rows, x_key, target / "message_breakdown_fixed", formats)
        if figure:
            written.append(figure)
    else:
        for group_name, panels in PANEL_GROUPS:
            note(group_name)
            figure = panel_figure(workload_rows, x_key, panels, series_key, f"{experiment}: {group_name} against {x_key}", target / group_name, formats, logx)
            if figure:
                written.append(figure)

        for key, label in LATENCY_INTERVALS:
            note(f"latency: {label}")
            figure = panel_figure(workload_rows, x_key, latency_panels(key, label), series_key,
                                  f"{experiment}: {label} against {x_key}",
                                  target / f"latency_{key}", formats, logx)
            if figure:
                written.append(figure)

        for group_name, panels in (("revisions", REVISION_PANELS), ("memory", MEMORY_PANELS)):
            note(group_name)
            figure = panel_figure(workload_rows, x_key, panels, series_key,
                                  f"{experiment}: {group_name} against {x_key}",
                                  target / group_name, formats, logx)
            if figure:
                written.append(figure)

        for metric, scope, name in RSS_TIMELINE_SERIES:
            note(f"memory over time ({scope})")
            figure = memory_timeline_figure(workload_rows, x_key, target / name, formats,
                                            metric=metric, scope=scope)
            if figure:
                written.append(figure)

        process_table = write_process_table(rows, x_key, target / "processes.csv")
        if process_table:
            note("process counts")
            written.append(process_table)

        note("load driven traffic breakdown")
        figure = load_traffic_breakdown_figure(workload_rows, x_key, target / "message_breakdown_load", formats)
        if figure:
            written.append(figure)

        note("fixed cost breakdown")
        figure = fixed_cost_breakdown_figure(rows, x_key, target / "message_breakdown_fixed", formats)
        if figure:
            written.append(figure)

        if any(row.value("traffic.fixed.idle") or row.value("resources.idle.nodes") for row in rows):
            note("fixed cost")
            figure = panel_figure(rows, x_key, IDLE_PANELS, series_key,
                                  f"{experiment}: fixed cost against {x_key}", target / "idle", formats, logx)
            if figure:
                written.append(figure)

    # check if compute_privacy.py has run for this experiment
    privacy_x = None if sensitivity else (x_key if distinct_x > 1 else None)
    written += privacy_figures(experiment, privacy, experiment_privacy, target, formats, privacy_x, sensitivity, note)

    return written


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--results", type=Path, required=True, help="results directory to walk")
    parser.add_argument("--experiment", default=None, help="only experiments matching this glob")
    parser.add_argument("--out", type=Path, default=None, help="output directory (default <results>/_plots)")
    parser.add_argument("--format", default="png", help="comma separated: png, pdf, svg")
    parser.add_argument("--quiet", action="store_true", help="no progress output")
    args = parser.parse_args()

    formats = [suffix.strip().lstrip(".") for suffix in args.format.split(",") if suffix.strip()]
    out_dir = args.out or (args.results / "_plots")

    loading = Progress(len(discover_runs(args.results, args.experiment, "metrics.json")), "reading", enabled=not args.quiet)
    rows, privacy = load_rows(args.results, args.experiment, loading)
    loading.finish()
    if not rows:
        sys.exit("no metrics.json found: run extract_metrics.py first")

    if args.experiment:
        suffix = args.experiment.replace("*", "x").replace("/", "_").replace("?", "q")
        totals = write_feasibility(rows, out_dir / f"feasibility.{suffix}.csv")
        print(f"--experiment is set: campaign wide runs.csv and privacy.csv were left untouched, "
              f"the per experiment tables are under {out_dir}", file=sys.stderr)
    else:
        write_runs_csv(rows, out_dir / "runs.csv")
        write_privacy_csv(privacy, out_dir / "privacy.csv")
        write_fact_universe_csv(privacy, out_dir / "fact_universe.csv")
        totals = write_feasibility(rows, out_dir / "feasibility.csv")

    by_experiment: dict[str, list[Row]] = defaultdict(list)
    for row in rows:
        by_experiment[row.experiment].append(row)

    progress = Progress(len(by_experiment), "plotting", enabled=not args.quiet)
    written = 0
    for experiment, experiment_rows in sorted(by_experiment.items()):
        figures = plot_experiment(experiment, experiment_rows, privacy, out_dir, formats, progress)
        written += len(figures)
        progress.log(f"{experiment}: {len(figures)} figure(s), {len(experiment_rows)} (run, system) pair(s)")
        progress.step(experiment)
    progress.finish()

    tables = 3 * len(by_experiment) + (1 if args.experiment else 3)
    print(f"\n{written} figure(s) and {tables} table(s) in {out_dir}")
    for regime, label in (("nominal", "within capacity"), ("overload", "saturated"),
                          ("degraded", "IMPERFECT WITH NO SATURATION SIGNAL - investigate"),
                          ("incomplete", "truncated records"),
                          ("idle", "no workload, excluded from correctness")):
        values = totals.get(regime)
        if not values:
            continue
        accuracy = (values["correct"] / values["expected_pairs"]) if values.get("expected_pairs") else None
        print(f"feasibility [{regime}: {label}]: {values.get('runs', 0)} (run, system) pair(s), "
              f"{values.get('events', 0)} events, {values.get('expected_pairs', 0)} expected pairs, "
              f"{values.get('wrong', 0)} wrong, {values.get('absent', 0)} absent, "
              f"{values.get('spurious', 0)} spurious"
              + (f", accuracy {accuracy:.6f}" if accuracy is not None else ""))


if __name__ == "__main__":
    main()