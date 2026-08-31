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
  feasibility.csv = correctness aggregated over all runs
  <experiment>/*.png = the figures of that experiment

which figures an experiment gets is decided by what is in its runs:

  a parameter sweep -> scalability panels against that parameter (rate, nodes, repeat, cap, ...)
  runs with no workload -> idle cost panels (per node cost against node count)
  netem varies -> latency and margin against the delay (one line per rate)
  privacy.json present -> exposure, disclosure and first spy curves against the compromise size
  one factor at a time -> sensitivity panels (one small multiple per varied parameter)

the x axis is the parameter that actually varies inside the experiment
if two parameters vary, the second becomes the series
"""
# TODO: change the x-axis on node count plots
#   plot against constraint nodes = nodes − 1, not nodes. That makes 22 the 1:1 point for the default catalog, 17 for sepsis, 12 for traffic fines — all readable as "one role per node" without the sink-counting arithmetic in the reader's head. Then add a small table giving, for each x, the process count of each deployment:
#       -> 22 constraint nodes: 
#           decentralized processes = 22 + sink
#           central processes = 22 forwarders + 1 coordinator + sink
#   report CPU both as totals and normalised per capped core, so a reader can see the +1 and confirm it doesnt carry the result
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
# parameters that identify a repetition
REPETITION_PARAMS = {"seed", "replicate", "start", "order_index"}
IGNORED_PARAMS = {"stream_dir", "manifest", "metrics", "violation_sampling", "quiescence_s", "max_wait_for_settlement_s", "idle_window_s", 
                  "msgtrace", "external_central_sink", "cases"}


def style_axes(axes, title: str, xlabel: str, ylabel: str, logx=False, logy=False) -> None:
    axes.set_title(title, fontsize=10)
    axes.set_xlabel(xlabel, fontsize=9)
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
        self.params["netem_ms"] = self.params.get("netem_ms", 0)

    def value(self, path: str):
        if path.startswith("correctness."):
            return find_nested_value(self.correctness, path.split(".", 1)[1])
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
                privacy[key] = {"params": metrics.get("params") or {},
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
    ("throughput.queues.event_q.max", "event queue peak depth", "events", 1.0, False),
    ("throughput.message_drops.total", "message drops", "messages", 1.0, False),
    ("throughput.closed_case_drops", "dropped after the case closed", "events + messages", 1.0, False),
]
LATENCY_PANELS = [
    ("latency_us.detection_t1_t3.p50", "detection latency p50 (t1 to t3)", "ms", 1e-3, False),
    ("latency_us.detection_t1_t3.p95", "detection latency p95", "ms", 1e-3, False),
    ("latency_us.detection_t1_t3.p99", "detection latency p99", "ms", 1e-3, False),
    ("latency_us.routing_t1_t2.p95", "routing delay p95 (t1 to t2)", "ms", 1e-3, False),
    ("latency_us.checking_t2_t3.p95", "checking delay p95 (t2 to t3)", "ms", 1e-3, False),
    ("latency_us.provisional_t3_t4.p95", "provisional window p95 (t3 to t4)", "ms", 1e-3, False),
]
RESOURCE_PANELS = [
    ("resources.cpu_seconds_per_event", "cpu per event", "cpu-ms/event", 1e3, False),
    ("resources.cpu_seconds_excl_sink", "cpu total (sink excluded)", "cpu-s", 1.0, False),
    ("resources.cpu_percent_max_node", "cpu of the busiest node", "% of one core", 1.0, False),
    ("resources.busiest_thread.cpu_percent", "cpu of the busiest thread", "% of one core", 1.0, False),
    ("resources.rss_kb_total_max", "resident memory (all nodes)", "MB", 1 / 1024, False),
]
MESSAGE_PANELS = [
    ("messages.derived.messages_per_event", "messages per event", "messages", 1.0, False),
    ("messages.derived.bytes_per_event_excl_sink", "bytes per event (sink excluded)", "bytes", 1.0, False),
    ("messages.derived.bytes_per_event_coordination", "coordination bytes per event", "bytes", 1.0, False),
    ("messages.derived.bytes_per_event_transport", "event transport bytes per event", "bytes", 1.0, False),
    ("messages.derived.messages_per_case", "messages per case", "messages", 1.0, False),
    ("messages.derived.coordination_ratio", "coordination share of traffic", "fraction", 1.0, False),
]
CORRECTNESS_PANELS = [
    ("correctness.wrong", "wrong verdicts", "pairs", 1.0, False),
    ("correctness.absent", "missing verdicts", "pairs", 1.0, False),
    ("revisions.pairs_with_multiple", "pairs with more than one revision", "pairs", 1.0, False),
    ("revisions.revisions_total", "revisions in total", "revisions", 1.0, False),
    ("revisions.final_after_send_end", "verdicts finalised after the stream ended", "pairs", 1.0, False),
    ("margin.coordination_slower_than_gap_fraction", "coordination slower than the event gap", "fraction", 1.0, False),
]
IDLE_PANELS = [
    ("resources.idle.cpu_seconds_per_node_second", "idle cpu per node", "cpu-s per node-second", 1.0, False),
    ("resources.idle.rss_kb_total", "idle resident memory (all nodes)", "MB", 1 / 1024, False),
    ("messages.derived.idle_bytes_per_node_second", "idle traffic per node", "bytes/s per node", 1.0, False),
    ("messages.idle.sent", "idle messages sent", "messages", 1.0, False),
]
PANEL_GROUPS = [
    ("throughput", THROUGHPUT_PANELS),
    ("latency", LATENCY_PANELS),
    ("resources", RESOURCE_PANELS),
    ("messages", MESSAGE_PANELS),
    ("correctness_cost", CORRECTNESS_PANELS),
]


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
    for index in range(len(usable_panels), figure_rows * columns):
        axes_grid[index // columns][index % columns].axis("off")

    handles, labels = axes_grid[0][0].get_legend_handles_labels()
    if handles:
        figure.legend(handles, labels, loc="lower center", ncol=min(4, len(labels)), fontsize=9,
                      frameon=False, bbox_to_anchor=(0.5, -0.01))
    figure.suptitle(title, fontsize=12)
    figure.tight_layout(rect=(0, 0.03, 1, 0.96))
    return write_figure(figure, out_path, formats)


def write_figure(figure, out_path: Path, formats) -> Path:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    written = None
    for suffix in formats:
        target = out_path.with_suffix(f".{suffix}")
        figure.savefig(target, dpi=150, bbox_inches="tight")
        written = written or target
    plt.close(figure)
    return written


# stacked bytes per event by payload class
def message_breakdown_figure(rows, x_key, out_path, formats):
    payload_totals: dict[tuple, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list)) # (system, param) -> (payload type -> bytes/event)
    for row in rows:
        events = row.value("throughput.events_sent") or 0
        by_type = row.value("messages.workload.by_payload_type") or {}
        if not events or not by_type:
            continue
        for payload_type, counts in by_type.items():
            payload_totals[(row.system, row.params.get(x_key))][payload_type].append(
                counts.get("bytes_sent", 0) / events)
    if not payload_totals:
        return None

    systems = sorted({system for system, _x in payload_totals})
    x_values = sorted({x for _system, x in payload_totals if x is not None}, key=lambda value: (value is None, value))
    payloads = sorted({payload for per_payload in payload_totals.values() for payload in per_payload},
                      key=lambda name: list(PAYLOAD_COLORS).index(name) if name in PAYLOAD_COLORS else 99)

    figure, axes = plt.subplots(figsize=(1.6 + 1.5 * max(1, len(x_values)) * len(systems), 4.0))
    width = 0.8 / max(1, len(systems))
    for system_index, system in enumerate(systems):
        bottoms = [0.0] * len(x_values)
        positions = [index + system_index * width - 0.4 + width / 2 for index in range(len(x_values))]
        for payload in payloads:
            heights = []
            for x_index, x in enumerate(x_values):
                values = payload_totals[(system, x)].get(payload, [])
                heights.append(summarize(values)[0] if values else 0.0)
            heights = [0.0 if math.isnan(height) else height for height in heights]
            axes.bar(positions, heights, width=width * 0.92, bottom=bottoms,
                     color=PAYLOAD_COLORS.get(payload, "#888888"),
                     edgecolor="white", linewidth=0.4,
                     label=payload if system_index == 0 else None)
            bottoms = [bottom + height for bottom, height in zip(bottoms, heights)]
        for position, total in zip(positions, bottoms):
            axes.text(position, total, system[:3], ha="center", va="bottom", fontsize=7, color="#333333")

    axes.set_xticks(range(len(x_values)))
    axes.set_xticklabels([str(x) for x in x_values])
    style_axes(axes, "bytes per event by payload class (bar label: system)", x_key, "bytes/event")
    axes.legend(fontsize=8, frameon=False, ncol=min(4, len(payloads)), loc="upper center", bbox_to_anchor=(0.5, -0.13))
    figure.tight_layout()
    return write_figure(figure, out_path, formats)


PRIVACY_PANELS = [
    ("case_exposure_named", "case exposure (named activities)", "fraction of a case"),
    ("case_exposure_with_anonymous", "case exposure incl. anonymous occurrences", "fraction of a case"),
    ("total_reconstruction_probability", "probability of reconstructing every case", "probability"),
    ("full_case_fraction", "cases fully named", "fraction of cases"),
    ("model_fact_disclosure", "process model disclosed", "fraction of model facts"),
    ("placement_fact_disclosure", "placement disclosed", "fraction of placement facts"),
    ("first_spy_precision", "first spy precision", "precision"),
    ("case_disclosure_set", "case disclosure (set knowledge)", "cd"),
    ("trace_disclosure_set", "trace disclosure (set knowledge)", "td"),
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

    handles, labels = axes_grid[0][0].get_legend_handles_labels()
    if handles:
        figure.legend(handles, labels, loc="lower center", ncol=min(4, len(labels)), fontsize=9,
                      frameon=False, bbox_to_anchor=(0.5, -0.01))
    figure.suptitle(f"{experiment}: local attacker" + (" (pool fraction)" if by_fraction else ""), fontsize=12)
    figure.tight_layout(rect=(0, 0.03, 1, 0.96))
    return write_figure(figure, out_path, formats)


# privacy against the swept parameter
PRIVACY_VS_PARAMETER = [
    ("case_exposure_named", "case exposure (named)", "fraction of a case"),
    ("case_exposure_with_anonymous", "case exposure incl. anonymous", "fraction of a case"),
    ("model_fact_disclosure", "process model disclosed", "fraction of model facts"),
    ("first_spy_precision", "first spy precision", "precision"),
]
PRIVACY_LEVELS = [("1 node", None, 1), ("25% of pool", 0.25, None), ("50% of pool", 0.5, None)]


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
                    else:
                        candidates = [size for size in per_size if metric in per_size[size]]
                        chosen = min(candidates, key=lambda size: abs(size / pool - fraction),
                                     default=None)
                    if chosen is None or metric not in per_size.get(chosen, {}):
                        continue
                    points.append((x, summarize(per_size[chosen][metric])[0]))
                if not points:
                    continue
                points.sort(key=lambda point: point[0])
                style = SYSTEM_STYLE.get(system, {"color": "#444444", "marker": "^", "label": system})
                axes.plot([p[0] for p in points], [p[1] for p in points], color=style["color"], marker=style["marker"], markersize=4, linewidth=1.6, 
                          linestyle=SERIES_LINESTYLES[level_index % len(SERIES_LINESTYLES)], label=f"{style['label']}, {level_name}")
        style_axes(axes, title, x_key, ylabel)
        axes.set_ylim(-0.02, 1.05)

    handles, labels = axes_grid[0][0].get_legend_handles_labels()
    if handles:
        figure.legend(handles, labels, loc="lower center", ncol=min(3, len(labels)), fontsize=8, frameon=False, bbox_to_anchor=(0.5, -0.06))
    figure.suptitle(f"{experiment}: privacy against {x_key}", fontsize=12)
    figure.tight_layout(rect=(0, 0.05, 1, 0.95))
    return write_figure(figure, out_path, formats)


# how many nodes an attacker needs before a case can be reconstructed 
def min_nodes_figure(privacy, experiment, out_path, formats):
    values: dict[str, list[float]] = defaultdict(list)
    for (exp, _run_id, system), block in privacy.items():
        if exp != experiment:
            continue
        median = (block["local"].get("min_nodes_full_case") or {}).get("p50")
        if median is not None:
            values[system].append(median)
    if not values:
        return None
    figure, axes = plt.subplots(figsize=(4.4, 3.4))
    systems = sorted(values)
    medians = [summarize(values[system])[0] for system in systems]
    axes.bar(systems, medians, color=[SYSTEM_STYLE.get(s, {}).get("color", "#888") for s in systems],
             width=0.5)
    for index, value in enumerate(medians):
        axes.text(index, value, f"{value:.0f}", ha="center", va="bottom", fontsize=9)
    style_axes(axes, "nodes needed to reconstruct a case (median case)", "", "compromised nodes")
    figure.tight_layout()
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

    handles, labels = axes_grid[0][0].get_legend_handles_labels()
    if handles:
        figure.legend(handles, labels, loc="lower center", ncol=min(4, len(labels)), fontsize=9, frameon=False, bbox_to_anchor=(0.5, -0.01))
    figure.suptitle(title, fontsize=12)
    figure.tight_layout(rect=(0, 0.02, 1, 0.97))
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
    ("idle_bytes_per_node_s", "messages.derived.idle_bytes_per_node_second", 1.0),
    ("messages_per_event", "messages.derived.messages_per_event", 1.0),
    ("bytes_per_event", "messages.derived.bytes_per_event_excl_sink", 1.0),
    ("coord_bytes_per_event", "messages.derived.bytes_per_event_coordination", 1.0),
    ("transport_bytes_per_event", "messages.derived.bytes_per_event_transport", 1.0),
    ("messages_per_case", "messages.derived.messages_per_case", 1.0),
    ("coordination_ratio", "messages.derived.coordination_ratio", 1.0),
    ("pairs_multi_revision", "revisions.pairs_with_multiple", 1.0),
    ("revisions_total", "revisions.revisions_total", 1.0),
    ("margin_p50_us", "margin.margin_p50_us", 1.0),
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
            metric_names |= {key for key, value in entry.items() if isinstance(value, dict)}
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


# feasibility: correctness over all runs split into nominal, overloaded and incomplete runs
def run_regime(row: Row) -> str:
    if not row.value("quality.complete"):
        return "incomplete"
    return "overload" if row.value("throughput.saturated") else "nominal"


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
        for regime in ("nominal", "overload", "incomplete"):
            if by_regime.get(regime):
                line(writer, regime, "ALL", "ALL", by_regime[regime])
    return {regime: dict(values) for regime, values in by_regime.items()}


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
        return written

    distinct_x = len({row.params.get(x_key) for row in rows})
    logx = bool(x_key in ("rate", "nodes", "repeat", "cases") and distinct_x > 3)

    if idle_only:
        note("idle cost")
        figure = panel_figure(rows, x_key, IDLE_PANELS, series_key,
                              f"{experiment}: idle cost of the deployment", target / "idle", formats, logx)
        if figure:
            written.append(figure)
    else:
        for group_name, panels in PANEL_GROUPS:
            note(group_name)
            figure = panel_figure(workload_rows, x_key, panels, series_key, f"{experiment}: {group_name} against {x_key}", target / group_name, formats, logx)
            if figure:
                written.append(figure)

        note("message breakdown")
        figure = message_breakdown_figure(workload_rows, x_key, target / "message_breakdown", formats)
        if figure:
            written.append(figure)

        if any(row.value("resources.idle.nodes") for row in rows):
            note("idle cost")
            figure = panel_figure(rows, x_key, IDLE_PANELS, series_key,
                                  f"{experiment}: idle cost against {x_key}", target / "idle", formats, logx)
            if figure:
                written.append(figure)

    # check if compute_privacy.py has run for this experiment
    if any(key[0] == experiment for key in privacy):
        note("privacy curves")
        privacy_x = None if sensitivity else (x_key if distinct_x > 1 else None)
        figure = privacy_figure(privacy, experiment, privacy_x, target / "privacy", formats)
        if figure:
            written.append(figure)
        figure = privacy_figure(privacy, experiment, privacy_x, target / "privacy_by_fraction",
                                formats, by_fraction=True)
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
                          ("incomplete", "truncated records")):
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