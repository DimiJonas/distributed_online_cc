#!/usr/bin/env python3
"""
saturation_report.py - aggregate the saturation signals of an experiment per system

  python3.12 simulation/saturation_report.py results/e0_find_saturation
  python3.12 simulation/saturation_report.py results/e0_find_saturation --json out.json --csv out.csv
  python3.12 simulation/saturation_report.py results/e0_find_saturation --x-param rate --cpu-threshold 85
  python3.12 simulation/saturation_report.py results/E0c --x-param cpu_cap_cores --filter rate=16000

it reads per run and per system:

  run.json               validity, settlement, pairs vs expected, timeline windows
  send/<system>.stdout   pacer summary: effective rate, late, resyncs, blocked_sends, max_send
  send/<system>.log      achieved rate per wall clock second, inter send gap quantiles
  resources/<system>     cpu and rss per process, split into the system under test and the tooling
  records/<system>/      queue depths and message drops 
  compare_results.json   correctness of the final verdicts

and then applies a saturation rule to each (system, rate) point. the point saturates if any of:

  LOSS        any message drop            work was destroyed; revisions repair late messages, not lost ones
  BACKLOG     a queue is deep AND growing  p95 depth > 25% of capacity and the depth trend still rising
  COMPUTE     cpu > 85% of the ceiling     the ceiling is the per node cap, the shared system cap, or one core for the busiest thread when the run was uncapped
  THROUGHPUT  effective/target < 0.95      unless the median second still hit target, which means one stall
  STALL       a socket write blocked >= 1s a node stopped reading its socket altogether

the sustainable rate is reported as the highest rate below the first saturating one.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import re
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from helpers import (Progress, cpu_ceiling, saturation_signals,  # noqa: E402
                     DEFAULT_SATURATION_THRESHOLDS)

SYSTEMS = ("decentralized", "central")

# swept parameters where a bigger value means more resource, so saturation is indirectly proportional
RESOURCE_PARAMS = ("cpu_cap_cores", "cpu_cap_cpus", "nodes")

# sink should be excluded from cpu
INFRA_ROLES = ("sink",)

# sink queue depth should be excluded from system saturation
INFRA_QUEUES = ("result_sink_q",)

ALL_QUEUES = ("event_q", "loopback_q", "result_sink_q", "gossip_out_q", "gossip_in_q")

INPUT_DROP_RECORDS = ("message_drop", "dropped_event")
REPAIR_DROP_RECORDS = ("correction_dropped",)
REPAIR_RESULT_DROP_REASONS = ("result_already_final",)
DROP_RECORDS = INPUT_DROP_RECORDS + REPAIR_DROP_RECORDS + ("result_dropped",)

# the pacer summary line: "rate=8e+03/s (target) 7.75e+03/s (effective) elapsed=9.91s late=4417 max_lag=2.5ms resyncs=626"
RE_TARGET = re.compile(r"rate=([\d.e+-]+)/s \(target\)")
RE_EFFECTIVE = re.compile(r"([\d.e+-]+)/s \(effective\)")
RE_ELAPSED = re.compile(r"elapsed=([\d.]+)s")
RE_LATE = re.compile(r"late=(\d+)")
RE_MAXLAG = re.compile(r"max_lag=([\d.]+)ms")
RE_RESYNC = re.compile(r"resyncs=(\d+)")
RE_BLOCKED = re.compile(r"blocked_sends\(>1ms\)=(\d+)")
RE_MAXSEND = re.compile(r"max_send=([\d.]+)ms")
RE_SENT = re.compile(r"sent=(\d+)")


CORE_PARAMS = ("cpu_cap_cores", "cpu_cap_cpus")


def infer_x_direction(x_parameter: str) -> str:
    return "decreasing" if x_parameter in RESOURCE_PARAMS else "increasing"


def parse_filters(raw_filters: list[str]) -> list[tuple[str, list[str]]]:
    filters: list[tuple[str, list[str]]] = []
    for raw in raw_filters or []:
        if "=" not in raw:
            raise ValueError(f"--filter needs key=value, got {raw!r}")
        key, _, values = raw.partition("=")
        key = key.strip()
        wanted = [value.strip() for value in values.split(",") if value.strip()]
        if not key or not wanted:
            raise ValueError(f"--filter needs key=value, got {raw!r}")
        filters.append((key, wanted))
    return filters


def _values_match(actual, wanted: str) -> bool:
    if actual is None:
        return wanted.lower() in ("none", "null", "-")
    if isinstance(actual, bool):
        return str(actual).lower() == wanted.lower()
    try:
        return math.isclose(float(actual), float(wanted), rel_tol=1e-9, abs_tol=0.0)
    except (TypeError, ValueError):
        return str(actual).strip().lower() == wanted.lower()


def run_matches_filters(params: dict, filters: list[tuple[str, list[str]]]) -> bool:
    for key, wanted in filters:
        if key not in params:
            return False
        if not any(_values_match(params[key], value) for value in wanted):
            return False
    return True



def quantile(sorted_values: list[float], q: float) -> float | None:
    if not sorted_values:
        return None
    return sorted_values[min(len(sorted_values) - 1, int(q * (len(sorted_values) - 1)))]


# least squares slope of depth over time(in queue slots per second)
def depth_slope_per_s(samples: list[tuple[int, int]]) -> float | None:
    if len(samples) < 8:
        return None
    t0 = samples[0][0]
    xs = [(t_us - t0) / 1e6 for t_us, _ in samples]
    ys = [float(depth) for _, depth in samples]
    span = xs[-1] - xs[0]
    if span <= 0:
        return None
    n = len(xs)
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    variance = sum((x - mean_x) ** 2 for x in xs)
    if variance <= 0:
        return None
    covariance = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    return covariance / variance


# the slope expressed as a fraction of capacity gained over the whole measurement window
def growth_over_window(samples: list[tuple[int, int]], capacity: int) -> float | None:
    slope = depth_slope_per_s(samples)
    if slope is None or not capacity:
        return None
    span_s = (samples[-1][0] - samples[0][0]) / 1e6
    if span_s <= 0:
        return None
    return slope * span_s / capacity


def read_jsonl(path: Path):
    with open(path) as jsonl_file:
        for line in jsonl_file:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


# parse send/<system>.stdout.txt
# NOTE: max_lag is capped at roughly one interval because the pacer reschedules when the lag exceeds one interval
def parse_sender(run_dir: Path, system: str) -> dict:

    sender_stats: dict = {}
    path = run_dir / "send" / f"{system}.stdout.txt"
    if not path.exists():
        return sender_stats
    text = path.read_text()

    def get_by_regex(regex, cast=float):
        match = regex.search(text)
        return cast(match.group(1)) if match else None

    sender_stats["target_rate"] = get_by_regex(RE_TARGET)
    sender_stats["effective_rate"] = get_by_regex(RE_EFFECTIVE)
    sender_stats["elapsed_s"] = get_by_regex(RE_ELAPSED)
    sender_stats["late"] = get_by_regex(RE_LATE, int) or 0
    sender_stats["max_lag_ms"] = get_by_regex(RE_MAXLAG) or 0.0
    sender_stats["resyncs"] = get_by_regex(RE_RESYNC, int) or 0
    sender_stats["blocked_sends"] = get_by_regex(RE_BLOCKED, int)
    sender_stats["max_send_ms"] = get_by_regex(RE_MAXSEND)
    sender_stats["events_sent"] = get_by_regex(RE_SENT, int)
    if sender_stats.get("target_rate") and sender_stats.get("effective_rate"):
        sender_stats["achieved_ratio"] = sender_stats["effective_rate"] / sender_stats["target_rate"]
    if sender_stats.get("events_sent") and sender_stats.get("late") is not None and sender_stats["events_sent"]:
        sender_stats["late_frac"] = sender_stats["late"] / sender_stats["events_sent"]
    return sender_stats


# parse acieved rate and inter send gaps from send/<system>.log
def parse_send_log(run_dir: Path, system: str) -> dict:

    path = run_dir / "send" / f"{system}.log"
    if not path.exists():
        return {}
    times: list[float] = []
    send_us: list[int] = []
    per_second: dict[int, int] = defaultdict(int)
    for row in read_jsonl(path):
        time = row.get("t")
        if time is None:
            continue
        times.append(time)
        per_second[int(time)] += 1
        if row.get("send_us") is not None:
            send_us.append(row["send_us"])
    if len(times) < 2:
        return {}
    gaps = sorted((times[i + 1] - times[i]) * 1e6 for i in range(len(times) - 1))
    send_us.sort()
    # drop the last (partial) second so it does not look like a rate collapse
    buckets = sorted(per_second)[:-1]
    rates = [per_second[b] for b in buckets] or [0]
    rates_sorted = sorted(rates)
    span = times[-1] - times[0]
    return {
        "effective_rate_log": (len(times) / span) if span > 0 else None,
        "send_log_events": len(times),
        "send_log_span_s": span,
        "gap_p50_us": quantile(gaps, 0.50),
        "gap_p95_us": quantile(gaps, 0.95),
        "gap_p99_us": quantile(gaps, 0.99),
        "gap_max_us": gaps[-1],
        "send_us_p50": quantile(send_us, 0.50),
        "send_us_p99": quantile(send_us, 0.99),
        "send_us_max": send_us[-1] if send_us else None,
        "rate_per_s_min": min(rates),
        "rate_per_s_p50": quantile(rates_sorted, 0.50),
        "rate_per_s_max": max(rates),
    }


# parse /proc samples from resources/<system>.jsonl
def parse_resources(run_dir: Path, system: str, window: tuple[int, int] | None, infra_roles: tuple[str, ...] = INFRA_ROLES) -> dict:

    path = run_dir / "resources" / f"{system}.jsonl"
    if not path.exists():
        return {}
    host: list[dict] = []
    procs: dict[int, list[dict]] = defaultdict(list)
    meta: dict[int, dict] = {}
    thread_samples: dict[int, list[dict]] = defaultdict(list)
    for row in read_jsonl(path):
        if window and not (window[0] <= row.get("t_us", 0) <= window[1]):
            continue
        if row.get("kind") == "host":
            host.append(row)
        elif row.get("kind") == "proc":
            procs[row["pid"]].append(row)
            meta[row["pid"]] = {"node_id": row.get("node_id"), "role": row.get("role"),
                                "group": row.get("group", "system")}
        elif row.get("kind") == "threads":
            thread_samples[row["pid"]].append(row)
    if len(host) < 2:
        return {}
    clock_tick = host[0].get("clk_tck", 100)
    wallclock_time = (host[-1]["t_us"] - host[0]["t_us"]) / 1e6
    if wallclock_time <= 0:
        return {}
    total_ticks = host[-1]["cpu_total_ticks"] - host[0]["cpu_total_ticks"]
    idle_ticks = host[-1]["cpu_idle_ticks"] - host[0]["cpu_idle_ticks"]
    resource_stats: dict = {"host_busy_pct": 100 * (1 - idle_ticks / total_ticks) if total_ticks else None, "window_s": wallclock_time}

    per_group: dict[str, list[tuple[float, int, dict]]] = defaultdict(list)
    for pid, samples in procs.items():
        if len(samples) < 2:
            continue
        used = ((samples[-1]["utime_ticks"] + samples[-1]["stime_ticks"])
                - (samples[0]["utime_ticks"] + samples[0]["stime_ticks"]))
        cpu = 100 * (used / clock_tick) / wallclock_time
        rss = max(sample.get("rss_kb", 0) for sample in samples)
        group = meta[pid]["group"]
        if group == "system" and meta[pid]["role"] in infra_roles:
            group = "infra"
        per_group[group].append((cpu, rss, meta[pid]))

    system_procs = per_group.get("system", [])
    if system_procs:
        system_procs.sort(key=lambda entry: -entry[0])
        busiest = system_procs[0]
        resource_stats["sys_cpu_sum_pct"] = sum(cpu_pct for cpu_pct, _, _ in system_procs)
        resource_stats["sys_cpu_max_pct"] = busiest[0]
        resource_stats["sys_cpu_max_node"] = f"{busiest[2]['role']}/{busiest[2]['node_id']}"
        resource_stats["sys_rss_sum_mb"] = sum(rss_kb for _, rss_kb, _ in system_procs) / 1024
        resource_stats["sys_rss_max_mb"] = max(rss_kb for _, rss_kb, _ in system_procs) / 1024
        resource_stats["sys_processes"] = len(system_procs)
        resource_stats["sys_cpu_profile"] = [
            {"node": f"{meta_row['role']}/{meta_row['node_id']}", "cpu_pct": cpu_pct,
             "rss_mb": rss_kb / 1024}
            for cpu_pct, rss_kb, meta_row in system_procs]

    infra_procs = per_group.get("infra", [])
    if infra_procs:
        busiest_infra = max(infra_procs, key=lambda entry: entry[0])
        resource_stats["infra_cpu_sum_pct"] = sum(cpu_pct for cpu_pct, _, _ in infra_procs)
        resource_stats["infra_cpu_max_pct"] = busiest_infra[0]
        resource_stats["infra_cpu_max_node"] = f"{busiest_infra[2]['role']}/{busiest_infra[2]['node_id']}"
        resource_stats["infra_rss_sum_mb"] = sum(rss_kb for _, rss_kb, _ in infra_procs) / 1024

    thread_stats = _busiest_thread(thread_samples, meta, clock_tick, infra_roles)
    resource_stats.update(thread_stats)

    tooling = per_group.get("tooling", [])
    if tooling:
        sender = max(tooling, key=lambda entry: entry[0])
        resource_stats["sender_cpu_pct"] = sender[0]
        resource_stats["sender_rss_mb"] = sender[1] / 1024
    return resource_stats


# cpu of every thread, reduced to the busiest one across the system processes
def _busiest_thread(thread_samples: dict[int, list[dict]], meta: dict[int, dict], clock_tick: int, infra_roles: tuple[str, ...]) -> dict:

    best = None # (sustained_pct, label, thread_name)
    peak_samples: list[float] = []
    peak_thread_count = 0
    for pid, samples in thread_samples.items():
        if len(samples) < 2:
            continue
        info = meta.get(pid) or {}
        if info.get("group", "system") != "system" or info.get("role") in infra_roles:
            continue
        peak_thread_count = max(peak_thread_count, max(sample.get("thread_count", 0) for sample in samples))
        label = f"{info.get('role')}/{info.get('node_id')}"

        first_sample: dict[int, tuple[int, int, str]] = {}
        last_sample: dict[int, tuple[int, int, str]] = {}
        for sample in samples:
            for thread_id, name, ticks in sample.get("threads", []):
                if thread_id not in first_sample:
                    first_sample[thread_id] = (sample["t_us"], ticks, name)
                last_sample[thread_id] = (sample["t_us"], ticks, name)
        for thread_id, (t0, ticks0, name) in first_sample.items():
            t1, ticks1, _ = last_sample[thread_id]
            span = (t1 - t0) / 1e6
            if span <= 0:
                continue
            pct = 100 * ((ticks1 - ticks0) / clock_tick) / span
            if best is None or pct > best[0]:
                best = (pct, label, name)

        # per interval values to report a peak
        for earlier, later in zip(samples, samples[1:]):
            span = (later["t_us"] - earlier["t_us"]) / 1e6
            if span <= 0:
                continue
            before = {thread_id: ticks for thread_id, _, ticks in earlier.get("threads", [])}
            interval_max = 0.0
            for thread_id, _, ticks in later.get("threads", []):
                if thread_id in before:
                    interval_max = max(interval_max, 100 * ((ticks - before[thread_id]) / clock_tick) / span)
            peak_samples.append(interval_max)

    if best is None:
        return {}
    peak_samples.sort()
    return {"sys_thread_max_pct": best[0], "sys_thread_max_node": best[1],
            "sys_thread_max_name": best[2],
            "sys_thread_peak_p95": quantile(peak_samples, 0.95),
            "sys_thread_peak_count": peak_thread_count}


# parse written records (queue depths, message drops) from records/<system>/node-*.jsonl
def parse_records(run_dir: Path, system: str, window: tuple[int, int] | None, roles_by_node: dict[int, str] | None = None, 
                  infra_roles: tuple[str, ...] = INFRA_ROLES, infra_queues: tuple[str, ...] = INFRA_QUEUES) -> dict:

    records_dir = run_dir / "records" / system
    record_files = sorted(records_dir.glob("node-*.jsonl")) if records_dir.exists() else []
    if not record_files:
        return {"records_present": False}
    roles_by_node = roles_by_node or {}

    # (node_id, queue) -> peak depth / capacity
    peak: dict[tuple[int, str], int] = {}
    capacity: dict[tuple[int, str], int] = {}
    # (node_id, queue) -> [(t_us, depth), ...] so a standing backlog can be told apart from a spike
    series: dict[tuple[int, str], list[tuple[int, int]]] = defaultdict(list)
    drops: dict[tuple[str, str], int] = defaultdict(int)
    drops_by_node: dict[str, int] = defaultdict(int)
    incomplete: list[str] = []
    dequeue_depths: list[int] = []

    for path in record_files:
        try:
            node_id = int(path.stem.split("-")[-1])
        except ValueError:
            node_id = -1
        marker_seen = False
        for row in read_jsonl(path):
            if row.get("type") == "writer_shutdown":
                marker_seen = True
            wallclock_time = row.get("timestamp_wallclock_us")
            if window and wallclock_time is not None and not (window[0] <= wallclock_time <= window[1]):
                continue
            kind = row.get("type")
            if kind == "queues":
                for queue in ALL_QUEUES:
                    value = row.get(queue)
                    if not value:
                        continue
                    # [current fill, max since last report, max capacity]
                    key = (node_id, queue)
                    if value[1] > peak.get(key, -1):
                        peak[key] = value[1]
                    capacity[key] = value[2]
                    if wallclock_time is not None:
                        series[key].append((wallclock_time, value[1]))
            elif kind == "event_dequeue":
                depth = row.get("q_length")
                if depth is not None:
                    dequeue_depths.append(depth)
            elif kind in DROP_RECORDS:
                drops[(kind, str(row.get("reason") or "unknown"))] += 1
                if kind in INPUT_DROP_RECORDS:
                    drops_by_node[f"node-{node_id}"] += 1
        if not marker_seen:
            incomplete.append(path.name)

    def _is_repair(kind: str, reason: str) -> bool:
        if kind in REPAIR_DROP_RECORDS:
            return True
        return kind == "result_dropped" and reason in REPAIR_RESULT_DROP_REASONS

    def _count(predicate) -> int:
        return sum(count for (kind, reason), count in drops.items() if predicate(kind, reason))

    def _reasons(predicate) -> dict:
        return {f"{kind}/{reason}": count for (kind, reason), count in sorted(drops.items())
                if predicate(kind, reason)}

    records_stats: dict = {
        "records_present": True, "record_files": len(record_files),
        "incomplete_record_files": incomplete,
        # unrecoverable input loss - the saturation signal
        "input_drops": _count(lambda kind, _r: kind in INPUT_DROP_RECORDS),
        "input_drop_reasons": _reasons(lambda kind, _r: kind in INPUT_DROP_RECORDS),
        "message_drops": _count(lambda kind, _r: kind == "message_drop"),
        "event_drops": _count(lambda kind, _r: kind == "dropped_event"),
        # repairs that never landed - explains wrong verdicts, not a capacity signal
        "repair_drops": _count(_is_repair),
        "repair_drop_reasons": _reasons(_is_repair),
        # the revision store discarding an older revision - normal, context only
        "stale_result_drops": _count(lambda kind, reason: kind == "result_dropped"
                                     and reason not in REPAIR_RESULT_DROP_REASONS),
        "drop_reasons": _reasons(lambda _k, _r: True),
        "input_drops_by_node": dict(sorted(drops_by_node.items(), key=lambda kv: -kv[1])),
    }

    # worst fill per queue across nodes and the overall worst that counts as a saturation signal
    signal_fill = 0.0
    signal_queue = None
    signal_node = None
    for queue in ALL_QUEUES:
        entries = [(node_id, peak[(node_id, queue)], capacity[(node_id, queue)])
                   for (node_id, q) in peak if q == queue]
        if not entries:
            continue
        fills = [(depth / cap if cap else 0.0, node_id, depth, cap) for node_id, depth, cap in entries]
        worst = max(fills)
        records_stats[f"{queue}_peak"] = worst[2]
        records_stats[f"{queue}_capacity"] = worst[3]
        records_stats[f"{queue}_fill"] = worst[0]
        records_stats[f"{queue}_worst_node"] = f"node-{worst[1]}"

        worst_key = (worst[1], queue)
        samples = sorted(series.get(worst_key, []))
        capacity_of_worst = worst[3] or 0
        depths = sorted(depth for _t, depth in samples)
        p95_depth = quantile(depths, 0.95)
        p99_depth = quantile(depths, 0.99)
        slope = depth_slope_per_s(samples)
        records_stats[f"{queue}_fill_p95"] = (p95_depth / capacity_of_worst) if (p95_depth is not None and capacity_of_worst) else None
        records_stats[f"{queue}_fill_p99"] = (p99_depth / capacity_of_worst) if (p99_depth is not None and capacity_of_worst) else None
        records_stats[f"{queue}_slope_per_s"] = slope
        records_stats[f"{queue}_growth_over_window"] = growth_over_window(samples, capacity_of_worst)
        records_stats[f"{queue}_samples"] = len(samples)

        # infra queues and infra nodes are measured but never used as a saturation signal
        if queue in infra_queues:
            continue
        signal_fills = [entry for entry in fills
                        if roles_by_node.get(entry[1]) not in infra_roles]
        if not signal_fills:
            continue
        worst_signal = max(signal_fills)
        if worst_signal[0] > signal_fill:
            signal_fill, signal_queue, signal_node = worst_signal[0], queue, f"node-{worst_signal[1]}"
    records_stats["queue_fill_max"] = signal_fill
    records_stats["queue_fill_max_queue"] = signal_queue
    records_stats["queue_fill_max_node"] = signal_node

    # every (node, queue) with samples
    queue_profile: list[dict] = []
    standing_best = (-1.0, None, None, None, None, None)
    for queue in ALL_QUEUES:
        if queue in infra_queues:
            continue
        entries = [(node_id, peak[(node_id, queue)], capacity[(node_id, queue)])
                   for (node_id, q) in peak if q == queue and roles_by_node.get(node_id) not in infra_roles]
        if not entries:
            continue
        for node_id, _depth, cap in entries:
            samples = sorted(series.get((node_id, queue), []))
            if not samples or not cap:
                continue
            depths = sorted(depth for _t, depth in samples)
            p95_fill = quantile(depths, 0.95) / cap
            queue_profile.append({
                "node": f"node-{node_id}", "queue": queue, "capacity": cap,
                "p95_fill": p95_fill, "p99_fill": quantile(depths, 0.99) / cap,
                "peak_fill": max(depths) / cap,
                "slope_per_s": depth_slope_per_s(samples),
                "growth": growth_over_window(samples, cap)})
            if p95_fill > standing_best[0]:
                standing_best = (p95_fill, queue, f"node-{node_id}",
                                 depth_slope_per_s(samples), quantile(depths, 0.99) / cap,
                                 growth_over_window(samples, cap))
    records_stats["queue_profile"] = sorted(queue_profile, key=lambda entry: -entry["p95_fill"])
    if standing_best[1]:
        records_stats["queue_fill_p95_max"] = standing_best[0]
        records_stats["queue_fill_p95_max_queue"] = standing_best[1]
        records_stats["queue_fill_p95_max_node"] = standing_best[2]
        records_stats["queue_slope_at_p95_max"] = standing_best[3]
        records_stats["queue_fill_p99_at_p95_max"] = standing_best[4]
        records_stats["queue_growth_at_p95_max"] = standing_best[5]
    records_stats["queue_signal_excludes"] = {"roles": list(infra_roles), "queues": list(infra_queues)}

    infra_fills = [(records_stats.get(f"{queue}_fill") or 0.0, queue) for queue in infra_queues
                   if f"{queue}_fill" in records_stats]
    if infra_fills:
        worst_infra = max(infra_fills)
        records_stats["infra_queue_fill_max"] = worst_infra[0]
        records_stats["infra_queue_fill_max_queue"] = worst_infra[1]
        records_stats["infra_queue_fill_max_node"] = records_stats.get(f"{worst_infra[1]}_worst_node")

    if dequeue_depths:
        dequeue_depths.sort()
        records_stats["dequeue_events"] = len(dequeue_depths)
        records_stats["dequeue_depth_p50"] = quantile(dequeue_depths, 0.50)
        records_stats["dequeue_depth_p95"] = quantile(dequeue_depths, 0.95)
        records_stats["dequeue_depth_p99"] = quantile(dequeue_depths, 0.99)
        records_stats["dequeue_depth_max"] = dequeue_depths[-1]
        records_stats["dequeue_backlog_frac"] = sum(1 for d in dequeue_depths if d > 0) / len(dequeue_depths)
    return records_stats


# collect stats for a specific systems run
METRICS_SCHEMA_MIN = 4


def _infra_split(per_node: dict, infra_roles: tuple[str, ...]) -> tuple[list, list]:
    system_nodes, infra_nodes = [], []
    for node_id, values in (per_node or {}).items():
        entry = {"node": f"{values.get('role')}/{node_id}",
                 "cpu_pct": values.get("cpu_percent") or 0.0,
                 "rss_mb": (values.get("rss_kb_max") or 0) / 1024}
        (infra_nodes if values.get("role") in infra_roles else system_nodes).append(entry)
    system_nodes.sort(key=lambda entry: -entry["cpu_pct"])
    infra_nodes.sort(key=lambda entry: -entry["cpu_pct"])
    return system_nodes, infra_nodes


def _queue_stats_from_metrics(system_metrics: dict, infra_roles, infra_queues) -> dict:
    throughput = system_metrics.get("throughput") or {}
    node_roles = ((system_metrics.get("quality") or {}).get("node_roles")) or {}
    profile = []
    for entry in throughput.get("queues_by_node") or []:
        role = node_roles.get(str(entry.get("node_id")))
        if entry.get("queue") in infra_queues or role in infra_roles:
            continue
        profile.append({"node": f"node-{entry.get('node_id')}", "queue": entry.get("queue"),
                        "capacity": entry.get("capacity"),
                        "p95_fill": entry.get("p95_fill"), "p99_fill": entry.get("p99_fill"),
                        "peak_fill": entry.get("peak_fill"),
                        "slope_per_s": entry.get("slope_per_s"),
                        "growth": entry.get("growth_over_window")})
    profile.sort(key=lambda entry: -(entry["p95_fill"] or 0))

    out: dict = {"records_present": True, "queue_profile": profile}
    if profile:
        worst = profile[0]
        out.update({"queue_fill_p95_max": worst["p95_fill"],
                    "queue_fill_p95_max_queue": worst["queue"],
                    "queue_fill_p95_max_node": worst["node"],
                    "queue_slope_at_p95_max": worst["slope_per_s"],
                    "queue_fill_p99_at_p95_max": worst["p99_fill"],
                    "queue_growth_at_p95_max": worst["growth"]})
        peak = max(profile, key=lambda entry: entry["peak_fill"] or 0)
        out.update({"queue_fill_max": peak["peak_fill"], "queue_fill_max_queue": peak["queue"],
                    "queue_fill_max_node": peak["node"]})

    # the sink queue is evaluation scaffolding: measured, warned about, never a saturation signal
    infra = [entry for entry in (throughput.get("queues_by_node") or [])
             if entry.get("queue") in infra_queues
             or node_roles.get(str(entry.get("node_id"))) in infra_roles]
    if infra:
        worst_infra = max(infra, key=lambda entry: entry.get("peak_fill") or 0)
        out.update({"infra_queue_fill_max": worst_infra.get("peak_fill"),
                    "infra_queue_fill_max_queue": worst_infra.get("queue"),
                    "infra_queue_fill_max_node": f"node-{worst_infra.get('node_id')}"})

    dequeue = throughput.get("event_q_at_dequeue") or {}
    out["dequeue_backlog_frac"] = dequeue.get("backlog_fraction")
    out["dequeue_depth_p95"] = dequeue.get("p95")
    out["dequeue_depth_p99"] = dequeue.get("p99")
    for entry in throughput.get("queues_by_node") or []:
        if entry.get("queue") == "event_q":
            out.setdefault("event_q_capacity", entry.get("capacity"))
    return out


def _drop_stats_from_metrics(system_metrics: dict) -> dict:
    throughput = system_metrics.get("throughput") or {}
    revisions = system_metrics.get("revisions") or {}
    message_drops = throughput.get("message_drops") or {}
    event_drops = throughput.get("event_drops") or {}
    correction_drops = revisions.get("correction_drops") or {}
    result_drops = revisions.get("result_drops") or {}

    input_reasons = {}
    for kind, bucket in (("message_drop", message_drops), ("dropped_event", event_drops)):
        for reason, count in (bucket.get("by_reason") or {}).items():
            input_reasons[f"{kind}/{reason}"] = count
    by_node: dict[str, int] = {}
    for bucket in (message_drops, event_drops):
        for node, count in (bucket.get("by_node") or {}).items():
            by_node[node] = by_node.get(node, 0) + count

    repair_reasons = {f"correction_dropped/{reason}": count
                      for reason, count in (correction_drops.get("by_reason") or {}).items()}
    stale = 0
    for reason, count in (result_drops.get("by_reason") or {}).items():
        if str(reason) in ("result_already_final", "ResultAlreadyFinal"):
            repair_reasons[f"result_dropped/{reason}"] = count
        else:
            stale += count

    return {
        "message_drops": message_drops.get("total") or 0,
        "event_drops": event_drops.get("total") or 0,
        "input_drops": (message_drops.get("total") or 0) + (event_drops.get("total") or 0),
        "input_drop_reasons": input_reasons,
        "input_drops_by_node": dict(sorted(by_node.items(), key=lambda item: -item[1])),
        "repair_drops": sum(repair_reasons.values()),
        "repair_drop_reasons": repair_reasons,
        "stale_result_drops": stale,
        "drop_reasons": {**input_reasons, **repair_reasons},
    }


def collect_point_from_metrics(run_dir: Path, system: str, x_param: str,
                               infra_roles: tuple[str, ...] = INFRA_ROLES,
                               infra_queues: tuple[str, ...] = INFRA_QUEUES) -> dict | None:
    metrics_path = run_dir / "metrics.json"
    if not metrics_path.exists():
        return None
    try:
        metrics = json.loads(metrics_path.read_text())
    except json.JSONDecodeError:
        return None
    if (metrics.get("schema_version") or 0) < METRICS_SCHEMA_MIN:
        return None
    system_metrics = (metrics.get("systems") or {}).get(system)
    if system_metrics is None:
        return None

    params = metrics.get("params") or {}
    quality = system_metrics.get("quality") or {}
    throughput = system_metrics.get("throughput") or {}
    resources = system_metrics.get("resources") or {}
    windows = system_metrics.get("windows") or {}
    revisions = system_metrics.get("revisions") or {}

    point: dict = {
        "run_id": metrics.get("run_id"),
        "run_dir": str(run_dir),
        "system": system,
        "x_param": x_param,
        "x": params.get(x_param),
        "status": metrics.get("status"),
        "order_index": metrics.get("order_index"),
        "seed": params.get("seed"),
        "replicate": metrics.get("replicate"),
        "msgtrace": params.get("msgtrace", True),
        "metrics": params.get("metrics", True),
        "nodes": params.get("nodes"),
        "hard_killed": len(quality.get("hard_killed_nodes") or []),
        "window_source": windows.get("load_source", "none"),
        "sender_preload_s": quality.get("sender_preload_s"),
        "sender_teardown_s": quality.get("sender_teardown_s"),
        "settlement_reason": quality.get("settlement_reason"),
        "settlement_s": quality.get("settlement_seconds"),
        "seconds_to_all_expected": quality.get("seconds_to_all_expected"),
        "pairs_in_db": quality.get("pairs_in_db"),
        "expected_pairs": quality.get("expected_pairs"),
        "incomplete_record_files": quality.get("writer_shutdown_missing") or [],
        "run_problems": [problem for problem in (list(metrics.get("problems") or [])
                                                 + list(quality.get("problems") or []))
                         if not _problem_is_other_system(problem, system)],
        "point_source": "metrics.json",
    }

    # sender: rates, pacing and the socket writes
    point["events_sent"] = throughput.get("events_sent")
    point["elapsed_s"] = windows.get("load_seconds")
    point["late"] = throughput.get("pacer_late") or 0
    point["max_lag_ms"] = throughput.get("pacer_max_lag_ms") or 0.0
    point["resyncs"] = throughput.get("pacer_resyncs") or 0
    point["blocked_sends"] = throughput.get("blocked_sends_over_1ms")
    max_send_us = throughput.get("max_send_us")
    point["max_send_ms"] = (max_send_us / 1000.0) if max_send_us is not None else None
    point["send_log_spills"] = throughput.get("send_log_spills")
    point["send_log_max_spill_us"] = throughput.get("send_log_max_spill_us")
    point["pacer_burst"] = throughput.get("pacer_burst")
    point["pacer_burst_seconds"] = throughput.get("pacer_burst_seconds")
    if point["events_sent"] and point["late"] is not None:
        point["late_frac"] = point["late"] / point["events_sent"]

    target = params.get("rate")
    source = "target: metrics.json params.rate"
    if target is None:
        target = throughput.get("offered_rate")
        source = "target: pacer summary (rounded)"
    effective = throughput.get("effective_rate")
    point["target_rate"] = float(target) if target is not None else None
    point["effective_rate"] = effective
    point["achieved_ratio"] = (effective / target) if (effective and target) else None
    point["sustained_ratio"] = None
    point["rate_source"] = f"{source}; effective: pacer summary"
    point["rate_source_exact"] = "rounded" not in source

    # resources
    system_nodes, infra_nodes = _infra_split(resources.get("per_node") or {}, infra_roles)
    if system_nodes:
        point["sys_cpu_profile"] = system_nodes
        point["sys_cpu_sum_pct"] = sum(entry["cpu_pct"] for entry in system_nodes)
        point["sys_cpu_max_pct"] = system_nodes[0]["cpu_pct"]
        point["sys_cpu_max_node"] = system_nodes[0]["node"]
        point["sys_rss_sum_mb"] = sum(entry["rss_mb"] for entry in system_nodes)
        point["sys_rss_max_mb"] = max(entry["rss_mb"] for entry in system_nodes)
        point["sys_processes"] = len(system_nodes)
    if infra_nodes:
        point["infra_cpu_sum_pct"] = sum(entry["cpu_pct"] for entry in infra_nodes)
        point["infra_cpu_max_pct"] = infra_nodes[0]["cpu_pct"]
        point["infra_cpu_max_node"] = infra_nodes[0]["node"]
        point["infra_rss_sum_mb"] = sum(entry["rss_mb"] for entry in infra_nodes)
    host_busy = resources.get("host_cpu_busy_fraction")
    point["host_busy_pct"] = (host_busy * 100.0) if host_busy is not None else None
    tooling = (resources.get("non_system") or {}).get("tooling") or {}
    point["sender_cpu_pct"] = tooling.get("cpu_percent")
    point["sender_rss_mb"] = (tooling.get("rss_kb_max") or 0) / 1024 or None

    point.update(_queue_stats_from_metrics(system_metrics, infra_roles, infra_queues))
    point.update(_drop_stats_from_metrics(system_metrics))
    point.update(cpu_ceiling(system_metrics.get("cpu_cap") or {}, point))

    # correctness of the final verdicts
    correctness = metrics.get("correctness") or {}
    per_system = (correctness.get("per_system") or {}).get(system) or {}
    for key in ("accuracy", "correct", "wrong", "absent", "spurious"):
        if key in per_system:
            point[key] = per_system[key]
    point["revisions_total"] = revisions.get("revisions_total")
    point["pairs_with_multiple_revisions"] = revisions.get("pairs_with_multiple")
    point["max_revisions_on_a_pair"] = revisions.get("max_on_a_pair")
    return point


def collect_point(run_dir: Path, system: str, x_param: str, infra_roles: tuple[str, ...] = INFRA_ROLES, infra_queues: tuple[str, ...] = INFRA_QUEUES) -> dict | None:
    run_file_path = run_dir / "run.json"
    if not run_file_path.exists():
        return None
    try:
        run_file = json.loads(run_file_path.read_text())
    except json.JSONDecodeError:
        return None
    # filter timeline by system
    timeline = next((timeline_entry for timeline_entry in run_file.get("systems_timeline", []) if timeline_entry.get("system") == system), None)
    if timeline is None:
        return None

    window = None
    window_source = "none"
    if timeline.get("pacer_start_us") and timeline.get("pacer_end_us"):
        window = (timeline["pacer_start_us"], timeline["pacer_end_us"])
        window_source = "pacer"
    elif timeline.get("send_start_us") and timeline.get("send_end_us"):
        window = (timeline["send_start_us"], timeline["send_end_us"])
        window_source = "process"

    point: dict = {
        "run_id": run_file.get("run_id"),
        "run_dir": str(run_dir),
        "system": system,
        "x_param": x_param,
        "x": run_file.get("params", {}).get(x_param),
        "status": run_file.get("status"),
        "order_index": run_file.get("order_index"),
        "seed": run_file.get("params", {}).get("seed"),
        "replicate": run_file.get("replicate"),
        "msgtrace": run_file.get("params", {}).get("msgtrace", True),
        "metrics": run_file.get("params", {}).get("metrics", True),
        "nodes": run_file.get("params", {}).get("nodes"),
        "hard_killed": len(timeline.get("hard_killed_nodes") or []),
        "window_source": window_source,
        "sender_preload_s": timeline.get("sender_preload_s"),
        "sender_teardown_s": timeline.get("sender_teardown_s"),
        "settlement_reason": timeline.get("system_settlement_reason"),
        "settlement_s": timeline.get("system_settlement_seconds"),
        "seconds_to_all_expected": timeline.get("seconds_to_all_expected"),
        "pairs_in_db": timeline.get("pairs_in_db"),
        "expected_pairs": timeline.get("expected_pairs"),
        "run_problems": [problem for problem in (list(run_file.get("problems") or [])
                                                 + list(timeline.get("problems") or []))
                         if not _problem_is_other_system(problem, system)],
        "point_source": "raw run files",
    }
    send_summary_path = run_dir / "send" / f"{system}.summary.json"
    if send_summary_path.exists():
        try:
            send_summary = json.loads(send_summary_path.read_text())
            point["send_log_spills"] = (send_summary.get("send_log") or {}).get("spills")
            point["send_log_max_spill_us"] = (send_summary.get("send_log") or {}).get("max_spill_us")
            point["pacer_burst"] = (send_summary.get("pacer") or {}).get("burst")
            point["pacer_burst_seconds"] = (send_summary.get("pacer") or {}).get("burst_seconds")
        except json.JSONDecodeError:
            pass
    point.update(parse_sender(run_dir, system))
    point.update(parse_send_log(run_dir, system))
    point.update(parse_resources(run_dir, system, window, infra_roles))
    roles_by_node = {node["node_id"]: node.get("role") for node in (timeline.get("nodes") or []) if node.get("node_id") is not None}
    point.update(parse_records(run_dir, system, window, roles_by_node, infra_roles, infra_queues))
    point.update(cpu_ceiling(timeline.get("cpu_cap") or {}, point))

    sources = []
    target = run_file.get("params", {}).get("rate")
    if target is not None:
        sources.append("target: run.json params.rate")
    else:
        target = point.get("target_rate")
        sources.append("target: pacer summary line (rounded)")
    effective = point.get("effective_rate_log")
    if effective is not None:
        sources.append("effective: send log")
    else:
        effective = point.get("effective_rate")
        sources.append("effective: pacer summary line (rounded)")
    point["target_rate"] = float(target) if target is not None else None
    point["effective_rate"] = effective
    point["achieved_ratio"] = (effective / target) if (effective and target) else None

    per_second_median = point.get("rate_per_s_p50")
    point["sustained_ratio"] = (per_second_median / target) if (per_second_median and target) else None
    sent = point.get("events_sent")
    elapsed = point.get("send_log_span_s") or point.get("elapsed_s")
    point["excess_over_ideal_s"] = (elapsed - sent / target) if (sent and target and elapsed) else None
    # achieved_ratio = (1 - excess/elapsed) by construction, so a single fixed stall anywhere in the run makes it fall as the rate rises simply because the run gets shorter
    # subtract the largest inter send gap: what is left is the deficit the system actually caused, and it is comparable across rates because it is expressed as a fraction of the run
    largest_gap_s = (point.get("gap_max_us") or 0) / 1e6
    excess = point.get("excess_over_ideal_s")
    if excess is not None and elapsed:
        point["residual_deficit_s"] = excess - largest_gap_s
        point["residual_deficit_frac"] = point["residual_deficit_s"] / elapsed
        point["largest_gap_s"] = largest_gap_s
        point["largest_gap_frac_of_deficit"] = (largest_gap_s / excess) if excess > 0 else None
    point["rate_source"] = "; ".join(sources)
    point["rate_source_exact"] = "rounded" not in point["rate_source"]

    # correctness of the final verdicts, from compare_results.json
    summary_path = run_dir / "compare_results.json"
    if summary_path.exists():
        try:
            summary = json.loads(summary_path.read_text())
            per_system = (summary.get("per_system") or {}).get(system) or {}
            for key in ("accuracy", "correct", "wrong", "absent", "spurious",
                        "revisions_total", "pairs_with_multiple_revisions", "max_revisions_on_a_pair"):
                if key in per_system:
                    point[key] = per_system[key]
        except json.JSONDecodeError:
            pass
    return point


# decide if the point shows saturation
SYSTEMS_FOR_PROBLEM_PREFIX = ("decentralized", "central")


def _problem_is_other_system(problem, system: str) -> bool:
    text = str(problem)
    for other in SYSTEMS_FOR_PROBLEM_PREFIX:
        if other != system and text.startswith(f"{other}:"):
            return True
    return False


def classify(point: dict, thresholds: dict) -> dict:

    reasons: list[str] = []
    warnings: list[str] = []
    notes: list[str] = []
    signals: list[dict] = []

    decision = saturation_signals(point, thresholds)
    reasons.extend(decision["reasons"])
    signals.extend(decision["signals"])
    cpu_fired, queue_reason_fired = decision["cpu_fired"], decision["queue_fired"]

    if point.get("repair_drops"):
        detail = ", ".join(f"{count}x {reason}" for reason, count
                           in (point.get("repair_drop_reasons") or {}).items())
        warnings.append(f"{point['repair_drops']} repair(s) never landed ({detail}) - a later, better "
                        f"verdict arrived and the revision mechanism could not apply it, either because "
                        f"the correction could not be matched or because the revocable window had "
                        f"already closed. not a capacity signal, but it is how a saturated system loses "
                        f"correctness - read it next to the wrong count")

    standing_fill = point.get("queue_fill_p95_max")
    if (standing_fill is not None and standing_fill > thresholds["queue_fill"]
            and not queue_reason_fired):
        warnings.append(f"{point.get('queue_fill_p95_max_queue')} on "
                        f"{point.get('queue_fill_p95_max_node')}: p95 depth {standing_fill:.0%} of "
                        f"capacity but the trend only rises by "
                        f"{fmt(point.get('queue_growth_at_p95_max'), '.1%')} of capacity across the "
                        f"window ({fmt(point.get('queue_slope_at_p95_max'), '+.1f')} slots/s) - a "
                        f"deep but STABLE queue is a buffer absorbing bursts, not saturation")

    capacity = point.get("event_q_capacity")
    depth_p95, depth_p99 = point.get("dequeue_depth_p95"), point.get("dequeue_depth_p99")
    if capacity:
        point["event_q_p95_fill"] = (depth_p95 / capacity) if depth_p95 is not None else None
        point["event_q_p99_fill"] = (depth_p99 / capacity) if depth_p99 is not None else None

    ratio = point.get("achieved_ratio")

    if point.get("cpu_cap_note"):
        warnings.append(f"cpu cap: {point['cpu_cap_note']}")

    # which nodes are hot
    hot = [entry for entry in (point.get("sys_cpu_profile") or [])
           if entry["cpu_pct"] >= thresholds["hot_cpu_pct"]]
    already_listed = cpu_fired and point.get("cpu_ceiling_basis") == "process"
    if hot and not already_listed:
        if thresholds.get("only_max_node"):
            notes.append(f"{len(hot)} node(s) above {thresholds['hot_cpu_pct']:.0f}% of a core, "
                         f"busiest {hot[0]['node']} at {hot[0]['cpu_pct']:.0f}% "
                         f"[--only-max-node hides the rest]")
        else:
            notes.append(f"{len(hot)} node(s) above {thresholds['hot_cpu_pct']:.0f}% of a core:")
            for entry in hot:
                of_cap_text = (f", {entry['cpu_pct'] / point['cpu_ceiling_pct']:.0%} of its cap"
                               if point.get("cpu_ceiling_pct") else "")
                notes.append(f"     {entry['node']}: {entry['cpu_pct']:.0f}% of a core"
                             f"{of_cap_text}, {entry['rss_mb']:.0f} MB")

    sustained = point.get("sustained_ratio")
    if ratio is not None and ratio < thresholds["achieved_ratio"] and not cpu_fired and not queue_reason_fired:
        warnings.append(f"effective/target={ratio:.3f} < {thresholds['achieved_ratio']} but nothing "
                        f"else fired: no cpu exhaustion, no backlog, no input loss"
                        + (f" (median second at {sustained:.3f} of target)" if sustained is not None else "")
                        + " - the load generator, a gc pause or a scheduler hiccup, NOT saturation")

    wrong = point.get("wrong")
    if wrong:
        warnings.append(f"{wrong} wrong final verdict(s) (accuracy {fmt(point.get('accuracy'), '.6f')}) "
                        f"- not a saturation signal by itself, but check it against the signals above")

    invalid: list[str] = []
    if point.get("hard_killed"):
        invalid.append(f"{point['hard_killed']} node(s) SIGKILLed")
    if point.get("expected_pairs") and (point.get("pairs_in_db") or 0) < point["expected_pairs"]:
        invalid.append(f"{point.get('pairs_in_db')}/{point['expected_pairs']} pairs at settlement")
    if point.get("incomplete_record_files"):
        invalid.append(f"{len(point['incomplete_record_files'])} truncated record file(s)")
    if point.get("status") == "invalid":
        invalid.append("run.json status=invalid")

    if point.get("window_source") == "process":
        warnings.append("no pacer window in run.json, so cpu was measured over the sender PROCESS "
                        "bounds, which also cover the payload preload and the send log flush while "
                        "the nodes are idle - every cpu percentage here is diluted by an unknown "
                        "factor. re-run with the current send_event_stream.py before comparing cpu")
    if point.get("send_log_spills"):
        warnings.append(f"the sender spilled its send log {point['send_log_spills']}x mid stream - that "
                        f"stalls the pacing loop and shows up as a fake throughput dip")

    sender_cpu = point.get("sender_cpu_pct")
    tooling_limited: list[str] = []
    if sender_cpu is not None and sender_cpu > thresholds["sender_cpu_pct"]:
        warnings.append(f"SENDER at {sender_cpu:.0f}% of a core - read the throughput numbers here "
                        f"with care, the load generator may be the limit")
    elif sender_cpu is None and point.get("target_rate"):
        warnings.append("sender cpu not sampled (old run) - cannot rule out a load generator limit")

    infra_fill = point.get("infra_queue_fill_max")
    if infra_fill is not None and infra_fill >= thresholds["infra_queue_fill_warn"]:
        warnings.append(f"{point.get('infra_queue_fill_max_queue')} on "
                        f"{point.get('infra_queue_fill_max_node')} reached {infra_fill:.0%} of capacity "
                        f"- evaluation scaffolding, excluded from the rule, but it can backpressure "
                        f"into the system and confound everything above it")

    return {"saturated": bool(reasons), "reasons": reasons, "invalid": invalid,
            "warnings": warnings, "notes": notes, "signals": signals,
            "tooling_limited": tooling_limited}


def fmt(value, spec="", dash="-"):
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return dash
    try:
        return format(value, spec)
    except (TypeError, ValueError):
        return str(value)


ROLE_INITIALS = {"constraint_node": "n", "central": "C", "forwarder": "f"}


def _short_node(name) -> str:
    if not name:
        return "-"
    text = str(name)
    if "/" in text:
        role, _, node_id = text.partition("/")
        if role.startswith("sink"):
            return "sink"
        for prefix, initial in ROLE_INITIALS.items():
            if role.startswith(prefix):
                return f"{initial}{node_id}"
        return f"{role[:1]}{node_id}"
    if text.startswith("node-"):
        return "n" + text[5:]
    return text[:12]


def print_legend(thresholds: dict) -> None:
    print()
    print("  COLUMNS")
    print("    dur_s     wall clock length of the send window. every rate/queue/cpu number below is measured over exactly this window.")
    print("    eff/s     events per second the sender actually delivered.   eff/tgt = that divided by the target rate.")
    print("    q_p95     95th percentile depth of the deepest queue, as a fraction of that queue's capacity. this is the standing depth:")
    print("              the level the queue sits at, not a spike. q_where names the queue and the node it was on.")
    print("    q_grow    least squares trend of that queue's depth over the send window, expressed as the fraction of capacity the fitted")
    print("              line gains from the START to the END of the window. +40% = the trend rises by 40% of the queue's capacity over the")
    print("              whole run. it is NOT per second and it is NOT a prediction of the next window. above +100% the trend would more")
    print("              than fill the queue within the window, so the queue is certainly clipping at capacity and the real growth is")
    print("              steeper than shown. a positive q_grow with a low q_p95 is harmless; the pair together is what matters.")
    print("    bklog     fraction of dequeues that found at least one more event already waiting behind them. 0% = the node was idle")
    print("              between events; 100% = there was always something queued. capacity independent, so it is comparable across")
    print("              queues and runs, and it moves long before q_p95 does.")
    print("    drops     input the system threw away: message_drop + dropped_event records. unrecoverable - a revision repairs a LATE")
    print("              message, never a destroyed one. this is the only drop kind that counts as saturation.")
    print("    nofix     repairs that never landed: correction_dropped records, plus result_dropped with reason result_already_final")
    print("              (the revocable window closed before the correction arrived). a better verdict existed and the revision")
    print("              mechanism could not apply it, so a wrong verdict survives to the end of the run. this is not a capacity")
    print("              signal - it is what capacity costs, and it is the first thing to read when acc is below 1.0. result_dropped")
    print("              with reason stale_revision is an OLDER revision arriving late, loses nothing, and is counted in the csv only.")
    print("    cpu_max   busiest single node process, in percent of one core.   cpu_sum = all node processes added up (the sink excluded).")
    print("    of_cap    cpu_max (or cpu_sum, or the busiest thread) as a fraction of the ceiling this run was actually given - see the")
    print("              header line of each table for which. 100% means it ran out of the cpu it was allowed to use.")
    print("    busiest   which node cpu_max was on.   sink = cpu of the result sink.   sender = cpu of the load generator, all in % of a core.")
    print("    wrong     final verdicts that disagree with the ground truth.   acc = fraction correct.")
    print()
    print("  VERDICT   when a system cannot keep up, the excess work has to go somewhere, and there are only four places it can go:")
    print("            into a buffer, back onto the sender, into the bin, or into extra time. which one you see depends on which")
    print("            buffer is tightest, so there is no single signal that always fires. a point is SATURATED if any of these did:")
    print(f"    LOSS        any message_drop or dropped_event record - work was destroyed")
    print(f"    BACKLOG     q_p95 > {thresholds['queue_fill']:.0%} of capacity AND q_grow > {thresholds['queue_growth']:.0%} - a buffer is deep and still filling")
    print(f"    CPU CAP     of_cap >= {thresholds['cpu_pinned']:.0f}% on its own, OR of_cap >= {thresholds['cpu_pct']:.0f}% together with eff/tgt < {thresholds['achieved_ratio']:.2f}")
    print(f"                (cause and consequence: neither is trustworthy alone, both together are)")
    print(f"    STALL       a single socket write blocked for >= {thresholds['stall_ms'] / 1000:.1f}s - a node stopped reading its socket")
    print("    a rate shortfall on its own NEVER counts. it is reported as a warning so it can be explained, not so it can decide.")
    print("    runs with no cpu cap have no ceiling to exhaust, so only LOSS, BACKLOG and STALL can fire for them.")
    print("    -> lines give the signals that fired, ?? lines are warnings that did NOT count, !! lines make the point unusable.")
    print("    when a threshold is crossed, every node that crossed it is listed under the signal, worst first. one hot node and twenty")
    print("    hot nodes are different findings. pass --only-max-node to collapse those lists back to the single worst node.")
    print(f"    .. lines are informational: every node at or above {thresholds['hot_cpu_pct']:.0f}% of one core, whether or not a cap exists.")


TABLE_HEADER = (f"{{x:>7}} {'runs':>5} {'dur_s':>7} {'eff/s':>8} {'eff/tgt':>8} | "
                f"{'q_p95':>6} {'q_grow':>7} {'q_where':>16} {'bklog':>6} {'drops':>6} {'nofix':>7} | "
                f"{'cpu_max':>8} {'of_cap':>7} {'busiest':>8} {'cpu_sum':>8} {'sink':>5} {'sender':>7} | "
                f"{'wrong':>6} {'acc':>9}  verdict")


def _median(values: list):
    clean = sorted(value for value in values if value is not None and not (isinstance(value, float) and math.isnan(value)))
    if not clean:
        return None
    return clean[len(clean) // 2]


def _mode(values: list):
    counts: dict = {}
    for value in values:
        if value is not None:
            counts[value] = counts.get(value, 0) + 1
    return max(counts, key=counts.get) if counts else None


def _row(x_text: str, runs_text: str, get, tag: str) -> str:
    queue_where = (f"{(get('queue_fill_p95_max_queue') or '-').replace('_q', '')}"
                   f"@{_short_node(get('queue_fill_p95_max_node'))}"
                   if get("queue_fill_p95_max_queue") else "-")
    return (f"{x_text:>7} {runs_text:>5} "
            f"{fmt(get('elapsed_s'), '.1f'):>7} "
            f"{fmt(get('effective_rate'), '.0f'):>8} "
            f"{fmt(get('achieved_ratio'), '.3f'):>8} | "
            f"{fmt(get('queue_fill_p95_max'), '.0%'):>6} "
            f"{fmt(get('queue_growth_at_p95_max'), '+.0%'):>7} "
            f"{queue_where:>16} "
            f"{fmt(get('dequeue_backlog_frac'), '.0%'):>6} "
            f"{fmt(get('input_drops')):>6} "
            f"{fmt(get('repair_drops')):>7} | "
            f"{fmt(get('sys_cpu_max_pct'), '.0f'):>8} "
            f"{fmt(get('cpu_of_ceiling'), '.0%'):>7} "
            f"{_short_node(get('sys_cpu_max_node')):>8} "
            f"{fmt(get('sys_cpu_sum_pct'), '.0f'):>8} "
            f"{fmt(get('infra_cpu_max_pct'), '.0f'):>5} "
            f"{fmt(get('sender_cpu_pct'), '.0f'):>7} | "
            f"{fmt(get('wrong')):>6} "
            f"{fmt(get('accuracy'), '.6f'):>9}  {tag}")


# the overview: one line per level, median across replicates (medians, not means, so one bad replicate cannot move a column)
def print_level_overview(system: str, points: list[dict], x_param: str) -> None:
    by_level: dict = {}
    for point in points:
        if point.get("x") is not None:
            by_level.setdefault(point["x"], []).append(point)
    if not by_level:
        return
    ceiling_labels = {point.get("cpu_ceiling_label") for point in points if point.get("cpu_ceiling_label")}
    print()
    print("=" * 158)
    print(f"  {system.upper()}   median per {x_param} level, {len(points)} run(s)")
    if ceiling_labels:
        print(f"  cpu ceiling: {' | '.join(sorted(ceiling_labels))}")
    print("=" * 158)
    print(TABLE_HEADER.format(x=x_param))
    print("-" * 158)
    for level in sorted(by_level):
        rows = by_level[level]
        numeric = ("elapsed_s", "effective_rate", "achieved_ratio", "queue_fill_p95_max",
                   "queue_growth_at_p95_max", "dequeue_backlog_frac", "input_drops",
                   "repair_drops",
                   "sys_cpu_max_pct", "cpu_of_ceiling", "sys_cpu_sum_pct", "infra_cpu_max_pct",
                   "sender_cpu_pct", "wrong", "accuracy")
        values = {key: _median([row.get(key) for row in rows]) for key in numeric}
        for key in ("queue_fill_p95_max_queue", "queue_fill_p95_max_node", "sys_cpu_max_node"):
            values[key] = _mode([row.get(key) for row in rows])
        saturated = sum(1 for row in rows if row["_class"]["saturated"])
        invalid = sum(1 for row in rows if row["_class"]["invalid"])
        tag = f"{saturated}/{len(rows)} SATURATED" if saturated else "ok"
        if invalid:
            tag += f"   !! {invalid} INVALID"
        print(_row(str(level), str(len(rows)), values.get, tag))


def print_system_table(system: str, points: list[dict], x_param: str) -> None:
    ceiling_labels = {point.get("cpu_ceiling_label") for point in points if point.get("cpu_ceiling_label")}
    print()
    print("=" * 158)
    print(f"  {system.upper()}   ({len(points)} point(s), x = {x_param})")
    if ceiling_labels:
        print(f"  cpu ceiling: {' | '.join(sorted(ceiling_labels))}")
    print("=" * 158)
    print(TABLE_HEADER.format(x=x_param))
    print("-" * 158)
    for point in points:
        verdict = point["_class"]
        if verdict["invalid"]:
            tag = "INVALID"
        elif verdict.get("tooling_limited"):
            tag = "TOOLING-LIMITED"
        elif verdict["saturated"]:
            tag = "SATURATED"
        else:
            tag = "ok"
        print(_row(str(point.get("x")), "", point.get, tag))
        for reason in verdict["reasons"]:
            print(f"{'':>7}   -> {reason}")
        for reason in verdict["invalid"]:
            print(f"{'':>7}   !! {reason}")
        for reason in verdict.get("tooling_limited", []):
            print(f"{'':>7}   ~~ {reason}")
        for warning in verdict["warnings"]:
            print(f"{'':>7}   ?? {warning}")
        for note in verdict.get("notes", []):
            print(f"{'':>7}   .. {note}")


def summarise_signals(points: list[dict]) -> list[str]:
    grouped: dict[tuple[str, str], list[tuple[float, str]]] = {}
    for point in points:
        for signal in point["_class"].get("signals", []):
            key = (signal["kind"], str(signal.get("where")))
            grouped.setdefault(key, []).append((signal.get("value"), point.get("run_id")))
    lines = []
    for (kind, where), hits in sorted(grouped.items(), key=lambda item: -len(item[1])):
        values = [value for value, _run in hits if value is not None]
        span = ""
        if values:
            if kind == "BACKLOG" or kind == "CPU CAP":
                span = (f", {min(values):.0%} of the threshold quantity"
                        if len(values) == 1 else
                        f", {min(values):.0%} to {max(values):.0%}")
            elif kind == "STALL":
                span = f", {min(values):.1f}s to {max(values):.1f}s"
            else:
                span = f", {min(values):.0f} to {max(values):.0f}"
        runs = [run for _value, run in hits]
        shown = ", ".join(runs[:3]) + (f" and {len(runs) - 3} more" if len(runs) > 3 else "")
        lines.append(f"{kind} at {where} in {len(hits)}/{len(points)} replicate(s){span}  [{shown}]")
    return lines


def summarise_system(system: str, points: list[dict], x_parameter: str, knee_frac: float = 0.5, x_direction: str = "increasing") -> dict:

    usable = [point for point in points
              if not point["_class"]["invalid"] and not point["_class"]["tooling_limited"]
              and point.get("x") is not None]
    tooling_limited = [point for point in points if point["_class"]["tooling_limited"]]
    invalid_points = [point["run_id"] for point in points if point["_class"]["invalid"]]

    by_level: dict[float, list[dict]] = {}
    for point in usable:
        by_level.setdefault(point["x"], []).append(point)
    ascending = sorted(by_level)
    levels = ascending if x_direction == "increasing" else list(reversed(ascending))

    saturating_frac = {level: sum(1 for point in by_level[level] if point["_class"]["saturated"])
                       / len(by_level[level]) for level in levels}
    is_saturating = {level: saturating_frac[level] >= knee_frac for level in levels}

    knee = None
    for index, level in enumerate(levels):
        if is_saturating[level] and all(is_saturating[beyond] for beyond in levels[index:]):
            knee = level
            break
    knee_index = levels.index(knee) if knee is not None else len(levels)

    early_minority = sorted(level for index, level in enumerate(levels)
                            if 0 < saturating_frac[level] < knee_frac and index < knee_index)
    early_majority = sorted(level for index, level in enumerate(levels)
                            if saturating_frac[level] >= knee_frac and index < knee_index)

    limit_index = min([levels.index(level) for level in early_majority] + [knee_index])
    unsaturated = [level for index, level in enumerate(levels)
                   if index < limit_index and not is_saturating[level]]
    sustainable = unsaturated[-1] if unsaturated else None

    tested_extreme = (max if x_direction == "increasing" else min)(
        (point["x"] for point in points if point.get("x") is not None), default=None)
    usable_extreme = (max if x_direction == "increasing" else min)(levels, default=None)

    summary = {
        "system": system,
        "points": len(points),
        "usable_points": len(usable),
        "x_direction": x_direction,
        "levels": ascending,
        "replicates_per_level": {level: len(by_level[level]) for level in ascending},
        "saturating_fraction": saturating_frac,
        "knee_frac": knee_frac,
        "invalid_points": invalid_points,
        "tooling_limited_points": [point["run_id"] for point in tooling_limited],
        "first_sustained_saturating_x": knee,
        "first_sustained_saturating_reasons": (
            summarise_signals(by_level[knee]) if knee is not None else []),
        "isolated_saturating_x": early_minority,
        "early_majority_saturating_x": early_majority,
        "highest_sustainable_x": sustainable,
        "sustainable_x": sustainable,
        "highest_tested_x": tested_extreme,
        "highest_usable_x": usable_extreme,
        "saturation_bracketed": knee is not None,
        "base_point_60pct": (round(sustainable * 0.6)
                             if sustainable and x_direction == "increasing" else None),
    }

    print()
    print(f"  {system}:")
    if not levels:
        print("    no usable points - every run was invalid or tooling limited, fix those first")
        return summary

    direction_note = ("larger is more load" if x_direction == "increasing"
                      else "larger is more resource, so saturation is expected at the LOW end")
    print(f"    {x_parameter} levels ({direction_note}), share of replicates that saturated "
          f"(a level counts as saturating at >= {knee_frac:.0%}):")
    for level in ascending:
        mark = "SAT" if is_saturating[level] else "   "
        count = sum(1 for point in by_level[level] if point["_class"]["saturated"])
        print(f"      {level:>8}  {count}/{len(by_level[level])}  {mark}")

    wrong_levels = {level: (sum(point.get("wrong") or 0 for point in by_level[level]),
                            sum(1 for point in by_level[level] if point.get("wrong")))
                    for level in ascending}
    dirty = [level for level in ascending if wrong_levels[level][0]]
    summary["levels_with_wrong_verdicts"] = dirty
    summary["wrong_verdicts_total"] = sum(count for count, _ in wrong_levels.values())
    if dirty:
        print(f"    correctness: wrong final verdicts at {x_parameter} = {dirty}")
        for level in dirty:
            total, runs = wrong_levels[level]
            print(f"      {level:>8}  {total} wrong verdict(s) across {runs}/{len(by_level[level])} "
                  f"replicate(s)")
        clean_side = [level for level in ascending if level not in dirty]
        if clean_side:
            print(f"      every other level was exactly correct. lowest level with wrong verdicts "
                  f"= {min(dirty)}")
    else:
        print(f"    correctness: 100% at every level tested")

    if tooling_limited:
        print(f"    !! {len(tooling_limited)} point(s) EXCLUDED as load generator limited "
              f"(the sender ran short of cpu, so their throughput says nothing about the system):")
        for point in tooling_limited:
            print(f"         {point['run_id']}: {point['_class']['tooling_limited'][0]}")
    if early_minority:
        print(f"    isolated saturating level(s) at {early_minority} - a minority of replicates "
              f"only, on the unstressed side of the knee. treated as noise, not as the onset.")
    if early_majority:
        print(f"    !! {x_parameter} = {early_majority} saturated in a MAJORITY of replicates but "
              f"recovered at higher levels - not the onset, but not noise either:")
        for level in early_majority:
            reasons = sorted({reason for point in by_level[level]
                              for reason in point["_class"]["reasons"]})
            count = sum(1 for point in by_level[level] if point["_class"]["saturated"])
            print(f"         {level} ({count}/{len(by_level[level])}): {'; '.join(reasons) or '-'}")
        print(f"       the sustainable value below does NOT step over these. investigate before "
              f"treating anything above them as safe.")

    if knee is None:
        if x_direction == "increasing":
            print(f"    NOT SATURATED anywhere in the tested range. highest usable {x_parameter} "
                  f"= {sustainable}.")
            print(f"    the saturation point is ABOVE the grid - extend it before reading a base "
                  f"point off this.")
        else:
            print(f"    NOT SATURATED even at the smallest {x_parameter} tested "
                  f"({min(ascending)}) - the requirement is BELOW the grid, test smaller budgets.")
        fit = extrapolate_cpu(usable) if x_direction == "increasing" else None
        if x_direction == "decreasing":
            demand = cpu_demand(by_level, levels, x_parameter)
            if demand:
                summary["cpu_demand"] = demand
                print(f"    cpu demand of the system under test (sum over the capped processes, "
                      f"median per level):")
                for level in ascending:
                    row = demand["per_level"].get(level)
                    if row:
                        budget = (f"{row['utilisation']:.0%} of the {row['budget_pct']:.0f}% budget"
                                  if row.get("utilisation") is not None else "budget unknown")
                        print(f"      {level:>8}  {row['cpu_sum_pct']:7.1f}%  ({budget})")
                print(f"    lowest observed demand = {demand['floor_pct']:.0f}% of a core "
                      f"= {demand['floor_cores']:.2f} cores, at {x_parameter} "
                      f"= {demand['floor_x']}")
                if demand.get("suggested_x") is not None:
                    print(f"    so the budget only starts to bind near {x_parameter} "
                          f"= {demand['suggested_x']} - extend the grid down to there, or raise the "
                          f"load instead")
        if fit:
            print(f"    linear fit of the cpu used against its cap: cpu% = {fit['intercept']:.1f} + "
                  f"{fit['slope'] * 1000:.2f} per 1000 {x_parameter}  ->  hits the "
                  f"{fit['ceiling_pct']:.0f}% cap near {x_parameter} = {fit['x_at_100']:,.0f}")
            print(f"    (extrapolation only - it says where the cpu cap would bind if nothing else "
                  f"binds first, not that the system will get there)")
            summary["cpu_fit"] = fit
    elif x_direction == "increasing":
        print(f"    saturation starts at {x_parameter} = {knee} and holds above it")
        for reason in summary["first_sustained_saturating_reasons"]:
            print(f"      because: {reason}")
        print(f"    highest sustainable {x_parameter} = {sustainable}")
        if summary["base_point_60pct"]:
            print(f"    60% base point = {summary['base_point_60pct']}")
    else:
        print(f"    saturated at {x_parameter} = {knee} and below")
        for reason in summary["first_sustained_saturating_reasons"]:
            print(f"      because: {reason}")
        print(f"    smallest sufficient {x_parameter} = {sustainable}")

    if invalid_points:
        print(f"    !! {len(invalid_points)} invalid point(s) excluded: {invalid_points}")
        if usable_extreme is not None and tested_extreme is not None and usable_extreme != tested_extreme:
            print(f"    !! the most stressed end of the grid ({tested_extreme}) has no usable "
                  f"points, so the usable range only reaches {usable_extreme}.")
            summary["saturation_bracketed"] = False
    return summary


# why a run with wrong verdicts is wrong
def explain_wrong_verdicts(point: dict) -> tuple[str, str]:
    def detail(key: str) -> str:
        return ", ".join(f"{count}x {reason}" for reason, count in (point.get(key) or {}).items())

    if point.get("input_drops"):
        return "loss", (f"{point['input_drops']} input record(s) dropped "
                        f"({detail('input_drop_reasons') or 'reason not recorded'}) - destroyed work "
                        f"cannot be repaired by a late revision, so the wrong verdicts follow from it. "
                        f"capacity, not a bug")
    if point.get("repair_drops"):
        return "repair", (f"{point['repair_drops']} repair(s) never landed "
                          f"({detail('repair_drop_reasons')}) - the better verdict existed, the revision "
                          f"mechanism could not apply it. result_already_final means the revocable "
                          f"window closed before the correction arrived, and case_already_closed / "
                          f"incomplete_peer_pull are the same timing story, so all three are capacity. "
                          f"no_recorded_evaluation and no_local_decision are not, so check those")
    if point.get("expected_pairs") and (point.get("pairs_in_db") or 0) < point["expected_pairs"]:
        return "incomplete", (f"only {point.get('pairs_in_db')}/{point['expected_pairs']} pairs reached "
                              f"the sink - verdicts that never arrived cannot be correct")
    if point.get("absent") or point.get("spurious"):
        return "incomplete", (f"absent={point.get('absent')} spurious={point.get('spurious')} - pairs are "
                              f"missing or extra, so this is a delivery problem, not a checking problem")
    if point.get("hard_killed") or point.get("incomplete_record_files"):
        return "incomplete", "a node was killed or a record file was truncated - the run did not finish cleanly"
    if point.get("settlement_reason") and point["settlement_reason"] != "complete":
        return "incomplete", (f"settlement ended as {point['settlement_reason']!r}, not 'complete' - the run "
                              f"may have been torn down before the last revisions landed")
    stale = point.get("stale_result_drops") or 0
    stale_note = (f". {stale} result(s) carrying an older revision were discarded, which is normal and "
                  f"loses nothing" if stale else "")
    if point["_class"]["saturated"]:
        return "unexplained_overloaded", ("the point is saturated but nothing was dropped and everything "
                                          "arrived, so overload does not explain the wrong verdicts by "
                                          "itself - most likely a race that outlived its correction "
                                          "window" + stale_note)
    return "bug", ("no drops, nothing missing, and no saturation signal - the system was keeping up and "
                   "still got verdicts wrong. that is a code or config fault" + stale_note)


def _grep_hint(point: dict) -> list[str]:
    run_dir = point.get("run_dir") or "<run_dir>"
    system = point.get("system", "<system>")
    records = f"{run_dir}/records/{system}"
    types = "|".join(DROP_RECORDS)
    return [f"""grep -hoE '"type":"({types})"' {records}/node-*.jsonl | sort | uniq -c""",
            f"""grep -ho '"reason":"[^"]*"' {records}/node-*.jsonl | sort | uniq -c | sort -rn""",
            f"""grep -lE '"type":"({types})"' {records}/node-*.jsonl"""]


def print_runs_to_investigate(points_by_system: dict, summaries: dict, x_parameter: str) -> None:
    buckets = {"bug": [], "repair": [], "unexplained_overloaded": [], "loss": [], "incomplete": [],
               "invalid": []}
    for system, points in points_by_system.items():
        sustainable = (summaries.get(system) or {}).get("sustainable_x")
        for point in points:
            verdict = point["_class"]
            nominal = (sustainable is not None and point.get("x") is not None
                       and point["x"] <= sustainable)
            if verdict["invalid"]:
                buckets["invalid"].append((point, "; ".join(verdict["invalid"]), nominal))
                continue
            if not point.get("wrong"):
                continue
            kind, why = explain_wrong_verdicts(point)
            buckets[kind].append((point, why, nominal))

    if not any(buckets.values()):
        print()
        print("  RUNS TO INVESTIGATE: none. every run finished cleanly and every wrong verdict is "
              "accounted for (there were none).")
        return

    print()
    print("=" * 158)
    print("  RUNS TO INVESTIGATE")
    print("=" * 158)

    def dump(title: str, rows: list, advice: str, needs_a_look: bool) -> None:
        if not rows:
            return
        rows = sorted(rows, key=lambda r: (r[0]["system"], r[0].get("x") or 0))
        print()
        print(f"  {title}  ({len(rows)} run(s))")
        for point, why, nominal in rows:
            bracket = "  [INSIDE THE NOMINAL BRACKET]" if nominal else ""
            print(f"    {point['system']:<14} {x_parameter}={point.get('x'):<8} {point.get('run_id')}"
                  f"{bracket}")
            print(f"      {point.get('wrong')} wrong, accuracy {fmt(point.get('accuracy'), '.6f')}: {why}")
            if needs_a_look:
                for line in _grep_hint(point):
                    print(f"      $ {line}")
        print(f"    -> {advice}")
        if not needs_a_look:
            print(f"       to look at one of them:")
            for line in _grep_hint(rows[0][0]):
                print(f"       $ {line}")

    dump("!! BUG SUSPECTS", buckets["bug"],
         "nothing about load explains these. fix the cause and re-run them. any that are marked INSIDE THE "
         "NOMINAL BRACKET also contaminate every downstream experiment, not just this sweep.", True)
    dump("?? WRONG VERDICTS EXPLAINED BY CORRECTIONS THAT COULD NOT BE APPLIED", buckets["repair"],
         "the checking was right, the repair never landed. read the reasons: the timing ones are capacity, "
         "the other two are not.", True)
    dump("?? WRONG VERDICTS UNDER LOAD, NOT EXPLAINED BY LOSS", buckets["unexplained_overloaded"],
         "saturated, but nothing was dropped and nothing is missing. check the revision history of a few "
         "pairs before deciding whether this is capacity or a correction window that is too short.", True)
    dump("ok WRONG VERDICTS EXPLAINED BY DROPPED MESSAGES", buckets["loss"],
         "expected: the system was past capacity and threw work away. not a bug. keep them as evidence of "
         "what saturation costs, or raise the queue sizes / lower the rate if you want them clean.", True)
    dump("ok WRONG VERDICTS EXPLAINED BY INCOMPLETE DELIVERY", buckets["incomplete"],
         "the verdicts never had a chance to be right. treat these as invalid rather than as correctness "
         "results.", False)
    if buckets["invalid"]:
        print()
        print(f"  !! INVALID / INCOMPLETE RUNS  ({len(buckets['invalid'])} run(s))")
        for point, why, nominal in sorted(buckets["invalid"],
                                          key=lambda r: (r[0]["system"], r[0].get("x") or 0)):
            bracket = "  [INSIDE THE NOMINAL BRACKET]" if nominal else ""
            print(f"    {point['system']:<14} {x_parameter}={point.get('x'):<8} "
                  f"{point.get('run_id')}{bracket}")
            print(f"      {why}")
        print("    -> these are excluded from the knee above, but extract_metrics and plot_results WILL "
              "still pick them up.")
        print("       investigate them, then delete the run directories and re-run those runs "
              "(run_simulations skips whatever is still on disk):")
        for run_dir in sorted({point.get("run_dir") for point, _why, _nominal in buckets["invalid"]
                               if point.get("run_dir")}):
            print(f"       rm -rf {run_dir}")


def cpu_demand(by_level: dict, levels: list, x_parameter: str) -> dict | None:
    per_level: dict = {}
    for level in levels:
        values = sorted(point["sys_cpu_sum_pct"] for point in by_level[level]
                        if point.get("sys_cpu_sum_pct") is not None)
        if not values:
            continue
        median = values[len(values) // 2]
        budget = float(level) * 100.0 if x_parameter in CORE_PARAMS and level else None
        per_level[level] = {"cpu_sum_pct": median, "budget_pct": budget,
                            "utilisation": (median / budget) if budget else None,
                            "replicates": len(values)}
    if not per_level:
        return None
    floor_x = min(per_level, key=lambda level: per_level[level]["cpu_sum_pct"])
    floor_pct = per_level[floor_x]["cpu_sum_pct"]
    suggested = None
    if x_parameter in CORE_PARAMS:
        # the smallest whole core count whose budget is still below the observed demand
        suggested = max(1, math.floor(floor_pct / 100.0))
    return {"per_level": per_level, "floor_x": floor_x, "floor_pct": floor_pct,
            "floor_cores": floor_pct / 100.0, "suggested_x": suggested,
            "metric": "sys_cpu_sum_pct"}


def extrapolate_cpu(points: list[dict]) -> dict | None:
    if not any(point.get("cpu_of_ceiling") is not None for point in points):
        return None
    key = "cpu_used_pct"
    pairs = [(point["x"], point[key]) for point in points
             if point.get("x") is not None and point.get(key) is not None]
    if len(pairs) < 3:
        return None
    xs = [x for x, _ in pairs]
    ys = [y for _, y in pairs]
    mx, my = sum(xs) / len(xs), sum(ys) / len(ys)
    denominator = sum((x - mx) ** 2 for x in xs)
    if denominator == 0:
        return None
    slope = sum((x - mx) * (y - my) for x, y in pairs) / denominator
    intercept = my - slope * mx
    if slope <= 0:
        return None
    ceiling = next((point["cpu_ceiling_pct"] for point in points if point.get("cpu_ceiling_pct")), 100.0)
    return {"slope": slope, "intercept": intercept, "x_at_100": (ceiling - intercept) / slope,
            "metric": key, "ceiling_pct": ceiling}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("experiment_dir", type=Path, help="results/<experiment_id>")
    parser.add_argument("--x-param", default="rate", help="the swept parameter (default: rate)")
    parser.add_argument("--systems", nargs="*", default=list(SYSTEMS))
    parser.add_argument("--quiet", action="store_true", help="no progress output")
    parser.add_argument("--achieved-ratio", type=float, default=DEFAULT_SATURATION_THRESHOLDS["achieved_ratio"], help="saturated below this effective/target ratio (default 0.95)")
    parser.add_argument("--sustained-ratio", type=float, default=0.95,
                        help="a level whose overall achieved ratio is low but whose MEDIAN per second rate is at least "
                             "this fraction of target is treated as stalled once, not saturated (default 0.95). set to 0 to always trust the overall ratio")
    parser.add_argument("--cpu-threshold", type=float, default=DEFAULT_SATURATION_THRESHOLDS["cpu_pct"], help="cpu %% of the cap that counts as exhausted WHEN the delivered rate also fell short (default 80). runs without a cpu cap have no ceiling, so this rule cannot fire for them")
    parser.add_argument("--queue-fill", type=float, default=DEFAULT_SATURATION_THRESHOLDS["queue_fill"],
                        help="a queue counts as saturated when its p95 depth exceeds this fraction of capacity AND "
                             "its depth is still growing (see --queue-slope). a peak above it is only a warning, "
                             "because one sample is not evidence of a standing backlog (default 0.25)")
    parser.add_argument("--queue-growth", type=float, default=DEFAULT_SATURATION_THRESHOLDS["queue_growth"],
                        help="a queue only counts as saturated when its depth TREND gains at least this fraction of "
                             "its capacity over the measurement window (default 0.05 = 5%%). expressing growth "
                             "relative to capacity and window length is what makes it comparable between a 1024 slot "
                             "gossip queue and an 8192 slot event queue, and between a 65s and a 13s run. a plain "
                             "'slope > 0' test fires on measurement noise")
    parser.add_argument("--source", choices=("auto", "metrics", "raw"), default="auto",
                        help="where to read each point from. 'metrics' uses metrics.json only (schema 4+), "
                             "which means records/ and resources/ can be deleted after extract_metrics.py "
                             "has run. 'raw' reads the run files directly. 'auto' (default) prefers "
                             "metrics.json and falls back to the raw files when it is missing or too old")
    parser.add_argument("--hot-cpu-pct", type=float, default=90.0,
                        help="nodes at or above this %% of ONE CORE are listed under each point as a hot "
                             "node (default 90). informational only - it never decides saturation, it just "
                             "shows where the load sat")
    parser.add_argument("--only-max-node", action="store_true",
                        help="when a saturation threshold is crossed, name only the single worst node "
                             "instead of every node that crossed it. the default lists all of them, "
                             "because whether one node or twenty is over the line is the difference "
                             "between a hot spot and a system wide limit")
    parser.add_argument("--per-run", action="store_true",
                        help="also print one row per replicate with the signals it fired. the default is the "
                             "median-per-level overview only")
    parser.add_argument("--cpu-pinned", type=float, default=DEFAULT_SATURATION_THRESHOLDS["cpu_pinned"],
                        help="a node (or the deployment) at this %% of its cpu cap counts as saturated on its "
                             "own, with no second signal needed (default 95)")
    parser.add_argument("--stall-ms", type=float, default=DEFAULT_SATURATION_THRESHOLDS["stall_ms"],
                        help="saturated when a single socket write blocked for at least this long (default 1000 = 1s). "
                             "this is a node that stopped reading its socket altogether, not slow processing - keep it "
                             "high enough that ordinary scheduling jitter cannot reach it")
    parser.add_argument("--sender-cpu-threshold", type=float, default=60.0,
                        help="above this sender cpu%% of one core, a lone achieved/target shortfall is attributed to the load generator rather than the system (default 60)")
    parser.add_argument("--x-direction", choices=["increasing", "decreasing"], default=None,
                        help="whether a bigger x means more stress. 'increasing' for a load sweep (rate), 'decreasing' for a resource sweep (cpu_cap_cores, nodes). "
                             "inferred from --x-param when not given")
    parser.add_argument("--filter", action="append", default=[], metavar="KEY=VALUE",
                        help="keep only runs whose run.json params match. repeatable, and a value list is allowed "
                             "(--filter rate=16000 --filter cases=2500,10000). use this to hold every swept parameter "
                             "except --x-param: if a level mixes runs at different loads, its saturating fraction is "
                             "an average over those loads and the knee is not readable")
    parser.add_argument("--knee-frac", type=float, default=0.5,
                        help="fraction of replicates at one x that must saturate for that x to count as saturating (default 0.5, i.e. a majority)")
    parser.add_argument("--infra-roles", nargs="*", default=list(INFRA_ROLES),
                        help="node roles that are evaluation scaffolding, not the system under test. "
                             f"measured and reported but excluded from every saturation signal (default: {' '.join(INFRA_ROLES)}). pass nothing to include everything")
    parser.add_argument("--infra-queue-fill-warn", type=float, default=0.9,
                        help="warn when an excluded queue (the result sink) reaches this fill. it is never a saturation signal, but a full sink can backpressure into the "
                             "system and confound the result (default 0.9)")
    parser.add_argument("--infra-queues", nargs="*", default=list(INFRA_QUEUES), help="queues excluded from the queue saturation signal because their depth "
                             f"reflects infrastructure, not load (default: {' '.join(INFRA_QUEUES)})")
    parser.add_argument("--include-invalid", action="store_true", help="keep runs that were SIGKILLed or short on results in the saturation search")
    parser.add_argument("--json", type=Path, default=None, help="write the full per point data here")
    parser.add_argument("--csv", type=Path, default=None, help="write a flat per point table here")
    args = parser.parse_args()

    try:
        filters = parse_filters(args.filter)
    except ValueError as error:
        print(error, file=sys.stderr)
        return 2

    # a resource sweep for example saturates at the low end, so inferred from --x-param unless forced
    x_direction = args.x_direction or infer_x_direction(args.x_param)

    thresholds = {"achieved_ratio": args.achieved_ratio, "sustained_ratio": args.sustained_ratio,
                  "cpu_pct": args.cpu_threshold, "cpu_pinned": args.cpu_pinned,
                  "queue_fill": args.queue_fill, "queue_growth": args.queue_growth,
                  "stall_ms": args.stall_ms, "sender_cpu_pct": args.sender_cpu_threshold,
                  "only_max_node": args.only_max_node, "hot_cpu_pct": args.hot_cpu_pct,
                  "infra_queue_fill_warn": args.infra_queue_fill_warn}

    run_directories = sorted(entry for entry in args.experiment_dir.iterdir() if (entry / "run.json").exists()) \
        if args.experiment_dir.is_dir() else []
    if not run_directories:
        print(f"no runs with a run.json under {args.experiment_dir}", file=sys.stderr)
        return 0

    filtered_out = 0
    if filters:
        kept = []
        for run_directory in run_directories:
            try:
                params = json.loads((run_directory / "run.json").read_text()).get("params", {})
            except (json.JSONDecodeError, OSError):
                continue
            if run_matches_filters(params, filters):
                kept.append(run_directory)
            else:
                filtered_out += 1
        run_directories = kept
        if not run_directories:
            wanted = ", ".join(key + "=" + ",".join(values) for key, values in filters)
            print(f"no runs under {args.experiment_dir} match {wanted} "
                  f"({filtered_out} run(s) filtered out)", file=sys.stderr)
            return 0

    points_by_system: dict[str, list[dict]] = {system: [] for system in args.systems}
    progress = Progress(len(run_directories) * len(args.systems), "collecting", enabled=not args.quiet)
    for run_directory in run_directories:
        for system in args.systems:
            progress.note(f"{run_directory.name} {system}")
            point = None
            if args.source in ("auto", "metrics"):
                point = collect_point_from_metrics(run_directory, system, args.x_param,
                                                   tuple(args.infra_roles), tuple(args.infra_queues))
            if point is None and args.source in ("auto", "raw"):
                point = collect_point(run_directory, system, args.x_param,
                                      tuple(args.infra_roles), tuple(args.infra_queues))
            progress.step(f"{run_directory.name} {system}")
            if point is None:
                continue
            point["_class"] = classify(point, thresholds)
            if args.include_invalid:
                point["_class"]["invalid"] = []
            points_by_system[system].append(point)
    progress.finish()

    for points in points_by_system.values():
        points.sort(key=lambda point: (point.get("x") is None, point.get("x"), point.get("run_id")))

    all_points = [point for points in points_by_system.values() for point in points]

    print(f"experiment: {args.experiment_dir}")
    direction_note = ("bigger x is more load, knee at the HIGH end" if x_direction == "increasing"
                      else "bigger x is more resource, knee at the LOW end")
    print(f"x param: {args.x_param} ({x_direction} - {direction_note})")
    if filters:
        wanted = ", ".join(key + "=" + ",".join(values) for key, values in filters)
        print(f"filter: {wanted}  ({len(run_directories)} run(s) kept, {filtered_out} filtered out)")
    print_legend(thresholds)
    if args.infra_roles or args.infra_queues:
        print(f"excluded from every signal: role(s) {args.infra_roles or 'none'}, "
              f"queue(s) {args.infra_queues or 'none'} - evaluation scaffolding, still reported "
              f"in the sink column and the json")
    if not any(point.get("records_present") for point in all_points):
        print("note: no records/ found - queue depth and message drop signals are UNAVAILABLE, the verdicts below rest on the pacer and cpu signals only")

    rounded_points = [point["run_id"] for point in all_points if not point.get("rate_source_exact", True)]
    if rounded_points:
        print(f"note: {len(rounded_points)} point(s) had no send log, so their achieved ratio comes from the "
              f"pacer summary line and is only accurate to a few significant digits")

    points_without_x = [point["run_id"] for point in all_points if point.get("x") is None]
    distinct_x_values = sorted({point["x"] for point in all_points if point.get("x") is not None})
    if points_without_x:
        available_params = sorted({key for run_directory in run_directories[:1]
                                   for key in (json.loads((run_directory / "run.json").read_text()).get("params") or {})})
        print(f"\n  !! --x-param {args.x_param!r} is not in params for {len(points_without_x)} of "
              f"{len(all_points)} point(s) - those points cannot be placed on the x axis and are "
              f"dropped from the saturation search.")
        print(f"     available params in this experiment: {', '.join(available_params)}")
    elif len(distinct_x_values) < 2:
        print(f"\n  !! --x-param {args.x_param!r} is constant at "
              f"{distinct_x_values[0] if distinct_x_values else '?'} across every run - there is no "
              f"sweep to find a knee in.")
        print(f"     pick the parameter that varies in [experiment.grid], e.g. --x-param rate.")

    for system in args.systems:
        if points_by_system[system]:
            print_level_overview(system, points_by_system[system], args.x_param)
    if not args.per_run:
        print()
        print("  (per run detail suppressed - pass --per-run to see every replicate and the signals it fired)")
    for system in args.systems:
        if points_by_system[system] and args.per_run:
            print_system_table(system, points_by_system[system], args.x_param)

    saturated_infra = [point for point in all_points
                       if (point.get("infra_queue_fill_max") or 0) >= args.infra_queue_fill_warn]
    if saturated_infra:
        worst = max(saturated_infra, key=lambda point: point["infra_queue_fill_max"])
        levels_hit = sorted({point["x"] for point in saturated_infra if point.get("x") is not None})
        print()
        print("=" * 132)
        print(f"  !! CONFOUND: {worst['infra_queue_fill_max_queue']} reached "
              f">= {args.infra_queue_fill_warn:.0%} of capacity in {len(saturated_infra)} of "
              f"{len(all_points)} point(s), at {args.x_param} {levels_hit}")
        print(f"     the result sink is evaluation scaffolding and is excluded from every saturation "
              f"signal, but a full sink queue")
        print(f"     backpressures through tcp into the nodes' result emitters and can stall the "
              f"event processors being measured.")
        if len(levels_hit) == len({point.get("x") for point in all_points}):
            print(f"     it is full at EVERY level tested, including the lowest - so this is not a "
                  f"consequence of load. raise the sink's")
            print(f"     mpsc_channel_size / max_batch_size and re run before trusting any knee "
                  f"found here.")
        print("=" * 132)

    print()
    print("=" * 132)
    print("  SUMMARY PER SYSTEM")
    print("=" * 132)
    summaries_by_system = {system: summarise_system(system, points_by_system[system], args.x_param, args.knee_frac, x_direction)
                           for system in args.systems if points_by_system[system]}

    print_runs_to_investigate(points_by_system, summaries_by_system, args.x_param)

    print()
    unbracketed_systems = [system for system, summary in summaries_by_system.items()
                           if not summary.get("saturation_bracketed")]
    if unbracketed_systems and x_direction == "increasing":
        print(f"  NO BASE POINT YET: {', '.join(unbracketed_systems)} never saturated (or the top of the grid is invalid).")
        print(f"  a '60% of sustainable' base point read off an unsaturated sweep is just 60% of wherever you stopped testing,")
        print(f"  not 60% of what the system can do. extend the grid until every system shows a sustained saturating point.")
    elif unbracketed_systems:
        print(f"  NO BINDING BUDGET YET: {', '.join(unbracketed_systems)} kept up even at the smallest {args.x_param} tested.")
        print(f"  the requirement is below the grid. either extend it downwards, or hold {args.x_param} and raise the load instead -")
        print(f"  a budget that only binds for one system is still a usable comparison, but say which system it binds for.")
    elif x_direction == "increasing":
        sustainable_by_system = {system: summary["sustainable_x"]
                                 for system, summary in summaries_by_system.items()
                                 if summary.get("sustainable_x") is not None}
        if len(sustainable_by_system) > 1:
            limiting_system = min(sustainable_by_system, key=sustainable_by_system.get)
            lowest_sustainable = sustainable_by_system[limiting_system]
            print(f"  the systems saturate at different loads:")
            for system, sustainable in sorted(sustainable_by_system.items(), key=lambda item: item[1]):
                print(f"    {system:<16} sustainable {args.x_param} = {sustainable:<8} "
                      f"60% base point = {round(sustainable * 0.6)}")
            print(f"  report these separately. if you also need ONE shared base point for the sensitivity experiments, it has to")
            print(f"  come from the lower one ({limiting_system}), otherwise {limiting_system} is already saturated at the base point and every")
            print(f"  downstream comparison is confounded.  shared 60% base point = {round(lowest_sustainable * 0.6)}")
    else:
        sufficient_by_system = {system: summary["sustainable_x"]
                                for system, summary in summaries_by_system.items()
                                if summary.get("sustainable_x") is not None}
        if len(sufficient_by_system) > 1:
            hungriest_system = max(sufficient_by_system, key=sufficient_by_system.get)
            print(f"  the systems need different budgets for this load:")
            for system, sufficient in sorted(sufficient_by_system.items(), key=lambda item: item[1]):
                print(f"    {system:<16} smallest sufficient {args.x_param} = {sufficient}")
            print(f"  a shared budget has to come from the hungrier one ({hungriest_system}) if BOTH must keep up, and from the")
            print(f"  leaner one if the point is to make both saturate. those are different experiments - pick one deliberately.")

    report_payload = {
        "experiment": str(args.experiment_dir),
        "x_param": args.x_param,
        "x_direction": x_direction,
        "filters": {key: values for key, values in filters},
        "runs_kept": len(run_directories),
        "runs_filtered_out": filtered_out,
        "thresholds": thresholds,
        "summary": summaries_by_system,
        "points": {system: [{key: value for key, value in point.items() if key != "_class"}
                            | {"verdict": point["_class"]} for point in points]
                   for system, points in points_by_system.items()},
    }
    if args.json:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        args.json.write_text(json.dumps(report_payload, indent=2, default=str))
        print(f"\n  json -> {args.json}")

    if args.csv:
        csv_rows = []
        for point in all_points:
            row = {key: value for key, value in point.items()
                   if key != "_class" and not isinstance(value, (dict, list))}
            row["saturated"] = point["_class"]["saturated"]
            row["invalid"] = bool(point["_class"]["invalid"])
            csv_rows.append(row)
        if csv_rows:
            field_names = sorted({key for row in csv_rows for key in row})
            args.csv.parent.mkdir(parents=True, exist_ok=True)
            with open(args.csv, "w", newline="") as csv_file:
                csv_writer = csv.DictWriter(csv_file, fieldnames=field_names)
                csv_writer.writeheader()
                csv_writer.writerows(csv_rows)
            print(f"  csv  -> {args.csv}")
    return 0


if __name__ == "__main__":
    sys.exit(main())