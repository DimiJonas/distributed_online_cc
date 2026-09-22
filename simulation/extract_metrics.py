#!/usr/bin/env python3
"""
extract_metrics.py - extract metrics from the generated files of a simulation run

writes metrics.json:  all one time metrics (throughput, latency, revisions, resources, message overhead, correctness) for each system
writes observations.json.gz:  what each node saw (local events, events inferable from coordination traffic, case end sightings) for computing privacy metrics with compute_privacy.py
writes disclosure_index.json.gz next to the stream's manifest.jsonl:  the compacted ground truth log, the trace variant index and the log baseline that compute_privacy.py needs

  python3.11 simulation/extract_metrics.py --results results/
  python3.11 simulation/extract_metrics.py --build-index results/_streams/sepsis/manifest.jsonl --bk-type set mult seq
  python3.11 simulation/extract_metrics.py --results results/ --experiment 'load_*' --force
  python3.11 simulation/extract_metrics.py --run results/load_sweep/rate1000__seed29

windows:
- idle = [idle_start_us, idle_end_us]
- load = [send_start_us, send_end_us]
- steady = load trimmed by --warmup-fraction/--cooldown-fraction
- settle = [send_end_us, system_settled_us]

rate and resource aggregates use the 'steady' (to exclude page cache warm up and tcp channel buildup)
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import math
import os
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from helpers import (CLK_TCK, load_system_configs, read_jsonl, quantile, slope_per_second,
                     percentiles, case_id_hash_fraction, discover_runs, Progress,
                     DISCLOSURE_INDEX_NAME as DEFAULT_NAME, ensure_index,
                     cpu_ceiling, saturation_signals, worst_queue)

SCHEMA_VERSION = 6

COORDINATION_PAYLOADS = ("conformance", "ce_gossip", "overlay")
CASE_CLOSED_REASON = "case closed"
TRANSPORT_PAYLOADS = ("event")
SINK_PAYLOADS = ("result")

# RSS samples are used to create a memory timeline
RSS_TIMELINE_BIN_S = 1.0
RSS_TIMELINE_MAX_BINS = 2000


# keeps the time windows of a run (idle, load, steady, settle) and provides some window functions
class Windows:
    def __init__(self, timeline: dict, warmup_fraction: float, cooldown_fraction: float):
        self.idle = (timeline.get("idle_start_us"), timeline.get("idle_end_us"))
        # load window is the pacing loop so it doesnt include reading the event stream to memory at the start and flushing send log at the end
        pacer_start, pacer_end = timeline.get("pacer_start_us"), timeline.get("pacer_end_us")
        process_start, process_end = timeline.get("send_start_us"), timeline.get("send_end_us")
        if pacer_start and pacer_end and pacer_end > pacer_start:
            self.load = (pacer_start, pacer_end)
            self.load_source = "pacer"
        else:
            self.load = (process_start, process_end)
            self.load_source = "process"
        self.process_load = (process_start, process_end)
        self.settle = (self.load[1], timeline.get("system_settled_us"))
        self.warmup_fraction = warmup_fraction
        self.cooldown_fraction = cooldown_fraction
        send_start, send_end = self.load
        if send_start is None or send_end is None or send_end <= send_start:
            self.steady = (send_start, send_end)
        else:
            span = send_end - send_start
            self.steady = (send_start + math.ceil(span * warmup_fraction), send_end - math.ceil(span * cooldown_fraction))

    def contains(self, window, t_us) -> bool:
        start, end = window
        return start is not None and end is not None and start <= t_us <= end

    def seconds(self, window) -> float:
        start, end = window
        if start is None or end is None or end <= start:
            return 0.0
        return (end - start) / 1e6

    def as_dict(self) -> dict:
        window = {"idle": list(self.idle), "load": list(self.load), "steady": list(self.steady),
                  "settle": list(self.settle),
                  "steady_seconds": self.seconds(self.steady),
                  "load_seconds": self.seconds(self.load),
                  "idle_seconds": self.seconds(self.idle),
                  "load_source": self.load_source,
                  "warmup_fraction": self.warmup_fraction, "cooldown_fraction": self.cooldown_fraction}
        # how much sender-only work sits outside the load window (preload before, flush after)
        process_start, process_end = self.process_load
        if self.load_source == "pacer" and process_start and process_end:
            window["sender_preload_seconds"] = (self.load[0] - process_start) / 1e6
            window["sender_teardown_seconds"] = (process_end - self.load[1]) / 1e6
        return window


# a single pass over records/<system>/node-*.jsonl
class Scan:

    def __init__(self, windows: Windows, node_roles: dict, case_limit: int | None):
        self.windows = windows
        self.node_roles = node_roles
        self.case_limit = case_limit

        self.t1: dict[str, int] = {} # instance -> timestamp
        self.t2: dict[str, int] = {} # instance -> timestamp
        self.instance_to_case_event: dict[str, tuple[str, str]] = {} # instance -> (case, event)
        self.input_node: dict[str, int] = {} # instance -> node id
        self.dequeue_node: dict[str, int] = {} # instance -> node id
        self.dequeue_depths: dict[int, list[int]] = {}
        
        # verdicts per (case, constraint): revision -> (t_us, result, trigger, instance)
        self.verdicts: dict[tuple[str, str], dict[int, tuple[int, str, str, str | None]]] = {}
        self.constraint_type: dict[str, str] = {} # constraint id -> constraint type

        # durations of the processing steps
        self.handling_us: list[float] = []
        self.parse_us: list[float] = []
        self.serde_us: list[float] = []

        self.messages = {"idle": self._msg_bucket(), "load": self._msg_bucket(), "settle": self._msg_bucket()}
        self.drops = {"total": 0, "by_reason": {}, "by_kind": {}}
        self.event_drops = {"total": 0, "by_reason": {}}   # events discarded before they were handled
        self.result_drops = {"total": 0, "by_reason": {}}
        self.correction_drops = {"total": 0, "by_reason": {}, "by_constraint_type": {}}
        self.correction_drop_pairs: set = set()
        self.queues: dict[str, list[tuple[float, float]]] = {} # name -> (timestamp, fill) during the load window
        self.queue_node_max: dict[str, dict[int, int]] = {} # queue name -> (node id -> peak depth) # name -> capacity
        self.queues_settle: dict[str, list[tuple[float, float]]] = {}
        self.queue_node_max_settle: dict[str, dict[int, int]] = {}
        self.queues_by_node: dict[tuple[str, int], list[tuple[float, int, int]]] = {}
        self.tcp_queue_max = 0 # highest fill of a tcp channel queue
        self.peer_churn = {"created": 0, "removed": 0}
        self.meta: dict[int, dict] = {} # meta infos at node startup
        self.writer_shutdown: set[int] = set() # > 0 means correct shutdown
        self.nodes_seen: set[int] = set() 

        # privacy observations
        self.local: dict[int, dict[str, set[str]]] = {} # node id -> (case -> {event ids})
        self.msg_case_constraint: dict[int, set[tuple[str, str, str]]] = {} # node id -> {(case, constraint, peer address)}
        self.messages_without_peer = 0   # coordination messages whose peer address could not be read
        self.msg_case_only: dict[int, set[str]] = {} # node id -> {case} --- from gossip, so no constraint
        self.case_end_seen: dict[int, dict[str, dict]] = {} # node id -> (case -> case end seen record)
        self.case_end_truth: dict[str, int] = {} # case -> node id
        self.events_inputs_per_node: dict[int, int] = {} # node id -> number of event inputs

    @staticmethod
    def _msg_bucket() -> dict:
        return {"sent": 0, "received": 0, "bytes_sent": 0, "bytes_received": 0,
                "by_payload_type": {}, "by_cc_msg_type": {}, "by_gossip_msg_type": {}}

    # if cases are limited for evaluation (just used for some very big synthetic event streams)
    # can impact the case disclosure metric by making traces unique that previously werent
    def _keep_case(self, case_id: str) -> bool:
        if self.case_limit is None:
            return True
        return case_id_hash_fraction(case_id) < self.case_limit

    def feed(self, line: dict) -> None:
        record_type = line.get("type")
        node_id = line.get("node_id")
        wallclock_time = line.get("timestamp_wallclock_us")
        if node_id is not None:
            self.nodes_seen.add(node_id)

        if record_type == "message":
            self._message(line, node_id, wallclock_time)
        elif record_type == "event_input":
            self._event_input(line, node_id, wallclock_time)
        elif record_type == "event_dequeue":
            instance = line["instance_id"]
            if instance not in self.t2 or wallclock_time < self.t2[instance]:
                self.t2[instance] = wallclock_time
                self.dequeue_node[instance] = node_id
            depth = line.get("q_length")
            if depth is not None and self.windows.contains(self.windows.load, wallclock_time):
                self.dequeue_depths.setdefault(node_id, []).append(depth)
            # record that the event was seen (important for central)
            case_id, event_id = line["case_id"], line["event_id"]
            if self._keep_case(case_id):
                self.local.setdefault(node_id, {}).setdefault(case_id, set()).add(event_id)
        elif record_type == "event_handled":
            self.handling_us.append(line.get("duration_us", 0))
        elif record_type == "result_emitted":
            self._result(line, wallclock_time)
        elif record_type == "message_drop":
            self.drops["total"] += 1
            reason = str(line.get("reason"))
            self.drops["by_reason"][reason] = self.drops["by_reason"].get(reason, 0) + 1
            payload_kind = str(line.get("kind"))
            self.drops["by_kind"][payload_kind] = self.drops["by_kind"].get(payload_kind, 0) + 1
            self._drop_node(self.drops, node_id)
        elif record_type == "dropped_event":
            self.event_drops["total"] += 1
            reason = str(line.get("reason"))
            self.event_drops["by_reason"][reason] = self.event_drops["by_reason"].get(reason, 0) + 1
            self._drop_node(self.event_drops, node_id)
        elif record_type == "result_dropped":
            self.result_drops["total"] += 1
            reason = line.get("reason") or "unknown"
            self.result_drops["by_reason"][reason] = self.result_drops["by_reason"].get(reason, 0) + 1
        elif record_type == "correction_dropped":
            self.correction_drops["total"] += 1
            reason = str(line.get("reason") or "unknown")
            self.correction_drops["by_reason"][reason] = self.correction_drops["by_reason"].get(reason, 0) + 1
            constraint = str(line.get("constraint_type") or "unknown")
            self.correction_drops["by_constraint_type"][constraint] = \
                self.correction_drops["by_constraint_type"].get(constraint, 0) + 1
            pair = (line.get("case_id"), line.get("constraint_id"))
            if all(pair):
                self.correction_drop_pairs.add(pair)
        elif record_type == "queues":
            self._queues(line, node_id, wallclock_time)
        elif record_type == "tcp_channel_queue_depth":
            self.tcp_queue_max = max(self.tcp_queue_max, int(line.get("depth", 0)))
        elif record_type == "case_end_seen":
            self._case_end(line, node_id, wallclock_time)
        elif record_type == "peer_churn":
            churn_type = line.get("event")
            self.peer_churn[churn_type] += 1
        elif record_type == "meta":
            self.meta[node_id] = {"mode": line.get("mode"), "constraints": line.get("constraints")}
        elif record_type == "writer_shutdown":
            self.writer_shutdown.add(node_id)

    def _message(self, line: dict, node_id: int, wallclock_time: int) -> None:
        bucket_name = ("idle" if self.windows.contains(self.windows.idle, wallclock_time)
                       else "load" if self.windows.contains(self.windows.load, wallclock_time)
                       else "settle" if self.windows.contains(self.windows.settle, wallclock_time)
                       else None)
        payload_type = str(line.get("payload_type"))
        direction = line.get("direction")
        byte_count = int(line.get("bytes", 0))
        if bucket_name:
            bucket = self.messages[bucket_name]
            if direction == "sent":
                bucket["sent"] += 1
                bucket["bytes_sent"] += byte_count
            else:
                bucket["received"] += 1
                bucket["bytes_received"] += byte_count
            per_type = bucket["by_payload_type"].setdefault(
                payload_type, {"sent": 0, "received": 0, "bytes_sent": 0, "bytes_received": 0})
            if direction == "sent":
                per_type["sent"] += 1
                per_type["bytes_sent"] += byte_count
            else:
                per_type["received"] += 1
                per_type["bytes_received"] += byte_count
            for key, field in (("by_cc_msg_type", "cc_msg_type"),
                               ("by_gossip_msg_type", "gossip_msg_type")):
                value = line.get(field)
                if not value:
                    continue
                per_subtype = bucket[key].setdefault(
                    value, {"sent": 0, "received": 0, "bytes_sent": 0, "bytes_received": 0})
                if direction == "sent":
                    per_subtype["sent"] += 1
                    per_subtype["bytes_sent"] += byte_count
                else:
                    per_subtype["received"] += 1
                    per_subtype["bytes_received"] += byte_count
        self.serde_us.append(line.get("serde_us", 0))

        # privacy: a coordination message tells the endpoints that this case touched this constraint.
        case_id = line.get("case_id")
        if case_id and payload_type in ("conformance", "ce_gossip") and self._keep_case(case_id):
            constraint_id = line.get("constraint_id")
            if constraint_id:
                peer = line.get("address") or line.get("peer_address")
                self.msg_case_constraint.setdefault(node_id, set()).add((case_id, constraint_id, peer))
                if peer is None:
                    self.messages_without_peer += 1
            else:
                self.msg_case_only.setdefault(node_id, set()).add(case_id)

    def _event_input(self, line: dict, node_id: int, wallclock_time: int) -> None:
        instance = line["instance_id"]
        if instance not in self.t1 or wallclock_time < self.t1[instance]:
            self.t1[instance] = wallclock_time
            self.input_node[instance] = node_id
        case_id, event_id = line["case_id"], line["event_id"]
        self.instance_to_case_event[instance] = (case_id, event_id)
        self.parse_us.append(line.get("parse_us", 0))
        self.events_inputs_per_node[node_id] = self.events_inputs_per_node.get(node_id, 0) + 1
        if self._keep_case(case_id):
            self.local.setdefault(node_id, {}).setdefault(case_id, set()).add(event_id)

    def _result(self, line: dict, wallclock_time: int) -> None:
        key = (line["case_id"], line["constraint_id"])
        revision = int(line.get("revision", 0))
        self.verdicts.setdefault(key, {})[revision] = (
            wallclock_time, line.get("result"), line.get("trigger"), line.get("instance_id"))
        self.constraint_type[line["constraint_id"]] = line.get("constraint_type")

    @staticmethod
    def _drop_node(bucket: dict, node_id) -> None:
        if node_id is None:
            return
        by_node = bucket.setdefault("by_node", {})
        key = f"node-{node_id}"
        by_node[key] = by_node.get(key, 0) + 1

    def _queues(self, line: dict, node_id: int, wallclock_time: int) -> None:
        if self.windows.contains(self.windows.load, wallclock_time):
            samples, node_max = self.queues, self.queue_node_max
        elif self.windows.contains(self.windows.settle, wallclock_time):
            samples, node_max = self.queues_settle, self.queue_node_max_settle
        else:
            return
        for name in ("event_q", "loopback_q", "result_sink_q", "gossip_out_q", "gossip_in_q"):
            depth_sample = line.get(name) # (current fill, peak fill since last report, max capacity)
            if not depth_sample:
                continue
            samples.setdefault(name, []).append((wallclock_time, depth_sample[1], depth_sample[2]))
            per_node = node_max.setdefault(name, {})
            per_node[node_id] = max(per_node.get(node_id, 0), depth_sample[1])
            if samples is self.queues:
                self.queues_by_node.setdefault((name, node_id), []).append(
                    (wallclock_time, depth_sample[1], depth_sample[2]))

    def _case_end(self, line: dict, node_id: int, wallclock_time: int) -> None:
        case_id = line["case_id"]
        source = line.get("source")
        if source == "end_event":
            self.case_end_truth[case_id] = node_id
        if not self._keep_case(case_id):
            return
        per_node = self.case_end_seen.setdefault(node_id, {})
        if case_id not in per_node or wallclock_time < per_node[case_id]["t_us"]:
            per_node[case_id] = {"t_us": wallclock_time, "source": source,
                                 "source_peer": line.get("source_peer")}


def resource_metrics(path: Path, windows: Windows, exclude_roles=("sink",)) -> dict:
    steady_first: dict[int, dict] = {} # pid -> first sample in steady window
    steady_last: dict[int, dict] = {} # pid -> last sample in steady window
    steady_rss_max: dict[int, int] = {} # pid -> max rss in steady window
    threads_max: dict[int, int] = {} # pid -> max amount of threads
    thread_first: dict[int, dict[int, int]] = {} # pid -> (thread_id -> ticks)
    thread_last: dict[int, dict[int, tuple[str, int]]] = {} # pid -> (thread_id -> (thread name, ticks))
    idle_first: dict[int, dict] = {} # pid -> first sample in idle window
    idle_last: dict[int, dict] = {} # pid -> last sample in idle window
    idle_rss_max: dict[int, int] = {} # pid -> max rss in idle window
    rss_bins: dict[int, dict[int, int]] = {} # time bin -> (pid -> last rss in that bin), whole run
    # processes outside the system under test (the sender, the sink), keyed by (group, pid)
    other_first: dict[tuple[str, int], dict] = {}
    other_last: dict[tuple[str, int], dict] = {}
    other_rss_max: dict[tuple[str, int], int] = {}
    other_role: dict[tuple[str, int], str] = {}
    host_first = host_last = None 

    for row in read_jsonl(path):
        sample_time = row.get("t_us")
        sample_type = row.get("kind")
        if sample_type == "host":
            if windows.contains(windows.steady, sample_time):
                host_first = host_first or row
                host_last = row
            continue
        # the load generator and the sink are not part of the system under test, but the sender's
        # cpu decides whether a throughput number measures the system or the tooling, so it is
        # summarised separately instead of being dropped
        if row.get("group") != "system":
            if sample_type == "proc" and windows.contains(windows.steady, sample_time):
                pid = row.get("pid")
                group = str(row.get("group") or "other")
                other_first.setdefault((group, pid), row)
                other_last[(group, pid)] = row
                other_rss_max[(group, pid)] = max(other_rss_max.get((group, pid), 0),
                                                  row.get("rss_kb", 0))
                other_role[(group, pid)] = row.get("role")
            continue
        pid = row.get("pid")
        if sample_type == "proc":
            if row.get("role") not in exclude_roles and row.get("rss_kb") is not None:
                bin_index = int(sample_time // int(RSS_TIMELINE_BIN_S * 1_000_000))
                rss_bins.setdefault(bin_index, {})[pid] = row["rss_kb"]
            if windows.contains(windows.steady, sample_time):
                steady_first.setdefault(pid, row)
                steady_last[pid] = row
                steady_rss_max[pid] = max(steady_rss_max.get(pid, 0), row.get("rss_kb", 0))
                threads_max[pid] = max(threads_max.get(pid, 0), row.get("threads", 0))
            if windows.contains(windows.idle, sample_time):
                idle_first.setdefault(pid, row)
                idle_last[pid] = row
                idle_rss_max[pid] = max(idle_rss_max.get(pid, 0), row.get("rss_kb", 0))
        elif sample_type == "threads" and windows.contains(windows.steady, sample_time):
            snapshot = {thread_id: ticks for thread_id, _, ticks in row.get("threads", [])}
            thread_first.setdefault(pid, snapshot)
            thread_last[pid] = {thread_id: (name, ticks) for thread_id, name, ticks in row.get("threads", [])}

    steady_s = windows.seconds(windows.steady)
    idle_s = windows.seconds(windows.idle)

    # contiguous series over the whole run
    rss_timeline = None
    if rss_bins:
        first_bin, last_bin = min(rss_bins), max(rss_bins)
        live: dict[int, int] = {}
        totals: list[int] = []
        peaks: list[int] = []
        for bin_index in range(first_bin, last_bin + 1):
            live.update(rss_bins.get(bin_index, {}))
            totals.append(sum(live.values()))
            peaks.append(max(live.values()) if live else 0)
        # keep the series small on long runs by taking every n-th bin
        stride = max(1, math.ceil(len(totals) / RSS_TIMELINE_MAX_BINS))
        rss_timeline = {
            "bin_seconds": RSS_TIMELINE_BIN_S * stride,
            "first_bin_us": first_bin * int(RSS_TIMELINE_BIN_S * 1_000_000),
            "rss_kb_total": totals[::stride],
            "rss_kb_max_node": peaks[::stride],
        }

    def cpu_seconds(interval_begin: dict, interval_end: dict) -> float:
        ticks = ((interval_end.get("utime_ticks", 0) + interval_end.get("stime_ticks", 0))
                 - (interval_begin.get("utime_ticks", 0) + interval_begin.get("stime_ticks", 0)))
        return max(0.0, ticks / CLK_TCK)

    per_node: dict[str, dict] = {}
    for pid, start in steady_first.items():
        end = steady_last.get(pid, start)
        node_id = start.get("node_id")
        seconds = cpu_seconds(start, end)
        per_node[str(node_id)] = {
            "role": start.get("role"),
            "cpu_seconds": seconds,
            "cpu_percent": (seconds / steady_s * 100.0) if steady_s else 0.0,
            "rss_kb_max": steady_rss_max.get(pid, 0),
            "threads_max": threads_max.get(pid, 0),
        }

    included = {node: values for node, values in per_node.items() if values["role"] not in exclude_roles}
    total_cpu = sum(values["cpu_seconds"] for values in per_node.values())
    total_cpu_excluded = sum(values["cpu_seconds"] for values in included.values())
    busiest_node = max(included.items(), key=lambda item: item[1]["cpu_seconds"], default=(None, {}))

    busiest_thread = {"node_id": None, "name": None, "cpu_percent": 0.0}
    for pid, ends in thread_last.items():
        starts = thread_first.get(pid, {})
        for thread_id, (name, ticks) in ends.items():
            seconds = max(0.0, (ticks - starts.get(thread_id, ticks)) / CLK_TCK)
            percent = (seconds / steady_s * 100.0) if steady_s else 0.0
            if percent > busiest_thread["cpu_percent"]:
                busiest_thread = {"node_id": steady_last[pid].get("node_id"), "name": name,
                                  "cpu_percent": percent, "tid": thread_id}

    idle_pids = [pid for pid, first in idle_first.items() if first.get("role") not in exclude_roles]
    idle_total_cpu = sum(cpu_seconds(idle_first[pid], idle_last.get(pid, idle_first[pid]))
                     for pid in idle_pids)
    host_busy = None
    if host_first and host_last:
        total_ticks = host_last["cpu_total_ticks"] - host_first["cpu_total_ticks"]
        idle_ticks = host_last["cpu_idle_ticks"] - host_first["cpu_idle_ticks"]
        host_busy = (1.0 - idle_ticks / total_ticks) if total_ticks > 0 else None

    non_system: dict[str, dict] = {}
    for key, first_sample in other_first.items():
        group, _pid = key
        seconds = cpu_seconds(first_sample, other_last.get(key, first_sample))
        entry = non_system.setdefault(group, {"cpu_seconds": 0.0, "rss_kb_max": 0, "processes": 0,
                                              "roles": []})
        entry["cpu_seconds"] += seconds
        entry["rss_kb_max"] = max(entry["rss_kb_max"], other_rss_max.get(key, 0))
        entry["processes"] += 1
        role = other_role.get(key)
        if role and role not in entry["roles"]:
            entry["roles"].append(role)
    for entry in non_system.values():
        entry["cpu_percent"] = (entry["cpu_seconds"] / steady_s * 100.0) if steady_s else 0.0

    return {
        "per_node": per_node,
        "non_system": non_system,
        "cpu_seconds_total": total_cpu_excluded,
        "cpu_seconds_excl_sink": total_cpu_excluded,  # kept for old plot code, same value as cpu_seconds_total
        "cpu_percent_mean_node": (sum(v["cpu_percent"] for v in included.values()) / len(included))
                                 if included else 0.0,
        "cpu_percent_max_node": busiest_node[1].get("cpu_percent", 0.0),
        "busiest_node_id": busiest_node[0],
        "busiest_thread": busiest_thread,
        "rss_kb_total_max": sum(v["rss_kb_max"] for v in included.values()),
        "rss_kb_max_node": max((v["rss_kb_max"] for v in included.values()), default=0),
        "rss_kb_mean_node": (sum(v["rss_kb_max"] for v in included.values()) / len(included))
                            if included else 0.0,
        "threads_total_max": sum(v["threads_max"] for v in included.values()),
        "rss_timeline": rss_timeline,
        "host_cpu_busy_fraction": host_busy,
        "idle": {
            "cpu_seconds": idle_total_cpu,
            "cpu_seconds_per_node_second": (idle_total_cpu / (len(idle_pids) * idle_s))
                                           if idle_pids and idle_s else 0.0,
            "rss_kb_total": sum(idle_rss_max[pid] for pid in idle_pids),
            "nodes": len(idle_pids),
        },
    }


def merge_message_buckets(first_bucket: dict, second_bucket: dict) -> dict:
    merged = {"sent": first_bucket["sent"] + second_bucket["sent"],
              "received": first_bucket["received"] + second_bucket["received"],
              "bytes_sent": first_bucket["bytes_sent"] + second_bucket["bytes_sent"],
              "bytes_received": first_bucket["bytes_received"] + second_bucket["bytes_received"],
              "by_payload_type": {}, "by_cc_msg_type": {}, "by_gossip_msg_type": {}}
    for bucket in (first_bucket, second_bucket):
        for payload_type, counts in bucket["by_payload_type"].items():
            target = merged["by_payload_type"].setdefault(
                payload_type, {"sent": 0, "received": 0, "bytes_sent": 0, "bytes_received": 0})
            for field, value in counts.items():
                target[field] += value
        for key in ("by_cc_msg_type", "by_gossip_msg_type"):
            for message_type, counts in bucket[key].items():
                target = merged[key].setdefault(
                    message_type, {"sent": 0, "received": 0, "bytes_sent": 0, "bytes_received": 0})
                # metrics written before subtype bytes were recorded hold a bare count
                if isinstance(counts, dict):
                    for field, value in counts.items():
                        target[field] = target.get(field, 0) + value
                else:
                    target["sent"] += counts
    return merged


# metrics from the sender
def send_metrics(send_dir: Path, system: str) -> dict:
    send_summary_path = send_dir / f"{system}.summary.json"
    send_summary = None
    if send_summary_path.exists():
        try:
            send_summary = json.loads(send_summary_path.read_text())
        except (OSError, json.JSONDecodeError):
            send_summary = None

    send_log_path = send_dir / f"{system}.log"
    if not send_log_path.exists():
        return {"events_sent": 0, "events_no_route": 0, "events_error": 0, "bytes_sent": 0, "cases": 0,
                "no_send_log": True,
                "warning": f"no send log at {send_log_path}: expected for an idle only run, "
                           f"a bug in any run that was supposed to send events"}

    counts = {"sent": 0, "no_route": 0, "error": 0}
    bytes_sent = 0
    cases: set[str] = set()
    for row in read_jsonl(send_log_path):
        status = str(row.get("status", ""))
        if status == "sent":
            counts["sent"] += 1
            bytes_sent += int(row.get("bytes", 0))
        elif status == "no_route":
            counts["no_route"] += 1
        elif status.startswith("error"):
            counts["error"] += 1
        if row.get("case_id"):
            cases.add(row["case_id"])

    send_summary_out = {"events_sent": counts["sent"], "events_no_route": counts["no_route"],
           "events_error": counts["error"], "bytes_sent": bytes_sent, "cases": len(cases)}
    if send_summary:
        pacer = send_summary.get("pacer", {})
        send_summary_out.update({
            "offered_rate": pacer.get("target_rate"),
            "effective_rate": pacer.get("effective_rate"),
            "pacer_late": pacer.get("late"),
            "pacer_max_lag_ms": pacer.get("max_lag_ms"),
            "pacer_resyncs": pacer.get("resyncs"),
            "sender_interrupted": send_summary.get("interrupted"),
            "blocked_sends_over_1ms": (send_summary.get("sends") or {}).get("blocked_over_1ms"),
            "max_send_us": (send_summary.get("sends") or {}).get("max_send_us"),
            "send_log_spills": (send_summary.get("send_log") or {}).get("spills"),
            "send_log_spill_us_total": (send_summary.get("send_log") or {}).get("spill_us_total"),
            "send_log_max_spill_us": (send_summary.get("send_log") or {}).get("max_spill_us"),
            "send_log_peak_buffer": (send_summary.get("send_log") or {}).get("peak_buffer"),
            "pacer_burst": pacer.get("burst"),
            "pacer_burst_seconds": pacer.get("burst_seconds"),
        })
    else:
        send_summary_out["warning"] = "no send summary json (run with a send script that supports --json-summary)"
    return send_summary_out


# 
def system_metrics(run_dir: Path, timeline: dict, params: dict, args) -> tuple[dict, dict]:
    system = timeline["system"]
    idle_only_run = bool(params.get("idle_only"))
    windows = Windows(timeline, args.warmup_fraction, args.cooldown_fraction)
    if windows.load_source == "process" and not idle_only_run:
        print(f"  !! {system}: no pacer_start_us/pacer_end_us in the timeline, using the sender "
              f"process bounds instead. the load window then also contains the payload preload and "
              f"the send log flush, during which the nodes are idle, so every cpu percentage and "
              f"the steady window are diluted. re-run with the current send_event_stream.py",
              file=sys.stderr)
    node_roles = {node["node_id"]: node["role"] for node in timeline.get("nodes", [])}
    observation_fraction = args.observation_case_fraction
    case_limit = None if observation_fraction is None or observation_fraction >= 1.0 else max(0.0, observation_fraction)

    scan = Scan(windows, node_roles, None)
    records_dir = run_dir / "records" / system
    record_files = sorted(records_dir.glob("node-*.jsonl"))
    reporter = getattr(args, "_progress", None)
    total_bytes = sum(record_file.stat().st_size for record_file in record_files) or 1
    read_bytes = 0
    for index, record_file in enumerate(record_files, start=1):
        for line in read_jsonl(record_file):
            scan.feed(line)
        read_bytes += record_file.stat().st_size
        if reporter is not None:
            reporter.note(f"{run_dir.name} {system}: records {index}/{len(record_files)} ({read_bytes / total_bytes * 100:.0f}% of {total_bytes / 1e6:.0f}MB)")

    end_event_arrival: dict[str, int] = {}
    for _, cases in scan.case_end_seen.items():
        for case_id, sighting in cases.items():
            if sighting["source"] != "end_event":
                continue
            previous = end_event_arrival.get(case_id)
            if previous is None or sighting["t_us"] < previous:
                end_event_arrival[case_id] = sighting["t_us"]

    # --- latency: select a pair by the arrival of its triggering event, follow it to the end
    routing, checking, detection, provisional, end_to_end = [], [], [], [], []
    case_end_verdict = []
    revision_histogram: dict[str, int] = {} # number of revisions -> how many results had this many revisions
    multi_revision_pairs_count = 0
    final_after_send_end = 0
    highest_revision = 0
    verdicts_by_trigger: dict[str, int] = {} # trigger -> number of (case, constraint) pairs

    for (case_id, constraint_id), revisions in scan.verdicts.items():
        ordered = sorted(revisions.items())
        first_revision, (t3, _r, trigger, instance) = ordered[0]
        last_revision, (t4, _r2, _t2, _i2) = ordered[-1]
        revision_count = len(ordered)
        revision_histogram[str(revision_count)] = revision_histogram.get(str(revision_count), 0) + 1
        if revision_count > 1:
            multi_revision_pairs_count += 1
        highest_revision = max(highest_revision, last_revision)
        verdicts_by_trigger[str(trigger)] = verdicts_by_trigger.get(str(trigger), 0) + 1
        if windows.load[1] and t4 > windows.load[1]:
            final_after_send_end += 1
        if windows.contains(windows.steady, t3):
            provisional.append(t4 - t3)

        if instance and instance in scan.t1:
            t1 = scan.t1[instance]
            if not windows.contains(windows.steady, t1):
                continue
            detection.append(t3 - t1)
            end_to_end.append(t4 - t1)
            if instance in scan.t2:
                routing.append(scan.t2[instance] - t1)
                checking.append(t3 - scan.t2[instance])
        elif trigger == "case_end":
            # no triggering event, so it is measured from the arrival of the end activity
            arrival = end_event_arrival.get(case_id)
            if arrival is not None and windows.contains(windows.steady, arrival):
                case_end_verdict.append(t4 - arrival)

    # --- gossip dissemination: time from the end event arriving to each node learning about it
    constraint_nodes = [node_id for node_id, role in node_roles.items() if role in ("constraint_node", "central")]
    constraint_node_ids = set(constraint_nodes)

    dissemination = []
    by_source = {"end_event": 0, "push_gossip": 0, "anti_entropy": 0}
    reach: dict[str, int] = {} # case id -> number of constraint nodes reached by the cases end notification
    sightings_outside_the_system = 0 # sightings at nodes holding no constraint role (the sink)
    for node, cases in scan.case_end_seen.items():
        if node not in constraint_node_ids:
            sightings_outside_the_system += len(cases)
            continue
        for case_id, sighting in cases.items():
            by_source[sighting["source"]] = by_source.get(sighting["source"], 0) + 1
            reach[case_id] = reach.get(case_id, 0) + 1
            origin = end_event_arrival.get(case_id)
            if origin is not None and sighting["source"] != "end_event":
                dissemination.append(sighting["t_us"] - origin)

    reach_fraction = None
    if reach and constraint_nodes:
        reach_fraction = (sum(reach.values()) / len(reach)) / len(constraint_nodes)

    cases_seen = {case_id for _instance, (case_id, _event) in scan.instance_to_case_event.items()}

    # --- throughput
    send = send_metrics(run_dir / "send", system)
    steady_duration_s = windows.seconds(windows.steady)
    def summarise_queues(samples_by_queue: dict, node_max_by_queue: dict) -> dict:
        out = {}
        for queue_name, samples in samples_by_queue.items():
            depth_values = [depth_value for _t, depth_value, _capacity in samples]
            capacities = {capacity for _t, _d, capacity in samples if capacity}
            utilizations = [depth_value / capacity for _t, depth_value, capacity in samples if capacity]
            per_node_max = node_max_by_queue.get(queue_name, {})
            busiest = max(per_node_max.items(), key=lambda item: item[1], default=(None, 0))
            out[queue_name] = {**percentiles(depth_values, (50, 95, 99)),
                               "capacity_min": min(capacities) if capacities else None,
                               "capacity_max": max(capacities) if capacities else None,
                               "max_utilization": max(utilizations) if utilizations else 0.0,
                               "busiest_node_id": busiest[0],
                               "busiest_node_max_depth": busiest[1],
                               "nodes_reporting": len(per_node_max),
                               "slope_per_s": slope_per_second([(t, depth_value)
                                                                for t, depth_value, _c in samples])}
        return out

    def summarise_depths(depth_values: list[int]) -> dict:
        summary = percentiles(depth_values, (50, 95, 99))
        summary["backlog_fraction"] = ((sum(1 for depth in depth_values if depth > 0) / len(depth_values))
                                       if depth_values else 0.0)
        return summary

    pooled_depths = [depth for depths in scan.dequeue_depths.values() for depth in depths]
    per_node_depths = {node_id: summarise_depths(depths) for node_id, depths in scan.dequeue_depths.items()}
    busiest_dequeue_node = max(per_node_depths, key=lambda node_id: (per_node_depths[node_id].get("p95", 0),
                                                                     per_node_depths[node_id].get("max", 0)),
                               default=None)
    dequeue_depth_out = {
        **summarise_depths(pooled_depths),
        "nodes_reporting": len(scan.dequeue_depths),
        "busiest_node_id": busiest_dequeue_node,
        "busiest_node": per_node_depths.get(busiest_dequeue_node),
    }

    # one entry per (node, queue) that was sampled, sorted by standing depth
    def summarise_queues_by_node(series_by_key: dict) -> list[dict]:
        out = []
        for (queue_name, node_id), samples in series_by_key.items():
            samples = sorted(samples)
            capacity = max((capacity for _t, _d, capacity in samples if capacity), default=0)
            depths = [depth for _t, depth, _c in samples]
            if not depths or not capacity:
                continue
            span_s = (samples[-1][0] - samples[0][0]) / 1e6
            slope = slope_per_second([(t, depth) for t, depth, _c in samples])
            out.append({
                "queue": queue_name,
                "node_id": node_id,
                "capacity": capacity,
                "samples": len(samples),
                "window_s": span_s,
                "p50_fill": quantile(sorted(depths), 0.50) / capacity,
                "p95_fill": quantile(sorted(depths), 0.95) / capacity,
                "p99_fill": quantile(sorted(depths), 0.99) / capacity,
                "peak_fill": max(depths) / capacity,
                "slope_per_s": slope,
                # fraction of capacity the fitted line gains from the start of the window to its end
                "growth_over_window": (slope * span_s / capacity) if span_s > 0 else None,
            })
        return sorted(out, key=lambda entry: -entry["p95_fill"])

    queues_by_node_out = summarise_queues_by_node(scan.queues_by_node)
    queues_out = summarise_queues(scan.queues, scan.queue_node_max)
    queues_settle_out = summarise_queues(scan.queues_settle, scan.queue_node_max_settle)

    closed_case_message_drops = scan.drops["by_reason"].get(CASE_CLOSED_REASON, 0)
    closed_case_event_drops = scan.event_drops["by_reason"].get(CASE_CLOSED_REASON, 0)
    closed_case_drops = closed_case_message_drops + closed_case_event_drops

    settle_pressure = [f"{queue_name} at {queue_stats['max_utilization']:.0%} of capacity during settle"
                       for queue_name, queue_stats in queues_settle_out.items()
                       if queue_stats.get("max_utilization", 0.0) > 0.5]

    # --- messages
    load_messages = scan.messages["load"]
    settle_messages = scan.messages["settle"]
    idle_messages = scan.messages["idle"]
    workload_messages = merge_message_buckets(load_messages, settle_messages)

    events = send.get("events_sent") or len(scan.t1) or 1
    cases_count = send.get("cases") or len(cases_seen) or 1
    idle_seconds = windows.seconds(windows.idle)
    settle_seconds = windows.seconds(windows.settle)
    # exclude sink since its not part of the overlay
    idle_nodes = max(1, sum(1 for node_id in scan.nodes_seen if node_roles.get(node_id) != "sink"))

    def sum_all(field: str, payloads) -> int:
        return sum(counts[field] for payload_type, counts in workload_messages["by_payload_type"].items() if payload_type in payloads)

    coordination_sent = sum_all("sent", COORDINATION_PAYLOADS)
    coordination_bytes = sum_all("bytes_sent", COORDINATION_PAYLOADS)
    transport_sent = sum_all("sent", TRANSPORT_PAYLOADS)
    transport_bytes = sum_all("bytes_sent", TRANSPORT_PAYLOADS)
    sink_sent = sum_all("sent", SINK_PAYLOADS)
    sink_bytes = sum_all("bytes_sent", SINK_PAYLOADS)
    payload_sent = workload_messages["sent"] - sink_sent
    payload_bytes = workload_messages["bytes_sent"] - sink_bytes

    resources = resource_metrics(run_dir / "resources" / f"{system}.jsonl", windows)

    queue_profile = [{"node": f"node-{entry['node_id']}", "queue": entry["queue"],
                      "capacity": entry["capacity"], "p95_fill": entry["p95_fill"],
                      "p99_fill": entry["p99_fill"], "growth": entry["growth_over_window"],
                      "slope_per_s": entry["slope_per_s"]}
                     for entry in queues_by_node_out]
    cpu_profile = sorted(({"node": f"{values.get('role')}/{node_id}",
                           "cpu_pct": values.get("cpu_percent") or 0.0,
                           "rss_mb": (values.get("rss_kb_max") or 0) / 1024}
                          for node_id, values in (resources.get("per_node") or {}).items()
                          if values.get("role") != "sink"),
                         key=lambda entry: -entry["cpu_pct"])
    busiest = cpu_profile[0] if cpu_profile else {}
    facts = {
        "input_drops": scan.drops["total"] + scan.event_drops["total"],
        "input_drop_reasons": {**{f"message_drop/{reason}": count
                                  for reason, count in scan.drops["by_reason"].items()},
                               **{f"dropped_event/{reason}": count
                                  for reason, count in scan.event_drops["by_reason"].items()}},
        "input_drops_by_node": {**(scan.drops.get("by_node") or {}),
                                **(scan.event_drops.get("by_node") or {})},
        "queue_profile": queue_profile,
        "sys_cpu_profile": cpu_profile,
        "sys_cpu_max_node": busiest.get("node"),
        "achieved_ratio": ((send.get("achieved_rate") / send["offered_rate"])
                           if send.get("achieved_rate") and send.get("offered_rate") else None),
        "max_send_ms": ((send.get("max_send_us") / 1000.0)
                        if send.get("max_send_us") is not None else None),
        **worst_queue(queue_profile),
    }
    facts.update(cpu_ceiling(timeline.get("cpu_cap") or {},
                             {"sys_cpu_max_pct": busiest.get("cpu_pct"),
                              "sys_cpu_sum_pct": sum(entry["cpu_pct"] for entry in cpu_profile)}))
    # an idle-only run offers no load so there is nothing for the saturation rules
    decision = ({"saturated": False, "reasons": []} if idle_only_run
                else saturation_signals(facts))
    saturation_reasons = list(decision["reasons"])
    if send.get("no_send_log") and not idle_only_run:
        saturation_reasons.append("no send log: the sender never ran, so every per event metric "
                                  "is meaningless")

    metrics = {
        "windows": windows.as_dict(),
        "nodes": {"total": len(node_roles),
                  "by_role": {role: sum(1 for node_role in node_roles.values() if node_role == role) for role in set(node_roles.values())},
                  "record_files": len(record_files)},
        "throughput": {
            **send,
            "achieved_rate_steady": (len([event_input_time for event_input_time in scan.t1.values() if windows.contains(windows.steady, event_input_time)]) / steady_duration_s)
                                    if steady_duration_s else 0.0,
            "events_with_input_record": len(scan.t1),
            "message_drops": scan.drops,
            "event_drops": scan.event_drops,
            "closed_case_drops": closed_case_drops,
            "closed_case_event_drops": closed_case_event_drops,
            "closed_case_message_drops": closed_case_message_drops,
            "queues": queues_out,
            "queues_by_node": queues_by_node_out,
            "queues_settle": queues_settle_out,
            "settle_queue_pressure": settle_pressure,
            "event_q_at_dequeue": dequeue_depth_out,
            "tcp_channel_queue_max": scan.tcp_queue_max,
            "saturated": bool(saturation_reasons),
            "saturation_reasons": saturation_reasons,
        },
        "latency_us": {
            "routing_t1_t2": percentiles(routing),
            "checking_t2_t3": percentiles(checking),
            "detection_t1_t3": percentiles(detection),
            "provisional_t3_t4": percentiles(provisional),
            "end_to_end_t1_t4": percentiles(end_to_end),
            "case_end_verdict": percentiles(case_end_verdict),
            "gossip_dissemination": {**percentiles(dissemination), "by_source": by_source, "reach_fraction": reach_fraction, "constraint_nodes": len(constraint_nodes),
                                     "cases_with_sightings": len(reach), "sightings_outside_the_system": sightings_outside_the_system},
            "handler_duration_us": percentiles(scan.handling_us),
            "parse_us": percentiles(scan.parse_us),
            "serde_us": percentiles(scan.serde_us),
        },
        "revisions": {
            "pairs": len(scan.verdicts),
            "pairs_with_multiple": multi_revision_pairs_count,
            "max_on_a_pair": max((len(revision) for revision in scan.verdicts.values()), default=0),
            "revisions_total": sum(len(revision) for revision in scan.verdicts.values()),
            "histogram": revision_histogram,
            "highest_revision_number": highest_revision,
            "by_trigger": verdicts_by_trigger,
            "final_after_send_end": final_after_send_end,
            "result_drops": scan.result_drops,
            "correction_drops": {
                **scan.correction_drops,
                "affected_pairs": len(scan.correction_drop_pairs),
            },
        },
        "resources": {
            **resources,
            "cpu_seconds_per_event": (resources["cpu_seconds_excl_sink"] / events) if events else None,
        },
        "messages": {
            "load": load_messages,
            "settle": settle_messages,
            "workload": workload_messages,
            "idle": idle_messages,
            "derived": {
                "messages_per_event": workload_messages["sent"] / events,
                "bytes_per_event": workload_messages["bytes_sent"] / events,
                "messages_per_event_excl_sink": payload_sent / events,
                "bytes_per_event_excl_sink": payload_bytes / events,
                "messages_per_event_coordination": coordination_sent / events,
                "bytes_per_event_coordination": coordination_bytes / events,
                "messages_per_event_transport": transport_sent / events,
                "bytes_per_event_transport": transport_bytes / events,
                "messages_per_event_sink": sink_sent / events,
                "bytes_per_event_sink": sink_bytes / events,
                "mean_bytes_per_coordination_message": (coordination_bytes / coordination_sent) if coordination_sent else 0.0,
                "mean_bytes_per_transport_message": (transport_bytes / transport_sent) if transport_sent else 0.0,
                "messages_per_case": workload_messages["sent"] / cases_count,
                "settle_share_of_messages": (settle_messages["sent"] / workload_messages["sent"]) if workload_messages["sent"] else 0.0,
                "settle_share_of_bytes": (settle_messages["bytes_sent"] / workload_messages["bytes_sent"]) if workload_messages["bytes_sent"] else 0.0,
                "idle_overlay_bytes_during_settle": (idle_messages["bytes_sent"] / idle_seconds * settle_seconds) if idle_seconds else 0.0,
                "coordination_ratio": (coordination_sent / payload_sent) if payload_sent else 0.0,
                "idle_bytes_per_second": (idle_messages["bytes_sent"] / idle_seconds) if idle_seconds else 0.0,
                "idle_bytes_per_node_second": (idle_messages["bytes_sent"] / (idle_seconds * idle_nodes)) if idle_seconds else 0.0,
            },
            "peer_churn": scan.peer_churn,
        },
        "quality": {
            "record_files": len(record_files),
            "writer_shutdown_missing": sorted(scan.nodes_seen - scan.writer_shutdown),
            "complete": not (scan.nodes_seen - scan.writer_shutdown),
            "problems": timeline.get("problems", []),
            "idle_only": idle_only_run,
            "no_send_log": bool(send.get("no_send_log")),
            "settlement_reason": timeline.get("system_settlement_reason"),
            "settlement_seconds": timeline.get("system_settlement_seconds"),
            "seconds_to_all_expected": timeline.get("seconds_to_all_expected"),
            "pairs_in_db": timeline.get("pairs_in_db"),
            "expected_pairs": timeline.get("expected_pairs"),
            "hard_killed_nodes": timeline.get("hard_killed_nodes") or [],
            "sender_preload_s": timeline.get("sender_preload_s"),
            "sender_teardown_s": timeline.get("sender_teardown_s"),
            "node_roles": {str(node_id): role for node_id, role in node_roles.items()},
        },
        "cpu_cap": timeline.get("cpu_cap"),
    }

    # --- observations for compute_privacy.py
    nodes = load_system_configs(run_dir / "sys" / system / "configs")
    # which activities each node watches
    metrics["placement"] = {str(node.node_id): {"role": node.role,
                                                "watches": sorted(node.event_ids or []),
                                                "constraints": len(node.constraints or [])}
                            for node in nodes}
    observations = build_observations(system, nodes, scan, case_limit)
    return metrics, observations


# collect observations from record scan for later privacy evaluation
def build_observations(system: str, nodes, scan: Scan, case_limit: int | None) -> dict:
    peer_address_to_node_id: dict[str, int] = {} # address -> node id
    for node in nodes:
        if node.tcp:
            peer_address_to_node_id[node.tcp] = node.node_id
        if node.overlay:
            peer_address_to_node_id[node.overlay] = node.node_id

    # make sure only the desired case fraction is used if a case limit is configured
    def keep(case_id: str) -> bool:
        return case_limit is None or case_id_hash_fraction(case_id) < case_limit

    # what a node learns locally
    local = {str(node): {case: sorted(events) for case, events in cases.items() if keep(case)}
             for node, cases in scan.local.items()}

    # what a node learns from coordination traffic
    inferred_partners: dict[str, dict[str, list[list]]] = {} # node id -> (case -> [[constraint, partner node]])
    unresolved_peers: set = set()
    resolved_pairs = 0
    for node, observed in scan.msg_case_constraint.items():
        per_case: dict[str, set[tuple]] = {}
        for case_id, constraint_id, peer_address in observed:
            if not keep(case_id):
                continue
            partner = peer_address_to_node_id.get(peer_address)
            if partner is None:
                unresolved_peers.add(peer_address)
                continue
            if partner == node:
                continue
            per_case.setdefault(case_id, set()).add((constraint_id, partner))
            resolved_pairs += 1
        inferred_partners[str(node)] = {case: sorted([list(pair) for pair in pairs])
                                        for case, pairs in per_case.items()}

    coordination_quality = {"observed_pairs": sum(len(observed) for observed in scan.msg_case_constraint.values()),
                            "resolved_pairs": resolved_pairs,
                            "messages_without_peer_address": scan.messages_without_peer,
                            "unresolved_peer_addresses": sorted(str(peer) for peer in unresolved_peers)[:10]}
    # gossip without a constraint id still reveals that the case exists
    cases_known_to_exist: dict[str, list[str]] = {} # node id -> [case ids]
    for node, cases in scan.msg_case_only.items():
        cases_known_to_exist[str(node)] = sorted(case for case in cases if keep(case))

    case_end = {str(node): cases for node, cases in scan.case_end_seen.items()}

    # which activities each node watches and which constraint roles it carries
    placement = {str(node.node_id): {"role": node.role,
                                     "watches": sorted(node.event_ids or []),
                                     "constraints": [{"id": c.get("id"), "type": c.get("type"),
                                                      "event_id": c.get("event_id"),
                                                      "role": c.get("role")}
                                                     for c in (node.constraints or [])]}
                 for node in nodes}

    node_roles = {node.node_id: node.role for node in nodes}
    if system == "central":
        attacker_node_pool = [node_id for node_id, node_role in node_roles.items() if node_role in ("central", "forwarder")]
    else:
        attacker_node_pool = [node_id for node_id, role in node_roles.items() if role == "constraint_node"]

    return {
        "system": system,
        "attacker_pool": sorted(attacker_node_pool),
        "node_roles": {str(node_id): node_role for node_id, node_role in node_roles.items()},
        "node_placement": placement,
        "peer_to_node": peer_address_to_node_id,
        "coordination_quality": coordination_quality,
        "local_events": local,
        "inferred_partners": inferred_partners,
        "cases_known_to_exist": cases_known_to_exist,
        "case_end_seen": case_end,
        "case_end_truth": {case: node for case, node in scan.case_end_truth.items()
                           if keep(case)},
        "case_limit_fraction": case_limit,
    }


# outputs of the evaluation itself (to exclude from signature)
GENERATED = ("metrics.json", "observations.json.gz")


def input_signature(run_dir: Path) -> str:
    parts = []
    for path in sorted(run_dir.rglob("*")):
        if path.is_file() and path.name not in GENERATED and not path.name.startswith("privacy"):
            try:
                parts.append(f"{path.relative_to(run_dir)}:{path.stat().st_size}")
            except OSError:
                continue
    return hashlib.blake2b("|".join(parts).encode(), digest_size=16).hexdigest()


def process_run(run_dir: Path, args) -> str:
    run_spec_path = run_dir / "run.json"
    if not run_spec_path.exists():
        return f"skip {run_dir}: no run.json"
    run_spec = json.loads(run_spec_path.read_text())
    metrics_out_path = run_dir / "metrics.json"
    signature = input_signature(run_dir)

    if metrics_out_path.exists() and not args.force:
        try:
            existing = json.loads(metrics_out_path.read_text())
            if (existing.get("provenance", {}).get("input_signature") == signature
                    and existing.get("schema_version") == SCHEMA_VERSION):
                return f"up to date {run_dir.name}"
        except (OSError, json.JSONDecodeError):
            pass

    params = run_spec.get("params", {})
    metrics = {
        "schema_version": SCHEMA_VERSION,
        "experiment": run_spec.get("experiment"),
        "run_id": run_spec.get("run_id"),
        "replicate": run_spec.get("replicate"),
        "params": params,
        "host": run_spec.get("host"),
        "cpu_count": run_spec.get("cpu_count"),
        "order_index": run_spec.get("order_index"),
        "provenance": {"input_signature": signature, "extractor": "extract_metrics.py",
                       "git": run_spec.get("git") or {}},
        "correctness": run_spec.get("compare_summary"),
        "status": run_spec.get("status"),
        "problems": run_spec.get("problems") or [],
        "systems": {},
    }
    observations = {"schema_version": SCHEMA_VERSION, "run_id": run_spec.get("run_id"),
                    "experiment": run_spec.get("experiment"), "params": params, "systems": {}}

    for timeline in run_spec.get("systems_timeline", []):
        system_metrics_out, observation = system_metrics(run_dir, timeline, params, args)
        metrics["systems"][timeline["system"]] = system_metrics_out
        observations["systems"][timeline["system"]] = observation

    metrics_out_path.write_text(json.dumps(metrics, indent=2, default=str))
    if not args.no_observations:
        with gzip.open(run_dir / "observations.json.gz", "wt") as handle:
            json.dump(observations, handle, default=str)

    index_note = ""
    if args.disclosure_index != "skip":
        manifest = (run_spec.get("stream") or {}).get("manifest")
        bk_types = None if args.bk_type == ["auto"] else tuple(args.bk_type)
        index_path, action = ensure_index(Path(manifest) if manifest else None, bk_types=bk_types,
                                          force=args.disclosure_index == "force",
                                          uniqueness_max=args.disclosure_uniqueness_max,
                                          log=None)
        if action == "built":
            index_note = f" (+ {index_path.name})"
        elif action == "skipped" and manifest:
            index_note = f" (no {DEFAULT_NAME}: {manifest} is gone)"

    return (f"wrote {metrics_out_path.relative_to(args.results) if args.results else metrics_out_path}"
            f"{index_note}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--results", type=Path, help="results directory to walk")
    parser.add_argument("--run", type=Path, action="append", default=[], help="a single run directory (repeatable)")
    parser.add_argument("--experiment", default=None, help="only experiments matching this glob")
    parser.add_argument("--force", action="store_true", help="re-extract even if up to date")
    parser.add_argument("--jobs", type=int, default=max(1, (os.cpu_count() or 2) // 2))
    parser.add_argument("--warmup-fraction", type=float, default=0.1, help="fraction of the load window dropped at the start (default 0.1)")
    parser.add_argument("--cooldown-fraction", type=float, default=0.0, help="fraction dropped at the end (default 0.0, latency is selected by t1)")
    parser.add_argument("--observation-case-fraction", type=float, default=1.0,
                        help="keep this fraction of cases in observations.json.gz, chosen by a hash of the case id so the same cases are kept in both systems "
                        "and across re-runs (default 1.0 = every case)")
    parser.add_argument("--no-observations", action="store_true", help="skip observations.json.gz (metrics only)")
    parser.add_argument("--build-index", type=Path, action="append", default=[], metavar="MANIFEST",
                        help=f"build {DEFAULT_NAME} for these stream manifests and exit, without extracting "
                             f"anything (repeatable). the same thing extraction does on its own, exposed "
                             f"for building an index for a stream up front")
    parser.add_argument("--disclosure-index", choices=("auto", "skip", "force"), default="auto",
                        help=f"build {DEFAULT_NAME} next to the stream manifest if it is not "
                             f"already there (default auto). it holds the compacted ground truth log and "
                             f"the trace variant index that compute_privacy.py needs, so once it exists "
                             f"the stream directory can be deleted and privacy can be recomputed with a "
                             f"new seed as often as you like")
    parser.add_argument("--bk-type", nargs="+", default=["auto"],
                        help="background knowledge types to index (set, mult, seq), or 'auto' (default) to "
                             "index only when the log is not already almost all unique traces")
    parser.add_argument("--disclosure-uniqueness-max", type=float, default=0.9,
                        help="under 'auto', skip the variant index when trace uniqueness is above this "
                             "(default 0.9). on synthetic streams nearly every trace is its own variant, "
                             "so the disclosure measures would describe the generator, not the deployment")
    parser.add_argument("--quiet", action="store_true", help="no progress output")
    args = parser.parse_args()

    if args.build_index:
        bk_types = None if args.bk_type == ["auto"] else tuple(args.bk_type)
        for manifest in args.build_index:
            path, action = ensure_index(manifest, bk_types=bk_types,
                                        force=args.disclosure_index == "force",
                                        uniqueness_max=args.disclosure_uniqueness_max,
                                        log=lambda text: print(text.strip()))
            if action == "existing":
                print(f"{path}: already there (--disclosure-index force to rebuild)")
            elif action == "skipped":
                print(f"{manifest}: not found or empty, nothing written")
        return

    runs = list(args.run)
    if args.results:
        runs += discover_runs(args.results, args.experiment, "run.json")
    if not runs:
        sys.exit("nothing to do: pass --results and/or --run")

    progress = Progress(len(runs), "extracting", enabled=not args.quiet)
    if args.jobs > 1 and len(runs) > 1:
        with ProcessPoolExecutor(max_workers=args.jobs) as pool:
            submitted = {pool.submit(process_run, run_dir, args): run_dir for run_dir in runs}
            progress.note(f"{len(runs)} run(s) over {args.jobs} worker(s)")
            for future in as_completed(submitted):
                run_dir = submitted[future]
                progress.log(future.result())
                progress.step(run_dir.name)
    else:
        args._progress = progress
        for run_dir in runs:
            progress.note(run_dir.name)
            message = process_run(run_dir, args)
            progress.log(message)
            progress.step(run_dir.name)
    progress.finish()


if __name__ == "__main__":
    main()