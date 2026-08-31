#!/usr/bin/env python3
"""
extract_metrics.py - extract metrics from the generated files of a simulation run

writes metrics.json:  all one time metrics (throughput, latency, revisions, resources, message overhead, margin, correctness) for each system
writes observations.json.gz:  what each node saw (local events, events inferable from coordination traffic, case end sightings)
    for computing privacy metrics with compute_privacy.py

  python3.11 simulation/extract_metrics.py --results results/
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
from bisect import bisect_left
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from helpers import CLK_TCK, load_system_configs, read_jsonl, quantile, slope_per_second, percentiles, case_id_hash_fraction, discover_runs, Progress

SCHEMA_VERSION = 2

COORDINATION_PAYLOADS = ("conformance", "ce_gossip", "overlay")
CASE_CLOSED_REASON = "case closed"
TRANSPORT_PAYLOADS = ("event")
SINK_PAYLOADS = ("result")


# keeps the time windows of a run (idle, load, steady, settle) and provides some window functions
class Windows:
    def __init__(self, timeline: dict, warmup_fraction: float, cooldown_fraction: float):
        self.idle = (timeline.get("idle_start_us"), timeline.get("idle_end_us"))
        self.load = (timeline.get("send_start_us"), timeline.get("send_end_us"))
        self.settle = (timeline.get("send_end_us"), timeline.get("system_settled_us"))
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
        return {"idle": list(self.idle), "load": list(self.load), "steady": list(self.steady),
                "settle": list(self.settle),
                "steady_seconds": self.seconds(self.steady),
                "load_seconds": self.seconds(self.load),
                "idle_seconds": self.seconds(self.idle),
                "warmup_fraction": self.warmup_fraction, "cooldown_fraction": self.cooldown_fraction}


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
        self.queues: dict[str, list[tuple[float, float]]] = {} # name -> (timestamp, fill) during the load window
        self.queue_node_max: dict[str, dict[int, int]] = {} # queue name -> (node id -> peak depth) # name -> capacity
        self.queues_settle: dict[str, list[tuple[float, float]]] = {}
        self.queue_node_max_settle: dict[str, dict[int, int]] = {}
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
        elif record_type == "dropped_event":
            self.event_drops["total"] += 1
            reason = str(line.get("reason"))
            self.event_drops["by_reason"][reason] = self.event_drops["by_reason"].get(reason, 0) + 1
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
                if value and direction == "sent":
                    bucket[key][value] = bucket[key].get(value, 0) + 1
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
    host_first = host_last = None 

    for row in read_jsonl(path):
        sample_time = row.get("t_us")
        sample_type = row.get("kind")
        if sample_type == "host":
            if windows.contains(windows.steady, sample_time):
                host_first = host_first or row
                host_last = row
            continue
        if row.get("group") != "system":
            continue
        pid = row.get("pid")
        if sample_type == "proc":
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

    idle_total_cpu = sum(cpu_seconds(idle_first[pid], idle_last.get(pid, idle_first[pid]))
                     for pid in idle_first)
    host_busy = None
    if host_first and host_last:
        total_ticks = host_last["cpu_total_ticks"] - host_first["cpu_total_ticks"]
        idle_ticks = host_last["cpu_idle_ticks"] - host_first["cpu_idle_ticks"]
        host_busy = (1.0 - idle_ticks / total_ticks) if total_ticks > 0 else None

    return {
        "per_node": per_node,
        "cpu_seconds_total": total_cpu,
        "cpu_seconds_excl_sink": total_cpu_excluded,
        "cpu_percent_mean_node": (sum(v["cpu_percent"] for v in included.values()) / len(included))
                                 if included else 0.0,
        "cpu_percent_max_node": busiest_node[1].get("cpu_percent", 0.0),
        "busiest_node_id": busiest_node[0],
        "busiest_thread": busiest_thread,
        "rss_kb_total_max": sum(v["rss_kb_max"] for v in per_node.values()),
        "threads_total_max": sum(v["threads_max"] for v in per_node.values()),
        "host_cpu_busy_fraction": host_busy,
        "idle": {
            "cpu_seconds": idle_total_cpu,
            "cpu_seconds_per_node_second": (idle_total_cpu / (len(idle_first) * idle_s))
                                           if idle_first and idle_s else 0.0,
            "rss_kb_total": sum(idle_rss_max.values()),
            "nodes": len(idle_first),
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
            for message_type, count in bucket[key].items():
                merged[key][message_type] = merged[key].get(message_type, 0) + count
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

    counts = {"sent": 0, "no_route": 0, "error": 0}
    bytes_sent = 0
    cases: set[str] = set()
    for row in read_jsonl(send_dir / f"{system}.log"):
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
        })
    else:
        send_summary_out["warning"] = "no send summary json (run with a send script that supports --json-summary)"
    return send_summary_out


# 
def system_metrics(run_dir: Path, timeline: dict, params: dict, args) -> tuple[dict, dict]:
    system = timeline["system"]
    windows = Windows(timeline, args.warmup_fraction, args.cooldown_fraction)
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
    dissemination = []
    by_source = {"end_event": 0, "push_gossip": 0, "anti_entropy": 0}
    reach: dict[str, int] = {} # case id -> number of nodes reached by the cases end notification
    for node, cases in scan.case_end_seen.items():
        for case_id, sighting in cases.items():
            by_source[sighting["source"]] = by_source.get(sighting["source"], 0) + 1
            reach[case_id] = reach.get(case_id, 0) + 1
            origin = end_event_arrival.get(case_id)
            if origin is not None and sighting["source"] != "end_event":
                dissemination.append(sighting["t_us"] - origin)

    constraint_nodes = [node_id for node_id, role in node_roles.items()
                        if role in ("constraint_node", "central")]
    reach_fraction = None
    if reach and constraint_nodes:
        reach_fraction = (sum(reach.values()) / len(reach)) / len(constraint_nodes)

    # --- margin: inter event gap of a case against the observed coordination latency
    gaps = []
    per_case_times: dict[str, list[int]] = {} # case -> [t1 from each instance]
    for instance, (case_id, _event) in scan.instance_to_case_event.items():
        if instance in scan.t1:
            per_case_times.setdefault(case_id, []).append(scan.t1[instance])
    for times in per_case_times.values():
        times.sort()
        gaps += [time_b - time_a for time_a, time_b in zip(times, times[1:])]
    coordination = checking or detection
    gap_p50 = quantile(gaps, 0.5)
    coordination_p50 = quantile(coordination, 0.5)
    negative_fraction = None
    if gaps and coordination:
        ordered_gaps = sorted(gaps)
        negative = sum(1 for value in coordination
                       if bisect_left(ordered_gaps, value) > len(ordered_gaps) / 2)
        negative_fraction = negative / len(coordination)

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

    queues_out = summarise_queues(scan.queues, scan.queue_node_max)
    queues_settle_out = summarise_queues(scan.queues_settle, scan.queue_node_max_settle)

    closed_case_message_drops = scan.drops["by_reason"].get(CASE_CLOSED_REASON, 0)
    closed_case_event_drops = scan.event_drops["by_reason"].get(CASE_CLOSED_REASON, 0)
    closed_case_drops = closed_case_message_drops + closed_case_event_drops
    other_message_drops = scan.drops["total"] - closed_case_message_drops
    other_event_drops = scan.event_drops["total"] - closed_case_event_drops

    saturation_reasons = []
    if (send.get("pacer_max_lag_ms") or 0) > 100:
        saturation_reasons.append("pacer lag over 100ms")
    if closed_case_drops > 0:
        saturation_reasons.append(f"{closed_case_drops} item(s) dropped after the case was closed "
                                  f"({closed_case_event_drops} event(s), {closed_case_message_drops} "
                                  f"message(s)): processing slower than the case end grace period")
    if other_message_drops > 0:
        saturation_reasons.append(f"{other_message_drops} message drops")
    if other_event_drops > 0:
        saturation_reasons.append(f"{other_event_drops} events dropped")
    for queue_name, queue_stats in queues_out.items():
        if queue_stats.get("max_utilization", 0.0) > 0.5:
            saturation_reasons.append(f"{queue_name} above half capacity")
        if queue_stats.get("slope_per_s", 0) > 1.0:
            saturation_reasons.append(f"{queue_name} growing")
    settle_pressure = [f"{queue_name} at {queue_stats['max_utilization']:.0%} of capacity during settle"
                       for queue_name, queue_stats in queues_settle_out.items()
                       if queue_stats.get("max_utilization", 0.0) > 0.5]

    # --- messages
    load_messages = scan.messages["load"]
    settle_messages = scan.messages["settle"]
    idle_messages = scan.messages["idle"]
    workload_messages = merge_message_buckets(load_messages, settle_messages)

    events = send.get("events_sent") or len(scan.t1) or 1
    cases_count = send.get("cases") or len(per_case_times) or 1
    idle_seconds = windows.seconds(windows.idle)
    settle_seconds = windows.seconds(windows.settle)
    idle_nodes = max(1, len(scan.nodes_seen))

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

    metrics = {
        "windows": windows.as_dict(),
        "nodes": {"total": len(node_roles),
                  "by_role": {role: sum(1 for node_role in node_roles.values() if node_role == role)
                              for role in set(node_roles.values())},
                  "record_files": len(record_files)},
        "throughput": {
            **send,
            "achieved_rate_steady": (len([event_input_time for event_input_time in scan.t1.values()
                                          if windows.contains(windows.steady, event_input_time)]) / steady_duration_s)
                                    if steady_duration_s else 0.0,
            "events_with_input_record": len(scan.t1),
            "message_drops": scan.drops,
            "event_drops": scan.event_drops,
            "closed_case_drops": closed_case_drops,
            "closed_case_event_drops": closed_case_event_drops,
            "closed_case_message_drops": closed_case_message_drops,
            "queues": queues_out,
            "queues_settle": queues_settle_out,
            "settle_queue_pressure": settle_pressure,
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
            "gossip_dissemination": {**percentiles(dissemination),
                                     "by_source": by_source,
                                     "reach_fraction": reach_fraction},
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
                "mean_bytes_per_coordination_message": (coordination_bytes / coordination_sent)
                                                       if coordination_sent else 0.0,
                "mean_bytes_per_transport_message": (transport_bytes / transport_sent)
                                                    if transport_sent else 0.0,
                "messages_per_case": workload_messages["sent"] / cases_count,
                "settle_share_of_messages": (settle_messages["sent"] / workload_messages["sent"]) if workload_messages["sent"] else 0.0,
                "settle_share_of_bytes": (settle_messages["bytes_sent"] / workload_messages["bytes_sent"])
                                         if workload_messages["bytes_sent"] else 0.0,
                "idle_overlay_bytes_during_settle": (idle_messages["bytes_sent"] / idle_seconds
                                                     * settle_seconds) if idle_seconds else 0.0,
                "coordination_ratio": (coordination_sent / payload_sent)
                                      if payload_sent else 0.0,
                "idle_bytes_per_second": (idle_messages["bytes_sent"] / idle_seconds)
                                         if idle_seconds else 0.0,
                "idle_bytes_per_node_second": (idle_messages["bytes_sent"]
                                               / (idle_seconds * idle_nodes))
                                              if idle_seconds else 0.0,
            },
            "peer_churn": scan.peer_churn,
        },
        "margin": {
            "inter_event_gap_us": percentiles(gaps, (5, 50, 95)),
            "coordination_latency_us": percentiles(coordination, (50, 95, 99)),
            "margin_p50_us": (gap_p50 - coordination_p50)
                             if gap_p50 is not None and coordination_p50 is not None else None,
            "coordination_slower_than_gap_fraction": negative_fraction,
        },
        "quality": {
            "record_files": len(record_files),
            "writer_shutdown_missing": sorted(scan.nodes_seen - scan.writer_shutdown),
            "complete": not (scan.nodes_seen - scan.writer_shutdown),
            "problems": timeline.get("problems", []),
            "settlement_reason": timeline.get("system_settlement_reason"),
            "pairs_in_db": timeline.get("pairs_in_db"),
            "expected_pairs": timeline.get("expected_pairs"),
        },
        "cpu_cap": timeline.get("cpu_cap"),
    }

    # --- observations for compute_privacy.py
    nodes = load_system_configs(run_dir / "sys" / system / "configs")
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

    node_roles = {node.node_id: node.role for node in nodes}
    if system == "central":
        attacker_node_pool = [node_id for node_id, node_role in node_roles.items() if node_role in ("central", "forwarder")]
    else:
        attacker_node_pool = [node_id for node_id, role in node_roles.items() if role == "constraint_node"]

    return {
        "system": system,
        "attacker_pool": sorted(attacker_node_pool),
        "node_roles": {str(node_id): node_role for node_id, node_role in node_roles.items()},
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
        "provenance": {"input_signature": signature, "extractor": "extract_metrics.py"},
        "correctness": run_spec.get("compare_summary"),
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
    return f"wrote {metrics_out_path.relative_to(args.results) if args.results else metrics_out_path}"


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
    parser.add_argument("--quiet", action="store_true", help="no progress output")
    args = parser.parse_args()

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