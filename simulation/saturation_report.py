#!/usr/bin/env python3
"""
saturation_report.py - aggregate the saturation signals of an experiment per system

  python3.12 simulation/saturation_report.py results/e0_find_saturation
  python3.12 simulation/saturation_report.py results/e0_find_saturation --json out.json --csv out.csv
  python3.12 simulation/saturation_report.py results/e0_find_saturation --x-param rate --cpu-threshold 85

it reads per run and per system:

  run.json               validity, settlement, pairs vs expected, timeline windows
  send/<system>.stdout   pacer summary: effective rate, late, resyncs, blocked_sends, max_send
  send/<system>.log      achieved rate per wall clock second, inter send gap quantiles
  resources/<system>     cpu and rss per process, split into the system under test and the harness
  records/<system>/      queue depths and message drops 
  compare_results.json   correctness of the final verdicts

and then applies a saturation rule to each (system, rate) point. the point saturates if any of:

  blocked_sends > 0                a node stopped reading its socket (the receiver side signal)
  achieved / target < 0.95         the system could not absorb the offered load
  any message_drop record          a queue overflowed
  max queue depth > 25% capacity   a long lived queue is filling up
  busiest system process cpu > 85% a single process is running out of core

the sustainable rate is reported as the highest rate below the first saturating one. 
the sender is reported separately: 
    if the sender itself is above the cpu threshold the point says so, because then the limit measured is the load generator, not the system under test.

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

SYSTEMS = ("decentralized", "central")

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


def quantile(sorted_values: list[float], q: float) -> float | None:
    if not sorted_values:
        return None
    return sorted_values[min(len(sorted_values) - 1, int(q * (len(sorted_values) - 1)))]


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


# parse /proc samples from resources/<system>.json
def parse_resources(run_dir: Path, system: str, window: tuple[int, int] | None) -> dict:

    path = run_dir / "resources" / f"{system}.jsonl"
    if not path.exists():
        return {}
    host: list[dict] = []
    procs: dict[int, list[dict]] = defaultdict(list)
    meta: dict[int, dict] = {}
    for row in read_jsonl(path):
        if window and not (window[0] <= row.get("t_us", 0) <= window[1]):
            continue
        if row.get("kind") == "host":
            host.append(row)
        elif row.get("kind") == "proc":
            procs[row["pid"]].append(row)
            meta[row["pid"]] = {"node_id": row.get("node_id"), "role": row.get("role"),
                                "group": row.get("group", "system")}
    if len(host) < 2:
        return {}
    clock_tick = host[0].get("clk_tck", 100)
    wallclock_time = (host[-1]["t_us"] - host[0]["t_us"]) / 1e6
    if wallclock_time <= 0:
        return {}
    total_ticks = host[-1]["cpu_total_ticks"] - host[0]["cpu_total_ticks"]
    idle_ticks = host[-1]["cpu_idle_ticks"] - host[0]["cpu_idle_ticks"]
    resource_stats: dict = {"host_busy_pct": 100 * (1 - idle_ticks / total_ticks) if total_ticks else None,
                 "window_s": wallclock_time}

    per_group: dict[str, list[tuple[float, int, dict]]] = defaultdict(list)
    for pid, samples in procs.items():
        if len(samples) < 2:
            continue
        used = ((samples[-1]["utime_ticks"] + samples[-1]["stime_ticks"])
                - (samples[0]["utime_ticks"] + samples[0]["stime_ticks"]))
        cpu = 100 * (used / clock_tick) / wallclock_time
        rss = max(s.get("rss_kb", 0) for s in samples)
        per_group[meta[pid]["group"]].append((cpu, rss, meta[pid]))

    system_procs = per_group.get("system", [])
    if system_procs:
        system_procs.sort(key=lambda x: -x[0])
        busiest = system_procs[0]
        resource_stats["sys_cpu_sum_pct"] = sum(cpu_pct for cpu_pct, _, _ in system_procs)
        resource_stats["sys_cpu_max_pct"] = busiest[0]
        resource_stats["sys_cpu_max_node"] = f"{busiest[2]['role']}/{busiest[2]['node_id']}"
        resource_stats["sys_rss_sum_mb"] = sum(rss_mb for _, rss_mb, _ in system_procs) / 1024
        resource_stats["sys_rss_max_mb"] = max(rss_mb for _, rss_mb, _ in system_procs) / 1024
        resource_stats["sys_processes"] = len(system_procs)

    harness = per_group.get("harness", [])
    if harness:
        sender = max(harness, key=lambda x: x[0])
        resource_stats["sender_cpu_pct"] = sender[0]
        resource_stats["sender_rss_mb"] = sender[1] / 1024
    return resource_stats


# parse written records (queue depths, message drops) from records/<system>/node-*.jsonl
def parse_records(run_dir: Path, system: str, window: tuple[int, int] | None) -> dict:

    records_dir = run_dir / "records" / system
    record_files = sorted(records_dir.glob("node-*.jsonl")) if records_dir.exists() else []
    if not record_files:
        return {"records_present": False}

    queues = ("event_q", "loopback_q", "result_sink_q", "gossip_out_q", "gossip_in_q")
    peak: dict[str, float] = {}
    capacity: dict[str, int] = {}
    worst_node: dict[str, str] = {}
    drops: dict[str, int] = defaultdict(int)
    incomplete: list[str] = []

    for path in record_files:
        last_type = None
        for row in read_jsonl(path):
            last_type = row.get("type")
            wallclock_time = row.get("timestamp_wallclock_us")
            if window and wallclock_time is not None and not (window[0] <= wallclock_time <= window[1]):
                continue
            kind = row.get("type")
            if kind == "queues":
                for queue in queues:
                    value = row.get(queue)
                    if not value:
                        continue
                    # [current fill, max since last report, max capacity]
                    if value[1] > peak.get(queue, -1):
                        peak[queue] = value[1]
                        worst_node[queue] = path.stem
                    capacity[queue] = value[2]
            elif kind == "message_drop":
                drops[row.get("reason") or "unknown"] += 1
        if last_type != "writer_shutdown":
            incomplete.append(path.name)

    records_stats: dict = {"records_present": True, "record_files": len(record_files), "incomplete_record_files": incomplete, 
                           "message_drops": sum(drops.values()), "drop_reasons": dict(drops)}
    worst_fill = 0.0
    for queue in queues:
        if queue not in peak:
            continue
        cap = capacity.get(queue) or 0
        fill = peak[queue] / cap if cap else 0.0
        records_stats[f"{queue}_peak"] = peak[queue]
        records_stats[f"{queue}_capacity"] = cap
        records_stats[f"{queue}_fill"] = fill
        records_stats[f"{queue}_worst_node"] = worst_node[queue]
        worst_fill = max(worst_fill, fill)
    records_stats["queue_fill_max"] = worst_fill
    return records_stats


# collect stats for a specific systems run
def collect_point(run_dir: Path, system: str, x_param: str) -> dict | None:
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
    if timeline.get("send_start_us") and timeline.get("send_end_us"):
        window = (timeline["send_start_us"], timeline["send_end_us"])

    point: dict = {
        "run_id": run_file.get("run_id"),
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
        "settlement_reason": timeline.get("system_settlement_reason"),
        "settlement_s": timeline.get("system_settlement_seconds"),
        "seconds_to_all_expected": timeline.get("seconds_to_all_expected"),
        "pairs_in_db": timeline.get("pairs_in_db"),
        "expected_pairs": timeline.get("expected_pairs"),
        "run_problems": list(run_file.get("problems") or []) + list(timeline.get("problems") or []),
    }
    point.update(parse_sender(run_dir, system))
    point.update(parse_send_log(run_dir, system))
    point.update(parse_resources(run_dir, system, window))
    point.update(parse_records(run_dir, system, window))

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
def classify(point: dict, thresholds: dict) -> dict:

    reasons: list[str] = []

    blocked = point.get("blocked_sends")
    sent = point.get("events_sent")
    blocked_fraction = (blocked / sent) if (blocked is not None and sent) else None
    max_send = point.get("max_send_ms")
    if blocked_fraction is not None and blocked_fraction > thresholds["blocked_frac"]:
        reasons.append(f"blocked_sends={blocked} ({blocked_fraction:.2%} of sends) > {thresholds['blocked_frac']:.2%} - a node is not keeping up with its socket")
    if max_send is not None and max_send > thresholds["max_send_ms"]:
        reasons.append(f"max_send={max_send:.1f}ms > {thresholds['max_send_ms']}ms - a node stopped reading its socket long enough to block the sender")

    ratio = point.get("achieved_ratio")
    if ratio is not None and ratio < thresholds["achieved_ratio"]:
        reasons.append(f"achieved/target={ratio:.3f} < {thresholds['achieved_ratio']}")

    if point.get("message_drops"):
        reasons.append(f"message_drops={point['message_drops']} {point.get('drop_reasons')}")

    fill = point.get("queue_fill_max")
    if fill is not None and fill > thresholds["queue_fill"]:
        worst = max(((point.get(f"{queue}_fill") or 0, queue) for queue in
                     ("event_q", "loopback_q", "result_sink_q", "gossip_out_q", "gossip_in_q")), default=(0, "?"))
        reasons.append(f"peak {worst[1]} depth at {fill:.0%} of capacity > {thresholds['queue_fill']:.0%}")

    cpu = point.get("sys_cpu_max_pct")
    if cpu is not None and cpu > thresholds["cpu_pct"]:
        reasons.append(f"busiest process {point.get('sys_cpu_max_node')} at {cpu:.0f}% of a core "
                       f"> {thresholds['cpu_pct']}%")

    # not a saturation signal
    invalid: list[str] = []
    if point.get("hard_killed"):
        invalid.append(f"{point['hard_killed']} node(s) SIGKILLed")
    if point.get("expected_pairs") and (point.get("pairs_in_db") or 0) < point["expected_pairs"]:
        invalid.append(f"{point.get('pairs_in_db')}/{point['expected_pairs']} pairs at settlement")
    if point.get("incomplete_record_files"):
        invalid.append(f"{len(point['incomplete_record_files'])} truncated record file(s)")
    if point.get("status") == "invalid":
        invalid.append("run.json status=invalid")

    # check if the sender is the limit
    warnings: list[str] = []
    sender_cpu = point.get("sender_cpu_pct")
    if sender_cpu is not None and sender_cpu > thresholds["cpu_pct"]:
        warnings.append(f"SENDER at {sender_cpu:.0f}% of a core - the load generator may be the limit here, not the system under test")
    elif sender_cpu is None and point.get("target_rate"):
        warnings.append("sender cpu not sampled (old run) - cannot rule out a load generator limit")

    return {"saturated": bool(reasons), "reasons": reasons, "invalid": invalid, "warnings": warnings}


def fmt(value, spec="", dash="-"):
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return dash
    try:
        return format(value, spec)
    except (TypeError, ValueError):
        return str(value)


def print_system_table(system: str, points: list[dict], x_param: str) -> None:
    print()
    print("=" * 132)
    print(f"  {system.upper()}   ({len(points)} point(s), x = {x_param})")
    print("=" * 132)
    header = (f"{x_param:>8} {'eff/tgt':>8} {'eff/s':>8} {'blkd':>5} {'maxsnd':>7} "
              f"{'late%':>6} {'resync':>7} {'qfill':>6} {'drops':>6} "
              f"{'cpu_max':>8} {'cpu_sum':>8} {'busiest':>16} {'sender':>7} {'rss_mb':>7} {'acc':>7}  verdict")
    print(header)
    print("-" * 132)
    for point in points:
        verdict = point["_class"]
        if verdict["invalid"]:
            tag = "INVALID"
        elif verdict["saturated"]:
            tag = "SATURATED"
        else:
            tag = "ok"
        print(f"{fmt(point.get('x')):>8} "
              f"{fmt(point.get('achieved_ratio'), '.3f'):>8} "
              f"{fmt(point.get('effective_rate'), '.0f'):>8} "
              f"{fmt(point.get('blocked_sends')):>5} "
              f"{fmt(point.get('max_send_ms'), '.1f'):>7} "
              f"{fmt(100 * point['late_frac'] if point.get('late_frac') is not None else None, '.1f'):>6} "
              f"{fmt(point.get('resyncs')):>7} "
              f"{fmt(point.get('queue_fill_max'), '.0%'):>6} "
              f"{fmt(point.get('message_drops')):>6} "
              f"{fmt(point.get('sys_cpu_max_pct'), '.1f'):>8} "
              f"{fmt(point.get('sys_cpu_sum_pct'), '.1f'):>8} "
              f"{fmt(point.get('sys_cpu_max_node')):>16} "
              f"{fmt(point.get('sender_cpu_pct'), '.1f'):>7} "
              f"{fmt(point.get('sys_rss_sum_mb'), '.0f'):>7} "
              f"{fmt(point.get('accuracy'), '.4f'):>7}  {tag}")
        for reason in verdict["reasons"]:
            print(f"{'':>8}   -> {reason}")
        for reason in verdict["invalid"]:
            print(f"{'':>8}   !! {reason}")
        for warning in verdict["warnings"]:
            print(f"{'':>8}   ?? {warning}")


def summarise_system(system: str, points: list[dict], x_param: str) -> dict:
    usable = [point for point in points if not point["_class"]["invalid"] and point.get("x") is not None]
    usable.sort(key=lambda point: point["x"])
    saturating = [point for point in usable if point["_class"]["saturated"]]

    # find sustained saturation
    first_sustained = None
    for index, point in enumerate(usable):
        if point["_class"]["saturated"] and all(usable_point["_class"]["saturated"] for usable_point in usable[index:]):
            first_sustained = point
            break
    outliers = [saturating_point for saturating_point in saturating if first_sustained is None or saturating_point["x"] < first_sustained["x"]]

    sustainable = None
    if first_sustained is not None:
        below = [point for point in usable if point["x"] < first_sustained["x"] and not point["_class"]["saturated"]]
        sustainable = max((point["x"] for point in below), default=None)
    elif usable:
        sustainable = max(point["x"] for point in usable)

    invalid_points = [point["run_id"] for point in points if point["_class"]["invalid"]]
    tested_max = max((point["x"] for point in points if point.get("x") is not None), default=None)
    usable_max = max((point["x"] for point in usable), default=None)

    summary = {
        "system": system,
        "points": len(points),
        "usable_points": len(usable),
        "invalid_points": invalid_points,
        "first_sustained_saturating_x": first_sustained["x"] if first_sustained else None,
        "first_sustained_saturating_reasons": first_sustained["_class"]["reasons"] if first_sustained else [],
        "isolated_saturating_x": [p["x"] for p in outliers],
        "highest_sustainable_x": sustainable,
        "highest_tested_x": tested_max,
        "highest_usable_x": usable_max,
        "saturation_bracketed": first_sustained is not None,
        "base_point_60pct": round(sustainable * 0.6) if sustainable else None,
    }

    print()
    print(f"  {system}:")
    if not usable:
        print("    no usable points - every run was invalid, fix those first")
        return summary
    if outliers:
        print(f"    isolated saturating point(s) at {summary['isolated_saturating_x']} with healthy points "
              f"above them - treated as outliers, not as the knee.")
        print(f"    repeat those with more replicates before trusting or discarding them.")
    if first_sustained is None:
        print(f"    NOT SATURATED anywhere in the tested range. highest tested {x_param} = {sustainable}.")
        print(f"    the saturation point is ABOVE the grid - extend it before reading a base point off this.")
        # a rudimentary linear extrapolation of cpu
        fit = extrapolate_cpu(usable)
        if fit:
            print(f"    linear fit of the busiest process: cpu% = {fit['intercept']:.1f} + "
                  f"{fit['slope'] * 1000:.2f} per 1000 {x_param}  ->  100% of a core near "
                  f"{x_param} = {fit['x_at_100']:,.0f}")
            print(f"    (extrapolation only, and 100% of one core is not necessarily the ceiling for a "
                  f"multi threaded process - treat it as where to aim the next grid)")
            summary["cpu_fit"] = fit
    else:
        print(f"    saturation starts at {x_param} = {first_sustained['x']} and holds above it")
        for reason in first_sustained["_class"]["reasons"]:
            print(f"      because: {reason}")
        print(f"    highest sustainable {x_param} = {sustainable}")
        if summary["base_point_60pct"]:
            print(f"    60% base point = {summary['base_point_60pct']}")
    if invalid_points:
        print(f"    !! {len(invalid_points)} invalid point(s) excluded: {invalid_points}")
        if usable_max is not None and tested_max is not None and usable_max < tested_max:
            print(f"    !! the top of the grid ({tested_max}) is invalid, so the usable range only "
                  f"reaches {usable_max}. re-run the invalid points before concluding anything about "
                  f"the saturation point.")
            summary["saturation_bracketed"] = False
    return summary


def extrapolate_cpu(points: list[dict]) -> dict | None:
    pairs = [(point["x"], point["sys_cpu_max_pct"]) for point in points
             if point.get("x") is not None and point.get("sys_cpu_max_pct") is not None]
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
    return {"slope": slope, "intercept": intercept, "x_at_100": (100 - intercept) / slope}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("experiment_dir", type=Path, help="results/<experiment_id>")
    parser.add_argument("--x-param", default="rate", help="the swept parameter (default: rate)")
    parser.add_argument("--systems", nargs="*", default=list(SYSTEMS))
    parser.add_argument("--achieved-ratio", type=float, default=0.95, help="saturated below this effective/target ratio (default 0.95)")
    parser.add_argument("--cpu-threshold", type=float, default=85.0, help="saturated above this cpu%% of one core on the busiest process (default 85)")
    parser.add_argument("--queue-fill", type=float, default=0.25, help="saturated above this peak queue depth / capacity (default 0.25)")
    parser.add_argument("--blocked-frac", type=float, default=0.001, help="saturated above this fraction of sends blocked >1ms (default 0.001)")
    parser.add_argument("--max-send-ms", type=float, default=50.0, help="saturated above this single worst socket write in ms (default 50)")
    parser.add_argument("--include-invalid", action="store_true", help="keep runs that were SIGKILLed or short on results in the saturation search")
    parser.add_argument("--json", type=Path, default=None, help="write the full per point data here")
    parser.add_argument("--csv", type=Path, default=None, help="write a flat per point table here")
    args = parser.parse_args()

    thresholds = {"achieved_ratio": args.achieved_ratio, "cpu_pct": args.cpu_threshold, "queue_fill": args.queue_fill, "blocked_frac": args.blocked_frac,
                  "max_send_ms": args.max_send_ms}

    run_directories = sorted(entry for entry in args.experiment_dir.iterdir() if (entry / "run.json").exists()) \
        if args.experiment_dir.is_dir() else []
    if not run_directories:
        print(f"no runs with a run.json under {args.experiment_dir}", file=sys.stderr)
        return 0

    points_by_system: dict[str, list[dict]] = {system: [] for system in args.systems}
    for run_directory in run_directories:
        for system in args.systems:
            point = collect_point(run_directory, system, args.x_param)
            if point is None:
                continue
            point["_class"] = classify(point, thresholds)
            if args.include_invalid:
                point["_class"]["invalid"] = []
            points_by_system[system].append(point)

    for points in points_by_system.values():
        points.sort(key=lambda point: (point.get("x") is None, point.get("x"), point.get("run_id")))

    all_points = [point for points in points_by_system.values() for point in points]

    print(f"experiment: {args.experiment_dir}")
    print(f"rule: saturated if blocked_sends>{args.blocked_frac:.2%} of sends OR max_send>{args.max_send_ms}ms "
          f"OR achieved/target<{args.achieved_ratio} OR any message_drop OR peak queue "
          f">{args.queue_fill:.0%} of capacity OR busiest process >{args.cpu_threshold}% of a core")
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
            print_system_table(system, points_by_system[system], args.x_param)

    print()
    print("=" * 132)
    print("  SUMMARY PER SYSTEM")
    print("=" * 132)
    summaries_by_system = {system: summarise_system(system, points_by_system[system], args.x_param)
                           for system in args.systems if points_by_system[system]}

    print()
    unbracketed_systems = [system for system, summary in summaries_by_system.items()
                           if not summary.get("saturation_bracketed")]
    if unbracketed_systems:
        print(f"  NO BASE POINT YET: {', '.join(unbracketed_systems)} never saturated (or the top of the grid is invalid).")
        print(f"  a '60% of sustainable' base point read off an unsaturated sweep is just 60% of wherever you stopped testing,")
        print(f"  not 60% of what the system can do. extend the grid until every system shows a sustained saturating point.")
    else:
        sustainable_by_system = {system: summary["highest_sustainable_x"]
                                 for system, summary in summaries_by_system.items()
                                 if summary.get("highest_sustainable_x") is not None}
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

    report_payload = {
        "experiment": str(args.experiment_dir),
        "x_param": args.x_param,
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