#!/usr/bin/env python3
"""
run_simulations.py - execute a batch of measurement runs for the distributed online conformance checking system (both decentralized and central) 
and collect everything an evaluation needs

  python3.11 simulation/sim_run.py --plan simulation/experiments.toml
  python3.11 simulation/sim_run.py --plan simulation/experiments.toml --list
  python3.11 simulation/sim_run.py --plan simulation/experiments.toml --only throughput_ladder
  python3.11 simulation/sim_run.py --plan simulation/experiments.toml --only 'sens_*' --force

what one run does

  1. gen-config -> both systems from the same catalog into <run>/sys/
  2. patch configs -> the fixed baseline that will not change in the runs (includes configs like channel sizes, ttls and timeouts)
  3. generate event stream -> gen-events (synthetic) or a prepared log stream (sepsis), cached under <results>/_streams/ and shared between runs
  4. per system, one after the other:
        launch nodes -> wait ready -> idle window -> send event stream -> system settlement -> stop
        while /proc is being sampled (cpu and rss of every node process) and the nodes write measurements to <roor>/records/
  5. compare_results.py  -> correctness of one system against the other and the ground truth (expected_results.jsonl)
  6. run.json            -> the parametrisation and the timeline markers to cut the records into windows for evaluation

"""
from __future__ import annotations

import argparse
import atexit
import fnmatch
import itertools
import json
import os
import random
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from helpers import (
    CpuCap, cpu_cap_preexec, cpu_cap_prefix, parse_cpu_cap, ensure_cpu_cap_possible,
    remove_quota_cgroups, worker_threads_for_node, worker_threads_info,
    DEFAULT_STOP_GRACE_S, GROUP_TOOLING, GROUP_SYSTEM, NodeInfo, ProcSampler, assert_addresses_free,
    binary_cmd, check_records_complete, checkpoint_wal, db_progress, kill_stale_nodes,
    load_expected, load_system_configs, log, netem_apply, netem_clear, now_us, read_jsonl,
    register_process, remove_sockets, run_logged, stable_key, stop_all_launched,
    stop_processes, wait_until_ready, write_toml,
)

# used to detect if another simulation is currently running
LOCKFILE = Path("/tmp/cc-sim.lock")


# check if the process that owns the LOCKFILE is still alive
def _process_alive(pid: str) -> bool:

    if not pid.isdigit():
        return False
    try:
        os.kill(int(pid), 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # exists, owned by someone else


# load experiments.toml
def load_plan(path: Path) -> dict:
    import tomllib
    with open(path, "rb") as file:
        return tomllib.load(file)


# from experiment.grid (cartesian product) and experiment.sensitivity (one at a time) create the different parametrisations to run on after another
def expand_experiment(exp: dict, defaults: dict) -> list[dict]:

    base = {**defaults.get("params", {}), **exp.get("params", {})}
    points: list[dict] = []

    grid = exp.get("grid") or {}
    if grid:
        keys = sorted(grid)
        for combo in itertools.product(*(grid[key] for key in keys)):
            points.append({**base, **dict(zip(keys, combo))})

    sensitivity = exp.get("sensitivity") or {}
    if sensitivity:
        if exp.get("include_base", True):
            points.append(dict(base))
        for key in sorted(sensitivity):
            for value in sensitivity[key]:
                if base.get(key) == value:
                    continue  # already covered by the included base
                points.append({**base, key: value})

    # just use the base run if nothing is configured explicitly
    if not points:
        points.append(dict(base))

    runs = []
    seeds = exp.get("seeds", [defaults.get("seed", 1)])
    replicates = int(exp.get("replicates", 1))
    label_keys = _varying_keys(points)
    for point in points:
        for seed in seeds:
            for replicate in range(replicates):
                params = dict(point)
                params["seed"] = seed
                label = "_".join(f"{key}{_fmt(value)}" for key, value in sorted(point.items())
                             if key in label_keys)
                run_id = f"{label or 'base'}__seed{seed}"
                if replicates > 1:
                    run_id += f"__r{replicate}"
                runs.append({
                    "experiment": exp["id"],
                    "run_id": run_id,
                    "replicate": replicate,
                    "kind": exp.get("kind", "scalability"),
                    "systems": exp.get("systems", ["decentralized", "central"]),
                    "params": params,
                })
    return runs


def shuffle_run_order(runs: list[dict], run_order: str, system_order: str, order_seed: int) -> list[dict]:

    rng = random.Random(order_seed)

    if run_order == "shuffle":
        rng.shuffle(runs)
    elif run_order == "shuffle-within-experiment":
        by_experiment: dict[str, list[dict]] = {}
        for run in runs:
            by_experiment.setdefault(run["experiment"], []).append(run)
        runs = []
        for experiment in by_experiment:
            block = by_experiment[experiment]
            rng.shuffle(block)
            runs += block
    elif run_order != "as-listed":
        raise ValueError(f"unknown run_order {run_order!r} "
                         "(expected as-listed | shuffle | shuffle-within-experiment)")

    if system_order == "shuffle":
        for run in runs:
            systems = list(run["systems"])
            rng.shuffle(systems)
            run["systems"] = systems
    elif system_order != "as-listed":
        raise ValueError(f"unknown system_order {system_order!r} (expected as-listed | shuffle)")

    for index, run in enumerate(runs):
        run["order_index"] = index
        run["order_seed"] = order_seed
        run["run_order"] = run_order
        run["system_order"] = system_order
    return runs


def _fmt(s) -> str:
    return str(s).replace(".", "p").replace("/", "-").replace(" ", "")


def _varying_keys(points: list[dict]) -> list[str]:
    if len(points) <= 1:
        return sorted(points[0].keys() & {"rate", "nodes", "repeat", "cases"}) if points else []
    keys = set()
    first = points[0]
    for point in points[1:]:
        for key, value in point.items():
            if first.get(key) != value:
                keys.add(key)
    return sorted(keys)



class RunPaths:
    def __init__(self, results_dir: Path, experiment: str, run_id: str):
        self.root = results_dir / experiment / run_id
        self.sys = self.root / "sys"                        # gen-config --out-dir where the node configs are written to
        self.records = self.root / "records"                # CC_METRICS_DIR/<system> stores raw node measurements
        self.resources = self.root / "resources"            # where the /proc samples are written (resource sampling)
        self.send = self.root / "send"                      # where the log from send_event_stream.py is written
        self.spec = self.root / "run.json"                  # the run specification

    def system_root(self, system: str) -> Path:
        return self.sys / system

    def configs(self, system: str) -> Path:
        return self.system_root(system) / "configs"

    def logs(self, system: str) -> Path:
        return self.system_root(system) / "logs"

    def db(self, system: str) -> Path:
        return self.system_root(system) / "cc-results.db"

    def mkdirs(self) -> None:
        for p in (self.root, self.sys, self.records, self.resources, self.send):
            p.mkdir(parents=True, exist_ok=True)


def generate_configs(plan: dict, repo: Path, paths: RunPaths, params: dict) -> None:
    gen_config_cmd = binary_cmd(repo, plan["defaults"].get("bin_dir"), "gen-config",
                     plan["defaults"].get("use_cargo", False))
    baseline = plan.get("baseline") or {}
    if baseline:
        baseline_path = paths.root / "baseline.toml"
        write_toml(baseline_path, baseline)
        gen_config_cmd += ["--baseline", str(baseline_path.resolve())]
    gen_config_cmd += ["--out-dir", str(paths.sys.resolve()),
            "--catalog", params["catalog"],
            "--role-assignment", params["role_assignment"],
            "--repeat", params["repeat"],
            "--addressing", "local"]
    if params.get("nodes"):
        gen_config_cmd += ["--nodes", params["nodes"]]
    if params.get("forwarders"):
        gen_config_cmd += ["--forwarders", params["forwarders"]]
    # give the central system a separate sink process (extra node) so cpu without the sink is computable on both systems
    if params.get("external_central_sink"):
        gen_config_cmd += ["--external-central-sink"]
    # overlay and gossip params that gen-config exposes as flags
    for flag, key in (("--case-end-ttl", "case_end_ttl"),
                      ("--anti-entropy-secs", "anti_entropy_secs"),
                      ("--anti-entropy-fanout", "anti_entropy_fanout"),
                      ("--push-fanout", "push_fanout"),
                      ("--shuffle-secs", "shuffle_secs"),
                      ("--shuffle-length", "shuffle_length"),
                      ("--cache-size", "cache_size"),
                      ("--ring-degree", "ring_degree"),
                      ("--idle-timeout-secs", "idle_timeout_secs")):
        if params.get(key) is not None:
            gen_config_cmd += [flag, params[key]]
    run_logged(gen_config_cmd, repo, paths.root / "gen-config.out")



# returns all the components of the event stream (dir, manifest, expected_results.jsonl, number of events, number of cases)
def ensure_stream(plan: dict, repo: Path, paths: RunPaths, params: dict, kind: str) -> dict:

    streams = Path(plan["defaults"]["results_dir"]) / "_streams"
    streams.mkdir(parents=True, exist_ok=True)

    if kind == "privacy" or params.get("stream_dir"):
        # a prepared event stream (e.g. sepsis) - only the ground truth may have to be derived
        stream_dir = (repo / params["stream_dir"]).resolve()
        manifest = stream_dir / params.get("manifest", "manifest.jsonl")
        catalog_key = stable_key({"catalog": params["catalog"], "repeat": params["repeat"], "stream": str(stream_dir)})
        expected = streams / f"expected_{catalog_key}.jsonl"
        if not expected.exists():
            compute_expected_cmd = binary_cmd(repo, plan["defaults"].get("bin_dir"), "compute-expected-results", plan["defaults"].get("use_cargo", False))
            compute_expected_cmd += ["--config-dir", str(paths.configs("decentralized").resolve()), "--manifest", str(manifest), "--out", str(expected), "--summary"]
            run_logged(compute_expected_cmd, repo, paths.root / "compute-expected-results.out")
    else:
        # generate an event stream
        key = stable_key({key: params.get(key) for key in
                          ("catalog", "repeat", "cases", "violation_rate", "violation_sampling", "step_secs", "case_gap_secs", "seed", "start", "no_jitter")})
        stream_dir = streams / f"synthetic_{key}"
        manifest = stream_dir / "manifest.jsonl"
        expected = stream_dir / "expected_results.jsonl"
        if not manifest.exists():
            tmp = stream_dir.with_suffix(".tmp")
            shutil.rmtree(tmp, ignore_errors=True)
            gen_events_cmd = binary_cmd(repo, plan["defaults"].get("bin_dir"), "gen-events", plan["defaults"].get("use_cargo", False))
            gen_events_cmd += ["--config-dir", str(paths.configs("decentralized").resolve()),
                    "--out-dir", str(tmp),
                    "--cases", params["cases"],
                    "--violation-rate", params["violation_rate"],
                    "--violation-sampling", params.get("violation_sampling", "exact"),
                    "--step-secs", params.get("step_secs", 1.0),
                    "--case-gap-secs", params.get("case_gap_secs", 10.0),
                    "--seed", params["seed"]]
            if params.get("start"):
                gen_events_cmd += ["--start", params["start"]]
            if params.get("no_jitter"):
                gen_events_cmd += ["--no-jitter"]
            run_logged(gen_events_cmd, repo, paths.root / "gen-events.out")
            tmp.rename(stream_dir)

    events = sum(1 for _ in open(manifest))
    cases = len({row["case_id"] for row in read_jsonl(manifest)})
    return {"dir": str(stream_dir), "manifest": str(manifest), "expected": str(expected),
            "events": events, "cases": cases,
            "expected_pairs": len(load_expected(expected)) if Path(expected).exists() else None}


def launch_system(plan: dict, repo: Path, paths: RunPaths, system: str, params: dict) -> tuple[dict[int, subprocess.Popen], list[NodeInfo], CpuCap, dict]:
    nodes = load_system_configs(paths.configs(system))
    if not nodes:
        raise RuntimeError(f"no configs for {system} in {paths.configs(system)}")
    assert_addresses_free(nodes)
    remove_sockets(nodes)
    for suffix in ("", "-wal", "-shm"):
        Path(str(paths.db(system)) + suffix).unlink(missing_ok=True)
    paths.logs(system).mkdir(parents=True, exist_ok=True)

    env = {
        "RUST_LOG": params.get("rust_log", plan["defaults"].get("rust_log", "warn")),
        "RUST_LOG_FILE": params.get("rust_log_file", plan["defaults"].get("rust_log_file", "warn")),
    }
    if params.get("metrics", True):
        records_dir = (paths.records / system).resolve()
        records_dir.mkdir(parents=True, exist_ok=True)
        env["CC_METRICS_DIR"] = str(records_dir)
        env["CC_METRICS_MSGTRACE"] = "1" if params.get("msgtrace", True) else "0"
    node_bin = repo / plan["defaults"].get("node_bin", "target/release/node")
    if not node_bin.exists():
        raise RuntimeError(f"node binary {node_bin} not found - build with `cargo build --release`")

    cpu_cap = parse_cpu_cap(params, plan["defaults"])
    cpu_cap_info = ensure_cpu_cap_possible(cpu_cap, len(nodes))

    tokio_runtime_sizing = params.get("worker_threads", plan["defaults"].get("worker_threads"))
    worker_nodes = sum(1 for node in nodes if node.role not in cpu_cap.exclude_roles)
    cpu_cap_info.update(worker_threads_info(cpu_cap, tokio_runtime_sizing, worker_nodes))

    if cpu_cap.enabled:
        log(f"  cpu cap: {cpu_cap.mode}/{cpu_cap.scope} cores={cpu_cap.cores} "
            f"system_cpus={cpu_cap_info.get('system_cpus', '-')} "
            f"excluded={','.join(cpu_cap.exclude_roles)}+sender")
        if cpu_cap_info.get("note"):
            log(f"    note: {cpu_cap_info['note']}")
        if cpu_cap_info.get("warning"):
            log(f"    !! {cpu_cap_info['warning']}")
    log(f"  tokio workers: policy={cpu_cap_info['worker_threads_policy']} "
        f"per_node={cpu_cap_info['worker_threads_per_node']} "
        f"total={cpu_cap_info.get('worker_threads_total', '-')} "
        f"over {worker_nodes} node(s) "
        f"(usable cpus/node={cpu_cap_info['usable_cpus_per_node']}, cpu cap {cpu_cap.mode})")
    if cpu_cap_info.get("worker_threads_warning"):
        log(f"    !! {cpu_cap_info['worker_threads_warning']}")

    procs: dict[int, subprocess.Popen] = {}
    log_dir = paths.logs(system).resolve()
    log_dir.mkdir(parents=True, exist_ok=True)
    try:
        for index, node in enumerate(nodes):
            stderr_file = open(log_dir / f"node-{node.node_id}-{node.role}.stderr", "w")
            node_env = dict(env)
            workers = worker_threads_for_node(cpu_cap, tokio_runtime_sizing, worker_nodes, node.role)
            if workers is not None:
                node_env["CC_WORKER_THREADS"] = str(workers)
            proc = subprocess.Popen(
                cpu_cap_prefix(cpu_cap, node.role, index)
                + [str(node_bin), "-c", node.path, "-l", str(log_dir)],
                preexec_fn=cpu_cap_preexec(cpu_cap, system, node.role, index),
                cwd=str(repo), env={**os.environ, **node_env},
                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                stderr=stderr_file, start_new_session=True)
            procs[node.node_id] = proc
            register_process(proc)
            time.sleep(float(plan["defaults"].get("node_start_stagger_s", 0.05)))
    except BaseException:
        stop_processes(procs)
        raise
    return procs, nodes, cpu_cap, cpu_cap_info


def send_stream(plan: dict, repo: Path, paths: RunPaths, system: str, params: dict, stream: dict,
                sampler: ProcSampler | None = None, cap: CpuCap | None = None) -> dict:
    sender = repo / plan["defaults"].get("send_script", "send_event_stream.py")
    sender_prefix = cpu_cap_prefix(cap, "sender", 0, tooling=True) if cap else []
    send_stream_cmd = sender_prefix + [plan["defaults"].get("python", "python3.11"), str(sender),
           "--config-dir", str(paths.configs(system).resolve()),
           "--log", str((paths.send / f"{system}.log").resolve()),
           "--stream", stream["manifest"],
           "--progress", "off"]
    if params.get("rate"):
        send_stream_cmd += ["--rate", str(params["rate"])]
    if params.get("burst"):
        send_stream_cmd += ["--burst", str(params["burst"])]
    if params.get("send_jitter"):
        send_stream_cmd += ["--jitter", str(params["send_jitter"])]

    sender_pid: dict = {}

    def on_start(popen: subprocess.Popen) -> None:
        sender_pid["pid"] = popen.pid
        if sampler is not None:
            sampler.add_target(popen.pid, group=GROUP_TOOLING, node_id=-1, role="sender")

    stream_start_time = now_us()
    try:
        proc = run_logged(send_stream_cmd, repo, paths.send / f"{system}.stdout.txt", check=False, timeout=float(plan["defaults"].get("send_timeout_s", 7200)),
                          on_start=on_start)
    finally:
        if sampler is not None and "pid" in sender_pid:
            sampler.remove_target(sender_pid["pid"])
    stream_stop_time = now_us()
    return {"send_start_us": stream_start_time, "send_end_us": stream_stop_time,
            "sender_pid": sender_pid.get("pid"), "sender_returncode": proc.returncode,
            "send_script_stdout": proc.stdout.strip().splitlines()[-3:]}


# wait for the system to settle (it stops producing results for a configured amount of time) or time out
def wait_for_system_settlement(paths: RunPaths, system: str, expected_pairs: int | None, params: dict) -> dict:
 
    quiescence = float(params.get("quiescence_s", 20))
    max_wait_for_settlement = float(params.get("max_wait_for_settlement_s", 240))
    db = paths.db(system)
    start = time.monotonic()
    last_change = start
    last = db_progress(db)
    complete_at = None
    while True:
        time.sleep(1.0)
        current = db_progress(db)
        now = time.monotonic()
        if current != last:
            last, last_change = current, now
        if expected_pairs and current[0] >= expected_pairs and complete_at is None:
            complete_at = now
        if now - last_change >= quiescence:
            reason = "quiescence"
            break
        if now - start >= max_wait_for_settlement:
            reason = "timeout"
            break
    return {
        "system_settled_us": now_us(),
        "system_settlement_seconds": time.monotonic() - start,
        "system_settlement_reason": reason,
        "pairs_in_db": last[0],
        "rows_in_db": last[1],
        "expected_pairs": expected_pairs,
        "seconds_to_all_expected": (complete_at - start) if complete_at else None,
        "quiescence_s": quiescence,
    }


def run_one_system(plan: dict, repo: Path, paths: RunPaths, system: str, params: dict, stream: dict) -> dict:
    log(f"  --- {system} ---")
    timeline: dict = {"system": system}
    procs, nodes, cpu_cap, cpu_cap_info = launch_system(plan, repo, paths, system, params)
    timeline["cpu_cap"] = cpu_cap_info
    sampler = None
    try:
        timeline["launch_us"] = now_us()
        ready_s = wait_until_ready(nodes, procs, timeout=float(plan["defaults"].get("ready_timeout_s", 90)))
        timeline["ready_us"] = now_us()
        timeline["ready_seconds"] = ready_s

        sampler = ProcSampler(
            targets={process.pid: {"node_id": node.node_id, "role": node.role, "group": GROUP_SYSTEM} for node, process in ((node, procs[node.node_id]) for node in nodes)},
            out_path=paths.resources / f"{system}.jsonl",
            interval=float(plan["defaults"].get("resource_interval_s", 0.25)),
            per_thread=bool(plan["defaults"].get("resource_per_thread", True)))
        sampler.start()

        # idle window: overlay + gossip run without any workload -> idle cost baseline
        idle = float(params.get("idle_window_s", 20))
        timeline["idle_start_us"] = now_us()
        time.sleep(idle)
        timeline["idle_end_us"] = now_us()

        timeline.update(send_stream(plan, repo, paths, system, params, stream, sampler=sampler, cap=cpu_cap))
        timeline.update(wait_for_system_settlement(paths, system, stream.get("expected_pairs"), params))
    finally:
        problems: list[str] = []
        try:
            if sampler:
                sampler.stop()
                sampler.join(timeout=5)
        except Exception as error:
            log(f"  !! resource sampler did not shut down cleanly: {error!r}")

        grace = float(plan["defaults"].get("stop_grace_s", DEFAULT_STOP_GRACE_S))
        timeline["hard_killed_nodes"] = stop_processes(procs, grace=grace)
        timeline["stopped_us"] = now_us()

        # trigger writing wal to db (before grace runs out)
        timeline["wal_checkpoint"] = checkpoint_wal(paths.db(system))
        if not timeline["wal_checkpoint"].get("checkpointed", False):
            problems.append(f"{system}: wal checkpoint incomplete: {timeline['wal_checkpoint']}")

        if timeline["hard_killed_nodes"]:
            problems.append(f"{system}: {len(timeline['hard_killed_nodes'])} node(s) ignored SIGTERM for {grace}s and were SIGKILLed: {timeline['hard_killed_nodes']}")
            log(f"  !! nodes {timeline['hard_killed_nodes']} ignored SIGTERM and were killed; records and result rows of this system may be incomplete")

        if params.get("metrics", True):
            incomplete = check_records_complete(paths.records / system)
            timeline["incomplete_records"] = incomplete
            if incomplete:
                problems.append(f"{system}: {len(incomplete)} incomplete record file(s): {incomplete[:5]}")
                log(f"  !! {len(incomplete)} incomplete record file(s) in {paths.records / system}")

        remove_sockets(nodes)
        remove_quota_cgroups()
        for suffix in ("-wal", "-shm"):
            Path(str(paths.db(system)) + suffix).unlink(missing_ok=True)
        timeline["problems"] = problems
    timeline["nodes"] = [{"node_id": n.node_id, "role": n.role, "events": n.event_ids, "constraints": len(n.constraints)} for n in nodes]
    return timeline


# run compare_results.py
def compare(plan: dict, repo: Path, paths: RunPaths, stream: dict, systems: list[str]) -> dict:

    wanted = [system for system in ("decentralized", "central") if system in systems]
    present = [system for system in wanted if paths.db(system).exists()]
    missing = [system for system in wanted if system not in present]
    result: dict = {"systems": present, "missing_dbs": missing, "out": None, "returncode": None, "summary": None}
    if not present:
        return result

    compare_results_cmd = [plan["defaults"].get("python", "python3.11"), str(repo / plan["defaults"].get("compare_script", "compare_results.py"))]
    for system in present:
        compare_results_cmd += [f"--{system}", str(paths.db(system).resolve())]
    summary_path = paths.root / "compare_results.json"
    # drop a stale summary from an earlier (force=true) run of the same run_id
    summary_path.unlink(missing_ok=True)
    compare_results_cmd += ["--expected", stream["expected"], "--rank-by", "revision", "--rank-report", "--json", str(summary_path.resolve())]

    out = paths.root / "compare_results.out"
    proc = run_logged(compare_results_cmd, repo, out, check=False)
    result["out"] = str(out)
    result["returncode"] = proc.returncode
    result["ok"] = proc.returncode in (0, 1)
    if summary_path.exists():
        try:
            result["summary"] = json.loads(summary_path.read_text())
        except json.JSONDecodeError:
            pass
    return result


def execute_run(plan: dict, repo: Path, results_dir: Path, run: dict, force: bool) -> None:
    paths = RunPaths(results_dir, run["experiment"], run["run_id"])
    if paths.spec.exists() and not force:
        log(f"skip {run['experiment']}/{run['run_id']} (already done - set force=true to run anyway)")
        return
    log(f"=== {run['experiment']}/{run['run_id']} ===")
    if paths.root.exists() and force:
        shutil.rmtree(paths.root)
    paths.mkdirs()

    params = run["params"]
    run_record = {**run, "started_at_us": now_us(), "host": os.uname().nodename, "cpu_count": os.cpu_count(), "systems_timeline": [],
              "problems": [], "baseline": plan.get("baseline", {})}

    netem = params.get("netem_ms")
    try:
        generate_configs(plan, repo, paths, params)
        stream = ensure_stream(plan, repo, paths, params, run["kind"])
        run_record["stream"] = stream
        if netem:
            netem_apply(float(netem), float(params.get("netem_jitter_ms", 0)), dev=plan["defaults"].get("netem_dev", "lo"),
                        sudo=plan["defaults"].get("netem_sudo", True))
        for system in run["systems"]:
            run_record["systems_timeline"].append(
                run_one_system(plan, repo, paths, system, params, stream))
            time.sleep(float(plan["defaults"].get("between_systems_s", 5)))
        comparison = compare(plan, repo, paths, stream, run["systems"])
        run_record["compare"] = comparison["out"]
        run_record["compare_returncode"] = comparison["returncode"]
        run_record["compare_ok"] = comparison["ok"]
        run_record["compare_summary"] = comparison["summary"]
        if comparison["missing_dbs"]:
            log(f"  !! no result database for: {', '.join(comparison['missing_dbs'])}")
        if not comparison["ok"]:
            log(f"  !! compare_results.py could not run (exit {comparison['returncode']}), see {comparison['out']}")

        problems: list[str] = []
        for timeline in run_record["systems_timeline"]:
            problems += timeline.get("problems", [])
            expected = timeline.get("expected_pairs")
            pairs = timeline.get("pairs_in_db")
            if expected and pairs is not None and pairs < expected:
                problems.append(f"{timeline['system']}: only {pairs}/{expected} pairs in the db at settlement (reason: {timeline.get('system_settlement_reason')})")
            if timeline.get("system_settlement_reason") == "timeout":
                problems.append(f"{timeline['system']}: settlement timed out, the system never went quiet")
            if timeline.get("sender_returncode") not in (0, None):
                problems.append(f"{timeline['system']}: sender exited with {timeline['sender_returncode']}")
        if comparison["missing_dbs"]:
            problems.append(f"no result database for: {', '.join(comparison['missing_dbs'])}")
        if not comparison["ok"]:
            problems.append(f"compare_results.py failed with exit {comparison['returncode']}")

        run_record["problems"] = problems
        run_record["status"] = "invalid" if problems else "ok"
        if problems:
            log(f"  !! run marked INVALID ({len(problems)} problem(s)):")
            for problem in problems:
                log(f"       - {problem}")
    except Exception as exc:
        run_record["status"] = "failed"
        run_record["error"] = f"{type(exc).__name__}: {exc}"
        log(f"  !! run failed: {run_record['error']}")
    finally:
        if netem:
            netem_clear(dev=plan["defaults"].get("netem_dev", "lo"), sudo=plan["defaults"].get("netem_sudo", True))
        run_record["finished_at_us"] = now_us()
        paths.spec.write_text(json.dumps(run_record, indent=2))
    log(f"  -> {paths.root}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--plan", required=True, type=Path)
    ap.add_argument("--repo-root", type=Path, default=None, help="project root (default: from the plan)")
    ap.add_argument("--results-dir", type=Path, default=None)
    ap.add_argument("--only", action="append", default=[], help="experiment id glob (repeatable)")
    ap.add_argument("--run", action="append", default=[], help="run id glob (repeatable)")
    ap.add_argument("--list", action="store_true", help="print the expanded run list and exit")
    ap.add_argument("--kill-stale", action="store_true", help="SIGKILL any leftover node processes of this binary before starting. refused while another simulation holds the lock file")
    ap.add_argument("--force", action="store_true", help="re-run runs that already have a run.json")
    ap.add_argument("--run-order", choices=["as-listed", "shuffle", "shuffle-within-experiment"],
                    default=None, help="execution order of the runs (default: from the plan, else as-listed)")
    ap.add_argument("--system-order", choices=["as-listed", "shuffle"], default=None,
                    help="order of decentralized/central within one run (default: from the plan, else as-listed)")
    ap.add_argument("--order-seed", type=int, default=None,
                    help="seed for the order randomization (default: from the plan, else 0). "
                         "the same seed always reproduces the same order")
    args = ap.parse_args()

    plan = load_plan(args.plan)
    defaults = plan.setdefault("defaults", {})
    repo = (args.repo_root or Path(defaults.get("repo_root", "."))).resolve()
    results_dir = (args.results_dir or Path(defaults.get("results_dir", "results"))).resolve()
    defaults["results_dir"] = str(results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)

    runs: list[dict] = []
    for experiment in plan.get("experiment", []):
        if args.only and not any(fnmatch.fnmatch(experiment["id"], experiment_id_pattern) for experiment_id_pattern in args.only):
            continue
        runs += expand_experiment(experiment, defaults)
    if args.run:
        runs = [run for run in runs if any(fnmatch.fnmatch(run["run_id"], run_id_pattern) for run_id_pattern in args.run)]

    run_order = args.run_order or defaults.get("run_order", "as-listed")
    system_order = args.system_order or defaults.get("system_order", "as-listed")
    order_seed = args.order_seed if args.order_seed is not None else int(defaults.get("order_seed", 0))
    runs = shuffle_run_order(runs, run_order, system_order, order_seed)
    if run_order != "as-listed" or system_order != "as-listed":
        log(f"run_order={run_order} system_order={system_order} order_seed={order_seed}")

    if args.list or not runs:
        for run in runs:
            print(f"{run['order_index']:>4} {run['experiment']:<24} {run['run_id']:<40} "
                  f"{'+'.join(run['systems']):<28} {json.dumps(run['params'], sort_keys=True)}")
        print(f"\n{len(runs)} run(s)  [run_order={run_order} system_order={system_order} order_seed={order_seed}]")
        return 0

   # check if another simulation is still running
    if LOCKFILE.exists():
        holder = LOCKFILE.read_text().strip()
        if _process_alive(holder):
            print(f"another simulation is running (pid {holder}, lock file {LOCKFILE}). "
                  "the nodes use fixed /tmp socket paths and fixed ports, so runs cannot overlap.", file=sys.stderr)
            return 2
        log(f"removing stale lock file {LOCKFILE} (pid {holder or '?'} is no longer running)")
        LOCKFILE.unlink(missing_ok=True)

    if args.kill_stale:
        node_bin = repo / defaults.get("node_bin", "target/release/node")
        killed = kill_stale_nodes(node_bin)
        log(f"killed {killed} stale node process(es) matching {node_bin}")

    def emergency_stop(signum=None, _frame=None):
        alive = stop_all_launched()
        if alive:
            log(f"stopped {alive} node process(es) during shutdown")
        LOCKFILE.unlink(missing_ok=True)
        if signum is not None:
            raise KeyboardInterrupt

    # handle ctrl+c to stop the script
    def interrupt(_signum, _frame):
        log("interrupted - stopping all node processes")
        stop_all_launched(grace=1.0)
        LOCKFILE.unlink(missing_ok=True)
        os._exit(130)

    signal.signal(signal.SIGINT, interrupt)
    atexit.register(stop_all_launched)
    for catch in (signal.SIGTERM, signal.SIGHUP):
        try:
            signal.signal(catch, emergency_stop)
        except (ValueError, OSError):
            pass

    LOCKFILE.write_text(str(os.getpid()))
    started = time.time()
    try:
        for i, run in enumerate(runs, 1):
            log(f"[{i}/{len(runs)}] {run['experiment']}/{run['run_id']}")
            execute_run(plan, repo, results_dir, run, args.force)
    except BaseException as error:
        log(f"aborting: {type(error).__name__}: {error}")
        stop_all_launched()
        raise
        return 130
    finally:
        LOCKFILE.unlink(missing_ok=True)
    log(f"done in {(time.time() - started) / 60:.1f} min -> {results_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())