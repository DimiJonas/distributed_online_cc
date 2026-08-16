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
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from helpers import (
    NodeInfo, ProcSampler, assert_addresses_free, binary_cmd, db_progress, kill_stale_nodes,
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
        for combo in itertools.product(*(grid[k] for k in keys)):
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
            for rep in range(replicates):
                params = dict(point)
                params["seed"] = seed
                label = "_".join(f"{k}{_fmt(v)}" for k, v in sorted(point.items())
                             if k in label_keys)
                run_id = f"{label or 'base'}__seed{seed}"
                if replicates > 1:
                    run_id += f"__r{rep}"
                runs.append({
                    "experiment": exp["id"],
                    "run_id": run_id,
                    "replicate": rep,
                    "kind": exp.get("kind", "scalability"),
                    "systems": exp.get("systems", ["decentralized", "central"]),
                    "params": params,
                })
    return runs


def _fmt(s) -> str:
    return str(s).replace(".", "p").replace("/", "-").replace(" ", "")


def _varying_keys(points: list[dict]) -> list[str]:
    if len(points) <= 1:
        return sorted(points[0].keys() & {"rate", "nodes", "repeat", "cases"}) if points else []
    keys = set()
    first = points[0]
    for point in points[1:]:
        for k, v in point.items():
            if first.get(k) != v:
                keys.add(k)
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


def launch_system(plan: dict, repo: Path, paths: RunPaths, system: str, params: dict) -> tuple[dict[int, subprocess.Popen], list[NodeInfo]]:
    nodes = load_system_configs(paths.configs(system))
    if not nodes:
        raise RuntimeError(f"no configs for {system} in {paths.configs(system)}")
    assert_addresses_free(nodes)
    remove_sockets(nodes)
    for suffix in ("", "-wal", "-shm"):
        Path(str(paths.db(system)) + suffix).unlink(missing_ok=True)
    paths.logs(system).mkdir(parents=True, exist_ok=True)

    records_dir = (paths.records / system).resolve()
    records_dir.mkdir(parents=True, exist_ok=True)

    # set env variables to turn on measurements and set log level
    env = {
        "CC_METRICS_DIR": str(records_dir),
        "CC_METRICS_MSGTRACE": "1" if params.get("msgtrace", True) else "0",
        "RUST_LOG": params.get("rust_log", plan["defaults"].get("rust_log", "warn")),
        "RUST_LOG_FILE": params.get("rust_log_file", plan["defaults"].get("rust_log_file", "warn")),
    }
    node_bin = repo / plan["defaults"].get("node_bin", "target/release/node")
    if not node_bin.exists():
        raise RuntimeError(f"node binary {node_bin} not found - build with `cargo build --release`")

    procs: dict[int, subprocess.Popen] = {}
    log_dir = paths.logs(system).resolve()
    log_dir.mkdir(parents=True, exist_ok=True)
    try:
        for node in nodes:
            stderr_file = open(log_dir / f"node-{node.node_id}-{node.role}.stderr", "w")
            proc = subprocess.Popen(
                [str(node_bin), "-c", node.path, "-l", str(log_dir)],
                cwd=str(repo), env={**os.environ, **env},
                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                stderr=stderr_file, start_new_session=True)
            procs[node.node_id] = proc
            register_process(proc)
            time.sleep(float(plan["defaults"].get("node_start_stagger_s", 0.05)))
    except BaseException:
        stop_processes(procs)
        raise
    return procs, nodes


def send_stream(plan: dict, repo: Path, paths: RunPaths, system: str, params: dict, stream: dict) -> dict:
    sender = repo / plan["defaults"].get("send_script", "send_event_stream.py")
    send_stream_cmd = [plan["defaults"].get("python", "python3.11"), str(sender),
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
    t0 = now_us() # command start time
    proc = run_logged(send_stream_cmd, repo, paths.send / f"{system}.stdout.txt", check=False, timeout=float(plan["defaults"].get("send_timeout_s", 7200)))
    t1 = now_us() # command end time
    return {"send_start_us": t0, "send_end_us": t1, "sender_returncode": proc.returncode, "send_script_stdout": proc.stdout.strip().splitlines()[-3:]}


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
    procs, nodes = launch_system(plan, repo, paths, system, params)
    sampler = None
    try:
        timeline["launch_us"] = now_us()
        ready_s = wait_until_ready(nodes, procs, timeout=float(plan["defaults"].get("ready_timeout_s", 90)))
        timeline["ready_us"] = now_us()
        timeline["ready_seconds"] = ready_s

        sampler = ProcSampler(
            targets={process.pid: {"node_id": node.node_id, "role": node.role} for node, process in ((node, procs[node.node_id]) for node in nodes)},
            out_path=paths.resources / f"{system}.jsonl",
            interval=float(plan["defaults"].get("resource_interval_s", 0.25)))
        sampler.start()

        # idle window: overlay + gossip run without any workload -> idle cost baseline
        idle = float(params.get("idle_window_s", 20))
        timeline["idle_start_us"] = now_us()
        time.sleep(idle)
        timeline["idle_end_us"] = now_us()

        timeline.update(send_stream(plan, repo, paths, system, params, stream))
        timeline.update(wait_for_system_settlement(paths, system, stream.get("expected_pairs"), params))
    finally:
        try:
            if sampler:
                sampler.stop()
                sampler.join(timeout=5)
        except Exception as error:
            log(f"  !! resource sampler did not shut down cleanly: {error!r}")
        timeline["hard_killed_nodes"] = stop_processes(procs)
        timeline["stopped_us"] = now_us()
        if timeline["hard_killed_nodes"]:
            log(f"  !! nodes {timeline['hard_killed_nodes']} ignored SIGTERM and were killed; their last records may be incomplete")
        remove_sockets(nodes)
        for suffix in ("-wal", "-shm"):
            Path(str(paths.db(system)) + suffix).unlink(missing_ok=True)
    timeline["nodes"] = [{"node_id": n.node_id, "role": n.role, "events": n.event_ids, "constraints": len(n.constraints)} for n in nodes]
    return timeline


# run compare_results.py
def compare(plan: dict, repo: Path, paths: RunPaths, stream: dict, systems: list[str]) -> tuple[str | None, int | None]:

    if not all(paths.db(s).exists() for s in ("decentralized", "central")) or len(systems) < 2:
        return (None, None)
    compare_results_cmd = [plan["defaults"].get("python", "python3.11"),
           str(repo / plan["defaults"].get("compare_script", "compare_results.py")),
           "--decentralized", str(paths.db("decentralized").resolve()),
           "--central", str(paths.db("central").resolve()),
           "--expected", stream["expected"],
           "--rank-by", "revision", "--rank-report"]
    out = paths.root / "compare_results.out"
    proc = run_logged(compare_results_cmd, repo, out, check=False) # exit code 1 just means "found differences"
    return (str(out), proc.returncode)


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
              "baseline": plan.get("baseline", {})}

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
        run_record["compare"], run_record["compare_returncode"] = compare(
            plan, repo, paths, stream, run["systems"])
        run_record["status"] = "ok"
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

    if args.list or not runs:
        for run in runs:
            print(f"{run['experiment']:<24} {run['run_id']:<40} {json.dumps(run['params'], sort_keys=True)}")
        print(f"\n{len(runs)} run(s)")
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